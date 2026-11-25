defmodule Electric.Replication.Eval.RunnerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Support.DbSetup

  alias Electric.Replication.Eval.Runner
  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.SublinkKey
  alias Support.PgExpressionGenerator

  describe "record_to_ref_values/3" do
    test "should build ref values from record with known types and nils" do
      refs = %{
        ["string"] => :text,
        ["int"] => :int4,
        ["null_int"] => :int4
      }

      assert {:ok,
              %{
                ["string"] => "test",
                ["int"] => 5,
                ["null_int"] => nil
              }} ==
               Runner.record_to_ref_values(refs, %{
                 "string" => "test",
                 "int" => "5",
                 "null_int" => nil
               })
    end

    test "the parse warning names the column and the type, not the value" do
      {_result, log} =
        ExUnit.CaptureLog.with_log(fn ->
          Runner.record_to_ref_values(%{["age"] => :int4}, %{"age" => "row-value-7f3a"})
        end)

      refute log =~ "row-value-7f3a"
      assert log =~ ~s|Could not parse a value as :int4 while casting "age"|
    end

    test "parses uuid refs as compact 16-byte binaries" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      {:ok, uuid_bytes} = Ecto.UUID.dump(uuid)

      assert {:ok, %{["id"] => ^uuid_bytes}} =
               Runner.record_to_ref_values(%{["id"] => :uuid}, %{"id" => uuid})
    end

    test "keeps json and jsonb refs as their raw text representation" do
      refs = %{
        ["jsonb_object"] => :jsonb,
        ["jsonb_scalar"] => :jsonb,
        ["json_scalar"] => :json,
        ["absent"] => :jsonb
      }

      assert {:ok,
              %{
                ["jsonb_object"] => ~S|{"a": {"b": [1, 2]}}|,
                ["jsonb_scalar"] => "false",
                ["json_scalar"] => ~S|"A410B"|,
                ["absent"] => nil
              }} ==
               Runner.record_to_ref_values(refs, %{
                 "jsonb_object" => ~S|{"a": {"b": [1, 2]}}|,
                 "jsonb_scalar" => "false",
                 "json_scalar" => ~S|"A410B"|,
                 "absent" => nil
               })
    end

    test "keeps a jsonb ref holding JSON null distinct from SQL NULL" do
      assert {:ok, %{["meta"] => "null"}} =
               Runner.record_to_ref_values(%{["meta"] => :jsonb}, %{"meta" => "null"})
    end

    test "keeps bytea refs as their raw canonical hex representation" do
      refs = %{["payload"] => :bytea, ["absent"] => :bytea}

      assert {:ok, %{["payload"] => "\\x68656c6c6f", ["absent"] => nil}} ==
               Runner.record_to_ref_values(refs, %{
                 "payload" => "\\x68656c6c6f",
                 "absent" => nil
               })
    end
  end

  describe "a column value this environment cannot parse" do
    @refs %{["deletedAt"] => :timestamptz, ["metaJson"] => :jsonb}
    @record %{"deletedAt" => "infinity", "metaJson" => ~S|"D32"|}

    test "is carried as a marker rather than failing the whole row" do
      {:ok, refs} = Runner.record_to_ref_values(@refs, @record)

      assert %Runner.Unreadable{type: :timestamptz} = refs[["deletedAt"]]
      assert refs[["metaJson"]] == ~S|"D32"|
    end

    test "answers the null tests the way Postgres answers them" do
      assert {:ok, false} = run(~S|"deletedAt" IS NULL|)
      assert {:ok, true} = run(~S|"deletedAt" IS NOT NULL|)
    end

    test "lets a predicate whose answer does not need it through" do
      assert {:ok, true} = run(~S|"deletedAt" IS NULL OR "metaJson" IS NOT NULL|)
    end

    test "refuses every comparison against it instead of guessing" do
      # A sentinel that merely flows into these would be answered by Erlang's term order rather
      # than refused, and the answer would be silent and wrong.
      assert {:error, _} = run(~S|"deletedAt" > '2020-01-01 00:00:00+00'::timestamptz|)
      assert {:error, _} = run(~S|"deletedAt" = '2020-01-01 00:00:00+00'::timestamptz|)
      assert {:error, _} = run(~S|"deletedAt" < '2020-01-01 00:00:00+00'::timestamptz|)
    end

    test "refuses a numeric comparison, which nothing else would have refused" do
      # The timestamptz comparisons above are refused by their own implementation, which matches on
      # `%DateTime{}`. The numeric comparators are `Kernel` operators with no such pattern, so a
      # marker flowing into one is answered by Erlang's term order -- a map outranks an integer, so
      # `NaN > 5` would come back **true** and admit the row. This is the case the gate exists for.
      refs = %{["reading"] => :float8}
      record = %{"reading" => "NaN"}
      {:ok, ref_values} = Runner.record_to_ref_values(refs, record)

      result =
        ~S|"reading" > 5.0|
        |> Parser.parse_and_validate_expression!(refs: refs)
        |> Runner.execute(ref_values)

      assert {:error, _} = result
    end

    test "refuses a comparison even where negation would turn a false into inclusion" do
      assert {:error, _} = run(~S|NOT ("deletedAt" > '2020-01-01 00:00:00+00'::timestamptz)|)
    end

    test "is keyed by its text on the left of a composite `IN (SELECT ...)`" do
      refs = %{
        ["deletedAt"] => :timestamptz,
        ["divisionId"] => :int4,
        ["$sublink", "0"] => {:array, {:row, [:timestamptz, :int4]}}
      }

      {:ok, ref_values} =
        Runner.record_to_ref_values(refs, %{"deletedAt" => "infinity", "divisionId" => "4"})

      member? = fn _path, value ->
        send(self(), {:looked_up, value})
        true
      end

      result =
        ~S|("deletedAt", "divisionId") IN (SELECT d, b FROM tester)|
        |> Parser.parse_and_validate_expression!(
          refs: refs,
          sublink_queries: %{0 => "SELECT d, b FROM tester"}
        )
        |> Runner.execute(ref_values, subquery_member?: member?)

      assert {:ok, true} = result
      assert_received {:looked_up, {%SublinkKey.Text{text: "infinity"}, 4}}
    end

    test "refuses a composite `IN (SELECT ...)` carrying a value with no key, which a tuple hides from the scan" do
      # `(a, b) IN (SELECT ...)` evaluates its left side through `RowExpr`, which builds its value
      # with `List.to_tuple/1`. The scan that refuses unreadable arguments recognises the marker
      # directly or inside a list, and a tuple is neither -- so one rides into the set lookup, which
      # answers `false` for a value nothing could read and drops the row. A type with no text key
      # (see `SublinkKey`) still yields such a marker.
      refs = %{
        ["level"] => :int4,
        ["divisionId"] => :int4,
        ["$sublink", "0"] => {:array, {:row, [:int4, :int4]}}
      }

      {:ok, ref_values} =
        Runner.record_to_ref_values(refs, %{"level" => "not a number", "divisionId" => "4"})

      result =
        ~S|("level", "divisionId") IN (SELECT d, b FROM tester)|
        |> Parser.parse_and_validate_expression!(
          refs: refs,
          sublink_queries: %{0 => "SELECT d, b FROM tester"}
        )
        |> Runner.execute(ref_values, subquery_member?: fn _path, _value -> false end)

      assert {:error, _} = result
    end

    defp run(clause) do
      {:ok, refs} = Runner.record_to_ref_values(@refs, @record)

      clause
      |> Parser.parse_and_validate_expression!(refs: @refs)
      |> Runner.execute(refs)
    end
  end

  describe "execute/2" do
    test "should correctly execute constant expressions" do
      assert {:ok, true} =
               ~S"1::boolean"
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})
    end

    test "should correctly take refs" do
      assert {:ok, true} =
               ~S|"test"|
               |> Parser.parse_and_validate_expression!(refs: %{["test"] => :bool})
               |> Runner.execute(%{["test"] => true})
    end

    test "should correctly apply functions" do
      assert {:ok, 2} =
               ~S|"test" + 1|
               |> Parser.parse_and_validate_expression!(refs: %{["test"] => :int4})
               |> Runner.execute(%{["test"] => 1})
    end

    test "should not apply strict functions to nil values" do
      assert {:ok, nil} =
               ~S|"test" + 1|
               |> Parser.parse_and_validate_expression!(refs: %{["test"] => :int4})
               |> Runner.execute(%{["test"] => nil})
    end

    test "should return error on invalid function application instead of crashing" do
      assert {:error, %{args: ["test", 1]}} =
               ~S|"test" + 1|
               |> Parser.parse_and_validate_expression!(refs: %{["test"] => :int4})
               |> Runner.execute(%{["test"] => "test"})
    end

    test "can evaluate AND expression with multiple conditions" do
      assert {:ok, true} =
               ~S|test > 1 AND test = 2 AND test < 3|
               |> Parser.parse_and_validate_expression!(refs: %{["test"] => :int4})
               |> Runner.execute(%{["test"] => 2})
    end

    test "can evaluate OR expression with nil values" do
      for {foo, bar} <- [{1, nil}, {nil, 1}] do
        assert {:ok, true} =
                 ~S|foo = 1 OR bar = 1|
                 |> Parser.parse_and_validate_expression!(
                   refs: %{["foo"] => :int4, ["bar"] => :int4}
                 )
                 |> Runner.execute(%{["foo"] => foo, ["bar"] => bar})
      end

      for {foo, bar} <- [{2, nil}, {nil, 2}] do
        assert {:ok, nil} =
                 ~S|foo = 1 OR bar = 1|
                 |> Parser.parse_and_validate_expression!(
                   refs: %{["foo"] => :int4, ["bar"] => :int4}
                 )
                 |> Runner.execute(%{["foo"] => foo, ["bar"] => bar})
      end
    end

    test "can evaluate IN expression with nil values" do
      assert {:ok, true} =
               ~S|1 IN (NULL, 1)|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})

      assert {:ok, nil} =
               ~S|2 IN (1, NULL)|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})

      assert {:ok, nil} =
               ~S|NULL IN (NULL, 1)|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})
    end

    test "should work with array types" do
      assert {:ok, [[1, 2], [3, 4]]} =
               ~S|ARRAY[ARRAY[1, x], ARRAY['3', 2 + 2]]|
               |> Parser.parse_and_validate_expression!(refs: %{["x"] => :int4})
               |> Runner.execute(%{["x"] => 2})

      assert {:ok, true} =
               ~S|x @> ARRAY[y]|
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :int4},
                   ["y"] => :int4
                 }
               )
               |> Runner.execute(%{["y"] => 1, ["x"] => [1, 2]})

      assert {:ok, nil} =
               ~S|x @> ARRAY['value']|
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :text}
                 }
               )
               |> Runner.execute(%{["x"] => nil})

      assert {:ok, nil} =
               ~S|x @> ARRAY[y]|
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :int4},
                   ["y"] => :int4
                 }
               )
               |> Runner.execute(%{["y"] => 1, ["x"] => nil})

      assert {:ok, true} =
               ~S|x::float[] = y::int4[]::float[]|
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :int4},
                   ["y"] => :text
                 }
               )
               |> Runner.execute(%{["y"] => "{1,2}", ["x"] => [1, 2]})

      assert {:ok, true} =
               ~S|y = ANY (x)|
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :int4},
                   ["y"] => :int8
                 }
               )
               |> Runner.execute(%{["y"] => 1, ["x"] => [1, 2]})

      assert {:ok, [[1, 2], [3, 4]]} =
               ~S/(ARRAY[1] || ARRAY[2]) || x/
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :float8}
                 }
               )
               |> Runner.execute(%{["x"] => [[3, 4]]})

      assert {:error, _} =
               ~S/(ARRAY[1] || ARRAY[2]) || x/
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["x"] => {:array, :int4}
                 }
               )
               |> Runner.execute(%{["x"] => [[[3, 4]]]})
    end

    test "supports coalesce as a variadic function" do
      expr =
        ~S|coalesce("value", 'fallback', 'unused')|
        |> Parser.parse_and_validate_expression!(refs: %{["value"] => :text})

      assert {:ok, "fallback"} = Runner.execute(expr, %{["value"] => nil})
      assert {:ok, "present"} = Runner.execute(expr, %{["value"] => "present"})
    end

    test "coalesce short-circuits later arguments" do
      expr =
        ~S|coalesce("value", 1 / "divisor")|
        |> Parser.parse_and_validate_expression!(
          refs: %{["value"] => :int4, ["divisor"] => :int4}
        )

      assert {:ok, 10} = Runner.execute(expr, %{["value"] => 10, ["divisor"] => 0})

      assert {:error, _} =
               Runner.execute(expr, %{["value"] => nil, ["divisor"] => 0})
    end

    test "coalesce skips unreachable constant branches after a non-null fallback" do
      expr =
        ~S|coalesce("value", 1, 1 / "divisor")|
        |> Parser.parse_and_validate_expression!(
          refs: %{["value"] => :int4, ["divisor"] => :int4}
        )

      assert {:ok, 1} = Runner.execute(expr, %{["value"] => nil, ["divisor"] => 0})
    end

    test "supports greatest as a variadic function and ignores nil arguments" do
      expr =
        ~S|greatest("value", 2, 3, NULL::int4)|
        |> Parser.parse_and_validate_expression!(refs: %{["value"] => :int4})

      assert {:ok, 3} = Runner.execute(expr, %{["value"] => nil})
      assert {:ok, 4} = Runner.execute(expr, %{["value"] => 4})

      assert {:ok, nil} =
               ~S|greatest(NULL::int4, NULL::int4)|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})
    end

    test "supports least as a variadic function for datetime values" do
      expr =
        ~S|least("value", date '2024-01-02', NULL::date)|
        |> Parser.parse_and_validate_expression!(refs: %{["value"] => :date})

      assert {:ok, ~D[2024-01-02]} = Runner.execute(expr, %{["value"] => nil})
      assert {:ok, ~D[2024-01-01]} = Runner.execute(expr, %{["value"] => ~D[2024-01-01]})

      assert {:ok, nil} =
               ~S|least(NULL::date, NULL::date)|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})
    end

    test "greatest and least order intervals by their span, as Postgres does" do
      expr =
        ~S|greatest("value", interval '1 month') = interval '1 month' AND least("value", interval '1 month') = "value"|
        |> Parser.parse_and_validate_expression!(refs: %{["value"] => :interval})

      # A month counts as 30 days: 29 days is the smaller of the two, 31 days the larger.
      assert {{:ok, true}, {:ok, false}} =
               {Runner.execute(expr, %{["value"] => PgInterop.Interval.parse!("P29D")}),
                Runner.execute(expr, %{["value"] => PgInterop.Interval.parse!("P31D")})}
    end

    test "supports concat as a variadic text function, skipping NULL arguments" do
      assert {:ok, "ab"} =
               ~S|concat('a', 'b')|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})

      assert {:ok, "ab"} =
               ~S|concat('a', NULL::text, 'b')|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})

      assert {:ok, ""} =
               ~S|concat(NULL::text, NULL::text)|
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})

      expr =
        ~S|concat('x', "mid", 'z')|
        |> Parser.parse_and_validate_expression!(refs: %{["mid"] => :text})

      assert {:ok, "xbz"} = Runner.execute(expr, %{["mid"] => "b"})
      assert {:ok, "xz"} = Runner.execute(expr, %{["mid"] => nil})

      assert {:ok, true} =
               ~S|ilike(concat('%', "value", '%'), '%foo%')|
               |> Parser.parse_and_validate_expression!(refs: %{["value"] => :text})
               |> Runner.execute(%{["value"] => "foo"})

      assert {:ok, nil} =
               ~S{'a' || NULL::text || 'b'}
               |> Parser.parse_and_validate_expression!()
               |> Runner.execute(%{})
    end

    test "concat() with zero arguments is not a supported overload" do
      assert {:error, msg} = Parser.parse_and_validate_expression(~S|concat()|)
      assert msg =~ "unknown or unsupported function concat/0"
    end

    test "subquery" do
      assert {:ok, true} =
               ~S|test IN (SELECT val FROM tester)|
               |> Parser.parse_and_validate_expression!(
                 refs: %{["test"] => :int4, ["$sublink", "0"] => {:array, :int4}},
                 sublink_queries: %{0 => "SELECT val FROM tester"}
               )
               |> Runner.execute(%{["test"] => 4, ["$sublink", "0"] => MapSet.new([2, 3, 4])})
    end

    test "subquery with callback-backed membership" do
      expr =
        ~S|test IN (SELECT val FROM tester)|
        |> Parser.parse_and_validate_expression!(
          refs: %{["test"] => :int4, ["$sublink", "0"] => {:array, :int4}},
          sublink_queries: %{0 => "SELECT val FROM tester"}
        )

      subquery_member? = fn
        ["$sublink", "0"], 4 -> true
        ["$sublink", "0"], _ -> false
      end

      assert {:ok, true} =
               Runner.execute(expr, %{["test"] => 4}, subquery_member?: subquery_member?)

      assert {:ok, false} =
               Runner.execute(expr, %{["test"] => 5}, subquery_member?: subquery_member?)
    end

    test "subquery with row expression" do
      assert {:ok, true} =
               ~S|(test1, test2) IN (SELECT val1, val2 FROM tester)|
               |> Parser.parse_and_validate_expression!(
                 refs: %{
                   ["test1"] => :int4,
                   ["test2"] => :int4,
                   ["$sublink", "0"] => {:array, {:row, [:int4, :int4]}}
                 },
                 sublink_queries: %{0 => "SELECT val1, val2 FROM tester"}
               )
               |> Runner.execute(%{
                 ["test1"] => 4,
                 ["test2"] => 5,
                 ["$sublink", "0"] => MapSet.new([{2, 3}, {4, 5}])
               })
    end

    test "NOT IN (SELECT ...) with a NULL in the set excludes non-members instead of the whole table" do
      expr =
        ~S|test NOT IN (SELECT val FROM tester)|
        |> Parser.parse_and_validate_expression!(
          refs: %{["test"] => :int4, ["$sublink", "0"] => {:array, :int4}},
          sublink_queries: %{0 => "SELECT val FROM tester"}
        )

      # Postgres makes `5 NOT IN (2, 3, NULL)` unknown, so the row is excluded. Two-valued
      # membership answered `false` here, and `NOT false` admitted every root-table row.
      assert {:ok, nil} =
               Runner.execute(expr, %{["test"] => 5, ["$sublink", "0"] => MapSet.new([2, 3, nil])})

      # A real match is still a definite exclusion under NOT IN.
      assert {:ok, false} =
               Runner.execute(expr, %{["test"] => 2, ["$sublink", "0"] => MapSet.new([2, 3, nil])})

      # A NULL-free set keeps admitting genuine non-members, so NOT IN is not over-restricted.
      assert {:ok, true} =
               Runner.execute(expr, %{["test"] => 5, ["$sublink", "0"] => MapSet.new([2, 3])})
    end

    test "IN (SELECT ...) with a NULL in the set is unknown for a non-member" do
      expr =
        ~S|test IN (SELECT val FROM tester)|
        |> Parser.parse_and_validate_expression!(
          refs: %{["test"] => :int4, ["$sublink", "0"] => {:array, :int4}},
          sublink_queries: %{0 => "SELECT val FROM tester"}
        )

      assert {:ok, nil} =
               Runner.execute(expr, %{["test"] => 5, ["$sublink", "0"] => MapSet.new([2, 3, nil])})

      assert {:ok, true} =
               Runner.execute(expr, %{["test"] => 3, ["$sublink", "0"] => MapSet.new([2, 3, nil])})
    end
  end

  describe "execute_for_record/3" do
    test "evaluates null checks over jsonb refs" do
      expr =
        Parser.parse_and_validate_expression!(
          ~S|("deletedAt" IS NULL OR "metaJson" IS NOT NULL) AND "gradeId" IS NOT NULL|,
          refs: %{
            ["deletedAt"] => :timestamptz,
            ["metaJson"] => :jsonb,
            ["gradeId"] => :text
          }
        )

      base = %{"deletedAt" => nil, "metaJson" => nil, "gradeId" => "gr1"}

      live_with_meta = %{base | "metaJson" => ~S|"A410B"|}
      deleted_with_meta = %{base | "deletedAt" => "2026-08-12T14:00:00Z", "metaJson" => "false"}
      json_null_meta = %{base | "metaJson" => "null"}
      deleted_without_meta = %{base | "deletedAt" => "2026-08-12T14:00:00Z"}

      assert {:ok, true} = Runner.execute_for_record(expr, live_with_meta)
      assert {:ok, true} = Runner.execute_for_record(expr, deleted_with_meta)
      # a jsonb value of JSON null is not SQL NULL
      assert {:ok, true} = Runner.execute_for_record(expr, json_null_meta)
      assert {:ok, false} = Runner.execute_for_record(expr, deleted_without_meta)
    end
  end

  describe "an ARRAY[...] argument with a NULL element" do
    setup [:with_shared_db]

    # The NULL is an element of an array that is not NULL, so a strict function over it still runs.
    for {clause, x} <- [
          {"1 = ANY(ARRAY[x, NULL])", 1},
          {"1 = ANY(ARRAY[x, NULL])", 2},
          {"ARRAY[x, NULL] = '{1,NULL}'", 1},
          {"ARRAY[x, NULL] @> '{1}'", 1},
          {"array_ndims(ARRAY[x, NULL]) = 1", 1}
        ] do
      test "#{clause} with x = #{x}", %{pool: pool} do
        clause = unquote(clause)
        x = unquote(x)

        %{rows: [[expected]]} =
          Postgrex.query!(pool, "SELECT #{clause} FROM (SELECT $1::int4 AS x) r", [x])

        expr = Parser.parse_and_validate_expression!(clause, refs: %{["x"] => :int4})
        assert {clause, x, Runner.execute(expr, %{["x"] => x})} == {clause, x, {:ok, expected}}
      end
    end
  end

  describe "execute/2 against PG results" do
    setup [:with_shared_db]

    @max_runs 10_000
    @max_run_time 1_000

    property "numeric expressions", %{pool: pool} do
      check all(
              clause <- PgExpressionGenerator.numeric_expression(),
              max_runs: @max_runs,
              max_run_time: @max_run_time
            ) do
        assert_runner_and_oracle_match(clause, pool)
      end
    end

    property "string expressions", %{pool: pool} do
      check all(
              clause <- PgExpressionGenerator.string_expression(),
              max_runs: @max_runs,
              max_run_time: @max_run_time
            ) do
        assert_runner_and_oracle_match(clause, pool)
      end
    end

    property "bool expressions", %{pool: pool} do
      check all(
              clause <- PgExpressionGenerator.bool_expression(),
              max_runs: @max_runs,
              max_run_time: @max_run_time
            ) do
        assert_runner_and_oracle_match(clause, pool)
      end
    end

    property "complex bool expressions", %{pool: pool} do
      check all(
              clause <- PgExpressionGenerator.complex_bool_expression(),
              max_runs: @max_runs,
              max_run_time: @max_run_time
            ) do
        assert_runner_and_oracle_match(clause, pool)
      end
    end

    property "array expressions", %{pool: pool} do
      check all(
              clause <- PgExpressionGenerator.array_expression(),
              max_runs: @max_runs,
              max_run_time: @max_run_time
            ) do
        assert_runner_and_oracle_match(clause, pool)
      end
    end

    defp execute_oracle(clause, db_conn) do
      case Postgrex.query(db_conn, "SELECT #{clause}", []) do
        {:ok, %Postgrex.Result{rows: [[result]]}} -> {:ok, result}
        {:error, %Postgrex.Error{postgres: %{message: reason}}} -> {:error, reason}
      end
    end

    defp execute_runner(clause) do
      try do
        clause
        |> Parser.parse_and_validate_expression!()
        |> Runner.execute(%{})
      rescue
        err -> {:error, err}
      end
    end

    defp assert_runner_and_oracle_match(clause, db_conn) do
      oracle_result = execute_oracle(clause, db_conn)
      runner_result = execute_runner(clause)

      case {runner_result, oracle_result} do
        {{:ok, runner_val}, {:ok, oracle_val}} ->
          # Both results are ok, we can compare them
          oracle_val = cast_decimals_to_floats(oracle_val)

          if is_number(oracle_val) do
            assert_in_delta(runner_val, oracle_val, abs(0.01 * oracle_val))
          else
            assert runner_val == oracle_val,
                   """
                   MISMATCH!

                   SQL: #{clause}

                   runner returned: #{inspect(runner_val)}
                   oracle returned: #{inspect(oracle_val)}
                   """
          end

          :ok

        {{:error, _}, {:error, _}} ->
          # Both results are errors - which is fine
          :ok

        {{:ok, nil}, {:error, _}} ->
          # Runner coalescing to nil rather than erroring is not ideal,
          # but we can live with it
          :ok

        {{:ok, _}, {:error, "value out of range:" <> _}} ->
          # We are fine with being able to handle values the oracle cannot
          :ok

        {{:ok, _}, {:error, "integer out of range"}} ->
          # We are fine with being able to handle values the oracle cannot
          :ok

        {{:error, err}, {:ok, _}} ->
          raise "Runner error: #{inspect(err)} for clause: #{clause}"

        {{:ok, _}, {:error, err}} ->
          raise "Oracle error: #{inspect(err)} for clause: #{clause}"
      end
    end

    defp cast_decimals_to_floats(value) when is_list(value) do
      Enum.map(value, &cast_decimals_to_floats/1)
    end

    defp cast_decimals_to_floats(%Decimal{} = decimal), do: Decimal.to_float(decimal)
    defp cast_decimals_to_floats(value), do: value
  end
end
