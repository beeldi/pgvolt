defmodule Electric.Shapes.Filter.InclusionIndexRoutingTest do
  use Support.TransactionCase, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["t1"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "an_array", array_type: "int8"},
                 %{name: "other_array", array_type: "float8"},
                 %{name: "numbers", array_type: "numeric"},
                 %{name: "words", array_type: "text"},
                 %{name: "times", array_type: "time"}
               ]
             )

  test "long arrays bound probe work and route exactly", %{db_conn: conn} do
    pairs = for i <- 1..160, do: {i, "an_array @> '{#{i},2048}'"}

    specs =
      pairs ++
        [
          {:root, "an_array @> '{}'"},
          {:intermediate, "an_array @> '{2,2}'"},
          {:leaf, "an_array @> '{{3,4},{5,6}}'"},
          {:shared_path, "an_array @> '{6,5,4,3}'"},
          {:nested, "an_array @> '{2}' AND other_array @> '{7}'"},
          {:nested_miss, "an_array @> '{2}' AND other_array @> '{9}'"},
          {:nested_same_field, "an_array @> '{2}' AND an_array @> '{2049}'"},
          {:other_field, "other_array @> '{2048}'"},
          {:missing, "an_array @> '{2,2049}'"}
        ]

    record = %{"an_array" => array(1..2048), "other_array" => "{7}"}
    fields = [an_array: "int8[]", other_array: "float8[]"]
    expected = postgres_matches(conn, specs, record, fields)

    filter = build_filter(specs, false)
    assert route(filter, record) == {expected, true}
    :erlang.garbage_collect()
    {:reductions, before} = Process.info(self(), :reductions)
    assert route(filter, record) == {expected, true}
    {:reductions, after_route} = Process.info(self(), :reductions)
    assert after_route - before < 500_000

    memo = build_filter(specs, true)
    assert route(memo, record) == {expected, true}
    assert route(memo, record) == {expected, true}

    for filter <- [filter, memo] do
      unreadable = %{record | "other_array" => "{NaN}"}
      postgres = postgres_matches(conn, specs, unreadable, fields)
      widened = MapSet.union(postgres, MapSet.new([:nested, :nested_miss, :other_field]))
      assert route(filter, unreadable) == {widened, false}
      assert route(filter, unreadable) == {widened, false}

      removed = [:intermediate, :leaf]
      filter = Enum.reduce(removed, filter, &Filter.remove_shape(&2, &1))
      remaining = Enum.reject(specs, fn {id, _} -> id in removed end)

      assert route(filter, record) == {postgres_matches(conn, remaining, record, fields), true}

      record = %{record | "an_array" => "{}"}
      assert route(filter, record) == {postgres_matches(conn, remaining, record, fields), true}
    end
  end

  test "paths off the row's walk do not add to what routing the row costs" do
    pairs = for i <- 1..50, do: {{:pair, i}, "an_array @> '{#{i},129}'"}
    specs = [{:empty, "an_array @> '{}'"} | pairs]
    singletons = for i <- 1_001..11_000, do: {{:singleton, i}, "an_array @> '{#{i}}'"}

    # The pairs make the walk outgrow the row's fixed allowance. The singletons share the field
    # but no element with the row, so a scan of the field's paths costs more than the walk.
    record = %{"an_array" => array(1..128)}
    expected = {MapSet.new([:empty]), true}
    narrow = build_filter(specs, false)
    wide = build_filter(specs ++ singletons, false)
    assert route(narrow, record) == expected
    assert route(wide, record) == expected

    assert route_reductions(wide, record) < 1.5 * route_reductions(narrow, record)
  end

  # Hashing a long binary costs time but no reductions, so this compares times. The scan used to
  # hash every element on the field and took about five times as long with long elements, and
  # still did for elements as long as one of the row's.
  @tag :performance
  test "long elements off the row's walk do not slow the path scan" do
    word = fn i -> String.pad_leading(Integer.to_string(i), 4, "0") end
    pairs = for i <- 1..50, do: {{:pair, i}, "words @> '{#{word.(i)},0129}'"}
    specs = [{:empty, "words @> '{}'"} | pairs]

    [short, long] =
      for size <- [8, 8_504] do
        singletons =
          for i <- 1..2_500,
              do: {{:singleton, i}, "words @> '{#{String.pad_trailing("z#{i}", size, "x")}}'"}

        elements = Enum.map(1..128, word) ++ [String.pad_trailing("!", size, "x")]
        record = %{"words" => "{" <> Enum.join(elements, ",") <> "}"}

        filter = build_filter(specs ++ singletons, false)
        assert route(filter, record) == {MapSet.new([:empty]), true}
        Enum.min(for _ <- 1..15, do: elem(:timer.tc(fn -> route(filter, record) end), 0))
      end

    assert long < 3 * short
  end

  test "NULL array elements never match a containment element", %{db_conn: conn} do
    specs = [
      {:empty, "an_array @> '{}'"},
      {:value, "an_array @> '{2}'"},
      {:null, "an_array @> '{NULL}'"},
      {:value_and_null, "an_array @> '{2,NULL}'"},
      {:text_null, ~s|words @> '{"NULL"}'|},
      {:sql_null, "words @> '{NULL}'"}
    ]

    fields = [an_array: "int8[]", words: "text[]"]
    record = %{"an_array" => "{{2,NULL},{1,2}}", "words" => ~s|{NULL,"NULL"}|}
    expected = postgres_matches(conn, specs, record, fields)

    for memo? <- [false, true] do
      filter = build_filter(specs, memo?)
      assert route(filter, record) == {expected, true}
      assert route(filter, record) == {expected, true}
    end
  end

  test "a negative numeric too small for a float does not match zero", %{db_conn: conn} do
    # Postgres has no negative numeric zero, so a `-0.0` read from a numeric is a nonzero value.
    specs = [
      {:contains, "numbers @> '{0}'"},
      {:overlaps, "numbers && '{0}'"},
      {:not_contains, "NOT (numbers @> '{0}')"}
    ]

    record = %{"numbers" => "{-0." <> String.duplicate("0", 399) <> "1}"}
    expected = postgres_matches(conn, specs, record, numbers: "numeric[]")
    assert expected == MapSet.new([:not_contains])

    # Only `@>` is indexed: the other two reach the where-clause evaluator.
    for memo? <- [false, true] do
      filter =
        Enum.reduce(specs, Filter.new(filter_routing_memo: memo?), fn {id, where}, filter ->
          Filter.add_shape(filter, id, Shape.new!("t1", where: where, inspector: @inspector))
        end)

      assert route(filter, record) == {expected, true}
    end
  end

  test "canonical numeric, text and temporal elements agree with Postgres", %{db_conn: conn} do
    specs = [
      {:numeric, "numbers @> '{1.0}'"},
      {:text, "words @> '{a,b}'"},
      {:time, "times @> '{12:00:00.000000}'"}
    ]

    fields = [numbers: "numeric[]", words: "text[]", times: "time[]"]
    record = %{"numbers" => "{1}", "words" => "{b,a,a}", "times" => "{12:00:00}"}
    filter = build_filter(specs, false)
    assert route(filter, record) == {postgres_matches(conn, specs, record, fields), true}
  end

  defp build_filter(specs, memo?) do
    Enum.reduce(specs, Filter.new(filter_routing_memo: memo?), fn {id, where}, filter ->
      shape = Shape.new!("t1", where: where, inspector: @inspector)
      assert Filter.indexed_shape?(shape)
      Filter.add_shape(filter, id, shape)
    end)
  end

  defp route(filter, record) do
    Filter.affected_shapes_exact(filter, %NewRecord{relation: {"public", "t1"}, record: record})
  end

  defp route_reductions(filter, record) do
    :erlang.garbage_collect()
    {:reductions, before} = Process.info(self(), :reductions)
    route(filter, record)
    {:reductions, after_route} = Process.info(self(), :reductions)
    after_route - before
  end

  defp postgres_matches(conn, specs, record, fields) do
    columns =
      fields
      |> Enum.with_index(1)
      |> Enum.map_join(", ", fn {{field, type}, i} -> "($#{i}::text)::#{type} AS #{field}" end)

    predicates = Enum.map_join(specs, ", ", fn {_id, where} -> "(#{where}) IS TRUE" end)
    params = Enum.map(fields, fn {field, _type} -> Map.fetch!(record, Atom.to_string(field)) end)

    %{rows: [matches]} =
      Postgrex.query!(conn, "SELECT #{predicates} FROM (SELECT #{columns}) r", params)

    for {{id, _where}, true} <- Enum.zip(specs, matches), into: MapSet.new(), do: id
  end

  defp array(values), do: "{" <> Enum.join(values, ",") <> "}"
end
