defmodule Electric.Shapes.Filter.Indexes.SubqueryIndexTest do
  use ExUnit.Case

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Parser.{Func, Ref}
  alias Electric.Replication.Eval.Runner
  alias Electric.Replication.Eval.SublinkKey
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.Indexes.SubqueryIndex
  alias Electric.Shapes.Filter.WhereCondition
  alias Electric.Shapes.WhereClause

  import Support.DbSetup, only: [with_shared_db: 1, scalar: 3]

  @subquery_ref ["$sublink", "0"]
  @field "par_id"

  setup do
    filter = Filter.new()
    condition_id = make_ref()
    WhereCondition.init(filter, condition_id)

    %{
      filter: filter,
      table: Filter.subquery_index(filter),
      condition_id: condition_id
    }
  end

  describe "shape-level metadata" do
    test "register_shape stores polarity and fallback used by exact evaluation", %{table: table} do
      SubqueryIndex.register_shape(table, "s1", make_plan())

      assert SubqueryIndex.fallback?(table, "s1")
      assert SubqueryIndex.membership_or_fallback?(table, "s1", @subquery_ref, 99)

      SubqueryIndex.register_shape(table, "s2", make_plan(polarity: :negated))

      refute SubqueryIndex.membership_or_fallback?(table, "s2", @subquery_ref, 99)
    end

    test "unregister_shape removes exact membership metadata", %{table: table} do
      SubqueryIndex.register_shape(table, "s1", make_plan())
      SubqueryIndex.add_value(table, "s1", @subquery_ref, 0, 5)

      assert SubqueryIndex.member?(table, "s1", @subquery_ref, 5)

      SubqueryIndex.unregister_shape(table, "s1")

      refute SubqueryIndex.member?(table, "s1", @subquery_ref, 5)
      refute SubqueryIndex.fallback?(table, "s1")
    end
  end

  describe "three-valued membership (NULL in dependency view)" do
    test "composite NOT IN agrees with Postgres when either key contains NULL", ctx do
      {:ok, %{db_conn: conn}} = with_shared_db(ctx)
      %{table: table} = ctx
      sql = "(parent_id, value) NOT IN (SELECT parent_id, value FROM parent)"

      where =
        Parser.parse_and_validate_expression!(sql,
          refs: %{
            ["parent_id"] => :int4,
            ["value"] => :int4,
            @subquery_ref => {:array, {:row, [:int4, :int4]}}
          },
          sublink_queries: %{0 => "SELECT parent_id, value FROM parent"}
        )

      view = MapSet.new([{2, 1}, {nil, 3}])
      seed(table, "s1", view)
      callback = WhereClause.subquery_member_from_index(table, "s1")

      results =
        for {a, b} <- [{nil, 1}, {nil, 2}, {nil, 3}, {2, 1}, {2, 3}, {2, nil}, {3, 1}] do
          expected =
            scalar(
              conn,
              """
              WITH parent(parent_id, value) AS (VALUES (2, 1), (NULL, 3))
              SELECT #{sql} FROM (VALUES ($1::int, $2::int)) AS child(parent_id, value)
              """,
              [a, b]
            )

          refs = %{@subquery_ref => view, ["parent_id"] => a, ["value"] => b}

          indexed =
            Runner.execute(where, Map.delete(refs, @subquery_ref), subquery_member?: callback)

          {expected, indexed, Runner.execute(where, refs)}
        end

      assert Enum.all?(results, fn {expected, indexed, live} ->
               indexed == {:ok, expected} and live == {:ok, expected}
             end),
             inspect(results)
    end

    test "member? answers true / nil / false the way Sublink.member? does", %{table: table} do
      seed(table, "s1", [2, 3, nil])
      seed(table, "s2", [2, 3])

      # A present value is a definite member over either view.
      assert SubqueryIndex.member?(table, "s1", @subquery_ref, 2) == true
      assert SubqueryIndex.member?(table, "s2", @subquery_ref, 2) == true

      # A non-member is unknown when the view holds a NULL, but a proven non-member without one.
      assert SubqueryIndex.member?(table, "s1", @subquery_ref, 5) == nil
      assert SubqueryIndex.member?(table, "s2", @subquery_ref, 5) == false

      # A NULL value is never a member of a non-empty view: unknown, never false.
      assert SubqueryIndex.member?(table, "s1", @subquery_ref, nil) == nil
      assert SubqueryIndex.member?(table, "s2", @subquery_ref, nil) == nil
    end

    test "member? over an empty seeded view is false, even for NULL", %{table: table} do
      SubqueryIndex.register_shape(table, "s1", make_plan())
      SubqueryIndex.mark_ready(table, "s1")

      assert SubqueryIndex.member?(table, "s1", @subquery_ref, 5) == false
      assert SubqueryIndex.member?(table, "s1", @subquery_ref, nil) == false
    end

    test "membership_or_fallback? carries the unknown through for a ready shape", %{table: table} do
      seed(table, "s1", [2, 3, nil])

      assert SubqueryIndex.membership_or_fallback?(table, "s1", @subquery_ref, 5) == nil
      assert SubqueryIndex.membership_or_fallback?(table, "s1", @subquery_ref, 2) == true
    end

    test "NOT IN routed through the index excludes a non-member when the view holds a NULL", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      where =
        Parser.parse_and_validate_expression!("test NOT IN (SELECT val FROM tester)",
          refs: %{["test"] => :int4, @subquery_ref => {:array, :int4}},
          sublink_queries: %{0 => "SELECT val FROM tester"}
        )

      register_node_shape(filter, table, condition_id, "s1", polarity: :negated)
      seed(table, "s1", [2, 3, nil])
      callback = WhereClause.subquery_member_from_index(table, "s1")

      # Postgres makes `5 NOT IN (2, 3, NULL)` unknown, so the row is excluded. The two-valued index
      # answered false here, and `NOT false` streamed every root-table row to the shape.
      assert {:ok, false} =
               WhereClause.includes_record_result(where, %{"test" => "5"}, callback)

      # A genuine match is still a definite exclusion under NOT IN.
      assert {:ok, false} =
               WhereClause.includes_record_result(where, %{"test" => "2"}, callback)
    end

    test "NOT IN through the index still admits a real non-member when no NULL is present", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      where =
        Parser.parse_and_validate_expression!("test NOT IN (SELECT val FROM tester)",
          refs: %{["test"] => :int4, @subquery_ref => {:array, :int4}},
          sublink_queries: %{0 => "SELECT val FROM tester"}
        )

      register_node_shape(filter, table, condition_id, "s1", polarity: :negated)
      seed(table, "s1", [2, 3])
      callback = WhereClause.subquery_member_from_index(table, "s1")

      assert {:ok, true} =
               WhereClause.includes_record_result(where, %{"test" => "5"}, callback)
    end
  end

  describe "node registration and updates" do
    test "add_shape registers node mappings for a dependency", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      register_node_shape(filter, table, condition_id, "s1")

      assert SubqueryIndex.has_positions?(table, "s1")
      assert [{^condition_id, @field}] = SubqueryIndex.positions_for_shape(table, "s1")
    end

    test "multiple shapes on the same node infer emptiness from node registrations", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      register_node_shape(filter, table, condition_id, "s1")
      register_node_shape(filter, table, condition_id, "s2")
      register_node_shape(filter, table, condition_id, "s3")

      # Shape handles are interned to integer ids inside the index, so the raw
      # node_shape rows carry the interned id rather than the handle string. We
      # assert one positive registration per shape on the node; handle-level
      # behaviour is checked via the public API (all_shape_ids) below.
      assert [
               {{:node_shape, {^condition_id, @field}, _sid1, []}, {0, :positive, _}},
               {{:node_shape, {^condition_id, @field}, _sid2, []}, {0, :positive, _}},
               {{:node_shape, {^condition_id, @field}, _sid3, []}, {0, :positive, _}}
             ] =
               Enum.sort(
                 :ets.select(table, [
                   {{{:node_shape, {condition_id, @field}, :_, :_}, :_}, [], [:"$_"]}
                 ])
               )

      assert :ok =
               SubqueryIndex.remove_shape(filter, condition_id, "s1", subquery_optimisation(), [])

      assert MapSet.new(["s2", "s3"]) == SubqueryIndex.all_shape_ids(filter, condition_id, @field)

      assert :ok =
               SubqueryIndex.remove_shape(filter, condition_id, "s2", subquery_optimisation(), [])

      assert :deleted =
               SubqueryIndex.remove_shape(filter, condition_id, "s3", subquery_optimisation(), [])

      assert [] ==
               :ets.select(table, [
                 {{{:node_shape, {condition_id, @field}, :_, :_}, :_}, [], [:"$_"]}
               ])

      assert [] == :ets.lookup(table, {:node_meta, {condition_id, @field}})
    end

    test "seed_membership updates node-local routing and exact membership", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      register_node_shape(filter, table, condition_id, "s1")

      SubqueryIndex.seed_membership(table, "s1", @subquery_ref, 0, MapSet.new([5]))
      SubqueryIndex.mark_ready(table, "s1")

      assert SubqueryIndex.member?(table, "s1", @subquery_ref, 5)

      assert MapSet.new(["s1"]) ==
               SubqueryIndex.affected_shapes(
                 filter,
                 condition_id,
                 @field,
                 %{"par_id" => "5"}
               )
    end

    test "negated nodes use local complement semantics", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      register_node_shape(filter, table, condition_id, "s1", polarity: :negated)

      SubqueryIndex.seed_membership(table, "s1", @subquery_ref, 0, MapSet.new([5]))
      SubqueryIndex.mark_ready(table, "s1")

      refute MapSet.member?(
               SubqueryIndex.affected_shapes(
                 filter,
                 condition_id,
                 @field,
                 %{"par_id" => "5"}
               ),
               "s1"
             )

      assert MapSet.member?(
               SubqueryIndex.affected_shapes(
                 filter,
                 condition_id,
                 @field,
                 %{"par_id" => "99"}
               ),
               "s1"
             )
    end

    test "a numeric key routes by its exact value and leaves no row once removed", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      register_node_shape(filter, table, condition_id, "s1", type: :numeric)
      seed(table, "s1", [numeric_key("9007199254740993"), numeric_key("NaN")])
      route = &SubqueryIndex.affected_shapes(filter, condition_id, @field, %{@field => &1})

      assert route.("9007199254740993.00") == MapSet.new(["s1"])
      assert route.("NaN") == MapSet.new(["s1"])
      assert route.("9007199254740992") == MapSet.new()

      SubqueryIndex.remove_value(table, "s1", @subquery_ref, 0, numeric_key("NaN"))
      assert route.("NaN") == MapSet.new()

      opts = subquery_optimisation(type: :numeric)
      SubqueryIndex.remove_shape(filter, condition_id, "s1", opts, [])
      SubqueryIndex.unregister_shape(table, "s1")

      assert :ets.select_count(table, [
               {{{:node_positive_member, :_, :_, :_, :_}, :_}, [], [true]}
             ]) == 0
    end

    test "remove_shape clears node registrations", %{
      filter: filter,
      table: table,
      condition_id: condition_id
    } do
      register_node_shape(filter, table, condition_id, "s1")
      SubqueryIndex.add_value(table, "s1", @subquery_ref, 0, 5)

      assert :deleted =
               SubqueryIndex.remove_shape(filter, condition_id, "s1", subquery_optimisation(), [])

      refute SubqueryIndex.has_positions?(table, "s1")

      SubqueryIndex.unregister_shape(table, "s1")

      refute SubqueryIndex.fallback?(table, "s1")
    end
  end

  describe "stack lookup" do
    test "stores and retrieves table ref by stack_id" do
      table = SubqueryIndex.new(stack_id: "test-stack-123")
      assert SubqueryIndex.for_stack("test-stack-123") == table
    end

    test "returns nil for unknown stack" do
      assert SubqueryIndex.for_stack("nonexistent-stack") == nil
    end
  end

  defp register_node_shape(filter, table, condition_id, shape_id, opts \\ []) do
    SubqueryIndex.register_shape(table, shape_id, make_plan(opts))
    :ok = SubqueryIndex.add_shape(filter, condition_id, shape_id, subquery_optimisation(opts), [])
  end

  defp subquery_optimisation(opts \\ []) do
    field = Keyword.get(opts, :field, @field)

    %{
      operation: "subquery",
      field: field,
      testexpr: %Ref{path: [field], type: Keyword.get(opts, :type, :int8)},
      subquery_ref: Keyword.get(opts, :subquery_ref, @subquery_ref),
      dep_index: Keyword.get(opts, :dep_index, 0),
      polarity: Keyword.get(opts, :polarity, :positive),
      and_where: Keyword.get(opts, :and_where)
    }
  end

  defp make_plan(opts \\ []) do
    polarity = Keyword.get(opts, :polarity, :positive)
    dep_index = Keyword.get(opts, :dep_index, 0)
    subquery_ref = Keyword.get(opts, :subquery_ref, @subquery_ref)
    field = Keyword.get(opts, :field, @field)
    type = Keyword.get(opts, :type, :int8)

    testexpr = %Ref{path: [field], type: type}
    ref = %Ref{path: subquery_ref, type: {:array, type}}

    ast = %Func{
      name: "sublink_membership_check",
      args: [testexpr, ref],
      type: :bool
    }

    %DnfPlan{
      disjuncts: [],
      disjuncts_positions: [],
      position_count: 1,
      positions: %{
        0 => %{
          ast: ast,
          sql: "fake",
          subquery?: true,
          negated?: polarity == :negated,
          dependency_index: dep_index,
          subquery_ref: subquery_ref,
          tag_columns: [field]
        }
      },
      dependency_positions: %{dep_index => [0]},
      dependency_disjuncts: %{},
      dependency_polarities: %{dep_index => polarity}
    }
  end

  # Removal must not scale with these dimensions. We measure removal reductions at a
  # small and a 20x-larger size and require the delta to stay within noise.
  @perf_small 1_000
  @perf_large 20_000
  @flatness_tolerance 3_000
  @perf_timeout 120_000

  defp reductions(fun) do
    # A collection that lands inside `fun` is charged to it as reductions; start from a clean heap.
    :erlang.garbage_collect()
    {:reductions, before} = :erlang.process_info(self(), :reductions)
    fun.()
    {:reductions, after_} = :erlang.process_info(self(), :reductions)
    after_ - before
  end

  # Replicates the index work done by Filter.remove_shape: per-node remove_shape,
  # then unregister_shape. branch_key is [] throughout these tests.
  defp remove_one(filter, condition_id, shape_id) do
    table = Filter.subquery_index(filter)
    SubqueryIndex.remove_shape(filter, condition_id, shape_id, subquery_optimisation(), [])
    SubqueryIndex.unregister_shape(table, shape_id)
  end

  defp seed(table, shape_id, values) do
    SubqueryIndex.seed_membership(table, shape_id, @subquery_ref, 0, MapSet.new(values))
    SubqueryIndex.mark_ready(table, shape_id)
  end

  defp numeric_key(text) do
    {:ok, key} = SublinkKey.parse(text, :numeric)
    key
  end

  describe "performance" do
    @tag :performance
    @tag timeout: @perf_timeout
    test "removal is O(1) in the total number of shapes" do
      measure = fn n ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_ids = for _ <- 1..n, do: make_ref()

        condition_ids
        |> Enum.with_index(1)
        |> Enum.each(fn {condition_id, i} ->
          WhereCondition.init(filter, condition_id)
          register_node_shape(filter, table, condition_id, i)
          seed(table, i, [1, 2, 3, 4, 5])
        end)

        {condition_ids |> Enum.at(div(n, 2)), div(n, 2) + 1, filter}
      end

      {cid_s, id_s, filter_s} = measure.(@perf_small)
      small = reductions(fn -> remove_one(filter_s, cid_s, id_s) end)

      {cid_l, id_l, filter_l} = measure.(@perf_large)
      large = reductions(fn -> remove_one(filter_l, cid_l, id_l) end)

      assert abs(large - small) < @flatness_tolerance,
             "removal grew with total shapes: #{small} -> #{large} reductions"
    end

    @tag :performance
    @tag timeout: @perf_timeout
    test "removal is O(1) in the number of shapes on the node" do
      measure = fn n ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_id = make_ref()
        WhereCondition.init(filter, condition_id)

        for i <- 1..n do
          register_node_shape(filter, table, condition_id, i)
          seed(table, i, [i, i + 1_000_000])
        end

        {condition_id, div(n, 2), filter}
      end

      {cid_s, id_s, filter_s} = measure.(@perf_small)
      small = reductions(fn -> remove_one(filter_s, cid_s, id_s) end)

      {cid_l, id_l, filter_l} = measure.(@perf_large)
      large = reductions(fn -> remove_one(filter_l, cid_l, id_l) end)

      assert abs(large - small) < @flatness_tolerance,
             "removal grew with shapes-on-node: #{small} -> #{large} reductions"
    end

    @tag :performance
    @tag timeout: @perf_timeout
    test "removal is O(1) in the number of shapes sharing each value" do
      # All shapes share the same values on one node, so each (node, value) is backed by
      # n shapes — the seeded production case. Removing one shape must not scan the others
      # sharing its values.
      shared = [1, 2, 3, 4, 5]

      measure = fn n ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_id = make_ref()
        WhereCondition.init(filter, condition_id)

        for i <- 1..n do
          register_node_shape(filter, table, condition_id, i)
          seed(table, i, shared)
        end

        {condition_id, div(n, 2), filter}
      end

      {cid_s, id_s, filter_s} = measure.(@perf_small)
      small = reductions(fn -> remove_one(filter_s, cid_s, id_s) end)

      {cid_l, id_l, filter_l} = measure.(@perf_large)
      large = reductions(fn -> remove_one(filter_l, cid_l, id_l) end)

      assert abs(large - small) < @flatness_tolerance,
             "removal grew with shapes-per-value: #{small} -> #{large} reductions"
    end

    @tag :performance
    @tag timeout: @perf_timeout
    test "removal cost grows with the removed shape's own view size" do
      measure = fn v ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_id = make_ref()
        WhereCondition.init(filter, condition_id)
        register_node_shape(filter, table, condition_id, "s")
        seed(table, "s", Enum.to_list(1..v))
        {condition_id, filter}
      end

      {cid_small, filter_small} = measure.(20)
      small = reductions(fn -> remove_one(filter_small, cid_small, "s") end)

      {cid_big, filter_big} = measure.(20 * 50)
      big = reductions(fn -> remove_one(filter_big, cid_big, "s") end)

      assert big > small * 5,
             "expected removal to scale with view size, got #{small} -> #{big}"
    end

    @tag :performance
    @tag timeout: @perf_timeout
    test "routing a numeric key is O(log n) in the number of member rows" do
      # A numeric key is a Decimal, a map, and a map in a match-spec key is a pattern: the
      # node-member lookup would scan every row of the index instead of one key range.
      measure = fn n ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_id = make_ref()
        WhereCondition.init(filter, condition_id)

        for i <- 1..n do
          register_node_shape(filter, table, condition_id, i, type: :numeric)
          seed(table, i, [numeric_key("#{i}.5")])
        end

        record = %{@field => "#{div(n, 2)}.50"}
        route = fn -> SubqueryIndex.affected_shapes(filter, condition_id, @field, record) end
        assert route.() == MapSet.new([div(n, 2)])
        reductions(route)
      end

      small = measure.(@perf_small)
      large = measure.(@perf_large)

      assert abs(large - small) < @flatness_tolerance,
             "routing grew with member rows: #{small} -> #{large} reductions"
    end

    @tag :performance
    @tag timeout: @perf_timeout
    test "routing an array key with lower bounds is O(log n) in the number of member rows" do
      # An array whose lower bounds are not 1 is a struct, so a map too, as a numeric key is.
      measure = fn n ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_id = make_ref()
        WhereCondition.init(filter, condition_id)

        for i <- 1..n do
          register_node_shape(filter, table, condition_id, i, type: {:array, :int4})
          {:ok, key} = SublinkKey.parse("[0:0]={#{i}}", {:array, :int4})
          seed(table, i, [key])
        end

        record = %{@field => "[0:0]={#{div(n, 2)}}"}
        route = fn -> SubqueryIndex.affected_shapes(filter, condition_id, @field, record) end
        assert route.() == MapSet.new([div(n, 2)])
        reductions(route)
      end

      small = measure.(@perf_small)
      large = measure.(@perf_large)

      assert abs(large - small) < @flatness_tolerance,
             "routing grew with member rows: #{small} -> #{large} reductions"
    end

    @tag :performance
    @tag timeout: @perf_timeout
    test "routing a numeric[] key is O(log n) in the number of member rows" do
      # A numeric[] key holds a Decimal per element, a struct inside a list.
      measure = fn n ->
        filter = Filter.new()
        table = Filter.subquery_index(filter)
        condition_id = make_ref()
        WhereCondition.init(filter, condition_id)

        for i <- 1..n do
          register_node_shape(filter, table, condition_id, i, type: {:array, :numeric})
          {:ok, key} = SublinkKey.parse("{#{i}.5}", {:array, :numeric})
          seed(table, i, [key])
        end

        record = %{@field => "{#{div(n, 2)}.50}"}
        route = fn -> SubqueryIndex.affected_shapes(filter, condition_id, @field, record) end
        assert route.() == MapSet.new([div(n, 2)])
        reductions(route)
      end

      small = measure.(@perf_small)
      large = measure.(@perf_large)

      assert abs(large - small) < @flatness_tolerance,
             "routing grew with member rows: #{small} -> #{large} reductions"
    end
  end
end
