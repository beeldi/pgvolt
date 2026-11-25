defmodule Electric.Shapes.FilterExactnessTest do
  @moduledoc """
  Is the collector's routing decision exact, or does it over-approximate?

  This is the linchpin for sharing conversion across a fan-out group. Today the collector calls
  `Filter.affected_shapes/2` to pick the shapes a change is routed to, and then every consumer
  re-derives the same answer by running `Shape.convert_change/3`, which evaluates the where
  clause again. If the collector's set is exactly the set of shapes whose `convert_change`
  returns a change, the consumer-side evaluation is recomputing a decision that has already been
  made, once per participating shape, and it can be hoisted.

  If instead the filter is a superset -- a cheap index prefilter that consumers refine -- then
  the consumer-side evaluation is load-bearing and nothing can be hoisted. Reading the code says
  exact: `WhereCondition.affected_shapes/3` walks the index tree and evaluates the residual
  `and_where` itself, and unindexed shapes go through `other_shape_matches?`, which runs the full
  `WhereClause.includes_record?`. This test is that claim stated as an executable assertion,
  because "I read it and it looked exact" is not a safe basis for deleting a correctness check.

  Exact, but with one escape hatch: when a predicate cannot be evaluated on a record at all,
  routing fails open and returns the shapes it cannot rule out. `affected_shapes_exact/2` reports
  that, and it is what the sharing path keys off, so the second half of this file pins both the
  reporting and the divergence it stands for.

  The shape mix deliberately spans every routing path: plain equality (indexed), an `IN` list
  (distributes to indexed disjuncts), equality with a residual conjunct (indexed + and_where),
  and clauses that do not optimise at all (`other_shapes`).

  It also spans two clause families, both of which reach the
  index only because this fork taught the optimiser to distribute them: array-overlap filters
  (`&&`) and an `IN` list carried alongside a disjunctive row filter. Before those rewrites each
  fell back to a linear re-evaluation of every shape, which is slow but exact; now they route
  through the index, which is where an exactness regression would silently drop a shape's
  changes.
  """
  use ExUnit.Case, async: false

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape

  import Support.EmployeesFixtures,
    only: [inspector: 1, insert: 1, key: 1, relation: 0, shape!: 2]

  @inspector inspector(
               ~w[id firmId divisionId status deletedAt metaJson gradeId] ++
                 [{"divisionIds", {:array, "text"}}]
             )

  defp shape!(where), do: shape!(@inspector, where: where)

  # One entry per routing path through the filter, so a regression in any of them shows up as a
  # divergence rather than being masked by the others.
  defp shape_specs do
    firms = ["F0", "F1"]
    divisions = ["D0", "D1", "D2", "D3"]

    equality =
      for p <- firms, do: {"eq-#{p}", ~s|"firmId" = '#{p}'|}

    in_list =
      for p <- firms do
        list = Enum.map_join(divisions, ",", &"'#{&1}'")
        {"in-#{p}", ~s|"firmId" = '#{p}' AND "divisionId" IN (#{list})|}
      end

    residual =
      for p <- firms, b <- divisions do
        {"res-#{p}-#{b}", ~s|"firmId" = '#{p}' AND "divisionId" = '#{b}' AND "deletedAt" IS NULL|}
      end

    # No leading indexable equality, so these land in `other_shapes` and are evaluated directly.
    unindexed =
      for b <- divisions do
        {"other-#{b}", ~s|"divisionId" != '#{b}' AND "status" IS NOT NULL|}
      end

    disjunction =
      for p <- firms do
        {"or-#{p}", ~s|"firmId" = '#{p}' OR "status" = 'shared'|}
      end

    # Array-overlap clauses: overlap on any shared division, which the optimiser distributes into
    # one inclusion-index disjunct each.
    overlap =
      for p <- firms, pair <- [["D0", "D1"], ["D2", "D3"]] do
        list = Enum.map_join(pair, ", ", &"'#{&1}'")

        {"arr-#{p}-#{Enum.join(pair, "")}",
         ~s|"firmId" = '#{p}' AND "divisionIds" && ARRAY[#{list}]::text[] AND "deletedAt" IS NULL|}
      end

    # A row filter whose OR sits beside the indexable IN list rather
    # than above it, so the disjunction is residual and the IN list must still distribute.
    or_row_filter =
      for p <- firms do
        list = Enum.map_join(divisions, ", ", &"'#{&1}'")

        {"orf-#{p}",
         ~s|"firmId" = '#{p}' AND "divisionId" IN (#{list}) AND ("deletedAt" IS NULL OR "metaJson" IS NOT NULL) AND "gradeId" IS NOT NULL|}
      end

    equality ++ in_list ++ residual ++ unindexed ++ disjunction ++ overlap ++ or_row_filter
  end

  defp record(firm, division, status, deleted_at, division_ids, {meta_json, grade_id}) do
    %{
      "id" => "eq-#{firm}-#{division}",
      "firmId" => firm,
      "divisionId" => division,
      "status" => status,
      "deletedAt" => deleted_at,
      "divisionIds" => division_ids,
      "metaJson" => meta_json,
      "gradeId" => grade_id
    }
  end

  # Includes values no shape mentions (firm "F9", division "D9") so misses are covered too, and
  # both nil and set `deletedAt` so the residual conjunct decides in both directions. The array
  # column spans a partial overlap, a disjoint set, the empty array and NULL, because overlap and
  # the `deletedAt` conjunct have to disagree somewhere for the residual to be load-bearing. The
  # row-filter pair walks all four corners of `(deletedAt IS NULL OR metaJson IS NOT NULL) AND
  # gradeId IS NOT NULL`.
  #
  # `id` stays a function of firm and division alone, so records differing only in the columns
  # added here collide on the record key. That is deliberate: it is what would expose a routing
  # memo whose key does not cover every field the clauses read.
  defp records do
    for firm <- ["F0", "F1", "F9"],
        division <- ["D0", "D2", "D9"],
        status <- ["active", "shared", nil],
        deleted_at <- [nil, "2026-07-31"],
        division_ids <- ["{D0,D9}", "{D2}", "{D9}", "{}", nil],
        row_filter <- [{nil, "gr-1"}, {"meta-1", "gr-1"}, {nil, nil}, {"meta-1", nil}] do
      record(firm, division, status, deleted_at, division_ids, row_filter)
    end
  end

  defp changes_for(record, index) do
    offset = LogOffset.new(Lsn.from_integer(1000), index)
    record_key = key(record["id"])

    [
      %Changes.NewRecord{
        relation: relation(),
        record: record,
        log_offset: offset,
        key: record_key
      },
      %Changes.DeletedRecord{
        relation: relation(),
        old_record: record,
        log_offset: offset,
        key: record_key
      }
    ]
  end

  defp build_filter(specs, shapes) do
    Enum.reduce(specs, Filter.new(stack_id: "exactness", filter_routing_memo: true), fn {id,
                                                                                         _where},
                                                                                        filter ->
      Filter.add_shape(filter, id, Map.fetch!(shapes, id))
    end)
  end

  defp equality_indexed_fields(filter) do
    filter.eq_index_table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{_condition_id, field, _value}, _} when is_binary(field) -> [field]
      _ -> []
    end)
    |> MapSet.new()
  end

  defp inclusion_indexed_fields(filter) do
    filter.incl_index_table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{_condition_id, field, _value}, _} when is_binary(field) -> [field]
      _ -> []
    end)
    |> MapSet.new()
  end

  # Exactness alone cannot tell an indexed route from a linear one: the fallback re-evaluates every
  # shape and is exact too. So the mix would keep passing if the two indexed clause families quietly
  # stopped optimising, losing the routing win with no test to show for it. Pin the layout on the
  # same filter the exactness assertion runs against.
  test "the indexed clause families reach the index rather than the linear fallback" do
    # Arrange
    specs = shape_specs()
    shapes = Map.new(specs, fn {id, where} -> {id, shape!(where)} end)

    # Act
    filter = build_filter(specs, shapes)

    # Assert
    assert MapSet.subset?(MapSet.new(["firmId", "divisionId"]), equality_indexed_fields(filter))
    assert MapSet.member?(inclusion_indexed_fields(filter), "divisionIds")
  end

  test "the array-overlap shapes emit for some of the records" do
    # Arrange
    specs = Enum.filter(shape_specs(), fn {id, _where} -> String.starts_with?(id, "arr-") end)

    changes =
      records()
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, index} -> changes_for(record, index) end)

    # Act
    emitting =
      MapSet.new(
        for {id, where} <- specs,
            shape = shape!(where),
            Enum.any?(changes, fn change ->
              Shape.convert_change(shape, change, stack_id: "exactness", shape_handle: id) != []
            end),
            do: id
      )

    # Assert
    assert emitting == MapSet.new(specs, fn {id, _where} -> id end)
  end

  test "the collector routes to exactly the shapes whose convert_change emits" do
    # Arrange
    specs = shape_specs()
    shapes = Map.new(specs, fn {id, where} -> {id, shape!(where)} end)
    filter = build_filter(specs, shapes)

    all_changes =
      records()
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, index} -> changes_for(record, index) end)

    # Act
    divergences =
      Enum.flat_map(all_changes, fn change ->
        {routed, exact?} = Filter.affected_shapes_exact(filter, change)

        assert exact?, "every predicate here is evaluable, so nothing may fail open"

        # The routing memo must be invisible: the cached answer is held to the same
        # per-shape conversion oracle as the freshly computed one.
        assert Filter.affected_shapes(filter, change) == routed

        converted =
          specs
          |> Enum.filter(fn {id, _} ->
            Shape.convert_change(Map.fetch!(shapes, id), change,
              stack_id: "exactness",
              shape_handle: id
            ) != []
          end)
          |> MapSet.new(fn {id, _} -> id end)

        case {MapSet.difference(routed, converted), MapSet.difference(converted, routed)} do
          {over, under} when over == %MapSet{} and under == %MapSet{} ->
            []

          {over, under} ->
            [
              %{
                change: change.__struct__,
                record:
                  Map.take(
                    change_record(change),
                    ~w(firmId divisionId status deletedAt divisionIds metaJson gradeId)
                  ),
                routed_but_not_converted: MapSet.to_list(over),
                converted_but_not_routed: MapSet.to_list(under)
              }
            ]
        end
      end)

    # Assert
    assert divergences == []
  end

  describe "predicates that cannot be evaluated" do
    @typed_inspector inspector(["id", {"num", "int8"}, "note"])

    defp typed_filter(specs) do
      Enum.reduce(specs, Filter.new(stack_id: "inexact", filter_routing_memo: true), fn {id,
                                                                                         where},
                                                                                        filter ->
        Filter.add_shape(filter, id, shape!(@typed_inspector, where: where))
      end)
    end

    defp typed_insert(num) do
      insert(%{"id" => "eq-1", "num" => num, "note" => "n"})
    end

    test "an unevaluable unindexed comparison routes open and says so" do
      # Arrange
      filter = typed_filter([{"gt", ~s|"num" > 5|}])

      # Act
      {routed, exact?} = Filter.affected_shapes_exact(filter, typed_insert("not-a-number"))

      # Assert
      assert MapSet.member?(routed, "gt"), "routing must keep failing open"
      refute exact?
    end

    test "an unparseable indexed field routes open and says so" do
      # Arrange
      filter = typed_filter([{"eq-5", ~s|"num" = 5|}, {"eq-6", ~s|"num" = 6|}])

      # Act
      {routed, exact?} = Filter.affected_shapes_exact(filter, typed_insert("not-a-number"))

      # Assert
      assert MapSet.equal?(routed, MapSet.new(["eq-5", "eq-6"])), "routing must keep failing open"
      refute exact?
    end

    test "an inexact decision is not memoised for later records with the same key" do
      # Arrange
      filter = typed_filter([{"eq-5", ~s|"num" = 5|}, {"eq-6", ~s|"num" = 6|}])

      # Act
      {_routed, false} = Filter.affected_shapes_exact(filter, typed_insert("not-a-number"))
      {routed, exact?} = Filter.affected_shapes_exact(filter, typed_insert("5"))

      # Assert
      assert exact?
      assert MapSet.equal?(routed, MapSet.new(["eq-5"]))
    end

    test "the marker does not leak from one change to the next" do
      # Arrange
      filter = typed_filter([{"gt", ~s|"num" > 5|}])

      # Act
      {_routed, false} = Filter.affected_shapes_exact(filter, typed_insert("not-a-number"))
      {routed, exact?} = Filter.affected_shapes_exact(filter, typed_insert("9"))

      # Assert
      assert exact?
      assert MapSet.equal?(routed, MapSet.new(["gt"]))
    end
  end

  defp change_record(%Changes.NewRecord{record: record}), do: record
  defp change_record(%Changes.DeletedRecord{old_record: record}), do: record
end
