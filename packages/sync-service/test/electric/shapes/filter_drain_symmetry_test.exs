defmodule Electric.Shapes.FilterDrainSymmetryTest do
  @moduledoc """
  Every clause shape `optimise_where/1` recognises has an `add_shape`/`remove_shape` pair, and the
  two must be exact inverses: after every shape is removed the filter must hold nothing.

  Two defects reached the shape log collector through this seam, both crashing it, and neither was
  visible to a behavioural assertion — routing answered correctly while the bookkeeping behind it
  was already inconsistent. So this walks the grammar exhaustively rather than by hand: every pair
  and every triple of clauses, in every removal order, against every table the filter owns.
  """
  use ExUnit.Case, async: false

  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"},
                 %{name: "categoryId", type: "text"},
                 %{name: "deletedAt", type: "timestamptz"},
                 %{name: "divisionIds", array_type: "text"}
               ]
             )

  # One clause per branch of `optimise_where/1`, plus degenerate constants and folded forms that
  # drop a ref.
  @clauses [
    nil,
    ~s|"firmId" = 'F0'|,
    ~s|'F0' = "firmId"|,
    ~s|"divisionId" IN ('b1', 'b2')|,
    ~s|"divisionId" IN ('b1')|,
    ~s|"divisionIds" @> ARRAY['b1']::text[]|,
    ~s|"divisionIds" @> ARRAY[]::text[]|,
    ~s|ARRAY['b1']::text[] <@ "divisionIds"|,
    ~s|ARRAY[]::text[] <@ "divisionIds"|,
    ~s|"divisionIds" && ARRAY['b1']::text[]|,
    ~s|"divisionIds" && ARRAY['b1', 'b2']::text[]|,
    ~s|"divisionIds" && ARRAY[]::text[]|,
    ~s|"firmId" = 'F0' AND "divisionId" IN ('b1', 'b2')|,
    ~s|"firmId" = 'F0' OR "categoryId" = 'c1'|,
    ~s|("divisionId" = 'b1' OR "categoryId" = 'c2') AND "deletedAt" IS NULL|,
    ~s|"divisionId" != 'b9'|,
    ~s|"deletedAt" IS NULL|,
    ~s|"firmId" = NULL|,
    ~s|"firmId" = 'F0' OR "divisionIds" && ARRAY[]::text[]|,
    ~s|true|,
    ~s|false|
  ]

  defp shape(nil), do: Shape.new!("employees", inspector: @inspector)
  defp shape(where), do: Shape.new!("employees", where: where, inspector: @inspector)

  # Every table the filter allocates, minus the counter (whose single row is the counter itself).
  defp residue(%Filter{} = filter) do
    [
      shapes: filter.shapes_table,
      tables: filter.tables_table,
      where_cond: filter.where_cond_table,
      other_shapes: filter.other_shapes_table,
      eq_index: filter.eq_index_table,
      incl_index: filter.incl_index_table,
      incl_index_paths: filter.incl_index_paths_table,
      relation_index: filter.relation_index_table,
      route_fields: filter.route_fields_table,
      route_memo: filter.route_memo_table,
      subquery_index: filter.subquery_index
    ]
    |> Enum.filter(fn {_, table} -> table != nil end)
    |> Enum.map(fn {name, table} -> {name, :ets.info(table)[:size]} end)
    |> Enum.reject(fn {_, size} -> size == 0 end)
  end

  defp drain(clauses, removal_order, memo?) do
    ids = Enum.map(1..length(clauses), &"s#{&1}")

    filter =
      clauses
      |> Enum.zip(ids)
      |> Enum.reduce(Filter.new(filter_routing_memo: memo?), fn {where, id}, f ->
        Filter.add_shape(f, id, shape(where))
      end)

    drained =
      removal_order
      |> Enum.map(&Enum.at(ids, &1))
      |> Enum.reduce(filter, fn id, f -> Filter.remove_shape(f, id) end)

    residue(drained)
  end

  defp orders(2), do: [[0, 1], [1, 0]]

  defp orders(3),
    do: [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]]

  defp combinations(_list, 0), do: [[]]
  defp combinations([], _), do: []

  defp combinations([head | tail], size),
    do: Enum.map(combinations(tail, size - 1), &[head | &1]) ++ combinations(tail, size)

  defp sweep(size, memo?) do
    for clauses <- combinations(@clauses, size),
        order <- orders(size),
        reduce: {[], 0} do
      {failures, checked} ->
        outcome =
          try do
            case drain(clauses, order, memo?) do
              [] -> :ok
              residue -> {:residue, residue}
            end
          rescue
            error -> {:raise, Exception.message(error) |> String.slice(0, 60)}
          end

        case outcome do
          :ok -> {failures, checked + 1}
          problem -> {[{clauses, order, problem} | failures], checked + 1}
        end
    end
  end

  defp report(size, memo?) do
    {failures, checked} = sweep(size, memo?)

    assert checked > 0

    assert failures == [], """
    Adding then removing every shape left the filter holding state, or raised. Either way the
    add/remove pair is not an inverse for these clauses, and the raise reaches the shape log
    collector unrescued.

    #{Enum.map_join(Enum.take(failures, 8), "\n\n", fn {clauses, order, problem} -> """
      clauses:  #{inspect(clauses)}
      order:    #{inspect(order)}
      problem:  #{inspect(problem)}
      """ end)}
    """
  end

  test "every pair of clauses drains in either removal order" do
    report(2, false)
  end

  # 7980 add/remove sequences run in ~3s alone; the base lane's concurrency starved it past the
  # 120s default. Headroom, not fewer clauses -- this is the exhaustive add/remove-inverse sweep.
  @tag timeout: 300_000
  test "every triple of clauses drains in every removal order" do
    report(3, false)
  end

  test "every pair drains with the routing memo on" do
    report(2, true)
  end

  # Subquery shapes need the real inspector, and they are the one branch whose state lives outside
  # the where-condition tree -- in the subquery index, which `residue/1` above also counts.
  describe "shapes with a dependency" do
    import Support.ComponentSetup
    import Support.DbSetup
    import Support.DbStructureSetup

    setup [
      :with_stack_id_from_test,
      :with_shared_db,
      :with_persistent_kv,
      :with_inspector,
      :with_sql_execute
    ]

    @subquery_clauses [
      "id IN (SELECT id FROM parent)",
      "NOT id IN (SELECT id FROM parent)",
      "par_id = 7 AND id IN (SELECT id FROM parent)",
      "id IN (SELECT id FROM parent) OR par_id = 7",
      "par_id = 7",
      "par_id IN (7, 8)",
      "par_id IS NULL"
    ]

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS child (id INT PRIMARY KEY, par_id INT REFERENCES parent(id))"
         ]
    test "every pair and triple drains, subquery index included", %{inspector: inspector} do
      shapes =
        Map.new(@subquery_clauses, fn where ->
          {where, Shape.new!("child", inspector: inspector, where: where)}
        end)

      failures =
        for size <- [2, 3],
            clauses <- combinations(@subquery_clauses, size),
            order <- orders(size),
            reduce: [] do
          acc ->
            ids = Enum.map(1..size, &"s#{&1}")

            filter =
              clauses
              |> Enum.zip(ids)
              |> Enum.reduce(Filter.new(), fn {where, id}, f ->
                Filter.add_shape(f, id, shapes[where])
              end)

            outcome =
              try do
                order
                |> Enum.map(&Enum.at(ids, &1))
                |> Enum.reduce(filter, fn id, f -> Filter.remove_shape(f, id) end)
                |> residue()
                |> case do
                  [] -> :ok
                  residue -> {:residue, residue}
                end
              rescue
                error -> {:raise, Exception.message(error) |> String.slice(0, 60)}
              end

            if outcome == :ok, do: acc, else: [{clauses, order, outcome} | acc]
        end

      assert failures == [], """
      A shape with a dependency left state behind, or raised, when removed.

      #{Enum.map_join(Enum.take(failures, 8), "\n\n", fn {clauses, order, problem} -> """
        clauses:  #{inspect(clauses)}
        order:    #{inspect(order)}
        problem:  #{inspect(problem)}
        """ end)}
      """
    end
  end
end
