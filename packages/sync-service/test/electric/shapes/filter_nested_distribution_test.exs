defmodule Electric.Shapes.FilterNestedDistributionTest do
  @moduledoc """
  Deciding whether `(a OR b) AND c` may distribute asks for the analysis of `c`, which the
  optimiser has just computed. Computed again at every level, a clause that nests that form costs
  twice as much per level: 18 levels took 160 ms, so a where clause of a few hundred bytes more
  held the process adding the shape for hours. The analysis has to stay linear in the clause.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.WhereCondition
  alias Electric.Shapes.Shape
  alias Electric.Shapes.WhereClause
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"}
               ]
             )

  # Two levels of nesting per step, so this stays inside the parser's depth limit of 100.
  @levels 40

  # (("divisionId" IN ('b1', 'b2') AND (...)) OR "firmId" = 'F2') OR "firmId" = 'F1' ...
  defp nested_where(levels) do
    Enum.reduce(1..levels, ~s|"firmId" = 'F0'|, fn level, inner ->
      ~s|("divisionId" IN ('b1', 'b2') AND (#{inner})) OR "firmId" = 'F#{level}'|
    end)
  end

  test "a deeply nested distribution is analysed and routes like the clause evaluates" do
    shape = Shape.new!("employees", where: nested_where(@levels), inspector: @inspector)

    assert is_boolean(WhereCondition.indexed_where?(shape.where))

    filter = Filter.new() |> Filter.add_shape("shape", shape)

    for firm <- ["F0", "F1", "F#{@levels}", "F#{@levels + 1}"],
        division <- ["b1", "b3"] do
      record = %{"id" => "1", "firmId" => firm, "divisionId" => division}
      change = %NewRecord{relation: {"public", "employees"}, record: record}

      expected = WhereClause.includes_record?(shape.where, record)
      routed = MapSet.member?(Filter.affected_shapes(filter, change), "shape")

      assert {firm, division, routed} == {firm, division, expected}
    end
  end

  # 4,000 terms keep the clause well within the SQL parser's input limit of 64 KB. The analysis
  # takes a few milliseconds here; quadratic in the chain, it took over a second.
  test "a long AND chain with no indexed term is analysed in linear time" do
    where = Enum.map_join(1..4_000, " AND ", fn _ -> "id<>'a'" end)
    shape = Shape.new!("employees", where: where, inspector: @inspector)

    {optimise_us, optimisation} = :timer.tc(fn -> WhereCondition.optimise(shape.where) end)
    {add_us, filter} = :timer.tc(fn -> Filter.add_shape(Filter.new(), "shape", shape) end)

    refute WhereCondition.indexed_optimisation?(optimisation)

    assert optimise_us < 200_000 and add_us < 200_000,
           "the analysis took #{optimise_us} us, the add #{add_us} us"

    assert filter |> Filter.remove_shape("shape") |> Filter.active_shapes() == []
  end

  test "removing a deeply nested distribution is analysed once too" do
    shape = Shape.new!("employees", where: nested_where(@levels), inspector: @inspector)
    filter = Filter.new() |> Filter.add_shape("shape", shape)

    Filter.remove_shape(filter, "shape")

    assert Filter.active_shapes(filter) == []
  end
end
