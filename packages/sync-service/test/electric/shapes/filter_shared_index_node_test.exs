defmodule Electric.Shapes.FilterSharedIndexNodeTest do
  @moduledoc """
  The correctness cases not covered by filter_removal_leaves_no_trace_test /
  filter_perimeter_memory_test:
  cross-firm overlapping IN lists, re-add after remove, and one shape reaching the
  SAME (field, value) index node twice with DIFFERENT residuals.
  """
  use ExUnit.Case, async: false

  alias Electric.Replication.Changes.NewRecord
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
                 %{name: "deletedAt", type: "timestamptz"}
               ]
             )

  @token ~s|'cache-buster:00003' = 'cache-buster:00003'|

  defp shape(where), do: Shape.new!("employees", where: where, inspector: @inspector)

  defp rec(firm, division, category \\ "c1", deleted \\ nil) do
    %NewRecord{
      relation: {"public", "employees"},
      record: %{
        "id" => "e1",
        "firmId" => firm,
        "divisionId" => division,
        "categoryId" => category,
        "deletedAt" => deleted
      }
    }
  end

  defp routed(f, change), do: f |> Filter.affected_shapes(change) |> Enum.sort()

  defp rows(f) do
    for t <- [f.where_cond_table, f.other_shapes_table, f.eq_index_table, f.shapes_table],
        do: :ets.info(t)[:size]
  end

  test "IN lists that overlap across different firms stay firm-isolated" do
    # Arrange - d5 and d6 appear in BOTH firms' perimeters
    f0 =
      ~s|"firmId" = 'F0' AND "divisionId" IN ('d4', 'd5', 'd6') AND "deletedAt" IS NULL AND | <>
        @token

    f1 =
      ~s|"firmId" = 'F1' AND "divisionId" IN ('d5', 'd6', 'd7') AND "deletedAt" IS NULL AND | <>
        @token

    # and a shape with NO firm predicate at all, covering the same divisions
    anyfirm = ~s|"divisionId" IN ('d5', 'd9') AND "deletedAt" IS NULL|

    f =
      Filter.new()
      |> Filter.add_shape("f0", shape(f0))
      |> Filter.add_shape("f1", shape(f1))
      |> Filter.add_shape("any", shape(anyfirm))

    # Act / Assert
    for {change, expected, label} <- [
          {rec("F0", "d5"), ["any", "f0"], "F0/d5 (shared division)"},
          {rec("F1", "d5"), ["any", "f1"], "F1/d5 (shared division)"},
          {rec("F0", "d7"), [], "F0/d7 (in F1's list only)"},
          {rec("F1", "d4"), [], "F1/d4 (in F0's list only)"},
          {rec("F2", "d5"), ["any"], "F2/d5 (unknown firm)"},
          {rec("F0", "d5", "c1", "2026-01-01 00:00:00Z"), [], "F0/d5 soft-deleted"}
        ] do
      actual = routed(f, change)
      assert actual == expected, "#{label}: expected #{inspect(expected)}, got #{inspect(actual)}"
    end
  end

  test "one shape hits the SAME (field,value) node twice with DIFFERENT residuals" do
    # Arrange - both disjuncts index on divisionId = 'd1', with different residuals
    where =
      ~s|("divisionId" = 'd1' AND "categoryId" = 'c1') OR ("divisionId" = 'd1' AND "deletedAt" IS NULL)|

    f = Filter.add_shape(Filter.new(), "s", shape(where))

    # Act / Assert - each residual must be independently satisfiable
    cases = [
      {rec("F0", "d1", "c1", "2026-01-01 00:00:00Z"), ["s"], "c1 + deleted (left disjunct only)"},
      {rec("F0", "d1", "c9", nil), ["s"], "c9 + live    (right disjunct only)"},
      {rec("F0", "d1", "c1", nil), ["s"], "c1 + live    (both disjuncts)"},
      {rec("F0", "d1", "c9", "2026-01-01 00:00:00Z"), [], "c9 + deleted (neither)"},
      {rec("F0", "d2", "c1", nil), [], "wrong division"}
    ]

    for {change, expected, label} <- cases do
      actual = routed(f, change)
      assert actual == expected, "#{label}: expected #{inspect(expected)}, got #{inspect(actual)}"
    end

    # Act - removal must unwind both branches of the shared node
    f = Filter.remove_shape(f, "s")

    # Assert
    assert rows(f) == [0, 0, 0, 0]
  end

  test "two DIFFERENT shapes sharing a (field,value) node with different residuals" do
    a = ~s|"firmId" = 'F0' AND "divisionId" IN ('d1', 'd2') AND "categoryId" = 'c1'|
    b = ~s|"firmId" = 'F0' AND "divisionId" IN ('d1', 'd3') AND "deletedAt" IS NULL|

    f =
      Filter.new()
      |> Filter.add_shape("a", shape(a))
      |> Filter.add_shape("b", shape(b))

    for {change, expected, label} <- [
          {rec("F0", "d1", "c1", nil), ["a", "b"], "d1 + c1 + live"},
          {rec("F0", "d1", "c9", nil), ["b"], "d1 + c9 + live"},
          {rec("F0", "d1", "c1", "2026-01-01 00:00:00Z"), ["a"], "d1 + c1 + deleted"},
          {rec("F0", "d2", "c1", nil), ["a"], "d2 (a only)"},
          {rec("F0", "d3", "c1", nil), ["b"], "d3 (b only)"}
        ] do
      actual = routed(f, change)
      assert actual == expected, "#{label}: expected #{inspect(expected)}, got #{inspect(actual)}"
    end

    # Act - remove only "a"; "b" must keep its half of the shared node
    f = Filter.remove_shape(f, "a")

    # Assert
    assert routed(f, rec("F0", "d1", "c1", nil)) == ["b"]
    assert routed(f, rec("F0", "d2", "c1", nil)) == []
    assert routed(f, rec("F0", "d3", "c1", nil)) == ["b"]
  end

  test "re-add after remove, and re-add after PARTIAL removal" do
    perim = fn i ->
      ~s|"firmId" = 'F0' AND "divisionId" IN ('d#{i}1', 'd#{i}2', 'd#{i}3') AND "deletedAt" IS NULL AND | <>
        @token
    end

    empty = Filter.new()

    f =
      Enum.reduce(1..5, empty, fn i, f -> Filter.add_shape(f, "h#{i}", shape(perim.(i))) end)

    after_add = rows(f)

    # Act - full drain then re-add the identical set
    f = Enum.reduce(1..5, f, fn i, f -> Filter.remove_shape(f, "h#{i}") end)
    assert rows(f) == [0, 0, 0, 0]

    f = Enum.reduce(1..5, f, fn i, f -> Filter.add_shape(f, "h#{i}", shape(perim.(i))) end)
    assert rows(f) == after_add, "re-add did not reproduce the original tree"
    assert routed(f, rec("F0", "d32")) == ["h3"]

    # Act - partial removal, then re-add the removed shape
    f = Filter.remove_shape(f, "h3")
    assert routed(f, rec("F0", "d32")) == []
    assert routed(f, rec("F0", "d42")) == ["h4"]

    f = Filter.add_shape(f, "h3", shape(perim.(3)))
    assert rows(f) == after_add
    assert routed(f, rec("F0", "d32")) == ["h3"]
    assert routed(f, rec("F0", "d42")) == ["h4"]

    # Act - 20 churn cycles on a single shape inside a populated filter
    f =
      Enum.reduce(1..20, f, fn _, f ->
        f |> Filter.remove_shape("h2") |> Filter.add_shape("h2", shape(perim.(2)))
      end)

    assert rows(f) == after_add, "churn leaked rows"
    assert routed(f, rec("F0", "d22")) == ["h2"]
  end
end
