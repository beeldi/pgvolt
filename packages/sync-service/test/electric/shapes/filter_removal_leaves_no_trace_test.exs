defmodule Electric.Shapes.FilterRemovalLeavesNoTraceTest do
  @moduledoc """
  A shape that outlives its removal in any filter table keeps being routed changes, and where
  a shape's where clause encodes row-level access, a leaked row means a revoked client keeps
  receiving rows.

  Removal mirrors the add walk by re-deriving the same optimisation decisions from the
  clause, which means every clause form reaches a different combination of index, residual
  and branch bookkeeping, and each one unwinds by its own path. Two forms were asserted
  before this, each over two or three tables. This asserts every form over every table.
  """
  use ExUnit.Case, async: true

  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @employees StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"},
                 %{name: "deletedAt", type: "text"},
                 %{name: "metaJson", type: "text"}
               ]
             )

  @ticket StubInspector.new(
            tables: ["ticket"],
            columns: [
              %{name: "id", type: "text", pk_position: 0},
              %{name: "firmId", type: "text"},
              %{name: "divisionIds", array_type: "text"},
              %{name: "deletedAt", type: "text"}
            ]
          )

  @clauses [
    {"no where clause", "employees", @employees, nil},
    {"a single equality", "employees", @employees, ~s|"firmId" = 'p1'|},
    {"a multi-division filter", "employees", @employees,
     ~s|"firmId" = 'p1' AND "divisionId" IN ('b1', 'b2', 'b3')|},
    {"a filter with a rotation token", "employees", @employees,
     ~s|"firmId" = 'p1' AND "divisionId" IN ('b1', 'b2') AND 'rot:7' = 'rot:7'|},
    {"a filter with an inert or", "employees", @employees,
     ~s|"firmId" = 'p1' AND "divisionId" IN ('b1', 'b2') AND ("deletedAt" IS NULL OR "metaJson" IS NOT NULL)|},
    {"two IN lists", "employees", @employees,
     ~s|"firmId" IN ('p1', 'p2') AND "divisionId" IN ('b1', 'b2')|},
    {"an unindexable clause", "employees", @employees, ~s|"firmId" != 'p1'|},
    {"an array containment", "ticket", @ticket, ~s|"divisionIds" @> ARRAY['b1']|},
    {"an array overlap", "ticket", @ticket,
     ~s|"firmId" = 'p1' AND "divisionIds" && ARRAY['b1', 'b2']|}
  ]

  defp tables(%Filter{} = filter) do
    [
      shapes: filter.shapes_table,
      tables: filter.tables_table,
      where_cond: filter.where_cond_table,
      other_shapes: filter.other_shapes_table,
      eq_index: filter.eq_index_table,
      incl_index: filter.incl_index_table,
      relation_index: filter.relation_index_table
    ]
    |> Map.new(fn {name, table} -> {name, :ets.info(table, :size)} end)
  end

  for {description, root_table, inspector, where} <- @clauses do
    test "removing a shape with #{description} leaves no trace" do
      # Arrange
      filter = Filter.new()
      empty = tables(filter)

      shape =
        Shape.new!(unquote(root_table),
          where: unquote(where),
          inspector: unquote(Macro.escape(inspector))
        )

      filter = Filter.add_shape(filter, "s1", shape)
      assert tables(filter) != empty, "the shape was never registered, so removal proves nothing"

      # Act
      filter = Filter.remove_shape(filter, "s1")

      # Assert
      assert tables(filter) == empty
      assert Filter.active_shapes(filter) == []
      refute Filter.has_shape?(filter, "s1")
    end

    test "removing one shape with #{description} leaves a second one intact" do
      # Arrange
      filter = Filter.new()

      shape =
        Shape.new!(unquote(root_table),
          where: unquote(where),
          inspector: unquote(Macro.escape(inspector))
        )

      filter = Filter.add_shape(filter, "s1", shape)
      filter = Filter.add_shape(filter, "s2", shape)
      both = tables(filter)

      # Act
      filter = Filter.remove_shape(filter, "s1")
      after_first = tables(filter)
      filter = Filter.remove_shape(filter, "s2")

      # Assert: the shared structure survives the first removal and only then unwinds.
      assert Filter.active_shapes(filter) == []
      assert after_first.shapes == both.shapes - 1
      assert tables(filter) == tables(Filter.new())
    end
  end
end
