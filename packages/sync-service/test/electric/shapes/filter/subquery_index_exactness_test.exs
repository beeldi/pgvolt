defmodule Electric.Shapes.Filter.Indexes.SubqueryIndexExactnessTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["table", "another_table"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "number", type: "int8"}
               ]
             )

  # `affected_shapes_exact/2` promises that an exact decision may be shared across a conversion
  # group without re-evaluation. A shape whose subquery view has not been seeded is routed from the
  # fallback set — an over-inclusive answer — so the decision must report itself inexact, the same
  # way every non-subquery routing path does when it widens to all shapes.
  describe "subquery routing exactness" do
    test "an unseeded subquery shape marks the routing decision inexact" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("table",
            where: "id IN (SELECT id FROM another_table)",
            inspector: @inspector
          )
        )

      change = %NewRecord{relation: {"public", "table"}, record: %{"id" => "1", "number" => "1"}}

      {affected, exact?} = Filter.affected_shapes_exact(filter, change)

      assert affected == MapSet.new(["s1"])
      refute exact?
    end

    test "a plain equality shape stays exact" do
      filter =
        Filter.new()
        |> Filter.add_shape("s1", Shape.new!("table", where: "id = 1", inspector: @inspector))

      change = %NewRecord{relation: {"public", "table"}, record: %{"id" => "1", "number" => "1"}}

      {affected, exact?} = Filter.affected_shapes_exact(filter, change)

      assert affected == MapSet.new(["s1"])
      assert exact?
    end
  end
end
