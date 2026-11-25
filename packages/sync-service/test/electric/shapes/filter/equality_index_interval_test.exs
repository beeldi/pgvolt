defmodule Electric.Shapes.Filter.Indexes.EqualityIndexIntervalTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["events"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "duration", type: "interval"}
               ]
             )

  # `interval = interval` compares by span with a month taken as 30 days, so Postgres calls
  # `interval '1 month'` and `interval '30 days'` equal. The equality index must route a record
  # carrying either spelling to a shape filtered on the other, or a live change both engines agree
  # matches is silently dropped from the shape.
  describe "interval equality routing" do
    test "routes a 30-day record to a shape filtered on 1 month" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("events", where: "duration = interval '1 month'", inspector: @inspector)
        )

      change = %NewRecord{relation: {"public", "events"}, record: %{"duration" => "30 days"}}

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new(["s1"])
    end

    test "routes a 1-month record to a shape filtered on 30 days" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("events", where: "duration = interval '30 days'", inspector: @inspector)
        )

      change = %NewRecord{relation: {"public", "events"}, record: %{"duration" => "1 mon"}}

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new(["s1"])
    end

    test "does not route an interval Postgres considers unequal" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("events", where: "duration = interval '1 month'", inspector: @inspector)
        )

      change = %NewRecord{relation: {"public", "events"}, record: %{"duration" => "31 days"}}

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new([])
    end
  end
end
