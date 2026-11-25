defmodule Electric.Shapes.Filter.Indexes.EqualityIndexTemporalConstTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["events"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "logged_at", type: "time"},
                 %{name: "created_at", type: "timestamp"}
               ]
             )

  # Postgres compares `time`/`timestamp` by instant, but the Elixir struct carries `microsecond` as
  # `{value, precision}` and the precision digit tracks the text spelling: the where const
  # `12:00:00.000000` parses to `{0, 6}` while the WAL record `12:00:00` parses to `{0, 0}`. On the
  # :set index those are distinct keys, so the shape gets its snapshot and never another row even
  # though both engines call the values equal. The index must route every WAL spelling of the value.
  describe "sub-second-precision const against a temporal column" do
    test "routes a time record to a shape whose const differs only in microsecond precision" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("events",
            where: "logged_at = time '12:00:00.000000'",
            inspector: @inspector
          )
        )

      for spelling <- ["12:00:00", "12:00:00.0", "12:00:00.000000"] do
        change = %NewRecord{
          relation: {"public", "events"},
          record: %{"id" => "1", "logged_at" => spelling}
        }

        affected = Filter.affected_shapes(filter, change)

        assert affected == MapSet.new(["s1"]),
               "a time record #{inspect(spelling)} was not routed to `logged_at = 12:00:00.000000`"
      end
    end

    test "routes a timestamp record to a shape whose const differs only in microsecond precision" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("events",
            where: "created_at = timestamp '2024-01-01 12:00:00.000000'",
            inspector: @inspector
          )
        )

      for spelling <- ["2024-01-01 12:00:00", "2024-01-01 12:00:00.000000"] do
        change = %NewRecord{
          relation: {"public", "events"},
          record: %{"id" => "1", "created_at" => spelling}
        }

        affected = Filter.affected_shapes(filter, change)

        assert affected == MapSet.new(["s1"]),
               "a timestamp record #{inspect(spelling)} was not routed to the microsecond const"
      end
    end

    test "does not route a time the operator considers unequal" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("events",
            where: "logged_at = time '12:00:00.000000'",
            inspector: @inspector
          )
        )

      change = %NewRecord{
        relation: {"public", "events"},
        record: %{"id" => "1", "logged_at" => "12:00:01"}
      }

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new([])
    end
  end
end
