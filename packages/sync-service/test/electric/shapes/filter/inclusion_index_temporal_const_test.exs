defmodule Electric.Shapes.Filter.Indexes.InclusionIndexTemporalConstTest do
  @moduledoc """
  The inclusion index keys its tree on array *elements* by term equality. A `time[]` element the
  where const spells `12:00:00.000000` parses to `%Time{microsecond: {0, 6}}` while the WAL record
  spells it `12:00:00` and parses to `{0, 0}`; Postgres calls the two equal, but keying the tree on
  the raw struct routes a change carrying the second spelling to no shape. These pin that both sides
  are canonicalised to Postgres's microsecond precision so the index and the interpreter agree.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["events"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "times", array_type: "time"}
               ]
             )

  defp change(record), do: %NewRecord{relation: {"public", "events"}, record: record}

  test "routes a time-array record whose element differs only in microsecond precision" do
    filter =
      Filter.new()
      |> Filter.add_shape(
        "s1",
        Shape.new!("events", where: "times @> '{12:00:00.000000}'", inspector: @inspector)
      )

    affected = Filter.affected_shapes(filter, change(%{"id" => "1", "times" => "{12:00:00}"}))

    assert affected == MapSet.new(["s1"])
  end

  test "does not route a time-array record whose element Postgres considers unequal" do
    filter =
      Filter.new()
      |> Filter.add_shape(
        "s1",
        Shape.new!("events", where: "times @> '{12:00:00.000000}'", inspector: @inspector)
      )

    affected = Filter.affected_shapes(filter, change(%{"id" => "1", "times" => "{12:00:01}"}))

    assert affected == MapSet.new()
  end
end
