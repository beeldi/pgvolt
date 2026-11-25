defmodule Electric.Shapes.Filter.Indexes.InclusionIndexMultidimTest do
  @moduledoc """
  Postgres's array containment operators (`@>`, `<@`, `&&`, `= ANY`) compare element membership
  regardless of dimension, so `{{1,2},{3,4}} @> {1}` holds. The inclusion index keys its tree on
  array *elements*, but the array parser returns a multidimensional value as nested lists; keying
  the tree on `[1, 2]` where a shape is registered under `1` routes such a change to no shape.
  These pin that both sides are flattened so the index and the interpreter agree on membership.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["t1"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "an_array", array_type: "int8"}
               ]
             )

  defp change(record), do: %NewRecord{relation: {"public", "t1"}, record: record}

  test "a multidimensional change value routes to a shape its flattened elements contain" do
    filter =
      Filter.new()
      |> Filter.add_shape(
        "s1",
        Shape.new!("t1", where: "an_array @> '{1}'", inspector: @inspector)
      )

    affected = Filter.affected_shapes(filter, change(%{"an_array" => "{{1,2},{3,4}}"}))

    assert affected == MapSet.new(["s1"])
  end

  test "a multidimensional change value that shares no element routes nowhere" do
    filter =
      Filter.new()
      |> Filter.add_shape(
        "s1",
        Shape.new!("t1", where: "an_array @> '{9}'", inspector: @inspector)
      )

    affected = Filter.affected_shapes(filter, change(%{"an_array" => "{{1,2},{3,4}}"}))

    assert affected == MapSet.new()
  end
end
