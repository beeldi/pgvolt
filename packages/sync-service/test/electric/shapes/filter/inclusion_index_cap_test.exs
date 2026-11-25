defmodule Electric.Shapes.Filter.Indexes.InclusionIndexCapTest do
  @moduledoc """
  `array_col @> ARRAY[v1, …, vn]` is indexed by storing the shape's sorted array as a root-to-leaf
  path in an ETS tree, one node per prefix. That is n(n+1)/2 stored path elements and an O(n^2)
  insert on the routing spine for a single clause, and it was uncapped: a ~10KB containment clause
  could drive the index to hundreds of MB and stall replication. `optimise_where/1` now leaves an
  array longer than `@max_inclusion_elements` unindexed, so it routes through `other_shapes` --
  correct, and linear in the array per change rather than quadratic at insert. These pin that the
  tree is never built for an over-cap clause and that routing stays correct on both sides of the cap.
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

  # Well past @max_inclusion_elements, in the regime the finding reproduced (n≈3200 → ~205MB); and
  # a handful, well under it.
  @over_cap 1..4000
  @under_cap 1..10

  defp change(record), do: %NewRecord{relation: {"public", "t1"}, record: record}

  defp record_array(values), do: "{" <> Enum.join(values, ",") <> "}"

  defp shape(values) do
    Shape.new!("t1",
      where: "an_array @> '#{record_array(values)}'",
      inspector: @inspector
    )
  end

  test "an over-cap @> array is left unindexed and builds no inclusion tree" do
    over_cap_shape = shape(@over_cap)

    filter = Filter.add_shape(Filter.new(), "s1", over_cap_shape)

    refute Filter.indexed_shape?(over_cap_shape)
    assert :ets.info(filter.incl_index_table, :size) == 0
  end

  test "an under-cap @> array is indexed and builds the inclusion tree" do
    under_cap_shape = shape(@under_cap)

    filter = Filter.add_shape(Filter.new(), "s1", under_cap_shape)

    assert Filter.indexed_shape?(under_cap_shape)
    assert :ets.info(filter.incl_index_table, :size) > 0
  end

  test "an over-cap @> shape still routes correctly through other_shapes" do
    filter = Filter.add_shape(Filter.new(), "s1", shape(@over_cap))

    contains_all =
      Filter.affected_shapes(filter, change(%{"an_array" => record_array(@over_cap)}))

    missing_one =
      Filter.affected_shapes(filter, change(%{"an_array" => record_array(1..3999)}))

    assert contains_all == MapSet.new(["s1"])
    assert missing_one == MapSet.new()
  end
end
