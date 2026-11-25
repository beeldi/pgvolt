defmodule Electric.Shapes.Filter.Indexes.InclusionIndexNestedCapTest do
  @moduledoc """
  A multidimensional containment constant (`array_col @> '{{1,…,n}}'`) parses to nested lists, and
  the inclusion index flattens the value before storing one ETS node per element. The cap that keeps
  `@>`/`<@` from building the O(n^2) spine counted the outer list, so a 1×n array slipped past it at
  outer arity 1 while allocating the same n(n+1)/2 path the flat over-cap array is refused for. These
  pin that the cap now counts the flattened elements, so an over-cap nested clause takes the same
  `other_shapes` fallback as its flat twin while a within-cap nested clause is still fully indexed.
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

  # Outer arity 1 in both, so the old cap always let them through; the leaf count is what differs.
  @over_cap 1..4000
  @under_cap 1..10

  defp change(record), do: %NewRecord{relation: {"public", "t1"}, record: record}

  defp flat_literal(values), do: "{" <> Enum.join(values, ",") <> "}"

  defp nested_literal(values), do: "{{" <> Enum.join(values, ",") <> "}}"

  defp nested_shape(values) do
    Shape.new!("t1", where: "an_array @> '#{nested_literal(values)}'", inspector: @inspector)
  end

  defp flat_shape(values) do
    Shape.new!("t1", where: "an_array @> '#{flat_literal(values)}'", inspector: @inspector)
  end

  test "an over-cap nested @> array is left unindexed and builds no inclusion tree" do
    over_cap_shape = nested_shape(@over_cap)

    filter = Filter.add_shape(Filter.new(), "s1", over_cap_shape)

    refute Filter.indexed_shape?(over_cap_shape)
    assert :ets.info(filter.incl_index_table, :size) == 0
  end

  test "an over-cap nested @> array takes the same fallback as an over-cap flat @> array" do
    nested = nested_shape(@over_cap)
    flat = flat_shape(@over_cap)

    nested_indexed? = Filter.indexed_shape?(nested)
    flat_indexed? = Filter.indexed_shape?(flat)

    nested_size =
      :ets.info(Filter.add_shape(Filter.new(), "s1", nested).incl_index_table, :size)

    flat_size =
      :ets.info(Filter.add_shape(Filter.new(), "s1", flat).incl_index_table, :size)

    assert nested_indexed? == flat_indexed?
    assert nested_size == flat_size
    assert nested_size == 0
  end

  test "an under-cap nested @> array is indexed and builds the inclusion tree" do
    under_cap_shape = nested_shape(@under_cap)

    filter = Filter.add_shape(Filter.new(), "s1", under_cap_shape)

    assert Filter.indexed_shape?(under_cap_shape)
    assert :ets.info(filter.incl_index_table, :size) > 0
  end

  test "an under-cap nested @> array builds the same tree as its flat twin" do
    nested_filter = Filter.add_shape(Filter.new(), "s1", nested_shape(@under_cap))
    flat_filter = Filter.add_shape(Filter.new(), "s1", flat_shape(@under_cap))

    nested_size = :ets.info(nested_filter.incl_index_table, :size)
    flat_size = :ets.info(flat_filter.incl_index_table, :size)

    assert nested_size == flat_size
    assert nested_size > 0
  end

  test "an over-cap nested @> shape still routes correctly through other_shapes" do
    filter = Filter.add_shape(Filter.new(), "s1", nested_shape(@over_cap))

    contains_all =
      Filter.affected_shapes(filter, change(%{"an_array" => flat_literal(@over_cap)}))

    missing_one =
      Filter.affected_shapes(filter, change(%{"an_array" => flat_literal(1..3999)}))

    assert contains_all == MapSet.new(["s1"])
    assert missing_one == MapSet.new()
  end

  test "an under-cap nested @> shape routes correctly through the index" do
    filter = Filter.add_shape(Filter.new(), "s1", nested_shape(@under_cap))

    contains_all =
      Filter.affected_shapes(filter, change(%{"an_array" => flat_literal(@under_cap)}))

    missing_one =
      Filter.affected_shapes(filter, change(%{"an_array" => flat_literal(1..9)}))

    assert contains_all == MapSet.new(["s1"])
    assert missing_one == MapSet.new()
  end
end
