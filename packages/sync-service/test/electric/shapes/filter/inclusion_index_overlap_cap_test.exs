defmodule Electric.Shapes.Filter.Indexes.InclusionIndexOverlapCapTest do
  @moduledoc """
  `a && ARRAY[v]` indexes as the one-element containment `a @> ARRAY[v]`, and a multi-element
  `a && ARRAY[v1, …, vn]` fans out to one such single-element `&&` disjunct per element, each
  re-entering the single-element clause. Both commit their value to the inclusion index through that
  single-element clause, which stored `[value]` uncapped: a multidimensional overlap constant
  (`a && '{{1,…,n}}'`) parses to a nested list whose one outer element is an n-element sub-array, so
  the tree grew the same n(n+1)/2 root-to-leaf spine the flat over-cap `@>` array is refused for.
  These pin that the single-element clause now counts the flattened elements, capping both the direct
  single-element overlap and every fan-out disjunct, while ordinary scalar and within-cap overlaps
  stay fully indexed and route through the tree.

  `const = ANY(array_ref)` is the third clause that commits `[value]` to the index, but its constant
  is the array's scalar element type, never a sub-array, so `[value]` always flattens to length one;
  it cannot receive a nested value and is left unchanged.
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

  # Outer arity 1 for the single-element clause and 2 for the fan-out, both under
  # @max_distributed_disjuncts, so the fan-out is never what refuses them; the leaf count is.
  @over_cap 1..4000
  @over_cap_a 1..2000
  @over_cap_b 2001..4000
  @under_cap_a 1..5
  @under_cap_b 6..10

  defp change(record), do: %NewRecord{relation: {"public", "t1"}, record: record}

  defp flat_literal(values), do: "{" <> Enum.join(values, ",") <> "}"

  defp single_nested_literal(values), do: "{{" <> Enum.join(values, ",") <> "}}"

  defp multi_nested_literal(first, second),
    do: "{{" <> Enum.join(first, ",") <> "},{" <> Enum.join(second, ",") <> "}}"

  defp overlap_shape(literal) do
    Shape.new!("t1", where: "an_array && '#{literal}'", inspector: @inspector)
  end

  describe "single-element `&&` clause" do
    test "an over-cap single-element overlap is left unindexed and builds no inclusion tree" do
      over_cap_shape = overlap_shape(single_nested_literal(@over_cap))

      filter = Filter.add_shape(Filter.new(), "s1", over_cap_shape)

      refute Filter.indexed_shape?(over_cap_shape)
      assert :ets.info(filter.incl_index_table, :size) == 0
    end

    test "an under-cap single-element overlap is indexed and builds the inclusion tree" do
      under_cap_shape = overlap_shape(single_nested_literal(@under_cap_a))

      filter = Filter.add_shape(Filter.new(), "s1", under_cap_shape)

      assert Filter.indexed_shape?(under_cap_shape)
      assert :ets.info(filter.incl_index_table, :size) > 0
    end

    test "an ordinary scalar overlap is indexed and builds the inclusion tree" do
      scalar_shape = overlap_shape(flat_literal([5]))

      filter = Filter.add_shape(Filter.new(), "s1", scalar_shape)

      assert Filter.indexed_shape?(scalar_shape)
      assert :ets.info(filter.incl_index_table, :size) > 0
    end

    test "an over-cap single-element overlap still routes correctly through other_shapes" do
      filter =
        Filter.add_shape(Filter.new(), "s1", overlap_shape(single_nested_literal(@over_cap)))

      overlaps = Filter.affected_shapes(filter, change(%{"an_array" => flat_literal([1])}))
      disjoint = Filter.affected_shapes(filter, change(%{"an_array" => flat_literal([99_999])}))

      assert overlaps == MapSet.new(["s1"])
      assert disjoint == MapSet.new()
    end
  end

  describe "multi-element `&&` fan-out clause" do
    test "an over-cap multi-element overlap builds no inclusion tree" do
      over_cap_shape = overlap_shape(multi_nested_literal(@over_cap_a, @over_cap_b))

      filter = Filter.add_shape(Filter.new(), "s1", over_cap_shape)

      assert :ets.info(filter.incl_index_table, :size) == 0
    end

    test "an under-cap multi-element overlap is indexed and builds the inclusion tree" do
      under_cap_shape = overlap_shape(multi_nested_literal(@under_cap_a, @under_cap_b))

      filter = Filter.add_shape(Filter.new(), "s1", under_cap_shape)

      assert :ets.info(filter.incl_index_table, :size) > 0
    end

    test "an over-cap multi-element overlap still routes correctly through other_shapes" do
      filter =
        Filter.add_shape(
          Filter.new(),
          "s1",
          overlap_shape(multi_nested_literal(@over_cap_a, @over_cap_b))
        )

      overlaps_first = Filter.affected_shapes(filter, change(%{"an_array" => flat_literal([1])}))

      overlaps_second =
        Filter.affected_shapes(filter, change(%{"an_array" => flat_literal([2500])}))

      disjoint = Filter.affected_shapes(filter, change(%{"an_array" => flat_literal([99_999])}))

      assert overlaps_first == MapSet.new(["s1"])
      assert overlaps_second == MapSet.new(["s1"])
      assert disjoint == MapSet.new()
    end
  end
end
