defmodule Electric.Shapes.FilterOverlapDistributionSymmetryTest do
  @moduledoc """
  `distributable_residual?/1` decided whether an AND's residual would multiply out by testing the
  node's *name* (`"or"`), but the fork added a second producer of `{:or, disjuncts}`: a
  multi-element `a && ARRAY[...]` fans out to one `@>` disjunct per element under the name `"&&"`.
  Testing the name let it through as inert, so `IN(n) AND &&(m)` distributed while the mirrored
  `&&(m) AND IN(n)` -- the same predicate -- dropped to the per-change `other_shapes` scan. This
  pins that clause order does not decide indexability.
  """
  use ExUnit.Case, async: true

  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["t1"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "divisionIds", array_type: "text"}
               ]
             )

  defp shape(where), do: Shape.new!("t1", where: where, inspector: @inspector)

  test "a commutative IN/overlap predicate is indexed the same way regardless of clause order" do
    in_then_overlap = shape(~s|id IN (1, 2, 3) AND "divisionIds" && ARRAY['b1', 'b2']::text[]|)
    overlap_then_in = shape(~s|"divisionIds" && ARRAY['b1', 'b2']::text[] AND id IN (1, 2, 3)|)

    # order indexes -- but the two orderings must AGREE, which the name-based guard did not deliver.
    # The name-based guard let `IN AND &&` through as indexed while its mirror stayed unindexed.
    assert Filter.indexed_shape?(in_then_overlap) == Filter.indexed_shape?(overlap_then_in)
    refute Filter.indexed_shape?(in_then_overlap)
  end
end
