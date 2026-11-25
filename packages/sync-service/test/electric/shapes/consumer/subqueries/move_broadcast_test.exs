defmodule Electric.Shapes.Consumer.Subqueries.MoveBroadcastTest do
  use ExUnit.Case, async: true

  alias Electric.Shapes.Consumer.Subqueries.MoveBroadcast

  # Tags of a shape `a IN sub0 OR (b IN sub1 AND c = 1)`: position 0 in the first disjunct,
  # positions 1 and 2 in the second.
  @tags ["a1//", "/b1/1"]
  @patterns %{0 => MapSet.new(["a1"])}

  describe "removed_by?/3" do
    test "a row leaves when its only active disjunct loses a position" do
      assert MoveBroadcast.removed_by?(@tags, [true, false, true], @patterns)
    end

    test "a row stays while another disjunct keeps each of its positions active" do
      refute MoveBroadcast.removed_by?(@tags, [true, true, true], @patterns)
    end

    test "a row leaves when its other disjunct holds a value no pattern names but was not active" do
      assert MoveBroadcast.removed_by?(@tags, [true, true, false], @patterns)
    end

    test "a row no pattern reaches stays, whatever its active conditions" do
      refute MoveBroadcast.removed_by?(@tags, [false, false, false], %{1 => MapSet.new(["b2"])})
    end

    test "a row without tags stays" do
      refute MoveBroadcast.removed_by?([], [], @patterns)
    end
  end

  describe "matched_by?/2" do
    test "matches a tag carrying a value a pattern names at its position" do
      assert MoveBroadcast.matched_by?(@tags, @patterns)
      refute MoveBroadcast.matched_by?(@tags, %{1 => MapSet.new(["a1"])})
      refute MoveBroadcast.matched_by?(@tags, %{})
    end
  end
end
