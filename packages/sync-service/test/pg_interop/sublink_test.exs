defmodule PgInterop.SublinkTest do
  use ExUnit.Case, async: true

  alias PgInterop.Sublink

  test "a complete composite miss uses the materialized view's NULL keys only" do
    ref = ["$sublink", "0"]
    moves = Enum.map(1..100_000, &{{&1, &1}, nil})

    view =
      %{ref => MapSet.new()}
      |> Electric.Shapes.Consumer.Subqueries.Views.apply_move(ref, moves, :move_in)
      |> Map.fetch!(ref)

    {:reductions, before} = Process.info(self(), :reductions)
    assert Sublink.member?({-1, -1}, view) == false
    {:reductions, after_lookup} = Process.info(self(), :reductions)
    assert after_lookup - before < 10_000

    views = %{ref => view}
    nullable_key = {{-1, nil}, nil}

    views =
      Electric.Shapes.Consumer.Subqueries.Views.apply_move(views, ref, [nullable_key], :move_in)

    assert Sublink.member?({-1, -1}, Map.fetch!(views, ref)) == nil
    assert Sublink.member?({1, 1}, Map.fetch!(views, ref)) == true
    assert Sublink.member?({-2, -2}, Map.fetch!(views, ref)) == false

    views =
      Electric.Shapes.Consumer.Subqueries.Views.apply_move(views, ref, [nullable_key], :move_out)

    assert Sublink.member?({-1, -1}, Map.fetch!(views, ref)) == false
  end

  describe "member?/2 three-valued membership" do
    test "returns true when the value is present, over a list and a set" do
      assert Sublink.member?(2, [1, 2, 3]) == true
      assert Sublink.member?(2, MapSet.new([1, 2, 3])) == true
    end

    test "returns false for a non-member when the set has no NULL" do
      assert Sublink.member?(9, [1, 2, 3]) == false
      assert Sublink.member?(9, MapSet.new([1, 2, 3])) == false
    end

    test "returns nil (unknown) for a non-member when a NULL is in the set" do
      assert Sublink.member?(9, [1, 2, nil]) == nil
      assert Sublink.member?(9, MapSet.new([1, 2, nil])) == nil
    end

    test "returns nil (unknown) when the value itself is NULL and the set is non-empty" do
      assert Sublink.member?(nil, [1, 2, 3]) == nil
      assert Sublink.member?(nil, MapSet.new([1, 2, 3])) == nil
    end

    test "returns false for any value against an empty set" do
      assert Sublink.member?(1, []) == false
      assert Sublink.member?(nil, []) == false
      assert Sublink.member?(1, MapSet.new([])) == false
      assert Sublink.member?(nil, MapSet.new([])) == false
    end

    test "a present value wins even when the set also carries a NULL" do
      assert Sublink.member?(2, [2, nil]) == true
      assert Sublink.member?(2, MapSet.new([2, nil])) == true
    end
  end
end
