defmodule Electric.Replication.Eval.ArraySubscriptTest do
  @moduledoc """
  Postgres arrays are N-dimensional rather than nested, so a subscript expression must supply
  one subscript per dimension. Supplying fewer is NULL, not the inner array -- which is the one
  place where modelling an array as nested lists, as this does, diverges. A slice is different:
  it may supply fewer and returns a sub-array of the same dimensionality.

  Every expected value here was read off Postgres 16.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner

  defp eval(query) do
    query
    |> Parser.parse_and_validate_expression!()
    |> Runner.execute(%{})
  end

  describe "subscripting a one-dimensional array" do
    test "an in-range subscript is the element" do
      assert {:ok, "a"} == eval("(ARRAY['a','b'])[1]")
    end

    test "a subscript past the end is unknown" do
      assert {:ok, nil} == eval("(ARRAY['a','b'])[5]")
    end

    test "a subscript before the start is unknown" do
      assert {:ok, nil} == eval("(ARRAY['a','b'])[0]")
    end
  end

  describe "subscripting a multidimensional array" do
    test "one subscript short of the dimensions is unknown, not the inner array" do
      assert {:ok, nil} == eval("(ARRAY[ARRAY[1,2],ARRAY[3,4]])[1]")
    end

    test "one subscript per dimension is the element" do
      assert {:ok, 2} == eval("(ARRAY[ARRAY[1,2],ARRAY[3,4]])[1][2]")
    end

    test "two short of three dimensions is still unknown" do
      assert {:ok, nil} == eval("(ARRAY[ARRAY[ARRAY[1,2]]])[1]")
    end
  end

  describe "slicing, where supplying fewer is allowed" do
    test "a slice of a one-dimensional array" do
      assert {:ok, ["b", "c"]} == eval("(ARRAY['a','b','c'])[2:3]")
    end

    test "a slice clipped by the end of the array" do
      assert {:ok, ["b"]} == eval("(ARRAY['a','b'])[2:5]")
    end

    test "a slice entirely past the end is empty rather than unknown" do
      assert {:ok, []} == eval("(ARRAY['a','b'])[5:6]")
    end

    test "a slice of a multidimensional array keeps its dimensionality" do
      assert {:ok, [[1, 2]]} == eval("(ARRAY[ARRAY[1,2],ARRAY[3,4]])[1:1]")
    end
  end
end
