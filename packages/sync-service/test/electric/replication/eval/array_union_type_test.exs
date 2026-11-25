defmodule Electric.Replication.Eval.ArrayUnionTypeTest do
  @moduledoc """
  An array literal's element type is the union of its elements' types, and a union does not
  depend on the order the elements were written in. Postgres accepts every clause here.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner

  defp eval(query) do
    query
    |> Parser.parse_and_validate_expression!()
    |> Runner.execute(%{})
  end

  describe "a mixed numeric array resolves the same either way round" do
    test "widest element first" do
      assert {:ok, true} == eval("1.5 = ANY (ARRAY[1.5, 2])")
    end

    test "narrowest element first" do
      assert {:ok, true} == eval("1.5 = ANY (ARRAY[2, 1.5])")
    end

    test "a non-member is false either way round" do
      assert {:ok, false} == eval("3.5 = ANY (ARRAY[1.5, 2])")
      assert {:ok, false} == eval("3.5 = ANY (ARRAY[2, 1.5])")
    end

    test "the same array as an IN list, which never depended on order" do
      assert {:ok, true} == eval("1.5 IN (1.5, 2)")
      assert {:ok, true} == eval("1.5 IN (2, 1.5)")
    end
  end

  describe "unions that were already order-independent stay so" do
    test "a uniform array" do
      assert {:ok, true} == eval("2 = ANY (ARRAY[1, 2])")
    end

    test "an array of one category with a NULL" do
      assert {:ok, nil} == eval("3 = ANY (ARRAY[1, NULL])")
    end

    test "elements from different categories are still rejected" do
      assert {:error, _} = Parser.parse_and_validate_expression("ARRAY[1, 'a']")
    end
  end
end
