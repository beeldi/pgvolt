defmodule Electric.Replication.Eval.InNullSemanticsTest do
  @moduledoc """
  `IN` over a list containing NULL is three-valued in Postgres, and a where clause is what
  decides whether a row reaches a client. Ground truth, taken from Postgres:

      'b' in ('a', null)          -> NULL
      'a' in ('a', null)          -> true
      'b' = any(array['a',null])  -> NULL
      'a' = any(array['a',null])  -> true
      null in ('a','b')           -> NULL
      'b' not in ('a', null)      -> NULL
      'a' not in ('a', null)      -> false
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner

  @refs %{["x"] => :text}

  defp eval(query, x) do
    query
    |> Parser.parse_and_validate_expression!(refs: @refs)
    |> Runner.execute(%{["x"] => x})
  end

  describe "IN over a list containing NULL" do
    test "a non-matching value is unknown, not false" do
      assert {:ok, nil} == eval(~s|"x" IN ('a', NULL)|, "b")
    end

    test "a matching value is true even though the list has a NULL" do
      assert {:ok, true} == eval(~s|"x" IN ('a', NULL)|, "a")
    end

    test "a NULL value is unknown against a list with no NULL" do
      assert {:ok, nil} == eval(~s|"x" IN ('a', 'b')|, nil)
    end
  end

  describe "= ANY over an array containing NULL" do
    test "a non-matching value is unknown, not false" do
      assert {:ok, nil} == eval(~s|"x" = ANY(ARRAY['a', NULL])|, "b")
    end

    test "a matching value is true even though the array has a NULL" do
      assert {:ok, true} == eval(~s|"x" = ANY(ARRAY['a', NULL])|, "a")
    end
  end

  describe "= ALL over an array containing NULL" do
    test "a value matching every non-NULL element is unknown, not true" do
      assert {:ok, nil} == eval(~s|"x" = ALL(ARRAY['a', NULL])|, "a")
    end

    test "a value differing from a non-NULL element is false" do
      assert {:ok, false} == eval(~s|"x" = ALL(ARRAY['a', NULL])|, "b")
    end

    test "a value matching every element is true" do
      assert {:ok, true} == eval(~s|"x" = ALL(ARRAY['a', 'a'])|, "a")
    end
  end

  describe "empty arrays" do
    test "ANY of nothing is false" do
      assert {:ok, false} == eval(~s|"x" = ANY(ARRAY[]::text[])|, "b")
    end

    test "ALL of nothing is true" do
      assert {:ok, true} == eval(~s|"x" = ALL(ARRAY[]::text[])|, "b")
    end
  end

  describe "multidimensional arrays" do
    # Postgres asks the question of every element regardless of dimension.
    @nested_refs %{["n"] => :int4}

    defp eval_nested(query, n) do
      query
      |> Parser.parse_and_validate_expression!(refs: @nested_refs)
      |> Runner.execute(%{["n"] => n})
    end

    test "ALL over a nested array compares every element" do
      assert {:ok, true} == eval_nested(~s|"n" > ALL('{{1},{2},{3}}'::int[])|, 4)
    end

    test "ANY over a nested array holding a NULL is unknown when nothing matches" do
      assert {:ok, nil} == eval_nested(~s|"n" > ANY('{{1},{NULL},{3}}'::int[])|, 0)
    end

    test "ALL over a nested array holding a NULL is unknown when everything else matches" do
      assert {:ok, nil} == eval_nested(~s|"n" > ALL('{{1},{NULL},{3}}'::int[])|, 4)
    end
  end

  describe "folded at parse time" do
    # A clause with no refs is evaluated by the parser rather than the runner. That is a second
    # implementation of the same application, and it was wrong in the same way.
    defp fold(query) do
      query
      |> Parser.parse_and_validate_expression!()
      |> Runner.execute(%{})
    end

    test "a non-matching constant ANY over a NULL-bearing array folds to unknown" do
      assert {:ok, nil} == fold(~s|'b' = ANY(ARRAY['a', NULL])|)
    end

    test "a matching constant ANY over a NULL-bearing array folds to true" do
      assert {:ok, true} == fold(~s|'a' = ANY(ARRAY['a', NULL])|)
    end

    test "a constant ALL over a NULL-bearing array folds to unknown" do
      assert {:ok, nil} == fold(~s|'a' = ALL(ARRAY['a', NULL])|)
    end
  end

  describe "the negation that makes it reachable" do
    # This is where the defect surfaced as row visibility rather than an odd boolean: with
    # `= ANY` answering false instead of unknown, `NOT (...)` answers true, and a row
    # Postgres would have excluded reaches the client.
    test "NOT of a non-matching ANY over a NULL-bearing array stays unknown" do
      assert {:ok, nil} == eval(~s|NOT ("x" = ANY(ARRAY['a', NULL]))|, "b")
    end
  end

  describe "NOT IN over a list containing NULL" do
    test "a non-matching value is unknown, not true" do
      assert {:ok, nil} == eval(~s|"x" NOT IN ('a', NULL)|, "b")
    end

    test "a matching value is false" do
      assert {:ok, false} == eval(~s|"x" NOT IN ('a', NULL)|, "a")
    end
  end
end
