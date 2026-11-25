defmodule Electric.Replication.Eval.NotEqualOperatorTest do
  @moduledoc """
  `<>` between two values of a type Electric orders through a compare function: dates, times,
  timestamps and intervals. `!=` is the same operator: the SQL parser reads it as `<>`.
  `IS [NOT] DISTINCT FROM` is built on `<>`. Ground truth, taken from Postgres 18:

      '2024-01-01'::date <> '2024-01-02'::date                                  -> true
      '2024-01-01'::date <> '2024-01-01'::date                                  -> false
      '2024-01-01 00:00:00+00'::timestamptz <> '2024-01-01 00:00:00+01'::timestamptz -> true
      '2024-01-01 00:00:00+00'::timestamptz <> '2024-01-01 01:00:00+01'::timestamptz -> false
      '1 day'::interval <> '25 hours'::interval                                 -> true
      '1 day'::interval <> '24 hours'::interval                                 -> false
      null::date <> '2024-01-01'::date                                          -> NULL
      '2024-01-01'::date <> null::date                                          -> NULL
      null::date <> null::date                                                  -> NULL
      '1 day'::interval is distinct from '24 hours'::interval                   -> false
      null::date is distinct from '2024-01-01'::date                            -> true
      null::date is distinct from null::date                                    -> false
      null::date is not distinct from '2024-01-01'::date                        -> false
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner

  # {type, the column's value as Postgres sends it, a different value, an equal value}
  @types [
    {:date, "2024-01-01", "2024-01-02", "2024-01-01"},
    {:time, "12:00:00", "13:00:00", "12:00:00"},
    {:timestamp, "2024-01-01 00:00:00", "2024-01-01 00:00:01", "2024-01-01 00:00:00"},
    {:timestamptz, "2024-01-01 00:00:00+00", "2024-01-01 00:00:00+01", "2024-01-01 01:00:00+01"},
    {:interval, "1 day", "25 hours", "24 hours"}
  ]

  defp eval(clause, type, value) do
    clause
    |> Parser.parse_and_validate_expression!(refs: %{["x"] => type})
    |> Runner.execute_for_record(%{"x" => value})
  end

  for {type, value, different, equal} <- @types do
    describe "#{type} <> #{type}" do
      test "is true for different values" do
        clause = ~s|"x" <> '#{unquote(different)}'::#{unquote(type)}|
        assert {:ok, true} == eval(clause, unquote(type), unquote(value))
      end

      test "is false for equal values" do
        clause = ~s|"x" <> '#{unquote(equal)}'::#{unquote(type)}|
        assert {:ok, false} == eval(clause, unquote(type), unquote(value))
      end

      test "is also spelled `!=`" do
        clause = ~s|"x" != '#{unquote(equal)}'::#{unquote(type)}|
        assert {:ok, false} == eval(clause, unquote(type), unquote(value))
      end

      test "is NULL when the column is NULL" do
        clause = ~s|"x" <> '#{unquote(equal)}'::#{unquote(type)}|
        assert {:ok, nil} == eval(clause, unquote(type), nil)
      end

      test "is NULL when the column on the right is NULL" do
        clause = ~s|'#{unquote(equal)}'::#{unquote(type)} <> "x"|
        assert {:ok, nil} == eval(clause, unquote(type), nil)
      end

      test "is NULL between two NULLs" do
        assert {:ok, nil} == eval(~s|"x" <> "x"|, unquote(type), nil)
      end
    end

    describe "#{type} IS [NOT] DISTINCT FROM #{type}" do
      test "IS DISTINCT FROM compares values and treats NULL as a value" do
        distinct = ~s|"x" IS DISTINCT FROM '#{unquote(different)}'::#{unquote(type)}|
        same = ~s|"x" IS DISTINCT FROM '#{unquote(equal)}'::#{unquote(type)}|

        assert {:ok, true} == eval(distinct, unquote(type), unquote(value))
        assert {:ok, false} == eval(same, unquote(type), unquote(value))
        assert {:ok, true} == eval(same, unquote(type), nil)
        assert {:ok, false} == eval(~s|"x" IS DISTINCT FROM "x"|, unquote(type), nil)
      end

      test "IS NOT DISTINCT FROM is its negation" do
        same = ~s|"x" IS NOT DISTINCT FROM '#{unquote(equal)}'::#{unquote(type)}|

        assert {:ok, true} == eval(same, unquote(type), unquote(value))
        assert {:ok, false} == eval(same, unquote(type), nil)
        assert {:ok, true} == eval(~s|"x" IS NOT DISTINCT FROM "x"|, unquote(type), nil)
      end
    end
  end
end
