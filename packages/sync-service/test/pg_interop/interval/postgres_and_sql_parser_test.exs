defmodule PgInterop.Interval.PostgresAndSQLParserTest do
  use ExUnit.Case, async: true
  alias PgInterop.Interval
  alias PgInterop.Interval.PostgresAndSQLParser
  doctest PgInterop.Interval.PostgresAndSQLParser, import: true

  describe "ago" do
    test "negates the interval once" do
      assert {{:ok, minus_one_day}, {:ok, one_day}} =
               {PostgresAndSQLParser.parse("1 day ago"), PostgresAndSQLParser.parse("-1 day ago")}

      assert {minus_one_day, one_day} == {Interval.parse!("P-1D"), Interval.parse!("P1D")}
    end

    test "a second ago is invalid input, as Postgres reads it" do
      assert {:error, "invalid input syntax at `ago`"} =
               PostgresAndSQLParser.parse("1 day ago ago")
    end
  end
end
