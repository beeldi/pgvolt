defmodule Electric.Replication.Eval.CollateClauseTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner

  defp matches?(query, record) do
    query
    |> Parser.parse_and_validate_expression!(refs: %{["x"] => :text})
    |> Runner.execute_for_record(record)
  end

  # Measured on postgres:17 with datcollate=en_US.utf8:
  #   SELECT 'B' >= 'a'                  -> t   (collated)
  #   SELECT ('B' COLLATE "C") >= 'a'    -> f   (byte order)
  # This evaluator only ever produces the byte-order answer, so a shape that means byte order needs
  # to be able to say so -- otherwise Postgres builds the snapshot with one ordering and the live
  # tail is filtered with another.
  describe "COLLATE \"C\"" do
    test "parses and evaluates as byte order, matching Postgres under COLLATE \"C\"" do
      assert {:ok, false} = matches?(~s|"x" COLLATE "C" >= 'a'|, %{"x" => "B"})
      assert {:ok, true} = matches?(~s|"x" COLLATE "C" < 'a'|, %{"x" => "B"})
    end

    test "POSIX is accepted as the same ordering" do
      assert {:ok, false} = matches?(~s|"x" COLLATE "POSIX" >= 'a'|, %{"x" => "B"})
    end

    test "works in the range form a partitioned shape uses" do
      assert {:ok, true} =
               matches?(~s|"x" COLLATE "C" >= 'A' AND "x" COLLATE "C" < 'c'|, %{"x" => "B"})

      assert {:ok, false} =
               matches?(~s|"x" COLLATE "C" >= 'a' AND "x" COLLATE "C" < 'c'|, %{"x" => "B"})
    end
  end

  describe "any other collation" do
    test "is refused rather than silently evaluated as byte order" do
      assert {:error, message} =
               Parser.parse_and_validate_expression(~s|"x" COLLATE "en_US" >= 'a'|,
                 refs: %{["x"] => :text}
               )

      assert message =~ "COLLATE"
      assert message =~ "byte-wise"
    end

    test "a collation named C outside pg_catalog is refused" do
      assert {:error, message} =
               Parser.parse_and_validate_expression(~s|"x" COLLATE public."C" >= 'a'|,
                 refs: %{["x"] => :text}
               )

      assert message =~ ~s|COLLATE "public.C" is not supported|
    end

    test "pg_catalog's own C collation is accepted" do
      assert {:ok, false} = matches?(~s|"x" COLLATE pg_catalog."C" >= 'a'|, %{"x" => "B"})
    end

    test "a schema-qualified non-C collation is refused too" do
      assert {:error, _} =
               Parser.parse_and_validate_expression(~s|"x" COLLATE pg_catalog."default" >= 'a'|,
                 refs: %{["x"] => :text}
               )
    end
  end
end
