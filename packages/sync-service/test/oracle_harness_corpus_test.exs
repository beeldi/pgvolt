defmodule Support.OracleHarness.CorpusCoverageTest do
  use ExUnit.Case, async: true

  alias Support.OracleHarness.StandardSchema
  alias Support.OracleHarness.WhereClauseGenerator

  # The differential oracle can only produce a counter-example to the LIKE, collation and
  # case-folding classes when its text universe carries the characters those classes turn on. These
  # guard that the corpus is no longer confined to lowercase ASCII `v0..v19`.
  @uppercase ~r/[A-Z]/
  @non_ascii ~r/[^\x00-\x7F]/u

  describe "stored value universe" do
    test "carries uppercase, newline, non-ASCII and backslash text" do
      values = StandardSchema.adversarial_values()

      assert Enum.any?(values, &(&1 =~ @uppercase)), "no uppercase value in #{inspect(values)}"
      assert Enum.any?(values, &String.contains?(&1, "\n")), "no newline value"
      assert Enum.any?(values, &(&1 =~ @non_ascii)), "no non-ASCII value"
      assert Enum.any?(values, &String.contains?(&1, "\\")), "no backslash value"
    end
  end

  describe "where-clause corpus" do
    test "value literals carry uppercase and non-ASCII text" do
      literals = WhereClauseGenerator.adversarial_value_literals()

      assert Enum.any?(literals, &(&1 =~ @uppercase))
      assert Enum.any?(literals, &(&1 =~ @non_ascii))
    end

    test "LIKE patterns carry uppercase, newline and escaped metacharacters" do
      patterns = WhereClauseGenerator.adversarial_like_patterns()

      assert Enum.any?(patterns, &(&1 =~ @uppercase))
      assert Enum.any?(patterns, &String.contains?(&1, "\n"))
      assert Enum.any?(patterns, &String.contains?(&1, "\\"))
    end
  end
end
