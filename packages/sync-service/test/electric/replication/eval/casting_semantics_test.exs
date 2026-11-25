defmodule Electric.Replication.Eval.CastingSemanticsTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.{Env, Parser, Runner}

  # Expectations read off PostgreSQL 18.6 (en_US.utf8, libc provider). Each case is a whole
  # expression rather than a bare function call, so a regression is caught in the shape the live
  # tail actually asks it in: parse, then run over a record. The snapshot half of every one of
  # these shapes is answered by Postgres itself, so a divergence here splits one shape into two
  # different row sets.

  @env Env.new()

  defp eval(sql, refs \\ %{}, record \\ %{}) do
    {:ok, tree} = Parser.parse_and_validate_expression(sql, refs: refs, env: @env)
    {:ok, value} = Runner.execute_for_record(tree, record)
    value
  end

  describe "bool cast to text renders the cast, not the wire form" do
    test "a boolean constant" do
      # `boolout` writes `t`/`f`, which is right for the wire and wrong for `::text`.
      assert eval("true::text") == "true"
      assert eval("false::text") == "false"
      assert eval("(1 = 1)::text = 'true'")
      refute eval("true::text = 't'")
    end

    test "a boolean column" do
      assert eval("x::text = 'true'", %{["x"] => :bool}, %{"x" => "t"})
      refute eval("x::text = 't'", %{["x"] => :bool}, %{"x" => "t"})
    end
  end

  describe "case folding follows the locale, one character at a time" do
    test "a character whose full-case mapping expands is left alone" do
      assert eval("upper('ß') = 'ß'")
      refute eval("upper('ß') = 'SS'")
      assert eval("upper('ﬁ') = 'ﬁ'")
      assert eval("upper('straße') = 'STRAßE'")
      refute eval("upper('straße') = 'STRASSE'")
    end

    test "one-to-one mappings still fold" do
      assert eval("upper('é') = 'É'")
      assert eval("lower('É') = 'é'")
      assert eval("upper('ǅ') = 'Ǆ'")
      assert eval("lower('ǅ') = 'ǆ'")
      assert eval("upper('abc') = 'ABC'")
    end

    test "folding a column value" do
      refute eval("upper(x) = 'STRASSE'", %{["x"] => :text}, %{"x" => "straße"})
      assert eval("upper(x) = x", %{["x"] => :text}, %{"x" => "ß"})
    end
  end

  describe "LIKE over a column, the way the live tail runs it" do
    test "a newline inside the value does not change the answer, and NOT LIKE withholds the row" do
      refs = %{["x"] => :text}
      assert eval("x LIKE '%secret%'", refs, %{"x" => "note\nsecret\nnote"})
      refute eval("x NOT LIKE '%secret%'", refs, %{"x" => "note\nsecret\nnote"})
      refute eval("x LIKE '%b'", refs, %{"x" => "ab\n"})
      assert eval("x NOT LIKE '%b'", refs, %{"x" => "ab\n"})
    end

    test "an escaped wildcard and a multi-byte character" do
      refs = %{["x"] => :text}
      assert eval("x LIKE 'a\\%b'", refs, %{"x" => "a%b"})
      refute eval("x LIKE 'a\\%b'", refs, %{"x" => "axb"})
      assert eval("x LIKE 'na_ve'", refs, %{"x" => "naïve"})
      assert eval("x ILIKE '%café%'", refs, %{"x" => "CAFÉ"})
    end
  end
end
