defmodule Electric.Postgres.IdentifiersOracleTest do
  @moduledoc """
  Differential test of `Identifiers.parse_relation/1` against real Postgres.

  A relation identifier that parses to something other than what Postgres would parse it to names a
  table that does not exist, so the shape either fails to create or never sees a change. The rules
  are not obvious — Postgres downcases only ASCII under a multibyte encoding, treats every byte
  above 127 as an identifier character with no Unicode classification at all, and strips trailing
  whitespace — so each one is asked of `parse_ident/1` rather than written down here.

  Electric is deliberately stricter than Postgres in two places, and those are pinned as their own
  table so that widening either is a decision rather than a drift.
  """
  use ExUnit.Case, async: false

  import Support.DbSetup

  alias Electric.Postgres.Identifiers

  setup :with_shared_db

  @agree [
    "foo",
    "FooBar",
    ~S|"FooBar"|,
    "foo.bar",
    ~S|"foo"."bar"|,
    ~S|"foo.woah"."bar"|,
    ~S|"foo".bar|,
    ~S|foo."bar"|,
    "foo_bar",
    "_foo",
    "foo$bar",
    "foo0",
    ~S|"a""b"|,
    ~S|" "|,
    ~S|"foo bar"|,
    "é",
    "café",
    ~S|"é"|,
    # Postgres lowercases only A-Z under a multibyte encoding, so the accent survives.
    "ÉCOLE",
    "ǅ",
    "İ",
    "ß"
  ]

  @both_reject [
    "0foo",
    "",
    ~S|""|,
    "foo bar",
    "foo-bar",
    "foo@bar",
    ~S|"Foo"Bar"|,
    ~S|"foo|,
    ~S|foo"|,
    ".",
    "a.",
    ".a"
  ]

  # Postgres accepts these; Electric refuses them, and refusing is the safe direction because the
  # parsed name reaches a catalog lookup and a quoted SQL fragment.
  @electric_stricter [
    # Postgres' identifier grammar is `[A-Za-z\200-\377_]` — every byte above 127, whatever it is.
    # Electric requires a Unicode letter, so a symbol is refused.
    {"®", ["®"]},
    {"foo®", ["foo®"]},
    # Postgres strips whitespace around an unquoted identifier and Electric refuses it. The trailing
    # newline is the case worth pinning: `$` also matches before a final newline, so the validator
    # used to accept that one form and no other whitespace at all.
    {" foo", ["foo"]},
    {"foo ", ["foo"]},
    {"foo\t", ["foo"]},
    {"foo\n", ["foo"]},
    # `parse_ident/1` parses any number of parts. A relation has at most two.
    {"a.b.c", ["a", "b", "c"]}
  ]

  for input <- @agree do
    test "agrees with Postgres on #{inspect(input)}", %{pool: pool} do
      # Act
      {:ok, {schema, table}} = Identifiers.parse_relation(unquote(input))

      # Assert
      assert parse_ident(pool, unquote(input)) == expected_parts(schema, table)
    end
  end

  for input <- @both_reject do
    test "is refused by both on #{inspect(input)}", %{pool: pool} do
      # Act
      electric = Identifiers.parse_relation(unquote(input))

      # Assert
      assert {:error, _} = electric
      assert parse_ident(pool, unquote(input)) == :error
    end
  end

  for {input, parts} <- @electric_stricter do
    test "is accepted by Postgres and refused by Electric: #{inspect(input)}", %{pool: pool} do
      # Act
      electric = Identifiers.parse_relation(unquote(input))

      # Assert
      assert {:error, _} = electric
      assert parse_ident(pool, unquote(input)) == unquote(parts)
    end
  end

  defp expected_parts("public", table), do: [table]
  defp expected_parts(schema, table), do: [schema, table]

  defp parse_ident(pool, input) do
    case Postgrex.transaction(pool, fn conn ->
           Postgrex.query(conn, "SELECT parse_ident($1)", [input])
         end) do
      {:ok, {:ok, %Postgrex.Result{rows: [[parts]]}}} -> parts
      _ -> :error
    end
  end
end
