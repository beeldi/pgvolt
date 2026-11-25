defmodule Electric.Shapes.ArrayElementEqualityTest do
  @moduledoc """
  An array comparison (`=`, `<>`, `@>`, `<@`, `&&`, `= ANY`) compares elements as their type does:
  a float's `-0` equals `0`, an `ARRAY[0]::float8[]` holds a float, and two spellings of one time or
  interval are one value. Each case is asked of Postgres, then of the evaluator, which decides
  whether a change belongs to the shape, and of the routing index, with its memo off and on.
  """
  use Support.TransactionCase, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Electric.Shapes.WhereClause
  alias Support.StubInspector

  @types %{
    "f8" => "float8",
    "f4" => "float4",
    "n" => "numeric",
    "t" => "time",
    "ts" => "timestamp",
    "tstz" => "timestamptz",
    "iv" => "interval"
  }

  @inspector StubInspector.new(
               tables: ["t1"],
               columns:
                 [
                   %{name: "id", type: "int8", pk_position: 0},
                   %{name: "sf8", type: "float8"},
                   %{name: "sn", type: "numeric"}
                 ] ++
                   Enum.map(@types, fn {name, type} -> %{name: name, array_type: type} end)
             )

  # {column, the row's value, where clause, what Postgres answers}
  @cases [
    {"f8", "{-0}", "f8 @> '{0}'", true},
    {"f8", "{0}", "f8 @> '{-0}'", true},
    {"f8", "{-0}", "f8 <@ '{0}'", true},
    {"f8", "{-0}", "'{0}' <@ f8", true},
    {"f8", "{-0}", "f8 && '{0}'", true},
    {"f8", "{-0}", "f8 && '{0,5}'", true},
    {"f8", "{-0}", "f8 = '{0}'", true},
    {"f8", "{-0}", "f8 <> '{0}'", false},
    {"f8", "{-0}", "0 = ANY(f8)", true},
    {"f8", "{-0}", "NOT (f8 @> '{0}')", false},
    {"f8", "{1}", "f8 @> '{-0}'", false},
    {"f4", "{-0}", "f4 @> '{0}'", true},
    {"f4", "{0}", "f4 @> '{-0}'", true},
    {"f4", "{-0}", "f4 = '{0}'", true},
    {"f4", "{-0}", "f4 && '{0}'", true},
    {"f8", "{0}", "f8 @> ARRAY[0]::float8[]", true},
    {"f8", "{0}", "f8 = ARRAY[0]::float8[]", true},
    {"f8", "{0}", "f8 && ARRAY[0]::float8[]", true},
    {"f8", "{0}", "f8 <@ ARRAY[0]::float8[]", true},
    {"f8", "{1.5}", "f8 @> ARRAY[1]::float8[]", false},
    {"f4", "{0}", "f4 @> ARRAY[0]::float4[]", true},
    {"f4", "{0}", "f4 = ARRAY[0]::float4[]", true},
    # A numeric too small for a float reads as -0.0, and Postgres does not call it zero.
    {"n", "{-0.#{String.duplicate("0", 399)}1}", "n @> '{0}'", false},
    {"n", "{-0.#{String.duplicate("0", 399)}1}", "n = '{-0.00}'", false},
    # Postgres has no negative numeric zero, so a `-0.00` constant is zero.
    {"n", "{0.00}", "n = '{-0.00}'", true},
    {"n", "{0.00}", "n <> '{-0.00}'", false},
    {"n", "{0.00}", "n @> '{-0.00}'", true},
    {"n", "{0.00}", "n = ARRAY[-0.00]", true},
    {"sn", "0.00", "sn = -0.00", true},
    # A numeric holds an integer exactly, where a float8 rounds it.
    {"n", "{9007199254740992,0}", "n = ARRAY[9007199254740993, 0.0]", false},
    {"n", "{9007199254740992,0}", "n <> ARRAY[9007199254740993, 0.0]", true},
    {"f8", "{9007199254740992}", "f8 = ARRAY[9007199254740993]::float8[]", true},
    {"t", "{12:00:00}", "t @> '{12:00}'", true},
    {"t", "{12:00:00}", "t @> '{12:00:00.000}'", true},
    {"t", "{12:00:00}", "t <@ '{12:00:00.000}'", true},
    {"t", "{12:00:00}", "t && '{12:00:00.000}'", true},
    {"t", "{12:00:00}", "t = '{12:00:00.000}'", true},
    {"t", "{12:00:00}", "t <> '{12:00:00.000}'", false},
    {"t", "{12:00:01}", "t @> '{12:00:00.000}'", false},
    {"ts", ~s|{"2024-01-01 12:00:00"}|, "ts @> '{2024-01-01 12:00:00.000}'", true},
    {"ts", ~s|{"2024-01-01 12:00:00"}|, "ts = '{2024-01-01 12:00:00.000}'", true},
    {"tstz", ~s|{"2024-01-01 12:00:00+00"}|, "tstz @> '{2024-01-01 12:00:00.000+00}'", true},
    {"tstz", ~s|{"2024-01-01 12:00:00+00"}|, "tstz @> '{2024-01-01 13:00:00+01}'", true},
    {"tstz", ~s|{"2024-01-01 12:00:00+00"}|, "tstz = '{2024-01-01 12:00:00.000+00}'", true},
    {"iv", ~s|{"1 day"}|, "iv @> '{24 hours}'", true},
    {"iv", ~s|{"1 day"}|, "iv @> ARRAY[interval '24 hours']", true},
    {"iv", ~s|{"1 day"}|, "iv = '{24 hours}'", true},
    {"iv", ~s|{"1 day"}|, "iv && '{24 hours}'", true},
    {"iv", ~s|{"1 day"}|, "iv <@ '{24 hours}'", true},
    {"iv", ~s|{"1 day"}|, "iv <> '{24 hours}'", false},
    {"iv", ~s|{"1 mon"}|, "iv @> '{30 days}'", true},
    {"iv", ~s|{"1 day"}|, "iv @> '{25 hours}'", false},
    # The equality and inclusion indexes key a float by the same function as the array elements.
    {"sf8", "-0", "sf8 = 0", true},
    {"sf8", "-0", "sf8 IN (0, 1)", true}
  ]

  for {column, value, where, expected} <- @cases do
    test "#{where} on #{String.slice(value, 0, 24)}", %{db_conn: conn} do
      column = unquote(column)
      value = unquote(value)
      where = unquote(where)
      expected = unquote(expected)

      assert postgres_answer(conn, column, value, where) == expected

      shape = Shape.new!("t1", where: where, inspector: @inspector)
      record = %{"id" => "1", column => value}

      assert {where, WhereClause.includes_record_result(shape.where, record)} ==
               {where, {:ok, expected}}

      for memo? <- [false, true] do
        filter = Filter.new(filter_routing_memo: memo?) |> Filter.add_shape("s", shape)
        change = %NewRecord{relation: {"public", "t1"}, record: record}
        routed? = filter |> Filter.affected_shapes(change) |> MapSet.member?("s")

        assert {where, memo?, routed?} == {where, memo?, expected}
      end
    end
  end

  defp postgres_answer(conn, column, value, where) do
    type =
      case column do
        "sf8" -> "float8"
        "sn" -> "numeric"
        _ -> Map.fetch!(@types, column) <> "[]"
      end

    %{rows: [[answer]]} =
      Postgrex.query!(
        conn,
        "SELECT (#{where}) IS TRUE FROM (SELECT ($1::text)::#{type} AS #{column}) r",
        [value]
      )

    answer
  end
end
