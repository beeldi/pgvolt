defmodule Electric.Shapes.ArrayLowerBoundsTest do
  @moduledoc """
  A row's array whose lower bounds are not all 1, which Postgres writes as `[0:1]={1,2}`, reads as
  Postgres reads it: `=` and `<>` compare its bounds, subscripts and slices count from them, and
  `@>`, `<@`, `&&` and `ANY` ignore them. Each case checks that Postgres writes the value that way,
  then asks the where clause of Postgres, of the evaluator, which decides whether a change belongs
  to the shape, and of the routing index, with its memo off and on.
  """
  use Support.TransactionCase, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Electric.Shapes.WhereClause
  alias Support.StubInspector

  @types %{"a" => "int4", "b" => "int8", "t" => "text", "d" => "date", "i" => "interval"}

  @inspector StubInspector.new(
               tables: ["t1"],
               columns:
                 [%{name: "id", type: "int8", pk_position: 0}] ++
                   Enum.map(@types, fn {name, type} -> %{name: name, array_type: type} end)
             )

  # {column, the row's value as Postgres writes it, where clause, what Postgres answers}
  @cases [
    {"a", "[0:1]={1,2}", "a = '{1,2}'", false},
    {"a", "[0:1]={1,2}", "a <> '{1,2}'", true},
    {"a", "[0:1]={1,2}", "a IN ('{1,2}', '{3}')", false},
    {"a", "[0:1]={1,2}", "NOT (a = '{1,2}')", true},
    {"a", "[0:1]={1,2}", "a @> '{1}'", true},
    {"a", "[0:1]={1,2}", "a <@ '{1,2,3}'", true},
    {"a", "[0:1]={1,2}", "a && '{2,5}'", true},
    {"a", "[0:1]={1,2}", "a @> '{3}'", false},
    {"a", "[0:1]={1,2}", "1 = ANY(a)", true},
    {"a", "[0:1]={1,2}", "3 = ANY(a)", false},
    {"a", "[0:1]={1,2}", "1 = ALL(a)", false},
    {"a", "[0:1]={1,2}", "a IS NOT NULL", true},
    {"a", "[0:1]={1,2}", "a[0] = 1", true},
    {"a", "[0:1]={1,2}", "a[1] = 2", true},
    {"a", "[0:1]={1,2}", "a[2] IS NULL", true},
    {"a", "[0:1]={1,2}", "array_ndims(a) = 1", true},
    {"a", "[0:3]={1,2,3,4}", "a[1:2] = '{2,3}'", true},
    {"a", "[0:3]={1,2,3,4}", "a[-5:1] = '{1,2}'", true},
    {"a", "[0:3]={1,2,3,4}", "a[3:9] = '{4}'", true},
    {"a", "[-2:-1]={1,2}", "a[-2] = 1", true},
    {"a", "[0:1]={1,2}", "a || 3 = '{1,2,3}'", false},
    {"a", "[0:1]={1,2}", "(a || 3)[2] = 3", true},
    {"a", "[0:1]={1,2}", "(0 || a)[0] = 0", true},
    {"a", "[0:1]={1,2}", "(a || '{3}'::int4[])[2] = 3", true},
    {"a", "[0:1]={1,2}", "'{3}'::int4[] || a = '{3,1,2}'", true},
    {"a", "[0:1]={1,2}", "a || '{}'::int4[] <> '{1,2}'", true},
    {"a", "[0:1]={1,2}", "a::int8[] = '{1,2}'::int8[]", false},
    {"a", "[0:1]={1,2}", "a::int8[] @> '{1}'::int8[]", true},
    {"a", "[0:1]={1,2}", "(a::int8[])[0] = 1", true},
    {"a", "[0:1][1:2]={{1,2},{3,4}}", "a[0][2] = 2", true},
    {"a", "[0:1][1:2]={{1,2},{3,4}}", "a[0:1][1] = '{{1},{3}}'", true},
    {"a", "[0:1][1:2]={{1,2},{3,4}}", "a @> '{4}'", true},
    {"a", "[0:1][1:2]={{1,2},{3,4}}", "a = '{{1,2},{3,4}}'", false},
    {"a", "[1:2][0:0]={{1},{2}}", "a[2][0] = 2", true},
    {"a", "[1:2][0:0]={{1},{2}}", "a = '{{1},{2}}'", false},
    {"b", "[5:6]={10,20}", "b = '{10,20}'", false},
    {"b", "[5:6]={10,20}", "b[6] = 20", true},
    {"b", "[5:6]={10,20}", "b @> '{20}'", true},
    {"t", "[2:3]={x,y}", "t = '{x,y}'", false},
    {"t", "[2:3]={x,y}", "t <> '{x,y}'", true},
    {"t", "[2:3]={x,y}", "t[2] = 'x'", true},
    {"t", "[2:3]={x,y}", "t @> '{y}'", true},
    {"t", "[2:3]={x,y}", "'y' = ANY(t)", true},
    {"a", "[0:1]={1,2}", "NOT (ARRAY[a] @> '{1}')", false},
    {"a", "[0:1]={1,2}", "(ARRAY[a])[1][0] = 1", true},
    {"a", "[0:1]={1,2}", "ARRAY[a] = '{{1,2}}'", false},
    {"a", "[0:1]={1,2}", "(ARRAY[a, a])[2][1] = 2", true},
    {"a", "[0:1][1:1]={{1},{2}}", "(ARRAY[a])[1][1][1] = 2", true},
    {"a", "[0:1]={9,10}", "greatest(a, ARRAY[1]) = ARRAY[1]", false},
    {"a", "[0:1]={9,10}", "least(a, ARRAY[1]) = ARRAY[1]", true},
    {"a", "[0:1]={1,2}", "least(a, ARRAY[1,2]) = ARRAY[1,2]", false},
    {"a", "[0:1]={1,2}", "greatest(a, ARRAY[1,2]) = ARRAY[1,2]", true},
    {"a", "[0:1][1:1]={{1},{2}}", "greatest(a, '{{1,2}}'::int4[]) = '{{1,2}}'", false},
    {"a", "[0:1]={1,2}", "greatest(a, '{}'::int4[]) = a", true},
    {"a", "[0:1]={1,5}", "greatest(a, '{1,NULL}'::int4[]) = '{1,NULL}'", true},
    {"t", "[0:1]={x,y}", ~s|greatest(t COLLATE "C", '{NULL,y}'::text[]) = '{NULL,y}'|, true},
    {"t", "[0:1][1:1]={{x},{y}}", ~s|least(t COLLATE "C", '{{NULL},{x}}'::text[]) = t|, true},
    {"d", "[0:0]={2024-01-31}", "greatest(d, '{2024-02-01}'::date[]) = '{2024-02-01}'", true},
    {"i", ~s|[0:0]={"31 days"}|, "greatest(i, '{2 mons}'::interval[]) = '{2 mons}'", true}
  ]

  for {column, value, where, expected} <- @cases do
    test "#{where} on #{value}", %{db_conn: conn} do
      column = unquote(column)
      value = unquote(value)
      where = unquote(where)
      expected = unquote(expected)

      assert postgres_text(conn, column, value) == value
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

  test "a where clause constant with lower bounds is refused" do
    for where <- ["a = '[0:1]={1,2}'", "a = '[0:1]={1,2}'::int4[]", "a @> '[2:2]={1}'"] do
      assert {:error, message} = Shape.new("t1", where: where, inspector: @inspector)

      assert inspect(message) =~
               "array constants with lower bounds other than 1 are not supported"
    end
  end

  test "a where clause constant whose lower bounds are all 1 is the plain array" do
    shape = Shape.new!("t1", where: "a = '[1:2]={1,2}'", inspector: @inspector)
    assert WhereClause.includes_record_result(shape.where, %{"a" => "{1,2}"}) == {:ok, true}
  end

  test "an array of arrays Postgres refuses to build is unevaluable",
       %{db_conn: conn} do
    wheres = [
      "ARRAY[a, '{1,2}'::int4[]] @> '{1}'",
      "ARRAY[a, '{}'::int4[]] @> '{1}'",
      "ARRAY[a, '{}'::int4[]] IS NOT NULL",
      "ARRAY[a, NULL] IS NULL",
      "COALESCE(ARRAY[a, '{}'::int4[]], a) IS NOT NULL"
    ]

    for where <- wheres do
      shape = Shape.new!("t1", where: where, inspector: @inspector)
      record = %{"id" => "1", "a" => "[0:1]={1,2}"}
      assert {where, WhereClause.includes_record_result(shape.where, record)} == {where, :error}

      # The error aborts the test's transaction, so each clause runs in a savepoint.
      Postgrex.query!(conn, "SAVEPOINT refused", [])

      assert_raise Postgrex.Error, ~r/matching dimensions/, fn ->
        postgres_answer(conn, "a", "[0:1]={1,2}", where)
      end

      Postgrex.query!(conn, "ROLLBACK TO SAVEPOINT refused", [])
    end
  end

  defp postgres_text(conn, column, value) do
    %{rows: [[text]]} =
      Postgrex.query!(conn, "SELECT (($1::text)::#{type(column)})::text", [value])

    text
  end

  defp postgres_answer(conn, column, value, where) do
    %{rows: [[answer]]} =
      Postgrex.query!(
        conn,
        "SELECT (#{where}) IS TRUE FROM (SELECT ($1::text)::#{type(column)} AS #{column}) r",
        [value]
      )

    answer
  end

  defp type(column), do: Map.fetch!(@types, column) <> "[]"
end
