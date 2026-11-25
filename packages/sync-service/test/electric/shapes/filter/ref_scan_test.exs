defmodule Electric.Shapes.Filter.RefScanTest do
  @moduledoc """
  `RefScan` answers in the parser's place, so a `:known` it gives has to be one the parser would
  give too. A clause stripped on a wrong `:known` cannot be rebuilt when its shape is removed, and
  that removal crashes the collector.

  The check is the same everywhere: take the columns the parser reads in a text, drop one of them
  from `used_refs`, and the scan must not answer `:known`.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Shapes.Filter.RefScan

  @long_name String.duplicate("x", 64)

  describe "scan/2" do
    test "settles the plain clauses" do
      for text <- [
            ~s|"firmId" = 'F0' AND "divisionId" IN ('b1', 'b2') AND "deletedAt" IS NULL AND 'cache-buster:000083' = 'cache-buster:000083'|,
            ~s|"firmId" = 'F0' AND id IN ('a', 'b') AND "deletedAt" IS NOT NULL|,
            ~s|"firmId" = 'F0' AND id < 'cmm9bg4'|,
            ~s|ID = 'a' OR NOT (Id <> 'it''s')|,
            ~s|"firmId"::text = 'F0' AND "rank" >= 12|,
            ~s|"tags" @> ARRAY['a', 'b']::text[] AND "active" IS TRUE|,
            ~s|"firmId" = '-- no comment' AND id = '/* nor this */' AND "name" = '"other"'|
          ] do
        assert scan_against_parser(text) == :known, text
      end
    end

    test "never passes over a column that used_refs lacks" do
      for text <- [
            # names Postgres does not keep as they are written
            ~s|ID = 'a' AND "ID" = 'b'|,
            ~s|U&"d\\0061t\\+000061" = 'a' AND u = 'b'|,
            ~s|U&"d\\0061t" = 'a' AND "d\\0061t" = 'b' AND u = 'c'|,
            ~s|"a""b" = 'x' AND a = 'y' AND b = 'z'|,
            ~s|"#{@long_name}" = 'a'|,
            ~s|#{@long_name} = 'a'|,
            ~s|idé = 'a' AND id = 'b'|,
            ~s|id$x = 'a' AND id = 'b'|,
            ~s|employees.id = 'a' AND employees = 'b' AND id = 'c'|,
            ~s|"employees"."id" = 'a'|,
            # strings that hide or show a name
            ~s|e = 'a' AND id = E'a\\'' AND other = 'b'|,
            ~s|id = 'a\\' AND other = 'b'|,
            ~s|u = 'a' AND id = U&'\\0061' AND other = 'b'|,
            ~s|id = $$a$$ AND other = $x$'$x$|,
            ~s|b = 'a' AND id::text = B'01'::text AND x = X'1F'::text|,
            ~s|id = 'a'\n'b' AND other = 'c'|,
            # comments
            ~s|id = 'a' /* ' */ AND other = ' id ' /* ' */|,
            ~s|id = 'a' -- '\nAND other = ' id ' -- '|,
            # words that are a column in one place and a keyword in another
            ~s|between = 'a' AND id BETWEEN 'a' AND 'b'|,
            ~s|value = 'a' AND name = 'b' AND text = 'c' AND date = 'd'|,
            ~s|id::text = 'a' AND text = 'b'|,
            ~s|id::double precision = 1 AND precision = 2|,
            ~s|lower(id) = 'a' AND lower = 'b'|,
            ~s|id LIKE 'a%' AND "like" = 'b'|,
            ~s|id IS DISTINCT FROM other|,
            ~s|coalesce(id, other) = 'a'|,
            ~s|id = ANY(ARRAY[other, 'a'])|,
            ~s|CASE WHEN id = 'a' THEN other ELSE third END = 'b'|,
            ~s|(id, other) = ('a', 'b')|,
            ~s|id = 1.5 AND other = 1e5 AND third = 0x1F|,
            ~s|id=other|,
            ~s|"id"="other"|,
            ~s|NOT(id)AND(other)|
          ] do
        assert scan_against_parser(text) in [:known, :unknown], text
      end
    end

    test "leaves what it does not recognise to the parser" do
      refs = %{["id"] => :text, ["u"] => :text, ["e"] => :text}

      for text <- [
            ~s|id = 'a' -- a comment|,
            ~s|id = 'a' /* a comment */|,
            ~s|id = E'a'|,
            ~s|id = U&'a'|,
            ~s|U&"id" = 'a'|,
            ~s|id = $$a$$|,
            ~s|id = $1|,
            ~s|id = 'a\\'|,
            ~s|"i""d" = 'a'|,
            ~s|employees.id = 'a'|,
            ~s|id = 1.5|,
            ~s|lower(id) = 'a'|,
            ~s|id LIKE 'a%'|,
            ~s|id = 'a'; id = 'b'|,
            ~s|id = 'unterminated|,
            ~s|"id = 'a'|
          ] do
        assert RefScan.scan(text, refs) == :unknown, text
      end

      # Postgres keeps 63 bytes of a name, so this one is not the column the text names
      assert RefScan.scan(~s|"#{@long_name}" = 'a'|, %{[@long_name] => :text}) == :unknown
      assert RefScan.scan(~s|#{@long_name} = 'a'|, %{[@long_name] => :text}) == :unknown
    end

    test "agrees with the parser over generated clauses" do
      :rand.seed(:exsss, {7, 13, 42})

      answers =
        for _ <- 1..4000 do
          scan_against_parser(clause(3))
        end

      # the generator has to reach both sides for the agreement to mean anything
      assert %{known: known, unknown: unknown} = Enum.frequencies(answers)
      assert known > 200
      assert unknown > 200
    end
  end

  # The scan's answer for a text whose every column is in `used_refs`, once it has been checked
  # against the parser for each column taken out of `used_refs` in turn.
  defp scan_against_parser(text) do
    case named_columns(text) do
      :error ->
        :unparsed

      columns ->
        refs = Map.new(columns, &{&1, :text})

        for column <- columns do
          assert RefScan.scan(text, Map.delete(refs, column)) == :unknown,
                 "#{inspect(text)} names #{inspect(column)}, which the scan passed over"
        end

        RefScan.scan(text, refs)
    end
  end

  defp named_columns(text) do
    case Parser.parse_query(text) do
      {:ok, ast} -> ast |> column_refs([]) |> Enum.uniq()
      _ -> :error
    end
  end

  defp column_refs(%PgQuery.ColumnRef{fields: fields}, acc),
    do: [Enum.map(fields, &field_name/1) | acc]

  defp column_refs(node, acc) when is_struct(node),
    do: node |> Map.from_struct() |> column_refs(acc)

  defp column_refs(node, acc) when is_map(node),
    do: Enum.reduce(node, acc, fn {_key, value}, acc -> column_refs(value, acc) end)

  defp column_refs(node, acc) when is_list(node), do: Enum.reduce(node, acc, &column_refs/2)

  defp column_refs(node, acc) when is_tuple(node),
    do: node |> Tuple.to_list() |> column_refs(acc)

  defp column_refs(_leaf, acc), do: acc

  defp field_name(%PgQuery.Node{node: {:string, %PgQuery.String{sval: name}}}), do: name
  defp field_name(%PgQuery.Node{node: {:a_star, _}}), do: "*"

  # A small grammar of where clauses, heavy on the forms a reader of the text can get wrong.
  defp clause(0), do: comparison()

  defp clause(depth) do
    case :rand.uniform(6) do
      1 -> "#{clause(depth - 1)} AND #{clause(depth - 1)}"
      2 -> "#{clause(depth - 1)} OR #{clause(depth - 1)}"
      3 -> "NOT (#{clause(depth - 1)})"
      4 -> "(#{clause(depth - 1)})#{gap()}"
      _ -> comparison()
    end
  end

  defp comparison do
    case :rand.uniform(9) do
      1 -> "#{column()} IN (#{literal()}, #{literal()})"
      2 -> "#{column()} IS NULL"
      3 -> "#{column()} IS NOT NULL"
      4 -> "#{column()}::#{type()} = #{literal()}"
      5 -> "#{literal()} = #{column()}"
      6 -> "#{column()}#{operator()}#{column()}"
      7 -> "#{function()}(#{column()}) = #{literal()}"
      _ -> "#{column()} #{operator()} #{literal()}"
    end
  end

  defp column do
    pick([
      ~s|id|,
      ~s|ID|,
      ~s|"id"|,
      ~s|"firmId"|,
      ~s|firmId|,
      ~s|"deletedAt"|,
      ~s|_x1|,
      ~s|"a b"|,
      ~s|"a""b"|,
      ~s|"it's"|,
      ~s|"-- name"|,
      ~s|employees.id|,
      ~s|"employees"."firmId"|,
      ~s|U&"\\0069d"|,
      ~s|u|,
      ~s|e|,
      ~s|b|,
      ~s|n|,
      ~s|x|,
      ~s|idé|,
      ~s|id$1|,
      ~s|#{@long_name}|,
      ~s|"#{@long_name}"|,
      # keywords, wherever Postgres takes one for a column
      ~s|between|,
      ~s|value|,
      ~s|name|,
      ~s|text|,
      ~s|date|,
      ~s|precision|,
      ~s|lower|,
      ~s|coalesce|,
      ~s|"like"|,
      ~s|"order"|,
      ~s|and|,
      ~s|or|,
      ~s|not|,
      ~s|in|,
      ~s|is|,
      ~s|null|,
      ~s|true|,
      ~s|false|,
      ~s|array|
    ])
  end

  defp literal do
    pick([
      ~s|'a'|,
      ~s|''|,
      ~s|'it''s'|,
      ~s|'-- id'|,
      ~s|'/* id */'|,
      ~s|'"other"'|,
      ~s|'a\\'|,
      ~s|E'a\\'b'|,
      ~s|e'a'|,
      ~s|U&'\\0061'|,
      ~s|B'01'::text|,
      ~s|X'1F'::text|,
      ~s|N'a'|,
      ~s|$$other$$|,
      ~s|$t$'$t$|,
      ~s|'a'\n'b'|,
      ~s|'a'::text|,
      ~s|12|,
      ~s|1.5|,
      ~s|1e5|,
      ~s|NULL|,
      ~s|TRUE|,
      ~s|ARRAY['a', 'b']|,
      ~s|other|,
      ~s|"other"|
    ])
  end

  defp operator,
    do: pick(["=", "<>", "!=", "<", ">", "<=", ">=", "||", "@>", "&&", "~", "!~", "LIKE"])

  defp type, do: pick([~s|text|, ~s|"text"|, ~s|TEXT|, ~s|int4|, ~s|text[]|, ~s|varchar(8)|])
  defp function, do: pick(~w(lower upper coalesce "lower" LOWER))
  defp gap, do: pick(["", "", "", " ", "\n", " /* id */ ", " /* ' */ ", " -- other\n"])

  defp pick(choices), do: Enum.random(choices)
end
