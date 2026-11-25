defmodule Electric.Replication.Eval.SqlGeneratorOracleTest do
  @moduledoc """
  Differential test of the generated snapshot SQL against the clause it was generated from.

  A shape's initial rows come from a query Electric writes by turning the parsed where clause back
  into SQL, and its later rows come from the in-process evaluator reading the same clause's AST. The
  two are only the same shape while those agree, and a disagreement is invisible: the client holds
  rows the tail will never update, or misses rows it will never be sent.

  The database here has `standard_conforming_strings` off, which is legal to set and which the
  running suite's own database does not have. Under it a plain literal reads a backslash as an
  escape, so any clause carrying one means something else — or nothing parseable at all — by the
  time the generated query reaches the server.
  """
  use ExUnit.Case, async: false

  @moduletag database_settings: ["standard_conforming_strings = off"]

  import Support.DbSetup

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner
  alias Electric.Replication.Eval.SqlGenerator

  setup [:with_shared_db, :with_table]

  @refs %{["firmId"] => :text, ["path"] => :text, ["tags"] => {:array, :text}}
  @columns ~w[firmId path tags]

  @rows [
    %{"firmId" => "F0", "path" => "C:\\Users\\me", "tags" => ["a\\b", "plain"]},
    %{"firmId" => "F0", "path" => "plain", "tags" => ["x"]},
    %{"firmId" => "F1", "path" => nil, "tags" => []},
    %{"firmId" => "F0", "path" => "quote'and\\slash", "tags" => ["q'z"]}
  ]

  @clauses [
    ~S|"firmId" = 'F0'|,
    ~S|"path" = 'C:\Users\me'|,
    ~S|"path" <> 'plain'|,
    ~S|"path" IS NULL|,
    ~S|"path" = 'quote''and\slash'|,
    ~S|"path" IN ('plain', 'C:\Users\me')|,
    ~S|"tags" && ARRAY['a\b']|,
    ~S|"tags" @> ARRAY['a\b']|,
    ~S|"firmId" = 'F0' AND "path" = 'C:\Users\me'|,
    ~S|"path" = 'ends with a backslash \' OR "firmId" = 'F0'|
  ]

  defp with_table(%{pool: pool}) do
    Postgrex.query!(pool, "DROP TABLE IF EXISTS generated_clauses", [])

    Postgrex.query!(
      pool,
      ~S|CREATE TABLE generated_clauses (id int PRIMARY KEY, "firmId" text, "path" text, "tags" text[])|,
      []
    )

    @rows
    |> Enum.with_index(1)
    |> Enum.each(fn {row, id} ->
      Postgrex.query!(
        pool,
        ~S|INSERT INTO generated_clauses (id, "firmId", "path", "tags") VALUES ($1, $2, $3, $4)|,
        [id, row["firmId"], row["path"], row["tags"]]
      )
    end)

    :ok
  end

  defp matching_ids(conn, clause) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(conn, "SELECT id FROM generated_clauses WHERE #{clause} ORDER BY id", [])

    List.flatten(rows)
  end

  defp generated(clause),
    do:
      clause
      |> Parser.parse_and_validate_expression!(refs: @refs)
      |> then(& &1.eval)
      |> SqlGenerator.to_sql()

  for clause <- @clauses do
    test "the SQL generated from #{clause} selects what the clause selects", %{pool: pool} do
      # Arrange
      clause = unquote(clause)

      # Act
      {:ok, {expected, actual}} =
        in_pinned_session(pool, fn conn ->
          {matching_ids(conn, clause), matching_ids(conn, generated(clause))}
        end)

      # Assert
      assert actual == expected, "generated: #{generated(clause)}"
    end

    test "the SQL generated from #{clause} means the same thing unpinned", %{pool: pool} do
      # Arrange
      clause = unquote(clause)
      {:ok, expected} = in_pinned_session(pool, &matching_ids(&1, clause))

      # Act
      actual = matching_ids(pool, generated(clause))

      # Assert
      assert actual == expected, "generated: #{generated(clause)}"
    end
  end

  test "a clause embedded verbatim, as a subquery dependency's is, needs the session Electric pins",
       %{pool: pool} do
    # Arrange
    clause = ~S|"path" = 'C:\Users\me'|

    # Act
    {:ok, pinned} = in_pinned_session(pool, &matching_ids(&1, clause))

    # Assert
    assert pinned == [1]

    assert_raise Postgrex.Error, fn -> matching_ids(pool, clause) end
  end

  test "the evaluator agrees with Postgres on every clause and row", %{pool: pool} do
    # Arrange
    expressions =
      Map.new(@clauses, fn clause ->
        {clause, Parser.parse_and_validate_expression!(clause, refs: @refs)}
      end)

    selected = Enum.map_join(@columns, ", ", fn column -> ~s|"#{column}"::text| end)

    # Act
    {:ok, {mismatches, compared}} =
      in_pinned_session(pool, fn conn ->
        for clause <- @clauses, id <- 1..length(@rows), reduce: {[], 0} do
          {acc, compared} ->
            %Postgrex.Result{rows: [values]} =
              Postgrex.query!(conn, "SELECT #{selected} FROM generated_clauses WHERE id = $1", [
                id
              ])

            record = Map.new(Enum.zip(@columns, values))

            %Postgrex.Result{rows: [[expected]]} =
              Postgrex.query!(conn, "SELECT (#{clause}) FROM generated_clauses WHERE id = $1", [
                id
              ])

            actual = Runner.execute_for_record(expressions[clause], record)

            case actual == {:ok, expected} do
              true -> {acc, compared + 1}
              false -> {[{clause, id, record, expected, actual} | acc], compared + 1}
            end
        end
      end)

    # Assert
    assert compared == length(@clauses) * length(@rows)

    assert mismatches == [], """
    #{Enum.map_join(mismatches, "\n\n", fn {clause, id, record, expected, actual} -> """
      #{clause}
        row #{id}:  #{inspect(record)}
        postgres:   #{inspect(expected)}
        evaluator:  #{inspect(actual)}
      """ end)}
    """
  end
end
