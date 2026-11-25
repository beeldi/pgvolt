defmodule Electric.Replication.Eval.ColumnPredicateOracleTest do
  @moduledoc """
  Differential test of the *column* evaluation path against real Postgres.

  The oracle in `runner_test.exs` compares constant expressions (`SELECT <clause>` with no columns),
  which exercises constant folding and never reaches `record_to_ref_values/3`, where a row's text
  values are parsed before the predicate runs.

  This suite pins a broad surface of column predicates across text, jsonb, boolean, array,
  timestamptz, and ICU-collated text columns and compares the in-process evaluator against Postgres
  for every row. Each record passed to the runner is produced by Postgres's text output under
  Electric's pinned display settings, matching what the replication stream delivers.
  """
  use ExUnit.Case, async: false

  import Support.DbSetup

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner

  setup [:with_shared_db, :with_table]

  @refs %{
    ["firmId"] => :text,
    ["divisionId"] => :text,
    ["authorId"] => :text,
    ["deletedAt"] => :timestamptz,
    ["metaJson"] => :jsonb,
    ["verified"] => :bool,
    ["groupIds"] => {:array, :text},
    ["slug"] => :text,
    ["teamId"] => :text,
    ["gradeId"] => :text,
    ["roleId"] => :text
  }

  @columns ~w[firmId divisionId authorId deletedAt metaJson verified groupIds slug
              teamId gradeId roleId]

  # Broad operator and type matrix across nullable, array, jsonb, boolean, timestamptz, and collated columns.
  @predicates [
    ~S|"firmId" = 'F0'|,
    ~S|"firmId" != 'F0'|,
    ~S|"deletedAt" IS NULL|,
    ~S|"deletedAt" IS NOT NULL|,
    ~S|"metaJson" IS NOT NULL|,
    ~S|"verified" = true|,
    ~S|"divisionId" IN ('D0','D1')|,
    ~S|"groupIds" != '{}'|,
    ~S|"groupIds" && ARRAY['g1','g2']::text[]|,
    # The one-element overlap takes the direct containment-index route; the long one exceeds the
    # distribution cap and stays unindexed; both must still match Postgres.
    ~S|"groupIds" && ARRAY['g1']::text[]|,
    ~S|"groupIds" && ARRAY['g1','g2','g3','g4','g5','g6','g7','g8','g9']::text[]|,
    ~S|"groupIds" @> ARRAY['g1']::text[]|,
    ~S|"groupIds" IS NULL|,
    ~S|"firmId" = 'F0' AND "groupIds" && ARRAY['g1','g9']::text[] AND "deletedAt" IS NULL|,
    # Degenerate constant predicates and empty array operands.
    ~S|false|,
    ~S|true|,
    ~S|'cache-buster:00003' = 'cache-buster:00003'|,
    ~S|"divisionId" IN ('D0')|,
    ~S|"groupIds" @> ARRAY[]::text[]|,
    ~S|ARRAY[]::text[] <@ "groupIds"|,
    ~S|"groupIds" && ARRAY[]::text[]|,
    # Compound nullability, disjunction, and multi-column predicates.
    ~S|"teamId" IS NOT NULL|,
    ~S|"firmId" IS NULL AND "teamId" IS NULL|,
    ~S|("deletedAt" IS NULL OR "metaJson" IS NOT NULL) AND "gradeId" IS NOT NULL|,
    ~S|"authorId" = 'System'|,
    ~S|("deletedAt" IS NULL OR "verified" IS NULL) AND "roleId" IS NOT NULL|,
    ~S|"firmId" IS NULL AND "groupIds" IS NOT NULL AND "groupIds" != '{}' AND "deletedAt" IS NULL|,
    ~S|"firmId" IS NULL|,
    ~S|("deletedAt" IS NULL OR "metaJson" IS NOT NULL) AND "authorId" IS NOT NULL|,
    ~S|"firmId" = 'F0' AND "divisionId" IN ('D0','D1')|,
    ~S|"firmId" = 'F0' OR "deletedAt" IS NOT NULL|,
    # Lexicographic range predicate pinned to the collation both evaluators share.
    ~S|"slug" >= 'm' COLLATE "C" AND "slug" < 'n' COLLATE "C"|
  ]

  # Edge values for each type: SQL NULL vs JSON null, empty array vs `'{}'`, accents and case for
  # collation ordering, and whitespace in raw-text jsonb.
  @rows [
    %{
      "firmId" => "F0",
      "divisionId" => "D0",
      "authorId" => "u1",
      "deletedAt" => nil,
      "metaJson" => ~S|"A410B"|,
      "verified" => true,
      "groupIds" => ["g1"],
      "slug" => "mars",
      "teamId" => "rg1",
      "gradeId" => "sq1",
      "roleId" => nil
    },
    %{
      "firmId" => "F1",
      "divisionId" => "D9",
      "authorId" => nil,
      "deletedAt" => "2026-08-12 14:00:00+00",
      "metaJson" => nil,
      "verified" => false,
      "groupIds" => [],
      "slug" => "Mars",
      "teamId" => nil,
      "gradeId" => nil,
      "roleId" => "spos1"
    },
    %{
      "firmId" => "F0",
      "divisionId" => "D1",
      "authorId" => "u2",
      "deletedAt" => nil,
      "metaJson" => "null",
      "verified" => nil,
      "groupIds" => ["g2", "g3"],
      "slug" => "Élan",
      "teamId" => "rg2",
      "gradeId" => nil,
      "roleId" => nil
    },
    %{
      "firmId" => nil,
      "divisionId" => nil,
      "authorId" => "System",
      "deletedAt" => "2026-01-01 00:00:00+00",
      "metaJson" => ~S|{ "a" : 1 }|,
      "verified" => true,
      "groupIds" => ["g9"],
      "slug" => "nova",
      "teamId" => nil,
      "gradeId" => "sq2",
      "roleId" => "spos2"
    },
    # A NULL array and an array carrying a NULL element, exercising three-valued array logic.
    %{
      "firmId" => "F0",
      "divisionId" => "D0",
      "authorId" => "u4",
      "deletedAt" => nil,
      "metaJson" => nil,
      "verified" => false,
      "groupIds" => nil,
      "slug" => "orion",
      "teamId" => nil,
      "gradeId" => nil,
      "roleId" => nil
    },
    %{
      "firmId" => "F0",
      "divisionId" => "D1",
      "authorId" => "u5",
      "deletedAt" => nil,
      "metaJson" => nil,
      "verified" => true,
      "groupIds" => ["g1", nil],
      "slug" => "pavo",
      "teamId" => "rg3",
      "gradeId" => "sq3",
      "roleId" => "spos3"
    },
    # Flips the null-firm predicate, which every other row leaves false: a row with nil firmId,
    # a non-empty groupIds array, and nil deletedAt. Also flips `"firmId" = 'F0' OR "deletedAt" IS NOT NULL`.
    %{
      "firmId" => nil,
      "divisionId" => "D2",
      "authorId" => "u7",
      "deletedAt" => nil,
      "metaJson" => nil,
      "verified" => false,
      "groupIds" => ["g1"],
      "slug" => "rigel",
      "teamId" => nil,
      "gradeId" => nil,
      "roleId" => nil
    },
    %{
      "firmId" => "F0",
      "divisionId" => "D1",
      "authorId" => "u6",
      "deletedAt" => nil,
      "metaJson" => nil,
      "verified" => true,
      "groupIds" => [nil],
      "slug" => "vega",
      "teamId" => nil,
      "gradeId" => "sq4",
      "roleId" => nil
    },
    # Timestamps Postgres stores and renders that `Env.parse_const/3` refuses (`infinity` and
    # 5-digit years). A refusal must still answer null tests and unrelated conjuncts like Postgres,
    # or the row leaves the shape while the snapshot still holds it.
    %{
      "firmId" => "F0",
      "divisionId" => "D1",
      "authorId" => "u8",
      "deletedAt" => "infinity",
      "metaJson" => ~S|"D32"|,
      "verified" => true,
      "groupIds" => ["g1"],
      "slug" => "sirius",
      "teamId" => "rg4",
      "gradeId" => "sq5",
      "roleId" => "spos5"
    },
    %{
      "firmId" => "F1",
      "divisionId" => "D2",
      "authorId" => "u9",
      "deletedAt" => "10000-01-01 00:00:00+00",
      "metaJson" => nil,
      "verified" => false,
      "groupIds" => ["g2"],
      "slug" => "tau",
      "teamId" => nil,
      "gradeId" => "sq6",
      "roleId" => nil
    }
  ]

  defp with_table(%{pool: pool}) do
    Postgrex.query!(pool, "DROP TABLE IF EXISTS predicate_oracle_rows", [])

    Postgrex.query!(
      pool,
      """
      CREATE TABLE predicate_oracle_rows (
        id int PRIMARY KEY,
        "firmId" text,
        "divisionId" text,
        "authorId" text,
        "deletedAt" timestamptz,
        "metaJson" jsonb,
        "verified" boolean,
        "groupIds" text[],
        "slug" text COLLATE "en-US-x-icu",
        "teamId" text,
        "gradeId" text,
        "roleId" text
      )
      """,
      []
    )

    @rows
    |> Enum.with_index(1)
    |> Enum.each(fn {row, id} ->
      Postgrex.query!(
        pool,
        """
        INSERT INTO predicate_oracle_rows
          (id, "firmId", "divisionId", "authorId", "deletedAt", "metaJson", "verified", "groupIds", "slug",
           "teamId", "gradeId", "roleId")
        VALUES ($1, $2, $3, $4, $5::text::timestamptz, $6::text::jsonb, $7, $8, $9, $10, $11, $12)
        """,
        [
          id,
          row["firmId"],
          row["divisionId"],
          row["authorId"],
          row["deletedAt"],
          row["metaJson"],
          row["verified"],
          row["groupIds"],
          row["slug"],
          row["teamId"],
          row["gradeId"],
          row["roleId"]
        ]
      )
    end)

    :ok
  end

  defp record_for(conn, id) do
    selected = Enum.map_join(@columns, ", ", fn column -> ~s|"#{column}"::text| end)

    %Postgrex.Result{rows: [values]} =
      Postgrex.query!(conn, "SELECT #{selected} FROM predicate_oracle_rows WHERE id = $1", [id])

    Map.new(Enum.zip(@columns, values))
  end

  defp postgres_says(conn, predicate, id) do
    %Postgrex.Result{rows: [[verdict]]} =
      Postgrex.query!(
        conn,
        "SELECT (#{predicate}) FROM predicate_oracle_rows WHERE id = $1",
        [id]
      )

    verdict
  end

  test "every column predicate agrees with Postgres on every row", %{pool: pool} do
    # Arrange
    expressions =
      Map.new(@predicates, fn predicate ->
        {predicate, Parser.parse_and_validate_expression!(predicate, refs: @refs)}
      end)

    {:ok, {mismatches, compared}} =
      in_pinned_session(pool, fn conn ->
        # Act
        for predicate <- @predicates, id <- 1..length(@rows), reduce: {[], 0} do
          {acc, compared} ->
            record = record_for(conn, id)
            expected = postgres_says(conn, predicate, id)
            actual = Runner.execute_for_record(expressions[predicate], record)

            if actual == {:ok, expected} do
              {acc, compared + 1}
            else
              {[{predicate, id, record, expected, actual} | acc], compared + 1}
            end
        end
      end)

    # Assert -- a comprehension that stopped producing pairs would report a clean run forever.
    assert compared == length(@predicates) * length(@rows)

    assert mismatches == [], """
    The in-process evaluator disagreed with Postgres on one of these predicates. Rows matched by the
    snapshot and not by the live tail (or the reverse) go stale on the client with no error
    anywhere.

    #{Enum.map_join(mismatches, "\n\n", fn {predicate, id, record, expected, actual} -> """
      #{predicate}
        row #{id}:  #{inspect(record)}
        postgres:   #{inspect(expected)}
        evaluator:  #{inspect(actual)}
      """ end)}
    """
  end

  # Constant predicates (`true`, `false`, and a no-op tautology) answer the same on every row by
  # construction, so they are exempt from the discrimination check below.
  @constant_predicates [
    ~S|true|,
    ~S|false|,
    ~S|'cache-buster:00003' = 'cache-buster:00003'|
  ]

  # Agreement is only worth what the rows make it prove: a predicate that answers the same on every
  # row agrees with Postgres for free. This pins that each one actually separates the row set, so a
  # later edit to @rows cannot quietly turn a predicate into a constant.
  test "every non-constant predicate separates the rows rather than answering the same everywhere",
       %{pool: pool} do
    # Act
    {:ok, verdicts} =
      in_pinned_session(pool, fn conn ->
        Map.new(@predicates, fn predicate ->
          {predicate, Enum.map(1..length(@rows), &postgres_says(conn, predicate, &1))}
        end)
      end)

    # Assert
    constant =
      verdicts
      |> Map.drop(@constant_predicates)
      |> Enum.filter(fn {_, answers} -> Enum.uniq(answers) |> length() == 1 end)
      |> Enum.map(fn {predicate, answers} -> {predicate, hd(answers)} end)

    assert constant == [], """
    These predicates answer the same on every row, so their agreement with the evaluator is free
    and proves nothing. Add a row that flips each one.

    #{Enum.map_join(constant, "\n", fn {predicate, answer} -> "  #{inspect(answer)} everywhere: #{predicate}" end)}
    """
  end

  # The reason a lexicographic range predicate has to carry `COLLATE "C"`, kept executable so
  # nobody removes the pin on the grounds that the tests pass without it. `slug` is declared with
  # an ICU collation on purpose: postgres alpine images use musl locales that answer byte-wise, so
  # a libc-collated column here would agree with the evaluator for the wrong reason.
  test "an unpinned range predicate is exactly what the collation pin exists to avoid", %{
    pool: pool
  } do
    # Arrange
    unpinned = ~S|"slug" >= 'm' AND "slug" < 'n'|
    expr = Parser.parse_and_validate_expression!(unpinned, refs: @refs)

    {:ok, verdicts} =
      in_pinned_session(pool, fn conn ->
        # Act
        for id <- 1..length(@rows) do
          record = record_for(conn, id)

          {record["slug"], postgres_says(conn, unpinned, id),
           Runner.execute_for_record(expr, record)}
        end
      end)

    # Assert
    divergent =
      Enum.filter(verdicts, fn {_slug, expected, actual} -> actual != {:ok, expected} end)

    refute divergent == [], """
    The unpinned range predicate agreed with Postgres on every row, so this harness is not
    exercising collation at all and the COLLATE "C" pin is untested here. Check that `slug` still
    carries its ICU collation and that the fixtures still contain mixed case or accents.

    #{inspect(verdicts)}
    """
  end
end
