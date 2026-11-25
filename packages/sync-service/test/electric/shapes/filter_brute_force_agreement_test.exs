defmodule Electric.Shapes.FilterBruteForceAgreementTest do
  @moduledoc """
  Differential test: whatever `Filter.affected_shapes/2` routes must equal a brute-force
  evaluation of every shape's own where clause against the record.
  """
  use ExUnit.Case, async: false

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.UpdatedRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Electric.Shapes.WhereClause
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"},
                 %{name: "categoryId", type: "text"},
                 %{name: "deletedAt", type: "timestamptz"},
                 %{name: "divisionIds", array_type: "text"}
               ]
             )

  @token ~s|'cache-buster:00003' = 'cache-buster:00003'|

  @clauses [
    # the primary clause, and its degenerate forms
    ~s|"firmId" = 'F0' AND "divisionId" IN ('b1', 'b2', 'b3') AND "deletedAt" IS NULL AND | <>
      @token,
    ~s|"firmId" = 'F0' AND "deletedAt" IS NULL AND | <> @token,
    ~s|"firmId" = 'F1' AND "divisionId" IN ('b2', 'b4') AND "deletedAt" IS NULL|,
    ~s|"divisionId" IN ('b1', 'b5')|,
    ~s|"divisionId" IN ('b3')|,
    # residual on the LEFT of the OR
    ~s|"deletedAt" IS NULL AND "divisionId" IN ('b1', 'b2')|,
    ~s|#{@token} AND "divisionId" IN ('b4', 'b5')|,
    # cross product of two IN lists
    ~s|"divisionId" IN ('b1', 'b2') AND "categoryId" IN ('c1', 'c2')|,
    ~s|"firmId" = 'F0' AND "divisionId" IN ('b1', 'b2', 'b3') AND "categoryId" IN ('c1', 'c3')|,
    # OR of heterogeneous branches
    ~s|("divisionId" = 'b1' OR "categoryId" = 'c2') AND "deletedAt" IS NULL|,
    ~s|("divisionId" = 'b1' AND "categoryId" = 'c1') OR "categoryId" = 'c3'|,
    ~s|(("divisionId" = 'b1' OR "divisionId" = 'b2') AND "firmId" = 'F0') OR "categoryId" = 'c3'|,
    # partially optimisable OR -> must stay unindexed but still route correctly
    ~s|"divisionId" = 'b1' OR "categoryId" != 'c9'|,
    ~s|"firmId" = 'F0' AND ("divisionId" != 'b9' OR "categoryId" = 'c1')|,
    # duplicates and single-element lists
    ~s|"divisionId" IN ('b1', 'b1', 'b2')|,
    ~s|"firmId" = 'F0' AND "divisionId" IN ('b1', 'b1') AND "deletedAt" IS NULL|,
    # negation / null handling
    ~s|"firmId" = 'F0' AND "deletedAt" IS NOT NULL|,
    ~s|"firmId" IN ('F0', 'F1') AND "divisionId" IN ('b1', 'b2', 'b3', 'b4')|,
    ~s|"categoryId" IS NULL AND "divisionId" IN ('b1', 'b2')|,
    # null constants: every one of these is unknown for every record, so the shape is empty
    ~s|"firmId" = NULL|,
    ~s|NULL = "firmId"|,
    ~s|"firmId" != NULL|,
    ~s|"firmId" = NULL AND "deletedAt" IS NULL|,
    ~s|"firmId" = 'F0' OR "divisionId" = NULL|,
    ~s|"divisionId" IN ('b1', NULL)|,
    ~s|"divisionId" IN (NULL)|,
    # Array-overlap clauses: one element takes the containment index directly, several distribute
    # into a disjunct per element, including over rows whose array is null or holds a null element.
    ~s|"divisionIds" && ARRAY['b1']::text[]|,
    ~s|"divisionIds" && ARRAY['b1', 'b2']::text[]|,
    ~s|"divisionIds" && ARRAY['b1', 'b1', 'b2']::text[]|,
    ~s|"divisionIds" @> ARRAY['b1']::text[]|,
    ~s|"firmId" = 'F0' AND "divisionIds" && ARRAY['b1', 'b2']::text[] AND "deletedAt" IS NULL|,
    ~s|"divisionIds" && ARRAY['b9']::text[] OR "categoryId" = 'c1'|,
    ~s|"divisionIds" IS NULL|,
    ~s|"divisionIds" && ARRAY['b1', 'b2']::text[] AND "categoryId" IN ('c1', 'c2')|,
    # Containment of the empty array is true for every array, overlap with it is false for every
    # array, and the optimiser's `@>`/`<@` clauses guard only `is_list`, so the empty list reaches
    # the containment index.
    ~s|"divisionIds" @> ARRAY[]::text[]|,
    ~s|ARRAY[]::text[] <@ "divisionIds"|,
    ~s|"divisionIds" && ARRAY[]::text[]|
  ]

  defp shape(where), do: Shape.new!("employees", where: where, inspector: @inspector)

  defp records do
    firms = ["F0", "F1", "F2", nil]
    divisions = ["b1", "b2", "b3", "b4", "b5", "b9", nil]
    categories = ["c1", "c2", "c3", "c9", nil]
    deleted = [nil, "2026-01-01 00:00:00Z"]

    # Replication-stream text, so an array arrives as its Postgres output form and a missing one
    # as nil. `{NULL}` and `{b1,NULL}` are the shapes the sibling operator lost the third truth
    # value on.
    groups = [nil, "{}", "{b1}", "{b2,b3}", "{b9}", "{NULL}", "{b1,NULL}"]

    for p <- firms, b <- divisions, c <- categories, d <- deleted, g <- groups do
      %{
        "id" => "e1",
        "firmId" => p,
        "divisionId" => b,
        "categoryId" => c,
        "deletedAt" => d,
        "divisionIds" => g
      }
    end
  end

  defp brute_force(shapes, record) do
    for {id, s} <- shapes,
        WhereClause.includes_record?(s.where, record),
        into: MapSet.new(),
        do: id
  end

  test "routing equals brute-force evaluation for every clause and record" do
    # Arrange
    shapes = @clauses |> Enum.with_index() |> Enum.map(fn {w, i} -> {"s#{i}", shape(w)} end)

    filter =
      Enum.reduce(shapes, Filter.new(), fn {id, s}, f -> Filter.add_shape(f, id, s) end)

    recs = records()

    # Act
    mismatches =
      Enum.flat_map(recs, fn record ->
        actual =
          Filter.affected_shapes(filter, %NewRecord{
            relation: {"public", "employees"},
            record: record
          })

        expected = brute_force(shapes, record)

        if actual == expected do
          []
        else
          [
            {record, MapSet.difference(actual, expected) |> Enum.sort(),
             MapSet.difference(expected, actual) |> Enum.sort()}
          ]
        end
      end)

    # Assert
    assert mismatches == [],
           "#{length(mismatches)} routing mismatches:\n" <>
             Enum.map_join(Enum.take(mismatches, 8), "\n", fn {r, extra, missing} ->
               "  MISMATCH #{inspect(r)}\n    over-routed: #{inspect(extra)}  MISSED: #{inspect(missing)}"
             end)
  end

  test "updates route on both old and new record" do
    shapes = @clauses |> Enum.with_index() |> Enum.map(fn {w, i} -> {"s#{i}", shape(w)} end)
    filter = Enum.reduce(shapes, Filter.new(), fn {id, s}, f -> Filter.add_shape(f, id, s) end)

    old = %{
      "id" => "e1",
      "firmId" => "F0",
      "divisionId" => "b1",
      "categoryId" => "c1",
      "deletedAt" => nil
    }

    new = %{
      "id" => "e1",
      "firmId" => "F0",
      "divisionId" => "b3",
      "categoryId" => "c3",
      "deletedAt" => nil
    }

    actual =
      Filter.affected_shapes(filter, %UpdatedRecord{
        relation: {"public", "employees"},
        old_record: old,
        record: new
      })

    expected = MapSet.union(brute_force(shapes, old), brute_force(shapes, new))

    assert actual == expected
  end

  # Every value in `records/0` parses. These do not, and Postgres emits every one of them: an
  # `infinity` or year-10000 timestamptz, and an array whose lower bound is not 1, which Postgres
  # renders with an explicit dimension prefix. The evaluator refuses all four, and routing may only
  # answer wider than the evaluator for the shapes that actually read the refused column.
  defp unreadable_records do
    deleted = [nil, "infinity", "-infinity", "10000-01-01 00:00:00+00"]
    groups = [nil, "{b1}", "[0:2]={b1,b2,b3}"]

    for p <- ["F0", "F2"], b <- ["b1", "b9"], d <- deleted, g <- groups do
      %{
        "id" => "e1",
        "firmId" => p,
        "divisionId" => b,
        "categoryId" => "c1",
        "deletedAt" => d,
        "divisionIds" => g
      }
    end
  end

  test "a value the evaluator cannot read widens routing only for the shapes that read it" do
    # Arrange
    shapes = @clauses |> Enum.with_index() |> Enum.map(fn {w, i} -> {"s#{i}", shape(w)} end)
    filter = Enum.reduce(shapes, Filter.new(), fn {id, s}, f -> Filter.add_shape(f, id, s) end)

    # Act
    findings =
      Enum.flat_map(unreadable_records(), fn record ->
        {actual, exact?} =
          Filter.affected_shapes_exact(filter, %NewRecord{
            relation: {"public", "employees"},
            record: record
          })

        expected = brute_force(shapes, record)

        excusable =
          for {id, s} <- shapes,
              WhereClause.includes_record_result(s.where, record) == :error,
              into: MapSet.new(),
              do: id

        missed = MapSet.difference(expected, actual)
        unexcused = MapSet.difference(MapSet.difference(actual, expected), excusable)
        silent? = actual != expected and exact?

        if MapSet.size(missed) == 0 and MapSet.size(unexcused) == 0 and not silent? do
          []
        else
          [{record, Enum.sort(missed), Enum.sort(unexcused), silent?}]
        end
      end)

    # Assert
    assert findings == [],
           Enum.map_join(Enum.take(findings, 8), "\n", fn {r, missed, unexcused, silent?} ->
             "  #{inspect(r)}\n    MISSED: #{inspect(missed)}  over-routed past the refused column: #{inspect(unexcused)}  over-routed silently: #{silent?}"
           end)
  end

  test "add then remove every shape leaves nothing routable and no rows" do
    shapes = @clauses |> Enum.with_index() |> Enum.map(fn {w, i} -> {"s#{i}", shape(w)} end)
    filter = Enum.reduce(shapes, Filter.new(), fn {id, s}, f -> Filter.add_shape(f, id, s) end)

    drained = Enum.reduce(shapes, filter, fn {id, _}, f -> Filter.remove_shape(f, id) end)

    rows =
      for t <- [
            drained.where_cond_table,
            drained.other_shapes_table,
            drained.eq_index_table,
            drained.shapes_table
          ],
          do: :ets.info(t)[:size]

    assert rows == [0, 0, 0, 0]

    Enum.each(records(), fn record ->
      assert Filter.affected_shapes(drained, %NewRecord{
               relation: {"public", "employees"},
               record: record
             }) == MapSet.new()
    end)
  end
end
