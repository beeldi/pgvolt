defmodule Electric.Integration.SubqueryEqualValuesTest do
  @moduledoc """
  A subquery compares keys Postgres calls equal as one key: a float's `-0` and `0`, intervals of
  one span written another way (`1 day` and `24:00:00`), `char(n)` values padded to two lengths,
  and `jsonb` values whose numbers have another scale (`{"rate": 1.0}` and `{"rate": 1}`).

  Each test first asks Postgres which employees the where clause selects, then checks that a live
  client receives exactly those rows.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer

  alias Electric.Client
  alias Electric.Client.ShapeDefinition

  @moduletag :tmp_dir
  @moduletag with_sql: [
               ~s|CREATE TABLE divisions (id text PRIMARY KEY, "firmId" int NOT NULL, rate float8, term interval, rates float8[], terms interval[], label char(5), labels char(5)[], meta jsonb, metas jsonb[])|,
               ~s|CREATE TABLE employees (id text PRIMARY KEY, "firmId" int NOT NULL, "divisionRate" float8, "divisionTerm" interval, "divisionRates" float8[], "divisionTerms" interval[], "divisionCode" char(3), "divisionCodes" char(3)[], "divisionMeta" jsonb, "divisionMetas" jsonb[])|
             ]

  setup [:with_unique_db, :with_sql_execute, :with_complete_stack, :with_electric_client]

  # {kind, employee column, division column, division key, [employee keys]}: each employee key is
  # equal to the division key in Postgres, and written another way. A division's `char(5)` key is
  # padded to five characters, an employee's `char(3)` key to three.
  @keys [
    {"float8", "divisionRate", "rate", "-0", ["0", "0.0"]},
    {"interval", "divisionTerm", "term", "1 day", ["24:00:00", "1440 minutes"]},
    {"float8[]", "divisionRates", "rates", "{-0,1}", ["{0,1}", "{0.0,1.0}"]},
    {"interval[]", "divisionTerms", "terms", ~s|{"1 day","1 mon"}|,
     [~s|{"24:00:00","30 days"}|, ~s|{"1440 minutes","720:00:00"}|]},
    {"char", "divisionCode", "label", "ab", ["ab", "ab "]},
    {"char[]", "divisionCodes", "labels", "{ab,cd}", ["{ab,cd}", ~s|{"ab ","cd "}|]},
    {"jsonb", "divisionMeta", "meta", ~s|{"rate": 1.0, "codes": [2]}|,
     [~s|{"codes": [2.00], "rate": 1}|, ~s|{"rate": 1.00, "codes": [2.0]}|]},
    {"jsonb at scale 34", "divisionMeta", "meta", ~s|{"rate": 1.#{String.duplicate("0", 34)}}|,
     [~s|{"rate": 1}|, ~s|{"rate": 1.0}|]},
    {"jsonb[]", "divisionMetas", "metas", ~S|{"{\"rate\": 1.0}","2"}|,
     [~S|{"{\"rate\": 1}","2.0"}|, ~S|{"{\"rate\": 1.00}","2.00"}|]}
  ]

  for {kind, column, parent_column, parent_key, child_keys} <- @keys,
      shape <- [:single, :composite] do
    test "a #{shape} #{kind} key moves in and out the employees whose key Postgres finds equal",
         %{client: client} = ctx do
      where = where(unquote(shape), unquote(column), unquote(parent_column))
      [moved_in_key, live_key] = unquote(child_keys)
      insert_employee!(ctx, "moved-in", unquote(column), moved_in_key)

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_up_to_date(consumer)

        insert_division!(ctx, "d1", unquote(parent_column), unquote(parent_key))
        assert selected(ctx, where) == ["moved-in"]
        assert_insert(consumer, %{"id" => "moved-in"})
        assert_up_to_date(consumer)

        insert_employee!(ctx, "live", unquote(column), live_key)
        assert selected(ctx, where) == ["live", "moved-in"]
        assert_insert(consumer, %{"id" => "live"})
        assert_up_to_date(consumer)

        query!(ctx, "DELETE FROM divisions WHERE id = 'd1'")
        assert selected(ctx, where) == []

        assert {:ok, deletes} =
                 await_count(consumer, 2, match: &match?(%{headers: %{operation: :delete}}, &1))

        assert deletes |> Enum.map(& &1.value["id"]) |> Enum.sort() == ["live", "moved-in"]
      end
    end
  end

  for {kind, column, parent_column, parent_key, [child_key, _]} <- @keys do
    test "adding a negated #{kind} key moves out the employees whose key Postgres finds equal",
         %{client: client} = ctx do
      where = ~s|"#{unquote(column)}" NOT IN (SELECT #{unquote(parent_column)} FROM divisions)|
      insert_employee!(ctx, "held", unquote(column), unquote(child_key))

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_insert(consumer, %{"id" => "held"})
        assert_up_to_date(consumer)

        insert_division!(ctx, "d1", unquote(parent_column), unquote(parent_key))
        assert selected(ctx, where) == []
        assert_delete(consumer, %{"id" => "held"})
      end
    end

    # Two divisions hold one key, written two ways: it stays in the subquery until both leave.
    test "two divisions writing a #{kind} key two ways keep it until the last leaves",
         %{client: client} = ctx do
      where = ~s|"#{unquote(column)}" IN (SELECT #{unquote(parent_column)} FROM divisions)|
      insert_division!(ctx, "d1", unquote(parent_column), unquote(parent_key))
      insert_division!(ctx, "d2", unquote(parent_column), unquote(child_key))
      insert_employee!(ctx, "held", unquote(column), unquote(child_key))

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_insert(consumer, %{"id" => "held"})
        assert_up_to_date(consumer)

        query!(ctx, "DELETE FROM divisions WHERE id = 'd1'")
        insert_employee!(ctx, "after", unquote(column), unquote(parent_key))
        assert selected(ctx, where) == ["after", "held"]

        # The first change the client sees is the employee added after, not a delete of the held
        # one.
        assert {:ok, [change]} =
                 await_count(consumer, 1,
                   match: &match?(%{headers: %{operation: op}} when op in [:insert, :delete], &1)
                 )

        assert %{headers: %{operation: :insert}, value: %{"id" => "after"}} = change

        query!(ctx, "DELETE FROM divisions WHERE id = 'd2'")
        assert selected(ctx, where) == []

        assert {:ok, deletes} =
                 await_count(consumer, 2, match: &match?(%{headers: %{operation: :delete}}, &1))

        assert deletes |> Enum.map(& &1.value["id"]) |> Enum.sort() == ["after", "held"]
      end
    end
  end

  defp where(:single, column, parent_column),
    do: ~s|"#{column}" IN (SELECT #{parent_column} FROM divisions)|

  defp where(:composite, column, parent_column),
    do: ~s|("firmId", "#{column}") IN (SELECT "firmId", #{parent_column} FROM divisions)|

  defp insert_division!(ctx, id, column, key) do
    query!(ctx, ~s|INSERT INTO divisions (id, "firmId", #{column}) VALUES ('#{id}', 1, '#{key}')|)
  end

  defp insert_employee!(ctx, id, column, key) do
    query!(
      ctx,
      ~s|INSERT INTO employees (id, "firmId", "#{column}") VALUES ('#{id}', 1, '#{key}')|
    )
  end

  defp query!(ctx, sql), do: Postgrex.query!(ctx.db_conn, sql, [])

  defp selected(ctx, where) do
    %{rows: rows} = query!(ctx, "SELECT id FROM employees WHERE #{where} ORDER BY id")
    List.flatten(rows)
  end
end
