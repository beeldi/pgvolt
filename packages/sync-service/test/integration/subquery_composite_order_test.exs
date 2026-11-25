defmodule Electric.Integration.SubqueryCompositeOrderTest do
  @moduledoc """
  A composite subquery pairs the row on its left with its select list in the order the list names
  its columns, as Postgres does, whatever their names' order.

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
               ~s|CREATE TABLE divisions (id int PRIMARY KEY, "firmId" int NOT NULL, rate float8)|,
               ~s|CREATE TABLE employees (id text PRIMARY KEY, "divisionId" int NOT NULL, "firmId" int NOT NULL, "divisionRate" float8)|
             ]

  # `id` sorts before `"firmId"`, so the select list is out of name order.
  @where ~s|("divisionId", "firmId") IN (SELECT id, "firmId" FROM divisions)|

  setup [:with_unique_db, :with_sql_execute, :with_complete_stack, :with_electric_client]

  test "a composite subquery out of name order selects the rows Postgres does",
       %{client: client} = ctx do
    query!(ctx, "INSERT INTO divisions VALUES (2, 7)")

    query!(ctx, """
    INSERT INTO employees VALUES
      ('snapshot', 2, 7), ('snapshot-swapped', 7, 2), ('moved', 3, 8), ('moved-swapped', 8, 3)
    """)

    with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: @where),
                    live: true
                  ) do
      assert selected(ctx, @where) == ["snapshot"]
      assert_insert(consumer, %{"id" => "snapshot"})
      assert_up_to_date(consumer)

      query!(ctx, "INSERT INTO employees VALUES ('live-swapped', 7, 2), ('live', 2, 7)")
      assert selected(ctx, @where) == ["live", "snapshot"]
      assert_insert(consumer, %{"id" => "live"})
      assert_up_to_date(consumer)

      query!(ctx, "INSERT INTO divisions VALUES (3, 8)")
      assert selected(ctx, @where) == ["live", "moved", "snapshot"]
      assert_insert(consumer, %{"id" => "moved"})
      assert_up_to_date(consumer)

      query!(ctx, "DELETE FROM divisions WHERE id = 2")
      assert selected(ctx, @where) == ["moved"]

      assert {:ok, deletes} =
               await_count(consumer, 2, match: &match?(%{headers: %{operation: :delete}}, &1))

      assert deletes |> Enum.map(& &1.value["id"]) |> Enum.sort() == ["live", "snapshot"]
    end
  end

  test "a negated composite subquery out of name order selects the rows Postgres does",
       %{client: client} = ctx do
    where = "NOT (#{@where})"
    query!(ctx, "INSERT INTO divisions VALUES (2, 7)")
    query!(ctx, "INSERT INTO employees VALUES ('snapshot', 2, 7), ('snapshot-swapped', 7, 2)")

    with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                    live: true
                  ) do
      assert selected(ctx, where) == ["snapshot-swapped"]
      assert_insert(consumer, %{"id" => "snapshot-swapped"})
      assert_up_to_date(consumer)

      query!(ctx, "INSERT INTO employees VALUES ('live', 2, 7), ('live-swapped', 7, 2)")
      assert selected(ctx, where) == ["live-swapped", "snapshot-swapped"]
      assert_insert(consumer, %{"id" => "live-swapped"})
      assert_up_to_date(consumer)
    end
  end

  # Sorted by name, `"firmId"` would pair with `"divisionRate"`, and the shape got a `400`.
  test "a composite subquery out of name order over columns of two types is accepted",
       %{client: client} = ctx do
    where = ~s|("divisionRate", "firmId") IN (SELECT rate, "firmId" FROM divisions)|
    query!(ctx, "INSERT INTO divisions VALUES (2, 7, 1.5)")

    query!(ctx, """
    INSERT INTO employees VALUES ('snapshot', 2, 7, 1.5), ('other-rate', 2, 7, 2.5)
    """)

    with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                    live: true
                  ) do
      assert selected(ctx, where) == ["snapshot"]
      assert_insert(consumer, %{"id" => "snapshot"})
      assert_up_to_date(consumer)

      query!(ctx, "INSERT INTO divisions VALUES (3, 8, 2.5)")
      query!(ctx, "INSERT INTO employees VALUES ('live', 3, 8, 2.5)")
      assert selected(ctx, where) == ["live", "snapshot"]
      assert_insert(consumer, %{"id" => "live"})
      assert_up_to_date(consumer)
    end
  end

  defp query!(ctx, sql), do: Postgrex.query!(ctx.db_conn, sql, [])

  defp selected(ctx, where) do
    %{rows: rows} = query!(ctx, "SELECT id FROM employees WHERE #{where} ORDER BY id")
    List.flatten(rows)
  end
end
