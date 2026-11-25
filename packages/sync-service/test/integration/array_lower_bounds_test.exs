defmodule Electric.Integration.ArrayLowerBoundsTest do
  @moduledoc """
  A row whose array has a lower bound other than 1, which an assignment such as `tags[0] = 'x'`
  gives it and which Postgres writes as `[0:1]={x,y}`, reaches a live client as Postgres decides.

  Each step first asks Postgres which employees the where clause selects, then checks that the
  live client receives that change.
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
               ~s|CREATE TABLE employees (id text PRIMARY KEY, "firmId" int NOT NULL, tags text[])|
             ]

  setup [:with_unique_db, :with_sql_execute, :with_complete_stack, :with_electric_client]

  test "a live client receives the changes of a row whose array starts at 0",
       %{client: client} = ctx do
    where = "tags @> '{lead}'"
    query!(ctx, "INSERT INTO employees VALUES ('snapshot', 1, '{lead}')")

    with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                    live: true
                  ) do
      assert_insert(consumer, %{"id" => "snapshot"})
      assert_up_to_date(consumer)

      query!(ctx, "INSERT INTO employees VALUES ('live', 1, '[0:1]={lead,ops}')")
      assert selected(ctx, where) == ["live", "snapshot"]
      assert_insert(consumer, %{"id" => "live", "tags" => "[0:1]={lead,ops}"})
      assert_up_to_date(consumer)

      query!(ctx, "UPDATE employees SET tags[0] = 'ops' WHERE id = 'snapshot'")
      assert selected(ctx, where) == ["live", "snapshot"]
      assert_update(consumer, %{"id" => "snapshot", "tags" => "[0:1]={ops,lead}"})
      assert_up_to_date(consumer)

      query!(ctx, "UPDATE employees SET tags[1] = 'ops' WHERE id = 'snapshot'")
      assert selected(ctx, where) == ["live"]
      assert_delete(consumer, %{"id" => "snapshot"})
      assert_up_to_date(consumer)
    end
  end

  test "a live client receives a row an equality on a bounded array selects",
       %{client: client} = ctx do
    where = "tags <> '{lead,ops}'"
    query!(ctx, "INSERT INTO employees VALUES ('plain', 1, '{lead,ops}')")

    with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                    live: true
                  ) do
      assert_up_to_date(consumer)

      query!(ctx, "INSERT INTO employees VALUES ('bounded', 1, '[0:1]={lead,ops}')")
      assert selected(ctx, where) == ["bounded"]
      assert_insert(consumer, %{"id" => "bounded"})
      assert_up_to_date(consumer)
    end
  end

  defp query!(ctx, sql), do: Postgrex.query!(ctx.db_conn, sql, [])

  defp selected(ctx, where) do
    %{rows: rows} = query!(ctx, "SELECT id FROM employees WHERE #{where} ORDER BY id")
    List.flatten(rows)
  end
end
