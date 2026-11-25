defmodule Electric.Integration.SubqueryArrayKeyTest do
  @moduledoc """
  A subquery whose key is an array column moves employees in and out as Postgres selects them.

  Each step first asks Postgres which employees the where clause selects, then checks that a live
  client receives exactly those changes, with no `must-refetch` in between.
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
               ~s|CREATE TABLE divisions (id text PRIMARY KEY, "firmId" int NOT NULL, codes int4[])|,
               ~s|CREATE TABLE employees (id text PRIMARY KEY, "firmId" int NOT NULL, "divisionCodes" int4[])|
             ]

  @single ~s|"divisionCodes" IN (SELECT codes FROM divisions)|
  @composite ~s|("divisionCodes", "firmId") IN (SELECT codes, "firmId" FROM divisions)|

  setup [:with_unique_db, :with_sql_execute, :with_complete_stack, :with_electric_client]

  for {kind, where} <- [single: @single, composite: @composite],
      {key, other} <- [{"{1,2}", "[0:1]={1,2}"}, {"[0:1]={1,2}", "{1,2}"}] do
    test "a #{kind} key #{key} moves employees into and out of the shape",
         %{client: client} = ctx do
      where = unquote(where)
      key = unquote(key)
      other = unquote(other)

      query!(
        ctx,
        "INSERT INTO employees VALUES ('snapshot', 1, '#{key}'), ('other', 1, '#{other}')"
      )

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_up_to_date(consumer)

        query!(ctx, "INSERT INTO divisions VALUES ('d1', 1, '#{key}')")
        assert selected(ctx, where) == ["snapshot"]
        assert_insert(consumer, %{"id" => "snapshot", "divisionCodes" => key})
        assert_up_to_date(consumer)

        query!(
          ctx,
          "INSERT INTO employees VALUES ('live', 1, '#{key}'), ('live-other', 1, '#{other}')"
        )

        assert selected(ctx, where) == ["live", "snapshot"]
        assert_insert(consumer, %{"id" => "live", "divisionCodes" => key})
        assert_up_to_date(consumer)

        query!(ctx, "DELETE FROM divisions WHERE id = 'd1'")
        assert selected(ctx, where) == []

        assert {:ok, deletes} =
                 await_count(consumer, 2, match: &match?(%{headers: %{operation: :delete}}, &1))

        assert deletes |> Enum.map(& &1.value["id"]) |> Enum.sort() == ["live", "snapshot"]
      end
    end
  end

  defp query!(ctx, sql), do: Postgrex.query!(ctx.db_conn, sql, [])

  defp selected(ctx, where) do
    %{rows: rows} = query!(ctx, "SELECT id FROM employees WHERE #{where} ORDER BY id")
    List.flatten(rows)
  end
end
