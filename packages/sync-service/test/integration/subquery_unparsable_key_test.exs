defmodule Electric.Integration.SubqueryUnparsableKeyTest do
  @moduledoc """
  A subquery's column can hold values Postgres stores but the evaluator's parse refuses: a float's
  `NaN` and infinities, a date or a timestamp at infinity or before the common era, a time of
  `24:00:00`, an interval at infinity. A shape over such a subquery serves them as Postgres
  selects them, in the snapshot, live, on a move-out and on a move-in.

  A deferrable primary key lets one transaction give a division a key another division still
  holds. The service cannot follow the subquery's values through that, so the shape is
  invalidated and its client refetches a snapshot that matches Postgres.

  Each step first asks Postgres which employees the where clause selects.
  """
  use ExUnit.Case, async: false

  # The client's default mapper parses a float's text into an Elixir float, which has no `NaN` or
  # infinity. The tests read only the `id`, so the values stay text.
  defmodule TextValues do
    @behaviour Electric.Client.ValueMapper

    @impl true
    def for_schema(_schema, _opts), do: & &1
  end

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer

  alias Electric.Client
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.Message.ControlMessage
  alias Electric.Client.ShapeDefinition
  alias Support.StreamConsumer

  @moduletag :tmp_dir
  @moduletag with_sql: [
               """
               CREATE TABLE divisions (
                 id text PRIMARY KEY DEFERRABLE INITIALLY DEFERRED,
                 "firmId" int NOT NULL,
                 rate float8,
                 rates float8[],
                 "openedOn" date,
                 "openedAt" timestamp,
                 "openedAtTz" timestamptz,
                 "opensAt" time,
                 term interval
               )
               """,
               """
               CREATE TABLE employees (
                 id text PRIMARY KEY,
                 "firmId" int NOT NULL,
                 "divisionRate" float8,
                 "divisionRates" float8[],
                 "divisionOpenedOn" date,
                 "divisionOpenedAt" timestamp,
                 "divisionOpenedAtTz" timestamptz,
                 "divisionOpensAt" time,
                 "divisionTerm" interval,
                 "divisionId" text
               )
               """
             ]

  setup [:with_unique_db, :with_sql_execute, :with_complete_stack, :with_electric_client]

  # {division column, employee column, a value the evaluator refuses, a value it parses}
  @cases [
    {"rate", "divisionRate", "NaN", "1.5"},
    {"rate", "divisionRate", "Infinity", "1.5"},
    {"rate", "divisionRate", "-Infinity", "1.5"},
    {"rates", "divisionRates", "{NaN,1}", "{1,2}"},
    {"openedOn", "divisionOpenedOn", "infinity", "2026-01-05"},
    {"openedOn", "divisionOpenedOn", "-infinity", "2026-01-05"},
    {"openedOn", "divisionOpenedOn", "0044-03-15 BC", "2026-01-05"},
    {"openedAt", "divisionOpenedAt", "infinity", "2026-01-05 09:00:00"},
    {"openedAt", "divisionOpenedAt", "0044-03-15 09:00:00 BC", "2026-01-05 09:00:00"},
    {"openedAtTz", "divisionOpenedAtTz", "-infinity", "2026-01-05 09:00:00+00"},
    {"opensAt", "divisionOpensAt", "24:00:00", "09:00:00"}
  ]

  @composite_cases [
    {"rate", "divisionRate", "NaN", "1.5"},
    {"rates", "divisionRates", "{NaN,1}", "{1,2}"},
    {"openedOn", "divisionOpenedOn", "infinity", "2026-01-05"},
    {"opensAt", "divisionOpensAt", "24:00:00", "09:00:00"}
  ]

  for {column, employee_column, value, plain} <- @cases do
    test "a single key #{value} over #{column} follows Postgres", ctx do
      where = ~s|"#{unquote(employee_column)}" IN (SELECT #{q(unquote(column))} FROM divisions)|

      follow_moves(
        ctx,
        where,
        unquote(column),
        unquote(employee_column),
        unquote(value),
        unquote(plain)
      )
    end
  end

  # `"firmId"` comes first: the columns of a composite subquery pair with the row on its left in name
  # order.
  for {column, employee_column, value, plain} <- @composite_cases do
    test "a composite key #{value} over #{column} follows Postgres", ctx do
      where =
        ~s|("firmId", "#{unquote(employee_column)}") IN (SELECT "firmId", #{q(unquote(column))} FROM divisions)|

      follow_moves(
        ctx,
        where,
        unquote(column),
        unquote(employee_column),
        unquote(value),
        unquote(plain)
      )
    end
  end

  # Postgres 17 added an interval at infinity.
  test "a single key infinity over an interval follows Postgres", ctx do
    if Support.TestUtils.fetch_pg_version(ctx.db_conn) >= 170_000 do
      where = ~s|"divisionTerm" IN (SELECT term FROM divisions)|
      follow_moves(ctx, where, "term", "divisionTerm", "infinity", "1 day")
    else
      assert {:error, _} = Postgrex.query(ctx.db_conn, "SELECT 'infinity'::interval", [])
    end
  end

  # Postgres calls `0` and `-0` equal, so a division leaving with one of them leaves the other's
  # employees in the shape.
  test "a signed zero beside a NaN in an array key follows Postgres", ctx do
    where = ~s|"divisionRates" IN (SELECT rates FROM divisions)|

    query!(
      ctx,
      ~s|INSERT INTO divisions (id, "firmId", rates) VALUES ('d-zero', 1, '{NaN,0}'), ('d-negative-zero', 1, '{NaN,-0}')|
    )

    query!(ctx, """
    INSERT INTO employees (id, "firmId", "divisionRates")
    VALUES ('zero', 1, '{NaN,0}'), ('negative-zero', 1, '{NaN,-0}')
    """)

    with_consumer Client.stream(
                    ctx.client,
                    ShapeDefinition.new!("employees", where: where, parser: {TextValues, []}),
                    live: true
                  ) do
      snapshot = selected(ctx, where)
      assert snapshot == ["negative-zero", "zero"]
      assert ids(next_changes(consumer, 2), :insert) == snapshot
      assert_up_to_date(consumer)

      query!(ctx, "DELETE FROM divisions WHERE id = 'd-negative-zero'")
      assert selected(ctx, where) == snapshot

      query!(
        ctx,
        ~s|INSERT INTO employees (id, "firmId", "divisionRates") VALUES ('later', 1, '{NaN,0}')|
      )

      assert selected(ctx, where) == ["later" | snapshot]
      assert ids(next_changes(consumer, 1), :insert) == ["later"]
    end
  end

  # A float key's move-out names its rows by a query, so the employees of both spellings leave
  # with the last division that holds the value, whichever spelling that division has.
  for {divisions, gone} <- [
        {["d-zero", "d-negative-zero"], "both divisions go"},
        {["d-zero"], "its sole division goes"}
      ] do
    test "a float zero beside a NaN leaves under either spelling when #{gone}", ctx do
      where = ~s|"divisionRates" IN (SELECT rates FROM divisions)|
      rates = %{"d-zero" => "{NaN,0}", "d-negative-zero" => "{NaN,-0}"}

      for id <- unquote(divisions) do
        query!(
          ctx,
          ~s|INSERT INTO divisions (id, "firmId", rates) VALUES ('#{id}', 1, '#{rates[id]}')|
        )
      end

      query!(ctx, """
      INSERT INTO employees (id, "firmId", "divisionRates")
      VALUES ('zero', 1, '{NaN,0}'), ('negative-zero', 1, '{NaN,-0}')
      """)

      with_consumer Client.stream(
                      ctx.client,
                      ShapeDefinition.new!("employees", where: where, parser: {TextValues, []}),
                      live: true
                    ) do
        snapshot = selected(ctx, where)
        assert snapshot == ["negative-zero", "zero"]
        assert ids(next_changes(consumer, 2), :insert) == snapshot
        assert_up_to_date(consumer)

        query!(ctx, "DELETE FROM divisions")
        assert selected(ctx, where) == []
        assert ids(next_changes(consumer, 2), :delete) == snapshot
      end
    end
  end

  test "a division key reused within one transaction invalidates the shape", ctx do
    where = ~s|"divisionOpenedOn" IN (SELECT "openedOn" FROM divisions)|

    query!(
      ctx,
      ~s|INSERT INTO divisions (id, "firmId", "openedOn") VALUES ('d1', 1, '2026-01-05')|
    )

    query!(ctx, """
    INSERT INTO employees (id, "firmId", "divisionOpenedOn")
    VALUES ('january', 1, '2026-01-05'), ('february', 1, '2026-02-01')
    """)

    with_consumer Client.stream(
                    ctx.client,
                    ShapeDefinition.new!("employees", where: where, parser: {TextValues, []}),
                    live: true
                  ) do
      assert_insert(consumer, %{"id" => "january"})
      assert_up_to_date(consumer)

      # The new division takes d1's key before the old one leaves: Postgres checks it at commit.
      query!(ctx, """
      DO $$ BEGIN
        INSERT INTO divisions (id, "firmId", "openedOn") VALUES ('d1', 1, '2026-02-01');
        DELETE FROM divisions WHERE id = 'd1' AND "openedOn" = '2026-01-05';
      END $$
      """)

      assert selected(ctx, where) == ["february"]

      assert {:ok, [%ControlMessage{control: :must_refetch}]} =
               await_count(consumer, 1,
                 match: &match?(%ControlMessage{control: :must_refetch}, &1)
               )

      assert {:ok, [%ChangeMessage{value: %{"id" => "february"}}]} =
               await_count(consumer, 1, match: &match?(%ChangeMessage{}, &1))

      assert_up_to_date(consumer)

      query!(
        ctx,
        ~s|INSERT INTO employees (id, "firmId", "divisionOpenedOn") VALUES ('later', 1, '2026-02-01')|
      )

      assert selected(ctx, where) == ["february", "later"]
      assert_insert(consumer, %{"id" => "later"})
    end
  end

  # A client of the divisions reads the log the subquery's view is built from when its shape is the
  # dependency's: the subquery selects the key, and the client asks for that column alone.
  test "a division key reused within one transaction wakes no client of the shape the subquery reads",
       ctx do
    where = ~s|"divisionId" IN (SELECT id FROM divisions)|

    query!(
      ctx,
      ~s|INSERT INTO divisions (id, "firmId", "openedOn") VALUES ('d1', 1, '2026-01-05')|
    )

    query!(
      ctx,
      ~s|INSERT INTO employees (id, "firmId", "divisionId") VALUES ('january', 1, 'd1')|
    )

    {:ok, employees} =
      StreamConsumer.start(
        Client.stream(
          ctx.client,
          ShapeDefinition.new!("employees", where: where, parser: {TextValues, []}),
          live: true
        )
      )

    {:ok, divisions} =
      StreamConsumer.start(
        Client.stream(
          ctx.client,
          ShapeDefinition.new!("divisions", columns: ["id"], parser: {TextValues, []}),
          live: true
        )
      )

    try do
      assert %ChangeMessage{headers: %{handle: employees_handle}} =
               assert_insert(employees, %{"id" => "january"})

      assert_up_to_date(employees)

      assert %ChangeMessage{headers: %{handle: divisions_handle}} =
               assert_insert(divisions, %{"id" => "d1"})

      assert_up_to_date(divisions)

      assert {:ok, %{shape_dependencies_handles: [^divisions_handle]}} =
               Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, employees_handle)

      query!(ctx, """
      DO $$ BEGIN
        INSERT INTO divisions (id, "firmId", "openedOn") VALUES ('d1', 1, '2026-02-01');
        DELETE FROM divisions WHERE id = 'd1' AND "openedOn" = '2026-01-05';
      END $$
      """)

      # Read as a log, the transaction inserts d1 again and then deletes it, so a client that read
      # it would drop the division Postgres kept. The client refetches instead.
      assert [%ControlMessage{control: :must_refetch}] = next_changes(divisions, 1)

      assert {:ok, [%ChangeMessage{value: %{"id" => "d1"}}]} =
               await_count(divisions, 1, match: &match?(%ChangeMessage{}, &1))
    after
      StreamConsumer.stop(employees)
      StreamConsumer.stop(divisions)
    end
  end

  # The divisions hold the value from the start, so the subquery's view reads it from the
  # dependency's snapshot. Then an employee with the value arrives live, the value leaves the
  # subquery and comes back.
  defp follow_moves(ctx, where, column, employee_column, value, plain) do
    query!(
      ctx,
      ~s|INSERT INTO divisions (id, "firmId", #{q(column)}) VALUES ('d-value', 1, '#{value}'), ('d-plain', 1, '#{plain}')|
    )

    query!(ctx, """
    INSERT INTO employees (id, "firmId", "#{employee_column}")
    VALUES ('value', 1, '#{value}'), ('plain', 1, '#{plain}'), ('other-firm', 2, '#{value}')
    """)

    with_consumer Client.stream(
                    ctx.client,
                    ShapeDefinition.new!("employees", where: where, parser: {TextValues, []}),
                    live: true
                  ) do
      snapshot = selected(ctx, where)
      assert "value" in snapshot
      assert ids(next_changes(consumer, length(snapshot)), :insert) == snapshot
      assert_up_to_date(consumer)

      query!(
        ctx,
        ~s|INSERT INTO employees (id, "firmId", "#{employee_column}") VALUES ('value-live', 1, '#{value}')|
      )

      assert selected(ctx, where) == Enum.sort(["value-live" | snapshot])
      assert ids(next_changes(consumer, 1), :insert) == ["value-live"]
      assert_up_to_date(consumer)

      held = selected(ctx, where)
      query!(ctx, "DELETE FROM divisions WHERE id = 'd-value'")
      left = held -- selected(ctx, where)
      assert "value-live" in left
      assert ids(next_changes(consumer, length(left)), :delete) == left

      query!(
        ctx,
        ~s|INSERT INTO divisions (id, "firmId", #{q(column)}) VALUES ('d-value', 1, '#{value}')|
      )

      assert selected(ctx, where) == held
      assert ids(next_changes(consumer, length(left)), :insert) == left
    end
  end

  # The next `count` changes, or a must-refetch among them, which fails the step that awaits them.
  defp next_changes(consumer, count) do
    assert {:ok, messages} =
             await_count(consumer, count,
               match:
                 &(match?(%ChangeMessage{}, &1) or
                     match?(%ControlMessage{control: :must_refetch}, &1))
             )

    messages
  end

  defp ids(messages, operation) do
    messages
    |> Enum.map(fn %ChangeMessage{headers: %{operation: ^operation}, value: %{"id" => id}} ->
      id
    end)
    |> Enum.sort()
  end

  defp q(column) when column in ["rate", "rates", "term"], do: column
  defp q(column), do: ~s|"#{column}"|

  defp query!(ctx, sql), do: Postgrex.query!(ctx.db_conn, sql, [])

  defp selected(ctx, where) do
    %{rows: rows} = query!(ctx, "SELECT id FROM employees WHERE #{where}")
    rows |> List.flatten() |> Enum.sort()
  end
end
