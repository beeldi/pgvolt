defmodule Electric.Integration.SubqueryNumericKeyTest do
  @moduledoc """
  A subquery compares `numeric` and `numeric[]` keys as Postgres does: exactly, and with `NaN` equal
  to `NaN`.

  Each test first asks Postgres which employees the where clause selects, then checks that a live
  client receives exactly those rows, with no `must-refetch` in between.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer

  alias Electric.Client
  alias Electric.Client.Message.ControlMessage
  alias Electric.Client.ShapeDefinition

  @moduletag :tmp_dir
  @moduletag with_sql: [
               ~s|CREATE TABLE divisions (id text PRIMARY KEY, "firmId" int NOT NULL, code numeric, codes numeric[])|,
               ~s|CREATE TABLE employees (id text PRIMARY KEY, "firmId" int NOT NULL, "divisionCode" numeric, "divisionCodes" numeric[])|,
               ~s|CREATE TABLE firms (id int PRIMARY KEY)|
             ]

  @composite ~s|("divisionCode", "firmId") IN (SELECT code, "firmId" FROM divisions)|
  @single ~s|"divisionCode" IN (SELECT code FROM divisions)|
  @array_composite ~s|("divisionCodes", "firmId") IN (SELECT codes, "firmId" FROM divisions)|
  @array_single ~s|"divisionCodes" IN (SELECT codes FROM divisions)|

  setup [:with_unique_db, :with_sql_execute, :with_complete_stack, :with_electric_client]

  for {kind, where} <- [composite: @composite, single: @single] do
    test "a NaN key moves employees into and out of a #{kind} shape", %{client: client} = ctx do
      where = unquote(where)
      query!(ctx, "INSERT INTO employees VALUES ('nan', 1, 'NaN'), ('one', 1, 1)")

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_up_to_date(consumer)

        query!(ctx, "INSERT INTO divisions VALUES ('d1', 1, 'NaN')")
        assert selected(ctx, where) == ["nan"]
        assert_insert(consumer, %{"id" => "nan"})
        assert_up_to_date(consumer)

        query!(ctx, "INSERT INTO employees VALUES ('nan-live', 1, 'NaN'), ('two', 1, 2)")
        assert selected(ctx, where) == ["nan", "nan-live"]
        assert_insert(consumer, %{"id" => "nan-live"})
        assert_up_to_date(consumer)

        query!(ctx, "DELETE FROM divisions WHERE id = 'd1'")
        assert selected(ctx, where) == []

        assert {:ok, deletes} =
                 await_count(consumer, 2, match: &match?(%{headers: %{operation: :delete}}, &1))

        assert deletes |> Enum.map(& &1.value["id"]) |> Enum.sort() == ["nan", "nan-live"]
      end
    end
  end

  # 9007199254740993 is the first integer a float64 cannot hold: it reads as 9007199254740992, and
  # 9007199254740995 as 9007199254740996.
  for {kind, where} <- [composite: @composite, single: @single] do
    test "a #{kind} key past float precision matches only its exact value",
         %{client: client} = ctx do
      where = unquote(where)
      query!(ctx, "INSERT INTO divisions VALUES ('d1', 1, 9007199254740993)")

      query!(ctx, """
      INSERT INTO employees VALUES
        ('snapshot-below', 1, 9007199254740992), ('snapshot-exact', 1, 9007199254740993.0),
        ('move-in-near', 1, 9007199254740996), ('move-in-exact', 1, 9007199254740995.00)
      """)

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert selected(ctx, where) == ["snapshot-exact"]
        assert_insert(consumer, %{"id" => "snapshot-exact"})
        assert_up_to_date(consumer)

        query!(ctx, """
        INSERT INTO employees VALUES
          ('live-below', 1, 9007199254740992), ('live-exact', 1, 9007199254740993.00)
        """)

        assert selected(ctx, where) == ["live-exact", "snapshot-exact"]
        assert_insert(consumer, %{"id" => "live-exact"})
        assert_up_to_date(consumer)

        query!(ctx, "INSERT INTO divisions VALUES ('d2', 1, 9007199254740995)")
        assert selected(ctx, where) == ["live-exact", "move-in-exact", "snapshot-exact"]
        assert_insert(consumer, %{"id" => "move-in-exact"})
        assert_up_to_date(consumer)
      end
    end

    test "a negated #{kind} key past float precision admits its neighbour",
         %{client: client} = ctx do
      where = "NOT (#{unquote(where)})"
      query!(ctx, "INSERT INTO divisions VALUES ('d1', 1, 9007199254740993)")

      query!(ctx, """
      INSERT INTO employees VALUES
        ('snapshot-exact', 1, 9007199254740993.0), ('snapshot-below', 1, 9007199254740992)
      """)

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert selected(ctx, where) == ["snapshot-below"]
        assert_insert(consumer, %{"id" => "snapshot-below"})
        assert_up_to_date(consumer)

        query!(ctx, """
        INSERT INTO employees VALUES
          ('live-exact', 1, 9007199254740993.00), ('live-below', 1, 9007199254740992)
        """)

        assert selected(ctx, where) == ["live-below", "snapshot-below"]
        assert_insert(consumer, %{"id" => "live-below"})
        assert_up_to_date(consumer)
      end
    end
  end

  # The same values as one-element arrays, with and without lower bounds.
  for {kind, where} <- [composite: @array_composite, single: @array_single],
      prefix <- ["", "[0:0]="] do
    test "a #{kind} #{prefix}{...} key past float precision matches only its exact value",
         %{client: client} = ctx do
      where = unquote(where)
      prefix = unquote(prefix)
      insert_division!(ctx, "d1", "#{prefix}{9007199254740993}")

      insert_employees!(ctx,
        "snapshot-below": "#{prefix}{9007199254740992}",
        "snapshot-exact": "#{prefix}{9007199254740993.0}",
        "move-in-near": "#{prefix}{9007199254740996}",
        "move-in-exact": "#{prefix}{9007199254740995.00}"
      )

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert selected(ctx, where) == ["snapshot-exact"]
        assert_insert(consumer, %{"id" => "snapshot-exact"})
        assert_up_to_date(consumer)

        insert_employees!(ctx,
          "live-below": "#{prefix}{9007199254740992}",
          "live-exact": "#{prefix}{9007199254740993.00}"
        )

        assert selected(ctx, where) == ["live-exact", "snapshot-exact"]
        assert_insert(consumer, %{"id" => "live-exact"})
        assert_up_to_date(consumer)

        insert_division!(ctx, "d2", "#{prefix}{9007199254740995}")
        assert selected(ctx, where) == ["live-exact", "move-in-exact", "snapshot-exact"]
        assert_insert(consumer, %{"id" => "move-in-exact"})
        assert_up_to_date(consumer)
      end
    end

    test "a negated #{kind} #{prefix}{...} key past float precision admits its neighbour",
         %{client: client} = ctx do
      where = "NOT (#{unquote(where)})"
      prefix = unquote(prefix)
      insert_division!(ctx, "d1", "#{prefix}{9007199254740993}")

      insert_employees!(ctx,
        "snapshot-exact": "#{prefix}{9007199254740993.0}",
        "snapshot-below": "#{prefix}{9007199254740992}"
      )

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert selected(ctx, where) == ["snapshot-below"]
        assert_insert(consumer, %{"id" => "snapshot-below"})
        assert_up_to_date(consumer)

        insert_employees!(ctx,
          "live-exact": "#{prefix}{9007199254740993.00}",
          "live-below": "#{prefix}{9007199254740992}"
        )

        assert selected(ctx, where) == ["live-below", "snapshot-below"]
        assert_insert(consumer, %{"id" => "live-below"})
        assert_up_to_date(consumer)
      end
    end
  end

  # Postgres writes a numeric with the scale it was given, so keys it calls equal can have
  # different text.
  for {kind, where, parent, child} <- [
        {"composite", @composite, {"code", "1.0"}, {"divisionCode", ["1.00", "1"]}},
        {"single", @single, {"code", "1.0"}, {"divisionCode", ["1.00", "1"]}},
        {"composite numeric[]", @array_composite, {"codes", "{1.0,2}"},
         {"divisionCodes", ["{1.00,2.0}", "{1,2.000}"]}},
        {"single numeric[]", @array_single, {"codes", "{1.0,2}"},
         {"divisionCodes", ["{1.00,2.0}", "{1,2.000}"]}}
      ] do
    test "removing a #{kind} key moves out employees whose key has another scale",
         %{client: client} = ctx do
      where = unquote(where)
      {parent_column, parent_key} = unquote(parent)
      {child_column, [moved_in_key, live_key]} = unquote(child)

      query!(
        ctx,
        ~s|INSERT INTO employees (id, "firmId", "#{child_column}") VALUES ('moved-in', 1, '#{moved_in_key}')|
      )

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_up_to_date(consumer)

        query!(
          ctx,
          ~s|INSERT INTO divisions (id, "firmId", #{parent_column}) VALUES ('d1', 1, '#{parent_key}')|
        )

        assert selected(ctx, where) == ["moved-in"]
        assert_insert(consumer, %{"id" => "moved-in"})
        assert_up_to_date(consumer)

        query!(
          ctx,
          ~s|INSERT INTO employees (id, "firmId", "#{child_column}") VALUES ('live', 1, '#{live_key}')|
        )

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

  # A move-out reads its tags at a snapshot taken after its own transaction, so that snapshot
  # already holds the rows the transaction wrote.
  for {kind, where, divisions, move} <- [
        {"composite", @composite, "('d1', 1, 1.0), ('d2', 1, 2)",
         "DELETE FROM divisions WHERE id = 'd1'"},
        {"single", @single, "('d1', 1, 1.0), ('d2', 1, 2)",
         "DELETE FROM divisions WHERE id = 'd1'"},
        {"negated single", "NOT (#{@single})", "('d2', 1, 3)",
         "INSERT INTO divisions VALUES ('d1', 1, 1.0)"}
      ] do
    test "a #{kind} key that leaves sends the employees its transaction writes as Postgres selects them",
         %{client: client} = ctx do
      where = unquote(where)
      query!(ctx, "INSERT INTO divisions VALUES #{unquote(divisions)}")

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_up_to_date(consumer)

        # The client holds none of these employees: each arrives with the key that leaves, and
        # the same transaction then moves one to a kept key and deletes another.
        assert {:ok, _} =
                 Postgrex.transaction(ctx.db_conn, fn conn ->
                   Postgrex.query!(conn, unquote(move), [])

                   Postgrex.query!(
                     conn,
                     "INSERT INTO employees VALUES ('same', 1, 1.0), ('moved', 1, 1.0), ('gone', 1, 1.0)",
                     []
                   )

                   Postgrex.query!(
                     conn,
                     ~s|UPDATE employees SET "divisionCode" = 2 WHERE id = 'moved'|,
                     []
                   )

                   Postgrex.query!(conn, "DELETE FROM employees WHERE id = 'gone'", [])
                 end)

        query!(ctx, "INSERT INTO employees VALUES ('after', 1, 2)")
        assert selected(ctx, where) == ["after", "moved"]

        assert {:ok, [moved, later]} =
                 await_count(consumer, 2,
                   match:
                     &(match?(%{headers: %{operation: _}}, &1) or
                         match?(%ControlMessage{control: :must_refetch}, &1))
                 )

        assert %{headers: %{operation: :insert}, value: %{"id" => "moved", "firmId" => 1}} =
                 moved

        assert %{headers: %{operation: :insert}, value: %{"id" => "after"}} = later
      end
    end
  end

  # Under a deferred primary key, a transaction can insert a second employee under the key of one
  # the client holds, and then delete both.
  test "a key that leaves still deletes an employee whose key a row of its transaction takes",
       %{client: client} = ctx do
    query!(
      ctx,
      "ALTER TABLE employees DROP CONSTRAINT employees_pkey, ADD PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED"
    )

    query!(ctx, "INSERT INTO divisions VALUES ('d1', 1, 1.0), ('d2', 1, 2)")
    query!(ctx, "INSERT INTO employees VALUES ('held', 1, 1.0)")

    with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: @single),
                    live: true
                  ) do
      assert_insert(consumer, %{"id" => "held"})
      assert_up_to_date(consumer)

      assert {:ok, _} =
               Postgrex.transaction(ctx.db_conn, fn conn ->
                 Postgrex.query!(conn, "DELETE FROM divisions WHERE id = 'd1'", [])
                 Postgrex.query!(conn, "INSERT INTO employees VALUES ('held', 1, 1.00)", [])
                 Postgrex.query!(conn, "DELETE FROM employees WHERE id = 'held'", [])
               end)

      query!(ctx, "INSERT INTO employees VALUES ('after', 1, 2)")
      assert selected(ctx, @single) == ["after"]

      assert {:ok, [held, later]} =
               await_count(consumer, 2,
                 match:
                   &(match?(%{headers: %{operation: _}}, &1) or
                       match?(%ControlMessage{control: :must_refetch}, &1))
               )

      assert %{headers: %{operation: :delete}, value: %{"id" => "held"}} = held
      assert %{headers: %{operation: :insert}, value: %{"id" => "after"}} = later
    end
  end

  for {kind, column, parent_column, parent_key, child_key} <- [
        {"single", "divisionCode", "code", "1.0", "1.00"},
        {"single numeric[]", "divisionCodes", "codes", "{1.0,2}", "{1.00,2.0}"}
      ] do
    test "adding a negated #{kind} key moves out employees whose key has another scale",
         %{client: client} = ctx do
      where = ~s|"#{unquote(column)}" NOT IN (SELECT #{unquote(parent_column)} FROM divisions)|
      child_key = unquote(child_key)

      query!(
        ctx,
        ~s|INSERT INTO employees (id, "firmId", "#{unquote(column)}") VALUES ('held', 1, '#{child_key}')|
      )

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_insert(consumer, %{"id" => "held"})
        assert_up_to_date(consumer)

        query!(
          ctx,
          ~s|INSERT INTO divisions (id, "firmId", #{unquote(parent_column)}) VALUES ('d1', 1, '#{unquote(parent_key)}')|
        )

        assert selected(ctx, where) == []
        assert_delete(consumer, %{"id" => "held"})
      end
    end

    test "adding a #{kind} key reaches employees held through another disjunct at another scale",
         %{client: client} = ctx do
      where =
        ~s|"#{unquote(column)}" IN (SELECT #{unquote(parent_column)} FROM divisions) OR "firmId" IN (SELECT id FROM firms)|

      child_key = unquote(child_key)
      query!(ctx, "INSERT INTO firms VALUES (2)")

      query!(
        ctx,
        ~s|INSERT INTO employees (id, "firmId", "#{unquote(column)}") VALUES ('held', 2, '#{child_key}'), ('joining', 1, '#{child_key}')|
      )

      with_consumer Client.stream(client, ShapeDefinition.new!("employees", where: where),
                      live: true
                    ) do
        assert_insert(consumer, %{"id" => "held"})
        assert_up_to_date(consumer)

        query!(
          ctx,
          ~s|INSERT INTO divisions (id, "firmId", #{unquote(parent_column)}) VALUES ('d1', 1, '#{unquote(parent_key)}')|
        )

        # The move-in has spliced once the employee it brings in arrives, so the firm's removal
        # applies at once instead of waiting behind it.
        assert_insert(consumer, %{"id" => "joining"})

        query!(ctx, "DELETE FROM firms WHERE id = 2")
        assert selected(ctx, where) == ["held", "joining"]

        query!(
          ctx,
          ~s|INSERT INTO employees (id, "firmId", "#{unquote(column)}") VALUES ('after', 1, '#{child_key}')|
        )

        assert selected(ctx, where) == ["after", "held", "joining"]

        # The removed firm must not delete an employee its division still selects.
        assert {:ok, [change]} =
                 await_count(consumer, 1,
                   match: &match?(%{headers: %{operation: op}} when op in [:insert, :delete], &1)
                 )

        assert %{headers: %{operation: :insert}, value: %{"id" => "after"}} = change
      end
    end
  end

  defp insert_division!(ctx, id, codes) do
    query!(ctx, ~s|INSERT INTO divisions (id, "firmId", codes) VALUES ('#{id}', 1, '#{codes}')|)
  end

  defp insert_employees!(ctx, rows) do
    values = Enum.map_join(rows, ", ", fn {id, codes} -> "('#{id}', 1, '#{codes}')" end)
    query!(ctx, ~s|INSERT INTO employees (id, "firmId", "divisionCodes") VALUES #{values}|)
  end

  defp query!(ctx, sql), do: Postgrex.query!(ctx.db_conn, sql, [])

  defp selected(ctx, where) do
    %{rows: rows} = query!(ctx, "SELECT id FROM employees WHERE #{where} ORDER BY id")
    List.flatten(rows)
  end
end
