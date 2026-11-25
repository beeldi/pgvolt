defmodule Electric.Postgres.SnapshotQueryTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  import Support.DbSetup

  alias Electric.Postgres.SnapshotQuery
  alias Electric.Shapes.Shape

  # Settings that differ from every display setting Electric relies on, so the snapshot query
  # has to override each of them.
  @moduletag database_settings: [
               "bytea_output = 'escape'",
               "DateStyle = 'SQL, MDY'",
               "TimeZone = 'America/New_York'",
               "extra_float_digits = 0",
               "IntervalStyle = 'postgres'"
             ]

  setup :with_unique_db

  @shape %Shape{root_table: {"public", "items"}, root_table_id: 1}

  test "runs the query with Electric's display settings and reports the snapshot", ctx do
    test_pid = self()

    result =
      SnapshotQuery.execute_for_shape(ctx.pool, "handle", @shape,
        stack_id: "stack",
        snapshot_info_fn: fn handle, pg_snapshot, lsn ->
          send(test_pid, {:snapshot_info, handle, pg_snapshot, lsn})
        end,
        query_fn: fn conn, _pg_snapshot, _lsn ->
          for setting <-
                ~w(bytea_output DateStyle TimeZone extra_float_digits IntervalStyle transaction_isolation transaction_read_only) do
            %{rows: [[value]]} = Postgrex.query!(conn, "SHOW #{setting}", [])
            {setting, value}
          end
        end
      )

    assert {:ok,
            [
              {"bytea_output", "hex"},
              {"DateStyle", "ISO, DMY"},
              {"TimeZone", "UTC"},
              {"extra_float_digits", "1"},
              {"IntervalStyle", "iso_8601"},
              {"transaction_isolation", "repeatable read"},
              {"transaction_read_only", "on"}
            ]} = result

    assert_received {:snapshot_info, "handle", {xmin, xmax, xip_list}, lsn}
    assert is_integer(xmin) and is_integer(xmax) and is_list(xip_list)
    assert is_integer(lsn) or is_struct(lsn)
  end

  test "sets up the snapshot in two statements before the query", ctx do
    test_pid = self()

    Repatch.patch(Postgrex, :query!, fn conn, statement, params ->
      send(test_pid, {:statement, statement})
      Repatch.real(Postgrex.query!(conn, statement, params))
    end)

    {:ok, :done} =
      SnapshotQuery.execute_for_shape(ctx.pool, "handle", @shape,
        stack_id: "stack",
        snapshot_info_fn: fn _, _, _ -> :ok end,
        query_fn: fn _conn, _pg_snapshot, _lsn -> :done end
      )

    assert_received {:statement, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"}
    assert_received {:statement, "SELECT pg_current_snapshot(), pg_current_wal_lsn(), " <> _}
    refute_received {:statement, _}
  end
end
