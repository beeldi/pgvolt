defmodule Electric.Connection.ManagerJournalTailTest do
  # In journal mode the replication client acks the slot once a transaction is journaled, before
  # the shape log collector has it. When the client dies and the shape subsystem keeps running,
  # the transactions still queued for the collector are only in the journal: Postgres resumes
  # past them.

  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.ShapeCache
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes.Shape
  alias Electric.StatusMonitor

  @moduletag :tmp_dir
  @moduletag replication_opts_overrides: [slot_temporary?: false]

  setup [:with_unique_db, :with_basic_tables]

  setup do
    for {key, value} <- [ack_after_journal: true, replication_pipeline_window: 1] do
      previous = Application.get_env(:electric, key)
      Application.put_env(:electric, key, value)
      on_exit(fn -> Application.put_env(:electric, key, previous) end)
    end

    :ok
  end

  setup :with_complete_stack

  setup ctx do
    shape = Shape.new!("serial_ids", inspector: ctx.inspector)
    {handle, _offset} = ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id)
    :started = ShapeCache.await_snapshot_start(handle, ctx.stack_id)

    insert!(ctx, 1)
    assert wait_for_ids(ctx, handle, [1])

    %{handle: handle}
  end

  test "a reconnect delivers the journaled transactions the running collector had not processed",
       ctx do
    # Arrange
    collector = ShapeLogCollector.name(ctx.stack_id)
    :sys.suspend(collector)
    Enum.each(2..5, &insert!(ctx, &1))
    before_last = current_wal_lsn(ctx)
    insert!(ctx, 6)
    assert Support.TestUtils.wait_until(fn -> slot_confirmed_past?(ctx, before_last) end, 10_000)

    # Act
    kill_replication_client(ctx)
    :sys.resume(collector)
    insert!(ctx, 7)

    # Assert
    assert wait_for_ids(ctx, ctx.handle, Enum.to_list(1..7)),
           "the shape log holds #{inspect(stored_ids(ctx, ctx.handle))}"
  end

  test "a reconnect with nothing journaled past the collector keeps the shape subsystem running",
       ctx do
    # Arrange
    shapes_supervisor = GenServer.whereis(Electric.Shapes.Supervisor.name(ctx.stack_id))
    insert!(ctx, 2)
    assert wait_for_ids(ctx, ctx.handle, [1, 2])

    # Act
    kill_replication_client(ctx)
    insert!(ctx, 3)

    # Assert
    assert wait_for_ids(ctx, ctx.handle, [1, 2, 3])
    assert GenServer.whereis(Electric.Shapes.Supervisor.name(ctx.stack_id)) == shapes_supervisor
    # The kept collector replayed the journal already, so no replay is owed to the shapes.
    assert :ok = StatusMonitor.wait_until_active(ctx.stack_id, timeout: 10_000)
    assert StatusMonitor.journal_replay(ctx.stack_id) == :settled
  end

  defp insert!(ctx, id),
    do: Postgrex.query!(ctx.db_conn, "INSERT INTO serial_ids (id) VALUES ($1)", [id])

  defp current_wal_lsn(ctx) do
    %Postgrex.Result{rows: [[lsn]]} =
      Postgrex.query!(ctx.db_conn, "SELECT pg_current_wal_insert_lsn()", [])

    lsn
  end

  defp slot_confirmed_past?(ctx, lsn) do
    %Postgrex.Result{rows: [[confirmed]]} =
      Postgrex.query!(
        ctx.db_conn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
        ["electric_test_slot_#{:erlang.phash2(ctx.stack_id)}"]
      )

    confirmed != nil and Lsn.compare(confirmed, lsn) == :gt
  end

  defp kill_replication_client(ctx) do
    pid = GenServer.whereis(ReplicationClient.name(ctx.stack_id))
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
  end

  defp wait_for_ids(ctx, handle, ids) do
    expected = Enum.map(ids, &to_string/1)
    Support.TestUtils.wait_until(fn -> stored_ids(ctx, handle) == expected end, 15_000)
  end

  defp stored_ids(ctx, handle) do
    storage = Storage.for_shape(handle, Storage.shared_opts(ctx.storage))

    LogOffset.last_before_real_offsets()
    |> Storage.get_log_stream(storage)
    |> Enum.map(&get_in(Jason.decode!(&1), ["value", "id"]))
  end
end
