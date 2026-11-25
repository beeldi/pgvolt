defmodule Electric.Replication.AckAfterJournalWriterReaderTest do
  # The two halves of ELECTRIC_ACK_AFTER_JOURNAL only ever meet on disk: the replication
  # client writes and fsyncs the transaction journal before acking the slot, and a freshly
  # booted ShapeLogCollector replays that journal into the fan-out. Both run here over one
  # journal directory, written by real Postgres transactions and read back by a real
  # collector — nothing between the writer and the reader is stubbed.
  #
  # It was written to close a coverage gap and immediately failed: the slot position and the
  # journal boundary were compared in different units. The client acks `flushed_wal + 1`,
  # Postgres's next-byte-expected convention, so every restart with a non-empty journal read
  # the slot as one past the journal's last commit and refused to boot. Journaled relation
  # messages, which Postgres streams before the first transaction, then crashed the replay on
  # a nil processed offset. Neither was reachable from tests that hand-write the journal and
  # hand-set the tracker, which is what every other test of this feature does.

  use ExUnit.Case, async: false
  use Repatch.ExUnit, assert_expectations: true

  alias Electric.LsnTracker
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TxnJournal
  alias Electric.Shapes.Shape
  alias Electric.StatusMonitor

  alias Support.TransactionConsumer

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils, only: [register_as_replication_client: 1]

  @moduletag :tmp_dir

  @assert_receive_db_timeout 2000
  @shape_handle "journal-writer-reader-shape"

  defmodule ConnectionManagerStub do
    def receive_casts(test_pid) do
      receive do
        {:"$gen_cast", :replication_client_streamed_first_message} ->
          send(test_pid, :streaming_started)
          receive_casts(test_pid)

        _ ->
          receive_casts(test_pid)
      end
    end
  end

  # Journals through the client but never confirms a flush, so no shape ever becomes
  # durable and the journal stays the only record of these transactions.
  def stream_to_test(event, test_pid) do
    ref = make_ref()
    send(test_pid, {:streamed, event})
    send(self(), {ref, :ok})
    ref
  end

  setup do
    %{connection_manager: spawn_link(ConnectionManagerStub, :receive_casts, [self()])}
  end

  setup [
    :with_stack_id_from_test,
    :with_slot_name,
    :with_unique_db,
    :with_basic_tables,
    :with_pure_file_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_noop_publication_manager,
    :with_persistent_kv,
    :with_status_monitor
  ]

  setup ctx do
    %{stack_id: stack_id, db_conn: conn} = ctx

    Postgrex.query!(conn, "CREATE PUBLICATION #{ctx.slot_name} FOR TABLE serial_ids", [])

    %{rows: [[oid]]} = Postgrex.query!(conn, "SELECT 'serial_ids'::regclass::oid", [])

    inspector =
      Support.StubInspector.new(
        tables: [{oid, {"public", "serial_ids"}}],
        columns: [%{name: "id", type: "int8", pk_position: 0}]
      )

    start_link_supervised!(
      {Registry, keys: :duplicate, name: Module.concat(__MODULE__, Registry)}
    )

    Repatch.patch(Electric.ShapeCache.ShapeStatus, :list_shapes, [mode: :shared], fn ^stack_id ->
      []
    end)

    Support.TestUtils.activate_mocks_for_descendant_procs(ShapeLogCollector)

    parent = self()

    Repatch.patch(StatusMonitor, :mark_shape_log_collector_ready, [mode: :shared], fn _, _ ->
      send(parent, :shape_log_collector_ready)
      :ok
    end)

    {Electric.ShapeCache.PureFileStorage, %{base_path: journal_base}} =
      Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage)

    replication_opts = [
      connection_opts: ctx.db_config,
      stack_id: stack_id,
      publication_name: ctx.slot_name,
      try_creating_publication?: false,
      slot_name: ctx.slot_name,
      handle_event: {__MODULE__, :stream_to_test, [self()]},
      connection_manager: ctx.connection_manager,
      ack_after_journal: true
    ]

    %{
      inspector: inspector,
      shape: Shape.new!("serial_ids", inspector: inspector),
      journal_base: journal_base,
      replication_opts: replication_opts,
      collector_opts: [
        stack_id: stack_id,
        inspector: inspector,
        persistent_kv: ctx.persistent_kv,
        ack_after_journal: true
      ]
    }
  end

  test "a freshly booted collector replays the journal the replication client wrote", ctx do
    # Arrange: the client streams real transactions, journals them, and acks the slot at
    # that boundary. No consumer exists yet, so nothing is flushed.
    client = start_client(ctx)

    streamed =
      Enum.map(1..3, fn id ->
        Postgrex.query!(ctx.db_conn, "INSERT INTO serial_ids (id) VALUES ($1)", [id])

        assert_receive {:streamed, %TransactionFragment{commit: commit} = fragment}
                       when not is_nil(commit),
                       @assert_receive_db_timeout

        fragment
      end)

    last_commit = List.last(streamed).last_log_offset.tx_offset
    confirmed = await_slot_ack(ctx, last_commit)

    gens = TxnJournal.list_generations(ctx.journal_base)

    # A keepalive over the drained pipeline may have widened the boundary past the
    # last commit with an idle marker; it must never sit below what was acked.
    assert TxnJournal.last_commit_tx_offset(ctx.journal_base, gens) >= last_commit

    Process.unlink(client)
    stop_supervised!(ReplicationClient)

    # Act: a fresh collector boots over that journal, with the slot resuming where
    # Postgres recorded it — the same value the client seeds the tracker with at connect.
    LsnTracker.set_last_processed_lsn(ctx.stack_id, confirmed)

    # Unlinked: a collector that refuses to boot must surface as a failed assertion below,
    # not as an exit signal that takes the test process with it.
    start_supervised!({ShapeLogCollector.Supervisor, ctx.collector_opts})
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
    assert_receive :shape_log_collector_ready

    # Assert: every journaled transaction reaches the consumer through the replay.
    for expected <- streamed do
      assert_receive {TransactionConsumer, ^consumer, [replayed]}
      assert replayed.xid == expected.xid
      assert replayed.last_log_offset == expected.last_log_offset
    end
  end

  test "a mode-off boot replays the leftover journal and then deletes it", ctx do
    # Arrange: a mode-on client journals transactions and acks the slot at that
    # boundary, then dies — the journal outlives it.
    client = start_client(ctx)

    streamed =
      Enum.map(1..3, fn id ->
        Postgrex.query!(ctx.db_conn, "INSERT INTO serial_ids (id) VALUES ($1)", [id])

        assert_receive {:streamed, %TransactionFragment{commit: commit} = fragment}
                       when not is_nil(commit),
                       @assert_receive_db_timeout

        fragment
      end)

    last_commit = List.last(streamed).last_log_offset.tx_offset
    confirmed = await_slot_ack(ctx, last_commit)
    assert TxnJournal.list_generations(ctx.journal_base) != []

    Process.unlink(client)
    stop_supervised!(ReplicationClient)

    # Act: the next boot runs with the journal mode OFF (the flag was rolled back).
    # The leftover must still replay — and then be deleted once the consumers are
    # durable past its last commit: no writer runs in this mode, so nothing else
    # reclaims it, and deleting on delivery alone would leave a crash window with
    # the WAL gone from both Postgres and the journal.
    LsnTracker.set_last_processed_lsn(ctx.stack_id, confirmed)

    collector_opts = Keyword.put(ctx.collector_opts, :ack_after_journal, false)
    start_supervised!({ShapeLogCollector.Supervisor, collector_opts})
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
    assert_receive :shape_log_collector_ready

    # Assert: the replay delivered everything, and the journal survives delivery —
    # the consumer has not reported durability yet.
    for expected <- streamed do
      assert_receive {TransactionConsumer, ^consumer, [replayed]}
      assert replayed.xid == expected.xid
    end

    ShapeLogCollector.active_shapes(ctx.stack_id)
    assert TxnJournal.list_generations(ctx.journal_base) != []

    # Act: the consumer reports a flush through the replayed boundary.
    ShapeLogCollector.notify_flushed(
      ctx.stack_id,
      @shape_handle,
      List.last(streamed).last_log_offset
    )

    # Assert: durability past the last commit reclaims the journal.
    assert_receive {:flush_boundary_updated, ^last_commit}
    ShapeLogCollector.active_shapes(ctx.stack_id)
    assert TxnJournal.list_generations(ctx.journal_base) == []
  end

  defp start_client(ctx) do
    pid =
      start_link_supervised!(%{
        id: ReplicationClient,
        start:
          {ReplicationClient, :start_link,
           [[stack_id: ctx.stack_id, replication_opts: ctx.replication_opts, timeout: nil]]},
        restart: :temporary
      })

    assert_receive :streaming_started, @assert_receive_db_timeout
    pid
  end

  defp await_slot_ack(ctx, last_commit) do
    confirmed =
      Enum.reduce_while(1..100, nil, fn _, _ ->
        Process.sleep(50)
        confirmed = get_confirmed_flush_lsn(ctx)

        if Lsn.to_integer(confirmed) >= last_commit,
          do: {:halt, confirmed},
          else: {:cont, confirmed}
      end)

    assert Lsn.to_integer(confirmed) >= last_commit,
           "confirmed_flush_lsn #{confirmed} never reached the journaled commit at #{last_commit}"

    confirmed
  end

  defp get_confirmed_flush_lsn(ctx) do
    %Postgrex.Result{rows: [[confirmed_flush_lsn]]} =
      Postgrex.query!(
        ctx.db_conn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
        [ctx.slot_name]
      )

    confirmed_flush_lsn
  end

  defp start_consumer!(ctx, id) do
    pid =
      start_supervised!(
        %{
          id: {:consumer, id},
          start:
            {TransactionConsumer, :start_link,
             [
               [
                 id: id,
                 stack_id: ctx.stack_id,
                 parent: self(),
                 shape: ctx.shape,
                 shape_handle: @shape_handle
               ]
             ]},
          restart: :temporary
        },
        []
      )

    {id, pid}
  end

  defp await_shape_registered(ctx) do
    Support.TestUtils.wait_until(fn ->
      @shape_handle in ShapeLogCollector.active_shapes(ctx.stack_id)
    end)
  end
end
