defmodule Electric.Replication.JournalReplaySeenWalTest do
  # The last-seen feed promises subscribers that everything at or below the published
  # position has already been fanned out; a buffered subquery move-in splices against
  # exactly that promise. Boot-time journal replay is where the promise is hardest to
  # keep: the collector fans out transactions the replication client never dispatched,
  # so the client's own pipeline looks drained while consumers are still behind, and a
  # watermark surviving from before the crash predates the very fan-out gap the journal
  # records. This drives the real client, collector, journal and Postgres keepalives
  # through that window and asserts the feed never runs ahead of actual deliveries.

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

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  @moduletag :tmp_dir

  @assert_receive_db_timeout 2000
  @shape_handle "journal-replay-shape"
  @replay_txn_count 6

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

  # Journals through the client but never reaches a collector, so the journal is the only
  # record of these transactions — the same state a crash between journal-ack and fan-out
  # leaves behind, without depending on where the kill lands.
  def stream_to_test(event, test_pid) do
    ref = make_ref()
    send(test_pid, {:streamed, event})
    send(self(), {ref, :ok})
    ref
  end

  # A consumer that reports each delivered event to the test and then holds the collector
  # for `delay_ms`, widening the replay window from microseconds to hundreds of
  # milliseconds so the keepalive's arrival is not a coin flip.
  defmodule SlowConsumer do
    use GenServer, restart: :temporary

    def name(stack_id, shape_handle),
      do: Electric.Shapes.ConsumerRegistry.name(stack_id, shape_handle)

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts,
        name: name(Keyword.fetch!(opts, :stack_id), Keyword.fetch!(opts, :shape_handle))
      )
    end

    def init(opts) do
      Process.flag(:trap_exit, true)
      state = Map.new(opts)

      ShapeLogCollector.add_shape(state.stack_id, state.shape_handle, state.shape, :create)

      {:ok, state}
    end

    def handle_call({:handle_event, event, _ctx}, _from, state) do
      send(state.parent, {__MODULE__, self(), event, System.monotonic_time(:millisecond)})
      Process.sleep(state.delay_ms)
      {:reply, :ok, state}
    end

    def handle_info(_msg, state), do: {:noreply, state}

    def terminate(_reason, state) do
      ShapeLogCollector.remove_shape(state.stack_id, state.shape_handle)
    end
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

    # `items` stays out of the publication on purpose: committing to it is WAL the walsender
    # decodes and skips, which is what makes Postgres emit primary keepalives on demand.
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

  test "a keepalive decoded during boot-time journal replay never out-promises the fan-out",
       ctx do
    # Arrange: boot 1 journals real transactions and acks the slot at the journal boundary.
    # No collector runs, so nothing is fanned out — the journal is the only record.
    client = start_client(ctx)

    streamed =
      Enum.map(1..@replay_txn_count, fn id ->
        Postgrex.query!(ctx.db_conn, "INSERT INTO serial_ids (id) VALUES ($1)", [id])

        assert_receive {:streamed, %TransactionFragment{commit: commit} = fragment}
                       when not is_nil(commit),
                       @assert_receive_db_timeout

        fragment
      end)

    last_commit = List.last(streamed).last_log_offset.tx_offset
    confirmed = await_slot_ack(ctx, last_commit)

    gens = TxnJournal.list_generations(ctx.journal_base)

    # Idle keepalives may already have widened the coverage with durable markers past
    # the last streamed commit, and pg14 journals empty commits for unpublished-table
    # transactions; every streamed commit must be covered either way.
    assert TxnJournal.last_commit_tx_offset(ctx.journal_base, gens) >= last_commit

    assert gens
           |> Enum.map(&TxnJournal.generation_summary(ctx.journal_base, &1).last_commit)
           |> Enum.reject(&is_nil/1)
           |> Enum.max() >= last_commit

    Process.unlink(client)
    stop_supervised!(ReplicationClient)

    # Act: boot 2 brings up the collector over that journal with a consumer that holds each
    # replayed transaction, then starts the real replication client. Postgres keeps emitting
    # keepalives because the driver commits to a table outside the publication.
    LsnTracker.set_last_processed_lsn(ctx.stack_id, confirmed)

    start_supervised!({ShapeLogCollector.Supervisor, ctx.collector_opts})
    consumer = start_slow_consumer!(ctx, 120)
    await_shape_registered(ctx)

    _client2 = start_client(ctx)
    keepalive_driver = start_keepalive_driver(ctx)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    # Assert: at the moment the first replayed transaction reaches the consumer, the feed
    # must sit below every transaction still queued in the replay — a boot keepalive held
    # by the replay marker, and a boot-1 watermark dropped when the marker was armed.
    assert_receive {SlowConsumer, ^consumer, %TransactionFragment{} = first_replayed, first_at},
                   5_000

    broadcast_at_first_delivery = LsnTracker.get_last_broadcast_lsn(ctx.stack_id)

    undelivered =
      streamed
      |> Enum.drop_while(
        &(&1.last_log_offset.tx_offset <= first_replayed.last_log_offset.tx_offset)
      )
      |> Enum.map(& &1.last_log_offset.tx_offset)

    assert undelivered != [], "expected transactions still queued behind the first replayed one"

    # Timing evidence that the window is real rather than an artifact of an instant replay.
    remaining_delivered_at =
      Enum.map(undelivered, fn _ ->
        assert_receive {SlowConsumer, ^consumer, %TransactionFragment{}, at}, 5_000
        at
      end)

    replay_finished_at = List.last(remaining_delivered_at)

    Process.exit(keepalive_driver, :kill)

    assert replay_finished_at - first_at > 100,
           "replay finished too fast to call this a window"

    assert broadcast_at_first_delivery < hd(undelivered),
           """
           The global last-seen LSN feed promised #{broadcast_at_first_delivery} while \
           transactions at #{inspect(undelivered)} had not been fanned out to any consumer. \
           A subquery move-in triggered by the first replayed transaction reads that promise \
           at subscribe time and splices ahead of commits its own snapshot already covers.
           """
  end

  defp start_client(ctx) do
    pid =
      start_link_supervised!(
        %{
          id: ReplicationClient,
          start:
            {ReplicationClient, :start_link,
             [[stack_id: ctx.stack_id, replication_opts: ctx.replication_opts, timeout: nil]]},
          restart: :temporary
        },
        []
      )

    assert_receive :streaming_started, @assert_receive_db_timeout
    pid
  end

  defp start_keepalive_driver(ctx) do
    db_config = ctx.db_config

    spawn(fn ->
      {:ok, conn} = Postgrex.start_link(db_config)

      Stream.repeatedly(fn ->
        Postgrex.query!(conn, "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'k')", [])
        Process.sleep(5)
      end)
      |> Stream.run()
    end)
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

  defp start_slow_consumer!(ctx, delay_ms) do
    start_supervised!(
      %{
        id: {:consumer, 1},
        start:
          {SlowConsumer, :start_link,
           [
             [
               stack_id: ctx.stack_id,
               parent: self(),
               shape: ctx.shape,
               shape_handle: @shape_handle,
               delay_ms: delay_ms
             ]
           ]},
        restart: :temporary
      },
      []
    )
  end

  defp await_shape_registered(ctx) do
    Support.TestUtils.wait_until(fn ->
      @shape_handle in ShapeLogCollector.active_shapes(ctx.stack_id)
    end)
  end
end
