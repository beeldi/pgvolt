defmodule Electric.Replication.AckAfterJournalRestartDriftTest do
  # Journal mode seeds the flush position from the journal so the boot guard can tell a lost
  # journal from an idle one. Seeding from the slot instead moved the ack a byte further on
  # every quiet restart (the slot reports one past what we last acked) until the guard -- which
  # tolerates exactly one -- refused to boot and told the operator to resync shapes that had
  # lost nothing. The invariant under idle-boundary markers: the slot may advance on keepalives,
  # but only in step with durable journal coverage, and the next boot always accepts.

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

  test "a quiet restart advances the slot only in step with durable journal coverage", ctx do
    # Arrange: boot 1 journals a real transaction and acks the slot at the journal boundary.
    client = start_client(ctx)

    Postgrex.query!(ctx.db_conn, "INSERT INTO serial_ids (id) VALUES ($1)", [1])

    assert_receive {:streamed, %TransactionFragment{commit: commit} = fragment}
                   when not is_nil(commit),
                   @assert_receive_db_timeout

    journal_boundary = fragment.last_log_offset.tx_offset

    assert wait_for_confirmed(ctx, journal_boundary + 1),
           "boot 1 confirmed_flush_lsn is #{confirmed_int(ctx)}, expected #{journal_boundary + 1}"

    gens = TxnJournal.list_generations(ctx.journal_base)
    assert TxnJournal.last_commit_tx_offset(ctx.journal_base, gens) >= journal_boundary

    Process.unlink(client)
    stop_supervised!(ReplicationClient)

    # Act: boot 2 sees no new published transaction, so its flush position is seeded from the
    # journal -- seeding from the slot instead would ack a byte further on every quiet restart.
    # Unpublished commits move the WAL head, and the resulting keepalives advance the ack, but
    # each advancement journals a durable idle-boundary marker first.
    client = start_client(ctx)

    assert wait_for_confirmed(ctx, journal_boundary + 2, fn ->
             Postgrex.query!(
               ctx.db_conn,
               "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'keepalive-bait')",
               []
             )

             send(client, :send_keepalive)
           end),
           "boot 2 never advanced past the journal boundary on idle keepalives"

    confirmed = confirmed_int(ctx)

    Process.unlink(client)
    stop_supervised!(ReplicationClient)

    # The ack ran ahead of the last real commit only in step with journal coverage,
    # durable before the ack. Coverage provenance is version-dependent — pg14 streams
    # empty Begin/Commit pairs for unpublished-table transactions and journals them as
    # real commits, pg15+ suppresses them so only idle markers advance — but the
    # invariant is the same either way.
    gens = TxnJournal.list_generations(ctx.journal_base)
    assert TxnJournal.last_commit_tx_offset(ctx.journal_base, gens) >= confirmed - 1

    # Assert: boot 3 reads the advanced slot, finds it covered by the journal, and boots
    # instead of demanding a resync of shapes that lost nothing.
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(confirmed))
    {:ok, _} = start_supervised({ShapeLogCollector.Supervisor, ctx.collector_opts})

    collector = ShapeLogCollector.monitor(ctx.stack_id)
    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    refute_receive {:DOWN, ^collector, :process, _pid, {%RuntimeError{message: _}, _}},
                   3_000,
                   "boot 3 refused to boot after two quiet restarts with an intact journal"
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

  defp wait_for_confirmed(ctx, target, poke \\ fn -> :ok end) do
    Enum.reduce_while(1..100, false, fn _, _ ->
      poke.()
      Process.sleep(50)

      if confirmed_int(ctx) >= target, do: {:halt, true}, else: {:cont, false}
    end)
  end

  defp confirmed_int(ctx) do
    %Postgrex.Result{rows: [[confirmed_flush_lsn]]} =
      Postgrex.query!(
        ctx.db_conn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
        [ctx.slot_name]
      )

    Lsn.to_integer(confirmed_flush_lsn)
  end
end
