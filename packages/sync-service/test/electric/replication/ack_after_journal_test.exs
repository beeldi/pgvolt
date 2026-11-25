defmodule Electric.Replication.AckAfterJournalTest do
  # Collector-side contract of ELECTRIC_ACK_AFTER_JOURNAL: at boot the collector replays
  # the client-written transaction journal into the fan-out (Postgres resumes at the acked
  # slot position, so everything below it must come from the journal), refuses to boot
  # when the slot outran the journal, and forwards the consumer-durable boundary to the
  # replication client as the journal GC watermark.
  use ExUnit.Case, async: false
  use Repatch.ExUnit, assert_expectations: true

  alias Electric.LsnTracker
  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TxnJournal
  alias Electric.Shapes.Shape
  alias Electric.StatusMonitor

  alias Support.TransactionConsumer

  import Support.TestUtils,
    only: [
      register_as_replication_client: 1,
      complete_txn_fragment: 3
    ]

  import Support.ComponentSetup

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_pure_file_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_noop_publication_manager,
    :with_persistent_kv
  ]

  @inspector Support.StubInspector.new(
               tables: [{1234, {"public", "test_table"}}],
               columns: [%{name: "id", type: "int8", pk_position: 0}]
             )

  @shape Shape.new!("test_table", inspector: @inspector)
  @shape_handle "journal-shape"

  setup ctx do
    %{stack_id: stack_id} = ctx

    registry_name = Module.concat(__MODULE__, Registry)
    start_link_supervised!({Registry, keys: :duplicate, name: registry_name})

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

    opts = [
      stack_id: stack_id,
      inspector: @inspector,
      persistent_kv: ctx.persistent_kv,
      ack_after_journal: true
    ]

    %{collector_opts: opts, registry: registry_name, journal_base: journal_base}
  end

  defp start_collector!(ctx) do
    {:ok, _pid} = start_supervised({ShapeLogCollector.Supervisor, ctx.collector_opts})
    :ok
  end

  defp start_consumer!(ctx, id, extra_opts \\ []) do
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
                 shape: @shape,
                 shape_handle: @shape_handle
               ]
               |> Keyword.merge(extra_opts)
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

  defp new_record(lsn, op) do
    %Changes.NewRecord{
      relation: {"public", "test_table"},
      record: %{"id" => "#{Lsn.to_integer(lsn)}-#{op}"},
      log_offset: LogOffset.new(lsn, op)
    }
  end

  defp write_journal!(ctx, events) do
    writer = TxnJournal.open_writer!(ctx.journal_base)
    writer = Enum.reduce(events, writer, &TxnJournal.append_event!(&2, &1))

    last_commit =
      events
      |> Enum.reverse()
      |> Enum.find_value(fn
        %Changes.TransactionFragment{commit: commit, last_log_offset: last}
        when not is_nil(commit) ->
          last.tx_offset

        _ ->
          nil
      end)

    TxnJournal.sync!(writer, last_commit)
  end

  test "replays the journal into the fan-out at boot and signals the GC watermark", ctx do
    lsn1 = Lsn.from_integer(30)
    lsn2 = Lsn.from_integer(40)
    txn1 = complete_txn_fragment(102, lsn1, [new_record(lsn1, 0)])
    txn2 = complete_txn_fragment(103, lsn2, [new_record(lsn2, 0)])
    write_journal!(ctx, [txn1, txn2])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, lsn2)

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
    assert_receive :shape_log_collector_ready

    # clock is refreshed on replay, so compare identity rather than the whole struct.
    for expected <- [txn1, txn2] do
      assert_receive {TransactionConsumer, ^consumer, [replayed]}
      assert replayed.xid == expected.xid
      assert replayed.last_log_offset == expected.last_log_offset
      assert length(replayed.changes) == length(expected.changes)
    end

    ShapeLogCollector.notify_flushed(ctx.stack_id, @shape_handle, LogOffset.new(lsn2, 0))

    assert_receive {:journal_consumer_boundary, 40}
  end

  test "replays pre-provenance persisted transaction structs without a full_xid field", ctx do
    lsn = Lsn.from_integer(30)

    fragment =
      complete_txn_fragment(102, lsn, [new_record(lsn, 0)])
      |> Map.delete(:full_xid)

    write_journal!(ctx, [fragment])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, lsn)
    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    assert_receive {TransactionConsumer, ^consumer, [replayed]}
    assert replayed.xid == 102
    assert replayed.full_xid == nil
    assert replayed.last_log_offset == fragment.last_log_offset
  end

  for old_format? <- [false, true] do
    @tag old_format: old_format?
    test "retained raw journal invalidates only full-provenance handles across restarts (old format=#{old_format?})",
         %{old_format: old_format?} = ctx do
      alias Electric.ShapeCache.ShapeStatus

      Electric.StackConfig.put(ctx.stack_id, :feature_flags, ["full_xid_source_positions"])
      lsn = Lsn.from_integer(30)
      fragment = complete_txn_fragment(102, lsn, [new_record(lsn, 0)])
      fragment = if old_format?, do: Map.delete(fragment, :full_xid), else: fragment
      write_journal!(ctx, [fragment])
      LsnTracker.set_last_processed_lsn(ctx.stack_id, lsn)

      {:ok, legacy_handle} = ShapeStatus.add_shape(ctx.stack_id, @shape)
      :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(ctx.stack_id, legacy_handle)
      start_collector!(ctx)
      consumer = start_consumer!(ctx, 1, shape_handle: legacy_handle, action: :restore)
      register_as_replication_client(ctx.stack_id)
      :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
      assert_receive {TransactionConsumer, ^consumer, [_replayed]}
      :ok = stop_supervised({:consumer, 1})
      :ok = stop_supervised(ShapeLogCollector.Supervisor)

      full_shape = %{@shape | source_positions: :full_xid, source_generation: "source-generation"}

      for _ <- 1..2 do
        {:ok, full_handle} = ShapeStatus.add_shape(ctx.stack_id, full_shape)
        :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(ctx.stack_id, full_handle)
        assert TxnJournal.list_generations(ctx.journal_base) != []

        start_collector!(ctx)
        active = ShapeLogCollector.active_shapes(ctx.stack_id)

        assert legacy_handle in active
        refute full_handle in active
        assert ShapeStatus.has_shape_handle?(ctx.stack_id, legacy_handle)
        refute ShapeStatus.has_shape_handle?(ctx.stack_id, full_handle)

        :ok = stop_supervised(ShapeLogCollector.Supervisor)
      end
    end
  end

  test "full transaction journal history preserves full-provenance handles", ctx do
    alias Electric.ShapeCache.ShapeStatus

    Electric.StackConfig.put(ctx.stack_id, :feature_flags, ["full_xid_source_positions"])
    lsn = Lsn.from_integer(30)
    fragment = %{complete_txn_fragment(102, lsn, [new_record(lsn, 0)]) | full_xid: 102}
    write_journal!(ctx, [fragment])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, lsn)
    full_shape = %{@shape | source_positions: :full_xid, source_generation: "source-generation"}
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, full_shape)
    :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)

    start_collector!(ctx)
    assert handle in ShapeLogCollector.active_shapes(ctx.stack_id)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, handle)
  end

  # A writer's flush entry is stamped when its first replayed transaction is delivered,
  # mid-replay, and later deliveries never refresh it — so a replay burst longer than
  # the grace period leaves every not-yet-flushed writer instantly "stalled" and the
  # first post-boot stall scans invalidate healthy shapes on every long boot replay.
  # The stall clock must start at the end of replay delivery.
  test "replay delivery does not count toward the flush-stall grace period", ctx do
    # replay past the grace period; it never reports a flush
    stack_id = ctx.stack_id
    parent = self()
    Electric.StackConfig.put(stack_id, :flush_stall_grace_period, 450)

    Repatch.patch(
      Electric.ShapeCache.ShapeCleaner,
      :remove_shapes_async,
      [mode: :shared],
      fn ^stack_id, handles ->
        send(parent, {:remove_shapes_async, handles})
        :ok
      end
    )

    txns =
      for offset <- [30, 40, 50] do
        lsn = Lsn.from_integer(offset)
        complete_txn_fragment(100 + offset, lsn, [new_record(lsn, 0)])
      end

    write_journal!(ctx, txns)
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(50))

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1, handle_event_delay: 300)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
    assert_receive :shape_log_collector_ready, 5000

    for expected <- txns do
      assert_receive {TransactionConsumer, ^consumer, [replayed]}
      assert replayed.xid == expected.xid
    end

    collector = stack_id |> ShapeLogCollector.name() |> GenServer.whereis()
    send(collector, :check_stalled_flushes)
    refute_receive {:flush_progress_challenged, _}, 100
    send(collector, :check_stalled_flushes)

    refute_receive {:remove_shapes_async, _}, 100
    refute_receive {:flush_progress_challenged, _}, 100
  end

  # Disabling the flag must not strand a journal: the slot was acked at the journal
  # boundary by the previous boot, so Postgres will never resend what it holds. The
  # generations are only deleted once the consumers are DURABLE past their last
  # commit — replay delivery alone leaves a crash window where the WAL would be
  # gone from both Postgres and the journal.
  test "replays a leftover journal when ack-after-journal is now off and deletes it only once consumers are durable",
       ctx do
    lsn = Lsn.from_integer(30)
    txn = complete_txn_fragment(105, lsn, [new_record(lsn, 0)])
    write_journal!(ctx, [txn])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, lsn)

    ctx = %{ctx | collector_opts: Keyword.put(ctx.collector_opts, :ack_after_journal, false)}

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    assert_receive {TransactionConsumer, ^consumer, [replayed]}
    assert replayed.xid == txn.xid
    :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
    assert TxnJournal.list_generations(ctx.journal_base) != []

    ShapeLogCollector.notify_flushed(ctx.stack_id, @shape_handle, LogOffset.new(lsn, 0))

    assert_receive {:flush_boundary_updated, 30}
    :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
    assert TxnJournal.list_generations(ctx.journal_base) == []
  end

  # The mode-off counterpart of "refuses to boot": with the flag off the flush
  # boundary drives the slot acks, so the slot legitimately runs past a leftover
  # journal awaiting reclaim and the gap proves nothing.
  test "a slot acknowledged past the journal does not block a mode-off boot", ctx do
    lsn = Lsn.from_integer(30)
    txn = complete_txn_fragment(107, lsn, [new_record(lsn, 0)])
    write_journal!(ctx, [txn])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(50))

    ctx = %{ctx | collector_opts: Keyword.put(ctx.collector_opts, :ack_after_journal, false)}

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    assert_receive :shape_log_collector_ready
    assert_receive {TransactionConsumer, ^consumer, [replayed]}
    assert replayed.xid == txn.xid
  end

  # A quiet stack writes one marker-bearing generation per boot; replay reclaims all
  # but the one still carrying the newest coverage, so the directory stays bounded.
  test "replay deletes marker-only generations except the coverage holder", ctx do
    lsn = Lsn.from_integer(30)
    txn = complete_txn_fragment(108, lsn, [new_record(lsn, 0)])

    writer1 = TxnJournal.open_writer!(ctx.journal_base)
    writer1 = TxnJournal.append_event!(writer1, txn)
    TxnJournal.sync!(writer1, 30)

    writer2 = TxnJournal.open_writer!(ctx.journal_base)
    writer2 = TxnJournal.append_event!(writer2, {:idle_slot_boundary, 40})
    TxnJournal.sync!(writer2, 40)

    writer3 = TxnJournal.open_writer!(ctx.journal_base)
    writer3 = TxnJournal.append_event!(writer3, {:idle_slot_boundary, 50})
    TxnJournal.sync!(writer3, 50)

    assert [gen1, _gen2, gen3] = TxnJournal.list_generations(ctx.journal_base)
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(50))

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    # generation and the coverage-holding marker generation survive
    assert_receive :shape_log_collector_ready
    assert_receive {TransactionConsumer, ^consumer, [replayed]}
    assert replayed.xid == txn.xid
    :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
    assert TxnJournal.list_generations(ctx.journal_base) == [gen1, gen3]
  end

  test "a fresh boot with an empty journal accepts new events after the slot position",
       ctx do
    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
    assert_receive :shape_log_collector_ready

    lsn = Lsn.from_integer(10)
    txn = complete_txn_fragment(100, lsn, [new_record(lsn, 0)])

    assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

    TransactionConsumer.assert_consume([consumer], [txn])
    ShapeLogCollector.notify_flushed(ctx.stack_id, @shape_handle, LogOffset.new(lsn, 0))
    assert_receive {:flush_boundary_updated, 10}
    assert_receive {:journal_consumer_boundary, 10}
  end

  # The option is usually absent, not set to false. That path must leave
  # the journal machinery entirely unarmed, otherwise flipping the default on would be
  # invisible here. "Absent" has to mean absent everywhere: an option left out of the stack
  # falls through to release configuration, so the environment is pinned too.
  test "arms nothing when the option is absent", ctx do
    previous = Application.get_env(:electric, :ack_after_journal)
    Application.put_env(:electric, :ack_after_journal, false)
    on_exit(fn -> Application.put_env(:electric, :ack_after_journal, previous) end)

    ctx = %{ctx | collector_opts: Keyword.delete(ctx.collector_opts, :ack_after_journal)}

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)
    assert_receive :shape_log_collector_ready

    assert %{txn_journal_base: nil} = :sys.get_state(ShapeLogCollector.name(ctx.stack_id))

    lsn = Lsn.from_integer(10)
    txn = complete_txn_fragment(100, lsn, [new_record(lsn, 0)])

    assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
    TransactionConsumer.assert_consume([consumer], [txn])
    ShapeLogCollector.notify_flushed(ctx.stack_id, @shape_handle, LogOffset.new(lsn, 0))

    # ack — never as a journal GC watermark.
    assert_receive {:flush_boundary_updated, 10}
    refute_receive {:journal_consumer_boundary, _}, 200
  end

  # The replication client acks keepalives over publication-free WAL by first journaling
  # an idle-boundary marker; the next boot must read the marker as coverage rather than
  # reporting the commit-to-slot gap as lost shape data.
  test "boots and replays past an idle-slot-boundary marker", ctx do
    lsn = Lsn.from_integer(30)
    txn = complete_txn_fragment(106, lsn, [new_record(lsn, 0)])
    write_journal!(ctx, [txn, {:idle_slot_boundary, 50}])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(50))

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    # the real transaction
    assert_receive :shape_log_collector_ready
    assert_receive {TransactionConsumer, ^consumer, [replayed]}
    assert replayed.xid == txn.xid
    refute_receive {TransactionConsumer, _, _}, 200
  end

  test "refuses to boot when the slot was acknowledged past the journal", ctx do
    lsn = Lsn.from_integer(30)
    txn = complete_txn_fragment(104, lsn, [new_record(lsn, 0)])
    write_journal!(ctx, [txn])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(50))

    start_collector!(ctx)

    # the collector crashing rather than as an exit in the caller.
    collector = ShapeLogCollector.monitor(ctx.stack_id)
    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    assert_receive {:DOWN, ^collector, :process, _pid, {%RuntimeError{message: message}, _}},
                   5_000

    assert message =~ "transaction journal ends at 30"
  end

  # The generation list is snapshotted in init while the used-marker is read in the replay
  # continuation. On a genuine first boot the replication client's writer opens between the two,
  # writing the marker before its first generation, so a marker read at replay time reports every
  # generation lost against an init snapshot that legitimately saw none. Both answers must name
  # the same instant.
  test "a first boot does not refuse when the writer creates the marker after the init snapshot",
       ctx do
    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    # the replication client opens its writer between the init snapshot and the replay: the
    # marker lands before the first generation, exactly the instant the guard must not read as loss
    _writer = TxnJournal.open_writer!(ctx.journal_base)
    assert TxnJournal.used?(ctx.journal_base)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    assert_receive :shape_log_collector_ready
    refute_receive {TransactionConsumer, ^consumer, _}, 200
  end

  # Every replayed generation is marker-only and none records coverage, so the coverage-holder
  # rule spares nothing. Emptying the directory would leave the used-marker over an empty
  # directory, which the next boot reads as every generation lost, so the newest must survive.
  test "replay keeps the newest generation when every replayed generation is empty", ctx do
    # and last_commit are nil
    _writer1 = TxnJournal.open_writer!(ctx.journal_base)
    _writer2 = TxnJournal.open_writer!(ctx.journal_base)

    assert [_gen1, gen2] = TxnJournal.list_generations(ctx.journal_base)
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(1))

    start_collector!(ctx)
    consumer = start_consumer!(ctx, 1)
    await_shape_registered(ctx)
    register_as_replication_client(ctx.stack_id)

    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    # marker over an empty directory as every generation lost
    assert_receive :shape_log_collector_ready
    :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
    assert TxnJournal.list_generations(ctx.journal_base) == [gen2]
  end

  # The fail-open opposite of "refuses to boot": a journal that synced a boundary but whose only
  # generation reads back nothing (a damaged newest generation, torn-tail tolerated) has no
  # readable coverage and none reclaimed, so it cannot demonstrate the acked history is replayable
  # and must refuse rather than accept the slot position on an empty replay.
  test "refuses to boot when a committed journal reads back no coverage", ctx do
    # only generation is corrupted so it streams back nothing
    lsn = Lsn.from_integer(30)
    txn = complete_txn_fragment(109, lsn, [new_record(lsn, 0)])
    write_journal!(ctx, [txn])
    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(101))

    assert [gen] = TxnJournal.list_generations(ctx.journal_base)

    File.write!(
      Path.join([ctx.journal_base, "txn-journal", "#{gen}.bin"]),
      <<255::32, 0::32>>
    )

    start_collector!(ctx)

    collector = ShapeLogCollector.monitor(ctx.stack_id)
    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    assert_receive {:DOWN, ^collector, :process, _pid, {%RuntimeError{message: message}, _}},
                   5_000

    assert message =~ "reads back no boundary"
  end
end
