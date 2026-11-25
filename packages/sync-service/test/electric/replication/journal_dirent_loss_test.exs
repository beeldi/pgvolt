defmodule Electric.Replication.JournalDirentLossTest do
  # The slot is never acked past a sync, which makes a journal entry that cannot be read back
  # at boot an invariant violation. Both entry points to that check are keyed on `File.ls`, so
  # a generation whose directory entry did not survive reads as "there was never a journal"
  # rather than as the violation it is -- and the coverage check never runs. The used-marker,
  # written beside the journal directory rather than inside it, is what tells the two apart.
  # Same arrangement as the "acknowledged past the journal" test, with the generation
  # unreachable by name instead of merely short.
  use ExUnit.Case, async: false
  use Repatch.ExUnit, assert_expectations: true

  alias Electric.LsnTracker
  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TxnJournal
  alias Electric.StatusMonitor

  import Support.TestUtils, only: [complete_txn_fragment: 3]
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

  setup ctx do
    %{stack_id: stack_id} = ctx

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

    opts = [
      stack_id: stack_id,
      inspector: @inspector,
      persistent_kv: ctx.persistent_kv,
      ack_after_journal: true
    ]

    %{collector_opts: opts, journal_base: journal_base}
  end

  test "refuses to boot when the journal's generation is unreachable by name", ctx do
    # Arrange — journal ends at commit 30 while the slot claims 50, exactly as the
    # existing coverage test, so the guard has something to fire on.
    lsn = Lsn.from_integer(30)

    txn =
      complete_txn_fragment(104, lsn, [
        %Changes.NewRecord{
          relation: {"public", "test_table"},
          record: %{"id" => "#{Lsn.to_integer(lsn)}-0"},
          log_offset: LogOffset.new(lsn, 0)
        }
      ])

    writer = TxnJournal.open_writer!(ctx.journal_base)
    writer = TxnJournal.append_event!(writer, txn)
    TxnJournal.sync!(writer, Lsn.to_integer(lsn))

    LsnTracker.set_last_processed_lsn(ctx.stack_id, Lsn.from_integer(50))

    gens = TxnJournal.list_generations(ctx.journal_base)
    assert gens != []

    # Act — the crash lost the directory block that names the generation. Every reader
    # here reaches the generation only by name, so an unlinked file is indistinguishable.
    Enum.each(gens, fn gen ->
      File.rm!(Path.join([ctx.journal_base, "txn-journal", "#{gen}.bin"]))
    end)

    {:ok, _pid} = start_supervised({ShapeLogCollector.Supervisor, ctx.collector_opts})

    collector = ShapeLogCollector.monitor(ctx.stack_id)
    :ok = ShapeLogCollector.mark_as_ready(ctx.stack_id)

    # Assert
    assert_receive {:DOWN, ^collector, :process, _pid, {%RuntimeError{message: message}, _}},
                   5_000

    assert message =~ "transaction journal"
  end
end
