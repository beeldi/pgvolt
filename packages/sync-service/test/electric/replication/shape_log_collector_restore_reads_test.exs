defmodule Electric.Replication.ShapeLogCollectorRestoreReadsTest do
  # The collector restores its shapes before the stack can go active, and every read of the
  # stored shapes decodes all of them: with a large shape count, one read takes seconds.
  # The restore needs one of them, to rebuild its routing. Dropping the subquery shapes,
  # reporting on the shapes, or checking them against the journal, is not a reason for another.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import ExUnit.CaptureLog
  import Support.ComponentSetup
  import Support.TestUtils, only: [complete_txn_fragment: 3]

  alias Electric.LsnTracker
  alias Electric.Postgres.Inspector
  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TxnJournal
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  @inspector Support.StubInspector.new(
               tables: [{1234, {"public", "test_table"}}],
               columns: [%{name: "id", type: "int8", pk_position: 0}]
             )

  @shape Shape.new!("test_table", where: "id = 1", inspector: @inspector)

  # how many subquery shapes the test stores, for the restore to read some of them before
  # their materializer and some of them after
  @subquery_shapes 8

  setup [
    :with_stack_id_from_test,
    :with_pure_file_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_noop_publication_manager,
    :with_persistent_kv
  ]

  setup ctx do
    %{stack_id: stack_id} = ctx
    parent = self()

    Repatch.patch(ShapeStatus, :reduce_shapes, [mode: :shared], fn stack_id, acc, fun ->
      send(parent, :read_of_the_stored_shapes)

      # not `Repatch.real/1`, which would unpatch every call the restore makes while it reads
      Repatch.super(
        ShapeStatus.reduce_shapes(stack_id, acc, fn {handle, _shape} = entry, acc ->
          send(parent, {:read_shape, handle})
          fun.(entry, acc)
        end)
      )
    end)

    # the restore's own read, which hands the shapes on undecoded
    Repatch.patch(ShapeStatus, :reduce_encoded_shapes, [mode: :shared], fn stack_id, acc, fun ->
      send(parent, :read_of_the_stored_shapes)

      Repatch.super(
        ShapeStatus.reduce_encoded_shapes(stack_id, acc, fn {handle, _oid, _shape} = entry, acc ->
          send(parent, {:read_shape, handle})
          fun.(entry, acc)
        end)
      )
    end)

    Support.TestUtils.activate_mocks_for_descendant_procs(ShapeLogCollector)

    {Electric.ShapeCache.PureFileStorage, %{base_path: journal_base}} =
      Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage)

    %{journal_base: journal_base}
  end

  test "a restore reads the stored shapes once", ctx do
    store_shape!(ctx, @shape)

    assert restore(ctx) == 1
  end

  # The routing state ShapeStatus keeps, one row per shape, takes a read of the stored shapes to
  # build, and the restore has one.
  test "a boot reads the stored shapes once, for the routing state too", ctx do
    indexed = store_shape!(ctx, @shape)
    unindexed = store_shape!(ctx, Shape.new!("test_table", inspector: @inspector))

    reads =
      count_store_reads(ctx.stack_id, fn ->
        # as a boot: the shape metadata, the refresh on getting the lock, then the restore
        :ok = ShapeStatus.ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
        :ok = ShapeStatus.initialize(ctx.stack_id)
        :ok = ShapeStatus.refresh(ctx.stack_id)
        restore(ctx)
      end)

    assert reads == 1
    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)

    assert [indexed, unindexed] ==
             ctx.stack_id
             |> ShapeStatus.reduce_publication_filters([], fn {handle, _filter}, handles ->
               [handle | handles]
             end)
             |> Enum.sort_by(&(&1 != indexed))
  end

  test "a restore drops the subquery shapes and their materializers in that one read", ctx do
    plain = store_shape!(ctx, @shape)
    subquery = store_subquery_shapes!(ctx)

    assert restore(ctx) == 1

    assert ShapeLogCollector.active_shapes(ctx.stack_id) == [plain]
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, plain)

    for {outer, materializer} <- subquery do
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, outer)
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, materializer)
    end

    # a materializer read first went into the routing as a plain shape and had to come out
    read = read_order()
    position = fn handle -> Enum.find_index(read, &(&1 == handle)) end

    assert Enum.any?(subquery, fn {outer, inner} -> position.(inner) < position.(outer) end)
    assert Enum.any?(subquery, fn {outer, inner} -> position.(outer) < position.(inner) end)
  end

  # Before the restore read the stored shapes once, the subquery shapes were dropped before the
  # report read them, so a dropped shape was never reported.
  test "a materializer dropped with its subquery shape is not reported as uncastable", ctx do
    subquery =
      store_subquery_shapes!(ctx, fn inner ->
        put_in(inner.where.used_refs, %{["id"] => :point})
      end)

    log = capture_log(fn -> restore(ctx) end)

    refute log =~ "Restored a shape filtering on"

    # the case this is about: a materializer the read reaches before the shape that names it
    read = read_order()
    position = fn handle -> Enum.find_index(read, &(&1 == handle)) end
    assert Enum.any?(subquery, fn {outer, inner} -> position.(inner) < position.(outer) end)
  end

  # Before the restore read the stored shapes once, the subquery shapes were dropped before it
  # introspected a single root table. A restore that is to drop a materializer must not fail on
  # its table: it would fail on every boot until the table could be introspected.
  test "a materializer dropped with its subquery shape does not wait for its table", ctx do
    subquery = store_subquery_shapes!(ctx)
    introspection_fails()

    capture_log(fn -> restore(ctx) end)

    assert ShapeLogCollector.active_shapes(ctx.stack_id) == []

    for {outer, materializer} <- subquery do
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, outer)
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, materializer)
    end

    # the case this is about: a materializer the read reaches before the shape that names it
    read = read_order()
    position = fn handle -> Enum.find_index(read, &(&1 == handle)) end
    assert Enum.any?(subquery, fn {outer, inner} -> position.(inner) < position.(outer) end)
  end

  # Skipping a shape the restore keeps is not an option: nothing would route its changes.
  test "a shape the restore keeps still waits for its table to be introspected", ctx do
    plain = store_shape!(ctx, @shape)
    store_subquery_shapes!(ctx)
    introspection_fails(3)

    capture_log(fn -> restore(ctx) end)

    assert ShapeLogCollector.active_shapes(ctx.stack_id) == [plain]

    assert %{partitions: %{partition_ownership: ownership}} =
             :sys.get_state(ShapeLogCollector.name(ctx.stack_id))

    assert ownership == %{{"public", "test_table"} => MapSet.new([plain])}
  end

  test "a shape the restore keeps still fails it when its table cannot be introspected", ctx do
    plain = store_shape!(ctx, @shape)
    store_subquery_shapes!(ctx)
    introspection_fails()

    capture_log(fn ->
      start_collector(ctx)
      collector = Process.monitor(GenServer.whereis(ShapeLogCollector.name(ctx.stack_id)))

      assert_receive {:DOWN, ^collector, :process, _pid, {%RuntimeError{message: message}, _}},
                     10_000

      assert message =~ "Failed to restore partition info for shape #{plain}"
    end)
  end

  # The relation tracker folds the publication filters as soon as the routing state is there. The
  # rows of the subquery shapes the restore drops must be gone by then: nothing tells the tracker
  # about that drop, so a filter it took for one of them would stay in the publication.
  test "the relation tracker never sees the subquery shapes the restore drops", ctx do
    plain = store_shape!(ctx, @shape)
    subquery = store_subquery_shapes!(ctx)

    # as a boot, which leaves the routing state to the restore
    :ok = ShapeStatus.ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)
    :ok = ShapeStatus.refresh(ctx.stack_id)

    folded = :atomics.new(1, [])

    tracker =
      Task.async(fn ->
        :ok = ShapeStatus.await_routing_state(ctx.stack_id)

        seen =
          ShapeStatus.reduce_publication_filters(ctx.stack_id, [], fn {handle, _filter}, seen ->
            [handle | seen]
          end)

        :atomics.put(folded, 1, 1)
        seen
      end)

    # The drop gives the tracker every chance to fold first: its first removal waits for that fold,
    # or for long enough to show that the fold has to wait for the drop.
    Repatch.patch(ShapeStatus, :remove_shape, [mode: :shared], fn stack_id, handle ->
      unless Process.put(:waited_for_the_tracker, true) do
        Enum.find(1..50, fn _ -> :atomics.get(folded, 1) == 1 or Process.sleep(10) end)
      end

      Repatch.real(ShapeStatus.remove_shape(stack_id, handle))
    end)

    restore(ctx)

    assert Task.await(tracker) == [plain]
    assert %{total: 1, indexed: 1, unindexed: 0} = ShapeStatus.shape_counts(ctx.stack_id)

    for {outer, materializer} <- subquery do
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, outer)
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, materializer)
    end
  end

  test "a journal that carries full transaction ids adds no read of the stored shapes", ctx do
    Electric.StackConfig.put(ctx.stack_id, :feature_flags, ["full_xid_source_positions"])
    handle = store_shape!(ctx, full_provenance(@shape))
    write_journal!(ctx, full_xid: 102)

    assert restore(ctx) == 1
    assert handle in ShapeLogCollector.active_shapes(ctx.stack_id)
  end

  test "a journal without full transaction ids still drops the full-provenance shapes", ctx do
    Electric.StackConfig.put(ctx.stack_id, :feature_flags, ["full_xid_source_positions"])
    handle = store_shape!(ctx, full_provenance(@shape))
    write_journal!(ctx, full_xid: nil)

    assert restore(ctx) == 2
    refute handle in ShapeLogCollector.active_shapes(ctx.stack_id)
    refute ShapeStatus.has_shape_handle?(ctx.stack_id, handle)
  end

  test "a restored shape filtering on a type Electric cannot cast is still reported", ctx do
    # as a shape stored before where clauses were refused for this would be
    uncastable = put_in(@shape.where.used_refs, %{["id"] => :point})
    handle = store_shape!(ctx, uncastable)

    log = capture_log(fn -> restore(ctx) end)

    assert log =~ ~s|Restored a shape filtering on "id" of type :point|
    assert handle in ShapeLogCollector.active_shapes(ctx.stack_id)
  end

  # Returns how many times the restore read the stored shapes.
  defp restore(ctx) do
    start_collector(ctx)

    # answered only once the restore, which runs in the collector's `handle_continue`, is over
    _ = ShapeLogCollector.active_shapes(ctx.stack_id)

    count_reads(0)
  end

  defp start_collector(ctx) do
    start_supervised!(
      {ShapeLogCollector.Supervisor,
       [
         stack_id: ctx.stack_id,
         inspector: @inspector,
         persistent_kv: ctx.persistent_kv,
         ack_after_journal: true
       ]},
      restart: :temporary
    )
  end

  # Every introspection of a root table fails from here on, or the first `failures` of them.
  defp introspection_fails(failures \\ :all) do
    calls = :counters.new(1, [])

    Repatch.patch(Inspector, :load_relation_info, [mode: :shared], fn oid, inspector ->
      :counters.add(calls, 1, 1)

      if failures == :all or :counters.get(calls, 1) <= failures,
        do: {:error, :connection_not_available},
        else: Repatch.super(Inspector.load_relation_info(oid, inspector))
    end)
  end

  defp count_reads(count) do
    receive do
      :read_of_the_stored_shapes -> count_reads(count + 1)
    after
      0 -> count
    end
  end

  # How many reads of the stored shapes `fun` makes, whichever process makes them: every checkout
  # of the store's connection is reported with what it is for.
  defp count_store_reads(stack_id, fun) do
    parent = self()
    handler_id = make_ref()

    :telemetry.attach(
      handler_id,
      [:electric, :shape_db, :pool, :checkout],
      fn _event, _measurements, metadata, _config ->
        if metadata.stack_id == stack_id and
             metadata.label in [:reduce_shapes, :reduce_encoded_shapes, :list_shapes] do
          send(parent, {:store_read, handler_id})
        end
      end,
      nil
    )

    try do
      fun.()
    after
      :telemetry.detach(handler_id)
    end

    count_store_reads(handler_id)
  end

  defp count_store_reads(handler_id) do
    receive do
      {:store_read, ^handler_id} -> 1 + count_store_reads(handler_id)
    after
      0 -> 0
    end
  end

  # The handles of the stored shapes, in the order the restore read them.
  defp read_order do
    receive do
      {:read_shape, handle} -> [handle | read_order()]
    after
      0 -> []
    end
  end

  defp store_shape!(ctx, shape) do
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
    handle
  end

  # Stores `@subquery_shapes` shapes with a subquery, each with its materializer, which
  # `materializer` may change first. Returns their `{outer, materializer}` handles.
  defp store_subquery_shapes!(ctx, materializer \\ & &1) do
    for n <- 1..@subquery_shapes do
      outer =
        Shape.new!("test_table",
          where: "id IN (SELECT id FROM public.test_table WHERE id > #{n})",
          inspector: @inspector
        )

      [inner] = outer.shape_dependencies
      inner = store_shape!(ctx, materializer.(inner))
      {store_shape!(ctx, %{outer | shape_dependencies_handles: [inner]}), inner}
    end
  end

  defp full_provenance(shape) do
    %{shape | source_positions: :full_xid, source_generation: "source-generation"}
  end

  defp write_journal!(ctx, full_xid: full_xid) do
    lsn = Lsn.from_integer(30)

    record = %Changes.NewRecord{
      relation: {"public", "test_table"},
      record: %{"id" => "1"},
      log_offset: LogOffset.new(lsn, 0)
    }

    fragment = %{complete_txn_fragment(102, lsn, [record]) | full_xid: full_xid}

    ctx.journal_base
    |> TxnJournal.open_writer!()
    |> TxnJournal.append_event!(fragment)
    |> TxnJournal.sync!(fragment.last_log_offset.tx_offset)

    LsnTracker.set_last_processed_lsn(ctx.stack_id, lsn)
  end
end
