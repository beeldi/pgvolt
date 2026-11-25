defmodule Electric.Replication.PublicationManager.RestartTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils

  require Repatch
  alias Electric.PersistentKV.Filesystem
  alias Electric.Postgres.Configuration
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.PublicationManager
  alias Electric.Replication.PublicationManager.Configurator
  alias Electric.Replication.PublicationManager.RelationTracker
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes.Consumer
  alias Electric.SnapshotError

  @moduletag :tmp_dir
  @moduletag db_pool_size: 8
  @moduletag connection_opt_overrides: [pool_size: 3]
  @moduletag replication_opts_overrides: [slot_temporary?: false]

  setup [:with_unique_db, :with_publication_name, :with_publication, :with_basic_tables]

  setup ctx do
    stack_id = full_test_name(ctx)
    relation = {"public", "items"}

    %{
      stack_id: stack_id,
      oid: lookup_relation_oid(ctx.pool, relation),
      relation: relation,
      persistent_kv: Filesystem.new!(root: Path.join(ctx.tmp_dir, "kv")),
      storage: {PureFileStorage, stack_id: stack_id, storage_dir: ctx.tmp_dir}
    }
  end

  for manual? <- [false, true] do
    @tag :barrier_recovery
    @tag restart_manual?: manual?
    test "recovers an unfinished drain on restart with manual mode set to #{manual?}", ctx do
      time_out_first_drain()
      start_stack(ctx, true)
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      {handle, shape} = store_shape(ctx)

      assert %SnapshotError{type: :publication_admission_pending} =
               assert_raise(SnapshotError, fn ->
                 PublicationManager.add_shape(ctx.stack_id, handle, shape)
               end)

      oid = ctx.oid

      if ctx.restart_manual? do
        assert {:ok, [_, [^oid], [], false | _]} =
                 Electric.PersistentKV.get(ctx.persistent_kv, barrier_key(ctx))
      end

      :ok = stop_supervised(Electric.StackSupervisor)
      start_stack(ctx, ctx.restart_manual?)

      assert_admitted_after_writer(ctx, writer, handle, shape)
    end
  end

  defp barrier_key(ctx) do
    Enum.find(File.ls!(ctx.persistent_kv.root), fn key ->
      String.starts_with?(key, "#{ctx.stack_id}:publication_barriers:")
    end)
  end

  @tag :barrier_recovery
  test "does not drain an already admitted table again on read-only restart", ctx do
    start_stack(ctx, true)
    Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

    {handle, shape} = store_shape(ctx)
    :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)

    :ok = stop_supervised(Electric.StackSupervisor)
    writer = start_writer(ctx)
    start_stack(ctx, true)

    assert :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)
    finish_writer(writer)
  end

  # The stack sizes the drains from the admin pool it starts: a pool of eight gives that pool two
  # connections, one of them left free of drains, and a pool of four gives it one, which a drain
  # holds only for a short wait on the writers. Without the size, three drains run.
  for {pool_size, drains, short_wait?} <- [{8, 1, false}, {4, 1, true}] do
    @tag :drain_cap
    @tag db_pool_size: pool_size
    test "sizes the drains from the admin pool of a pool of #{pool_size}", ctx do
      start_stack(ctx, false)
      state = :sys.get_state(Configurator.name(ctx.stack_id))

      assert state.max_concurrent_drains == unquote(drains)
      assert is_integer(state.drain_lock_timeout) == unquote(short_wait?)
    end
  end

  for {mode, damage} <- [automatic: :corrupt, manual: :malformed] do
    @tag :unreadable_barrier
    test "rebuilds a #{damage} barrier record and drains restored tables in #{mode} mode", ctx do
      start_stack(ctx, true)
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      {handle, shape} = store_shape(ctx)
      :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)
      :ok = stop_supervised(Electric.StackSupervisor)

      kv = ctx.persistent_kv
      key = barrier_key(ctx)
      assert {:ok, [timeline, [], [_], false | _]} = Electric.PersistentKV.get(kv, key)

      case unquote(damage) do
        :corrupt -> File.write!(Path.join(kv.root, key), <<131, 0>>)
        :malformed -> :ok = Electric.PersistentKV.set(kv, key, [timeline, [], [[ctx.oid]], false])
      end

      writer = start_writer(ctx)
      start_stack(ctx, unquote(mode) == :manual)
      assert_admitted_after_writer(ctx, writer, handle, shape)

      oid = ctx.oid

      assert {:ok, [^timeline, [], [[^oid, "public", "items", _]], true | _]} =
               Electric.PersistentKV.get(kv, key)
    end
  end

  for {membership, restart_workers?} <- [replacement: false, replacement: true, absent: true] do
    @tag :changed_membership
    @tag if restart_workers?, do: :restart_cleanup_owed, else: :catalog_removal_gate
    @tag membership: membership
    @tag restart_workers?: restart_workers?
    test "waits for the stale SQLite row after boot (#{membership}, restart workers: #{restart_workers?})",
         ctx do
      start_stack(ctx, true)
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      {handle, shape} = store_shape(ctx)
      :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)
      :ok = stop_supervised(Electric.StackSupervisor)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])

      writer =
        if ctx.membership == :replacement do
          writer = start_writer(ctx)

          Postgrex.query!(
            ctx.pool,
            "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items",
            []
          )

          writer
        end

      release = pause_removal_writes(ctx, handle)
      start_stack(ctx, ctx.membership == :replacement)

      assert_receive {:removal_queued, ^handle}
      assert WriteBuffer.is_tombstoned?(ctx.stack_id, handle) == ctx.restart_workers?

      unless ctx.restart_workers? do
        refute MapSet.member?(WriteBuffer.pending_shape_handles(ctx.stack_id), handle)
      end

      assert shape_on_disk?(ctx.stack_id, handle)

      if ctx.restart_workers? do
        restart_publication_workers(ctx)
        assert {_, filters} = RelationTracker.fetch_current_filters!(stack_id: ctx.stack_id)
        assert MapSet.size(filters) == 0

        revision = System.unique_integer([:positive, :monotonic])

        Configurator.configure_publication(
          ctx.stack_id,
          {revision, MapSet.new([{ctx.oid, ctx.relation}])}
        )

        assert wait_until(fn ->
                 :sys.get_state(Configurator.name(ctx.stack_id)).checked_filter_revision >=
                   revision
               end)
      end

      assert %{drains: drains} = :sys.get_state(Configurator.name(ctx.stack_id))
      assert drains == %{}, "the writer drain started while the stale row was still in SQLite"
      if ctx.membership == :absent, do: assert(publication_empty?(ctx))
      assert shape_on_disk?(ctx.stack_id, handle)

      release.()
      refute shape_on_disk?(ctx.stack_id, handle)

      shape =
        generate_shape(
          {ctx.oid, ctx.relation},
          %Expr{query: "id IS NOT NULL", used_refs: %{["id"] => :text}}
        )

      {:ok, replacement} = ShapeStatus.add_shape(ctx.stack_id, shape)

      if writer do
        assert_admitted_after_writer(ctx, writer, replacement, shape)
      else
        assert :ok = PublicationManager.add_shape(ctx.stack_id, replacement, shape)
        refute publication_empty?(ctx)
      end
    end
  end

  @tag :cleanup_after_catalog_delete
  test "finishes teardown when a cleaner dies after the row is deleted", ctx do
    start_stack(ctx, true)
    Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
    {handle, shape} = store_shape(ctx)
    :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)

    parent = self()
    attempts = :atomics.new(1, [])
    activate_mocks_for_descendant_procs(ShapeCleaner)

    Repatch.patch(ShapeStatus, :remove_shape, [mode: :shared], fn
      stack_id, ^handle ->
        case :atomics.add_get(attempts, 1, 1) do
          1 ->
            :ok = WriteBuffer.remove_shape(stack_id, handle)
            :ok = WriteBuffer.flush_sync(stack_id)
            send(parent, :row_deleted_before_detach)
            Process.exit(self(), :kill)

          2 ->
            send(parent, {:retrying_cleanup, self()})
            receive(do: (:finish_cleanup -> :ok))
            Repatch.real(ShapeStatus.remove_shape(stack_id, handle))

          _ ->
            Repatch.real(ShapeStatus.remove_shape(stack_id, handle))
        end

      stack_id, other ->
        Repatch.real(ShapeStatus.remove_shape(stack_id, other))
    end)

    Repatch.patch(Consumer, :stop, [mode: :shared], fn stack_id, shape_handle, reason ->
      send(parent, {:stopping_consumer, shape_handle})
      Repatch.real(Consumer.stop(stack_id, shape_handle, reason))
    end)

    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
    writer = start_writer(ctx)
    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
    check_publication(ctx)

    assert_receive :row_deleted_before_detach
    assert %{drains: %{} = drains} = :sys.get_state(Configurator.name(ctx.stack_id))
    assert drains == %{}, "admission began while the stale handle still validated"
    assert :ok = ShapeStatus.validate_shape_handle(ctx.stack_id, handle, shape)
    refute shape_on_disk?(ctx.stack_id, handle)
    assert_receive {:retrying_cleanup, cleaner}
    monitor = Process.monitor(cleaner)
    send(cleaner, :finish_cleanup)
    assert_receive {:stopping_consumer, ^handle}
    assert_receive {:DOWN, ^monitor, :process, ^cleaner, :normal}
    assert :error = ShapeStatus.validate_shape_handle(ctx.stack_id, handle, shape)
    assert {_, filters} = RelationTracker.fetch_current_filters!(stack_id: ctx.stack_id)
    assert MapSet.size(filters) == 0

    {:ok, replacement} = ShapeStatus.add_shape(ctx.stack_id, shape)
    assert_admitted_after_writer(ctx, writer, replacement, shape)
  end

  defp check_publication(ctx) do
    Configurator.configure_publication(
      ctx.stack_id,
      RelationTracker.fetch_current_filters!(stack_id: ctx.stack_id)
    )
  end

  defp pause_removal_writes(ctx, handle) do
    parent = self()
    blocked = :atomics.new(2, [])
    :atomics.put(blocked, 1, 1)
    activate_mocks_for_descendant_procs(ShapeCleaner)

    unless ctx.restart_workers? do
      # Model a deletion the buffer dropped, so only the direct catalog read can find the row.
      Repatch.patch(WriteBuffer, :remove_shape, [mode: :shared], fn stack_id, shape_handle ->
        if shape_handle == handle and :atomics.get(blocked, 1) == 1,
          do: :ok,
          else: Repatch.real(WriteBuffer.remove_shape(stack_id, shape_handle))
      end)
    end

    Repatch.patch(WriteBuffer, :handle_call, [mode: :shared], fn
      :flush_sync = message, from, state ->
        if :atomics.get(blocked, 1) == 1,
          do: {:reply, {:error, :lock_contention}, state},
          else: Repatch.real(WriteBuffer.handle_call(message, from, state))

      message, from, state ->
        Repatch.real(WriteBuffer.handle_call(message, from, state))
    end)

    Repatch.patch(ShapeStatus, :remove_shape, [mode: :shared], fn stack_id, shape_handle ->
      if shape_handle == handle and :atomics.compare_exchange(blocked, 2, 0, 1) == :ok do
        Repatch.allow(parent, WriteBuffer.name(stack_id))
        :ok = WriteBuffer.pause_flush(stack_id)
        :ok = Repatch.real(ShapeStatus.remove_shape(stack_id, shape_handle))

        unless ctx.restart_workers? do
          Repatch.allow(parent, Configurator.name(stack_id))
          ops = WriteBuffer.operations_table_name(stack_id)
          shapes = WriteBuffer.shapes_table_name(stack_id)

          if :ets.take(shapes, {:tombstone, handle}) != [] do
            :ets.match_delete(ops, {:_, {:remove, handle}, :_, :_})
            :ets.update_counter(shapes, :count_remove, {2, -1, 0, 0})
          end
        end

        send(parent, {:removal_queued, handle})
        :ok
      else
        Repatch.real(ShapeStatus.remove_shape(stack_id, shape_handle))
      end
    end)

    fn ->
      :atomics.put(blocked, 1, 0)
      :ok = WriteBuffer.resume_flush(ctx.stack_id)
      unless ctx.restart_workers?, do: :ok = WriteBuffer.remove_shape(ctx.stack_id, handle)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
    end
  end

  defp restart_publication_workers(ctx) do
    supervisor = PublicationManager.Supervisor.name(ctx.stack_id)
    :ok = Supervisor.terminate_child(supervisor, Configurator)
    :ok = Supervisor.terminate_child(supervisor, RelationTracker)
    {:ok, _} = Supervisor.restart_child(supervisor, RelationTracker)
    {:ok, _} = Supervisor.restart_child(supervisor, Configurator)
    :ok
  end

  defp shape_on_disk?(stack_id, handle) do
    ShapeDb.Connection.checkout!(
      stack_id,
      :handle_exists?,
      &ShapeDb.Query.handle_exists?(&1, handle)
    )
  end

  defp assert_admitted_after_writer(ctx, writer, handle, shape) do
    tracker = RelationTracker.name(ctx.stack_id)
    oid = ctx.oid
    adder = Task.async(fn -> PublicationManager.add_shape(ctx.stack_id, handle, shape) end)

    assert wait_until(fn ->
             scalar(
               ctx.pool,
               """
               SELECT EXISTS (
                 SELECT 1 FROM pg_locks l
                 WHERE l.relation = $1 AND l.mode = 'ShareLock' AND NOT l.granted
               )
               """,
               [oid]
             )
           end),
           "expected the restored drain to wait on the writer"

    assert wait_until(fn ->
             match?(%{waiters: %{^oid => [{{_, _}, ^handle}]}}, :sys.get_state(tracker))
           end),
           "expected the shape's admission call to still be waiting"

    finish_writer(writer)
    assert :ok = Task.await(adder)
  end

  defp publication_empty?(ctx) do
    Configuration.get_publication_tables!(ctx.pool, ctx.publication_name) == []
  end

  defp start_stack(ctx, manual?) do
    ctx
    |> Map.delete(:connection_opt_overrides)
    |> Map.update!(:db_config, &Keyword.delete(&1, :pool_size))
    |> Map.update!(:pooled_db_config, &Keyword.delete(&1, :pool_size))
    |> Map.put(:manual_table_publishing?, manual?)
    |> start_stack_supervisor!(
      ctx.stack_id,
      ctx.persistent_kv,
      ctx.storage,
      Electric.stack_events_registry(),
      ctx.publication_name
    )
  end

  defp store_shape(ctx) do
    shape = generate_shape({ctx.oid, ctx.relation})
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    storage = Storage.for_shape(handle, Storage.for_stack(ctx.stack_id))
    writer = Storage.init_writer!(storage, shape)
    :ok = Storage.set_pg_snapshot(%{xmin: 1, xmax: 2, xip_list: []}, storage)
    :ok = Electric.Shapes.mark_snapshot_started(storage, ctx.stack_id, handle)
    :ok = Electric.Shapes.make_new_snapshot!([], storage, ctx.stack_id, handle)
    Storage.terminate(writer)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    {handle, shape}
  end

  defp time_out_first_drain do
    activate_mocks_for_descendant_procs(PublicationManager.Configurator)
    drains = :atomics.new(1, [])

    Repatch.patch(
      Configuration,
      :wait_for_in_flight_writers_on_published_table,
      [mode: :shared],
      fn pool, publication, filter, expected ->
        if :atomics.add_get(drains, 1, 1) == 1 do
          {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
        else
          Repatch.real(
            Configuration.wait_for_in_flight_writers_on_published_table(
              pool,
              publication,
              filter,
              expected
            )
          )
        end
      end
    )
  end

  # The lock a write takes, with no transaction id: a write's id would hold back the creation of a
  # replication slot by any other test on the server, a stack start's included, while it is open.
  defp start_writer(ctx) do
    parent = self()

    writer =
      Task.async(fn ->
        Postgrex.transaction(ctx.pool, fn conn ->
          Postgrex.query!(conn, "LOCK TABLE items IN ROW EXCLUSIVE MODE", [])

          send(parent, :writer_open)
          receive(do: (:commit -> :ok))
        end)
      end)

    assert_receive :writer_open
    writer
  end

  defp finish_writer(writer) do
    send(writer.pid, :commit)
    assert {:ok, :ok} = Task.await(writer)
  end
end
