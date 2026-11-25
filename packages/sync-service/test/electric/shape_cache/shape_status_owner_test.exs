defmodule Electric.ShapeCache.ShapeStatusOwnerTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.TestUtils, only: [wait_until: 1]

  alias Electric.ShapeCache
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.ShapeCache.ShapeStatusOwner
  alias Electric.Shapes.Shape
  alias Electric.StatusMonitor
  alias Electric.Timeline

  @moduletag :tmp_dir

  @inspector Support.StubInspector.new(
               tables: [{1, {"public", "items"}}],
               columns: [%{name: "id", type: "int8", pk_position: 0}]
             )

  setup [
    :with_stack_id_from_test,
    :with_status_monitor,
    :with_async_deleter,
    :with_pure_file_storage,
    :with_shape_cleaner,
    :with_shape_db
  ]

  test "standby keeps an in-progress snapshot and serves only completed shapes", ctx do
    pending_shape = store_shape(ctx, "pending", 1)
    completed_shape = store_shape(ctx, "completed", 2)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "completed")
    completed_dir = storage_dir(ctx, "completed")
    pending_dir = storage_dir(ctx, "pending")
    orphan_dir = storage_dir(ctx, "orphan")
    start_owner(ctx)

    assert ShapeDb.handle_exists?(ctx.stack_id, "pending")
    assert Enum.all?([pending_dir, completed_dir, orphan_dir], &File.dir?/1)
    refute ShapeStatus.has_shape_handle?(ctx.stack_id, "pending")
    assert ShapeStatus.snapshot_started?(ctx.stack_id, "completed")

    assert {"completed", _offset} =
             ShapeCache.resolve_shape_handle(nil, completed_shape, ctx.stack_id, read_only?: true)

    assert is_nil(
             ShapeCache.resolve_shape_handle(nil, pending_shape, ctx.stack_id, read_only?: true)
           )

    :ok = ShapeStatusOwner.refresh(ctx.stack_id)

    refute ShapeDb.handle_exists?(ctx.stack_id, "pending")
    assert ShapeDb.handle_exists?(ctx.stack_id, "completed")
    assert wait_until(fn -> not File.exists?(pending_dir) and not File.exists?(orphan_dir) end)
    assert File.dir?(completed_dir)
  end

  # The running instance can complete a snapshot after the standby loaded metadata. SQLite then
  # says complete, but the standby has no metadata for the shape, so its snapshot wait answered
  # `:unknown` and the request failed. A handle a standby resolves must be one it can serve.
  test "standby leaves a shape completed after its boot to takeover", ctx do
    late_shape = store_shape(ctx, "late", 1)
    loaded_shape = store_shape(ctx, "loaded", 2)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "loaded")
    start_owner(ctx)

    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "late")
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    assert ShapeDb.snapshot_complete?(ctx.stack_id, "late")

    assert is_nil(
             ShapeCache.resolve_shape_handle(nil, late_shape, ctx.stack_id, read_only?: true)
           )

    assert {"loaded", _offset} =
             ShapeCache.resolve_shape_handle(nil, loaded_shape, ctx.stack_id, read_only?: true)

    assert :started = ShapeCache.await_snapshot_start("loaded", ctx.stack_id)
  end

  # The owner holds the catalog locked while it commits. A standby read that met the lock past the
  # busy timeout came back as an error, which the check took for an incomplete shape: a live
  # request read that as a rotation and sent the client to refetch the whole shape.
  @tag shape_db_opts: [exclusive_mode: true]
  test "standby keeps serving a loaded shape while the owner holds the catalog locked", ctx do
    loaded_shape = store_shape(ctx, "loaded", 1)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "loaded")
    start_owner(ctx)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
    {:ok, holder} = ShapeDb.Sqlite3.open(path, [])
    :ok = ShapeDb.Connection.execute(holder, "BEGIN EXCLUSIVE")

    try do
      assert {:error, _} = ShapeDb.snapshot_complete?(ctx.stack_id, "loaded")

      assert {"loaded", _offset} =
               ShapeCache.resolve_shape_handle("loaded", loaded_shape, ctx.stack_id,
                 read_only?: true
               )
    after
      :ok = ShapeDb.Connection.execute(holder, "ROLLBACK")
      :ok = ShapeDb.Connection.close(holder)
    end
  end

  test "an empty completed set leaves both snapshot and other storage until takeover", ctx do
    store_shape(ctx, "pending", 1)
    pending_dir = storage_dir(ctx, "pending")
    orphan_dir = storage_dir(ctx, "orphan")
    {_module, storage_opts} = ctx.storage
    File.mkdir_p!(storage_opts.tmp_dir)

    start_owner(ctx)

    assert File.dir?(storage_opts.tmp_dir)
    assert Enum.all?([pending_dir, orphan_dir], &File.dir?/1)
    assert ShapeDb.handle_exists?(ctx.stack_id, "pending")

    :ok = ShapeStatusOwner.refresh(ctx.stack_id)

    refute ShapeDb.handle_exists?(ctx.stack_id, "pending")
    refute File.exists?(storage_opts.base_path)
    refute File.exists?(storage_opts.tmp_dir)
  end

  # A timeline change leaves a purge of every shape owed until the connection manager ends it,
  # and a purge step that fails leaves the old timeline's shapes in the catalog and on disk. The
  # owner loaded them all the same, and a standby served them read-only.
  test "leaves the catalog's shapes unserved while a purge is owed", ctx do
    shape = store_shape(ctx, "stale", 1)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "stale")
    dir = storage_dir(ctx, "stale")
    timeline_opts = timeline_opts(ctx)
    :ok = Timeline.store_irrecoverable_timeline(1, timeline_opts)

    start_owner_owing_a_purge(ctx, timeline_opts: timeline_opts)

    refute ShapeStatus.has_shape_handle?(ctx.stack_id, "stale")
    assert is_nil(ShapeCache.resolve_shape_handle(nil, shape, ctx.stack_id, read_only?: true))

    assert :purge_owed = ShapeStatusOwner.refresh(ctx.stack_id)

    refute ShapeStatus.has_shape_handle?(ctx.stack_id, "stale")
    assert is_nil(ShapeCache.resolve_shape_handle(nil, shape, ctx.stack_id, read_only?: true))
    # The purge removes them, not the owner.
    assert ShapeDb.handle_exists?(ctx.stack_id, "stale")
    assert File.dir?(dir)
  end

  # A manager that dies once it stored the owed purge leaves the metadata the owner loaded before.
  # The refresh of the manager that takes over reloaded it from the catalog the purge still owes.
  # The owner unloaded it then, under a shape subsystem still running: a relation tracker that
  # folded it as it was cleared took tables out of the publication. The manager unloads it once
  # it has stopped that subsystem.
  test "a refresh while a purge is owed loads nothing and leaves the unload to the caller", ctx do
    shape = store_shape(ctx, "stale", 1)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "stale")
    timeline_opts = timeline_opts(ctx)
    start_owner(ctx, timeline_opts: timeline_opts)

    assert {"stale", _offset} =
             ShapeCache.resolve_shape_handle(nil, shape, ctx.stack_id, read_only?: true)

    store_shape(ctx, "later", 2)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "later")
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    :ok = Timeline.store_irrecoverable_timeline(1, timeline_opts)
    assert :purge_owed = ShapeStatusOwner.refresh(ctx.stack_id)

    refute ShapeStatus.has_shape_handle?(ctx.stack_id, "later")
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, "stale")
    assert ShapeDb.handle_exists?(ctx.stack_id, "later")
  end

  # The owner saw the purge owed at its start. A read of the record that failed at the refresh
  # took the purge as done and loaded the shapes it still owes, served read-only until the manager
  # checked the timeline.
  test "a refresh that cannot read the stored timeline leaves the metadata unloaded", ctx do
    shape = store_shape(ctx, "stale", 1)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "stale")
    timeline_opts = timeline_opts(ctx)
    :ok = Timeline.store_irrecoverable_timeline(1, timeline_opts)
    start_owner_owing_a_purge(ctx, timeline_opts: timeline_opts)
    refute ShapeStatus.has_shape_handle?(ctx.stack_id, "stale")

    key = "timeline_id_#{ctx.stack_id}"
    :ok = Electric.PersistentKV.set(timeline_opts[:persistent_kv], key, "not a timeline")
    assert Timeline.load_timeline(timeline_opts) == :unreadable

    assert :purge_owed = ShapeStatusOwner.refresh(ctx.stack_id)

    refute ShapeStatus.has_shape_handle?(ctx.stack_id, "stale")
    assert is_nil(ShapeCache.resolve_shape_handle(nil, shape, ctx.stack_id, read_only?: true))
  end

  # The owner skipped the load and marked the metadata ready all the same: the health check
  # reported the stack waiting, which serves shapes read-only, while requests waited for it to be
  # active.
  test "a start that finds a purge owed reports the stack starting until it is opened", ctx do
    timeline_opts = timeline_opts(ctx)
    :ok = Timeline.store_irrecoverable_timeline(1, timeline_opts)
    owner = start_owner_owing_a_purge(ctx, timeline_opts: timeline_opts)

    :ok = StatusMonitor.reopen_shape_metadata(ctx.stack_id, owner)
    :ok = StatusMonitor.wait_for_messages_to_be_processed(ctx.stack_id)

    refute StatusMonitor.shape_metadata_closed?(ctx.stack_id)
    assert StatusMonitor.service_status(ctx.stack_id) == :waiting
  end

  # A refresh that finds the purge owed leaves the metadata loaded at boot to its caller, which can
  # die before it acts on the reply: the metadata was served read-only to the manager that took
  # over, until its own check.
  test "a refresh that finds a purge owed stops serving the metadata loaded at boot", ctx do
    store_shape(ctx, "stale", 1)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "stale")
    timeline_opts = timeline_opts(ctx)
    start_owner(ctx, timeline_opts: timeline_opts)
    :ok = Timeline.store_irrecoverable_timeline(1, timeline_opts)

    assert :purge_owed = ShapeStatusOwner.refresh(ctx.stack_id)

    assert ShapeStatus.has_shape_handle?(ctx.stack_id, "stale")
    assert_closed_for_the_purge(ctx)
  end

  # The connection manager stops the shape subsystem while a refresh leaves the metadata
  # unloaded, until one loads it again. That one took the cleanup of a refresh beside running
  # consumers, through the collector, which was not running: it left on disk the storage of every
  # shape gone from the catalog.
  test "a refresh after an unloaded one cleans the storage as a first one does", ctx do
    store_shape(ctx, "pending", 1)
    pending_dir = storage_dir(ctx, "pending")
    orphan_dir = storage_dir(ctx, "orphan")
    {_module, storage_opts} = ctx.storage
    timeline_opts = timeline_opts(ctx)
    :ok = Timeline.store_irrecoverable_timeline(1, timeline_opts)
    start_owner_owing_a_purge(ctx, timeline_opts: timeline_opts)

    assert :purge_owed = ShapeStatusOwner.refresh(ctx.stack_id)
    assert Enum.all?([pending_dir, orphan_dir], &File.dir?/1)

    :ok = Timeline.store_timeline({1, 1}, timeline_opts)
    :ok = ShapeStatusOwner.refresh(ctx.stack_id)

    refute ShapeDb.handle_exists?(ctx.stack_id, "pending")
    refute File.exists?(storage_opts.base_path)
  end

  defp timeline_opts(ctx),
    do: [stack_id: ctx.stack_id, persistent_kv: Electric.PersistentKV.Memory.new!()]

  defp store_shape(ctx, handle, id) do
    shape = Shape.new!("items", inspector: @inspector, where: "id = #{id}")
    {:ok, _digest} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
    shape
  end

  defp storage_dir(ctx, handle) do
    {PureFileStorage, opts} = ctx.storage
    dir = PureFileStorage.shape_data_dir(opts.base_path, handle)
    File.mkdir_p!(dir)
    dir
  end

  defp start_owner(ctx, opts \\ []) do
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    start_supervised!({ShapeStatusOwner, [stack_id: ctx.stack_id] ++ opts})

    assert {:ok, :read_only} =
             StatusMonitor.wait_until(ctx.stack_id, :read_only,
               timeout: ExUnit.configuration()[:assert_receive_timeout]
             )
  end

  # The metadata is marked ready once the owner's start is done, and serves nothing.
  defp start_owner_owing_a_purge(ctx, opts) do
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    owner = start_supervised!({ShapeStatusOwner, [stack_id: ctx.stack_id] ++ opts})
    _state = :sys.get_state(owner)
    :ok = StatusMonitor.wait_for_messages_to_be_processed(ctx.stack_id)

    assert_closed_for_the_purge(ctx)
    owner
  end

  defp assert_closed_for_the_purge(%{stack_id: stack_id}) do
    assert StatusMonitor.shape_metadata_closed?(stack_id)
    assert StatusMonitor.status(stack_id).shape == :starting
    assert StatusMonitor.service_status(stack_id) == :starting
  end
end
