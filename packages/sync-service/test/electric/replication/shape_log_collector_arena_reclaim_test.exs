defmodule Electric.Replication.ShapeLogCollectorArenaReclaimTest do
  # Reclaiming payload-arena generations takes a scan of the log directory of every stored shape,
  # and the collector runs it in its init, before the stack can go active: minutes of a restart
  # with a large shape count, often with little to reclaim. A boot must not pay for the scan until
  # the generations left by earlier boots hold enough to be worth it.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import ExUnit.CaptureLog
  import Support.ComponentSetup

  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @moduletag :tmp_dir

  @inspector StubInspector.new(
               tables: ["items"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "value", type: "text"}
               ]
             )

  setup [
    :with_stack_id_from_test,
    :with_async_deleter,
    :with_persistent_kv,
    :with_pure_file_storage,
    :with_shape_db,
    :with_one_shape,
    :with_earlier_generations
  ]

  test "a boot keeps what earlier generations hold while it is under the threshold", ctx do
    log = capture_log(fn -> boot_collector(ctx) end)

    assert File.exists?(ctx.unreferenced)
    assert log =~ "Arena generation reclaim left for a later boot"
  end

  test "a boot under the threshold still prunes the generations that hold nothing", ctx do
    boot_collector(ctx)

    refute File.exists?(ctx.empty)
  end

  test "a boot reclaims an unreferenced generation once the threshold is reached", ctx do
    boot_collector(ctx, payload_arena_reclaim_min_bytes: 1)

    refute File.exists?(ctx.unreferenced)
    refute File.exists?(ctx.empty)
  end

  test "a threshold of zero reclaims on every boot", ctx do
    boot_collector(ctx, payload_arena_reclaim_min_bytes: 0)

    refute File.exists?(ctx.unreferenced)
  end

  # A response begun before a restart still reads a removed shape's log, and resolves its refs. A
  # cleanup that moves a shape away after the scan listed it fails the scan, and the boot prunes
  # only the empty generations. Whichever shape the scan reads first, the cleanup removes the other
  # during that read.
  test "a boot keeps the generation of a shape a cleanup removes during its scan", ctx do
    {first_dir, first_gen} = write_ref_shape(ctx, "first-shape-handle", 3)
    {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 4)
    test_pid = self()

    Repatch.patch(LogFile, :arena_generations_in_log, [mode: :shared], fn path ->
      for dir <- [first_dir, second_dir],
          not String.starts_with?(path, dir <> "/"),
          File.exists?(dir) do
        File.rm_rf!(dir)
        send(test_pid, {:removed, dir})
      end

      Repatch.real(LogFile.arena_generations_in_log(path))
    end)

    Support.TestUtils.activate_mocks_for_descendant_procs(ShapeLogCollector)

    log = capture_log(fn -> boot_collector(ctx, payload_arena_reclaim_min_bytes: 1) end)

    assert_received {:removed, removed_dir}
    refute_received {:removed, _dir}
    assert File.exists?(first_gen)
    assert File.exists?(second_gen)
    refute File.exists?(ctx.empty)

    skipped = %File.Error{
      reason: :enoent,
      action: "list directory",
      path: Path.join(removed_dir, "log")
    }

    assert log =~ "Arena generation reclaim skipped, pruning empty only: #{inspect(skipped)}"
  end

  # A remount that takes the store away leaves the directory under its mount point, empty say,
  # which lists without error. Read as a store with no shape, it gave the reclaim an empty keep set,
  # and the store back for the reclaim lost every generation a shape still reads from.
  test "a boot keeps the generations when its scan lists the store root without the arena", ctx do
    {_dir, gen} = write_ref_shape(ctx, "ref-shape-handle", 3)
    {PureFileStorage, %{base_path: base_path}} = ctx.storage

    Repatch.patch(File, :ls, [mode: :shared], fn
      ^base_path ->
        File.rename!(base_path, base_path <> ".away")
        File.mkdir!(base_path)
        listing = Repatch.real(File.ls(base_path))
        File.rmdir!(base_path)
        File.rename!(base_path <> ".away", base_path)
        listing

      path ->
        Repatch.real(File.ls(path))
    end)

    Support.TestUtils.activate_mocks_for_descendant_procs(ShapeLogCollector)

    log = capture_log(fn -> boot_collector(ctx, payload_arena_reclaim_min_bytes: 1) end)

    assert File.exists?(gen)
    refute File.exists?(ctx.empty)
    assert log =~ "Arena generation reclaim skipped, pruning empty only"
    assert log =~ "lists none of payload-arena"
  end

  defp boot_collector(ctx, opts \\ []) do
    start_supervised!(
      {ShapeLogCollector.Supervisor,
       [
         stack_id: ctx.stack_id,
         inspector: @inspector,
         persistent_kv: ctx.persistent_kv,
         shared_conversion: true,
         shared_payload_arena: true
       ] ++ opts},
      restart: :temporary
    )

    # answered only once the collector's init is over
    _ = :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
  end

  # One stored shape for the collector to restore.
  defp with_one_shape(ctx) do
    :ok = ShapeStatus.initialize(ctx.stack_id)

    shape = Shape.new!("items", inspector: @inspector, where: "id = 1")
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeStatus.mark_snapshot_complete(ctx.stack_id, handle)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    :ok
  end

  # A stored shape whose log holds a ref into a generation of its own: its directory, and the
  # generation's file.
  defp write_ref_shape(ctx, shape_handle, discriminator) do
    {PureFileStorage, base_opts} = ctx.storage
    shape = Shape.new!("items", inspector: @inspector, where: "id = 1")
    opts = PureFileStorage.for_shape(shape_handle, base_opts)

    writer = PureFileStorage.init_writer!(opts, shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    arena = PayloadArena.open_writer!(base_opts.base_path, discriminator)
    {arena, ref} = PayloadArena.append!(arena, "k1", ~S|{"n":1}|)
    PayloadArena.sync!(arena)

    writer =
      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "k1", :insert, ~S|{"n":1}|, ref}],
        writer
      )

    PureFileStorage.terminate(writer)

    {PureFileStorage.shape_data_dir(opts),
     Path.join([base_opts.base_path, "payload-arena", "#{arena.gen}.bin"])}
  end

  # What two writers of an earlier boot leave behind: one generation holding a payload no shape
  # log refers to, and one that was opened and never appended to.
  defp with_earlier_generations(ctx) do
    {_storage, %{base_path: base_path}} = ctx.storage

    writer = PayloadArena.open_writer!(base_path, 1)
    {writer, _ref} = PayloadArena.append!(writer, "orphan", ~S|{"n":99}|)
    PayloadArena.sync!(writer)

    empty = PayloadArena.open_writer!(base_path, 2)

    dir = Path.join(base_path, "payload-arena")

    %{
      unreferenced: Path.join(dir, "#{writer.gen}.bin"),
      empty: Path.join(dir, "#{empty.gen}.bin")
    }
  end
end
