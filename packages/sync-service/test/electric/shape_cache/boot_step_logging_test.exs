defmodule Electric.ShapeCache.BootStepLoggingTest do
  # A restart that was silent before the HTTP port opened, and between two later log lines,
  # could not be attributed to a step. Each step that reads the whole shape store or the whole
  # storage tree must report its own duration.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Support.ComponentSetup

  alias Electric.Replication.ShapeLogCollector
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

  setup [:with_stack_id_from_test, :with_async_deleter]

  defp took(step), do: ~r/Boot step #{Regex.escape(step)} took \d+ms/

  @tag shape_db_opts: [exclusive_mode: true]
  test "opening the shape store reports the integrity check", ctx do
    log = capture_log(fn -> with_shape_db(ctx) end)

    assert log =~ took("shape_db.integrity_check")
  end

  describe "shape metadata" do
    setup [:with_persistent_kv, :with_pure_file_storage, :with_shape_db, :with_one_shape]

    test "initialize/1 reports each walk of the store", ctx do
      log = capture_log(fn -> :ok = ShapeStatus.initialize(ctx.stack_id) end)

      assert log =~ took("shape_status.initialize.populate_shape_meta_table")
    end

    test "refresh/1 reports each walk of the store and the scan of the storage tree", ctx do
      log = capture_log(fn -> :ok = ShapeStatus.refresh(ctx.stack_id) end)

      assert log =~ took("shape_status.refresh.validate_existing_shapes")
      assert log =~ took("shape_status.refresh.refresh_shape_routing_state")
      assert log =~ took("shape_status.refresh.populate_shape_meta_table")
      assert log =~ took("shape_status.refresh.reclaim_orphaned_shape_storage")
    end
  end

  describe "shape log collector" do
    setup [:with_persistent_kv, :with_pure_file_storage, :with_shape_db, :with_one_shape]

    test "reports the arena scan and each walk of the restore", ctx do
      log =
        capture_log(fn ->
          start_supervised!(
            {ShapeLogCollector.Supervisor,
             [
               stack_id: ctx.stack_id,
               inspector: @inspector,
               persistent_kv: ctx.persistent_kv,
               shared_conversion: true,
               shared_payload_arena: true
             ]},
            restart: :temporary
          )

          # answered only once the restore, which runs in the collector's `handle_continue`, is over
          _ = :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
        end)

      assert log =~ took("shape_log_collector.reclaim_arena_generations")
      assert log =~ took("shape_log_collector.prune_subquery_shapes")
      assert log =~ took("shape_log_collector.invalidate_shapes_without_journal_provenance")
      assert log =~ took("shape_log_collector.restore_filters")
      assert log =~ took("shape_log_collector.restore_routing_state")
      assert log =~ "Restored filters for 1 shapes"
    end
  end

  # One stored shape, so that no walk takes the empty-store shortcut.
  defp with_one_shape(ctx) do
    :ok = ShapeStatus.initialize(ctx.stack_id)

    shape = Shape.new!("items", inspector: @inspector, where: "id = 1")
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeStatus.mark_snapshot_complete(ctx.stack_id, handle)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    %{shape_handle: handle}
  end
end
