defmodule Electric.ShapeCache.RefreshDetachesInvalidShapesTest do
  @moduledoc """
  A catalog refresh runs at lock acquisition with the shape subsystem already live. It deletes
  the rows of snapshot-incomplete shapes and must detach them the way any removal does -- stop
  the consumer and drop the log-collector entry -- because that entry pins the stack's ack and,
  through it, the upstream Postgres WAL. Routing those handles through a storage-only reclaim
  leaves a live consumer writing into an unlinked directory while its flush entry grows the WAL
  against a shape that no longer exists.

  This pins the routing decision in `refresh/1`: the invalid handles reach the detaching removal,
  never the storage-only one. The detaching removal's own behaviour (consumer stopped, entry
  dropped, storage reclaimed) is covered beside the real consumer in `shape_cleaner_test.exs`.
  """
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.TestUtils, only: [patch_calls: 2]

  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes.Shape

  @inspector Support.StubInspector.new(
               tables: [{1, {"public", "items"}}],
               columns: [
                 %{
                   name: "id",
                   type: "int8",
                   type_id: {20, 1},
                   pk_position: 0,
                   is_generated: false
                 },
                 %{name: "value", type: "text", type_id: {25, 1}, is_generated: false}
               ]
             )

  defmodule NoopStorage do
    def get_all_stored_shape_handles(_opts), do: {:ok, MapSet.new()}
    def cleanup_all!(_opts), do: :ok
  end

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_async_deleter, :with_shape_db]

  test "refresh detaches invalid shapes rather than only reclaiming their storage", ctx do
    Electric.StackConfig.put(ctx.stack_id, Electric.ShapeCache.Storage, {NoopStorage, []})
    :ok = ShapeStatus.initialize(ctx.stack_id)

    {:ok, complete} =
      ShapeStatus.add_shape(ctx.stack_id, Shape.new!("items", inspector: @inspector))

    {:ok, incomplete} =
      ShapeStatus.add_shape(
        ctx.stack_id,
        Shape.new!("items", inspector: @inspector, where: "value = 'mid-snapshot'")
      )

    # Only `complete` finished its snapshot, so `refresh/1` flags `incomplete` as invalid and
    # deletes its row -- the state a manager restart mid-snapshot leaves behind.
    :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(ctx.stack_id, complete)

    parent = self()

    patch_calls(ShapeCleaner,
      remove_detached_shapes_async: fn _stack_id, handles ->
        send(parent, {:detached, handles})
        :ok
      end,
      remove_shape_storage_async: fn _stack_id, handles ->
        send(parent, {:storage_only, handles})
        :ok
      end
    )

    :ok = ShapeStatus.refresh(ctx.stack_id)

    assert_receive {:detached, [^incomplete]},
                   ExUnit.configuration()[:assert_receive_timeout],
                   "an invalid shape must be detached (consumer stopped, flush entry released)"

    refute_received {:storage_only, [^incomplete]}
  end
end
