defmodule Electric.ShapeCache.RefreshKeepsInflightShapeStorageTest do
  @moduledoc """
  A catalog refresh runs at lock acquisition with the shape subsystem already live, so a shape can
  be created concurrently while it runs. `refresh/1` rebuilds the in-memory meta table from
  `reduce_shape_meta`, which reads persisted SQLite alone and ignores the write buffer, then
  reclaims every stored directory whose handle is absent from that table. A create whose write
  landed in the buffer after `refresh/1`'s flush is therefore missing from the rebuilt table while
  its create is still in flight -- and its directory must not be reclaimed, or the shape ends up
  registered with no data once the buffer flushes.

  This pins that guard: a directory whose handle the authoritative catalog (write buffer, then
  SQLite) still knows is left alone, telling "not yet in the partial rebuild" apart from "genuinely
  removed". A live consumer is not required for the leak, so none is started here; the consumer
  path is covered by `RefreshDetachesInvalidShapesTest`.
  """
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.TestUtils, only: [patch_calls: 2]

  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
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

  defmodule StoredHandlesStorage do
    def get_all_stored_shape_handles(opts),
      do: {:ok, MapSet.new(Keyword.fetch!(opts, :stored_handles))}

    def cleanup_all!(_opts), do: :ok
  end

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_async_deleter, :with_shape_db]

  test "refresh keeps storage for an in-flight shape buffered but absent from the rebuilt table",
       ctx do
    shape = Shape.new!("items", inspector: @inspector, where: "value = 'in-flight'")
    {_hash, inflight} = Shape.generate_id(shape)

    Electric.StackConfig.put(
      ctx.stack_id,
      Electric.ShapeCache.Storage,
      {StoredHandlesStorage, stored_handles: [inflight]}
    )

    :ok = ShapeStatus.initialize(ctx.stack_id)

    # The create's write reaches the buffer after refresh's flush would have run, so it never lands
    # in SQLite: buffered, no meta row, its directory already on disk. A no-op flush models that
    # ordering deterministically -- refresh's flush cannot capture a write that arrives later.
    {:ok, _digest} = ShapeDb.add_shape(ctx.stack_id, shape, inflight)
    patch_calls(WriteBuffer, flush_sync: fn _stack_id, _timeout -> :ok end)

    parent = self()

    patch_calls(ShapeCleaner,
      remove_shape_storage_async: fn _stack_id, handles ->
        send(parent, {:reclaimed_storage, handles})
        :ok
      end,
      remove_detached_shapes_async: fn _stack_id, handles ->
        send(parent, {:detached, handles})
        :ok
      end
    )

    :ok = ShapeStatus.refresh(ctx.stack_id)

    assert WriteBuffer.has_handle?(ctx.stack_id, inflight) == true,
           "precondition: the in-flight shape must still be buffered, not persisted to SQLite"

    refute_received {:reclaimed_storage, _},
                    "a live shape still known to the write buffer must keep its storage"
  end
end
