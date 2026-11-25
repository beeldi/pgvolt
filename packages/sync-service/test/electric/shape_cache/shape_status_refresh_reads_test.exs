defmodule Electric.ShapeCache.ShapeStatusRefreshReadsTest do
  # A boot builds its routing state once, from the collector's read of the stored shapes, and a
  # later lock acquisition refreshes it. Reading the stored shapes decodes every one of them:
  # seconds with a large shape count. A refresh must not pay for that again when the
  # store holds the handles the boot saw.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup

  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape

  defmodule NoopStorage do
    def get_all_stored_shape_handles(_opts), do: {:ok, MapSet.new()}
    def cleanup_all!(_opts), do: :ok
  end

  @inspector Support.StubInspector.new(
               tables: [{1, {"public", "items"}}, {2, {"public", "other_table"}}],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "value", type: "text"}
               ]
             )

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_async_deleter, :with_shape_db]

  setup ctx do
    Electric.StackConfig.put(ctx.stack_id, Electric.ShapeCache.Storage, {NoopStorage, []})
    :ok = ShapeStatus.initialize(ctx.stack_id)

    indexed = store_shape!(ctx, indexed_shape("a"))
    unindexed = store_shape!(ctx, unindexed_shape())

    # as a boot leaves them: stored, and read once by the collector's restore, which builds the
    # routing state
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)

    routing_state =
      ShapeStatus.reduce_shapes(ctx.stack_id, ShapeStatus.new_routing_state(), fn
        {handle, shape}, routing_state ->
          ShapeStatus.add_to_routing_state(routing_state, handle, shape)
      end)

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, routing_state)

    parent = self()

    Repatch.patch(ShapeDb, :reduce_shapes, fn stack_id, acc, fun ->
      send(parent, :read_of_the_stored_shapes)
      Repatch.real(ShapeDb.reduce_shapes(stack_id, acc, fun))
    end)

    %{indexed: indexed, unindexed: unindexed}
  end

  test "a refresh of the store the boot saw does not read the stored shapes", ctx do
    :ok = ShapeStatus.refresh(ctx.stack_id)

    refute_received :read_of_the_stored_shapes
    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  test "a refresh forgets a shape another instance removed, without reading the stored shapes",
       ctx do
    :ok = ShapeDb.remove_shape(ctx.stack_id, ctx.indexed)

    :ok = ShapeStatus.refresh(ctx.stack_id)

    refute_received :read_of_the_stored_shapes
    assert %{total: 1, indexed: 0, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)

    # the shape it kept is still counted down when it goes
    :ok = ShapeStatus.remove_shape(ctx.stack_id, ctx.unindexed)
    assert %{total: 0, indexed: 0, unindexed: 0} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  test "a refresh reads the stored shapes for one another instance stored", ctx do
    {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, indexed_shape("b"), "stored-by-another")
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "stored-by-another")

    :ok = ShapeStatus.refresh(ctx.stack_id)

    assert_received :read_of_the_stored_shapes
    assert %{total: 3, indexed: 2, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)

    :ok = ShapeStatus.remove_shape(ctx.stack_id, "stored-by-another")
    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  defp store_shape!(ctx, shape) do
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
    handle
  end

  defp indexed_shape(value) do
    Shape.new!("items", inspector: @inspector, where: "value = '#{value}'")
  end

  defp unindexed_shape, do: Shape.new!("public.other_table", inspector: @inspector)
end
