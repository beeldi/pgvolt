defmodule Electric.ShapeCache.ShapeStatusRoutingStateTest do
  # The routing state, one row per shape, takes a read of the stored shapes to build, which
  # decodes every one of them: seconds with a large shape count. The collector's restore
  # reads them anyway, so a boot builds the routing state from that read rather than from one of
  # its own, and until then it is pending.
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

    # as a boot finds them
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)

    parent = self()

    Repatch.patch(ShapeDb, :reduce_shapes, fn stack_id, acc, fun ->
      send(parent, :read_of_the_stored_shapes)
      Repatch.real(ShapeDb.reduce_shapes(stack_id, acc, fun))
    end)

    %{indexed: indexed, unindexed: unindexed}
  end

  test "a boot leaves the routing state to the collector's read of the stored shapes", ctx do
    assert :error = ShapeStatus.shape_counts(ctx.stack_id)

    # from the shape metadata, which holds the same handles: the store's own count would wait
    # for the collector's read to give its connection back
    assert ShapeStatus.count_shapes(ctx.stack_id) == 2
    refute_received :read_of_the_stored_shapes

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))

    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)

    assert Enum.sort([
             {ctx.indexed, {{1, {"public", "items"}}, false}},
             {ctx.unindexed, {{2, {"public", "other_table"}}, false}}
           ]) == publication_filters(ctx)

    # each shape is counted down when it goes
    :ok = ShapeStatus.remove_shape(ctx.stack_id, ctx.indexed)
    assert %{total: 1, indexed: 0, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  test "a refresh before that read leaves the routing state to it", ctx do
    :ok = ShapeStatus.refresh(ctx.stack_id)

    refute_received :read_of_the_stored_shapes
    assert :error = ShapeStatus.shape_counts(ctx.stack_id)

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))
    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  test "a shape removed after that read is neither counted nor kept", ctx do
    routing_state = read_routing_state(ctx)

    :ok = ShapeStatus.remove_shape(ctx.stack_id, ctx.indexed)
    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, routing_state)

    assert %{total: 1, indexed: 0, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)
    assert [{ctx.unindexed, {{2, {"public", "other_table"}}, false}}] == publication_filters(ctx)
  end

  # A restore runs again when the shape subsystem restarts in active mode, where `add_shape/2`
  # and `remove_shape/2` keep the routing state.
  test "a routing state already restored is kept as it is", ctx do
    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))
    added = store_shape!(ctx, indexed_shape("b"))
    :ok = ShapeStatus.remove_shape(ctx.stack_id, ctx.unindexed)

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, ShapeStatus.new_routing_state())

    assert %{total: 2, indexed: 2, unindexed: 0} = ShapeStatus.shape_counts(ctx.stack_id)
    assert [ctx.indexed, added] |> Enum.sort() == publication_filters(ctx) |> handles()
  end

  # The connection manager unloads the shape metadata while the timeline record cannot be read,
  # and stops the shape subsystem until a load or a purge. A relation tracker that restarts in
  # between restores from the routing state, and published an empty filter set from an unload's.
  test "an unload leaves the routing state pending for the collector's next restore", ctx do
    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))
    :ok = ShapeStatus.unload(ctx.stack_id)

    waiter = Task.async(fn -> ShapeStatus.await_routing_state(ctx.stack_id) end)
    refute Task.yield(waiter, 100)
    assert :error = ShapeStatus.shape_counts(ctx.stack_id)
    assert ShapeStatus.count_shapes(ctx.stack_id) == 0

    :ok = ShapeStatus.refresh(ctx.stack_id)
    refute Task.yield(waiter, 100)
    assert ShapeStatus.count_shapes(ctx.stack_id) == 2

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))

    assert :ok = Task.await(waiter)
    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(ctx.stack_id)
    assert Enum.sort([ctx.indexed, ctx.unindexed]) == publication_filters(ctx) |> handles()
  end

  # A restart of the relation tracker submits each shape's filter under the name the shape was
  # stored with, and a shape stored before its table was renamed is found stale by that name.
  test "a shape added under a table's new name leaves the old name to the shapes stored with it",
       ctx do
    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))
    renamed = store_shape!(ctx, renamed_shape("b"))

    assert Enum.sort([
             {ctx.indexed, {{1, {"public", "items"}}, false}},
             {ctx.unindexed, {{2, {"public", "other_table"}}, false}},
             {renamed, {{1, {"public", "renamed_items"}}, false}}
           ]) == publication_filters(ctx)

    :ok = ShapeStatus.remove_shape(ctx.stack_id, renamed)

    assert Enum.sort([
             {ctx.indexed, {{1, {"public", "items"}}, false}},
             {ctx.unindexed, {{2, {"public", "other_table"}}, false}}
           ]) == publication_filters(ctx)
  end

  test "a boot gives each shape on a renamed table the name it was stored with", ctx do
    renamed = store_shape!(ctx, renamed_shape("b"))
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))

    assert Enum.sort([
             {ctx.indexed, {{1, {"public", "items"}}, false}},
             {ctx.unindexed, {{2, {"public", "other_table"}}, false}},
             {renamed, {{1, {"public", "renamed_items"}}, false}}
           ]) == publication_filters(ctx)
  end

  # The relation tracker keeps the first name it meets for a table, and the read of the stored
  # shapes it restored from met them in handle order: a table renamed while shapes stored under
  # both names remain went by the name of its shape with the lowest handle, whichever that is.
  test "the relation tracker meets each renamed table under its lowest handle's name first",
       ctx do
    {before_the_boot, since_the_boot} = Enum.split(11..30, 10)

    for oid <- before_the_boot, do: store_shapes_on_renamed_table!(ctx, oid)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)
    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))
    for oid <- since_the_boot, do: store_shapes_on_renamed_table!(ctx, oid)

    first_names =
      ShapeStatus.reduce_publication_filters(ctx.stack_id, %{}, fn
        {_handle, {{oid, relation}, _with_generated_cols?}}, first_names ->
          Map.put_new(first_names, oid, relation)
      end)

    assert Map.new(11..30, &{&1, lowest_handle_name(&1)})
           |> Map.merge(%{1 => {"public", "items"}, 2 => {"public", "other_table"}}) ==
             first_names
  end

  # The collector's restore carries the routing state through its read, and every garbage
  # collection of the collector copies what that read's accumulator holds.
  test "the routing state a read builds keeps its rows out of the reading process's heap" do
    shape = indexed_shape("a")

    [one, thousand] =
      for n <- [1, 1_000] do
        Enum.reduce(1..n, ShapeStatus.new_routing_state(), fn i, routing_state ->
          ShapeStatus.add_to_routing_state(routing_state, "handle-#{i}", shape)
        end)
      end

    assert :erts_debug.flat_size(thousand) == :erts_debug.flat_size(one)
  end

  test "await_routing_state/1 returns once the routing state is restored", ctx do
    waiter = Task.async(fn -> ShapeStatus.await_routing_state(ctx.stack_id) end)
    refute Task.yield(waiter, 100)

    :ok = ShapeStatus.restore_routing_state(ctx.stack_id, read_routing_state(ctx))

    assert :ok = Task.await(waiter)
    assert :ok = ShapeStatus.await_routing_state(ctx.stack_id)
  end

  test "await_routing_state/1 returns once a reset empties the store", ctx do
    waiter = Task.async(fn -> ShapeStatus.await_routing_state(ctx.stack_id) end)
    refute Task.yield(waiter, 100)

    :ok = ShapeStatus.reset(ctx.stack_id)

    assert :ok = Task.await(waiter)
    assert %{total: 0, indexed: 0, unindexed: 0} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  test "a boot on an empty store has nothing to wait for", ctx do
    :ok = ShapeStatus.reset(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)

    assert :ok = ShapeStatus.await_routing_state(ctx.stack_id)
    assert %{total: 0, indexed: 0, unindexed: 0} = ShapeStatus.shape_counts(ctx.stack_id)
  end

  defp read_routing_state(ctx) do
    ShapeStatus.reduce_shapes(ctx.stack_id, ShapeStatus.new_routing_state(), fn
      {handle, shape}, routing_state ->
        ShapeStatus.add_to_routing_state(routing_state, handle, shape)
    end)
  end

  defp publication_filters(ctx) do
    ctx.stack_id
    |> ShapeStatus.reduce_publication_filters([], &[&1 | &2])
    |> Enum.sort()
  end

  defp handles(publication_filters), do: Enum.map(publication_filters, &elem(&1, 0))

  defp store_shape!(ctx, shape) do
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
    handle
  end

  defp indexed_shape(value) do
    Shape.new!("items", inspector: @inspector, where: "value = '#{value}'")
  end

  defp unindexed_shape, do: Shape.new!("public.other_table", inspector: @inspector)

  # a shape on the same table as `indexed_shape/1`, created after that table was renamed
  defp renamed_shape(value),
    do: %{indexed_shape(value) | root_table: {"public", "renamed_items"}}

  # A shape on table `oid` under its old name, then one under its new name. The one under the old
  # name has the lower handle on even tables, the one under the new name on odd ones.
  defp store_shapes_on_renamed_table!(ctx, oid) do
    {old, new} = if rem(oid, 2) == 0, do: {"#{oid}-1", "#{oid}-2"}, else: {"#{oid}-2", "#{oid}-1"}

    for {handle, name} <- [{old, "items_#{oid}"}, {new, "renamed_items_#{oid}"}] do
      Repatch.patch(Shape, :generate_id, [force: true], fn shape ->
        {Shape.hash(shape), handle}
      end)

      shape = %{indexed_shape("a") | root_table_id: oid, root_table: {"public", name}}
      ^handle = store_shape!(ctx, shape)
    end
  end

  defp lowest_handle_name(oid) when rem(oid, 2) == 0, do: {"public", "items_#{oid}"}
  defp lowest_handle_name(oid), do: {"public", "renamed_items_#{oid}"}
end
