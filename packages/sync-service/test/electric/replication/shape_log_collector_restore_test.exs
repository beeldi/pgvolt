defmodule Electric.Replication.ShapeLogCollectorRestoreTest do
  # The collector's restore spreads the stored shapes over several processes, by the root table
  # the store files them under (`ShapeLogCollector.Restore`). What it builds must be what the
  # collector built alone, one shape after the other.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup

  alias Electric.Replication.Changes
  alias Electric.Replication.ShapeLogCollector
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  @columns [%{name: "id", type: "int8", pk_position: 0}]

  # enough tables for every restore process to have some
  @tables for n <- 1..12, do: {16_000 + n, {"public", "table_#{n}"}}

  # a table dropped and created again under the same name, with shapes stored on both
  @recreated [{17_001, {"public", "recreated"}}, {17_002, {"public", "recreated"}}]

  @inspector Support.StubInspector.new(tables: @tables ++ @recreated, columns: @columns)

  setup [
    :with_stack_id_from_test,
    :with_pure_file_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_noop_publication_manager,
    :with_persistent_kv
  ]

  setup do
    Support.TestUtils.activate_mocks_for_descendant_procs(ShapeLogCollector)
    :ok
  end

  test "restores the shapes of every table, whichever process restores them", ctx do
    stored =
      for {_oid, {_schema, name} = table} <- @tables, id <- [nil, 1, 2, 3] do
        where = if id, do: "id = #{id}"
        {store_shape!(ctx, Shape.new!(name, where: where, inspector: @inspector)), table, id}
      end

    handles = MapSet.new(stored, &elem(&1, 0))
    state = restore(ctx)

    assert MapSet.new(ShapeLogCollector.active_shapes(ctx.stack_id)) == handles
    assert state.subscriptions == MapSet.size(handles)
    assert MapSet.new(Map.keys(state.shape_owners)) == handles
    assert state.dependency_layers == [handles]
    assert %{total: total} = ShapeStatus.shape_counts(ctx.stack_id)
    assert total == MapSet.size(handles)

    # every change reaches the shapes of its table that it matches, and no other
    for {_oid, table} <- @tables, id <- 1..4 do
      change = %Changes.NewRecord{relation: table, record: %{"id" => "#{id}"}}

      expected =
        for {handle, ^table, shape_id} <- stored,
            shape_id in [nil, id],
            into: MapSet.new(),
            do: handle

      assert Filter.affected_shapes(state.event_router.filter, change) == expected
    end

    assert state.partitions.partition_ownership ==
             stored
             |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
             |> Map.new(fn {table, handles} -> {table, MapSet.new(handles)} end)

    # a shard worker holds the shapes routed to it, before the collector sends it anything
    if state.workers do
      for {pid, index} <- Enum.with_index(Tuple.to_list(state.workers)) do
        assert pid |> :sys.get_state() |> Map.fetch!(:shapes) |> Map.keys() |> MapSet.new() ==
                 for({handle, ^index} <- state.shape_owners, into: MapSet.new(), do: handle)
      end
    end
  end

  # A process analyses a shape's clause once, for both its routing state, indexed or not, and the
  # filter: each shape must get its own analysis in both.
  test "restores each shape indexed or not, and routed, as its clause says", ctx do
    clauses = [
      {nil, fn _id -> true end},
      {"id = 1", &(&1 == 1)},
      {"id > 1", &(&1 > 1)},
      {"id IN (1, 2)", &(&1 in [1, 2])},
      {"id = 1 OR id > 2", &(&1 == 1 or &1 > 2)},
      {"id > 1 AND id = 3", &(&1 == 3)}
    ]

    stored =
      for {_oid, {_schema, name} = table} <- @tables, {where, matches?} <- clauses do
        shape = Shape.new!(name, where: where, inspector: @inspector)
        {store_shape!(ctx, shape), table, Filter.indexed_shape?(shape), matches?}
      end

    # as a boot finds them, which leaves the routing state, and so the counts, to the restore
    :ok = ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
    :ok = ShapeStatus.initialize(ctx.stack_id)
    assert :error = ShapeStatus.shape_counts(ctx.stack_id)

    state = restore(ctx)

    indexed = Enum.count(stored, &elem(&1, 2))

    assert ShapeStatus.shape_counts(ctx.stack_id) == %{
             total: length(stored),
             indexed: indexed,
             unindexed: length(stored) - indexed
           }

    for {_oid, table} <- @tables, id <- 1..4 do
      change = %Changes.NewRecord{relation: table, record: %{"id" => "#{id}"}}

      expected =
        for {handle, ^table, _indexed?, matches?} <- stored,
            matches?.(id),
            into: MapSet.new(),
            do: handle

      assert Filter.affected_shapes(state.event_router.filter, change) == expected
    end
  end

  # A table's conditions in the filter are keyed by its name, and only one process may write them.
  test "restores the shapes of two tables under one name", ctx do
    [first, second] =
      for table <- @recreated do
        Support.StubInspector.new(tables: [table], columns: @columns)
      end

    stored = [
      store_shape!(ctx, Shape.new!("recreated", where: "id = 1", inspector: first)),
      store_shape!(ctx, Shape.new!("recreated", where: "id = 1", inspector: second)),
      store_shape!(ctx, Shape.new!("recreated", where: "id = 2", inspector: second)),
      store_shape!(ctx, Shape.new!("recreated", inspector: first))
    ]

    [first_1, second_1, second_2, first_all] = stored
    state = restore(ctx)

    assert MapSet.new(ShapeLogCollector.active_shapes(ctx.stack_id)) == MapSet.new(stored)
    assert state.subscriptions == 4

    change = fn id ->
      %Changes.NewRecord{relation: {"public", "recreated"}, record: %{"id" => "#{id}"}}
    end

    assert Filter.affected_shapes(state.event_router.filter, change.(1)) ==
             MapSet.new([first_1, second_1, first_all])

    assert Filter.affected_shapes(state.event_router.filter, change.(2)) ==
             MapSet.new([second_2, first_all])

    assert state.partitions.partition_ownership == %{
             {"public", "recreated"} => MapSet.new(stored)
           }
  end

  # The store files a shape under its root table alone. A shape filed under another is still
  # restored as a shape on its root table.
  test "restores a shape the store files under another table", ctx do
    [{_, {_, name}}, {other_oid, other_table} | _] = @tables
    misfiled = store_shape!(ctx, Shape.new!(name, where: "id = 1", inspector: @inspector))
    other = store_shape!(ctx, Shape.new!(elem(other_table, 1), inspector: @inspector))
    :ok = ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
    execute!(ctx, "UPDATE relations SET oid = #{other_oid} WHERE handle = #{blob(misfiled)}")

    state = restore(ctx)

    assert MapSet.new(ShapeLogCollector.active_shapes(ctx.stack_id)) ==
             MapSet.new([misfiled, other])

    assert Filter.affected_shapes(
             state.event_router.filter,
             %Changes.NewRecord{relation: {"public", name}, record: %{"id" => "1"}}
           ) == MapSet.new([misfiled])

    assert state.partitions.partition_ownership == %{
             {"public", name} => MapSet.new([misfiled]),
             other_table => MapSet.new([other])
           }
  end

  # The processes decode the shapes: what fails them must fail the restore as it failed the
  # collector, rather than leave it waiting on them.
  test "a stored shape that cannot be decoded fails the restore", ctx do
    [{_, {_, name}} | _] = @tables
    store_shape!(ctx, Shape.new!(name, inspector: @inspector))
    broken = store_shape!(ctx, Shape.new!(name, where: "id = 1", inspector: @inspector))
    :ok = ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
    execute!(ctx, "UPDATE shapes SET shape = x'00' WHERE handle = #{blob(broken)}")

    ExUnit.CaptureLog.capture_log(fn ->
      start_collector(ctx)
      collector = Process.monitor(GenServer.whereis(ShapeLogCollector.name(ctx.stack_id)))

      assert_receive {:DOWN, ^collector, :process, _pid,
                      {:badarg, [{:erlang, :binary_to_term, [<<0>>], _} | _]}},
                     10_000
    end)
  end

  # Returns the collector's state once its restore is over.
  defp restore(ctx) do
    :ok = ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
    start_collector(ctx)
    :sys.get_state(ShapeLogCollector.name(ctx.stack_id))
  end

  defp start_collector(ctx) do
    start_supervised!(
      {ShapeLogCollector.Supervisor,
       [
         stack_id: ctx.stack_id,
         inspector: @inspector,
         persistent_kv: ctx.persistent_kv,
         router_shards: 2,
         shared_conversion: true,
         worker_prepare: true
       ]},
      restart: :temporary
    )
  end

  defp store_shape!(ctx, shape) do
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
    handle
  end

  defp execute!(ctx, sql) do
    :ok =
      ShapeDb.Connection.checkout_write!(ctx.stack_id, :test, fn %{conn: conn} ->
        ShapeDb.Connection.execute(conn, sql)
      end)
  end

  defp blob(handle), do: "x'#{Base.encode16(handle)}'"
end
