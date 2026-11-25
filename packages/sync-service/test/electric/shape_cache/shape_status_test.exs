defmodule Electric.ShapeCache.ShapeStatusTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes.Shape

  import Support.ComponentSetup

  # Minimal storage module that avoids Repatch shared-mode mocking.
  # Using a real module instead of {Mock.Storage, []} prevents crashes when
  # Repatch cleanup from concurrent async tests removes shared hooks.
  #
  # cleanup_all! notifies the test process during a synchronous refresh.
  defmodule NoopStorage do
    def get_all_stored_shape_handles(_opts), do: {:ok, MapSet.new()}

    def cleanup_all!(_opts) do
      if pid = Process.get(:cleanup_all_test_pid) do
        send(pid, :cleanup_all_called)
      end

      :ok
    end
  end

  @inspector Support.StubInspector.new(
               tables: [{1, {"public", "items"}}, {2, {"public", "other_table"}}],
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

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_async_deleter, :with_shape_db]

  test "starts empty", ctx do
    {:ok, state, []} = new_state(ctx)
    assert [] = ShapeStatus.list_shapes(state)
  end

  test "disabling full transaction provenance invalidates only those persisted shapes", ctx do
    legacy = shape!()
    full = %{legacy | source_positions: :full_xid, source_generation: "source-generation-a"}
    {:ok, state, [legacy_handle, full_handle]} = new_state(ctx, shapes: [legacy, full])
    ShapeStatus.ShapeDb.mark_snapshot_complete(state, legacy_handle)
    ShapeStatus.ShapeDb.mark_snapshot_complete(state, full_handle)
    Electric.StackConfig.put(state, :feature_flags, ["full_xid_source_positions"])
    assert {:ok, [], 2} = ShapeStatus.ShapeDb.validate_existing_shapes(state)
    Electric.StackConfig.put(state, :feature_flags, [])
    assert {:ok, [^full_handle], 1} = ShapeStatus.ShapeDb.validate_existing_shapes(state)
    assert ShapeStatus.ShapeDb.handle_exists?(state, legacy_handle)
    refute ShapeStatus.ShapeDb.handle_exists?(state, full_handle)
    Electric.StackConfig.put(state, :feature_flags, ["full_xid_source_positions"])
    assert {:ok, [], 1} = ShapeStatus.ShapeDb.validate_existing_shapes(state)
  end

  test "deletes orphaned shape data for an empty catalog during the first refresh", ctx do
    Process.put(:cleanup_all_test_pid, self())
    {:ok, state, []} = new_state(ctx)
    refute_received :cleanup_all_called

    assert {:ok, 0} = ShapeStatus.refresh_shape_meta(state, initial?: true)
    assert_received :cleanup_all_called
  end

  test "can add shapes", ctx do
    {:ok, state, []} = new_state(ctx)
    shape = shape!()
    assert {:ok, shape_handle} = ShapeStatus.add_shape(state, shape)
    assert [{^shape_handle, ^shape}] = ShapeStatus.list_shapes(state)
  end

  test "can count shapes by indexability", ctx do
    {:ok, state, []} = new_state(ctx)

    assert {:ok, indexed_handle} = ShapeStatus.add_shape(state, shape!("indexed"))
    assert {:ok, _unindexed_handle} = ShapeStatus.add_shape(state, shape2!())

    assert %{total: 2, indexed: 1, unindexed: 1} = ShapeStatus.shape_counts(state)

    assert :ok = ShapeStatus.remove_shape(state, indexed_handle)
    assert %{total: 1, indexed: 0, unindexed: 1} = ShapeStatus.shape_counts(state)
  end

  test "can delete shape instances", ctx do
    {:ok, state, []} = new_state(ctx)
    shape_1 = shape!()
    assert {:ok, shape_handle_1} = ShapeStatus.add_shape(state, shape_1)

    shape_2 = shape2!()

    assert {:ok, shape_handle_2} = ShapeStatus.add_shape(state, shape_2)

    assert Enum.sort_by([{shape_handle_1, shape_1}, {shape_handle_2, shape_2}], &elem(&1, 0)) ==
             ShapeStatus.list_shapes(state) |> Enum.sort_by(&elem(&1, 0))

    assert :ok = ShapeStatus.remove_shape(state, shape_handle_1)
    assert [{^shape_handle_2, ^shape_2}] = ShapeStatus.list_shapes(state)

    assert [^shape_handle_2] =
             ShapeStatus.list_shape_handles_for_relations(state, [
               {shape_2.root_table_id, {"public", "table"}}
             ])

    assert [] =
             ShapeStatus.list_shape_handles_for_relations(state, [
               {shape_1.root_table_id, {"public", "other_table"}}
             ])
  end

  test "fetch_handle_by_shape/2", ctx do
    {:ok, state, []} = new_state(ctx)
    shape = shape!()

    assert :error = ShapeStatus.fetch_handle_by_shape(state, shape)

    assert {:ok, shape_handle} = ShapeStatus.add_shape(state, shape)

    assert {:ok, ^shape_handle} = ShapeStatus.fetch_handle_by_shape(state, shape)

    assert :ok = ShapeStatus.remove_shape(state, shape_handle)
    assert :error = ShapeStatus.fetch_handle_by_shape(state, shape)
  end

  test "fetch_shape_by_handle/2", ctx do
    shape = shape!()
    {:ok, state, [shape_handle]} = new_state(ctx, shapes: [shape])

    assert {:ok, ^shape} = ShapeStatus.fetch_shape_by_handle(state, shape_handle)
    assert :error = ShapeStatus.fetch_shape_by_handle(state, "not-my-handle")
  end

  test "has_shape_handle?/2", ctx do
    {:ok, state, [shape_handle]} = new_state(ctx, shapes: [shape!()])
    assert ShapeStatus.has_shape_handle?(state, shape_handle)
    refute ShapeStatus.has_shape_handle?(state, "no-such-shape")
  end

  test "validate_shape_handle/3", ctx do
    shape1 = shape!("one")
    shape2 = shape!("two")

    {:ok, state, [shape_handle1, shape_handle2]} = new_state(ctx, shapes: [shape1, shape2])

    assert :ok = ShapeStatus.validate_shape_handle(state, shape_handle1, shape1)
    assert :ok = ShapeStatus.validate_shape_handle(state, shape_handle2, shape2)

    # not a valid handle
    assert :error = ShapeStatus.validate_shape_handle(state, "not-the-handle", shape1)
    # wrong handle for the shape
    assert :error = ShapeStatus.validate_shape_handle(state, shape_handle1, shape2)
  end

  test "validate_shape_handle/3 rejects a different shape sharing a Shape.hash/1 bucket", ctx do
    {value, colliding_value} = shape_hash_collision()
    shape = shape!(value)
    colliding_shape = shape!(colliding_value)

    # The pair is a regression fixture only while it really is two different shapes landing in one
    # hash bucket. Assert both halves, so a change to the hashing fails here loudly rather than
    # leaving the case below passing for the wrong reason.
    assert Shape.hash(shape) == Shape.hash(colliding_shape)
    refute Shape.comparable(shape) == Shape.comparable(colliding_shape)

    {:ok, state, [shape_handle]} = new_state(ctx, shapes: [shape])

    assert :ok = ShapeStatus.validate_shape_handle(state, shape_handle, shape)
    assert :error = ShapeStatus.validate_shape_handle(state, shape_handle, colliding_shape)
  end

  # Two `value = '...'` predicates whose shapes share a `Shape.hash/1` value. `:erlang.phash2/1`
  # ranges over 2^27, so the birthday search settles around 14.5k candidates and the bound below is
  # slack rather than tight. Only the query string moves, which keeps the search off the parser;
  # the two winners are parsed normally by `shape!/1` and rechecked by the caller.
  defp shape_hash_collision do
    template = shape!("collide-0")

    1..200_000
    |> Enum.reduce_while(%{}, fn n, seen ->
      query = String.replace_suffix(template.where.query, "0'", "#{n}'")
      hash = Shape.hash(%{template | where: %{template.where | query: query}})

      case Map.fetch(seen, hash) do
        {:ok, prior} -> {:halt, {"collide-#{prior}", "collide-#{n}"}}
        :error -> {:cont, Map.put(seen, hash, n)}
      end
    end)
    |> case do
      {_value, _colliding_value} = pair -> pair
      %{} -> flunk("no Shape.hash/1 collision within 200_000 candidates")
    end
  end

  # The table goes with its owner while the stack restarts it, and the shape cache stamps the
  # consumer it has just started then: a raise would fail the routing of the change the consumer
  # was started for.
  test "a read time the table of shapes is gone for is not taken", ctx do
    test_pid = self()

    owner =
      spawn(fn ->
        {:ok, _state, [handle]} = new_state(ctx, shapes: [shape!()])
        send(test_pid, {:added, handle})
        receive(do: (:stop -> :ok))
      end)

    assert_receive {:added, handle}
    ref = Process.monitor(owner)
    send(owner, :stop)
    assert_receive {:DOWN, ^ref, :process, ^owner, :normal}
    refute ShapeStatus.available?(ctx.stack_id)

    refute ShapeStatus.update_last_read_time_to_now(ctx.stack_id, handle)
  end

  describe "list_shapes/2" do
    test "returns shapes with dependencies in a topological order", ctx do
      {:ok, state, []} = new_state(ctx)

      outer =
        %{shape_dependencies: [inner]} =
        Shape.new!("public.items",
          where: "id IN (SELECT id FROM other_table)",
          inspector: @inspector
        )

      {:ok, inner_handle} = ShapeStatus.add_shape(state, inner)
      outer = %{outer | shape_dependencies_handles: [inner_handle]}
      {:ok, outer_handle} = ShapeStatus.add_shape(state, outer)

      assert [{^inner_handle, _}, {^outer_handle, _}] = ShapeStatus.list_shapes(state)
    end

    test "returns a shape whose dependency is not in the store rather than looping", ctx do
      {:ok, state, []} = new_state(ctx)

      outer =
        Shape.new!("public.items",
          where: "id IN (SELECT id FROM other_table)",
          inspector: @inspector
        )

      outer = %{outer | shape_dependencies_handles: ["a-handle-no-shape-in-this-store-has"]}
      {:ok, outer_handle} = ShapeStatus.add_shape(state, outer)

      parent = self()

      {pid, ref} =
        spawn_monitor(fn -> send(parent, {:listed, ShapeStatus.list_shapes(state)}) end)

      receive do
        {:listed, shapes} ->
          Process.demonitor(ref, [:flush])
          assert [{^outer_handle, _}] = shapes

        {:DOWN, ^ref, :process, ^pid, reason} ->
          flunk("listing raised: #{inspect(reason)}")
      after
        2_000 ->
          Process.exit(pid, :kill)
          flunk("listing did not terminate: a dependency handle no shape holds has no ordering")
      end
    end
  end

  describe "least_recently_used/2" do
    setup ctx do
      {:ok, state, []} = new_state(ctx)
      %{state: state}
    end

    test "returns the shape that was least recently updated", %{state: state} do
      {:ok, shape1} = ShapeStatus.add_shape(state, shape!())
      {:ok, shape2} = ShapeStatus.add_shape(state, shape2!())

      now = System.monotonic_time()
      ShapeStatus.update_last_read_time(state, shape2, now)
      ShapeStatus.update_last_read_time(state, shape1, now + 10)

      assert {[^shape2], +0.0} = ShapeStatus.least_recently_used(state, _count = 1)
    end

    test "does not return shapes which have only just been created and didn't have update_last_read_time_to_now called",
         %{
           state: state
         } do
      {:ok, _} = ShapeStatus.add_shape(state, shape!())
      {:ok, _} = ShapeStatus.add_shape(state, shape2!())

      assert {[], _} = ShapeStatus.least_recently_used(state, _count = 1)
    end

    test "returns empty list if no shapes have been added", %{state: state} do
      assert {[], _} = ShapeStatus.least_recently_used(state, _count = 1)
    end

    test "returns empty list if all shapes have been deleted", %{state: state} do
      {:ok, shape1} = ShapeStatus.add_shape(state, shape!())
      {:ok, shape2} = ShapeStatus.add_shape(state, shape2!())

      now = System.monotonic_time()
      ShapeStatus.update_last_read_time(state, shape2, now)
      ShapeStatus.update_last_read_time(state, shape1, now + 10)

      ShapeStatus.remove_shape(state, shape1)
      ShapeStatus.remove_shape(state, shape2)

      assert {[], +0.0} = ShapeStatus.least_recently_used(state, _count = 1)
    end

    test "returns all shapes when count exceeds total shapes", %{state: state} do
      {:ok, shape1} = ShapeStatus.add_shape(state, shape!())
      {:ok, shape2} = ShapeStatus.add_shape(state, shape2!())

      now = System.monotonic_time()
      ShapeStatus.update_last_read_time(state, shape2, now)
      ShapeStatus.update_last_read_time(state, shape1, now + 10)

      {handles, _} = ShapeStatus.least_recently_used(state, _count = 100)
      assert length(handles) == 2
      assert shape1 in handles
      assert shape2 in handles
    end

    test "returns correct N shapes when N < total shapes", %{state: state} do
      # Add 5 shapes with staggered updates
      now = System.monotonic_time()

      shapes =
        for i <- 1..5 do
          {:ok, handle} = ShapeStatus.add_shape(state, shape!("test_#{i}"))
          ShapeStatus.update_last_read_time(state, handle, now + i * 10)
          handle
        end

      # Request the 3 least recently used (should be the first 3)
      {result_handles, _} = ShapeStatus.least_recently_used(state, _count = 3)
      assert length(result_handles) == 3

      expected_handles = Enum.take(shapes, 3)
      assert expected_handles == result_handles
    end

    test "returns shapes in order from least to most recently used", %{state: state} do
      now = System.monotonic_time()
      {:ok, shape1} = ShapeStatus.add_shape(state, shape!("oldest"))
      {:ok, shape2} = ShapeStatus.add_shape(state, shape!("middle"))
      {:ok, shape3} = ShapeStatus.add_shape(state, shape!("newest"))

      ShapeStatus.update_last_read_time(state, shape1, now)
      ShapeStatus.update_last_read_time(state, shape2, now + 5)
      ShapeStatus.update_last_read_time(state, shape3, now + 10)

      assert {[^shape1, ^shape2, ^shape3], _} = ShapeStatus.least_recently_used(state, _count = 3)
    end

    test "a refresh keeps the read times its clients earned", %{state: state} do
      {:ok, shape1} = ShapeStatus.add_shape(state, shape!("a"))
      {:ok, shape2} = ShapeStatus.add_shape(state, shape!("b"))
      {:ok, shape3} = ShapeStatus.add_shape(state, shape!("c"))

      # Only a completed snapshot survives the refresh's validation; without this the shapes are dropped
      # as half-built and the refresh has nothing to restamp.
      for handle <- [shape1, shape2, shape3] do
        :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle)
      end

      # Read them so the use order is exactly the reverse of the handle order. A shared stamp leaves the
      # tree nothing but the handle to sort on, and a real handle leads with a hash — so reversing the two
      # is what tells "evicted by use" apart from "evicted by name".
      [most_recent, middle, least_recent] = Enum.sort([shape1, shape2, shape3])
      now = System.monotonic_time()
      ShapeStatus.update_last_read_time(state, least_recent, now)
      ShapeStatus.update_last_read_time(state, middle, now + 5)
      ShapeStatus.update_last_read_time(state, most_recent, now + 10)

      assert {[^least_recent, ^middle, ^most_recent], _} =
               ShapeStatus.least_recently_used(state, _count = 3)

      :ok = ShapeStatus.refresh(state)

      assert {[^least_recent, ^middle, ^most_recent], _} =
               ShapeStatus.least_recently_used(state, _count = 3)
    end

    test "returns shapes with same timestamp in arbitrary order", %{state: state} do
      {:ok, shape1} = ShapeStatus.add_shape(state, shape!("1"))
      {:ok, shape2} = ShapeStatus.add_shape(state, shape!("2"))
      {:ok, shape3} = ShapeStatus.add_shape(state, shape!("3"))

      now = System.monotonic_time()
      ShapeStatus.update_last_read_time(state, shape1, now + 10)
      ShapeStatus.update_last_read_time(state, shape2, now)
      ShapeStatus.update_last_read_time(state, shape3, now)

      assert {shapes, +0.0} = ShapeStatus.least_recently_used(state, _count = 2)
      assert shapes |> Enum.sort() == [shape2, shape3] |> Enum.sort()
    end

    use ExUnitProperties

    @tag slow: true
    property "returns correct number of shapes in LRU order", %{state: state} do
      check all(
              num_shapes <- StreamData.integer(1..100),
              count <- StreamData.integer(0..100),
              timestamps <-
                StreamData.list_of(
                  StreamData.integer(1..10_000),
                  length: num_shapes
                ),
              :ok <- ShapeStatus.reset(state)
            ) do
        shape_handles =
          for {timestamp, i} <- Enum.with_index(timestamps) do
            {:ok, handle} = ShapeStatus.add_shape(state, shape!("property_test_#{i}"))
            ShapeStatus.update_last_read_time(state, handle, timestamp)
            {handle, timestamp}
          end

        {result_handles, _time} = ShapeStatus.least_recently_used(state, count)

        expected_count = min(count, num_shapes)
        assert length(result_handles) == expected_count

        # Sort by timestamp (and handle for stable ordering when timestamps are equal)
        sorted_by_timestamp = Enum.sort_by(shape_handles, fn {handle, ts} -> {ts, handle} end)

        expected_handles =
          sorted_by_timestamp |> Enum.take(expected_count) |> Enum.map(&elem(&1, 0))

        assert Enum.sort(result_handles) == Enum.sort(expected_handles)
      end
    end
  end

  describe "high concurrency" do
    @num_shapes_to_seed 10_000
    setup ctx do
      {:ok, state, []} = new_state(ctx)

      for i <- 1..@num_shapes_to_seed do
        ShapeStatus.add_shape(state, shape!("seed_#{i}"))
      end

      {:ok, state: state}
    end

    @tag slow: true
    test "add and delete of the same shape should never fail", ctx do
      %{state: state} = ctx

      shape = shape!()
      ShapeStatus.add_shape(state, shape)

      add_task =
        Task.async(fn ->
          for _ <- 1..1000 do
            case ShapeStatus.fetch_handle_by_shape(state, shape) do
              :error -> ShapeStatus.add_shape(state, shape)
              _ -> :ok
            end
          end
        end)

      remove_tasks =
        for _ <- 1..1000 do
          Task.async(fn ->
            case ShapeStatus.fetch_handle_by_shape(state, shape) do
              :error -> :ok
              {:ok, handle} -> ShapeStatus.remove_shape(state, handle)
            end
          end)
        end

      Task.await_many([add_task | remove_tasks], 20_000)
    end
  end

  # The purge that follows a timeline change resets the catalog, and retries when that fails. A
  # read-only request serves only a shape the in-memory tables hold.
  describe "reset/1" do
    for {step, failing_sql} <- [{"statement", "DELETE FROM shapes"}, {"commit", "COMMIT"}] do
      test "a reset whose #{step} fails returns the failure with no shape left to serve", ctx do
        {:ok, state, [handle]} = new_state(ctx, shapes: [shape!()])
        :ok = ShapeStatus.ShapeDb.WriteBuffer.flush_sync(state)
        assert ShapeStatus.has_shape_handle?(state, handle)

        Repatch.patch(ShapeStatus.ShapeDb.Sqlite3, :execute, fn conn, sql ->
          if sql == unquote(failing_sql),
            do: {:error, "database or disk is full"},
            else: Repatch.real(ShapeStatus.ShapeDb.Sqlite3.execute(conn, sql))
        end)

        assert {:error, _} = ShapeStatus.reset(state)
        refute ShapeStatus.has_shape_handle?(state, handle)
        assert ShapeStatus.ShapeDb.handle_exists?(state, handle)
      end
    end
  end

  describe "refresh/1" do
    test "picks up new shapes added to SQLite", ctx do
      {:ok, state, [handle]} = new_state(ctx, shapes: [shape!()])
      ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle)

      # Add a new shape directly to SQLite (simulating another instance)
      new_shape = shape!("new_from_other_instance")
      {:ok, _hash} = ShapeStatus.ShapeDb.add_shape(state, new_shape, "other-handle-123")
      ShapeStatus.ShapeDb.mark_snapshot_complete(state, "other-handle-123")

      refute ShapeStatus.has_shape_handle?(state, "other-handle-123")

      :ok = ShapeStatus.refresh(state)

      assert ShapeStatus.has_shape_handle?(state, "other-handle-123")
      # Original shape still present
      assert ShapeStatus.has_shape_handle?(state, handle)
    end

    test "removes shapes deleted from SQLite", ctx do
      {:ok, state, [handle]} = new_state(ctx, shapes: [shape!()])
      ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle)

      assert ShapeStatus.has_shape_handle?(state, handle)

      # Remove from SQLite directly (simulating another instance's cleanup)
      :ok = ShapeStatus.ShapeDb.remove_shape(state, handle)

      # Still in ETS
      assert ShapeStatus.has_shape_handle?(state, handle)

      :ok = ShapeStatus.refresh(state)

      # Now gone from ETS
      refute ShapeStatus.has_shape_handle?(state, handle)
    end

    test "updates snapshot_complete flag", ctx do
      # Create two shapes, one with complete snapshot, one without
      {:ok, state, [handle_a, handle_b]} =
        new_state(ctx, shapes: [shape!("a"), shape!("b")])

      # Mark both complete so validate_existing_shapes doesn't reject them
      ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle_a)
      ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle_b)

      # ETS still has old value (snapshot_started? = false from add_shape)
      refute ShapeStatus.snapshot_started?(state, handle_b)

      :ok = ShapeStatus.refresh(state)

      # Now updated from SQLite
      assert ShapeStatus.snapshot_started?(state, handle_b)
    end

    test "existing entries remain visible during refresh", ctx do
      {:ok, state, [handle]} = new_state(ctx, shapes: [shape!()])
      ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle)

      # Refresh uses upsert (not clear-then-populate), so the entry
      # should be visible at all times. We verify it's present after refresh.
      :ok = ShapeStatus.refresh(state)

      assert ShapeStatus.has_shape_handle?(state, handle)
      assert ShapeStatus.snapshot_started?(state, handle)
    end

    # Validation keeps a shape whose snapshot-complete mark a failed flush left queued. Read back
    # from the catalog alone, its snapshot was not started, and its next request, which found no
    # consumer for it after a restart of the shape subsystem, removed the shape.
    test "keeps the snapshot of a shape whose completion is still queued", ctx do
      {:ok, state, [handle]} = new_state(ctx, shapes: [shape!()])
      :ok = ShapeStatus.ShapeDb.WriteBuffer.flush_sync(state)
      :ok = ShapeStatus.mark_snapshot_started(state, handle)
      {:ok, path} = ShapeStatus.ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      {:ok, other} = ShapeStatus.ShapeDb.Sqlite3.open(path, [])

      :ok =
        ShapeStatus.ShapeDb.Connection.execute(other, """
        CREATE TRIGGER refuse_mark BEFORE UPDATE OF snapshot_complete ON shapes
        BEGIN SELECT RAISE(ABORT, 'refused'); END
        """)

      :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle)

      try do
        {result, _log} = ExUnit.CaptureLog.with_log(fn -> ShapeStatus.refresh(state) end)
        assert :ok = result
        assert ShapeStatus.has_shape_handle?(state, handle)
        assert ShapeStatus.snapshot_started?(state, handle)
      after
        :ok = ShapeStatus.ShapeDb.Connection.execute(other, "DROP TRIGGER refuse_mark")
        :ok = ShapeStatus.ShapeDb.Connection.close(other)
      end
    end

    # A complete shape whose removal is still queued is gone, though the catalog keeps its row
    # until the removal commits. The refresh put it back, its snapshot complete, and a request for
    # its handle read the storage its removal had taken.
    test "leaves out a complete shape whose removal is queued", ctx do
      {:ok, state, [handle]} = new_state(ctx, shapes: [shape!()])
      :ok = ShapeStatus.mark_snapshot_started(state, handle)
      :ok = ShapeStatus.ShapeDb.mark_snapshot_complete(state, handle)
      :ok = ShapeStatus.ShapeDb.WriteBuffer.flush_sync(state)
      {:ok, path} = ShapeStatus.ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      {:ok, other} = ShapeStatus.ShapeDb.Sqlite3.open(path, [])

      :ok =
        ShapeStatus.ShapeDb.Connection.execute(other, """
        CREATE TRIGGER refuse_removal BEFORE DELETE ON shapes
        BEGIN SELECT RAISE(ABORT, 'refused'); END
        """)

      :ok = ShapeStatus.remove_shape(state, handle)

      try do
        {result, _log} = ExUnit.CaptureLog.with_log(fn -> ShapeStatus.refresh(state) end)
        assert :ok = result
        refute ShapeStatus.has_shape_handle?(state, handle)
        refute ShapeStatus.snapshot_started?(state, handle)
      after
        :ok = ShapeStatus.ShapeDb.Connection.execute(other, "DROP TRIGGER refuse_removal")
        :ok = ShapeStatus.ShapeDb.Connection.close(other)
      end
    end
  end

  defp shape!, do: shape!("test")

  defp shape!(val) do
    where = "value = '#{val}'"

    assert {:ok, %Shape{where: %{query: ^where}} = shape} =
             Shape.new("items", inspector: @inspector, where: where)

    shape
  end

  defp shape2! do
    assert {:ok, %Shape{where: nil} = shape} =
             Shape.new("public.other_table", inspector: @inspector)

    shape
  end

  defp new_state(ctx, opts \\ []) do
    Electric.StackConfig.put(ctx.stack_id, Electric.ShapeCache.Storage, {NoopStorage, []})

    stored_shapes = Access.get(opts, :stored_shapes, [])

    :ok = ShapeStatus.initialize(ctx.stack_id)

    for {handle, shape} <- stored_shapes do
      {:ok, _hash} = ShapeStatus.ShapeDb.add_shape(ctx.stack_id, shape, handle)
    end

    shapes = Keyword.get(opts, :shapes, [])

    shape_handles =
      for shape <- shapes do
        {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
        shape_handle
      end

    {:ok, ctx.stack_id, shape_handles}
  end
end
