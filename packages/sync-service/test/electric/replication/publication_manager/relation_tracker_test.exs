defmodule Electric.Replication.PublicationManager.RelationTrackerTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.TestUtils

  alias Electric.PersistentKV
  alias Electric.Replication.PublicationManager.Configurator
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.PublicationManager.RelationTracker
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.SnapshotError

  @moduletag :tmp_dir

  @refresh_period 50

  setup [
    :with_stack_id_from_test,
    :with_in_memory_storage,
    :with_shape_status,
    :with_persistent_kv
  ]

  @items {1234, {"public", "items"}}

  setup ctx do
    test_pid = self()
    {:ok, connection_status} = Agent.start_link(fn -> :starting end)

    Repatch.patch(Configurator, :configure_publication, [mode: :shared], fn _stack_id, filters ->
      send(test_pid, {:configure_publication, filters})
      :ok
    end)

    Repatch.patch(Electric.StatusMonitor, :status, [mode: :shared], fn _stack_id ->
      status = %{conn: Agent.get(connection_status, & &1), shape: :up}
      send(test_pid, {:swept, status.conn})
      status
    end)

    start_supervised!(%{
      id: RelationTracker,
      start:
        {RelationTracker, :start_link,
         [
           [
             stack_id: ctx.stack_id,
             publication_name: "relation_tracker_test_publication",
             persistent_kv: ctx.persistent_kv,
             database_timeline: Map.get(ctx, :database_timeline),
             refresh_period: @refresh_period
           ]
         ]},
      restart: :temporary
    })

    Repatch.allow(test_pid, RelationTracker.name(ctx.stack_id))

    %{connection_status: connection_status}
  end

  describe "periodic publication refresh" do
    test "keeps sweeping after one lands while the stack is inactive", ctx do
      # Arrange
      assert_receive {:swept, :starting}
      refute_received {:configure_publication, _}

      # Act
      Agent.update(ctx.connection_status, fn _ -> :up end)

      # Assert
      assert_receive {:configure_publication, _}
    end

    test "keeps sweeping while a shape waits for its relation to be published", ctx do
      # Arrange
      Agent.update(ctx.connection_status, fn _ -> :up end)
      shape = generate_shape({1234, {"public", "items"}})

      # Act
      spawn(fn -> RelationTracker.add_shape(ctx.stack_id, "waiting_shape_handle", shape) end)
      assert_receive {:configure_publication, {revision, filters}}
      assert MapSet.size(filters) == 1

      # Assert
      assert_receive {:configure_publication, {newer, ^filters}}
      assert newer > revision
    end

    test "keeps sweeping while shape churn keeps the tracker busy", ctx do
      # Arrange
      Agent.update(ctx.connection_status, fn _ -> :up end)

      busy =
        spawn_link(fn ->
          Stream.iterate(0, &(&1 + 1))
          |> Enum.each(fn index ->
            RelationTracker.remove_shape(ctx.stack_id, "churn_shape_handle_#{index}")
            Process.sleep(div(@refresh_period, 5))
          end)
        end)

      on_exit(fn -> Process.exit(busy, :kill) end)

      # Act: let the churn run past a full period, then drop everything it provoked so that
      # anything arriving afterwards can only be the sweep.
      Process.sleep(@refresh_period * 2)
      flush_swept()

      # Assert
      assert_receive {:swept, :up}
    end
  end

  describe "publication stops publishing generated columns" do
    test "removes the shapes it fails with the error that says why", ctx do
      # Arrange
      test_pid = self()

      Repatch.patch(ShapeCleaner, :remove_shapes_async, [mode: :shared], fn _stack_id,
                                                                            handles,
                                                                            reason ->
        send(test_pid, {:removed, handles, reason})
        :ok
      end)

      Agent.update(ctx.connection_status, fn _ -> :up end)

      spawn(fn ->
        GenServer.call(
          RelationTracker.name(ctx.stack_id),
          {:add_shape, "generated_column_shape", {{1234, {"public", "items"}}, true}}
        )
      end)

      assert_receive {:configure_publication, _}

      # Act
      RelationTracker.notify_publication_status(
        %{
          can_alter_publication?: true,
          publishes_all_operations?: true,
          publishes_generated_columns?: false
        },
        stack_id: ctx.stack_id
      )

      # Assert
      assert_receive {:removed, ["generated_column_shape"],
                      {:error, %SnapshotError{type: :publication_missing_generated_columns}}}
    end
  end

  describe "a table out of the publication" do
    # What a boot finds when the table of its stored shapes is out of the publication: they were
    # stored before it left, and what was written to it since never reached them.
    test "gives up the shapes from before and keeps a new one waiting for it", ctx do
      # Arrange
      {:ok, restored} = ShapeStatus.add_shape(ctx.stack_id, shape_on_items("1"))
      :ok = ShapeStatus.mark_snapshot_started(ctx.stack_id, restored)
      {:ok, idle} = ShapeStatus.add_shape(ctx.stack_id, shape_on_items("2"))
      :ok = ShapeStatus.mark_snapshot_started(ctx.stack_id, idle)
      restart_relation_tracker(ctx)

      # the restored shape's consumer runs the publication step, and so does a new shape's
      restored_step =
        Task.async(fn ->
          catch_error(RelationTracker.add_shape(ctx.stack_id, restored, shape_on_items("1")))
        end)

      new_step =
        Task.async(fn -> RelationTracker.add_shape(ctx.stack_id, "new", shape_on_items("3")) end)

      await_waiters(ctx, 2)

      # Act
      stale = take_out_of_publication(ctx)

      # Assert
      assert Enum.sort([{restored, 1234}, {idle, 1234}]) == Enum.sort(stale)

      assert %RuntimeError{message: "Table public.items was out of the publication" <> _} =
               Task.await(restored_step)

      refute Task.yield(new_step, 100)
      notify_configured(ctx)
      assert :ok = Task.await(new_step)
    end

    test "gives up a shape served before its table left the publication", ctx do
      # Arrange
      served_step =
        Task.async(fn ->
          RelationTracker.add_shape(ctx.stack_id, "served", shape_on_items("1"))
        end)

      assert_receive {:configure_publication, _}
      notify_configured(ctx)
      assert :ok = Task.await(served_step)

      # Act: a sweep found the table out of the publication
      stale = take_out_of_publication(ctx)

      # Assert
      assert [{"served", 1234}] == stale
    end

    test "keeps a shape that comes in meanwhile waiting for the addition", ctx do
      # Arrange
      served_step =
        Task.async(fn ->
          RelationTracker.add_shape(ctx.stack_id, "served", shape_on_items("1"))
        end)

      assert_receive {:configure_publication, _}
      notify_configured(ctx)
      assert :ok = Task.await(served_step)

      # Act
      take_out_of_publication(ctx)

      new_step =
        Task.async(fn -> RelationTracker.add_shape(ctx.stack_id, "new", shape_on_items("2")) end)

      # Assert
      refute Task.yield(new_step, 100)
      notify_configured(ctx)
      assert :ok = Task.await(new_step)
    end

    # Its consumer suspends and takes no snapshot before it waits again, whatever runs meanwhile.
    test "keeps a new shape told to retry until it waits again", ctx do
      # Arrange
      step =
        Task.async(fn ->
          catch_error(RelationTracker.add_shape(ctx.stack_id, "new", shape_on_items("1")))
        end)

      await_waiters(ctx, 1)

      RelationTracker.notify_relation_configuration_result(
        @items,
        {:error, SnapshotError.connection_not_available()},
        stack_id: ctx.stack_id
      )

      assert %SnapshotError{type: :connection_not_available} = Task.await(step)

      # Act: a pass runs before the shape waits again
      stale = take_out_of_publication(ctx)

      # Assert
      assert [] == stale
    end

    test "owes a drain of the table's writers until it is configured", ctx do
      # Arrange
      take_out_of_publication(ctx)
      assert MapSet.new([1234]) == RelationTracker.barriers_owed(stack_id: ctx.stack_id)

      # Act: the addition is held back, then goes through
      RelationTracker.notify_relation_configuration_result(
        @items,
        {:error, SnapshotError.connection_not_available()},
        stack_id: ctx.stack_id
      )

      held = RelationTracker.barriers_owed(stack_id: ctx.stack_id)
      notify_configured(ctx)

      # Assert
      assert MapSet.new([1234]) == held
      assert MapSet.new() == RelationTracker.barriers_owed(stack_id: ctx.stack_id)
    end

    @tag :barrier_recovery
    @tag database_timeline: ["1", "1"]
    test "rechecks completed drains after a timeline change on the same database", ctx do
      opts = [stack_id: ctx.stack_id]
      other = {5678, {"public", "other_items"}}
      RelationTracker.owe_barriers(MapSet.new([@items, other]), opts)
      RelationTracker.notify_relation_configuration_result(@items, {:ok, {:drained, 42}}, opts)

      assert {MapSet.new([5678]), %{@items => 42}, false, MapSet.new()} ==
               RelationTracker.barrier_state(opts)

      restart_relation_tracker(%{ctx | database_timeline: ["1", "2"]})

      assert {MapSet.new([1234, 5678]), %{@items => 42}, false, MapSet.new()} ==
               RelationTracker.barrier_state(opts)

      restart_relation_tracker(%{ctx | database_timeline: ["2", "2"]})
      assert {MapSet.new(), %{}, false, MapSet.new()} == RelationTracker.barrier_state(opts)
    end

    @tag :pending_cleanup_on_timeline_change
    @tag database_timeline: ["1", "1"]
    test "preserves a pending membership cleanup on a new timeline", ctx do
      opts = [stack_id: ctx.stack_id]
      RelationTracker.owe_barriers(MapSet.new([@items]), opts)
      assert_receive {PersistentKV.Memory, {:set, key, _}}
      record = [["1", "1"], [1234], [[1234, "public", "items", 42]], false]
      :ok = PersistentKV.set(ctx.persistent_kv, key, record)

      restart_relation_tracker(%{ctx | database_timeline: ["1", "2"]})

      assert {MapSet.new([1234]), %{@items => 42}, false, MapSet.new([1234])} ==
               RelationTracker.barrier_state(opts)

      assert {:ok, [["1", "2"], [1234], [[1234, "public", "items", 42]], false, [1234], []]} =
               PersistentKV.get(ctx.persistent_kv, key)
    end

    @tag :barrier_scope
    @tag database_timeline: ["1", "1"]
    test "keeps each publication and database system's barrier record across switches", ctx do
      opts = [stack_id: ctx.stack_id]
      owed = MapSet.new([1234])
      filters = MapSet.new([@items])
      memory = Electric.PersistentKV.Memory

      RelationTracker.owe_barriers(filters, opts)
      assert_receive {^memory, {:set, original, [["1", "1"], [1234], [], false | _]}}

      other = Map.put(ctx, :publication_name, "other/publication")
      restart_relation_tracker(other)
      assert {MapSet.new(), %{}, false, MapSet.new()} == RelationTracker.barrier_state(opts)
      RelationTracker.owe_barriers(filters, opts)
      assert_receive {^memory, {:set, other_key, [["1", "1"], [1234], [], false | _]}}
      assert other_key != original
      assert other_key =~ ~r/:publication_barriers:[[:xdigit:]]{64}$/

      restart_relation_tracker(%{ctx | database_timeline: ["2", "1"]})
      assert {MapSet.new(), %{}, false, MapSet.new()} == RelationTracker.barrier_state(opts)
      RelationTracker.owe_barriers(filters, opts)

      restart_relation_tracker(ctx)
      assert {^owed, %{}, false, _} = RelationTracker.barrier_state(opts)
      restart_relation_tracker(other)
      assert {^owed, %{}, false, _} = RelationTracker.barrier_state(opts)
    end

    for {failure, failed_reads, expected_oid, recheck_unserved?} <- [
          {:transient, 1, 5678, false},
          {:persistent, 3, 1234, true}
        ] do
      @tag :barrier_read_failure
      @tag failed_reads: failed_reads
      test "recovers after a #{failure} error reading the barrier record", ctx do
        opts = [stack_id: ctx.stack_id]
        other = {5678, {"public", "other_items"}}
        RelationTracker.owe_barriers(MapSet.new([other]), opts)
        assert_receive {PersistentKV.Memory, {:set, key, _record}}
        assert {:ok, _} = ShapeStatus.add_shape(ctx.stack_id, shape_on_items("1"))

        {:ok, remaining} = Agent.start_link(fn -> ctx.failed_reads end)

        barrier_kv =
          PersistentKV.Serialized.new!(
            backend: ctx.persistent_kv,
            encoder: {__MODULE__, :barrier_record, []},
            decoder: {__MODULE__, :read_barrier_record, [remaining, self()]}
          )

        restart_relation_tracker(%{ctx | persistent_kv: barrier_kv})
        assert_receive :barrier_read_failed

        recovered =
          {MapSet.new([unquote(expected_oid)]), %{}, unquote(recheck_unserved?), MapSet.new()}

        assert recovered == RelationTracker.barrier_state(opts)

        if ctx.failed_reads > 1 do
          assert_receive :barrier_read_failed
          assert_receive :barrier_read_failed
          assert_receive {PersistentKV.Memory, {:set, ^key, [nil, [1234], [], true | _]}}
          restart_relation_tracker(ctx)
          assert recovered == RelationTracker.barrier_state(opts)
        end
      end
    end

    @tag :timed_out
    test "a timed-out admission removes only its own waiter", ctx do
      take_out_of_publication(ctx)
      tracker = RelationTracker.name(ctx.stack_id)
      shape = shape_on_items("1")

      expired =
        Task.async(fn ->
          assert_raise SnapshotError, fn ->
            RelationTracker.add_shape(ctx.stack_id, "waiting", shape)
          end

          :sys.get_state(tracker)
        end)

      assert await_waiters(ctx, 1)

      waiting =
        Task.async(fn ->
          filter = RelationTracker.publication_filter(shape)
          GenServer.call(tracker, {:add_shape, "waiting", filter}, :infinity)
        end)

      assert await_waiters(ctx, 2)
      waiting_pid = waiting.pid

      assert %{waiters: %{1234 => [{{^waiting_pid, _}, "waiting"}]}} =
               Task.await(expired, 10_000)

      assert {_, filters} = RelationTracker.fetch_current_filters!(stack_id: ctx.stack_id)
      assert MapSet.new([@items]) == filters

      assert MapSet.new([1234]) == RelationTracker.barriers_owed(stack_id: ctx.stack_id)

      notify_configured(ctx)
      assert :ok = Task.await(waiting)
    end

    test "keeps every shape when the table is added for its first shapes", ctx do
      # Arrange
      steps =
        for n <- ["1", "2"] do
          Task.async(fn -> RelationTracker.add_shape(ctx.stack_id, n, shape_on_items(n)) end)
        end

      await_waiters(ctx, 2)

      # Act
      stale = take_out_of_publication(ctx)
      notify_configured(ctx)

      # Assert
      assert [] == stale
      assert [:ok, :ok] = Task.await_many(steps)
    end
  end

  defp take_out_of_publication(ctx) do
    RelationTracker.take_relations_out_of_publication([@items], stack_id: ctx.stack_id)
  end

  def barrier_record(record), do: {:ok, record}

  def read_barrier_record(record, remaining, parent) do
    if Agent.get_and_update(remaining, fn remaining -> {remaining > 0, remaining - 1} end) do
      send(parent, :barrier_read_failed)
      {:error, :eio}
    else
      barrier_record(record)
    end
  end

  defp notify_configured(ctx) do
    RelationTracker.notify_relation_configuration_result(@items, {:ok, :configured},
      stack_id: ctx.stack_id
    )
  end

  defp shape_on_items(id) do
    generate_shape(@items, %Expr{query: "id = '#{id}'", used_refs: %{["id"] => :text}})
  end

  defp await_waiters(ctx, count) do
    tracker = RelationTracker.name(ctx.stack_id)
    wait_until(fn -> length(Map.get(:sys.get_state(tracker).waiters, 1234, [])) == count end)
  end

  # Starts the tracker again, as a boot does, so that it restores the shapes stored by then. The
  # patches reach it only once it is allowed, which its restore can beat: the restore's request to
  # the configurator is not waited for, and the tracker sweeps no more.
  defp restart_relation_tracker(ctx) do
    stop_supervised!(RelationTracker)

    start_supervised!(%{
      id: RelationTracker,
      start:
        {RelationTracker, :start_link,
         [
           [
             stack_id: ctx.stack_id,
             publication_name:
               Map.get(ctx, :publication_name, "relation_tracker_test_publication"),
             persistent_kv: ctx.persistent_kv,
             database_timeline: Map.get(ctx, :database_timeline),
             refresh_period: 60_000
           ]
         ]},
      restart: :temporary
    })

    Repatch.allow(self(), RelationTracker.name(ctx.stack_id))
    :ok = RelationTracker.wait_for_restore(ctx.stack_id)
  end

  defp flush_swept do
    receive do
      {:swept, _} -> flush_swept()
    after
      0 -> :ok
    end
  end
end
