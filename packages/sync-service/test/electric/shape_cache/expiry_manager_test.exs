defmodule Electric.ExpiryManagerTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  alias Electric.ShapeCache.ExpiryManager
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Support.Fixtures

  import Support.ComponentSetup
  import Support.TestUtils

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_status_monitor,
    :with_in_memory_storage,
    :with_shape_status
  ]

  @max_shapes 10

  setup %{stack_id: stack_id} = ctx do
    test_pid = self()

    expiry_manager =
      start_supervised!(
        {ExpiryManager,
         max_shapes: @max_shapes,
         max_expired_per_run: Map.get(ctx, :max_expired_per_run, 1_000),
         period: Map.get(ctx, :period, 1),
         stack_id: stack_id}
      )

    # The "arms the next check" test installs its own blocking remove_shapes patch, and
    # Repatch forbids patching a function that is already patched, so it opts out here.
    unless ctx[:custom_remove_shapes] do
      Repatch.patch(ShapeCleaner, :remove_shapes, [mode: :shared], fn stack_id, shape_handles ->
        send(test_pid, {:expiring, length(shape_handles)})
        Enum.each(shape_handles, &ShapeStatus.remove_shape(stack_id, &1))
      end)

      Repatch.allow(self(), expiry_manager)
    end

    %{expiry_manager: expiry_manager}
  end

  describe "when stack is active" do
    setup :set_status_to_active

    test "expires shapes if shape count has gone over max_shapes", ctx do
      for i <- 1..(@max_shapes + 1) do
        ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))
      end

      assert_receive {:expiring, _}
    end

    # The period is long enough that no sweep fires on its own: seeding then driving one run by hand is
    # what makes the batch size read off a known store size rather than off however far seeding had got.
    @tag max_expired_per_run: 5, period: 60_000
    test "expires at most one batch per run, however far over the limit the store is", ctx do
      for i <- 1..(@max_shapes + 60) do
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))

        # A shape that has never been read is held back as too young to expire, so without this the
        # sweep runs against an empty candidate set and the batch proves nothing.
        ShapeStatus.update_last_read_time_to_now(ctx.stack_id, handle)
      end

      send(ctx.expiry_manager, :maybe_expire_shapes)

      assert_receive {:expiring, count}
      assert count == 5
    end

    # The batch size is far below the excess, and the period is far longer than the test, so the
    # only way the store gets back to its limit is the manager queueing the next batch itself.
    @tag max_expired_per_run: 5, period: 60_000
    test "keeps expiring batches back to back while the store is still over the limit", ctx do
      for i <- 1..(@max_shapes + 20) do
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))
        ShapeStatus.update_last_read_time_to_now(ctx.stack_id, handle)
      end

      send(ctx.expiry_manager, :maybe_expire_shapes)

      for _batch <- 1..4, do: assert_receive({:expiring, 5})
      refute_receive {:expiring, _}, 100
      assert ShapeStatus.count_shapes(ctx.stack_id) == @max_shapes
    end

    # Shapes created while a batch runs keep the count where it was: the store grows as fast as a
    # batch drains it, and the next batch is still due at once.
    @tag max_expired_per_run: 5, period: 60_000
    @tag :custom_remove_shapes
    test "keeps expiring batches while new shapes arrive as fast as a batch removes them", ctx do
      test_pid = self()
      stack_id = ctx.stack_id
      first_batch? = :counters.new(1, [])

      add_read_shape = fn i ->
        {:ok, handle} = ShapeStatus.add_shape(stack_id, Fixtures.Shape.new(i))
        ShapeStatus.update_last_read_time_to_now(stack_id, handle)
      end

      Repatch.patch(ShapeCleaner, :remove_shapes, [mode: :shared], fn ^stack_id, handles ->
        send(test_pid, {:expiring, length(handles)})
        Enum.each(handles, &ShapeStatus.remove_shape(stack_id, &1))

        if :counters.get(first_batch?, 1) == 0 do
          :counters.add(first_batch?, 1, 1)
          Enum.each(1001..1005, add_read_shape)
        end

        :ok
      end)

      Repatch.allow(self(), ctx.expiry_manager)

      Enum.each(1..(@max_shapes + 20), add_read_shape)

      send(ctx.expiry_manager, :maybe_expire_shapes)

      for _batch <- 1..5, do: assert_receive({:expiring, 5})
      refute_receive {:expiring, _}, 100
      assert ShapeStatus.count_shapes(stack_id) == @max_shapes
    end

    # A batch that cannot expire anything leaves the store over its limit. Queueing another batch
    # then would spin the manager, so it waits for the period instead.
    @tag max_expired_per_run: 5, period: 60_000
    @tag :custom_remove_shapes
    test "does not queue another batch when a batch removed nothing", ctx do
      test_pid = self()

      Repatch.patch(ShapeCleaner, :remove_shapes, [mode: :shared], fn _stack_id, handles ->
        send(test_pid, {:expiring, length(handles)})
      end)

      Repatch.allow(self(), ctx.expiry_manager)

      for i <- 1..(@max_shapes + 20) do
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))
        ShapeStatus.update_last_read_time_to_now(ctx.stack_id, handle)
      end

      send(ctx.expiry_manager, :maybe_expire_shapes)

      assert_receive {:expiring, 5}
      refute_receive {:expiring, _}, 200
    end

    test "does not expires shapes if shape count has not gone over max_shapes", ctx do
      for i <- 1..@max_shapes do
        ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))
      end

      refute_receive {:expiring, _}, 100
    end

    # A slow removal must not stall the cadence: the next check has to be armed before the batch
    # is removed, so its timer fires while removal is still running. We block removal, then read
    # the manager's own mailbox — the queued :maybe_expire_shapes is only there if the check was
    # scheduled first.
    @tag period: 20
    @tag :custom_remove_shapes
    test "arms the next check before it starts removing a batch", ctx do
      test_pid = self()

      Repatch.patch(ShapeCleaner, :remove_shapes, [mode: :shared], fn stack_id, shape_handles ->
        send(test_pid, :removing)

        receive do
          :proceed -> :ok
        end

        {:messages, mailbox} = Process.info(self(), :messages)
        send(test_pid, {:next_check_pending?, :maybe_expire_shapes in mailbox})

        Enum.each(shape_handles, &ShapeStatus.remove_shape(stack_id, &1))
      end)

      Repatch.allow(self(), ctx.expiry_manager)

      for i <- 1..(@max_shapes + 3) do
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))
        ShapeStatus.update_last_read_time_to_now(ctx.stack_id, handle)
      end

      assert_receive :removing
      # Give the period timer (armed before the block) time to fire into the manager's mailbox.
      Process.sleep(100)
      send(ctx.expiry_manager, :proceed)

      assert_receive {:next_check_pending?, true}
    end
  end

  describe "when stack is not active" do
    test "does not expires shapes even if shape count has gone over max_shapes", ctx do
      for i <- 1..(@max_shapes + 1) do
        ShapeStatus.add_shape(ctx.stack_id, Fixtures.Shape.new(i))
      end

      refute_receive {:expiring, _}, 100
    end
  end

  describe "without a shape ceiling" do
    test "says so, because nothing else will", ctx do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _state} =
                   ExpiryManager.init(
                     max_shapes: nil,
                     max_expired_per_run: 1_000,
                     period: 60_000,
                     stack_id: ctx.stack_id
                   )
        end)

      assert log =~ "Shape expiry is disabled"
      assert log =~ "grows without bound"
    end

    test "a configured ceiling is not warned about", ctx do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _state} =
                   ExpiryManager.init(
                     max_shapes: 10,
                     max_expired_per_run: 1_000,
                     period: 60_000,
                     stack_id: ctx.stack_id
                   )
        end)

      refute log =~ "Shape expiry is disabled"
    end
  end
end
