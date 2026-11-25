defmodule Electric.LsnTrackerTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup,
    only: [with_lsn_tracker: 1, with_registry: 1, with_stack_id_from_test: 1]

  alias Electric.LsnTracker
  alias Electric.Postgres.Lsn

  setup [:with_stack_id_from_test]

  describe "get_last_processed_lsn/1" do
    setup ctx do
      LsnTracker.initialize(ctx.stack_id)
      :ok
    end

    test "returns nil when not yet populated", %{stack_id: stack_id} do
      assert LsnTracker.get_last_processed_lsn(stack_id) == nil
    end

    test "returns inital lsn", %{stack_id: stack_id} do
      lsn = Lsn.from_integer(7)
      LsnTracker.set_last_processed_lsn(stack_id, lsn)

      assert LsnTracker.get_last_processed_lsn(stack_id) == lsn
    end

    test "returns last set lsn", %{stack_id: stack_id} do
      lsn = Lsn.from_integer(7)
      LsnTracker.set_last_processed_lsn(stack_id, lsn)

      lsn = Lsn.from_integer(77)
      LsnTracker.set_last_processed_lsn(stack_id, lsn)

      lsn = Lsn.from_integer(111)
      LsnTracker.set_last_processed_lsn(stack_id, lsn)

      assert LsnTracker.get_last_processed_lsn(stack_id) == lsn
    end
  end

  describe "initialize_last_processed_lsn/2" do
    setup ctx do
      LsnTracker.initialize(ctx.stack_id)
      :ok
    end

    test "sets lsn when not previously set", %{stack_id: stack_id} do
      lsn = Lsn.from_integer(42)
      assert :ok = LsnTracker.initialize_last_processed_lsn(stack_id, lsn)
      assert LsnTracker.get_last_processed_lsn(stack_id) == lsn
    end

    test "accepts integer and converts to Lsn", %{stack_id: stack_id} do
      assert :ok = LsnTracker.initialize_last_processed_lsn(stack_id, 100)
      assert LsnTracker.get_last_processed_lsn(stack_id) == Lsn.from_integer(100)
    end

    test "does not overwrite existing lsn", %{stack_id: stack_id} do
      initial_lsn = Lsn.from_integer(50)
      LsnTracker.set_last_processed_lsn(stack_id, initial_lsn)

      new_lsn = Lsn.from_integer(100)
      assert :ok = LsnTracker.initialize_last_processed_lsn(stack_id, new_lsn)

      # Should still be the initial LSN, not the new one
      assert LsnTracker.get_last_processed_lsn(stack_id) == initial_lsn
    end

    test "is idempotent - can be called multiple times", %{stack_id: stack_id} do
      lsn1 = Lsn.from_integer(10)
      lsn2 = Lsn.from_integer(20)

      assert :ok = LsnTracker.initialize_last_processed_lsn(stack_id, lsn1)
      assert :ok = LsnTracker.initialize_last_processed_lsn(stack_id, lsn2)

      # First call wins
      assert LsnTracker.get_last_processed_lsn(stack_id) == lsn1
    end
  end

  describe "broadcast_last_seen_lsn/2" do
    setup [:with_registry, :with_lsn_tracker]

    test "delivers messages to processes registered for global_lsn_updates", ctx do
      LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)

      assert_receive {:global_last_seen_lsn, 42}
    end

    test "replays the most recent broadcast to newly registered processes", ctx do
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)

      assert {:ok, _} = LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      assert_receive {:global_last_seen_lsn, 42}
    end

    test "delivers to multiple registered processes", ctx do
      test_pid = self()
      LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      {other_pid, ref} =
        spawn_monitor(fn ->
          LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)
          send(test_pid, :registered)

          receive do
            {:global_last_seen_lsn, lsn} -> send(test_pid, {:got_lsn, lsn})
          end
        end)

      assert_receive :registered

      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 99)

      assert_receive {:global_last_seen_lsn, 99}
      assert_receive {:got_lsn, 99}
      assert_receive {:DOWN, ^ref, :process, ^other_pid, :normal}
    end

    test "is a no-op when no processes are registered", ctx do
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)
      refute_receive {:global_last_seen_lsn, _}
    end

    test "suppresses broadcasts that do not advance the position", ctx do
      LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)
      assert_receive {:global_last_seen_lsn, 42}

      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 41)
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)
      refute_receive {:global_last_seen_lsn, _}

      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 43)
      assert_receive {:global_last_seen_lsn, 43}
    end

    test "replays the highest position to new subscribers despite older late writes", ctx do
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 7)

      assert {:ok, _} = LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      assert_receive {:global_last_seen_lsn, 42}
    end
  end

  describe "reset_last_broadcast_lsn/1" do
    setup [:with_registry, :with_lsn_tracker]

    test "drops the watermark so a regressed feed recovers", ctx do
      # Arrange: the pre-purge database's last position is far above the restored one.
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1_000_000)
      LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)
      assert_receive {:global_last_seen_lsn, 1_000_000}

      # Act: the purge discards the watermark; the restored database streams lower LSNs.
      :ok = LsnTracker.reset_last_broadcast_lsn(ctx.stack_id)
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 500)

      # Assert: the feed is alive again and new subscribers read the real position.
      assert_receive {:global_last_seen_lsn, 500}
      assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 500
    end

    test "is a no-op without an initialized tracker" do
      assert :ok = LsnTracker.reset_last_broadcast_lsn("never-initialized-stack")
    end

    # A collector that starts with a journal tail to replay drops the watermark, and a request
    # that read it before vouches for it only if the generation has not moved.
    test "moves the replay generation", ctx do
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1_000)
      {incarnation, count} = LsnTracker.replay_generation(ctx.stack_id)

      :ok = LsnTracker.reset_last_broadcast_lsn(ctx.stack_id)

      assert LsnTracker.replay_generation(ctx.stack_id) == {incarnation, count + 1}
    end
  end

  describe "journal replay marker" do
    setup [:with_lsn_tracker]

    test "is down by default, up after mark, down after clear", ctx do
      refute LsnTracker.journal_replay_pending?(ctx.stack_id)

      :ok = LsnTracker.mark_journal_replay_pending(ctx.stack_id)
      assert LsnTracker.journal_replay_pending?(ctx.stack_id)

      :ok = LsnTracker.clear_journal_replay_pending(ctx.stack_id)
      refute LsnTracker.journal_replay_pending?(ctx.stack_id)
    end

    test "reads as down without an initialized tracker" do
      refute LsnTracker.journal_replay_pending?("never-initialized-stack")
    end

    test "journal_replay_settled? tracks the flag inversely", %{stack_id: stack_id} do
      assert LsnTracker.journal_replay_settled?(stack_id)

      :ok = LsnTracker.mark_journal_replay_pending(stack_id)
      refute LsnTracker.journal_replay_settled?(stack_id)

      :ok = LsnTracker.clear_journal_replay_pending(stack_id)
      assert LsnTracker.journal_replay_settled?(stack_id)
    end

    # The two predicates deliberately fail in opposite directions: the pending read
    # fails open (a stale broadcast position is cheap), the settled read fails
    # closed (it releases the slot's durability pin).
    test "journal_replay_settled? fails closed without an initialized tracker" do
      refute LsnTracker.journal_replay_settled?("never-initialized-stack")
    end
  end

  # A writer stopped with the shape subsystem can lose a transaction the collector fanned out, so
  # the watermark it raised says nothing of what the restarted subsystem holds until the replay
  # delivers it again.
  describe "a replay from the slot" do
    setup [:with_registry, :with_lsn_tracker]

    test "starts from the slot and drops the broadcast watermark", ctx do
      :ok = LsnTracker.set_last_processed_lsn(ctx.stack_id, 900)
      :ok = LsnTracker.set_slot_flushed_lsn(ctx.stack_id, 200)
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 800)

      assert {incarnation, 0} = LsnTracker.replay_generation(ctx.stack_id)
      :ok = LsnTracker.request_replay_from_slot(ctx.stack_id)
      assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 0
      assert LsnTracker.replay_generation(ctx.stack_id) == {incarnation, 1}

      # The collector being stopped fans out once more.
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 850)

      :ok = LsnTracker.apply_replay_from_slot(ctx.stack_id)
      assert LsnTracker.get_last_processed_lsn(ctx.stack_id) == Lsn.from_integer(200)
      assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 0
      assert LsnTracker.replay_generation(ctx.stack_id) == {incarnation, 2}
    end

    # A promotion check bounds what Electric took from the old stream by the positions that
    # survive, and the watermark a replay or a journal tail dropped can be the only one past the
    # new server's switchpoint.
    test "keeps what the dropped watermarks covered until it is forgotten", ctx do
      :ok = LsnTracker.set_slot_flushed_lsn(ctx.stack_id, 200)
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1000)
      assert LsnTracker.dropped_high_water(ctx.stack_id) == nil

      :ok = LsnTracker.request_replay_from_slot(ctx.stack_id)
      assert LsnTracker.dropped_high_water(ctx.stack_id) == 999

      # The collector being stopped fans out once more.
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1100)
      :ok = LsnTracker.apply_replay_from_slot(ctx.stack_id)
      assert LsnTracker.dropped_high_water(ctx.stack_id) == 1099

      # A collector that starts with a journal tail drops the watermark too.
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1500)
      :ok = LsnTracker.reset_last_broadcast_lsn(ctx.stack_id)
      assert LsnTracker.dropped_high_water(ctx.stack_id) == 1499

      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 600)
      :ok = LsnTracker.reset_last_broadcast_lsn(ctx.stack_id)
      assert LsnTracker.dropped_high_water(ctx.stack_id) == 1499

      :ok = LsnTracker.forget_dropped_high_water(ctx.stack_id)
      assert LsnTracker.dropped_high_water(ctx.stack_id) == nil
    end

    # The tracker outlives the connection manager that drops the watermark. A drop that took it
    # before it recorded what it covered, and died in between, left the promotion check neither.
    test "keeps the watermark when the drop dies before it records what it covered", ctx do
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1000)

      Repatch.patch(LsnTracker, :raise_dropped_high_water, fn _table, _lsn ->
        raise "the manager died"
      end)

      assert_raise RuntimeError, fn -> LsnTracker.request_replay_from_slot(ctx.stack_id) end
      assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 1000
      assert LsnTracker.dropped_high_water(ctx.stack_id) == nil
    end

    # A broadcast can raise the watermark between the drop's read and its delete. A delete by key
    # took the raised one and kept no more than what the read covered.
    test "covers a watermark a broadcast raises while the drop records the one it read", ctx do
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1000)

      # The first recording stands for that time: the broadcast lands in it, and every later
      # recording is the real one.
      Repatch.patch(LsnTracker, :raise_dropped_high_water, fn _table, _lsn ->
        :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1200)
        Repatch.restore(LsnTracker, :raise_dropped_high_water, 2)
        :ok
      end)

      :ok = LsnTracker.request_replay_from_slot(ctx.stack_id)
      assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 0
      assert LsnTracker.dropped_high_water(ctx.stack_id) == 1199
    end

    test "keeps the watermark of a start that replays nothing", ctx do
      :ok = LsnTracker.set_last_processed_lsn(ctx.stack_id, 900)
      :ok = LsnTracker.set_slot_flushed_lsn(ctx.stack_id, 200)
      :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 800)

      :ok = LsnTracker.apply_replay_from_slot(ctx.stack_id)
      assert LsnTracker.get_last_processed_lsn(ctx.stack_id) == Lsn.from_integer(900)
      assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 800
      assert {_incarnation, 0} = LsnTracker.replay_generation(ctx.stack_id)
    end

    test "takes nothing without an initialized tracker" do
      assert :ok = LsnTracker.apply_replay_from_slot("never-initialized-stack")
    end

    test "has no replay generation without an initialized tracker" do
      assert LsnTracker.replay_generation("never-initialized-stack") == nil
    end

    # The table goes with the shape status owner, and the one created again counts from its own
    # start: a request that read the generation of the one before would find it again.
    test "has another replay generation in a tracker created again", ctx do
      before = LsnTracker.replay_generation(ctx.stack_id)

      true = :ets.delete(LsnTracker.stack_ref(ctx.stack_id))
      :ok = LsnTracker.initialize(ctx.stack_id)

      assert {_incarnation, 0} = LsnTracker.replay_generation(ctx.stack_id)
      refute LsnTracker.replay_generation(ctx.stack_id) == before
    end

    # A table created again between two lookups paired the name of one table with the count of
    # the next, which the test above, recreating it between two reads, does not stage.
    test "keeps the table's name and its count in one row", ctx do
      table = LsnTracker.stack_ref(ctx.stack_id)
      assert [{:replay_generation, incarnation, 0}] = :ets.lookup(table, :replay_generation)

      :ok = LsnTracker.request_replay_from_slot(ctx.stack_id)
      assert [{:replay_generation, ^incarnation, 1}] = :ets.lookup(table, :replay_generation)
    end
  end
end
