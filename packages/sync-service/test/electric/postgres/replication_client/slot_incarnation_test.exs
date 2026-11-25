defmodule Electric.Postgres.ReplicationClient.SlotIncarnationTest do
  use ExUnit.Case, async: true

  alias Electric.PersistentKV
  alias Electric.Postgres.ReplicationClient.SlotIncarnation

  @stack_id "slot-incarnation-test"
  @key "replication_slot_incarnation_#{@stack_id}"

  defp attempt(kv, outcome, slot_name \\ "slot") do
    prepared = SlotIncarnation.prepare(kv, @stack_id, slot_name)
    SlotIncarnation.settle(kv, @stack_id, prepared, outcome)
  end

  test "keeps the incarnation of an existing slot and rotates it for a created one" do
    kv = PersistentKV.Memory.new!()

    created = attempt(kv, :created)
    assert attempt(kv, :existing) == created
    assert attempt(kv, :existing) == created

    recreated = attempt(kv, :created)
    assert recreated != created
    assert attempt(kv, :existing) == recreated
  end

  test "an existing slot without a recorded incarnation gets a fresh one that then holds" do
    kv = PersistentKV.Memory.new!()

    first = attempt(kv, :existing)
    assert is_binary(first)
    assert attempt(kv, :existing) == first
  end

  test "an attempt interrupted before settling cannot keep the previous incarnation" do
    kv = PersistentKV.Memory.new!()
    settled = attempt(kv, :created)

    # The process died after CREATE_REPLICATION_SLOT was sent: the slot may now be new.
    _unsettled = SlotIncarnation.prepare(kv, @stack_id, "slot")

    after_crash = attempt(kv, :existing)
    assert after_crash != settled
    assert attempt(kv, :existing) == after_crash
  end

  test "an ordinary start over an existing slot discards its provisional purge" do
    kv = PersistentKV.Memory.new!()
    incarnation = attempt(kv, :existing)

    prepared = SlotIncarnation.prepare(kv, @stack_id, "slot", :required)
    assert SlotIncarnation.settle(kv, @stack_id, prepared, :existing) == incarnation
    assert SlotIncarnation.shape_purge(kv, @stack_id) == nil
  end

  test "an interrupted or successful replacement leaves a purge across later starts" do
    for outcome <- [:interrupted, :created] do
      kv = PersistentKV.Memory.new!()
      prepared = SlotIncarnation.prepare(kv, @stack_id, "slot", :required)
      if outcome == :created, do: SlotIncarnation.settle(kv, @stack_id, prepared, outcome)

      attempt(kv, :existing)
      attempt(kv, :existing)
      assert SlotIncarnation.shape_purge(kv, @stack_id) == :required

      assert :ok = SlotIncarnation.clear_shape_purge(kv, @stack_id)
      attempt(kv, :existing)
      assert SlotIncarnation.shape_purge(kv, @stack_id) == nil
    end
  end

  test "a record for another slot does not name this one" do
    kv = PersistentKV.Memory.new!()
    other = attempt(kv, :created, "other_slot")

    assert attempt(kv, :existing, "slot") != other
  end

  test "an unreadable record yields a fresh incarnation" do
    kv = PersistentKV.Memory.new!([{@key, "not a record"}])
    assert is_binary(attempt(kv, :existing))
  end

  test "without persistent state every connection is a new incarnation" do
    assert attempt(nil, :existing) != attempt(nil, :existing)
  end

  @tag :tmp_dir
  test "survives the filesystem store", %{tmp_dir: tmp_dir} do
    kv = PersistentKV.Filesystem.new!(root: tmp_dir)
    created = attempt(kv, :created)

    reopened = PersistentKV.Filesystem.new!(root: tmp_dir)
    assert attempt(reopened, :existing) == created
  end
end
