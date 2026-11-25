defmodule Electric.Replication.ShapeLogCollectorModeOffJournalReclaimOrderingTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.ShapeLogCollector.FlushTracker
  alias Electric.Replication.TxnJournal

  @moduletag :tmp_dir

  defp gen_with_no_string_representation, do: %{}

  defp tracker_durable_through(tx_offset) do
    %{FlushTracker.new() | last_global_flushed_offset: LogOffset.new(tx_offset, 0)}
  end

  test "mode-off reclaim records reclaimed coverage durably before deleting the generations", %{
    tmp_dir: tmp_dir
  } do
    state = %{
      txn_journal_base: tmp_dir,
      journal_gens_awaiting_durability: {[gen_with_no_string_representation()], 5},
      flush_tracker: tracker_durable_through(10)
    }

    assert_raise Protocol.UndefinedError, fn ->
      ShapeLogCollector.maybe_delete_durably_replayed_journal(state)
    end

    assert TxnJournal.coverage_record(tmp_dir).reclaimed == 5
  end

  test "mode-off reclaim leaves the boundary untouched until consumers are durable past it", %{
    tmp_dir: tmp_dir
  } do
    state = %{
      txn_journal_base: tmp_dir,
      journal_gens_awaiting_durability: {[1], 5},
      flush_tracker: tracker_durable_through(4)
    }

    result = ShapeLogCollector.maybe_delete_durably_replayed_journal(state)

    assert result.journal_gens_awaiting_durability == {[1], 5}
    assert TxnJournal.coverage_record(tmp_dir).reclaimed == nil
  end
end
