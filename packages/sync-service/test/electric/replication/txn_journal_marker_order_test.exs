defmodule Electric.Replication.TxnJournalMarkerOrderTest do
  # The used-marker tells a stack whose acknowledged history is gone from one that never journaled
  # anything: a marker beside no generation at all is read, at every later boot, as the first. So
  # the marker may only be made durable once a generation exists to be found by name.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  alias Electric.Replication.TxnJournal

  @moduletag :tmp_dir

  test "a failure while the first generation is made durable leaves no marker without a journal",
       %{tmp_dir: base_path} do
    journal_dir = Path.join(base_path, "txn-journal")

    # The directory sync that makes the generation's entry durable fails, and the entry is lost
    # with it, as when the host goes down before the entry reached disk.
    Repatch.patch(Electric.Utils, :sync_dir!, [mode: :local], fn dir ->
      if dir == journal_dir do
        File.rm_rf!(journal_dir)
        raise "the host went down"
      else
        Repatch.real(Electric.Utils.sync_dir!(dir))
      end
    end)

    assert_raise RuntimeError, "the host went down", fn -> TxnJournal.open_writer!(base_path) end

    assert TxnJournal.list_generations(base_path) != [] or not TxnJournal.used?(base_path)
  end

  test "the marker is in place once the writer is open", %{tmp_dir: base_path} do
    writer = TxnJournal.open_writer!(base_path)

    assert TxnJournal.used?(base_path)
    assert TxnJournal.list_generations(base_path) == [writer.gen]
  end
end
