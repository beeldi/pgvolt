defmodule Electric.Replication.TxnJournalGcOrderingTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.TxnJournal

  @moduletag :tmp_dir

  defp gen_with_no_string_representation, do: %{}

  test "records reclaimed coverage durably before deleting the generation", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    writer = %{writer | closed_gens: [{gen_with_no_string_representation(), 5}]}

    assert_raise Protocol.UndefinedError, fn -> TxnJournal.gc!(writer, 10) end

    assert TxnJournal.coverage_record(tmp_dir).reclaimed == 5
  end

  test "gc! deletes reclaimable generations and records their boundary", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    dir = Path.join(tmp_dir, "txn-journal")
    File.write!(Path.join(dir, "1.bin"), "")
    writer = %{writer | closed_gens: [{1, 5}]}

    writer = TxnJournal.gc!(writer, 10)

    refute File.exists?(Path.join(dir, "1.bin"))
    assert writer.closed_gens == []
    assert TxnJournal.coverage_record(tmp_dir).reclaimed == 5
  end
end
