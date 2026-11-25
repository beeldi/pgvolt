defmodule Electric.Replication.TxnJournalDurabilityTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.Lsn
  alias Electric.Replication.TxnJournal

  @moduletag :tmp_dir

  # `do_sync/2` datasyncs a generation's bytes; that never covers the directory entry naming it.
  # Every reader reaches a generation by name, and the boot guard keys off `list_generations/1`, so
  # a lost entry reads as "there was never a journal" rather than as the invariant violation it is.
  #
  # SCOPE -- read before extending: this proves the durable path is taken and works on the real
  # filesystem (`sync_dir!/1` raises on failure). It does not prove the entry survives power loss;
  # that needs a crash-consistency fault injector, and the syscalls were checked with strace instead.
  test "opening a generation makes its directory entry durable", %{tmp_dir: tmp_dir} do
    # Arrange / Act
    writer = TxnJournal.open_writer!(tmp_dir)

    # Assert -- reachable by name, which is the only way any reader finds it.
    assert TxnJournal.list_generations(tmp_dir) == [writer.gen]
    assert File.exists?(Path.join([tmp_dir, "txn-journal", "#{writer.gen}.bin"]))
  end

  test "a rolled generation is reachable by name too", %{tmp_dir: tmp_dir} do
    # Arrange
    writer = TxnJournal.open_writer!(tmp_dir)
    first = writer.gen

    # Act -- a commit sync is what rolls, and rolling opens a new generation file.
    writer = TxnJournal.append_event!(writer, {:probe, 1})
    writer = TxnJournal.sync!(writer, Lsn.to_integer(Lsn.from_integer(10)))

    # Assert
    assert first in TxnJournal.list_generations(tmp_dir)
    assert writer.gen in TxnJournal.list_generations(tmp_dir)
  end
end
