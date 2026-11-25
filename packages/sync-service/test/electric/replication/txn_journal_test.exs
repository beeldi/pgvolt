defmodule Electric.Replication.TxnJournalTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Replication.TxnJournal

  require Repatch

  @moduletag :tmp_dir

  defp fragment(tx_offset) do
    %TransactionFragment{
      xid: tx_offset,
      lsn: Electric.Postgres.Lsn.from_integer(tx_offset),
      last_log_offset: LogOffset.new(tx_offset, 0),
      has_begin?: true,
      commit: %Changes.Commit{},
      changes: [
        %Changes.NewRecord{
          relation: {"public", "test_table"},
          record: %{"id" => "#{tx_offset}"},
          log_offset: LogOffset.new(tx_offset, 0)
        }
      ],
      change_count: 1
    }
  end

  defp relation do
    %Changes.Relation{id: 1234, schema: "public", table: "test_table", columns: []}
  end

  defp journal_dir(tmp_dir), do: Path.join(tmp_dir, "txn-journal")

  test "round-trips appended events in order", %{tmp_dir: tmp_dir} do
    events = [fragment(10), relation(), fragment(20)]

    writer = TxnJournal.open_writer!(tmp_dir)
    writer = Enum.reduce(events, writer, &TxnJournal.append_event!(&2, &1))
    TxnJournal.sync!(writer, 20)

    gens = TxnJournal.list_generations(tmp_dir)
    assert length(gens) == 1
    assert TxnJournal.stream_events!(tmp_dir, gens) |> Enum.to_list() == events
    assert TxnJournal.last_commit_tx_offset(tmp_dir, gens) == 20
  end

  test "an idle-slot-boundary marker widens the commit boundary", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    writer = TxnJournal.append_event!(writer, fragment(10))
    writer = TxnJournal.sync!(writer, 10)

    writer = TxnJournal.append_event!(writer, {:idle_slot_boundary, 9_000_000})
    TxnJournal.sync!(writer, 9_000_000)

    # ack still finds the slot inside the journal's coverage.
    gens = TxnJournal.list_generations(tmp_dir)
    assert TxnJournal.last_commit_tx_offset(tmp_dir, gens) == 9_000_000

    assert TxnJournal.stream_events!(tmp_dir, gens) |> Enum.to_list() ==
             [fragment(10), {:idle_slot_boundary, 9_000_000}]
  end

  test "a relation-only journal has no commit boundary", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)

    writer = TxnJournal.append_event!(writer, relation())
    TxnJournal.sync!(writer)

    gens = TxnJournal.list_generations(tmp_dir)
    assert TxnJournal.last_commit_tx_offset(tmp_dir, gens) == nil
  end

  describe "last_commit_log_offset/2" do
    test "is nil when the journal holds no transaction commit", %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, relation())
      writer = TxnJournal.append_event!(writer, {:idle_slot_boundary, 500})
      writer = TxnJournal.sync!(writer, 500)
      writer = TxnJournal.append_event!(writer, %{fragment(600) | commit: nil})
      TxnJournal.sync!(writer)

      assert TxnJournal.last_commit_log_offset(tmp_dir, TxnJournal.list_generations(tmp_dir)) ==
               nil
    end

    test "is where the last committed transaction ends, inside that transaction",
         %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, fragment(10))
      writer = TxnJournal.sync!(writer, 10)
      writer = TxnJournal.append_event!(writer, %{fragment(20) | commit: nil})

      writer =
        TxnJournal.append_event!(writer, %{fragment(20) | last_log_offset: LogOffset.new(20, 4)})

      writer = TxnJournal.sync!(writer, 20)
      writer = TxnJournal.append_event!(writer, {:idle_slot_boundary, 500})
      writer = TxnJournal.sync!(writer, 500)
      writer = TxnJournal.append_event!(writer, %{fragment(600) | commit: nil})
      TxnJournal.sync!(writer)

      assert TxnJournal.last_commit_log_offset(tmp_dir, TxnJournal.list_generations(tmp_dir)) ==
               LogOffset.new(20, 4)
    end
  end

  test "reading stops at a torn tail", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    writer = TxnJournal.append_event!(writer, fragment(10))
    writer = TxnJournal.sync!(writer, 10)
    [gen] = TxnJournal.list_generations(tmp_dir)

    File.write!(Path.join(journal_dir(tmp_dir), "#{gen}.bin"), <<999::32, 0::16>>, [:append])
    _writer = writer

    assert TxnJournal.stream_events!(tmp_dir, [gen]) |> Enum.to_list() == [fragment(10)]
  end

  test "reading stops at a corrupt frame", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    writer = TxnJournal.append_event!(writer, fragment(10))
    writer = TxnJournal.append_event!(writer, fragment(20))
    TxnJournal.sync!(writer, 20)
    [gen] = TxnJournal.list_generations(tmp_dir)
    path = Path.join(journal_dir(tmp_dir), "#{gen}.bin")

    contents = File.read!(path)
    corrupted = binary_part(contents, 0, byte_size(contents) - 1) <> <<255>>
    File.write!(path, corrupted)

    assert TxnJournal.stream_events!(tmp_dir, [gen]) |> Enum.to_list() == [fragment(10)]
  end

  # A tail is the only frame a crash can leave torn. Anywhere else the frames behind it are
  # real and journal-durable, and the slot was acked past them, so skipping the rest of that
  # generation drops transactions Postgres will never resend.
  test "reading raises on an unreadable frame in a generation that is not the newest",
       %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    writer = TxnJournal.append_event!(writer, fragment(10))
    writer = TxnJournal.append_event!(writer, fragment(20))
    writer = TxnJournal.sync!(writer, 20)
    writer = %{writer | position: 64 * 1024 * 1024}
    writer = TxnJournal.append_event!(writer, fragment(30))
    TxnJournal.sync!(writer, 30)

    assert [old_gen, _newest] = gens = TxnJournal.list_generations(tmp_dir)
    old_path = Path.join(journal_dir(tmp_dir), "#{old_gen}.bin")
    contents = File.read!(old_path)

    corrupted =
      binary_part(contents, 0, 12) <>
        <<Bitwise.bxor(:binary.at(contents, 12), 255)>> <>
        binary_part(contents, 13, byte_size(contents) - 13)

    File.write!(old_path, corrupted)

    assert_raise RuntimeError, ~r/unreadable before its end/, fn ->
      TxnJournal.stream_events!(tmp_dir, gens) |> Enum.to_list()
    end
  end

  test "gc! deletes only closed generations the boundary has passed", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    dir = journal_dir(tmp_dir)
    File.write!(Path.join(dir, "1.bin"), "old")
    File.write!(Path.join(dir, "2.bin"), "newer")
    writer = %{writer | closed_gens: [{2, 30}, {1, 10}]}

    writer = TxnJournal.gc!(writer, 15)

    refute File.exists?(Path.join(dir, "1.bin"))
    assert File.exists?(Path.join(dir, "2.bin"))
    assert writer.closed_gens == [{2, 30}]
    assert File.exists?(Path.join(dir, "#{writer.gen}.bin"))
  end

  # Relations arrive mid transaction and sync without a commit boundary. Rolling there
  # would label the closed generation with the PREVIOUS commit, and gc! would delete it —
  # along with the open transaction's leading fragments — as soon as that previous commit
  # became consumer-durable.
  test "a relation sync never rolls the generation", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)
    writer = TxnJournal.append_event!(writer, fragment(10))
    writer = TxnJournal.sync!(writer, 10)
    writer = %{writer | position: 64 * 1024 * 1024}

    writer = TxnJournal.append_event!(writer, fragment(20))
    writer = TxnJournal.sync!(writer)

    assert writer.closed_gens == []
    assert writer.last_synced_tx_offset == 10

    # A commit sync may roll, and the closed generation is then labelled with the commit
    # it actually ends on.
    writer = TxnJournal.sync!(writer, 20)
    assert [{_gen, 20}] = writer.closed_gens
  end

  test "spans generations in order when streaming", %{tmp_dir: tmp_dir} do
    writer1 = TxnJournal.open_writer!(tmp_dir)
    writer1 = TxnJournal.append_event!(writer1, fragment(10))
    TxnJournal.sync!(writer1, 10)

    writer2 = TxnJournal.open_writer!(tmp_dir)
    writer2 = TxnJournal.append_event!(writer2, fragment(20))
    TxnJournal.sync!(writer2, 20)

    gens = TxnJournal.list_generations(tmp_dir)
    assert length(gens) == 2

    assert TxnJournal.stream_events!(tmp_dir, gens) |> Enum.to_list() == [
             fragment(10),
             fragment(20)
           ]

    assert TxnJournal.last_commit_tx_offset(tmp_dir, gens) == 20
  end

  # Generation ids are stamped from the wall clock, which can step backwards. Replay order
  # and the "only the newest may have a torn tail" rule both come from sorting those ids.
  test "opens a generation above the ones on disk when the clock has stepped back", %{
    tmp_dir: tmp_dir
  } do
    ahead = (System.system_time(:millisecond) + :timer.hours(1)) * 256
    File.mkdir_p!(journal_dir(tmp_dir))
    File.write!(Path.join(journal_dir(tmp_dir), "#{ahead}.bin"), <<>>)

    writer = TxnJournal.open_writer!(tmp_dir)

    assert writer.gen > ahead
    assert List.last(TxnJournal.list_generations(tmp_dir)) == writer.gen
  end

  test "coverage_record reads a default before anything is recorded", %{tmp_dir: tmp_dir} do
    assert TxnJournal.coverage_record(tmp_dir) == %{committed?: false, reclaimed: nil}
  end

  # The record is what tells a journal that acked a boundary and lost it from one that never
  # acked one, and what a generation's absence means. Both facts only move forward, so a stale
  # write can never widen what the readable generations are asked to account for.
  test "record_coverage! latches committed? and only advances reclaimed", %{tmp_dir: tmp_dir} do
    :ok = TxnJournal.record_coverage!(tmp_dir, committed?: true)
    :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 50)
    :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 30)
    :ok = TxnJournal.record_coverage!(tmp_dir, committed?: false)

    assert TxnJournal.coverage_record(tmp_dir) == %{committed?: true, reclaimed: 50}
  end

  # A journal that syncs a commit has acked a boundary; the record has to say so durably and
  # independently of the generations, or the boot guard cannot tell a stack whose acknowledged
  # history is gone from one that never acked anything.
  test "sync! records that a boundary was committed into the journal", %{tmp_dir: tmp_dir} do
    writer = TxnJournal.open_writer!(tmp_dir)

    writer = TxnJournal.append_event!(writer, fragment(10))
    writer = TxnJournal.sync!(writer, 10)

    assert writer.committed_recorded?
    assert TxnJournal.coverage_record(tmp_dir) == %{committed?: true, reclaimed: nil}
  end

  # The coverage a reclaimed generation carried leaves the journal with it, so the record has to
  # take it over. An adopted generation with no commit of its own carries the -1 sentinel and
  # reclaims nothing, so it must not lower the boundary.
  test "gc! hands the reclaimed boundary to the coverage record", %{tmp_dir: tmp_dir} do
    # boundary is about to pass
    writer = TxnJournal.open_writer!(tmp_dir)
    dir = journal_dir(tmp_dir)
    File.write!(Path.join(dir, "1.bin"), "committed")
    File.write!(Path.join(dir, "2.bin"), "adopted")
    writer = %{writer | closed_gens: [{2, -1}, {1, 10}]}

    _writer = TxnJournal.gc!(writer, 15)

    assert TxnJournal.coverage_record(tmp_dir) == %{committed?: false, reclaimed: 10}
  end

  describe "high_water/1" do
    test "is nil when the journal shows nothing taken", %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, relation())
      TxnJournal.sync!(writer)

      assert TxnJournal.high_water(tmp_dir) == nil
    end

    # A fragment reaches the collector before its commit does, and the consumers may already
    # have written it; the journal's commit boundary alone would miss it.
    test "counts a fragment whose commit has not arrived", %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, fragment(10))
      writer = TxnJournal.sync!(writer, 10)
      writer = TxnJournal.append_event!(writer, %{fragment(30) | commit: nil})
      TxnJournal.sync!(writer)

      assert TxnJournal.last_commit_tx_offset(tmp_dir, TxnJournal.list_generations(tmp_dir)) ==
               10

      assert TxnJournal.high_water(tmp_dir) == 30
    end

    test "counts an idle boundary", %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, fragment(10))
      writer = TxnJournal.sync!(writer, 10)
      writer = TxnJournal.append_event!(writer, {:idle_slot_boundary, 500})
      TxnJournal.sync!(writer, 500)

      assert TxnJournal.high_water(tmp_dir) == 500
    end

    test "counts a boundary whose generation was reclaimed", %{tmp_dir: tmp_dir} do
      :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 700)

      assert TxnJournal.high_water(tmp_dir) == 700
    end

    # The generations it cannot list may hold transactions past the reclaimed coverage. A
    # symlink to itself fails the listing as a root user too.
    test "fails when the journal cannot be listed", %{tmp_dir: tmp_dir} do
      :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 700)
      File.ln_s!("txn-journal", Path.join(tmp_dir, "txn-journal"))

      assert_raise File.Error, ~r/list directory/, fn -> TxnJournal.high_water(tmp_dir) end
    end

    # A journal that wrote a generation always keeps one, so with none left the transactions
    # they held are gone, past the reclaimed coverage maybe, whether the directory went with
    # them or not.
    test "fails when every generation it wrote is gone", %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, fragment(900))
      TxnJournal.sync!(writer, 900)
      :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 700)

      File.rm!(Path.join(journal_dir(tmp_dir), "#{writer.gen}.bin"))
      assert_raise RuntimeError, ~r/no generation/, fn -> TxnJournal.high_water(tmp_dir) end

      File.rm_rf!(journal_dir(tmp_dir))
      assert_raise RuntimeError, ~r/no generation/, fn -> TxnJournal.high_water(tmp_dir) end
    end

    # A marker read as absent lets a journal with no generation left pass for one that never
    # wrote any. A symlink to itself fails the read as a root user too.
    test "fails when the used marker cannot be read", %{tmp_dir: tmp_dir} do
      :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 700)
      File.ln_s!("txn-journal.used", Path.join(tmp_dir, "txn-journal.used"))

      assert_raise File.Error, ~r/read file stats/, fn -> TxnJournal.high_water(tmp_dir) end
    end

    # A stat reads a symlink whose target is gone as absent, and a marker read as absent lets a
    # journal with no generation left pass for one that never wrote any.
    test "fails when every generation it wrote is gone and its used marker is a dangling symlink",
         %{tmp_dir: tmp_dir} do
      writer = TxnJournal.open_writer!(tmp_dir)
      writer = TxnJournal.append_event!(writer, fragment(900))
      TxnJournal.sync!(writer, 900)
      :ok = TxnJournal.record_coverage!(tmp_dir, reclaimed: 700)
      File.rm!(Path.join(journal_dir(tmp_dir), "#{writer.gen}.bin"))

      marker = Path.join(tmp_dir, "txn-journal.used")
      File.rm!(marker)
      File.ln_s!("txn-journal.used.moved", marker)

      assert_raise RuntimeError, ~r/no generation/, fn -> TxnJournal.high_water(tmp_dir) end
    end

    # A base path that is a symlink whose target is away reads, to the marker's looks and to the
    # listing, as a journal that never wrote a generation, and the coverage read once the target
    # is back gives the high water, behind the transactions the generations hold.
    test "fails on a base path that is a symlink whose target is away for its listing",
         %{tmp_dir: tmp_dir} do
      {store, linked} = linked_journal(tmp_dir)
      assert TxnJournal.high_water(linked) == 900

      away = store <> ".away"
      coverage = Path.join(linked, "txn-journal.coverage")
      File.rename!(store, away)

      Repatch.patch(File, :read, fn path ->
        if path == coverage and File.exists?(away), do: File.rename!(away, store)
        Repatch.real(File.read(path))
      end)

      error = assert_raise File.Error, fn -> TxnJournal.high_water(linked) end
      assert {error.reason, error.action, error.path} == {:enoent, "list directory", linked}
    end

    # The base path names a journal directory its listing missed while the target was away, and
    # the generations are read from a listing made again.
    test "reads the generations of a journal its listing missed while its base path was away",
         %{tmp_dir: tmp_dir} do
      {store, linked} = linked_journal(tmp_dir)
      dir = journal_dir(linked)

      Repatch.patch(File, :ls, fn path ->
        if path == dir and not Process.get(:journal_listed, false) do
          Process.put(:journal_listed, true)
          File.rename!(store, store <> ".away")
          listing = Repatch.real(File.ls(path))
          File.rename!(store <> ".away", store)
          listing
        else
          Repatch.real(File.ls(path))
        end
      end)

      assert TxnJournal.high_water(linked) == 900
    end

    # A marker the looks missed while the target was away, in a base path whose listing names it
    # once the target is back, still says the journal wrote a generation.
    test "fails when every generation is gone and its base path was away for the marker's looks",
         %{tmp_dir: tmp_dir} do
      {store, linked} = linked_journal(tmp_dir)
      Enum.each(File.ls!(journal_dir(store)), &File.rm!(Path.join(journal_dir(store), &1)))
      marker = Path.join(linked, "txn-journal.used")

      Repatch.patch(File, :stat, fn path ->
        if path == marker, do: File.rename!(store, store <> ".away")
        Repatch.real(File.stat(path))
      end)

      Repatch.patch(File, :lstat, fn path ->
        stat = Repatch.real(File.lstat(path))
        if path == marker, do: File.rename!(store <> ".away", store)
        stat
      end)

      assert_raise RuntimeError, ~r/no generation/, fn -> TxnJournal.high_water(linked) end
    end

    # A base path a remount takes away for the marker's looks and the journal's listing reads, to
    # both, as a journal that never wrote, and one back by the look after the base path's listing
    # gives the coverage as the high water, behind the transactions the generations hold.
    test "fails on a base path a remount brings back after its listing missed it",
         %{tmp_dir: tmp_dir} do
      store = journal(Path.join(tmp_dir, "store"))
      marker = Path.join(store, "txn-journal.used")

      Repatch.patch(File, :stat, fn path ->
        if path == marker, do: File.rename!(store, store <> ".away")
        Repatch.real(File.stat(path))
      end)

      # The second look at the base path is the one after the journal's listing missed it.
      Repatch.patch(File, :lstat, fn path ->
        if path == store do
          looks = Process.get(:base_looks, 0) + 1
          Process.put(:base_looks, looks)
          if looks == 2, do: File.rename!(store <> ".away", store)
        end

        Repatch.real(File.lstat(path))
      end)

      error = assert_raise File.Error, fn -> TxnJournal.high_water(store) end
      assert {error.reason, error.action, error.path} == {:enoent, "list directory", store}
    end

    # One back only after the look that follows the base path's listing: the scan found it gone,
    # and the coverage read after it stops short of the transactions the generation holds.
    test "has no high water once its scan found the base path gone", %{tmp_dir: tmp_dir} do
      store = journal(Path.join(tmp_dir, "store"))
      marker = Path.join(store, "txn-journal.used")

      Repatch.patch(File, :stat, fn path ->
        if path == marker, do: File.rename!(store, store <> ".away")
        Repatch.real(File.stat(path))
      end)

      Repatch.patch(File, :lstat, fn path ->
        stat = Repatch.real(File.lstat(path))

        if path == store do
          looks = Process.get(:base_looks, 0) + 1
          Process.put(:base_looks, looks)
          if looks == 2, do: File.rename!(store <> ".away", store)
        end

        stat
      end)

      assert TxnJournal.high_water(store) == nil
      assert File.exists?(Path.join(store, "txn-journal.coverage"))
    end

    # A remount that takes the base path away can leave the directory under its mount point, empty
    # say, which lists to the marker's look and to the journal's listing as a journal that never
    # wrote. Back before the coverage is read, it gives that coverage alone as the high water.
    test "fails on a base path a remount empties for its listings and brings back for its coverage",
         %{tmp_dir: tmp_dir} do
      store = journal(Path.join(tmp_dir, "store"))
      marker = Path.join(store, "txn-journal.used")
      coverage = Path.join(store, "txn-journal.coverage")

      Repatch.patch(File, :stat, fn path ->
        if path == marker and not Process.get(:remounted, false) do
          Process.put(:remounted, true)
          File.rename!(store, store <> ".away")
          File.mkdir!(store)
        end

        Repatch.real(File.stat(path))
      end)

      Repatch.patch(File, :read, fn path ->
        if path == coverage and File.dir?(store <> ".away") do
          File.rmdir!(store)
          File.rename!(store <> ".away", store)
        end

        Repatch.real(File.read(path))
      end)

      assert_raise RuntimeError, ~r/once its coverage was read/, fn ->
        TxnJournal.high_water(store)
      end
    end

    test "is the coverage alone of a base path that holds it alone", %{tmp_dir: tmp_dir} do
      store = Path.join(tmp_dir, "store")
      File.mkdir_p!(store)
      :ok = TxnJournal.record_coverage!(store, reclaimed: 700)

      assert TxnJournal.high_water(store) == 700
    end
  end

  # A journal that wrote a generation through 900 and reclaimed its coverage to 700, under a store
  # reached through a symlink.
  defp linked_journal(tmp_dir) do
    store = journal(Path.join(tmp_dir, "store"))
    linked = Path.join(tmp_dir, "linked")
    File.ln_s!(store, linked)
    {store, linked}
  end

  defp journal(store) do
    writer = TxnJournal.open_writer!(store)
    writer = TxnJournal.append_event!(writer, fragment(900))
    TxnJournal.sync!(writer, 900)
    :ok = TxnJournal.record_coverage!(store, reclaimed: 700)
    store
  end
end
