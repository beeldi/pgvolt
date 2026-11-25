defmodule Electric.Replication.JournalStrictnessTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.TxnJournal

  @moduletag :tmp_dir

  # Corruption in a generation that is not the newest is not a crash tail, and every read of it
  # has to say so. The tail-tolerant generation is the newest one ON DISK: derive it from the
  # caller's list instead and a single-generation read hands tolerance to whatever it was asked
  # for, which is how `generation_summary/2` and the per-generation `last_commit_tx_offset/2`
  # used to accept a truncated read as a whole one.
  defp frame(term) do
    bin = :erlang.term_to_binary(term)
    <<byte_size(bin)::32, :erlang.crc32(bin)::32, bin::binary>>
  end

  defp write_gen!(base, gen, iodata) do
    dir = Path.join(base, "txn-journal")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "#{gen}.bin"), iodata)
  end

  setup %{tmp_dir: tmp_dir} do
    # 256 carries a good frame, then a frame whose crc does not match its payload, then more
    # bytes: media corruption mid-file, not a torn tail. 512 is a clean, newer generation.
    write_gen!(tmp_dir, 256, [frame(%{a: 1}), <<0, 0, 0, 8, 9, 9, 9, 9>>, "xxxxxxxx"])
    write_gen!(tmp_dir, 512, [frame(%{b: 2})])

    %{base: tmp_dir}
  end

  test "the full-list read refuses the corrupt older generation", %{base: base} do
    assert_raise RuntimeError, ~r/unreadable before its end/, fn ->
      TxnJournal.stream_events!(base, [256, 512]) |> Enum.to_list()
    end
  end

  test "reading that generation on its own refuses it too", %{base: base} do
    assert_raise RuntimeError, ~r/unreadable before its end/, fn ->
      TxnJournal.stream_events!(base, [256]) |> Enum.to_list()
    end
  end

  test "generation_summary refuses it rather than reporting the surviving prefix", %{base: base} do
    assert_raise RuntimeError, ~r/unreadable before its end/, fn ->
      TxnJournal.generation_summary(base, 256)
    end
  end

  test "the per-generation last_commit_tx_offset refuses it", %{base: base} do
    assert_raise RuntimeError, ~r/unreadable before its end/, fn ->
      TxnJournal.last_commit_tx_offset(base, [256])
    end
  end

  # The control: the newest generation's torn tail is still the one corruption every read
  # accepts, whether it is asked for alone or alongside the others.
  test "a torn tail in the newest generation stays acceptable", %{base: base} do
    File.rm!(Path.join([base, "txn-journal", "256.bin"]))
    write_gen!(base, 512, [frame(%{b: 2}), <<0, 0, 0, 8, 9, 9, 9, 9>>])

    assert TxnJournal.stream_events!(base, [512]) |> Enum.to_list() == [%{b: 2}]
    assert TxnJournal.generation_summary(base, 512) == %{coverage: nil, last_commit: nil}
  end
end
