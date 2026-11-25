defmodule Electric.ShapeCache.PureFileStorage.LogFileTest do
  use ExUnit.Case, async: true
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.Replication.LogOffset

  @moduletag :tmp_dir

  # 16 bytes of offset, 4 of key size, then the key, then op type and flag.
  defp json_size_field_position(key), do: 16 + 4 + byte_size(key) + 1 + 1

  defp write_log!(tmp_dir, entries) do
    path = Path.join(tmp_dir, "log")
    LogFile.write_log_file(entries, path)
    path
  end

  defp off(tx, op), do: %LogOffset{tx_offset: tx, op_offset: op}

  defp until_offset(path, min, max),
    do:
      %PureFileStorage{}
      |> LogFile.stream_jsons_until_offset(path, 0, min, max)
      |> Enum.to_list()

  describe "trim/4" do
    test "fails without cutting the log when it cannot seek to the search start", %{
      tmp_dir: tmp_dir
    } do
      path =
        write_log!(tmp_dir, [
          {off(1, 1), "key1", :insert, "value1"},
          {off(2, 2), "key2", :insert, "value2"}
        ])

      log = File.read!(path)

      assert_raise File.Error, ~r/could not seek/, fn -> LogFile.trim(path, -1, off(2, 2)) end
      assert File.read!(path) == log
    end

    # The chunk index can point past the end of a torn log.
    test "leaves a log that ends before the search start as it is", %{tmp_dir: tmp_dir} do
      path =
        write_log!(tmp_dir, [
          {off(1, 1), "key1", :insert, "value1"},
          {off(2, 2), "key2", :insert, "value2"}
        ])

      log = File.read!(path)

      assert :short_of_boundary == LogFile.trim(path, byte_size(log) + 100, off(3, 3))
      assert File.read!(path) == log
    end
  end

  describe "stream_entries/3" do
    test "reads back every entry it wrote", %{tmp_dir: tmp_dir} do
      path =
        write_log!(tmp_dir, [
          {%LogOffset{tx_offset: 1, op_offset: 1}, "key1", :insert, "value1"},
          {%LogOffset{tx_offset: 2, op_offset: 2}, "key2", :insert, "value2"}
        ])

      assert [{{%LogOffset{tx_offset: 1}, _, "key1", _, _, _, "value1"}, 0}, {_, _}] =
               path |> LogFile.stream_entries(0) |> Enum.to_list()
    end

    test "raises on a read error rather than reporting the stream as finished", %{
      tmp_dir: tmp_dir
    } do
      key = "key1"

      path =
        write_log!(tmp_dir, [
          {%LogOffset{tx_offset: 1, op_offset: 1}, key, :insert, "value1"},
          {%LogOffset{tx_offset: 2, op_offset: 2}, "key2", :insert, "value2"}
        ])

      # A json size far past anything the file could hold makes the payload read answer
      # {:error, :enomem}, which is what a failing device or a damaged size field looks like from
      # here. A torn tail is deliberately NOT this case — a partial write returns a short binary.
      file = File.open!(path, [:read, :write, :raw])
      :file.pwrite(file, json_size_field_position(key), <<Bitwise.bsl(1, 60)::64>>)
      File.close(file)

      # Halting here would hand the caller an empty list and let a key index or an inlined
      # compaction copy be built from a log that stopped at the first unreadable byte.
      assert_raise File.Error, fn ->
        path |> LogFile.stream_entries(0) |> Enum.to_list()
      end
    end
  end

  describe "open_labeled_logs!/2" do
    test "closes the handles already opened when a later open fails", %{tmp_dir: tmp_dir} do
      # handle it opened so we can see whether it was closed.
      good = Path.join(tmp_dir, "good")
      File.write!(good, "not empty")
      missing = Path.join(tmp_dir, "missing")
      test_pid = self()

      open_fun = fn path ->
        case File.open(path, [:read, :raw, :read_ahead]) do
          {:ok, file} = ok ->
            send(test_pid, {:opened, file})
            ok

          error ->
            error
        end
      end

      assert_raise File.Error, fn ->
        LogFile.open_labeled_logs!([{0, good}, {1, missing}], open_fun)
      end

      assert_received {:opened, handle}

      refute match?({:ok, _}, :file.read(handle, 1)),
             "the handle opened before the failing one must be closed, not leaked"
    end

    test "returns every labeled handle when all inputs open", %{tmp_dir: tmp_dir} do
      a = Path.join(tmp_dir, "a")
      b = Path.join(tmp_dir, "b")
      File.write!(a, "a")
      File.write!(b, "b")

      opened = LogFile.open_labeled_logs!([{0, a}, {1, b}])

      assert Map.keys(opened) |> Enum.sort() == [0, 1]
      Enum.each(opened, fn {_, file} -> File.close(file) end)
    end
  end

  describe "stream_jsons_until_offset/5" do
    test "streams entries after the exclusive min through the inclusive max and stops before the tail",
         %{tmp_dir: tmp_dir} do
      path =
        write_log!(tmp_dir, [
          {off(1, 1), "k1", :insert, "j1"},
          {off(1, 2), "k2", :insert, "j2"},
          {off(1, 3), "k3", :insert, "j3"},
          {off(1, 4), "k4", :insert, "j4"},
          {off(1, 5), "k5", :insert, "j5"}
        ])

      result = until_offset(path, off(1, 1), off(1, 3))

      assert result == ["j2", "j3"]
    end

    test "returns a large row spanning many read blocks byte-for-byte", %{tmp_dir: tmp_dir} do
      large = String.duplicate("x", 200_000)

      path =
        write_log!(tmp_dir, [
          {off(1, 1), "k1", :insert, "small-before"},
          {off(1, 2), "k2", :insert, large},
          {off(1, 3), "k3", :insert, "small-after"}
        ])

      result = until_offset(path, off(1, 0), off(1, 3))

      assert result == ["small-before", large, "small-after"]
    end

    test "assembles a large boundary row and excludes the entry past it", %{tmp_dir: tmp_dir} do
      large = String.duplicate("y", 150_000)

      path =
        write_log!(tmp_dir, [
          {off(1, 1), "k1", :insert, large},
          {off(1, 2), "k2", :insert, "beyond-max"}
        ])

      result = until_offset(path, off(1, 0), off(1, 1))

      assert result == [large]
    end

    test "stops at the first entry past a committed offset that is absent", %{tmp_dir: tmp_dir} do
      path =
        write_log!(tmp_dir, [
          {off(1, 1), "k1", :insert, "j1"},
          {off(1, 2), "k2", :insert, "j2"},
          {off(1, 5), "k5", :insert, "j5"}
        ])

      result = until_offset(path, off(1, 0), off(1, 3))

      assert result == ["j1", "j2"]
    end

    test "returns an empty stream when every entry precedes the exclusive min", %{
      tmp_dir: tmp_dir
    } do
      path =
        write_log!(tmp_dir, [
          {off(1, 1), "k1", :insert, "j1"},
          {off(1, 2), "k2", :insert, "j2"}
        ])

      result = until_offset(path, off(1, 5), off(1, 9))

      assert result == []
    end
  end
end
