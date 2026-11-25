defmodule Electric.ShapeCache.PureFileStorage.KeyIndex do
  alias Electric.ShapeCache.Storage
  alias Electric.Utils
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.Replication.LogOffset

  @key_index_entry_size 1 + 16 + 1 + 8 + 8
  @key_index_full_size @key_index_entry_size + 4

  @spec make_entry(LogFile.log_item_with_sizes(), non_neg_integer(), non_neg_integer()) ::
          {iodata(), iodata_size :: non_neg_integer()}
  def make_entry(
        {offset, key_size, key, op_type, _, json_size, _},
        log_file_entry_start_pos,
        label \\ 0
      ) do
    {[
       <<key_size + @key_index_entry_size::32, label::8, LogOffset.to_int128(offset)::binary,
         op_type::8, log_file_entry_start_pos::64, json_size::64>>,
       key
     ], key_size + @key_index_full_size}
  end

  def expected_position(pos, {_, key_size, _, _, _, _, _}),
    do: @key_index_full_size + key_size + pos

  def read_key_file(path) do
    File.open!(path, [:read, :raw, :read_ahead], fn file ->
      Stream.unfold({file, 0}, fn {file, pos} ->
        with <<total_size::32, _label::8, tx_offset::64, op_offset::64, op_type::8,
               log_file_entry_start_pos::64, json_size::64>> <-
               IO.binread(file, @key_index_full_size),
             <<key::binary-size(^total_size - @key_index_entry_size)>> <-
               IO.binread(file, total_size - @key_index_entry_size) do
          {{key, LogOffset.new(tx_offset, op_offset), op_type, log_file_entry_start_pos,
            json_size}, {file, pos + total_size + 4}}
        else
          :eof ->
            nil

          _ ->
            raise Storage.Error,
              kind: :data_loss,
              message:
                "Incomplete keyfile entry at #{pos} (started reading from #{pos}, file #{path})"
        end
      end)
      |> Enum.to_list()
    end)
  end

  def stream_for_actions(path, starting_pos \\ 0) do
    Stream.resource(
      fn ->
        file = File.open!(path, [:read, :raw, :read_ahead])
        {:ok, ^starting_pos} = :file.position(file, starting_pos)
        {file, starting_pos}
      end,
      fn {file, pos} ->
        with <<total_size::32, label::8, tx_offset::64, op_offset::64, op_type::8,
               log_file_entry_start_pos::64, json_size::64>> <-
               IO.binread(file, @key_index_full_size),
             <<key::binary-size(^total_size - @key_index_entry_size)>> <-
               IO.binread(file, total_size - @key_index_entry_size) do
          {[
             {{total_size - @key_index_entry_size, key}, label,
              LogOffset.new(tx_offset, op_offset), op_type, log_file_entry_start_pos, json_size}
           ], {file, pos + total_size + 4}}
        else
          :eof ->
            {:halt, {file, pos}}

          _ ->
            raise Storage.Error,
              kind: :data_loss,
              message:
                "Incomplete keyfile entry at #{pos} (started reading from #{starting_pos}, file #{path})"
        end
      end,
      fn {file, _} -> File.close(file) end
    )
  end

  @doc """
  This is a "side-write" function when log file is being written start-to-end.

  "Live" keyfile appends shouldn't be done using this function because we're not controlling
  flush points.
  """
  # label 0 is reserved for main log, which shouldn't be written through this function.
  def write_from_stream(stream, path, label) when label != 0 do
    Utils.stream_add_side_effect(
      stream,
      # We're using delayed writes to avoid interfering with writing the log. Write size here is 64KB or 1s delay
      # It's used here because we're writing a line per log line, so this reduces disk contention
      fn -> {File.open!(path, [:write, :raw, :delayed_write]), 0} end,
      fn line, {file, write_position} ->
        {data, _} = make_entry(line, write_position, label)

        IO.binwrite(file, data)

        {file, LogFile.expected_position(write_position, line)}
      end,
      # A delayed write reports its failure on a later write or on the close, so the close is the
      # last chance to notice that this index does not describe the log it was built from.
      &(:ok = File.close(elem(&1, 0)))
    )
  end

  def create_from_log(log_file_path, key_index_path, end_pos \\ :eof) do
    LogFile.stream_entries(log_file_path, 0, end_pos)
    |> Stream.map(fn {entry, pos} -> elem(make_entry(entry, pos), 0) end)
    |> Stream.into(File.stream!(key_index_path, [:delayed_write]))
    |> Stream.run()
  end

  # By key, then by offset: a compaction sorts the new part of the log with the previous compacted
  # generation, and sorted by key alone, a key in both came out in file order, the new part first.
  # The merge then took the older compacted entry for the key's newest, kept its offset and value,
  # and dropped every update to the key since the previous compaction.
  def sort(inputs, output) do
    :ok =
      :file_sorter.sort(inputs |> Enum.map(&to_charlist/1), to_charlist(output),
        format: fn <<_label::8, offset::binary-size(16),
                     _::binary-size(@key_index_entry_size - 17), key::binary>> ->
          {key, offset}
        end
      )
  end

  def copy_adjusting_positions(source, target, starting_pos, adjustment) when adjustment <= 0 do
    source
    |> stream_for_actions(starting_pos)
    |> Stream.map(fn {{key_size, key}, label, offset, op_type, start_pos, json_size} ->
      make_entry(
        {offset, key_size, key, op_type, 0, json_size, ""},
        start_pos + adjustment,
        label
      )
      |> elem(0)
    end)
    |> Stream.into(File.stream!(target, [:delayed_write]))
    |> Stream.run()
  end
end
