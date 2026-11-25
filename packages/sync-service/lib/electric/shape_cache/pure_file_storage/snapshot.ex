defmodule Electric.ShapeCache.PureFileStorage.Snapshot do
  @moduledoc false

  alias Electric.ShapeCache.PureFileStorage, as: ST
  alias Electric.ShapeCache.Storage

  import Electric.ShapeCache.PureFileStorage,
    only: [discard_shape_after_data_loss!: 2, safely_open_file!: 4, snapshot_chunk_final?: 2]

  @write_buffer_size 64 * 1024

  @doc """
  Write the stream of pre-formatted JSON lines of the initial query into the snapshot files.

  Snapshot storage is slightly different from main file storage, because
  our system requires one additional property: reads concurrent with writes.
  Because snapshots are a common operation, the fastest way to serve them to a client
  is to stream them out as they are being written.

  To fascilitate this property without overcomplicating the system, the snapshot chunks
  are stored as separate files as they are being written, with a `0x04` byte (end-of-transmission)
  appended to the end. Apart from that, the file is json-chunk formatted (jsonl with trailing commas)
  to allow for socket copies later on.

  The clients of the storage expect the snapshot chunks to be read in their entirety, which gives us freedom
  to do batched writes for performance, because reads are always going to be faster than we're writing.

  `on_written` gets the last chunk's number once that chunk's end-of-transmission marker is
  written, before the chunk is synced and closed. A chunk ended by a boundary is synced and closed
  once the stream has a next item or has ended, so that one the stream ended on is synced after
  `on_written` too.
  """
  def write_snapshot_stream!(
        stream,
        %ST{} = opts,
        on_written \\ fn _ -> :ok end,
        write_buffer \\ @write_buffer_size
      ) do
    stream
    |> Stream.transform(
      fn -> {0, nil, {[], 0}, nil} end,
      fn line, {chunk_num, file, {buffer, buffer_size}, ended} ->
        close_ended_chunk!(ended, opts, chunk_num)
        file = file || open_snapshot_chunk_to_write(opts, chunk_num)

        case line do
          :chunk_boundary ->
            IO.binwrite(file, [buffer, <<4::utf8>>])
            {[], {chunk_num + 1, nil, {[], 0}, file}}

          line ->
            line_size = IO.iodata_length(line)

            if buffer_size + line_size > write_buffer do
              IO.binwrite(file, [buffer, line, ",\n"])
              {[chunk_num], {chunk_num, file, {[], 0}, nil}}
            else
              {[chunk_num],
               {chunk_num, file, {[buffer, line, ",\n"], buffer_size + line_size + 2}, nil}}
            end
        end
      end,
      fn {chunk_num, file, {buffer, _}, ended} ->
        cond do
          not is_nil(file) ->
            IO.binwrite(file, [buffer, <<4::utf8>>])
            on_written.(chunk_num)
            close_chunk!(file, opts, chunk_num)
            {[chunk_num], {chunk_num, nil, {[], 0}, nil}}

          is_nil(ended) and chunk_num == 0 ->
            # Special case if the source stream has ended before we started writing any chunks - we need to create the empty file for the first chunk.
            file = open_snapshot_chunk_to_write(opts, chunk_num)
            IO.binwrite(file, [buffer, <<4::utf8>>])
            on_written.(chunk_num)
            close_chunk!(file, opts, chunk_num)
            {[chunk_num], {chunk_num, nil, {[], 0}, nil}}

          true ->
            on_written.(chunk_num - 1)
            close_ended_chunk!(ended, opts, chunk_num)
            {[chunk_num - 1], {chunk_num, nil, {[], 0}, nil}}
        end
      end,
      fn {_chunk_num, file, _, ended} ->
        if file, do: File.close(file)
        if ended, do: File.close(ended)
      end
    )
    |> Enum.reduce(0, fn chunk_num, _ -> chunk_num end)
  end

  # A snapshot's end is recorded durably once its chunks are written (`ST.make_new_snapshot!/2`),
  # so each chunk is synced through the descriptor that wrote it, and a failed close fails the
  # snapshot: synced later through another descriptor, a write error reported at this close was
  # lost, and the record named a chunk the device had not kept.
  defp close_chunk!(file, opts, chunk_num),
    do: Electric.Utils.datasync_and_close!(file, chunk_file_path(opts, chunk_num))

  # The chunk before `chunk_num`, ended by a boundary and still open.
  defp close_ended_chunk!(nil, _opts, _chunk_num), do: :ok
  defp close_ended_chunk!(file, opts, chunk_num), do: close_chunk!(file, opts, chunk_num - 1)

  def chunk_file_path(%ST{} = opts, chunk_num),
    do: ST.shape_log_path(opts, "#{chunk_num}.jsonsnapshot")

  defp open_snapshot_chunk_to_write(opts, chunk_num) do
    File.open!(chunk_file_path(opts, chunk_num), [:write, :exclusive, :raw])
  end

  @doc """
  Stream JSON lines of a given chunk.

  Streams out the lines without trailing commas or line breaks. If chunk is in the process
  of being written, then follows it as it's being written emitting lines as they appear.

  If chunk file still doesn't exist, will wait for up to 5 seconds for the file to appear.
  """
  def stream_chunk_lines(%ST{} = opts, chunk_num) do
    path = chunk_file_path(opts, chunk_num)

    Stream.resource(
      fn ->
        case safely_open_file!(
               opts,
               path,
               [:raw, :read, :read_ahead],
               &wait_and_open(&1, &2, opts.snapshot_file_timeout)
             ) do
          {:ok, file} ->
            {file, nil, "", false}

          {:halt, :data_removed} ->
            :halt
        end
      end,
      fn
        :halt ->
          {:halt, []}

        {file, eof_seen, incomplete_line, final_before_read} ->
          case IO.binread(file, :line) do
            {:error, reason} ->
              raise Storage.Error,
                message: "failed to read #{inspect(path)}: #{inspect(reason)}"

            :eof ->
              cond do
                # The wait below exists for a reader that has caught up with a writer still
                # producing this file. Once the snapshot is marked complete every chunk file at
                # or below its last chunk has been written in full and closed, so a missing
                # end-of-transmission marker there is a file that lost its tail rather than one
                # still arriving -- and waiting 90s for bytes that cannot come turns a corrupt
                # snapshot into a 90s stall per client, on every retry, for as long as the shape
                # exists. Only a read that started once the chunk was known final can tell: the
                # writer may finish the file between this read's eof and the check below.
                final_before_read ->
                  discard_shape_after_data_loss!(
                    opts,
                    "Snapshot chunk #{inspect(path)} is complete but ends without its " <>
                      "end-of-transmission marker; the storage has lost durable data"
                  )

                snapshot_chunk_final?(opts, chunk_num) ->
                  {[], {file, eof_seen, incomplete_line, true}}

                is_nil(eof_seen) ->
                  # First time we see eof after any valid lines, we store a timestamp
                  {[], {file, System.monotonic_time(:millisecond), incomplete_line, false}}

                # If it's been 90s without any new lines, and also we've not seen <<4>>,
                # then likely something is wrong
                System.monotonic_time(:millisecond) - eof_seen > 90_000 ->
                  raise Storage.Error, message: "Snapshot hasn't updated in 90s"

                true ->
                  # Sleep a little and check for new lines
                  Process.sleep(20)
                  {[], {file, eof_seen, incomplete_line, false}}
              end

            # The 4 byte marker (ASCII "end of transmission") indicates the end of the snapshot file.
            <<4::utf8>> ->
              {:halt, {file, nil, "", final_before_read}}

            line ->
              cond do
                :binary.last(line) != ?\n ->
                  # Not a full line
                  {[], {file, nil, incomplete_line <> line, final_before_read}}

                line == "\n" ->
                  {[binary_slice(incomplete_line, 0..-2//1)], {file, nil, "", final_before_read}}

                true ->
                  {[incomplete_line <> binary_slice(line, 0..-3//1)],
                   {file, nil, "", final_before_read}}
              end
          end
      end,
      fn
        [] -> :ok
        {file, _, _, _} -> File.close(file)
      end
    )
  end

  defp wait_and_open(_path, _, time_left) when time_left <= 0 do
    {:error, :enoent}
  end

  defp wait_and_open(path, modes, time_left) do
    start = System.monotonic_time(:millisecond)

    case File.open(path, modes) do
      {:ok, file} ->
        {:ok, file}

      {:error, :enoent} ->
        Process.sleep(20)
        wait_and_open(path, modes, time_left - (System.monotonic_time(:millisecond) - start))

      {:error, _reason} = error ->
        error
    end
  end
end
