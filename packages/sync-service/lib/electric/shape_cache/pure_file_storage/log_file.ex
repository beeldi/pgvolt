defmodule Electric.ShapeCache.PureFileStorage.LogFile do
  @moduledoc false
  alias Electric.ShapeCache.PureFileStorage, as: PFS
  alias Electric.ShapeCache.PureFileStorage.ActionFile
  alias Electric.ShapeCache.PureFileStorage.KeyIndex
  alias Electric.ShapeCache.PureFileStorage.ActionFile
  alias Electric.ShapeCache.PureFileStorage.ChunkIndex
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.LogItems
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.LogChunker

  import Bitwise
  import Electric.Replication.LogOffset, only: :macros

  import Electric.ShapeCache.PureFileStorage,
    only: [discard_shape_after_data_loss!: 2, safely_open_file!: 3]

  # 16 bytes offset + 4 bytes key size + 1 byte op type + 1 byte processed flag + 8 bytes json size = 30 bytes
  @line_overhead 16 + 4 + 1 + 1 + 8

  # Flag bit marking an entry whose payload is a fixed-size ref into the shared payload
  # arena rather than the JSON itself. The frame stays self-describing (json_size is the
  # ref's byte size), so scanning, trimming and chunk-index arithmetic are unchanged.
  # Bit 0 remains the compaction-processed flag; ref entries never carry it because
  # compaction inlines refs before processing.
  @ref_flag 0b10
  def ref_flag, do: @ref_flag
  defguard is_ref_flag(flag) when (flag &&& @ref_flag) != 0

  @type operation_type() :: :insert | :update | :delete
  @type op_type() :: ?u | ?i | ?d
  # We're allowing tuple offsets to avoid struct creation in the hot path
  @type offset() ::
          {tx_offset :: non_neg_integer(), op_offset :: non_neg_integer()} | LogOffset.t()

  @typedoc "Log item that can be written to the log file"
  @type normal_log_item() ::
          {offset(), key :: String.t(), op_type :: operation_type(), json :: String.t()}
  @typedoc """
  Log item that can be read from the log file, but with precomputed
  `byte_size(key)` and `byte_size(json)` values, and with `op_type` as a byte
  """
  @type log_item_with_sizes() ::
          {offset(), key_size :: non_neg_integer(), key :: String.t(), op_type :: op_type(),
           processed_flag :: non_neg_integer(), json_size :: non_neg_integer(),
           json :: String.t()}
  @type log_item() :: normal_log_item() | log_item_with_sizes()

  @typedoc """
  Paths to the log file, chunk index, and key index files, used in conjuction
  """
  @type log_and_supporting() ::
          {log_file_path :: String.t(), chunk_index_path :: String.t(),
           key_index_path :: String.t()}

  @spec make_entry(log_item_with_sizes()) :: {iodata(), iodata_size :: non_neg_integer()}
  # Arena-ref entry: the payload written to the log is the ref binary; the JSON rides along
  # in the tuple only for the live-tail ETS mirror, and the real payload size only for chunk
  # accounting — neither reaches the file.
  def make_entry({offset, 0, "", op_type, flag, ref_size, {ref, _json, _payload_size}})
      when is_ref_flag(flag) do
    {[
       LogOffset.to_int128(offset),
       <<0::32, op_type::8, flag::8, ref_size::64>>,
       ref
     ], ref_size + @line_overhead}
  end

  def make_entry({offset, key_size, key, op_type, flag, json_size, json}) do
    {[
       LogOffset.to_int128(offset),
       <<key_size::32>>,
       key,
       <<op_type::8, flag::8, json_size::64>>,
       json
     ], key_size + json_size + @line_overhead}
  end

  @doc """
  Frame a run of prepared fan-out-class log items once for every member shape.

  The returned block's `frame` is byte-identical to what `make_entry/1` produces for each
  item in order, so a member appending the block writes exactly the bytes it would have
  written appending the items one at a time. `entries_meta` records, per entry, its log
  offset, its end position inside the frame, and the cumulative real payload size — enough
  for chunk-boundary arithmetic and replay slicing without re-parsing the frame.
  """
  @spec build_framed_block([log_item()]) :: Electric.Replication.Changes.FramedBlock.t()
  def build_framed_block([_ | _] = log_items) do
    {frame_iodata, meta_reversed, ets_reversed, frame_size, payload_size, count} =
      Enum.reduce(log_items, {[], [], [], 0, 0, 0}, fn item,
                                                       {frame, meta, ets, fsize, psize, count} ->
        {offset, json, payload_bytes, normalized} = normalize_block_item(item)
        {iodata, iodata_size} = make_entry(normalized)
        frame_end = fsize + iodata_size
        payload_cum = psize + payload_bytes

        {[frame | iodata], [{offset, frame_end, payload_cum} | meta],
         [{LogOffset.to_tuple(offset), json} | ets], frame_end, payload_cum, count + 1}
      end)

    [{last_offset, _, _} | _] = meta_reversed
    entries_meta = meta_reversed |> Enum.reverse() |> List.to_tuple()
    {first_offset, _, _} = elem(entries_meta, 0)

    %Electric.Replication.Changes.FramedBlock{
      frame: IO.iodata_to_binary(frame_iodata),
      frame_size: frame_size,
      count: count,
      payload_size: payload_size,
      ets_lines: Enum.reverse(ets_reversed),
      entries_meta: entries_meta,
      first_offset: first_offset,
      last_offset: last_offset
    }
  end

  defp normalize_block_item({offset, key, op_type, json}) when is_binary(json) do
    {offset, json, byte_size(json),
     {offset, byte_size(key), key, get_op_type(op_type), 0, byte_size(json), json}}
  end

  defp normalize_block_item({offset, _key, op_type, json, ref}) when is_binary(ref) do
    {offset, json, byte_size(json),
     {offset, 0, "", get_op_type(op_type), @ref_flag, byte_size(ref),
      {ref, json, byte_size(json)}}}
  end

  @doc """
  Write a log file based on the stream of log items.
  """
  @spec write_log_file(
          log_stream :: Enumerable.t(log_item()),
          log_file_path :: String.t(),
          chunk_size :: non_neg_integer()
        ) :: log_and_supporting()
  def write_log_file(
        log_stream,
        log_file_path,
        chunk_size \\ LogChunker.default_chunk_size_threshold()
      ) do
    log_stream
    |> normalize_log_stream()
    |> ChunkIndex.write_from_stream(log_file_path <> ".chunk_index", chunk_size)
    |> KeyIndex.write_from_stream(log_file_path <> ".key_index", 1)
    |> Stream.map(fn
      {log_offset, key_size, key, op_type, flag, json_size, json} ->
        # Add processed flag (0 for unprocessed) to header
        [
          <<offset(log_offset)::binary, key_size::32, key::binary, op_type::8, flag::8,
            json_size::64>>,
          json
        ]
    end)
    |> Stream.into(File.stream!(log_file_path))
    |> Stream.run()

    {log_file_path, log_file_path <> ".chunk_index", log_file_path <> ".key_index"}
  end

  defp read_line(file) do
    with <<tx_offset::64, op_offset::64, key_size::32>> <- IO.binread(file, 20),
         <<key::binary-size(^key_size)>> <- IO.binread(file, key_size),
         <<op_type::8, processed_flag::8, json_size::64>> <- IO.binread(file, 10),
         <<json::binary-size(^json_size)>> <- IO.binread(file, json_size) do
      {LogOffset.new(tx_offset, op_offset), key_size, key, op_type, processed_flag, json_size,
       json}
    end
  end

  @doc """
  Normalize the log stream to have precomputed key and json sizes.
  """
  @spec normalize_log_stream(Enumerable.t(log_item())) :: Enumerable.t(log_item_with_sizes())
  def normalize_log_stream(stream) do
    Stream.map(stream, fn
      {log_offset, key, op_type, json} ->
        {log_offset, byte_size(key), key, get_op_type(op_type), 0, byte_size(json), json}

      {_, _, _, _, _, _, _} = formed_line ->
        formed_line
    end)
  end

  @doc """
  Get the expected byte position in the file after the given log item is written.

  Used by other modules that know the log file structure.
  """
  @spec expected_position(non_neg_integer(), log_item_with_sizes()) :: non_neg_integer()
  def expected_position(
        current_position,
        {_log_offset, key_size, _key, _op_type, _processed_flag, json_size, _json}
      ) do
    current_position + key_size + json_size + @line_overhead
  end

  def expected_position(current_position, key_size, json_size) do
    current_position + key_size + json_size + @line_overhead
  end

  @doc """
  Get the expected byte position of the JSON for the given log item after it's written.

  Used by other modules that know the log file structure.
  """
  @spec expected_json_position(non_neg_integer(), log_item_with_sizes() | non_neg_integer()) ::
          non_neg_integer()
  def expected_json_position(current_position, {_, key_size, _, _, _, _, _}) do
    current_position + key_size + @line_overhead
  end

  def expected_json_position(current_position, key_size) when is_integer(key_size) do
    current_position + key_size + @line_overhead
  end

  # compaction only
  @spec stream_entries(
          log_file_path :: String.t(),
          start_position :: non_neg_integer(),
          excl_end_pos :: non_neg_integer() | :eof
        ) :: Enumerable.t({log_item_with_sizes(), non_neg_integer()})
  def stream_entries(log_file_path, start_position, end_position \\ :eof) do
    Stream.resource(
      fn ->
        file = File.open!(log_file_path, [:read, :raw, :read_ahead])
        :file.position(file, start_position)
        {file, start_position}
      end,
      fn
        {file, position} when position >= end_position ->
          {:halt, {file, position}}

        {file, position} ->
          with <<tx_offset::64, op_offset::64, key_size::32>> <- IO.binread(file, 20),
               <<key::binary-size(^key_size)>> <- IO.binread(file, key_size),
               <<op_type::8, flag::8, json_size::64>> <- IO.binread(file, 10),
               <<json::binary-size(^json_size)>> <- IO.binread(file, json_size) do
            entry =
              {LogOffset.new(tx_offset, op_offset), key_size, key, op_type, flag, json_size, json}

            {[{entry, position}], {file, expected_position(position, entry)}}
          else
            # An I/O error is not the end of the stream. Halting on one hands back a short log that
            # reads as complete, and both callers build something durable out of it — a key index, or
            # the inlined copy compaction swaps in — so the truncation outlives the error that caused
            # it. `read_back_jsons` below already separates the two; this reader did not. A torn tail
            # still halts, because a partial write comes back as a short binary rather than an error.
            {:error, reason} -> raise File.Error, path: log_file_path, reason: reason
            _ -> {:halt, {file, position}}
          end
      end,
      &File.close(elem(&1, 0))
    )
  end

  @doc """
  Write a copy of the log segment `[0, end_pos)` with every arena-ref entry materialized
  into a self-contained entry (key and JSON restored from the arena, ref flag cleared).

  Compaction's key index, action file and merge internals all read payloads back from the
  log file by position, which a fixed-size ref cannot satisfy; running them on an inlined
  copy keeps those internals untouched and naturally ages shared payloads out of the log.

  Returns the size of the written copy.
  """
  @spec write_inlined_copy!(String.t(), String.t(), non_neg_integer(), String.t()) ::
          non_neg_integer()
  def write_inlined_copy!(src_path, dst_path, end_pos, base_path) do
    # On a raise mid-copy the caller (a compaction task) dies and the raw handles die with
    # the process, so only the success path needs an explicit close.
    File.open!(dst_path, [:write, :raw, :binary], fn dst ->
      stream_entries(src_path, 0, end_pos)
      |> Enum.reduce(PayloadArena.open_reader(base_path), fn {entry, _pos}, reader ->
        {reader, entry} = inline_entry(reader, entry)
        {iodata, _size} = make_entry(entry)
        IO.binwrite(dst, iodata)
        reader
      end)
    end)
    |> PayloadArena.close_reader!()

    File.stat!(dst_path).size
  end

  defp inline_entry(reader, {offset, _key_size, _key, op_type, flag, _json_size, ref})
       when is_ref_flag(flag) do
    {reader, {key, json}} = PayloadArena.read_payload!(reader, ref)
    {reader, {offset, byte_size(key), key, op_type, 0, byte_size(json), json}}
  end

  defp inline_entry(reader, entry), do: {reader, entry}

  @doc """
  The set of payload-arena generations reachable from the ref entries in one log file.

  A generation the arena still holds is dead once no surviving log resolves a ref into it, so the
  reclaimer needs the reachable set to know which files it may drop. Only uncompacted log entries
  carry refs -- compaction inlines them (`write_inlined_copy!` clears the flag) -- so a compacted
  file yields the empty set at the cost of a scan.
  """
  @spec arena_generations_in_log(String.t()) :: MapSet.t(non_neg_integer())
  def arena_generations_in_log(log_file_path) do
    stream_entries(log_file_path, 0, :eof)
    |> Enum.reduce(MapSet.new(), fn
      {{_offset, _key_size, _key, _op_type, flag, _json_size, ref}, _pos}, gens
      when is_ref_flag(flag) ->
        MapSet.put(gens, PayloadArena.ref_generation(ref))

      {_entry, _pos}, gens ->
        gens
    end)
  end

  # Read the log range lazily in bounded blocks rather than all at once.
  #
  # Every emitted json is a sub-binary of the block it was read from, and the
  # response-streaming pipeline can hold an in-flight element (and everything
  # it references) for the whole lifetime of a serve — including one stalled
  # on a slow client. Bounding the read block bounds the memory each stalled
  # connection can pin.
  @read_block_bytes 64 * 1024

  def stream_jsons(
        %PFS{} = opts,
        log_file_path,
        start_position,
        end_position,
        exclusive_min_offset
      ) do
    Stream.resource(
      fn ->
        case safely_open_file!(opts, log_file_path, [:read, :raw]) do
          {:ok, file} -> {file, start_position, ""}
          {:halt, :data_removed} -> :halt
        end
      end,
      fn
        :halt ->
          {:halt, []}

        {file, position, binary_rest} when position >= end_position ->
          if binary_rest == "" do
            {:halt, {file, position, binary_rest}}
          else
            # A partial entry at the end of the range means the file is
            # shorter than the chunk index says it should be.
            discard_shape_after_data_loss!(
              opts,
              "Log #{log_file_path} ends mid-entry at #{end_position}, short of what the " <>
                "chunk index describes; the storage has lost durable data"
            )
          end

        {file, position, binary_rest} ->
          read_size = min(@read_block_bytes, end_position - position)

          case :file.pread(file, position, read_size) do
            {:ok, data} ->
              {jsons, rest} =
                extract_jsons_from_binary(binary_rest <> data, exclusive_min_offset, nil)

              {jsons, {file, position + byte_size(data), rest}}

            :eof ->
              discard_shape_after_data_loss!(
                opts,
                "Log #{log_file_path} ends at #{position}, short of the #{end_position} the " <>
                  "chunk index describes; the storage has lost durable data"
              )

            {:error, reason} ->
              raise File.Error,
                path: log_file_path,
                reason: reason,
                action: "pread(#{position}, #{read_size})"
          end
      end,
      fn
        [] -> :ok
        {file, _position, _rest} -> File.close(file)
      end
    )
  end

  @tail_read_block_bytes 4096

  def stream_jsons_until_offset(
        %PFS{} = opts,
        log_file_path,
        start_position,
        exclusive_min_offset,
        inclusive_max_offset
      ) do
    Stream.resource(
      fn ->
        case safely_open_file!(opts, log_file_path, [:read, :raw]) do
          {:ok, file} ->
            {:ok, ^start_position} = :file.position(file, start_position)
            {file, ""}

          {:halt, :data_removed} ->
            :halt
        end
      end,
      fn
        :halt ->
          {:halt, []}

        {file, :done} ->
          {:halt, {file, ""}}

        {file, binary_rest} ->
          case :file.read(file, next_read_size(binary_rest)) do
            {:ok, data} ->
              case extract_jsons_from_binary(
                     binary_rest <> data,
                     exclusive_min_offset,
                     inclusive_max_offset
                   ) do
                {jsons, :halt} -> {jsons, {file, :done}}
                {jsons, rest} -> {jsons, {file, rest}}
              end

            :eof ->
              {:halt, {file, binary_rest}}
          end
      end,
      fn
        [] -> :ok
        {file, _} -> File.close(file)
      end
    )
  end

  defp next_read_size(<<_::128, key_size::32, tail::binary>> = pending)
       when byte_size(tail) >= key_size + 10 do
    <<_key::binary-size(^key_size), _::8, _::8, json_size::64, _::binary>> = tail
    max(@tail_read_block_bytes, @line_overhead + key_size + json_size - byte_size(pending))
  end

  defp next_read_size(_pending), do: @tail_read_block_bytes

  @spec extract_jsons_from_binary(binary(), LogOffset.t(), LogOffset.t() | nil) ::
          Enumerable.t(String.t())
  defp extract_jsons_from_binary(binary, exclusive_min_offset, inclusive_max_offset, acc \\ [])
  defp extract_jsons_from_binary(<<>>, _, _, acc), do: {Enum.reverse(acc), ""}

  defp extract_jsons_from_binary(
         <<tx_offset1::64, op_offset1::64, key_size::32, _::binary-size(key_size), _::8, _flag::8,
           json_size::64, _::binary-size(json_size), rest::binary>>,
         %LogOffset{
           tx_offset: tx_offset2,
           op_offset: op_offset2
         } = log_offset,
         inclusive_max_offset,
         acc
       )
       when tx_offset1 < tx_offset2 or (tx_offset1 == tx_offset2 and op_offset1 <= op_offset2),
       do: extract_jsons_from_binary(rest, log_offset, inclusive_max_offset, acc)

  defp extract_jsons_from_binary(
         <<tx_offset1::64, op_offset1::64, key_size::32, _::binary-size(key_size), _::8, flag::8,
           json_size::64, json::binary-size(json_size), _::binary>>,
         _log_offset,
         %LogOffset{tx_offset: tx_offset2, op_offset: op_offset2},
         acc
       )
       when tx_offset1 == tx_offset2 and op_offset1 == op_offset2,
       do: {Enum.reverse([tag_payload(flag, json) | acc]), :halt}

  defp extract_jsons_from_binary(
         <<tx_offset1::64, op_offset1::64, key_size::32, _::binary-size(key_size), _::8, _flag::8,
           json_size::64, _json::binary-size(json_size), _::binary>>,
         _log_offset,
         %LogOffset{tx_offset: tx_offset2, op_offset: op_offset2},
         acc
       )
       when tx_offset1 > tx_offset2 or (tx_offset1 == tx_offset2 and op_offset1 > op_offset2),
       do: {Enum.reverse(acc), :halt}

  defp extract_jsons_from_binary(
         <<_::128, key_size::32, _::binary-size(key_size), _::8, flag::8, json_size::64,
           json::binary-size(json_size), rest::binary>>,
         log_offset,
         inclusive_max_offset,
         acc
       ),
       do:
         extract_jsons_from_binary(rest, log_offset, inclusive_max_offset, [
           tag_payload(flag, json) | acc
         ])

  defp extract_jsons_from_binary(rest, _, _, acc),
    do: {Enum.reverse(acc), rest}

  # Arena-ref entries surface as `{:ref, ref}` for `PayloadArena.resolve_refs/2` to turn back
  # into the payload JSON; inline entries pass through as the stored binary.
  defp tag_payload(flag, payload) when is_ref_flag(flag), do: {:ref, payload}
  defp tag_payload(_flag, json), do: json

  defp get_op_type(:insert), do: ?i
  defp get_op_type(:update), do: ?u
  defp get_op_type(:delete), do: ?d

  @doc "Serialize a non-infinite non-negative offset to a 16-byte binary"
  @spec offset(offset()) :: binary
  def offset(%LogOffset{tx_offset: tx_offset, op_offset: op_offset}),
    do: <<tx_offset::64, op_offset::64>>

  def offset({tx_offset, op_offset}), do: <<tx_offset::64, op_offset::64>>

  defp process_json(json) do
    json
    |> Jason.decode!()
    |> LogItems.keep_generic_headers()
    |> Jason.encode!()
  end

  @doc """
  Truncate the log file at the given persisted offset, starting the search from the given position.

  Returns `:ok` when the log holds the entry at the offset, `:past_boundary` when it holds none
  there but one past it, and `:short_of_boundary` when it ends before reaching the offset.

  A read that fails raises, and trims nothing. With `read_errors: :return` it returns
  `:unreadable` instead, for a caller probing a boundary it can fall back from.
  """
  def trim(log_file_path, search_start_pos, last_persisted_offset, opts \\ []) do
    File.open!(log_file_path, [:raw, :read, :write, :read_ahead], fn file ->
      # A seek that fails leaves the scan at the start of the file while it counts positions from
      # the search start, and the trim then cuts the log where no entry ends.
      case :file.position(file, search_start_pos) do
        {:ok, _} -> :ok
        {:error, reason} -> raise File.Error, reason: reason, action: "seek", path: log_file_path
      end

      Stream.unfold(search_start_pos, fn position ->
        case read_entry(file) do
          # An entry cut short is a tail a crash left, for the trim to remove. A read that fails
          # says nothing about the bytes, and trimming there dropped durable entries for it.
          {:error, reason} ->
            {{:read_error, reason}, position}

          :partial ->
            {{nil, position}, position}

          {offset, read} ->
            {{offset, position + read, read}, position + read}
        end
      end)
      |> Enum.find_value(fn
        {:read_error, _reason} = failed -> failed
        # The scan ran out of readable entries without ever reaching the persisted offset, so
        # the file stops short of what the metadata claims is durable. Only the caller knows
        # whether that is possible for this store, so report it rather than trimming quietly.
        {nil, position} -> {:short_of_boundary, position}
        {offset, _, _} when is_log_offset_lt(offset, last_persisted_offset) -> false
        # On the persisted offset, trim what's after it
        {^last_persisted_offset, position, _} -> {:ok, position}
        # On an offset larger than the persisted offset, trim the line itself and the rest of the file
        {_, position, width} -> {:past_boundary, position - width}
      end)
      |> case do
        {:read_error, reason} ->
          if Keyword.get(opts, :read_errors) == :return,
            do: :unreadable,
            else: raise(File.Error, reason: reason, action: "read", path: log_file_path)

        {status, position} ->
          # A truncate that fails leaves the bytes past the position, and recovery judges what the
          # chunk index covers by where the log ends: reported as done, it read as lost data.
          case truncate_at(file, position) do
            :ok ->
              status

            {:error, reason} ->
              raise File.Error, reason: reason, action: "truncate", path: log_file_path
          end
      end
    end)
  end

  # A log that already ends at the position needs no truncate, and one that failed there failed
  # the reopen of an intact log. A log that ends before it is one the scan found short: a truncate
  # past the end grew it with zeros, which a later scan reads as entries.
  defp truncate_at(file, position) do
    case :file.position(file, :eof) do
      {:ok, eof} when eof <= position -> :ok
      {:ok, _} -> truncate_file(file, position)
      error -> error
    end
  end

  defp truncate_file(file, position) do
    with {:ok, _} <- :file.position(file, position), do: :file.truncate(file)
  end

  defp read_entry(file) do
    with <<tx_offset::64, op_offset::64, key_size::32>> <- IO.binread(file, 20),
         <<_::binary-size(^key_size)>> <- IO.binread(file, key_size),
         <<_::8, _::8, json_size::64>> <- IO.binread(file, 10),
         <<_::binary-size(^json_size)>> <- IO.binread(file, json_size) do
      {LogOffset.new(tx_offset, op_offset), 20 + key_size + json_size + 10}
    else
      {:error, _reason} = error -> error
      _ -> :partial
    end
  end

  alias Electric.LogItems

  def merge_with_actions(
        action_file_path,
        paths_with_labels,
        resulting_file,
        chunk_size,
        merge_fun \\ &LogItems.merge_updates/2
      ) do
    ActionFile.stream(action_file_path)
    |> Stream.transform(
      fn -> open_labeled_logs!(paths_with_labels) end,
      # Actions are in log-offset order, so we can apply them as we go along
      fn {_offset, label, _entry_start, action}, files ->
        file = Map.fetch!(files, label)

        case action do
          :skip ->
            skip_line(file)
            {[], files}

          :keep ->
            {[file |> read_line() |> process_line()], files}

          {:compact, offsets} ->
            {[compact_lines(file, files, offsets, merge_fun)], files}
        end
      end,
      &Enum.each(&1, fn {_, file} -> File.close(file) end)
    )
    |> write_log_file(resulting_file, chunk_size)
  end

  @doc false
  # Open every labeled input up front, as the merge's `start_fun`. Opening them in one
  # comprehension leaked the handles opened before a failing one: the raise escaped `start_fun`, so
  # the accumulator was never handed back and the transform's `after_fun` never ran to close them.
  # Closing what we already hold before re-raising keeps a mid-way `:enoent` from stranding a
  # descriptor per surviving input until the next GC.
  @type log_opener() :: (String.t() -> {:ok, File.io_device()} | {:error, File.posix()})
  @spec open_labeled_logs!([{term(), String.t()}], log_opener()) :: %{term() => File.io_device()}
  def open_labeled_logs!(
        paths_with_labels,
        open_fun \\ &File.open(&1, [:read, :raw, :read_ahead])
      ) do
    Enum.reduce(paths_with_labels, %{}, fn {label, path}, opened ->
      case open_fun.(path) do
        {:ok, file} ->
          Map.put(opened, label, file)

        {:error, reason} ->
          Enum.each(opened, fn {_, file} -> File.close(file) end)
          raise File.Error, path: path, reason: reason, action: "open"
      end
    end)
  end

  defp skip_line(file) do
    _ = read_line(file)
    :ok
  end

  defp process_line({_, _, _, _, 1, _, _} = line), do: line

  defp process_line({offset, key_size, key, op_type, 0, _, json}) do
    new_json = process_json(json)
    {offset, key_size, key, op_type, 1, byte_size(new_json), new_json}
  end

  defp compact_lines(original_file, files, offsets, merge_fun) do
    # The line to be replaced with compaction will keep it's offset & key
    {offset, key_size, key, op_type, _, _, _} = read_line(original_file)

    # Save position
    saved_positions =
      Map.new(files, fn {label, file} ->
        {:ok, position} = :file.position(file, :cur)
        {label, position}
      end)

    merged_json =
      offsets
      # Group reads to be efficient, but try to limit loading the JSONs to 10MB at a time.
      # In the worst case when JSONs exceed 10MB, we'll just read one at a time.
      |> chunk_expected_reads(bytes: 1024 * 1024 * 10)
      |> Stream.flat_map(fn [{label, _, _} | _] = offsets ->
        file = Map.fetch!(files, label)
        read_offsets = Enum.map(offsets, fn {_, pos, length} -> {pos, length} end)

        case :file.pread(file, read_offsets) do
          {:ok, results} -> results
          {:error, reason} -> raise File.Error, path: original_file, reason: reason
          :eof -> raise "unexpected end of file while reading back jsons from the log"
        end
      end)
      |> Stream.map(&Jason.decode!/1)
      |> Enum.reduce(fn new, acc -> merge_fun.(acc, new) end)
      |> Jason.encode!()

    # Restore position to continue reading in the outer loop
    Enum.each(saved_positions, fn {label, position} ->
      :file.position(Map.fetch!(files, label), {:bof, position})
    end)

    {offset, key_size, key, op_type, 1, byte_size(merged_json), merged_json}
  end

  @spec chunk_expected_reads(
          Enumerable.t({position :: non_neg_integer(), size :: non_neg_integer()}),
          bytes: non_neg_integer()
        ) :: Enumerable.t(list({position :: non_neg_integer(), size :: non_neg_integer()}))
  defp chunk_expected_reads(stream, bytes: chunk_size) do
    Stream.chunk_while(
      stream,
      {nil, 0, []},
      fn
        {label, _, size} = item, {old_label, total_size, [_ | _] = acc}
        when total_size > chunk_size or old_label != label ->
          {:cont, Enum.reverse(acc), {label, size, [item]}}

        {label, _, size} = item, {_, total_size, acc} ->
          {:cont, {label, total_size + size, [item | acc]}}
      end,
      fn
        {_, _, []} -> {:cont, []}
        {_, _, acc} -> {:cont, Enum.reverse(acc), []}
      end
    )
  end
end
