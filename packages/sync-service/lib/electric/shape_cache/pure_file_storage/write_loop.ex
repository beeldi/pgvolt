defmodule Electric.ShapeCache.PureFileStorage.WriteLoop do
  @moduledoc false
  # This module encapsulates the write loop of the PureFileStorage, and is responsible for
  # appending log lines to the log and flushing the buffer to disk.
  # There are many steps to this, so it's split out into a separate module.

  alias Electric.Replication.Changes.FramedBlock
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.ChunkIndex
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.Replication.LogOffset
  import Electric.Replication.LogOffset
  import Record
  import Electric.ShapeCache.PureFileStorage.SharedRecords

  defrecord :open_files,
    json_file: nil,
    chunk_file: nil

  # Record that controls the writer's progress & flush logic
  #
  # `last_persisted_offset` is the WRITTEN boundary: bytes the OS has in the log file, which is
  # what reads and ETS trimming require. `last_synced_offset` is the DURABLE boundary: bytes
  # datasync has confirmed, which is what the flushed notification (and through it the
  # replication slot ack) and the persisted metadata file require. Syncing every 64 KB flush
  # conflated the two and put ~15 datasyncs per shape per transaction inside the consumer's
  # reply to the collector; syncing at transaction boundaries only was measured at 1.8x
  # end-to-end throughput on the fan-out harness.
  defrecord :writer_acc,
    ets_line_buffer: [],
    buffer: [],
    buffer_size: 0,
    last_seen_offset: LogOffset.last_before_real_offsets(),
    last_seen_txn_offset: LogOffset.last_before_real_offsets(),
    last_persisted_offset: LogOffset.last_before_real_offsets(),
    last_persisted_txn_offset: LogOffset.last_before_real_offsets(),
    last_synced_offset: LogOffset.last_before_real_offsets(),
    last_synced_txn_offset: LogOffset.last_before_real_offsets(),
    unsynced_bytes: 0,
    write_position: 0,
    bytes_in_chunk: 0,
    times_flushed: 0,
    chunk_started?: false,
    cached_chunk_boundaries: {LogOffset.last_before_real_offsets(), []},
    open_files: {:open_files, nil, nil},
    last_deduped_offset: nil

  defguardp is_chunk_file_open(acc)
            when elem(acc, 0) == :writer_acc and
                   elem(elem(acc, writer_acc(:open_files)), open_files(:chunk_file)) != nil

  def last_persisted_txn_offset(writer_acc(last_persisted_txn_offset: res)), do: res
  def last_seen_offset(writer_acc(last_seen_offset: res)), do: res
  def cached_chunk_boundaries(writer_acc(cached_chunk_boundaries: res)), do: res
  def has_flushed_since?(writer_acc(times_flushed: res), previous_ref), do: res != previous_ref
  def times_flushed(writer_acc(times_flushed: res)), do: res

  # A compaction moves the log's tail to the start of a new file, so the positions the writer holds
  # move with it and nothing else does. This built a fresh record instead, which dropped the open
  # chunk and the offsets: the next write opened a chunk over the one still open, readers lost the
  # rows the compaction kept, and a restart could trim rows the shape had acknowledged.
  def adjust_write_positions(
        writer_acc(write_position: write_position, cached_chunk_boundaries: cached) = acc,
        log_file_pos
      ) do
    writer_acc(acc,
      write_position: write_position + log_file_pos,
      cached_chunk_boundaries: rebase_cached_chunks(cached, log_file_pos)
    )
  end

  # Chunks that end at or before the cut are in the compacted generation now: they leave the
  # cache, and the last of them becomes its floor.
  defp rebase_cached_chunks({floor, chunks}, delta) do
    {compacted, kept} =
      Enum.split_while(chunks, fn {_, {_, end_pos}} ->
        not is_nil(end_pos) and end_pos + delta <= 0
      end)

    floor =
      case List.last(compacted) do
        nil -> floor
        {{_, max}, _} -> max
      end

    {floor,
     Enum.map(kept, fn {offsets, {start_pos, end_pos}} ->
       {offsets, {start_pos + delta, end_pos && end_pos + delta}}
     end)}
  end

  @doc """
  Initialize the writer from disk. At the point of recovery, all offsets are assumed to be the same - last txn boundary.
  """
  def init_from_disk(opts) do
    last_persisted_txn_offset = Keyword.fetch!(opts, :last_persisted_txn_offset)
    write_position = Keyword.fetch!(opts, :write_position)
    bytes_in_chunk = Keyword.fetch!(opts, :bytes_in_chunk)
    chunk_started? = Keyword.fetch!(opts, :chunk_started?)
    chunks = Keyword.fetch!(opts, :chunks)

    writer_acc(
      last_persisted_offset: last_persisted_txn_offset,
      last_persisted_txn_offset: last_persisted_txn_offset,
      last_synced_offset: last_persisted_txn_offset,
      last_synced_txn_offset: last_persisted_txn_offset,
      last_seen_offset: last_persisted_txn_offset,
      last_seen_txn_offset: last_persisted_txn_offset,
      write_position: write_position,
      bytes_in_chunk: bytes_in_chunk,
      chunk_started?: chunk_started?,
      cached_chunk_boundaries: chunks
    )
  end

  @doc """
  Flush the buffer (if any) and close all files
  """
  def flush_and_close_all(writer_acc(open_files: open_files) = acc, state) do
    acc = flush_buffer(acc, state)

    Tuple.to_list(open_files)
    |> Enum.reject(&is_nil/1)
    |> Enum.each(&File.close/1)

    writer_acc(acc, open_files: open_files())
  end

  @doc """
  Append log lines from a transaction fragment.

  Unlike `append_to_log!/3`, this does NOT advance `last_seen_txn_offset` or
  call `register_complete_txn`. Transaction completion should be signaled
  separately via `register_complete_txn/2` after the commit is received.

  This ensures that on crash/recovery, `fetch_latest_offset` returns the
  last committed transaction offset, not a mid-transaction offset.
  """
  def append_fragment_to_log!(txn_lines, writer_acc(times_flushed: times_flushed) = acc, state) do
    acc = ensure_json_file_open(acc, state)

    txn_lines
    |> Enum.reduce(acc, fn
      %FramedBlock{last_offset: last}, writer_acc(last_seen_txn_offset: min_offset) = acc
      when is_log_offset_lte(last, min_offset) ->
        # Whole block already persisted (replay), no-op
        writer_acc(acc, last_deduped_offset: last)

      %FramedBlock{} = block, acc ->
        append_block_to_buffer(block, acc, state)

      {offset, _, _, _, _, _, _}, writer_acc(last_seen_txn_offset: min_offset) = acc
      when is_log_offset_lte(offset, min_offset) ->
        # Line already persisted, no-op
        writer_acc(acc, last_deduped_offset: offset)

      {offset, _, _, _, _, _, _} = line, acc ->
        acc
        |> maybe_write_opening_chunk_boundary(state, offset)
        |> add_to_buffer(line)
        |> maybe_write_closing_chunk_boundary(state)
        |> maybe_flush_buffer(state)
    end)
    |> close_chunk_file()
    |> confirm_deduplicated_lines_durable()
    |> case do
      # Only when nothing is buffered AND nothing written is awaiting datasync can the flush
      # timer be dropped — the timer is now also what drives the durable boundary forward.
      writer_acc(buffer_size: 0, last_synced_offset: same, last_persisted_offset: same) = acc ->
        {acc, cancel_flush_timer: true}

      acc ->
        {acc, schedule_flush: times_flushed}
    end
  end

  @doc """
  Append a stream of log lines to the log.
  """
  def append_to_log!(txn_lines, acc, state) do
    {acc, opts} = append_fragment_to_log!(txn_lines, acc, state)
    {finalize_txn(acc, state), opts}
  end

  # A fragment whose every line deduplicates writes no bytes, so nothing is buffered, no flush is
  # scheduled and no flushed notification is ever sent. That is exactly what boot replay does when
  # it re-delivers commits the previous run already persisted — and the collector's flush tracker
  # only ever clears a shape's entry on a notification, so the shape stays pinned until the stall
  # watchdog invalidates it and every client holding it refetches. Restate the boundary instead of
  # going quiet: the tracker matches the offset exactly, so it has to be the deduplicated line's
  # own offset, not the writer's global boundary. Offsets rise within a fragment while the boundary
  # they are judged against does not, so deduplication is always a prefix: every write path clears
  # the marker, and a marker that survives to here means the fragment wrote nothing at all.
  defp confirm_deduplicated_lines_durable(writer_acc(last_deduped_offset: nil) = acc), do: acc

  defp confirm_deduplicated_lines_durable(
         writer_acc(last_deduped_offset: offset, last_synced_offset: synced) = acc
       )
       when is_log_offset_lte(offset, synced) do
    send(self(), {Storage, :flushed, offset})
    writer_acc(acc, last_deduped_offset: nil)
  end

  # Deduplication is judged against the seen boundary, which can outrun the durable one while
  # bytes await datasync. Claiming a flush there would ack the slot past what a crash could lose.
  defp confirm_deduplicated_lines_durable(acc), do: writer_acc(acc, last_deduped_offset: nil)

  ### Working with the buffer

  # Arena-ref line: the file gets the fixed-size ref entry, but the live-tail ETS mirror
  # needs the real JSON (reads serve it verbatim) and chunk accounting needs the real payload
  # size (chunks size client response pages, which serve resolved payloads).
  defp add_to_buffer(
         writer_acc(
           buffer: buffer,
           buffer_size: buffer_size,
           write_position: write_position,
           ets_line_buffer: ets_line_buffer,
           bytes_in_chunk: bytes_in_chunk
         ) =
           acc,
         {offset, _, _, _, _, _, {_ref, json, payload_size}} = line
       ) do
    {iodata, iodata_size} = LogFile.make_entry(line)

    writer_acc(acc,
      buffer: [buffer | iodata],
      ets_line_buffer: [{LogOffset.to_tuple(offset), json} | ets_line_buffer],
      buffer_size: buffer_size + iodata_size,
      write_position: write_position + iodata_size,
      bytes_in_chunk: bytes_in_chunk + payload_size,
      last_seen_offset: offset,
      last_deduped_offset: nil
    )
  end

  defp add_to_buffer(
         writer_acc(
           buffer: buffer,
           buffer_size: buffer_size,
           write_position: write_position,
           ets_line_buffer: ets_line_buffer,
           bytes_in_chunk: bytes_in_chunk
         ) =
           acc,
         {offset, _, _, _, _, json_size, json} = line
       ) do
    {iodata, iodata_size} = LogFile.make_entry(line)

    writer_acc(acc,
      buffer: [buffer | iodata],
      ets_line_buffer: [{LogOffset.to_tuple(offset), json} | ets_line_buffer],
      buffer_size: buffer_size + iodata_size,
      write_position: write_position + iodata_size,
      bytes_in_chunk: bytes_in_chunk + json_size,
      last_seen_offset: offset,
      last_deduped_offset: nil
    )
  end

  # A framed block is a whole run of class entries whose bytes were framed once by the
  # routing side. The fast path appends the shared frame in one step; anything that breaks
  # the one-step invariants (a chunk boundary inside the block, or a replay boundary
  # straddling it) falls back to the exact per-entry pipeline over frame slices, so boundary
  # and flush semantics stay byte-identical with the unblocked path.
  defp append_block_to_buffer(
         %FramedBlock{} = block,
         writer_acc(last_seen_txn_offset: min_offset, bytes_in_chunk: bytes_in_chunk) = acc,
         writer_state(opts: %{chunk_bytes_threshold: maximum}) = state
       ) do
    cond do
      is_log_offset_lte(block.first_offset, min_offset) ->
        explode_block(block, acc, state)

      bytes_in_chunk + block.payload_size < maximum ->
        writer_acc(
          buffer: buffer,
          buffer_size: buffer_size,
          write_position: write_position,
          ets_line_buffer: ets_line_buffer
        ) = acc = maybe_write_opening_chunk_boundary(acc, state, block.first_offset)

        writer_acc(acc,
          buffer: [buffer | block.frame],
          ets_line_buffer: [block.ets_lines | ets_line_buffer],
          buffer_size: buffer_size + block.frame_size,
          write_position: write_position + block.frame_size,
          bytes_in_chunk: bytes_in_chunk + block.payload_size,
          last_seen_offset: block.last_offset,
          last_deduped_offset: nil
        )
        |> maybe_flush_buffer(state)

      true ->
        explode_block(block, acc, state)
    end
  end

  defp explode_block(%FramedBlock{} = block, acc, state) do
    {acc, _ets_rest, _fstart, _pstart} =
      Enum.reduce(0..(block.count - 1), {acc, block.ets_lines, 0, 0}, fn i,
                                                                         {acc,
                                                                          [ets_line | ets_rest],
                                                                          fstart, pstart} ->
        {offset, fend, pcum} = elem(block.entries_meta, i)

        acc =
          case acc do
            writer_acc(last_seen_txn_offset: min) when is_log_offset_lte(offset, min) ->
              writer_acc(acc, last_deduped_offset: offset)

            acc ->
              entry_bin = binary_part(block.frame, fstart, fend - fstart)

              acc
              |> maybe_write_opening_chunk_boundary(state, offset)
              |> add_framed_entry_to_buffer(
                offset,
                entry_bin,
                fend - fstart,
                pcum - pstart,
                ets_line
              )
              |> maybe_write_closing_chunk_boundary(state)
              |> maybe_flush_buffer(state)
          end

        {acc, ets_rest, fend, pcum}
      end)

    acc
  end

  defp add_framed_entry_to_buffer(
         writer_acc(
           buffer: buffer,
           buffer_size: buffer_size,
           write_position: write_position,
           ets_line_buffer: ets_line_buffer,
           bytes_in_chunk: bytes_in_chunk
         ) = acc,
         offset,
         entry_bin,
         entry_size,
         payload_bytes,
         ets_line
       ) do
    writer_acc(acc,
      buffer: [buffer | entry_bin],
      ets_line_buffer: [ets_line | ets_line_buffer],
      buffer_size: buffer_size + entry_size,
      write_position: write_position + entry_size,
      bytes_in_chunk: bytes_in_chunk + payload_bytes,
      last_seen_offset: offset,
      last_deduped_offset: nil
    )
  end

  ### Working with chunk boundaries

  defp maybe_write_opening_chunk_boundary(writer_acc(chunk_started?: true) = acc, _, _), do: acc

  defp maybe_write_opening_chunk_boundary(
         writer_acc(write_position: pos) = acc,
         writer_state(opts: opts) = state,
         offset
       ) do
    writer_acc(acc, chunk_started?: true)
    |> ensure_chunk_file_open(state)
    |> write_to_chunk_file(ChunkIndex.make_half_entry(offset, pos, 0))
    |> add_opening_chunk_boundary_to_cache(offset, pos)
    |> update_chunk_boundaries_cache(opts)
  end

  defp maybe_write_closing_chunk_boundary(
         writer_acc(bytes_in_chunk: total) = acc,
         writer_state(opts: %{chunk_bytes_threshold: maximum})
       )
       when total < maximum,
       do: acc

  defp maybe_write_closing_chunk_boundary(
         writer_acc(last_seen_offset: offset, write_position: position) = acc,
         writer_state(opts: opts) = state
       ) do
    writer_acc(acc, chunk_started?: false, bytes_in_chunk: 0)
    |> ensure_chunk_file_open(state)
    |> write_to_chunk_file(ChunkIndex.make_half_entry(offset, position, 0))
    |> add_closing_chunk_boundary_to_cache(offset, position)
    |> update_chunk_boundaries_cache(opts)
    |> flush_buffer(state)
  end

  defp write_to_chunk_file(
         writer_acc(open_files: open_files(chunk_file: chunk_file)) = acc,
         entry
       )
       when not is_nil(chunk_file) do
    IO.binwrite(chunk_file, entry)
    acc
  end

  ### Working with chunk boundaries cache

  # We're keeping at most 3 latest chunks in memory. Adding a new one pushes the oldest one out
  defp add_opening_chunk_boundary_to_cache(
         writer_acc(cached_chunk_boundaries: {boundary, cached_chunks}) = acc,
         offset,
         pos
       )
       when length(cached_chunks) < 3 do
    writer_acc(acc,
      cached_chunk_boundaries: {boundary, cached_chunks ++ [{{offset, nil}, {pos, nil}}]}
    )
  end

  defp add_opening_chunk_boundary_to_cache(
         writer_acc(cached_chunk_boundaries: {_, [{{_, max}, _} | cached_chunks]}) = acc,
         offset,
         pos
       ) do
    writer_acc(acc,
      cached_chunk_boundaries: {max, cached_chunks ++ [{{offset, nil}, {pos, nil}}]}
    )
  end

  defp add_closing_chunk_boundary_to_cache(
         writer_acc(cached_chunk_boundaries: {boundary, cached_chunks}) = acc,
         max,
         end_pos
       ) do
    [{{min, nil}, {start_pos, nil}} | rest] = Enum.reverse(cached_chunks)

    writer_acc(acc,
      cached_chunk_boundaries:
        {boundary, Enum.reverse([{{min, max}, {start_pos, end_pos}} | rest])}
    )
  end

  defp update_chunk_boundaries_cache(writer_acc(cached_chunk_boundaries: cached) = acc, opts) do
    PureFileStorage.update_chunk_boundaries_cache(opts, cached)
    acc
  end

  ### Working with files

  defp ensure_json_file_open(writer_acc(open_files: open_files(json_file: x)) = acc, _state)
       when not is_nil(x),
       do: acc

  defp ensure_json_file_open(
         writer_acc(open_files: open_files(json_file: nil) = open_files) = acc,
         state
       ) do
    files = open_files(open_files, json_file: open_file(state, :json_file))

    writer_acc(acc, open_files: files)
  end

  defp ensure_chunk_file_open(writer_acc(open_files: open_files(chunk_file: x)) = acc, _state)
       when not is_nil(x),
       do: acc

  defp ensure_chunk_file_open(
         writer_acc(open_files: open_files(chunk_file: nil) = open_files) = acc,
         state
       ) do
    files = open_files(open_files, chunk_file: open_file(state, :chunk_file))

    writer_acc(acc, open_files: files)
  end

  defp close_chunk_file(writer_acc() = acc) when not is_chunk_file_open(acc), do: acc

  defp close_chunk_file(
         writer_acc(open_files: open_files(chunk_file: chunk_file) = open_files) = acc
       ) do
    File.close(chunk_file)
    writer_acc(acc, open_files: open_files(open_files, chunk_file: nil))
  end

  defdelegate open_file(state, type), to: PureFileStorage

  ### Working with ets

  defp maybe_store_lines_in_ets(writer_acc(buffer_size: 0) = acc, _state), do: acc

  defp maybe_store_lines_in_ets(
         writer_acc(ets_line_buffer: buffer) = acc,
         writer_state(ets: ets)
       ) do
    # Framed blocks contribute their line list as one element; flatten preserves tuples.
    :ets.insert(ets, List.flatten(buffer))

    acc
  end

  defp trim_ets(writer_acc() = acc, writer_state(ets: ets)) do
    :ets.delete_all_objects(ets)
    acc
  end

  ### Working with flush

  @delayed_write 64 * 1024

  # A storm can run for minutes without a quiet flush_period, so unsynced bytes (and with them
  # the replication-slot ack and the crash-replay window) must be bounded by volume too, not
  # only by the timer.
  @sync_bytes_threshold 8 * 1024 * 1024

  defp maybe_flush_buffer(writer_acc(buffer_size: size) = acc, state)
       when size >= @delayed_write do
    acc = flush_buffer(acc, state, false)

    case acc do
      writer_acc(unsynced_bytes: unsynced) when unsynced >= @sync_bytes_threshold ->
        sync_written(acc, state)

      acc ->
        acc
    end
  end

  defp maybe_flush_buffer(acc, _), do: acc

  @doc """
  Flush the buffer if it's not empty.

  `sync?: false` only hands the bytes to the OS: reads and ETS trimming are safe (they need the
  bytes in the file, not on disk) but nothing durability-facing may advance. `sync?: true`
  additionally datasyncs and moves the durable boundary: the flushed notification and the
  metadata file only ever reflect synced bytes, so a crash can lose an unsynced tail without
  the metadata claiming it — recovery trims the log to the metadata offset, and the slot was
  never acked past it.
  """
  def flush_buffer(acc, state, sync? \\ true)

  def flush_buffer(writer_acc(buffer_size: 0) = acc, state, sync?) do
    if sync?, do: sync_written(acc, state), else: acc
  end

  def flush_buffer(
        writer_acc(
          buffer: buffer,
          buffer_size: buffer_size,
          last_seen_offset: last_seen_offset,
          last_seen_txn_offset: last_seen_txn,
          last_persisted_txn_offset: last_written_txn,
          times_flushed: times_flushed,
          unsynced_bytes: unsynced_bytes,
          open_files: open_files(json_file: json_file)
        ) = acc,
        writer_state(opts: opts) = state,
        sync?
      ) do
    IO.binwrite(json_file, buffer)

    # The bytes are readable from the file now, so the in-memory lines can go and the cached
    # read boundary advances — durability is the sync point's business, not this one's.
    PureFileStorage.update_persistence_cache(
      opts,
      last_seen_txn,
      last_seen_offset,
      last_seen_txn,
      last_written_txn
    )

    acc =
      writer_acc(acc,
        buffer: [],
        buffer_size: 0,
        ets_line_buffer: [],
        last_persisted_offset: last_seen_offset,
        last_persisted_txn_offset: last_seen_txn,
        times_flushed: times_flushed + 1,
        unsynced_bytes: unsynced_bytes + buffer_size
      )
      |> trim_ets(state)

    if sync?, do: sync_written(acc, state), else: acc
  end

  defp sync_written(
         writer_acc(
           last_synced_offset: synced,
           last_persisted_offset: written,
           last_synced_txn_offset: synced_txn,
           last_persisted_txn_offset: written_txn
         ) = acc,
         _state
       )
       when synced == written and synced_txn == written_txn,
       do: acc

  defp sync_written(writer_acc(open_files: open_files(json_file: nil)) = acc, _state), do: acc

  # Every byte is already durable and there is nothing left to datasync, but the commit that
  # arrived after they were synced moved the transaction boundary with no byte behind it to
  # carry the metadata write. Returning early here leaves the metadata FILE naming the previous
  # transaction while the flushed notification for these bytes has already gone out and, through
  # it, the slot ack: a machine crash then loses the buffered written marker and recovery trims
  # the log back to a transaction Postgres will not resend.
  defp sync_written(
         writer_acc(
           last_synced_offset: synced,
           last_persisted_offset: written,
           last_synced_txn_offset: synced_txn,
           last_persisted_txn_offset: written_txn
         ) = acc,
         state
       )
       when synced == written do
    writer_acc(acc, last_synced_txn_offset: written_txn)
    |> update_persistance_metadata(state, synced_txn)
  end

  defp sync_written(
         writer_acc(
           open_files: open_files(json_file: json_file),
           last_persisted_offset: written,
           last_persisted_txn_offset: written_txn,
           last_synced_txn_offset: synced_txn
         ) = acc,
         state
       ) do
    # The flushed notification below is the only durability claim in the system: it advances the
    # replication slot ack and lets the journal generation covering these bytes be reclaimed. A
    # datasync that failed (ENOSPC, EIO) and was ignored would release both copies of data that
    # never reached the disk, so it must take the writer down instead.
    :ok = :file.datasync(json_file)

    # Tell the parent process that we've durably flushed up to this point
    send(self(), {Storage, :flushed, written})

    writer_acc(acc,
      last_synced_offset: written,
      last_synced_txn_offset: written_txn,
      unsynced_bytes: 0
    )
    |> update_persistance_metadata(state, synced_txn)
  end

  defp update_persistance_metadata(
         writer_acc(
           last_persisted_txn_offset: last_persisted_txn,
           last_persisted_offset: last_persisted_offset,
           last_seen_txn_offset: last_seen_txn
         ) = acc,
         writer_state(opts: opts),
         old_last_persisted_txn_offset
       ) do
    PureFileStorage.update_global_persistence_information(
      opts,
      last_persisted_txn,
      last_persisted_offset,
      last_seen_txn,
      old_last_persisted_txn_offset
    )

    acc
  end

  # This helper function must be called after the last log items of a transaction has been
  # written. It ensures that txn offset is advanced forward in the writer state.
  #
  # Syncing here is what "sync at transaction boundaries" means: bytes the txn already
  # flushed become durable (and claimable by the metadata file) at its commit, while bytes
  # still in the buffer stay deliberately unsynced — sync_written covers flushed bytes only,
  # so a small txn's durability waits for the flush timer or the volume threshold, and the
  # replication slot is never acked past what a crash could lose.
  defp finalize_txn(acc, state) do
    writer_acc(last_seen_offset: offset) = acc

    acc
    |> writer_acc(last_seen_txn_offset: offset)
    |> maybe_store_lines_in_ets(state)
    |> register_complete_txn(state)
    |> sync_written(state)
  end

  defp register_complete_txn(
         writer_acc(
           last_seen_offset: last_seen,
           last_persisted_offset: last_persisted,
           last_persisted_txn_offset: prev_persisted_txn
         ) = acc,
         writer_state(opts: opts)
       ) do
    acc =
      if last_seen == last_persisted do
        writer_acc(acc, last_persisted_txn_offset: last_seen, last_seen_txn_offset: last_seen)
      else
        writer_acc(acc, last_seen_txn_offset: last_seen)
      end

    # Cache only: the metadata FILE may not claim this txn until its bytes are datasynced,
    # which is sync_written's job at the next timer/threshold/terminate sync point.
    writer_acc(
      last_persisted_txn_offset: last_persisted_txn,
      last_persisted_offset: last_persisted_offset,
      last_seen_txn_offset: last_seen_txn
    ) = acc

    PureFileStorage.update_persistence_cache(
      opts,
      last_persisted_txn,
      last_persisted_offset,
      last_seen_txn,
      prev_persisted_txn
    )

    acc
  end

  @doc """
  Signal that a transaction has been committed.

  This updates `last_seen_txn_offset` and potentially `last_persisted_txn_offset`
  to mark the transaction as complete. Should be called after all fragments
  have been written via `append_fragment_to_log!/3`.
  """
  def signal_txn_commit(acc, state) do
    finalize_txn(acc, state)
  end
end
