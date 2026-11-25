defmodule Electric.ShapeCache.PureFileStorage do
  @moduledoc """
  Main architecture & feature overview:
  ---
  - 2 file formats: for snapshots and for main log, because snapshots have a requirement to be streamed as they're written
  - Snapshot format: comma-separated JSON lines (for future copy-to-socket possibilities), one file per chunk, ends with a `0x04` byte (end-of-transmission) to distinguish EOF because reader is up to date with writer from EOF because writer is finished
  - Main log format:
  - Log file: binary file, formatted as:

    ```elixir
    <<tx_offset::64, op_offset::64,
      key_size::32, key::binary(key_size),
      op_type::8, flag::8,
      json_size::64 json::binary(json_size)>>
    ```
  - Chunk file: binary file, formatted as:

    ```elixir
    <<min_tx_offset::64, min_op_offset::64, start_pos::64, key_start_pos::64,
      max_tx_offset::64, max_op_offset::64, end_pos::64, key_end_pos::64>>
    ```
    where start_pos & end_pos can be used for full chunk read into memory if needed, and min/max offsets are inclusive. Last chunk might not have the max/end part of the binary (i.e. it's half width)
  - Writes are buffered at 64kb or 1s boundary, and the main pointer is the "last persisted full txn offset" - it's updated atomically, and last, and the readers are expected to respect that pointer as an upper bound for reading - any entires in the log file beyond that are considered volatile and might be trimmed in case the writer hard-crashes without flushing. Any reads beyond that boundary should rely on system being live (see 2 next points).
  - For read consistency on live tail of the log, buffered writes are also made available to readers through an ETS. Anything not flushed to disk yet is addressable inside an ETS, and deleted from there as soon as flushed
  - Buffering is __not__ transaction-aligned. Flush might include multiple transactions, or be done mid-transaction. To allow for consistent reads, we maintain an in-memory pointer to "last written offset" (always gte than "last persisted full txn offset") which acts as a definitely-synced boundary for ongoing reads which read part of the transaction from ETS and disk when a transaction is partially written.
  - This 2-layer setup is there to allow for read-consistent buffered writes without reader processes going to the writer.
  - In case the writer is offline and in-memory ETS/buffer is not present, reads still succeed using on-disk information (i.e. last persisted full txn offset).
  """

  alias Electric.ProcessRegistry
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.LogChunker
  alias Electric.ShapeCache.PureFileStorage.ActionFile
  alias Electric.ShapeCache.PureFileStorage.ChunkIndex
  alias Electric.ShapeCache.PureFileStorage.FileInfo
  alias Electric.ShapeCache.PureFileStorage.KeyIndex
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.ShapeCache.PureFileStorage.Snapshot
  alias Electric.ShapeCache.PureFileStorage.WriteLoop
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.Storage
  alias Electric.Utils

  import LogOffset
  import Electric.ShapeCache.PureFileStorage.SharedRecords

  import File, only: [write!: 3]

  require Logger

  @behaviour Electric.ShapeCache.Storage

  # Struct that can be used to create a writer_state record or a reader
  @version 1

  # A log holding payload-arena refs cannot be read by a version that predates them: the ref
  # occupies the payload slot and carries its own size, so an older reader finds a well-formed
  # entry and hands the raw ref bytes to a client as JSON. Claiming a distinct version makes
  # that reader discard the shape and rebuild it instead of serving nonsense.
  @arena_version 2

  # Checksum-bearing refs are four bytes wider, and the release that introduced refs matches the
  # narrow form as an exact-width binary, so it does not merely misread one -- it crashes on it.
  # A distinct version makes that release discard the shape, exactly as the pre-arena one does.
  @arena_checksum_version 3

  # Versions this build serves as they stand, and the only thing a discard may be decided on.
  # Requiring the recorded version to equal the one this boot would write discards every pre-arena
  # log in the stack the moment any one shape writes the first arena generation, even though ref
  # entries are self-describing and those logs read back exactly as they are. Refs of both widths
  # are self-describing to this build, so a log recorded under any of these reads as it stands.
  @readable_versions [@version, @arena_version, @arena_checksum_version]

  defstruct [
    :buffer_ets,
    :chunk_bytes_threshold,
    :shape_handle,
    :stack_id,
    :stack_ets,
    read_only?: false,
    snapshot_file_timeout: :timer.seconds(5),
    version: @version
  ]

  # Explicitly specify which keys are actually stored as metadata files on disk
  # to avoid unnecessary FS operations for non-persistent keys
  @stored_keys [
    :version,
    :latest_name,
    :last_persisted_txn_offset,
    :last_written_txn_offset,
    :snapshot_started?,
    :pg_snapshot,
    :last_snapshot_chunk,
    :compaction_started?,
    :compaction_boundary,
    :durable_generation
  ]

  # Including `compaction_started?` would require bumping the version of the storage,
  # as there are cases where we would have a file with "false" stored in it.
  # For `snapshot_started?` we only have the metadata file if it has been set.
  @boolean_stored_keys [:snapshot_started?]

  # keys to populate an empty cache with and to preserve on writer termination
  # for access via the read path
  @read_path_keys [
    :cached_chunk_boundaries,
    :compaction_boundary,
    :last_persisted_offset,
    :last_persisted_txn_offset,
    :last_seen_txn_offset,
    :last_snapshot_chunk,
    :latest_name,
    :snapshot_started?
  ]

  @clean_cache_keys storage_meta_keys() -- @read_path_keys

  def shared_opts(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    storage_dir = Keyword.get(opts, :storage_dir, "./shapes")
    # Always scope the provided storage dir by stack id
    base_path = Path.join(storage_dir, stack_id)

    # base the tmp dir at the root of the storage (.tmp/stack_id not
    # stack_id/.tmp) so it's easy to remove
    tmp_dir = Keyword.get(opts, :tmp_dir, Path.join([storage_dir, ".tmp", stack_id]))

    %{
      base_path: base_path,
      tmp_dir: tmp_dir,
      stack_id: stack_id,
      chunk_bytes_threshold:
        Keyword.get(opts, :chunk_bytes_threshold) || LogChunker.default_chunk_size_threshold(),
      flush_period: Keyword.get(opts, :flush_period) || :timer.seconds(1),
      compaction_config: %{
        period: Keyword.get(opts, :compaction_period) || :timer.minutes(10),
        keep_complete_chunks: Keyword.get(opts, :keep_complete_chunks) || 2
      }
    }
  end

  def for_shape(shape_handle, %{stack_id: stack_id} = stack_opts) do
    stack_ets = stack_ets(stack_id)
    buffer_ets = :ets.lookup_element(stack_ets, shape_handle, storage_meta(:ets_table) + 1, nil)

    %__MODULE__{
      buffer_ets: buffer_ets,
      chunk_bytes_threshold: stack_opts.chunk_bytes_threshold,
      read_only?: Map.get(stack_opts, :read_only?, false),
      shape_handle: shape_handle,
      stack_id: stack_id,
      stack_ets: :ets.whereis(stack_ets)
    }
  end

  @log_dir "log"
  @metadata_dir "metadata"
  @snapshot_dir "snapshot"
  @boot_dir "boot"

  @doc false
  def stack_ets(stack_id), do: :"Electric.ShapeCache.PureFileStorage:#{stack_id}"

  defp stack_task_supervisor(stack_id),
    do: ProcessRegistry.name(stack_id, __MODULE__.TaskSupervisor)

  def shape_data_dir(%__MODULE__{} = shape_opts) do
    shape_data_dir(shape_opts, [])
  end

  def shape_data_dir(%__MODULE__{stack_id: stack_id, shape_handle: shape_handle}, suffix) do
    base_path = Storage.opt_for_stack(stack_id, :base_path)
    shape_data_dir(base_path, shape_handle, suffix)
  end

  @doc false
  def shape_data_dir(base_path, shape_handle, suffix \\ [])
      when is_binary(base_path) and is_binary(shape_handle) do
    # nest storage to limit number of files per directory
    <<p1::binary-2, p2::binary-2, _::binary>> = shape_handle
    Path.join([base_path, p1, p2, shape_handle | suffix])
  end

  defp shape_log_dir(opts), do: shape_data_dir(opts, [@log_dir])
  def shape_log_path(opts, filename), do: shape_data_dir(opts, [@log_dir, filename])

  defp shape_metadata_dir(opts), do: shape_data_dir(opts, [@metadata_dir])
  defp shape_metadata_path(opts, filename), do: shape_data_dir(opts, [@metadata_dir, filename])

  defp shape_snapshot_dir(opts), do: shape_data_dir(opts, [@snapshot_dir])
  defp shape_snapshot_path(opts, filename), do: shape_data_dir(opts, [@snapshot_dir, filename])

  defp tmp_dir(%__MODULE__{} = opts), do: Storage.opt_for_stack(opts.stack_id, :tmp_dir)

  def stack_start_link(opts) do
    sync_storage_root!(opts.base_path)

    Supervisor.start_link(
      [
        {Agent,
         fn ->
           :ets.new(stack_ets(opts.stack_id), [
             :named_table,
             :set,
             :public,
             keypos: storage_meta(:shape_handle) + 1,
             read_concurrency: true,
             write_concurrency: :auto
           ])
         end},
        {Task.Supervisor, name: stack_task_supervisor(opts.stack_id)},
        # TODO: remove once we're sure that no install has un-nested storage
        # directories
        {Task, fn -> remove_unnested_storage(opts.base_path) end}
      ],
      strategy: :one_for_one
    )
  end

  # Lists `base_path/*/*/*` one level at a time: `Path.wildcard/1` gives the same set, but builds
  # every path whole as a charlist and sorts them all first, which took about ten times as long as
  # this with a large shape count, in the refresh a boot waits on. As with the wildcard, dot
  # entries are left out and a level that cannot be listed counts as empty.
  def get_all_stored_shape_handles(%{base_path: base_path}) do
    shape_handles =
      for p1 <- list_dir(base_path),
          p2 <- list_dir(Path.join(base_path, p1)),
          shape_handle <- list_dir(Path.join([base_path, p1, p2])),
          into: MapSet.new(),
          do: shape_handle

    {:ok, shape_handles}
  end

  defp list_dir(path) do
    case File.ls(path) do
      {:ok, names} -> Enum.reject(names, &String.starts_with?(&1, "."))
      {:error, _} -> []
    end
  end

  @doc """
  The highest transaction offset any shape stored under `base_path` keeps across a restart: the
  further of the two boundaries its writer recovers to, and of the commit its boot record carries
  from a written marker an earlier boot left. Nil when no shape is stored.

  With `root_names: names` a store root that does not list, or whose listing names none of
  `names`, fails the scan: a remount that takes the root away for its listing leaves nothing
  there, or the directory under its mount point, and either holds no shape to it. The promotion
  check reads the journal's bound from that root first and passes the names the journal keeps
  there, so the root named one of them.
  """
  @spec stored_high_water(String.t(), keyword()) :: non_neg_integer() | nil
  def stored_high_water(base_path, opts \\ []) do
    shape_dirs =
      case Keyword.fetch(opts, :root_names) do
        {:ok, names} -> stored_shape_dirs!(base_path, [], names)
        :error -> stored_shape_dirs!(base_path)
      end

    shape_dirs
    |> Enum.flat_map(fn shape_dir ->
      case shape_look!(shape_dir) do
        :gone ->
          []

        :ok ->
          Enum.flat_map([:last_written_txn_offset, :last_persisted_txn_offset], fn key ->
            path = Path.join([shape_dir, @metadata_dir, "#{key}.bin"])
            read_or_absent!(shape_dir, path, fn -> marker_offset!(path) end)
          end) ++ recorded_high_water(shape_dir)
      end
    end)
    |> Enum.max(fn -> nil end)
  end

  # A written marker that does not decode fails the scan, though recovery reads it as none: a run
  # with the journal off served the commits it named, and the journal of a later run does not bound
  # them.
  defp marker_offset!(path) do
    case File.read(path) do
      {:ok, bin} -> [:erlang.binary_to_term(bin).tx_offset]
      {:error, :enoent} -> []
      {:error, reason} -> raise File.Error, reason: reason, action: "read file", path: path
    end
  end

  # A shape's path holds a directory, or nothing in a shard that still lists: a cleanup renames a
  # shape out of its shard whole, the boot's reclaim of an orphan say, and what it moved away is
  # served no more. Anything else fails the scan: a symlinked shape whose target is away, or a
  # shape under a shard that does not resolve, is served again once its target is back.
  defp shape_look!(shape_dir) do
    case File.stat(shape_dir) do
      {:ok, %File.Stat{type: :directory}} ->
        :ok

      {:ok, _stat} ->
        raise File.Error, reason: :enotdir, action: "read directory", path: shape_dir

      {:error, :enoent} ->
        if moved_out_of_shard?(shape_dir),
          do: :gone,
          else: raise(File.Error, reason: :enoent, action: "read directory", path: shape_dir)

      {:error, reason} ->
        raise File.Error, reason: reason, action: "read directory", path: shape_dir
    end
  end

  defp moved_out_of_shard?(shape_dir) do
    case File.ls(Path.dirname(shape_dir)) do
      {:ok, names} -> Path.basename(shape_dir) not in names
      {:error, _reason} -> false
    end
  end

  # A marker a shape has yet to write is none, and so is one in a directory it has yet to create.
  # A read finds nothing in a shape that does not resolve, a symlinked shape whose target is away
  # say, though the marker is back with the target. So a read that finds nothing is followed by a
  # look at the shape and at the marker's directory, a read again, and a look at the shape, and the
  # marker is none only if the second read finds nothing too.
  defp read_or_absent!(shape_dir, path, read) do
    with [] <- read.(),
         :ok <- shape_look!(shape_dir),
         :ok <- marker_dir_look!(path),
         [] <- read.(),
         :ok <- shape_look!(shape_dir) do
      []
    else
      :gone -> []
      found -> found
    end
  end

  # A marker in a directory that is a symlink whose target is away is not none: it is back with the
  # target.
  defp marker_dir_look!(path) do
    dir = Path.dirname(path)

    case File.lstat(dir) do
      {:ok, %File.Stat{type: :symlink}} ->
        case File.stat(dir) do
          {:ok, _stat} -> :ok
          {:error, reason} -> raise File.Error, reason: reason, action: "read file", path: path
        end

      {:ok, _stat} ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        raise File.Error, reason: reason, action: "read file", path: path
    end
  end

  # The two levels above a shape's directory are named after the first two pairs of bytes of its
  # handle (`shape_data_dir/3`). An entry with another name, the journal's directory say, holds no
  # shape, so one that cannot be listed does not fail the scan, and neither does a file. A shard
  # the listing found is listed in turn, and one that cannot be fails the scan: a symlinked shard
  # whose target is away for a moment serves the shapes under it by handle again once it is back,
  # and nothing removes a shard directory. A store moved away whole by a purge fails it too, which
  # both callers take as no answer.
  defp shards(paths),
    do: Enum.filter(paths, &(byte_size(Path.basename(&1)) == 2 and not file?(&1)))

  defp shard_entries!(shard), do: list_dir!(shard, [])

  # An entry that is neither a directory nor a symlink: no shape can sit under it.
  defp file?(path),
    do:
      match?(
        {:ok, %File.Stat{type: type}} when type not in [:directory, :symlink],
        File.lstat(path)
      )

  # The entries three levels down that sit at the path their name maps to (`shape_data_dir/3`). The
  # service reads a shape only at its handle's path, so an entry elsewhere, a copy moved there by
  # hand say, is read by no response, and what it holds, or fails to list, says nothing of a shape.
  # Nor does an entry there that is itself a file, a stray one say: no shape can sit under it. A
  # symlink there is kept whatever it resolves to, for one retargeted at a file after the listing
  # hid a shape a response still reads, and the reads that follow fail on it. So is one the look
  # cannot find: the reads that follow tell a shape that does not resolve from one a cleanup moved
  # away, which the arena scan fails on and the promotion scan passes over.
  defp stored_shape_dirs!(base_path, root_nothing_under \\ [:enoent, :enotdir], root_names \\ nil) do
    base_path
    |> store_root_entries!(root_nothing_under)
    |> naming_one_of!(base_path, root_names)
    |> shards()
    |> Enum.flat_map(&(&1 |> shard_entries!() |> shards()))
    |> Enum.flat_map(&shard_entries!/1)
    |> Enum.filter(&at_handle_path?(base_path, &1))
    |> Enum.reject(&file?/1)
  end

  # A store root not created yet holds no shape. One that is a symlink whose target is away serves
  # its shapes again once the target is back, and fails the scan.
  defp store_root_entries!(base_path, nothing_under) do
    case ls_dir!(base_path, nothing_under) do
      {:ok, paths} ->
        paths

      {:error, reason} ->
        if match?({:ok, %File.Stat{type: :symlink}}, File.lstat(base_path)),
          do: raise(File.Error, reason: reason, action: "list directory", path: base_path),
          else: []
    end
  end

  defp naming_one_of!(paths, _base_path, nil), do: paths

  defp naming_one_of!(paths, base_path, names) do
    if Enum.any?(paths, &(Path.basename(&1) in names)),
      do: paths,
      else:
        raise(Storage.Error,
          message: "the store root at #{base_path} lists none of #{Enum.join(names, ", ")}"
        )
  end

  defp at_handle_path?(base_path, path) do
    case Path.basename(path) do
      <<_::binary-4, _::binary>> = name -> shape_data_dir(base_path, name) == path
      _name -> false
    end
  end

  # The paths in `path`, dot entries left out, as `Path.wildcard/1` gives them. The wildcard also
  # leaves out what it cannot list, a directory that can be searched and not read say, though a
  # shape there can hold the only trace of a commit past a promotion's switch point: a listing that
  # fails raises. A path gone, or one that is not a directory, has nothing under it when the
  # caller names the reason.
  defp list_dir!(path, nothing_under) do
    case ls_dir!(path, nothing_under) do
      {:ok, paths} -> paths
      {:error, _reason} -> []
    end
  end

  # As `list_dir!/2`, with a path there is nothing under told apart from an empty directory.
  defp ls_dir!(path, nothing_under) do
    case File.ls(path) do
      {:ok, names} ->
        {:ok, for(name <- names, not String.starts_with?(name, "."), do: Path.join(path, name))}

      {:error, reason} ->
        if reason in nothing_under,
          do: {:error, reason},
          else: raise(File.Error, reason: reason, action: "list directory", path: path)
    end
  end

  # A record a crash took was written unsynced, so it carried no commit past the claim, and one
  # that cannot be read fails the scan (`read_boot_record_file/1`).
  defp recorded_high_water(shape_dir) do
    boot_dir = Path.join(shape_dir, @boot_dir)

    read_or_absent!(shape_dir, Path.join(boot_dir, "written_boot.bin"), fn ->
      case read_boot_record_file(boot_dir) do
        {_boot, _marker, :unknown} ->
          raise Storage.Error,
            message: "the written marker an earlier boot left in #{shape_dir} was unreadable"

        {_boot, _marker, high_water} when is_integer(high_water) ->
          [high_water]

        _ ->
          []
      end
    end)
  end

  @doc """
  The set of payload-arena generations every surviving shape log still resolves a ref into.

  The arena reclaimer keeps exactly these and drops the rest, so the scan must find every ref: a
  generation missed here would be read as dead and its live payloads lost. Refs live only in
  uncompacted `latest` logs -- compaction inlines them -- so those are what is scanned; a read
  error makes `stream_entries` raise, and the caller must let that abort the reclaim rather than
  delete on a partial answer. A torn tail ends the log's scan, as it ends its reads. A shard or log
  directory that cannot be listed raises too, and so does a shape's log path that is not a
  directory: the shapes under it are still served by handle, and their refs would be read as none.
  A shape a cleanup moves away after the scan listed it raises, and so does a log gone between its
  listing and its read, as any read error does: a response begun before a restart still reads a
  removed shape's log, or a compacted generation, through the descriptor it opened, and resolves
  the refs it reads next. So does a log directory gone from a shape whose version is written, which
  its writer created first, and one the listing found gone and the looks after it found back. A
  symlink at a shape's path that resolves to a file raises too: one retargeted after the listing
  hid a shape a response still reads. A shape moved away before the scan lists it is not seen.
  Only an entry at the path its name maps to is scanned, as only that one is served. The store's
  root must list, and name the arena's directory: the boot scans only a store whose arena holds
  payloads, and a root that is a symlink whose target is away for the listing, or the directory
  a remount leaves under its mount point in the store's place, would read as holding no shape.
  """
  @spec referenced_arena_generations(String.t()) :: MapSet.t(non_neg_integer())
  def referenced_arena_generations(base_path) do
    base_path
    |> stored_shape_dirs!([], [PayloadArena.dir_name()])
    |> Enum.reduce(MapSet.new(), fn shape_dir, gens ->
      shape_dir
      |> shape_log_paths!()
      |> Enum.filter(&(Path.basename(&1) =~ ~r/\Alog\.latest\..*\.jsonfile\.bin\z/))
      |> Enum.reduce(gens, &MapSet.union(&2, LogFile.arena_generations_in_log(&1)))
    end)
  end

  # A shape directory its writer has yet to give a log directory holds no log. One the scan listed
  # and then found gone was moved away by a cleanup during the scan, logs and all. The look follows
  # a symlink, as the reads do: a shape directory, or a log directory, that is a symlink whose
  # target moved away is gone too, and a response still reads the logs it held through the
  # descriptor it opened. A log path that is not a directory, a symlink retargeted at a file say,
  # cannot be listed, and the logs a response opened before through it are not seen: it fails the
  # scan. So does a log directory the listing found gone and the looks after it found back, a
  # symlinked one whose target was away for a moment say: the listing saw none of its logs.
  defp shape_log_paths!(shape_dir) do
    log_dir = Path.join(shape_dir, @log_dir)

    case ls_dir!(log_dir, [:enoent]) do
      {:ok, [_ | _] = paths} ->
        paths

      listing ->
        with {:ok, _stat} <- File.stat(shape_dir),
             {:ok, stat} <- log_dir_stat(shape_dir, log_dir),
             :ok <- still_unlisted(listing, stat),
             {:ok, _stat} <- File.stat(shape_dir) do
          []
        else
          {:error, reason} ->
            raise File.Error, reason: reason, action: "list directory", path: log_dir
        end
    end
  end

  defp still_unlisted({:error, :enoent}, :none), do: :ok
  defp still_unlisted({:error, :enoent}, _found), do: {:error, :enoent}
  defp still_unlisted({:ok, []}, _found), do: :ok

  # A log directory not created yet is none, where one that is a symlink whose target is gone is,
  # and so is one gone from a shape whose version is written: the writer creates the log directory
  # before it writes the version, and a cleanup moves the shape directory away whole. The shape
  # directory is looked at first and last: a symlinked shape whose target was away for the listing
  # hides its log directory and its version from these looks too, and a cleanup that moves the
  # shape away during them is seen.
  defp log_dir_stat(shape_dir, log_dir) do
    case File.lstat(log_dir) do
      {:ok, %File.Stat{type: :symlink}} -> File.stat(log_dir)
      {:error, :enoent} -> version_stat(shape_dir)
      other -> other
    end
  end

  defp version_stat(shape_dir) do
    version_path = Path.join([shape_dir, @metadata_dir, "version.bin"])

    case File.stat(version_path) do
      {:ok, _stat} -> {:error, :enoent}
      {:error, reason} when reason in [:enoent, :enotdir] -> {:ok, :none}
      {:error, reason} -> raise File.Error, reason: reason, action: "stat", path: version_path
    end
  end

  def drop_all_ets_entries(stack_id) do
    try do
      :ets.delete_all_objects(stack_ets(stack_id))
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  def cleanup!(%__MODULE__{} = shape_opts) do
    stack_storage = Storage.for_stack(shape_opts.stack_id)
    Storage.cleanup!(stack_storage, shape_opts.shape_handle)
  end

  def cleanup!(stack_opts, shape_handle) do
    # This call renames the `shape_data_dir` out of the shape storage path. On
    # linux renames are atomic so there's no need for a marker to catch
    # half-deleted data
    with :ok <-
           Electric.AsyncDeleter.delete(
             stack_opts.stack_id,
             shape_data_dir(stack_opts.base_path, shape_handle),
             stack_opts.base_path
           ) do
      :ets.delete(stack_ets(stack_opts.stack_id), shape_handle)
      :ok
    end
  end

  # Each directory is its own root: one on another filesystem than the stack's trash, a volume
  # mounted at it say, gives its entries to a trash inside it.
  def cleanup_all!(%{stack_id: stack_id, base_path: base_path, tmp_dir: tmp_dir}) do
    with :ok <- Electric.AsyncDeleter.delete(stack_id, base_path, base_path),
         :ok <- Electric.AsyncDeleter.delete(stack_id, tmp_dir, tmp_dir) do
      drop_all_ets_entries(stack_id)
    end
  end

  def schedule_compaction(compaction_config) do
    half_period = div(compaction_config.period, 2)

    # Schedule with jitter to avoid all compactions happening at the same time
    Process.send_after(
      self(),
      {Storage, {__MODULE__, :scheduled_compaction, [compaction_config]}},
      compaction_config.period + Enum.random(-half_period..half_period)
    )
  end

  def scheduled_compaction(writer_state() = state, compaction_config) do
    schedule_compaction(compaction_config)
    compact(state, compaction_config.keep_complete_chunks)
  end

  # The writer compacts the generation it appends to. The cache can name the one before: a read
  # that filled it from the disk across a switch, once the table holding it was recreated, inserts
  # the name the switch replaced, and the switch's cache update found no entry. A run from that
  # generation cut at positions of its log, and its switch removed the one the disk names.
  def compact(writer_state(opts: opts, latest_name: latest_name) = state, keep_complete_chunks) do
    compact(opts, keep_complete_chunks, latest_name)
    state
  end

  def compact(%__MODULE__{} = opts, keep_complete_chunks),
    do: compact(opts, keep_complete_chunks, latest_name(opts))

  defp compact(%__MODULE__{} = opts, keep_complete_chunks, source)
       when is_integer(keep_complete_chunks) and keep_complete_chunks >= 0 do
    # Keep the last 2 chunks as-is so that anything that relies on the live stream and
    # transactional information/LSNs always has something to work with.
    case compaction_cut(opts, keep_complete_chunks, source) do
      {end_offset, file_pos} ->
        if disk_names?(opts, source),
          do: prepare_compaction(opts, end_offset, file_pos, source),
          else: :ok

      # Not enough chunks to warrant compaction
      nil ->
        :ok
    end
  end

  # A run starts only from the generation the disk names: one from another, or under a name the
  # disk cannot give, is discarded at its end (`handle_compaction_finished/5`). Such a name can
  # last, and each period then scanned, merged and synced the whole log to throw the output away.
  defp disk_names?(%__MODULE__{} = opts, source) do
    case latest_name_on_disk(opts) do
      {:ok, ^source} ->
        true

      on_disk ->
        Logger.warning(
          "Skipping a compaction of shape #{opts.shape_handle} from #{source}: " <>
            disk_name_clause(on_disk)
        )

        false
    end
  end

  # The writer closes a chunk at a write position, so a complete chunk can hold bytes not yet
  # synced, or a transaction not yet committed. Compacted, they would be served whole below the
  # boundary, and recovery trims only the latest generation. The cut is the newest complete chunk
  # past the kept ones that ends at or below the durable claim: taking only the one chunk the
  # count names skipped the run whenever that chunk was past the claim, and a shape whose runs
  # always found an open transaction there was never compacted.
  #
  # The cut also ends past the compaction boundary. A latest generation holding a chunk at or below
  # it is a switch left unfinished, and a cut there published a boundary below rows the compacted
  # generation already held, which the next reopen refused.
  #
  # The claim and the boundary are read once a chunk is past the kept ones, and a read of them
  # that fails, with no file descriptor left say, skips the run: it raised out of the consumer,
  # which removed the shape, on a log too short to compact as well. The index's size alone tells
  # whether a chunk is past them, before the index is opened, and an open or a read of it that
  # fails skips the run too. A claim whose bytes do not decode skips it as well, as the writer
  # writes the claim again at the next flush that moves it; a boundary is written only at a
  # switch, and one that does not decode discards the shape (`read_compaction_boundary/1`).
  defp compaction_cut(%__MODULE__{} = opts, keep_complete_chunks, source) do
    with [_ | _] = candidates <- compaction_candidates(opts, keep_complete_chunks, source),
         {:ok, claim, boundary} <- compaction_bounds(opts) do
      Enum.find_value(candidates, fn {{_, end_offset}, {_, end_pos}, _} ->
        if is_log_offset_lte(end_offset, claim) and is_log_offset_lt(boundary, end_offset),
          do: {end_offset, end_pos}
      end)
    else
      _ -> nil
    end
  end

  # The complete chunks past the kept ones, newest first.
  defp compaction_candidates(%__MODULE__{} = opts, keep_complete_chunks, source) do
    path = chunk_file(opts, source)

    case ChunkIndex.complete_chunk_count(path) do
      {:ok, count} when count > keep_complete_chunks ->
        path
        |> ChunkIndex.read_chunk_file()
        |> Enum.reject(&match?({{_, nil}, _, _}, &1))
        |> Enum.drop(-keep_complete_chunks)
        |> Enum.reverse()

      _ ->
        []
    end
  rescue
    error in [File.Error, CaseClauseError] ->
      Logger.warning(
        "Skipping a compaction of shape #{opts.shape_handle}: its chunk index cannot be read: " <>
          Exception.message(error)
      )

      []
  end

  defp compaction_bounds(%__MODULE__{} = opts) do
    case read_compaction_boundary(opts) do
      {:ok, {boundary, _}} ->
        {:ok, durable_claim(opts), boundary}

      {:error, reason} ->
        skip_unreadable_bounds(opts, inspect(reason))
    end
  rescue
    error in [CaseClauseError, ArgumentError] ->
      skip_unreadable_bounds(opts, Exception.message(error))
  end

  defp skip_unreadable_bounds(%__MODULE__{} = opts, message) do
    Logger.warning(
      "Skipping a compaction of shape #{opts.shape_handle}: its durable claim or its " <>
        "compaction boundary cannot be read: " <> message
    )

    :error
  end

  defp durable_claim(%__MODULE__{} = opts),
    do: read_metadata!(opts, :last_persisted_txn_offset) || LogOffset.last_before_real_offsets()

  defp prepare_compaction(%__MODULE__{} = opts, end_offset, file_pos, source) do
    # Just-in-case file-existence-based lock
    if !read_cached_metadata(opts, :compaction_started?) do
      write_cached_metadata!(opts, :compaction_started?, true)

      Task.Supervisor.start_child(
        stack_task_supervisor(opts.stack_id),
        __MODULE__,
        :make_compacted_files,
        [self(), opts, end_offset, file_pos, source]
      )

      :ok
    else
      :already_in_progress
    end
  rescue
    # A latch that cannot be taken on disk, with no file descriptor left say, raised out of the
    # consumer, which removed the shape: the run does not start, and the next one tries again.
    error in [File.Error, Storage.Error] ->
      Logger.warning(
        "Skipping a compaction of shape #{opts.shape_handle}: its latch cannot be taken on " <>
          "disk: " <> Exception.message(error)
      )

      :ok
  end

  def make_compacted_files(parent, %__MODULE__{} = opts, offset, log_file_pos, source)
      when is_pid(parent) do
    # Every intermediate file below was named after the stack alone, so two shapes compacting at
    # the same time -- which a per-shape schedule spread over one jittered period makes ordinary,
    # not rare -- read and wrote each other's key index, action file and merged log, and each then
    # published the result as its own compacted generation. `File.touch!` compounded it: it does
    # not truncate, so a leftover from a crashed run was adopted as this shape's previous
    # generation. A directory per run keeps concurrent compactions apart, including two runs of one
    # shape, and turns the cleanup into a single removal that also runs when the compaction raises.
    run_dir = Path.join([tmp_dir(opts), opts.shape_handle, Utils.uuid4()])

    try do
      compact_into(parent, opts, offset, log_file_pos, source, run_dir)
    catch
      kind, reason ->
        # Nothing monitors this task, and the latch that keeps one compaction per shape is only
        # released by the completion message. Failing silently therefore did not cost a
        # compaction, it cost every future one: the latch is on disk, so the shape stayed shut
        # after a restart too, and its log grew without bound for as long as it existed.
        send(parent, {Storage, {__MODULE__, :handle_compaction_failed, []}})
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      rm_rf!(run_dir)
    end
  end

  defp compact_into(parent, %__MODULE__{} = opts, offset, log_file_pos, source, run_dir) do
    mkdir_p!(run_dir)

    current_suffix = source
    {_, compacted_suffix} = compaction_boundary(opts)

    # We're copying parts of the file & keyfile to the tmp dir, because we expect tmp dir to be on a faster FS
    if compacted_suffix do
      File.copy!(key_file(opts, compacted_suffix), Path.join(run_dir, "compacted.keyfile"))
    else
      File.touch!(Path.join(run_dir, "compacted.keyfile"))
    end

    # Every read below (key index, sequential merge reads, pread of JSONs by position) must
    # see self-contained entries, which arena-ref entries are not. When the stack has an
    # arena, compact from an inlined copy of the segment; the original file stays untouched
    # for the post-compaction tail copy.
    {latest_part_log, latest_part_end} =
      maybe_inline_arena_refs(opts, current_suffix, log_file_pos, run_dir)

    KeyIndex.create_from_log(
      latest_part_log,
      Path.join(run_dir, "latest_part.keyfile"),
      latest_part_end
    )

    KeyIndex.sort(
      [Path.join(run_dir, "latest_part.keyfile"), Path.join(run_dir, "compacted.keyfile")],
      Path.join(run_dir, "merged.keyfile")
    )

    action_file =
      ActionFile.create_from_key_index(
        Path.join(run_dir, "merged.keyfile"),
        Path.join(run_dir, "merged.actionfile")
      )

    files =
      if compacted_suffix,
        do: %{0 => latest_part_log, 1 => json_file(opts, compacted_suffix)},
        else: %{0 => latest_part_log}

    {log_file, chunk_file, key_file} =
      LogFile.merge_with_actions(
        action_file,
        files,
        Path.join(run_dir, "merged.logfile"),
        opts.chunk_bytes_threshold
      )

    # A compaction task outlives a writer killed under it, and the writer that reopens clears the
    # latch and can compact from the same generation. With the clock behind the current suffix,
    # both runs took the same next one, and the second to copy rewrote files the first may already
    # have published. The suffix therefore also clears every generation on disk, and each output is
    # created, never replaced: a run that finds its suffix taken fails instead.
    new_compacted_suffix =
      next_suffix("compacted", [compacted_suffix | generations_on_disk(opts, "compacted")])

    # Tmp dir is likely to be on the different FS, so we need to in-process copy instead of rename
    #    side-note, Erlang doesn't expose kernel-level copy :(
    #    see: https://github.com/erlang/otp/issues/7273
    copy_to_new_file!(log_file, json_file(opts, new_compacted_suffix))
    copy_to_new_file!(chunk_file, chunk_file(opts, new_compacted_suffix))
    copy_to_new_file!(key_file, key_file(opts, new_compacted_suffix))

    send(
      parent,
      {Storage,
       {__MODULE__, :handle_compaction_finished,
        [offset, new_compacted_suffix, log_file_pos, source]}}
    )
  end

  defp generations_on_disk(%__MODULE__{} = opts, prefix) do
    for path <- opts |> shape_log_dir() |> Path.join("log.#{prefix}.*.*.bin") |> Path.wildcard(),
        position = generation_position(prefix, path),
        is_integer(position),
        uniq: true,
        do: "#{prefix}.#{position}"
  end

  # Synced here, off the writer, because the boundary that names the file is durable
  # (`handle_compaction_finished/5`).
  defp copy_to_new_file!(source, destination) do
    File.open!(destination, [:write, :raw, :exclusive], fn output ->
      File.open!(source, [:read, :raw], fn input -> {:ok, _} = :file.copy(input, output) end)
    end)

    Utils.datasync_file!(destination)
  end

  # Trims the log to how far it can be trusted after a restart and returns that boundary.
  # `last_persisted_txn_offset` is bytes on the platter and `last_written_txn_offset` bytes in the
  # file, which survive everything except the machine, so recovery keeps the written ones when the
  # machine has not restarted since and the log still reaches them. Both keys are read from disk: a
  # live writer keeps its written boundary in the cache under the persisted key, so the cache cannot
  # tell the two apart.
  #
  # The writer takes the boundary it starts from as synced and reports a replayed transaction under
  # it flushed at once, which moves the slot ack and releases the journal copy. The written bytes
  # are therefore datasynced, and the durable claim moved to them, before they are handed on. A
  # tail no record of the current boot vouches for (`read_boot_record/1`), or a log that stops
  # short of the marker, is one the machine may have lost, and recovery falls back to the durable
  # boundary, past which the source still holds every transaction.
  defp recover_log!(%__MODULE__{} = opts, suffix, boot_record) do
    persisted =
      read_metadata!(opts, :last_persisted_txn_offset) || LogOffset.last_before_real_offsets()

    written = if vouches_for_marker?(opts, boot_record), do: read_written_boundary(opts)

    cond do
      not is_nil(written) and is_log_offset_lt(persisted, written) and
          trimmed_to_written?(opts, written, suffix) ->
        File.open!(json_file(opts, suffix), [:read, :raw], &(:ok = :file.datasync(&1)))
        make_generation_durable!(opts, suffix)
        write_metadata!(opts, :last_persisted_txn_offset, written, true)
        {written, suffix}

      is_real_offset(persisted) and covers_latest?(opts, persisted) ->
        # The init refuses a boundary past the claim; nothing is published before it does.
        refuse_boundary_past_claim!(opts, compaction_boundary(opts), persisted)
        suffix = start_empty_latest!(opts, suffix)
        Utils.sync_dir!(shape_metadata_dir(opts))
        {persisted, suffix}

      true ->
        trim_log!(opts, persisted, suffix)

        # A writer killed after renaming its durable claim but before syncing the metadata directory
        # leaves a claim this read sees and a machine crash can still take back. The writer about to
        # open takes the boundary as synced, so the rename has to be durable before it does.
        if is_real_offset(persisted) do
          make_generation_durable!(opts, suffix)
          Utils.sync_dir!(shape_metadata_dir(opts))
        end

        {persisted, suffix}
    end
  end

  # The guard of the covered clause of `finish_interrupted_switch!/4`.
  defp covers_latest?(%__MODULE__{} = opts, claim) do
    case compaction_boundary(opts) do
      {boundary, compacted} when not is_nil(compacted) -> is_log_offset_lte(claim, boundary)
      _ -> false
    end
  end

  # A claim the compacted generation covers leaves nothing the shape needs in the latest
  # generation, so the reopen starts an empty one past it and leaves its files as they are, for a
  # read already under way in them: trimmed in place, their index or log cut the entries that read
  # had found, which then failed or took the shape for lost. Nothing here reads or syncs them, so a
  # read or a sync that fails on them costs nothing; the next publish or compaction sweeps them
  # (`publish_empty_latest!/2`). The compacted generation and its name are durable before the new
  # name drops the copy of their entries the latest generation held.
  defp start_empty_latest!(%__MODULE__{} = opts, suffix) do
    suffix =
      if empty_generation?(opts, suffix) do
        suffix
      else
        boundary = compaction_boundary(opts)

        # The latest name too: a build that renamed it into place with no sync left its content to
        # the page cache, and the directory sync below made the name durable ahead of it, so a
        # machine crash before the new name failed the next reopen on a name it could not decode.
        opts
        |> compacted_generation_files!(boundary)
        |> Enum.concat([
          shape_metadata_path(opts, "compaction_boundary.bin"),
          shape_metadata_path(opts, "latest_name.bin")
        ])
        |> Enum.each(&Utils.datasync_file!/1)

        sync_log_directories!(opts)
        # A compaction writer killed before it synced the metadata directory left the boundary's
        # rename to it. The new name is renamed in that directory too, and a machine crash can keep
        # that rename and take the earlier one back: an empty latest generation under no boundary
        # then failed the reopen of a shape the compacted generation holds.
        Utils.sync_dir!(shape_metadata_dir(opts))
        publish_empty_latest!(opts, suffix)
      end

    make_generation_durable!(opts, suffix)
    suffix
  end

  # Before a reopen hands on a claim, the files of the generation the metadata names,
  # `{latest_name, compacted suffix}`, and every directory entry down to them have to be durable,
  # and so before the metadata directory sync makes those names durable. Neither is a given: the
  # entries are synced at a shape's first claim, which a claim written by a build without that
  # sync never went through, and a compaction renames both names with its files still unsynced.
  # The two names are files too, renamed into place with no sync, so their content is synced with
  # the generation's: a name whose entry is durable and whose content is not fails the next reopen.
  # The generation is recorded after the syncs and with no sync of its own: a later reopen of the
  # same generation skips them, and a record a crash loses costs one more pass, never a skipped one.
  defp make_generation_durable!(%__MODULE__{} = opts, suffix) do
    {_, compacted_suffix} = boundary = compaction_boundary(opts)
    generation = {suffix, compacted_suffix}

    if read_durable_generation(opts) != generation do
      opts
      |> generation_files!(suffix, boundary)
      |> Enum.each(&Utils.datasync_file!/1)

      sync_log_directories!(opts)
      record_durable_generation(opts, generation)
    end

    :ok
  end

  # The record only spares a later reopen the syncs above, so failing to write it, on a full disk
  # say, costs that pass again and never the shape. A failed sync is different: it fails the
  # reopen, since a retry can report success for bytes the failure already dropped.
  defp record_durable_generation(%__MODULE__{} = opts, generation) do
    write_metadata!(opts, :durable_generation, generation)
  rescue
    error in [File.Error, Storage.Error] ->
      Logger.warning(
        "Could not record the durable generation of shape #{opts.shape_handle}: " <>
          Exception.message(error)
      )
  end

  # A compaction publishes its three files before it names them, so a compacted name with a file
  # missing is a generation the machine lost. Reads below the boundary go to that generation, so
  # recording it as durable would serve them empty for good. A latest generation has no key index,
  # and a missing chunk index there is for the trim to judge.
  defp generation_files!(%__MODULE__{} = opts, suffix, {_, compacted_suffix} = boundary) do
    latest =
      Enum.filter(
        [json_file(opts, suffix), chunk_file(opts, suffix), key_file(opts, suffix)],
        &present?/1
      )

    compacted =
      if is_nil(compacted_suffix),
        do: [],
        else: compacted_generation_files!(opts, boundary)

    names =
      Enum.filter(
        [
          shape_metadata_path(opts, "latest_name.bin"),
          shape_metadata_path(opts, "compaction_boundary.bin")
        ],
        &present?/1
      )

    latest ++ compacted ++ names
  end

  # A compaction writes its three files whole, and keeps the entry at the end of each key's run,
  # the one at its cut included, so its chunk index ends at its boundary, on a complete chunk,
  # where its log and its key index end. A build that named them with their files unsynced could
  # leave, after a machine crash, the names over files cut short or emptied: present, synced and
  # recorded as durable, they served the reads below the boundary short or empty.
  defp compacted_generation_files!(%__MODULE__{} = opts, {boundary, suffix}) do
    [log, chunks, keys] =
      files =
      Enum.map(
        [json_file(opts, suffix), chunk_file(opts, suffix), key_file(opts, suffix)],
        &present!(opts, &1)
      )

    if ChunkIndex.finished_log_ends(chunks) ==
         {boundary, FileInfo.get_file_size!(log), FileInfo.get_file_size!(keys)} do
      files
    else
      raise Storage.Error,
        kind: :data_loss,
        message:
          "Compacted log #{suffix} of shape #{opts.shape_handle} does not end where its index " <>
            "and its boundary #{inspect(boundary)} do; the storage has lost durable data"
    end
  end

  defp present?(path) do
    case File.stat(path) do
      {:ok, _} -> true
      {:error, :enoent} -> false
      {:error, reason} -> raise File.Error, reason: reason, action: "stat", path: path
    end
  end

  defp present!(%__MODULE__{} = opts, path) do
    if present?(path) do
      path
    else
      raise Storage.Error,
        kind: :data_loss,
        message:
          "Compacted log file #{Path.basename(path)} of shape #{opts.shape_handle} is missing; " <>
            "the storage has lost durable data"
    end
  end

  # Written with no sync, so a machine crash can keep its name and lose its content, which then
  # names no generation. One that cannot be opened or read names none either: the record only
  # spares the pass, and the pass fails the reopen on its own when it cannot sync the files.
  defp read_durable_generation(%__MODULE__{} = opts) do
    case File.read(shape_metadata_path(opts, "durable_generation.bin")) do
      {:ok, bin} -> :erlang.binary_to_term(bin)
      {:error, _reason} -> nil
    end
  rescue
    ArgumentError -> nil
  end

  # The written marker is renamed into place with no sync, so a machine crash can keep its name
  # and lose its content. It is a hint the log has to confirm, never a claim: content that does
  # not decode is no hint, and recovery falls back to the durable boundary. The durable marker is
  # synced before its rename, so it stays strict and an unreadable one fails the reopen.
  defp read_written_boundary(%__MODULE__{} = opts) do
    case read_metadata!(opts, :last_written_txn_offset) do
      %LogOffset{} = written -> written
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  # Nor can the log confirm bytes whose length a machine crash kept: a block lost below the file's
  # end reads back as zeros, with the framing around it intact, and the log carries no checksum to
  # tell. So the marker is a hint only in the boot whose page cache still holds the bytes past the
  # durable claim. Each writer records its boot before it appends (`record_boot!/3`), with the
  # bytes of the marker it found then, so a record naming the current boot vouches for the markers
  # that boot wrote: any that holds other bytes. A missing or undecodable record, as a build before
  # it leaves, vouches for none. The record sits apart from the marker, which keeps the format a
  # rollback reads, in a directory of its own (`write_boot_record!/4`).
  #
  # A record that cannot be read vouches for none either, and carries a commit no one knows, which
  # fails a promotion's scan as a torn marker does: failing the reopen instead would remove a shape
  # whose log is intact.
  defp read_boot_record(%__MODULE__{} = opts) do
    read_boot_record_file(shape_boot_dir(opts))
  rescue
    error in File.Error ->
      Logger.warning(
        "Could not read the boot record of shape #{opts.shape_handle}, so it reopens at its " <>
          "durable claim: " <> Exception.message(error)
      )

      :unreadable
  end

  # A record that does not decode is one a crash took, which was written unsynced and so carried no
  # commit past the claim. One that cannot be read may carry one, and fails a promotion's scan.
  defp read_boot_record_file(boot_dir) do
    path = Path.join(boot_dir, "written_boot.bin")

    case File.open(path, [:read, :raw], &IO.binread(&1, :eof)) do
      {:ok, bin} when is_binary(bin) -> decode_boot_record(bin)
      {:ok, :eof} -> nil
      {:ok, {:error, reason}} -> raise File.Error, reason: reason, action: "read file", path: path
      {:error, :enoent} -> nil
      {:error, reason} -> raise File.Error, reason: reason, action: "open file", path: path
    end
  end

  defp decode_boot_record(bin) do
    case :erlang.binary_to_term(bin) do
      {boot, _marker, _high_water} = record when is_binary(boot) -> record
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp vouches_for_marker?(%__MODULE__{} = opts, {boot, found, _high_water}),
    do: boot == boot_id() and found != :unknown and read_marker(opts) not in [found, :unknown]

  defp vouches_for_marker?(%__MODULE__{}, record) when record in [nil, :unreadable], do: false

  # A reopen leaves the marker it finds in place: one a run with the journal off left names commits
  # clients read, which a promotion's scan goes by (`stored_high_water/1`), and a replay from a
  # claim below that commit can replace the file before it reaches the commit again. So the record
  # carries the commit a marker names past the claim the reopen recovered to, in a new boot or in
  # the same one: a marker the record did not vouch for, or one the log no longer reaches.
  # A record is synced before the writer appends when it carries a commit past the claim, and when
  # it replaces another boot's: a volume that moves back to that boot's machine after a crash here
  # could otherwise find the old record beside this boot's marker and the bytes the crash zeroed.
  # One that does neither is written unsynced, since losing it loses nothing. For the same reason a
  # write that fails fails the reopen, and a reopen in the same boot syncs the record's directory:
  # the writer that renamed the record into place may have been killed before its sync.
  defp record_boot!(%__MODULE__{} = opts, boot_record, claim) do
    boot = boot_id()
    marker = read_marker(opts)
    commit = marker_commit(marker)
    uncovered = if not claimed?(commit, claim), do: commit
    carried = carried_high_water(boot_record)
    high_water = later_high_water(carried, uncovered)

    case boot_record do
      {^boot, _found, ^high_water} -> Utils.sync_dir!(shape_boot_dir(opts))
      {^boot, found, _carried} -> write_boot_record!(opts, {boot, found, high_water}, claim, true)
      _ -> write_boot_record!(opts, {boot, marker, high_water}, claim, not is_nil(boot_record))
    end
  end

  # The record has a directory to itself, so syncing it makes no other name durable ahead of its
  # content: not an unsynced written marker's, which a promotion's scan fails on when torn, nor
  # `last_snapshot_chunk`'s, ahead of the snapshot chunks it names. A record that carries a commit
  # past the claim also needs the directory's own entry, which a crash takes with every record in
  # it, so the shape directory is synced first. With no durable claim, so is every directory above
  # it: the first claim syncs them, and until then a crash can take the shape's directory, record
  # and all, while the catalog keeps the shape. A record a reopen in the same boot finds was renamed
  # into place after those syncs. Any other record can lose its entry, as losing the record loses
  # nothing. It is made with `mkdir`, not `mkdir -p`, so a writer whose shape a cleanup removed
  # fails rather than recreating the shape's directory.
  defp write_boot_record!(%__MODULE__{} = opts, {_, _, high_water} = record, claim, replaces?) do
    dir = shape_boot_dir(opts)

    case File.mkdir(dir) do
      :ok -> :ok
      {:error, :eexist} -> :ok
      {:error, reason} -> raise File.Error, reason: reason, action: "make directory", path: dir
    end

    carries? = not claimed?(high_water, claim)

    cond do
      not carries? -> :ok
      is_real_offset(claim) -> Utils.sync_dir!(shape_data_dir(opts))
      true -> sync_log_directories!(opts)
    end

    write_term_file!(dir, "written_boot.bin", record, replaces? or carries?)
  end

  defp shape_boot_dir(opts), do: shape_data_dir(opts, [@boot_dir])

  # The written marker's bytes, which tell the marker a boot found from the files later writes
  # rename over it whatever the file system does with inode numbers: nothing writes a marker in
  # place. A marker this boot writes with the same bytes names a commit the record carries or the
  # claim covers, and the record vouching for one marker fewer costs a replay from the claim. Nil
  # when there is no marker, and `:unknown` when it cannot be read.
  defp read_marker(%__MODULE__{} = opts) do
    case File.read(shape_metadata_path(opts, "last_written_txn_offset.bin")) do
      {:ok, bin} -> bin
      {:error, :enoent} -> nil
      {:error, _} -> :unknown
    end
  end

  # The commit a written marker names: `:unknown` when a crash took its content or it cannot be
  # read, which fails a promotion's scan as such a marker does.
  defp marker_commit(nil), do: nil
  defp marker_commit(:unknown), do: :unknown

  defp marker_commit(bin) do
    case :erlang.binary_to_term(bin) do
      %LogOffset{tx_offset: tx_offset} -> tx_offset
      _ -> :unknown
    end
  rescue
    ArgumentError -> :unknown
  end

  defp carried_high_water({_boot, _marker, high_water}), do: high_water
  defp carried_high_water(:unreadable), do: :unknown
  defp carried_high_water(nil), do: nil

  defp later_high_water(:unknown, _), do: :unknown
  defp later_high_water(_, :unknown), do: :unknown
  defp later_high_water(nil, high_water), do: high_water
  defp later_high_water(high_water, nil), do: high_water
  defp later_high_water(a, b), do: Kernel.max(a, b)

  defp claimed?(nil, _claim), do: true
  defp claimed?(:unknown, _claim), do: false
  defp claimed?(high_water, %LogOffset{tx_offset: tx_offset}), do: high_water <= tx_offset

  # The kernel's boot id, read once, which changes when the machine restarts and with it the page
  # cache. Where the platform gives none, a token of this VM's: a writer restarted in the VM keeps
  # its tail, and a VM restart, which may come with a machine restart, keeps none. Two first calls
  # racing on such a token cost at most a promotion.
  defp boot_id do
    case :persistent_term.get({__MODULE__, :boot_id}, nil) do
      nil ->
        boot = read_boot_id() || "vm-" <> Base.encode16(:crypto.strong_rand_bytes(16))
        :persistent_term.put({__MODULE__, :boot_id}, boot)
        boot

      boot ->
        boot
    end
  end

  defp read_boot_id do
    case :os.type() do
      {:unix, :linux} -> File.read("/proc/sys/kernel/random/boot_id")
      {:unix, :darwin} -> System.cmd("/usr/sbin/sysctl", ["-n", "kern.bootsessionuuid"])
      _ -> nil
    end
    |> case do
      {:ok, boot} -> non_empty(boot)
      {boot, 0} -> non_empty(boot)
      _ -> nil
    end
  rescue
    ErlangError -> nil
  end

  defp non_empty(boot) do
    case String.trim(boot) do
      "" -> nil
      boot -> boot
    end
  end

  defp first_durable_claim?(old_claim, new_claim),
    do: not is_real_offset(old_claim) and is_real_offset(new_claim)

  # A datasync makes the log's bytes durable, not the directory entries that lead to them, and
  # every entry from the storage directory down to the log can be new with the shape, with its
  # stack or with the service, which creates a missing storage directory. A machine crash that
  # keeps the claim and unlinks the log loses transactions the slot has already moved past, so each
  # entry is synced, deepest first, before the first claim names one. The chain ends at the storage
  # directory's parent, which holds its entry; the stack made the entries above it durable when it
  # started (`sync_storage_root!/1`).
  defp sync_log_directories!(%__MODULE__{} = opts) do
    shape_dir = shape_data_dir(opts)
    prefix_dir = Path.dirname(shape_dir)
    outer_prefix_dir = Path.dirname(prefix_dir)
    stack_dir = Path.dirname(outer_prefix_dir)
    storage_dir = Path.dirname(stack_dir)

    Enum.each(
      [shape_log_dir(opts), shape_dir, prefix_dir, outer_prefix_dir, stack_dir, storage_dir],
      &Utils.sync_dir!/1
    )

    sync_storage_parent!(Path.dirname(storage_dir), storage_dir)
  end

  # A host that provisions the storage directory can leave its parent closed to the service, 0711
  # say. The service could not have created the storage directory in a parent it may not write, so
  # the entry naming it is the host's to make durable, and such a parent is left to the host. So is
  # the parent of a storage directory mounted where it is, which names the mount. One the service
  # may write but not open, or a sync that fails once the directory is open, still raises: the
  # entry there may be the service's own.
  defp sync_storage_parent!(dir, storage_dir) do
    Utils.sync_dir!(dir)
  rescue
    error in File.Error ->
      if error.action == "open directory to sync" and error.reason in [:eacces, :eperm] and
           (not writable_dir?(dir) or not same_device?(storage_dir, dir)),
         do: :ok,
         else: reraise(error, __STACKTRACE__)
  end

  defp writable_dir?(dir) do
    case File.stat(dir) do
      {:ok, %File.Stat{access: access}} -> access in [:write, :read_write]
      {:error, _} -> true
    end
  end

  # The storage directory's parent can be new with the service too, and so can every directory
  # above it: `FileInfo.mkdir_p/1` makes each one that is missing, and another part of the service
  # may have made them first, since `ELECTRIC_STORAGE_DIR` holds more than the storage directory.
  # The chain a claim or a snapshot syncs stops at that parent (`sync_log_directories!/1`), so the
  # stack makes the entries above it durable as it starts, before it writes a shape: each one up
  # to the first directory the service could not have made, one in a parent it may not write or
  # at the root of a mount, or the first it cannot open (`synced_above?/1`). A storage directory
  # mounted where it is was there before the service, and so was every directory above it.
  # Nothing the service runs removes them.
  defp sync_storage_root!(base_path) do
    storage_dir = Path.dirname(base_path)
    mkdir_p!(storage_dir)
    storage_parent = Path.dirname(storage_dir)

    if same_device?(storage_dir, storage_parent),
      do: sync_entries_above!(storage_parent),
      else: :ok
  end

  # Each parent is the directory's own `..`, so the walk takes the path `mkdir_p!/1` took: after a
  # symlink, `..` names the parent of the link's target, where a lexical parent (`Path.expand/1`,
  # `Path.dirname/1`) names the link's.
  defp sync_entries_above!(dir) do
    parent = Path.join(dir, "..")

    if not same_directory?(dir, parent) and writable_dir?(parent) and same_device?(dir, parent) and
         synced_above?(parent) do
      sync_entries_above!(parent)
    else
      :ok
    end
  end

  # A directory above the storage directory's parent that the service may write but not open ends
  # the walk, and so does one whose path, a `..` longer at each step, grows too long to open. The
  # service cannot sync it, and the directories there are mostly the host's, made before the
  # service ran: the one holding a mount on the same device, or the working directory's parent
  # when `ELECTRIC_STORAGE_DIR` is relative. Failing there failed every start of the stack, where
  # `main` syncs none of them.
  defp synced_above?(dir) do
    Utils.sync_dir!(dir)
    true
  rescue
    error in File.Error ->
      if error.action == "open directory to sync" and
           error.reason in [:eacces, :eperm, :enametoolong] do
        Logger.info(
          "Not syncing the directories above the storage directory from #{dir}: " <>
            Exception.message(error)
        )

        false
      else
        reraise error, __STACKTRACE__
      end
  end

  # The root is its own `..`.
  defp same_directory?(dir, parent) do
    case {File.stat(dir), File.stat(parent)} do
      {{:ok, %File.Stat{major_device: device, inode: inode}},
       {:ok, %File.Stat{major_device: device, inode: inode}}} ->
        true

      _ ->
        false
    end
  end

  defp same_device?(dir, parent) do
    case {File.stat(dir), File.stat(parent)} do
      {{:ok, %File.Stat{major_device: device}}, {:ok, %File.Stat{major_device: device}}} -> true
      {{:ok, _}, {:ok, _}} -> false
      _ -> true
    end
  end

  # `trim_log!/3` for the written boundary, which is no claim: an index or a log that stops short
  # of it is unsynced bytes a machine crash took, not lost data, so it answers false and leaves
  # the caller to trim to the durable boundary. So is an index that stops short of the log there:
  # a chunk opening whose write failed, or that the crash took, past an index that covers the
  # durable boundary. Promoting the marker first made the coverage check refuse the shape. The
  # marker names the last entry the writer handed the file, so a log without that entry and with
  # one past it is a write that failed under a later one that did not: the marker is no hint then.
  # Neither is a log whose read fails on the way: the trim to the durable boundary reads only up to
  # that boundary, and reports a failure below it, or leaves a latest generation it holds no
  # durable entry of to be emptied (`trim_log!/3`) or replaced (`start_empty_latest!/2`).
  #
  # An open of the log or a read of its index that fails is no proof either. Raised, it failed
  # the reopen before the trim to the durable boundary, which a covering compaction spares any
  # read of the latest generation. The index reads match on what each read returns, so a failed
  # one raises a `MatchError` or a `CaseClauseError` there, and a size read of the log or an open
  # of the index that fails raises a `Storage.Error`.
  defp trimmed_to_written?(%__MODULE__{} = opts, written, suffix) do
    {log_search_start_pos, _} = ChunkIndex.realign_and_trim(chunk_file(opts, suffix), written)

    not lost_chunk_index?(opts, suffix, written) and
      LogFile.trim(json_file(opts, suffix), log_search_start_pos, written, read_errors: :return) ==
        :ok and
      not chunk_index_short_of_log?(opts, suffix)
  rescue
    error in [File.Error, Storage.Error, MatchError, CaseClauseError] ->
      Logger.warning(
        "Could not check the written boundary of shape #{opts.shape_handle}, " <>
          "recovering to its durable boundary: " <> Exception.message(error)
      )

      false
  end

  # The version to record for a shape whose writer is about to open. Arena-active is read from the
  # stack's resolved flag first, then from generations already on disk: the flag is set at the
  # routing process's init, before any shape writer initialises this boot, so the first shape to
  # open after the arena is enabled already records the arena version and `initialise_filesystem!`
  # promotes it -- durable before that writer can append its first ref, which is what a log holding
  # refs must have. The on-disk fallback keeps a stack whose flag has since been turned back off
  # from re-offering a log full of refs to a reader that cannot parse them.
  defp required_version(%__MODULE__{} = opts) do
    if arena_active?(opts), do: @arena_checksum_version, else: opts.version
  end

  defp arena_active?(%__MODULE__{} = opts) do
    Electric.StackConfig.lookup(opts.stack_id, :payload_arena_active?, false) or
      case Electric.StackConfig.lookup(opts.stack_id, Storage) do
        {__MODULE__, %{base_path: base_path}} -> PayloadArena.any_generations?(base_path)
        _ -> false
      end
  end

  defp maybe_inline_arena_refs(%__MODULE__{} = opts, current_suffix, log_file_pos, run_dir) do
    base_path = Storage.opt_for_stack(opts.stack_id, :base_path)

    if PayloadArena.any_generations?(base_path) do
      inlined = Path.join(run_dir, "latest_part.inlined.logfile")

      size =
        LogFile.write_inlined_copy!(
          json_file(opts, current_suffix),
          inlined,
          log_file_pos,
          base_path
        )

      {inlined, size}
    else
      {json_file(opts, current_suffix), log_file_pos}
    end
  end

  @doc false
  def handle_compaction_failed(writer_state(opts: opts) = state) do
    release_compaction_latch(opts)
    state
  end

  # The cache holds the live latch; the copy on disk serves only a cache filled again while this
  # writer lives, and the writer's next start clears it. A write of it that failed, with no file
  # descriptor left say, raised out of the consumer, which removed the shape, after a switch that
  # had succeeded too. The copy is removed then, which takes no descriptor, and the latch released
  # in the cache: a cache filled again from the copy left on disk took the latch back, and every
  # later run found one in progress.
  defp release_compaction_latch(%__MODULE__{} = opts) do
    write_cached_metadata!(opts, :compaction_started?, false)
  rescue
    error in [File.Error, Storage.Error] ->
      Logger.warning(
        "Could not release the compaction latch of shape #{opts.shape_handle} on disk: " <>
          Exception.message(error)
      )

      remove_latch_copy(opts)
      release_cached_latch(opts)
  end

  # When the copy cannot be removed too, a cache filled again takes the latch back, until the
  # writer's next start clears it.
  defp remove_latch_copy(%__MODULE__{} = opts) do
    case File.rm(shape_metadata_path(opts, "compaction_started?.bin")) do
      :ok ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Could not remove the compaction latch of shape #{opts.shape_handle} from disk: " <>
            inspect(reason)
        )
    end
  end

  # As `write_cached_metadata!/3` does, a cache with no entry for the shape is left alone.
  defp release_cached_latch(%__MODULE__{} = opts) do
    write_metadata_cache(opts, :compaction_started?, false)
  rescue
    ArgumentError -> :ok
  end

  # A run compacted from another generation than the one the writer appends to, and the disk
  # names, cut at positions of that other log: the tail copy took them from this one, and its
  # removal of the generations past the run's took the one the disk names. Nothing is named yet,
  # so the run's files go, and the next run compacts from the writer's generation. A read of the
  # disk's name that fails, or finds no name, discards the run the same way: it raised out of the
  # consumer, which removed the shape, where the next run can switch. A run from another
  # generation than the writer's is discarded before the disk is read. So is one whose switch
  # cannot read the compaction boundary it replaces, read before the writer's files close: that
  # read raised before the switch could return a writer or release the latch.
  @doc false
  def handle_compaction_finished(
        writer_state(opts: opts, latest_name: latest_name) = state,
        offset,
        new_suffix,
        log_file_pos,
        source
      ) do
    on_disk = if source == latest_name, do: latest_name_on_disk(opts), else: :not_read

    with {:ok, ^latest_name} <- on_disk,
         {:ok, {_, compacted_suffix}} <- stored_compaction_boundary(opts) do
      switch_to_compacted!(state, offset, new_suffix, log_file_pos, compacted_suffix)
    else
      on_disk -> discard_compaction(state, new_suffix, source, on_disk)
    end
  end

  # The boundary, or why it cannot be read, with no file descriptor left say.
  defp stored_compaction_boundary(%__MODULE__{} = opts) do
    with {:error, reason} <- read_compaction_boundary(opts),
         do: {:error, {:compaction_boundary, inspect(reason)}}
  end

  # The boundary a run reads, or the error that kept its file from being read. Bytes that do not
  # decode to a boundary are lost data, not a failed read: a run that skipped on them skipped every
  # later one, and a switch discarded on them left them in place. The shape is discarded, as a read
  # of its log that meets lost bytes discards it.
  defp read_compaction_boundary(%__MODULE__{} = opts) do
    path = shape_metadata_path(opts, "compaction_boundary.bin")

    case File.open(path, [:read, :raw], &IO.binread(&1, :eof)) do
      {:ok, bytes} when is_binary(bytes) -> {:ok, decode_compaction_boundary!(opts, bytes)}
      {:ok, :eof} -> {:ok, decode_compaction_boundary!(opts, <<>>)}
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, :enoent} -> {:ok, {LogOffset.before_all(), nil}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_compaction_boundary!(%__MODULE__{} = opts, bytes) do
    case decode_term(bytes) do
      {:ok, {%LogOffset{}, suffix} = boundary} when is_binary(suffix) or is_nil(suffix) ->
        boundary

      {:ok, nil} ->
        {LogOffset.before_all(), nil}

      _ ->
        discard_shape_after_data_loss!(
          opts,
          "its compaction boundary does not decode to a boundary (#{byte_size(bytes)} bytes)"
        )
    end
  end

  defp decode_term(bytes) do
    {:ok, :erlang.binary_to_term(bytes)}
  rescue
    ArgumentError -> :error
  end

  # "latest.0" for a store an earlier build wrote with no name. Raises nothing: a record that does
  # not decode, or holds no name, is an error the caller logs.
  defp latest_name_on_disk(%__MODULE__{} = opts) do
    case File.read(shape_metadata_path(opts, "latest_name.bin")) do
      {:ok, bytes} -> decode_latest_name(bytes)
      {:error, :enoent} -> {:ok, "latest.0"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_latest_name(bytes) do
    case :erlang.binary_to_term(bytes) do
      name when is_binary(name) -> {:ok, name}
      other -> {:error, {:not_a_name, other}}
    end
  rescue
    ArgumentError -> {:error, :undecodable}
  end

  defp disk_name_clause({:ok, name}), do: "the disk names #{name}"

  defp disk_name_clause({:error, reason}),
    do: "the disk's name cannot be read (#{inspect(reason)})"

  defp discard_compaction(
         writer_state(opts: opts, latest_name: latest_name) = state,
         suffix,
         source,
         on_disk
       ) do
    disk =
      case on_disk do
        :not_read ->
          ""

        {:error, {:compaction_boundary, message}} ->
          ", its compaction boundary cannot be read: " <> message

        on_disk ->
          ", " <> disk_name_clause(on_disk)
      end

    Logger.warning(
      "Discarding a compaction of shape #{opts.shape_handle} from #{source}: " <>
        "its writer appends to #{latest_name}" <> disk
    )

    for path <- [json_file(opts, suffix), chunk_file(opts, suffix), key_file(opts, suffix)],
        do: remove_generation_file(opts, path)

    handle_compaction_failed(state)
  end

  defp switch_to_compacted!(
         writer_state(opts: opts, latest_name: current_latest_suffix) = state,
         offset,
         new_suffix,
         log_file_pos,
         current_compacted_suffix
       ) do
    # This work is being done while the writer is stopped, so that the copy & trim doesn't miss anything,
    # but work here should be fast because writer is blocked.
    state = close_all_files(state)
    writer_state(writer_acc: writer_acc) = state

    # Each name is written durably, and only once both generations are: the next durable claim
    # syncs the metadata directory, which made a name durable ahead of its unsynced files, and a
    # machine crash then kept a claim on a generation it had lost. A switch that fails before the
    # names leaves both on the generations they named, and the error stops the writer.
    new_latest_suffix =
      copy_tail_to_new_latest!(opts, current_latest_suffix, offset, log_file_pos)

    set_compaction_boundary(opts, {offset, new_suffix})

    writer_acc = WriteLoop.adjust_write_positions(writer_acc, -log_file_pos)
    set_latest_name(opts, new_latest_suffix, WriteLoop.cached_chunk_boundaries(writer_acc))
    release_compaction_latch(opts)

    Task.Supervisor.start_child(stack_task_supervisor(opts.stack_id), fn ->
      drop_generations_older_than_previous(opts, [
        {"latest", current_latest_suffix},
        {"compacted", current_compacted_suffix}
      ])
    end)

    writer_state(state, latest_name: new_latest_suffix, writer_acc: writer_acc)
  end

  # Copies the log past a compaction's cut, at `log_file_pos`, and the chunks past `offset`, to a
  # new latest generation, and makes it durable with its entry and the compacted generation's,
  # whose files the task synced. The copy's result was ignored: a disk that filled up mid-copy
  # left the new log short, its index whole, and every read past the cut discarded the shape. The
  # new suffix clears every generation on disk, so a copy never truncates another's file.
  defp copy_tail_to_new_latest!(%__MODULE__{} = opts, current_suffix, offset, log_file_pos) do
    on_disk = generations_on_disk(opts, "latest")
    remove_unnamed_latest(opts, current_suffix, on_disk)
    new_suffix = next_suffix("latest", [current_suffix | on_disk])

    source = json_file(opts, current_suffix)
    destination = json_file(opts, new_suffix)
    tail_size = FileInfo.get_file_size!(source) - log_file_pos

    File.open!(source, [:read, :raw], fn input ->
      {:ok, ^log_file_pos} = :file.position(input, log_file_pos)

      File.open!(destination, [:write, :raw], fn output ->
        case :file.copy(input, output) do
          {:ok, ^tail_size} ->
            :ok

          {:ok, copied} ->
            raise Storage.Error,
              message: "copied #{copied} of #{tail_size} bytes from #{source} to #{destination}"

          {:error, reason} ->
            raise File.Error, reason: reason, action: "copy the log tail to", path: destination
        end
      end)
    end)

    ChunkIndex.copy_adjusting_positions(
      chunk_file(opts, current_suffix),
      chunk_file(opts, new_suffix),
      offset,
      -log_file_pos,
      0
    )

    Utils.datasync_file!(destination)
    Utils.datasync_file!(chunk_file(opts, new_suffix))
    Utils.sync_dir!(shape_log_dir(opts))

    new_suffix
  end

  # A read holds the file names it will open from the moment it reads the metadata, and only opens them
  # when the response is produced -- so deleting the generation a compaction supersedes pulls the file out
  # from under every request already in flight, and an `:enoent` on a shape whose directory still exists
  # raises rather than being tolerated. Keeping the previous generation until the next compaction gives
  # those reads a whole compaction period to finish, and sweeping by suffix rather than deleting two known
  # names also reclaims whatever a restart between the swap and the cleanup left behind.
  #
  # The sweep goes by the generation published before this one, not by a count: a run whose writer
  # was killed under it never publishes, takes a suffix past every generation on disk, and can
  # finish after the run that does, so the newest suffixes on disk need not be the published ones.
  # Such files are swept once a later publish puts them below its previous generation.
  defp drop_generations_older_than_previous(%__MODULE__{} = opts, previous) do
    for {prefix, previous_suffix} <- previous,
        floor = suffix_position(prefix, previous_suffix),
        is_integer(floor),
        path <- opts |> shape_log_dir() |> Path.join("log.#{prefix}.*.*.bin") |> Path.wildcard(),
        position = generation_position(prefix, path),
        is_integer(position) and position < floor,
        do: remove_generation_file(opts, path)
  end

  # Removing a generation only frees space, and a file that cannot go costs only its own: one that
  # raised ended the whole removal, and every file after it stayed, at each retry too.
  defp remove_generation_file(%__MODULE__{} = opts, path) do
    rm_rf!(path)
    :ok
  rescue
    error in File.Error ->
      Logger.warning(
        "Could not remove #{Path.basename(path)} of shape #{opts.shape_handle}: " <>
          Exception.message(error)
      )
  end

  defp generation_position(prefix, path) do
    with ["log", ^prefix, position | _rest] <- path |> Path.basename() |> String.split("."),
         {value, ""} <- Integer.parse(position) do
      value
    else
      _ -> nil
    end
  end

  def get_total_disk_usage(%{base_path: base_path}),
    do: FileInfo.recursive_disk_usage(base_path)

  def set_pg_snapshot(pg_snapshot, %__MODULE__{} = opts),
    do: write_cached_metadata!(opts, :pg_snapshot, pg_snapshot)

  def mark_snapshot_as_started(%__MODULE__{} = opts),
    do: write_cached_metadata!(opts, :snapshot_started?, true)

  def snapshot_started?(%__MODULE__{} = opts),
    do: read_cached_metadata(opts, :snapshot_started?) || false

  def compaction_boundary(%__MODULE__{} = opts),
    do: read_metadata!(opts, :compaction_boundary) || {LogOffset.before_all(), nil}

  def set_compaction_boundary(%__MODULE__{stack_ets: stack_ets} = opts, boundary) do
    :ets.update_element(
      stack_ets,
      opts.shape_handle,
      {storage_meta(:compaction_boundary) + 1, boundary}
    )

    write_metadata!(opts, :compaction_boundary, boundary, true)
  end

  def latest_name(%__MODULE__{} = opts),
    do: read_cached_metadata(opts, :latest_name) || "latest.0"

  # The name the readers of this stack hold, read without filling the cache from disk.
  defp cached_latest_name(%__MODULE__{} = opts),
    do:
      :ets.lookup_element(opts.stack_ets, opts.shape_handle, storage_meta(:latest_name) + 1, nil)

  # The cached chunk positions are relative to the file the name points at, so readers get the
  # two in one update: a reader that paired the new name with the old file's positions read past
  # the end of the new file and discarded the shape as having lost data. The cache follows the
  # disk, and until it does readers stay on the old generation, which the sweep keeps.
  def set_latest_name(
        %__MODULE__{stack_ets: stack_ets} = opts,
        name,
        cached_chunk_boundaries
      ) do
    write_metadata!(opts, :latest_name, name, true)

    :ets.update_element(stack_ets, opts.shape_handle, [
      {storage_meta(:latest_name) + 1, name},
      {storage_meta(:cached_chunk_boundaries) + 1, cached_chunk_boundaries}
    ])

    :ok
  end

  # The suffix a new file replaces is deleted moments later, so a repeat is destructive: a repeated
  # `latest.` makes the copy destination alias its own source and open-for-write truncates it; a
  # repeated `compacted.` names the file the cleanup task is about to remove. A wall clock repeats
  # (two compactions in one millisecond) and runs backwards (NTP), and neither failure reports an
  # error. The value only has to be distinct and increasing; it is never read as a timestamp.
  defp next_suffix(prefix, in_play) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

    ceiling =
      in_play
      |> Enum.map(&suffix_position(prefix, &1))
      |> Enum.reject(&is_nil/1)
      |> Enum.max(&>=/2, fn -> nil end)

    "#{prefix}.#{if ceiling && ceiling >= now, do: ceiling + 1, else: now}"
  end

  defp suffix_position(prefix, suffix) when is_binary(suffix) do
    with [^prefix, position] <- String.split(suffix, ".", parts: 2),
         {value, ""} <- Integer.parse(position) do
      value
    else
      _ -> nil
    end
  end

  defp suffix_position(_prefix, _suffix), do: nil

  def fetch_latest_offset(%__MODULE__{} = opts) do
    {:ok, read_latest_offset(opts)}
  end

  def fetch_pg_snapshot(%__MODULE__{} = opts) do
    {:ok, read_cached_metadata(opts, :pg_snapshot)}
  end

  defp read_latest_offset(%__MODULE__{} = opts) do
    read_multiple_cached_metadata(opts, [
      :last_seen_txn_offset,
      :last_persisted_txn_offset,
      :last_snapshot_chunk
    ])
    |> latest_offset()
  end

  defp latest_offset(metadata) do
    latest_offset =
      Keyword.get(metadata, :last_seen_txn_offset) ||
        Keyword.get(metadata, :last_persisted_txn_offset)

    if is_nil(latest_offset) or LogOffset.is_virtual_offset(latest_offset),
      do: Keyword.get(metadata, :last_snapshot_chunk) || LogOffset.last_before_real_offsets(),
      else: latest_offset
  end

  def start_link(_), do: :ignore

  def init_writer!(shape_opts, shape_definition),
    do: init_writer!(shape_opts, shape_definition, :create)

  def init_restored_writer!(shape_opts, shape_definition),
    do: init_writer!(shape_opts, shape_definition, :restore)

  defp init_writer!(shape_opts, shape_definition, action) do
    table = :ets.new(:in_memory_storage, [:ordered_set, :protected])

    {initial_acc, suffix} = initialise_filesystem!(shape_opts, action)

    register_with_stack(
      shape_opts,
      table,
      WriteLoop.last_persisted_txn_offset(initial_acc),
      compaction_boundary(shape_opts),
      suffix,
      WriteLoop.cached_chunk_boundaries(initial_acc)
    )

    # The latch means "a compaction task is running". A task can outlive the writer that started
    # it, but its completion message is addressed to a pid this instance no longer has, so it can
    # never release the latch itself, and it never publishes. A run started here does not share
    # its files either: it takes a suffix past them, or one of the two fails (`compact_into/6`).
    # Left set -- by a kill during a restart as much as by a crash -- it silences compaction for
    # this shape for the rest of its life.
    write_cached_metadata!(shape_opts, :compaction_started?, false)

    if shape_definition.storage.compaction == :enabled do
      shape_opts.stack_id
      |> Storage.opt_for_stack(:compaction_config)
      |> schedule_compaction()
    end

    writer_state(
      writer_acc: initial_acc,
      latest_name: suffix,
      opts: shape_opts,
      ets: table
    )
  end

  def hibernate(writer_state() = state) do
    close_all_files(state)
  end

  def terminate(writer_state(ets: ets_table, opts: opts) = state) do
    close_all_files(state)
    clean_shape_ets_entry(opts)
    try(do: :ets.delete(ets_table), rescue: (_ -> true))
  end

  # remove cached values not needed for the read path
  defp clean_shape_ets_entry(%__MODULE__{} = opts) do
    write_metadata_cache(
      opts,
      for(k <- @clean_cache_keys, do: {k, storage_meta_unset(k)})
    )
  end

  # The flush leaves nothing for a pending timer, which still carried the flush count it was set
  # at: once the count had moved on, the timer did nothing when it fired and stayed set, and an
  # append that followed scheduled no flush of its own, leaving its bytes buffered and unsynced.
  defp close_all_files(writer_state(writer_acc: acc, write_timer: timer) = state) do
    if not is_nil(timer), do: Process.cancel_timer(timer)
    writer_state(state, writer_acc: WriteLoop.flush_and_close_all(acc, state), write_timer: nil)
  end

  # A restored shape had its storage created with it, so a restore that finds none creates none:
  # a removal that took the shape while its consumer was starting removed the files, and a
  # writer opened on the empty path would take a second snapshot under the retired handle.
  def initialise_filesystem!(%__MODULE__{} = opts, action \\ :create) do
    version = required_version(opts)
    on_disk_version = read_version(opts)
    new? = on_disk_version == :missing and not holds_storage?(opts) and not cached_snapshot?(opts)

    if new? and action == :restore, do: refuse_rebuild_under_handle!(opts, on_disk_version)

    initialize? =
      if not new? and
           (on_disk_version not in @readable_versions or not snapshot_complete?(opts) or
              not snapshot_started?(opts) or is_nil(read_metadata!(opts, :pg_snapshot))) do
        refuse_rebuild_under_handle!(opts, on_disk_version)
      else
        new?
      end

    if initialize? do
      create_directories!(opts)
    end

    # The name on disk, not the cached one: a writer killed in a compaction's switch leaves the
    # two apart, and a writer reopened in the same stack would append to a generation a restart
    # does not read. A new shape's name is durable: the version write and the first claim sync the
    # metadata directory, which would keep the name with content a crash took.
    suffix =
      read_metadata!(opts, :latest_name) || write_metadata!(opts, :latest_name, "latest.0", true)

    boot_record = if not initialize?, do: read_boot_record(opts)

    {last_persisted_txn_offset, suffix, json_file_size, chunks} =
      if initialize? do
        {LogOffset.last_before_real_offsets(), suffix, 0, []}
      else
        {last_persisted_txn_offset, suffix} = recover_log!(opts, suffix, boot_record)
        boundary = compaction_boundary(opts)
        refuse_boundary_past_claim!(opts, boundary, last_persisted_txn_offset)
        refuse_chunk_index_short_of_log!(opts, suffix, last_persisted_txn_offset)
        suffix = finish_interrupted_switch!(opts, suffix, boundary, last_persisted_txn_offset)

        {
          last_persisted_txn_offset,
          suffix,
          FileInfo.get_file_size!(json_file(opts, suffix)) || 0,
          ChunkIndex.read_last_n_chunks(chunk_file(opts, suffix), 4)
        }
      end

    record_boot!(opts, boot_record, last_persisted_txn_offset)

    {{_, chunk_end_offset}, {start_pos, end_pos}, _} =
      List.last(chunks, {{nil, :empty}, {0, nil}, nil})

    # If the last chunk is complete, we take the end as position to calculate chunk size
    position = end_pos || start_pos

    # finally write the version file which also acts as our
    # "is this a valid storage dir" test
    #
    # A surviving pre-arena log is promoted here rather than discarded above. The marker exists to
    # stop an older reader, so it must be durable before the writer this call is about to open can
    # append a ref, and it must never move backwards or a log already holding refs would start
    # claiming a version that cannot describe it. `initialize?` covers every missing
    # `on_disk_version`, so the comparison only ever sees an integer.
    if initialize? or version > on_disk_version do
      write_metadata!(opts, :version, version, true)
    end

    {WriteLoop.init_from_disk(
       last_persisted_txn_offset: last_persisted_txn_offset,
       write_position: json_file_size,
       bytes_in_chunk: json_file_size - position,
       chunk_started?: is_nil(chunk_end_offset),
       chunks: reformat_chunks_for_cache(chunks)
     ), suffix}
  end

  # A version file whose bytes do not decode to a version, an integer, from a damaged disk or a
  # build that wrote it without a sync, names no version this build can read, and is refused as
  # one. An error reading the file is not the file's content, and raises as a `File.Error`. A
  # missing file is `:missing`, which no file's content can read as.
  defp read_version(%__MODULE__{} = opts) do
    path = shape_metadata_path(opts, "version.bin")

    case File.read(path) do
      {:ok, bytes} ->
        try do
          case :erlang.binary_to_term(bytes) do
            version when is_integer(version) -> version
            _other -> :undecodable
          end
        rescue
          ArgumentError -> :undecodable
        end

      {:error, :enoent} ->
        :missing

      {:error, reason} ->
        raise File.Error, reason: reason, action: "read file", path: path
    end
  end

  # The version file is written durably before the snapshot starts or the log takes a
  # transaction. A snapshot marker, a log with bytes or a durable claim with no version file is
  # storage that lost the file, not a new shape. A compaction that covers the claim leaves the
  # latest generation empty, so the log's bytes alone do not tell, and a compaction runs only on
  # chunks a flush wrote, after it made the claim durable.
  defp holds_storage?(%__MODULE__{} = opts) do
    read_metadata!(opts, :snapshot_started?) or
      not is_nil(read_metadata!(opts, :last_persisted_txn_offset)) or
      case read_metadata!(opts, :latest_name) do
        nil -> false
        suffix -> (FileInfo.get_file_size!(json_file(opts, suffix)) || 0) > 0
      end
  end

  # The read path keeps a shape's snapshot start and end cached while it has no writer, a
  # suspended shape's among them, and a cleanup drops them with the files. Cached over a disk that
  # holds no storage, they name a snapshot clients may have read whose files went from under the
  # running stack, and the writer would put them back in the cache over an empty log.
  defp cached_snapshot?(%__MODULE__{} = opts) do
    case :ets.lookup(opts.stack_ets, opts.shape_handle) do
      [storage_meta(snapshot_started?: started?, last_snapshot_chunk: last_chunk)] ->
        started? == true or not is_nil(last_chunk)

      [] ->
        false
    end
  end

  # A shape clients may have read keeps its handle only while its storage goes on from what they
  # read. Rebuilt under the same handle, it answered a client resuming from an offset of the old
  # log with a snapshot taken later and the log after it, and the client never saw the changes
  # between the two. A snapshot can be read chunk by chunk while it is written, so even one that
  # never completed may have been read. So the reopen raises: the consumer's abnormal exit
  # removes the shape, and its clients refetch under a new handle. The files stay for that
  # removal, boot record included.
  @spec refuse_rebuild_under_handle!(%__MODULE__{}, term()) :: no_return()
  defp refuse_rebuild_under_handle!(%__MODULE__{} = opts, on_disk_version) do
    reason =
      cond do
        on_disk_version == :undecodable ->
          "has a version file that holds no version"

        on_disk_version == :missing and not holds_storage?(opts) and not cached_snapshot?(opts) ->
          "has no storage to restore"

        on_disk_version == :missing and not holds_storage?(opts) ->
          "has lost its files while its snapshot is still cached"

        on_disk_version == :missing ->
          "has lost its version file"

        on_disk_version not in @readable_versions ->
          "is stored as version #{inspect(on_disk_version)}, which this build cannot read"

        not snapshot_complete?(opts) ->
          "has no complete snapshot"

        # The mark is written unsynced, and a crash can take it while the record of the last chunk
        # survives. Without it the snapshotter would take the snapshot again under the handle.
        not snapshot_started?(opts) ->
          "has a complete snapshot and no mark that it started"

        true ->
          "has no snapshot position"
      end

    raise Storage.Error,
      kind: :data_loss,
      message: "Shape #{opts.shape_handle} #{reason}; it is removed, not rebuilt under its handle"
  end

  # A compaction cuts at or below the durable claim, and recovery trims only the latest generation.
  # A build that cut past the claim could leave, after a crash, a boundary above the offset the
  # shape resumes from: the compacted generation served rows of transactions the log no longer
  # held, and reads below the boundary skipped their replay in the latest generation.
  defp refuse_boundary_past_claim!(%__MODULE__{} = opts, {boundary, _}, claim) do
    if is_log_offset_lt(claim, boundary) do
      raise Storage.Error,
        kind: :data_loss,
        message:
          "Compaction boundary #{inspect(boundary)} of shape #{opts.shape_handle} is past its " <>
            "durable offset #{inspect(claim)}; the storage has lost durable data"
    end
  end

  # A compaction's switch names the compacted generation, then the new latest one, so a writer
  # killed between the two leaves a latest generation that still starts at or below the boundary.
  # Reads route those rows to the compacted generation, but the next compaction merged the latest
  # from its first byte with the compacted one, and a row in both came out of the merge twice.
  # The reopen finishes the switch, from the chunk that ends at the boundary, after the trim, so
  # the copy takes only what recovery kept.
  #
  # A compacted generation that covers the claim holds every entry of the latest one the shape
  # needs, so the reopen gives the shape an empty latest generation instead: recovery has already
  # done so for the durable claim (`start_empty_latest!/2`), and this does for a promoted written
  # marker. A latest log or chunk index lost there is taken for the compaction, and a build that
  # synced nothing after one could lose either. With the index lost, the copy found no cut and kept
  # a log of compacted entries behind an empty index, which the next compaction merged again; with
  # the log lost, the copy failed and the shape was removed.
  defp finish_interrupted_switch!(%__MODULE__{} = opts, suffix, {boundary, compacted}, claim)
       when not is_nil(compacted) and is_log_offset_lte(claim, boundary) do
    if empty_generation?(opts, suffix), do: suffix, else: publish_empty_latest!(opts, suffix)
  end

  # The chunk that ends at the boundary was in the latest index when the compaction cut there, and
  # the claim is past it. An index that no longer has it, ending on that chunk's opening say, lost
  # the cut: the switch kept the latest generation whole, and the next compaction cut at an earlier
  # chunk and published a boundary below rows the compacted generation held.
  defp finish_interrupted_switch!(%__MODULE__{} = opts, suffix, compaction_boundary, claim) do
    with {boundary, compacted} when not is_nil(compacted) <- compaction_boundary,
         path = chunk_file(opts, suffix),
         %LogOffset{} = first <- chunk_start(ChunkIndex.get_nth_chunk(path, 0)),
         true <- is_log_offset_lte(first, boundary),
         {_, {_, cut}, _} <-
           path |> ChunkIndex.read_chunk_file() |> Enum.find(&match?({{_, ^boundary}, _, _}, &1)) do
      new_suffix = copy_tail_to_new_latest!(opts, suffix, boundary, cut)
      write_metadata!(opts, :latest_name, new_suffix, true)
      new_suffix
    else
      nil ->
        raise Storage.Error,
          kind: :data_loss,
          message:
            "Chunk index for shape #{opts.shape_handle} lost the chunk ending at its compaction " <>
              "boundary #{inspect(elem(compaction_boundary, 0))} below its persisted offset " <>
              "#{inspect(claim)}; the storage has lost durable data"

      _ ->
        suffix
    end
  end

  defp chunk_start({:complete, {first, _}, _, _}), do: first
  defp chunk_start({:incomplete, first, _, _}), do: first
  defp chunk_start(:error), do: :error

  # The writer writes a chunk's start before the first line in it, so a log byte past the last
  # complete chunk belongs to an open one, and an index that ends on a complete chunk short of the
  # log lost the entries for the bytes past it. A build that synced nothing after a write or a
  # switch could lose them while the log kept the lines they map. Every read is driven by the
  # index, so the shape advertised its claim and served none of those lines; an append then opened
  # a chunk past them, and a switch finished here copied them behind its cut.
  defp refuse_chunk_index_short_of_log!(%__MODULE__{} = opts, suffix, claim) do
    if chunk_index_short_of_log?(opts, suffix) do
      raise_lost_durable_data!(
        opts,
        claim,
        "Chunk index for shape #{opts.shape_handle} stops short of its log below its " <>
          "persisted offset"
      )
    end

    :ok
  end

  defp chunk_index_short_of_log?(%__MODULE__{} = opts, suffix) do
    case ChunkIndex.get_last_boundary(chunk_file(opts, suffix)) do
      {:incomplete, _, _, _} ->
        false

      {:complete, _, end_pos, _} ->
        end_pos < (FileInfo.get_file_size!(json_file(opts, suffix)) || 0)
    end
  end

  defp empty_generation?(%__MODULE__{} = opts, suffix) do
    Enum.all?(
      [json_file(opts, suffix), chunk_file(opts, suffix)],
      &(FileInfo.file_size(&1) == {:ok, 0})
    )
  end

  # The new generation's files and their entry are durable before the name that points at them,
  # as a switch's are (`copy_tail_to_new_latest!/4`).
  #
  # The generation it replaces stays for a read already under way in it, and the ones before that
  # go, as a compaction sweeps them (`drop_generations_older_than_previous/2`). Left to the next
  # compaction, a replay that a kill ended before its first commit left a copy of its tail at each
  # reopen, and no compaction runs before a commit moves the claim. They go before the new name,
  # while the disk still names the generation they precede: swept after it, a kill between the two
  # left them, and the next reopen found the new generation empty, published nothing and swept
  # nothing.
  defp publish_empty_latest!(%__MODULE__{} = opts, suffix) do
    on_disk = generations_on_disk(opts, "latest")
    remove_unnamed_latest(opts, suffix, on_disk)
    sweep_replaced_generations(opts, suffix)
    new_suffix = next_suffix("latest", [suffix | on_disk])

    for path <- [json_file(opts, new_suffix), chunk_file(opts, new_suffix)] do
      File.open!(path, [:write, :raw, :exclusive], fn _file -> :ok end)
      Utils.datasync_file!(path)
    end

    Utils.sync_dir!(shape_log_dir(opts))
    write_metadata!(opts, :latest_name, new_suffix, true)
    new_suffix
  end

  # A latest generation past the one the disk names was never named: a switch or a publish killed
  # between writing its files and naming them leaves it, and no read opens it, as the cache takes a
  # name only once the disk has it (`set_latest_name/3`). A reopen killed in each copy of a large
  # tail kept one more such copy at each retry, so each new generation removes them first. Like
  # the sweep, a removal that fails costs only space.
  defp remove_unnamed_latest(%__MODULE__{} = opts, named, on_disk) do
    floor = suffix_position("latest", named)

    for suffix <- on_disk,
        position = suffix_position("latest", suffix),
        is_integer(floor) and is_integer(position) and position > floor,
        path <- opts |> shape_log_dir() |> Path.join("log.#{suffix}.*.bin") |> Path.wildcard(),
        do: remove_generation_file(opts, path)

    :ok
  end

  # The sweep only frees space, so one that fails costs the reopen nothing: the next publish or
  # compaction sweeps the same files.
  #
  # A writer killed between a switch's name on disk and its name in the cache leaves the readers of
  # this stack on the generation before the one the disk names, and a reopen in the same stack
  # removed it under them. The sweep keeps whichever of the two names is older.
  defp sweep_replaced_generations(%__MODULE__{} = opts, suffix) do
    floor =
      [suffix, cached_latest_name(opts)]
      |> Enum.filter(&suffix_position("latest", &1))
      |> Enum.min_by(&suffix_position("latest", &1), fn -> suffix end)

    drop_generations_older_than_previous(opts, [{"latest", floor}])
  end

  # A store that exists but has never committed a transaction has nothing to reach, so an empty
  # log is expected rather than lost.
  defp trim_log!(%__MODULE__{} = opts, last_persisted_offset, suffix)
       when last_persisted_offset == unquote(Macro.escape(LogOffset.last_before_real_offsets())) do
    {log_search_start_pos, _} =
      ChunkIndex.realign_and_trim(chunk_file(opts, suffix), last_persisted_offset)

    json_file(opts, suffix)
    |> LogFile.trim(log_search_start_pos, last_persisted_offset, read_errors: :return)
    |> empty_unread_latest!(opts, suffix)
  end

  defp trim_log!(%__MODULE__{} = opts, last_persisted_offset, suffix) do
    # Persisted offset writes are guaranteed to be last & atomic, so we can use it as a marker for the end of the log

    # First, we need to make sure that chunk file is fine: it should be aligned, and last chunk shoudn't overshoot the
    # new end of log.
    {log_search_start_pos, _} =
      ChunkIndex.realign_and_trim(chunk_file(opts, suffix), last_persisted_offset)

    # A chunk boundary is written before the log line it opens and the persisted offset is
    # written last, so an index left holding nothing under a real persisted offset is not a
    # state the writer can crash into -- it is the index having been lost. The realignment above
    # produces it silently, because a record damaged past the persisted offset is indistinguishable
    # from one written after it, and trimming is the right answer to the second. The log it maps is
    # untouched and metadata still advertises the offset it ends at, but every read is driven by
    # the index: the shape answers `up-to-date` at that offset with none of the rows behind it, and
    # nothing rebuilds the index or invalidates the shape. Measured on the engine, one flipped byte
    # in a chunk record turns a 5 061-byte response into a 73-byte one carrying zero rows.
    if lost_chunk_index?(opts, suffix, last_persisted_offset) do
      raise_lost_durable_data!(
        opts,
        last_persisted_offset,
        "Chunk index for shape #{opts.shape_handle} is empty below its persisted offset"
      )
    end

    # Now, we'll search for the first line that's greater than the last persisted offset and truncate the log there
    case LogFile.trim(json_file(opts, suffix), log_search_start_pos, last_persisted_offset) do
      :ok ->
        :ok

      # The writer claims the last entry it synced, so a log with an entry past the claim and none
      # at it lost the claimed one, as a failed write nothing checked leaves it under a later write
      # that landed. Replay then skipped the transaction as already stored. The exception is a claim
      # a compaction covers: its entry is in the compacted generation.
      :past_boundary ->
        raise_lost_durable_data!(
          opts,
          last_persisted_offset,
          "Log for shape #{opts.shape_handle} holds no entry at its persisted offset"
        )

      :short_of_boundary ->
        # The metadata file only ever claims datasynced bytes, so a log that stops before the
        # offset it claims has lost bytes the shape already acknowledged — and the log carries
        # no checksums, so nothing downstream would notice: reads simply end early while the
        # shape keeps advertising the offset it can no longer serve. Refuse to resume from it.
        raise_lost_durable_data!(
          opts,
          last_persisted_offset,
          "Log for shape #{opts.shape_handle} ends before its persisted offset"
        )
    end
  end

  # With no transaction claimed, no entry of the latest generation is durable, and the source still
  # holds every one of them. A read that fails there failed the reopen over bytes no claim vouched
  # for, after the written-marker probe had already fallen back from the same read, so the
  # generation is emptied instead, its index first: the trim would have cut every entry too.
  defp empty_unread_latest!(:unreadable, %__MODULE__{} = opts, suffix) do
    for path <- [chunk_file(opts, suffix), json_file(opts, suffix)] do
      case FileInfo.truncate(path, 0) do
        :ok -> :ok
        {:error, reason} -> raise File.Error, reason: reason, action: "truncate", path: path
      end
    end

    :ok
  end

  defp empty_unread_latest!(_trimmed, _opts, _suffix), do: :ok

  # A compaction moves every offset at or below its boundary out of the latest generation and into
  # the compacted one, so a latest generation that no longer reaches the offset the shape has
  # served is that compaction having happened rather than bytes going missing. Without this the
  # guards above turned the first restart after any compaction into a lost-data claim, and the
  # shape was rebuilt from scratch.
  defp raise_lost_durable_data!(%__MODULE__{} = opts, last_persisted_offset, what) do
    {compacted_through, _suffix} = compaction_boundary(opts)

    if is_log_offset_lte(last_persisted_offset, compacted_through) do
      :ok
    else
      raise Storage.Error,
        kind: :data_loss,
        message: "#{what} #{inspect(last_persisted_offset)}; the storage has lost durable data"
    end
  end

  defp lost_chunk_index?(%__MODULE__{} = opts, suffix, last_persisted_offset) do
    FileInfo.file_size(chunk_file(opts, suffix)) in [{:ok, 0}, {:error, :enoent}] and
      FileInfo.file_size(json_file(opts, suffix)) != {:ok, 0} and
      not is_log_offset_lte(last_persisted_offset, LogOffset.last_before_real_offsets())
  end

  # optimization for snapshot started boolean to avoid expensive file open
  defp read_metadata!(%__MODULE__{} = opts, key) when key in @boolean_stored_keys,
    do: FileInfo.exists?(shape_metadata_path(opts, "#{key}.bin"))

  defp read_metadata!(%__MODULE__{} = opts, key) when key in @stored_keys do
    case File.open(
           shape_metadata_path(opts, "#{key}.bin"),
           [:read, :raw],
           &(&1 |> IO.binread(:eof) |> :erlang.binary_to_term())
         ) do
      {:ok, value} -> value
      {:error, :enoent} -> nil
    end
  end

  defp read_metadata!(%__MODULE__{} = _opts, _key), do: nil

  # Read metadata with ETS-first, disk-fallback pattern
  defp read_cached_metadata(%__MODULE__{} = opts, key) do
    case read_multiple_cached_metadata(opts, [key]) do
      [{^key, nil}] -> read_metadata!(opts, key)
      [{^key, value}] -> value
    end
  end

  defp read_multiple_cached_metadata(%__MODULE__{} = opts, keys) do
    opts
    |> read_or_initialize_metadata(keys)
    |> expand_storage_meta(keys)
  end

  defp read_or_initialize_metadata(%__MODULE__{read_only?: true} = opts, keys) do
    # Read-only means this node is not the writer of these bytes: either another instance holds
    # the lock, or this one holds it and its own shape subsystem has not started writing yet.
    # Either way a cached entry can go stale under a writer this node does not control, so each
    # read costs ~1-2ms of file I/O instead.
    read_from_disk_without_caching(opts, keys)
  end

  defp read_or_initialize_metadata(%__MODULE__{shape_handle: handle} = opts, keys) do
    case :ets.lookup(opts.stack_ets, handle) do
      [] -> populate_read_through_cache!(opts, keys)
      [storage_meta() = meta] -> meta
    end
  end

  defp read_from_disk_without_caching(%__MODULE__{shape_handle: handle} = opts, extra_keys) do
    read_keys = Enum.into(extra_keys, MapSet.new(@read_path_keys))

    keys =
      for key <- in_read_order(read_keys) do
        {key, read_metadata!(opts, key)}
      end

    create_storage_meta([{:shape_handle, handle} | keys])
  end

  # A compaction's switch writes the boundary, then the latest name, and the keys were read in
  # term order, the boundary first: a switch between the two reads paired the previous boundary
  # with the new latest generation, which starts past the rows between the two boundaries, and a
  # read there skipped them. The name is read after the offsets, so the generation it names holds
  # every row they reach, and the boundary last, so it is at least as new as the name.
  defp in_read_order(keys),
    do: Enum.sort_by(keys, &Map.get(%{latest_name: 1, compaction_boundary: 2}, &1, 0))

  defp populate_read_through_cache!(%__MODULE__{} = opts, extra_keys) do
    %{shape_handle: handle, stack_ets: stack_ets} = opts

    read_keys = Enum.into(extra_keys, MapSet.new(@read_path_keys))

    keys =
      for key <- in_read_order(read_keys) do
        {key, read_metadata!(opts, key)}
      end

    meta = create_storage_meta([{:shape_handle, handle} | keys])

    # prevent race conditions where a writer initialises the ets
    # for a shape after we've checked for existence
    :ets.insert_new(stack_ets, meta)

    meta
  end

  defp write_metadata!(%__MODULE__{} = opts, key, value) when key in @boolean_stored_keys do
    path = Path.join(shape_metadata_dir(opts), "#{key}.bin")

    if value,
      do: write!(path, <<>>, [:write, :raw]),
      else: Electric.AsyncDeleter.delete(opts.stack_id, path)
  end

  defp write_metadata!(%__MODULE__{} = opts, key, value),
    do: write_metadata!(opts, key, value, false)

  defp write_metadata!(%__MODULE__{} = opts, key, value, durable?)
       when key in @stored_keys,
       do: write_term_file!(shape_metadata_dir(opts), to_string(key) <> ".bin", value, durable?)

  defp write_metadata!(%__MODULE__{} = _opts, _key, value, _durable?) do
    value
  end

  defp write_term_file!(dir, name, value, durable?) do
    path = Path.join(dir, name)
    tmp_path = path <> ".tmp"

    # Opt-in and deliberately narrow: the other metadata writes are on the hot path and only need to
    # survive a VM restart, so syncing them would hand back the cost the write/datasync split exists
    # to avoid.
    modes = if durable?, do: [:write, :raw, :sync], else: [:write, :raw]
    write!(tmp_path, :erlang.term_to_binary(value), modes)

    rename!(tmp_path, path)

    if durable?, do: Utils.sync_dir!(dir)

    value
  end

  # Write metadata to both disk and ETS
  defp write_cached_metadata!(%__MODULE__{} = opts, key, value) do
    # Write to disk first
    write_metadata!(opts, key, value)

    try do
      write_metadata_cache(opts, key, value)

      :ok
    rescue
      # ETS entry doesn't exist yet, that's okay
      ArgumentError -> :ok
    end
  end

  defp write_metadata_cache(%__MODULE__{} = opts, key, value) do
    write_metadata_cache(opts, [{key, value}])
  end

  defp write_metadata_cache(%__MODULE__{} = opts, key_values) when is_list(key_values) do
    %{stack_ets: stack_ets, shape_handle: handle} = opts

    updates = for {key, value} <- key_values, do: {storage_meta_key_pos(key), value}

    :ets.update_element(stack_ets, handle, updates)
  end

  defp last_snapshot_chunk(%__MODULE__{} = opts),
    do: read_cached_metadata(opts, :last_snapshot_chunk)

  @doc """
  Report that this shape's bytes are gone, discarding it so a reader can rebuild it.

  Damage found when a shape is reopened retires it and the next reader rebuilds it from Postgres.
  Damage found during a read had no such route: nothing in `lib/` catches `Storage.Error`, so the
  request failed, the shape stayed exactly as broken, and every client retried into the same bytes
  forever. This sends it to the same place.

  Only genuine loss may come through here. The transient half of `Storage.Error` is the machine
  being out of descriptors, space or memory, and a rebuild costs more of whatever ran out, on every
  shape at once.

  Scheduling the rebuild is best effort; reporting the loss is not. `remove_shape_async/2` is a
  synchronous call onto the cleanup supervisor that schedules the asynchronous work, so it exits
  wherever that supervisor is not running -- during shutdown, or on any stack that reads storage
  without the full shape lifecycle. Letting that through would replace a precise account of what
  was lost with a GenServer exit and lose the message with it.

  A read-only reader reports the loss and leaves the shape alone. It is not the writer of these
  bytes: it is an instance waiting for the lock, reading what the running one owns, or one whose
  own shape subsystem has not started. The cleanup supervisor and the catalog's write buffer run
  before the lock, so a removal from there deleted the owner's catalog row and the directory under
  its consumer. The owner discards the shape when its own read meets the loss.
  """
  @spec discard_shape_after_data_loss!(%__MODULE__{}, String.t()) :: no_return()
  def discard_shape_after_data_loss!(%__MODULE__{read_only?: true} = opts, message) do
    Logger.error("Shape #{opts.shape_handle} lost data, kept for its writer: #{message}")

    raise Storage.Error, kind: :data_loss, message: message
  end

  def discard_shape_after_data_loss!(%__MODULE__{} = opts, message) do
    Logger.error("Discarding shape #{opts.shape_handle}: #{message}")

    try do
      ShapeCleaner.remove_shape_async(opts.stack_id, opts.shape_handle)
    catch
      kind, reason ->
        Logger.warning(
          "Could not schedule removal of shape #{opts.shape_handle}: #{inspect({kind, reason})}"
        )
    end

    raise Storage.Error, kind: :data_loss, message: message
  end

  @doc """
  Whether this snapshot chunk file has been written in full and can never grow again.

  A reader streaming a snapshot concurrently with its writer has to tolerate reaching the end of
  a file the writer has not finished, which is what the wait in `Snapshot.stream_chunk_lines/2`
  is for. `last_snapshot_chunk` is written only once the whole stream has been written and every
  chunk file closed, so once it covers this chunk there are no more bytes coming and waiting can
  only delay the failure.
  """
  @spec snapshot_chunk_final?(%__MODULE__{}, non_neg_integer()) :: boolean()
  def snapshot_chunk_final?(%__MODULE__{} = opts, chunk_num) do
    case last_snapshot_chunk(opts) do
      %LogOffset{op_offset: last_chunk_num} -> chunk_num <= last_chunk_num
      _ -> false
    end
  end

  # The record on disk, not the cache: the cache shows a snapshot's end from its last chunk's
  # marker, before the syncs and the record, and a snapshot killed or failing in between leaves
  # nothing on disk to resume from (`make_new_snapshot!/2`).
  defp snapshot_complete?(%__MODULE__{} = opts) do
    not is_nil(read_metadata!(opts, :last_snapshot_chunk))
  end

  defp create_directories!(%__MODULE__{} = opts) do
    mkdir_p!(shape_log_dir(opts))
    mkdir_p!(shape_metadata_dir(opts))
  end

  defp register_with_stack(opts, table, stable_offset, compaction_boundary, suffix, chunks) do
    metadata =
      read_multiple_cached_metadata(opts, [
        :snapshot_started?,
        :compaction_started?,
        :last_snapshot_chunk,
        :pg_snapshot
      ])

    snapshot_started = Keyword.get(metadata, :snapshot_started?) || false
    compaction_started = Keyword.get(metadata, :compaction_started?) || false
    last_snapshot_chunk = Keyword.get(metadata, :last_snapshot_chunk)
    pg_snapshot = Keyword.get(metadata, :pg_snapshot)

    # we can just insert here, ignoring any existing values because the writer
    # as full authority over the cached values in the ets
    :ets.insert(
      opts.stack_ets,
      storage_meta(
        shape_handle: opts.shape_handle,
        ets_table: table,
        last_persisted_txn_offset: stable_offset,
        last_persisted_offset: stable_offset,
        last_seen_txn_offset: stable_offset,
        compaction_boundary: compaction_boundary,
        latest_name: suffix,
        snapshot_started?: snapshot_started,
        pg_snapshot: pg_snapshot,
        compaction_started?: compaction_started,
        last_snapshot_chunk: last_snapshot_chunk,
        cached_chunk_boundaries: chunks
      )
    )
  end

  defp reformat_chunks_for_cache([]), do: {LogOffset.last_before_real_offsets(), []}

  defp reformat_chunks_for_cache([{{_, max}, _, _} | [_, _, _] = rest]),
    do: {max, Enum.map(rest, fn {offsets, pos, _key_pos} -> {offsets, pos} end)}

  defp reformat_chunks_for_cache(chunks),
    do:
      {LogOffset.last_before_real_offsets(),
       Enum.map(chunks, fn {offsets, pos, _key_pos} -> {offsets, pos} end)}

  @type chunk ::
          {{min :: LogOffset.t(), max :: LogOffset.t()},
           {log_start :: pos_integer(), log_end :: pos_integer()}}
  @spec read_boundary_info(%__MODULE__{}) :: {
          latest_path :: String.t(),
          compacted :: {boundary :: LogOffset.t(), path :: String.t()},
          cached_chunks :: {prev_max :: LogOffset.t() | nil, [chunk]}
        }
  defp read_boundary_info(%__MODULE__{} = opts) do
    metadata =
      read_multiple_cached_metadata(opts, [
        :latest_name,
        :compaction_boundary,
        :cached_chunk_boundaries
      ])

    normalize_boundary_info(
      opts,
      Keyword.get(metadata, :latest_name),
      Keyword.get(metadata, :compaction_boundary),
      Keyword.get(metadata, :cached_chunk_boundaries)
    )
  end

  defp normalize_boundary_info(opts, latest_name, compaction_boundary, cached_chunk_boundaries) do
    {
      latest_name || latest_name(opts),
      compaction_boundary || compaction_boundary(opts),
      cached_chunk_boundaries || {nil, []}
    }
  end

  def open_file(writer_state(opts: opts, latest_name: latest_name), type) do
    open_file(opts, latest_name, type)
  end

  defp open_file(opts, suffix, :json_file),
    do: File.open!(json_file(opts, suffix), [:append, :raw])

  # We're opening the chunk file in sync mode because writes there are rare but we prefer for them
  # to be atomic
  defp open_file(opts, suffix, :chunk_file),
    do: File.open!(chunk_file(opts, suffix), [:append, :raw, :sync])

  defp open_file(opts, suffix, :key_file), do: File.open!(key_file(opts, suffix), [:append, :raw])

  def get_chunk_end_log_offset(offset, %__MODULE__{}) when is_min_offset(offset),
    do: LogOffset.new(0, 0)

  def get_chunk_end_log_offset(offset, %__MODULE__{} = opts)
      when is_virtual_offset(offset) and not is_last_virtual_offset(offset) do
    case last_snapshot_chunk(opts) do
      # We don't have the "last one", so optimistically give the next chunk pointer.
      # If it turns out we're actually done, then this pointer will give beginning of txn log when requested with.
      nil -> LogOffset.increment(offset)
      # This is easy - we want to read next chunk and we know we can
      last when is_log_offset_lt(offset, last) -> LogOffset.increment(offset)
      # Requested chunk is at the end or beyond the end of the snapshot, serve from txn log. If no chunk is yet present, get end of log
      _ -> get_chunk_end_log_offset(LogOffset.last_before_real_offsets(), opts)
    end
  end

  def get_chunk_end_log_offset(offset, %__MODULE__{} = opts) do
    case fetch_chunk(offset, opts) do
      {:ok, max_offset, _} -> max_offset
      :error -> nil
    end
  end

  defp fetch_chunk(offset, %__MODULE__{} = opts, boundary_info \\ nil) do
    {latest_name, {compaction_boundary, compacted_name}, {cached_min, chunks}} =
      boundary_info || read_boundary_info(opts)

    # Any virtual offsets are handled elsewhere - normalize them to the last before real offsets for main log
    offset = if is_virtual_offset(offset), do: LogOffset.last_before_real_offsets(), else: offset

    cond do
      LogOffset.is_log_offset_lt(offset, compaction_boundary) ->
        ChunkIndex.fetch_chunk(chunk_file(opts, compacted_name), offset)

      not is_nil(cached_min) and LogOffset.is_log_offset_lte(cached_min, offset) ->
        find_chunk_positions_in_cache(chunks, offset)

      true ->
        ChunkIndex.fetch_chunk(chunk_file(opts, latest_name), offset)
    end
  end

  defp find_chunk_positions_in_cache([], _), do: :error
  defp find_chunk_positions_in_cache([{{_, nil}, positions}], _), do: {:ok, nil, positions}

  defp find_chunk_positions_in_cache([{{_, max}, positions} | _], offset)
       when is_log_offset_lt(offset, max),
       do: {:ok, max, positions}

  defp find_chunk_positions_in_cache([{{_, max}, _} | rest], offset)
       when is_log_offset_lte(max, offset),
       do: find_chunk_positions_in_cache(rest, offset)

  # The catalog marks the snapshot complete once this returns, and that mark can outlive a machine
  # crash before the shape makes a durable claim of its own. So `last_snapshot_chunk` is written
  # durably, and the chunks it names (synced as each closes, `Snapshot.write_snapshot_stream!/4`)
  # and the directory entries that lead to them are durable before it. A chunk a crash cut short is
  # caught by the reader, at its missing end-of-transmission marker, but a block lost below a
  # chunk's end reads back as zeros, which the reader serves.
  #
  # Readers see the end once the last chunk's marker is written, before those syncs, as they did
  # when the end was recorded at once. An end seen only once its record was in place gave a read
  # of the offset after the last chunk, during the syncs, the pointer to a next chunk as its bound,
  # and the same read once the end was seen the end of the log: two responses to one request, so
  # a client revalidating the first was sent a new response where the shape had not changed.
  #
  # A snapshot that fails once its end is cached takes the end back out of the cache, so that this
  # node reads the disk for it, as before the end was cached early: a consumer that suspended while
  # its snapshot ran is not there to remove the shape, and its readers would read on to an end no
  # record backs. A writer opened on the shape reads the record itself (`snapshot_complete?/1`),
  # which also covers a snapshot killed before it could take the end back. Only an attempt that
  # cached an end takes one out: an attempt on a shape whose snapshot is already there fails at its
  # first chunk, and leaves that snapshot's end in the cache and its record on disk.
  def make_new_snapshot!(stream, %__MODULE__{} = opts) do
    shown = :atomics.new(1, [])

    try do
      last_chunk_num =
        Snapshot.write_snapshot_stream!(stream, opts, fn last_chunk_num ->
          :atomics.put(shown, 1, 1)
          cache_snapshot_end(opts, last_chunk_num)
        end)

      sync_log_directories!(opts)
      write_metadata!(opts, :last_snapshot_chunk, LogOffset.new(0, last_chunk_num), true)
      :ok
    catch
      kind, reason ->
        if :atomics.get(shown, 1) == 1, do: cache_snapshot_end(opts, nil)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp cache_snapshot_end(%__MODULE__{} = opts, last_chunk_num) do
    end_offset = if last_chunk_num, do: LogOffset.new(0, last_chunk_num)
    write_metadata_cache(opts, :last_snapshot_chunk, end_offset)
  rescue
    # ETS entry doesn't exist yet, that's okay
    ArgumentError -> :ok
  end

  def write_move_in_snapshot!(stream, name, %__MODULE__{} = opts) do
    path = shape_snapshot_path(opts, name)
    FileInfo.mkdir_p(shape_snapshot_dir(opts))

    stream
    |> Stream.map(fn [key, tags, json] ->
      tags_binary = Enum.map(tags, &[<<byte_size(&1)::16>>, &1])

      [
        <<byte_size(key)::32>>,
        <<byte_size(json)::64>>,
        <<?i::8>>,
        <<length(tags)::16>>,
        tags_binary,
        key,
        json
      ]
    end)
    |> Stream.into(File.stream!(path, [:delayed_write]))
    |> Stream.run()

    :ok
  end

  def append_move_in_snapshot_to_log!(
        name,
        writer_state(opts: opts, writer_acc: acc) = state,
        skip_row?
      ) do
    starting_offset = WriteLoop.last_seen_offset(acc)

    writer_state(writer_acc: acc) =
      state =
      Stream.resource(
        fn ->
          {File.open!(shape_snapshot_path(opts, name), [:read, :raw, :read_ahead]),
           LogOffset.increment(starting_offset)}
        end,
        fn {file, offset} ->
          with {:meta, <<key_size::32, json_size::64, op_type::8, tag_count::16>>} <-
                 {:meta, IO.binread(file, 15)},
               tags = read_tags(file, tag_count),
               <<key::binary-size(^key_size)>> <- IO.binread(file, key_size),
               <<json::binary-size(^json_size)>> <- IO.binread(file, json_size) do
            if skip_row?.(key, tags, json) do
              {[], {file, offset}}
            else
              {[{offset, key_size, key, op_type, 0, json_size, json}],
               {file, LogOffset.increment(offset)}}
            end
          else
            {:meta, :eof} ->
              {:halt, {file, offset}}

            _ ->
              raise Storage.Error,
                kind: :data_loss,
                message: "Incomplete move-in snapshot file at #{shape_snapshot_path(opts, name)}"
          end
        end,
        fn {file, _} ->
          File.close(file)
          FileInfo.delete(shape_snapshot_path(opts, name))
        end
      )
      |> append_to_log!(state)

    inserted_range = {starting_offset, WriteLoop.last_seen_offset(acc)}

    {inserted_range, state}
  end

  defp read_tags(file, tag_count) do
    for _ <- 1..tag_count//1 do
      <<tag_size::16>> = IO.binread(file, 2)
      <<tag::binary-size(^tag_size)>> = IO.binread(file, tag_size)
      tag
    end
  end

  def append_control_message!(control_message, writer_state(writer_acc: acc) = state)
      when is_binary(control_message) do
    offset = WriteLoop.last_seen_offset(acc)
    inserted_offset = LogOffset.increment(offset)

    state =
      [{inserted_offset, 0, "", ?c, 0, byte_size(control_message), control_message}]
      |> append_to_log!(state)

    inserted_range = {offset, inserted_offset}
    {inserted_range, state}
  end

  def get_log_stream(%LogOffset{} = min_offset, %LogOffset{} = max_offset, opts)
      when is_last_virtual_offset(min_offset) or is_real_offset(min_offset) do
    stream_main_log(min_offset, max_offset, opts)
  end

  def get_log_stream(
        %LogOffset{op_offset: op_offset} = min_offset,
        %LogOffset{} = max_offset,
        %__MODULE__{} = opts
      ) do
    # Single ETS lookup to get both snapshot_started? and last_snapshot_chunk
    metadata = read_multiple_cached_metadata(opts, [:snapshot_started?, :last_snapshot_chunk])

    snapshot_started? = Keyword.get(metadata, :snapshot_started?) || false
    last_snapshot_chunk = Keyword.get(metadata, :last_snapshot_chunk)

    if not snapshot_started? and not shape_gone?(opts) do
      raise(Storage.Error, message: "Snapshot not started")
    end

    case {last_snapshot_chunk, min_offset} do
      {_, x} when is_min_offset(x) ->
        Snapshot.stream_chunk_lines(opts, 0)

      {%LogOffset{} = latest, min_offset} when is_log_offset_lt(min_offset, latest) ->
        # Stream next chunk of snapshot
        Snapshot.stream_chunk_lines(opts, op_offset + 1)

      {nil, offset} ->
        # Try streaming the next chunk if the file already exists, otherwise wait for the file or end of snapshot to be announced
        # where either event should happen shortly, we just either hit a file switch or just before the storage was updated
        wait_for_chunk_file_or_snapshot_end(opts, op_offset + 1, offset, max_offset)

      {%LogOffset{}, offset} ->
        read_past_snapshot(offset, max_offset, opts)
    end
  end

  # A virtual bound ends the read inside the snapshot, below every entry of the log. A
  # compacted chunk is read whole whatever the bound, so a read the end of the snapshot
  # overtook after the bound was taken handed back entries the next read streamed again.
  defp read_past_snapshot(_min_offset, max_offset, _opts) when is_virtual_offset(max_offset),
    do: []

  defp read_past_snapshot(min_offset, max_offset, opts),
    do: stream_main_log(min_offset, max_offset, opts)

  defp stream_main_log(min_offset, max_offset, %__MODULE__{} = opts) do
    storage_meta(
      ets_table: ets,
      last_persisted_offset: last_persisted,
      last_persisted_txn_offset: last_persisted_txn,
      last_seen_txn_offset: last_seen,
      latest_name: latest_name,
      compaction_boundary: compaction,
      cached_chunk_boundaries: cached_boundaries
    ) = read_or_initialize_metadata(opts, [])

    last_persisted =
      last_persisted ||
        latest_offset(
          last_seen_txn_offset: last_seen,
          last_persisted_txn_offset: last_persisted_txn
        )

    last_seen = last_seen || last_persisted
    boundary_info = normalize_boundary_info(opts, latest_name, compaction, cached_boundaries)

    upper_read_bound = LogOffset.min(max_offset, last_seen)

    # Convert upper_read_bound to tuple for comparison with ETS offsets
    upper_read_bound_tuple = LogOffset.to_tuple(upper_read_bound)

    cond do
      is_log_offset_lte(last_persisted, min_offset) and is_nil(ets) ->
        []

      is_log_offset_lte(last_persisted, min_offset) ->
        # Pure ETS read case
        case read_range_from_ets_cache(ets, min_offset, upper_read_bound) do
          {_data, last_offset}
          when is_nil(last_offset) or last_offset < upper_read_bound_tuple ->
            # Empty or partial read - ETS was cleared by a concurrent flush.
            # Data is now on disk (flush writes to disk before clearing ETS),
            # so read directly from there using existing boundary info.
            stream_from_disk(opts, min_offset, upper_read_bound, boundary_info)

          {data, _last_offset} ->
            data
        end

      is_log_offset_lte(upper_read_bound, last_persisted) ->
        stream_from_disk(opts, min_offset, upper_read_bound, boundary_info)

      true ->
        # Mixed disk + ETS case
        # Because ETS may be cleared by a flush in a parallel process, we're reading it out into memory.
        # It's expected to be fairly small in the worst case, up 64KB
        case read_range_from_ets_cache(ets, last_persisted, upper_read_bound) do
          {_upper_range, last_offset}
          when is_nil(last_offset) or last_offset < upper_read_bound_tuple ->
            # Empty or partial read - ETS was cleared by a concurrent flush.
            # Data is now on disk, so read the full range from there.
            stream_from_disk(opts, min_offset, upper_read_bound, boundary_info)

          {upper_range, _last_offset} ->
            stream_from_disk(opts, min_offset, last_persisted, boundary_info)
            |> Stream.concat(upper_range)
        end
    end
  end

  defp wait_for_chunk_file_or_snapshot_end(
         opts,
         chunk_number,
         min_offset,
         max_offset,
         max_wait_time \\ :timer.seconds(60),
         total_wait_time \\ 0
       )

  defp wait_for_chunk_file_or_snapshot_end(_, _, _, _, max, total) when total >= max,
    do: raise("Snapshot hasn't updated in #{max}ms")

  defp wait_for_chunk_file_or_snapshot_end(
         %__MODULE__{} = opts,
         chunk_number,
         min_offset,
         max_offset,
         max_wait_time,
         total_wait_time
       ) do
    if File.exists?(Snapshot.chunk_file_path(opts, chunk_number), [:raw]) do
      Snapshot.stream_chunk_lines(opts, chunk_number)
    else
      # The end is recorded once every chunk file is written, but the writer can create this chunk
      # and record the end after the look above, so the record says whether the chunk is part of
      # the snapshot. It is read once: read again to tell the end, a record that appeared between
      # the two reads was taken for an end the chunk was not part of. The chunk's rows were
      # skipped, and the response's offset told the client it had them.
      case last_snapshot_chunk(opts) do
        %LogOffset{op_offset: last_chunk_num} when chunk_number <= last_chunk_num ->
          Snapshot.stream_chunk_lines(opts, chunk_number)

        # The routing above read no end, and this lookup reads the disk when the cache has none,
        # as the lookup of a chunk's end does: a reader in read-only mode reads the disk alone,
        # and an end recorded since the routing bounds a read in the log. Answered with nothing,
        # the read's bound told the reader it had the log up to there.
        %LogOffset{} ->
          read_past_snapshot(min_offset, max_offset, opts)

        nil ->
          if shape_gone?(opts) do
            []
          else
            Process.sleep(50)

            wait_for_chunk_file_or_snapshot_end(
              opts,
              chunk_number,
              min_offset,
              max_offset,
              max_wait_time,
              total_wait_time + 50
            )
          end
      end
    end
  end

  # Returns {data, last_offset_read} where last_offset_read is the offset tuple of the
  # last entry read, or nil if no entries were read. This allows callers to detect
  # partial reads due to concurrent ETS clearing.
  @spec read_range_from_ets_cache(:ets.tid() | nil, LogOffset.t(), LogOffset.t()) ::
          {list(), LogOffset.t_tuple() | nil}
  defp read_range_from_ets_cache(nil, _min, _max), do: {[], nil}

  defp read_range_from_ets_cache(ets, %LogOffset{} = min, %LogOffset{} = max) do
    read_range_from_ets_cache(ets, LogOffset.to_tuple(min), LogOffset.to_tuple(max), [], nil)
  end

  @spec read_range_from_ets_cache(
          :ets.tid(),
          LogOffset.t_tuple(),
          LogOffset.t_tuple(),
          list(),
          LogOffset.t_tuple() | nil
        ) :: {list(), LogOffset.t_tuple() | nil}
  defp read_range_from_ets_cache(ets, min, {max_tx, max_op} = max, acc, last_offset) do
    case safe_next_lookup(ets, min) do
      :ets_dead ->
        {Enum.reverse(acc), last_offset}

      :"$end_of_table" ->
        {Enum.reverse(acc), last_offset}

      {{min_tx, min_op}, _} when min_tx > max_tx or (min_tx == max_tx and min_op > max_op) ->
        {Enum.reverse(acc), last_offset}

      {new_min, [{_, item}]} ->
        read_range_from_ets_cache(ets, new_min, max, [item | acc], new_min)
    end
  end

  # The owning Consumer's buffer ETS table may be destroyed during terminate.
  # Falling back is safe because terminate flushes to disk before deleting.
  defp safe_next_lookup(ets, min) do
    :ets.next_lookup(ets, min)
  rescue
    ArgumentError -> :ets_dead
  end

  defp stream_from_disk(%__MODULE__{}, min_offset, max_offset, _)
       when is_log_offset_lte(max_offset, min_offset),
       do: []

  # Entries whose payload lives in the shared payload arena surface from extraction as
  # `{:ref, ref}`; one resolver around the whole lazy chunk-concatenation turns them back
  # into JSON. Activation is per entry (the flag bit), not config, so mixed logs and
  # rollback both read correctly.
  defp stream_from_disk(%__MODULE__{} = opts, min_offset, max_offset, boundary_info) do
    do_stream_from_disk(opts, min_offset, max_offset, boundary_info)
    |> PayloadArena.resolve_refs(
      Storage.opt_for_stack(opts.stack_id, :base_path),
      &discard_shape_after_data_loss!(opts, &1)
    )
  end

  defp do_stream_from_disk(%__MODULE__{}, min_offset, max_offset, _)
       when is_log_offset_lte(max_offset, min_offset),
       do: []

  defp do_stream_from_disk(
         %__MODULE__{} = opts,
         min_offset,
         max_offset,
         {_, {compaction_boundary, _}, _} = boundary_info
       ) do
    suffix = get_suffix(min_offset, boundary_info)

    case fetch_chunk(min_offset, opts, boundary_info) do
      # A compacted chunk holds only committed, key-merged entries, and a merge sits at the last
      # offset of its run: reading up to an offset inside such a chunk must still yield that entry,
      # so a straddling compacted chunk is read whole. The live log is different -- the writer
      # closes chunk boundaries at the write position, not the committed txn offset, so a straddling
      # latest chunk can carry rows past max_offset that were never committed, and those are bounded
      # out through the offset-aware stream below.
      {:ok, chunk_end_offset, {start_pos, end_pos}}
      when not is_nil(end_pos) and
             (is_log_offset_lte(chunk_end_offset, max_offset) or
                is_log_offset_lt(min_offset, compaction_boundary)) ->
        LogFile.stream_jsons(
          opts,
          json_file(opts, suffix),
          start_pos,
          end_pos,
          min_offset
        )
        |> Stream.concat(do_stream_from_disk(opts, chunk_end_offset, max_offset, boundary_info))

      {:ok, _chunk_end_offset, {start_pos, end_pos}} when not is_nil(end_pos) ->
        LogFile.stream_jsons_until_offset(
          opts,
          json_file(opts, suffix),
          start_pos,
          min_offset,
          max_offset
        )

      {:ok, nil, {start_pos, nil}} ->
        LogFile.stream_jsons_until_offset(
          opts,
          json_file(opts, suffix),
          start_pos,
          min_offset,
          max_offset
        )

      :error ->
        read_on_in_newer_generations(opts, min_offset, max_offset, boundary_info)
    end
  end

  # A read routes each chunk by the names it read first, and the cleanup after a switch removes
  # the generation published before the previous one. Two switches between those names and a
  # chunk lookup removed the index they named, which reads as a shape that has logged nothing:
  # the response ended there with no error, short of the offset it advertised. The names are read
  # again, and a read whose generations moved on goes on from the same offset in the newer ones.
  defp read_on_in_newer_generations(opts, min_offset, max_offset, {latest, compaction, _}) do
    case read_boundary_info(opts) do
      {^latest, ^compaction, _} -> []
      newer -> do_stream_from_disk(opts, min_offset, max_offset, newer)
    end
  end

  defp get_suffix(min_offset, {_, {compaction_boundary, compacted_name}, _})
       when is_log_offset_lt(min_offset, compaction_boundary),
       do: compacted_name

  defp get_suffix(_, {latest_name, _, _}), do: latest_name

  def append_to_log!(txn_lines, state) do
    write_log_items(txn_lines, state, with: &WriteLoop.append_to_log!/3)
  end

  @doc """
  Append log items from a transaction fragment.

  Unlike `append_to_log!/2`, this does NOT advance `last_seen_txn_offset` or
  call `register_complete_txn`. Transaction completion should be signaled
  separately via `signal_txn_commit!/2`.

  This ensures that on crash/recovery, `fetch_latest_offset` returns the
  last committed transaction offset, not a mid-transaction offset.
  """
  def supports_txn_fragment_streaming?, do: true

  def append_fragment_to_log!(txn_fragment_lines, state) do
    write_log_items(txn_fragment_lines, state, with: &WriteLoop.append_fragment_to_log!/3)
  end

  defp write_log_items(log_items, writer_state(writer_acc: acc) = state, with: write_loop_fn) do
    log_items
    |> normalize_log_stream()
    |> write_loop_fn.(acc, state)
    |> case do
      {acc, cancel_flush_timer: true} ->
        timer_ref = writer_state(state, :write_timer)
        if not is_nil(timer_ref), do: Process.cancel_timer(timer_ref)
        writer_state(state, writer_acc: acc, write_timer: nil)

      {acc, schedule_flush: times_flushed} ->
        writer_state(state, writer_acc: acc)
        |> schedule_flush(times_flushed)
    end
  end

  @doc """
  Signal that a transaction has committed.

  Updates `last_seen_txn_offset` and persists metadata to mark the transaction
  as complete. Should be called after all fragments have been written via
  `append_fragment_to_log!/2`.
  """
  # xid is not actually used here since it's not possible for transaction writes to interleave.
  # It's part of the function signature for testing.
  def signal_txn_commit!(_xid, writer_state(writer_acc: acc) = state) do
    acc = WriteLoop.signal_txn_commit(acc, state)
    writer_state(state, writer_acc: acc)
  end

  def update_chunk_boundaries_cache(opts, boundaries) do
    :ets.update_element(
      opts.stack_ets,
      opts.shape_handle,
      {storage_meta(:cached_chunk_boundaries) + 1, boundaries}
    )
  end

  # Contract for this behaviour is that for any messages with behaviour as the tag, the MFA will
  # be called with current writer state prepended, and the return value will be used as the new state
  defp schedule_flush(writer_state(writer_acc: acc, write_timer: timer, opts: opts) = state, old) do
    if WriteLoop.has_flushed_since?(acc, old) or is_nil(timer) do
      if not is_nil(timer), do: Process.cancel_timer(timer)

      flush_period = Storage.opt_for_stack(opts.stack_id, :flush_period)

      ref =
        Process.send_after(
          self(),
          {Storage, {__MODULE__, :perform_scheduled_flush, [WriteLoop.times_flushed(acc)]}},
          flush_period
        )

      writer_state(state, write_timer: ref)
    else
      state
    end
  end

  def perform_scheduled_flush(writer_state(writer_acc: acc) = state, requested) do
    if not WriteLoop.has_flushed_since?(acc, requested) do
      # No flushes happened between the scheduling of the flush and now, so we can just do a normal flush
      writer_state(state, writer_acc: WriteLoop.flush_buffer(acc, state), write_timer: nil)
    else
      state
    end
  end

  def update_global_persistence_information(
        %__MODULE__{} = opts,
        last_persisted_txn_offset,
        last_persisted_offset,
        last_seen_txn_offset,
        old_last_persisted_txn_offset
      ) do
    if old_last_persisted_txn_offset != last_persisted_txn_offset do
      if first_durable_claim?(old_last_persisted_txn_offset, last_persisted_txn_offset),
        do: sync_log_directories!(opts)

      # Durable, unlike every other metadata write: a claim less durable than the data it describes
      # loses acked transactions on machine crash, and the slot has already moved past them.
      write_metadata!(opts, :last_persisted_txn_offset, last_persisted_txn_offset, true)
    end

    update_persistence_cache(
      opts,
      last_persisted_txn_offset,
      last_persisted_offset,
      last_seen_txn_offset,
      old_last_persisted_txn_offset
    )
  end

  @doc """
  Advance the in-memory read boundaries without touching the metadata file.

  Safe for bytes that are written but not yet datasynced: reads and same-VM writer restarts
  see the file through the page cache, while a machine crash loses this cache together with
  the unsynced bytes it describes. The metadata FILE is the durability claim and only moves
  via `update_global_persistence_information/5` after a datasync.
  """
  def update_persistence_cache(
        %__MODULE__{} = opts,
        last_persisted_txn_offset,
        last_persisted_offset,
        last_seen_txn_offset,
        old_last_persisted_txn_offset
      ) do
    # This boundary describes bytes that are in the file, not bytes on the platter, so it has to
    # be recorded somewhere with the same survival envelope as those bytes. Memory is not: every
    # restart short of a machine crash keeps the file and loses the memory, and recovery trims
    # the log to whatever boundary it can find. The write carries no datasync -- buying that back
    # is the entire point of the sync split -- and runs only when the boundary moved: a commit
    # whose bytes are still buffered leaves it in place, and rewriting it anyway put a file
    # rename on every transaction of every shape.
    if old_last_persisted_txn_offset != last_persisted_txn_offset do
      write_metadata!(opts, :last_written_txn_offset, last_persisted_txn_offset)
    end

    write_metadata_cache(
      opts,
      last_persisted_txn_offset: last_persisted_txn_offset,
      last_persisted_offset: last_persisted_offset,
      last_seen_txn_offset: last_seen_txn_offset
    )
  rescue
    ArgumentError -> true
  end

  defp normalize_log_stream(stream) do
    Stream.map(stream, fn
      # Pre-framed class block: bytes were framed once by the routing side; the write loop
      # appends it wholesale.
      %Electric.Replication.Changes.FramedBlock{} = block ->
        block

      {log_offset, key, op_type, json} ->
        {log_offset, byte_size(key), key, get_op_type(op_type), 0, byte_size(json), json}

      # Prepared line carrying a payload-arena ref: key and JSON live in the arena, the log
      # entry stores only the ref. Key size is 0 on disk; compaction restores it from the
      # arena when inlining. The composite payload slot keeps the JSON for the live-tail ETS
      # mirror and the real payload size for chunk accounting.
      {log_offset, _key, op_type, json, ref} when is_binary(ref) ->
        {log_offset, 0, "", get_op_type(op_type), LogFile.ref_flag(), byte_size(ref),
         {ref, json, byte_size(json)}}

      {_, _, _, _, _, _, _} = formed_line ->
        formed_line
    end)
  end

  defp get_op_type(:insert), do: ?i
  defp get_op_type(:update), do: ?u
  defp get_op_type(:delete), do: ?d

  defp mkdir_p!(path) do
    case FileInfo.mkdir_p(path) do
      :ok -> :ok
      {:error, reason} -> raise Storage.Error, message: inspect(reason)
    end
  end

  defp rm_rf!(path) do
    File.rm_rf!(path)
  end

  defp rename!(path1, path2) do
    case FileInfo.rename(path1, path2) do
      :ok ->
        :ok

      {:error, reason} ->
        raise Storage.Error, message: inspect(reason) <> " while renaming #{path1} to #{path2}"
    end
  end

  @doc false
  def chunk_file(%__MODULE__{} = opts, suffix) do
    shape_log_path(opts, "log.#{suffix}.chunk.bin")
  end

  @doc false
  def json_file(%__MODULE__{} = opts, suffix) do
    shape_log_path(opts, "log.#{suffix}.jsonfile.bin")
  end

  @doc false
  def key_file(%__MODULE__{} = opts, suffix) do
    shape_log_path(opts, "log.#{suffix}.keyfile.bin")
  end

  @type file_opener() :: (Path.t(), [File.mode()] ->
                            {:ok, File.file_descriptor()} | {:error, File.posix()})
  @doc false
  # Open a file for streaming. Returns `{:halt, :data_removed}` if the shape data has been
  # removed (i.e. the shape itself has been removed). Streaming functions should return
  # an empty stream rather than raise if this returns `{:halt, :data_removed}`.
  @spec safely_open_file!(%__MODULE__{}, Path.t(), [File.mode()], file_opener()) ::
          {:ok, File.file_descriptor()} | {:halt, :data_removed} | no_return()
  def safely_open_file!(%__MODULE__{} = opts, path, modes, open_fun \\ &File.open/2) do
    case open_fun.(path, modes) do
      {:ok, file} ->
        {:ok, file}

      {:error, _reason} = error ->
        handle_open_error!(opts, path, error)
    end
  end

  defp handle_open_error!(%__MODULE__{} = opts, path, {:error, :enoent}) do
    if shape_gone?(opts) do
      {:halt, :data_removed}
    else
      raise File.Error, path: path, reason: :enoent
    end
  end

  defp handle_open_error!(%__MODULE__{}, path, {:error, reason}) do
    raise File.Error, path: path, reason: reason
  end

  defp shape_gone?(%__MODULE__{} = opts) do
    !FileInfo.exists?(shape_data_dir(opts))
  end

  defp remove_unnested_storage(base_dir) do
    "#{base_dir}/*"
    |> Path.wildcard()
    |> Enum.filter(fn path ->
      name = Path.basename(path)
      Regex.match?(~r/\d{6,}-\d{8,}/, name)
    end)
    |> tap(fn dirs ->
      Logger.notice("Removing #{length(dirs)} old storage directories")
    end)
    |> Enum.each(fn path ->
      # remove with a delay - don't want this task to interfere with critical processes
      File.rm_rf(path)
      Process.sleep(20)
    end)
  end
end
