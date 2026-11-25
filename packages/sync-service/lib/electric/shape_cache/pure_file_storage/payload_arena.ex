defmodule Electric.ShapeCache.PureFileStorage.PayloadArena do
  @moduledoc false
  # Shared payload store for fan-out-class log items.
  #
  # When a change reaches N shapes of the same fan-out class, the router encodes the log-item
  # JSON once — but each of the N consumers still wrote its own copy of those bytes into its
  # own log file. The arena stores `key <> json` exactly once, appended by the routing process
  # at prepare time, and the per-shape logs store a fixed-size ref instead
  # (see `Electric.ShapeCache.PureFileStorage.LogFile` for the on-disk framing).
  #
  # Durability contract: the arena is datasynced at every commit fragment BEFORE the routing
  # decisions are handed to consumers, so any ref a consumer can ever flush is already backed
  # by durable arena bytes. Mid-transaction refs may outlive the arena tail on crash, but
  # recovery trims every shape log to its last committed transaction boundary and the
  # replication slot was never acked past it, so Postgres resends those transactions and the
  # payloads are re-appended. A dangling ref is therefore an invariant violation, and the
  # reader reports one as data loss.
  #
  # The arena is shared, so a flipped byte in it is not one shape's problem: every shape in the
  # fan-out class reads the same payload. Each ref therefore carries a CRC32 of the payload and
  # one of the key compaction merges on, checked on every read, so corruption surfaces as a
  # classified failure that discards the shape rather than as a wrong row served to a client.
  #
  # Each writer boot opens a fresh generation file and never appends to an old one, so a
  # crash-garbage tail in a previous generation is dead bytes, never a hazard.
  #
  # A generation is reclaimed once no surviving shape log resolves a ref into it -- compaction
  # inlines refs and shape deletion drops them, so churn eventually empties a generation of
  # references. `reclaim_generations!/2` deletes those, keeping only the generations a caller-
  # supplied scan of every surviving log found reachable; without it, generations that had held a
  # payload accumulated like the pre-arena per-shape copies did, bounded by nothing but the volume.

  alias Electric.ShapeCache.Storage
  alias Electric.Utils

  @arena_dir "payload-arena"
  @buffer_bytes 512 * 1024

  @doc """
  The name of the arena's directory in the store's base path.
  """
  @spec dir_name() :: String.t()
  def dir_name, do: @arena_dir

  defmodule Writer do
    @moduledoc false
    defstruct [:dir, :file, :gen, position: 0, buffer: [], buffer_size: 0, dirty?: false]
  end

  @doc """
  Open a fresh generation for appending. `discriminator` keeps concurrently-opened writers
  (one per shard worker) from racing for the same generation id: the low byte of the id is
  the discriminator, the rest is a timestamp.
  """
  @spec open_writer!(String.t(), non_neg_integer()) :: %Writer{}
  def open_writer!(base_path, discriminator \\ 0) when discriminator in 0..255 do
    dir = Path.join(base_path, @arena_dir)
    File.mkdir_p!(dir)
    Utils.sync_dir!(base_path)

    gen = fresh_gen(dir, System.system_time(:millisecond) * 256 + discriminator)
    file = File.open!(gen_path(dir, gen), [:write, :raw, :binary])

    # `sync!/1` datasyncs a generation's bytes but never the entries naming it, and a reader
    # reaches a generation only by name, so a lost entry turns refs already published as durable
    # into dangling ones -- read as data loss on a shape whose payload bytes never left the disk.
    Utils.sync_dir!(dir)

    %Writer{dir: dir, file: file, gen: gen}
  end

  # A generation id names the file every ref pointing into it resolves through, and the wall
  # clock it is stamped from steps backwards on an NTP correction or a VM migration. A backwards
  # step could otherwise hand a new writer the id of a still-referenced generation, and surviving
  # refs would resolve to whatever the new file holds at that offset. Starting past the highest id
  # already on disk keeps the sequence increasing whatever the clock does, in whole steps of 256 so
  # the discriminator survives. `reclaim_generations!/2` only ever removes generations no ref
  # reaches, so an id it frees has no surviving ref to collide with even if the clock lands on it.
  defp fresh_gen(dir, candidate) do
    case highest_gen(dir) do
      highest when is_integer(highest) and highest >= candidate ->
        candidate + 256 * div(highest - candidate + 256, 256)

      _ ->
        candidate
    end
  end

  defp highest_gen(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        entries
        |> Enum.flat_map(fn entry ->
          case Integer.parse(entry) do
            {gen, ".bin"} -> [gen]
            _ -> []
          end
        end)
        |> case do
          [] -> nil
          gens -> Enum.max(gens)
        end

      {:error, _} ->
        nil
    end
  end

  defp gen_path(dir, gen), do: Path.join(dir, "#{gen}.bin")

  @doc """
  Append one payload, returning the ref binary that the per-shape log entries store.
  """
  @spec append!(%Writer{}, key :: binary(), json :: binary()) :: {%Writer{}, ref :: binary()}
  def append!(%Writer{} = writer, key, json) when is_binary(key) and is_binary(json) do
    key_size = byte_size(key)
    json_size = byte_size(json)

    ref =
      <<json_size::64, key_size::32, writer.gen::64, writer.position::64, :erlang.crc32(json)::32,
        :erlang.crc32(key)::32>>

    writer = %{
      writer
      | buffer: [writer.buffer, key, json],
        buffer_size: writer.buffer_size + key_size + json_size,
        position: writer.position + key_size + json_size,
        dirty?: true
    }

    {maybe_flush(writer), ref}
  end

  defp maybe_flush(%Writer{buffer_size: size} = writer) when size >= @buffer_bytes,
    do: flush(writer)

  defp maybe_flush(writer), do: writer

  defp flush(%Writer{buffer_size: 0} = writer), do: writer

  # Positioned write, never sequential: the router discards a partially-appended writer when
  # a class preparation fails mid-change, rolling `position` back. Any bytes such a discarded
  # suffix already flushed are then simply overwritten by the next flush at the same
  # position, instead of desyncing the file from the tracked position.
  defp flush(%Writer{} = writer) do
    :ok = :file.pwrite(writer.file, writer.position - writer.buffer_size, writer.buffer)
    %{writer | buffer: [], buffer_size: 0}
  end

  @doc """
  Make everything appended so far durable. Called at commit boundaries before the routing
  decisions leave the routing process.
  """
  @spec sync!(%Writer{}) :: %Writer{}
  def sync!(%Writer{dirty?: false} = writer), do: writer

  def sync!(%Writer{} = writer) do
    writer = flush(writer)
    :ok = :file.datasync(writer.file)
    %{writer | dirty?: false}
  end

  @doc """
  Resolve ref elements of a log stream into their payload JSONs, passing inline JSONs through.

  Consumes elements shaped `json :: binary()` or `{:ref, ref :: binary()}` (as emitted by
  `LogFile` extraction) and emits JSON binaries in order. Generation file handles are opened
  lazily per stream and closed when the stream halts.
  """
  @spec resolve_refs(Enumerable.t(), String.t(), (String.t() -> no_return())) ::
          Enumerable.t(binary())
  def resolve_refs(stream, base_path, report_data_loss \\ &raise_data_loss!/1) do
    dir = Path.join(base_path, @arena_dir)

    Stream.transform(
      stream,
      fn -> %{} end,
      fn
        json, files when is_binary(json) ->
          {[json], files}

        {:ref, ref}, files ->
          {json_size, key_size, gen, pos, checksum, _key_checksum} = parse_ref(ref)
          {file, files} = gen_file(files, dir, gen, report_data_loss)

          case :file.pread(file, pos + key_size, json_size) do
            {:ok, json} when byte_size(json) == json_size ->
              verify!("payload", json, checksum, gen, pos, report_data_loss)
              {[json], files}

            other ->
              report_data_loss.(
                "dangling payload-arena ref: gen=#{gen} pos=#{pos} size=#{json_size} -> #{inspect(other)}"
              )
          end
      end,
      fn files -> Enum.each(files, fn {_gen, file} -> File.close(file) end) end
    )
  end

  defp parse_ref(
         <<json_size::64, key_size::32, gen::64, pos::64, checksum::32, key_checksum::32>>
       ),
       do: {json_size, key_size, gen, pos, checksum, key_checksum}

  # Backwards compatibility: refs written before the key checksum existed carry only the payload
  # checksum, and refs written before either carry neither. Both are read with the coverage they
  # have until no pre-checksum shape logs remain on disk.
  defp parse_ref(<<json_size::64, key_size::32, gen::64, pos::64, checksum::32>>),
    do: {json_size, key_size, gen, pos, checksum, nil}

  defp parse_ref(<<json_size::64, key_size::32, gen::64, pos::64>>),
    do: {json_size, key_size, gen, pos, nil, nil}

  @doc """
  The generation a ref resolves through. Sits at a fixed offset the checksum variants share, so
  the reclaimer reads it without caring which one the log wrote.
  """
  @spec ref_generation(binary()) :: non_neg_integer()
  def ref_generation(<<_json_size::64, _key_size::32, gen::64, _rest::binary>>), do: gen

  defp verify!(_part, _bytes, nil, _gen, _pos, _report_data_loss), do: :ok

  defp verify!(part, bytes, checksum, gen, pos, report_data_loss) do
    case :erlang.crc32(bytes) do
      ^checksum ->
        :ok

      actual ->
        report_data_loss.(
          "corrupted payload-arena #{part}: gen=#{gen} pos=#{pos} size=#{byte_size(bytes)} " <>
            "checksum=#{checksum} actual=#{actual}"
        )
    end
  end

  defp raise_data_loss!(message), do: raise(Storage.Error, kind: :data_loss, message: message)

  @doc """
  Delete generations that were opened but never appended to.

  A writer creates its generation file eagerly, so every boot leaves one empty file per
  writer behind and nothing reclaims it. An empty generation holds no payload, so no ref
  anywhere can resolve through it and dropping it is always safe -- but only while no writer
  is live, or the unlink would strand a writer that already holds the file open and every ref
  it goes on to hand out would dangle. The routing process links every writer it opens, so
  its own init is the one moment where that is guaranteed, and it is where this is called.
  """
  @spec prune_empty_generations!(String.t()) :: :ok
  def prune_empty_generations!(base_path) do
    dir = Path.join(base_path, @arena_dir)

    with {:ok, entries} <- File.ls(dir) do
      Enum.each(entries, fn entry ->
        path = Path.join(dir, entry)

        case {Integer.parse(entry), File.stat(path)} do
          {{_gen, ".bin"}, {:ok, %File.Stat{size: 0}}} -> File.rm(path)
          _ -> :ok
        end
      end)
    end

    :ok
  end

  @doc """
  Bytes held by the generations on disk.

  Read before this boot's writers open, it is the most `reclaim_generations!/2` could give back:
  every generation on disk was then left by an earlier boot.
  """
  @spec stored_bytes(String.t()) :: non_neg_integer()
  def stored_bytes(base_path) do
    dir = Path.join(base_path, @arena_dir)

    case File.ls(dir) do
      {:ok, entries} ->
        Enum.reduce(entries, 0, fn entry, bytes ->
          case {Integer.parse(entry), File.stat(Path.join(dir, entry))} do
            {{_gen, ".bin"}, {:ok, %File.Stat{size: size}}} -> bytes + size
            _ -> bytes
          end
        end)

      {:error, _} ->
        0
    end
  end

  @doc """
  Delete every generation not named in `keep_gens`.

  `keep_gens` is the set of generations a surviving shape log still resolves a ref into, which the
  caller computes by scanning those logs. A generation absent from it holds only payloads no ref
  reaches -- compaction inlined them, or the shapes that held them were deleted -- so its bytes are
  dead and reclaiming them is what bounds arena disk under sustained churn; without this only empty
  generations were ever pruned and every generation that had held a payload accumulated forever.

  Subsumes `prune_empty_generations!/1`: an empty generation is reachable from no ref, so it is
  never in `keep_gens`. Carries that function's safety condition unchanged -- a generation is
  dropped only while no writer holds it open, which is why the routing process calls this from its
  own init, before it opens this boot's writer. The caller must pass a set built from every
  surviving log; a generation missing from an incomplete scan would be read as dead and its live
  payloads lost, so a caller that cannot scan a log completely must not call this.
  """
  @spec reclaim_generations!(String.t(), MapSet.t(non_neg_integer())) :: :ok
  def reclaim_generations!(base_path, keep_gens) do
    dir = Path.join(base_path, @arena_dir)

    with {:ok, entries} <- File.ls(dir) do
      Enum.each(entries, fn entry ->
        case Integer.parse(entry) do
          {gen, ".bin"} ->
            if not MapSet.member?(keep_gens, gen), do: File.rm(Path.join(dir, entry))

          _ ->
            :ok
        end
      end)
    end

    :ok
  end

  @doc """
  Whether this stack has ever written arena generations. When it hasn't, no log anywhere in
  the stack can contain ref entries, so ref-aware pre-passes (compaction inlining) can be
  skipped entirely.
  """
  @spec any_generations?(String.t()) :: boolean()
  def any_generations?(base_path) do
    match?({:ok, [_ | _]}, File.ls(Path.join(base_path, @arena_dir)))
  end

  defmodule Reader do
    @moduledoc false
    defstruct [:dir, files: %{}]
  end

  @doc """
  Handle-caching reader for bulk ref materialization (compaction inlining). Read back the
  full `{key, json}` payload of one ref.
  """
  @spec open_reader(String.t()) :: %Reader{}
  def open_reader(base_path), do: %Reader{dir: Path.join(base_path, @arena_dir)}

  @spec read_payload!(%Reader{}, ref :: binary()) ::
          {%Reader{}, {key :: binary(), json :: binary()}}
  def read_payload!(%Reader{} = reader, ref) do
    {json_size, key_size, gen, pos, checksum, key_checksum} = parse_ref(ref)
    {file, files} = gen_file(reader.files, reader.dir, gen, &raise_data_loss!/1)

    case :file.pread(file, pos, key_size + json_size) do
      {:ok, <<key::binary-size(^key_size), json::binary-size(^json_size)>>} ->
        verify!("payload", json, checksum, gen, pos, &raise_data_loss!/1)
        verify!("key", key, key_checksum, gen, pos, &raise_data_loss!/1)
        {%{reader | files: files}, {key, json}}

      other ->
        raise_data_loss!("dangling payload-arena ref: gen=#{gen} pos=#{pos} -> #{inspect(other)}")
    end
  end

  def close_reader!(%Reader{files: files}) do
    Enum.each(files, fn {_gen, file} -> File.close(file) end)
    :ok
  end

  defp gen_file(files, dir, gen, report_data_loss) do
    case files do
      %{^gen => file} -> {file, files}
      _ -> open_gen(files, dir, gen, report_data_loss)
    end
  end

  # A generation that is not on disk is the same loss as a ref pointing past the end of one, and
  # only the classified form reaches the shape's discard-and-refetch path. Every other open
  # failure is the machine rather than the bytes, and a rebuild costs more of whatever ran out.
  defp open_gen(files, dir, gen, report_data_loss) do
    path = gen_path(dir, gen)

    case File.open(path, [:read, :raw, :binary]) do
      {:ok, file} ->
        {file, Map.put(files, gen, file)}

      {:error, :enoent} ->
        report_data_loss.("missing payload-arena generation: gen=#{gen} path=#{path}")

      {:error, reason} ->
        raise Storage.Error,
          kind: :transient,
          message: "could not open payload-arena generation #{gen}: #{inspect(reason)}"
    end
  end
end
