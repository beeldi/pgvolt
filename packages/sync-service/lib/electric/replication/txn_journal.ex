defmodule Electric.Replication.TxnJournal do
  @moduledoc false
  # Durable ingest journal for ack-after-journal mode (ELECTRIC_ACK_AFTER_JOURNAL).
  #
  # The replication client owns the writer: it appends every replication event (transaction
  # fragments and relation messages) here exactly as dispatched — before routing or any
  # consumer write — datasyncs at commit fragments, and only then acks the slot, without
  # waiting for the per-shape fan-out. It also runs the GC, driven by the consumer-durable
  # boundary the collector's FlushTracker notifies it of.
  #
  # The collector only reads: at boot it replays the generations left behind through the
  # normal routing path (per-shape storage skips what it already holds), so Postgres never
  # resends an acked transaction. The slot is never acked past a sync, which makes a journal
  # entry that cannot be read back at boot an invariant violation.
  #
  # Frame: <<byte_size(bin)::32, :erlang.crc32(bin)::32, bin::binary>> with bin the
  # term_to_binary of the event. Each writer boot opens a fresh generation file and rolls
  # it at @roll_bytes; a closed generation is deleted once the consumer-durable flush
  # boundary passes its last commit, so steady-state disk usage is bounded by consumer lag.

  alias Electric.Replication.LogOffset
  alias Electric.Utils

  @journal_dir "txn-journal"
  @buffer_bytes 512 * 1024
  @roll_bytes 64 * 1024 * 1024

  defmodule Writer do
    @moduledoc false
    defstruct [
      :dir,
      :file,
      :gen,
      position: 0,
      buffer: [],
      buffer_size: 0,
      dirty?: false,
      last_synced_tx_offset: nil,
      closed_gens: [],
      committed_recorded?: false
    ]
  end

  @doc """
  Whether journal mode has ever written a generation under this base path.

  Kept OUTSIDE the journal directory on purpose: its whole job is to be readable when that
  directory is not. Losing the storage tree wholesale — the operator's own reset — takes the
  marker with it, so a genuine reset still reads as first boot.
  """
  @spec used?(String.t()) :: boolean()
  def used?(base_path), do: File.exists?(used_marker_path(base_path))

  defp used_marker_path(base_path), do: Path.join(base_path, "#{@journal_dir}.used")

  @doc """
  The names the journal keeps in its base path: its directory, its used marker and its coverage
  record. A base path whose journal shows a high water names one of them.
  """
  @spec base_path_names() :: [String.t()]
  def base_path_names, do: [@journal_dir, "#{@journal_dir}.used", "#{@journal_dir}.coverage"]

  defp used!(base_path) do
    path = used_marker_path(base_path)

    case File.stat(path) do
      {:ok, _stat} -> true
      {:error, :enoent} -> marker_entry?(path)
      {:error, reason} -> raise File.Error, reason: reason, action: "read file stats", path: path
    end
  end

  # The marker says what it says by being there, so one that is a symlink whose target is gone,
  # which a stat reads as absent, still says the journal wrote a generation. So does one the looks
  # miss in a base path whose target is away for them, if its listing finds the marker. A base path
  # gone for the looks and its listing is `:absent`.
  defp marker_entry?(path) do
    case File.lstat(path) do
      {:ok, _stat} ->
        true

      {:error, :enoent} ->
        case base_entries!(Path.dirname(path)) do
          :absent -> :absent
          names -> Path.basename(path) in names
        end

      {:error, reason} ->
        raise File.Error, reason: reason, action: "read file stats", path: path
    end
  end

  # The names in the journal's base path, which lists an entry a look does not find there: one
  # that does not resolve, or one in a base path that is a symlink whose target was away for the
  # look. A base path that does not list is `:absent` only if a look after the listing does not
  # find it either. One there by then fails the scan: a symlink whose target is back, or a
  # directory a remount brought back, holds the journal again.
  defp base_entries!(base_path) do
    case File.ls(base_path) do
      {:ok, names} ->
        names

      {:error, reason} when reason in [:enoent, :enotdir] ->
        case File.lstat(base_path) do
          {:error, absent} when absent in [:enoent, :enotdir] -> :absent
          _ -> raise File.Error, reason: reason, action: "list directory", path: base_path
        end

      {:error, reason} ->
        raise File.Error, reason: reason, action: "list directory", path: base_path
    end
  end

  defp coverage_marker_path(base_path), do: Path.join(base_path, "#{@journal_dir}.coverage")

  @doc """
  What the generations on disk no longer have to prove.

  `committed?` turns true the first time a boundary is synced into this journal and never goes
  back: a journal that never held one cannot have lost one, and that is what separates a stack
  which has not acked anything yet from one whose acknowledged history is gone. `reclaimed` is
  the highest boundary whose generation was deleted because the consumers were durable past it --
  data the journal is entitled not to hold any more, and which the readable generations must
  therefore not be asked to account for.

  Kept OUTSIDE the journal directory for the same reason as the used-marker: its whole job is to
  be readable when that directory is not.
  """
  @spec coverage_record(String.t()) :: %{
          committed?: boolean(),
          reclaimed: non_neg_integer() | nil
        }
  def coverage_record(base_path) do
    with {:ok, bin} <- File.read(coverage_marker_path(base_path)),
         %{committed?: _, reclaimed: _} = record <- safe_decode(bin) do
      record
    else
      _ -> %{committed?: false, reclaimed: nil}
    end
  end

  defp safe_decode(bin) do
    :erlang.binary_to_term(bin, [:safe])
  rescue
    ArgumentError -> nil
  end

  @doc """
  Record that a boundary was synced into the journal, or that generations covering one were
  reclaimed. Both facts only ever move forward, and both are written durably: a claim less
  durable than the generations it accounts for would be lost by exactly the crash it exists to
  classify.
  """
  @spec record_coverage!(String.t(), keyword()) :: :ok
  def record_coverage!(base_path, updates) do
    current = coverage_record(base_path)

    reclaimed =
      case {current.reclaimed, Keyword.get(updates, :reclaimed)} do
        {nil, new} -> new
        {old, nil} -> old
        {old, new} -> max(old, new)
      end

    record = %{
      committed?: current.committed? or Keyword.get(updates, :committed?, false),
      reclaimed: reclaimed
    }

    if record != current do
      path = coverage_marker_path(base_path)
      tmp = path <> ".tmp"
      File.open!(tmp, [:write, :raw, :sync], &:file.write(&1, :erlang.term_to_binary(record)))
      File.rename!(tmp, path)
      Utils.sync_dir!(base_path)
    end

    :ok
  end

  defp mark_used!(base_path) do
    path = used_marker_path(base_path)

    unless File.exists?(path) do
      File.open!(path, [:write, :raw, :sync], &:file.write(&1, "1"))
      Utils.sync_dir!(base_path)
    end
  end

  @spec open_writer!(String.t()) :: %Writer{}
  def open_writer!(base_path) do
    dir = Path.join(base_path, @journal_dir)
    File.mkdir_p!(dir)
    seal_tail!(dir)

    gen = fresh_gen(dir, System.system_time(:millisecond) * 256)
    file = File.open!(gen_path(dir, gen), [:write, :raw, :binary])

    # `do_sync/2` datasyncs a generation's bytes but never the entry naming it. Every reader reaches
    # a generation by name, so a lost entry is indistinguishable from a journal that never existed --
    # and the boot guard reads that as "nothing to replay" instead of the invariant violation it is,
    # turning acked-but-unreplayed transactions into silent loss.
    Utils.sync_dir!(dir)

    # The marker is a claim that acknowledged transactions have a journal behind them, so it comes
    # only once a generation can be found by name. Made first, a crash or failure before the entry
    # was durable left a marker beside an empty journal directory, which every later boot reads as
    # an acknowledged journal that was lost.
    mark_used!(base_path)

    %Writer{dir: dir, file: file, gen: gen}
  end

  # A crash mid-write can leave the newest generation ending in a partial frame, and every read
  # accepts that tail only while the generation stays the newest. The fresh generation opened next
  # takes that place, so the tail is cut back to the last whole frame first, durably: left in place
  # it reads as corruption from then on, at every boot. Those bytes were never datasynced, so the
  # slot was never acked past them and Postgres resends them.
  defp seal_tail!(dir) do
    with gen when is_integer(gen) <- highest_gen(dir),
         path = gen_path(dir, gen),
         {:ok, %File.Stat{size: size}} <- File.stat(path),
         whole when whole < size <- whole_frames_bytes(path) do
      File.open!(path, [:read, :write, :raw, :binary], fn file ->
        {:ok, ^whole} = :file.position(file, whole)
        :ok = :file.truncate(file)
        :ok = :file.datasync(file)
      end)
    end

    :ok
  end

  defp whole_frames_bytes(path) do
    File.open!(path, [:read, :raw, :binary, {:read_ahead, @buffer_bytes}], fn file ->
      Stream.repeatedly(fn -> read_frame(file) end)
      |> Enum.reduce_while(0, fn
        {:ok, bin}, bytes -> {:cont, bytes + byte_size(bin) + 8}
        _end_or_torn, bytes -> {:halt, bytes}
      end)
    end)
  end

  @doc """
  Take ownership of generations left by previous boots so the regular consumer-boundary GC
  reclaims them. A generation holding no commit was never acked into — Postgres re-streams
  its content — so it is marked reclaimable at the first boundary that arrives.
  """
  @spec adopt_generations!(%Writer{}, String.t()) :: %Writer{}
  def adopt_generations!(%Writer{} = writer, base_path) do
    dir = Path.join(base_path, @journal_dir)
    gens = list_generations(base_path)
    newest = List.last(gens)

    adopted =
      gens
      |> Enum.reject(&(&1 == writer.gen))
      |> Enum.map(fn gen -> {gen, last_commit_in_generation(dir, gen, gen == newest) || -1} end)

    %{writer | closed_gens: adopted ++ writer.closed_gens}
  end

  # Generation ids order replay: `list_generations/1` sorts them, and `stream_events!/2`
  # tolerates a torn tail only in the last one. They are stamped from the wall clock, which
  # steps backwards on an NTP correction or a VM migration — a generation opened after such
  # a step would sort ahead of the ones it follows, replaying out of order and reading the
  # live, half-written file strictly. Starting past the highest id already on disk keeps the
  # sequence increasing whatever the clock does, in whole steps of 256 so the low byte the
  # caller may use as a discriminator survives.
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
        case Enum.flat_map(entries, &parse_gen/1) do
          [] -> nil
          gens -> Enum.max(gens)
        end

      {:error, _} ->
        nil
    end
  end

  defp parse_gen(entry) do
    case Integer.parse(entry) do
      {gen, ".bin"} -> [gen]
      _ -> []
    end
  end

  defp gen_path(dir, gen), do: Path.join(dir, "#{gen}.bin")

  @spec append_event!(%Writer{}, term()) :: %Writer{}
  def append_event!(%Writer{} = writer, event) do
    bin = :erlang.term_to_binary(event)
    frame = [<<byte_size(bin)::32, :erlang.crc32(bin)::32>>, bin]
    frame_size = byte_size(bin) + 8

    writer = %{
      writer
      | buffer: [writer.buffer, frame],
        buffer_size: writer.buffer_size + frame_size,
        position: writer.position + frame_size,
        dirty?: true
    }

    maybe_flush(writer)
  end

  defp maybe_flush(%Writer{buffer_size: size} = writer) when size >= @buffer_bytes,
    do: flush(writer)

  defp maybe_flush(writer), do: writer

  defp flush(%Writer{buffer_size: 0} = writer), do: writer

  defp flush(%Writer{} = writer) do
    :ok = :file.pwrite(writer.file, writer.position - writer.buffer_size, writer.buffer)
    %{writer | buffer: [], buffer_size: 0}
  end

  @doc """
  Make everything appended so far durable and record the commit boundary it covers.
  Called at commit fragments before the slot boundary is advanced past them. The 1-arity
  form (relation messages) syncs without moving the recorded boundary.
  """
  @spec sync!(%Writer{}) :: %Writer{}
  def sync!(%Writer{} = writer), do: do_sync(writer, writer.last_synced_tx_offset)

  @spec sync!(%Writer{}, non_neg_integer() | nil) :: %Writer{}
  def sync!(%Writer{} = writer, commit_tx_offset) do
    writer
    |> mark_committed(commit_tx_offset)
    |> do_sync(commit_tx_offset)
    |> maybe_roll()
  end

  # One durable write in the life of a volume, guarded by an in-memory flag: everything after the
  # first synced boundary is already covered by the same claim.
  defp mark_committed(%Writer{committed_recorded?: true} = writer, _commit_tx_offset), do: writer
  defp mark_committed(%Writer{} = writer, nil), do: writer

  defp mark_committed(%Writer{} = writer, _commit_tx_offset) do
    record_coverage!(Path.dirname(writer.dir), committed?: true)
    %{writer | committed_recorded?: true}
  end

  defp do_sync(%Writer{} = writer, commit_tx_offset) do
    writer =
      if writer.dirty? do
        writer = flush(writer)
        :ok = :file.datasync(writer.file)
        %{writer | dirty?: false}
      else
        writer
      end

    %{writer | last_synced_tx_offset: commit_tx_offset}
  end

  # Rolling only at a COMMIT sync keeps closed generations torn-tail free and correctly
  # labelled: their last frame is always covered by a datasync and is always the commit
  # they are labelled with. Rolling at a relation sync, which happens mid transaction,
  # would label the closed generation with the PREVIOUS commit — and GC would then delete
  # it, taking the open transaction's leading fragments with it, as soon as that previous
  # commit became consumer-durable.
  defp maybe_roll(%Writer{position: position} = writer) when position < @roll_bytes, do: writer

  defp maybe_roll(%Writer{} = writer) do
    File.close(writer.file)

    gen = fresh_gen(writer.dir, System.system_time(:millisecond) * 256)
    file = File.open!(gen_path(writer.dir, gen), [:write, :raw, :binary])
    Utils.sync_dir!(writer.dir)

    %{
      writer
      | file: file,
        gen: gen,
        position: 0,
        closed_gens: [{writer.gen, writer.last_synced_tx_offset} | writer.closed_gens]
    }
  end

  @doc """
  Drop closed generations whose last commit the consumer-durable boundary has passed.
  The live generation is never deleted.
  """
  @spec gc!(%Writer{}, non_neg_integer()) :: %Writer{}
  def gc!(%Writer{} = writer, consumer_boundary_tx_offset) do
    {dead, kept} =
      Enum.split_with(writer.closed_gens, fn {_gen, last_tx_offset} ->
        is_integer(last_tx_offset) and last_tx_offset <= consumer_boundary_tx_offset
      end)

    # Persist reclaimed coverage before the delete: a crash in between drops these generations'
    # boundary from the next boot's journal coverage and fails the restart check.
    case Enum.filter(dead, fn {_gen, last} -> is_integer(last) and last >= 0 end) do
      [] ->
        :ok

      reclaimed ->
        record_coverage!(
          Path.dirname(writer.dir),
          reclaimed: reclaimed |> Enum.map(fn {_gen, last} -> last end) |> Enum.max()
        )
    end

    Enum.each(dead, fn {gen, _} -> File.rm(gen_path(writer.dir, gen)) end)

    %{writer | closed_gens: kept}
  end

  @doc """
  Delete the given generations outright. Used by the boot replay when no journal
  writer will run this boot (mode off): nothing else reclaims them, and a fully
  replayed journal left on disk trips the replay coverage check on the next boot
  once the slot advances past its last commit.
  """
  @spec delete_generations!(String.t(), [non_neg_integer()]) :: :ok
  def delete_generations!(base_path, gens) do
    dir = Path.join(base_path, @journal_dir)
    Enum.each(gens, fn gen -> File.rm(gen_path(dir, gen)) end)
    :ok
  end

  @spec list_generations(String.t()) :: [non_neg_integer()]
  def list_generations(base_path) do
    case File.ls(Path.join(base_path, @journal_dir)) do
      {:ok, entries} -> entries |> Enum.flat_map(&parse_gen/1) |> Enum.sort()
      {:error, _} -> []
    end
  end

  # A journal that cannot be listed reads as empty to `list_generations/1`, and its high water
  # would fall back to the reclaimed coverage, behind the transactions its generations hold: a
  # promotion check that took it would keep shapes the old primary streamed past the switch point.
  # A journal that lost every generation, its directory with them, reads the same: in journal mode
  # one that wrote a generation always keeps one (the collector's boot guard). A run with the mode
  # off deletes the last once the consumers are durable past it, and may go on taking transactions
  # with no journal: the coverage it leaves cannot tell whether it did, and like a journal that
  # holds nothing yet, it cannot bound what the run took. The marker is read before the listing,
  # as that guard reads it: a writer opening between the two names its generation first. A marker
  # that cannot be read fails the scan, where `used?/1` reads it as absent. A directory the listing
  # does not find is not there only if the base path's listing does not name it either: a base path
  # that is a symlink whose target is away, or one a remount takes away, would read to both as a
  # journal that never wrote, and one back before the coverage is read would give that coverage as
  # the high water. A base path the scan finds gone, at the marker's looks or at the listing, is
  # `:absent` whatever a read after finds there. One the base path names is listed again, and fails
  # the scan if it still cannot be.
  defp list_generations!(base_path) do
    dir = Path.join(base_path, @journal_dir)
    used? = used!(base_path)

    gens =
      case File.ls(dir) do
        {:error, :enoent} ->
          case base_entries!(base_path) do
            :absent -> :absent
            names -> if @journal_dir in names, do: generations!(dir, File.ls(dir)), else: []
          end

        listing ->
          generations!(dir, listing)
      end

    cond do
      :absent in [used?, gens] ->
        :absent

      gens == [] and used? ->
        raise "the transaction journal at #{dir} holds no generation, though it has written one"

      true ->
        gens
    end
  end

  defp generations!(_dir, {:ok, entries}),
    do: entries |> Enum.flat_map(&parse_gen/1) |> Enum.sort()

  defp generations!(dir, {:error, reason}),
    do: raise(File.Error, reason: reason, action: "list directory", path: dir)

  @doc """
  Stream the journaled events of the given generations in append order.

  An unreadable frame in the newest generation ends the stream: generations roll only at
  datasync boundaries, so a torn frame there is the crash tail and everything the slot was
  acked past lies before it. The same frame in an older generation is not a tail — media
  corruption, a truncated file, a partially restored volume — and skipping it leaves a
  silent gap in the middle of the replay, so it raises. The caller's coverage check cannot
  catch that on its own: it reads through this stream and would see the surviving suffix.

  Which generation is the crash tail is a fact about the journal, not about `gens`: it is the
  newest one on disk. Deriving it from the argument instead would hand tail tolerance to the
  last element of whatever list arrives, so a single-generation read — the shape every
  per-generation caller uses — would silently accept corruption anywhere in any generation.
  """
  @spec stream_events!(String.t(), [non_neg_integer()]) :: Enumerable.t()
  def stream_events!(base_path, gens) do
    dir = Path.join(base_path, @journal_dir)
    newest = base_path |> list_generations() |> List.last()

    Stream.flat_map(gens, fn gen -> stream_generation(dir, gen, gen == newest) end)
  end

  defp stream_generation(dir, gen, tail?) do
    gen_path(dir, gen)
    |> frames(tail?)
    |> Stream.map(&:erlang.binary_to_term/1)
  end

  defp frames(path, tail?) do
    Stream.resource(
      fn -> File.open!(path, [:read, :raw, :binary, {:read_ahead, @buffer_bytes}]) end,
      fn file ->
        case read_frame(file) do
          {:ok, bin} ->
            {[bin], file}

          :eof ->
            {:halt, file}

          :unreadable when tail? ->
            {:halt, file}

          :unreadable ->
            raise "Transaction journal generation #{path} is unreadable before its end. " <>
                    "Transactions the replication slot was already acked past are missing " <>
                    "from it, so the shapes have to be resynced from Postgres."
        end
      end,
      fn file -> File.close(file) end
    )
  end

  defp read_frame(file) do
    case :file.read(file, 8) do
      :eof ->
        :eof

      {:ok, <<size::32, crc::32>>} ->
        case :file.read(file, size) do
          {:ok, bin} when byte_size(bin) == size ->
            if :erlang.crc32(bin) == crc, do: {:ok, bin}, else: :unreadable

          _ ->
            :unreadable
        end

      _ ->
        :unreadable
    end
  end

  @doc """
  One-pass per-generation accounting: `coverage` is the boundary the generation
  contributes to the boot check (commits and idle markers alike), `last_commit` the
  newest real transaction commit it holds, nil when it holds none. A generation with
  a nil `last_commit` fans out nothing on replay.
  """
  @spec generation_summary(String.t(), non_neg_integer()) :: %{
          coverage: non_neg_integer() | nil,
          last_commit: non_neg_integer() | nil
        }
  def generation_summary(base_path, gen) do
    stream_events!(base_path, [gen])
    |> Enum.reduce(%{coverage: nil, last_commit: nil}, fn
      %{commit: commit, last_log_offset: %LogOffset{tx_offset: tx_offset}}, acc
      when not is_nil(commit) ->
        %{
          coverage: max(acc.coverage || 0, tx_offset),
          last_commit: max(acc.last_commit || 0, tx_offset)
        }

      {:idle_slot_boundary, tx_offset}, acc ->
        %{acc | coverage: max(acc.coverage || 0, tx_offset)}

      _event, acc ->
        acc
    end)
  end

  @doc """
  The log offset the last journaled transaction ends at, or nil when the given generations hold
  no transaction commit. Unlike `last_commit_tx_offset/2` it ignores idle-slot boundaries, which
  carry no transaction, and keeps the position inside the transaction, so it can be compared
  with what a collector processed of a transaction delivered in fragments.
  """
  @spec last_commit_log_offset(String.t(), [non_neg_integer()]) :: LogOffset.t() | nil
  def last_commit_log_offset(base_path, gens) do
    stream_events!(base_path, gens)
    |> Enum.reduce(nil, fn
      %{commit: commit, last_log_offset: %LogOffset{} = offset}, nil when not is_nil(commit) ->
        offset

      %{commit: commit, last_log_offset: %LogOffset{} = offset}, acc when not is_nil(commit) ->
        LogOffset.max(acc, offset)

      _event, acc ->
        acc
    end)
  end

  @doc """
  The last journal-durable commit boundary across the given generations, or nil when the
  journal holds no commit. Used by boot to check the journal covers the acked slot position.
  """
  @spec last_commit_tx_offset(String.t(), [non_neg_integer()]) :: non_neg_integer() | nil
  def last_commit_tx_offset(base_path, gens) do
    stream_events!(base_path, gens) |> reduce_last_commit()
  end

  @doc """
  The highest position the journal shows Electric took from the replication stream: a journaled
  fragment, committed or not, an idle boundary, or a boundary whose generation was reclaimed.
  Nil when it shows none, and when the scan finds the base path gone: one a remount brings back
  before the coverage is read would show that coverage alone, behind the transactions the
  generations hold. Raises when the journal's directory or its used marker cannot be read,
  when it holds no generation though the journal has written one, or when a coverage shown alone
  was read from a base path whose generations or used marker appear after their listing.
  """
  @spec high_water(String.t()) :: non_neg_integer() | nil
  def high_water(base_path) do
    case list_generations!(base_path) do
      :absent ->
        nil

      # A remount can leave the directory under its mount point in place of the base path, empty
      # say, so the listings find neither the used marker nor a generation, and bring the base path
      # back before its coverage is read. The listings run again after that read, and a coverage
      # they no longer find alone fails the scan.
      [] ->
        case coverage_record(base_path).reclaimed do
          nil ->
            nil

          reclaimed ->
            if list_generations!(base_path) == [],
              do: reclaimed,
              else:
                raise(
                  "the transaction journal at #{base_path} showed a generation or its used marker once its coverage was read"
                )
        end

      gens ->
        base_path
        |> stream_events!(gens)
        |> Enum.reduce(coverage_record(base_path).reclaimed, fn
          %{last_log_offset: %LogOffset{tx_offset: tx_offset}}, acc -> max(acc || 0, tx_offset)
          {:idle_slot_boundary, tx_offset}, acc -> max(acc || 0, tx_offset)
          _event, acc -> acc
        end)
    end
  end

  defp last_commit_in_generation(dir, gen, tail?) do
    stream_generation(dir, gen, tail?) |> reduce_last_commit()
  end

  defp reduce_last_commit(events) do
    Enum.reduce(events, nil, fn
      %{commit: commit, last_log_offset: %LogOffset{tx_offset: tx_offset}}, acc
      when not is_nil(commit) ->
        max(acc || 0, tx_offset)

      # Durable record of a keepalive ack over WAL that carried no journaled commit:
      # the slot was advanced to this position, so the journal's coverage must report
      # it or the next boot reads the gap as lost shape data.
      {:idle_slot_boundary, tx_offset}, acc ->
        max(acc || 0, tx_offset)

      _event, acc ->
        acc
    end)
  end
end
