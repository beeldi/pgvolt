defmodule Electric.ShapeCache.PureFileStorageTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.TestUtils
  import Electric.ShapeCache.PureFileStorage.SharedRecords

  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.ChunkIndex
  alias Electric.ShapeCache.PureFileStorage.FileInfo
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.ShapeCache.PureFileStorage.Snapshot
  alias Electric.ShapeCache.PureFileStorage.WriteLoop
  alias Electric.Shapes.Shape

  require Repatch
  require WriteLoop

  @moduletag :tmp_dir
  setup [
    :with_stack_id_from_test,
    :with_async_deleter
  ]

  @shape_handle "the-shape-handle"
  @shape %Shape{
    root_table: {"public", "items"},
    root_table_id: 1,
    root_pk: ["id"],
    selected_columns: ["id"],
    explicitly_selected_columns: ["id"],
    where:
      Electric.Replication.Eval.Parser.parse_and_validate_expression!("id != '1'",
        refs: %{["id"] => :text}
      )
  }

  @xid 100
  @lsn 100

  @fragments txn_fragments(@xid, @lsn, [
               %{
                 changes: [
                   %Changes.NewRecord{
                     relation: {"public", "test_table"},
                     record: %{"id" => "5"},
                     log_offset: LogOffset.new(@lsn, 0)
                   },
                   %Changes.UpdatedRecord{
                     relation: {"public", "test_table"},
                     old_record: %{"id" => "1"},
                     record: %{"id" => "1", "foo" => "bar"},
                     log_offset: LogOffset.new(@lsn, 2),
                     changed_columns: MapSet.new(["foo"])
                   }
                 ]
               },
               %{
                 changes: [
                   %Changes.UpdatedRecord{
                     relation: {"public", "test_table"},
                     old_record: %{"id" => "3"},
                     record: %{"id" => "3", "another" => "update"},
                     log_offset: LogOffset.new(@lsn, 4),
                     changed_columns: MapSet.new(["another"])
                   }
                 ]
               },
               %{
                 changes: [
                   %Changes.NewRecord{
                     relation: {"public", "test_table"},
                     record: %{"id" => "6"},
                     log_offset: LogOffset.new(@lsn, 6)
                   }
                 ]
               },
               %{
                 changes: [
                   %Changes.DeletedRecord{
                     relation: {"public", "test_table"},
                     old_record: %{"id" => "2"},
                     log_offset: LogOffset.new(@lsn, 8),
                     last?: true
                   }
                 ]
               }
             ])

  describe "shared_opts/1" do
    test "carries a configured compaction period through to the writer's schedule" do
      # The period is what decides whether a churned log is ever collapsed, and it was reachable
      # only by editing the source: no environment variable and no supervisor option set it.
      assert %{compaction_config: %{period: 30_000}} =
               PureFileStorage.shared_opts(
                 stack_id: "compaction-period-test",
                 storage_dir: "/tmp/compaction-period-test",
                 compaction_period: 30_000
               )
    end

    test "falls back to ten minutes when no period is configured" do
      assert %{compaction_config: %{period: 600_000}} =
               PureFileStorage.shared_opts(
                 stack_id: "compaction-period-default-test",
                 storage_dir: "/tmp/compaction-period-default-test"
               )
    end
  end

  defp start_storage(ctx) do
    base_opts =
      PureFileStorage.shared_opts(
        stack_id: ctx.stack_id,
        storage_dir: ctx.tmp_dir,
        chunk_bytes_threshold: ctx[:chunk_size] || 10 * 1024 * 1024,
        flush_period: ctx[:flush_period] || 1000
      )

    storage_base = {PureFileStorage, base_opts}
    start_link_supervised!(Storage.stack_child_spec(storage_base))

    %{base_opts: base_opts, opts: PureFileStorage.for_shape(@shape_handle, base_opts)}
  end

  # The bound the HTTP API gives a read from `offset` (`Electric.Shapes.Api`), with the latest
  # offset as `Electric.ShapeCache.fetch_latest_offset/3` hands it over: a virtual one as `0_inf`.
  defp read_bound(opts, offset) do
    {:ok, latest} = PureFileStorage.fetch_latest_offset(opts)
    last_before_real = LogOffset.last_before_real_offsets()

    latest =
      if LogOffset.compare(latest, last_before_real) == :gt, do: latest, else: last_before_real

    LogOffset.min(PureFileStorage.get_chunk_end_log_offset(offset, opts) || latest, latest)
  end

  # A request's read of the bound from `offset`, started in a sync of the snapshot and answered
  # while the snapshot waits in that sync, or `nil`.
  defp read_bound_in_sync(opts, path, offset) do
    reader = Task.async(fn -> read_bound(opts, offset) end)

    answer =
      case Task.yield(reader, 5_000) || Task.shutdown(reader) do
        {:ok, bound} -> bound
        _ -> nil
      end

    send(self(), {:bound_in_sync, path, answer})
  end

  # Whether a writer opened on the shape, once `writer` is closed, takes its snapshot as there. A
  # reopen of a shape whose snapshot is incomplete starts the shape again, or, where the reopen
  # refuses such a shape, raises a data loss error; neither takes the snapshot.
  defp reopen_takes_snapshot?(opts, writer) do
    PureFileStorage.terminate(writer)

    try do
      reopened = PureFileStorage.init_writer!(opts, @shape)
      started? = PureFileStorage.snapshot_started?(opts)
      PureFileStorage.terminate(reopened)
      started?
    rescue
      error in Storage.Error ->
        assert error.kind == :data_loss
        false
    end
  end

  defp bounds_in_syncs(acc) do
    receive do
      {:bound_in_sync, path, bound} -> bounds_in_syncs([{path, bound} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp seed_log(opts, key_prefix) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)
    PureFileStorage.terminate(append_rows(writer, key_prefix, 0))
  end

  defp seed_lines(opts, lines), do: PureFileStorage.terminate(seed_lines_open(opts, lines))

  # The writer that wrote the lines, flushed and still open.
  defp seed_lines_open(opts, lines) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)
    PureFileStorage.hibernate(PureFileStorage.append_to_log!(lines, writer))
  end

  defp store_commit(base_opts, handle, tx_offset) do
    opts = PureFileStorage.for_shape(handle, base_opts)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    [{LogOffset.new(tx_offset, 0), "key", :insert, ~S|{"id":"1"}|}]
    |> PureFileStorage.append_to_log!(writer)
    |> PureFileStorage.terminate()

    opts
  end

  defp append_rows(writer, key_prefix, offset_base) do
    PureFileStorage.append_to_log!(rows(key_prefix, offset_base), writer)
  end

  defp rows(key_prefix, offset_base) do
    for i <- 1..20 do
      key = "#{key_prefix}-#{offset_base + i}"

      {LogOffset.new(offset_base + i, 0), key, :insert,
       ~s|{"key":"#{key}","value":{"id":"#{key}"},| <>
         ~s|"headers":{"operation":"insert","relation":["public","test_table"]}}|}
    end
  end

  defp await_compaction_cleanup(stack_id, attempts \\ 250) do
    supervisor = Electric.ProcessRegistry.name(stack_id, PureFileStorage.TaskSupervisor)

    cond do
      Task.Supervisor.children(supervisor) == [] -> :ok
      attempts == 0 -> flunk("compaction cleanup never finished")
      true -> Process.sleep(20) && await_compaction_cleanup(stack_id, attempts - 1)
    end
  end

  defp generation_count(opts, prefix) do
    opts
    |> PureFileStorage.shape_data_dir(["log"])
    |> Path.join("log.#{prefix}.*.jsonfile.bin")
    |> Path.wildcard()
    |> length()
  end

  defp log_path(opts, suffix, kind),
    do: opts |> PureFileStorage.shape_data_dir(["log"]) |> Path.join("log.#{suffix}.#{kind}.bin")

  defp metadata_path(opts, key),
    do: opts |> PureFileStorage.shape_data_dir(["metadata"]) |> Path.join("#{key}.bin")

  defp names_on_disk(opts),
    do:
      for(
        key <- [:latest_name, :compaction_boundary],
        do: {key, File.read!(metadata_path(opts, key))}
      )

  defp latest_generations(opts) do
    for path <- opts |> log_path("latest.*", "jsonfile") |> Path.wildcard(),
        do: path |> Path.basename(".jsonfile.bin") |> String.replace_prefix("log.", "")
  end

  defp latest_files(opts) do
    opts
    |> log_path("latest.*", "*")
    |> Path.wildcard()
    |> Enum.map(&Path.basename/1)
    |> Enum.sort()
  end

  defp generation_files(suffixes, kinds),
    do: Enum.sort(for suffix <- suffixes, kind <- kinds, do: "log.#{suffix}.#{kind}.bin")

  # The latest generation's files, in the order a removal by suffix takes them.
  defp generation_paths(opts, suffixes),
    do: for(suffix <- suffixes, kind <- ["chunk", "jsonfile"], do: log_path(opts, suffix, kind))

  defp plant_generation_files(opts, suffixes) do
    paths = generation_paths(opts, suffixes)
    Enum.each(paths, &File.write!(&1, ""))
    paths
  end

  # The cleanup runs in a task the writer starts, so the refusal is shared with it.
  defp refuse_to_remove(path) do
    Repatch.patch(PureFileStorage, :rm_rf!, [mode: :shared], fn target ->
      if target == path,
        do:
          raise(File.Error, reason: :eio, action: "remove files and directories from", path: path),
        else: File.rm_rf!(target)
    end)
  end

  # A switch whose cache row names the generation it replaced. A read that fills the row from the
  # disk before a switch, and inserts it once the switch's update found no row, leaves it so: the
  # helper puts the row the read took back after the switch updated it, the state that read leaves
  # rather than its timing.
  defp switch_under_a_stale_cache_row(opts) do
    seed_lines(opts, rows("row", 0))
    writer = PureFileStorage.init_writer!(opts, @shape)
    previous = PureFileStorage.latest_name(opts)
    :ok = PureFileStorage.compact(opts, 2)
    assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

    # The row a read takes from the disk before the switch, put back after it.
    true = :ets.delete(opts.stack_ets, opts.shape_handle)
    assert previous == PureFileStorage.latest_name(opts)
    [read_before_the_switch] = :ets.lookup(opts.stack_ets, opts.shape_handle)
    writer = apply_storage_message(writer, message)
    true = :ets.delete(opts.stack_ets, opts.shape_handle)
    true = :ets.insert_new(opts.stack_ets, read_before_the_switch)

    assert previous == PureFileStorage.latest_name(opts)

    refute previous ==
             opts |> metadata_path(:latest_name) |> File.read!() |> :erlang.binary_to_term()

    {writer, previous}
  end

  # The durable claim's file opens with an error, as with no file descriptor left.
  defp refuse_the_claim_read, do: refuse_the_open_of("last_persisted_txn_offset.bin")

  # The open of every file named `name` fails as with no file descriptor left.
  defp refuse_the_open_of(name) do
    Repatch.patch(File, :open, fn path, modes, fun ->
      if Path.basename(path) == name,
        do: {:error, :emfile},
        else: Repatch.real(File.open(path, modes, fun))
    end)
  end

  # An empty file, bytes that are no term, and a term that is no boundary.
  defp undecodable_boundaries,
    do: [<<>>, "not a term", :erlang.term_to_binary({:not, :a, :boundary})]

  defp discards_reported_to_this_process(stack_id) do
    parent = self()

    Repatch.patch(ShapeCleaner, :remove_shape_async, fn ^stack_id, handle ->
      send(parent, {:discarded, handle})
      :ok
    end)
  end

  # A started run holds the latch on disk until its end releases it.
  defp compaction_latched?(opts),
    do: opts |> metadata_path(:compaction_started?) |> File.read!() |> :erlang.binary_to_term()

  # The record goes in once the run finished; the next run, under the record restored, switches.
  defp assert_discarded_under_the_disk_record(opts, record) do
    seed_lines(opts, rows("row", 0))
    writer = PureFileStorage.init_writer!(opts, @shape)
    latest = PureFileStorage.latest_name(opts)
    path = metadata_path(opts, :latest_name)
    named = File.read!(path)
    writer = PureFileStorage.compact(writer, 0)

    assert_receive {Storage,
                    {PureFileStorage, :handle_compaction_finished, [_, discarded, _, ^latest]} =
                      message},
                   5000

    File.write!(path, record)
    writer = apply_storage_message(writer, message)

    assert PureFileStorage.latest_name(opts) == latest

    for kind <- ["jsonfile", "chunk", "keyfile"],
        do: refute(File.exists?(log_path(opts, discarded, kind)))

    File.write!(path, named)
    writer = PureFileStorage.compact(writer, 0)
    assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
    writer = apply_storage_message(writer, message)

    refute PureFileStorage.latest_name(opts) == latest
    assert for(i <- 1..20, do: "row-#{i}") == log_keys(opts, LogOffset.first())
    PureFileStorage.terminate(writer)
  end

  defp put_names_on_disk(opts, names),
    do: Enum.each(names, fn {key, bytes} -> File.write!(metadata_path(opts, key), bytes) end)

  defp point_compaction_at_a_missing_generation(opts) do
    PureFileStorage.set_compaction_boundary(opts, {LogOffset.before_all(), "compacted.404"})
  end

  defp apply_storage_message(writer, {module, function, args}) do
    apply(module, function, [writer | args])
  end

  defp log_keys(opts, from) do
    PureFileStorage.get_log_stream(from, LogOffset.last(), opts)
    |> Enum.map(&(Jason.decode!(&1) |> Map.fetch!("key")))
  end

  # Seeds a log whose last row closes a chunk at the claim, compacts all of it, and kills the
  # switch between its names: the compacted generation covers the claim, and the latest name
  # still points at the generation that holds every row.
  defp kill_a_covering_switch_between_its_names(opts) do
    seed_lines(opts, lines_whose_last_row_closes_a_chunk())
    writer = PureFileStorage.init_writer!(opts, @shape)
    :ok = PureFileStorage.compact(opts, 0)

    assert_receive {Storage,
                    {PureFileStorage, :handle_compaction_finished, [offset, _, _, _]} = message},
                   5000

    assert offset == LogOffset.new(21, 0)
    kill_the_switch_between_its_names(writer, message)
  end

  defp kill_the_switch_between_its_names(writer, message) do
    Repatch.patch(PureFileStorage, :set_latest_name, [mode: :local], fn opts, name, chunks ->
      if Process.put(:killed_between_names, true),
        do: Repatch.real(PureFileStorage.set_latest_name(opts, name, chunks)),
        else: raise("killed between the names")
    end)

    assert_raise RuntimeError, fn -> apply_storage_message(writer, message) end
  end

  defp lines_whose_last_row_closes_a_chunk do
    [
      {LogOffset.new(1, 0), "gone", :insert,
       ~s|{"key":"gone","value":{"id":"gone"},"headers":{"operation":"insert"}}|},
      {LogOffset.new(2, 0), "gone", :delete, ~s|{"key":"gone","headers":{"operation":"delete"}}|}
      | Enum.drop(rows("row", 0), 2) ++
          [
            {LogOffset.new(21, 0), "last", :insert,
             ~s|{"key":"last","value":{"id":"last","b":"#{String.duplicate("b", 300)}"},| <>
               ~s|"headers":{"operation":"insert","relation":["public","test_table"]}}|}
          ]
    ]
  end

  # Kills a compaction's switch between its names, cuts the latest chunk index `kept` entries past
  # the boundary's (before it when negative), plus `halves` half records, with log lines left past
  # the cut, and checks the reopen refuses to name a copy.
  defp assert_reopen_refuses_a_switch_with_its_index_cut(
         opts,
         kept,
         halves \\ 0,
         refusal \\ ~r/stops short of its log/
       ) do
    seed_lines(opts, rows("row", 0))
    writer = PureFileStorage.init_writer!(opts, @shape)
    :ok = PureFileStorage.compact(opts, 2)
    assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
    latest = PureFileStorage.latest_name(opts)

    Repatch.patch(PureFileStorage, :set_latest_name, [mode: :local], fn opts, name, chunks ->
      if Process.put(:killed_between_names, true),
        do: Repatch.real(PureFileStorage.set_latest_name(opts, name, chunks)),
        else: raise("killed between the names")
    end)

    assert_raise RuntimeError, fn -> apply_storage_message(writer, message) end

    {boundary, _} = PureFileStorage.compaction_boundary(opts)
    chunks = log_path(opts, latest, "chunk")
    entries = chunks |> ChunkIndex.read_chunk_file() |> Enum.to_list()
    at = Enum.find_index(entries, &match?({{_, ^boundary}, _, _}, &1)) + kept
    assert at < length(entries) - 1
    assert {{_, max}, _, _} = Enum.at(entries, at)
    refute is_nil(max)

    File.open!(chunks, [:read, :write, :raw], fn file ->
      {:ok, _} = :file.position(file, (at + 1) * 64 + halves * 32)
      :ok = :file.truncate(file)
    end)

    error =
      assert_raise Storage.Error, refusal, fn ->
        PureFileStorage.init_writer!(opts, @shape)
      end

    assert error.kind == :data_loss
    assert :erlang.binary_to_term(File.read!(metadata_path(opts, :latest_name))) == latest
  end

  defp covered_keys, do: ["gone", "gone"] ++ for(i <- 3..20, do: "row-#{i}") ++ ["last"]

  defp compacted_keys(opts, message) do
    opts
    |> PureFileStorage.init_writer!(@shape)
    |> apply_storage_message(message)

    PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
    |> Enum.map(&(Jason.decode!(&1) |> Map.fetch!("key")))
  end

  describe "required_version on a first-enable boot" do
    setup [:start_storage]

    test "records the arena version before any generation exists on disk", ctx do
      # the window in which a shape used to be recorded pre-arena and then handed a ref it could
      # not describe.
      Electric.StackConfig.put(ctx.stack_id, :payload_arena_active?, true)

      refute Electric.ShapeCache.PureFileStorage.PayloadArena.any_generations?(
               ctx.base_opts.base_path
             )

      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)

      # next restart lands in a log recorded as arena-capable.
      version_path = PureFileStorage.shape_data_dir(ctx.opts, ["metadata", "version.bin"])
      recorded = File.read!(version_path) |> :erlang.binary_to_term()

      assert recorded > ctx.opts.version
    end
  end

  describe "reads without writer -" do
    setup [:start_storage]

    test "snapshot only reads from disk", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(
               LogOffset.before_all(),
               LogOffset.last_before_real_offsets(),
               opts
             )
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]
    end

    # The writer can create a snapshot's last chunk and record its end between a reader's look for
    # the chunk and its look at the record.
    test "reads a chunk that appears as the snapshot's end is recorded", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      File.write!(Snapshot.chunk_file_path(opts, 0), [~S|{"test": 1}|, ",\n", <<4>>])

      Repatch.patch(PureFileStorage, :last_snapshot_chunk, [mode: :local], fn opts ->
        path = Snapshot.chunk_file_path(opts, 1)
        unless File.exists?(path), do: File.write!(path, [~S|{"test": 2}|, ",\n", <<4>>])
        LogOffset.new(0, 1)
      end)

      assert PureFileStorage.get_log_stream(
               LogOffset.new(0, 0),
               LogOffset.last_before_real_offsets(),
               opts
             )
             |> Enum.to_list() == [~S|{"test": 2}|]

      PureFileStorage.terminate(writer)
    end

    # The writer can also create the chunk and record the end after the reader found no record,
    # before it looks at the record again.
    test "reads a chunk that appears once the reader found no record of the end", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      File.write!(Snapshot.chunk_file_path(opts, 0), [~S|{"test": 1}|, ",\n", <<4>>])
      lookups = :counters.new(1, [])

      Repatch.patch(PureFileStorage, :last_snapshot_chunk, [mode: :local], fn opts ->
        :counters.add(lookups, 1, 1)

        if :counters.get(lookups, 1) == 1 do
          File.write!(Snapshot.chunk_file_path(opts, 1), [~S|{"test": 2}|, ",\n", <<4>>])
          nil
        else
          LogOffset.new(0, 1)
        end
      end)

      assert PureFileStorage.get_log_stream(
               LogOffset.new(0, 0),
               LogOffset.last_before_real_offsets(),
               opts
             )
             |> Enum.to_list() == [~S|{"test": 2}|]

      PureFileStorage.terminate(writer)
    end

    # The routing reads the cached end, and the chunk wait falls back to the disk: a reader in
    # read-only mode reads the disk alone, and an end recorded since the routing is on disk for the
    # wait. The lookup of the chunk's end reads it there too and bounds the next read in the log,
    # which then has to stream the log: the bound tells the reader it has every entry up to it.
    test "reads the log once the snapshot's end is on disk and not yet cached", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|},
            {LogOffset.new(11, 0), "test_key", :insert, ~S|{"test": 2}|}
          ],
          writer
        )

      File.write!(Snapshot.chunk_file_path(opts, 0), [~S|{"test": 0}|, ",\n", <<4>>])

      File.write!(
        PureFileStorage.shape_data_dir(opts, ["metadata", "last_snapshot_chunk.bin"]),
        :erlang.term_to_binary(LogOffset.new(0, 0))
      )

      bound = read_bound(opts, LogOffset.new(0, 0))
      assert bound == LogOffset.new(11, 0)

      assert PureFileStorage.get_log_stream(LogOffset.new(0, 0), bound, opts) |> Enum.to_list() ==
               [~S|{"test": 1}|, ~S|{"test": 2}|]

      PureFileStorage.terminate(writer)
    end

    # The end shows at the last chunk's end-of-transmission marker, as upstream shows it, and a
    # request during the syncs that follow is answered at once with the bound a later one gets.
    # Shown only once its record was in place, the end gave that request the pointer to a next
    # chunk, and a client revalidating the response got a new one from a shape that had not
    # changed. Shown only once its record was durable, it held the request for the syncs. A chunk
    # the stream ended with a boundary on was synced before the stream said it had ended.
    for {ending, stream} <- [
          "inside its last chunk": [~S|{"test": 0}|],
          "on a chunk boundary": [~S|{"test": 0}|, :chunk_boundary]
        ] do
      test "answers a read during the snapshot's syncs as a later read, ending #{ending}",
           %{opts: opts} do
        writer = PureFileStorage.init_writer!(opts, @shape)
        PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
        PureFileStorage.mark_snapshot_as_started(opts)

        Repatch.patch(Electric.Utils, :datasync_and_close!, [mode: :local], fn file, path ->
          read_bound_in_sync(opts, path, LogOffset.new(0, 0))
          Repatch.real(Electric.Utils.datasync_and_close!(file, path))
        end)

        Repatch.patch(Electric.Utils, :sync_dir!, [mode: :local], fn dir ->
          read_bound_in_sync(opts, dir, LogOffset.new(0, 0))
          Repatch.real(Electric.Utils.sync_dir!(dir))
        end)

        PureFileStorage.make_new_snapshot!(unquote(stream), opts)

        bound = read_bound(opts, LogOffset.new(0, 0))
        assert bound == LogOffset.last_before_real_offsets()

        in_syncs = bounds_in_syncs([])
        assert Enum.any?(in_syncs, fn {path, _} -> String.ends_with?(path, ".jsonsnapshot") end)
        assert Enum.any?(in_syncs, fn {path, _} -> Path.basename(path) == "metadata" end)
        assert Enum.reject(in_syncs, fn {_, in_sync} -> in_sync == bound end) == []

        PureFileStorage.terminate(writer)
      end
    end

    # A consumer that suspended while its snapshot ran is not told the snapshot failed, and the
    # shape stays. A snapshot that fails after showing its end takes the end back, so that this
    # node's readers do not read on to it.
    test "a snapshot that fails after showing its end takes it back", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      failed = :atomics.new(1, [])

      Repatch.patch(Electric.Utils, :sync_dir!, [mode: :local], fn dir ->
        if :atomics.compare_exchange(failed, 1, 0, 1) == :ok do
          send(self(), {:end_shown, PureFileStorage.snapshot_chunk_final?(opts, 0)})
          raise File.Error, reason: :eio, action: "sync directory", path: dir
        else
          Repatch.real(Electric.Utils.sync_dir!(dir))
        end
      end)

      assert_raise File.Error, fn ->
        PureFileStorage.make_new_snapshot!([~S|{"test": 0}|], opts)
      end

      assert_received {:end_shown, true}
      refute PureFileStorage.snapshot_chunk_final?(opts, 0)
      refute reopen_takes_snapshot?(opts, writer)
    end

    # A snapshot killed in its syncs takes nothing back, and leaves the end it showed in the cache
    # with no record on disk. A writer opened on the shape afterwards reads the record, so the end
    # alone does not make the snapshot complete.
    test "a writer reopened after a snapshot killed in its syncs does not take it for complete",
         %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      test_pid = self()

      snapshotter =
        spawn(fn ->
          receive do
            :go -> PureFileStorage.make_new_snapshot!([~S|{"test": 0}|], opts)
          end
        end)

      on_exit(fn -> Process.exit(snapshotter, :kill) end)

      Repatch.patch(Electric.Utils, :sync_dir!, [mode: :shared], fn dir ->
        if self() == snapshotter do
          send(test_pid, {:in_sync, self()})
          Process.sleep(:infinity)
        end

        Repatch.real(Electric.Utils.sync_dir!(dir))
      end)

      Repatch.allow(test_pid, snapshotter)
      ref = Process.monitor(snapshotter)
      send(snapshotter, :go)
      assert_receive {:in_sync, ^snapshotter}
      Process.exit(snapshotter, :kill)
      assert_receive {:DOWN, ^ref, :process, ^snapshotter, :killed}

      assert PureFileStorage.snapshot_chunk_final?(opts, 0)
      refute reopen_takes_snapshot?(opts, writer)
    end

    # A snapshot taken again on a shape that holds one fails at its first chunk, which is there. A
    # failure took the end out of the cache and the record off the disk whichever attempt they came
    # from, and the next writer opened on the shape took the snapshot that is there for incomplete.
    test "a snapshot that fails on a shape holding one keeps that snapshot's end", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      :ok = PureFileStorage.make_new_snapshot!([~S|{"test": 0}|], opts)
      record = PureFileStorage.shape_data_dir(opts, ["metadata", "last_snapshot_chunk.bin"])

      assert %File.Error{reason: :eexist} =
               assert_raise(File.Error, fn ->
                 PureFileStorage.make_new_snapshot!([~S|{"test": 1}|], opts)
               end)

      assert File.exists?(record)

      # The end stays in the cache too: with the record moved aside, the cache alone answers.
      File.rename!(record, record <> ".aside")
      assert PureFileStorage.snapshot_chunk_final?(opts, 0)
      File.rename!(record <> ".aside", record)
      PureFileStorage.terminate(writer)

      writer = PureFileStorage.init_writer!(opts, @shape)
      assert PureFileStorage.snapshot_started?(opts)
      assert PureFileStorage.snapshot_chunk_final?(opts, 0)

      assert PureFileStorage.get_log_stream(LogOffset.before_all(), LogOffset.new(0, 0), opts)
             |> Enum.to_list() == [~S|{"test": 0}|]

      PureFileStorage.terminate(writer)
    end

    # A read whose chunk is on disk needs no end: one that opened the end's record first failed on
    # an error opening it, though the chunk was there to stream.
    test "streams a written chunk of a snapshot in progress without opening its end's record",
         %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)

      for n <- 0..1,
          do: File.write!(Snapshot.chunk_file_path(opts, n), [~s|{"chunk": #{n}}|, ",\n", <<4>>])

      # An open of the record fails, and with an error other than a missing file.
      File.mkdir_p!(PureFileStorage.shape_data_dir(opts, ["metadata", "last_snapshot_chunk.bin"]))

      for {offset, line} <- [
            {LogOffset.before_all(), ~S|{"chunk": 0}|},
            {LogOffset.new(0, 0), ~S|{"chunk": 1}|}
          ] do
        assert [line] ==
                 offset
                 |> PureFileStorage.get_log_stream(LogOffset.last_before_real_offsets(), opts)
                 |> Enum.to_list()
      end

      PureFileStorage.terminate(writer)
    end

    test "active log reads", %{opts: opts, stack_id: stack_id} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|},
            {LogOffset.new(11, 0), "test_key", :insert, ~S|{"test": 2}|}
          ],
          writer
        )

      # This cleans the shapes ets cache, retaining only the values
      # required for the read-path
      PureFileStorage.terminate(writer)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(11, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      assert PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.last(), opts)
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]

      # this simulates a cold start
      stack_ets = PureFileStorage.stack_ets(stack_id)
      :ets.delete(stack_ets, @shape_handle)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(11, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      assert PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.last(), opts)
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]
    end

    # The 90s wait in the snapshot reader is for a reader that has caught up with a writer still
    # producing the file. A completed snapshot cannot grow, so a chunk that lost its
    # end-of-transmission marker is loss — and waiting for it turned a corrupt snapshot into a
    # 90s stall per client, on every retry, for as long as the shape existed.
    test "a completed snapshot chunk missing its end marker fails without waiting", %{
      opts: opts,
      base_opts: base_opts
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      path =
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle, [
          "log",
          "0.jsonsnapshot"
        ])

      %{size: size} = File.stat!(path)
      {:ok, file} = :file.open(path, [:write, :read, :raw])
      {:ok, _} = :file.position(file, size - 1)
      :ok = :file.truncate(file)
      :ok = :file.close(file)

      started = System.monotonic_time(:millisecond)

      read = fn ->
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )
        |> Enum.to_list()
      end

      error = assert_raise Storage.Error, ~r/lost durable data/, read
      assert error.kind == :data_loss
      assert System.monotonic_time(:millisecond) - started < 5_000
    end

    # A reader following the writer can reach the end of a chunk, and the writer can then write
    # the rest, close it and mark the snapshot complete before the reader asks whether the chunk
    # is final. Taking that eof for loss discarded fresh shapes milliseconds after their creation.
    test "a chunk the writer finishes between the reader's eof and its completion check is read in full",
         %{opts: opts, base_opts: base_opts, stack_id: stack_id} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      path =
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle, [
          "log",
          "0.jsonsnapshot"
        ])

      [first_line, rest] = String.split(File.read!(path), "\n", parts: 2)
      File.write!(path, first_line <> "\n")

      parent = self()

      Repatch.patch(ShapeCleaner, :remove_shape_async, fn ^stack_id, handle ->
        send(parent, {:discarded, handle})
        :ok
      end)

      Repatch.patch(PureFileStorage, :snapshot_chunk_final?, fn _opts, 0 ->
        File.write!(path, rest, [:append])
        true
      end)

      assert PureFileStorage.get_log_stream(
               LogOffset.before_all(),
               LogOffset.last_before_real_offsets(),
               opts
             )
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]

      refute_received {:discarded, _}
    end

    # Damage found at reopen retires the shape and the next reader rebuilds it. Damage found at
    # read had no such route, so the request failed and every client retried into the same bytes
    # forever. It has to reach the same place.
    # Truncation is caught because the chunk index says how long the file should be. An entry
    # altered in place says nothing: the frame carries a 16-byte offset, a key size, an op type,
    # a flag byte and a json size, and no checksum over any of it. This pins what that costs
    # today, so the entry can be read as a measurement rather than an argument.
    test "an entry altered in place is served to the client as if it were durable", %{
      opts: opts,
      stack_id: stack_id
    } do
      seed_log(opts, "corrupt")

      [log_path] =
        opts
        |> PureFileStorage.shape_data_dir(["log"])
        |> Path.join("log.*.jsonfile.bin")
        |> Path.wildcard()

      original = File.read!(log_path)
      assert String.contains?(original, ~s|"id":"corrupt-7"|)

      # Same length, so every size field in the frame stays exactly right and only the payload
      # bytes differ -- the one shape of damage nothing in the format can detect.
      File.write!(log_path, String.replace(original, ~s|"id":"corrupt-7"|, ~s|"id":"corrupt-9"|))
      assert byte_size(File.read!(log_path)) == byte_size(original)

      parent = self()

      Repatch.patch(ShapeCleaner, :remove_shape_async, fn ^stack_id, handle ->
        send(parent, {:discarded, handle})
        :ok
      end)

      jsons =
        PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
        |> Enum.to_list()
        |> Enum.map(&IO.iodata_to_binary/1)

      refute_received {:discarded, _},
                      "altered bytes were detected, so this no longer covers the case"

      served = Enum.filter(jsons, &String.contains?(&1, ~s|"key":"corrupt-7"|))

      assert [entry] = served
      assert String.contains?(entry, ~s|"id":"corrupt-9"|)
      refute String.contains?(entry, ~s|"id":"corrupt-7"|)
    end

    test "a data-loss read discards the shape so it can be rebuilt", %{
      opts: opts,
      base_opts: base_opts,
      stack_id: stack_id
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      path =
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle, [
          "log",
          "0.jsonsnapshot"
        ])

      %{size: size} = File.stat!(path)
      {:ok, file} = :file.open(path, [:write, :read, :raw])
      {:ok, _} = :file.position(file, size - 1)
      :ok = :file.truncate(file)
      :ok = :file.close(file)

      parent = self()

      Repatch.patch(ShapeCleaner, :remove_shape_async, fn ^stack_id, handle ->
        send(parent, {:discarded, handle})
        :ok
      end)

      assert_raise Storage.Error, fn ->
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )
        |> Enum.to_list()
      end

      assert_received {:discarded, @shape_handle}
    end

    # A read-only reader is not the writer of these bytes: during a rolling deploy it is the
    # instance waiting for the lock, reading what the running one owns. Removing the shape from
    # there deletes the running instance's catalog row and directory under its consumer.
    test "a data-loss read by a read-only reader reports the loss and keeps the shape", %{
      opts: opts,
      base_opts: base_opts,
      stack_id: stack_id
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      path =
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle, [
          "log",
          "0.jsonsnapshot"
        ])

      %{size: size} = File.stat!(path)
      {:ok, file} = :file.open(path, [:write, :read, :raw])
      {:ok, _} = :file.position(file, size - 1)
      :ok = :file.truncate(file)
      :ok = :file.close(file)

      parent = self()

      Repatch.patch(ShapeCleaner, :remove_shape_async, fn ^stack_id, handle ->
        send(parent, {:discarded, handle})
        :ok
      end)

      error =
        assert_raise Storage.Error, ~r/lost durable data/, fn ->
          PureFileStorage.get_log_stream(
            LogOffset.before_all(),
            LogOffset.last_before_real_offsets(),
            %{opts | read_only?: true}
          )
          |> Enum.to_list()
        end

      assert error.kind == :data_loss
      refute_received {:discarded, _}
    end

    test "reads survive the later deletion of the snapshot file", %{
      opts: opts,
      base_opts: base_opts
    } do
      opts = %{opts | snapshot_file_timeout: 50}
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      stream =
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert Enum.to_list(stream) == []
    end

    test "reads to a deleted shape do not raise", %{opts: opts, base_opts: base_opts} do
      opts = %{opts | snapshot_file_timeout: 50}
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      stream =
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )

      assert Enum.to_list(stream) == []
    end

    test "enoent returned from an existing shape does raise", %{opts: opts} do
      opts = %{opts | snapshot_file_timeout: 50}
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      chunk_file_path = PureFileStorage.Snapshot.chunk_file_path(opts, 0)

      File.rm!(chunk_file_path)

      assert_raise File.Error, fn ->
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )
        |> Enum.to_list()
      end
    end
  end

  describe "read-through cache -" do
    setup [:start_storage]

    test "is always populated", %{
      opts: opts,
      stack_id: stack_id
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      stack_ets = PureFileStorage.stack_ets(stack_id)

      # Should populate read through cache even while writer is active
      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      assert [_] = :ets.lookup(stack_ets, @shape_handle)

      PureFileStorage.terminate(writer)

      # terminating the writer does not delete the entry, just cleans the writer-only fields
      assert [_] = :ets.lookup(stack_ets, @shape_handle)

      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      # Verify cache is retained when writer is re-activated
      PureFileStorage.init_writer!(opts, @shape)

      assert [_] = :ets.lookup(stack_ets, @shape_handle)
    end

    test "is cleaned of writer metadata after writer termination", %{
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords,
        only: [storage_meta: 0, storage_meta: 1]

      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      stack_ets = PureFileStorage.stack_ets(stack_id)

      # Should populate read through cache even while writer is active
      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      assert [storage_meta() = meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(ets_table: ets_table) = meta
      assert is_list(:ets.info(ets_table))

      PureFileStorage.terminate(writer)

      # terminating the writer does not delete the entry, just cleans the writer-only fields
      assert [storage_meta() = meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(ets_table: nil) = meta
      assert :undefined == :ets.info(ets_table)
    end

    test "subsequent reads use cached values without disk access", %{
      opts: opts,
      base_opts: base_opts
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      # First read populates cache
      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      # Delete shape failes to confirm cache is used
      File.rm_rf!(Path.join([base_opts.base_path, @shape_handle]))

      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}
    end

    test "cache is cleaned up on shape deletion", %{opts: opts, stack_id: stack_id} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)
      PureFileStorage.terminate(writer)

      # Populate cache
      PureFileStorage.snapshot_started?(opts)

      stack_ets = PureFileStorage.stack_ets(stack_id)
      assert [_meta] = :ets.lookup(stack_ets, @shape_handle)

      # Verify cache entry is removed on cleanup
      {PureFileStorage, base_opts} = Storage.for_stack(stack_id)
      PureFileStorage.cleanup!(base_opts, @shape_handle)
      assert :ets.lookup(stack_ets, @shape_handle) == []
    end

    test "reads work for missing metadata", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.terminate(writer)

      assert PureFileStorage.snapshot_started?(opts) == false

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.last_before_real_offsets()}
    end
  end

  describe "key index writes" do
    setup [:start_storage]

    test "are correct", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      suffix = PureFileStorage.latest_name(opts)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|},
            {LogOffset.new(11, 0), "test_key", :update, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~S|{"test":2}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      key_file = PureFileStorage.key_file(opts, suffix)

      PureFileStorage.KeyIndex.create_from_log(
        PureFileStorage.json_file(opts, suffix),
        key_file
      )

      assert File.exists?(key_file)

      assert PureFileStorage.KeyIndex.read_key_file(PureFileStorage.key_file(opts, suffix)) == [
               {"test_key", LogOffset.new(10, 0), ?i, 0, byte_size(~S|{"test":1}|)},
               {"test_key", LogOffset.new(11, 0), ?u, 48, byte_size(~S|{"test":2}|)},
               {"test_key", LogOffset.new(12, 0), ?d, 96, byte_size(~S|{"test":2}|)}
             ]
    end

    @tag chunk_size: 5
    test "are correct with small chunks too", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        for i <- 1..10 do
          %Changes.UpdatedRecord{
            relation: {"public", "test_table"},
            old_record: %{"id" => "sameid", "name" => "Test#{i - 1}"},
            record: %{"id" => "sameid", "name" => "Test#{i}"},
            log_offset: LogOffset.new(i, 0),
            changed_columns: MapSet.new(["name"])
          }
        end
        |> changes_to_log_items()
        |> PureFileStorage.append_to_log!(writer)

      PureFileStorage.terminate(writer)

      key_file = PureFileStorage.key_file(opts, PureFileStorage.latest_name(opts))

      PureFileStorage.KeyIndex.create_from_log(
        PureFileStorage.json_file(opts, PureFileStorage.latest_name(opts)),
        key_file
      )

      assert File.exists?(key_file)

      assert PureFileStorage.KeyIndex.read_key_file(key_file) == [
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(1, 0), 117, 0, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(2, 0), 117, 251, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(3, 0), 117, 502, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(4, 0), 117, 753, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(5, 0), 117, 1004, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(6, 0), 117, 1255, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(7, 0), 117, 1506, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(8, 0), 117, 1757, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(9, 0), 117, 2008, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(10, 0), 117, 2259, 193}
             ]
    end
  end

  describe "chunk reads" do
    setup [:start_storage, :with_started_writer]

    @tag chunk_size: 100
    test "correctly finds a chunk to read from", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(10, 0), LogOffset.new(10, 0)}, {0, 150}, {0, 0}},
               {{LogOffset.new(11, 0), LogOffset.new(11, 0)}, {150, 300}, {0, 0}},
               {{LogOffset.new(12, 0), LogOffset.new(12, 0)}, {300, 450}, {0, 0}}
             ]

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(10, 0), opts)
             |> Enum.to_list() == [~s|{"test":"#{long_word}1"}|]

      assert PureFileStorage.get_log_stream(LogOffset.new(10, 0), LogOffset.new(11, 0), opts)
             |> Enum.to_list() == [~s|{"test":"#{long_word}2"}|]

      assert PureFileStorage.get_log_stream(LogOffset.new(11, 0), LogOffset.new(12, 0), opts)
             |> Enum.to_list() == [~s|{"test":"#{long_word}3"}|]
    end

    @tag chunk_size: 300
    test "does not leak rows past max_offset from a closed straddling chunk", %{
      writer: writer,
      opts: opts
    } do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          for tx_offset <- 1..7 do
            {LogOffset.new(tx_offset, 0), "test_key", :insert,
             ~s|{"test":"#{long_word}#{tx_offset}"}|}
          end,
          writer
        )

      PureFileStorage.terminate(writer)

      assert [
               {{%LogOffset{tx_offset: 1, op_offset: 0}, %LogOffset{tx_offset: 3, op_offset: 0}},
                _, _},
               {{%LogOffset{tx_offset: 4, op_offset: 0}, %LogOffset{tx_offset: 6, op_offset: 0}},
                _, _}
               | _
             ] =
               PureFileStorage.ChunkIndex.read_chunk_file(
                 PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
               )

      streamed =
        PureFileStorage.get_log_stream(LogOffset.new(1, 0), LogOffset.new(4, 0), opts)
        |> Enum.to_list()

      assert streamed == [
               ~s|{"test":"#{long_word}2"}|,
               ~s|{"test":"#{long_word}3"}|,
               ~s|{"test":"#{long_word}4"}|
             ]
    end

    test "survive the deletion of the chunk file mid stream", %{
      writer: writer,
      opts: opts,
      base_opts: base_opts
    } do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      stream = PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert Enum.to_list(stream) == []
    end

    test "returns an empty stream if the files have been deleted", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      shape_data_dir = PureFileStorage.shape_data_dir(opts)

      File.rm_rf!(shape_data_dir)

      assert [] =
               PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
               |> Enum.to_list()
    end

    test "returns an empty stream if the shape has been deleted", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)
      PureFileStorage.cleanup!(opts)

      assert [] = PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
    end

    test "raises if the files are gone but the shape still exists", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      json_file = PureFileStorage.json_file(opts, "latest.0")

      File.rm!(json_file)

      assert_raise File.Error, fn ->
        PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
        |> Enum.to_list()
      end
    end

    test "returns empty stream if some files deleted and the shape has gone", %{
      writer: writer,
      opts: opts,
      base_opts: base_opts
    } do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert [] =
               PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
               |> Enum.to_list()
    end

    test "correctly skips over lines when max offset is less than the one written", %{
      writer: writer,
      opts: opts
    } do
      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"1"}|},
            {LogOffset.new(12, 0), "test_key", :update, ~s|{"test":"2"}|},
            {LogOffset.new(14, 0), "test_key", :delete, ~s|{"test":"3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(11, 0), opts)
             |> Enum.to_list() == [~s|{"test":"1"}|]
    end
  end

  describe "crash recovery" do
    # These tests make use of known log file structures to test that the storage recovers correctly
    # If underlying file structure changes, these tests will need to be updated.
    setup [:start_storage, :with_started_writer]

    setup %{writer: writer, opts: opts} = ctx do
      if Map.get(ctx, :init_log, true) do
        writer =
          PureFileStorage.append_to_log!(
            [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
            writer
          )

        PureFileStorage.terminate(writer)

        assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

        assert PureFileStorage.fetch_pg_snapshot(opts) == {:ok, %{xmin: 100}}
      end

      :ok
    end

    test "incomplete transaction write before crash is discarded", %{opts: opts} do
      # Transaction got partially flushed, but not "closed" i.e. persisted boundary wasn't updated
      File.open!(
        PureFileStorage.json_file(opts, PureFileStorage.latest_name(opts)),
        [:append, :raw],
        fn file ->
          json = Jason.encode!(%{test: 2})

          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(11, 0))::binary, 4::32, "test"::binary, ?i::8,
              0::8, byte_size(json)::64, json::binary>>
          )
        end
      )

      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(10, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      # After recovery we see the same line
      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test", :insert, ~S|{"test":3}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert [~S|{"test":1}|, ~S|{"test":2}|, ~S|{"test":3}|] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.to_list()
    end

    test "chunk boundary without an actual write is trimmed", %{opts: opts} do
      # Transaction got partially flushed, but not "closed" i.e. persisted boundary wasn't updated
      File.open!(
        PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts)),
        [:append, :raw],
        fn file ->
          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(20, 0))::binary, 100::64, 100::64>>
          )
        end
      )

      # And a partial write cut midline just for good measure
      File.open!(
        PureFileStorage.json_file(opts, PureFileStorage.latest_name(opts)),
        [:append, :raw],
        fn file ->
          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(20, 0))::binary, 4::32, "test"::binary, ?i::8,
              0::8, 0::32>>
          )
        end
      )

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(10, 0), LogOffset.new(20, 0)}, {0, 100}, {0, 100}}
             ]

      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(10, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      # After recovery we see the same line
      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test", :insert, ~S|{"test":3}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)
      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) == nil

      assert [~S|{"test":1}|, ~S|{"test":2}|, ~S|{"test":3}|] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.to_list()

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(10, 0), nil}, {0, nil}, {0, nil}}
             ]
    end

    @tag init_log: false
    test "correctly handles incomplete chunks as part of the recovery", %{opts: opts} do
      path = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      File.mkdir_p!(Path.dirname(path))

      File.open!(
        path,
        [:append, :raw],
        fn file ->
          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(20, 0))::binary, 100::64, 100::64>>
          )
        end
      )

      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(0, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test", :insert, ~S|{"test":3}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)
      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) == nil

      assert [~S|{"test":2}|, ~S|{"test":3}|] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.to_list()

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(11, 0), nil}, {0, nil}, {0, nil}}
             ]
    end
  end

  describe "stored_high_water/1" do
    setup [:start_storage]

    test "is nil when no shape is stored", %{base_opts: base_opts} do
      assert PureFileStorage.stored_high_water(base_opts.base_path) == nil
    end

    test "is nil when the base path does not exist", %{base_opts: base_opts} do
      assert PureFileStorage.stored_high_water(Path.join(base_opts.base_path, "missing")) == nil
    end

    test "fails on a base path that does not list when the root must name an entry",
         %{base_opts: base_opts} do
      missing = Path.join(base_opts.base_path, "missing")

      error =
        assert_raise File.Error, fn ->
          PureFileStorage.stored_high_water(missing, root_names: ["marker"])
        end

      assert {error.reason, error.action, error.path} == {:enoent, "list directory", missing}
    end

    test "fails on a base path whose listing names none of the entries it must name",
         %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)

      assert_raise Storage.Error, ~r/lists none of marker, other/, fn ->
        PureFileStorage.stored_high_water(base_opts.base_path, root_names: ["marker", "other"])
      end

      File.write!(Path.join(base_opts.base_path, "other"), "")

      assert PureFileStorage.stored_high_water(base_opts.base_path,
               root_names: ["marker", "other"]
             ) ==
               30
    end

    test "is the furthest commit any shape keeps", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      store_commit(base_opts, "shape-b", 70)

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 70
    end

    test "counts a commit that reached the file but not the disk", %{base_opts: base_opts} do
      opts = store_commit(base_opts, "shape-a", 30)
      offset = LogOffset.new(90, 0)

      PureFileStorage.update_persistence_cache(opts, offset, offset, offset, LogOffset.new(30, 0))

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90
    end

    # A run with the journal off serves commits past the durable claim, and the written marker is
    # what the scan has of them: one whose content a crash took leaves them unbounded.
    test "fails on a written marker whose content a crash took", %{base_opts: base_opts} do
      opts = store_commit(base_opts, "shape-a", 30)
      File.write!(metadata_path(opts, "last_written_txn_offset"), "")

      assert_raise ArgumentError, fn -> PureFileStorage.stored_high_water(base_opts.base_path) end
    end

    test "fails on a durable claim that does not decode", %{base_opts: base_opts} do
      opts = store_commit(base_opts, "shape-a", 30)
      File.write!(metadata_path(opts, "last_persisted_txn_offset"), "")

      assert_raise ArgumentError, fn -> PureFileStorage.stored_high_water(base_opts.base_path) end
    end

    # A directory that can be searched and not read still serves the shapes under it by handle,
    # and the scan left them out. A symlink to itself fails the listing as a root user too.
    test "fails on a level it cannot list", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      File.mkdir_p!(Path.join(base_opts.base_path, "un"))
      File.ln_s!("li", Path.join([base_opts.base_path, "un", "li"]))

      assert_raise File.Error, ~r/list directory/, fn ->
        PureFileStorage.stored_high_water(base_opts.base_path)
      end
    end

    test "takes a file beside the shapes for a level with nothing under it", %{
      base_opts: base_opts
    } do
      store_commit(base_opts, "shape-a", 30)
      File.write!(Path.join(base_opts.base_path, "zz"), "")
      File.write!(Path.join([base_opts.base_path, "sh", "zz"]), "")

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 30
    end

    test "takes a file at the path a shape's name maps to for no shape", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      stray = PureFileStorage.shape_data_dir(base_opts.base_path, "stray-file")
      File.mkdir_p!(Path.dirname(stray))
      File.write!(stray, "")

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 30
    end

    # A symlinked shape can be retargeted at a file after the scan listed it, which then reads no
    # commit of the shape it hid.
    test "fails on a symlink at the path a shape's name maps to that resolves to a file", %{
      base_opts: base_opts
    } do
      store_commit(base_opts, "shape-a", 30)
      linked = PureFileStorage.shape_data_dir(base_opts.base_path, "linked-shape")
      File.mkdir_p!(Path.dirname(linked))
      file = Path.join(base_opts.base_path, "linked-file")
      File.write!(file, "")
      File.ln_s!(file, linked)

      assert_raise File.Error, ~r/not a directory/, fn ->
        PureFileStorage.stored_high_water(base_opts.base_path)
      end
    end

    # A symlinked shape whose target is away after the listing found it reads as one with no
    # marker, and the commits it served past the journal come back with its target.
    test "fails on a symlinked shape whose target is away after the listing", %{
      base_opts: base_opts
    } do
      store_commit(base_opts, "shape-a", 30)
      opts = store_commit(base_opts, "linked-shape", 90)
      linked = PureFileStorage.shape_data_dir(opts)
      target = Path.join(base_opts.base_path, "linked-target")
      File.rename!(linked, target)
      File.ln_s!(target, linked)
      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90

      Repatch.patch(PureFileStorage, :stored_shape_dirs!, fn base_path ->
        dirs = Repatch.real(Repatch.private(PureFileStorage.stored_shape_dirs!(base_path)))
        File.rename!(target, target <> ".away")
        dirs
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.stored_high_water(base_opts.base_path) end

      assert {error.reason, error.path} == {:enoent, linked}

      Repatch.restore(PureFileStorage, :stored_shape_dirs!, 1)
      File.rename!(target <> ".away", target)
      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90
    end

    # The same for a shape whose metadata directory is a symlink: the shape resolves, and its
    # markers are away with the target.
    test "fails on a marker whose directory is a symlink whose target is away", %{
      base_opts: base_opts
    } do
      store_commit(base_opts, "shape-a", 30)
      opts = store_commit(base_opts, "linked-shape", 90)
      metadata_dir = PureFileStorage.shape_data_dir(opts, ["metadata"])
      target = Path.join(base_opts.base_path, "linked-metadata")
      File.rename!(metadata_dir, target)
      File.ln_s!(target, metadata_dir)
      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90

      File.rename!(target, target <> ".away")

      error =
        assert_raise File.Error, fn -> PureFileStorage.stored_high_water(base_opts.base_path) end

      assert {error.reason, Path.dirname(error.path)} == {:enoent, metadata_dir}

      File.rename!(target <> ".away", target)
      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90
    end

    # A symlinked shape whose target is away only while its markers are read reads as one with no
    # marker, though the looks before and after its reads find it, and the commits it served past
    # the journal come back with its target.
    test "fails on a symlinked shape whose target is away only for its reads", %{
      base_opts: base_opts
    } do
      store_commit(base_opts, "shape-a", 30)
      opts = store_commit(base_opts, "linked-shape", 90)
      linked = PureFileStorage.shape_data_dir(opts)
      target = Path.join(base_opts.base_path, "linked-target")
      away = target <> ".away"
      File.rename!(linked, target)
      File.ln_s!(target, linked)

      # The target goes away at the shape's first marker read and is back once its boot record
      # has been read.
      Repatch.patch(File, :read, fn path ->
        if String.starts_with?(path, linked <> "/") and File.exists?(target),
          do: File.rename!(target, away)

        Repatch.real(File.read(path))
      end)

      Repatch.patch(PureFileStorage, :recorded_high_water, fn shape_dir ->
        found = Repatch.real(Repatch.private(PureFileStorage.recorded_high_water(shape_dir)))
        if File.exists?(away), do: File.rename!(away, target)
        found
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.stored_high_water(base_opts.base_path) end

      assert {error.reason, error.path} == {:enoent, linked}

      Repatch.restore(File, :read, 1)
      Repatch.restore(PureFileStorage, :recorded_high_water, 1)
      if File.exists?(away), do: File.rename!(away, target)
      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90
    end

    # A marker of a symlinked shape whose target is away for that marker's first read only is read
    # again once the looks after find the shape, and the commit it names is kept.
    test "reads again a marker of a symlinked shape whose target was away for its first read", %{
      base_opts: base_opts
    } do
      store_commit(base_opts, "shape-a", 30)
      opts = store_commit(base_opts, "linked-shape", 90)
      linked = PureFileStorage.shape_data_dir(opts)
      target = Path.join(base_opts.base_path, "linked-target")
      away = target <> ".away"
      File.rename!(linked, target)
      File.ln_s!(target, linked)

      Repatch.patch(File, :read, fn path ->
        if String.starts_with?(path, linked <> "/") and not Process.get({:read, path}, false) do
          Process.put({:read, path}, true)
          File.rename!(target, away)
          read = Repatch.real(File.read(path))
          File.rename!(away, target)
          read
        else
          Repatch.real(File.read(path))
        end
      end)

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90
    end

    # A cleanup renames a shape out of its shard whole, the boot's reclaim of an orphan as the
    # stack takes the lock say, and what it moved away is served no more: the scan of a store whose
    # every position is below the switch point does not fail on it.
    test "passes over a shape a cleanup moves away during the scan", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      opts = store_commit(base_opts, "orphan-shape", 90)
      orphan = PureFileStorage.shape_data_dir(opts)
      moved = Path.join(base_opts.base_path, "orphan-moved")

      Repatch.patch(PureFileStorage, :stored_shape_dirs!, fn base_path ->
        dirs = Repatch.real(Repatch.private(PureFileStorage.stored_shape_dirs!(base_path)))
        File.rename!(orphan, moved)
        dirs
      end)

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 30

      Repatch.restore(PureFileStorage, :stored_shape_dirs!, 1)
      File.rename!(moved, orphan)
      assert PureFileStorage.stored_high_water(base_opts.base_path) == 90

      Repatch.patch(File, :read, fn path ->
        if String.starts_with?(path, orphan <> "/") and File.exists?(orphan),
          do: File.rename!(orphan, moved)

        Repatch.real(File.read(path))
      end)

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 30
    end

    # A symlinked store root whose target is away for the listing reads as a store with no shape,
    # and the shapes it serves come back with its target.
    test "fails on a store root that is a symlink whose target is away", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      linked = base_opts.base_path <> "-linked"
      File.ln_s!(base_opts.base_path, linked)
      assert PureFileStorage.stored_high_water(linked) == 30

      File.rename!(base_opts.base_path, base_opts.base_path <> ".away")

      try do
        assert_raise File.Error, ~r/list directory/, fn ->
          PureFileStorage.stored_high_water(linked)
        end
      after
        File.rename!(base_opts.base_path <> ".away", base_opts.base_path)
      end
    end

    test "leaves out an entry no shape can sit under", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      File.ln_s!("backups", Path.join(base_opts.base_path, "backups"))
      File.ln_s!("backups", Path.join([base_opts.base_path, "sh", "backups"]))

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 30
    end

    # The service reads a shape only at the path its handle maps to, so an entry three levels down
    # anywhere else is read by no response: a symlink to itself there failed every scan, and a copy
    # moved there counted a commit no shape serves.
    test "leaves out an entry away from the path its name maps to", %{base_opts: base_opts} do
      store_commit(base_opts, "shape-a", 30)
      store_commit(base_opts, "other-shape", 90)
      shard = Path.join([base_opts.base_path, "sh", "ap"])
      File.ln_s!("backup", Path.join(shard, "backup"))

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, "other-shape"),
        Path.join(shard, "other-shape")
      )

      assert PureFileStorage.stored_high_water(base_opts.base_path) == 30
    end
  end

  describe "cleanup on a store the stack's trash cannot take" do
    setup [:start_storage, :with_started_writer]

    # A volume mounted for the shapes under the storage directory puts the store on another
    # filesystem than the stack's trash, and a rename does not cross filesystems.
    setup %{tmp_dir: tmp_dir, stack_id: stack_id} do
      trash_dir = Electric.AsyncDeleter.trash_dir(tmp_dir, stack_id)

      Repatch.patch(Electric.AsyncDeleter, :do_rename, fn path, dir, attempts ->
        if dir == trash_dir,
          do: {:error, :exdev},
          else:
            Repatch.real(Repatch.private(Electric.AsyncDeleter.do_rename(path, dir, attempts)))
      end)

      :ok
    end

    test "a purge empties the store and its tmp dir", %{base_opts: base_opts, writer: writer} do
      PureFileStorage.terminate(writer)
      assert {:ok, stored} = PureFileStorage.get_all_stored_shape_handles(base_opts)
      assert MapSet.member?(stored, @shape_handle)
      File.mkdir_p!(Path.join(base_opts.tmp_dir, "run"))

      assert :ok = PureFileStorage.cleanup_all!(base_opts)

      assert {:ok, stored} = PureFileStorage.get_all_stored_shape_handles(base_opts)
      assert stored == MapSet.new()
      assert File.ls!(base_opts.tmp_dir) == [".electric_trash"]
    end

    test "a shape's cleanup removes its directory", %{base_opts: base_opts, writer: writer} do
      PureFileStorage.terminate(writer)

      assert :ok = PureFileStorage.cleanup!(base_opts, @shape_handle)
      refute File.exists?(PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle))
    end
  end

  describe "get_all_stored_shape_handles/1" do
    test "lists what the base path's wildcard three levels down lists", %{tmp_dir: tmp_dir} do
      base_path = Path.join(tmp_dir, "shapes")
      handles = ["128584483-1770721672609826", "128599999-1770721672609827", "35237783-17707"]

      for handle <- handles, do: File.mkdir_p!(PureFileStorage.shape_data_dir(base_path, handle))

      # What sits beside the shape directories: dot entries at each level, which the wildcard
      # leaves out, and files at each level, which it lists only on the last one.
      for dir <- [base_path, Path.join(base_path, "12"), Path.join([base_path, "12", "85"])] do
        File.mkdir_p!(Path.join(dir, ".hidden/aa/bb"))
        File.write!(Path.join(dir, ".dotfile"), "")
        File.write!(Path.join(dir, "stray-file"), "")
      end

      wildcard =
        "#{base_path}/*/*/*" |> Path.wildcard() |> MapSet.new(&Path.basename/1)

      assert {:ok, listed} = PureFileStorage.get_all_stored_shape_handles(%{base_path: base_path})
      assert listed == wildcard
      assert listed == MapSet.new(["stray-file" | handles])
    end

    test "lists nothing under a base path that does not exist", %{tmp_dir: tmp_dir} do
      assert {:ok, listed} =
               PureFileStorage.get_all_stored_shape_handles(%{
                 base_path: Path.join(tmp_dir, "missing")
               })

      assert listed == MapSet.new()
    end
  end

  describe "chunk writes - " do
    setup [:start_storage, :with_started_writer]

    @tag chunk_size: 11
    test "chunk size is counted by JSON size and not full entry size", %{
      writer: writer,
      opts: opts
    } do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      # Chunk shoudn't be closed, because byte_size(~S|{"test":1}|) == 10 < 11
      refute PureFileStorage.get_chunk_end_log_offset(LogOffset.new(9, 0), opts) ==
               LogOffset.new(10, 0)
    end
  end

  describe "resumption" do
    setup [:start_storage]

    @tag chunk_size: 30
    test "correctly continues a chunk after a reboot", %{opts: opts} do
      %{writer: writer} = with_started_writer(%{opts: opts})

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) == nil

      writer = PureFileStorage.init_writer!(opts, @shape)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test_key", :insert, ~S|{"test":3}|},
            {LogOffset.new(13, 0), "test_key", :insert, ~S|{"test":4}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) ==
               LogOffset.new(11, 0)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(13, 0), opts)
             |> Enum.to_list() == [~S|{"test":1}|, ~S|{"test":2}|, ~S|{"test":3}|, ~S|{"test":4}|]
    end

    test "get_chunk_end_log_offset/2 returns nil when no chunk file is found", %{
      base_opts: base_opts,
      opts: opts
    } do
      chunk_index_path =
        Path.join([base_opts.base_path, @shape_handle, "log", "log.latest.0.chunk.bin"])

      refute File.exists?(chunk_index_path)

      assert nil == PureFileStorage.get_chunk_end_log_offset(LogOffset.new(1, 0), opts)
    end
  end

  describe "flush timer" do
    setup [:start_storage, :with_started_writer]
    @describetag flush_period: 100

    test "flush message arrives after flush period", %{writer: writer} do
      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
    end

    @tag flush_period: 100
    test "multiple writes cause only one flush message", %{writer: writer} do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      refute_receive {Storage, {PureFileStorage, :perform_scheduled_flush, _}}, 200
    end

    @tag flush_period: 50
    test "state after flush is correct", %{writer: writer} do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}

      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      PureFileStorage.append_to_log!(
        [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [1]}}
    end

    @tag flush_period: 50
    test "hibernate with empty buffer doesn't schedule a flush", %{writer: writer} do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}

      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      PureFileStorage.append_to_log!(
        [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [1]}}
      assert_receive {Storage, :flushed, _last_seen_offset}
      refute_receive {Storage, {PureFileStorage, :perform_scheduled_flush, _}}, 200

      _writer = PureFileStorage.hibernate(writer)

      refute_receive {Storage, :flushed, _last_seen_offset}, 200
    end

    @flush_alignment_bytes 64 * 1024
    @tag flush_period: 100
    test "should run scheduled flush after empty buffer alignment", %{writer: writer} do
      large_data = String.duplicate("x", @flush_alignment_bytes + 1000)

      # write small piece of data to trigger scheduling of flush
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      # write large piece of data that goes over buffer limit and forces a flush
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key", :insert, ~s|{"test":"#{large_data}"}|}],
          writer
        )

      # next small piece of data should schedule new flush with larger flush counter
      PureFileStorage.append_to_log!(
        [{LogOffset.new(12, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      # On a slow machine the first small write's flush timer can fire before the
      # forced flush, delivering a counter-0 message ahead of the one under test;
      # what matters is that the post-forced-flush write scheduled a fresh flush
      # with a bumped counter.
      wait_for_bumped_flush = fn recurse ->
        assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [times_flushed]}}
        if times_flushed > 0, do: times_flushed, else: recurse.(recurse)
      end

      assert wait_for_bumped_flush.(wait_for_bumped_flush) > 0
      refute_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [_]}}
    end
  end

  # The write path has two boundaries: bytes handed to the OS, and bytes datasynced. Only the
  # latter emits {Storage, :flushed, _} and so drives the flush boundary and the replication
  # slot ack. These pin what makes the durable one advance.
  describe "sync points" do
    setup [:start_storage, :with_started_writer]
    # Past every timeout below, so a timer tick can never be what syncs.
    @describetag flush_period: 10_000
    # Past everything written below, so a chunk boundary can never be what syncs.
    @describetag chunk_size: 64 * 1024 * 1024

    @delayed_write 64 * 1024

    test "a buffer-size flush writes without syncing and the commit syncs it", %{writer: writer} do
      offset = LogOffset.new(10, 0)
      large_data = String.duplicate("x", @delayed_write + 1000)

      writer =
        PureFileStorage.append_fragment_to_log!(
          [{offset, "test_key", :insert, ~s|{"test":"#{large_data}"}|}],
          writer
        )

      refute_receive {Storage, :flushed, _}, 100

      PureFileStorage.signal_txn_commit!(@xid, writer)

      assert_receive {Storage, :flushed, ^offset}
    end

    test "syncs once unsynced bytes cross the bound, with no commit and no timer",
         %{writer: writer} do
      megabyte = String.duplicate("x", 1024 * 1024)

      write = fn op, writer ->
        PureFileStorage.append_fragment_to_log!(
          [{LogOffset.new(10, op), "test_key", :insert, ~s|{"test":"#{megabyte}"}|}],
          writer
        )
      end

      writer = Enum.reduce(1..7, writer, write)

      refute_receive {Storage, :flushed, _}, 100

      Enum.reduce(8..9, writer, write)

      assert_receive {Storage, :flushed, %LogOffset{}}
    end

    test "a commit whose bytes stay in the buffer leaves the written boundary file alone",
         %{writer: writer, opts: opts} do
      # Arrange
      large_data = String.duplicate("x", @delayed_write + 1000)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{large_data}"}|}],
          writer
        )

      boundary_file =
        Path.join(
          PureFileStorage.shape_data_dir(opts, ["metadata"]),
          "last_written_txn_offset.bin"
        )

      %File.Stat{inode: inode} = File.stat!(boundary_file)

      # Act
      PureFileStorage.append_to_log!(
        [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      # Assert
      assert %File.Stat{inode: ^inode} = File.stat!(boundary_file)
    end
  end

  # Boot replay re-delivers commits the previous run already persisted. Those transactions write
  # nothing, so the writer used to go silent — and the collector's flush tracker, which clears a
  # shape's entry only on a flushed notification carrying that exact offset, kept the shape pinned
  # until the stall watchdog invalidated it.
  describe "replay deduplication" do
    setup [:start_storage, :with_started_writer]
    # Past every timeout below, so a timer tick can never be what notifies.
    @describetag flush_period: 10_000
    @describetag chunk_size: 64 * 1024 * 1024

    test "a fully deduplicated transaction re-confirms the durable offset", %{writer: writer} do
      offset = LogOffset.new(10, 0)

      line =
        {offset, "test_key", :insert, ~s|{"test":"#{String.duplicate("x", @delayed_write)}"}|}

      writer = PureFileStorage.append_fragment_to_log!([line], writer)
      writer = PureFileStorage.signal_txn_commit!(@xid, writer)
      assert_receive {Storage, :flushed, ^offset}

      writer = PureFileStorage.append_fragment_to_log!([line], writer)
      PureFileStorage.signal_txn_commit!(@xid, writer)

      assert_receive {Storage, :flushed, ^offset}
    end

    test "a deduplicated framed block re-confirms the durable offset", %{writer: writer} do
      payload = String.duplicate("x", @delayed_write)

      lines =
        Enum.map(1..2, fn i ->
          {LogOffset.new(10, i), "key#{i}", :insert, ~s|{"test":"#{payload}"}|}
        end)

      last_offset = LogOffset.new(10, 2)
      block = LogFile.build_framed_block(lines)

      writer = PureFileStorage.append_fragment_to_log!([block], writer)
      writer = PureFileStorage.signal_txn_commit!(@xid, writer)
      assert_receive {Storage, :flushed, ^last_offset}

      writer =
        PureFileStorage.append_fragment_to_log!([LogFile.build_framed_block(lines)], writer)

      PureFileStorage.signal_txn_commit!(@xid, writer)

      assert_receive {Storage, :flushed, ^last_offset}
    end

    test "a deduplicated transaction stays silent while its bytes await datasync",
         %{writer: writer} do
      offset = LogOffset.new(10, 0)
      line = {offset, "test_key", :insert, ~S|{"test":1}|}

      writer = PureFileStorage.append_fragment_to_log!([line], writer)
      writer = PureFileStorage.signal_txn_commit!(@xid, writer)
      refute_receive {Storage, :flushed, _}, 100

      writer = PureFileStorage.append_fragment_to_log!([line], writer)
      writer = PureFileStorage.signal_txn_commit!(@xid, writer)

      refute_receive {Storage, :flushed, _}, 100

      PureFileStorage.perform_scheduled_flush(writer, 0)

      assert_receive {Storage, :flushed, ^offset}
    end

    test "a replay straddling the transaction confirms only what it wrote", %{writer: writer} do
      payload = String.duplicate("x", @delayed_write)

      line = fn op ->
        {LogOffset.new(10, op), "key#{op}", :insert, ~s|{"test":"#{payload}"}|}
      end

      writer = PureFileStorage.append_fragment_to_log!([line.(1)], writer)
      writer = PureFileStorage.signal_txn_commit!(@xid, writer)
      assert_receive {Storage, :flushed, _}

      writer = PureFileStorage.append_fragment_to_log!([line.(1), line.(2)], writer)
      PureFileStorage.signal_txn_commit!(@xid, writer)

      new_offset = LogOffset.new(10, 2)
      assert_receive {Storage, :flushed, ^new_offset}
      refute_receive {Storage, :flushed, _}, 100
    end
  end

  # The flushed notification is the only durability claim in the system: it advances the
  # replication slot ack and releases the journal generation covering these bytes. `IO.binwrite`
  # raises on a failed write, but `:file.datasync` only returns the error, so ignoring it would
  # release both copies of data that never reached the disk.
  describe "failing disk" do
    setup [:start_storage, :with_started_writer]
    @describetag flush_period: 10_000
    @describetag chunk_size: 64 * 1024 * 1024

    test "a failed log write raises instead of advancing the boundaries", %{writer: writer} do
      writer =
        PureFileStorage.append_fragment_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      close_log_file(writer)

      assert_raise ErlangError, fn -> PureFileStorage.perform_scheduled_flush(writer, 0) end
      refute_receive {Storage, :flushed, _}, 100
    end

    test "a failed datasync raises instead of claiming a flush", %{writer: writer} do
      payload = String.duplicate("x", @delayed_write)

      writer =
        PureFileStorage.append_fragment_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{payload}"}|}],
          writer
        )

      close_log_file(writer)

      assert_raise MatchError, fn -> PureFileStorage.signal_txn_commit!(@xid, writer) end
      refute_receive {Storage, :flushed, _}, 100
    end
  end

  describe "schedule_compaction/1" do
    setup [:start_storage]

    test "sends a message to the calling process within the predefined time period", ctx do
      compaction_config = Map.put(ctx.base_opts.compaction_config, :period, 5)
      PureFileStorage.schedule_compaction(compaction_config)

      assert_receive {Storage, {PureFileStorage, :scheduled_compaction, [^compaction_config]}}
    end
  end

  describe "compact/2" do
    @describetag chunk_size: 5
    setup [:start_storage]

    test "two shapes compacting at the same time each keep only their own rows", ctx do
      first = PureFileStorage.for_shape("first-shape", ctx.base_opts)
      second = PureFileStorage.for_shape("second-shape", ctx.base_opts)
      seed_log(first, "first")
      seed_log(second, "second")

      [first_message, second_message] =
        [first, second]
        |> Enum.map(fn opts ->
          Task.async(fn ->
            :ok = PureFileStorage.compact(opts, 0)

            receive do
              {Storage, message} -> message
            after
              5000 -> flunk("compaction of #{opts.shape_handle} never reported back")
            end
          end)
        end)
        |> Task.await_many(10_000)

      assert compacted_keys(first, first_message) == for(i <- 1..20, do: "first-#{i}")
      assert compacted_keys(second, second_message) == for(i <- 1..20, do: "second-#{i}")
    end

    test "a compaction that raises releases the latch instead of silencing the shape", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      point_compaction_at_a_missing_generation(ctx.opts)

      :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage, {PureFileStorage, :handle_compaction_failed, []} = message}, 5000
      _writer = apply_storage_message(writer, message)
      PureFileStorage.set_compaction_boundary(ctx.opts, {LogOffset.before_all(), nil})
      assert :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _}}, 5000
    end

    # A read bounded inside the snapshot, at the next chunk while the end was not recorded, can
    # start once it is. A compacted chunk is read whole, and the entries it handed back were
    # streamed again by the next read, from that bound.
    test "a read bounded inside the snapshot takes nothing from a compacted log", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      assert PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.new(0, 1), ctx.opts)
             |> Enum.to_list() == []

      assert length(log_keys(ctx.opts, LogOffset.new(0, 1))) == 20

      PureFileStorage.terminate(writer)
    end

    test "a stream opened before a compaction still reads after the superseded files are gone",
         ctx do
      # stream is lazy, so a request already being served holds the file names it will open long before
      # it opens them.
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      stream = PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), ctx.opts)

      _writer = apply_storage_message(writer, message)
      await_compaction_cleanup(ctx.stack_id)

      assert Enum.count(stream) == 20
    end

    test "repeated compactions keep the generations bounded rather than accumulating", ctx do
      seed_log(ctx.opts, "row")

      # supersedes the one before it.
      Enum.each(1..3, fn round ->
        writer = PureFileStorage.init_writer!(ctx.opts, @shape)
        :ok = PureFileStorage.compact(ctx.opts, 0)

        assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message},
                       5000

        PureFileStorage.terminate(apply_storage_message(writer, message))
        await_compaction_cleanup(ctx.stack_id)

        ctx.opts
        |> PureFileStorage.init_writer!(@shape)
        |> append_rows("row", round * 100)
        |> PureFileStorage.terminate()
      end)

      assert generation_count(ctx.opts, "latest") == 2
      assert generation_count(ctx.opts, "compacted") == 2
    end

    # The writer caches the positions of its last chunks for readers. A compaction moves the log's
    # tail to the start of a new file, and readers got the new name with the old file's positions:
    # a read of the rows the compaction kept went past the end of the new file, and the shape was
    # discarded as having lost data.
    test "a read right after a compaction finds the rows it kept", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

      writer = apply_storage_message(writer, message)

      assert ["row-19", "row-20"] == log_keys(ctx.opts, LogOffset.new(18, 0))
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # After a compaction the writer went on from a blank state, with no open chunk and no cached
    # chunks: rows it wrote next were missing from reads, and after a restart some were gone.
    @tag chunk_size: 300
    test "a writer that compacted keeps every row it writes next", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 1)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      expected = for(i <- 1..20, do: "row-#{i}") ++ for(i <- 101..120, do: "row-#{i}")

      writer = writer |> apply_storage_message(message) |> append_rows("row", 100)

      assert expected == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      assert expected == log_keys(ctx.opts, LogOffset.first())
    end

    # The writer closes a chunk at a write position, so a complete chunk can hold a transaction not
    # yet committed, or not synced: compacted, it would be served whole below the boundary, and
    # recovery trims only the latest generation. A run that found its one candidate chunk past the
    # claim compacted nothing, and a shape with a transaction open there at every run never was.
    test "a compaction stops at the newest chunk under the durable claim", ctx do
      seed_log(ctx.opts, "row")

      writer =
        ctx.opts
        |> PureFileStorage.init_writer!(@shape)
        |> then(&PureFileStorage.append_fragment_to_log!(rows("row", 100), &1))

      assert :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [boundary, _, _, _]}},
                     5000

      assert LogOffset.new(20, 0) == boundary
      writer = PureFileStorage.signal_txn_commit!(@xid, writer)
      PureFileStorage.terminate(writer)
      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [boundary, _, _, _]}},
                     5000

      assert LogOffset.new(120, 0) == boundary
    end

    # With no chunk to keep, the lookup of the chunk to cut at took the log's open chunk for the
    # last complete one, returned it as incomplete, and the compaction raised on it.
    @tag chunk_size: 300
    test "a compaction that keeps no chunk cuts before the open one", ctx do
      seed_log(ctx.opts, "row")
      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      chunk_index = log_path(ctx.opts, "latest.0", "chunk")
      assert {:incomplete, _, _, _} = ChunkIndex.get_nth_chunk(chunk_index, -1)
      assert {:complete, {_, last_complete}, _, _} = ChunkIndex.get_nth_chunk(chunk_index, -2)

      assert :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [boundary, _, _, _]}},
                     5000

      assert last_complete == boundary
    end

    # A run whose writer was killed under it never publishes, takes a suffix past every generation
    # on disk, and can finish after the live run has copied its files: the cleanup kept the two
    # newest suffixes, the killed runs', and deleted the generation the writer had just published.
    test "the cleanup keeps the generation published under the files of killed runs", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [_, published, _, _]} =
                        message},
                     5000

      "compacted." <> position = published

      for n <- 1..2, kind <- ["jsonfile", "chunk", "keyfile"] do
        File.cp!(
          log_path(ctx.opts, published, kind),
          log_path(ctx.opts, "compacted.#{String.to_integer(position) + n}", kind)
        )
      end

      writer = apply_storage_message(writer, message)
      await_compaction_cleanup(ctx.stack_id)

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do: assert(File.exists?(log_path(ctx.opts, published, kind)))

      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    test "a writer clears a latch its predecessor never heard released", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      point_compaction_at_a_missing_generation(ctx.opts)
      :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_failed, []}}, 5000
      PureFileStorage.terminate(writer)
      PureFileStorage.set_compaction_boundary(ctx.opts, {LogOffset.before_all(), nil})

      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)

      assert :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _}}, 5000
    end

    # A compaction task outlives a writer killed under it and never publishes. With the clock behind
    # the current generation, the next run took the suffix that orphan took and rewrote its files.
    test "a compaction takes a suffix past every generation on disk", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [boundary, current, _, _]} =
                        message},
                     5000

      writer = writer |> apply_storage_message(message) |> append_rows("row", 100)
      await_compaction_cleanup(ctx.stack_id)
      ahead = DateTime.utc_now() |> DateTime.to_unix(:millisecond) |> Kernel.+(3_600_000)

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do:
            File.cp!(
              log_path(ctx.opts, current, kind),
              log_path(ctx.opts, "compacted.#{ahead}", kind)
            )

      PureFileStorage.set_compaction_boundary(ctx.opts, {boundary, "compacted.#{ahead}"})
      orphan = log_path(ctx.opts, "compacted.#{ahead + 1}", "jsonfile")
      File.write!(orphan, "a killed writer's run")

      :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, [_, suffix, _, _]}},
                     5000

      refute suffix == "compacted.#{ahead + 1}"
      assert File.read!(orphan) == "a killed writer's run"
      PureFileStorage.terminate(writer)
    end

    # The same two runs, when both take their suffix before either has written to it.
    test "a compaction that finds its suffix taken fails rather than replace those files", ctx do
      seed_log(ctx.opts, "row")
      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)

      Repatch.patch(PureFileStorage, :json_file, [mode: :shared], fn opts, suffix ->
        path = log_path(opts, suffix, "jsonfile")

        if String.starts_with?(suffix, "compacted.") and not File.exists?(path),
          do: File.write!(path, "another run's")

        path
      end)

      :ok = PureFileStorage.compact(ctx.opts, 0)

      assert_receive {Storage, {PureFileStorage, :handle_compaction_failed, []}}, 5000
      assert [taken] = Path.wildcard(log_path(ctx.opts, "compacted.*", "jsonfile"))
      assert File.read!(taken) == "another run's"
    end

    # A read from disk took the boundary, then the latest name: a switch between the two paired
    # the boundary before it with the latest generation after it, which starts past the rows the
    # switch compacted, and the read skipped them.
    test "a read from disk across a switch finds every row", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

      writer
      |> apply_storage_message(message)
      |> append_rows("row", 100)
      |> PureFileStorage.terminate()

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      before = names_on_disk(ctx.opts)
      writer = apply_storage_message(writer, message)
      switched = names_on_disk(ctx.opts)
      refute switched == before
      put_names_on_disk(ctx.opts, before)

      Repatch.patch(PureFileStorage, :read_metadata!, [mode: :local], fn opts, key ->
        value =
          case {key, File.read(metadata_path(opts, key))} do
            {:snapshot_started?, read} -> match?({:ok, _}, read)
            {_, {:ok, bytes}} -> :erlang.binary_to_term(bytes)
            {_, {:error, :enoent}} -> nil
          end

        if key in [:latest_name, :compaction_boundary] and
             is_nil(Process.put(:switched_between_reads, true)),
           do: put_names_on_disk(opts, switched)

        value
      end)

      # From the last virtual offset, the read takes the names once, in a single metadata read.
      assert for(i <- 1..20, do: "row-#{i}") ++ for(i <- 101..120, do: "row-#{i}") ==
               log_keys(%{ctx.opts | read_only?: true}, LogOffset.last_before_real_offsets())

      assert Process.get(:switched_between_reads)
      PureFileStorage.terminate(writer)
    end

    # The key index was sorted by key alone, so a key in both the new part of the log and the
    # previous compacted generation came out of the sort in file order, the new part first. The
    # merge took the older compacted entry for the key's newest: the generation kept its offset
    # and value, every update since the previous compaction was lost, and it ended short of its
    # boundary.
    test "a key updated across two compactions keeps its newest value", ctx do
      update = fn n ->
        {LogOffset.new(n, 0), "same", :update,
         ~s|{"key":"same","value":{"id":"same","n":"#{n}"},| <>
           ~s|"headers":{"operation":"update","relation":["public","test_table"]}}|}
      end

      seed_lines(ctx.opts, Enum.map(1..10, update))

      for range <- [11..20, nil] do
        writer = PureFileStorage.init_writer!(ctx.opts, @shape)
        :ok = PureFileStorage.compact(ctx.opts, 0)

        assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message},
                       5000

        writer = apply_storage_message(writer, message)

        if range,
          do:
            PureFileStorage.terminate(
              PureFileStorage.append_to_log!(Enum.map(range, update), writer)
            ),
          else: PureFileStorage.terminate(writer)
      end

      assert {%LogOffset{tx_offset: 20, op_offset: 0}, _} =
               PureFileStorage.compaction_boundary(ctx.opts)

      opts = %{ctx.opts | read_only?: true}

      assert [%{"value" => %{"id" => "same", "n" => "20"}}] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.map(&Jason.decode!/1)

      PureFileStorage.terminate(PureFileStorage.init_writer!(ctx.opts, @shape))
    end

    # A read routes each chunk by the names it read first, and the cleanup after a switch removes
    # the generation published before the previous one. Two switches between those names and a
    # chunk lookup removed the index they named, which reads as a shape that has logged nothing:
    # the response ended there with no error, short of the offset it advertised.
    test "a read whose generation two switches removed goes on in the newer ones", ctx do
      seed_log(ctx.opts, "row")

      first_names = [
        latest_name: :erlang.term_to_binary(PureFileStorage.latest_name(ctx.opts)),
        compaction_boundary: :erlang.term_to_binary(PureFileStorage.compaction_boundary(ctx.opts))
      ]

      for offset_base <- [100, 200] do
        writer = PureFileStorage.init_writer!(ctx.opts, @shape)
        :ok = PureFileStorage.compact(ctx.opts, 2)

        assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message},
                       5000

        writer
        |> apply_storage_message(message)
        |> append_rows("row", offset_base)
        |> PureFileStorage.terminate()

        await_compaction_cleanup(ctx.stack_id)
      end

      refute File.exists?(log_path(ctx.opts, "latest.0", "chunk"))
      switched = names_on_disk(ctx.opts)
      put_names_on_disk(ctx.opts, first_names)

      Repatch.patch(ChunkIndex, :fetch_chunk, [mode: :local], fn path, offset ->
        if is_nil(Process.put(:switched_before_lookup, true)),
          do: put_names_on_disk(ctx.opts, switched)

        Repatch.real(ChunkIndex.fetch_chunk(path, offset))
      end)

      assert for(base <- [0, 100, 200], i <- 1..20, do: "row-#{base + i}") ==
               log_keys(%{ctx.opts | read_only?: true}, LogOffset.last_before_real_offsets())

      assert Process.get(:switched_before_lookup)
    end

    # A writer killed between a switch's two names left a latest generation that still held the
    # rows its boundary had moved to the compacted one, and the next compaction merged both
    # copies: an insert and a delete of one key each came out of it twice. A chunk large enough to
    # hold both copies hands both to a reader.
    @tag chunk_size: 300
    test "a reopen finishes a switch killed between its names", ctx do
      seed_lines(ctx.opts, [
        {LogOffset.new(1, 0), "gone", :insert,
         ~s|{"key":"gone","value":{"id":"gone"},"headers":{"operation":"insert"}}|},
        {LogOffset.new(2, 0), "gone", :delete,
         ~s|{"key":"gone","headers":{"operation":"delete"}}|}
        | Enum.drop(rows("row", 0), 2)
      ])

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

      Repatch.patch(PureFileStorage, :set_latest_name, [mode: :local], fn opts, name, chunks ->
        if Process.put(:killed_between_names, true),
          do: Repatch.real(PureFileStorage.set_latest_name(opts, name, chunks)),
          else: raise("killed between the names")
      end)

      assert_raise RuntimeError, fn -> apply_storage_message(writer, message) end

      ctx.opts
      |> PureFileStorage.init_writer!(@shape)
      |> append_rows("row", 100)
      |> PureFileStorage.terminate()

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      assert ["gone", "gone"] ++
               for(i <- 3..20, do: "row-#{i}") ++ for(i <- 101..120, do: "row-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())

      PureFileStorage.terminate(writer)
    end

    # A switch killed between its names leaves its new latest generation unnamed, and so does a
    # reopen that finishes the switch and is killed before its name. Each retry copied the tail to
    # a new generation and kept every one before it, so a reopen killed in each copy of a large
    # tail filled the disk.
    @tag chunk_size: 300
    test "a reopen that finishes a switch removes the latest generations no name points at",
         ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      named = PureFileStorage.latest_name(ctx.opts)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      kill_the_switch_between_its_names(writer, message)
      assert [unnamed] = latest_generations(ctx.opts) -- [named]

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)

      published = PureFileStorage.latest_name(ctx.opts)
      assert published not in [named, unnamed]
      assert latest_files(ctx.opts) == generation_files([named, published], ["chunk", "jsonfile"])

      assert for(i <- 1..20, do: "row-#{i}") ==
               log_keys(ctx.opts, LogOffset.last_before_real_offsets())

      PureFileStorage.terminate(writer)
    end

    # A read that fills the cache from the disk across a switch, once the entry was gone, inserts
    # the name the switch replaced. A compaction from that name cut at positions of the generation
    # before, and its switch removed the one the writer appends to, with every row written since.
    @tag chunk_size: 300
    test "a compaction under a cache that names the generation before compacts the writer's",
         ctx do
      {writer, _previous} = switch_under_a_stale_cache_row(ctx.opts)
      writer = writer |> append_rows("row", 100) |> PureFileStorage.hibernate()

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      assert for(i <- 1..20, do: "row-#{i}") ++ for(i <- 101..120, do: "row-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())

      PureFileStorage.terminate(writer)
    end

    # The disk names the writer's generation, so the run never starts: its end discarded it, after
    # it had scanned and merged the whole log.
    @tag chunk_size: 300
    test "a compaction from another generation than the writer's does not start", ctx do
      {writer, _previous} = switch_under_a_stale_cache_row(ctx.opts)
      writer = writer |> append_rows("row", 100) |> PureFileStorage.hibernate()
      named = names_on_disk(ctx.opts)

      :ok = PureFileStorage.compact(ctx.opts, 0)

      refute compaction_latched?(ctx.opts)
      assert names_on_disk(ctx.opts) == named

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      assert for(i <- 1..20, do: "row-#{i}") ++ for(i <- 101..120, do: "row-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())

      PureFileStorage.terminate(writer)
    end

    # The writer's own generation is not the one the disk names: the run switches to neither.
    @tag chunk_size: 300
    test "a compaction the disk names another generation for is discarded", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      path = metadata_path(ctx.opts, :latest_name)
      named = File.read!(path)
      writer = PureFileStorage.compact(writer, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [_, discarded, _, ^latest]} =
                        message},
                     5000

      File.write!(path, :erlang.term_to_binary("latest.9"))
      writer = apply_storage_message(writer, message)

      assert File.read!(path) == :erlang.term_to_binary("latest.9")
      assert PureFileStorage.latest_name(ctx.opts) == latest

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do: refute(File.exists?(log_path(ctx.opts, discarded, kind)))

      File.write!(path, named)
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # A read of the disk's name that failed raised out of the consumer, which removed the shape.
    @tag chunk_size: 300
    test "a compaction whose switch cannot read the disk's name is discarded", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      path = metadata_path(ctx.opts, :latest_name)
      named = File.read!(path)
      writer = PureFileStorage.compact(writer, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [_, discarded, _, ^latest]} =
                        message},
                     5000

      File.rm!(path)
      File.mkdir!(path)
      writer = apply_storage_message(writer, message)

      assert PureFileStorage.latest_name(ctx.opts) == latest

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do: refute(File.exists?(log_path(ctx.opts, discarded, kind)))

      File.rmdir!(path)
      File.write!(path, named)
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # The switch read the boundary it replaces after it had checked the disk's name: a read that
    # failed, with no file descriptor left say, raised before the switch could return a writer or
    # release the latch, out of the consumer, which removed the shape.
    @tag chunk_size: 300
    test "a compaction whose switch cannot read the compaction boundary is discarded", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      writer = PureFileStorage.compact(writer, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [_, discarded, _, ^latest]} =
                        message},
                     5000

      refuse_the_open_of("compaction_boundary.bin")

      {writer, log} =
        ExUnit.CaptureLog.with_log(fn -> apply_storage_message(writer, message) end)

      assert log =~ "its compaction boundary cannot be read"

      Repatch.restore(File, :open, 3)
      refute compaction_latched?(ctx.opts)
      assert PureFileStorage.latest_name(ctx.opts) == latest

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do: refute(File.exists?(log_path(ctx.opts, discarded, kind)))

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # Bytes that do not decode to a boundary were taken for a read that failed: every run skipped
    # on them, and a switch discarded on them left them in place. They are lost data.
    @tag chunk_size: 300
    test "a compaction discards the shape whose compaction boundary does not decode", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      path = metadata_path(ctx.opts, :compaction_boundary)
      discards_reported_to_this_process(ctx.opts.stack_id)

      for bytes <- undecodable_boundaries() do
        File.write!(path, bytes)

        error =
          assert_raise Storage.Error, ~r/compaction boundary does not decode/, fn ->
            PureFileStorage.compact(writer, 0)
          end

        assert error.kind == :data_loss
        assert_received {:discarded, @shape_handle}
        refute compaction_latched?(ctx.opts)
      end

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 300
    test "a compaction whose switch finds a boundary that does not decode discards the shape",
         ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      path = metadata_path(ctx.opts, :compaction_boundary)
      discards_reported_to_this_process(ctx.opts.stack_id)

      for bytes <- undecodable_boundaries() do
        File.rm(path)
        writer = PureFileStorage.compact(writer, 0)

        assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message},
                       5000

        File.write!(path, bytes)

        error =
          assert_raise Storage.Error, ~r/compaction boundary does not decode/, fn ->
            apply_storage_message(writer, message)
          end

        assert error.kind == :data_loss
        assert_received {:discarded, @shape_handle}
        assert PureFileStorage.latest_name(ctx.opts) == latest
        assert File.read!(path) == bytes
        PureFileStorage.handle_compaction_failed(writer)
      end

      PureFileStorage.terminate(writer)
    end

    # The discard's release of the latch on disk failed as the name's read did, with no file
    # descriptor left say: it raised out of the consumer, which removed the shape.
    @tag chunk_size: 300
    test "a discard that cannot release the latch on disk lets the next run start", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      path = metadata_path(ctx.opts, :latest_name)
      named = File.read!(path)
      latch_tmp = metadata_path(ctx.opts, :compaction_started?) <> ".tmp"
      writer = PureFileStorage.compact(writer, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [_, discarded, _, ^latest]} =
                        message},
                     5000

      File.rm!(path)
      File.mkdir!(path)
      File.mkdir!(latch_tmp)
      writer = apply_storage_message(writer, message)

      # The copy on disk is removed, and the latch released in the cache.
      refute File.exists?(metadata_path(ctx.opts, :compaction_started?))

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do: refute(File.exists?(log_path(ctx.opts, discarded, kind)))

      File.rmdir!(latch_tmp)
      File.rmdir!(path)
      File.write!(path, named)
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # A switch that had written both names and then could not release the latch on disk raised out
    # of the consumer, which removed the shape its compaction had just served.
    @tag chunk_size: 300
    test "a switch that cannot release the latch on disk keeps the switched writer", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      latch_tmp = metadata_path(ctx.opts, :compaction_started?) <> ".tmp"
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

      File.mkdir!(latch_tmp)
      writer = apply_storage_message(writer, message)
      File.rmdir!(latch_tmp)

      new_latest = PureFileStorage.latest_name(ctx.opts)
      refute new_latest == latest
      refute File.exists?(metadata_path(ctx.opts, :compaction_started?))
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())

      # The latch is released in the cache and its copy removed: the next run starts and switches.
      writer = PureFileStorage.hibernate(append_rows(writer, "more", 20))
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == new_latest

      assert for(i <- 1..20, do: "row-#{i}") ++ for(i <- 21..40, do: "more-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())

      PureFileStorage.terminate(writer)
    end

    # A switch whose release of the latch on disk failed released it in the cache alone, and a
    # cache filled again from the disk took it back: every later run found one in progress.
    @tag chunk_size: 300
    test "a latch whose release failed on disk stays released in a cache filled again", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      latch_tmp = metadata_path(ctx.opts, :compaction_started?) <> ".tmp"
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

      File.mkdir!(latch_tmp)
      writer = apply_storage_message(writer, message)
      File.rmdir!(latch_tmp)
      new_latest = PureFileStorage.latest_name(ctx.opts)
      refute new_latest == latest

      writer = PureFileStorage.hibernate(append_rows(writer, "more", 20))
      true = :ets.delete(ctx.opts.stack_ets, ctx.opts.shape_handle)
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == new_latest

      assert for(i <- 1..20, do: "row-#{i}") ++ for(i <- 21..40, do: "more-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())

      PureFileStorage.terminate(writer)
    end

    # A latch that could not be taken on disk raised out of the consumer, which removed the shape.
    @tag chunk_size: 300
    test "a compaction whose latch cannot be taken on disk does not start", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      latch_tmp = metadata_path(ctx.opts, :compaction_started?) <> ".tmp"
      File.mkdir!(latch_tmp)

      {writer, log} = ExUnit.CaptureLog.with_log(fn -> PureFileStorage.compact(writer, 0) end)
      assert log =~ "its latch cannot be taken on disk"

      File.rmdir!(latch_tmp)
      refute compaction_latched?(ctx.opts)
      assert PureFileStorage.latest_name(ctx.opts) == latest

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # The durable claim was read before the chunk count: a read that failed, with no file
    # descriptor left say, raised out of the consumer, which removed the shape, on a log too short
    # to compact as well.
    @tag chunk_size: 300
    test "a compaction skips the run while its durable claim cannot be read", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      refuse_the_claim_read()

      {writer, log} = ExUnit.CaptureLog.with_log(fn -> PureFileStorage.compact(writer, 1000) end)
      refute log =~ "Skipping a compaction"

      {writer, log} = ExUnit.CaptureLog.with_log(fn -> PureFileStorage.compact(writer, 0) end)
      assert log =~ "its durable claim or its compaction boundary cannot be read"

      Repatch.restore(File, :open, 3)
      refute compaction_latched?(ctx.opts)
      assert PureFileStorage.latest_name(ctx.opts) == latest

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # The chunk index was opened before its size told whether a chunk is past the kept ones: an open
    # that failed, with no file descriptor left say, raised out of the consumer, which removed the
    # shape, on a log too short to compact as well.
    @tag chunk_size: 300
    test "a compaction skips the run while its chunk index cannot be opened", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      chunks = log_path(ctx.opts, latest, "chunk")

      # Complete chunks, and the half record of the open one, which counts for none.
      %{size: size} = File.stat!(chunks)
      complete = div(size, 64)
      assert complete >= 1
      assert rem(size, 64) == 32

      refuse_the_open_of(Path.basename(chunks))

      {writer, log} =
        ExUnit.CaptureLog.with_log(fn -> PureFileStorage.compact(writer, complete) end)

      refute log =~ "Skipping a compaction"

      {writer, log} =
        ExUnit.CaptureLog.with_log(fn -> PureFileStorage.compact(writer, complete - 1) end)

      assert log =~ "its chunk index cannot be read"

      Repatch.restore(File, :open, 3)
      refute compaction_latched?(ctx.opts)
      assert PureFileStorage.latest_name(ctx.opts) == latest

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # A run that started while the disk named its generation ends after the writer's moved on.
    @tag chunk_size: 300
    test "a compaction that ends on another generation than the writer's is discarded", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      path = metadata_path(ctx.opts, :latest_name)
      named = File.read!(path)
      boundary = PureFileStorage.compaction_boundary(ctx.opts)
      writer = PureFileStorage.compact(writer, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [offset, discarded, log_file_pos, ^latest]}},
                     5000

      message =
        {PureFileStorage, :handle_compaction_finished,
         [offset, discarded, log_file_pos, "latest.7"]}

      # A run from another generation than the writer's is discarded before the disk is read: the
      # record on disk still names the writer's, and a read of it would discard the run as well.
      Repatch.patch(File, :read, fn read_path ->
        if read_path == path,
          do: flunk("the disk's name was read"),
          else: Repatch.real(File.read(read_path))
      end)

      writer = apply_storage_message(writer, message)
      Repatch.restore(File, :read, 1)

      refute compaction_latched?(ctx.opts)
      assert File.read!(path) == named
      assert PureFileStorage.compaction_boundary(ctx.opts) == boundary
      assert PureFileStorage.latest_name(ctx.opts) == latest

      for kind <- ["jsonfile", "chunk", "keyfile"],
          do: refute(File.exists?(log_path(ctx.opts, discarded, kind)))

      PureFileStorage.terminate(writer)
    end

    # A name the disk could not give discarded each run at its end, after it had scanned, merged
    # and synced the whole log, once every period for as long as the name stayed unreadable.
    @tag chunk_size: 300
    test "a compaction does not start while the disk's name cannot be read", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      latest = PureFileStorage.latest_name(ctx.opts)
      path = metadata_path(ctx.opts, :latest_name)
      named = File.read!(path)
      File.rm!(path)
      File.mkdir!(path)

      writer = PureFileStorage.compact(writer, 0)

      refute compaction_latched?(ctx.opts)
      assert PureFileStorage.latest_name(ctx.opts) == latest

      File.rmdir!(path)
      File.write!(path, named)
      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      refute PureFileStorage.latest_name(ctx.opts) == latest
      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # A record that decodes to no name raised as the discard logged it, and one that does not
    # decode raised as the switch read it: either ended the consumer, which removed the shape.
    @tag chunk_size: 300
    test "a compaction whose disk record holds no name is discarded", ctx do
      assert_discarded_under_the_disk_record(ctx.opts, :erlang.term_to_binary({"latest", 9}))
    end

    @tag chunk_size: 300
    test "a compaction whose disk record does not decode is discarded", ctx do
      assert_discarded_under_the_disk_record(ctx.opts, "")
    end

    # A removal that raised ended the whole removal: every file after the one that could not go
    # stayed, at each new generation too.
    @tag chunk_size: 300
    test "a switch that cannot remove one unnamed file removes the others", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      assert "latest.0" == PureFileStorage.latest_name(ctx.opts)
      [stuck | removable] = plant_generation_files(ctx.opts, ["latest.1", "latest.2"])
      refuse_to_remove(stuck)

      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      assert File.exists?(stuck)
      assert [] == Enum.filter(removable, &File.exists?/1)
      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 300
    test "a cleanup that cannot remove one replaced file removes the others", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)
      await_compaction_cleanup(ctx.stack_id)

      # Below the generation the switch published, so the next switch's cleanup takes them.
      planted = plant_generation_files(ctx.opts, ["latest.1"])
      [stuck | removable] = generation_paths(ctx.opts, ["latest.0"]) ++ planted
      refuse_to_remove(stuck)

      writer = PureFileStorage.compact(writer, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)
      await_compaction_cleanup(ctx.stack_id)

      assert File.exists?(stuck)
      assert [] == Enum.filter(removable, &File.exists?/1)
      PureFileStorage.terminate(writer)
    end

    # With the compacted generation covering the claim, the trim takes a latest chunk index a
    # crash took for the compaction. A switch killed between its names then kept its log of rows
    # the compacted generation holds behind an empty index, the copy found no cut in it, and the
    # next compaction merged that log from its first byte again.
    @tag chunk_size: 300
    test "a reopen whose switch lost its latest chunk index keeps no compacted rows", ctx do
      kill_a_covering_switch_between_its_names(ctx.opts)
      File.rm!(log_path(ctx.opts, "latest.0", "chunk"))

      ctx.opts
      |> PureFileStorage.init_writer!(@shape)
      |> append_rows("row", 100)
      |> PureFileStorage.terminate()

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      writer = apply_storage_message(writer, message)

      assert covered_keys() ++ for(i <- 101..120, do: "row-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())

      PureFileStorage.terminate(writer)
    end

    # The same switch with its latest log lost and its chunk index whole: the copy started past
    # the end of the empty log the trim left, failed, and the shape was removed, though the
    # compacted generation holds every row it had claimed.
    @tag chunk_size: 300
    test "a reopen whose switch lost its latest log serves the compacted rows", ctx do
      kill_a_covering_switch_between_its_names(ctx.opts)
      File.rm!(log_path(ctx.opts, "latest.0", "jsonfile"))

      ctx.opts
      |> PureFileStorage.init_writer!(@shape)
      |> append_rows("row", 100)
      |> PureFileStorage.terminate()

      assert covered_keys() ++ for(i <- 101..120, do: "row-#{i}") ==
               log_keys(ctx.opts, LogOffset.first())
    end

    # A build that synced nothing after a switch could lose the chunk index entries past a later
    # compaction's boundary while the log kept the lines they map. A switch killed between its
    # names then copied those lines behind an empty index, and the shape advertised its claim and
    # served none of them.
    @tag chunk_size: 300
    test "a reopen refuses a switch whose latest chunk index stops at the boundary", ctx do
      assert_reopen_refuses_a_switch_with_its_index_cut(ctx.opts, 0)
    end

    # The same with one chunk past the boundary left: the copy kept it, and reads past it came
    # back empty.
    @tag chunk_size: 300
    test "a reopen refuses a switch whose latest chunk index stops short of its log", ctx do
      assert_reopen_refuses_a_switch_with_its_index_cut(ctx.opts, 1)
    end

    # The same with the boundary's own entry lost: the copy found no cut, the reopen kept the
    # latest generation as it was, and reads past the index came back empty.
    @tag chunk_size: 300
    test "a reopen refuses a switch whose latest chunk index lost its cut", ctx do
      assert_reopen_refuses_a_switch_with_its_index_cut(ctx.opts, -1)
    end

    # The same with the opening of the boundary's chunk left: the index ends on an open chunk, which
    # no log byte past it shows short, and the reopen found no cut and kept the latest generation
    # whole for the next compaction to cut below the boundary.
    @tag chunk_size: 300
    test "a reopen refuses a switch whose latest chunk index kept only the opening of its cut",
         ctx do
      assert_reopen_refuses_a_switch_with_its_index_cut(
        ctx.opts,
        -1,
        1,
        ~r/lost the chunk ending/
      )
    end

    # A latest generation with a chunk at or below the boundary is a switch left unfinished: a cut
    # there published a boundary below rows the compacted generation held.
    @tag chunk_size: 300
    test "a compaction never cuts at or below the compaction boundary", ctx do
      writer = seed_lines_open(ctx.opts, rows("row", 0))

      {{_, last}, _, _} =
        ctx.opts
        |> log_path(PureFileStorage.latest_name(ctx.opts), "chunk")
        |> ChunkIndex.read_chunk_file()
        |> Enum.reject(&match?({{_, nil}, _, _}, &1))
        |> List.last()

      assert {:ok, claim} = PureFileStorage.fetch_latest_offset(ctx.opts)
      refute LogOffset.compare(last, claim) == :gt
      PureFileStorage.set_compaction_boundary(ctx.opts, {last, "compacted.1"})

      # A run that found a cut would hold the latch, and the second call would find it taken.
      assert :ok = PureFileStorage.compact(ctx.opts, 0)
      assert :ok = PureFileStorage.compact(ctx.opts, 0)
      PureFileStorage.terminate(writer)
    end

    # With no switch to finish: the reopen kept the index, an append opened a chunk past the lines
    # it lost, and a later switch copied those lines behind its cut.
    @tag chunk_size: 300
    test "a reopen refuses a log whose chunk index stops short of it", ctx do
      seed_lines(ctx.opts, rows("row", 0))
      chunks = log_path(ctx.opts, "latest.0", "chunk")
      entries = chunks |> ChunkIndex.read_chunk_file() |> Enum.to_list()
      assert length(entries) > 2
      assert {{_, max}, _, _} = Enum.at(entries, 1)
      refute is_nil(max)

      File.open!(chunks, [:read, :write, :raw], fn file ->
        {:ok, _} = :file.position(file, 2 * 64)
        :ok = :file.truncate(file)
      end)

      error =
        assert_raise Storage.Error, ~r/stops short of its log/, fn ->
          PureFileStorage.init_writer!(ctx.opts, @shape)
        end

      assert error.kind == :data_loss
    end

    # A last chunk that is complete and ends at the claim leaves no log byte past it: the switch
    # keeping it is whole, and the reopen finishes it.
    @tag chunk_size: 300
    test "a reopen finishes a switch whose kept chunk ends at the claim", ctx do
      writer = seed_lines_open(ctx.opts, lines_whose_last_row_closes_a_chunk())
      :ok = PureFileStorage.compact(ctx.opts, 1)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished, [boundary, _, _, _]} =
                        message},
                     5000

      refute boundary == LogOffset.new(21, 0)
      latest = PureFileStorage.latest_name(ctx.opts)
      kill_the_switch_between_its_names(writer, message)

      assert {:complete, last, end_pos, _} =
               ChunkIndex.get_last_boundary(log_path(ctx.opts, latest, "chunk"))

      assert last == LogOffset.new(21, 0)
      assert end_pos == File.stat!(log_path(ctx.opts, latest, "jsonfile")).size
      assert :erlang.binary_to_term(File.read!(metadata_path(ctx.opts, :latest_name))) == latest

      assert {^boundary, compacted} =
               :erlang.binary_to_term(File.read!(metadata_path(ctx.opts, :compaction_boundary)))

      refute is_nil(compacted)

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      refute :erlang.binary_to_term(File.read!(metadata_path(ctx.opts, :latest_name))) == latest
      assert covered_keys() == log_keys(ctx.opts, LogOffset.first())
      PureFileStorage.terminate(writer)
    end

    # The switch flushed the buffer under a pending flush timer and left the timer set: it fired
    # with a flush count that had moved on, did nothing and stayed set, and the commit after it
    # scheduled no flush, so its bytes stayed buffered and unsynced.
    # Ten rows to a chunk, and the twentieth closes one, so the small commits below stay buffered.
    @tag chunk_size: 1000, flush_period: 50
    test "a commit after a compaction gets a flush of its own", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 1)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000

      writer =
        [{LogOffset.new(101, 0), "small", :insert, ~S|{"n":101}|}]
        |> PureFileStorage.append_to_log!(writer)
        |> apply_storage_message(message)

      writer =
        receive do
          {Storage, {PureFileStorage, :perform_scheduled_flush, _} = flush} ->
            apply_storage_message(writer, flush)
        after
          200 -> writer
        end

      PureFileStorage.append_to_log!(
        [{LogOffset.new(102, 0), "small", :insert, ~S|{"n":102}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, _}}, 1000
    end

    # The switch ignored the result of its tail copy: a disk that filled up mid-copy left the new
    # log short under a whole index, both names went to it, and reads past the cut discarded the
    # shape. Here the new log is `/dev/full`, whose writes fail as a full disk's do, and whose
    # sync is skipped, as a short file's would succeed.
    test "a switch whose tail copy fails names neither generation", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      current = PureFileStorage.latest_name(ctx.opts)
      names = {current, PureFileStorage.compaction_boundary(ctx.opts)}

      Repatch.patch(PureFileStorage, :json_file, [mode: :local], fn opts, suffix ->
        path = log_path(opts, suffix, "jsonfile")

        if String.starts_with?(suffix, "latest.") and suffix != current and
             not File.exists?(path),
           do: File.ln_s!("/dev/full", path)

        path
      end)

      Repatch.patch(Electric.Utils, :datasync_file!, [mode: :local], fn path ->
        if File.read_link(path) == {:ok, "/dev/full"},
          do: :ok,
          else: Repatch.real(Electric.Utils.datasync_file!(path))
      end)

      assert_raise File.Error, ~r/enospc|no space/i, fn ->
        apply_storage_message(writer, message)
      end

      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)

      assert names ==
               {PureFileStorage.latest_name(ctx.opts),
                PureFileStorage.compaction_boundary(ctx.opts)}

      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
    end

    # A copy can also end early with no error and report the bytes it copied. Here the tail is
    # reported a byte longer than the file holds, so the whole copy falls a byte short of it.
    test "a switch whose tail copy comes up short names neither generation", ctx do
      seed_log(ctx.opts, "row")
      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      :ok = PureFileStorage.compact(ctx.opts, 2)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, _} = message}, 5000
      current = PureFileStorage.latest_name(ctx.opts)
      names = {current, PureFileStorage.compaction_boundary(ctx.opts)}
      source = log_path(ctx.opts, current, "jsonfile")

      Repatch.patch(FileInfo, :get_file_size!, [mode: :local], fn path ->
        size = Repatch.real(FileInfo.get_file_size!(path))

        if path == source and is_nil(Process.put(:tail_reported_long, true)),
          do: size + 1,
          else: size
      end)

      assert_raise Storage.Error, ~r/copied \d+ of \d+ bytes/, fn ->
        apply_storage_message(writer, message)
      end

      assert Process.get(:tail_reported_long)
      _writer = PureFileStorage.init_writer!(ctx.opts, @shape)

      assert names ==
               {PureFileStorage.latest_name(ctx.opts),
                PureFileStorage.compaction_boundary(ctx.opts)}

      assert for(i <- 1..20, do: "row-#{i}") == log_keys(ctx.opts, LogOffset.first())
    end
  end

  describe "hibernation" do
    setup [:start_storage]

    test "correctly continues writing after hibernation", %{opts: opts} do
      %{writer: writer} = with_started_writer(%{opts: opts})

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(13, 0), opts)
             |> Enum.to_list() == [~S|{"test":1}|]

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":2}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(12, 0), opts)
             |> Enum.to_list() == [~S|{"test":1}|, ~S|{"test":2}|]
    end
  end

  describe "ETS read/write race condition" do
    setup [:start_storage, :with_started_writer]
    @describetag flush_period: 50

    test "reader falls back to disk when ETS is empty due to concurrent flush", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      # Write data - goes to ETS buffer
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      # Wait for and trigger flush - data now on disk, ETS cleared
      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      # Get the fresh metadata after flush
      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(last_persisted_offset: fresh_last_persisted, ets_table: ets_ref) =
               fresh_meta

      assert fresh_last_persisted == LogOffset.new(10, 0)
      assert :ets.info(ets_ref, :size) == 0

      # Create stale metadata - same as fresh but with old last_persisted
      # This simulates what a reader would see if it read metadata BEFORE the flush
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.last_before_real_offsets(),
          # Also set last_seen so upper_read_bound covers our data
          last_seen_txn_offset: LogOffset.new(10, 0)
        )

      # Insert stale metadata - reader will think data is in ETS
      :ets.insert(stack_ets, stale_meta)

      # Reader sees stale metadata (last_persisted = before_real), tries ETS, gets empty.
      # Fix detects empty ETS and falls back to disk using upper_read_bound.
      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(0, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|],
             "Reader should fall back to disk when ETS is empty"

      PureFileStorage.terminate(writer)
    end

    test "reader falls back to disk when ETS returns partial data due to concurrent flush", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      # Write multiple entries - all go to ETS buffer
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test": 2}|}],
          writer
        )

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(12, 0), "test_key", :insert, ~S|{"test": 3}|}],
          writer
        )

      # Wait for and trigger flush - all data now on disk, ETS cleared
      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      # Get the fresh metadata after flush
      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(last_persisted_offset: fresh_last_persisted, ets_table: ets_ref) =
               fresh_meta

      assert fresh_last_persisted == LogOffset.new(12, 0)
      assert :ets.info(ets_ref, :size) == 0

      # Simulate partial ETS state: put only SOME entries back in ETS
      # This simulates what a reader would see if ETS was cleared mid-iteration
      :ets.insert(ets_ref, {LogOffset.to_tuple(LogOffset.new(10, 0)), ~S|{"test": 1}|})
      # Entry at offset 11 and 12 are "missing" - simulating they were cleared mid-read

      # Create stale metadata - reader thinks all data should be in ETS
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.last_before_real_offsets(),
          # Set last_seen so upper_read_bound covers all our data
          last_seen_txn_offset: LogOffset.new(12, 0)
        )

      # Insert stale metadata
      :ets.insert(stack_ets, stale_meta)

      # Reader sees stale metadata, tries ETS, gets only entry at offset 10.
      # Fix detects partial read (last_offset < upper_read_bound) and falls back to disk.
      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(0, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|, ~S|{"test": 2}|, ~S|{"test": 3}|],
             "Reader should fall back to disk when ETS returns partial data"

      PureFileStorage.terminate(writer)
    end

    test "reader falls back to disk when ETS table is deleted (pure ETS path)", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      _writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)
      assert storage_meta(ets_table: ets_ref) = fresh_meta

      # Stale last_persisted <= min_offset forces the pure ETS branch in stream_main_log
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.last_before_real_offsets(),
          last_seen_txn_offset: LogOffset.new(10, 0)
        )

      :ets.insert(stack_ets, stale_meta)
      :ets.delete(ets_ref)

      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(5, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|]

      PureFileStorage.cleanup!(opts)
    end

    test "reader falls back to disk when ETS table is deleted (mixed disk + ETS path)", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key2", :insert, ~S|{"test": 2}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [1]}}
      _writer = PureFileStorage.perform_scheduled_flush(writer, 1)

      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)
      assert storage_meta(ets_table: ets_ref) = fresh_meta

      # Stale last_persisted between the two offsets forces the mixed disk+ETS branch
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.new(10, 0),
          last_seen_txn_offset: LogOffset.new(11, 0)
        )

      :ets.insert(stack_ets, stale_meta)
      :ets.delete(ets_ref)

      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(0, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|, ~S|{"test": 2}|]

      PureFileStorage.cleanup!(opts)
    end
  end

  describe "remove_unnested_storage/1" do
    test "removes un-nested storage directories but leaves the nested ones", ctx do
      base_opts =
        PureFileStorage.shared_opts(
          stack_id: ctx.stack_id,
          storage_dir: ctx.tmp_dir,
          chunk_bytes_threshold: ctx[:chunk_size] || 10 * 1024 * 1024,
          flush_period: 1000
        )

      nested_dirs = [
        PureFileStorage.shape_data_dir(base_opts.base_path, "128584483-1770721672609826"),
        PureFileStorage.shape_data_dir(base_opts.base_path, "35237783-1770721660697706")
      ]

      unnested_dirs = [
        Path.join(base_opts.base_path, "128584483-1770721672609826"),
        Path.join(base_opts.base_path, "35237783-1770721660697706")
      ]

      for dir <- nested_dirs ++ unnested_dirs do
        File.mkdir_p!(dir)
      end

      storage_base = {PureFileStorage, base_opts}
      start_link_supervised!(Storage.stack_child_spec(storage_base))
      assert validate_dir_cleanup(nested_dirs, unnested_dirs)
    end

    defp validate_dir_cleanup(nested_dirs, unnested_dirs, n \\ 500)

    defp validate_dir_cleanup(nested_dirs, unnested_dirs, 0) do
      validate_dir_required_state?(nested_dirs, unnested_dirs)
    end

    defp validate_dir_cleanup(nested_dirs, unnested_dirs, n) do
      if validate_dir_required_state?(nested_dirs, unnested_dirs) do
        true
      else
        Process.sleep(10)
        validate_dir_cleanup(nested_dirs, unnested_dirs, n - 1)
      end
    end

    defp validate_dir_required_state?(nested_dirs, unnested_dirs) do
      Enum.concat(
        Enum.map(nested_dirs, fn path ->
          File.dir?(path)
        end),
        Enum.map(unnested_dirs, fn path ->
          not File.dir?(path)
        end)
      )
      |> Enum.all?()
    end
  end

  describe "append_fragment_to_log!()" do
    @describetag flush_period: 10

    setup [:start_storage, :with_started_writer]

    test "writes items to the log without assuming they add up to a complete transaction",
         %{opts: opts, writer: writer} do
      # Verify the initial state of storage
      assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

      last_before_real_offset = LogOffset.last_before_real_offsets()

      assert %{
               last_persisted_offset: ^last_before_real_offset,
               last_seen_txn_offset: ^last_before_real_offset,
               last_persisted_txn_offset: ^last_before_real_offset
             } = storage_internal_state(opts)

      # Write a couple of txn fragments to the shape log.
      # For every fragment we verify that storage doesn't consider to have stored a complete transaction
      Enum.each(@fragments, fn fragment ->
        log_items = changes_to_log_items(fragment.changes, xid: @xid)
        writer = PureFileStorage.append_fragment_to_log!(log_items, writer)
        assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}

        # Since storage code isn't executed inside a Consumer process here, we have to call the function ourselves
        # to update the internal state of storage.
        PureFileStorage.perform_scheduled_flush(writer, 0)

        # Last persisted offset advances as each new txn fragment gets written.
        # Transaction offsets remain virtual since we've only written a txn fragment and not a full txn.
        offset = fragment.last_log_offset

        assert %{
                 last_persisted_offset: ^offset,
                 last_seen_txn_offset: ^last_before_real_offset,
                 last_persisted_txn_offset: ^last_before_real_offset
               } = storage_internal_state(opts)

        assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

        assert [] == get_log_items_from_storage(LogOffset.first(), LogOffset.last(), opts)
      end)
    end
  end

  describe "signal_txn_commit!()" do
    @describetag flush_period: 10

    setup [:start_storage, :with_started_writer]

    test "signals the commit boundary to the storage allowing it to advance the txn offset",
         %{opts: opts, writer: writer} do
      # Verify the initial state of storage
      assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

      last_before_real_offset = LogOffset.last_before_real_offsets()

      assert %{
               last_persisted_offset: ^last_before_real_offset,
               last_seen_txn_offset: ^last_before_real_offset,
               last_persisted_txn_offset: ^last_before_real_offset
             } = storage_internal_state(opts)

      # Write a couple of txn fragments to the shape log.
      {writer, last_offset} =
        Enum.reduce(@fragments, {writer, nil}, fn fragment, {writer, _} ->
          log_items = changes_to_log_items(fragment.changes, xid: @xid)
          writer = PureFileStorage.append_fragment_to_log!(log_items, writer)
          {writer, fragment.last_log_offset}
        end)

      # Last persisted offset advances as each new txn fragment gets written.
      # Transaction offsets remain virtual since we've only written a txn fragment and not a full txn.
      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      assert %{
               last_persisted_offset: ^last_offset,
               last_seen_txn_offset: ^last_before_real_offset,
               last_persisted_txn_offset: ^last_before_real_offset
             } = storage_internal_state(opts)

      assert [] == get_log_items_from_storage(LogOffset.first(), LogOffset.last(), opts)

      # Signal the end of the transaction
      PureFileStorage.signal_txn_commit!(@xid, writer)

      assert %{
               last_persisted_offset: ^last_offset,
               last_seen_txn_offset: ^last_offset,
               last_persisted_txn_offset: ^last_offset
             } = storage_internal_state(opts)

      assert [i1, i2, i3, i4, i5] =
               get_log_items_from_storage(LogOffset.first(), LogOffset.last(), opts)

      lsn = to_string(@lsn)

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 0,
                 "operation" => "insert",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"5"',
               "value" => %{"id" => "5"}
             } == i1

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 2,
                 "operation" => "update",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"1"',
               "value" => %{"foo" => "bar", "id" => "1"}
             } == i2

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 4,
                 "operation" => "update",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"3"',
               "value" => %{"another" => "update", "id" => "3"}
             } == i3

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 6,
                 "operation" => "insert",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"6"',
               "value" => %{"id" => "6"}
             } == i4

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 8,
                 "operation" => "delete",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid],
                 "last" => true
               },
               "key" => ~s'"public"."test_table"/"2"',
               "value" => %{"id" => "2"}
             } == i5
    end
  end

  defp with_started_writer(%{opts: opts}) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    %{writer: writer}
  end

  # Closing the handle behind the writer's back is the cheapest way to make its next write or
  # datasync fail the way a full or failing disk would.
  defp close_log_file(writer) do
    files = WriteLoop.writer_acc(writer_state(writer, :writer_acc), :open_files)
    :ok = File.close(WriteLoop.open_files(files, :json_file))
  end

  defp get_log_items_from_storage(min_offset, max_offset, storage_impl) do
    PureFileStorage.get_log_stream(min_offset, max_offset, storage_impl)
    |> Enum.map(&Jason.decode!/1)
  end

  defp storage_internal_state(opts) do
    [metadata] = :ets.lookup(opts.stack_ets, @shape_handle)

    %{
      last_persisted_txn_offset: storage_meta(metadata, :last_persisted_txn_offset),
      last_persisted_offset: storage_meta(metadata, :last_persisted_offset),
      last_seen_txn_offset: storage_meta(metadata, :last_seen_txn_offset),
      last_snapshot_chunk: storage_meta(metadata, :last_snapshot_chunk),
      cached_chunk_boundaries: storage_meta(metadata, :cached_chunk_boundaries)
    }
  end
end
