defmodule Electric.ShapeCache.PureFileStorage.HardKillRecoveryTest do
  @moduledoc """
  Covers the gap the existing "crash recovery" tests leave open: every reopen in
  `pure_file_storage_test.exs` and `storage_implementations_test.exs` is preceded by a clean
  `Storage.terminate/1`. Nothing kills the writer process and reopens the storage, which is
  exactly where a wrongly-omitted durability write would show up.

  Here the writer runs in its own process, that process is killed with `Process.exit(pid, :kill)`
  so no terminate callback can run, the storage stack is torn down, and the storage is reopened
  from disk alone.

  The line these arms draw is between memory and the file, not between one restart and another.
  Bytes still in the writer's buffer are gone the moment the process is, and must not come back.
  Bytes it already handed the OS are in the file, and every failure short of the machine itself
  keeps them — so recovery has to keep them too, however little of that write was synced.

  The payload-arena arms cover the other half of that contract: a log line stores a ref, not
  the payload, so the two writes have their own durability ordering. The writer syncs the
  arena at the commit fragment, before the refs it backs reach a consumer.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup

  require Repatch

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.ShapeCache.PureFileStorage.Snapshot
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir
  # Strictly above `@write_timeout`, so a write that never lands fails as that rather than as an
  # opaque test timeout fired at the same moment.
  @moduletag timeout: 120_000
  setup [:with_stack_id_from_test, :with_async_deleter]

  # A snapshot chunk's descriptor that tells the test of each write, sync and close made on it, the
  # way `:file` calls the module a descriptor names, and fails its datasync or its close, once done,
  # when the test asks.
  defmodule ChunkDescriptor do
    def wrap(file, path, test, failing),
      do: {:file_descriptor, __MODULE__, %{file: file, path: path, test: test, failing: failing}}

    def write({:file_descriptor, __MODULE__, %{file: file} = data}, bytes) do
      result = :file.write(file, bytes)
      send(data.test, {:chunk_written, data.path})
      result
    end

    def datasync({:file_descriptor, __MODULE__, %{file: file} = data}) do
      result = :file.datasync(file)
      send(data.test, {:synced, {:chunk_descriptor, data.path}, nil})
      if data.failing == :datasync, do: {:error, :eio}, else: result
    end

    def close({:file_descriptor, __MODULE__, %{file: file} = data}) do
      result = :file.close(file)
      send(data.test, {:chunk_closed, data.path})
      if data.failing == :close, do: {:error, :eio}, else: result
    end
  end

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

  # 96 KiB clears the 64 KiB @delayed_write threshold in maybe_flush_buffer/2, so the commit is
  # really on disk before the kill. Anything smaller stays in the writer's buffer.
  @flushing_payload String.duplicate("x", 96 * 1024)

  # Small enough to stay in the arena writer's own 512 KiB buffer, so only sync! can put it
  # on disk before the kill.
  @arena_payload ~s|{"arena":1}|

  defp start_stack(ctx, chunk_bytes_threshold \\ 10 * 1024 * 1024) do
    base_opts =
      PureFileStorage.shared_opts(
        stack_id: ctx.stack_id,
        storage_dir: ctx.tmp_dir,
        chunk_bytes_threshold: chunk_bytes_threshold,
        flush_period: 600_000
      )

    start_supervised!(Storage.stack_child_spec({PureFileStorage, base_opts}))
    PureFileStorage.for_shape(@shape_handle, base_opts)
  end

  defp stack_child_id(ctx),
    do: Storage.stack_child_spec({PureFileStorage, %{stack_id: ctx.stack_id}}).id

  @write_timeout 60_000

  # Runs the writes in a throwaway process, then kills it without any chance to terminate.
  defp write_then_kill(opts, payloads) do
    test_pid = self()

    writer_pid =
      spawn(fn ->
        writer = PureFileStorage.init_writer!(opts, @shape)
        PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
        PureFileStorage.mark_snapshot_as_started(opts)
        PureFileStorage.make_new_snapshot!([], opts)

        payloads
        |> Enum.with_index(1)
        |> Enum.reduce(writer, fn {payload, n}, w ->
          w
          |> then(
            &PureFileStorage.append_fragment_to_log!(
              [{LogOffset.new(n, 0), "test_key", :insert, payload}],
              &1
            )
          )
          |> then(&PureFileStorage.signal_txn_commit!(n, &1))
        end)

        send(test_pid, :written)
        Process.sleep(:infinity)
      end)

    await_writer(writer_pid)

    kill_and_await(writer_pid)
  end

  # The writer is spawned unlinked so that killing it is the thing under test, which also means a
  # writer that raises has nothing to report on: waiting for its message alone turns any crash into
  # a mailbox that stays empty until the clock runs out. Monitoring makes the crash the answer.
  defp await_writer(pid) do
    ref = Process.monitor(pid)

    written =
      receive do
        {:written, payload_ref} ->
          payload_ref

        :written ->
          :ok

        {:DOWN, ^ref, :process, ^pid, reason} ->
          flunk("the writer died before reporting its write: #{inspect(reason)}")
      after
        @write_timeout ->
          flunk("the writer neither reported a write nor died within #{@write_timeout}ms")
      end

    Process.demonitor(ref, [:flush])
    written
  end

  # Writes one arena-backed log line and kills the process owning both writers. `sync_arena?`
  # selects the ordering; `padding` decides whether the log line clears the write loop's
  # buffer threshold and reaches the file, since a ref line is only 28 bytes on disk.
  defp arena_write_then_kill(ctx, opts, sync_arena?, padding) do
    test_pid = self()
    base_path = base_path(ctx)

    writer_pid =
      spawn(fn ->
        arena = PayloadArena.open_writer!(base_path)
        {arena, ref} = PayloadArena.append!(arena, "arena-key", @arena_payload)
        if sync_arena?, do: PayloadArena.sync!(arena)

        writer = PureFileStorage.init_writer!(opts, @shape)
        PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
        PureFileStorage.mark_snapshot_as_started(opts)
        PureFileStorage.make_new_snapshot!([], opts)

        writer
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(1, 0), "arena-key", :insert, @arena_payload, ref}] ++ padding,
            &1
          )
        )
        |> then(&PureFileStorage.signal_txn_commit!(1, &1))

        send(test_pid, {:written, ref})
        Process.sleep(:infinity)
      end)

    ref = await_writer(writer_pid)

    kill_and_await(writer_pid)
    ref
  end

  defp kill_and_await(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5000
  end

  defp base_path(ctx), do: Storage.opt_for_stack(ctx.stack_id, :base_path)

  defp json_file_path(ctx) do
    PureFileStorage.shape_data_dir(base_path(ctx), @shape_handle, [
      "log",
      "log.latest.0.jsonfile.bin"
    ])
  end

  defp chunk_file_path(ctx) do
    PureFileStorage.shape_data_dir(base_path(ctx), @shape_handle, [
      "log",
      "log.latest.0.chunk.bin"
    ])
  end

  defp log_stream(opts) do
    PureFileStorage.get_log_stream(
      LogOffset.last_before_real_offsets(),
      LogOffset.last(),
      opts
    )
    |> Enum.to_list()
  end

  defp corrupt_at(path, position, bytes) do
    {:ok, file} = :file.open(path, [:write, :read, :raw])
    {:ok, ^position} = :file.position(file, position)
    :ok = :file.write(file, bytes)
    :ok = :file.close(file)
  end

  defp truncate_at(path, position) do
    {:ok, file} = :file.open(path, [:write, :read, :raw])
    {:ok, ^position} = :file.position(file, position)
    :ok = :file.truncate(file)
    :ok = :file.close(file)
  end

  defp reopen(ctx, chunk_bytes_threshold \\ 10 * 1024 * 1024) do
    stop_supervised!(stack_child_id(ctx))
    opts = start_stack(ctx, chunk_bytes_threshold)
    _writer = PureFileStorage.init_writer!(opts, @shape)
    opts
  end

  defp reopen_writer(ctx) do
    stop_supervised!(stack_child_id(ctx))
    opts = start_stack(ctx)
    {opts, PureFileStorage.init_writer!(opts, @shape)}
  end

  # The payload clears the write buffer's threshold, so the commit reaches the file.
  defp append_flushed_txn(writer, n) do
    writer
    |> then(
      &PureFileStorage.append_fragment_to_log!(
        [{LogOffset.new(n, 0), "test_key", :insert, ~s|{"n":#{n},"b":"#{@flushing_payload}"}|}],
        &1
      )
    )
    |> then(&PureFileStorage.signal_txn_commit!(n, &1))
  end

  test "a committed but unflushed txn is not recovered after a hard kill", ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    write_then_kill(opts, [~s|{"n":1}|])
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

    assert [] ==
             PureFileStorage.get_log_stream(
               LogOffset.last_before_real_offsets(),
               LogOffset.last(),
               opts
             )
             |> Enum.to_list()
  end

  test "a committed and flushed txn is recovered after a hard kill", ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)

    assert 1 ==
             PureFileStorage.get_log_stream(
               LogOffset.last_before_real_offsets(),
               LogOffset.last(),
               opts
             )
             |> Enum.count()
  end

  test "flushed txns are recovered and trailing unflushed ones are discarded after a hard kill",
       ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|,
      ~s|{"n":3}|
    ])

    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(2, 0)} == PureFileStorage.fetch_latest_offset(opts)

    assert 2 ==
             PureFileStorage.get_log_stream(
               LogOffset.last_before_real_offsets(),
               LogOffset.last(),
               opts
             )
             |> Enum.count()
  end

  # A kill leaves whole entries behind, because the process dies between writes. A machine that
  # loses power dies during one, so the file can end mid-entry — the case none of the arms above
  # produce. The log carries no checksums, so a short file is indistinguishable from a complete
  # one except by what it fails to contain.
  #
  # Here the tear lands below the offset the metadata file already claims as durable. Nothing
  # can give those bytes back, and reads used to simply stop early while the shape went on
  # advertising the offset it could no longer serve. Recovery has to refuse instead.
  test "a log that lost bytes below its persisted offset refuses to reopen", ctx do
    # Arrange
    opts = start_stack(ctx)

    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|
    ])

    # Act: tear the last entry in half, as an interrupted write would leave it.
    path = json_file_path(ctx)
    %{size: size} = File.stat!(path)
    truncate_at(path, size - div(byte_size(@flushing_payload), 2))

    # Assert
    assert_raise Storage.Error, ~r/lost durable data/, fn -> reopen(ctx) end
  end

  # Damage to a chunk record is not caught by the realignment that reads it. A record whose offset
  # sits past the persisted one is exactly what a write after the last commit leaves behind, so
  # trimming it is right; a record corrupted into the same shape is indistinguishable, and trimming
  # is then wrong. Trim the last one and the index is empty, while the log it maps is untouched and
  # metadata still advertises the offset it ends at.
  #
  # That state cannot be reached by crashing: a chunk boundary is written before the log line it
  # opens and the persisted offset is written last. So an empty index under a real persisted offset
  # is loss, and resuming from it is worse than refusing — every read is driven by the index, so the
  # shape answers `up-to-date` at the offset it advertises with none of the rows behind it, forever,
  # with nothing to rebuild the index or invalidate the shape.
  test "a chunk index emptied below its persisted offset refuses to reopen", ctx do
    # Arrange
    opts = start_stack(ctx)

    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|
    ])

    # Act: corrupt the chunk record's offset upwards, which is what realignment trims away.
    path = chunk_file_path(ctx)
    {:ok, file} = :file.open(path, [:write, :read, :raw])
    :ok = :file.pwrite(file, 0, <<0xFFFFFFFFFFFFFFFF::64>>)
    :ok = :file.close(file)

    # Assert
    assert_raise Storage.Error, ~r/lost durable data/, fn -> reopen(ctx) end
  end

  # A store whose log is empty has no rows for a missing index to hide, so the guard above must not
  # fire on it — that is every shape between its snapshot and its first logged transaction.
  test "a shape that has logged nothing reopens with no chunk index", ctx do
    # Arrange
    opts = start_stack(ctx)
    _writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    # Act
    opts = reopen(ctx)

    # Assert
    assert [] == log_stream(opts)
  end

  # The boundary check above scans forward from the last chunk's start, not from byte zero, so
  # it can only see damage in the final chunk. Damage older than that is invisible at boot, which
  # is the half of the contract nothing pinned. The two halves used to recover differently: a torn
  # final chunk refuses to reopen and the shape is rebuilt, while this one reopened clean, kept
  # advertising an offset it could no longer serve, and turned every read into a permanent
  # failure — nothing catches a read-time raise, so there was nothing to converge on.
  #
  # Detecting it at boot still costs a whole-file scan, which is the wrong trade at a large shape
  # count, and an entry checksum is still the fix worth having. What changed is the outcome once a
  # read does hit it: the read reports loss rather than a bare RuntimeError, and discards the shape
  # so the next reader rebuilds it.
  test "damage below the last chunk boundary survives reopen and fails every read", ctx do
    # Arrange: a chunk threshold well under the payload size, so the entry corrupted below is
    # several chunks behind the boundary the reopen scan starts from.
    threshold = 100 * 1024
    opts = start_stack(ctx, threshold)

    write_then_kill(
      opts,
      for(n <- 1..5, do: ~s|{"n":#{n},"b":"#{@flushing_payload}"}|)
    )

    opts = reopen(ctx, threshold)
    assert {:ok, LogOffset.new(5, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 5 == opts |> log_stream() |> Enum.count()

    # Act: overwrite the first entry's json_size in place, as a partial page writeback that
    # landed out of order would. The file keeps its length, so this is corruption rather than
    # the truncation the arms above cover. The field sits at
    # tx_offset(8) + op_offset(8) + key_size(4) + "test_key"(8) + op_type(1) + flag(1).
    corrupt_at(json_file_path(ctx), 30, <<0xFFFFFFFFFFFFFFFF::64>>)

    opts = reopen(ctx, threshold)

    # Assert
    assert {:ok, LogOffset.new(5, 0)} == PureFileStorage.fetch_latest_offset(opts),
           "the shape still advertises the offset it can no longer serve"

    parent = self()

    Repatch.patch(ShapeCleaner, :remove_shape_async, fn _stack_id, handle ->
      send(parent, {:discarded, handle})
      :ok
    end)

    error = assert_raise Storage.Error, ~r/lost durable data/, fn -> log_stream(opts) end
    assert error.kind == :data_loss
    assert_received {:discarded, @shape_handle}
  end

  # The reader has two ways to discover the log is shorter than the chunk index claims: a partial
  # entry inside the range (above), and a read that runs off the end of the file entirely. The
  # second is reached by widening a chunk's end position rather than damaging the log, and it
  # survives reopen because the realignment only ever examines the last record's offsets.
  test "a chunk index describing more log than exists reports loss and discards the shape", ctx do
    # Arrange
    threshold = 100 * 1024
    opts = start_stack(ctx, threshold)

    write_then_kill(
      opts,
      for(n <- 1..5, do: ~s|{"n":#{n},"b":"#{@flushing_payload}"}|)
    )

    opts = reopen(ctx, threshold)
    assert 5 == opts |> log_stream() |> Enum.count()

    # Act: a full record is min(16) + log_start(8) + key_start(8) + max(16) + log_end(8) +
    # key_end(8), so the first chunk's end position sits at byte 48.
    corrupt_at(chunk_file_path(ctx), 48, <<0x7FFFFFFFFFFFFFFF::64>>)
    opts = reopen(ctx, threshold)

    parent = self()

    Repatch.patch(ShapeCleaner, :remove_shape_async, fn _stack_id, handle ->
      send(parent, {:discarded, handle})
      :ok
    end)

    # Assert
    error = assert_raise Storage.Error, ~r/lost durable data/, fn -> log_stream(opts) end
    assert error.kind == :data_loss
    assert_received {:discarded, @shape_handle}
  end

  test "a trailing entry written but never committed is trimmed, not mistaken for loss", ctx do
    # Arrange
    opts = start_stack(ctx)

    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|
    ])

    # Act: append a whole entry's worth of bytes past the persisted boundary, as a write that
    # reached the file but whose transaction never committed would leave behind.
    path = json_file_path(ctx)
    File.write!(path, :binary.copy(<<0>>, 4096), [:append])

    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(2, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 2 == length(log_stream(opts))
  end

  test "a synced arena survives the hard kill that loses the log line referencing it", ctx do
    # Arrange
    opts = start_stack(ctx)
    base_path = base_path(ctx)

    # Act
    ref = arena_write_then_kill(ctx, opts, true, [])
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert [] == log_stream(opts)

    assert [@arena_payload] ==
             [{:ref, ref}] |> PayloadArena.resolve_refs(base_path) |> Enum.to_list()
  end

  test "a recovered log line whose arena was never synced discards the shape", ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    arena_write_then_kill(ctx, opts, false, [
      {LogOffset.new(1, 2), "pad", :insert, ~s|{"b":"#{@flushing_payload}"}|}
    ])

    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 2)} == PureFileStorage.fetch_latest_offset(opts)

    error = assert_raise Storage.Error, fn -> log_stream(opts) end

    assert error.kind == :data_loss
    assert error.message =~ "dangling payload-arena ref"
  end

  test "a beam restart keeps a committed txn whose bytes reached the file", ctx do
    # Arrange
    opts = start_stack(ctx)
    test_pid = self()

    writer_pid =
      spawn(fn ->
        writer = PureFileStorage.init_writer!(opts, @shape)
        PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
        PureFileStorage.mark_snapshot_as_started(opts)
        PureFileStorage.make_new_snapshot!([], opts)

        writer
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1}|}],
            &1
          )
        )
        |> then(&PureFileStorage.signal_txn_commit!(1, &1))
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(2, 0), "test_key", :insert, ~s|{"n":2,"b":"#{@flushing_payload}"}|}],
            &1
          )
        )

        send(test_pid, :written)
        Process.sleep(:infinity)
      end)

    # The writer does real file I/O on payloads sized to clear the write loop's buffer, so this
    # waits on work whose duration tracks how loaded the machine is, not on a protocol round trip.
    # At 5s it timed out roughly one run in four here, failing whichever test happened to be
    # slowest and reading as a defect in that test rather than in this helper.
    assert_receive :written, @write_timeout

    # Readers were served txn 1 before anything restarted, so losing it later is not a lost
    # write, it is a retraction of something a client already has.
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    kill_and_await(writer_pid)

    # The bytes left the buffer, so they are in the file and only a machine crash can take
    # them. Everything short of that -- a process restart, an OOM kill, a supervisor giving up --
    # keeps the file and loses only memory.
    assert File.read!(json_file_path(ctx)) =~ ~s|{"n":1}|

    # Act
    opts = reopen(ctx)

    # Assert
    assert File.read!(json_file_path(ctx)) =~ ~s|{"n":1}|,
           "recovery deleted a committed transaction that was still in the file"

    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
  end

  # The written boundary is the further of the two on-disk claims once a transaction has been
  # synced before it. Here txn 1 is durable, so the metadata file names it; txn 2 then reaches the
  # file when the next append flushes the buffer, with no datasync. A kill and a restart of the
  # stack leave the file whole, so recovery has to keep txn 2 as well, not trim back to txn 1.
  test "a restart keeps a written txn that follows a synced one", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    assert File.read!(json_file_path(ctx)) =~ ~s|{"n":2}|

    # Act
    opts = reopen(ctx)

    # Assert
    assert File.read!(json_file_path(ctx)) =~ ~s|{"n":2}|,
           "recovery deleted a committed transaction that was still in the file"

    assert {:ok, LogOffset.new(2, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 2 == opts |> log_stream() |> Enum.count()
  end

  # The reopened writer takes the boundary it recovered to as synced, so a replayed txn 2 would be
  # reported flushed at once and the slot moved past it. That is only safe once txn 2 is on the
  # platter and the durable claim names it.
  test "a restart makes the written txn it keeps durable", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    assert LogOffset.new(1, 0) == durable_boundary(ctx)

    # Act
    reopen(ctx)

    # Assert
    assert LogOffset.new(2, 0) == durable_boundary(ctx)
  end

  # A machine that loses power can keep the written marker, which is never synced, and lose the
  # bytes it names. The log then ends partway through txn 2: what it lost was never claimed, and
  # the source still holds it, so recovery goes back to the durable txn 1 rather than refuse.
  test "a log torn under its written boundary reopens at its durable one", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    truncate_at(json_file_path(ctx), entries_end(json_file_path(ctx), 1) + 7)

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # The written marker names the last entry the writer handed the file. A log that holds an entry
  # past it and not the entry itself lost that write under a later one that landed, as a failed
  # write nothing checked leaves it. The marker is no hint then: recovery goes back to txn 1.
  test "a log missing the entry its written marker names reopens at its durable boundary", ctx do
    # Arrange: cut txn 2 out of the log and keep txn 3 past it.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    log = json_file_path(ctx)
    contents = File.read!(log)
    {txn_1_end, txn_2_end} = {entries_end(log, 1), entries_end(log, 2)}
    assert entries_end(log, 3) == byte_size(contents)

    File.write!(
      log,
      binary_part(contents, 0, txn_1_end) <>
        binary_part(contents, txn_2_end, byte_size(contents) - txn_2_end)
    )

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # Txn 1 closes a chunk and is durable. Txn 2 opens the next chunk, and its line and the written
  # marker reach the file while the chunk opening does not: a failed index write, or one a machine
  # crash took. The index covers the log up to txn 1 and the source still holds txn 2, so recovery
  # goes back to txn 1 rather than promote the marker and refuse an index short of the log.
  test "a reopen in the same boot whose index lost the chunk past its claim reopens at the claim",
       ctx do
    # Arrange: chunks close at 90 KiB, so txn 1 closes one, and txn 3 flushes txn 2 without
    # closing the next one, which would sync and move the claim.
    threshold = 90 * 1024
    opts = start_stack(ctx, threshold)
    write_synced_and_written_then_kill(opts, String.duplicate("x", 70 * 1024))
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
    assert File.read!(json_file_path(ctx)) =~ ~s|{"n":2}|

    assert [{{_, first_max}, {0, first_end}, _} | _] =
             PureFileStorage.ChunkIndex.read_chunk_file(chunk_file_path(ctx))

    assert first_max == LogOffset.new(1, 0)
    assert first_end == entries_end(json_file_path(ctx), 1)
    truncate_at(chunk_file_path(ctx), 64)

    # Act
    opts = reopen(ctx, threshold)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # A truncate that fails leaves the bytes past the claim, and the chunk index, which ends at the
  # claim, stops short of the log. That is an I/O failure for the reopen to report, not lost data.
  test "a reopen whose log truncate fails reports it rather than lost data", ctx do
    # Arrange: chunks close at 64 KiB, so each commit closes one and the index ends at the claim.
    threshold = 64 * 1024
    opts = start_stack(ctx, threshold)

    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|
    ])

    assert LogOffset.new(2, 0) == durable_boundary(ctx)
    log = json_file_path(ctx)

    assert {:complete, last, end_pos, _} =
             PureFileStorage.ChunkIndex.get_last_boundary(chunk_file_path(ctx))

    assert last == LogOffset.new(2, 0)
    assert end_pos == File.stat!(log).size
    File.write!(log, :binary.copy(<<0>>, 4096), [:append])
    fail_writes_of(log)

    # Act & Assert
    assert_raise File.Error, ~r/could not truncate/, fn -> reopen(ctx, threshold) end
  end

  # A log that already ends at its claim has nothing past it to cut. The reopen leaves it as it is,
  # so a truncate that would have failed does not fail the reopen of an intact log.
  test "a reopen of a log that ends at its claim does not truncate it", ctx do
    # Arrange: chunks close at 64 KiB, so each commit closes one and syncs the claim.
    threshold = 64 * 1024
    opts = start_stack(ctx, threshold)

    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|
    ])

    assert LogOffset.new(2, 0) == durable_boundary(ctx)
    log = json_file_path(ctx)
    assert entries_end(log, 2) == File.stat!(log).size
    fail_writes_of(log)

    # Act
    opts = reopen(ctx, threshold)

    # Assert
    assert {:ok, LogOffset.new(2, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 2 == opts |> log_stream() |> Enum.count()
  end

  # A read that fails says nothing about the bytes it did not return, unlike an entry the log cuts
  # short, which is a tail a crash left. The reopen reports the failure and keeps every byte.
  test "a reopen whose log read fails reports it and keeps the log", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    log = json_file_path(ctx)
    size = File.stat!(log).size
    fail_reads_of(log)

    # Act & Assert
    assert_raise File.Error, ~r/could not read/, fn -> reopen(ctx) end
    assert size == File.stat!(log).size
  end

  # The written marker is a hint past the durable claim, and so are the bytes it names. A read that
  # fails there leaves the hint unproven: the reopen goes back to the claim, whose trim reads only
  # up to it, rather than fail over bytes it never needed.
  test "a reopen whose log read fails past its claim reopens at the claim", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    log = json_file_path(ctx)
    claim_end = entries_end(log, 1)
    fail_reads_from(claim_end)

    # Act
    opts = reopen(ctx)

    # Assert
    assert Process.get(:failed_log_reads, 0) > 0, "no read past the claim failed"
    assert claim_end == File.stat!(log).size
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # With no transaction claimed, nothing in the log is durable. A read that fails there, from its
  # first byte, failed the reopen once the written-marker probe had fallen back from the same read.
  test "a reopen whose log read fails with no claim empties the log", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_metadata_file(ctx, "last_persisted_txn_offset", LogOffset.last_before_real_offsets())
    fail_reads_from(0)

    # Act
    opts = reopen(ctx)

    # Assert
    assert Process.get(:failed_log_reads, 0) > 0, "no log read failed"
    assert [0, 0] == Enum.map([json_file_path(ctx), chunk_file_path(ctx)], &File.stat!(&1).size)
    assert [] == log_stream(opts)
    assert LogOffset.last_before_real_offsets() == durable_boundary(ctx)
  end

  # A compacted generation that covers the claim holds every durable entry, so the latest one holds
  # none. A read of it that fails gives the shape an empty latest generation rather than fail the
  # reopen, and leaves the old one's files to a read already under way in them: emptied in place,
  # they ended that read short.
  test "a reopen whose latest log read fails under a covering compaction starts an empty one",
       ctx do
    # Arrange: txn 1 compacted, txns 1 to 3 in the latest generation a switch left named.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    old_files = for {:file, path} <- generation_files(ctx, "latest.1"), do: path
    old_contents = Enum.map(old_files, &File.read!/1)
    fail_reads_from(0)

    # Act
    {opts, writer} = reopen_writer(ctx)

    # Assert
    assert Process.get(:failed_log_reads, 0) > 0, "no log read failed"
    published = read_term(metadata_file(ctx, "latest_name"))
    assert published not in ["latest.0", "latest.1"]
    files = for {:file, path} <- generation_files(ctx, published), do: path
    assert [0, 0] == Enum.map(files, &File.stat!(&1).size)
    assert old_contents == Enum.map(old_files, &File.read!/1)
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
    assert LogOffset.new(1, 0) == durable_boundary(ctx)

    # The writer and the readers go to the new generation, and the old files keep their bytes.
    writer = append_flushed_txn(writer, 4)
    assert published == PureFileStorage.latest_name(opts)
    assert [1, 4] == opts |> log_stream() |> Enum.map(&entry_number/1)
    assert File.stat!(hd(files)).size > 0
    assert old_contents == Enum.map(old_files, &File.read!/1)
    PureFileStorage.terminate(writer)
  end

  # With a claim the compacted generation covers, the latest generation holds nothing the shape
  # needs. A read already under way in it found entries past the claim, which a trim in place cut.
  test "a reopen under a covering compaction leaves the latest generation's files as they are",
       ctx do
    # Arrange: txn 1 compacted, txns 1 to 3 readable in the latest generation a switch left named,
    # under a record of an earlier boot so txn 1 is the claim the reopen trusts. Chunks close at
    # 90 KiB, so txn 1 closes one and txn 2 opens the next: the index holds an entry past the
    # claim, which a realignment to the claim cuts.
    threshold = 90 * 1024
    opts = start_stack(ctx, threshold)
    write_synced_and_written_then_kill(opts, String.duplicate("x", 70 * 1024))
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    write_boot_record(ctx, "an earlier boot")
    old_files = [_log, chunks] = for {:file, path} <- generation_files(ctx, "latest.1"), do: path

    assert [{{_, first_max}, _, _}, {{second_min, nil}, _, _}] =
             PureFileStorage.ChunkIndex.read_chunk_file(chunks)

    assert {first_max, second_min} == {LogOffset.new(1, 0), LogOffset.new(2, 0)}
    old_contents = Enum.map(old_files, &File.read!/1)

    # Act
    opts = reopen(ctx, threshold)

    # Assert
    assert old_contents == Enum.map(old_files, &File.read!/1)
    assert read_term(metadata_file(ctx, "latest_name")) not in ["latest.0", "latest.1"]
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # Nothing of the latest generation a covering compaction replaces is handed on, so a sync that
  # fails on its files fails nothing.
  test "a reopen under a covering compaction syncs nothing of the latest generation it replaces",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    write_boot_record(ctx, "an earlier boot")
    old_files = for {:file, path} <- generation_files(ctx, "latest.1"), do: path

    Repatch.patch(Electric.Utils, :datasync_file!, [mode: :local], fn path ->
      if path in old_files,
        do: raise(File.Error, reason: :eio, action: "datasync", path: path),
        else: Repatch.real(Electric.Utils.datasync_file!(path))
    end)

    # Act
    opts = reopen(ctx)

    # Assert
    published = read_term(metadata_file(ctx, "latest_name"))
    assert published not in ["latest.0", "latest.1"]
    assert published == PureFileStorage.latest_name(opts)
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
  end

  # The written marker is a hint past the durable claim. An open of the log that fails is no proof
  # of it, and with a compacted generation covering the claim, the reopen needs no read of the
  # latest generation: it raised there and failed the reopen of a shape it could open at txn 1.
  test "a reopen in the same boot whose latest log cannot be opened under a covering compaction starts an empty one",
       ctx do
    # Arrange: txn 1 compacted, txns 1 to 3 in the latest generation a switch left named, and the
    # written marker at txn 2 under a record of this boot.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    [{:file, log}, _] = generation_files(ctx, "latest.1")
    fail_opens_of(log)

    # Act
    opts = reopen(ctx)

    # Assert
    assert Process.get(:failed_opens, 0) > 0, "no open of the log failed"
    assert read_term(metadata_file(ctx, "latest_name")) not in ["latest.0", "latest.1"]
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # So is a read of the chunk index that fails.
  test "a reopen in the same boot whose latest chunk index cannot be read under a covering compaction starts an empty one",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    [_, {:file, chunks}] = generation_files(ctx, "latest.1")
    fail_size_reads_of(chunks)

    # Act
    opts = reopen(ctx)

    # Assert
    assert Process.get(:failed_size_reads, 0) > 0, "no read of the chunk index failed"
    assert read_term(metadata_file(ctx, "latest_name")) not in ["latest.0", "latest.1"]
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # So is a size read of the log that fails once the check has trimmed it to the marker.
  test "a reopen in the same boot whose latest log size cannot be read under a covering compaction starts an empty one",
       ctx do
    # Arrange: the latest chunk index ends a complete chunk at txn 2, where the written marker is,
    # so the check trims txn 3 off the log, then compares the index's end with the log's size.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    [{:file, log}, {:file, chunks}] = generation_files(ctx, "latest.1")
    txn_2_end = entries_end(log, 2)

    File.write!(
      chunks,
      PureFileStorage.ChunkIndex.make_full_entry(
        {LogOffset.new(1, 0), LogOffset.new(2, 0)},
        {0, txn_2_end},
        {0, 0}
      )
    )

    fail_size_reads_of(log)

    # Act
    opts = reopen(ctx)

    # Assert
    assert File.stat!(log).size == txn_2_end, "the check did not trim the log to the marker"
    assert Process.get(:failed_size_reads, 0) > 0, "no size read of the log failed"
    assert read_term(metadata_file(ctx, "latest_name")) not in ["latest.0", "latest.1"]
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # Each reopen under a covering compaction publishes a new latest generation. A replay that a kill
  # ends before its first commit leaves its tail in the one it wrote, and no compaction runs before
  # a commit moves the claim: until then, each reopen kept another copy of that tail.
  test "a reopen under a covering compaction removes the generations before the one it replaces",
       ctx do
    # Arrange: txn 1 compacted, under a record of an earlier boot so txn 1 is the claim the reopen
    # trusts. A first reopen replaces the latest generation a switch left named, which it keeps,
    # and removes the one before. A replay then writes a tail to the generation it published and
    # is killed before its commit.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    write_boot_record(ctx, "an earlier boot")
    before = for {:file, path} <- generation_files(ctx, "latest.0"), do: path
    reopen(ctx)
    first = read_term(metadata_file(ctx, "latest_name"))
    assert first not in ["latest.0", "latest.1"]
    assert [] == Enum.filter(before, &File.exists?/1)
    replaced = for {:file, path} <- generation_files(ctx, "latest.1"), do: path
    [first_log, _] = kept = for {:file, path} <- generation_files(ctx, first), do: path
    File.write!(first_log, :binary.copy(<<1>>, 4096))
    write_boot_record(ctx, "an earlier boot")

    # Act
    opts = reopen(ctx)

    # Assert
    published = read_term(metadata_file(ctx, "latest_name"))
    assert published not in ["latest.0", "latest.1", first]
    assert [] == Enum.filter(replaced, &File.exists?/1)
    assert Enum.all?(kept, &File.exists?/1)
    assert [1] == opts |> log_stream() |> Enum.map(&entry_number/1)
  end

  # Removed after the new name, the generations before the replaced one stayed when a kill came
  # between the two, and the next reopen found the new generation empty, published nothing and
  # removed nothing: a replay killed before its commit then kept its tail again at each round.
  test "a reopen under a covering compaction removes the generations before the one it replaces before the new name",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    write_boot_record(ctx, "an earlier boot")
    before = for {:file, path} <- generation_files(ctx, "latest.0"), do: path
    parent = self()

    Repatch.patch(File, :rm_rf!, [mode: :local], fn path ->
      send(parent, {:removed, path, read_term(metadata_file(ctx, "latest_name"))})
      Repatch.real(File.rm_rf!(path))
    end)

    # Act
    reopen(ctx)

    # Assert
    assert read_term(metadata_file(ctx, "latest_name")) not in ["latest.0", "latest.1"]

    for path <- before do
      assert_received {:removed, ^path, named}
      assert named == "latest.1", "#{path} was removed after the new name"
    end
  end

  # A writer killed between a switch's name on disk and its name in the cache leaves the readers of
  # its stack on the generation before the one the disk names. A reopen in the same stack removed
  # it under them, and a read that already held its name failed on a missing file.
  test "a reopen in the same stack keeps the latest generation its readers hold", ctx do
    # Arrange: the stack's cache names latest.0, as the killed writer left it, and the disk names
    # latest.1, which a covering compaction makes the reopen replace.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    keep_compacted_through_first_txn(ctx, "compacted.0")
    write_boot_record(ctx, "an earlier boot")
    assert "latest.0" == PureFileStorage.latest_name(opts)
    held = for {:file, path} <- generation_files(ctx, "latest.0"), do: path

    # Act
    writer = PureFileStorage.init_writer!(opts, @shape)

    # Assert
    assert read_term(metadata_file(ctx, "latest_name")) not in ["latest.0", "latest.1"]
    assert Enum.all?(held, &File.exists?/1)
    PureFileStorage.terminate(writer)
  end

  # The writer claims the last entry it synced. A log holding an entry past the claim and none at
  # it lost the claimed one under a later write that landed, and replay skips that transaction as
  # already stored. With no compaction covering the claim, that is lost data.
  test "a log missing the entry its claim names refuses to reopen", ctx do
    # Arrange: claim txn 2, cut it out of the log and keep txn 3 past it, under a record of an
    # earlier boot so the claim is the only boundary the reopen trusts.
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    log = json_file_path(ctx)
    contents = File.read!(log)
    {txn_1_end, txn_2_end} = {entries_end(log, 1), entries_end(log, 2)}

    File.write!(
      log,
      binary_part(contents, 0, txn_1_end) <>
        binary_part(contents, txn_2_end, byte_size(contents) - txn_2_end)
    )

    write_metadata_file(ctx, "last_persisted_txn_offset", LogOffset.new(2, 0))
    write_boot_record(ctx, "an earlier boot")

    # Act & Assert
    assert_raise Storage.Error, ~r/holds no entry at its persisted offset/, fn -> reopen(ctx) end
  end

  # The written marker is renamed into place with no sync, so a machine crash can keep its name and
  # lose its content. An unreadable hint is no hint: recovery goes back to the durable txn 1 rather
  # than refuse to reopen.
  test "an empty written marker reopens at the durable boundary", ctx do
    assert_torn_written_marker_reopens_at_durable_boundary(ctx, fn _marker -> "" end)
  end

  test "a partly written marker reopens at the durable boundary", ctx do
    assert_torn_written_marker_reopens_at_durable_boundary(ctx, fn marker ->
      binary_part(marker, 0, div(byte_size(marker), 2))
    end)
  end

  # A machine crash can also keep the marker and the log's length and lose a block of txn 2 below
  # them, which reads back as zeros with the framing around it intact. The log cannot tell, so the
  # marker is no hint once the boot the writer recorded has ended, and recovery goes back to the
  # durable txn 1.
  test "a log whose writer recorded an earlier boot reopens at the durable boundary", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    payload = ~s|{"n":2}|
    log = json_file_path(ctx)

    corrupt_at(
      log,
      entries_end(log, 2) - byte_size(payload),
      :binary.copy(<<0>>, byte_size(payload))
    )

    write_boot_record(ctx, "an earlier boot")

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # A build that recorded no boot vouches for no unsynced byte.
  test "a log with no boot recorded reopens at the durable boundary", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(boot_record_file(ctx))

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # The boot sits in a record of its own, so a rollback to a build that reads the marker as an
  # offset still reads it.
  test "the written marker stays an offset", ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    write_synced_and_written_then_kill(opts)

    # Assert
    assert LogOffset.new(2, 0) ==
             ctx
             |> metadata_file("last_written_txn_offset")
             |> File.read!()
             |> :erlang.binary_to_term()
  end

  # A reopen in a later boot trims the log back to its durable claim, and the trim can leave the
  # earlier boot's tail in place, a truncate a machine crash took back say. The record that reopen writes names
  # the current boot, so it has to tell the marker it found from the ones this boot writes, or the
  # next reopen in the same boot would take the zeroed txn 2 as written.
  test "a reopen in the same boot after a trim that kept an earlier boot's tail keeps the claim",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    payload = ~s|{"n":2}|
    log = json_file_path(ctx)
    tail_end = entries_end(log, 2)
    corrupt_at(log, tail_end - byte_size(payload), :binary.copy(<<0>>, byte_size(payload)))
    write_boot_record(ctx, "an earlier boot")
    keep_tail_on_first_trim()
    reopen(ctx)
    assert File.stat!(log).size >= tail_end, "the first reopen trimmed the tail"

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # A marker this boot writes after a boot change holds bytes of its own, which the record vouches
  # for, so a reopen in the same boot keeps the txn it names.
  test "a reopen in the same boot keeps a txn a marker written since the boot change names",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot")
    opts = reopen(ctx)
    write_committed_then_kill(opts, [2, 3])

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(3, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert LogOffset.new(3, 0) == durable_boundary(ctx)
  end

  # A file system can number a file anew across a remount, a network or user-space one say, with
  # its bytes and its mtime as they were. The marker an earlier boot left is then a file of another
  # number, so a record that named the marker by its inode vouched for it in the next reopen of the
  # same boot, which took the zeroed txn 2 as written.
  test "a reopen in the same boot after the earlier boot's marker is renumbered keeps the claim",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    payload = ~s|{"n":2}|
    log = json_file_path(ctx)

    corrupt_at(
      log,
      entries_end(log, 2) - byte_size(payload),
      :binary.copy(<<0>>, byte_size(payload))
    )

    write_boot_record(ctx, "an earlier boot")
    keep_tail_on_first_trim()
    reopen(ctx)
    renumber(metadata_file(ctx, "last_written_txn_offset"))

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # A log can stop short of a marker the record vouches for, and the reopen then replays from the
  # claim, whose markers name commits below the one the marker it found named. So the record
  # carries that commit for a promotion's scan, durably before the writer appends.
  test "a reopen in the same boot that falls back below its marker keeps the commit the marker named",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    truncate_at(json_file_path(ctx), entries_end(json_file_path(ctx), 1) + 7)
    record_metadata_writes()
    record_syncs(ctx)

    # Act
    opts = reopen(ctx)
    write_metadata_file(ctx, "last_written_txn_offset", LogOffset.new(1, 0))

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert_durable_write(recorded_writes_and_syncs(), "written_boot", boot_dir(ctx))
    assert 2 == PureFileStorage.stored_high_water(base_path(ctx))
  end

  # A volume can move to another machine and back to one that has not restarted. A record that
  # replaces another boot's is durable before the writer appends, or a crash here could leave that
  # boot's record beside a marker past the claim, for its machine to trust. The marker the reopen
  # finds stays as it is, and nothing in its directory is synced on its account: the record names
  # its file instead.
  test "a writer that replaces an earlier boot's record makes it durable and leaves the marker",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot")
    record_metadata_writes()
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    writes_and_syncs = recorded_writes_and_syncs()
    assert_durable_write(writes_and_syncs, "written_boot", boot_dir(ctx))

    refute Enum.any?(
             writes_and_syncs,
             &match?({:written, "last_written_txn_offset.bin.tmp", _}, &1)
           ),
           "the reopen rewrote the marker: #{inspect(writes_and_syncs)}"

    assert LogOffset.new(2, 0) == read_term(metadata_file(ctx, "last_written_txn_offset"))
  end

  # A first record that carries a commit past the claim is durable before the writer appends too,
  # since the next marker this boot writes replaces the one that named the commit.
  test "a first boot record that carries a commit past the claim is durable", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(boot_record_file(ctx))
    record_metadata_writes()
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    assert_durable_write(recorded_writes_and_syncs(), "written_boot", boot_dir(ctx))
  end

  # A run with the journal off serves clients the commits its marker names, and a promotion of the
  # source keeps the shapes only if their scan shows none past the switch point. A reopen in a
  # later boot rewrites the log from the durable claim, and a marker this boot writes before the
  # replay reaches txn 2 names an earlier commit, so the record carries the one the marker it found
  # named.
  test "a reopen in a later boot keeps the commit its written marker named for a promotion's scan",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot")

    # Act
    reopen(ctx)
    write_metadata_file(ctx, "last_written_txn_offset", LogOffset.new(1, 0))

    # Assert
    assert 2 == PureFileStorage.stored_high_water(base_path(ctx))
  end

  # A boot that reopens the shape after another rewrote its log carries on what that boot's record
  # carried, which no marker on disk names any more.
  test "a reopen in a later boot keeps the commit an earlier boot's record carried", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot", 7)

    # Act
    reopen(ctx)
    write_metadata_file(ctx, "last_written_txn_offset", LogOffset.new(1, 0))

    # Assert
    assert 7 == PureFileStorage.stored_high_water(base_path(ctx))
  end

  # A marker a crash tore, or one the reopen cannot read, named commits no one can know any more,
  # so the record carries that, and a promotion's scan fails on it as on the marker.
  test "a reopen in a later boot that finds a torn written marker fails a promotion's scan",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.write!(metadata_file(ctx, "last_written_txn_offset"), "")
    write_boot_record(ctx, "an earlier boot")

    # Act
    reopen(ctx)
    write_metadata_file(ctx, "last_written_txn_offset", LogOffset.new(1, 0))

    # Assert
    assert_raise Storage.Error, fn -> PureFileStorage.stored_high_water(base_path(ctx)) end
  end

  test "a reopen in a later boot that cannot read the written marker opens and fails a promotion's scan",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot")
    marker = metadata_file(ctx, "last_written_txn_offset")

    Repatch.patch(File, :read, [mode: :local], fn
      ^marker -> {:error, :eacces}
      path -> Repatch.real(File.read(path))
    end)

    Repatch.patch(File, :open, [mode: :local], fn
      ^marker, _modes, _fun -> {:error, :eacces}
      path, modes, fun -> Repatch.real(File.open(path, modes, fun))
    end)

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    Repatch.restore(File, :read, 1, mode: :local)
    assert_raise Storage.Error, fn -> PureFileStorage.stored_high_water(base_path(ctx)) end
  end

  # A marker whose name a crash keeps and whose content it takes fails the scan of what the shapes
  # hold that a promotion of the source goes by, so a shape that has logged nothing has none.
  test "a shape that has logged no transaction has no written marker", ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    write_then_kill(opts, [])

    # Assert
    refute File.exists?(metadata_file(ctx, "last_written_txn_offset"))
  end

  # A writer killed between renaming a record into place and syncing its directory leaves one the
  # next reopen in the same boot finds in place, so the reopen makes the record's name durable
  # before the writer appends. With no durable claim it syncs the record's directory alone: a sync
  # of the metadata directory would make the names there durable ahead of their content, an
  # unsynced written marker's among them.
  test "a reopen in the same boot with no durable claim makes the boot record durable", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [])

    # Act
    {record, syncs} = reopen_after_an_unsynced_boot_record(ctx)

    # Assert
    metadata_dir = metadata_dir(ctx)
    assert {{:dir, boot_dir(ctx)}, record} in syncs
    refute Enum.any?(syncs, &match?({{:dir, ^metadata_dir}, _}, &1))
  end

  # A recovery to a durable claim syncs the metadata directory, which the record has left.
  test "a reopen in the same boot with a durable claim makes the boot record durable", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)

    # Act
    {record, syncs} = reopen_after_an_unsynced_boot_record(ctx)

    # Assert
    assert {{:dir, boot_dir(ctx)}, record} in syncs
  end

  # For the same reason, a writer that cannot replace the record does not append under it.
  test "a reopen that cannot replace an earlier boot's record fails", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot")
    File.mkdir_p!(boot_record_file(ctx) <> ".tmp")

    # Act and assert
    assert_raise File.Error, fn -> reopen(ctx) end
  end

  # A crash takes a record with its directory's entry when only the record's own directory was
  # synced, so a record that carries a commit past the claim makes that entry durable before it is
  # renamed into place.
  test "a boot record that carries a commit past the claim makes its directory's entry durable first",
       ctx do
    # Arrange
    write_past_the_claim_of_a_durable_generation(ctx)
    File.rm_rf!(boot_dir(ctx))
    record_metadata_writes()
    record_mkdirs()
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    writes_and_syncs = recorded_writes_and_syncs()
    assert_durable_write(writes_and_syncs, "written_boot", boot_dir(ctx))
    assert_synced_before_the_record(writes_and_syncs, ctx, [shape_dir(ctx)])
  end

  # Until a shape's first durable claim, nothing has synced the entries above its directory, and a
  # crash can take the directory with the record in it while the catalog keeps the shape. So a
  # record that carries a commit past no claim makes every one of them durable first.
  test "a boot record that carries a commit past no durable claim makes every directory above it durable first",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_unsynced_then_kill(opts)
    File.rm_rf!(boot_dir(ctx))
    record_metadata_writes()
    record_mkdirs()
    record_syncs(ctx)

    # Act
    opts = reopen(ctx)

    # Assert
    assert 0 == opts |> log_stream() |> Enum.count()
    writes_and_syncs = recorded_writes_and_syncs()
    assert_durable_write(writes_and_syncs, "written_boot", boot_dir(ctx))
    assert_synced_before_the_record(writes_and_syncs, ctx, tl(log_directories(ctx)))
  end

  # So a reopen in the same boot that finds such a record finds its entry durable.
  test "a reopen that cannot sync the shape directory leaves the earlier boot's record", ctx do
    # Arrange
    write_past_the_claim_of_a_durable_generation(ctx)
    write_boot_record(ctx, "an earlier boot")
    refuse_to_open(shape_dir(ctx))

    # Act and assert
    assert_raise File.Error, fn -> reopen(ctx) end
    assert {"an earlier boot", nil, nil} == read_term(boot_record_file(ctx))
  end

  # A record that cannot be read may carry a commit past the claim that no marker names any more,
  # so a promotion's scan does not take it for none. Nor does the reopen take it for this boot's,
  # which would vouch for txn 2: it opens at the durable claim and carries a commit no one knows,
  # durably, rather than fail and have its shape removed.
  test "a boot record that cannot be read opens at the durable claim and fails a promotion's scan",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    fail_reads_of(boot_record_file(ctx))
    record_metadata_writes()
    record_syncs(ctx)

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert_durable_write(recorded_writes_and_syncs(), "written_boot", boot_dir(ctx))
    assert_raise File.Error, fn -> PureFileStorage.stored_high_water(base_path(ctx)) end
    Repatch.restore(File, :open, 3, mode: :local)
    assert_raise Storage.Error, fn -> PureFileStorage.stored_high_water(base_path(ctx)) end
  end

  # A shape clients may have read is removed, never rebuilt under its handle: there a client
  # resuming from an offset of the old log was answered from a later snapshot. The reopen raises
  # and touches nothing, the boot record included; the removal its consumer's exit starts deletes
  # the files. A later build's files, say, are ones this build cannot read.
  test "a reopen of a shape this build cannot read raises and leaves its files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    write_boot_record(ctx, "an earlier boot", 7)
    write_metadata_file(ctx, "version", -1)
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/version -1/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
    assert {"an earlier boot", nil, 7} == read_term(boot_record_file(ctx))
  end

  # The record of a snapshot's last chunk is written unsynced, and the catalog can keep the
  # snapshot complete through a crash that takes it.
  test "a reopen of a shape whose snapshot record is gone raises and leaves its files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(metadata_file(ctx, "last_snapshot_chunk"))
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/no complete snapshot/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # The mark of a snapshot's start is written unsynced too, and a crash can take it while the
  # record of the last chunk survives. A writer opened without it would leave the snapshotter to
  # take the snapshot again under the handle, whatever the catalog says.
  test "a reopen of a shape whose snapshot start mark is gone raises and leaves its files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(metadata_file(ctx, "snapshot_started?"))
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/no mark that it started/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # Damaged bytes do not decode to a version, and are not a reason to rebuild the shape either.
  test "a reopen of a shape whose version does not decode raises and leaves its files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.write!(metadata_file(ctx, "version"), <<131, 97>>)
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/holds no version/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # Bytes that decode to a term other than a version name none either, whatever the term, the
  # one the reopen gives a missing file included, and on a shape that holds nothing else.
  test "a reopen of a shape whose version file holds a term that is no version raises and leaves its files",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    opts |> PureFileStorage.init_writer!(@shape) |> PureFileStorage.terminate()
    File.write!(metadata_file(ctx, "version"), :erlang.term_to_binary(:missing))
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/holds no version/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  test "a reopen of a shape whose snapshot position is gone raises and leaves its files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(metadata_file(ctx, "pg_snapshot"))
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/no snapshot position/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # The version file is written durably before the snapshot starts or the log takes a
  # transaction. A writer opened on storage without it as on a new shape would write the log
  # from its first byte again, under the same handle.
  test "a reopen of a shape whose version file is gone raises and leaves its files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(metadata_file(ctx, "version"))
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/lost its version file/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # The log's bytes alone, with no durable claim, say the storage lost its version file.
  test "a reopen of a log whose version file, snapshot marker and durable claim are gone raises",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(metadata_file(ctx, "version"))
    File.rm!(metadata_file(ctx, "snapshot_started?"))
    File.rm!(metadata_file(ctx, "last_persisted_txn_offset"))
    assert File.stat!(Path.join([shape_dir(ctx), "log", "log.latest.0.jsonfile.bin"])).size > 0
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/lost its version file/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # A compaction that covers the durable claim leaves the latest generation empty, so the claim
  # can be all that is left of the log there.
  test "a reopen of an empty latest generation under a durable claim, its version file and snapshot marker gone, raises",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    File.rm!(metadata_file(ctx, "version"))
    File.rm!(metadata_file(ctx, "snapshot_started?"))
    File.write!(Path.join([shape_dir(ctx), "log", "log.latest.0.jsonfile.bin"]), "")
    files = shape_files(ctx)

    # Act and assert
    error = assert_raise Storage.Error, ~r/lost its version file/, fn -> reopen(ctx) end
    assert :data_loss == error.kind
    assert files == shape_files(ctx)
  end

  # The read path keeps a shape's snapshot cached while it has no writer. A directory removed
  # under the running stack leaves it there, over a disk that reads as a new shape's.
  test "a reopen in the same stack of a shape whose files are gone and whose snapshot is cached raises",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    opts |> open_writer_with_synced_txns(1..1) |> PureFileStorage.terminate()
    assert PureFileStorage.snapshot_started?(opts)
    File.rm_rf!(shape_dir(ctx))

    # Act and assert
    error =
      assert_raise Storage.Error, ~r/lost its files while its snapshot is still cached/, fn ->
        PureFileStorage.init_writer!(opts, @shape)
      end

    assert :data_loss == error.kind
  end

  # A restored shape had its storage created with it. A removal that took the shape while its
  # consumer was starting moved its files away and dropped its cached snapshot, so the disk reads
  # as a new shape's, and the restore creates nothing there.
  test "a restored writer raises on a shape with no storage and creates none", ctx do
    # Arrange
    opts = start_stack(ctx)
    refute File.exists?(shape_dir(ctx))

    # Act and assert
    error =
      assert_raise Storage.Error, ~r/has no storage to restore/, fn ->
        PureFileStorage.init_restored_writer!(opts, @shape)
      end

    assert :data_loss == error.kind
    refute File.exists?(shape_dir(ctx))

    # The storage that is there, it opens.
    opts |> open_writer_with_synced_txns(1..1) |> PureFileStorage.terminate()
    opts |> PureFileStorage.init_restored_writer!(@shape) |> PureFileStorage.terminate()
  end

  # The version file comes last when a new shape's writer opens, so a crash there leaves the
  # directories and the generation name without it. Such a shape holds nothing, and opens as new.
  test "a reopen of a shape that holds nothing but its directories opens it as new", ctx do
    # Arrange
    opts = start_stack(ctx)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.terminate(writer)
    File.rm!(metadata_file(ctx, "version"))

    # Act
    opts = reopen(ctx)

    # Assert
    assert File.exists?(metadata_file(ctx, "version"))
    refute PureFileStorage.snapshot_started?(opts)
  end

  # A host can provision the storage directory under a parent the service may neither open nor
  # write, 0711 say. The service could not have created the storage directory there, so the first
  # claim goes on.
  test "the first durable claim goes on when the storage directory's parent is closed to the service",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    storage_parent = Path.dirname(ctx.tmp_dir)
    refuse_to_open(storage_parent)

    Repatch.patch(File, :stat, [mode: :local], fn
      ^storage_parent -> {:ok, %File.Stat{type: :directory, access: :none}}
      path -> Repatch.real(File.stat(path))
    end)

    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    # Act
    writer
    |> then(
      &PureFileStorage.append_fragment_to_log!(
        [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1,"b":"#{@flushing_payload}"}|}],
        &1
      )
    )
    |> then(&PureFileStorage.signal_txn_commit!(1, &1))

    # Assert
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # A parent the service may write but not open can hold an entry the service made, which a crash
  # could take with every claim below it, so the first claim fails rather than go on.
  test "the first durable claim fails when the storage directory's parent can be written but not opened",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    # The snapshot syncs the same parent, so the refusal comes after it, for the claim alone.
    PureFileStorage.make_new_snapshot!([], opts)
    refuse_to_open(Path.dirname(ctx.tmp_dir))

    # Act and assert
    assert_raise File.Error, ~r/open directory to sync/, fn ->
      writer
      |> then(
        &PureFileStorage.append_fragment_to_log!(
          [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1,"b":"#{@flushing_payload}"}|}],
          &1
        )
      )
      |> then(&PureFileStorage.signal_txn_commit!(1, &1))
    end
  end

  # A storage directory mounted where it is was there before the service, so the entry naming it
  # is the host's, in a parent the service may write but not open.
  test "the first durable claim goes on when the storage directory is mounted in a parent the service may write but not open",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)
    mount_at(ctx.tmp_dir)
    refuse_to_open(Path.dirname(ctx.tmp_dir))

    # Act
    writer
    |> then(
      &PureFileStorage.append_fragment_to_log!(
        [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1,"b":"#{@flushing_payload}"}|}],
        &1
      )
    )
    |> then(&PureFileStorage.signal_txn_commit!(1, &1))

    # Assert
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # `ELECTRIC_STORAGE_DIR` holds the storage directory, `shapes`, and the service makes it with
  # every missing directory above it, or finds them made by another part of the service. A claim
  # or a snapshot syncs the entries up to the one naming `shapes`, and the stack's start those
  # above it.
  test "a stack start syncs the entries above the storage directory that the service could have made",
       ctx do
    # Arrange
    volume = Path.join(ctx.tmp_dir, "volume")
    root = Path.join(volume, "electric")
    record_syncs(ctx, fn -> nil end)

    opts =
      PureFileStorage.shared_opts(stack_id: ctx.stack_id, storage_dir: Path.join(root, "shapes"))

    # Act
    {:ok, stack} = PureFileStorage.stack_start_link(opts)

    # Assert
    synced = for {{:dir, dir}, _} <- recorded_syncs(), do: dir
    Supervisor.stop(stack)
    assert File.dir?(Path.join(root, "shapes"))

    for dir <- [volume, ctx.tmp_dir] do
      assert dir_id(dir) in Enum.map(synced, &dir_id/1),
             "#{dir} was not synced: #{inspect(synced)}"
    end
  end

  # The service makes the storage directory through the path it is given, and after a symlink
  # `..` leads to the parent of the link's target, not to the directory that holds the link.
  test "a stack start syncs the parents the storage directory was made in when its path takes `..` after a symlink",
       ctx do
    # Arrange
    tree = Path.join(ctx.tmp_dir, "tree")
    File.mkdir_p!(Path.join(tree, "current"))
    aliases = Path.join(ctx.tmp_dir, "aliases")
    File.mkdir_p!(aliases)
    File.ln_s!(Path.join(tree, "current"), Path.join(aliases, "current"))
    root = Path.join([aliases, "current", "..", "tenant", "electric"])
    record_syncs(ctx, fn -> nil end)

    opts =
      PureFileStorage.shared_opts(stack_id: ctx.stack_id, storage_dir: Path.join(root, "shapes"))

    # Act
    {:ok, stack} = PureFileStorage.stack_start_link(opts)

    # Assert
    synced = for {{:dir, dir}, _} <- recorded_syncs(), do: dir_id(dir)
    Supervisor.stop(stack)
    assert File.dir?(Path.join([tree, "tenant", "electric", "shapes"]))

    for dir <- [Path.join(tree, "tenant"), tree, ctx.tmp_dir] do
      assert dir_id(dir) in synced, "#{dir} was not synced"
    end

    refute dir_id(aliases) in synced
  end

  # A storage directory mounted where it is was there before the service, and so was every
  # directory above it: one the service may write but not open must not stop the stack.
  test "a stack start syncs nothing above a storage directory mounted where it is", ctx do
    # Arrange
    volume = Path.join(ctx.tmp_dir, "volume")
    root = Path.join(volume, "electric")
    storage_dir = Path.join(root, "shapes")
    File.mkdir_p!(storage_dir)
    mount_at(storage_dir)
    record_syncs(ctx, fn -> nil end)
    opts = PureFileStorage.shared_opts(stack_id: ctx.stack_id, storage_dir: storage_dir)

    # Act
    {:ok, stack} = PureFileStorage.stack_start_link(opts)

    # Assert
    synced = for {{:dir, dir}, _} <- recorded_syncs(), do: dir_id(dir)
    Supervisor.stop(stack)

    for dir <- [volume, ctx.tmp_dir] do
      refute dir_id(dir) in synced, "#{dir} was synced"
    end
  end

  # A directory above the storage directory that the service may write but not open, the one that
  # holds a mount on the same device or the parent of the working directory a relative path starts
  # from, is the host's, and so is one whose path grows too long to open: the stack starts, and
  # the walk syncs nothing above it.
  for reason <- [:eacces, :enametoolong] do
    @tag refused: reason
    test "a stack start goes on past a directory above the storage directory it cannot open (#{reason})",
         ctx do
      # Arrange
      volume = Path.join(ctx.tmp_dir, "volume")
      root = Path.join(volume, "electric")
      File.mkdir_p!(volume)
      refuse_to_open_dir(volume, ctx.refused)

      opts =
        PureFileStorage.shared_opts(
          stack_id: ctx.stack_id,
          storage_dir: Path.join(root, "shapes")
        )

      # Act
      {:ok, stack} = PureFileStorage.stack_start_link(opts)

      # Assert
      synced = for {{:dir, dir}, _} <- recorded_syncs(), do: dir_id(dir)
      Supervisor.stop(stack)
      assert File.dir?(Path.join(root, "shapes"))
      assert_received {:refused, _path}

      for dir <- [volume, ctx.tmp_dir] do
        refute dir_id(dir) in synced, "#{dir} was synced"
      end
    end
  end

  # A datasync makes the log's bytes durable, not the directory entries that lead to them, and
  # every one of those entries is new with the shape.
  test "the first durable claim follows the sync of the log's directories", ctx do
    # Arrange
    opts = start_stack(ctx)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    # The snapshot syncs the same directories, so only the claim's own syncs are recorded.
    PureFileStorage.make_new_snapshot!([], opts)
    record_syncs(ctx)

    # Act
    writer
    |> then(
      &PureFileStorage.append_fragment_to_log!(
        [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1,"b":"#{@flushing_payload}"}|}],
        &1
      )
    )
    |> then(&PureFileStorage.signal_txn_commit!(1, &1))

    # Assert
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
    assert_log_directories_synced_before_the_claim(ctx)
  end

  # The catalog's mark that the snapshot is complete can outlive a machine crash before any claim,
  # and a block a crash lost below a chunk's end reads back as zeros the reader serves.
  test "a snapshot's chunks and their directories are synced before its last chunk is recorded durably",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    _writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    record_metadata_writes()
    record_syncs(ctx)
    record_chunk_descriptors()

    # Act
    PureFileStorage.make_new_snapshot!([~S|{"n":1}|, :chunk_boundary, ~S|{"n":2}|], opts)

    # Assert
    writes_and_syncs = recorded_writes_and_syncs()
    chunks = for n <- 0..1, do: {:chunk_descriptor, Snapshot.chunk_file_path(opts, n)}
    directories = for dir <- log_directories(ctx), do: {:dir, dir}

    before_the_record =
      Enum.take_while(
        writes_and_syncs,
        &(not match?({:written, "last_snapshot_chunk" <> _, _}, &1))
      )

    for target <- chunks ++ directories do
      assert {:synced, target} in before_the_record,
             "#{inspect(target)} not synced first: #{inspect(writes_and_syncs)}"
    end

    # Each chunk's last write is in what its sync covers, and its close reports what the sync did not.
    for {:chunk_descriptor, path} = chunk <- chunks do
      events = Enum.with_index(before_the_record)
      last_write = events |> Enum.filter(&match?({{:chunk_written, ^path}, _}, &1)) |> List.last()
      sync = Enum.find(events, &match?({{:synced, ^chunk}, _}, &1))
      close = Enum.find(events, &match?({{:chunk_closed, ^path}, _}, &1))

      assert (last_write && sync && close &&
                elem(last_write, 1) < elem(sync, 1)) and elem(sync, 1) < elem(close, 1),
             "#{path} not written, synced and closed in that order: #{inspect(writes_and_syncs)}"
    end

    assert_durable_write(writes_and_syncs, "last_snapshot_chunk", metadata_dir(ctx))
  end

  # A write the device fails after it returned is reported to the descriptor that wrote it, at its
  # sync or at its close: a chunk whose sync or close fails is not one the end may name.
  for failing <- [:datasync, :close] do
    test "a snapshot whose chunk fails its #{failing} fails before its last chunk is recorded",
         ctx do
      # Arrange
      opts = start_stack(ctx)
      _writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      record_chunk_descriptors(unquote(failing))

      # Act
      error =
        assert_raise File.Error, fn ->
          PureFileStorage.make_new_snapshot!([~S|{"n":1}|], opts)
        end

      # Assert
      assert %File.Error{action: unquote(to_string(failing)), reason: :eio} = error
      assert error.path == Snapshot.chunk_file_path(opts, 0)
      refute File.exists?(metadata_file(ctx, "last_snapshot_chunk"))
    end
  end

  # With nothing synced before the kill, the promotion on reopen is the shape's first claim.
  test "a promotion that makes the first durable claim syncs the log's directories first", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_unsynced_then_kill(opts)
    refute File.exists?(durable_boundary_path(ctx))
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
    assert_log_directories_synced_before_the_claim(ctx)
  end

  # A writer killed after renaming its durable claim but before syncing the metadata directory
  # leaves a claim the next writer reads and a machine crash can still take back. That writer takes
  # the boundary as synced, so the reopen makes the rename durable first.
  test "a reopen syncs the durable claim it hands the writer", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
    record_syncs(ctx)

    # Act
    opts = reopen(ctx)

    # Assert
    assert {{:dir, metadata_dir(ctx)}, LogOffset.new(1, 0)} in recorded_syncs()
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
  end

  # A claim the shape's first claim did not sync the entries for, as one a build without that sync
  # wrote, looks like any claim with no generation recorded: the reopen syncs the log and every
  # directory down to it before the metadata directory, once.
  test "a reopen with no generation recorded syncs the log and its directories first", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    files = generation_files(ctx, "latest.0")
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    assert_synced_before_the_metadata_directory(
      ctx,
      files ++ Enum.map(log_directories(ctx), &{:dir, &1})
    )
  end

  # The same holds when the reopen promotes a written boundary past a claim that is already real.
  test "a promotion past a claim with no generation recorded syncs the log's directories first",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    files = generation_files(ctx, "latest.0")
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    syncs = recorded_syncs()

    for target <- files ++ Enum.map(log_directories(ctx), &{:dir, &1}) do
      assert {target, LogOffset.new(1, 0)} in syncs,
             "#{inspect(target)} was not synced before the claim moved"
    end

    assert LogOffset.new(2, 0) == durable_boundary(ctx)
  end

  test "a second reopen of the same generation syncs only the claim's and the boot record's directories",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    assert [
             {{:dir, metadata_dir(ctx)}, LogOffset.new(1, 0)},
             {{:dir, boot_dir(ctx)}, LogOffset.new(1, 0)}
           ] == recorded_syncs()
  end

  # A compaction renames `compaction_boundary` and `latest_name` with the generation's files still
  # unsynced, and a writer killed after that leaves both names for the reopen, whose metadata
  # directory sync would make them durable ahead of the files. The names' own content is unsynced
  # too, and a name that survives with no content fails the next reopen. The compacted generation
  # covers the claim here, so the reopen names an empty latest generation in place of the one the
  # compaction left, and syncs none of that one's files.
  test "a reopen after a compaction syncs the new generation before its names", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    replaced = generation_files(ctx, "latest.1")
    files = generation_files(ctx, "compacted.0") ++ name_files(ctx, ["compaction_boundary"])
    log_dir = Path.join(shape_dir(ctx), "log")
    record_syncs(ctx)
    record_metadata_writes()

    # Act
    reopen(ctx)

    # Assert: the compacted generation and its name before the metadata directory, which makes
    # that name durable, and the new latest generation before its name.
    published = read_term(metadata_file(ctx, "latest_name"))
    assert published not in ["latest.0", "latest.1"]
    events = recorded_writes_and_syncs()
    metadata_at = Enum.find_index(events, &(&1 == {:synced, {:dir, metadata_dir(ctx)}}))
    assert metadata_at, "the metadata directory was not synced: #{inspect(events)}"

    for target <- files ++ [{:dir, log_dir}] do
      at = Enum.find_index(events, &(&1 == {:synced, target}))

      assert at && at < metadata_at,
             "#{inspect(target)} was not synced before the metadata directory: #{inspect(events)}"
    end

    published_files = for {:file, path} <- generation_files(ctx, published), do: path
    assert_published_before_named(events, published_files, log_dir)
    assert_durable_write(events, "latest_name", metadata_dir(ctx))
    syncs = for {:synced, target} <- events, do: target
    assert [] == Enum.filter(replaced, &(&1 in syncs))
  end

  # A writer killed between the two renames leaves the new compacted generation under the latest
  # name the last reopen already made durable.
  test "a reopen after a kill between a compaction's names syncs the compacted files", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.0")
    files = generation_files(ctx, "compacted.0") ++ name_files(ctx, ["compaction_boundary"])
    record_syncs(ctx)

    # Act
    reopen(ctx)

    # Assert
    assert_synced_before_the_metadata_directory(
      ctx,
      files ++ [{:dir, Path.join(shape_dir(ctx), "log")}]
    )
  end

  # A build before this one synced nothing after a compaction, so a crash could take the new
  # latest generation's chunk index, and with the compacted generation covering the claim the trim
  # takes that for the compaction. The writer created the index at its first append, and nothing
  # synced that entry, so the reopen publishes an empty generation: its files and their entry
  # durable before its name, and its name before the writer opens. A later reopen keeps it.
  test "a reopen publishes an empty latest generation for a claim a compaction covers", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    [{:file, log}, {:file, chunks}] = generation_files(ctx, "latest.1")
    File.write!(log, "")
    File.rm!(chunks)
    record_syncs(ctx)
    record_metadata_writes()

    # Act
    reopen(ctx)

    # Assert
    published = read_term(metadata_file(ctx, "latest_name"))
    assert published not in ["latest.0", "latest.1"]
    files = for {:file, path} <- generation_files(ctx, published), do: path
    assert Enum.map(files, &File.stat!(&1).size) == [0, 0]
    writes_and_syncs = recorded_writes_and_syncs()
    assert_published_before_named(writes_and_syncs, files, Path.join(shape_dir(ctx), "log"))
    assert_durable_write(writes_and_syncs, "latest_name", metadata_dir(ctx))
    reopen(ctx)
    assert read_term(metadata_file(ctx, "latest_name")) == published
  end

  # A compaction writer killed before it synced the metadata directory leaves the boundary's rename
  # to the reopen. The latest name is renamed in that directory too, and a machine crash can keep
  # that rename and take the boundary's back, leaving an empty latest generation under none.
  test "a reopen under a covering compaction syncs the boundary's name before it renames the latest name",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    record_syncs(ctx)
    record_metadata_writes()

    # Act
    reopen(ctx)

    # Assert
    events = recorded_writes_and_syncs()
    boundary = {:synced, {:file, metadata_file(ctx, "compaction_boundary")}}
    boundary_at = Enum.find_index(events, &(&1 == boundary))
    named_at = Enum.find_index(events, &(&1 == {:renamed, "latest_name.bin"}))
    assert boundary_at && named_at, inspect(events)

    assert {:synced, {:dir, metadata_dir(ctx)}} in Enum.slice(events, boundary_at..named_at//1),
           "the metadata directory was not synced between the boundary and the latest name: " <>
             inspect(events)
  end

  # A build before this one renamed the latest name into place with no sync. The metadata directory
  # sync that makes the boundary's rename durable makes that name's too, ahead of its content, and
  # a machine crash before the new name left one the next reopen could not decode.
  test "a reopen under a covering compaction syncs the latest name before the metadata directory",
       ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    record_syncs(ctx)
    record_metadata_writes()

    # Act
    reopen(ctx)

    # Assert
    events = recorded_writes_and_syncs()
    name = {:synced, {:file, metadata_file(ctx, "latest_name")}}
    name_at = Enum.find_index(events, &(&1 == name))
    metadata_at = Enum.find_index(events, &(&1 == {:synced, {:dir, metadata_dir(ctx)}}))
    named_at = Enum.find_index(events, &(&1 == {:renamed, "latest_name.bin"}))
    assert name_at && metadata_at && named_at, inspect(events)

    assert name_at < metadata_at and metadata_at < named_at,
           "the latest name was not synced before the metadata directory: " <> inspect(events)
  end

  # A compaction publishes its three files before it names them, so a name with a file missing is
  # a generation the machine lost, and every read below its boundary would come back empty.
  test "a reopen refuses a compacted generation with a file missing", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    File.rm!(Path.join([shape_dir(ctx), "log", "log.compacted.0.chunk.bin"]))

    # Act
    error = assert_raise Storage.Error, fn -> reopen(ctx) end

    # Assert
    assert error.kind == :data_loss
    assert error.message =~ "log.compacted.0.chunk.bin"
    refute {"latest.1", "compacted.0"} == durable_generation(ctx)
  end

  # A build that named a compaction's files before syncing them could leave, after a machine
  # crash, the names over a file cut short: the reopen found every file present, recorded the
  # generation durable, and reads below the boundary came back short or empty.
  test "a reopen refuses a compacted generation whose index ends short of its log", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    File.write!(Path.join([shape_dir(ctx), "log", "log.compacted.0.chunk.bin"]), "")

    # Act
    error = assert_raise Storage.Error, fn -> reopen(ctx) end

    # Assert
    assert error.kind == :data_loss
    assert error.message =~ "compacted.0"
    refute {"latest.1", "compacted.0"} == durable_generation(ctx)
  end

  # The same build could leave all three files emptied, which match an empty index, or cut short
  # on a chunk boundary, which match each other: either ends short of the boundary it names.
  test "a reopen refuses a compacted generation that ends short of its boundary", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")

    for kind <- ["jsonfile", "chunk", "keyfile"],
        do: File.write!(Path.join([shape_dir(ctx), "log", "log.compacted.0.#{kind}.bin"]), "")

    # Act
    error = assert_raise Storage.Error, fn -> reopen(ctx) end

    # Assert
    assert error.kind == :data_loss
    assert error.message =~ "compacted.0"
    refute {"latest.1", "compacted.0"} == durable_generation(ctx)
  end

  # Cut short on a chunk boundary, the three files still match each other: only the boundary they
  # are named under tells that the generation ends before it.
  test "a reopen refuses a compacted generation cut short on a chunk boundary", ctx do
    # Arrange
    opts = start_stack(ctx)

    write_then_kill(opts, [
      ~s|{"n":1,"b":"#{@flushing_payload}"}|,
      ~s|{"n":2,"b":"#{@flushing_payload}"}|
    ])

    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    log = Path.join([shape_dir(ctx), "log", "log.compacted.0.jsonfile.bin"])
    first_chunk_end = entries_end(log, 1)
    truncate_at(log, first_chunk_end)

    File.write!(
      Path.join([shape_dir(ctx), "log", "log.compacted.0.chunk.bin"]),
      PureFileStorage.ChunkIndex.make_full_entry(
        {LogOffset.new(1, 0), LogOffset.new(1, 0)},
        {0, first_chunk_end},
        {0, 0}
      )
    )

    write_metadata_file(ctx, "compaction_boundary", {LogOffset.new(2, 0), "compacted.0"})

    # Act
    error = assert_raise Storage.Error, fn -> reopen(ctx) end

    # Assert
    assert error.kind == :data_loss
    assert error.message =~ "compacted.0"
    refute {"latest.1", "compacted.0"} == durable_generation(ctx)
  end

  # A build that cut a compaction past the durable claim could leave, after a crash, a boundary
  # above the offset the shape resumes from, over rows of transactions the log no longer holds.
  test "a reopen refuses a compaction boundary past its durable claim", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    reopen(ctx)
    switch_generation(ctx, compacted: "compacted.0", latest: "latest.1")
    write_metadata_file(ctx, "last_persisted_txn_offset", LogOffset.last_before_real_offsets())
    File.rm(metadata_file(ctx, "last_written_txn_offset"))

    # Act
    error = assert_raise Storage.Error, fn -> reopen(ctx) end

    # Assert
    assert error.kind == :data_loss
    assert error.message =~ "past its durable offset"
  end

  # A compaction's switch writes the new name to disk, then to the cache. A writer killed between
  # the two leaves the cache on a generation a restart does not read, and a writer reopened in the
  # same stack took it from there: what it then made durable was gone at the next restart.
  test "a reopen in the same stack appends to the generation named on disk", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    log_dir = Path.join(shape_dir(ctx), "log")
    copy_generation(log_dir, "latest.0", "latest.1")
    write_metadata_file(ctx, "latest_name", "latest.1")
    append_then_kill(opts, 2, ~s|{"n":2,"b":"#{@flushing_payload}"}|)

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(2, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 2 == opts |> log_stream() |> Enum.count()
  end

  # A compaction renamed both names with its files unsynced, and the next durable claim's sync of
  # the metadata directory made the names durable ahead of the files they point at: a machine
  # crash then kept a claim on a generation it had lost. Both generations, files and entries, are
  # synced before the first name, and each name is written durably.
  test "a compaction syncs each generation before the name that points at it", ctx do
    # Arrange
    opts = start_stack(ctx, 5)
    writer = open_writer_with_synced_txns(opts, 1..3)
    log_dir = Path.join(shape_dir(ctx), "log")

    {:complete, {_, offset}, {_, position}, _} =
      PureFileStorage.ChunkIndex.get_nth_chunk(
        Path.join(log_dir, "log.latest.0.chunk.bin"),
        -1,
        only_complete?: true
      )

    record_syncs(ctx, fn ->
      {read_term(metadata_file(ctx, "compaction_boundary")),
       read_term(metadata_file(ctx, "latest_name"))}
    end)

    record_metadata_writes()

    # Act
    PureFileStorage.make_compacted_files(self(), opts, offset, position, "latest.0")

    assert_receive {Storage,
                    {PureFileStorage, :handle_compaction_finished, [_, compacted, _, _]} = message}

    writer = apply_storage_message(writer, message)

    # Assert
    syncs = recorded_syncs()
    latest = PureFileStorage.latest_name(opts)

    assert_synced_before_named(
      syncs,
      generation_files(ctx, compacted) ++ generation_files(ctx, latest) ++ [{:dir, log_dir}],
      &match?({_, {{_, ^compacted}, _}}, &1)
    )

    assert {{:dir, metadata_dir(ctx)}, {{offset, compacted}, "latest.0"}} in syncs
    assert {{:dir, metadata_dir(ctx)}, {{offset, compacted}, latest}} in syncs
    assert_written_with_sync("compaction_boundary")
    assert_written_with_sync("latest_name")
    PureFileStorage.terminate(writer)
  end

  # The version write syncs the metadata directory, which makes a new shape's `latest_name` durable
  # whatever its content: a crash could keep a name that does not decode, and fail every reopen.
  test "a new shape's generation name is durable before its version", ctx do
    # Arrange
    opts = start_stack(ctx)

    record_syncs(ctx, fn ->
      {File.exists?(metadata_file(ctx, "latest_name")),
       File.exists?(metadata_file(ctx, "version"))}
    end)

    record_metadata_writes()

    # Act
    _writer = PureFileStorage.init_writer!(opts, @shape)

    # Assert
    assert {{:dir, metadata_dir(ctx)}, {true, false}} in recorded_syncs()
    assert_written_with_sync("latest_name")
  end

  # The generation record only spares a later reopen the syncs it made, so a reopen that cannot
  # write it, on a full disk say, still opens, and the next one syncs again.
  test "a reopen that cannot record its generation still opens", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    File.mkdir_p!(metadata_file(ctx, "durable_generation") <> ".tmp")

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert nil == durable_generation(ctx)
  end

  # Nor does a record that cannot be opened cost the shape: the reopen runs the pass it would have
  # spared.
  test "a reopen that cannot open its generation record syncs the generation and opens", ctx do
    # Arrange
    opts = start_stack(ctx)
    write_then_kill(opts, [~s|{"n":1,"b":"#{@flushing_payload}"}|])
    File.mkdir_p!(metadata_file(ctx, "durable_generation"))
    record_syncs(ctx)

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert_synced_before_the_metadata_directory(ctx, generation_files(ctx, "latest.0"))
  end

  defp assert_torn_written_marker_reopens_at_durable_boundary(ctx, tear) do
    # Arrange
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)

    written_marker =
      PureFileStorage.shape_data_dir(base_path(ctx), @shape_handle, [
        "metadata",
        "last_written_txn_offset.bin"
      ])

    File.write!(written_marker, written_marker |> File.read!() |> tear.())

    # Act
    opts = reopen(ctx)

    # Assert
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    assert 1 == opts |> log_stream() |> Enum.count()
    assert LogOffset.new(1, 0) == durable_boundary(ctx)
  end

  # Reports each directory sync and file datasync this process makes, in order, with what `now`
  # reads at the time: by default the durable claim (nil before the first).
  defp record_syncs(ctx, now \\ nil) do
    parent = self()
    now = now || fn -> read_term(durable_boundary_path(ctx)) end

    Repatch.patch(Electric.Utils, :sync_dir!, [mode: :local], fn dir ->
      send(parent, {:synced, {:dir, dir}, now.()})
      Repatch.real(Electric.Utils.sync_dir!(dir))
    end)

    Repatch.patch(Electric.Utils, :datasync_file!, [mode: :local], fn path ->
      send(parent, {:synced, {:file, path}, now.()})
      Repatch.real(Electric.Utils.datasync_file!(path))
    end)
  end

  # Wraps each snapshot chunk opened for writing in a `ChunkDescriptor`, so its datasync is seen on
  # the descriptor that wrote the chunk, and `failing` (`:datasync` or `:close`) fails there.
  defp record_chunk_descriptors(failing \\ nil) do
    test = self()

    Repatch.patch(File, :open!, [mode: :local], fn path, modes ->
      file = Repatch.real(File.open!(path, modes))

      if String.ends_with?(path, ".jsonsnapshot") and :write in modes,
        do: ChunkDescriptor.wrap(file, path, test, failing),
        else: file
    end)
  end

  defp read_term(path) do
    case File.read(path) do
      {:ok, bytes} -> :erlang.binary_to_term(bytes)
      {:error, :enoent} -> nil
    end
  end

  defp recorded_syncs do
    receive do
      {:synced, target, claim} -> [{target, claim} | recorded_syncs()]
    after
      0 -> []
    end
  end

  # The directory sync after a rename makes the name durable whatever the file holds, so the
  # content has to be synced before the rename: a sync of the directory alone does not show it.
  defp record_metadata_writes do
    parent = self()

    Repatch.patch(File, :write!, [mode: :local], fn path, content, modes ->
      send(parent, {:written, Path.basename(path), modes})
      Repatch.real(File.write!(path, content, modes))
    end)

    Repatch.patch(PureFileStorage.FileInfo, :rename, [mode: :local], fn from, to ->
      result = Repatch.real(PureFileStorage.FileInfo.rename(from, to))
      send(parent, {:renamed, Path.basename(to)})
      result
    end)
  end

  defp assert_written_with_sync(key) do
    writes = metadata_writes(key <> ".bin.tmp")
    assert writes != [], "#{key} was not written"

    assert Enum.all?(writes, &(:sync in &1)),
           "#{key} was written without :sync: #{inspect(writes)}"
  end

  # Reports each directory `File.mkdir/1` makes, in order with the writes and syncs.
  defp record_mkdirs do
    parent = self()

    Repatch.patch(File, :mkdir, [mode: :local], fn path ->
      result = Repatch.real(File.mkdir(path))
      if result == :ok, do: send(parent, {:made, path})
      result
    end)
  end

  # The metadata writes, directories made and syncs recorded so far, in the order they were made.
  defp recorded_writes_and_syncs do
    receive do
      {:made, dir} -> [{:made, dir} | recorded_writes_and_syncs()]
      {:written, name, modes} -> [{:written, name, modes} | recorded_writes_and_syncs()]
      {:renamed, name} -> [{:renamed, name} | recorded_writes_and_syncs()]
      {:synced, target, _claim} -> [{:synced, target} | recorded_writes_and_syncs()]
      {:chunk_written, path} -> [{:chunk_written, path} | recorded_writes_and_syncs()]
      {:chunk_closed, path} -> [{:chunk_closed, path} | recorded_writes_and_syncs()]
    after
      0 -> []
    end
  end

  # Every write of the key is synced before its rename, and its directory after the last rename.
  defp assert_durable_write(writes_and_syncs, key, dir) do
    name = key <> ".bin.tmp"

    writes = for {:written, ^name, modes} <- writes_and_syncs, do: modes

    assert writes != [], "#{key} was not written"

    assert Enum.all?(writes, &(:sync in &1)),
           "#{key} was written without :sync: #{inspect(writes_and_syncs)}"

    renames =
      for {{:renamed, renamed}, at} <- Enum.with_index(writes_and_syncs),
          renamed == key <> ".bin",
          do: at

    assert length(renames) == length(writes), "#{key} was not renamed into place"

    assert {:synced, {:dir, dir}} in Enum.drop(writes_and_syncs, List.last(renames) + 1),
           "#{dir} was not synced after #{key} was renamed: #{inspect(writes_and_syncs)}"
  end

  # Each of `files` is synced, then `dir`, and only then the latest name written and renamed into
  # place, once: a name written earlier as well points at files a crash can still take.
  defp assert_published_before_named(writes_and_syncs, files, dir) do
    written =
      for {{:written, "latest_name.bin.tmp", _}, at} <- Enum.with_index(writes_and_syncs),
          do: at

    assert length(written) == 1,
           "the latest name was not written exactly once: #{inspect(writes_and_syncs)}"

    [written_at] = written

    named = for {{:renamed, "latest_name.bin"}, at} <- Enum.with_index(writes_and_syncs), do: at

    assert length(named) == 1,
           "the latest name was not renamed into place exactly once: " <>
             inspect(writes_and_syncs)

    [named_at] = named

    synced_at =
      for file <- files do
        at = Enum.find_index(writes_and_syncs, &(&1 == {:synced, {:file, file}}))
        assert at, "#{file} was not synced: #{inspect(writes_and_syncs)}"
        at
      end

    dir_at =
      writes_and_syncs
      |> Enum.with_index()
      |> Enum.find_value(fn {entry, at} ->
        if entry == {:synced, {:dir, dir}} and at > Enum.max(synced_at), do: at
      end)

    assert dir_at && dir_at < written_at && written_at < named_at,
           "#{dir} was not synced between the files and the latest name: " <>
             inspect(writes_and_syncs)
  end

  # The record's directory is made, each of `dirs` synced, and only then the record renamed into
  # place and its directory synced.
  defp assert_synced_before_the_record(writes_and_syncs, ctx, dirs) do
    boot_dir = boot_dir(ctx)
    made_at = Enum.find_index(writes_and_syncs, &(&1 == {:made, boot_dir}))
    assert made_at, "the record's directory was not made: #{inspect(writes_and_syncs)}"
    {_, [_made | since_made]} = Enum.split(writes_and_syncs, made_at)
    renamed_at = Enum.find_index(since_made, &(&1 == {:renamed, "written_boot.bin"}))
    assert renamed_at, "the record was not renamed into place: #{inspect(writes_and_syncs)}"
    {before_renamed, [_renamed | since_renamed]} = Enum.split(since_made, renamed_at)

    for dir <- dirs do
      assert {:synced, {:dir, dir}} in before_renamed,
             "#{dir} was not synced between making the record's directory and renaming the " <>
               "record: #{inspect(writes_and_syncs)}"
    end

    assert {:synced, {:dir, boot_dir}} in since_renamed,
           "the record's directory was not synced after the rename: #{inspect(writes_and_syncs)}"
  end

  defp metadata_writes(name) do
    receive do
      {:written, ^name, modes} -> [modes | metadata_writes(name)]
    after
      0 -> []
    end
  end

  defp assert_log_directories_synced_before_the_claim(ctx) do
    syncs = recorded_syncs()

    for dir <- log_directories(ctx) do
      assert {{:dir, dir}, nil} in syncs, "#{dir} was not synced before the claim"
    end
  end

  # Each target is synced, and in that order, while no name points at the generation yet.
  defp assert_synced_before_named(syncs, targets, named?) do
    named_at = Enum.find_index(syncs, named?) || length(syncs)
    synced = syncs |> Enum.take(named_at) |> Enum.map(&elem(&1, 0))

    Enum.reduce(targets, 0, fn target, from ->
      at = synced |> Enum.drop(from) |> Enum.find_index(&(&1 == target))
      assert at, "#{inspect(target)} was not synced before it was named: #{inspect(syncs)}"
      from + at + 1
    end)
  end

  # The first trim of a log answers as if it had cut it and leaves every byte in place.
  defp keep_tail_on_first_trim do
    Process.put(:keep_tail, true)

    Repatch.patch(PureFileStorage.LogFile, :trim, [mode: :local], fn path,
                                                                     position,
                                                                     offset,
                                                                     opts ->
      if Process.delete(:keep_tail),
        do: :ok,
        else: Repatch.real(PureFileStorage.LogFile.trim(path, position, offset, opts))
    end)
  end

  # Opens `path` without `:write`, so a truncate in the function given to `File.open/3` fails, with
  # the other modes, `:read_ahead` among them, as the caller gave them.
  defp fail_writes_of(path) do
    Repatch.patch(File, :open, [mode: :local], fn
      ^path, modes, fun -> Repatch.real(File.open(path, List.delete(modes, :write), fun))
      other, modes, fun -> Repatch.real(File.open(other, modes, fun))
    end)
  end

  # Opens `path` write-only, so the read in the function given to `File.open/3` fails (`EBADF`).
  defp fail_reads_of(path) do
    Repatch.patch(File, :open, [mode: :local], fn
      ^path, _modes, fun -> Repatch.real(File.open(path, [:append, :raw], fun))
      other, modes, fun -> Repatch.real(File.open(other, modes, fun))
    end)
  end

  # Every open of `path` fails (`EIO`), each counted in `:failed_opens`.
  defp fail_opens_of(path) do
    Repatch.patch(File, :open, [mode: :local], fn
      ^path, _modes, _fun ->
        Process.put(:failed_opens, Process.get(:failed_opens, 0) + 1)
        {:error, :eio}

      other, modes, fun ->
        Repatch.real(File.open(other, modes, fun))
    end)
  end

  # Every read of the size of `path` fails (`EIO`), each counted in `:failed_size_reads`.
  defp fail_size_reads_of(path) do
    Repatch.patch(PureFileStorage.FileInfo, :file_size, [mode: :local], fn
      ^path ->
        Process.put(:failed_size_reads, Process.get(:failed_size_reads, 0) + 1)
        {:error, :eio}

      other ->
        Repatch.real(PureFileStorage.FileInfo.file_size(other))
    end)
  end

  # Every read of a log entry at or past byte `from` fails (`EIO`), each counted in
  # `:failed_log_reads`; every read before it goes through.
  defp fail_reads_from(from) do
    Repatch.patch(PureFileStorage.LogFile, :read_entry, [mode: :local], fn file ->
      case :file.position(file, :cur) do
        {:ok, position} when position >= from ->
          Process.put(:failed_log_reads, Process.get(:failed_log_reads, 0) + 1)
          {:error, :eio}

        _ ->
          Repatch.super(PureFileStorage.LogFile.read_entry(file))
      end
    end)
  end

  # `dir` reads as the root of a mount: on a device of its own.
  defp mount_at(dir) do
    Repatch.patch(File, :stat, [mode: :local], fn
      ^dir ->
        {:ok, %File.Stat{} = stat} = Repatch.real(File.stat(dir))
        {:ok, %{stat | major_device: stat.major_device + 1}}

      path ->
        Repatch.real(File.stat(path))
    end)
  end

  defp dir_id(dir) do
    %File.Stat{major_device: device, inode: inode} = File.stat!(dir)
    {device, inode}
  end

  defp refuse_to_open(dir) do
    Repatch.patch(Electric.Utils, :sync_dir!, [mode: :local], fn
      ^dir -> raise File.Error, reason: :eacces, action: "open directory to sync", path: dir
      other -> Repatch.real(Electric.Utils.sync_dir!(other))
    end)
  end

  # Refuses the sync of `dir` for `reason` whatever path names it, the `..` of a directory below it
  # say, and records the refusal and the syncs of the others.
  defp refuse_to_open_dir(dir, reason) do
    parent = self()
    id = dir_id(dir)

    Repatch.patch(Electric.Utils, :sync_dir!, [mode: :local], fn path ->
      if File.dir?(path) and dir_id(path) == id do
        send(parent, {:refused, path})
        raise File.Error, reason: reason, action: "open directory to sync", path: path
      end

      send(parent, {:synced, {:dir, path}, nil})
      Repatch.real(Electric.Utils.sync_dir!(path))
    end)
  end

  # A writer in this process, holding txns `range` each synced at its commit.
  defp open_writer_with_synced_txns(opts, range) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    Enum.reduce(range, writer, fn n, writer ->
      writer
      |> then(
        &PureFileStorage.append_fragment_to_log!(
          [{LogOffset.new(n, 0), "key-#{n}", :insert, log_entry("key-#{n}")}],
          &1
        )
      )
      |> then(&PureFileStorage.signal_txn_commit!(n, &1))
    end)
  end

  # An entry as the shape log holds it, which a compaction parses to merge, padded to flush.
  defp log_entry(key) do
    ~s|{"key":"#{key}","value":{"id":"#{key}","b":"#{@flushing_payload}"},| <>
      ~s|"headers":{"operation":"insert","relation":["public","test_table"]}}|
  end

  defp apply_storage_message(writer, {module, function, args}),
    do: apply(module, function, [writer | args])

  defp assert_synced_before_the_metadata_directory(ctx, targets) do
    syncs = Enum.map(recorded_syncs(), &elem(&1, 0))
    metadata_at = Enum.find_index(syncs, &(&1 == {:dir, metadata_dir(ctx)}))
    assert metadata_at, "the metadata directory was not synced: #{inspect(syncs)}"

    for target <- targets do
      at = Enum.find_index(syncs, &(&1 == target))

      assert at && at < metadata_at,
             "#{inspect(target)} was not synced before the metadata directory: #{inspect(syncs)}"
    end
  end

  # Every directory from the storage directory's parent down to the log.
  defp log_directories(ctx) do
    shape_dir = shape_dir(ctx)
    prefix_dir = Path.dirname(shape_dir)
    outer_prefix_dir = Path.dirname(prefix_dir)
    stack_dir = Path.dirname(outer_prefix_dir)
    storage_dir = Path.dirname(stack_dir)

    [
      Path.join(shape_dir, "log"),
      shape_dir,
      prefix_dir,
      outer_prefix_dir,
      stack_dir,
      storage_dir,
      Path.dirname(storage_dir)
    ]
  end

  # A latest generation is a log and its chunk index; a compacted one also has a key index.
  defp generation_files(ctx, "compacted." <> _ = suffix),
    do: generation_files(ctx, suffix, ["jsonfile", "chunk", "keyfile"])

  defp generation_files(ctx, suffix), do: generation_files(ctx, suffix, ["jsonfile", "chunk"])

  defp generation_files(ctx, suffix, kinds) do
    for kind <- kinds do
      path = Path.join([shape_dir(ctx), "log", "log.#{suffix}.#{kind}.bin"])
      assert File.exists?(path), "#{path} is missing"
      {:file, path}
    end
  end

  defp name_files(ctx, keys), do: for(key <- keys, do: {:file, metadata_file(ctx, key)})

  # Leaves the log as a compaction to `compacted` and `latest` does when its writer is killed: the
  # new generations' files, and the names it renamed with no sync. `compaction_boundary` is renamed
  # first, so `latest: "latest.0"` is a kill between the two renames.
  defp switch_generation(ctx, compacted: compacted, latest: latest) do
    log_dir = Path.join(shape_dir(ctx), "log")
    copy_generation(log_dir, "latest.0", compacted)
    File.write!(Path.join(log_dir, "log.#{compacted}.keyfile.bin"), "")
    if latest != "latest.0", do: copy_generation(log_dir, "latest.0", latest)

    # A compacted index ends on a complete chunk, where the log and the key index end.
    File.write!(
      Path.join(log_dir, "log.#{compacted}.chunk.bin"),
      PureFileStorage.ChunkIndex.make_full_entry(
        {LogOffset.new(1, 0), LogOffset.new(1, 0)},
        {0, File.stat!(Path.join(log_dir, "log.#{compacted}.jsonfile.bin")).size},
        {0, 0}
      )
    )

    write_metadata_file(ctx, "compaction_boundary", {LogOffset.new(1, 0), compacted})
    if latest != "latest.0", do: write_metadata_file(ctx, "latest_name", latest)
  end

  defp entry_number(entry),
    do: entry |> IO.iodata_to_binary() |> Jason.decode!() |> Map.fetch!("n")

  # The compacted generation holds only txn 1, as a compaction that cut at its boundary leaves it.
  defp keep_compacted_through_first_txn(ctx, compacted) do
    log_dir = Path.join(shape_dir(ctx), "log")
    log = Path.join(log_dir, "log.#{compacted}.jsonfile.bin")
    txn_1_end = entries_end(log, 1)
    truncate_at(log, txn_1_end)

    File.write!(
      Path.join(log_dir, "log.#{compacted}.chunk.bin"),
      PureFileStorage.ChunkIndex.make_full_entry(
        {LogOffset.new(1, 0), LogOffset.new(1, 0)},
        {0, txn_1_end},
        {0, 0}
      )
    )
  end

  defp copy_generation(log_dir, from, to) do
    for kind <- ["jsonfile", "chunk"],
        do:
          File.cp!(
            Path.join(log_dir, "log.#{from}.#{kind}.bin"),
            Path.join(log_dir, "log.#{to}.#{kind}.bin")
          )
  end

  defp metadata_file(ctx, key), do: Path.join(metadata_dir(ctx), "#{key}.bin")

  defp durable_generation(ctx) do
    case File.read(metadata_file(ctx, "durable_generation")) do
      {:ok, bytes} -> :erlang.binary_to_term(bytes)
      {:error, :enoent} -> nil
    end
  end

  # Opens a writer on the stack as it stands, commits txn `n` and kills the writer.
  defp append_then_kill(opts, n, payload) do
    test_pid = self()

    writer_pid =
      spawn(fn ->
        opts
        |> PureFileStorage.init_writer!(@shape)
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(n, 0), "test_key", :insert, payload}],
            &1
          )
        )
        |> then(&PureFileStorage.signal_txn_commit!(n, &1))

        send(test_pid, :written)
        Process.sleep(:infinity)
      end)

    assert_receive :written, @write_timeout
    kill_and_await(writer_pid)
  end

  defp write_metadata_file(ctx, key, value) do
    path = Path.join(metadata_dir(ctx), "#{key}.bin")
    File.write!(path <> ".tmp", :erlang.term_to_binary(value))
    File.rename!(path <> ".tmp", path)
  end

  defp shape_dir(ctx), do: PureFileStorage.shape_data_dir(base_path(ctx), @shape_handle)

  # Every file under the shape's directory, with its content.
  defp shape_files(ctx) do
    for path <- Path.wildcard(Path.join(shape_dir(ctx), "**"), match_dot: true),
        File.regular?(path),
        into: %{},
        do: {path, File.read!(path)}
  end

  defp metadata_dir(ctx), do: Path.join(shape_dir(ctx), "metadata")

  defp boot_dir(ctx), do: Path.join(shape_dir(ctx), "boot")

  defp boot_record_file(ctx), do: Path.join(boot_dir(ctx), "written_boot.bin")

  # A record naming `boot`, which found no marker and carries `high_water`.
  defp write_boot_record(ctx, boot, high_water \\ nil) do
    File.mkdir_p!(boot_dir(ctx))
    File.write!(boot_record_file(ctx), :erlang.term_to_binary({boot, nil, high_water}))
  end

  # A reopen in a later boot renames its record into place and fails to sync it, then the shape
  # reopens in the same boot. Returns the record and the syncs that reopen made, each with the
  # record on disk at the time.
  defp reopen_after_an_unsynced_boot_record(ctx) do
    write_boot_record(ctx, "an earlier boot")
    refuse_to_open(boot_dir(ctx))
    assert_raise File.Error, fn -> reopen(ctx) end
    record = read_term(boot_record_file(ctx))
    assert {boot, _marker, _high_water} = record
    assert boot != "an earlier boot", "the record was not renamed into place"
    Repatch.restore(Electric.Utils, :sync_dir!, 1, mode: :local)
    record_syncs(ctx, fn -> read_term(boot_record_file(ctx)) end)
    reopen(ctx)
    {record, recorded_syncs()}
  end

  # Opens a writer on the stack as it stands, commits txns `committed`, and kills the writer once
  # the append of an uncommitted txn after them has flushed them to the file with no datasync.
  defp write_committed_then_kill(opts, committed) do
    test_pid = self()
    pending = List.last(committed) + 1

    writer_pid =
      spawn(fn ->
        committed
        |> Enum.reduce(PureFileStorage.init_writer!(opts, @shape), fn n, writer ->
          writer
          |> then(
            &PureFileStorage.append_fragment_to_log!(
              [{LogOffset.new(n, 0), "test_key", :insert, ~s|{"n":#{n}}|}],
              &1
            )
          )
          |> then(&PureFileStorage.signal_txn_commit!(n, &1))
        end)
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [
              {LogOffset.new(pending, 0), "test_key", :insert,
               ~s|{"n":#{pending},"b":"#{@flushing_payload}"}|}
            ],
            &1
          )
        )

        send(test_pid, :written)
        Process.sleep(:infinity)
      end)

    assert_receive :written, @write_timeout
    kill_and_await(writer_pid)
  end

  # Leaves a shape whose generation a reopen has made durable, so the next reopen syncs none of its
  # directories on that account, with a written marker on txn 4 past its durable claim on txn 2.
  defp write_past_the_claim_of_a_durable_generation(ctx) do
    opts = start_stack(ctx)
    write_synced_and_written_then_kill(opts)
    opts = reopen(ctx)
    write_committed_then_kill(opts, [3, 4])
    assert LogOffset.new(2, 0) == durable_boundary(ctx)
    assert LogOffset.new(4, 0) == read_term(metadata_file(ctx, "last_written_txn_offset"))
  end

  # Gives the file a new inode with its bytes and its mtime, as a remount that numbers files anew
  # does.
  defp renumber(path) do
    %File.Stat{inode: inode, mtime: mtime} = File.stat!(path, time: :posix)
    File.cp!(path, path <> ".renumbered")
    File.touch!(path <> ".renumbered", mtime)
    File.rename!(path <> ".renumbered", path)
    assert %File.Stat{mtime: ^mtime} = File.stat!(path, time: :posix)
    assert File.stat!(path).inode != inode
  end

  # Txn 1 is committed and reaches the file with no datasync when the append of an uncommitted
  # txn 2 flushes the buffer, so the only claim on disk is the written one.
  defp write_unsynced_then_kill(opts) do
    test_pid = self()

    writer_pid =
      spawn(fn ->
        writer = PureFileStorage.init_writer!(opts, @shape)
        PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
        PureFileStorage.mark_snapshot_as_started(opts)
        PureFileStorage.make_new_snapshot!([], opts)

        writer
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1}|}],
            &1
          )
        )
        |> then(&PureFileStorage.signal_txn_commit!(1, &1))
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(2, 0), "test_key", :insert, ~s|{"n":2,"b":"#{@flushing_payload}"}|}],
            &1
          )
        )

        send(test_pid, :written)
        Process.sleep(:infinity)
      end)

    assert_receive :written, @write_timeout
    assert {:ok, LogOffset.new(1, 0)} == PureFileStorage.fetch_latest_offset(opts)
    kill_and_await(writer_pid)
  end

  # Txn 1 is synced, txn 2 is committed and reaches the file with no datasync when the append of an
  # uncommitted txn 3 flushes the buffer, and readers were served txn 2 before the kill.
  defp write_synced_and_written_then_kill(opts, third_payload \\ @flushing_payload) do
    test_pid = self()

    writer_pid =
      spawn(fn ->
        writer = PureFileStorage.init_writer!(opts, @shape)
        PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
        PureFileStorage.mark_snapshot_as_started(opts)
        PureFileStorage.make_new_snapshot!([], opts)

        writer
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(1, 0), "test_key", :insert, ~s|{"n":1,"b":"#{@flushing_payload}"}|}],
            &1
          )
        )
        |> then(&PureFileStorage.signal_txn_commit!(1, &1))
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(2, 0), "test_key", :insert, ~s|{"n":2}|}],
            &1
          )
        )
        |> then(&PureFileStorage.signal_txn_commit!(2, &1))
        |> then(
          &PureFileStorage.append_fragment_to_log!(
            [{LogOffset.new(3, 0), "test_key", :insert, ~s|{"n":3,"b":"#{third_payload}"}|}],
            &1
          )
        )

        send(test_pid, :written)
        Process.sleep(:infinity)
      end)

    assert_receive :written, @write_timeout
    assert {:ok, LogOffset.new(2, 0)} == PureFileStorage.fetch_latest_offset(opts)
    kill_and_await(writer_pid)
  end

  defp durable_boundary(ctx) do
    ctx
    |> durable_boundary_path()
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  defp durable_boundary_path(ctx) do
    PureFileStorage.shape_data_dir(base_path(ctx), @shape_handle, [
      "metadata",
      "last_persisted_txn_offset.bin"
    ])
  end

  # The byte position where the first `count` log entries end.
  defp entries_end(path, count) do
    log = File.read!(path)

    Enum.reduce(1..count, 0, fn _, position ->
      <<_::binary-size(^position), _offset::128, key_size::32, rest::binary>> = log
      <<_key::binary-size(^key_size), _::16, json_size::64, _::binary>> = rest
      position + 20 + key_size + 10 + json_size
    end)
  end
end
