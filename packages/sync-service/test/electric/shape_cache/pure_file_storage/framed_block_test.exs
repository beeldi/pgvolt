defmodule Electric.ShapeCache.PureFileStorage.FramedBlockTest do
  # Write-path parity battery for framed class blocks: a shape appending a FramedBlock must
  # produce byte-identical on-disk state (log file AND chunk index) and identical served
  # reads to a shape appending the same items one at a time. Every test compares a "plain"
  # arm against a "blocked" arm on separate shape handles under the same stack.
  use ExUnit.Case, async: true

  import Support.ComponentSetup

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  @shape %Shape{
    root_table: {"public", "items"},
    root_table_id: 1,
    root_pk: ["id"],
    selected_columns: ["id"],
    explicitly_selected_columns: ["id"]
  }

  setup [:with_stack_id_from_test, :with_async_deleter]

  setup ctx do
    base_opts =
      PureFileStorage.shared_opts(
        stack_id: ctx.stack_id,
        storage_dir: ctx.tmp_dir,
        chunk_bytes_threshold: ctx[:chunk_size] || 10 * 1024 * 1024,
        flush_period: ctx[:flush_period] || 1000
      )

    start_link_supervised!(Storage.stack_child_spec({PureFileStorage, base_opts}))

    %{base_opts: base_opts}
  end

  defp start_shape(ctx, handle) do
    opts = PureFileStorage.for_shape(handle, ctx.base_opts)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)
    {opts, writer}
  end

  defp inline_line(i, json) do
    {LogOffset.new(10 + i, 0), "k#{i}", :insert, json}
  end

  defp lines(range), do: Enum.map(range, &inline_line(&1, ~s|{"payload":#{&1}}|))

  # The blocked arm frames `block_range` once and keeps the rest per-entry, mirroring the
  # assembler's output shape (a block never carries the run's trailing change).
  defp arms(all_range, block_range) do
    plain = lines(all_range)

    blocked =
      Enum.flat_map(all_range, fn
        i when i == block_range.first ->
          [LogFile.build_framed_block(lines(block_range))]

        i ->
          if i in block_range, do: [], else: [inline_line(i, ~s|{"payload":#{i}}|)]
      end)

    {plain, blocked}
  end

  defp assert_engaged(blocked_lines) do
    assert Enum.any?(blocked_lines, &match?(%Electric.Replication.Changes.FramedBlock{}, &1)),
           "blocked arm carries no FramedBlock — the parity check would be vacuous"
  end

  defp on_disk(ctx, handle, kind) do
    [path] =
      Path.wildcard(Path.join(ctx.base_opts.base_path, "**/log.*.#{kind}.bin"))
      |> Enum.filter(&String.contains?(&1, handle))

    File.read!(path)
  end

  defp served(opts, up_to \\ 100) do
    PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(up_to, 0), opts)
    |> Enum.to_list()
  end

  test "fast path: one block append is byte-identical to per-entry appends", ctx do
    {plain_lines, blocked_lines} = arms(1..8, 2..7)
    assert_engaged(blocked_lines)

    {plain_opts, plain_writer} = start_shape(ctx, "plain-fast")
    {blocked_opts, blocked_writer} = start_shape(ctx, "block-fast")

    plain_writer = PureFileStorage.append_to_log!(plain_lines, plain_writer)
    blocked_writer = PureFileStorage.append_to_log!(blocked_lines, blocked_writer)

    assert served(plain_opts) == served(blocked_opts)

    PureFileStorage.terminate(plain_writer)
    PureFileStorage.terminate(blocked_writer)

    assert on_disk(ctx, "plain-fast", "jsonfile") == on_disk(ctx, "block-fast", "jsonfile")
    assert on_disk(ctx, "plain-fast", "chunk") == on_disk(ctx, "block-fast", "chunk")

    assert PureFileStorage.fetch_latest_offset(plain_opts) ==
             PureFileStorage.fetch_latest_offset(blocked_opts)
  end

  test "arena-ref blocks are byte-identical to per-entry ref appends", ctx do
    arena = PayloadArena.open_writer!(ctx.base_opts.base_path)

    {ref_lines, arena} =
      Enum.map_reduce(1..8, arena, fn i, arena ->
        json = ~s|{"payload":#{i}}|
        {arena, ref} = PayloadArena.append!(arena, "k#{i}", json)
        {{LogOffset.new(10 + i, 0), "k#{i}", :insert, json, ref}, arena}
      end)

    PayloadArena.sync!(arena)

    {head, rest} = Enum.split(ref_lines, 1)
    {run, tail} = Enum.split(rest, 6)
    blocked_lines = head ++ [LogFile.build_framed_block(run)] ++ tail
    assert_engaged(blocked_lines)

    {plain_opts, plain_writer} = start_shape(ctx, "plain-refs")
    {blocked_opts, blocked_writer} = start_shape(ctx, "block-refs")

    plain_writer = PureFileStorage.append_to_log!(ref_lines, plain_writer)
    blocked_writer = PureFileStorage.append_to_log!(blocked_lines, blocked_writer)

    assert served(plain_opts) == served(blocked_opts)

    PureFileStorage.terminate(plain_writer)
    PureFileStorage.terminate(blocked_writer)

    assert on_disk(ctx, "plain-refs", "jsonfile") == on_disk(ctx, "block-refs", "jsonfile")
    assert on_disk(ctx, "plain-refs", "chunk") == on_disk(ctx, "block-refs", "chunk")
  end

  @tag chunk_size: 40
  test "a chunk boundary inside the block keeps chunk-index parity (explode path)", ctx do
    # ~16 bytes of payload per entry against a 40-byte chunk threshold: boundaries land
    # inside the framed run, forcing the per-entry fallback.
    {plain_lines, blocked_lines} = arms(1..9, 2..8)
    assert_engaged(blocked_lines)

    {plain_opts, plain_writer} = start_shape(ctx, "plain-chunk")
    {blocked_opts, blocked_writer} = start_shape(ctx, "block-chunk")

    plain_writer = PureFileStorage.append_to_log!(plain_lines, plain_writer)
    blocked_writer = PureFileStorage.append_to_log!(blocked_lines, blocked_writer)

    assert served(plain_opts) == served(blocked_opts)

    PureFileStorage.terminate(plain_writer)
    PureFileStorage.terminate(blocked_writer)

    assert on_disk(ctx, "plain-chunk", "jsonfile") == on_disk(ctx, "block-chunk", "jsonfile")
    assert on_disk(ctx, "plain-chunk", "chunk") == on_disk(ctx, "block-chunk", "chunk")
  end

  test "a replay straddling the block appends only the unpersisted entries", ctx do
    {_plain, blocked_head} = arms(1..4, 1..3)

    {opts, writer} = start_shape(ctx, "replay-shape")
    writer = PureFileStorage.append_to_log!(blocked_head, writer)

    # Replayed fragment: a block spanning entries 3..8 — 3 and 4 are already committed.
    replay_block = LogFile.build_framed_block(lines(3..8))
    writer = PureFileStorage.append_to_log!([replay_block], writer)
    PureFileStorage.terminate(writer)

    assert served(opts) == Enum.map(1..8, fn i -> ~s|{"payload":#{i}}| end)
  end

  test "an entirely replayed block is skipped", ctx do
    {opts, writer} = start_shape(ctx, "skip-shape")
    writer = PureFileStorage.append_to_log!(lines(1..6), writer)

    replay_block = LogFile.build_framed_block(lines(2..5))
    writer = PureFileStorage.append_to_log!([replay_block], writer)
    PureFileStorage.terminate(writer)

    assert served(opts) == Enum.map(1..6, fn i -> ~s|{"payload":#{i}}| end)
  end

  test "block lines are served from the live tail before any flush", ctx do
    {opts, writer} = start_shape(ctx, "tail-shape")

    block = LogFile.build_framed_block(lines(1..5))
    _writer = PureFileStorage.append_to_log!([block, inline_line(6, ~s|{"payload":6}|)], writer)

    assert served(opts) == Enum.map(1..6, fn i -> ~s|{"payload":#{i}}| end)
  end
end
