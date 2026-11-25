defmodule Electric.Replication.JournalTornTailRestartTest do
  # A crash in the middle of a journal write leaves the newest generation ending in a partial
  # frame: bytes the writer issued but never datasynced, so the slot was never acked past them and
  # Postgres resends them. Every read accepts that tail while the generation is the newest on disk.
  # The next boot's writer opens a fresh generation, and from then on the torn one is an older
  # generation, which every read refuses as corruption.

  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  alias Electric.Replication.LogOffset
  alias Electric.Replication.TxnJournal
  alias Electric.ShapeCache
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir
  @moduletag replication_opts_overrides: [slot_temporary?: false]

  setup [:with_unique_db, :with_basic_tables]

  setup do
    previous = Application.get_env(:electric, :ack_after_journal)
    Application.put_env(:electric, :ack_after_journal, true)
    on_exit(fn -> Application.put_env(:electric, :ack_after_journal, previous) end)
  end

  setup :with_complete_stack

  setup ctx do
    {Electric.ShapeCache.PureFileStorage, %{base_path: journal_base}} =
      Electric.StackConfig.lookup(ctx.stack_id, Electric.ShapeCache.Storage)

    shape = Shape.new!("serial_ids", inspector: ctx.inspector)
    {handle, _offset} = ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id)
    :started = ShapeCache.await_snapshot_start(handle, ctx.stack_id)

    insert!(ctx, 1)
    assert wait_for_ids(ctx, handle, [1])

    # The shape's log takes a transaction once its snapshot has started, before the snapshot is
    # synced and marked complete, and a boot drops a shape whose mark the crash left unwritten.
    # The stack's write buffer commits the mark on its own schedule, so the poll reads the catalog
    # alone: a flush asked for here waits for a held connection longer than its call may.
    assert Support.TestUtils.wait_until(
             fn ->
               try do
                 ShapeDb.snapshot_complete?(ctx.stack_id, handle) == true
               catch
                 # A read that waits on a held connection is tried again.
                 :exit, {:timeout, _} -> false
               end
             end,
             15_000
           ),
           "the shape's snapshot must be marked complete before the crash"

    %{handle: handle, journal_base: journal_base}
  end

  test "the first transaction journaled after a boot over a torn tail reaches the shape", ctx do
    # Arrange
    ctx = crash_restart(ctx, &tear_newest_generation!/1)
    assert :ok == wait_until_active(ctx), diagnostics(ctx)

    # Act
    insert!(ctx, 2)

    # Assert
    assert wait_for_ids(ctx, ctx.handle, [1, 2]), diagnostics(ctx)
  end

  test "the restart after that first transaction boots and keeps streaming", ctx do
    # Arrange
    ctx = crash_restart(ctx, &tear_newest_generation!/1)
    assert :ok == wait_until_active(ctx), diagnostics(ctx)
    assert_received {:torn_gen, torn_gen}
    insert!(ctx, 2)
    assert wait_for_ids(ctx, ctx.handle, [1, 2]), diagnostics(ctx)
    assert Enum.max(TxnJournal.list_generations(ctx.journal_base)) > torn_gen

    # Act
    ctx = crash_restart(ctx, fn _base -> :ok end)

    # Assert
    assert :ok == wait_until_active(ctx), diagnostics(ctx)
    insert!(ctx, 3)
    assert wait_for_ids(ctx, ctx.handle, [1, 2, 3]), diagnostics(ctx)
  end

  test "the same two restarts over an intact journal keep streaming", ctx do
    # Arrange
    ctx = crash_restart(ctx, fn _base -> :ok end)
    assert :ok == wait_until_active(ctx), diagnostics(ctx)
    insert!(ctx, 2)
    assert wait_for_ids(ctx, ctx.handle, [1, 2]), diagnostics(ctx)

    # Act
    ctx = crash_restart(ctx, fn _base -> :ok end)

    # Assert
    assert :ok == wait_until_active(ctx), diagnostics(ctx)
    insert!(ctx, 3)
    assert wait_for_ids(ctx, ctx.handle, [1, 2, 3]), diagnostics(ctx)
  end

  defp crash_restart(ctx, on_disk) do
    sup_id = brutally_stop_complete_stack(ctx)
    on_disk.(ctx.journal_base)

    stack_supervisor =
      start_stack_supervisor!(
        ctx,
        ctx.stack_id,
        ctx.persistent_kv,
        ctx.storage,
        ctx.stack_events_registry,
        ctx.publication_name,
        id: sup_id,
        await_ready?: false
      )

    Map.merge(ctx, %{stack_supervisor: stack_supervisor, stack_supervisor_id: sup_id})
  end

  # What a short write leaves: the header of the next frame and the first half of its payload.
  # That frame belongs to a transaction whose commit was never datasynced, so never acked.
  defp tear_newest_generation!(base) do
    newest = base |> TxnJournal.list_generations() |> List.last()
    bin = :erlang.term_to_binary({:never_acked, String.duplicate("x", 4096)})
    frame = <<byte_size(bin)::32, :erlang.crc32(bin)::32, bin::binary>>
    path = Path.join([base, "txn-journal", "#{newest}.bin"])
    File.write!(path, binary_part(frame, 0, div(byte_size(frame), 2)), [:append])
    send(self(), {:torn_gen, newest})
  end

  defp wait_until_active(ctx),
    do: Electric.StatusMonitor.wait_until_active(ctx.stack_id, timeout: 15_000)

  defp diagnostics(ctx) do
    generations =
      Enum.map(TxnJournal.list_generations(ctx.journal_base), fn gen ->
        {gen, File.stat!(Path.join([ctx.journal_base, "txn-journal", "#{gen}.bin"])).size}
      end)

    errors = drain_stack_errors([])

    "journal generations (id, bytes): #{inspect(generations)}\n" <>
      "stack errors since the restart (#{length(errors)}):\n" <> Enum.join(errors, "\n")
  end

  defp drain_stack_errors(acc) do
    receive do
      {:stack_status, _ref, {kind, %{message: message}}}
      when kind in [:connection_error, :config_error] ->
        drain_stack_errors([String.slice(message, 0, 400) | acc])

      {:stack_status, _ref, _event} ->
        drain_stack_errors(acc)
    after
      0 -> acc |> Enum.reverse() |> Enum.uniq()
    end
  end

  defp insert!(ctx, id),
    do: Postgrex.query!(ctx.db_conn, "INSERT INTO serial_ids (id) VALUES ($1)", [id])

  defp wait_for_ids(ctx, handle, ids) do
    expected = Enum.map(ids, &to_string/1)
    Support.TestUtils.wait_until(fn -> stored_ids(ctx, handle) == expected end, 15_000)
  end

  defp stored_ids(ctx, handle) do
    storage = Storage.for_shape(handle, Storage.shared_opts(ctx.storage))

    LogOffset.last_before_real_offsets()
    |> Storage.get_log_stream(storage)
    |> Enum.map(&get_in(Jason.decode!(&1), ["value", "id"]))
  end
end
