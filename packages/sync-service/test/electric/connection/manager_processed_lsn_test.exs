defmodule Electric.Connection.ManagerProcessedLsnTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup

  require Repatch

  alias Electric.Connection.Manager
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Replication.TxnJournal
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_lsn_tracker,
    :with_async_deleter,
    :with_pure_file_storage
  ]

  setup %{storage: {PureFileStorage, %{base_path: base_path}}} do
    %{base_path: base_path}
  end

  @shape %Shape{
    root_table: {"public", "items"},
    root_table_id: 1,
    root_pk: ["id"],
    selected_columns: ["id"],
    explicitly_selected_columns: ["id"]
  }

  # On a cold boot after a promotion nothing in memory predates this boot, and the new server's
  # slot may lag what the old one was acked to. What the shape logs keep is the one bound that
  # does not depend on the journal accounting for itself.
  describe "processed_lsn/2" do
    test "has no bound while the journal holds nothing", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 100)
      TxnJournal.open_writer!(ctx.base_path) |> TxnJournal.sync!()
      store_shape_commit(ctx, "shape-a", 300)

      assert {:error, reason} = Manager.processed_lsn(ctx.stack_id, ctx.base_path)
      assert reason =~ "journal holds nothing"
    end

    test "counts shape commits stored while the journal was off", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, {:idle_slot_boundary, 100})
      TxnJournal.sync!(writer, 100)
      store_shape_commit(ctx, "shape-a", 300)

      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 300}
    end

    test "counts shape commits whose journal generation was lost", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(100))
      writer = TxnJournal.sync!(writer, 100)
      writer = TxnJournal.sync!(%{writer | position: 64 * 1024 * 1024}, 100)
      writer = TxnJournal.append_event!(writer, fragment(300))
      writer = TxnJournal.sync!(writer, 300)
      File.rm!(Path.join([ctx.base_path, "txn-journal", "#{writer.gen}.bin"]))
      store_shape_commit(ctx, "shape-a", 300)

      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 300}
    end

    # The shape logs then hold only what the consumers wrote, and the reclaimed coverage stops
    # short of the journal's last transaction: neither bounds what the old primary sent.
    test "has no bound once the journal's directory is gone", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(900))
      TxnJournal.sync!(writer, 900)
      :ok = TxnJournal.record_coverage!(ctx.base_path, reclaimed: 700)
      File.rm_rf!(Path.join(ctx.base_path, "txn-journal"))
      store_shape_commit(ctx, "shape-a", 300)

      assert {:error, reason} = Manager.processed_lsn(ctx.stack_id, ctx.base_path)
      assert reason =~ "no generation"
    end

    # A run with the journal off deletes the last generation once the consumers are durable past
    # it, as the collector does, coverage first, and may go on taking transactions no journal sees.
    test "has no bound once a run with the journal off reclaimed its last generation", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(900))
      TxnJournal.sync!(writer, 900)
      :ok = TxnJournal.record_coverage!(ctx.base_path, reclaimed: 900)
      :ok = TxnJournal.delete_generations!(ctx.base_path, [writer.gen])
      store_shape_commit(ctx, "shape-a", 300)

      assert {:error, reason} = Manager.processed_lsn(ctx.stack_id, ctx.base_path)
      assert reason =~ "no generation"
    end

    # The journal read its bound from the store root, so a root the shape scan cannot list is one a
    # remount took away for that listing: back by the look after it, it serves the shape again.
    test "has no bound once the shape scan cannot list the store root", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(100))
      TxnJournal.sync!(writer, 100)
      store_shape_commit(ctx, "shape-a", 300)
      base_path = ctx.base_path

      Repatch.patch(File, :ls, fn
        ^base_path ->
          File.rename!(base_path, base_path <> ".away")
          listing = Repatch.real(File.ls(base_path))
          File.rename!(base_path <> ".away", base_path)
          listing

        path ->
          Repatch.real(File.ls(path))
      end)

      assert {:error, reason} = Manager.processed_lsn(ctx.stack_id, ctx.base_path)
      assert reason =~ "unreadable"
    end

    # A remount that takes the store root away can leave the directory under its mount point, empty
    # say, which lists without error and holds none of the journal's entries the bound came from.
    test "has no bound once the shape scan lists the store root without the journal", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(100))
      TxnJournal.sync!(writer, 100)
      store_shape_commit(ctx, "shape-a", 300)
      base_path = ctx.base_path

      Repatch.patch(File, :ls, fn
        ^base_path ->
          File.rename!(base_path, base_path <> ".away")
          File.mkdir!(base_path)
          listing = Repatch.real(File.ls(base_path))
          File.rmdir!(base_path)
          File.rename!(base_path <> ".away", base_path)
          listing

        path ->
          Repatch.real(File.ls(path))
      end)

      assert {:error, reason} = Manager.processed_lsn(ctx.stack_id, ctx.base_path)
      assert reason =~ "lists none of txn-journal"
    end

    test "counts shape commits past a journal cut at a frame boundary", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(100))
      writer = TxnJournal.sync!(writer, 100)
      writer = TxnJournal.append_event!(writer, fragment(300))
      writer = TxnJournal.sync!(writer, 300)
      path = Path.join([ctx.base_path, "txn-journal", "#{writer.gen}.bin"])
      File.write!(path, binary_part(File.read!(path), 0, frame_size(fragment(100))))
      store_shape_commit(ctx, "shape-a", 300)

      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 300}
    end

    test "counts shape commits behind an unreadable journal frame", ctx do
      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 50)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(100))
      writer = TxnJournal.sync!(writer, 100)
      writer = TxnJournal.append_event!(writer, fragment(200))
      writer = TxnJournal.sync!(writer, 200)
      writer = TxnJournal.append_event!(writer, fragment(300))
      writer = TxnJournal.sync!(writer, 300)
      path = Path.join([ctx.base_path, "txn-journal", "#{writer.gen}.bin"])
      contents = File.read!(path)
      at = frame_size(fragment(100)) + 10

      corrupted =
        binary_part(contents, 0, at) <>
          <<Bitwise.bxor(:binary.at(contents, at), 255)>> <>
          binary_part(contents, at + 1, byte_size(contents) - at - 1)

      File.write!(path, corrupted)
      store_shape_commit(ctx, "shape-a", 300)

      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 300}
    end

    test "is the highest of the shape logs, the journal and the positions held in memory",
         ctx do
      store_shape_commit(ctx, "shape-a", 300)
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(500))
      TxnJournal.sync!(writer, 500)

      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 200)
      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 500}

      Electric.LsnTracker.set_last_processed_lsn(ctx.stack_id, 900)
      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 900}
    end

    # A keepalive fanned out with no journal boundary can be the only position past the new
    # server's switchpoint. A refresh that found a purge owed asked for a replay from the slot,
    # which dropped it before the check.
    test "counts the broadcast watermark a replay from the slot dropped", ctx do
      writer = TxnJournal.open_writer!(ctx.base_path)
      writer = TxnJournal.append_event!(writer, fragment(500))
      TxnJournal.sync!(writer, 500)
      :ok = Electric.LsnTracker.set_slot_flushed_lsn(ctx.stack_id, 200)
      :ok = Electric.LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 1000)

      :ok = Electric.LsnTracker.request_replay_from_slot(ctx.stack_id)
      :ok = Electric.LsnTracker.apply_replay_from_slot(ctx.stack_id)

      assert Manager.processed_lsn(ctx.stack_id, ctx.base_path) == {:ok, 999}
    end

    test "has no bound without a journal", %{stack_id: stack_id} do
      Electric.LsnTracker.set_last_processed_lsn(stack_id, 100)

      assert {:error, reason} = Manager.processed_lsn(stack_id, nil)
      assert reason =~ "ELECTRIC_ACK_AFTER_JOURNAL"
    end
  end

  defp store_shape_commit(%{storage: {PureFileStorage, storage_opts}}, handle, tx_offset) do
    opts = PureFileStorage.for_shape(handle, storage_opts)
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    [{LogOffset.new(tx_offset, 0), "key", :insert, ~S|{"id":"1"}|}]
    |> PureFileStorage.append_to_log!(writer)
    |> PureFileStorage.terminate()
  end

  defp frame_size(event), do: byte_size(:erlang.term_to_binary(event)) + 8

  defp fragment(tx_offset) do
    %TransactionFragment{
      xid: tx_offset,
      lsn: Electric.Postgres.Lsn.from_integer(tx_offset),
      last_log_offset: LogOffset.new(tx_offset, 0),
      has_begin?: true,
      commit: %Changes.Commit{},
      changes: [
        %Changes.NewRecord{
          relation: {"public", "test_table"},
          record: %{"id" => "#{tx_offset}"},
          log_offset: LogOffset.new(tx_offset, 0)
        }
      ],
      change_count: 1
    }
  end
end
