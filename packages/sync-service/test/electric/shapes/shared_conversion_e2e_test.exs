defmodule Electric.Shapes.SharedConversionE2eTest do
  @moduledoc """
  End-to-end proof that a change prepared once for a fan-out class lands correctly in every
  member's storage. The router-level test proves the prepared bytes are right; this one proves
  the consumers actually write them — which is exactly what the destroyed-shapes bug faked.

  One scenario exercises every combination in a single run:

    * two shapes in the same fan-out class, with partially overlapping membership, so the same
      transaction delivers shared `PreparedChange`s (rows both match) and plain changes (rows
      only one matches) to the same consumer, interleaved;
    * a run of identical-membership rows long enough to be framed once for the whole class;
    * one single-fragment transaction (the `build_and_handle_txn` short-circuit that used to
      destroy shapes) and one multi-fragment transaction (the write path).
  """
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TxnJournal
  alias Electric.ShapeCache
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  import Support.ComponentSetup
  import Support.TestUtils, only: [patch_shape_status: 1, txn_fragment: 4]

  @columns [
    %{name: "id", type: "text", pk_position: 0},
    %{name: "firmId", type: "text"},
    %{name: "divisionId", type: "text"},
    %{name: "note", type: "text"}
  ]

  @inspector StubInspector.new(tables: ["employees"], columns: @columns)

  # Repatch's recompile list is owned by test_helper.exs and must stay complete; setting
  # Repatch up from here would make that process-global choice from one file's partial view.
  setup_all do
    if :ets.whereis(:repatch_module_states) == :undefined do
      raise "Repatch is not set up: this suite needs the test_helper prewarm (unset SKIP_REPATCH_PREWARM)"
    end

    :ok
  end

  setup :with_stack_id_from_test

  setup _ctx do
    dir = Path.join(System.tmp_dir!(), "shared-e2e-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{tmp_dir: dir}
  end

  setup [
    :with_registry,
    :with_pure_file_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_persistent_kv,
    :with_status_monitor,
    :with_dynamic_consumer_supervisor,
    :with_noop_publication_manager,
    :with_shape_cleaner
  ]

  setup ctx do
    Electric.StackConfig.put(ctx.stack_id, ShapeCache.Storage, ctx.storage)
    Electric.StackConfig.put(ctx.stack_id, :inspector, @inspector)

    journaled = if collector_flags(ctx)[:ack_after_journal], do: seed_journal!(ctx)

    start_link_supervised!({
      ShapeLogCollector.Supervisor,
      [stack_id: ctx.stack_id, persistent_kv: ctx.persistent_kv, inspector: @inspector] ++
        Map.get(ctx, :collector_flags, [])
    })

    ShapeLogCollector.mark_as_ready(ctx.stack_id)
    %{journaled_fragment: journaled}
  end

  defp collector_flags(ctx), do: Map.get(ctx, :collector_flags, [])

  # The journal is written by the replication client, before the collector ever sees the
  # transaction, so only a journal already on disk at boot makes the replay path run.
  defp seed_journal!(ctx) do
    {Electric.ShapeCache.PureFileStorage, %{base_path: base_path}} = ctx.storage
    lsn = Lsn.from_integer(30)

    fragment =
      txn_fragment(500, lsn, [change(999, "B0-0", lsn, 0)], has_begin?: true, has_commit?: true)

    base_path
    |> TxnJournal.open_writer!()
    |> TxnJournal.append_event!(fragment)
    |> TxnJournal.sync!(fragment.last_log_offset.tx_offset)

    fragment
  end

  defp row(i, division) do
    %{"id" => "eq-#{i}", "firmId" => "F0", "divisionId" => division, "note" => "n#{i}"}
  end

  defp change(i, division, lsn, op) do
    %Changes.NewRecord{
      relation: {"public", "employees"},
      record: row(i, division),
      log_offset: LogOffset.new(lsn, op),
      key: ~s|"public"."employees"/"eq-#{i}"|
    }
  end

  defp start_consumer!(ctx, handle, shape) do
    {:ok, pid} =
      Shapes.DynamicConsumerSupervisor.start_shape_consumer(
        ctx.stack_id,
        %{shape_handle: handle, stack_id: ctx.stack_id}
      )

    Shapes.Consumer.initialize_shape(pid, shape, %{action: :create})
    :started = Shapes.Consumer.await_snapshot_start(ctx.stack_id, handle, 60_000)
    pid
  end

  defp log_lines(ctx, handle) do
    Storage.get_log_stream(LogOffset.first(), Storage.for_shape(handle, ctx.storage))
    |> Enum.map(&IO.iodata_to_binary/1)
  end

  # The baseline the flagged run is compared against: every flag pinned off rather than left
  # to the environment, so this arm keeps describing the default path wherever it runs.
  @tag collector_flags: [
         router_shards: 1,
         shared_conversion: false,
         shared_payload_arena: false,
         worker_prepare: false,
         shared_fragment_blocks: false,
         ack_after_journal: false
       ]
  test "a shared change lands byte-correct in storage for every class member", ctx do
    run_shared_conversion_scenario(ctx)
  end

  # Every assertion in the scenario is content-exact (line counts, cross-shape byte
  # identity, ids, txids, last-markers), so passing under the full flag stack proves the
  # flagged pipeline — sharded routing with scatter-ahead, worker prepare, shared payload
  # arena, framed blocks, ack-after-journal — writes logs byte-identical to the default
  # path's. Each flag is separately pinned to a condition the scenario would otherwise
  # satisfy vacuously: co-resident shards, a framed block per transaction, and a journal
  # seeded before boot.
  @tag collector_flags: [
         router_shards: 2,
         shared_conversion: true,
         shared_payload_arena: true,
         worker_prepare: true,
         shared_fragment_blocks: true,
         ack_after_journal: true
       ]
  test "the full flag stack lands byte-identical logs", ctx do
    run_shared_conversion_scenario(ctx)
  end

  # Each of these only decides how a shared conversion is packaged, framed or parallelized.
  # Set without the conversion itself they have nothing to act on, and the collector turns
  # them off rather than half-applying them -- so this run must be the default path exactly,
  # not a third behaviour reachable only by misconfiguration.
  @tag collector_flags: [
         router_shards: 2,
         shared_conversion: false,
         shared_payload_arena: true,
         worker_prepare: true,
         shared_fragment_blocks: true
       ]
  test "the packaging flags are inert without the conversion they package", ctx do
    run_shared_conversion_scenario(ctx)
  end

  defp run_shared_conversion_scenario(ctx) do
    # Arrange
    # Same fan-out class (same table, both filtered, all columns, same pk/replica/mode),
    # different perimeters: a1 sees divisions B0-0 and B0-1, a2 sees B0-0 and B0-9.
    a1 =
      Shape.new!("public.employees",
        where: ~s|"firmId" = 'F0' AND "divisionId" IN ('B0-0','B0-1')|,
        inspector: @inspector
      )

    a2 =
      Shape.new!("public.employees",
        where: ~s|"firmId" = 'F0' AND "divisionId" IN ('B0-0','B0-9')|,
        inspector: @inspector
      )

    assert Shape.fanout_class(a1) == Shape.fanout_class(a2),
           "shapes fell out of one fan-out class; this scenario no longer exercises sharing"

    # Same placement function the router workers slice with: a class split across shards
    # holds a single member per worker, shares nothing, and still passes every assertion
    # below.
    shards = collector_flags(ctx)[:router_shards] || 1

    assert :erlang.phash2("shape-a1", shards) == :erlang.phash2("shape-a2", shards),
           "the fan-out class is split across router shards; nothing is shared on the worker path"

    shape_map = %{"shape-a1" => a1, "shape-a2" => a2}
    patch_shape_status(fetch_shape_by_handle: fn _, h -> Map.fetch(shape_map, h) end)

    test_pid = self()

    record_blocks = fn lines ->
      Enum.each(lines, fn
        %Changes.FramedBlock{count: count} -> send(test_pid, {:framed_block, self(), count})
        _line -> :ok
      end)
    end

    # Complete transactions and streamed fragments reach storage through different entry
    # points. The writer state is opaque, so blocks are attributed by the appending consumer.
    Support.TestUtils.patch_storage(
      append_to_log!: fn lines, writer ->
        record_blocks.(lines)
        Repatch.real(Electric.ShapeCache.Storage, :append_to_log!, [lines, writer])
      end,
      append_fragment_to_log!: fn lines, writer ->
        record_blocks.(lines)
        Repatch.real(Electric.ShapeCache.Storage, :append_fragment_to_log!, [lines, writer])
      end
    )

    Support.TestUtils.patch_snapshotter(fn consumer, handle, _shape, %{snapshot_fun: snap} ->
      GenServer.cast(consumer, {:pg_snapshot_known, handle, {100, 101, []}})
      snap.([])
      GenServer.cast(consumer, {:snapshot_started, handle})
    end)

    Support.TestUtils.activate_mocks_for_descendant_procs(Shapes.Consumer)
    Support.TestUtils.activate_mocks_for_descendant_procs(ShapeCache.ShapeCleaner)

    handles_by_consumer =
      Map.new(shape_map, fn {handle, shape} -> {start_consumer!(ctx, handle, shape), handle} end)

    # Rows 2 and 5 are only in a1's perimeter, so their changes reach a1 as plain structs
    # (a class of one is never shared) while the surrounding rows arrive prepared. Rows 6..11
    # are one uninterrupted run of identical membership, long enough to be framed once for
    # the class instead of appended per shape.
    division = fn i -> if i in [2, 5], do: "B0-1", else: "B0-0" end

    # Act
    # A transaction the previous boot journaled and acked: replayed into the fan-out before
    # this collector was marked ready, so Postgres re-sending it must be dropped.
    Enum.each(List.wrap(ctx.journaled_fragment), fn fragment ->
      :ok = ShapeLogCollector.handle_event(fragment, ctx.stack_id)
    end)

    # Txn 1: one fragment carrying begin and commit — the short-circuit path.
    lsn1 = Lsn.from_integer(1_000)
    txn1_changes = Enum.map(0..11, fn i -> change(i, division.(i), lsn1, i) end)

    :ok =
      ShapeLogCollector.handle_event(
        txn_fragment(501, lsn1, txn1_changes, has_begin?: true, has_commit?: true),
        ctx.stack_id
      )

    # Txn 2: the same shape of data split across two fragments — the write path. The split
    # keeps the whole shared run inside the second fragment.
    lsn2 = Lsn.from_integer(2_000)
    txn2_changes = Enum.map(0..11, fn i -> change(100 + i, division.(i), lsn2, i) end)
    {frag_a, frag_b} = Enum.split(txn2_changes, 6)

    :ok =
      ShapeLogCollector.handle_event(
        txn_fragment(502, lsn2, frag_a, has_begin?: true, has_commit?: false),
        ctx.stack_id
      )

    :ok =
      ShapeLogCollector.handle_event(
        txn_fragment(502, lsn2, frag_b, has_begin?: false, has_commit?: true),
        ctx.stack_id
      )

    # Assert
    assert Electric.Shapes.ConsumerRegistry.active_consumer_count(ctx.stack_id) == 2,
           "a consumer died during publication — the destroyed-shapes bug is back"

    a1_lines = log_lines(ctx, "shape-a1")
    a2_lines = log_lines(ctx, "shape-a2")

    assert length(a1_lines) == 24
    assert length(a2_lines) == 20

    # Every line a2 holds must be byte-identical to a1's line for the same row: shared rows
    # were encoded once for the whole class, and each txn's final row (11 and 111, in both
    # perimeters) is re-encoded per shape for its last-marker yet must come out identical.
    a1_by_key = Map.new(a1_lines, fn line -> {Jason.decode!(line)["key"], line} end)

    Enum.each(a2_lines, fn line ->
      key = Jason.decode!(line)["key"]
      assert Map.fetch!(a1_by_key, key) == line
    end)

    decoded = Enum.map(a1_lines, &Jason.decode!/1)

    # eq-999 is the journaled row: its presence would mean the boot replay never consumed
    # the journal and the collector accepted the transaction a second time.
    assert Enum.map(decoded, & &1["value"]["id"]) ==
             Enum.map(Enum.to_list(0..11) ++ Enum.to_list(100..111), &"eq-#{&1}")

    Enum.each(decoded, fn item ->
      %{"value" => %{"id" => "eq-" <> i, "divisionId" => b, "firmId" => "F0"}} = item
      assert b == division.(rem(String.to_integer(i), 100))
      assert item["headers"]["operation"] == "insert"
    end)

    assert Enum.map(decoded, & &1["headers"]["txids"]) ==
             List.duplicate([501], 12) ++ List.duplicate([502], 12)

    # Exactly the final change of each transaction carries the last-marker, in both logs —
    # the marker is per shape, so a prepared encoding must never leak into it.
    assert decoded
           |> Enum.map(&(&1["headers"]["last"] == true))
           |> Enum.with_index()
           |> Enum.filter(&elem(&1, 0))
           |> Enum.map(&elem(&1, 1)) == [11, 23]

    a2_decoded = Enum.map(a2_lines, &Jason.decode!/1)

    assert a2_decoded
           |> Enum.map(&(&1["headers"]["last"] == true))
           |> Enum.with_index()
           |> Enum.filter(&elem(&1, 0))
           |> Enum.map(&elem(&1, 1)) == [9, 19]

    expected_blocks = expected_framed_blocks(ctx)

    assert framed_block_appends(handles_by_consumer, length(expected_blocks)) == expected_blocks

    refute_received {:framed_block, _consumer, _count}
  end

  # Each transaction's run of six identical-membership changes is framed once for the class,
  # carrying the first five (the trailing change always rides individually). Both members
  # append that one block, so a flag-off run produces none at all.
  # Framing packages a shared conversion, so it has nothing to frame without one -- which is
  # what makes the follower flags inert rather than half-applied when set on their own.
  defp expected_framed_blocks(ctx) do
    flags = collector_flags(ctx)

    if flags[:shared_fragment_blocks] && flags[:shared_conversion],
      do: [{"shape-a1", 5}, {"shape-a1", 5}, {"shape-a2", 5}, {"shape-a2", 5}],
      else: []
  end

  defp framed_block_appends(handles_by_consumer, expected_count) do
    Enum.map(1..expected_count//1, fn _ ->
      assert_receive {:framed_block, consumer, count}
      {Map.fetch!(handles_by_consumer, consumer), count}
    end)
    |> Enum.sort()
  end
end
