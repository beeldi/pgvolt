defmodule Electric.ShapeCache.PureFileStorage.RollbackCompatibilityTest do
  @moduledoc """
  A payload-arena ref lives in the payload slot of an otherwise ordinary log entry and carries
  its own size, so a reader that predates the arena finds a well-formed entry and hands the raw
  ref bytes to a client as JSON. Nothing about the entry is malformed enough to be noticed.

  The only thing standing between that reader and a client is the version this storage claims on
  disk, because a version it does not recognise makes it discard the shape and rebuild it. So the
  claim is what these tests assert, on the file itself: it is the whole interop surface with a
  release that cannot be changed retroactively.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir
  setup [:with_stack_id_from_test, :with_async_deleter]

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

  # The version a release without the payload arena writes and accepts.
  @pre_arena_version 1

  # The version the release that introduced refs writes and accepts. It matches a ref as an
  # exact-width binary, so a checksum-bearing one crashes it rather than merely misreading.
  @narrow_ref_version 2

  defp start_stack(ctx) do
    base_opts =
      PureFileStorage.shared_opts(
        stack_id: ctx.stack_id,
        storage_dir: ctx.tmp_dir,
        chunk_bytes_threshold: 10 * 1024 * 1024,
        flush_period: 600_000
      )

    start_supervised!(Storage.stack_child_spec({PureFileStorage, base_opts}))
    PureFileStorage.for_shape(@shape_handle, base_opts)
  end

  defp base_path(ctx), do: Storage.opt_for_stack(ctx.stack_id, :base_path)

  defp metadata_file(ctx, filename) do
    PureFileStorage.shape_data_dir(base_path(ctx), @shape_handle, ["metadata", filename])
  end

  defp claimed_version(ctx) do
    ctx |> metadata_file("version.bin") |> File.read!() |> :erlang.binary_to_term()
  end

  defp seed_arena(ctx) do
    arena = PayloadArena.open_writer!(base_path(ctx))
    {arena, _ref} = PayloadArena.append!(arena, "arena-key", ~s|{"arena":1}|)
    PayloadArena.sync!(arena)
  end

  defp write_one_txn(opts) do
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
  end

  test "a log with no arena bytes stays readable by a release that predates the arena", ctx do
    # Arrange
    opts = start_stack(ctx)

    # Act
    write_one_txn(opts)

    # Assert
    assert @pre_arena_version == claimed_version(ctx),
           "claiming a newer version with no arena bytes would wipe every shape on upgrade"
  end

  test "a stack whose arena holds bytes claims a version the older release will not read", ctx do
    # Arrange
    opts = start_stack(ctx)
    seed_arena(ctx)

    # Act
    write_one_txn(opts)

    # Assert
    assert @pre_arena_version != claimed_version(ctx),
           "an older release would read this log's refs as JSON and serve them to a client"

    assert @narrow_ref_version != claimed_version(ctx),
           "the release that predates checksums crashes on a wider ref rather than misreading it"
  end

  test "the claim outlives the arena being switched off, while its bytes are still there", ctx do
    # Arrange
    opts = start_stack(ctx)
    seed_arena(ctx)
    write_one_txn(opts)
    claimed = claimed_version(ctx)

    # Act
    # Nothing here re-enables the arena: the collector opens no writer, exactly as it would not
    # after the operator removed the flag. The refs it already wrote are still in the log.
    stop_supervised!(Storage.stack_child_spec({PureFileStorage, %{stack_id: ctx.stack_id}}).id)
    opts = start_stack(ctx)
    _writer = PureFileStorage.init_writer!(opts, @shape)

    # Assert
    assert claimed == claimed_version(ctx),
           "dropping the claim would re-offer a log full of refs to a reader that cannot parse it"

    assert File.exists?(metadata_file(ctx, "pg_snapshot.bin")),
           "the shape was wiped and rebuilt, which is what an unchanged claim exists to avoid"
  end
end
