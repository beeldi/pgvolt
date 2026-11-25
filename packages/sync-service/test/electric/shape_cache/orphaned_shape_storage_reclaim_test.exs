defmodule Electric.ShapeCache.OrphanedShapeStorageReclaimTest do
  @moduledoc """
  A shape whose storage reclaim fails leaves its directory behind with no metadata row, and
  nothing enumerates the disk afterwards, so those bytes survive every restart. Lock acquisition
  makes this instance the store's sole writer, and that is where the reclaim runs: right after the
  refresh answers, while the boot goes on, since replication waits for that answer and listing
  every shape directory can take a long time at boot with a large shape count.

  The orphan is produced through a real reclaim against a trash directory that cannot accept a
  rename, rather than by planting a directory.
  """
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils, only: [patch_calls: 2]
  import Plug.Test
  import ExUnit.CaptureLog

  alias Electric.AsyncDeleter
  alias Electric.Plug.Router
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatusOwner
  alias Electric.ShapeCache.Storage
  alias Electric.StatusMonitor
  alias Support.TestUtils

  @moduletag :tmp_dir

  describe "on lock acquisition" do
    setup [:with_unique_db, :with_basic_tables, :with_sql_execute]

    setup :with_complete_stack

    setup(ctx) do
      :ok = StatusMonitor.wait_until_active(ctx.stack_id, timeout: 5_000)
      %{opts: Router.init(build_router_opts(ctx))}
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "a directory with no metadata row is reclaimed, and the live shapes are not", ctx do
      # Arrange
      orphan = create_shape(ctx, "value != 'orphan'")
      live = create_shape(ctx, "value != 'live'")
      leak_shape_storage(ctx.stack_id, orphan)

      # Act
      :ok = ShapeStatusOwner.refresh(ctx.stack_id)

      # Assert
      assert TestUtils.wait_until(fn -> not File.dir?(shape_dir(ctx.stack_id, orphan)) end),
             "a directory carrying no metadata row must not survive lock acquisition"

      assert File.dir?(shape_dir(ctx.stack_id, live)),
             "a shape that still has a metadata row must keep its storage"
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "the refresh answers before the reclaim has listed the shape directories", ctx do
      # Arrange
      orphan = create_shape(ctx, "value != 'orphan'")
      _live = create_shape(ctx, "value != 'live'")
      leak_shape_storage(ctx.stack_id, orphan)
      hold_shape_directory_listing(ctx.stack_id)

      # Act
      refresh = Task.async(fn -> ShapeStatusOwner.refresh(ctx.stack_id) end)

      # Assert
      assert_receive {:listing_held, lister}, 5_000

      assert Task.yield(refresh, 2_000) == {:ok, :ok},
             "replication waits for the refresh, so it must not wait for the listing"

      assert File.dir?(shape_dir(ctx.stack_id, orphan))
      send(lister, :release_listing)

      assert TestUtils.wait_until(fn -> not File.dir?(shape_dir(ctx.stack_id, orphan)) end),
             "the reclaim must still run once the refresh has answered"
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "a shape store too busy for the catalog check keeps the orphan and the stack", ctx do
      # Arrange
      orphan = create_shape(ctx, "value != 'orphan'")
      _live = create_shape(ctx, "value != 'live'")
      leak_shape_storage(ctx.stack_id, orphan)
      owner = GenServer.whereis(ShapeStatusOwner.name(ctx.stack_id))
      time_out_catalog_checks(owner)

      # Act
      log =
        capture_log(fn ->
          :ok = ShapeStatusOwner.refresh(ctx.stack_id)
          # the reclaim runs in the owner once the refresh has answered
          :sys.get_state(owner)
        end)

      # Assert
      assert_received {:catalog_check, ^orphan, timeout}

      assert timeout == ShapeDb.boot_read_timeout(),
             "the check runs while the boot's restores hold the store, so it waits as long as they do"

      assert log =~ "orphaned shape storage not reclaimed"

      assert GenServer.whereis(ShapeStatusOwner.name(ctx.stack_id)) == owner,
             "a cleanup must not take down the process that owns the shape tables"

      assert File.dir?(shape_dir(ctx.stack_id, orphan)),
             "an orphan the catalog could not rule out stays for the next lock acquisition"
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "nothing is reclaimed when the shape store holds no shapes at all", ctx do
      # Arrange
      orphan = create_shape(ctx, "value != 'orphan'")
      leak_shape_storage(ctx.stack_id, orphan)
      assert ShapeStatus.count_shapes(ctx.stack_id) == 0

      # Act
      :ok = ShapeStatusOwner.refresh(ctx.stack_id)

      # Assert
      refute TestUtils.wait_until(fn -> not File.dir?(shape_dir(ctx.stack_id, orphan)) end, 500),
             "an empty store cannot be told apart from one whose rows failed to load, so a " <>
               "diff against it must delete nothing"
    end
  end

  defp create_shape(ctx, where) do
    conn =
      conn("GET", "/v1/shape?table=items&offset=-1&where=" <> URI.encode_www_form(where))
      |> Router.call(ctx.opts)

    assert %{status: 200} = conn
    [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")

    # The response comes before the snapshot is synced and marked complete, and the refresh
    # counts a shape valid only once its mark is queued or written: one that counts none
    # reclaims nothing.
    assert TestUtils.wait_until(fn ->
             MapSet.member?(ShapeDb.WriteBuffer.queued_snapshot_completions(ctx.stack_id), handle) or
               ShapeDb.snapshot_complete?(ctx.stack_id, handle) == true
           end),
           "the shape's snapshot must be complete before the refresh counts it"

    handle
  end

  defp leak_shape_storage(stack_id, shape_handle) do
    trash_dir = AsyncDeleter.trash_dir!(stack_id)
    File.rm_rf!(trash_dir)
    File.mkdir_p!(Path.dirname(trash_dir))
    File.write!(trash_dir, "")

    :ok = ShapeCleaner.remove_shapes(stack_id, [shape_handle])

    File.rm_rf!(trash_dir)
    File.mkdir_p!(trash_dir)

    refute ShapeStatus.has_shape_handle?(stack_id, shape_handle)

    assert File.dir?(shape_dir(stack_id, shape_handle)),
           "the failed reclaim must leave the directory behind, or the test proves nothing"
  end

  defp hold_shape_directory_listing(stack_id) do
    test_pid = self()

    patch_calls(PureFileStorage,
      get_all_stored_shape_handles: fn opts ->
        send(test_pid, {:listing_held, self()})

        receive do
          :release_listing ->
            Repatch.real(PureFileStorage.get_all_stored_shape_handles(opts))
        end
      end
    )

    Repatch.allow(test_pid, GenServer.whereis(ShapeStatusOwner.name(stack_id)))
  end

  # The way NimblePool gives up on a checkout that has waited its whole timeout.
  defp time_out_catalog_checks(owner) do
    test_pid = self()

    patch_calls(ShapeDb,
      handle_exists?: fn _stack_id, shape_handle, timeout ->
        send(test_pid, {:catalog_check, shape_handle, timeout})
        exit({:timeout, {NimblePool, :checkout, [:read_pool]}})
      end
    )

    Repatch.allow(test_pid, owner)
  end

  defp shape_dir(stack_id, shape_handle) do
    PureFileStorage.shape_data_dir(Storage.opt_for_stack(stack_id, :base_path), shape_handle)
  end
end
