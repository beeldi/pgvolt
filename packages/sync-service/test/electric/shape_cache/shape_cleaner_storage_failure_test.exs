defmodule Electric.ShapeCache.ShapeCleanerStorageFailureTest do
  @moduledoc """
  Removal takes the shape out of `ShapeStatus` first and tells the log collector last, so a
  storage failure in between leaves the shape unroutable while its flush entry still pins the
  stack's ack. Nothing lists the handle afterwards, so no later call can heal it.

  The failure is produced through a real rename that cannot succeed rather than by stubbing the
  storage layer.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Plug.Test
  import ExUnit.CaptureLog

  alias Electric.AsyncDeleter
  alias Electric.Plug.Router
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeCleaner.CleanupTaskSupervisor
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.StatusMonitor
  alias Support.TestUtils

  @moduletag :tmp_dir

  describe "when the shape's storage cannot be reclaimed" do
    setup [:with_unique_db, :with_basic_tables, :with_sql_execute]

    setup :with_complete_stack

    setup(ctx) do
      :ok = StatusMonitor.wait_until_active(ctx.stack_id, timeout: 5_000)
      %{opts: Router.init(build_router_opts(ctx))}
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "reports the failure instead of raising, and still detaches the shape", ctx do
      # Arrange
      conn = conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(ctx.opts)
      assert %{status: 200} = conn
      [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")

      block_the_trash_directory(ctx.stack_id)

      # Act
      result = ShapeCleaner.remove_shapes(ctx.stack_id, [handle])

      # Assert
      assert :ok = result,
             "a shape whose storage cannot be reclaimed must not take the caller down with it"

      refute ShapeStatus.has_shape_handle?(ctx.stack_id, handle),
             "the shape is already out of ShapeStatus by the time storage is touched"
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "a shape that cannot be reclaimed does not abort the rest of the batch", ctx do
      # Arrange
      handles =
        for value <- ["a", "b"] do
          conn =
            conn(
              "GET",
              "/v1/shape?table=items&offset=-1&where=" <>
                URI.encode_www_form("value != '#{value}'")
            )
            |> Router.call(ctx.opts)

          assert %{status: 200} = conn
          [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")
          handle
        end

      assert length(Enum.uniq(handles)) == 2

      block_the_trash_directory(ctx.stack_id)

      # Act
      assert :ok = ShapeCleaner.remove_shapes(ctx.stack_id, handles)

      # Assert
      for handle <- handles do
        refute ShapeStatus.has_shape_handle?(ctx.stack_id, handle),
               "the first failing reclaim must not strand the handles after it"
      end
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "an async reclaim that fails on one shape still attempts the ones after it", ctx do
      # Arrange
      handles =
        for value <- ["a", "b"] do
          conn =
            conn(
              "GET",
              "/v1/shape?table=items&offset=-1&where=" <>
                URI.encode_www_form("value != '#{value}'")
            )
            |> Router.call(ctx.opts)

          assert %{status: 200} = conn
          [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")
          handle
        end

      block_the_trash_directory(ctx.stack_id)

      # Act
      log =
        capture_log(fn ->
          :ok = ShapeCleaner.remove_shape_storage_async(ctx.stack_id, handles)

          assert TestUtils.wait_until(fn ->
                   Task.Supervisor.children(CleanupTaskSupervisor.name(ctx.stack_id)) == []
                 end)
        end)

      # Assert
      attempts = length(String.split(log, "AsyncDeleter: rename failed")) - 1

      assert attempts == 2,
             "each shape must get its own reclaim attempt, but only #{attempts} was made"
    end
  end

  defp block_the_trash_directory(stack_id) do
    trash_dir = AsyncDeleter.trash_dir!(stack_id)
    File.rm_rf!(trash_dir)
    File.mkdir_p!(Path.dirname(trash_dir))
    File.write!(trash_dir, "")
  end
end
