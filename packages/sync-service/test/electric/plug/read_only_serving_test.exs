defmodule Electric.Plug.ReadOnlyServingTest do
  @moduledoc """
  Drives a real stack into the state where shape metadata is loaded but the shape
  subsystem is not running, and asserts what the HTTP API serves for a shape that is
  already complete on disk.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Plug.Test

  alias Electric.Plug.Router
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.StatusMonitor

  @moduletag :tmp_dir

  describe "shape subsystem down, connection subsystem up" do
    setup [:with_unique_db, :with_basic_tables, :with_sql_execute]

    setup :with_complete_stack

    setup(ctx) do
      :ok = StatusMonitor.wait_until_active(ctx.stack_id, timeout: 5_000)
      %{opts: Router.init(build_router_opts(ctx, stack_ready_timeout: 500))}
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "serves a shape whose log is already on disk", %{opts: opts, stack_id: stack_id} do
      assert %{status: 200} = conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(opts)

      :ok = Electric.CoreSupervisor.stop_shapes_supervisor(stack_id: stack_id)

      assert Support.TestUtils.wait_until(fn ->
               match?(%{shape: :read_only}, StatusMonitor.status(stack_id))
             end),
             "stopping the shape subsystem must settle status to :read_only"

      conn = conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(opts)

      assert %{status: 200} = conn,
             "shape metadata is loaded and the log is complete on disk, so this node can serve it"

      assert [%{"value" => %{"value" => "test value 1"}}, _up_to_date] =
               Jason.decode!(conn.resp_body)
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')"]
    test "does not cache shape metadata while it is not the writer", ctx do
      conn = conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(ctx.opts)
      assert %{status: 200} = conn
      [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")

      :ok = Electric.CoreSupervisor.stop_shapes_supervisor(stack_id: ctx.stack_id)

      assert Support.TestUtils.wait_until(fn ->
               match?(%{shape: :read_only}, StatusMonitor.status(ctx.stack_id))
             end),
             "stopping the shape subsystem must settle status to :read_only"

      stack_ets = PureFileStorage.stack_ets(ctx.stack_id)
      :ets.delete(stack_ets, handle)

      assert %{status: 200} =
               conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(ctx.opts)

      assert :ets.lookup(stack_ets, handle) == [],
             "a node that is not the writer must not cache metadata a writer it does not control can change"
    end
  end
end
