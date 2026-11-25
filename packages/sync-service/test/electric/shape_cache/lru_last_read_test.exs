defmodule Electric.ShapeCache.LruLastReadTest do
  @moduledoc """
  `ExpiryManager` sheds the least recently used shapes once a ceiling is configured, and
  ranks them on the `last_read` stamp. Eviction is only right if the stamp means "a client
  asked for this shape", not "this shape delivered data" -- a rarely-changing shape that a
  client polls constantly must not rank as idle. These cases drive real requests through the
  router and measure the stamp for each kind of request a client actually makes.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Plug.Test

  alias Electric.Plug.Router
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.StatusMonitor

  @moduletag :tmp_dir

  @an_hour System.convert_time_unit(3600, :second, :native)

  describe "last_read" do
    setup [:with_unique_db, :with_basic_tables, :with_sql_execute]

    setup :with_complete_stack

    setup(ctx) do
      :ok = StatusMonitor.wait_until_active(ctx.stack_id, timeout: 5_000)
      %{opts: Router.init(build_router_opts(ctx, long_poll_timeout: 200))}
    end

    setup(ctx) do
      conn = conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(ctx.opts)
      assert %{status: 200} = conn

      %{
        handle: header(conn, "electric-handle"),
        offset: header(conn, "electric-offset")
      }
    end

    test "a request that returns log data moves it", ctx do
      # Arrange
      age_the_shape(ctx)

      Postgrex.query!(ctx.db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'second')", [])

      # Act
      conn =
        conn("GET", shape_url(ctx, live: true, offset: ctx.offset)) |> Router.call(ctx.opts)

      # Assert
      assert %{status: 200} = conn
      assert age_in_minutes(ctx) < 1
    end

    test "a live long poll that times out with no changes moves it", ctx do
      # Arrange
      age_the_shape(ctx)

      # Act
      conn =
        conn("GET", shape_url(ctx, live: true, offset: ctx.offset)) |> Router.call(ctx.opts)

      # Assert
      assert %{status: 200} = conn

      assert age_in_minutes(ctx) < 1,
             "a client polling a shape that never changes is still using it"
    end

    test "an up-to-date non-live request moves it", ctx do
      # Arrange
      age_the_shape(ctx)

      # Act
      conn = conn("GET", shape_url(ctx, offset: ctx.offset)) |> Router.call(ctx.opts)

      # Assert
      assert %{status: 200} = conn
      assert age_in_minutes(ctx) < 1
    end

    test "a shape nobody asks for keeps its age", ctx do
      # Arrange
      age_the_shape(ctx)

      # Assert
      assert age_in_minutes(ctx) > 59,
             "without this the other cases cannot tell a moved stamp from an unreadable age"
    end
  end

  defp shape_url(ctx, opts) do
    query =
      [table: "items", handle: ctx.handle, offset: Keyword.fetch!(opts, :offset)]
      |> then(&if(opts[:live], do: &1 ++ [live: true], else: &1))
      |> URI.encode_query()

    "/v1/shape?" <> query
  end

  defp age_the_shape(%{stack_id: stack_id, handle: handle}) do
    true =
      ShapeStatus.update_last_read_time(stack_id, handle, System.monotonic_time() - @an_hour)
  end

  defp age_in_minutes(%{stack_id: stack_id, handle: handle}) do
    {[^handle], age} = ShapeStatus.least_recently_used(stack_id, 1)
    age
  end

  defp header(conn, name) do
    [value] = Plug.Conn.get_resp_header(conn, name)
    value
  end
end
