defmodule Electric.Plug.RouterWaitForLsnTest do
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Plug.Test

  alias Electric.Plug.Router

  @moduletag :tmp_dir

  @long_poll_timeout 30_000

  setup [:with_unique_db, :with_basic_tables, :with_sql_execute]
  setup :with_complete_stack

  setup ctx do
    :ok =
      Electric.StatusMonitor.wait_until_active(ctx.stack_id,
        timeout: ExUnit.configuration()[:assert_receive_timeout]
      )

    %{opts: Router.init(build_router_opts(ctx, long_poll_timeout: @long_poll_timeout))}
  end

  describe "a request without wait_for_lsn" do
    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "gets upstream's up-to-date message", %{opts: opts} do
      {handle, offset} = snapshot(opts)

      assert [%{"headers" => headers}] =
               get(opts, "table=items&handle=#{handle}&offset=#{offset}")

      assert %{"control" => "up-to-date", "global_last_seen_lsn" => _} = headers
      refute Map.has_key?(headers, "covered_lsn")
    end
  end

  describe "a non-live request with wait_for_lsn" do
    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "covers the transactions of the shape's last change", %{opts: opts, db_conn: db_conn} do
      {handle, offset} = snapshot(opts)
      Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'second')", [])

      task = live(opts, "table=items&handle=#{handle}&offset=#{offset}")
      assert %{status: 200} = Task.await(task)

      assert [
               %{"headers" => %{"operation" => "insert", "lsn" => change_lsn}},
               %{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}
             ] = get(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=0")

      assert String.to_integer(covered_lsn) >= String.to_integer(change_lsn)
    end
  end

  # A request that read the covered position can check the shape's log against a shape subsystem
  # started again since, to replay from the slot what a stopped writer lost: the log is at the
  # same offset until the replay delivers that, and the position read before promised it.
  describe "a request with wait_for_lsn overtaken by a replay from the slot" do
    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "covers nothing it read before the replay", %{opts: opts, db_conn: db_conn} = ctx do
      {handle, offset} = snapshot(opts)
      Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'second')", [])

      task = live(opts, "table=items&handle=#{handle}&offset=#{offset}")
      assert %{status: 200} = conn = Task.await(task)
      [offset] = Plug.Conn.get_resp_header(conn, "electric-offset")
      assert [%{"headers" => %{"lsn" => change_lsn}} | _] = Jason.decode!(conn.resp_body)

      # Fanned out past the shape's last change, as other tables' transactions take it: the
      # coverage the watermark gives is above what the log's own chunk end gives.
      watermark = String.to_integer(change_lsn) + 1000
      :ok = Electric.LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, watermark)

      assert [%{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}] =
               get(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=0")

      assert String.to_integer(covered_lsn) >= watermark - 1
      {incarnation, count} = Electric.LsnTracker.replay_generation(ctx.stack_id)

      # The subsystem is stopped for the replay as the request checks the shape's consumer, and the
      # consumer of the one started again holds what it was handed.
      Repatch.patch(Electric.Shapes.Consumer, :holds_handled_transactions?, fn stack_id,
                                                                               _shape_handle,
                                                                               _timeout ->
        :ok = Electric.LsnTracker.request_replay_from_slot(stack_id)
        true
      end)

      assert [%{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}] =
               get(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=0")

      assert String.to_integer(covered_lsn) < watermark - 1
      assert Electric.LsnTracker.replay_generation(ctx.stack_id) == {incarnation, count + 1}
    end
  end

  # The tracker's table goes with the shape status owner. One created again as the request checks
  # the shape's consumer counts its replay generations from its own start, where the one before
  # may have counted none either.
  describe "a request with wait_for_lsn overtaken by a tracker created again" do
    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "covers nothing it read from the one before", %{opts: opts, db_conn: db_conn} = ctx do
      {handle, offset} = snapshot(opts)
      Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'second')", [])

      task = live(opts, "table=items&handle=#{handle}&offset=#{offset}")
      assert %{status: 200} = conn = Task.await(task)
      [offset] = Plug.Conn.get_resp_header(conn, "electric-offset")
      assert [%{"headers" => %{"lsn" => change_lsn}} | _] = Jason.decode!(conn.resp_body)

      watermark = String.to_integer(change_lsn) + 1000
      :ok = Electric.LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, watermark)

      Repatch.patch(Electric.Shapes.Consumer, :holds_handled_transactions?, fn stack_id,
                                                                               _shape_handle,
                                                                               _timeout ->
        true = :ets.delete(Electric.LsnTracker.stack_ref(stack_id))
        :ok = Electric.LsnTracker.initialize(stack_id)
        true
      end)

      assert [%{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}] =
               get(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=0")

      assert String.to_integer(covered_lsn) < watermark - 1
    end
  end

  describe "a live request with wait_for_lsn" do
    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "answers without a change once an idle database's last commit is covered", %{
      opts: opts,
      db_conn: db_conn
    } do
      {handle, offset} = snapshot(opts)
      Postgrex.query!(db_conn, "INSERT INTO serial_ids (id) VALUES (1)", [])
      target = wal_flush_lsn(db_conn) - 1

      task =
        live(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=#{target}")

      assert %{status: 200} = conn = Task.await(task)

      assert [%{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}] =
               Jason.decode!(conn.resp_body)

      assert String.to_integer(covered_lsn) >= target
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "returns a change of the shape that lands while it waits", %{
      opts: opts,
      db_conn: db_conn
    } do
      {handle, offset} = snapshot(opts)

      task =
        live(
          opts,
          "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=18446744073709551615"
        )

      Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'second')", [])

      assert %{status: 200} = conn = Task.await(task)

      assert [
               %{"headers" => %{"operation" => "insert", "lsn" => change_lsn}},
               %{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}
             ] = Jason.decode!(conn.resp_body)

      assert String.to_integer(covered_lsn) >= String.to_integer(change_lsn)
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "covers up to the position before the WAL end a keepalive names", %{
      opts: opts,
      stack_id: stack_id
    } do
      {handle, offset} = snapshot(opts)
      wal_end = 0x10000000000
      Electric.LsnTracker.broadcast_last_seen_lsn(stack_id, wal_end)

      assert [%{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}] =
               get(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=0")

      assert String.to_integer(covered_lsn) == wal_end - 1
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "covers no further than the fan-out when the processed position is ahead of it", %{
      opts: opts,
      stack_id: stack_id
    } do
      {handle, offset} = snapshot(opts)
      slot_position = 0x10000000000
      Electric.LsnTracker.set_last_processed_lsn(stack_id, slot_position)

      assert [%{"headers" => %{"control" => "up-to-date", "covered_lsn" => covered_lsn}}] =
               get(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=0")

      assert String.to_integer(covered_lsn) < slot_position
    end

    @tag with_sql: ["INSERT INTO items VALUES (gen_random_uuid(), 'first')"]
    test "sends a must-refetch for a truncate the shape is removed for", %{
      opts: opts,
      db_conn: db_conn
    } do
      {handle, offset} = snapshot(opts)
      target = wal_flush_lsn(db_conn)

      task =
        live(opts, "table=items&handle=#{handle}&offset=#{offset}&wait_for_lsn=#{target}")

      Postgrex.query!(db_conn, "TRUNCATE items", [])

      assert %{status: 409} = Task.await(task)
    end
  end

  describe "wait_for_lsn" do
    test "is refused when it is not an LSN", %{opts: opts} do
      for value <- ["", "abc", "-1", "18446744073709551616"] do
        conn = conn("GET", "/v1/shape?table=items&offset=-1&wait_for_lsn=#{value}")
        assert %{status: 400} = Router.call(conn, opts)
      end
    end

    test "is refused on a shape with a subquery", %{opts: opts} do
      where = URI.encode_www_form("id IN (SELECT id FROM items WHERE value = 'first')")
      conn = conn("GET", "/v1/shape?table=items&offset=-1&where=#{where}&wait_for_lsn=1")

      assert %{status: 400} = Router.call(conn, opts)
    end

    test "is refused on a server-sent-events request", %{opts: opts} do
      {handle, offset} = snapshot(opts)

      conn =
        conn(
          "GET",
          "/v1/shape?table=items&handle=#{handle}&offset=#{offset}&live=true&live_sse=true&wait_for_lsn=1"
        )

      assert %{status: 400} = Router.call(conn, opts)
    end
  end

  defp snapshot(opts) do
    conn = conn("GET", "/v1/shape?table=items&offset=-1") |> Router.call(opts)
    assert %{status: 200} = conn
    [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")
    [offset] = Plug.Conn.get_resp_header(conn, "electric-offset")
    catch_up(opts, handle, offset)
  end

  defp catch_up(opts, handle, offset) do
    conn =
      conn("GET", "/v1/shape?table=items&handle=#{handle}&offset=#{offset}") |> Router.call(opts)

    assert %{status: 200} = conn
    [next_offset] = Plug.Conn.get_resp_header(conn, "electric-offset")

    if Plug.Conn.get_resp_header(conn, "electric-up-to-date") == [],
      do: catch_up(opts, handle, next_offset),
      else: {handle, next_offset}
  end

  defp get(opts, query) do
    conn = conn("GET", "/v1/shape?" <> query) |> Router.call(opts)
    assert %{status: 200} = conn
    Jason.decode!(conn.resp_body)
  end

  defp live(opts, query) do
    Task.async(fn -> conn("GET", "/v1/shape?" <> query <> "&live=true") |> Router.call(opts) end)
  end

  defp wal_flush_lsn(db_conn) do
    %{rows: [[lsn]]} =
      Postgrex.query!(db_conn, "SELECT (pg_current_wal_flush_lsn() - '0/0')::bigint", [])

    lsn
  end
end
