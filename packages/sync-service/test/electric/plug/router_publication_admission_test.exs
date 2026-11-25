defmodule Electric.Plug.RouterPublicationAdmissionTest do
  use ExUnit.Case, async: false

  import Plug.Test
  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils, only: [wait_until: 1]

  alias Electric.Plug.Router
  alias Electric.Shapes.Consumer
  alias Electric.Shapes.Consumer.Materializer
  alias Electric.SnapshotError

  @moduletag :tmp_dir
  @moduletag database_settings: ["lock_timeout = '1s'"]

  setup [:with_unique_db, :with_basic_tables, :with_publication, :with_complete_stack]

  for {mode, manual?} <- [automatic: false, manual: true] do
    @tag manual_table_publishing?: manual?
    test "the next GET succeeds after a held admission in #{mode} mode", ctx do
      assert :ok =
               Electric.StatusMonitor.wait_until_active(ctx.stack_id,
                 timeout: ExUnit.configuration()[:assert_receive_timeout]
               )

      opts = Router.init(build_router_opts(ctx))
      parent = self()

      if ctx.manual_table_publishing? do
        Postgrex.query!(ctx.db_conn, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      end

      writer_conn =
        start_link_supervised!({Postgrex, Electric.Utils.deobfuscate_password(ctx.db_config)},
          id: :writer_conn
        )

      writer =
        Task.async(fn ->
          Postgrex.transaction(writer_conn, fn tx ->
            Postgrex.query!(tx, "INSERT INTO items VALUES (gen_random_uuid(), 'in flight')", [])
            send(parent, :written)
            # However long the first request takes, the writer stays in flight until released.
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive :written

      if ctx.manual_table_publishing? do
        Postgrex.query!(
          ctx.db_conn,
          "ALTER PUBLICATION \"#{ctx.publication_name}\" ADD TABLE items",
          []
        )
      end

      # The admission is held, whether its lock wait or the request's wait for it ran out first.
      assert %{status: 503, resp_body: body} = get_shape(opts)
      assert %{"message" => message} = Jason.decode!(body)
      assert message in admission_pending_messages()

      assert [{handle, _shape}] = Electric.ShapeCache.list_shapes(ctx.stack_id)
      consumer = Consumer.whereis(ctx.stack_id, handle)
      assert is_pid(consumer) and Process.alive?(consumer)

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)

      assert %{status: 200, resp_body: body} = conn = get_shape(opts)
      assert Plug.Conn.get_resp_header(conn, "electric-handle") == [handle]
      assert Consumer.whereis(ctx.stack_id, handle) == consumer

      assert [
               %{"value" => %{"value" => "in flight"}},
               %{"headers" => %{"control" => "snapshot-end"}}
             ] =
               Jason.decode!(body)
    end
  end

  # A shape with a subquery reads its inner shape through a materializer, which waits for the inner
  # shape's snapshot to start. A held admission of the inner shape's table answers that wait with
  # each try that fails: the materializer waits on, and the request is served once the writer in
  # flight on that table is drained.
  for {mode, manual?} <- [automatic: false, manual: true] do
    @tag manual_table_publishing?: manual?
    test "a subquery on a table whose admission is held is served in #{mode} mode", ctx do
      assert :ok =
               Electric.StatusMonitor.wait_until_active(ctx.stack_id,
                 timeout: ExUnit.configuration()[:assert_receive_timeout]
               )

      opts = Router.init(build_router_opts(ctx))
      parent = self()

      for statement <- [
            "CREATE TABLE parent (id INT PRIMARY KEY, value INT NOT NULL)",
            "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT NOT NULL, value INT NOT NULL)",
            "INSERT INTO parent (id, value) VALUES (1, 1), (2, 2)",
            "INSERT INTO child (id, parent_id, value) VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)"
          ],
          do: Postgrex.query!(ctx.db_conn, statement, [])

      if ctx.manual_table_publishing? do
        for table <- ["parent", "child"],
            do: Postgrex.query!(ctx.db_conn, "ALTER TABLE #{table} REPLICA IDENTITY FULL", [])

        Postgrex.query!(
          ctx.db_conn,
          "ALTER PUBLICATION \"#{ctx.publication_name}\" ADD TABLE child",
          []
        )
      end

      writer_conn =
        start_link_supervised!({Postgrex, Electric.Utils.deobfuscate_password(ctx.db_config)},
          id: :writer_conn
        )

      writer =
        Task.async(fn ->
          Postgrex.transaction(writer_conn, fn tx ->
            Postgrex.query!(tx, "INSERT INTO parent (id, value) VALUES (3, 1)", [])
            send(parent, :written)
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive :written

      if ctx.manual_table_publishing? do
        Postgrex.query!(
          ctx.db_conn,
          "ALTER PUBLICATION \"#{ctx.publication_name}\" ADD TABLE parent",
          []
        )
      end

      request =
        Task.async(fn ->
          Router.call(
            conn("GET", "/v1/shape", %{
              table: "child",
              offset: "-1",
              where: "parent_id IN (SELECT id FROM parent WHERE value = 1)"
            }),
            opts
          )
        end)

      assert wait_until(fn -> materializer_of(ctx, {"public", "parent"}) end)
      {inner, materializer} = materializer_of(ctx, {"public", "parent"})
      watch = Process.monitor(materializer)

      # The second answer comes after the materializer's wait began.
      for _try <- 1..2 do
        assert {:error, %SnapshotError{type: :publication_admission_pending}} =
                 Consumer.await_snapshot_start(ctx.stack_id, inner, 5_000)
      end

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)

      assert %{status: 200, resp_body: body} = Task.await(request, 30_000)

      assert ["1", "3"] =
               body
               |> Jason.decode!()
               |> Enum.flat_map(fn
                 %{"value" => %{"id" => id}} -> [id]
                 _control -> []
               end)
               |> Enum.sort()

      refute_received {:DOWN, ^watch, :process, _, _}
    end
  end

  # The handle of the shape on the table and its materializer, once both run.
  defp materializer_of(ctx, table) do
    with [handle] <-
           for(
             {handle, %{root_table: ^table}} <- Electric.ShapeCache.list_shapes(ctx.stack_id),
             do: handle
           ),
         pid when is_pid(pid) <- GenServer.whereis(Materializer.name(ctx.stack_id, handle)) do
      {handle, pid}
    else
      _ -> nil
    end
  end

  defp admission_pending_messages do
    [
      SnapshotError.publication_admission_pending().message,
      SnapshotError.publication_admission_timeout().message
    ]
  end

  defp get_shape(opts), do: Router.call(conn("GET", "/v1/shape?table=items&offset=-1"), opts)
end
