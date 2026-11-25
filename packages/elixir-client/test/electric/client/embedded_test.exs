defmodule Electric.Client.EmbeddedTest do
  use ExUnit.Case, async: false

  alias Electric.Client
  alias Electric.Client.Message.{ChangeMessage, ControlMessage}
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.ShapeState

  import Support.DbSetup
  import Support.ClientHelpers

  defp stream(ctx) do
    client_stream(ctx, [])
  end

  defp stream(ctx, opts) when is_list(opts) do
    client_stream(ctx, opts)
  end

  defp stream(ctx, limit) when is_integer(limit) do
    client_stream(ctx, []) |> Enum.take(limit)
  end

  defp client_stream(ctx, opts) do
    Client.stream(ctx.client, ctx.shape, opts)
  end

  defp with_embedded_client(_ctx) do
    {:ok, client} = Electric.Client.embedded()
    [client: client]
  end

  setup [:with_unique_table, :with_embedded_client]

  setup(ctx) do
    shape = ShapeDefinition.new!(ctx.tablename)

    on_exit(fn ->
      ExUnit.CaptureLog.capture_log(fn ->
        Client.delete_shape(ctx.client, shape)
      end)
    end)

    [shape: shape]
  end

  test "streams an empty shape", ctx do
    assert [%ControlMessage{control: :up_to_date}] = stream(ctx, 1)
  end

  test "streams a non empty shape", ctx do
    %{tablename: table} = ctx

    {:ok, id1} = insert_item(ctx)
    {:ok, id2} = insert_item(ctx)
    {:ok, id3} = insert_item(ctx)

    # snapshot values
    msgs = stream(ctx, 4)

    assert [
             %ChangeMessage{
               headers: %{operation: :insert, relation: ["public", ^table]},
               value: %{"id" => ^id1}
             },
             %ChangeMessage{
               headers: %{operation: :insert, relation: ["public", ^table]},
               value: %{"id" => ^id2}
             },
             %ChangeMessage{
               headers: %{operation: :insert, relation: ["public", ^table]},
               value: %{"id" => ^id3}
             },
             up_to_date()
           ] = msgs

    # 1 timestamp for the snapshot, 1 for the up-to-date response
    assert length(Enum.uniq_by(msgs, & &1.request_timestamp)) == 2
  end

  test "streams live data changes", ctx do
    {:ok, id1} = insert_item(ctx)

    parent = self()
    stream = stream(ctx)

    {:ok, _task} =
      start_supervised(
        {Task,
         fn ->
           stream
           |> Stream.each(&send(parent, {:stream, 1, &1}))
           |> Stream.run()
         end},
        id: {:stream, 1}
      )

    {:ok, _task} =
      start_supervised(
        {Task,
         fn ->
           stream
           |> Stream.each(&send(parent, {:stream, 2, &1}))
           |> Stream.run()
         end},
        id: {:stream, 2}
      )

    assert_receive {:stream, 1, %ChangeMessage{value: %{"id" => ^id1}}}, 5000
    assert_receive {:stream, 1, up_to_date()}
    assert_receive {:stream, 2, %ChangeMessage{value: %{"id" => ^id1}}}, 5000
    assert_receive {:stream, 2, up_to_date()}
    refute_receive _

    {:ok, {id2, id3}} =
      with_transaction(ctx, fn ctx ->
        {:ok, id2} = insert_item(ctx)
        {:ok, id3} = insert_item(ctx)
        {id2, id3}
      end)

    assert_receive {:stream, 1, %ChangeMessage{value: %{"id" => ^id2}}}, 5000
    assert_receive {:stream, 1, %ChangeMessage{value: %{"id" => ^id3}}}, 5000
    assert_receive {:stream, 1, up_to_date()}

    assert_receive {:stream, 2, %ChangeMessage{value: %{"id" => ^id2}}}, 5000
    assert_receive {:stream, 2, %ChangeMessage{value: %{"id" => ^id3}}}, 5000
    assert_receive {:stream, 2, up_to_date()}

    refute_receive _
  end

  test "sends full rows with replica: :full", ctx do
    {:ok, id1} = insert_item(ctx, title: "Changing item")
    parent = self()
    stream = stream(ctx, replica: :full)

    {:ok, _task} =
      start_supervised(
        {Task,
         fn ->
           stream
           |> Stream.each(&send(parent, {:stream, 1, &1}))
           |> Stream.run()
         end},
        id: {:stream, 1}
      )

    assert_receive {:stream, 1, %ChangeMessage{value: %{"id" => ^id1}}}, 5000
    assert_receive {:stream, 1, up_to_date()}
    refute_receive _

    :ok = update_item(ctx, id1, value: 999)

    assert_receive {:stream, 1,
                    %ChangeMessage{
                      value: %{"id" => ^id1, "value" => 999, "title" => "Changing item"}
                    }},
                   500

    assert_receive {:stream, 1, up_to_date()}
    refute_receive _
  end

  test "supports shapes with where clauses and column lists", ctx do
    %{tablename: table} = ctx

    shape = ShapeDefinition.new!(table, where: "value IS NOT NULL", columns: ["id", "value"])

    {:ok, id1} = insert_item(ctx)
    {:ok, id2} = insert_item(ctx)
    {:ok, id3} = insert_item(ctx)

    # snapshot values
    msgs = stream(%{ctx | shape: shape}, 4)

    assert [
             %ChangeMessage{
               headers: %{operation: :insert, relation: ["public", ^table]},
               value: %{"id" => ^id1}
             },
             %ChangeMessage{
               headers: %{operation: :insert, relation: ["public", ^table]},
               value: %{"id" => ^id2}
             },
             %ChangeMessage{
               headers: %{operation: :insert, relation: ["public", ^table]},
               value: %{"id" => ^id3}
             },
             up_to_date()
           ] = msgs

    # 1 timestamp for the snapshot, 1 for the up-to-date response
    assert length(Enum.uniq_by(msgs, & &1.request_timestamp)) == 2
  end

  describe "parameterised where clauses" do
    test "streams a shape whose params are given as a map", ctx do
      %{tablename: table} = ctx

      {:ok, id1} = insert_item(ctx, title: "wanted")
      {:ok, id2} = insert_item(ctx, title: "wanted too")
      {:ok, _id3} = insert_item(ctx, title: "unwanted")

      shape =
        ShapeDefinition.new!(table,
          where: "id IN ($1::uuid, $2::uuid) AND value = $3::int8",
          params: %{1 => id1, 2 => id2, 3 => 0}
        )

      assert [
               %ChangeMessage{value: %{"id" => ^id1, "title" => "wanted"}},
               %ChangeMessage{value: %{"id" => ^id2, "title" => "wanted too"}},
               up_to_date()
             ] = stream(%{ctx | shape: shape}, 3)
    end

    test "streams a shape whose params are given as a list", ctx do
      %{tablename: table} = ctx

      {:ok, id1} = insert_item(ctx, title: "wanted")
      {:ok, _id2} = insert_item(ctx, title: "unwanted")

      shape = ShapeDefinition.new!(table, where: "title = $1", params: ["wanted"])

      assert [
               %ChangeMessage{value: %{"id" => ^id1, "title" => "wanted"}},
               up_to_date()
             ] = stream(%{ctx | shape: shape}, 2)
    end

    test "polls a shape with params", ctx do
      %{tablename: table, client: client} = ctx

      {:ok, id1} = insert_item(ctx, title: "wanted")
      {:ok, id2} = insert_item(ctx, title: "wanted too")
      {:ok, _id3} = insert_item(ctx, title: "unwanted")

      shape =
        ShapeDefinition.new!(table,
          where: "id IN ($1::uuid, $2::uuid)",
          params: %{1 => id1, 2 => id2}
        )

      on_exit(fn ->
        ExUnit.CaptureLog.capture_log(fn -> Client.delete_shape(client, shape) end)
      end)

      assert {:ok, messages, %ShapeState{}} =
               Client.poll(client, shape, ShapeState.new())

      ids = for %ChangeMessage{value: %{"id" => id}} <- messages, do: id
      assert Enum.sort(ids) == Enum.sort([id1, id2])
    end

    test "deletes a shape that has params", ctx do
      %{tablename: table} = ctx
      {:ok, client} = Client.embedded(allow_shape_deletion?: true)

      {:ok, id1} = insert_item(ctx)

      shape = ShapeDefinition.new!(table, where: "id = $1::uuid", params: [id1])

      assert {:ok, [_ | _], %ShapeState{shape_handle: handle}} =
               Client.poll(client, shape, ShapeState.new())

      assert %Electric.Client.Fetch.Response{status: 202} =
               Client.delete_shape(client, shape)

      assert {:ok, [_ | _], %ShapeState{shape_handle: new_handle}} =
               Client.poll(client, shape, ShapeState.new())

      assert new_handle != handle

      Client.delete_shape(client, shape)
    end

    test "reports a missing param as a request error", ctx do
      %{tablename: table, client: client} = ctx

      shape = ShapeDefinition.new!(table, where: "title = $1")

      assert {:error, %Electric.Client.Error{message: message}} =
               Client.poll(client, shape, ShapeState.new())

      assert inspect(message) =~ "parameter $1 was not provided"
    end
  end

  test "rejects zero-length columns", ctx do
    %{tablename: table} = ctx

    shape = ShapeDefinition.new!(table, where: "value IS NOT NULL", columns: ["id", ""])

    assert_raise Electric.Client.Error, fn ->
      _msgs = stream(%{ctx | shape: shape}, 4)
    end
  end

  test "is able to read response body in separate processes", ctx do
    parent = self()

    {:ok, _id1} = insert_item(ctx)
    {:ok, _id2} = insert_item(ctx)
    {:ok, _id3} = insert_item(ctx)

    stream = stream(ctx, live: false)

    {:ok, _} =
      Task.start_link(fn ->
        Enum.each(stream, &send(parent, {:msg, &1}))
      end)

    assert_receive {:msg, %ChangeMessage{}}, 1000
    assert_receive {:msg, %ChangeMessage{}}, 1000
    assert_receive {:msg, %ChangeMessage{}}, 1000
    assert_receive {:msg, up_to_date()}, 1000
  end

  test "sends a must-refetch message if the table is truncated", ctx do
    parent = self()
    stream = stream(ctx)

    {:ok, _} =
      start_supervised(
        {Task,
         fn ->
           Enum.each(stream, &send(parent, {:msg, &1}))
         end}
      )

    {:ok, _id1} = insert_item(ctx)
    assert_receive {:msg, %ChangeMessage{}}, 1000
    assert_receive {:msg, up_to_date()}, 1000

    Postgrex.query!(ctx.db_conn, ~s|TRUNCATE TABLE "#{ctx.tablename}"|, [])

    assert_receive {:msg, %ControlMessage{control: :must_refetch}}, 5000
  end
end
