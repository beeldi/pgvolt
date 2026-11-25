defmodule Electric.ShapeCache.ShapeStatusBootRefreshTest do
  # A boot reads the stored shapes' metadata into the meta table in `initialize/1`, then reads it
  # all again in the refresh that follows the lock: seconds of a boot with a large shape count.
  # That second read is only needed when another connection committed to the store in
  # between, which SQLite's data version tells.
  #
  # Not async: a log captured here holds what every test running meanwhile logs, and another
  # module's boot logs the same refresh step.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Support.ComponentSetup

  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape

  defmodule NoopStorage do
    def get_all_stored_shape_handles(_opts), do: {:ok, MapSet.new()}
    def cleanup_all!(_opts), do: :ok
  end

  @inspector Support.StubInspector.new(
               tables: [{1, {"public", "items"}}],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "value", type: "text"}
               ]
             )

  @moduletag :tmp_dir

  @kept_as_read "Shape metadata unchanged in the store since it was read, kept as read"
  @read_again ~r/Boot step shape_status\.refresh\.populate_shape_meta_table took \d+ms/

  for {mode, shape_db_opts} <- [exclusive: [exclusive_mode: true], pooled: []] do
    describe "with the store in #{mode} mode" do
      @describetag shape_db_opts: shape_db_opts

      setup [:with_stack_id_from_test, :with_async_deleter, :with_shape_db, :with_stored_shapes]

      test "a boot's refresh keeps what the boot read when nothing else wrote the store", ctx do
        :ok = ShapeStatus.initialize(ctx.stack_id)

        log = capture_log(fn -> :ok = ShapeStatus.refresh(ctx.stack_id) end)

        assert log =~ @kept_as_read
        refute log =~ @read_again
        assert Enum.all?(ctx.handles, &ShapeStatus.has_shape_handle?(ctx.stack_id, &1))
      end

      test "a commit by another connection makes the boot's refresh read the store again",
           ctx do
        :ok = ShapeStatus.initialize(ctx.stack_id)
        [removed, kept] = ctx.handles

        {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
        {:ok, conn} = ShapeDb.Sqlite3.open(path, [])
        {:ok, stmt} = ShapeDb.Sqlite3.prepare(conn, "DELETE FROM shapes WHERE handle = ?")
        :ok = ShapeDb.Sqlite3.bind(stmt, [{:blob, removed}])
        :done = ShapeDb.Sqlite3.step(conn, stmt)
        :ok = ShapeDb.Connection.close(conn)

        log = capture_log(fn -> :ok = ShapeStatus.refresh(ctx.stack_id) end)

        refute log =~ @kept_as_read
        assert log =~ @read_again
        refute ShapeStatus.has_shape_handle?(ctx.stack_id, removed)
        assert ShapeStatus.has_shape_handle?(ctx.stack_id, kept)
      end

      test "only the first refresh after the boot's read can keep it", ctx do
        :ok = ShapeStatus.initialize(ctx.stack_id)
        :ok = ShapeStatus.refresh(ctx.stack_id)

        log = capture_log(fn -> :ok = ShapeStatus.refresh(ctx.stack_id) end)

        assert log =~ @read_again
      end

      test "a refresh once the routing state is restored reads the store again", ctx do
        :ok = ShapeStatus.initialize(ctx.stack_id)

        routing_state =
          ShapeStatus.reduce_shapes(ctx.stack_id, ShapeStatus.new_routing_state(), fn
            {handle, shape}, routing_state ->
              ShapeStatus.add_to_routing_state(routing_state, handle, shape)
          end)

        :ok = ShapeStatus.restore_routing_state(ctx.stack_id, routing_state)

        log = capture_log(fn -> :ok = ShapeStatus.refresh(ctx.stack_id) end)

        assert log =~ @read_again
      end
    end
  end

  # Two shapes in the store, as a previous instance left them.
  defp with_stored_shapes(ctx) do
    Electric.StackConfig.put(ctx.stack_id, Electric.ShapeCache.Storage, {NoopStorage, []})

    handles =
      for value <- ["a", "b"] do
        handle = "handle-#{value}"
        {:ok, _digest} = ShapeDb.add_shape(ctx.stack_id, shape!(value), handle)
        :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
        handle
      end

    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    %{handles: handles}
  end

  defp shape!(value), do: Shape.new!("items", inspector: @inspector, where: "value = '#{value}'")
end
