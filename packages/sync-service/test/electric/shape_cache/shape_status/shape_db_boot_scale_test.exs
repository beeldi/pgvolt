defmodule Electric.ShapeCache.ShapeStatus.ShapeDbBootScaleTest do
  @moduledoc """
  Exercises shape store boot restore at high shape counts.

  Runs in exclusive mode on purpose: in exclusive mode no read pool is started and the single
  write connection is registered for reads too, so every boot restore queues on one connection.
  """
  use ExUnit.Case, async: false

  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  import Support.ComponentSetup

  @moduletag :tmp_dir
  # Quarantined behind :memory (not :slow — --include slow would override the exclude):
  # populating a large number of shapes and materializing them four times
  # concurrently is the point of the repro, and that peak OOMs standard CI hosts. Run
  # with --include memory (and optionally a smaller BOOT_SCALE_SHAPE_COUNT) on a host
  # with several GB of headroom.
  @moduletag :memory
  @moduletag timeout: :infinity

  @shape_count String.to_integer(System.get_env("BOOT_SCALE_SHAPE_COUNT", "60000"))

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"}
               ]
             )

  @tag shape_db_opts: [exclusive_mode: true]
  setup [:with_stack_id_from_test, :with_shape_db]

  setup do
    previous = Application.get_env(:electric, :shape_db_boot_read_timeout)
    on_exit(fn -> Application.put_env(:electric, :shape_db_boot_read_timeout, previous) end)
    :ok
  end

  # A restricted-perimeter shape: a 45-division IN list plus the rotation token.
  # Shape size drives the cost of the boot read, so a thin fixture would not reproduce anything.
  defp perimeter_where(index) do
    divisions = Enum.map_join(1..45, ", ", &"'b#{index * 45 + &1}'")

    ~s|"firmId" = 'F0' AND "divisionId" IN (#{divisions}) AND 'cache-buster:00007' = 'cache-buster:00007'|
  end

  # Flushed in batches: the write buffer holds every pending shape in memory, and a single
  # flush of every shape both exhausts the heap and blows the default 5s flush call timeout.
  defp populate(stack_id, count) do
    1..count
    |> Enum.chunk_every(2_000)
    |> Enum.each(fn batch ->
      Enum.each(batch, fn i ->
        shape = Shape.new!("employees", inspector: @inspector, where: perimeter_where(i))
        {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-#{i}")
      end)

      :ok = WriteBuffer.flush_sync(stack_id, 120_000)
    end)
  end

  defp attempt_list_shapes(stack_id) do
    ShapeDb.list_shapes(stack_id)
    :completed
  catch
    :exit, _ -> :timed_out
  end

  defp ms(fun) do
    started = System.monotonic_time()
    result = fun.()
    {System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond), result}
  end

  test "the boot read sequence completes on a large shape store", ctx do
    # Arrange
    {populate_ms, :ok} = ms(fn -> populate(ctx.stack_id, @shape_count) end)
    {:ok, listed} = ShapeDb.list_shapes(ctx.stack_id)
    assert length(listed) == @shape_count

    IO.puts("\n  shapes:            #{@shape_count} (populated in #{populate_ms}ms)")

    # Act
    # `reduce_shapes` holds the connection only for the `list_shapes` read, so that read
    # IS the boot hold every other restore queues behind.
    {hold_ms, _} = ms(fn -> ShapeDb.list_shapes(ctx.stack_id) end)

    {meta_hold_ms, _} =
      ms(fn -> ShapeDb.reduce_shape_meta(ctx.stack_id, 0, fn _meta, acc -> acc + 1 end) end)

    IO.puts("  reduce_shape_meta: #{meta_hold_ms}ms")

    # The four boot restores: shape meta, routing state, publication filters, log collector.
    {concurrent_ms, results} =
      ms(fn ->
        [
          Task.async(fn ->
            ShapeDb.reduce_shape_meta(ctx.stack_id, 0, fn _m, acc -> acc + 1 end)
          end),
          Task.async(fn -> ShapeDb.list_shapes(ctx.stack_id) end),
          Task.async(fn -> ShapeDb.list_shapes(ctx.stack_id) end),
          Task.async(fn -> ShapeDb.list_shapes(ctx.stack_id) end)
        ]
        |> Task.await_many(:infinity)
      end)

    IO.puts("  single hold:       #{hold_ms}ms")
    IO.puts("  4 boot restores:   #{concurrent_ms}ms")
    IO.puts("  old 10s default:   #{if hold_ms > 10_000, do: "EXCEEDED", else: "not exceeded"}")

    # Assert
    assert length(results) == 4
    assert Enum.all?(results, &(&1 != :error))
  end

  test "the old 10s checkout default cannot boot this store", ctx do
    # Arrange
    :ok = populate(ctx.stack_id, @shape_count)
    Application.put_env(:electric, :shape_db_boot_read_timeout, 10_000)

    # Act
    # One restore holds the only connection; the rest get the pre-fix 10s to acquire it.
    holder =
      Task.async(fn -> ShapeDb.reduce_shape_meta(ctx.stack_id, 0, fn _m, acc -> acc + 1 end) end)

    queued =
      [
        Task.async(fn -> attempt_list_shapes(ctx.stack_id) end),
        Task.async(fn -> attempt_list_shapes(ctx.stack_id) end),
        Task.async(fn -> attempt_list_shapes(ctx.stack_id) end)
      ]
      |> Task.await_many(:infinity)

    Task.await(holder, :infinity)

    timed_out = Enum.count(queued, &(&1 == :timed_out))
    IO.puts("\n  queued restores timed out at 10s: #{timed_out}/3")

    # Assert
    assert timed_out > 0, "expected the 10s default to starve at least one queued boot restore"
  end
end
