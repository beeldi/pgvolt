defmodule Electric.ShapeCache.ShapeStatus.ShapeDbListShapesMemoryTest do
  @moduledoc """
  Measures what `list_shapes/1` costs in memory on a large shape store.

  `Query.list_shapes/1` runs `fetch_all` and deserializes every row with
  `binary_to_term`, and `ShapeDb.list_shapes/1` then forces the whole thing through
  `Enum.uniq_by`. So the full list of `Shape` structs is resident at once, per caller.
  """
  use ExUnit.Case, async: false

  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  import Support.ComponentSetup

  @moduletag :tmp_dir
  # Quarantined behind :memory (not :slow — --include slow would override the exclude):
  # holding the full per-caller shape list resident is the measurement, and the
  # concurrent-callers case OOMs standard CI hosts. Run with --include memory (and
  # optionally a smaller MEM_SHAPE_COUNT) on a host with headroom.
  @moduletag :memory
  @moduletag timeout: :infinity

  @shape_count String.to_integer(System.get_env("MEM_SHAPE_COUNT", "10000"))

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

  defp perimeter_where(index) do
    divisions = Enum.map_join(1..45, ", ", &"'b#{index * 45 + &1}'")

    ~s|"firmId" = 'F0' AND "divisionId" IN (#{divisions}) AND 'cache-buster:00007' = 'cache-buster:00007'|
  end

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

  # Collecting only the caller leaves every other process's garbage in the total and
  # inflates the baseline by gigabytes, so sweep the whole VM before measuring.
  defp settled_total do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    Process.sleep(300)
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    Process.sleep(200)
    :erlang.memory(:total)
  end

  defp breakdown do
    memory = :erlang.memory()

    "processes=#{mb(memory[:processes])}MB binary=#{mb(memory[:binary])}MB ets=#{mb(memory[:ets])}MB"
  end

  # Attributes ETS memory to named tables directly, rather than inferring ownership
  # from how the total scales with shape count.
  defp top_ets(limit) do
    word = :erlang.system_info(:wordsize)

    :ets.all()
    |> Enum.map(fn table ->
      {:ets.info(table, :name), :ets.info(table, :memory), :ets.info(table, :size)}
    end)
    |> Enum.filter(fn {name, memory, _size} -> not is_nil(name) and is_integer(memory) end)
    |> Enum.map(fn {name, memory, size} -> {name, memory * word, size} end)
    |> Enum.sort_by(fn {_name, bytes, _size} -> -bytes end)
    |> Enum.take(limit)
  end

  defp shape_db_bytes(tmp_dir) do
    tmp_dir
    |> Path.join("**/shape-db/*.sqlite")
    |> Path.wildcard()
    |> Enum.map(&File.stat!(&1).size)
    |> Enum.sum()
  end

  defp mb(bytes), do: Float.round(bytes / 1024 / 1024, 1)

  # Samples global BEAM memory while `fun` runs, so the peak includes the transient
  # deserialization garbage, not just what survives.
  defp with_peak(fun) do
    parent = self()

    sampler =
      spawn(fn ->
        sample = fn sample, peak ->
          receive do
            {:stop, from} -> send(from, {:peak, peak})
          after
            5 -> sample.(sample, max(peak, :erlang.memory(:total)))
          end
        end

        sample.(sample, :erlang.memory(:total))
      end)

    result = fun.()
    send(sampler, {:stop, parent})

    peak =
      receive do
        {:peak, peak} -> peak
      after
        5_000 -> 0
      end

    {peak, result}
  end

  test "per-shape retained cost and peak of a single list_shapes", ctx do
    # Arrange
    :ok = populate(ctx.stack_id, @shape_count)
    baseline = settled_total()

    # Act
    {peak, {:ok, shapes}} = with_peak(fn -> ShapeDb.list_shapes(ctx.stack_id) end)
    retained = settled_total() - baseline

    # Keeps the list reachable across the retained measurement.
    assert length(shapes) == @shape_count

    per_shape = retained / @shape_count

    IO.puts("\n  shapes:                #{@shape_count}")
    IO.puts("  shape-db file:         #{mb(shape_db_bytes(ctx.tmp_dir))} MB")
    IO.puts("  baseline total:        #{mb(baseline)} MB  (#{breakdown()})")

    Enum.each(top_ets(5), fn {name, bytes, size} ->
      IO.puts("    ets #{name}: #{mb(bytes)} MB (#{size} rows)")
    end)

    IO.puts("  retained by result:    #{mb(retained)} MB")
    IO.puts("  peak during call:      #{mb(peak)} MB (delta #{mb(peak - baseline)} MB)")
    IO.puts("  per-shape retained:    #{Float.round(per_shape / 1024, 2)} KB")
    IO.puts("  ---- extrapolated retained, one caller ----")

    Enum.each([60_000, 150_000], fn n ->
      IO.puts("  #{n} shapes:         #{mb(per_shape * n)} MB")
    end)

    # Assert
    assert retained > 0
  end

  test "a fold over the same shapes never holds them all", ctx do
    # Arrange
    :ok = populate(ctx.stack_id, @shape_count)
    baseline = settled_total()

    # Act
    {list_peak, {:ok, shapes}} = with_peak(fn -> ShapeDb.list_shapes(ctx.stack_id) end)
    assert length(shapes) == @shape_count
    shapes = nil
    _ = shapes
    :erlang.garbage_collect()

    {fold_peak, folded} =
      with_peak(fn ->
        ShapeDb.reduce_shapes(ctx.stack_id, 0, fn {_handle, _shape}, n -> n + 1 end)
      end)

    # Assert
    assert folded == @shape_count

    IO.puts("\n  list_shapes peak delta:  #{mb(list_peak - baseline)} MB")
    IO.puts("  reduce_shapes peak delta: #{mb(fold_peak - baseline)} MB")

    assert fold_peak - baseline < div(list_peak - baseline, 4),
           "the fold peaked at #{mb(fold_peak - baseline)} MB against #{mb(list_peak - baseline)} MB " <>
             "for the list it folds over; it is materialising what it should be streaming"
  end

  test "concurrent boot callers each hold their own full copy", ctx do
    # Arrange
    :ok = populate(ctx.stack_id, @shape_count)
    baseline = settled_total()

    # Act
    # A boot has several restores reading the shape list around the same time.
    {peak, results} =
      with_peak(fn ->
        [
          Task.async(fn -> ShapeDb.list_shapes(ctx.stack_id) end),
          Task.async(fn -> ShapeDb.list_shapes(ctx.stack_id) end),
          Task.async(fn -> ShapeDb.list_shapes(ctx.stack_id) end)
        ]
        |> Task.await_many(:infinity)
      end)

    IO.puts("\n  3 concurrent callers peak: #{mb(peak)} MB (delta #{mb(peak - baseline)} MB)")

    # Assert
    assert length(results) == 3
  end
end
