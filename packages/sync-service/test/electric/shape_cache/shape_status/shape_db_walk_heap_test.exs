defmodule Electric.ShapeCache.ShapeStatus.ShapeDbWalkHeapTest do
  # A walk decodes every stored shape on the heap of the process that asked for it, and a boot
  # restore keeps what it builds there. On the heap a process starts with, the walk collected
  # garbage once every other shape, each collection copying a share of that growing state: in a
  # synthetic 100,000-shape benchmark, 26 s of a 41 s filter restore went to 44,800 collections.
  use ExUnit.Case, async: true

  import Support.ComponentSetup

  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @moduletag :tmp_dir
  @moduletag shape_db_opts: [exclusive_mode: true]

  @shape_count 1_000

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"}
               ]
             )

  setup [:with_stack_id_from_test, :with_shape_db, :with_firm_perimeter_shapes]

  test "a walk does not collect garbage for every few shapes", ctx do
    {handles, collections} =
      count_collections(fn ->
        ShapeDb.reduce_shapes(ctx.stack_id, [], fn {handle, _shape}, handles ->
          [handle | handles]
        end)
      end)

    assert length(handles) == @shape_count

    assert collections < div(@shape_count, 20),
           "#{collections} collections to walk #{@shape_count} shapes"
  end

  test "the walk leaves its caller on the heap floor it had", ctx do
    in_new_process(fn ->
      before = heap_floor()
      ShapeDb.reduce_shapes(ctx.stack_id, nil, fn _shape, acc -> acc end)

      assert heap_floor() == before
    end)
  end

  test "a caller configured with more room than the walk asks for keeps it", ctx do
    in_new_process(fn ->
      Process.flag(:min_heap_size, 32 * 1024 * 1024)
      configured = heap_floor()[:min_heap_size]

      during =
        ShapeDb.reduce_shapes(ctx.stack_id, nil, fn _shape, _acc -> heap_floor() end)

      assert during[:min_heap_size] == configured
      assert heap_floor()[:min_heap_size] == configured
    end)
  end

  defp heap_floor do
    {:garbage_collection, settings} = Process.info(self(), :garbage_collection)
    Keyword.take(settings, [:min_heap_size, :min_bin_vheap_size])
  end

  defp in_new_process(fun), do: fun |> Task.async() |> Task.await(:infinity)

  # The collections of the process that runs `fun`, and of that process alone.
  defp count_collections(fun) do
    test = self()

    task =
      Task.async(fn ->
        :erlang.trace(self(), true, [:garbage_collection, {:tracer, test}])
        fun.()
      end)

    result = Task.await(task, :infinity)
    delivered = :erlang.trace_delivered(task.pid)
    assert_receive {:trace_delivered, _pid, ^delivered}

    {result, count_trace_messages(task.pid, 0)}
  end

  defp count_trace_messages(pid, count) do
    receive do
      {:trace, ^pid, kind, _info} when kind in [:gc_minor_start, :gc_major_start] ->
        count_trace_messages(pid, count + 1)

      {:trace, ^pid, _kind, _info} ->
        count_trace_messages(pid, count)
    after
      0 -> count
    end
  end

  # The where clause of a firm perimeter: one firm, the divisions its user can see.
  defp with_firm_perimeter_shapes(ctx) do
    for i <- 1..@shape_count do
      divisions = Enum.map_join(1..45, ", ", &"'division-#{i}-#{&1}'")

      shape =
        Shape.new!("employees",
          inspector: @inspector,
          where: ~s|"firmId" = 'firm-#{i}' AND "divisionId" IN (#{divisions})|
        )

      {:ok, _digest} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-#{i}")
    end

    :ok = WriteBuffer.flush_sync(ctx.stack_id, 120_000)
  end
end
