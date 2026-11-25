defmodule Electric.ShapeCache.ColdStartCostTest do
  # Not async: async modules start while mix is still compiling the remaining test files, and on
  # a 2-vCPU runner that made the same step up to 50x slower from one run to the next.
  use ExUnit.Case, async: false
  use Repatch.ExUnit, assert_expectations: true

  alias Electric.ShapeCache
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes
  alias Electric.Shapes.Shape

  import Support.ComponentSetup

  @stub_inspector Support.StubInspector.new(
                    tables: [{1, {"public", "items"}}],
                    columns: [
                      %{
                        name: "id",
                        type: "int8",
                        type_id: {20, 1},
                        pk_position: 0,
                        is_generated: false
                      },
                      %{name: "value", type: "text", type_id: {25, 1}, is_generated: false}
                    ]
                  )

  # {xmin, xmax, xip_list}
  @pg_snapshot_xmin_10 {10, 11, [10]}

  @moduletag :tmp_dir

  setup do
    %{inspector: @stub_inspector, pool: nil}
  end

  setup [
    :with_persistent_kv,
    :with_stack_id_from_test,
    :with_async_deleter,
    :with_pure_file_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_shape_cleaner,
    :with_status_monitor
  ]

  # `get_or_create_shape_handle/3` falls through to a `GenServer.call` on a cache miss, and the
  # handler does the whole creation - the ShapeDb write and the consumer start - inside the
  # ShapeCache process. Every creation on a cold stack is therefore serialised through one mailbox,
  # and each waiting caller holds a slot for the full duration against a 30s call timeout. What that
  # costs a cold start is one number times the shape population, and the population can be large,
  # so the number is worth having. Snapshots are stubbed: this measures the serialised segment, not
  # the per-shape Postgres read that runs off it.
  describe "cold-start shape creation cost" do
    setup [
      :with_noop_publication_manager,
      :with_log_chunking,
      :with_registry,
      :with_shape_log_collector
    ]

    @tag :performance
    @tag timeout: :infinity
    test "reports the serialised per-shape cost of get_or_create_shape_handle/3", ctx do
      Support.TestUtils.patch_snapshotter(fn parent, shape_handle, _shape, _opts ->
        GenServer.cast(parent, {:pg_snapshot_known, shape_handle, @pg_snapshot_xmin_10})
        GenServer.cast(parent, {:snapshot_started, shape_handle})
      end)

      with_shape_cache(ctx)

      shapes =
        Enum.map(1..200, fn n ->
          Shape.new!("items", where: "id = #{n}", inspector: @stub_inspector)
        end)

      {micros, _} =
        :timer.tc(fn ->
          Enum.each(shapes, &ShapeCache.get_or_create_shape_handle(&1, ctx.stack_id))
        end)

      per_shape = micros / length(shapes)

      IO.puts(
        "\n### cold-start creation: #{Float.round(per_shape / 1000, 2)} ms/shape " <>
          "=> #{Float.round(per_shape * 100_000 / 1_000_000, 1)}s to recreate 100k serially ###"
      )

      assert ShapeCache.count_shapes(ctx.stack_id) == length(shapes)
    end

    # The sequential number above says what the queue drains at. It cannot say what a client
    # experiences, because a client does not wait for its own creation - it waits for everyone ahead
    # of it. Reconnecting clients arrive together and ask for different shapes, so they queue rather
    # than coalesce, and the last one's wait is the whole queue. `@call_timeout` is 30s, and a caller
    # that gives up retries into the same queue.
    @tag :performance
    @tag timeout: :infinity
    test "reports what the last concurrent caller waits for", ctx do
      Support.TestUtils.patch_snapshotter(fn parent, shape_handle, _shape, _opts ->
        GenServer.cast(parent, {:pg_snapshot_known, shape_handle, @pg_snapshot_xmin_10})
        GenServer.cast(parent, {:snapshot_started, shape_handle})
      end)

      with_shape_cache(ctx)

      callers = 200

      shapes =
        Enum.map(1..callers, fn n ->
          Shape.new!("items", where: "id = #{n}", inspector: @stub_inspector)
        end)

      waits =
        shapes
        |> Enum.map(fn shape ->
          Task.async(fn ->
            {micros, _} =
              :timer.tc(fn -> ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id) end)

            micros
          end)
        end)
        |> Task.await_many(:infinity)

      slowest = Enum.max(waits)
      per_shape_ms = Enum.sum(waits) / callers / 1000

      IO.puts(
        "\n### #{callers} concurrent callers, distinct shapes: slowest waited " <>
          "#{Float.round(slowest / 1000, 1)} ms; mean wait #{Float.round(per_shape_ms, 1)} ms. " <>
          "A caller sees the 30s timeout once ~#{round(30_000 / (slowest / callers / 1000))} " <>
          "are queued ahead of it ###"
      )

      assert ShapeCache.count_shapes(ctx.stack_id) == callers
    end

    # What the ShapeCache handler itself does, against what a caller actually waits for. Every step
    # the handler runs is timed directly against real calls, because the two obvious shortcuts both
    # lie here:
    #
    #   Deriving the expensive step as a remainder over the caller's wall time attributes machine
    #   load to the handler. The caller waits on a box that each creation has just given a pile of
    #   asynchronous consumer initialisation to, and that is not queueing behind the handler.
    #
    #   Profilers misrank this chain outright - `eprof` blames the write buffer's `ets:insert/2` for
    #   ~65% of the process's own time, and `fprof` ranks `ShapeStatus.add_shape` above the consumer
    #   start on accumulated time. Measured directly, both are a rounding error.
    #
    # `Consumer.initialize_shape/3` is not in the sum because it is a bare `send/2`: its own cost is
    # unmeasurable, and what it triggers runs in another process. That asynchronous work is the
    # difference between the two numbers this test prints.
    #
    # Absolutes move with the runner, so read the ratio, which is measured in one run.
    @tag :performance
    @tag timeout: :infinity
    test "the handler's own work is a fraction of what a caller waits for", ctx do
      Support.TestUtils.patch_snapshotter(fn parent, shape_handle, _shape, _opts ->
        GenServer.cast(parent, {:pg_snapshot_known, shape_handle, @pg_snapshot_xmin_10})
        GenServer.cast(parent, {:snapshot_started, shape_handle})
      end)

      with_shape_cache(ctx)

      count = 200

      step_shapes =
        Enum.map(1..count, fn n ->
          Shape.new!("items", where: "id = #{200_000 + n}", inspector: @stub_inspector)
        end)

      full_shapes =
        Enum.map(1..count, fn n ->
          Shape.new!("items", where: "id = #{n}", inspector: @stub_inspector)
        end)

      {lookup_micros, _} =
        :timer.tc(fn ->
          Enum.each(full_shapes, &ShapeStatus.fetch_handle_by_shape_critical(ctx.stack_id, &1))
        end)

      {catalog_micros, handles} =
        :timer.tc(fn ->
          Enum.map(step_shapes, fn shape ->
            {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
            handle
          end)
        end)

      {spawn_micros, _} =
        :timer.tc(fn ->
          Enum.each(handles, fn handle ->
            {:ok, _pid} =
              Shapes.DynamicConsumerSupervisor.start_shape_consumer(ctx.stack_id, %{
                stack_id: ctx.stack_id,
                shape_handle: handle
              })
          end)
        end)

      {last_read_micros, _} =
        :timer.tc(fn ->
          Enum.each(handles, &ShapeStatus.update_last_read_time_to_now(ctx.stack_id, &1))
        end)

      {full_micros, _} =
        :timer.tc(fn ->
          Enum.each(full_shapes, &ShapeCache.get_or_create_shape_handle(&1, ctx.stack_id))
        end)

      handler_micros = lookup_micros + catalog_micros + spawn_micros + last_read_micros
      per = fn m -> Float.round(m / count / 1000, 3) end

      IO.puts(
        "\n### handler steps #{per.(handler_micros)} ms/shape" <>
          " (lookup #{per.(lookup_micros)}, catalog #{per.(catalog_micros)}," <>
          " spawn #{per.(spawn_micros)}, last_read #{per.(last_read_micros)})" <>
          " | caller-observed creation #{per.(full_micros)} ms/shape ###"
      )

      assert ShapeCache.count_shapes(ctx.stack_id) == length(step_shapes) + length(full_shapes)
      assert handler_micros * 2 < full_micros
    end

    # Same-shape callers are the case upstream #4372 addresses: the work is deduplicated inside the
    # handler, so only the queueing remains. Measuring it separately is what says whether coalescing
    # would help a workload where reconnecting clients mostly want *different* shapes.
    @tag :performance
    @tag timeout: :infinity
    test "reports what concurrent callers for one shape wait for", ctx do
      Support.TestUtils.patch_snapshotter(fn parent, shape_handle, _shape, _opts ->
        GenServer.cast(parent, {:pg_snapshot_known, shape_handle, @pg_snapshot_xmin_10})
        GenServer.cast(parent, {:snapshot_started, shape_handle})
      end)

      with_shape_cache(ctx)

      callers = 200
      shape = Shape.new!("items", where: "id = 1", inspector: @stub_inspector)

      waits =
        1..callers
        |> Enum.map(fn _ ->
          Task.async(fn ->
            {micros, _} =
              :timer.tc(fn -> ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id) end)

            micros
          end)
        end)
        |> Task.await_many(:infinity)

      IO.puts(
        "\n### #{callers} concurrent callers, ONE shape: slowest waited " <>
          "#{Float.round(Enum.max(waits) / 1000, 1)} ms; mean " <>
          "#{Float.round(Enum.sum(waits) / callers / 1000, 1)} ms ###"
      )

      assert ShapeCache.count_shapes(ctx.stack_id) == 1
    end
  end
end
