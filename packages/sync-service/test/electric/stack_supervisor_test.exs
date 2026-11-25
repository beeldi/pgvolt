defmodule Electric.StackSupervisorTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  alias Electric.StackSupervisor

  import Support.ComponentSetup
  import Support.DbSetup

  describe "settings" do
    @describetag :tmp_dir
    @describetag stack_tweaks: [subquery_buffer_max_transactions: 3]
    setup [:with_unique_db, :with_complete_stack]

    test "a stack keeps the subquery buffer limit it was started with", ctx do
      # Subquery shapes read the limit from the stack's config when they start.
      assert Electric.StackConfig.lookup(ctx.stack_id, :subquery_buffer_max_transactions) == 3
    end
  end

  describe "Telemetry" do
    setup [:with_stack_id_from_test]

    test "count_shapes/2 emits split shape metrics", ctx do
      stack_id = ctx.stack_id

      Repatch.patch(Electric.ShapeCache, :shape_counts, fn _stack_id ->
        %{total: 7, indexed: 4, unindexed: 3}
      end)

      Repatch.patch(Electric.Shapes.ConsumerRegistry, :active_consumer_count, fn _stack_id ->
        2
      end)

      handler_id = {__MODULE__, make_ref()}

      :telemetry.attach_many(
        handler_id,
        [
          [:electric, :shapes, :total_shapes],
          [:electric, :shapes, :active_shapes]
        ],
        fn event_name, measurements, metadata, pid ->
          send(pid, {event_name, measurements, metadata})
        end,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      StackSupervisor.Telemetry.count_shapes(stack_id, %{})

      assert_receive {[:electric, :shapes, :total_shapes],
                      %{count: 7, count_indexed: 4, count_unindexed: 3}, %{stack_id: ^stack_id}}

      assert_receive {[:electric, :shapes, :active_shapes], %{count: 2}, %{stack_id: ^stack_id}}
    end

    # At boot the shapes are counted from the collector's restore, and an instance waiting for
    # the lock does not restore them: until then there is no total to report.
    test "count_shapes/2 emits no shape totals before the shapes are counted", ctx do
      stack_id = ctx.stack_id

      Repatch.patch(Electric.ShapeCache, :shape_counts, fn _stack_id -> :error end)

      Repatch.patch(Electric.Shapes.ConsumerRegistry, :active_consumer_count, fn _stack_id ->
        0
      end)

      handler_id = {__MODULE__, make_ref()}

      :telemetry.attach_many(
        handler_id,
        [
          [:electric, :shapes, :total_shapes],
          [:electric, :shapes, :active_shapes]
        ],
        fn event_name, measurements, metadata, pid ->
          send(pid, {event_name, measurements, metadata})
        end,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert :ok = StackSupervisor.Telemetry.count_shapes(stack_id, %{})

      assert_receive {[:electric, :shapes, :active_shapes], %{count: 0}, %{stack_id: ^stack_id}}
      refute_received {[:electric, :shapes, :total_shapes], _, _}
    end
  end
end
