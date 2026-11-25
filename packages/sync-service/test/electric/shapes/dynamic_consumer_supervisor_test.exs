defmodule Electric.Shapes.DynamicConsumerSupervisorTest do
  @moduledoc """
  The partition count is fixed for the life of the supervisor, so a store that outgrows it stays
  at that density. When no shape ceiling is configured the count falls back to the scheduler
  count — the same unset setting that disables expiry — so the stack allowed to grow without
  bound is also the one sized as though it would not. The boot line has to say which input chose
  the number, or an under-sized stack is indistinguishable from a deliberately sized one.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Support.ComponentSetup, only: [with_stack_id_from_test: 1, with_registry: 1]

  alias Electric.Shapes.DynamicConsumerSupervisor

  setup [:with_stack_id_from_test, :with_registry]

  defp start(ctx, opts) do
    capture_log(fn ->
      pid = start_supervised!({DynamicConsumerSupervisor, [stack_id: ctx.stack_id] ++ opts})
      assert is_pid(pid)
    end)
  end

  test "names the scheduler-count fallback and the density it implies", ctx do
    # Act
    log = start(ctx, [])

    # Assert
    assert log =~ "sized from the scheduler count because no shape ceiling is configured"
    assert log =~ "4000 shapes per partition"
  end

  test "attributes the count to the ceiling when one is configured", ctx do
    # Act
    log = start(ctx, max_shapes: 40_000)

    # Assert
    assert log =~ "for a ceiling of 40000 shapes"
    refute log =~ "no shape ceiling is configured"
  end

  test "an explicit partition count is reported as the operator's own choice", ctx do
    # Act
    log = start(ctx, partitions: 2, max_shapes: 40_000)

    # Assert: the ceiling did not choose this, so attributing it to one would be a lie.
    assert log =~ "with 2 partitions (configured)"
    refute log =~ "ceiling"
  end
end
