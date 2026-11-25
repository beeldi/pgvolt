defmodule ElectricTelemetry.SystemMonitorTest do
  use ExUnit.Case, async: false

  @opts %{
    intervals_and_thresholds: %{
      long_gc_threshold: 50,
      long_schedule_threshold: 150,
      long_message_queue_enable_threshold: 20,
      long_message_queue_disable_threshold: 0
    }
  }

  setup do
    on_exit(fn -> :erlang.system_monitor(:undefined) end)
  end

  test "schedules periodic self garbage collection" do
    {:ok, pid} = start_supervised({ElectricTelemetry.SystemMonitor, @opts})

    %{garbage_collect_timer: first_timer} = :sys.get_state(pid)
    assert is_reference(first_timer)

    send(pid, :periodic_garbage_collect)
    Process.sleep(50)

    %{garbage_collect_timer: next_timer} = :sys.get_state(pid)
    assert is_reference(next_timer)
    assert next_timer != first_timer
  end

  test "prunes dead pids from the tracked message-queue set on recheck" do
    {:ok, pid} = start_supervised({ElectricTelemetry.SystemMonitor, @opts})

    dead_pid = spawn(fn -> :ok end)
    ref = Process.monitor(dead_pid)
    assert_receive {:DOWN, ^ref, :process, ^dead_pid, _reason}

    send(pid, {:monitor, dead_pid, :long_message_queue, true})
    assert MapSet.member?(:sys.get_state(pid).long_message_queue_pids, dead_pid)

    send(pid, :recheck_message_queues)

    refute MapSet.member?(:sys.get_state(pid).long_message_queue_pids, dead_pid)
  end
end
