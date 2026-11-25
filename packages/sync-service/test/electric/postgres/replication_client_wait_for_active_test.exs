defmodule Electric.Postgres.ReplicationClientWaitForActiveTest do
  @moduledoc """
  A parked replication event waits on the status monitor through `StatusMonitor.wait_until_async/2`,
  which leaves a process monitor on the replication client. These cases pin that the client drops
  that monitor when the wait ends, since it lives as long as the connection and would otherwise
  carry one more monitor for every stack that was not ready when an event arrived.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Support.ComponentSetup, only: [with_stack_id_from_test: 1, with_registry: 1]

  alias Electric.Postgres.ReplicationClient
  alias Electric.Postgres.ReplicationClient.MessageConverter
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.StatusMonitor

  setup [:with_stack_id_from_test, :with_registry]

  setup ctx do
    {:ok, status_monitor} =
      Agent.start_link(fn -> nil end)

    # A stand-in for the status monitor process: the wait only monitors its pid.
    ref = Process.monitor(status_monitor, tag: {StatusMonitor, :down})

    state = %State{
      handle_event: {__MODULE__, :unused, []},
      publication_name: "test_pub",
      stack_id: ctx.stack_id,
      step: :streaming,
      message_converter: MessageConverter.new(max_tx_size: nil, max_batch_size: 100),
      wait_for_active_ref: {ref, :parked_event}
    }

    %{status_monitor: status_monitor, ref: ref, state: state}
  end

  test "drops the monitor when the stack reports active", ctx do
    assert {:process, ctx.status_monitor} in monitors()

    capture_log(fn ->
      assert {:noreply, %State{wait_for_active_ref: nil}} =
               ReplicationClient.handle_info(
                 {{StatusMonitor, ctx.ref}, {:ok, :active}},
                 ctx.state
               )
    end)

    refute {:process, ctx.status_monitor} in monitors()
  end

  test "drops the monitor on a stale reply", ctx do
    stale = %{ctx.state | wait_for_active_ref: nil}

    assert {:noreply, ^stale} =
             ReplicationClient.handle_info({{StatusMonitor, ctx.ref}, {:ok, :active}}, stale)

    refute {:process, ctx.status_monitor} in monitors()
  end

  test "reads the status monitor going down as the end of the wait", ctx do
    Process.unlink(ctx.status_monitor)
    Agent.stop(ctx.status_monitor)
    assert_receive {{StatusMonitor, :down}, ref, :process, _pid, _reason}
    assert ref == ctx.ref

    capture_log(fn ->
      assert {:noreply, %State{wait_for_active_ref: nil}} =
               ReplicationClient.handle_info(
                 {{StatusMonitor, :down}, ref, :process, ctx.status_monitor, :normal},
                 ctx.state
               )
    end)
  end

  defp monitors do
    {:monitors, monitors} = Process.info(self(), :monitors)
    monitors
  end
end
