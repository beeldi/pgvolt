defmodule Electric.Shapes.Api.ServeWatchdogTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Support.ComponentSetup, only: [with_stack_id_from_test: 1]

  alias Electric.AdmissionControl
  alias Electric.Shapes.Api.ServeWatchdog

  @shape_handle "the-shape-handle"

  setup :with_stack_id_from_test

  setup ctx do
    watchdog = start_supervised!({ServeWatchdog, stack_id: ctx.stack_id})
    [watchdog: watchdog]
  end

  # Spawns a process standing in for a request handler: it runs `setup_fun`
  # (arm/disarm must be called from the process being watched), reports back,
  # then hangs like a handler blocked in a socket write.
  defp victim(setup_fun) do
    parent = self()

    pid =
      spawn(fn ->
        result = setup_fun.()
        send(parent, {:registered, result})
        Process.sleep(:infinity)
      end)

    ref = Process.monitor(pid)
    assert_receive {:registered, result}
    {pid, ref, result}
  end

  test "kills a process whose write outlives its deadline", ctx do
    watchdog = ctx.watchdog
    events_ref = attach_reap_handler(ctx)

    log =
      capture_log(fn ->
        {pid, ref, _} = victim(fn -> ServeWatchdog.arm(watchdog, @shape_handle, 30) end)

        assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
        # The warning is logged just before the reap event.
        assert_receive {:reaped, ^events_ref, _meta}, 1_000
      end)

    assert log =~ "Terminated stalled shape response serve"
  end

  test "does not kill a write that is still within its deadline", ctx do
    watchdog = ctx.watchdog
    {pid, ref, _} = victim(fn -> ServeWatchdog.arm(watchdog, @shape_handle, 60_000) end)

    refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
    assert Process.alive?(pid)
  end

  test "does not kill a process whose write was disarmed", ctx do
    watchdog = ctx.watchdog

    {pid, ref, _} =
      victim(fn ->
        timer = ServeWatchdog.arm(watchdog, @shape_handle, 30)
        ServeWatchdog.disarm(timer)
      end)

    refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
    assert Process.alive?(pid)
  end

  test "a timeout that fires after the write completed is a no-op", ctx do
    # The race an asynchronous cancel cannot prevent: the timer fires and its
    # message is already in flight when the handler disarms. Delivering such
    # a stale timeout must not kill the handler — disarm ends the write, so
    # the watchdog cannot claim it.
    watchdog = ctx.watchdog

    {pid, ref, write} =
      victim(fn ->
        write = ServeWatchdog.arm(watchdog, @shape_handle, 60_000)
        ServeWatchdog.disarm(write)
        write
      end)

    send(watchdog, fired(pid, write))

    refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
    assert Process.alive?(pid)
  end

  test "a timeout for a re-armed write does not kill on the stale ref", ctx do
    # Same race, one step later: the handler has moved on to its next write
    # when a stale timeout for the previous one arrives.
    watchdog = ctx.watchdog

    {pid, ref, old_write} =
      victim(fn ->
        write = ServeWatchdog.arm(watchdog, @shape_handle, 60_000)
        ServeWatchdog.disarm(write)
        _next = ServeWatchdog.arm(watchdog, @shape_handle, 60_000)
        write
      end)

    send(watchdog, fired(pid, old_write))

    refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
    assert Process.alive?(pid)
  end

  test "a handler that finished its write and moved on is not killed for it", ctx do
    # The watchdog decides to kill, then is slowed down (here by a log handler that blocks)
    # before the kill lands. The handler must not be able to finish its write and move on
    # to its next one in between, or the kill would land on the next write.
    watchdog = ctx.watchdog
    handler_id = :"serve_watchdog_hold_#{System.unique_integer([:positive])}"

    :ok =
      :logger.add_handler(handler_id, __MODULE__.HoldingLogHandler, %{
        config: %{watchdog: watchdog, test: self()}
      })

    on_exit(fn -> :logger.remove_handler(handler_id) end)

    parent = self()

    pid =
      spawn(fn ->
        write = ServeWatchdog.arm(watchdog, @shape_handle, 30)
        send(parent, :armed)

        receive do
          :finish_write -> ServeWatchdog.disarm(write)
        end

        _next = ServeWatchdog.arm(watchdog, @shape_handle, 60_000)
        send(parent, :moved_on)
        Process.sleep(:infinity)
      end)

    ref = Process.monitor(pid)
    assert_receive :armed
    assert_receive {:holding, ^watchdog}, 1_000

    send(pid, :finish_write)
    Process.sleep(100)
    send(watchdog, :release)

    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    refute_received :moved_on, "the handler was killed after it had moved on to its next write"
  end

  test "a timeout for a handler that died on its own is a no-op", ctx do
    watchdog = ctx.watchdog
    events_ref = attach_reap_handler(ctx)
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {:write, ServeWatchdog.arm(watchdog, @shape_handle, 60_000)})
      end)

    assert_receive {:write, write}
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    # Deliver a fire for the dead pid directly; the watchdog must survive and report nothing.
    send(watchdog, fired(pid, write))
    refute_receive {:reaped, ^events_ref, _meta}, 200
    assert Process.alive?(watchdog)
  end

  test "a handler that finishes its write after the watchdog claimed it stops there", ctx do
    # The watchdog has claimed the write but its kill has not landed yet: the handler's
    # disarm must not return, so the handler cannot go on to anything else.
    watchdog = ctx.watchdog
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        write = ServeWatchdog.arm(watchdog, @shape_handle, 60_000)
        send(parent, {:write, write})

        receive do
          :finish_write -> ServeWatchdog.disarm(write)
        end

        send(parent, :moved_on)
      end)

    assert_receive {:write, {_timer, writes, write}}
    # The watchdog's claim, as it makes it just before the kill.
    :ok = :atomics.compare_exchange(writes, 1, write, -1)

    send(pid, :finish_write)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    refute_received :moved_on
  end

  test "the killed handler's admission permit is given back exactly once", ctx do
    watchdog = ctx.watchdog
    stack_id = ctx.stack_id

    events_ref = attach_reap_handler(ctx)

    # Another request's permit, which must survive the reap.
    :ok = AdmissionControl.try_acquire(stack_id, :existing, max_concurrent: 10)

    {pid, ref, _} =
      victim(fn ->
        :ok = AdmissionControl.try_acquire_owned(stack_id, :existing, max_concurrent: 10)
        ServeWatchdog.arm(watchdog, @shape_handle, 30)
      end)

    assert %{existing: 2} = AdmissionControl.get_current(stack_id)

    capture_log(fn ->
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

      assert_receive {:reaped, ^events_ref, %{stack_id: ^stack_id, shape_handle: @shape_handle}},
                     1_000
    end)

    # AdmissionControl gives the owned permit back once it sees the exit.
    assert_eventually_existing(stack_id, 1)
  end

  test "a killed handler without a permit releases nothing", ctx do
    watchdog = ctx.watchdog
    stack_id = ctx.stack_id

    events_ref = attach_reap_handler(ctx)

    capture_log(fn ->
      {pid, ref, _} = victim(fn -> ServeWatchdog.arm(watchdog, @shape_handle, 30) end)

      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      assert_receive {:reaped, ^events_ref, _meta}, 1_000
    end)

    assert %{existing: 0, initial: 0} = AdmissionControl.get_current(stack_id)
  end

  test "resolve is nil without a running watchdog or without a stack at all", ctx do
    stop_supervised!(ServeWatchdog)
    assert ServeWatchdog.resolve(ctx.stack_id) == nil
    assert ServeWatchdog.resolve(ctx.stack_id <> "-no-such-stack") == nil
  end

  defmodule HoldingLogHandler do
    # Holds the watchdog inside its warning until the test sends it :release.
    def log(%{level: :warning, meta: %{pid: pid}}, %{config: %{watchdog: pid, test: test}}) do
      send(test, {:holding, pid})

      receive do
        :release -> :ok
      after
        5_000 -> :ok
      end
    end

    def log(_event, _config), do: :ok
  end

  defp fired(pid, {timer, writes, write}),
    do: {:timeout, timer, {:stalled_write, pid, @shape_handle, writes, write}}

  defp assert_eventually_existing(stack_id, expected, attempts \\ 100) do
    case AdmissionControl.get_current(stack_id) do
      %{existing: ^expected} ->
        :ok

      _current when attempts > 0 ->
        Process.sleep(10)
        assert_eventually_existing(stack_id, expected, attempts - 1)

      current ->
        flunk("expected #{expected} existing permits, got #{inspect(current)}")
    end
  end

  defp attach_reap_handler(ctx) do
    events_ref = make_ref()
    parent = self()
    handler_id = {ctx.stack_id, :reap_test}

    :telemetry.attach(
      handler_id,
      [:electric, :plug, :serve_shape, :reaped],
      fn _event, _measurements, meta, _config -> send(parent, {:reaped, events_ref, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    events_ref
  end
end
