defmodule Electric.Shapes.Api.ServeWatchdog do
  @moduledoc """
  Terminates shape response serves whose clients stop accepting data.

  A serve writing to a socket whose peer stops draining blocks inside the
  socket write and cannot recover on its own: the kernel-level TCP send
  timeout fires only when a driver-level send makes no progress for its
  entire window, and in practice a connection draining even a trickle keeps
  resetting it. Such serves never complete, so they emit no telemetry, and
  each pins its in-flight response data and a file descriptor for as long as
  the connection survives — a population of them accumulates with connection
  count (reproduced in test/integration/stalled_serve_memory_test.exs).

  ## Mechanism

  Around each bounded socket write, the request handler arms a BIF timer
  (`:erlang.start_timer/3`) addressed at this server and cancels it when the
  write completes. Timers are per-scheduler O(1) and a cancelled timer never
  sends anything, so this server's load is proportional to *stalls* — the
  rare event it exists for — rather than to write throughput, and deadlines
  fire exactly rather than with sweep slack.

  Each write has a number, kept in an atomic of the handler while the write
  is armed. Disarming swaps it back to 0; before it kills, this server swaps
  it to -1. Only one of the two swaps can succeed, so a timeout racing a
  completed write (or landing after the handler has moved on to another
  request on a keepalive connection) is a no-op. When this server wins, a
  handler that reaches `disarm/1` before the kill lands kills itself there,
  so the kill can never land on whatever the handler would do next.

  ## What a kill must clean up

  `Process.exit(pid, :kill)` destroys the handler without running another
  instruction. Its admission permit is owned by the handler process (see
  `Electric.AdmissionControl.try_acquire_owned/3`), so AdmissionControl gives
  it back when the handler exits; this server does not touch it. On
  observing the `:killed` exit this server logs a warning and emits a
  `[:electric, :plug, :serve_shape, :reaped]` telemetry event with the stack
  and shape, since the handler's own telemetry emission dies with it and
  reaped serves would otherwise be invisible to request metrics.

  The handler's root OTel span is not ended by the kill; orphaned spans are
  dropped by the SDK's sweeper.

  ## Scope

  Serves are only guarded under adapters where the plug runs in the
  socket-owning process (Bandit HTTP/1) — see `Response.send_stream/2` for
  the gate. Arming toward a stack without a running server is a silent
  no-op, so serving degrades to unguarded rather than failing. A serve
  resolves this server once, when its response starts, so a serve in
  flight while this server restarts stays unguarded until it ends.
  """
  use GenServer

  require Logger

  @writes_key {__MODULE__, :writes}
  # Slots of the per-handler atomic: the number of the armed write (0 when none, -1 once this
  # server has claimed it), and the counter that numbers writes.
  @armed 1
  @counter 2
  @claimed -1

  def start_link(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    GenServer.start_link(__MODULE__, stack_id, name: name(stack_id))
  end

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  @doc """
  Resolve the watchdog for a stack, or nil if none is running — including
  when the stack's process registry itself is absent, so callers uniformly
  degrade to unguarded serving.
  """
  def resolve(stack_id) do
    GenServer.whereis(name(stack_id))
  rescue
    ArgumentError -> nil
  end

  @doc """
  Arm a deadline for a socket write the calling process is about to make.

  Returns the write to pass to `disarm/1` once the write completes.
  """
  def arm(watchdog, shape_handle, timeout_ms) when is_pid(watchdog) do
    writes = writes()
    write = :atomics.add_get(writes, @counter, 1)
    :atomics.put(writes, @armed, write)

    timer =
      :erlang.start_timer(
        timeout_ms,
        watchdog,
        {:stalled_write, self(), shape_handle, writes, write}
      )

    {timer, writes, write}
  end

  @doc """
  Disarm a deadline after the write completed.

  If the watchdog claimed the write first, the calling process is being
  killed: it kills itself here rather than go on to anything else.
  """
  def disarm({timer, writes, write}) do
    :erlang.cancel_timer(timer, async: true, info: false)

    if :atomics.compare_exchange(writes, @armed, write, 0) != :ok do
      Process.exit(self(), :kill)
    end

    :ok
  end

  defp writes do
    case Process.get(@writes_key) do
      nil ->
        writes = :atomics.new(2, signed: true)
        Process.put(@writes_key, writes)
        writes

      writes ->
        writes
    end
  end

  @impl GenServer
  def init(stack_id) do
    Process.set_label({:serve_watchdog, stack_id})
    Logger.metadata(stack_id: stack_id)
    {:ok, %{stack_id: stack_id, pending_kills: %{}}}
  end

  @impl GenServer
  def handle_info({:timeout, _timer, {:stalled_write, pid, shape_handle, writes, write}}, state) do
    # Monitor before claiming: once the claim succeeds the handler may kill itself in
    # `disarm/1` before the exit below lands, and that must still be seen as :killed.
    mref = Process.monitor(pid)

    if :atomics.compare_exchange(writes, @armed, write, @claimed) == :ok do
      # The handler is blocked inside a socket write and cannot process
      # messages, so only an untrappable exit can terminate it.
      Process.exit(pid, :kill)
      {:noreply, put_in(state.pending_kills[mref], %{shape_handle: shape_handle})}
    else
      # The write completed (or the handler moved on to another write) before
      # the cancel could beat the firing timer.
      Process.demonitor(mref, [:flush])
      {:noreply, state}
    end
  end

  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    {entry, pending_kills} = Map.pop(state.pending_kills, mref)

    # A handler already dead when its timer fired is down with :noproc: it was not reaped.
    if entry && reason == :killed do
      Logger.warning(
        "Terminated stalled shape response serve: client did not accept an " <>
          "in-flight write within its deadline",
        shape_handle: entry.shape_handle
      )

      :telemetry.execute([:electric, :plug, :serve_shape, :reaped], %{count: 1}, %{
        stack_id: state.stack_id,
        shape_handle: entry.shape_handle
      })
    end

    {:noreply, %{state | pending_kills: pending_kills}}
  end
end
