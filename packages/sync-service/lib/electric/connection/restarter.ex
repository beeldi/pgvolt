defmodule Electric.Connection.Restarter do
  @moduledoc """
  Gen server responsible for shutting down and restarting the connection subsystem.

  It makes sure to update StatusMonitor with the current subsystem state to maintain correct
  behaviour of other components of the system that depend on the database availability, such
  as:

    - HTTP API server processing shape requests
    - publication manager
    - schema reconciler

  """

  use GenServer

  alias Electric.StatusMonitor

  def name(stack_ref) do
    Electric.ProcessRegistry.name(stack_ref, __MODULE__)
  end

  @doc """
  Stop the connection subsystem, closing all database connections.

  This lets the database server scale its compute to zero if it supports this feature and has
  no other sessions.

  Inside Electric, the shape subsystem keeps running.

  ## Implementation notes

  Currently, this function stops only the Connection.Manager process which shuts down all types
  of database connections linked to it. When a new shape request arrives, it will immediately
  stop the Shapes.Supervisor and restart the Connection.Manager, which then in turn starts
  a fresh Shapes.Supervisor again.
  """
  def stop_connection_subsystem(stack_id) do
    GenServer.cast(name(stack_id), :stop_connection_subsystem)
  end

  @doc """
  Restore the connection subsystem after it had been stopped by `stop_connection_subsystem/1`.

  ## Implementation notes
  To restore the subsystem, the Shapes.Supervisor is stopped first before getting
  restarted by the Connection.Manager later. The Connection.Manager itself is started
  via a `Supervisor.restart_child()` call.
  """
  def restore_connection_subsystem(stack_id) do
    with %{conn: :sleeping} <- StatusMonitor.status(stack_id) do
      GenServer.cast(name(stack_id), :restore_connection_subsystem)
    end

    :ok
  end

  @doc """
  The connection manager running now, or `nil` while none runs: between a stop and the start that
  follows it, or while the database connections sleep.

  A connection manager stands for one run of the connection subsystem. It owns the admin pool and
  the replication client, which sets the publication up as it starts. Once it stops, its
  supervisor, or a new one, starts another in its place.
  """
  def connection_manager(stack_id) do
    GenServer.whereis(Electric.Connection.Manager.name(stack_id))
  end

  @doc """
  Restart the connection subsystem.

  A request names a connection manager, by default the one running when it is made. A caller that
  decides from something it checked names the manager `connection_manager/1` returned before the
  check. Once that manager has stopped, another has replaced it or will, and the request is
  answered without restarting again.
  """
  def restart_connection_subsystem(stack_id) do
    restart_connection_subsystem(stack_id, connection_manager(stack_id))
  end

  def restart_connection_subsystem(stack_id, manager) do
    GenServer.call(name(stack_id), {:restart_connection_subsystem, manager})
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name(opts))
  end

  def init(opts) do
    # wait_until_conn_up_ref is used as an exclusion mechanism when the database connections are
    # sleeping: multiple concurrent shape requests will trigger the restart of the
    # connection subsystem once because they will all be serialized through this Restarter
    # process.
    {:ok,
     %{
       stack_id: Keyword.fetch!(opts, :stack_id),
       stack_events_registry: Keyword.fetch!(opts, :stack_events_registry),
       wait_until_conn_up_ref: nil
     }}
  end

  def handle_cast(:stop_connection_subsystem, state) do
    StatusMonitor.database_connections_going_to_sleep(state.stack_id)
    Electric.Connection.Manager.Supervisor.stop_connection_manager(stack_id: state.stack_id)

    Electric.StackSupervisor.dispatch_stack_event(
      state.stack_events_registry,
      state.stack_id,
      :scaled_down_database_connections
    )

    {:noreply, state}
  end

  def handle_cast(:restore_connection_subsystem, %{wait_until_conn_up_ref: nil} = state) do
    StatusMonitor.database_connections_waking_up(state.stack_id)
    Electric.Connection.Manager.Supervisor.restart(stack_id: state.stack_id)

    ref = StatusMonitor.wait_until_async(state.stack_id, :active)

    {:noreply, %{state | wait_until_conn_up_ref: ref}}
  end

  def handle_cast(:restore_connection_subsystem, state) do
    # Ignore the restart request since we're already waiting on the connection subsystem to
    # start.
    {:noreply, state}
  end

  # A stop can take a while, and a caller that times out and asks again queues another request
  # behind it. Each one would stop the subsystem that just started, as often as the requests came
  # in, until the connection supervisor gave up on its restart limit and the stack stopped. A
  # request made while no manager ran came during a restart, which answers it too, or while the
  # database connections sleep, and the manager that wakes them sets the publication up.
  def handle_call({:restart_connection_subsystem, manager}, _from, state) do
    if is_pid(manager), do: stop_subsystem_of(manager, state.stack_id)
    {:reply, :ok, state}
  end

  def handle_info({{StatusMonitor, ref}, {:ok, :active}}, %{wait_until_conn_up_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | wait_until_conn_up_ref: nil}}
  end

  # The wait can also end without the stack becoming active: there may be no status monitor to ask
  # part-way through a restart, or it can die while the wait is outstanding. The latch has to clear
  # either way — the `:restore_connection_subsystem` clause above drops every later request while it
  # is set, so a latch that never clears leaves the connection subsystem unrestorable for good.
  def handle_info(
        {{StatusMonitor, ref}, {:error, :not_running}},
        %{wait_until_conn_up_ref: ref} = state
      ) do
    {:noreply, %{state | wait_until_conn_up_ref: nil}}
  end

  def handle_info(
        {{StatusMonitor, :down}, ref, :process, _pid, _reason},
        %{wait_until_conn_up_ref: ref} = state
      ) do
    {:noreply, %{state | wait_until_conn_up_ref: nil}}
  end

  def handle_info({{StatusMonitor, ref}, _result}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, state}
  end

  def handle_info({{StatusMonitor, :down}, _ref, :process, _pid, _reason}, state),
    do: {:noreply, state}

  # The manager's supervisor, as upstream stops it: it stops the connection resolver before the
  # manager, which can be waiting on it, where a stop of the manager alone waits for that wait to
  # end. Only while the named manager runs: the supervisor outlives a manager that stops on its
  # own, and starts another, which sets the publication up again. And only the connection
  # subsystem's supervisor, by the pid the registered manager names as its parent: once the
  # subsystem has stopped, its name can already belong to the one started in its place.
  defp stop_subsystem_of(manager, stack_id) do
    subsystem = connection_subsystem(stack_id)

    with ^manager <- connection_manager(stack_id),
         {:parent, ^subsystem} when is_pid(subsystem) <- Process.info(manager, :parent),
         do: Supervisor.stop(subsystem)
  catch
    :exit, _reason -> :ok
  end

  defp connection_subsystem(stack_id) do
    GenServer.whereis(Electric.Connection.Manager.Supervisor.name(stack_id: stack_id))
  end
end
