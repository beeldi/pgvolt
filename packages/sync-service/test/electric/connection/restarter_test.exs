defmodule Electric.Connection.RestarterTest do
  @moduledoc """
  The restore path is guarded by a single-shot latch: while a restore is waiting on the stack to
  come back, every later request is dropped. These cases are about the latch clearing on each of
  the ways that wait can end — including the two that are not the stack becoming active.
  """
  use ExUnit.Case, async: false

  alias Electric.Connection.Restarter
  alias Electric.StatusMonitor

  @timeout ExUnit.configuration()[:assert_receive_timeout]

  # A stand-in connection manager. Its stop waits until the test lets it go or has finished.
  defmodule SlowStop do
    use GenServer

    # As the connection manager's: its supervisor waits for its stop however long it takes.
    def child_spec(arg),
      do: %{id: :manager, start: {__MODULE__, :start_link, [arg]}, shutdown: :infinity}

    def start_link({stack_id, test_pid}) do
      GenServer.start_link(__MODULE__, test_pid, name: Electric.Connection.Manager.name(stack_id))
    end

    @impl true
    def init(test_pid) do
      Process.flag(:trap_exit, true)
      send(test_pid, {:manager_started, self()})
      {:ok, test_pid}
    end

    # Waits on the resolver, as the connection manager does while it validates a connection.
    @impl true
    def handle_cast({:wait_on, resolver}, test_pid) do
      GenServer.call(resolver, :validate, :infinity)
      {:noreply, test_pid}
    end

    @impl true
    def terminate(_reason, test_pid) do
      monitor = Process.monitor(test_pid)
      send(test_pid, {:stopping, self()})

      receive do
        :stop -> :ok
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      end
    end
  end

  # A stand-in connection resolver, which never answers, as on a connection slow to open.
  defmodule SilentResolver do
    use GenServer

    def child_spec(arg), do: %{id: :resolver, start: {__MODULE__, :start_link, [arg]}}

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call(:validate, from, test_pid) do
      send(test_pid, {:validating, from})
      {:noreply, test_pid}
    end
  end

  setup {Support.ComponentSetup, :with_stack_id_from_test}

  setup %{stack_id: stack_id} do
    monitor = start_supervised!({StatusMonitor, stack_id: stack_id})
    StatusMonitor.database_connections_going_to_sleep(stack_id)
    StatusMonitor.wait_for_messages_to_be_processed(stack_id)

    start_supervised!(
      {Restarter, stack_id: stack_id, stack_events_registry: :unused_by_the_restore_path}
    )

    %{monitor: monitor, connection_supervisor: start_connection_supervisor(stack_id)}
  end

  describe "restoring the connection subsystem" do
    test "drops a second request while the first is still waiting", ctx do
      # Arrange
      request_restore(ctx.stack_id)
      assert_restarted(ctx.connection_supervisor)
      supervisor = start_connection_supervisor(ctx.stack_id)

      # Act
      request_restore(ctx.stack_id)

      # Assert
      refute_restarted(supervisor)
    end

    test "honours a later request once the stack reports active", ctx do
      # Arrange
      request_restore(ctx.stack_id)
      assert_restarted(ctx.connection_supervisor)
      supervisor = start_connection_supervisor(ctx.stack_id)

      # Act
      Support.TestUtils.set_status_to_active(%{stack_id: ctx.stack_id})
      request_restore(ctx.stack_id)

      # Assert
      assert_restarted(supervisor)
    end

    test "holds no monitor on the status monitor once the stack reports active", ctx do
      # Arrange
      restarter = GenServer.whereis(Restarter.name(ctx.stack_id))
      request_restore(ctx.stack_id)
      assert_restarted(ctx.connection_supervisor)
      :sys.get_state(restarter)
      assert {:process, ctx.monitor} in monitors_of(restarter)

      # Act
      Support.TestUtils.set_status_to_active(%{stack_id: ctx.stack_id})
      await_wait_ended(restarter)

      # Assert
      refute {:process, ctx.monitor} in monitors_of(restarter)
    end

    test "gets no late down message after the status monitor exits following a wait", ctx do
      # Arrange
      restarter = GenServer.whereis(Restarter.name(ctx.stack_id))
      request_restore(ctx.stack_id)
      Support.TestUtils.set_status_to_active(%{stack_id: ctx.stack_id})
      await_wait_ended(restarter)

      # Act
      :erlang.trace(restarter, true, [:receive])
      monitor_ref = Process.monitor(ctx.monitor)
      Process.exit(ctx.monitor, :kill)
      assert_receive {:DOWN, ^monitor_ref, :process, _, :killed}

      # Assert
      refute_receive {:trace, ^restarter, :receive, {_tag, _ref, :process, _pid, _reason}}
    end

    test "honours a later request after the status monitor dies mid-wait", ctx do
      # Arrange
      request_restore(ctx.stack_id)
      assert_restarted(ctx.connection_supervisor)
      supervisor = start_connection_supervisor(ctx.stack_id)

      # Act
      monitor_ref = Process.monitor(ctx.monitor)
      Process.exit(ctx.monitor, :kill)
      assert_receive {:DOWN, ^monitor_ref, :process, _, :killed}
      request_restore(ctx.stack_id)

      # Assert
      assert_restarted(supervisor)
    end
  end

  describe "restarting the connection subsystem" do
    setup ctx do
      # The connection supervisor starts the subsystem again each time a restart stops it. The
      # subsystem holds a stand-in manager, whose stop waits for the test's word, as a drain can
      # hold up the connection manager's, and a stand-in resolver started after it.
      Supervisor.stop(ctx.connection_supervisor)

      subsystem = %{
        id: :subsystem,
        type: :supervisor,
        start:
          {Supervisor, :start_link,
           [
             [{SlowStop, {ctx.stack_id, self()}}, {SilentResolver, self()}],
             [
               strategy: :one_for_all,
               name: Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id)
             ]
           ]}
      }

      start_supervised!(%{
        id: :connection_supervisor,
        type: :supervisor,
        start: {Supervisor, :start_link, [[subsystem], [strategy: :one_for_one]]}
      })

      assert_receive {:manager_started, _}
      :ok
    end

    test "answers the requests made while a restart ran with that restart", ctx do
      # Arrange
      restart = Task.async(fn -> request_restart(ctx.stack_id) end)
      assert_receive {:stopping, manager}

      requests = for _ <- 1..3, do: Task.async(fn -> request_restart(ctx.stack_id) end)
      restarter = GenServer.whereis(Restarter.name(ctx.stack_id))

      assert Support.TestUtils.wait_until(fn ->
               Process.info(restarter, :message_queue_len) == {:message_queue_len, 3}
             end)

      # Act
      send(manager, :stop)

      # Assert
      assert [:ok, :ok, :ok, :ok] = Task.await_many([restart | requests], @timeout)
      assert_receive {:manager_started, _}
      refute_receive {:manager_started, _}, 200
    end

    test "honours a request made after the last restart finished", ctx do
      # Arrange
      restart = Task.async(fn -> request_restart(ctx.stack_id) end)
      assert_receive {:stopping, manager}
      send(manager, :stop)
      assert :ok = Task.await(restart, @timeout)
      assert_receive {:manager_started, _}

      # Act
      restart = Task.async(fn -> request_restart(ctx.stack_id) end)
      assert_receive {:stopping, manager}
      send(manager, :stop)

      # Assert
      assert :ok = Task.await(restart, @timeout)
      assert_receive {:manager_started, _}
    end

    test "answers a request naming a manager that has since stopped", ctx do
      # Arrange
      manager = Restarter.connection_manager(ctx.stack_id)
      restart = Task.async(fn -> request_restart(ctx.stack_id) end)
      assert_receive {:stopping, ^manager}
      send(manager, :stop)
      assert :ok = Task.await(restart, @timeout)
      assert_receive {:manager_started, replacement}

      # Act
      result = Restarter.restart_connection_subsystem(ctx.stack_id, manager)

      # Assert
      assert :ok = result
      refute_received {:stopping, _}
      assert Process.alive?(replacement)
    end

    test "answers a request naming a manager its supervisor has since replaced", ctx do
      # Arrange: the manager stops on its own, and its supervisor, still running, starts another.
      named = Restarter.connection_manager(ctx.stack_id)

      subsystem =
        GenServer.whereis(Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id))

      manager = GenServer.whereis(Electric.Connection.Manager.name(ctx.stack_id))
      Process.exit(manager, :kill)
      assert_receive {:manager_started, replacement}

      # Act
      request = Task.async(fn -> Restarter.restart_connection_subsystem(ctx.stack_id, named) end)

      # Assert
      refute_receive {:stopping, _}
      assert :ok = Task.await(request, @timeout)
      assert Process.alive?(replacement)

      assert ^subsystem =
               GenServer.whereis(
                 Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id)
               )
    end

    test "stops a manager waiting on the connection resolver", ctx do
      # Arrange: the manager waits on the resolver, and is told to stop once that wait ends.
      manager = Restarter.connection_manager(ctx.stack_id)

      subsystem =
        GenServer.whereis(Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id))

      [resolver] = for {:resolver, pid, _, _} <- Supervisor.which_children(subsystem), do: pid
      GenServer.cast(manager, {:wait_on, resolver})
      assert_receive {:validating, _}
      send(manager, :stop)

      # Act
      result = request_restart(ctx.stack_id)

      # Assert
      assert :ok = result
      assert_receive {:manager_started, replacement}
      refute replacement == manager
    end

    test "stops nothing for a manager started outside the connection subsystem", ctx do
      # Arrange: the subsystem's manager stops, and a process another supervisor started takes
      # the manager's name while the subsystem still runs.
      subsystem =
        GenServer.whereis(Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id))

      manager = Restarter.connection_manager(ctx.stack_id)
      stop = Task.async(fn -> Supervisor.terminate_child(subsystem, :manager) end)
      assert_receive {:stopping, ^manager}
      send(manager, :stop)
      assert :ok = Task.await(stop, @timeout)
      name = Electric.Connection.Manager.name(ctx.stack_id)

      other =
        start_supervised!(%{
          id: :other,
          type: :supervisor,
          start:
            {Supervisor, :start_link,
             [
               [%{id: :manager, start: {Agent, :start_link, [fn -> nil end, [name: name]]}}],
               [strategy: :one_for_one]
             ]}
        })

      named = Restarter.connection_manager(ctx.stack_id)

      # Act
      result = Restarter.restart_connection_subsystem(ctx.stack_id, named)

      # Assert
      assert :ok = result
      assert Process.alive?(subsystem)
      assert Process.alive?(other)
      assert Process.alive?(named)
    end

    test "stops nothing for a request naming another child of the connection subsystem", ctx do
      # Arrange: the connection resolver, which the subsystem's supervisor started too.
      registered = Restarter.connection_manager(ctx.stack_id)

      subsystem =
        GenServer.whereis(Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id))

      [resolver] = for {:resolver, pid, _, _} <- Supervisor.which_children(subsystem), do: pid

      # Act
      request =
        Task.async(fn -> Restarter.restart_connection_subsystem(ctx.stack_id, resolver) end)

      # Assert
      refute_receive {:stopping, _}
      assert :ok = Task.await(request, @timeout)
      assert Process.alive?(resolver)
      assert ^registered = Restarter.connection_manager(ctx.stack_id)
    end
  end

  # Between a stop and the start that follows it, or while the connections sleep, no manager runs.
  test "answers a request made while no connection manager ran", ctx do
    # Arrange
    restarter = GenServer.whereis(Restarter.name(ctx.stack_id))
    assert nil == Restarter.connection_manager(ctx.stack_id)

    # Act
    result = Restarter.restart_connection_subsystem(ctx.stack_id)

    # Assert
    assert :ok = result
    assert ^restarter = GenServer.whereis(Restarter.name(ctx.stack_id))

    assert ctx.connection_supervisor ==
             GenServer.whereis(
               Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id)
             )
  end

  test "answers a request made while no connection subsystem ran", ctx do
    # Arrange
    restarter = GenServer.whereis(Restarter.name(ctx.stack_id))
    Supervisor.stop(ctx.connection_supervisor)

    # Act
    result = Restarter.restart_connection_subsystem(ctx.stack_id)

    # Assert
    assert :ok = result
    assert ^restarter = GenServer.whereis(Restarter.name(ctx.stack_id))
  end

  # A request for the manager running now. It waits for a stop the test holds up to the suite's
  # receive timeout, where the public call gives up after 5 s.
  defp request_restart(stack_id) do
    manager = Restarter.connection_manager(stack_id)
    GenServer.call(Restarter.name(stack_id), {:restart_connection_subsystem, manager}, @timeout)
  end

  # The request is a cast and the status monitor answers the wait from its own process, so no
  # call the test makes orders the answer before what comes next: the cleared latch does.
  defp await_wait_ended(restarter) do
    assert Support.TestUtils.wait_until(fn ->
             is_nil(:sys.get_state(restarter).wait_until_conn_up_ref)
           end)
  end

  defp monitors_of(pid) do
    {:monitors, monitors} = Process.info(pid, :monitors)
    monitors
  end

  # A real supervisor under the name the restore path stops, so that "the subsystem was restarted"
  # is observed as the thing actually happening rather than as a stubbed call.
  defp start_connection_supervisor(stack_id) do
    {:ok, pid} =
      Supervisor.start_link([],
        strategy: :one_for_one,
        name: Electric.Connection.Manager.Supervisor.name(stack_id: stack_id)
      )

    Process.unlink(pid)
    pid
  end

  defp assert_restarted(supervisor) do
    ref = Process.monitor(supervisor)
    assert_receive {:DOWN, ^ref, :process, ^supervisor, _}
  end

  defp refute_restarted(supervisor) do
    ref = Process.monitor(supervisor)
    refute_receive {:DOWN, ^ref, :process, ^supervisor, _}, 200
    Process.demonitor(ref, [:flush])
  end

  # The public wrapper first checks that the connections are actually sleeping; these cases are
  # about the exclusion latch behind it, so they hand the request straight to the process.
  defp request_restore(stack_id) do
    GenServer.cast(Restarter.name(stack_id), :restore_connection_subsystem)
  end
end
