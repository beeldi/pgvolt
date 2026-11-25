defmodule Electric.StatusMonitorTest do
  use ExUnit.Case, async: true

  alias Electric.StatusMonitor
  import Support.TestUtils, only: [set_status_to_active: 1]

  setup {Support.ComponentSetup, :with_stack_id_from_test}

  describe "service_status/1" do
    test "returns :starting before any conditions are met", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      assert StatusMonitor.service_status(stack_id) == :starting
    end

    test "returns :starting when conn is waiting but shapes not loaded", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      # pg_lock_acquired is not set, so conn is :waiting_on_lock, but shape is :starting
      assert StatusMonitor.service_status(stack_id) == :starting
    end

    test "returns :waiting when conn waiting on lock and shapes loaded", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.service_status(stack_id) == :waiting
    end

    test "returns :starting once shapes are loaded and the lock is held but conn is still progressing",
         %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.service_status(stack_id) == :starting
    end

    test "returns :starting when conn is up but the shape pipeline is not ready", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.service_status(stack_id) == :starting
    end

    # Upstream's contract: a node whose shape pipeline is still up owns the next write, so it is
    # starting again rather than waiting on another node's lock.
    test "returns :starting when an active stack loses the lock", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.status(stack_id) == %{conn: :waiting_on_lock, shape: :up}
      assert StatusMonitor.service_status(stack_id) == :starting
    end

    test "returns :active when fully operational", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      set_status_to_active(%{stack_id: stack_id})

      assert StatusMonitor.service_status(stack_id) == :active
    end

    test "returns :sleeping when connections scaled down", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.database_connections_going_to_sleep(stack_id)
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.service_status(stack_id) == :sleeping
    end
  end

  describe "status/1" do
    test "when not started, returns :waiting_on_lock", %{stack_id: stack_id} do
      assert StatusMonitor.status(stack_id) == %{conn: :waiting_on_lock, shape: :starting}
    end

    test "when started but no signals have been received, returns :waiting_on_lock", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      assert StatusMonitor.status(stack_id) == %{conn: :waiting_on_lock, shape: :starting}
    end

    test "when pg_lock_acquired has been received, returns :starting", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :starting, shape: :starting}
    end

    test "when all conditions are met, returns :up", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :up, shape: :up}
    end

    test "when integrety checks not passed, returns :starting", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :starting, shape: :up}
    end

    test "when replication client not ready, returns :starting", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :starting, shape: :up}
    end

    test "when connection pool not ready, returns :starting", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :starting, shape: :up}
    end

    test "when shape log collector not ready, shape returns :read_only", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :up, shape: :read_only}
    end

    test "when canary process not ready, shape returns :read_only", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :up, shape: :read_only}
    end

    test "when a process dies, it's condition is reset", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())

      test_process = self()

      process =
        Task.async(fn ->
          StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
          send(test_process, :ready)

          receive do
            :exit -> :ok
          end

          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      end

      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :up, shape: :up}

      send(process.pid, :exit)
      Task.shutdown(process)
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :starting, shape: :up}

      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id) == %{conn: :up, shape: :up}
    end
  end

  # A stack that stops its shape subsystem to purge every shape serves none of them from the
  # metadata it still holds, read-only included, until the connection manager opens it again.
  describe "close_shape_metadata/2" do
    test "admits no read-only request until the metadata is opened again", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id).shape == :read_only

      refute StatusMonitor.shape_metadata_closed?(stack_id)
      :ok = StatusMonitor.close_shape_metadata(stack_id, "a purge of every shape is owed")
      assert StatusMonitor.status(stack_id).shape == :starting
      assert StatusMonitor.shape_metadata_closed?(stack_id)

      assert {:error, _timeout} = StatusMonitor.wait_until(stack_id, :read_only, timeout: 10)

      :ok = StatusMonitor.reopen_shape_metadata(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id).shape == :read_only
      refute StatusMonitor.shape_metadata_closed?(stack_id)
    end

    # The close is kept for the connection manager that takes over: the one that made it died.
    test "stays closed when the process that made it goes down", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      closer =
        spawn(fn ->
          :ok = StatusMonitor.close_shape_metadata(stack_id, "a purge of every shape is owed")
        end)

      ref = Process.monitor(closer)
      assert_receive {:DOWN, ^ref, :process, ^closer, :normal}
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.shape_metadata_closed?(stack_id)
      assert StatusMonitor.status(stack_id).shape == :starting
    end

    # An owner of the metadata that restarts loads it again and marks it ready, and served the
    # shapes of the purge the close stands for before a connection manager resolved it.
    test "stays closed when the metadata is marked ready again", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      :ok = StatusMonitor.close_shape_metadata(stack_id, "a purge of every shape is owed")

      owner = spawn(fn -> Process.sleep(:infinity) end)
      StatusMonitor.mark_shape_metadata_ready(stack_id, owner)
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.shape_metadata_closed?(stack_id)
      assert StatusMonitor.status(stack_id).shape == :starting

      :ok = StatusMonitor.reopen_shape_metadata(stack_id, owner)
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      refute StatusMonitor.shape_metadata_closed?(stack_id)
      assert StatusMonitor.status(stack_id).shape == :read_only
    end

    # A connection manager can die while its close waits in the monitor's queue. The one that
    # takes over asked before the monitor stored it, and took a missing timeline for a first
    # start where the manager before it had found a purge owed.
    test "is answered after a close sent before the question", %{stack_id: stack_id} do
      monitor = start_link_supervised!({StatusMonitor, stack_id: stack_id})
      :ok = :sys.suspend(monitor)
      reason = "a purge of every shape is owed"
      spawn(fn -> StatusMonitor.close_shape_metadata(stack_id, reason) end)
      assert eventually(fn -> queued_call?(monitor, {:close_shape_metadata, reason}) end)

      assert answered_after_resuming(monitor, :shape_metadata_closed?, fn ->
               StatusMonitor.shape_metadata_closed?(stack_id)
             end)
    end

    test "is not closed with no monitor running", %{stack_id: stack_id} do
      refute StatusMonitor.shape_metadata_closed?(stack_id)
    end

    test "unless locked, closes only until the lock is recorded", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      reason = "a purge of every shape is owed"

      assert StatusMonitor.close_shape_metadata_unless_locked(stack_id, reason)
      assert StatusMonitor.shape_metadata_closed?(stack_id)

      :ok = StatusMonitor.reopen_shape_metadata(stack_id, self())
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())

      refute StatusMonitor.close_shape_metadata_unless_locked(stack_id, reason)
      refute StatusMonitor.shape_metadata_closed?(stack_id)
      assert StatusMonitor.status(stack_id).shape == :read_only
    end

    test "an opening with nothing closed changes nothing", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      :ok = StatusMonitor.reopen_shape_metadata(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id).shape == :starting
    end
  end

  describe "new_slot_purge_owed?/1" do
    # A connection manager can die while its record of the purge a new slot owes waits in the
    # monitor's queue. The one that takes over read it before the monitor stored it, found the
    # slot in place and the timeline unchanged, and served the shapes from before the slot.
    test "is answered after a record sent before the question", %{stack_id: stack_id} do
      monitor = start_link_supervised!({StatusMonitor, stack_id: stack_id})
      :ok = :sys.suspend(monitor)
      spawn(fn -> StatusMonitor.owe_new_slot_purge(stack_id, true) end)
      assert eventually(fn -> queued_call?(monitor, {:owe_new_slot_purge, true}) end)

      assert answered_after_resuming(monitor, :new_slot_purge_owed?, fn ->
               StatusMonitor.new_slot_purge_owed?(stack_id)
             end)
    end

    test "is not owed with no monitor running", %{stack_id: stack_id} do
      refute StatusMonitor.new_slot_purge_owed?(stack_id)
    end
  end

  describe "wait_until_active/2" do
    test "waits until all conditions are met", %{stack_id: stack_id} do
      test_process = self()
      stop_supervised!(Electric.ProcessRegistry.registry_name(stack_id))

      Task.async(fn ->
        assert StatusMonitor.wait_until_active(stack_id,
                 timeout: ExUnit.configuration()[:assert_receive_timeout]
               ) == :ok

        send(test_process, :active)
      end)

      start_link_supervised!({Electric.ProcessRegistry, stack_id: stack_id})
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())

      refute_receive :active, 20
      assert StatusMonitor.mark_shape_log_collector_ready(stack_id, self()) == :ok
      assert_receive :active
    end

    test "allows timeout: :infinity", %{stack_id: stack_id} do
      test_process = self()
      stop_supervised!(Electric.ProcessRegistry.registry_name(stack_id))

      Task.async(fn ->
        assert StatusMonitor.wait_until_active(stack_id, timeout: :infinity) == :ok
        send(test_process, :active)
      end)

      start_link_supervised!({Electric.ProcessRegistry, stack_id: stack_id})
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())

      refute_receive :active, 20
      assert StatusMonitor.mark_shape_log_collector_ready(stack_id, self()) == :ok
      assert_receive :active
    end

    test "returns error on timeout when process registry is not present", %{stack_id: stack_id} do
      stop_supervised!(Electric.ProcessRegistry.registry_name(stack_id))

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Stack ID not recognised: #{stack_id}"}
    end

    test "returns error on timeout when status monitor is not present", %{stack_id: stack_id} do
      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Status monitor not found for stack ID: #{stack_id}"}
    end

    test "returns error on timeout when mark_pg_lock_acquired not received", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Timeout waiting for Postgres lock acquisition"}
    end

    test "returns error on timeout when mark_replication_client_ready not received", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())

      assert {:error, "Timeout waiting for replication client to be ready" <> _} =
               StatusMonitor.wait_until_active(stack_id, timeout: 1)
    end

    test "returns error on timeout when metadata mark_connection_pool_ready not received", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Timeout waiting for database connection pool (metadata) to be ready"}
    end

    test "returns error on timeout when snapshot mark_connection_pool_ready not received", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Timeout waiting for database connection pool (snapshot) to be ready"}
    end

    test "returns error on timeout when mark_shape_log_collector_ready not received", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Timeout waiting for shape data to be loaded"}
    end

    test "returns error on timeout waiting for integrety checks", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error, "Timeout waiting for integrety checks"}
    end

    test "returns explicit error on timeout when supplied", %{
      stack_id: stack_id
    } do
      error_message = "Some error message"

      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_connection_pool_as_errored(stack_id, :snapshot, error_message)

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) ==
               {:error,
                "Timeout waiting for database connection pool (snapshot) to be ready: #{error_message}"}
    end

    test "returns :conn_sleeping when connections are sleeping", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.database_connections_going_to_sleep(stack_id)
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.wait_until_active(stack_id, timeout: 1) == :conn_sleeping
    end

    test "returns error if stack is terminated before fully initialized", %{stack_id: stack_id} do
      parent = self()

      {:via, mod, name} = StatusMonitor.name(stack_id)

      pid =
        start_supervised!(
          {Task,
           fn ->
             mod.register_name(name, self())

             send(parent, {:monitor, :ready})

             Process.sleep(:infinity)
           end}
        )

      assert_receive {:monitor, :ready}

      task =
        Task.async(fn ->
          send(parent, {:monitor, :wait})
          StatusMonitor.wait_until_active(stack_id, timeout: 100)
        end)

      ref = Process.monitor(pid)

      assert_receive {:monitor, :wait}

      Process.exit(
        pid,
        Enum.random([:shutdown, {:shutdown, :normal}, {:error, "broken"}, :kill])
      )

      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      # the actual returned error is caused by the status monitor pid not
      # existing, not by the exit message
      assert {:error, _message} = Task.await(task)
    end
  end

  describe "wait_until/3" do
    test "with :read_only returns {:ok, :active} when fully active", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      set_status_to_active(%{stack_id: stack_id})

      assert StatusMonitor.wait_until(stack_id, :read_only, timeout: 100) == {:ok, :active}
    end

    test "with :read_only returns {:ok, :read_only} when shape metadata is ready", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.wait_until(stack_id, :read_only, timeout: 100) == {:ok, :read_only}
    end

    test "with :read_only opens once the lock is held and the connection is still progressing",
         %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.service_status(stack_id) == :starting
      assert StatusMonitor.wait_until(stack_id, :read_only, timeout: 100) == {:ok, :read_only}
    end

    test "with :read_only opens when the connection is up but the shape pipeline is not", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.service_status(stack_id) == :starting
      assert StatusMonitor.wait_until(stack_id, :read_only, timeout: 100) == {:ok, :read_only}
    end

    test "with :read_only stays shut while this node is still the writer", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert %{shape: :up} = StatusMonitor.status(stack_id)

      assert {:error, _} = StatusMonitor.wait_until(stack_id, :read_only, timeout: 50),
             "a node whose shape subsystem is up owns the next write, so the read-only path " <>
               "would answer from metadata behind what the client already holds"
    end

    test "with :read_only waits until shape metadata becomes ready", %{stack_id: stack_id} do
      test_process = self()

      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      Task.async(fn ->
        result =
          StatusMonitor.wait_until(stack_id, :read_only,
            timeout: ExUnit.configuration()[:assert_receive_timeout]
          )

        send(test_process, {:result, result})
      end)

      refute_receive {:result, _}, 50
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      assert_receive {:result, {:ok, :read_only}}
    end

    test "with :active returns {:ok, :active} when fully active", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      set_status_to_active(%{stack_id: stack_id})

      assert StatusMonitor.wait_until(stack_id, :active, timeout: 100) == {:ok, :active}
    end

    test "with :active waits and does not return for :read_only", %{stack_id: stack_id} do
      test_process = self()

      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      Task.async(fn ->
        result = StatusMonitor.wait_until(stack_id, :active, timeout: 100)
        send(test_process, {:result, result})
      end)

      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      # Should not return for read_only — must wait for active
      refute_receive {:result, {:ok, :read_only}}, 50
      assert_receive {:result, {:error, _}}
    end

    test "with :active waits until all conditions are met", %{stack_id: stack_id} do
      test_process = self()
      stop_supervised!(Electric.ProcessRegistry.registry_name(stack_id))

      Task.async(fn ->
        result =
          StatusMonitor.wait_until(stack_id, :active,
            timeout: ExUnit.configuration()[:assert_receive_timeout]
          )

        send(test_process, {:result, result})
      end)

      start_link_supervised!({Electric.ProcessRegistry, stack_id: stack_id})
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      StatusMonitor.mark_replication_client_ready(stack_id, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :admin, self())
      StatusMonitor.mark_connection_pool_ready(stack_id, :snapshot, self())
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.mark_supervisor_processes_ready(stack_id, self())
      StatusMonitor.mark_integrety_checks_passed(stack_id, self())

      refute_receive {:result, _}, 20
      StatusMonitor.mark_shape_log_collector_ready(stack_id, self())
      assert_receive {:result, {:ok, :active}}
    end

    test "returns :conn_sleeping when connections are sleeping", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.database_connections_going_to_sleep(stack_id)
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.wait_until(stack_id, :read_only, timeout: 100) == :conn_sleeping
      assert StatusMonitor.wait_until(stack_id, :active, timeout: 100) == :conn_sleeping
    end

    test "returns error on timeout", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      assert {:error, _} = StatusMonitor.wait_until(stack_id, :read_only, timeout: 1)
      assert {:error, _} = StatusMonitor.wait_until(stack_id, :active, timeout: 1)
    end
  end

  describe "wait_until_async/2" do
    test "replies immediately when already active", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      set_status_to_active(%{stack_id: stack_id})

      ref = StatusMonitor.wait_until_async(stack_id, :active)
      assert_receive {{StatusMonitor, ^ref}, {:ok, :active}}
    end

    test "replies immediately with :read_only when shape metadata is ready", %{
      stack_id: stack_id
    } do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      ref = StatusMonitor.wait_until_async(stack_id, :read_only)
      assert_receive {{StatusMonitor, ^ref}, {:ok, :read_only}}
    end

    test "notifies when status transitions to active", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      ref = StatusMonitor.wait_until_async(stack_id, :active)
      refute_receive {{StatusMonitor, ^ref}, _}, 50

      set_status_to_active(%{stack_id: stack_id})
      assert_receive {{StatusMonitor, ^ref}, {:ok, :active}}
    end

    test "does not notify :active waiter for :read_only transition", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      ref = StatusMonitor.wait_until_async(stack_id, :active)

      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      refute_receive {{StatusMonitor, ^ref}, _}, 50
    end

    test "notifies :read_only waiter when shape metadata becomes ready", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      ref = StatusMonitor.wait_until_async(stack_id, :read_only)
      refute_receive {{StatusMonitor, ^ref}, _}, 50

      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      assert_receive {{StatusMonitor, ^ref}, {:ok, :read_only}}
    end

    test "reports a failure instead of raising when the status monitor is not running", %{
      stack_id: stack_id
    } do
      # Act
      ref = StatusMonitor.wait_until_async(stack_id, :active)

      # Assert
      assert_receive {{StatusMonitor, ^ref}, {:error, :not_running}}
    end

    test "reports a failure when the status monitor dies while the wait is outstanding", %{
      stack_id: stack_id
    } do
      # Arrange
      pid = start_supervised!({StatusMonitor, stack_id: stack_id})
      ref = StatusMonitor.wait_until_async(stack_id, :active)
      refute_receive {{StatusMonitor, ^ref}, _}, 50

      # Act
      Process.exit(pid, :kill)

      # Assert
      assert_receive {{StatusMonitor, :down}, ^ref, :process, ^pid, :killed}
    end

    test "the returned reference is the monitor on the status monitor, so a caller can drop it",
         %{stack_id: stack_id} do
      pid = start_supervised!({StatusMonitor, stack_id: stack_id})
      set_status_to_active(%{stack_id: stack_id})

      ref = StatusMonitor.wait_until_async(stack_id, :active)
      assert_receive {{StatusMonitor, ^ref}, {:ok, :active}}
      assert {:process, pid} in monitors_of_self()

      Process.demonitor(ref, [:flush])

      refute {:process, pid} in monitors_of_self()
    end

    test "supports multiple concurrent waiters at different levels", %{stack_id: stack_id} do
      start_link_supervised!({StatusMonitor, stack_id: stack_id})

      ref_ro = StatusMonitor.wait_until_async(stack_id, :read_only)
      ref_active = StatusMonitor.wait_until_async(stack_id, :active)

      # Transition to read_only — only the read_only waiter should be notified
      StatusMonitor.mark_shape_metadata_ready(stack_id, self())
      assert_receive {{StatusMonitor, ^ref_ro}, {:ok, :read_only}}
      refute_receive {{StatusMonitor, ^ref_active}, _}, 50

      # Transition to active — the active waiter should be notified
      set_status_to_active(%{stack_id: stack_id})
      assert_receive {{StatusMonitor, ^ref_active}, {:ok, :active}}
    end
  end

  defp monitors_of_self do
    {:monitors, monitors} = Process.info(self(), :monitors)
    monitors
  end

  # Asks with `read` while the suspended monitor holds what was sent to it, and resumes it once
  # the question waits in its queue too, or was answered without it.
  defp answered_after_resuming(monitor, request, read) do
    reader = Task.async(read)
    assert eventually(fn -> queued_call?(monitor, request) or not Process.alive?(reader.pid) end)
    :ok = :sys.resume(monitor)
    Task.await(reader)
  end

  defp queued_call?(pid, request) do
    {:messages, messages} = Process.info(pid, :messages)
    Enum.any?(messages, &match?({:"$gen_call", _from, ^request}, &1))
  end

  defp eventually(check, attempts \\ 200) do
    cond do
      check.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(5)
        eventually(check, attempts - 1)
    end
  end
end
