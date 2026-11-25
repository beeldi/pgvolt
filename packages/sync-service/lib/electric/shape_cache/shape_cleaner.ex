defmodule Electric.ShapeCache.ShapeCleaner do
  @moduledoc """
  Removes a shape (consumer, status entry, on-disk data and publication entry) on demand.
  """

  alias Electric.Shapes.Consumer
  alias Electric.Replication.PublicationManager
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.ShapeCleaner.CleanupTaskSupervisor
  alias Electric.Telemetry.OpenTelemetry

  require Logger

  @type shape_handle() :: Electric.shape_handle()
  @type stack_id() :: Electric.stack_id()

  @shutdown_cleanup {:shutdown, :cleanup}
  @shutdown_suspend {:shutdown, :suspend}

  # Public API
  def consumer_cleanup_reason, do: @shutdown_cleanup
  def consumer_suspend_reason, do: @shutdown_suspend

  @spec remove_shapes(stack_id(), [shape_handle()], term()) :: :ok | {:error, term()}
  def remove_shapes(stack_id, shape_handles, reason \\ @shutdown_cleanup)
      when is_list(shape_handles) do
    do_remove_shapes(stack_id, shape_handles, reason, & &1.())
  end

  defp do_remove_shapes(stack_id, shape_handles, reason, call_shape_db, await_teardown? \\ false) do
    OpenTelemetry.with_span(
      "shape_cleaner.remove_shapes",
      [],
      stack_id,
      fn ->
        valid_handles =
          remove_shapes_immediate(stack_id, shape_handles, reason, call_shape_db, await_teardown?)

        if await_teardown? do
          Enum.each(valid_handles, &PublicationManager.remove_shape(stack_id, &1))
        end

        remove_shapes_deferred(stack_id, valid_handles, await_teardown?)
        OpenTelemetry.stop_and_save_intervals(total_attribute: "remove_shape.total_duration_us")
        :ok
      end
    )
  end

  @doc """
  `remove_shapes/2` for a caller that must not exit on a ShapeDb connection held past its checkout
  deadline, such as the log collector: it asks for the connection again, as a cleanup task does,
  until `wait_ms` have passed, and then exits with the last checkout timeout.
  """
  @spec remove_shapes_waiting(stack_id(), [shape_handle()], pos_integer()) :: :ok
  def remove_shapes_waiting(stack_id, shape_handles, wait_ms)
      when is_list(shape_handles) and is_integer(wait_ms) and wait_ms > 0 do
    deadline = System.monotonic_time(:millisecond) + wait_ms

    do_remove_shapes(
      stack_id,
      shape_handles,
      @shutdown_cleanup,
      &wait_for_shape_db(&1, deadline)
    )
  end

  @spec remove_shape(stack_id(), shape_handle(), term()) :: :ok | {:error, term()}
  def remove_shape(stack_id, shape_handle, reason \\ @shutdown_cleanup) do
    remove_shapes(stack_id, List.wrap(shape_handle), reason)
  end

  @spec remove_shapes_async(stack_id(), [shape_handle()], term()) :: :ok
  def remove_shapes_async(stack_id, shape_handles, reason \\ @shutdown_cleanup) do
    CleanupTaskSupervisor.perform_async(stack_id, fn ->
      activate_mocked_functions_from_test_process()

      do_remove_shapes(stack_id, shape_handles, reason, &wait_for_shape_db/1)
    end)
  end

  @type removal_task() :: {pid(), :atomics.atomics_ref()}

  @doc "Starts cleanup and provides a completion flag for callers also waiting on the teardown."
  @spec start_removing_shapes(stack_id(), [shape_handle()], term()) ::
          {:ok, removal_task()} | {:error, term()}
  def start_removing_shapes(stack_id, shape_handles, reason \\ @shutdown_cleanup) do
    completed = :atomics.new(1, [])

    with {:ok, pid} <-
           Task.Supervisor.start_child(CleanupTaskSupervisor.name(stack_id), fn ->
             Logger.metadata(stack_id: stack_id)
             Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)
             activate_mocked_functions_from_test_process()

             :ok = do_remove_shapes(stack_id, shape_handles, reason, &wait_for_shape_db/1, true)
             :atomics.put(completed, 1, 1)
           end) do
      {:ok, {pid, completed}}
    end
  end

  @spec remove_shape_async(stack_id(), shape_handle()) :: :ok
  def remove_shape_async(stack_id, shape_handle) do
    remove_shapes_async(stack_id, List.wrap(shape_handle))
  end

  @spec remove_shapes_for_relations(list(Electric.oid_relation()), stack_id(), term()) :: :ok
  def remove_shapes_for_relations(stack_id, relations, reason \\ @shutdown_cleanup)

  def remove_shapes_for_relations(_stack_id, [], _reason) do
    :ok
  end

  def remove_shapes_for_relations(stack_id, relations, reason) do
    # We don't want for this call to be blocking because it will be called in `PublicationManager`
    # if it notices a discrepancy in the schema

    CleanupTaskSupervisor.perform_async(stack_id, fn ->
      activate_mocked_functions_from_test_process()

      affected_shapes =
        wait_for_shape_db(fn ->
          ShapeStatus.list_shape_handles_for_relations(stack_id, relations)
        end)

      Logger.notice(fn ->
        "Cleaning up all shapes for relations #{inspect(relations)}: #{length(affected_shapes)} shapes total"
      end)

      do_remove_shapes(stack_id, affected_shapes, reason, &wait_for_shape_db/1)
    end)
  end

  @doc """
  Remove shapes whose metadata row is already gone.

  `remove_shapes/3` cannot serve this case: with no row left for the handle it takes the
  `:data_removed` branch, which stops no consumer and reclaims no storage. Deleting the
  directory on its own would leave that consumer writing into an unlinked file and holding
  the flush entry that pins the stack's ack, and through it the upstream WAL.
  """
  @spec remove_detached_shapes_async(stack_id(), [shape_handle()]) :: :ok
  def remove_detached_shapes_async(_stack_id, []) do
    :ok
  end

  def remove_detached_shapes_async(stack_id, shape_handles) do
    CleanupTaskSupervisor.perform_async(stack_id, fn ->
      activate_mocked_functions_from_test_process()

      stack_storage = Storage.for_stack(stack_id)

      Enum.each(shape_handles, fn shape_handle ->
        Consumer.stop(stack_id, shape_handle, @shutdown_cleanup)
        :ok = Electric.Replication.ShapeLogCollector.remove_shape(stack_id, shape_handle)

        stack_storage
        |> Storage.cleanup!(shape_handle)
        |> report_storage_reclaim(shape_handle)
      end)
    end)
  end

  @spec remove_shape_storage_async(stack_id(), [shape_handle()]) :: :ok
  def remove_shape_storage_async(_stack_id, []) do
    :ok
  end

  def remove_shape_storage_async(stack_id, shape_handles) do
    CleanupTaskSupervisor.perform_async(stack_id, fn ->
      activate_mocked_functions_from_test_process()

      stack_storage = Storage.for_stack(stack_id)

      Enum.each(shape_handles, fn shape_handle ->
        stack_storage
        |> Storage.cleanup!(shape_handle)
        |> report_storage_reclaim(shape_handle)
      end)
    end)
  end

  @type reason() :: {:shutdown, :cleanup} | {:shutdown, :suspend} | term()
  @spec handle_writer_termination(stack_id(), shape_handle(), reason()) :: :removed | :ok
  def handle_writer_termination(stack_id, shape_handle, @shutdown_cleanup) do
    Logger.info("Removing shape #{inspect(shape_handle)}")

    remove_shape_async(stack_id, shape_handle)

    :removed
  end

  def handle_writer_termination(stack_id, shape_handle, @shutdown_suspend) do
    # deregister the consumer without removing it from the rest of the system
    # the next time a txn comes in matching this consumer it will be re-started
    # by the consumer registry as per any other lazily loaded consumer
    Electric.Shapes.ConsumerRegistry.remove_consumer(shape_handle, stack_id)
  end

  # Any `{:shutdown, x}` other than the two Electric-tagged reasons above is
  # not a deliberate stop and falls through to the abnormal-shutdown clause.
  def handle_writer_termination(_stack_id, _shape_handle, reason)
      when reason in [:normal, :killed, :shutdown] do
    :ok
  end

  def handle_writer_termination(stack_id, shape_handle, reason) do
    reason_message =
      case reason do
        {error, stacktrace} when is_tuple(error) and is_list(stacktrace) ->
          Exception.format(:error, error, stacktrace)

        other ->
          inspect(other)
      end

    Logger.notice(
      "Removing shape #{inspect(shape_handle)} due to abnormal shutdown: #{reason_message}"
    )

    remove_shape_async(stack_id, shape_handle)

    :removed
  end

  defp remove_shapes_immediate(stack_id, shape_handles, reason, call_shape_db, await_teardown?)
       when is_list(shape_handles) do
    OpenTelemetry.with_child_span(
      "shape_cleaner.remove_shapes.remove_shapes_immediate",
      [count: length(shape_handles)],
      stack_id,
      fn ->
        Enum.flat_map(shape_handles, fn shape_handle ->
          case remove_shape_immediate(
                 stack_id,
                 shape_handle,
                 reason,
                 call_shape_db,
                 await_teardown?
               ) do
            :ok -> [shape_handle]
            {:error, :data_removed} -> []
          end
        end)
      end
    )
  end

  defp remove_shape_immediate(stack_id, shape_handle, reason, call_shape_db, await_teardown?) do
    OpenTelemetry.start_interval(:"remove_shape.shape_status_remove.duration_us")
    result = call_shape_db.(fn -> ShapeStatus.remove_shape(stack_id, shape_handle) end)

    case result do
      result
      when result == :ok or (await_teardown? and result == {:error, {:enoshape, shape_handle}}) ->
        OpenTelemetry.start_interval(:"remove_shape.shape_consumer_stop.duration_us")
        stop_consumer(stack_id, shape_handle, reason, await_teardown?)

        OpenTelemetry.start_interval(:"remove_shape.storage_cleanup.duration_us")

        stack_id
        |> Storage.for_stack()
        |> Storage.cleanup!(shape_handle)
        |> report_storage_reclaim(shape_handle)

        # The shape left ShapeStatus above, so nothing names this handle again — and the log
        # collector holds the flush entry pinning the stack's ack. Postgres retains WAL until
        # it is released, so this runs even when the reclaim above failed.
        OpenTelemetry.start_interval(:"remove_shape.shape_log_collector_remove.duration_us")
        :ok = Electric.Replication.ShapeLogCollector.remove_shape(stack_id, shape_handle)

        :ok

      {:error, _reason} ->
        # The shape is already gone from ShapeStatus, but an earlier removal chain may
        # have died between the ShapeStatus removal and the ShapeLogCollector removal,
        # leaving the shape's flush entry pinned. The SLC removal is idempotent, so
        # re-issue it instead of assuming it ever completed.
        :ok = Electric.Replication.ShapeLogCollector.remove_shape(stack_id, shape_handle)
        {:error, :data_removed}
    end
  end

  defp stop_consumer(stack_id, shape_handle, reason, false),
    do: Consumer.stop(stack_id, shape_handle, reason)

  defp stop_consumer(stack_id, shape_handle, reason, true) do
    # A timed-out stop sends an asynchronous kill. Wait for the consumer to exit before
    # declaring teardown complete, even then.
    pid = Consumer.whereis(stack_id, shape_handle)
    monitor = if pid, do: Process.monitor(pid)
    Consumer.stop(stack_id, shape_handle, reason)
    if monitor, do: receive(do: ({:DOWN, ^monitor, :process, ^pid, _} -> :ok))
  end

  # A cleanup task has no caller to hand a failure to, and nothing runs it again; the log
  # collector cannot die for a busy connection either (remove_shapes_waiting/3). Dying while
  # a storm of shape requests holds the ShapeDb connection past its checkout deadline would
  # leave every shape it had not reached in place, so it asks for the connection again. Both
  # calls wrapped here check out the connection before they write anything, so a retry never
  # repeats a write.
  #
  # The collector holds replication while it waits, so its wait ends at a deadline: past it the
  # connection is not busy but stuck, and the collector exits to re-stream from the slot, as it
  # does for a gather that stalls.
  defp wait_for_shape_db(fun, deadline \\ :infinity) do
    fun.()
  catch
    :exit, {:timeout, {NimblePool, :checkout, _args}} = reason ->
      if deadline != :infinity and System.monotonic_time(:millisecond) >= deadline do
        Logger.error("Shape cleanup gave up waiting for the busy ShapeDb connection")
        exit(reason)
      end

      Logger.warning("Shape cleanup is waiting for the busy ShapeDb connection")
      wait_for_shape_db(fun, deadline)
  end

  defp report_storage_reclaim(:ok, _shape_handle), do: :ok

  defp report_storage_reclaim(result, shape_handle) do
    Logger.error(
      "Could not reclaim storage for shape #{inspect(shape_handle)}: #{inspect(result)}. " <>
        "The shape is detached; its bytes stay on disk until a later reclaim succeeds."
    )
  end

  defp remove_shapes_deferred(stack_id, shape_handles, await_teardown?)
       when is_list(shape_handles) do
    OpenTelemetry.start_interval(:"remove_shape.remove_shapes_deferred.duration_us")
    :ok = CleanupTaskSupervisor.cleanup_async(stack_id, shape_handles, not await_teardown?)
  end

  if Mix.env() == :test do
    def activate_mocked_functions_from_test_process do
      Support.TestUtils.activate_mocked_functions_for_module(__MODULE__)
    end
  else
    def activate_mocked_functions_from_test_process, do: :noop
  end
end
