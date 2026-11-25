defmodule Electric.ShapeCache.ShapeStatusOwner do
  @moduledoc """
  Owns the ETS table and the ShapeStatus state.

  This process creates the ETS table for shapes and initializes
  `Electric.ShapeCache.ShapeStatus` early in the supervision tree so that
  dependent processes (e.g., shape consumers) can use a single, shared
  ShapeStatus instance regardless of their own supervisor start order.

  Initialization is done asynchronously via `handle_continue` to avoid
  blocking the supervision tree startup on potentially slow SQLite reads.
  """

  use GenServer

  require Logger

  alias Electric.ShapeCache.ShapeStatus

  @schema NimbleOptions.new!(
            stack_id: [type: :string, required: true],
            timeline_opts: [type: :keyword_list, default: []]
          )

  @purge_check_interval 1_000

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  @doc """
  Refresh shape metadata from SQLite after lock acquisition.

  Uses an infinite timeout because on laggy storage with an incorrectly
  shut-down database this can take a long time.

  Returns once the metadata is refreshed. The reclaim of orphaned shape storage runs after that,
  in this process: replication waits for this call, and the reclaim's listing of every shape
  directory can take a long time at boot with a large shape count. A refresh
  called meanwhile waits for it.

  Returns `:purge_owed` when a purge of every shape is owed, or the stored timeline cannot be
  read: nothing is loaded, serving is closed (`Electric.StatusMonitor.close_shape_metadata/2`),
  and the caller unloads the metadata once it has stopped what reads it (`ShapeStatus.unload/1`),
  then purges the catalog before it serves shapes again, or refreshes again once it finds that
  the record owes no purge.
  """
  def refresh(stack_id) do
    GenServer.call(name(stack_id), :refresh, :infinity)
  end

  def start_link(opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @schema) do
      opts = Map.new(opts)
      GenServer.start_link(__MODULE__, opts, name: name(opts.stack_id))
    end
  end

  @impl true
  def init(config) do
    stack_id = config.stack_id

    Process.set_label({:shape_status_owner, stack_id})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    :ok = Electric.LsnTracker.initialize(stack_id)

    {:ok,
     %{
       stack_id: stack_id,
       timeline_opts: config.timeline_opts,
       initial_refresh?: true,
       purge_check: nil
     }, {:continue, :initialize}}
  end

  @impl true
  def handle_continue(:initialize, state) do
    purge_owed? = purge_owed?(state)

    # The metadata marked ready below serves nothing while a purge is owed, until the connection
    # manager has checked the timeline or purged: the stack reports itself starting, not waiting.
    if purge_owed?, do: close_for_owed_purge(state)

    # Initialize shape metadata from SQLite so we can serve shapes in
    # read-only mode before the advisory lock is acquired
    :ok = ShapeStatus.initialize(state.stack_id, load?: not purge_owed?)

    # Signal that shape metadata is loaded and shapes can be served read-only
    Electric.StatusMonitor.mark_shape_metadata_ready(state.stack_id, self())

    Logger.notice("Shape metadata initialized, entering read-only mode")

    if state.timeline_opts != [], do: schedule_purge_check()

    {:noreply, state, :hibernate}
  end

  def handle_continue({:reclaim_orphaned_shape_storage, valid_shape_count}, state) do
    :ok = ShapeStatus.reclaim_orphaned_shape_storage(state.stack_id, valid_shape_count)
    {:noreply, state, :hibernate}
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    # The metadata loaded before a manager that left the purge owed died is the caller's to
    # unload: a shape subsystem still running reads it, and is stopped first.
    if purge_owed?(state) do
      # The metadata loaded at boot stays loaded until the caller unloads it, and is not served
      # meanwhile: a caller that dies before it acts on the reply leaves it closed for the next.
      close_for_owed_purge(state)
      Logger.notice("Shape metadata not loaded: a purge of every shape is owed")
      {:reply, :purge_owed, %{state | initial_refresh?: false}}
    else
      # Serving closed for a purge found owed stays closed: a record that owes none here may be
      # one whose owed purge a connection manager failed to write. The manager opens it again
      # once its own timeline check finds nothing owed, or its purge ends.
      {:ok, valid_shape_count} =
        ShapeStatus.refresh_shape_meta(state.stack_id, initial?: initial_refresh?(state))

      {:reply, :ok, %{state | initial_refresh?: false},
       {:continue, {:reclaim_orphaned_shape_storage, valid_shape_count}}}
    end
  end

  # The lock holder can confirm a purge of every shape while this instance waits for the lock and
  # serves the metadata it loaded: those shapes missed writes made before the new slot's stream
  # starts. The shared record is read once a second while serving read-only, in a process of its
  # own, so that neither a request, the status monitor nor the refresh after the lock waits on
  # that read. The monitor makes the close only until it records the lock: the status the check
  # reads can be older than the lock, and the manager that took it purges or reopens what a close
  # made before it left closed. The check is linked to this process: one that outlived it would
  # reach the monitor by name, and could close an instance that loaded its metadata since.
  @impl true
  def handle_info(:check_purge, %{purge_check: nil} = state) do
    {:ok, pid} = Task.start_link(fn -> check_purge(state) end)
    {:noreply, %{state | purge_check: {pid, Process.monitor(pid)}}}
  end

  # A check that comes while another runs is dropped: the next starts once that one ends.
  def handle_info(:check_purge, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{purge_check: {_, ref}} = state) do
    schedule_purge_check()
    {:noreply, %{state | purge_check: nil}, :hibernate}
  end

  defp schedule_purge_check, do: Process.send_after(self(), :check_purge, @purge_check_interval)

  defp check_purge(%{stack_id: stack_id, timeline_opts: timeline_opts}) do
    Logger.metadata(stack_id: stack_id)

    if Electric.StatusMonitor.service_status(stack_id) == :waiting and
         Electric.Timeline.purge_confirmed?(timeline_opts) and
         Electric.StatusMonitor.close_shape_metadata_unless_locked(
           stack_id,
           "a purge of every shape is owed"
         ) do
      Logger.notice("Shape metadata closed: the lock holder confirmed a purge of every shape")
    end
  end

  # The first refresh runs before any consumer starts, and so does one while the shape subsystem is
  # stopped, as the connection manager keeps it while the metadata is unloaded. The refresh that
  # loads it again came after one that left it unloaded, and took the detaching removal, which
  # needs the collector: it left the storage of every shape no longer in the catalog on disk.
  defp initial_refresh?(%{initial_refresh?: true}), do: true

  defp initial_refresh?(state),
    do: is_nil(GenServer.whereis(Electric.Shapes.Supervisor.name(state.stack_id)))

  defp close_for_owed_purge(%{stack_id: stack_id}),
    do: Electric.StatusMonitor.close_shape_metadata(stack_id, "a purge of every shape is owed")

  # A timeline change leaves the purge owed in the stored timeline until the connection manager
  # completes it. A failed purge step can leave the old timeline's shapes in the catalog and their
  # logs on disk, and loading them here served them again. A read of the record that fails says
  # nothing about the marker it holds, so it loads none of them either.
  defp purge_owed?(%{timeline_opts: []}), do: false

  defp purge_owed?(%{timeline_opts: timeline_opts}),
    do: Electric.Timeline.purge_owed?(timeline_opts)
end
