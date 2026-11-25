defmodule Electric.StatusMonitor do
  @moduledoc false
  use GenServer

  require Logger

  @type status() :: %{
          conn: :waiting_on_lock | :starting | :up | :sleeping,
          shape: :starting | :read_only | :up
        }

  @conditions [
    :pg_lock_acquired,
    :replication_client_ready,
    :admin_connection_pool_ready,
    :snapshot_connection_pool_ready,
    :shape_log_collector_ready,
    :supervisor_processes_ready,
    :integrety_checks_passed,
    :shape_metadata_ready
  ]

  @default_results for condition <- @conditions, into: %{}, do: {condition, {false, %{}}}

  @db_state_key :db_state
  @closed_key :shape_metadata_closed
  @journal_replay_key :shape_journal_replay
  @new_slot_purge_key :new_slot_purge_owed
  @spin_prevention_delay 10

  def start_link(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    GenServer.start_link(__MODULE__, stack_id, name: name(stack_id))
  end

  def init(stack_id) do
    Process.set_label({:status_monitor, stack_id})
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    :ets.new(ets_table(stack_id), [:named_table, :protected, read_concurrency: true])

    {:ok, %{stack_id: stack_id, waiters: MapSet.new()}}
  end

  @doc """
  Returns the high-level service status as a single atom.

  - `:active` — fully operational
  - `:waiting` — waiting on advisory lock, shape metadata loaded (can serve existing shapes read-only)
  - `:starting` — system is initializing (metadata not yet loaded or connection progressing)
  - `:sleeping` — connections scaled down

  This is what `/v1/health` reports and is a public contract. What a node can *serve* in
  each state is a separate question, answered by `wait_until/3`.
  """
  @spec service_status(String.t()) :: :active | :waiting | :starting | :sleeping
  def service_status(stack_id) do
    case status(stack_id) do
      %{conn: :up, shape: :up} -> :active
      %{conn: :waiting_on_lock, shape: :read_only} -> :waiting
      %{conn: :sleeping} -> :sleeping
      _ -> :starting
    end
  end

  @spec status(String.t()) :: status()
  def status(stack_id) do
    table = ets_table(stack_id)

    results = results(table)

    conn_status =
      case db_state(table) do
        :up -> conn_status_from_results(results)
        :sleeping -> :sleeping
      end

    shape_status = shape_status_from_results(results)

    %{conn: conn_status, shape: shape_status}
  end

  defp conn_status_from_results(%{pg_lock_acquired: {false, _}}), do: :waiting_on_lock

  defp conn_status_from_results(%{
         replication_client_ready: {true, _},
         admin_connection_pool_ready: {true, _},
         snapshot_connection_pool_ready: {true, _},
         integrety_checks_passed: {true, _}
       }),
       do: :up

  defp conn_status_from_results(_), do: :starting

  defp shape_status_from_results(%{@closed_key => _reason}), do: :starting

  defp shape_status_from_results(%{
         @journal_replay_key => :pending,
         pg_lock_acquired: {true, _}
       }),
       do: :starting

  defp shape_status_from_results(%{
         shape_metadata_ready: {true, _},
         shape_log_collector_ready: {true, _},
         supervisor_processes_ready: {true, _}
       }),
       do: :up

  defp shape_status_from_results(%{shape_metadata_ready: {true, _}}), do: :read_only

  defp shape_status_from_results(_), do: :starting

  def database_connections_going_to_sleep(stack_id) do
    GenServer.cast(name(stack_id), :database_connections_going_to_sleep)
  end

  def database_connections_waking_up(stack_id) do
    GenServer.cast(name(stack_id), :database_connections_waking_up)
  end

  def mark_shape_metadata_ready(stack_id, pid) do
    mark_condition_met(stack_id, :shape_metadata_ready, pid)
  end

  def hold_shape_serving_for_journal_replay(stack_id) do
    GenServer.call(name(stack_id), :hold_shape_serving_for_journal_replay)
  catch
    :exit, {:noproc, _} -> :ok
  end

  def prepare_journal_serving(stack_id, replay_pending?) do
    GenServer.call(name(stack_id), {:prepare_journal_serving, replay_pending?})
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  `:pending` from the moment the slot is about to seed the processed LSN over a journal until its
  replay has reached the shapes, then `:settled`. `nil` until a connection under the lock first
  prepares journal serving, which a stack without a journal never does; a leftover journal
  reclaimed with the switch off leaves it `:settled`.
  """
  @spec journal_replay(Electric.stack_id()) :: :pending | :settled | nil
  def journal_replay(stack_id) do
    :ets.lookup_element(ets_table(stack_id), @journal_replay_key, 2, nil)
  rescue
    ArgumentError -> nil
  end

  @doc """
  Stops the API serving the shapes the loaded metadata holds, read-only or not, until
  `reopen_shape_metadata/2`. Returns once no new request is admitted on it.

  A stack that stops its shape subsystem to purge every shape went from up to read-only as the
  collector stopped, and served the shapes it was about to purge from what it still had loaded.

  The close is kept apart from the metadata being loaded: an owner of the metadata that restarts
  loads it again and marks it ready, which serves nothing while it is closed.
  """
  @spec close_shape_metadata(String.t(), String.t()) :: :ok
  def close_shape_metadata(stack_id, reason) when is_binary(reason) do
    GenServer.call(name(stack_id), {:close_shape_metadata, reason})
  catch
    # With no monitor running, nothing is admitted.
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Closes the shape metadata as `close_shape_metadata/2` does unless the monitor has recorded the
  advisory lock as acquired, and returns whether it closed it.

  A connection manager that acquires the lock decides about the metadata after it, and reopens
  what it finds closed once it has purged or checked the timeline. A close made from an earlier
  read of the status, handled after the lock, would outlast that reopen.
  """
  @spec close_shape_metadata_unless_locked(String.t(), String.t()) :: boolean()
  def close_shape_metadata_unless_locked(stack_id, reason) when is_binary(reason) do
    GenServer.call(name(stack_id), {:close_shape_metadata_unless_locked, reason})
  catch
    :exit, {:noproc, _} -> false
  end

  @doc """
  Whether `close_shape_metadata/2` closed the shape metadata and nothing served it again since.
  The close outlives the connection manager that made it: one that takes over finds the purge
  that manager found owed, which the stored timeline may not record.

  The monitor answers after the closes sent to it before: a manager that died while its close
  waited in the queue left it there for the next one.
  """
  @spec shape_metadata_closed?(String.t()) :: boolean()
  def shape_metadata_closed?(stack_id) do
    GenServer.call(name(stack_id), :shape_metadata_closed?)
  rescue
    ArgumentError -> false
  catch
    :exit, {:noproc, _} -> false
  end

  @doc """
  Serves the shape metadata again, held by `pid`, if `close_shape_metadata/2` closed it; does
  nothing otherwise.
  """
  @spec reopen_shape_metadata(String.t(), pid()) :: :ok
  def reopen_shape_metadata(stack_id, pid) when is_pid(pid) do
    GenServer.cast(name(stack_id), {:reopen_shape_metadata, pid})
  end

  @doc """
  Records, with `true`, that a replication slot created over the shapes of an earlier one owes
  their purge, or clears that once the purge is done or nothing was there to purge.

  The record outlives the connection manager that made it, as the close does. The slot is in
  place for the one that takes over, and the stored timeline unchanged when the marker the slot
  owes was not written: that one purges from this record.
  """
  @spec owe_new_slot_purge(String.t(), boolean()) :: :ok
  def owe_new_slot_purge(stack_id, owed?) when is_boolean(owed?) do
    GenServer.call(name(stack_id), {:owe_new_slot_purge, owed?})
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Whether a replication slot was created over the shapes of an earlier one and their purge is
  not done yet: see `owe_new_slot_purge/2`.

  The monitor answers after the records sent to it before: a manager that died while its record
  waited in the queue left it there for the next one.
  """
  @spec new_slot_purge_owed?(String.t()) :: boolean()
  def new_slot_purge_owed?(stack_id) do
    GenServer.call(name(stack_id), :new_slot_purge_owed?)
  rescue
    ArgumentError -> false
  catch
    :exit, {:noproc, _} -> false
  end

  def mark_pg_lock_acquired(stack_id, lock_pid) do
    mark_condition_met(stack_id, :pg_lock_acquired, lock_pid)
  end

  def mark_replication_client_ready(stack_id, client_pid) do
    mark_condition_met(stack_id, :replication_client_ready, client_pid)
  end

  def mark_connection_pool_ready(stack_id, :admin, pool_pid) do
    mark_condition_met(stack_id, :admin_connection_pool_ready, pool_pid)
  end

  def mark_connection_pool_ready(stack_id, :snapshot, pool_pid) do
    mark_condition_met(stack_id, :snapshot_connection_pool_ready, pool_pid)
  end

  def mark_shape_log_collector_ready(stack_id, collector_pid) do
    mark_condition_met(stack_id, :shape_log_collector_ready, collector_pid)
  end

  def mark_supervisor_processes_ready(stack_id, canary_pid) do
    mark_condition_met(stack_id, :supervisor_processes_ready, canary_pid)
  end

  def mark_integrety_checks_passed(stack_id, connection_manager_pid) do
    mark_condition_met(stack_id, :integrety_checks_passed, connection_manager_pid)
  end

  def mark_pg_lock_as_errored(stack_id, message) when is_binary(message) do
    mark_condition_as_errored(stack_id, :pg_lock_acquired, message)
  end

  def mark_replication_client_as_errored(stack_id, message) when is_binary(message) do
    mark_condition_as_errored(stack_id, :replication_client_ready, message)
  end

  def mark_connection_pool_as_errored(stack_id, :admin, message) when is_binary(message) do
    mark_condition_as_errored(stack_id, :admin_connection_pool_ready, message)
  end

  def mark_connection_pool_as_errored(stack_id, :snapshot, message) when is_binary(message) do
    mark_condition_as_errored(stack_id, :snapshot_connection_pool_ready, message)
  end

  defp mark_condition_as_errored(stack_id, condition, error) do
    GenServer.cast(name(stack_id), {:condition_errored, condition, error})
  end

  defp mark_condition_met(stack_id, condition, process) do
    GenServer.cast(name(stack_id), {:condition_met, condition, process})
  end

  @doc """
  Whether the API serves existing shapes now: the stack is at least `:read_only` (see
  `wait_until/3`), or its connections are scaled down, which a request wakes.
  """
  @spec serves_existing_shapes?(String.t()) :: boolean()
  def serves_existing_shapes?(stack_id) do
    level_reached(:read_only, stack_id) != :not_ready
  end

  @doc """
  Wait until the system reaches at least the required level.

  Levels (ordered):
  - `:read_only` — can serve existing shapes (waiting on lock, metadata loaded)
  - `:active` — fully operational, can create shapes and stream changes

  Returns:
  - `{:ok, :active}` when fully operational
  - `{:ok, :read_only}` when existing shapes can be served (only for `:read_only` level)
  - `:conn_sleeping` when connections are sleeping
  - `{:error, message}` on timeout
  """
  def wait_until(stack_id, level, opts \\ [])
      when level in [:read_only, :active] do
    case level_reached(level, stack_id) do
      {:ok, _mode} = reached ->
        reached

      :conn_sleeping ->
        if Keyword.get(opts, :block_on_conn_sleeping, false) do
          do_wait_until(stack_id, level, opts)
        else
          :conn_sleeping
        end

      :not_ready ->
        do_wait_until(stack_id, level, opts)
    end
  end

  defp do_wait_until(stack_id, level, opts) do
    timeout = Keyword.fetch!(opts, :timeout)

    try do
      case stack_id |> name() |> GenServer.whereis() do
        nil ->
          maybe_retry_wait_until(
            stack_id,
            level,
            opts,
            timeout,
            "Status monitor not found for stack ID: #{stack_id}"
          )

        pid when is_pid(pid) ->
          GenServer.call(pid, {:wait_until, level, timeout}, :infinity)
      end
    rescue
      ArgumentError ->
        maybe_retry_wait_until(
          stack_id,
          level,
          opts,
          timeout,
          "Stack ID not recognised: #{stack_id}"
        )
    catch
      :exit, _reason ->
        maybe_retry_wait_until(
          stack_id,
          level,
          opts,
          timeout,
          "Stack #{inspect(stack_id)} has terminated"
        )
    end
  end

  defp maybe_retry_wait_until(_stack_id, _level, _opts, timeout, last_error)
       when timeout <= @spin_prevention_delay do
    {:error, last_error}
  end

  defp maybe_retry_wait_until(stack_id, level, opts, timeout, _) do
    Process.sleep(@spin_prevention_delay)

    remaining_timeout =
      case timeout do
        :infinity -> :infinity
        _ -> timeout - @spin_prevention_delay
      end

    wait_until(stack_id, level, Keyword.put(opts, :timeout, remaining_timeout))
  end

  @doc "Convenience wrapper: wait until fully active. Returns `:ok` on success."
  def wait_until_active(stack_id, opts \\ []) do
    case wait_until(stack_id, :active, opts) do
      {:ok, :active} -> :ok
      other -> other
    end
  end

  @doc """
  Non-blocking version of `wait_until/3`.

  Subscribes to status updates from StatusMonitor. Returns a reference, and the caller receives
  exactly one message for it, tagged `{#{inspect(__MODULE__)}, ref}` so it is distinguishable from
  other replies in the mailbox:

    * `{{#{inspect(__MODULE__)}, ref}, {:ok, level}}` once the status reaches the requested level
    * `{{#{inspect(__MODULE__)}, ref}, {:error, :not_running}}` when there is no status monitor to
      ask, which is the normal state part-way through a stack restart
    * `{{#{inspect(__MODULE__)}, :down}, ref, :process, pid, reason}` when the monitor dies
      while the wait is outstanding

  No timeout is applied, so the last two matter: the wait is a bare `send/2` with no link, and a
  caller that only matches the success shape stays parked on a reply that can no longer arrive.

  The returned reference is also the process monitor that watches the status monitor. It stays
  on the caller until the monitor goes down, so a caller that receives the first reply calls
  `Process.demonitor(ref, [:flush])`. Otherwise every successful wait leaves one more monitor on
  a long-lived caller, and a late down message when the status monitor eventually exits.
  """
  @spec wait_until_async(String.t(), :read_only | :active) :: reference()
  def wait_until_async(stack_id, level) when level in [:read_only, :active] do
    case stack_id |> name() |> GenServer.whereis() do
      nil ->
        ref = make_ref()
        send(self(), {{__MODULE__, ref}, {:error, :not_running}})
        ref

      pid ->
        ref = Process.monitor(pid, tag: {__MODULE__, :down})

        send(pid, {:"$gen_call", {self(), {__MODULE__, ref}}, {:wait_until, level, :infinity}})

        ref
    end
  end

  # Only used in tests
  def wait_for_messages_to_be_processed(stack_id) do
    GenServer.call(name(stack_id), :wait_for_messages_to_be_processed)
  end

  def handle_cast({:condition_met, condition, process}, state)
      when condition in @conditions do
    Process.monitor(process, tag: {:down, condition})
    table = ets_table(state.stack_id)
    :ets.insert(table, {condition, {true, %{process: process}}})

    # Replayed rows may still be in the local writer's buffers. Resume only
    # once the collector has replayed them and the writer path can serve them.
    if condition in [:shape_log_collector_ready, :supervisor_processes_ready] and
         :ets.lookup_element(table, @journal_replay_key, 2, nil) == :pending and
         condition_met?(table, :shape_log_collector_ready) and
         condition_met?(table, :supervisor_processes_ready) do
      :ets.insert(table, {@journal_replay_key, :settled})
    end

    {:noreply, maybe_reply_to_waiters(state)}
  end

  def handle_cast({:condition_errored, condition, error}, state) do
    :ets.insert(ets_table(state.stack_id), {condition, {false, %{error: error}}})
    {:noreply, state}
  end

  def handle_cast({:reopen_shape_metadata, pid}, state) do
    table = ets_table(state.stack_id)

    cond do
      not :ets.member(table, @closed_key) ->
        {:noreply, state}

      match?([{_, {true, _}}], :ets.lookup(table, :shape_metadata_ready)) ->
        :ets.delete(table, @closed_key)
        {:noreply, maybe_reply_to_waiters(state)}

      true ->
        :ets.delete(table, @closed_key)
        handle_cast({:condition_met, :shape_metadata_ready, pid}, state)
    end
  end

  def handle_cast(:database_connections_going_to_sleep, state) do
    :ets.insert(ets_table(state.stack_id), {@db_state_key, :sleeping})
    {:noreply, state}
  end

  def handle_cast(:database_connections_waking_up, state) do
    # Only update the ETS table on the first request. Subsequent requests will just wait for the stack to become active.
    case :ets.lookup_element(ets_table(state.stack_id), @db_state_key, 2) do
      :sleeping -> :ets.insert(ets_table(state.stack_id), {@db_state_key, :up})
      :up -> :noop
    end

    {:noreply, state}
  end

  def handle_call({:wait_until, level, timeout}, from, %{waiters: waiters} = state) do
    case check_level(level, state.stack_id) do
      {:ok, _} = reply ->
        {:reply, reply, state}

      :not_ready ->
        if timeout != :infinity do
          Process.send_after(self(), {:timeout_waiter, {from, level}}, timeout)
        end

        {:noreply, %{state | waiters: MapSet.put(waiters, {from, level})}}
    end
  end

  def handle_call(:wait_for_messages_to_be_processed, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call(:hold_shape_serving_for_journal_replay, _from, state) do
    table = ets_table(state.stack_id)
    :ets.insert(table, {@journal_replay_key, :pending})
    :ets.delete(table, :shape_log_collector_ready)
    {:reply, :ok, state}
  end

  def handle_call({:prepare_journal_serving, replay_pending?}, _from, state) do
    table = ets_table(state.stack_id)

    # On reconnect the manager can keep a running collector that has already replayed the journal.
    if replay_pending? and not condition_met?(table, :shape_log_collector_ready) do
      :ets.insert(table, {@journal_replay_key, :pending})
    else
      :ets.insert_new(table, {@journal_replay_key, :settled})
    end

    {:reply, :ok, state}
  end

  def handle_call({:close_shape_metadata, reason}, _from, state) do
    :ets.insert(ets_table(state.stack_id), {@closed_key, reason})
    {:reply, :ok, state}
  end

  def handle_call({:close_shape_metadata_unless_locked, reason}, _from, state) do
    table = ets_table(state.stack_id)

    case :ets.lookup(table, :pg_lock_acquired) do
      [{_, {true, _}}] ->
        {:reply, false, state}

      _ ->
        :ets.insert(table, {@closed_key, reason})
        {:reply, true, state}
    end
  end

  def handle_call(:shape_metadata_closed?, _from, state) do
    {:reply, :ets.member(ets_table(state.stack_id), @closed_key), state}
  end

  def handle_call({:owe_new_slot_purge, true}, _from, state) do
    :ets.insert(ets_table(state.stack_id), {@new_slot_purge_key, true})
    {:reply, :ok, state}
  end

  def handle_call({:owe_new_slot_purge, false}, _from, state) do
    :ets.delete(ets_table(state.stack_id), @new_slot_purge_key)
    {:reply, :ok, state}
  end

  def handle_call(:new_slot_purge_owed?, _from, state) do
    {:reply, :ets.member(ets_table(state.stack_id), @new_slot_purge_key), state}
  end

  defp check_level(level, stack_id) do
    case level_reached(level, stack_id) do
      {:ok, _mode} = reached -> reached
      _ -> :not_ready
    end
  end

  defp condition_met?(table, condition) do
    match?({true, _}, :ets.lookup_element(table, condition, 2, nil))
  end

  @spec level_reached(:read_only | :active, String.t()) ::
          {:ok, :active | :read_only} | :conn_sleeping | :not_ready
  defp level_reached(level, stack_id) do
    case status(stack_id) do
      %{conn: :up, shape: :up} -> {:ok, :active}
      %{conn: :sleeping} -> :conn_sleeping
      %{shape: :read_only} when level == :read_only -> {:ok, :read_only}
      _ -> :not_ready
    end
  end

  def handle_info({{:down, condition}, _ref, :process, pid, _reason}, state) do
    :ets.match_delete(ets_table(state.stack_id), {:_, {true, %{process: pid}}})

    Logger.warning(
      "#{inspect(__MODULE__)} condition failed: #{inspect(condition)}. Status #{inspect(status(state.stack_id))}"
    )

    {:noreply, state}
  end

  def handle_info({:timeout_waiter, {from, _level} = waiter}, state) do
    if MapSet.member?(state.waiters, waiter) do
      GenServer.reply(from, {:error, timeout_message(state.stack_id)})
      {:noreply, %{state | waiters: MapSet.delete(state.waiters, waiter)}}
    else
      {:noreply, state}
    end
  end

  defp maybe_reply_to_waiters(%{waiters: waiters} = state)
       when map_size(waiters) == 0,
       do: state

  defp maybe_reply_to_waiters(state) do
    waiters =
      Enum.reduce(state.waiters, state.waiters, fn {from, level} = waiter, acc ->
        case check_level(level, state.stack_id) do
          {:ok, _} = reply ->
            GenServer.reply(from, reply)
            MapSet.delete(acc, waiter)

          :not_ready ->
            acc
        end
      end)

    %{state | waiters: waiters}
  end

  defp db_state(table) do
    :ets.lookup_element(table, @db_state_key, 2, :up)
  rescue
    ArgumentError ->
      # This happens when the table is not found, which means the
      # process has not been started yet
      :up
  end

  defp results(table) do
    results = table |> :ets.tab2list() |> Map.new()
    Map.merge(@default_results, results)
  rescue
    ArgumentError ->
      # This happens when the table is not found, which means the
      # process has not been started yet
      @default_results
  end

  def timeout_message(stack_id) do
    case stack_id |> ets_table() |> results() do
      %{timeout_message: message} when is_binary(message) ->
        message

      %{pg_lock_acquired: {false, details}} ->
        "Timeout waiting for Postgres lock acquisition" <> format_details(details)

      %{replication_client_ready: {false, details}} when details == %{} ->
        "Timeout waiting for replication client to be ready. " <>
          "Check that you don't have pending transactions in the database. " <>
          "Electric has to wait for all pending transactions to commit or rollback " <>
          "before it can create the replication slot."

      %{replication_client_ready: {false, details}} ->
        "Timeout waiting for replication client to be ready" <> format_details(details)

      %{admin_connection_pool_ready: {false, details}} ->
        "Timeout waiting for database connection pool (metadata) to be ready" <>
          format_details(details)

      %{snapshot_connection_pool_ready: {false, details}} ->
        "Timeout waiting for database connection pool (snapshot) to be ready" <>
          format_details(details)

      %{@closed_key => reason} ->
        "Timeout waiting for shape metadata to be loaded" <> format_details(%{error: reason})

      %{@journal_replay_key => :pending} ->
        "Timeout waiting for transaction journal replay"

      %{shape_metadata_ready: {false, details}} ->
        "Timeout waiting for shape metadata to be loaded" <> format_details(details)

      %{shape_log_collector_ready: {false, details}} ->
        "Timeout waiting for shape data to be loaded" <> format_details(details)

      %{supervisor_processes_ready: {false, details}} ->
        "Timeout waiting for stack restart" <> format_details(details)

      %{integrety_checks_passed: {false, details}} ->
        "Timeout waiting for integrety checks" <> format_details(details)
    end
  end

  defp format_details(%{error: error}), do: ": #{error}"
  defp format_details(_), do: ""

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  defp ets_table(stack_id), do: :"#{inspect(__MODULE__)}:#{stack_id}"
end
