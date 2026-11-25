defmodule Electric.Replication.PublicationManager.Configurator do
  @moduledoc """
  Configures and maintains a PostgreSQL publication on behalf of
  Electric.Replication.PublicationManager.RelationTracker.

  It receives requests to update the publication such that a given
  set of relations are published with REPLICA IDENTITY FULL, and
  performs the necessary SQL commands to ensure that the publication
  matches the requested set of relations.

  Each relation is updated individually to avoid blocking all other
  operations on the publication due to locks held on individual tables.
  """
  use GenServer

  require Logger

  alias Electric.Replication.PublicationManager.RelationTracker
  alias Electric.Postgres.Configuration
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Utils
  alias Electric.Telemetry.OpenTelemetry

  @enforce_keys [
    :stack_id,
    :publication_name,
    :db_pool,
    :manual_table_publishing?,
    :can_alter_publication?,
    :scheduled_filters,
    :update_debounce_timeout,
    :scheduled_update_ref
  ]

  # How long an addition waits for the removal of a shape on its table from before to reach the
  # disk, from the removal's start, and how often it looks. A shape waiting for the addition gives
  # up after five seconds.
  @removal_timeout 3_000
  @removal_recheck_interval 50
  # Bound the drain work and leave room for DDL in the default four-connection admin pool.
  @max_concurrent_drains 3
  # How long a drain that holds the only connection of the admin pool waits for the writers on its
  # table the first time it tries: other users of the pool wait about as long as for a busy query.
  # Each try of the table the writers outlast doubles the wait, up to the five seconds a drain on a
  # larger pool waits. The connection is held that long and for the membership read after it.
  @single_connection_drain_lock_timeout 100
  @single_connection_drain_max_lock_timeout 5_000
  @recheck_initial_delay 100
  @recheck_max_delay 5_000

  defstruct @enforce_keys ++
              [
                checked_filter_revision: 0,
                max_concurrent_drains: @max_concurrent_drains,
                drain_lock_timeout: nil,
                drain_lock_timeouts: %{},
                drains: %{},
                queued_drains: [],
                removals_to_confirm: %{},
                slot_watch: nil,
                recheck_ref: nil,
                recheck_delay: @recheck_initial_delay
              ]

  @typep removal() :: %{
           relations: MapSet.t(Electric.relation_id()),
           task: ShapeCleaner.removal_task(),
           deadline: integer(),
           read:
             {pid(), reference(), reference()}
             | {:answered, boolean()}
             | {:behind, pid()}
             | nil
         }

  @typep drain() ::
           {Electric.oid_relation(), Electric.relation_id(), Configuration.publication_status()}

  @type state :: %__MODULE__{
          stack_id: Electric.stack_id(),
          publication_name: String.t(),
          manual_table_publishing?: boolean(),
          can_alter_publication?: boolean(),
          scheduled_filters: RelationTracker.filter_snapshot() | nil,
          update_debounce_timeout: timeout(),
          scheduled_update_ref: nil | reference(),
          checked_filter_revision: non_neg_integer(),
          max_concurrent_drains: pos_integer(),
          drain_lock_timeout: pos_integer() | nil,
          drain_lock_timeouts: %{
            Electric.oid_relation() => {Electric.relation_id(), pos_integer()}
          },
          drains: %{reference() => {drain(), Task.t()}},
          queued_drains: [drain()],
          removals_to_confirm: %{Electric.shape_handle() => removal()},
          slot_watch: reference() | nil,
          recheck_ref: reference() | nil,
          recheck_delay: pos_integer()
        }

  def name(stack_ref), do: Electric.ProcessRegistry.name(stack_ref, __MODULE__)

  @doc false
  # A drain holds an admin connection while it waits for the writers on its table, up to the
  # action timeout. One connection is left to the configurator's own reads and to every other user
  # of the pool. A pool of one has none to spare, and runs one drain at a time, which waits for the
  # writers only briefly each time it tries (`drain_lock_timeout/1`).
  def max_concurrent_drains(nil), do: @max_concurrent_drains

  def max_concurrent_drains(admin_pool_size),
    do: admin_pool_size |> Kernel.-(1) |> min(@max_concurrent_drains) |> max(1)

  # A drain that cannot have its lock within the wait fails as one past the action timeout does,
  # and the shapes waiting on the table retry: a writer still open is waited out over tries. A
  # failed `LOCK` leaves the lock queue, and lets in the writers queued behind it, so a table whose
  # writers keep overlapping is drained only by a try that waits as long as they last: each try of a
  # table whose wait ran out doubles its next wait (`lengthen_drain_wait/2`). A try that fails
  # otherwise, a lost connection say, leaves it as it was. A drained table starts over, and a table
  # no longer owed a drain loses its wait (`keep_drain_waits/3`). A wait is the membership's it was
  # measured on: a table dropped from the publication and added again starts over too.
  defp drain_lock_timeout(1), do: @single_connection_drain_lock_timeout
  defp drain_lock_timeout(_admin_pool_size), do: nil

  defp drain_wait(%{drain_lock_timeout: nil}, _drain), do: nil

  defp drain_wait(state, {filter, membership, _status}) do
    case Map.get(state.drain_lock_timeouts, filter) do
      {^membership, wait} -> wait
      _none_or_another_membership -> state.drain_lock_timeout
    end
  end

  defp lengthen_drain_wait(%{drain_lock_timeout: nil} = state, _drain), do: state

  defp lengthen_drain_wait(state, {filter, membership, _status} = drain) do
    wait = min(drain_wait(state, drain) * 2, @single_connection_drain_max_lock_timeout)
    %{state | drain_lock_timeouts: Map.put(state.drain_lock_timeouts, filter, {membership, wait})}
  end

  defp reset_drain_wait(state, filter),
    do: %{state | drain_lock_timeouts: Map.delete(state.drain_lock_timeouts, filter)}

  defp keep_drain_waits(state, owed, filters) do
    waits =
      Map.filter(state.drain_lock_timeouts, fn {{oid, _rel} = filter, _wait} ->
        MapSet.member?(owed, oid) and MapSet.member?(filters, filter)
      end)

    %{state | drain_lock_timeouts: waits}
  end

  def configure_publication(opts, snapshot) do
    GenServer.cast(name(opts), {:update_publication, snapshot})
  end

  def start_link(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)

    opts = Keyword.put_new(opts, :db_pool, Electric.Connection.Manager.admin_pool(stack_id))

    GenServer.start_link(__MODULE__, opts, name: name(stack_id))
  end

  @impl true
  def init(opts) do
    opts = Map.new(opts)

    Process.set_label({:publication_manager_configurator, opts.stack_id})
    Logger.metadata(stack_id: opts.stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: opts.stack_id)

    {
      :ok,
      %__MODULE__{
        stack_id: opts.stack_id,
        publication_name: opts.publication_name,
        db_pool: opts.db_pool,
        manual_table_publishing?: opts.manual_table_publishing?,
        can_alter_publication?: true,
        scheduled_filters: nil,
        update_debounce_timeout: opts.update_debounce_timeout,
        scheduled_update_ref: nil,
        max_concurrent_drains: max_concurrent_drains(Map.get(opts, :admin_pool_size)),
        drain_lock_timeout: drain_lock_timeout(Map.get(opts, :admin_pool_size))
      },
      {:continue, :fetch_initial_filters}
    }
  end

  @impl true
  def handle_cast({:update_publication, {revision, _}}, state)
      when revision <= state.checked_filter_revision do
    {:noreply, state}
  end

  def handle_cast({:update_publication, {revision, _}}, %{scheduled_filters: {newer, _}} = state)
      when revision <= newer do
    {:noreply, state}
  end

  def handle_cast({:update_publication, snapshot}, %{scheduled_update_ref: nil} = state) do
    ref =
      :erlang.start_timer(state.update_debounce_timeout, self(), :debounced_update_publication)

    {:noreply, %{state | scheduled_filters: snapshot, scheduled_update_ref: ref}}
  end

  def handle_cast({:update_publication, snapshot}, state) do
    {:noreply, %{state | scheduled_filters: snapshot}}
  end

  @impl true
  def handle_info(
        {:timeout, ref, :debounced_update_publication},
        %{scheduled_update_ref: ref} = state
      ) do
    {
      :noreply,
      %{state | scheduled_filters: nil, scheduled_update_ref: nil},
      {:continue, {:check_publication, state.scheduled_filters}}
    }
  end

  def handle_info({:timeout, _ref, :debounced_update_publication}, state), do: {:noreply, state}

  def handle_info({:timeout, ref, :recheck_publication}, %{recheck_ref: ref} = state) do
    filters = RelationTracker.fetch_current_filters!(stack_id: state.stack_id)
    {:noreply, %{state | recheck_ref: nil}, {:continue, {:check_publication, filters}}}
  end

  def handle_info({:timeout, _ref, :recheck_publication}, state), do: {:noreply, state}

  def handle_info({ref, result}, %{drains: drains} = state) when is_map_key(drains, ref) do
    Process.demonitor(ref, [:flush])
    {{drain, _task}, drains} = Map.pop!(drains, ref)
    handle_drain_result(drain, result, %{state | drains: drains})
  end

  # The answer of a catalog read a pass stopped waiting for is kept for the next pass, which it
  # starts: a store whose reads all outlast a pass's budget would otherwise never confirm a removal.
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Enum.find(state.removals_to_confirm, &match?({_handle, %{read: {_, ^ref, _}}}, &1)) do
      {handle, %{read: {_read, _reply, monitor}} = removal} ->
        Process.demonitor(monitor, [:flush])
        keep_read_answer(handle, removal, result, state)

      nil ->
        {:noreply, state}
    end
  end

  # A read that ended with no answer, a kill from outside say, leaves its removal to a pass soon
  # after, which starts another: with no shape waiting on its table, no other pass may come before
  # the periodic one. Its reply, had it sent one, came before this.
  def handle_info({:DOWN, monitor, :process, _read, _reason}, state) do
    case Enum.find(state.removals_to_confirm, &match?({_handle, %{read: {_, _, ^monitor}}}, &1)) do
      {handle, removal} ->
        removals = Map.put(state.removals_to_confirm, handle, %{removal | read: nil})
        {:noreply, recheck_soon(%{state | removals_to_confirm: removals})}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:catalog_read_slot_freed, watch, :process, _read, _reason},
        %{slot_watch: watch} = state
      ) do
    {:noreply, recheck_soon(%{state | slot_watch: nil})}
  end

  def handle_info({:catalog_read_slot_freed, _watch, :process, _read, _reason}, state),
    do: {:noreply, state}

  # A drain that finds its table other than its pass planned cancels the other drains, with any
  # answer they have sent, as when a pass finds the publication changed: the change may have moved
  # their tables after they verified them, a migration that drops tables and adds them back say, or
  # the whole publication. A recheck stays owed should the check that runs now fail on a lost
  # connection.
  defp handle_drain_result(_drain, :publication_changed, state) do
    filters = RelationTracker.fetch_current_filters!(stack_id: state.stack_id)

    {:noreply, recheck_soon(update_drains([], state)), {:continue, {:check_publication, filters}}}
  end

  defp handle_drain_result({filter, membership, _status}, {:ok, observed}, state)
       when membership == observed do
    notify_filter_result(filter, {:ok, {:drained, membership}}, state)
    {:noreply, state |> reset_drain_wait(filter) |> start_drains()}
  end

  defp handle_drain_result(drain, {:ok, _other_membership}, state) do
    handle_drain_result(drain, :publication_changed, state)
  end

  defp handle_drain_result(
         {filter, _membership, _status} = drain,
         {:lock_wait_ran_out, result},
         state
       ) do
    notify_filter_result(filter, result, state)
    {:noreply, state |> lengthen_drain_wait(drain) |> start_drains()}
  end

  defp handle_drain_result({filter, _membership, _status}, {:error, _} = result, state) do
    notify_filter_result(filter, result, state)
    {:noreply, start_drains(state)}
  end

  defp keep_read_answer(handle, removal, {:ok, exists?}, state) when is_boolean(exists?) do
    removals = Map.put(state.removals_to_confirm, handle, %{removal | read: {:answered, exists?}})
    filters = RelationTracker.fetch_current_filters!(stack_id: state.stack_id)

    {:noreply, %{state | removals_to_confirm: removals},
     {:continue, {:check_publication, filters}}}
  end

  defp keep_read_answer(handle, removal, _error, state) do
    removals = Map.put(state.removals_to_confirm, handle, %{removal | read: nil})
    {:noreply, recheck_soon(%{state | removals_to_confirm: removals})}
  end

  @impl true
  def terminate(_reason, state) do
    cancel_drains(state.drains)
  end

  @impl true
  def handle_continue(:fetch_initial_filters, state) do
    filters = RelationTracker.fetch_current_filters!(stack_id: state.stack_id)
    {:noreply, state, {:continue, {:check_publication, filters}}}
  end

  def handle_continue({:check_publication, {revision, _}}, state)
      when revision <= state.checked_filter_revision do
    {:noreply, state}
  end

  def handle_continue({:check_publication, {revision, filters}}, state) do
    state =
      %{state | checked_filter_revision: revision}
      |> cancel_superseded_update()
      |> cancel_recheck()

    case check_publication_status(state) do
      {:ok, status} ->
        state = %{state | can_alter_publication?: status.can_alter_publication?}

        RelationTracker.notify_publication_status(status, stack_id: state.stack_id)

        {:noreply, state, {:continue, {:configure_filters, filters, status}}}

      {:error, err} ->
        handle_catalog_error(err, state)
    end
  end

  def handle_continue({:configure_filters, filters, checked_status}, state) do
    case determine_publication_relation_actions(state, filters, checked_status) do
      {:ok, status, served, recheck_unserved?, cleanup_owed, dropped_oids,
       %{
         memberships: memberships,
         to_preserve: to_preserve,
         to_invalidate: to_invalidate,
         to_add: to_add,
         to_drop: to_drop,
         to_configure_replica_identity: to_configure_replica_identity
       }} ->
        state = %{state | can_alter_publication?: status.can_alter_publication?}

        changed =
          for {{oid, _} = filter, membership} <- served,
              not MapSet.member?(dropped_oids, oid),
              memberships[filter] != membership,
              into: MapSet.new(),
              do: filter

        dropping = if can_update_publication?(state), do: to_drop, else: MapSet.new()

        stale =
          to_add
          |> MapSet.union(changed)
          |> MapSet.union(
            MapSet.filter(filters, fn {oid, _} -> MapSet.member?(cleanup_owed, oid) end)
          )

        newly_owed =
          if recheck_unserved? or not can_update_publication?(state) do
            MapSet.reject(to_preserve, &is_map_key(served, &1))
          else
            MapSet.new()
          end

        # Record drops before attempting them so a restart after the database commits still owes
        # the barrier. Record stale handles before starting cleanup, so a restart can finish it
        # even after the handles are gone from memory.
        {owed, blocked} =
          RelationTracker.owe_barriers(
            dropping |> MapSet.union(newly_owed) |> MapSet.union(stale),
            stack_id: state.stack_id,
            dropped_oids: dropped_oids,
            cleanup_relations: stale
          )

        state = keep_drain_waits(state, owed, filters)
        to_add = without_blocked(to_add, blocked)
        to_preserve = without_blocked(to_preserve, blocked)
        to_drop = without_blocked(to_drop, blocked)
        to_configure_replica_identity = without_blocked(to_configure_replica_identity, blocked)
        to_configure_only = MapSet.difference(to_configure_replica_identity, to_add)

        {held, state} =
          if can_update_publication?(state),
            do:
              hold_tables_with_removals_pending(
                to_preserve |> MapSet.union(to_add) |> MapSet.union(to_configure_only),
                state
              ),
            else: hold_tables_with_removals_pending(to_preserve, state)

        {to_drain, ready} =
          to_preserve
          |> MapSet.difference(held)
          |> Enum.split_with(fn {oid, _rel} -> MapSet.member?(owed, oid) end)

        notify_filters_result(ready, {:ok, :configured}, state)
        notify_filters_result(to_invalidate, {:error, :schema_changed}, state)
        drains = for filter <- to_drain, do: {filter, Map.fetch!(memberships, filter), status}
        state = update_drains(drains, state)

        if can_update_publication?(state) do
          to_add = MapSet.difference(to_add, held)
          to_configure_only = MapSet.difference(to_configure_only, held)

          to_configure_replica_identity =
            MapSet.difference(to_configure_replica_identity, held)

          to_add_and_configure = MapSet.intersection(to_add, to_configure_replica_identity)
          to_add_only = MapSet.difference(to_add, to_configure_replica_identity)

          # Notes on avoiding deadlocks
          # - `ALTER TABLE` should be after the publication altering, because it takes out an exclusive lock over this table,
          #   but the publication altering takes out a shared lock on all mentioned tables, so a concurrent transaction will
          #   deadlock if the order is reversed, and we've seen this happen even within the context of a single process perhaps
          #   across multiple calls from separate deployments or timing issues.
          # - It is important for all table operations to also occur in the same order to avoid deadlocks due to
          #   lock ordering issues, so despite splitting drop and add operations we sort them and process them together
          #   in a sorted single pass
          relation_actions =
            [
              Enum.map(to_add_only, &{:add, &1, nil}),
              Enum.map(to_add_and_configure, &{:add_and_configure, &1, nil}),
              Enum.map(to_configure_only, &{:configure, &1, Map.fetch!(memberships, &1)}),
              Enum.map(to_drop, &{:drop, &1, nil})
            ]
            |> Stream.concat()
            |> Enum.sort(&(elem(&1, 1) <= elem(&2, 1)))

          if relation_actions != [] do
            to_add_list = for {_, rel} <- to_add, do: Utils.relation_to_sql(rel)
            to_drop_list = for {_, rel} <- to_drop, do: Utils.relation_to_sql(rel)
            to_invalidate_list = for {_, rel} <- to_invalidate, do: Utils.relation_to_sql(rel)

            to_configure_list =
              for {_, rel} <- to_configure_replica_identity, do: Utils.relation_to_sql(rel)

            Logger.notice(
              "Configuring publication #{state.publication_name} to " <>
                "drop #{inspect(to_drop_list)} tables, " <>
                "add #{inspect(to_add_list)} tables, " <>
                "and configure replica identity for #{inspect(to_configure_list)} tables " <>
                "- skipping altered tables #{inspect(to_invalidate_list)}",
              publication_alter_drop_tables: to_drop_list,
              publication_alter_add_tables: to_add_list,
              publication_alter_configure_replica_identity: to_configure_list,
              publication_alter_invalid_tables: to_invalidate_list
            )
          end

          {:noreply, state,
           {:continue, {:perform_relation_actions, relation_actions, status, false}}}
        else
          notify_filters_result(to_add, {:error, :relation_missing_from_publication}, state)

          notify_filters_result(
            to_configure_only,
            {:error, :misconfigured_replica_identity},
            state
          )

          if MapSet.size(to_add) == 0 and MapSet.size(to_configure_replica_identity) == 0 do
            Logger.notice(fn ->
              tables = for {_, rel} <- to_preserve, do: Utils.relation_to_sql(rel)

              "Verified publication #{state.publication_name} to include #{inspect(tables)} " <>
                "tables with REPLICA IDENTITY FULL"
            end)
          end

          {:noreply, schedule_recheck(false, state)}
        end

      # The shapes waiting wait for the check that reads the change, soon after. Each drain verifies
      # its table against a status read before the change, and its answer, sent already or still
      # to come, would admit a shape on a publication that may no longer stand: the drains go, the
      # answers in the mailbox with them, and the pass after the change starts again those still
      # owed, whose barriers the tracker keeps.
      :publication_changed ->
        {:noreply, recheck_soon(update_drains([], state))}

      {:error, err} ->
        handle_catalog_error(err, state)
    end
  end

  def handle_continue({:perform_relation_actions, [], _status, recheck?}, state) do
    Logger.debug("Processed all publication relation actions")
    {:noreply, schedule_recheck(recheck?, state)}
  end

  def handle_continue(
        {:perform_relation_actions, [{action, filter, membership} | rest], status, recheck?},
        state
      ) do
    {_oid, rel} = filter

    res =
      OpenTelemetry.with_span(
        "publication_manager.update_publication_relation",
        [action: inspect(action), relation: Utils.relation_to_sql(rel)],
        state.stack_id,
        fn -> do_publication_update(action, filter, {membership, status}, state) end
      )

    # An update that finds the publication changed under it knows what a pass whose snapshot finds
    # it changed knows: the drains go the same way, the answers queued with them, and so do the
    # updates after it, a drop among them, which verifies nothing and would alter a publication
    # that replaced the one the pass read. A check soon after reads the change and plans again. One
    # that finds only its table changed cancels the drains too, since the change may have moved
    # their tables after they verified them, a migration that drops tables and adds them back say,
    # and the updates after it go on.
    case publication_change(res, status, state) do
      :publication ->
        notify_filter_result(filter, {:error, :publication_changed}, state)
        {:noreply, schedule_recheck(true, update_drains([], state))}

      :table ->
        notify_filter_result(filter, res, state)

        {:noreply, update_drains([], state),
         {:continue, {:perform_relation_actions, rest, status, true}}}

      :none ->
        notify_filter_result(filter, res, state)
        {:noreply, state, {:continue, {:perform_relation_actions, rest, status, recheck?}}}
    end
  end

  # An update that finds its table other than the pass planned, or that Postgres refuses for
  # privilege or for a publication it does not find, has the publication read again. A migration
  # can hand the publication to another role after the pass read it as Electric's, in the
  # transaction that adds the table, or drop it, or drop it and create it again: the update is
  # refused, or finds the table in no publication the pass read. Unless the publication stands, the
  # pass ends. A table changed in a publication that stands ends no pass: the updates after it go
  # on.
  defp publication_change({:error, :publication_changed}, status, state),
    do: if(publication_stands?(status, state), do: :table, else: :publication)

  defp publication_change({:error, %Postgrex.Error{postgres: %{code: code}}}, status, state)
       when code in [:insufficient_privilege, :undefined_object],
       do: if(publication_stands?(status, state), do: :none, else: :publication)

  defp publication_change(_res, _status, _state), do: :none

  defp cancel_superseded_update(%{scheduled_filters: {revision, _}} = state)
       when revision <= state.checked_filter_revision do
    Process.cancel_timer(state.scheduled_update_ref)
    %{state | scheduled_filters: nil, scheduled_update_ref: nil}
  end

  defp cancel_superseded_update(state), do: state

  defp cancel_recheck(%{recheck_ref: nil} = state), do: state

  defp cancel_recheck(state) do
    Process.cancel_timer(state.recheck_ref)
    %{state | recheck_ref: nil}
  end

  defp schedule_recheck(false, state), do: %{state | recheck_delay: @recheck_initial_delay}

  defp schedule_recheck(true, state) do
    ref = :erlang.start_timer(state.recheck_delay, self(), :recheck_publication)
    %{state | recheck_ref: ref, recheck_delay: min(state.recheck_delay * 2, @recheck_max_delay)}
  end

  defp recheck_soon(%{recheck_ref: nil} = state), do: schedule_recheck(true, state)
  defp recheck_soon(state), do: state

  defp handle_catalog_error(err, state) do
    notify_global_configuration_error(err, state)
    transient? = transient_catalog_error?(err)
    # Scheduling advances the delay. Keep that retry pending even if this pass already
    # consumed the timer, whether it came from the timer itself or an intervening update.
    retry? = state.recheck_delay > @recheck_initial_delay and transient?
    # A check that found no connection says nothing of the tables drains wait on. On an admin pool
    # of one the drain holds the connection the check queued for, and cancelling it would make the
    # checks that keep coming undo every drain. A drain verifies its table under its lock, and the
    # next check that succeeds keeps or cancels it. The tracker's filters say which tables a shape
    # asks for: a drain of any other, running or queued, would hold a connection for no shape until
    # that next check. They are the tracker's as the check fails, not those the check was asked
    # for: a shape can ask for a table while the check runs, after its last shape went, and the
    # drain it needs then keeps the wait it has run up.
    state =
      if transient? do
        {_revision, requested} = RelationTracker.fetch_current_filters!(stack_id: state.stack_id)
        drop_unrequested_drains(state, requested)
      else
        update_drains([], state)
      end

    {:noreply, schedule_recheck(retry?, state)}
  end

  defp drop_unrequested_drains(state, filters) do
    requested? = fn {filter, _membership, _status} -> MapSet.member?(filters, filter) end
    running = for {_ref, {drain, _task}} <- state.drains, requested?.(drain), do: drain

    waits =
      Map.filter(state.drain_lock_timeouts, fn {filter, _} -> MapSet.member?(filters, filter) end)

    update_drains(
      running ++ Enum.filter(state.queued_drains, requested?),
      %{state | drain_lock_timeouts: waits}
    )
  end

  defp transient_catalog_error?(%DBConnection.ConnectionError{}), do: true

  defp transient_catalog_error?(%Electric.SnapshotError{type: :connection_not_available}),
    do: true

  defp transient_catalog_error?(%Postgrex.Error{postgres: %{pg_code: <<"08", _::binary>>}}),
    do: true

  defp transient_catalog_error?(%Postgrex.Error{postgres: %{code: code}}),
    do: code in [:query_canceled, :lock_not_available, :deadlock_detected, :admin_shutdown]

  defp transient_catalog_error?(_), do: false

  defp without_blocked(filters, blocked) do
    if MapSet.size(blocked) == 0,
      do: filters,
      else: MapSet.reject(filters, fn {oid, _} -> MapSet.member?(blocked, oid) end)
  end

  @spec check_publication_status(state(), non_neg_integer()) ::
          {:ok, Configuration.publication_status()} | {:error, any()}
  defp check_publication_status(state, rechecks \\ 1) do
    # A restart asked for names the connection manager whose admin pool answered: once that one
    # has stopped, its replacement sets the publication up again, and stopping the replacement
    # gains nothing.
    manager = Electric.Connection.Restarter.connection_manager(state.stack_id)

    Configuration.run_handling_db_connection_errors(fn ->
      case publication_status!(state, manager, rechecks) do
        :recheck ->
          check_publication_status(state, rechecks - 1)

        # No one manager ran across either check, so the answer may predate the publication the
        # manager now running sets up. The request fails as one whose connection went away,
        # which a shape's snapshot retries, and a later check that one manager spans decides.
        {:unsettled, status} ->
          Logger.warning(
            "Publication #{inspect(state.publication_name)} checked twice while the connection " <>
              "manager changed, found #{inspect(status)}; the next request checks it again"
          )

          {:error, Electric.SnapshotError.connection_not_available()}

        :not_found ->
          err = Electric.DbConfigurationError.publication_missing(state.publication_name)
          handle_publication_fatally_misconfigured(err, manager, state)
          {:error, err}

        %{publishes_all_operations?: false} ->
          err =
            Electric.DbConfigurationError.publication_missing_operations(state.publication_name)

          handle_publication_fatally_misconfigured(err, manager, state)
          {:error, err}

        status ->
          {:ok, status}
      end
    end)
  end

  # A publication found in place stands, whichever admin pool answered. A manager keeps its name
  # until its admin pool has stopped, and the next one's pool starts after it, so a check that
  # begins and ends with the same running manager was answered by its pool. One that begins or
  # ends with none, or with another, may have reached a pool whose manager came and went, before
  # the manager after it set the publication up as it starts: the check runs again, once.
  defp publication_status!(state, manager, rechecks) do
    status = Configuration.check_publication_status!(state.db_pool, state.publication_name)

    cond do
      match?(%{publishes_all_operations?: true}, status) ->
        status

      is_pid(manager) and
          Electric.Connection.Restarter.connection_manager(state.stack_id) == manager ->
        status

      rechecks > 0 ->
        :recheck

      true ->
        {:unsettled, status}
    end
  end

  @spec determine_publication_relation_actions(
          state(),
          RelationTracker.relation_filters(),
          Configuration.publication_status()
        ) ::
          {:ok, Configuration.publication_status(), RelationTracker.served_relations(), boolean(),
           MapSet.t(Electric.relation_id()), MapSet.t(Electric.relation_id()),
           Configuration.relation_actions()}
          | :publication_changed
          | {:error, any()}
  defp determine_publication_relation_actions(state, filters, checked_status) do
    {owed, served, recheck_unserved?, cleanup} =
      RelationTracker.barrier_state(stack_id: state.stack_id)

    # The publication is read with its tables, in one snapshot: the relations changed since the
    # filters were read, the memberships, the replica identities, the publication's status and the
    # relations still in place. A status other than the check's is kept outside the transaction
    # too: a commit that finds the connection lost raises in place of the read's result, and its
    # error would keep the drains the change cancels.
    seen = make_ref()

    read =
      Configuration.read_in_one_snapshot(state.db_pool, 5, fn conn ->
        res =
          Configuration.determine_publication_relation_actions!(
            conn,
            state.publication_name,
            filters
          )

        read_status = Configuration.read_publication_status!(conn, state.publication_name)

        # A status other than the check's ends the read: the pass acts on none of it, and a query
        # after it that failed would hide the change behind an error that keeps the drains.
        if same_publication?(checked_status, read_status) do
          missing =
            owed
            |> MapSet.union(MapSet.new(served, fn {{oid, _}, _} -> oid end))
            |> MapSet.difference(MapSet.new(res.memberships, fn {{oid, _}, _} -> oid end))

          existing = Configuration.existing_relation_oids!(conn, Enum.to_list(missing))

          {:ok, read_status, MapSet.difference(missing, existing), res}
        else
          send(self(), {seen, read_status})
          {:ok, read_status, MapSet.new(), res}
        end
      end)

    receive do
      {^seen, read_status} ->
        settled_status(checked_status, read_status, state)
    after
      0 ->
        with {:ok, read_status, dropped_oids, res} <- read,
             {:ok, status} <- settled_status(checked_status, read_status, state) do
          {:ok, status, served, recheck_unserved?, cleanup, dropped_oids, res}
        end
    end
  end

  # The check read the publication before the snapshot the pass acts on, and the tracker took what
  # it publishes then. A migration in between can have replaced the publication or changed what it
  # publishes, deletes or generated columns say, with its tables in place: the pass acts only on a
  # publication its snapshot found as the check did. Otherwise it acts on nothing and answers no
  # one, and a check soon after reads the publication again and answers the shapes waiting.
  # It alters the publication only if both reads found it Electric's. Read only at the check, an
  # ownership that a migration handed to another role in the transaction that added a table made
  # that table one Electric had added itself, which the pass served without the drain owed to the
  # writers in flight across its addition. An ownership only one of the two reads found is taken
  # for another role's: the pass drains the tables it serves and alters nothing, whichever read was
  # right.
  defp settled_status(checked, read, state) do
    if same_publication?(checked, read) do
      {:ok,
       %{
         checked
         | can_alter_publication?: checked.can_alter_publication? and read.can_alter_publication?
       }}
    else
      Logger.notice(
        "Publication #{inspect(state.publication_name)} changed between its check and the read " <>
          "of its tables, from #{inspect(checked)} to #{inspect(read)}; checking it again"
      )

      :publication_changed
    end
  end

  defp same_publication?(checked, read) do
    is_map(read) and
      Map.delete(read, :can_alter_publication?) == Map.delete(checked, :can_alter_publication?)
  end

  @spec do_publication_update(
          :add_and_configure | :add | :configure | :drop,
          Electric.oid_relation(),
          {Electric.relation_id() | nil, Configuration.publication_status()},
          state()
        ) :: {:ok, :dropped | {:drained, Electric.relation_id()}} | {:error, any()}
  defp do_publication_update(action, filter, expected, state)

  defp do_publication_update(action, filter, expected, state)
       when action in [:add, :add_and_configure, :configure] do
    publication = state.publication_name

    update_table =
      case action do
        :add ->
          &Configuration.add_table_to_publication(&1, publication, filter)

        :add_and_configure ->
          &Configuration.configure_table_for_replication(&1, publication, filter)

        :configure ->
          &Configuration.set_table_replica_identity_full(&1, filter)
      end

    case Configuration.update_and_verify_table_membership(
           state.db_pool,
           publication,
           filter,
           expected,
           update_table
         ) do
      {:ok, membership} ->
        {:ok, {:drained, membership}}

      {:error, %Postgrex.Error{postgres: %{code: code}} = reason}
      when code in [:query_canceled, :lock_not_available, :deadlock_detected] ->
        hold_publication_update(filter, reason)

      {:error, %Electric.DbConfigurationError{type: :table_lock_timeout} = reason} ->
        hold_publication_update(filter, reason)

      {:error, _} = error ->
        error
    end
  end

  defp do_publication_update(:drop, filter, _expected, state) do
    with :ok <-
           Configuration.drop_table_from_publication(
             state.db_pool,
             state.publication_name,
             filter
           ) do
      {:ok, :dropped}
    end
  end

  # The publication stands when it is the one the pass's check found, publishing what it did, and
  # Electric owns it if the pass found it Electric's. A read that fails says nothing, and the
  # publication is taken for changed.
  defp publication_stands?(status, state) do
    read =
      Configuration.run_handling_db_connection_errors(fn ->
        Configuration.read_publication_status!(state.db_pool, state.publication_name)
      end)

    same_publication?(status, read) and
      (read.can_alter_publication? or not status.can_alter_publication?)
  end

  defp hold_publication_update({_oid, relation}, reason) do
    Logger.warning(
      "Holding publication admission for #{Utils.relation_to_sql(relation)}: #{inspect(reason)}"
    )

    {:error, :publication_update_pending}
  end

  defp handle_publication_fatally_misconfigured(err, _, %{manual_table_publishing?: true}),
    do:
      Logger.warning("""
      Publication fatal misconfiguration: #{inspect(err)}
      Recovery attempt skipped due to manual table publishing mode.
      Please ensure that the publication is created and configured correctly.
      """)

  defp handle_publication_fatally_misconfigured(err, manager, state) do
    Logger.warning("""
    Publication fatal misconfiguration: #{inspect(err)}
    Attempting recovery through a restart and fresh setup of connection subsystem.
    """)

    Electric.Connection.Restarter.restart_connection_subsystem(state.stack_id, manager)
    :ok
  end

  defp can_update_publication?(%__MODULE__{
         can_alter_publication?: can_alter,
         manual_table_publishing?: manual
       }) do
    can_alter and not manual
  end

  # What was written to a table while it was out of the publication never reaches the stream. The
  # shapes on it from before are removed, and their removal is on disk, before the table goes back
  # in: once the addition commits, a restart finds the table in place and nothing left to say what
  # those shapes lack. The removal runs in a cleanup task, and has three seconds from its start to
  # reach the disk. A table whose removals have not is held for this pass, and the shapes waiting
  # for it retry, which checks again. The other tables go in. A table another session has put back
  # meanwhile is held the same way, as one whose replica identity is repaired: it is served once
  # the removal of the shapes on it from before has reached the disk. The tracker persists the
  # removals not confirmed yet, so a restart of both processes looks at each again, and starts it
  # again if it never reached its shape.
  # A replacement membership can hide a gap between checks, so it needs the same cleanup.
  defp hold_tables_with_removals_pending(admitting, state) do
    owed = RelationTracker.removals_owed(stack_id: state.stack_id)

    {known, new} =
      Map.split_with(owed, fn {handle, _} -> is_map_key(state.removals_to_confirm, handle) end)

    known =
      Map.new(known, fn {handle, relations} ->
        {handle, %{Map.fetch!(state.removals_to_confirm, handle) | relations: relations}}
      end)

    pending = start_removal(new, known, state)
    admitted = MapSet.new(admitting, fn {oid, _rel} -> oid end)

    {gating, unrelated} =
      Map.split_with(pending, fn {_handle, %{relations: relations}} ->
        not MapSet.disjoint?(relations, admitted)
      end)

    unconfirmed = await_removals_on_disk(gating, removal_deadline(gating), state, unrelated)
    held = held_relations(admitting, unconfirmed)

    # A removal no table waits on is looked at once a pass, so that its record goes once it is on
    # disk, and a restart does not start it again.
    unrelated =
      confirm_removals(
        unrelated,
        System.monotonic_time(:millisecond) + @removal_recheck_interval,
        state,
        unconfirmed
      )

    if MapSet.size(held) > 0 do
      Logger.warning(
        "Holding #{Enum.map_join(held, ", ", &Utils.relation_to_sql(elem(&1, 1)))} " <>
          "of publication #{state.publication_name}: the removal of " <>
          "#{map_size(unconfirmed)} shapes on them from before is not on disk"
      )

      # A held admission, which a fresh shape's consumer waits out in place: a consumer that
      # suspended before its first snapshot would leave its handle with no process.
      notify_filters_result(
        held,
        {:error, Electric.SnapshotError.publication_admission_pending()},
        state
      )
    end

    state = %{state | removals_to_confirm: Map.merge(unrelated, unconfirmed)}
    {held, watch_catalog_read_slot(state)}
  end

  # A read a configurator before this one left running, or one started for a removal no longer
  # owed, holds the catalog read slot with no reply or exit coming here, and a removal waiting for
  # the slot would be looked at again only when a pass next came. Its end wakes this one. The read
  # watched is the one the removal found in the slot, not one looked up again: a read that ends in
  # between leaves the slot empty, and the monitor of its pid reports that end all the same.
  defp watch_catalog_read_slot(%{slot_watch: nil, removals_to_confirm: pending} = state)
       when map_size(pending) > 0 do
    with false <- Enum.any?(Map.values(pending), &read_running?/1),
         read when is_pid(read) <-
           Enum.find_value(pending, fn {_handle, removal} -> queued_behind(removal) end) do
      %{state | slot_watch: Process.monitor(read, tag: :catalog_read_slot_freed)}
    else
      _ -> state
    end
  end

  defp watch_catalog_read_slot(state), do: state

  # One task for the shapes on each table, so that a table whose shapes are slow to stop holds no
  # other back.
  defp start_removal(removals, pending, state) do
    removals
    |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
    |> Enum.reduce(pending, fn {relations, handles}, pending ->
      {:ok, task} =
        ShapeCleaner.start_removing_shapes(
          state.stack_id,
          handles,
          ShapeCleaner.consumer_cleanup_reason()
        )

      deadline = System.monotonic_time(:millisecond) + @removal_timeout

      Enum.into(handles, pending, fn handle ->
        {handle, %{relations: relations, task: task, deadline: deadline, read: nil}}
      end)
    end)
  end

  # A removal that ran out of time in an earlier pass is looked at once more.
  defp removal_deadline(pending) do
    floor = System.monotonic_time(:millisecond) + @removal_recheck_interval
    pending |> Map.values() |> Enum.reduce(floor, &max(&1.deadline, &2))
  end

  defp held_relations(admitting, unconfirmed) do
    Enum.reduce(unconfirmed, MapSet.new(), fn {_handle, %{relations: relations}}, held ->
      admitting
      |> MapSet.filter(fn {oid, _rel} -> MapSet.member?(relations, oid) end)
      |> MapSet.union(held)
    end)
  end

  defp await_removals_on_disk(pending, deadline, state, others) do
    pending = confirm_removals(pending, deadline, state, others)

    if map_size(pending) > 0 and
         System.monotonic_time(:millisecond) + @removal_recheck_interval < deadline do
      Process.sleep(@removal_recheck_interval)
      await_removals_on_disk(pending, deadline, state, others)
    else
      pending
    end
  end

  # A removal still running is waited for: it stops the shape's consumer before the table goes
  # in. Once it has ended, the catalog is read, after a flush if the buffer still holds a write of
  # one of the shapes: a flush on every addition also wrote out the catalog entries of shapes still
  # taking their snapshot, and took the time of the read. The read has time of its own, so that a
  # flush that took all of the pass's leaves it some. A read left running past a pass's deadline
  # keeps its catalog connection, so no read starts while one is: a store whose reads stall would
  # otherwise take one more connection each pass, and the reads every other lookup needs with them.
  # Its answer, once it comes, stands in for a read in the next pass. `others` are the removals of
  # this pass not in `pending`, whose reads count too.
  defp confirm_removals(pending, deadline, state, others) do
    {running, ended} =
      Map.split_with(pending, fn {_handle, %{task: {pid, _completed}}} ->
        Process.alive?(pid)
      end)

    if map_size(ended) == 0 do
      pending
    else
      buffered = buffered_after_flush(state.stack_id, Map.keys(ended), deadline)
      deadline = max(deadline, System.monotonic_time(:millisecond) + @removal_recheck_interval)

      own_read? = Enum.any?(Map.values(pending) ++ Map.values(others), &read_running?/1)
      slot_holder = if not own_read?, do: catalog_reader(state.stack_id)
      reading? = own_read? or is_pid(slot_holder)

      {restart, unconfirmed, confirmed, _reading?} =
        Enum.reduce(ended, {[], running, [], reading?}, fn
          {handle, %{task: {_, completed}} = removal}, {restart, unconfirmed, done, reading?} ->
            result =
              cond do
                :atomics.get(completed, 1) != 1 ->
                  :not_started

                match?({:answered, _exists?}, removal.read) ->
                  removal_on_disk(state.stack_id, handle, buffered, removal.read)

                read_running?(removal) ->
                  {:queued, removal.read}

                is_pid(slot_holder) ->
                  {:behind, slot_holder}

                reading? ->
                  :queued

                true ->
                  removal_on_disk(state.stack_id, handle, buffered, {:deadline, deadline})
              end

            case result do
              :on_disk ->
                {restart, unconfirmed, [handle | done], reading?}

              :queued ->
                {restart, Map.put(unconfirmed, handle, %{removal | read: nil}), done, reading?}

              {:queued, read} ->
                {restart, Map.put(unconfirmed, handle, %{removal | read: read}), done, true}

              {:behind, _slot_holder} = behind ->
                {restart, Map.put(unconfirmed, handle, %{removal | read: behind}), done, true}

              :not_started ->
                {[{handle, removal.relations} | restart], unconfirmed, done, reading?}
            end
        end)

      :ok = RelationTracker.removals_confirmed(confirmed, stack_id: state.stack_id)
      start_removal(restart, unconfirmed, state)
    end
  end

  defp buffered_after_flush(stack_id, handles, deadline) do
    buffered = WriteBuffer.pending_shape_handles(stack_id)

    if buffered == :unavailable or Enum.any?(handles, &MapSet.member?(buffered, &1)) do
      flush_shape_catalog(stack_id, deadline)
      WriteBuffer.pending_shape_handles(stack_id)
    else
      buffered
    end
  end

  defp flush_shape_catalog(stack_id, deadline) do
    WriteBuffer.flush_sync(stack_id, max(deadline - System.monotonic_time(:millisecond), 1))
  catch
    :exit, _ -> :error
  end

  # The buffer drops operations only after committing or discarding them. Its entries are
  # checked before SQLite so a pending insert cannot disappear between the two reads.
  # Recover a removal interrupted between setting its mark and queuing the operation.
  defp removal_on_disk(stack_id, handle, buffered, read) do
    cond do
      ShapeStatus.has_shape_handle?(stack_id, handle) ->
        :not_started

      WriteBuffer.queue_marked_removal(stack_id, handle) or buffered == :unavailable or
          MapSet.member?(buffered, handle) ->
        :queued

      true ->
        case catalog_answer(stack_id, handle, read) do
          true ->
            WriteBuffer.remove_shape(stack_id, handle)
            :queued

          false ->
            :on_disk

          {:running, read} ->
            {:queued, read}

          {:behind, _slot_holder} = behind ->
            behind

          :out_of_time ->
            :queued
        end
    end
  rescue
    _ -> :queued
  catch
    :exit, _ -> :queued
  end

  # A read counts as running until its reply or its exit is taken in, not until its process ends:
  # one that answered during a pass has its reply waiting, which a read started in its place would
  # leave unmatched.
  defp read_running?(%{read: {_read, _reply, _monitor}}), do: true
  defp read_running?(_removal), do: false

  # The read that held the catalog read slot when the removal was last looked at, if not one of this
  # configurator's.
  defp queued_behind(%{read: {:behind, read}}), do: read
  defp queued_behind(_removal), do: nil

  # A read holds the stack's catalog read slot, a name in the stack's registry, while it runs. The
  # slot outlives the configurator and the removals it tracks: a read left running for a removal no
  # longer owed, or by a configurator that has since restarted, still holds it, and no other read
  # starts until it ends.
  defp catalog_reader(stack_id) do
    GenServer.whereis(Electric.ProcessRegistry.name(stack_id, __MODULE__.CatalogRead))
  rescue
    # A stack whose registry is not up has no read running.
    ArgumentError -> nil
  end

  defp read_handle_exists_in_slot(stack_id, handle, timeout) do
    registry = Electric.ProcessRegistry.registry_name(stack_id)

    case Registry.register(registry, {__MODULE__.CatalogRead, nil}, nil) do
      {:ok, _owner} -> read_handle_exists(stack_id, handle, timeout)
      {:error, {:already_registered, read}} -> {:error, {:catalog_read_running, read}}
    end
  end

  # The answer of a read an earlier pass stopped waiting for stands while the checks before it find
  # the shape still gone and none of its writes in the buffer: its entry goes back in the catalog
  # only with an addition of the shape, which those checks see.
  defp catalog_answer(_stack_id, _handle, {:answered, exists?}), do: exists?

  defp catalog_answer(stack_id, handle, {:deadline, deadline}) do
    case deadline - System.monotonic_time(:millisecond) do
      timeout when timeout > 0 -> catalog_has_handle?(stack_id, handle, timeout)
      _out_of_time -> :out_of_time
    end
  end

  # The checkout's timeout bounds the wait for a connection, not the read, so the read runs in a
  # process the pass stops waiting for at the deadline. It is left to finish rather than killed: a
  # connection killed in a read is reopened, which in exclusive mode reruns the boot integrity
  # check on the only connection. It is monitored, not linked, so no exit of its own, a kill from
  # outside included, reaches the configurator, and a reply after the deadline is kept for the
  # next pass.
  defp catalog_has_handle?(stack_id, handle, timeout) do
    parent = self()
    reply = make_ref()
    callers = [parent | Process.get(:"$callers", [])]
    ancestors = [parent | Process.get(:"$ancestors", [])]

    {read, monitor} =
      spawn_monitor(fn ->
        Process.put(:"$callers", callers)
        Process.put(:"$ancestors", ancestors)
        activate_task_mocks()
        send(parent, {reply, read_handle_exists_in_slot(stack_id, handle, timeout)})
      end)

    receive do
      {^reply, {:ok, exists?}} ->
        Process.demonitor(monitor, [:flush])
        exists?

      # A read whose answer was taken in this pass can still hold the slot, for as long as it takes
      # to exit, and no reply or exit of it is still to come here.
      {^reply, {:error, {:catalog_read_running, slot_holder}}} ->
        Process.demonitor(monitor, [:flush])
        {:behind, slot_holder}

      {^reply, {:error, reason}} ->
        Process.demonitor(monitor, [:flush])
        exit(reason)

      {:DOWN, ^monitor, :process, ^read, reason} ->
        exit(reason)
    after
      timeout ->
        {:running, {read, reply, monitor}}
    end
  end

  defp read_handle_exists(stack_id, handle, timeout) do
    {:ok,
     ShapeDb.Connection.checkout!(
       stack_id,
       :handle_exists?,
       &ShapeDb.Query.handle_exists?(&1, handle),
       timeout
     )}
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, reason}
  end

  # Carry a pending lock across checks of the same membership. If a check finds the table absent,
  # changed, or in need of replica identity repair, the earlier lock can no longer admit it.
  # A drain still requested keeps its place in the queue, and new or retried drains go behind it,
  # so tables whose drains keep timing out cannot keep taking the slots ahead of one that waits.
  defp update_drains(requested, state) do
    requested_set = MapSet.new(requested)

    {drains, obsolete} =
      Map.split_with(state.drains, fn {_ref, {drain, _task}} ->
        MapSet.member?(requested_set, drain)
      end)

    cancel_drains(obsolete)
    active = MapSet.new(drains, fn {_ref, {drain, _task}} -> drain end)
    kept = Enum.filter(state.queued_drains, &MapSet.member?(requested_set, &1))
    placed = MapSet.union(active, MapSet.new(kept))
    added = requested |> Enum.uniq() |> Enum.reject(&MapSet.member?(placed, &1))
    start_drains(%{state | drains: drains, queued_drains: kept ++ added})
  end

  defp start_drains(%{queued_drains: []} = state), do: state

  defp start_drains(%{drains: drains} = state)
       when map_size(drains) >= state.max_concurrent_drains,
       do: state

  # A pending SHARE lock on one table leaves the configurator free to check and admit others.
  defp start_drains(%{queued_drains: [drain | rest]} = state) do
    lock_timeout = drain_wait(state, drain)

    task =
      Task.async(fn ->
        Logger.metadata(stack_id: state.stack_id)
        activate_task_mocks()

        drain_writers_before_serving(
          drain,
          state.db_pool,
          state.publication_name,
          lock_timeout
        )
      end)

    start_drains(%{
      state
      | drains: Map.put(state.drains, task.ref, {drain, task}),
        queued_drains: rest
    })
  end

  defp cancel_drains(drains) do
    Enum.each(drains, fn {_ref, {_drain, task}} -> Task.shutdown(task, :brutal_kill) end)
  end

  if Mix.env() == :test do
    defp activate_task_mocks,
      do: Support.TestUtils.activate_mocked_functions_for_module(__MODULE__)
  else
    defp activate_task_mocks, do: :ok
  end

  defp drain_writers_before_serving(
         {{_oid, rel} = oid_rel, membership, status},
         db_pool,
         publication,
         lock_timeout
       ) do
    table = Utils.relation_to_sql(rel)
    expected = {membership, status}

    result =
      case wait_for_writers(db_pool, publication, oid_rel, expected, lock_timeout) do
        # The minimal manual-publishing role (SELECT + REPLICATION) cannot take the SHARE lock, so the
        # role that adds the table must drain writers as it does (see the manual publishing guide).
        # Electric cannot here, so serve the table and let that role own the ordering. Only the
        # `LOCK`'s own denial says so: a privilege error before it holds the table as any error does.
        {:error, {:lock_denied, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}}} ->
          Logger.notice(
            "Cannot drain in-flight writers for #{table} without table privileges; " <>
              "the role that adds it to the publication must drain them as it does."
          )

          Configuration.run_handling_db_connection_errors(fn ->
            Configuration.published_table_membership!(db_pool, publication, oid_rel, expected)
          end)

        result ->
          result
      end

    case result do
      {:ok, _membership} = drained ->
        drained

      {:error, :publication_changed} ->
        :publication_changed

      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} ->
        :publication_changed

      # A writer still open past the timeout, or a lost connection: a snapshot taken now could miss
      # what that writer wrote before the addition. The shapes on the table are kept.
      {:error, reason} ->
        Logger.warning(
          "Holding #{table} until the writers in flight on it are drained: #{inspect(reason)}"
        )

        held = {:error, Electric.SnapshotError.publication_admission_pending()}

        case reason do
          %Electric.DbConfigurationError{type: :table_lock_timeout} -> {:lock_wait_ran_out, held}
          _other -> held
        end
    end
  end

  defp wait_for_writers(db_pool, publication, oid_rel, expected, nil) do
    Configuration.wait_for_in_flight_writers_on_published_table(
      db_pool,
      publication,
      oid_rel,
      expected
    )
  end

  defp wait_for_writers(db_pool, publication, oid_rel, expected, lock_timeout) do
    Configuration.wait_for_in_flight_writers_on_published_table(
      db_pool,
      publication,
      oid_rel,
      expected,
      lock_timeout
    )
  end

  defp notify_filters_result(filters, reply, state) do
    for filter <- filters, do: notify_filter_result(filter, reply, state)
  end

  defp notify_filter_result(filter, {:ok, _} = reply, state) do
    RelationTracker.notify_relation_configuration_result(filter, reply, stack_id: state.stack_id)
  end

  defp notify_filter_result(filter, {:error, err}, state) do
    reply = {:error, publication_error(err, filter, state)}
    RelationTracker.notify_relation_configuration_result(filter, reply, stack_id: state.stack_id)
  end

  # A transient error says nothing of the shapes waiting, a canceled catalog query as much as a
  # lost connection: they wait for a check that succeeds, as those waiting on a retained drain do.
  defp notify_global_configuration_error(%Electric.SnapshotError{} = err, state) do
    RelationTracker.notify_configuration_error({:error, err}, stack_id: state.stack_id)
  end

  defp notify_global_configuration_error(err, state) do
    if transient_catalog_error?(err),
      do:
        notify_global_configuration_error(
          Electric.SnapshotError.publication_admission_pending(),
          state
        ),
      else: RelationTracker.notify_configuration_error({:error, err}, stack_id: state.stack_id)
  end

  defp publication_error(:relation_missing_from_publication, oid_rel, state) do
    tail =
      cond do
        state.manual_table_publishing? ->
          "the ELECTRIC_MANUAL_TABLE_PUBLISHING setting prevents Electric from adding it"

        not state.can_alter_publication? ->
          "Electric lacks privileges to add it"
      end

    {_oid, rel} = oid_rel
    table = rel |> Utils.relation_to_sql() |> Utils.quote_name()

    %Electric.DbConfigurationError{
      type: :relation_missing_from_publication,
      message:
        "Database table #{table} is missing from " <>
          "the publication #{Utils.quote_name(state.publication_name)} and " <>
          tail
    }
  end

  defp publication_error(:misconfigured_replica_identity, oid_rel, _state) do
    {_oid, rel} = oid_rel
    table = rel |> Utils.relation_to_sql() |> Utils.quote_name()

    %Electric.DbConfigurationError{
      type: :misconfigured_replica_identity,
      message: "Database table #{table} does not have its replica identity set to FULL"
    }
  end

  defp publication_error(:schema_changed, oid_rel, _state) do
    {_oid, rel} = oid_rel

    table = rel |> Utils.relation_to_sql() |> Utils.quote_name()

    %Electric.DbConfigurationError{
      type: :schema_changed,
      message: "Database table #{table} has been dropped or renamed"
    }
  end

  defp publication_error(reason, _oid_rel, _state), do: reason
end
