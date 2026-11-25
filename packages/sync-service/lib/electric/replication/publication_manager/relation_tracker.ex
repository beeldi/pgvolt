defmodule Electric.Replication.PublicationManager.RelationTracker do
  @moduledoc """
  Provides interface for shapes to register and deregister themselves
  from a publication, and tracks the overall set of relations that need
  to be published using reference counting.

  Relies on Electric.Replication.PublicationManager.Configurator
  to perform the actual publication updates and handles status updates
  to reply to shapes requesting to be registered.
  """
  use GenServer

  alias Electric.BootStep
  alias Electric.PersistentKV
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes.Shape
  alias Electric.Telemetry.OpenTelemetry

  require Logger

  @typep shape_handle() :: Electric.shape_handle()
  @typep stack_id() :: Electric.stack_id()

  @barrier_retry_initial 100
  @barrier_retry_max 5_000
  @barrier_read_retry_delays [10, 20]

  defstruct [
    :stack_id,
    :publication_name,
    :publication_refresh_period,
    :tracked_handles_table,
    :persistent_kv,
    :database_timeline,
    oid_to_relation: %{},
    relation_ref_counts: %{},
    prepared_relation_filters: MapSet.new(),
    submitted_relation_filters: MapSet.new(),
    committed_relation_filters: MapSet.new(),
    # The relations covered by the configuration request the Configurator is
    # currently working on, or nil when none is outstanding.
    in_flight_relation_filters: nil,
    waiters: %{},
    # start with optimistic assumption about what the
    # publication supports (altering and generated columns)
    # and rely on the first check to correct that
    publishes_generated_columns?: true,
    # Relations observed out of the publication, dropped by the configurator, or about to be
    # drained before admission. These and completed external drains survive restarts.
    barriers_owed: MapSet.new(),
    served_relations: %{},
    cleanup_owed: MapSet.new(),
    recheck_unserved?: false,
    unpersisted_barriers: MapSet.new(),
    barrier_write_retry: nil,
    # The shapes taken off a relation found out of the publication, each with its relations, until
    # the Configurator confirms their removal on disk. Persisted before cleanup starts.
    removals_owed: %{},
    # By relation, the shapes that have waited for the publication and not been admitted yet.
    awaiting_admission: %{}
  ]

  @type relation_filters() :: MapSet.t(Electric.oid_relation())
  @type filter_snapshot() :: {pos_integer(), relation_filters()}
  @type served_relations() :: %{Electric.oid_relation() => Electric.relation_id()}
  @typep internal_relation_filters() :: MapSet.t(Electric.relation_id())
  @type publication_filter() :: {Electric.oid_relation(), with_generated_cols :: boolean()}
  @typep waiter() :: {GenServer.from(), shape_handle()}
  @typep state() :: %__MODULE__{
           stack_id: Electric.stack_id(),
           relation_ref_counts: %{Electric.relation_id() => non_neg_integer()},
           oid_to_relation: %{Electric.relation_id() => Electric.relation()},
           tracked_handles_table: atom(),
           prepared_relation_filters: internal_relation_filters(),
           submitted_relation_filters: internal_relation_filters(),
           committed_relation_filters: internal_relation_filters(),
           in_flight_relation_filters: internal_relation_filters() | nil,
           waiters: %{Electric.relation_id() => [waiter(), ...]},
           publication_name: String.t(),
           publishes_generated_columns?: boolean(),
           publication_refresh_period: non_neg_integer(),
           persistent_kv: PersistentKV.t(),
           database_timeline: list() | nil,
           barriers_owed: internal_relation_filters(),
           cleanup_owed: internal_relation_filters(),
           recheck_unserved?: boolean(),
           unpersisted_barriers: internal_relation_filters(),
           barrier_write_retry: pos_integer() | nil,
           served_relations: %{
             Electric.relation_id() => {Electric.relation(), Electric.relation_id()}
           },
           removals_owed: %{shape_handle() => internal_relation_filters()},
           awaiting_admission: %{Electric.relation_id() => MapSet.t(shape_handle())}
         }

  def name(stack_ref) do
    Electric.ProcessRegistry.name(stack_ref, __MODULE__)
  end

  @doc """
  What the publication must hold for a shape: its root table, and whether it selects generated
  columns. `ShapeStatus` keeps one per shape, which is what a restore reads.
  """
  @spec publication_filter(Shape.t()) :: publication_filter()
  def publication_filter(%Shape{root_table: relation, root_table_id: oid, flags: flags}),
    do: {{oid, relation}, Map.get(flags, :selects_generated_columns, false)}

  @spec add_shape(stack_id(), shape_handle(), Electric.Shapes.Shape.t()) :: :ok
  def add_shape(stack_id, shape_handle, shape) do
    pub_filter = publication_filter(shape)

    case GenServer.call(name(stack_id), {:add_shape, shape_handle, pub_filter}) do
      :ok -> :ok
      {:error, err} -> raise err
    end
  catch
    :exit, {:timeout, {GenServer, :call, _}} ->
      GenServer.cast(name(stack_id), {:cancel_waiter, shape.root_table_id, shape_handle, self()})
      raise Electric.SnapshotError.publication_admission_timeout()
  end

  @spec remove_shape(stack_id(), shape_handle()) :: :ok
  def remove_shape(stack_id, shape_handle) do
    case GenServer.call(name(stack_id), {:remove_shape, shape_handle}) do
      :ok -> :ok
      {:error, err} -> raise err
    end
  end

  @spec wait_for_restore(stack_id(), Keyword.t()) :: :ok
  def wait_for_restore(stack_id, opts \\ []) do
    GenServer.call(name(stack_id), :wait_for_restore, Keyword.get(opts, :timeout, :infinity))
  end

  @spec notify_relation_configuration_result(
          Electric.oid_relation(),
          {:ok, term()} | {:error, any()},
          Keyword.t()
        ) :: :ok
  def notify_relation_configuration_result(oid_rel, result, opts) do
    server = Access.get(opts, :server, name(opts))
    GenServer.cast(server, {:relation_configuration_result, oid_rel, result})
  end

  @doc """
  Takes the relations out of the ready set ahead of their addition to the publication, and returns
  the shapes on them that may lack what was written to them while they were out, each with its
  relation. Each table owes a drain before it can be admitted again.
  """
  @spec take_relations_out_of_publication([Electric.oid_relation()], Keyword.t()) :: [
          {shape_handle(), Electric.relation_id()}
        ]
  def take_relations_out_of_publication(oid_rels, opts) do
    server = Access.get(opts, :server, name(opts))

    {_owed, _blocked, stale} =
      GenServer.call(server, {:owe_barriers, [], [], oid_rels}, :infinity)

    stale
  end

  @doc """
  Records obligations and takes the supplied relations out of the ready set. Returns all
  outstanding obligations and the OIDs held while their state is not yet durable.
  `:cleanup_relations` also records the handles that may have missed writes.
  """
  @spec owe_barriers(relation_filters(), Keyword.t()) ::
          {internal_relation_filters(), internal_relation_filters()}
  def owe_barriers(oid_rels, opts) do
    server = Access.get(opts, :server, name(opts))
    dropped_oids = Access.get(opts, :dropped_oids, MapSet.new())
    cleanup_relations = Access.get(opts, :cleanup_relations, [])

    {owed, blocked, _stale} =
      GenServer.call(
        server,
        {:owe_barriers, oid_rels, dropped_oids, cleanup_relations},
        :infinity
      )

    {owed, blocked}
  end

  @spec barriers_owed(Keyword.t()) :: MapSet.t(Electric.relation_id())
  def barriers_owed(opts), do: elem(barrier_state(opts), 0)

  @spec barrier_state(Keyword.t()) ::
          {internal_relation_filters(), served_relations(), boolean(),
           internal_relation_filters()}
  def barrier_state(opts) do
    server = Access.get(opts, :server, name(opts))
    GenServer.call(server, :barrier_state, :infinity)
  end

  @doc """
  The shapes `take_relations_out_of_publication/2` returned whose removal is not confirmed on disk
  yet, each with its relations.
  """
  @spec removals_owed(Keyword.t()) :: %{shape_handle() => MapSet.t(Electric.relation_id())}
  def removals_owed(opts) do
    server = Access.get(opts, :server, name(opts))
    GenServer.call(server, :removals_owed, :infinity)
  end

  @spec removals_confirmed([shape_handle()], Keyword.t()) :: :ok
  def removals_confirmed([], _opts), do: :ok

  def removals_confirmed(handles, opts) do
    server = Access.get(opts, :server, name(opts))
    GenServer.cast(server, {:removals_confirmed, handles})
  end

  @spec notify_configuration_error({:error, any()}, Keyword.t()) :: :ok
  def notify_configuration_error(result, opts) do
    server = Access.get(opts, :server, name(opts))
    GenServer.cast(server, {:configuration_error, result})
  end

  @spec notify_publication_status(
          Electric.Postgres.Configuration.publication_status(),
          Keyword.t()
        ) :: :ok
  def notify_publication_status(status, opts) do
    server = Access.get(opts, :server, name(opts))
    GenServer.cast(server, {:publication_status, status})
  end

  @spec fetch_current_filters!(Keyword.t()) :: filter_snapshot()
  def fetch_current_filters!(opts) do
    server = Access.get(opts, :server, name(opts))
    # give an infinite timeout because this call can come in when the RelationTracker
    # is still initialising
    GenServer.call(server, :fetch_current_filters, :infinity)
  end

  def start_link(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    GenServer.start_link(__MODULE__, opts, name: name(stack_id))
  end

  # --- Private API ---

  @impl true
  def init(opts) do
    opts = Map.new(opts)

    Process.set_label({:publication_manager_relation_tracker, opts.stack_id})
    Logger.metadata(stack_id: opts.stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: opts.stack_id)

    # Using an ETS table for trackin shape handles as it is an unbounded
    # set that could grow large enough for GC pauses to matter. It is set
    # to public as a task populates it on startup, and it is scoped privately
    # enough such that defensive programming is unnecessary.
    tracked_handles_table =
      :ets.new(
        :"relation_tracker_tracked_handles:#{opts.stack_id}",
        [:named_table, :public, :set]
      )

    state = %__MODULE__{
      stack_id: opts.stack_id,
      publication_name: opts.publication_name,
      publication_refresh_period: opts.refresh_period,
      tracked_handles_table: tracked_handles_table,
      persistent_kv: opts.persistent_kv,
      database_timeline: Map.get(opts, :database_timeline)
    }

    {:ok, schedule_refresh(state), {:continue, :restore_relations}}
  end

  @impl true
  def handle_continue(:restore_relations, state) do
    OpenTelemetry.with_span(
      "publication_manager.restore_relations",
      [],
      state.stack_id,
      fn ->
        # Those filters come from the routing state, which the collector's restore builds from
        # the one read of the stored shapes a boot makes.
        :ok =
          BootStep.measure("relation_tracker.await_routing_state", fn ->
            ShapeStatus.await_routing_state(state.stack_id)
          end)

        # Build initial state in an ephemeral Task process so that to avoid
        # retaining the rows it folds over in this process's heap.
        start = System.monotonic_time()

        # From the filters ShapeStatus keeps in memory, not from a read of the stored shapes: the
        # store has one connection, which a read holds until it has decoded every shape, and the
        # collector's restore reads them too, so a read here makes that restore wait for it.
        state =
          Task.async(fn ->
            ShapeStatus.reduce_publication_filters(
              state.stack_id,
              state,
              fn {shape_handle, publication_filter}, state ->
                add_shape_to_publication_filters(shape_handle, publication_filter, state)
              end
            )
          end)
          |> Task.await(:infinity)

        Logger.notice(
          "Restored publication filters in #{System.convert_time_unit(System.monotonic_time() - start, :native, :millisecond)}ms"
        )

        # Notify the Configurator of the restored filters. This is necessary
        # when the RelationTracker restarts while the Configurator is still
        # running, as the Configurator only fetches filters on its own startup.
        state = state |> load_barrier_state() |> update_publication_if_necessary()

        {:noreply, state}
      end
    )
  end

  @impl true
  def handle_call({:add_shape, shape_handle, publication_filter}, from, state) do
    {{oid, _relation} = oid_rel, with_gen_cols} = publication_filter

    # if the relation is already committed AND part of the last made
    # update submission, we can consider it ready
    relation_ready? =
      MapSet.member?(state.submitted_relation_filters, oid) and
        MapSet.member?(state.committed_relation_filters, oid)

    state = add_shape_to_publication_filters(shape_handle, publication_filter, state)
    state = update_publication_if_necessary(state)

    cond do
      # if the publication doesn't support generated columns, fail any shapes
      # that require them immediately
      with_gen_cols and not state.publishes_generated_columns? ->
        {
          :reply,
          {:error,
           Electric.DbConfigurationError.publication_missing_generated_columns(
             state.publication_name
           )},
          state
        }

      MapSet.member?(state.unpersisted_barriers, oid) ->
        state = add_waiter(from, shape_handle, publication_filter, state)
        {:noreply, hold_admission(oid_rel, state)}

      relation_ready? ->
        {:reply, :ok, admit(oid, [shape_handle], state)}

      # otherwise, add the caller to the waiters list and reply when the
      # publication is ready
      true ->
        state = add_waiter(from, shape_handle, publication_filter, state)
        {:noreply, state}
    end
  end

  def handle_call({:remove_shape, shape_handle}, _from, state) do
    state = remove_shape_from_publication_filters(shape_handle, state)
    state = update_publication_if_necessary(state)

    # never wait for removals - reply immediately and let publication manager
    # reconcile the publication, otherwise you run into issues where only the last
    # removal fails and all others succeed. No removal guarantees anything about
    # the state of the publication.
    {:reply, :ok, state}
  end

  def handle_call(:wait_for_restore, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:owe_barriers, oid_rels, dropped_oids, cleanup_relations}, _from, state) do
    {stale, relations} = shapes_predating_addition(cleanup_relations, state)
    cleanup_oids = MapSet.new(cleanup_relations, &elem(&1, 0))
    oids = MapSet.new(oid_rels, &elem(&1, 0))

    removals =
      Enum.reduce(stale, state.removals_owed, fn {handle, oid}, removals ->
        Map.update(removals, handle, MapSet.new([oid]), &MapSet.put(&1, oid))
      end)

    state = record_barriers(oids, MapSet.new(dropped_oids), cleanup_oids, removals, state)

    durable = Enum.reject(relations, &MapSet.member?(state.unpersisted_barriers, &1.oid))

    stale =
      Enum.reject(stale, fn {_handle, oid} -> MapSet.member?(state.unpersisted_barriers, oid) end)

    state = take_shapes_predating_addition(stale, durable, state)
    {:reply, {state.barriers_owed, state.unpersisted_barriers, stale}, state}
  end

  def handle_call(:barrier_state, _from, state) do
    served =
      Map.new(state.served_relations, fn {oid, {rel, membership}} -> {{oid, rel}, membership} end)

    owed = MapSet.union(state.barriers_owed, state.unpersisted_barriers)
    {:reply, {owed, served, state.recheck_unserved?, state.cleanup_owed}, state}
  end

  def handle_call(:removals_owed, _from, state) do
    durable =
      Map.reject(state.removals_owed, fn {_handle, oids} ->
        not MapSet.disjoint?(oids, state.unpersisted_barriers)
      end)

    {:reply, durable, state}
  end

  def handle_call(:fetch_current_filters, _from, state) do
    {:reply, filter_snapshot(state), state}
  end

  @impl true
  def handle_cast({:cancel_waiter, oid, handle, pid}, state) do
    waiters = Map.get(state.waiters, oid, [])

    waiters =
      case Enum.reject(waiters, fn {{caller, _tag}, shape_handle} ->
             caller == pid and shape_handle == handle
           end) do
        [] -> Map.delete(state.waiters, oid)
        remaining -> Map.put(state.waiters, oid, remaining)
      end

    {:noreply, %{state | waiters: waiters}}
  end

  def handle_cast({:removals_confirmed, handles}, state) do
    record = %{barrier_record(state) | removals_owed: Map.drop(state.removals_owed, handles)}
    {:noreply, persist_barrier_state(record, [], state)}
  end

  def handle_cast({:publication_status, status}, state) do
    # if the publication has switched from being able to publish generated columns
    # to not being able to publish them, we need to fail any shapes that depend on
    # that feature
    state =
      if state.publishes_generated_columns? and not status.publishes_generated_columns?,
        do: fail_generated_column_shapes(state),
        else: state

    {:noreply, %{state | publishes_generated_columns?: status.publishes_generated_columns?}}
  end

  def handle_cast({:relation_configuration_result, {oid, _rel}, {:ok, :dropped}}, state) do
    new_committed_filters = MapSet.delete(state.committed_relation_filters, oid)

    state =
      clear_in_flight_if_committed(%{state | committed_relation_filters: new_committed_filters})

    {:noreply, state}
  end

  def handle_cast({:relation_configuration_result, oid_rel, {:ok, :configured}}, state) do
    {:noreply, configure_relation(oid_rel, state.served_relations, state)}
  end

  def handle_cast(
        {:relation_configuration_result, {oid, rel} = oid_rel, {:ok, {:drained, membership}}},
        state
      ) do
    served = Map.put(state.served_relations, oid, {rel, membership})
    {:noreply, configure_relation(oid_rel, served, state)}
  end

  def handle_cast({:relation_configuration_result, {oid, _} = oid_rel, {:error, reason}}, state)
      when reason in [:publication_changed, :publication_update_pending] do
    # Revoke readiness until the next check, which still needs the saved membership to detect
    # whether the table left the publication.
    committed = MapSet.delete(state.committed_relation_filters, oid)
    {:noreply, hold_admission(oid_rel, %{state | committed_relation_filters: committed})}
  end

  # A table waiting for a barrier or for stale shape removals to reach disk. The shapes waiting
  # for it retry, and those still on it are kept.
  def handle_cast(
        {:relation_configuration_result, oid_rel,
         {:error, %Electric.SnapshotError{type: type} = error}},
        state
      )
      when type in [:connection_not_available, :publication_admission_pending] do
    state = reply_to_relation_waiters(oid_rel, {:error, error}, state)
    {:noreply, %{state | in_flight_relation_filters: nil}}
  end

  def handle_cast({:relation_configuration_result, oid_rel, {:error, error}}, state) do
    log_level = if is_known_publication_error(error), do: :warning, else: :error

    Logger.log(
      log_level,
      "Failed to configure publication for relation #{inspect(oid_rel)}: #{inspect(error)}",
      relation: inspect(oid_rel)
    )

    state = reply_to_relation_waiters(oid_rel, {:error, error}, state)

    if not is_struct(error, DBConnection.ConnectionError) do
      ShapeCleaner.remove_shapes_for_relations(
        state.stack_id,
        [oid_rel],
        {:error, Electric.SnapshotError.from_error(error)}
      )
    end

    {:noreply, %{state | in_flight_relation_filters: nil}}
  end

  def handle_cast({:configuration_error, {:error, error}}, state) do
    state = %{reply_to_all_waiters({:error, error}, state) | in_flight_relation_filters: nil}
    {:noreply, state}
  end

  defp configure_relation({oid, _} = oid_rel, served, state) do
    # An addition, a replica identity change, or the drain of a published table satisfies the
    # barrier. For the last case, the operator takes it if Electric lacks the table privileges.
    state =
      if MapSet.member?(state.unpersisted_barriers, oid),
        do: state,
        else:
          persist_barrier_state(
            %{
              barrier_record(state)
              | barriers_owed: MapSet.delete(state.barriers_owed, oid),
                served_relations: served,
                cleanup_owed: MapSet.delete(state.cleanup_owed, oid)
            },
            [oid],
            state
          )

    if MapSet.member?(state.unpersisted_barriers, oid) do
      state
    else
      admitted = for {_from, handle} <- Map.get(state.waiters, oid, []), do: handle
      state = reply_to_relation_waiters(oid_rel, :ok, state)
      new_committed_filters = MapSet.put(state.committed_relation_filters, oid)
      state = admit(oid, admitted, state)

      clear_in_flight_if_committed(%{state | committed_relation_filters: new_committed_filters})
    end
  end

  defp record_barriers(oids, dropped_oids, cleanup_oids, removals, state) do
    owed = MapSet.union(oids, cleanup_oids)
    affected = MapSet.union(owed, dropped_oids)
    cleanup = state.cleanup_owed |> MapSet.union(cleanup_oids) |> MapSet.difference(dropped_oids)
    forget_baseline = MapSet.union(dropped_oids, MapSet.difference(oids, cleanup))

    record = %{
      barriers_owed: state.barriers_owed |> MapSet.union(owed) |> MapSet.difference(dropped_oids),
      served_relations: Map.drop(state.served_relations, MapSet.to_list(forget_baseline)),
      cleanup_owed: cleanup,
      removals_owed: removals
    }

    state = %{
      state
      | committed_relation_filters: MapSet.difference(state.committed_relation_filters, affected)
    }

    persist_barrier_state(record, affected, state)
  end

  defp load_barrier_state(state) do
    case read_barrier_state(state, @barrier_read_retry_delays) do
      {:ok, record} ->
        load_barrier_record(record, state)

      {:error, :not_found} ->
        state

      {:error, reason} ->
        rebuild_barriers(reason, state)
    end
  end

  defp load_barrier_record([timeline, owed, served, recheck_unserved? | rest], state)
       when is_boolean(recheck_unserved?) do
    with true <- valid_barrier_entries?(owed, served),
         served = decode_served(served),
         owed = MapSet.new(owed),
         {:ok, cleanup, removals} <- decode_cleanup(rest, owed, served) do
      record = %{
        barriers_owed: owed,
        served_relations: served,
        cleanup_owed: cleanup,
        removals_owed: removals
      }

      restore_barriers(timeline, record, %{state | recheck_unserved?: recheck_unserved?})
    else
      _ -> rebuild_barriers(:malformed, state)
    end
  end

  defp load_barrier_record(_, state), do: rebuild_barriers(:malformed, state)

  # The previous format retained the old membership while cleanup was pending.
  defp decode_cleanup([], owed, served) do
    {:ok, MapSet.intersection(owed, MapSet.new(Map.keys(served))), %{}}
  end

  defp decode_cleanup([cleanup, removals], _owed, _served) when is_list(removals) do
    if valid_oids?(cleanup) and
         Enum.all?(removals, fn
           [handle, oids] when is_binary(handle) -> valid_oids?(oids) and oids != []
           _ -> false
         end) do
      {:ok, MapSet.new(cleanup),
       Map.new(removals, fn [handle, oids] -> {handle, MapSet.new(oids)} end)}
    else
      :error
    end
  end

  defp decode_cleanup(_, _, _), do: :error

  defp read_barrier_state(state, delays) do
    case read_barrier_record(state) do
      {:error, reason} when reason not in [:not_found, :corrupted] and delays != [] ->
        [delay | remaining] = delays
        Process.sleep(delay)
        read_barrier_state(state, remaining)

      result ->
        result
    end
  end

  defp read_barrier_record(state) do
    PersistentKV.get(state.persistent_kv, barrier_key(state))
  rescue
    error in File.Error -> {:error, error.reason}
  catch
    :exit, {reason, {GenServer, :call, _}} -> {:error, reason}
  end

  defp valid_barrier_entries?(owed, served)
       when is_integer(length(owed)) and is_integer(length(served)) do
    valid_oids?(owed) and
      Enum.all?(served, fn
        [oid, schema, name, membership] ->
          is_integer(oid) and oid > 0 and is_binary(schema) and is_binary(name) and
            is_integer(membership) and membership > 0

        _ ->
          false
      end)
  end

  defp valid_barrier_entries?(_, _), do: false

  defp valid_oids?(oids), do: is_list(oids) and Enum.all?(oids, &(is_integer(&1) and &1 > 0))

  defp restore_barriers(timeline, record, state) when timeline == state.database_timeline do
    struct!(state, record)
  end

  defp restore_barriers(
         [system_id, timeline],
         record,
         %{database_timeline: [current, _]} = state
       )
       when system_id == current and (is_binary(timeline) or is_integer(timeline)) do
    # A rewind can undo a publication change. Keep the baseline to detect gaps in
    # shapes that survive promotion, and require new drains on the new timeline.
    owed = Enum.into(Map.keys(record.served_relations), record.barriers_owed)
    persist_barrier_state(%{record | barriers_owed: owed}, owed, state)
  end

  defp restore_barriers(_, _record, state), do: rebuild_barriers(:malformed, state)

  defp rebuild_barriers(reason, state) do
    Logger.warning(
      "Could not read publication barriers: #{inspect(reason)}. " <>
        "Requiring drains before admitting tables without a recorded barrier"
    )

    owed = state.prepared_relation_filters

    write_barrier_state(%{
      state
      | barriers_owed: owed,
        unpersisted_barriers: owed,
        recheck_unserved?: true
    })
  end

  defp decode_served(served) do
    Map.new(served, fn [oid, schema, name, membership] ->
      {oid, {{schema, name}, membership}}
    end)
  end

  defp barrier_record(state) do
    Map.take(state, [:barriers_owed, :served_relations, :cleanup_owed, :removals_owed])
  end

  defp persist_barrier_state(record, affected, state) do
    if record == barrier_record(state) do
      state
    else
      stage_barrier_write(record, affected, state)
    end
  end

  defp stage_barrier_write(record, affected, state) do
    state = struct!(state, record)
    state = %{state | unpersisted_barriers: Enum.into(affected, state.unpersisted_barriers)}

    state = if state.barrier_write_retry, do: state, else: write_barrier_state(state)

    if state.barrier_write_retry do
      Enum.reduce(affected, state, fn oid, state ->
        if Map.has_key?(state.waiters, oid),
          do: hold_admission(expand_oid(oid, state), state),
          else: state
      end)
    else
      state
    end
  end

  defp hold_admission(oid_rel, state) do
    reply = {:error, Electric.SnapshotError.publication_admission_pending()}
    %{reply_to_relation_waiters(oid_rel, reply, state) | in_flight_relation_filters: nil}
  end

  defp write_barrier_state(state) do
    rows =
      for {oid, {{schema, name}, membership}} <- state.served_relations,
          do: [oid, schema, name, membership]

    data = [
      state.database_timeline,
      Enum.to_list(state.barriers_owed),
      rows,
      state.recheck_unserved?,
      Enum.to_list(state.cleanup_owed),
      Enum.map(state.removals_owed, fn {handle, oids} -> [handle, Enum.to_list(oids)] end)
    ]

    case write_barrier_record(state, data) do
      :ok ->
        %{state | unpersisted_barriers: MapSet.new(), barrier_write_retry: nil}

      {:error, reason} ->
        delay = state.barrier_write_retry || @barrier_retry_initial

        Logger.warning(
          "Could not persist publication barriers: #{inspect(reason)}. Retrying in #{delay}ms"
        )

        Process.send_after(self(), :retry_barrier_write, delay)
        %{state | barrier_write_retry: min(delay * 2, @barrier_retry_max)}
    end
  end

  defp write_barrier_record(state, data) do
    PersistentKV.set(state.persistent_kv, barrier_key(state), data)
  rescue
    error in File.Error -> {:error, error.reason}
  catch
    :exit, {reason, {GenServer, :call, _}} -> {:error, reason}
  end

  defp barrier_key(state) do
    system_id = List.first(state.database_timeline || [])
    identity = :erlang.term_to_binary({state.publication_name, system_id})
    scope = :crypto.hash(:sha256, identity) |> Base.encode16(case: :lower)
    "#{state.stack_id}:publication_barriers:#{scope}"
  end

  @impl true
  def handle_info(:retry_barrier_write, state) do
    state = write_barrier_state(state)
    # Recheck the publication before admitting a table whose drain was recorded by this write.
    {:noreply, if(state.barrier_write_retry, do: state, else: update_publication(state))}
  end

  def handle_info(:refresh_publication, state) do
    state =
      case Electric.StatusMonitor.status(state.stack_id) do
        %{conn: :up} ->
          update_publication(state)

        status ->
          Logger.debug("Publication update skipped due to inactive stack: #{inspect(status)}")
          state
      end

    {:noreply, schedule_refresh(state)}
  end

  # The sweep is what recovers a publication that drifted without any shape noticing — a table
  # dropped from it out of band, or a replica identity reset by a migration. Neither changes the
  # relation's columns, so the schema reconciler sees nothing, and no shape ever asks again for a
  # relation it already holds. It re-arms unconditionally, including on the branches that skip the
  # work: a restart, a failover or a wait on the advisory lock are exactly the windows it exists to
  # recover from, and dropping the timer there retires the sweep permanently.
  @spec schedule_refresh(state()) :: state()
  defp schedule_refresh(state) do
    Process.send_after(self(), :refresh_publication, state.publication_refresh_period)
    state
  end

  @spec update_publication_if_necessary(state()) :: state()
  defp update_publication_if_necessary(state) do
    if update_needed?(state), do: update_publication(state), else: state
  end

  @spec update_publication(state()) :: state()
  defp update_publication(state) do
    Electric.Replication.PublicationManager.Configurator.configure_publication(
      state.stack_id,
      filter_snapshot(state)
    )

    %{
      state
      | submitted_relation_filters: state.prepared_relation_filters,
        in_flight_relation_filters: state.prepared_relation_filters
    }
  end

  defp filter_snapshot(state) do
    # The configurator may outlive a tracker restart.
    {System.unique_integer([:positive, :monotonic]),
     expand_oids(state.prepared_relation_filters, state)}
  end

  @spec expand_oids(MapSet.t(Electric.relation_id()), state()) ::
          MapSet.t(Electric.oid_relation())
  defp expand_oids(%MapSet{} = oids, state) do
    MapSet.new(oids, &expand_oid(&1, state))
  end

  @spec expand_oid(Electric.relation_id(), state()) :: Electric.oid_relation()
  defp expand_oid(oid, %{oid_to_relation: oid_to_relation}) do
    {oid, Map.fetch!(oid_to_relation, oid)}
  end

  defp update_needed?(%__MODULE__{
         prepared_relation_filters: prepared,
         submitted_relation_filters: submitted,
         committed_relation_filters: committed,
         in_flight_relation_filters: in_flight
       }) do
    prepared_changed? = not MapSet.equal?(prepared, submitted)

    # "submitted but not yet committed" is the legitimate "the last request
    # didn't fully go through, retry it" signal. But we must not act on it while
    # a request for the same relations is still outstanding, otherwise every
    # add_shape/remove_shape in that window would send another duplicate request.
    # If a request dies without ever reporting back, the periodic refresh is the fallback
    # that retries.
    retry_needed? = is_nil(in_flight) and not MapSet.equal?(submitted, committed)

    prepared_changed? or retry_needed?
  end

  @spec clear_in_flight_if_committed(state()) :: state()
  defp clear_in_flight_if_committed(
         %__MODULE__{
           committed_relation_filters: committed,
           in_flight_relation_filters: in_flight
         } = state
       ) do
    if not is_nil(in_flight) and MapSet.equal?(committed, in_flight) do
      %{state | in_flight_relation_filters: nil}
    else
      state
    end
  end

  @spec add_shape_to_publication_filters(shape_handle(), publication_filter(), state()) :: state()
  defp add_shape_to_publication_filters(
         shape_handle,
         {{oid, relation} = rel_key, _} = pub_filter,
         state
       ) do
    if is_handle_tracked?(shape_handle, state) do
      Logger.debug("Shape handle already tracked: #{inspect(shape_handle)}")
      state
    else
      state = Map.update!(state, :oid_to_relation, &Map.put_new(&1, oid, relation))
      state = track_shape_handle(shape_handle, pub_filter, state)
      do_update_relation_filters(rel_key, :add, state)
    end
  end

  @spec remove_shape_from_publication_filters(shape_handle(), state()) :: state()
  defp remove_shape_from_publication_filters(shape_handle, state) do
    if is_handle_tracked?(shape_handle, state) do
      {oid, _rel} = rel_key = fetch_tracked_shape_relation!(shape_handle, state)
      state = untrack_shape_handle(shape_handle, state)
      state = admit(oid, [shape_handle], state)
      do_update_relation_filters(rel_key, :remove, state)
    else
      Logger.debug("Shape handle already not tracked: #{inspect(shape_handle)}")
      state
    end
  end

  @spec do_update_relation_filters(
          Electric.oid_relation(),
          :add | :remove,
          state()
        ) :: state()
  defp do_update_relation_filters({oid, _rel} = rel_key, operation, %__MODULE__{} = state) do
    %{
      prepared_relation_filters: prepared,
      relation_ref_counts: counts,
      oid_to_relation: oid_lookup
    } = state

    current = Map.get(counts, oid, 0)

    new_count =
      case operation do
        :add -> current + 1
        :remove -> max(current - 1, 0)
      end

    # we could rederive the prepared filters from the keys of the counts map
    # but since we're keeping both around might as well not iterate over the
    # whole map every time
    {prepared, counts, oid_lookup} =
      cond do
        new_count == 0 and current > 0 ->
          # if the oid is not referenced then remove from the lookup
          # so that if the table name has changed we get the new name
          # as shapes are defined on it
          {MapSet.delete(prepared, oid), Map.delete(counts, oid), Map.delete(oid_lookup, oid)}

        current == 0 and new_count > 0 ->
          {MapSet.put(prepared, oid), Map.put(counts, oid, new_count), oid_lookup}

        new_count > 0 ->
          {prepared, Map.put(counts, oid, new_count), oid_lookup}

        true ->
          {prepared, counts, oid_lookup}
      end

    # reply_to_relation_waiters both replies and clears the waiter list for this oid. Its result
    # must be carried forward, or the waiters stay in the map forever (a slow leak) and a later
    # config result replies a second time to a call already completed here.
    state =
      if not MapSet.member?(prepared, oid) do
        reply_to_relation_waiters(
          rel_key,
          {:error, %RuntimeError{message: "Shape removed before updating publication"}},
          state
        )
      else
        state
      end

    %{
      state
      | prepared_relation_filters: prepared,
        relation_ref_counts: counts,
        oid_to_relation: oid_lookup
    }
  end

  @spec add_waiter(GenServer.from(), shape_handle(), publication_filter(), state()) ::
          state()
  defp add_waiter(from, shape_handle, pub_filter, %__MODULE__{waiters: waiters} = state) do
    {{oid, _relaion}, _} = pub_filter
    from_tuple = {from, shape_handle}

    %{
      state
      | waiters: Map.update(waiters, oid, [from_tuple], &[from_tuple | &1]),
        awaiting_admission:
          Map.update(state.awaiting_admission, oid, MapSet.new([shape_handle]), fn handles ->
            MapSet.put(handles, shape_handle)
          end)
    }
  end

  # A shape that has waited for the publication and been told to retry takes no snapshot before it
  # waits again, so the table being out does not touch it until the publication admits it.
  defp admit(oid, handles, state) do
    case Map.fetch(state.awaiting_admission, oid) do
      {:ok, awaiting} ->
        awaiting = MapSet.difference(awaiting, MapSet.new(handles))

        awaiting_admission =
          if MapSet.size(awaiting) == 0,
            do: Map.delete(state.awaiting_admission, oid),
            else: Map.put(state.awaiting_admission, oid, awaiting)

        %{state | awaiting_admission: awaiting_admission}

      :error ->
        state
    end
  end

  @spec reply_to_relation_waiters(Electric.oid_relation(), any(), state()) :: state()
  defp reply_to_relation_waiters({oid, _rel}, reply, %__MODULE__{waiters: waiters} = state) do
    rel_waiters = Map.get(waiters, oid, [])
    for {from, _} <- rel_waiters, do: GenServer.reply(from, reply)
    %{state | waiters: Map.delete(waiters, oid)}
  end

  @spec reply_to_all_waiters(any(), state()) :: state()
  defp reply_to_all_waiters(reply, %__MODULE__{waiters: waiters} = state) do
    for {_oid, rel_waiters} <- waiters,
        {from, _} <- rel_waiters,
        do: GenServer.reply(from, reply)

    %{state | waiters: %{}}
  end

  # A table the configurator has to add is out of the publication, and what was written to it
  # meanwhile never reached the replication stream. A shape on it from before may lack those
  # writes: one restored or served already, or one whose snapshot has started. Only a shape the
  # publication has not admitted yet, with no snapshot yet, is whole, as its snapshot runs after
  # the addition: one waiting for it, or one told to retry, which waits again first. The others are
  # for the configurator to remove, and those of them waiting are told why. On a table added for
  # its first shapes, every tracked shape is one not admitted yet, so the tracked shapes are only
  # read when the count says some other one is there, and in one pass for every table.
  defp shapes_predating_addition(oid_rels, state) do
    relations =
      for {oid, rel} <- oid_rels do
        {stale_waiters, waiters} =
          state.waiters
          |> Map.get(oid, [])
          |> Enum.split_with(fn {_from, handle} ->
            ShapeStatus.snapshot_started?(state.stack_id, handle)
          end)

        whole =
          state.awaiting_admission
          |> Map.get(oid, MapSet.new())
          |> MapSet.filter(fn handle ->
            is_handle_tracked?(handle, state) and
              not ShapeStatus.snapshot_started?(state.stack_id, handle)
          end)

        %{oid: oid, rel: rel, stale_waiters: stale_waiters, waiters: waiters, whole: whole}
      end

    whole = Enum.reduce(relations, MapSet.new(), &MapSet.union(&1.whole, &2))

    scan =
      for %{oid: oid, whole: whole} <- relations,
          Map.get(state.relation_ref_counts, oid, 0) > MapSet.size(whole),
          do: {{:"$1", oid, :_}, [], [{{:"$1", oid}}]}

    stale =
      if scan == [] do
        []
      else
        state.tracked_handles_table
        |> :ets.select(scan)
        |> Enum.reject(fn {handle, _oid} -> MapSet.member?(whole, handle) end)
      end

    {stale, relations}
  end

  defp take_shapes_predating_addition(stale, relations, state) do
    stale_counts = Enum.frequencies_by(stale, fn {_handle, oid} -> oid end)

    for %{oid: oid, rel: rel, stale_waiters: stale_waiters} <- relations do
      table = Electric.Utils.relation_to_sql(rel)

      error = %RuntimeError{
        message:
          "Table #{table} was out of the publication and the shape may lack what was written " <>
            "to it meanwhile"
      }

      for {from, _handle} <- stale_waiters, do: GenServer.reply(from, {:error, error})

      if count = stale_counts[oid] do
        Logger.warning(
          "Table #{table} is out of publication #{state.publication_name}: removing " <>
            "#{count} shapes on it that may lack what was written to it while it was out"
        )
      end
    end

    waiters =
      Enum.reduce(relations, state.waiters, fn
        %{oid: oid, waiters: []}, waiters -> Map.delete(waiters, oid)
        %{oid: oid, waiters: kept}, waiters -> Map.put(waiters, oid, kept)
      end)

    awaiting_admission =
      Enum.reduce(relations, state.awaiting_admission, fn %{oid: oid, whole: whole}, awaiting ->
        if MapSet.size(whole) == 0,
          do: Map.delete(awaiting, oid),
          else: Map.put(awaiting, oid, whole)
      end)

    %{state | waiters: waiters, awaiting_admission: awaiting_admission}
  end

  # In case the publication switches from publishing to not publishing generated columns,
  # we fail any shapes and waiters that depend on that feature. We use an inefficient O(n)
  # scan through our tracked shapes to find those that depend on generated columns as this
  # is only expected to happen in the rare cases of publication reconfiguration at runtime.
  @spec fail_generated_column_shapes(state()) :: state()
  defp fail_generated_column_shapes(state) do
    missing_gen_col_error =
      Electric.DbConfigurationError.publication_missing_generated_columns(state.publication_name)

    oid_to_handles_to_fail =
      :ets.foldl(
        fn
          {handle, oid, true}, acc -> Map.update(acc, oid, [handle], &[handle | &1])
          {_handle, _oid, false}, acc -> acc
        end,
        Map.new(),
        state.tracked_handles_table
      )

    # scan through and reply to any waiters for shapes that require generated columns
    new_waiters =
      oid_to_handles_to_fail
      |> Enum.reduce(state.waiters, fn {oid, handles_to_fail}, waiters ->
        if rel_waiters = Map.get(waiters, oid) do
          {to_fail, to_keep} =
            rel_waiters |> Enum.split_with(fn {_from, handle} -> handle in handles_to_fail end)

          for {from, _} <- to_fail,
              do: GenServer.reply(from, {:error, missing_gen_col_error})

          if to_keep == [],
            do: Map.delete(waiters, oid),
            else: Map.put(waiters, oid, to_keep)
        else
          waiters
        end
      end)

    if map_size(oid_to_handles_to_fail) > 0 do
      # schedule removals for any tracked shapes that require generated columns
      handles = oid_to_handles_to_fail |> Map.values() |> List.flatten()

      ShapeCleaner.remove_shapes_async(
        state.stack_id,
        handles,
        {:error, Electric.SnapshotError.from_error(missing_gen_col_error)}
      )
    end

    %{state | waiters: new_waiters}
  end

  @spec fetch_tracked_shape_relation!(shape_handle(), state()) :: Electric.oid_relation()
  defp fetch_tracked_shape_relation!(
         shape_handle,
         %__MODULE__{
           tracked_handles_table: tracked_handles_table
         } = state
       ) do
    oid = :ets.lookup_element(tracked_handles_table, shape_handle, 2)
    expand_oid(oid, state)
  end

  @spec track_shape_handle(shape_handle(), publication_filter(), state()) :: state()
  defp track_shape_handle(
         shape_handle,
         {{oid, _relation}, generated?},
         %__MODULE__{tracked_handles_table: tracked_handles_table} = state
       ) do
    true = :ets.insert_new(tracked_handles_table, {shape_handle, oid, generated?})
    state
  end

  @spec untrack_shape_handle(shape_handle(), state()) :: state()
  defp untrack_shape_handle(
         shape_handle,
         %__MODULE__{tracked_handles_table: tracked_handles_table} = state
       ) do
    true = :ets.delete(tracked_handles_table, shape_handle)
    state
  end

  defp is_handle_tracked?(shape_handle, %__MODULE__{tracked_handles_table: tracked_handles_table}) do
    :ets.member(tracked_handles_table, shape_handle)
  end

  defp is_known_publication_error(%Electric.DbConfigurationError{}), do: true
  defp is_known_publication_error(%DBConnection.ConnectionError{}), do: true

  defp is_known_publication_error(%Postgrex.Error{postgres: %{code: code}})
       when code in [
              :insufficient_privilege,
              :undefined_table,
              :undefined_function
            ],
       do: true

  defp is_known_publication_error(_), do: false
end
