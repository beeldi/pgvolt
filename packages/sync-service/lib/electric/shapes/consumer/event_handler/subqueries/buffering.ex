defmodule Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering do
  # Handles events while a dependency move is waiting for its snapshot boundary.

  @behaviour Electric.Shapes.Consumer.EventHandler

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes.Transaction
  alias Electric.Shapes.Consumer.EffectList
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Steady
  alias Electric.Shapes.Consumer.Subqueries.ActiveMove
  alias Electric.Shapes.Consumer.Subqueries.IndexChanges
  alias Electric.Shapes.Consumer.Subqueries.MoveQueue
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.Consumer.Subqueries.SplicePlan
  alias Electric.Shapes.Consumer.Subqueries.Views

  @enforce_keys [:shape_info, :queue, :active_move]
  defstruct [:shape_info, :queue, :active_move]

  @type t() :: %__MODULE__{
          shape_info: ShapeInfo.t(),
          queue: MoveQueue.t(),
          active_move: ActiveMove.t()
        }

  @spec start(
          ShapeInfo.t(),
          Views.t(),
          MoveQueue.t(),
          IndexChanges.move(),
          [String.t()],
          keyword()
        ) :: {:ok, t(), [Effects.t()]}
  def start(
        %ShapeInfo{} = shape_info,
        views,
        %MoveQueue{} = queue,
        {dep_move_kind, dep_index, values, txids, txids8} = move,
        subquery_ref,
        opts \\ []
      )
      when is_map(views) do
    state = %__MODULE__{
      shape_info: shape_info,
      queue: queue,
      active_move:
        views
        |> ActiveMove.start(dep_index, dep_move_kind, subquery_ref, values, txids, txids8)
        |> ActiveMove.carry_latest_seen_lsn(Keyword.get(opts, :latest_seen_lsn))
        |> Map.put(:kind, Keyword.get(opts, :kind, :move_in))
        |> Map.put(:query_patterns?, Keyword.get(opts, :query_patterns?, false))
    }

    effects =
      EffectList.new()
      |> maybe_subscribe_global_lsn(Keyword.get(opts, :subscribe_global_lsn?, true))
      |> EffectList.append_all(
        IndexChanges.effects_for_buffering(state.shape_info.dnf_plan, move, subquery_ref)
      )
      |> EffectList.append(start_move_in_query_effect(state))
      |> EffectList.to_list()

    {:ok, state, effects}
  end

  @impl true
  def handle_event(%__MODULE__{} = state, %Transaction{} = txn) do
    next_active_move = ActiveMove.buffer_txn(state.active_move, txn)

    if ActiveMove.buffered_txn_count(next_active_move) > state.shape_info.buffer_max_transactions do
      {:error, :buffer_overflow}
    else
      state
      |> Map.put(:active_move, next_active_move)
      |> maybe_splice()
    end
  end

  def handle_event(%__MODULE__{} = state, {:global_last_seen_lsn, lsn}) do
    next_active_move = ActiveMove.record_seen_lsn(state.active_move, Lsn.from_integer(lsn))

    state
    |> Map.put(:active_move, next_active_move)
    |> maybe_splice()
  end

  def handle_event(%__MODULE__{} = state, {:materializer_changes, dep_handle, payload}) do
    subquery_ref = RefResolver.ref_from_dep_handle!(state.shape_info.ref_resolver, dep_handle)
    dep_index = subquery_ref |> List.last() |> String.to_integer()
    dep_view = Views.current(state.active_move.views_after_move, subquery_ref)

    {:ok, %{state | queue: MoveQueue.enqueue(state.queue, dep_index, payload, dep_view)}, []}
  end

  def handle_event(%__MODULE__{} = state, {:pg_snapshot_known, snapshot}) do
    state
    |> Map.put(:active_move, ActiveMove.record_snapshot!(state.active_move, snapshot))
    |> maybe_splice()
  end

  def handle_event(
        %__MODULE__{} = state,
        {:query_move_in_complete, snapshot_name, row_count, row_bytes, move_in_lsn, patterns}
      ) do
    state
    |> Map.put(
      :active_move,
      ActiveMove.record_query_complete!(
        state.active_move,
        snapshot_name,
        row_count,
        row_bytes,
        move_in_lsn,
        patterns
      )
    )
    |> maybe_splice()
  end

  defp maybe_splice(%__MODULE__{active_move: active_move} = state) do
    if ActiveMove.ready_to_splice?(active_move) do
      splice(state)
    else
      {:ok, state, []}
    end
  end

  defp splice(%{active_move: active_move} = state) do
    steady_state = %Steady{
      shape_info: state.shape_info,
      views: active_move.views_after_move,
      queue: state.queue
    }

    # The queued moves reach the client after the splice, which writes the buffered changes against
    # the removals among them, so the drain runs first. A queued move the drain cannot apply
    # retires the shape, as it does outside a splice.
    with :ok <- validate_replacement_scope(active_move, state.queue),
         {:ok, next_state, drain_effects} <-
           Steady.drain_queue(
             steady_state,
             EffectList.new(),
             subscription_active?: true,
             latest_seen_lsn: active_move.latest_seen_lsn
           ),
         drained_views = drained_views(next_state),
         {:ok, splice_plan} <-
           SplicePlan.build(
             active_move,
             state.shape_info,
             drained_views,
             drained_removals(state.queue, drained_views, state.shape_info)
           ) do
      index_effects =
        IndexChanges.effects_for_complete(
          state.shape_info.dnf_plan,
          {active_move.dep_move_kind, active_move.dep_index, active_move.values,
           active_move.txids, active_move.txids8},
          active_move.subquery_ref
        )

      effects =
        splice_plan.effects
        |> EffectList.new()
        |> EffectList.append_all(index_effects)
        |> EffectList.append_all(EffectList.to_list(drain_effects))
        |> maybe_notify_flushed(splice_plan.flushed_log_offset)
        |> maybe_unsubscribe_global_lsn(next_state)

      {:ok, next_state, EffectList.to_list(effects)}
    end
  end

  # The views once the drain applied the moves it could: a move it starts brings its own changes,
  # removals among them, which its splice writes against the views before it.
  defp drained_views(%Steady{views: views}), do: views
  defp drained_views(%__MODULE__{active_move: active_move}), do: active_move.views_before_move

  defp maybe_unsubscribe_global_lsn(effects, %Steady{}),
    do: EffectList.append(effects, %Effects.UnsubscribeGlobalLsn{})

  defp maybe_unsubscribe_global_lsn(effects, %__MODULE__{}), do: effects

  # The queued values the drain removed, by dependency, with the transactions of their entry: a
  # positive view's removals the drained views lack, a negated view's additions they hold. A
  # removal the drain leaves to the move it starts, or queued behind that move, is the move's to
  # apply.
  defp drained_removals(queue, drained_views, %ShapeInfo{} = shape_info) do
    for {dep_index, polarity} <- shape_info.dnf_plan.dependency_polarities,
        queued = if(polarity == :negated, do: queue.move_in, else: queue.move_out),
        {values, txids, txids8} <- [Map.get(queued, dep_index)],
        subquery_ref = RefResolver.ref_from_dep_index!(shape_info.ref_resolver, dep_index),
        drained = Views.current(drained_views, subquery_ref),
        removed =
          Enum.filter(values, &(MapSet.member?(drained, elem(&1, 0)) == (polarity == :negated))),
        removed != [],
        into: %{},
        do: {dep_index, {removed, txids, txids8}}
  end

  defp validate_replacement_scope(%ActiveMove{kind: :replace} = move, queue) do
    # This snapshot compares one dependency. When the same transaction also
    # changes a queued dependency, splicing them separately can drop a row
    # that remains visible through the other dependency.
    if MoveQueue.other_dependency_in_transaction?(queue, move.dep_index, move.txids, move.txids8),
      do: {:error, :overlapping_dependency_replacement},
      else: :ok
  end

  defp validate_replacement_scope(%ActiveMove{}, _queue), do: :ok

  defp start_move_in_query_effect(%__MODULE__{shape_info: shape_info, active_move: active_move}) do
    %Effects.StartMoveInQuery{
      dnf_plan: shape_info.dnf_plan,
      trigger_dep_index: active_move.dep_index,
      values: active_move.values,
      views_before_move: active_move.views_before_move,
      views_after_move: active_move.views_after_move,
      kind: active_move.kind,
      query_patterns?: active_move.query_patterns?
    }
  end

  defp maybe_subscribe_global_lsn(effects, true) do
    EffectList.append(effects, %Effects.SubscribeGlobalLsn{})
  end

  defp maybe_subscribe_global_lsn(effects, false), do: effects

  defp maybe_notify_flushed(effects, nil), do: effects

  defp maybe_notify_flushed(effects, log_offset) do
    EffectList.append(effects, %Effects.NotifyFlushed{log_offset: log_offset})
  end
end
