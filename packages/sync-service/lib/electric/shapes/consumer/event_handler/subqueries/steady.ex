defmodule Electric.Shapes.Consumer.EventHandler.Subqueries.Steady do
  # Handles events while the handler is in its steady, non-buffering state.

  @behaviour Electric.Shapes.Consumer.EventHandler

  alias Electric.Replication.Changes.Transaction
  alias Electric.Shapes.Consumer.EffectList
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering
  alias Electric.Shapes.Consumer.TransactionConverter
  alias Electric.Shapes.Consumer.Subqueries.IndexChanges
  alias Electric.Shapes.Consumer.Subqueries.MoveBroadcast
  alias Electric.Shapes.Consumer.Subqueries.MoveQueue
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.Consumer.Subqueries.Views

  @enforce_keys [:shape_info, :views]
  defstruct [:shape_info, :views, queue: MoveQueue.new()]

  @type t() :: %__MODULE__{
          shape_info: ShapeInfo.t(),
          views: Views.t(),
          queue: MoveQueue.t()
        }

  @impl true
  def handle_event(%__MODULE__{} = state, %Transaction{} = txn) do
    with {:ok, effects} <- append_txn_effects(txn, state.shape_info, state.views) do
      {:ok, state, effects}
    end
  end

  def handle_event(%__MODULE__{} = state, {:global_last_seen_lsn, _lsn}) do
    # Straggler message after unsubscribe; ignore.
    {:ok, state, []}
  end

  def handle_event(%__MODULE__{} = state, {:materializer_changes, dep_handle, payload}) do
    subquery_ref = RefResolver.ref_from_dep_handle!(state.shape_info.ref_resolver, dep_handle)
    dep_index = subquery_ref |> List.last() |> String.to_integer()
    dep_view = Views.current(state.views, subquery_ref)
    next_state = %{state | queue: MoveQueue.enqueue(state.queue, dep_index, payload, dep_view)}

    with {:ok, next_state, effects} <- drain_queue(next_state, EffectList.new()) do
      {:ok, next_state, EffectList.to_list(effects)}
    end
  end

  def handle_event(%__MODULE__{}, {:pg_snapshot_known, _snapshot}) do
    raise ArgumentError, "received {:pg_snapshot_known, snapshot} while no move-in is buffering"
  end

  def handle_event(
        %__MODULE__{},
        {:query_move_in_complete, _snapshot_name, _row_count, _row_bytes, _move_in_lsn, _patterns}
      ) do
    raise ArgumentError,
          "received {:query_move_in_complete, snapshot_name, row_count, row_bytes, move_in_lsn} while no move-in is buffering"
  end

  @spec drain_queue(t(), EffectList.t(), keyword()) ::
          {:ok, t() | Buffering.t(), EffectList.t()} | {:error, term()}
  def drain_queue(%__MODULE__{} = state, effects, opts \\ []) do
    case MoveQueue.pop_next(state.queue) do
      nil ->
        {:ok, state, effects}

      {{_, dep_index, _, _, _} = move, queue} ->
        plan = state.shape_info.dnf_plan
        polarity = Map.fetch!(plan.dependency_polarities, dep_index)
        subquery_ref = RefResolver.ref_from_dep_index!(state.shape_info.ref_resolver, dep_index)
        dep_view = state.views[subquery_ref]

        {{dep_move_kind, _, values, txids, txids8} = move, queue} =
          if polarity == :negated,
            do: pair_replacement(plan, move, queue, dep_view),
            else: {move, queue}

        subscription_active? = Keyword.get(opts, :subscription_active?, false)
        latest_seen_lsn = Keyword.get(opts, :latest_seen_lsn)

        kind = outer_move_kind(polarity, dep_move_kind)

        query_patterns? =
          MoveBroadcast.query_patterns?(plan, dep_index, values, dep_view, kind)

        direct_exclusion? = kind == :move_out and not query_patterns?

        cond do
          polarity == :negated and dep_move_kind == :move_out and
            MoveQueue.removal_after_addition?(queue, dep_index) and
              empties_view?(dep_view, values) ->
            # The removal reached an addition from an earlier commit, and
            # another dependency's queued move keeps them apart. Alone it
            # would empty the view, which no commit did: NOT IN an empty set
            # holds for a NULL key.
            {:error, :overlapping_dependency_replacement}

          direct_exclusion? and
              MoveBroadcast.ambiguous_text_pair?(state.shape_info, dep_index, values) ->
            # The existing tag cannot address either pair individually. A SQL
            # snapshot here also defers subsequent exclusions past root WAL.
            {:error, :ambiguous_text_pair_exclusion}

          direct_exclusion? ->
            views = Views.apply_move(state.views, subquery_ref, values, dep_move_kind)

            broadcast_values =
              if dep_move_kind == :move_in do
                MoveBroadcast.include_null_tag(
                  values,
                  state.views[subquery_ref],
                  views[subquery_ref]
                )
              else
                values
              end

            index_effects =
              IndexChanges.effects_for_complete(state.shape_info.dnf_plan, move, subquery_ref)

            effects =
              effects
              |> EffectList.append(
                MoveBroadcast.effect_for_move_out(
                  dep_index,
                  broadcast_values,
                  txids,
                  txids8,
                  state.shape_info
                )
              )
              |> EffectList.append_all(index_effects)

            drain_queue(%{state | queue: queue, views: views}, effects, opts)

          true ->
            with {:ok, next_state, start_effects} <-
                   Buffering.start(
                     state.shape_info,
                     state.views,
                     queue,
                     move,
                     subquery_ref,
                     kind: kind,
                     query_patterns?: query_patterns?,
                     subscribe_global_lsn?: not subscription_active?,
                     latest_seen_lsn: latest_seen_lsn
                   ) do
              {:ok, next_state, EffectList.append_all(effects, start_effects)}
            end
        end
    end
  end

  defp pair_replacement(plan, move, queue, dep_view) do
    case MoveQueue.pair_with_move_in(move, queue) do
      {{:replace, dep_index, {outs, _ins} = values, _, _}, _} = paired ->
        # With independent dependencies, let removals from each view activate
        # their disjuncts before additions deactivate the old ones. A
        # replacement needing a query still has to compare both halves at once,
        # and so does one whose removals empty the view: NOT IN an empty set
        # holds for a NULL key, so the removals alone would show the rows with
        # one that the additions hide again.
        if map_size(plan.dependency_polarities) == 1 or
             empties_view?(dep_view, outs) or
             MoveBroadcast.query_patterns?(plan, dep_index, values, dep_view, :replace),
           do: paired,
           else: {move, queue}

      unpaired ->
        unpaired
    end
  end

  defp empties_view?(view, outs) do
    keys = PgInterop.Sublink.keys(view)

    MapSet.size(keys) <= length(outs) and
      MapSet.subset?(keys, MapSet.new(outs, &elem(&1, 0)))
  end

  defp outer_move_kind(:negated, :move_in), do: :move_out
  defp outer_move_kind(:negated, :move_out), do: :move_in
  defp outer_move_kind(_, move_kind), do: move_kind

  defp append_txn_effects(%Transaction{} = txn, %ShapeInfo{} = shape_info, views)
       when is_map(views) do
    with {:ok, effects} <-
           TransactionConverter.transaction_to_effects(
             txn,
             shape_info.shape,
             stack_id: shape_info.stack_id,
             shape_handle: shape_info.shape_handle,
             extra_refs: {views, views},
             dnf_plan: shape_info.dnf_plan
           ) do
      effects =
        effects
        |> EffectList.new()
        |> EffectList.append(%Effects.NotifyFlushed{log_offset: txn.last_log_offset})
        |> EffectList.to_list()

      {:ok, effects}
    end
  end
end
