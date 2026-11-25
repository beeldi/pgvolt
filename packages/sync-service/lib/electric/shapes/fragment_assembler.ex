defmodule Electric.Shapes.FragmentAssembler do
  @moduledoc """
  The per-shape half of event routing.

  `Electric.Shapes.EventRouter` decides which shapes a change reaches and prepares shared
  conversions once per fan-out class; an assembler turns those routing decisions into
  per-shape `TransactionFragment`s, tracking which shapes have seen Begin and which are in
  the transaction so commits fan out correctly (including commit-only empty fragments).

  The unsharded collector runs one assembler over the full decision list; each router shard
  runs one over its slice of the decisions. Both paths execute exactly this code, so the
  sharded and single-process pipelines cannot drift.
  """

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.ShapeCache.PureFileStorage.LogFile

  defstruct current_xid: nil,
            shapes_seen_begin: MapSet.new(),
            shapes_in_txn: MapSet.new(),
            blocks?: false,
            unblockable: MapSet.new()

  # A framed block pays once it replaces at least two per-shape appends beyond the trailing
  # change that always rides individually.
  @min_block_run 3

  @type t :: %__MODULE__{}
  @type shape_id :: any()
  @type decision ::
          {change :: struct(), prepared_groups :: [{struct(), [shape_id()]}],
           unprepared_shape_ids :: [shape_id()]}

  @spec new(keyword()) :: t()
  def new(opts \\ []), do: %__MODULE__{blocks?: Keyword.get(opts, :blocks?, false)}

  @doc """
  Mark shapes whose consumers must receive individual change structs (a subscribed
  materializer walks them), excluding them from framed-block membership. A mark lives
  exactly as long as the shape it names; `remove_shape/2` reclaims it.
  """
  @spec mark_unblockable(t(), [shape_id()]) :: t()
  def mark_unblockable(%__MODULE__{} = assembler, shape_ids) do
    %{assembler | unblockable: Enum.into(shape_ids, assembler.unblockable)}
  end

  @spec remove_shape(t(), shape_id()) :: t()
  def remove_shape(%__MODULE__{} = assembler, shape_id) do
    %{
      assembler
      | shapes_seen_begin: MapSet.delete(assembler.shapes_seen_begin, shape_id),
        shapes_in_txn: MapSet.delete(assembler.shapes_in_txn, shape_id),
        unblockable: MapSet.delete(assembler.unblockable, shape_id)
    }
  end

  @doc """
  Build per-shape fragments from routing decisions.

  `decisions` must already be restricted to this assembler's shapes; changes whose shape list
  is empty after slicing are skipped by the accumulation. Returns
  `{fragments_by_shape_id, assembler}`.
  """
  @spec assemble(t(), TransactionFragment.t(), [decision()]) ::
          {%{shape_id() => TransactionFragment.t()}, t()}
  def assemble(%__MODULE__{} = assembler, %TransactionFragment{} = fragment, decisions) do
    assembler = maybe_start_transaction(assembler, fragment)

    # The routed accumulator holds only what must be tracked per change; begins, commits,
    # participation and relation sets are derived once per fragment per shape. Doing that
    # bookkeeping inside the per-change loop cost ~5 persistent-structure updates per
    # (row, shape) — at storm fan-out, two thirds of the router's per-row budget.
    {routed, open_run} =
      Enum.reduce(decisions, {%{}, nil}, fn {change, prepared_groups, unprepared_ids},
                                            {routed, run} ->
        relation = change.relation

        case {prepared_groups, unprepared_ids} do
          # A change reaching exactly one prepared class and nothing else can extend a run of
          # identical-membership changes, framed once for the whole class at flush. The struct
          # guard keeps deferred-mode `{:prepare, ids}` markers out of runs — they never reach
          # an assembler today, but a marker in a run would frame garbage.
          {[{%Changes.PreparedChange{} = prepared_change, shape_ids}], []} ->
            case run do
              {^shape_ids, ^relation, items, count} ->
                {routed, {shape_ids, relation, [prepared_change | items], count + 1}}

              _ ->
                routed = flush_run(routed, run)

                if assembler.blocks? and blockable_members?(assembler, shape_ids) do
                  {routed, {shape_ids, relation, [prepared_change], 1}}
                else
                  {Enum.reduce(
                     shape_ids,
                     routed,
                     &put_routed_change(&2, &1, prepared_change, relation)
                   ), nil}
                end
            end

          _ ->
            routed = flush_run(routed, run)

            routed =
              Enum.reduce(prepared_groups, routed, fn {prepared_change, shape_ids}, routed ->
                Enum.reduce(
                  shape_ids,
                  routed,
                  &put_routed_change(&2, &1, prepared_change, relation)
                )
              end)

            {Enum.reduce(unprepared_ids, routed, &put_routed_change(&2, &1, change, relation)),
             nil}
        end
      end)

    routed = flush_run(routed, open_run)

    participating = Map.keys(routed)

    begin_shapes =
      if assembler.current_xid != nil do
        Enum.reject(participating, &MapSet.member?(assembler.shapes_seen_begin, &1))
      else
        []
      end

    assembler = %{
      assembler
      | shapes_seen_begin: Enum.into(begin_shapes, assembler.shapes_seen_begin),
        shapes_in_txn: Enum.into(participating, assembler.shapes_in_txn)
    }

    {commit_only_shapes, assembler} =
      if fragment.commit == nil do
        {[], assembler}
      else
        commit_only =
          assembler.shapes_in_txn
          |> MapSet.difference(MapSet.new(participating))
          |> MapSet.to_list()

        {commit_only, reset_transaction_state(assembler)}
      end

    fragments =
      build_shape_fragments(
        routed,
        MapSet.new(begin_shapes),
        commit_only_shapes,
        fragment.commit,
        fragment
      )

    {fragments, assembler}
  end

  defp put_routed_change(routed, shape_id, change_for_shape, relation, weight \\ 1) do
    Map.update(
      routed,
      shape_id,
      {[change_for_shape], weight, [relation]},
      fn {changes, count, relations} ->
        {[change_for_shape | changes], count + weight, prepend_relation(relations, relation)}
      end
    )
  end

  defp blockable_members?(%__MODULE__{unblockable: unblockable}, shape_ids) do
    MapSet.size(unblockable) == 0 or
      not Enum.any?(shape_ids, &MapSet.member?(unblockable, &1))
  end

  defp flush_run(routed, nil), do: routed

  # The run's newest change always rides individually: the txn-end `last?` marking rewrites
  # a change per shape, and keeping the frame free of it means a block never needs splitting.
  defp flush_run(routed, {shape_ids, relation, [trailing | rest_newest_first], count}) do
    if count < @min_block_run do
      items_oldest_first = Enum.reverse([trailing | rest_newest_first])

      Enum.reduce(shape_ids, routed, fn id, routed ->
        Enum.reduce(items_oldest_first, routed, &put_routed_change(&2, id, &1, relation))
      end)
    else
      log_items =
        rest_newest_first
        |> Enum.reverse()
        |> Enum.flat_map(& &1.log_items)

      block = LogFile.build_framed_block(log_items)

      Enum.reduce(shape_ids, routed, fn id, routed ->
        routed
        |> put_routed_change(id, block, relation, count - 1)
        |> put_routed_change(id, trailing, relation)
      end)
    end
  end

  # Exact relation sets are built per fragment from this list; consecutive changes almost
  # always share a relation, so a head check keeps it short without a set per (row, shape).
  defp prepend_relation([relation | _] = relations, relation), do: relations
  defp prepend_relation(relations, relation), do: [relation | relations]

  defp maybe_start_transaction(assembler, %TransactionFragment{has_begin?: false}), do: assembler

  defp maybe_start_transaction(%__MODULE__{} = assembler, %TransactionFragment{
         xid: xid,
         has_begin?: true
       }) do
    %{
      assembler
      | current_xid: xid,
        shapes_seen_begin: MapSet.new(),
        shapes_in_txn: MapSet.new()
    }
  end

  defp reset_transaction_state(%__MODULE__{} = assembler) do
    %{
      assembler
      | current_xid: nil,
        shapes_seen_begin: MapSet.new(),
        shapes_in_txn: MapSet.new()
    }
  end

  defp build_shape_fragments(routed, begin_set, commit_only_shapes, commit, %TransactionFragment{
         xid: xid,
         full_xid: full_xid,
         lsn: lsn,
         last_log_offset: last_log_offset
       }) do
    fragments =
      Map.new(routed, fn {shape_id, {reversed_changes, change_count, relations}} ->
        fragment = %TransactionFragment{
          xid: xid,
          full_xid: full_xid,
          lsn: lsn,
          last_log_offset: last_log_offset,
          has_begin?: MapSet.member?(begin_set, shape_id),
          commit: commit,
          changes: Enum.reverse(reversed_changes),
          affected_relations: MapSet.new(relations),
          change_count: change_count
        }

        {shape_id, fragment}
      end)

    # Shapes that took part in an earlier fragment of this transaction but carry no change in
    # this one still receive the commit, as an otherwise empty fragment.
    Enum.reduce(commit_only_shapes, fragments, fn shape_id, fragments ->
      Map.put(fragments, shape_id, %TransactionFragment{
        xid: xid,
        full_xid: full_xid,
        lsn: lsn,
        last_log_offset: last_log_offset,
        has_begin?: false,
        commit: commit,
        changes: [],
        affected_relations: MapSet.new(),
        change_count: 0
      })
    end)
  end
end
