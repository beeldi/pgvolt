defmodule Electric.Shapes.Consumer.Subqueries.SplicePlan do
  @moduledoc false

  alias Electric.Replication.Changes.Transaction
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Consumer.EffectList
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.Subqueries.ActiveMove
  alias Electric.Shapes.Consumer.Subqueries.MoveBroadcast
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.Consumer.Subqueries.Views
  alias Electric.Shapes.Consumer.TransactionConverter
  alias Electric.Shapes.Shape

  @enforce_keys [:effects]
  defstruct [:effects, :flushed_log_offset]

  @type t() :: %__MODULE__{
          effects: [Effects.t()],
          flushed_log_offset: LogOffset.t() | nil
        }

  # `drained_views` are the views once the drain applied the moves queued behind this one, before
  # the move it starts, and `removals` the queued values they removed, by dependency, with the
  # transactions of their queue entry. Their
  # effects follow the buffered changes, so a change buffered with a queued removal writes its row
  # against the views after that removal: the client never receives a row the drain deletes right
  # after, which Postgres never selected when the removal committed with or before it. A queued
  # addition waits for its own query, which sends the rows it brings in.
  @spec build(ActiveMove.t(), ShapeInfo.t(), Views.t(), %{non_neg_integer() => tuple()}) ::
          {:ok, t()} | {:error, term()}
  def build(%ActiveMove{} = active_move, %ShapeInfo{} = shape_info, drained_views, removals) do
    {pre_txns, post_txns} = ActiveMove.split_buffer(active_move)
    after_move = active_move.views_after_move
    written_views = removals_applied(after_move, after_move, drained_views, shape_info)

    with {:ok, pre_ops, written} <-
           convert_txns(
             pre_txns,
             shape_info,
             pre_snapshot_refs(active_move, shape_info, written_views)
           ),
         skip_rows = skip_rows(active_move, shape_info, drained_views, removals, written),
         {:ok, post_ops, _written} <-
           convert_txns(
             post_txns,
             shape_info,
             post_snapshot_refs(active_move, shape_info, written_views, written, skip_rows)
           ) do
      effects =
        EffectList.new()
        |> EffectList.append_all(pre_ops)
        |> EffectList.append_all(MoveBroadcast.effects_for_buffered_move(active_move, shape_info))
        |> maybe_append_move_in_snapshot(active_move, skip_rows)
        |> EffectList.append_all(post_ops)
        |> EffectList.to_list()

      {:ok,
       %__MODULE__{
         effects: effects,
         flushed_log_offset: ActiveMove.last_buffered_log_offset(active_move)
       }}
    end
  end

  # A move-out's rows leave at its own transaction, and its snapshot only names their tags. A
  # change committed before that snapshot writes its row against the views after the move, so a
  # row inserted with the moved key never reaches the client. The row it replaces is the one the
  # client holds: the views before the move decide it until an earlier change in the buffer wrote
  # that row, and the views after it from then on, so a row whose insert was dropped gets no update
  # or delete.
  defp pre_snapshot_refs(%ActiveMove{kind: :move_out} = active_move, _shape_info, written_views) do
    [
      extra_refs: {active_move.views_before_move, written_views},
      old_refs_once_written: written_views
    ]
  end

  # A negated replacement's rows leave with the values entering the dependency at its own
  # transaction, and its snapshot sends the rows the values leaving it let in. A change committed
  # before that snapshot writes its row against the views with the entering values added, which
  # leave the snapshot's rows to it, and reads the row it replaces as a move-out's change does.
  defp pre_snapshot_refs(%ActiveMove{kind: :replace, views_after_move: views} = move, _, views) do
    snapshot_views = snapshot_views(move)
    [extra_refs: {move.views_before_move, snapshot_views}, old_refs_once_written: snapshot_views]
  end

  defp pre_snapshot_refs(%ActiveMove{views_after_move: views} = active_move, _shape_info, views),
    do: active_move.views_before_move

  # A move-in's snapshot sends the rows it brings in, except those the views before it select
  # (`Querying.move_in_where_clause/5`), so its earlier changes write their rows against those
  # views, with a replacement's entering values added. A row they write must also hold once the
  # drained removals apply: a row only a removed value selects is one Postgres had dropped, or
  # drops within the splice. A row the views after the removals keep reads the views the snapshot
  # leaves it to; another reads them with the removals applied, so it leaves the client. Once an
  # earlier change in the buffer wrote a row, the change that replaces it reads the side that write
  # read.
  defp pre_snapshot_refs(%ActiveMove{} = active_move, shape_info, written_views) do
    snapshot_views = snapshot_views(active_move)
    dropped = removals_applied(snapshot_views, snapshot_views, written_views, shape_info)

    refs = fn record ->
      if included?(shape_info, record, written_views), do: snapshot_views, else: dropped
    end

    [extra_refs: {active_move.views_before_move, refs}, old_refs_once_written: refs]
  end

  # The views whose rows a move-in's snapshot leaves to the commits before it.
  defp snapshot_views(%ActiveMove{kind: :replace, values: {_outs, ins}} = active_move),
    do: Views.apply_move(active_move.views_before_move, active_move.subquery_ref, ins, :move_in)

  defp snapshot_views(%ActiveMove{} = active_move), do: active_move.views_before_move

  # After the snapshot, the client holds the rows the views after the move select, except a row an
  # earlier change wrote, which it holds as the drained removals leave it: that change wrote it
  # against them, the move's control leaves it as written, and the snapshot leaves it out when they
  # take it out (`skip_rows/5`). A row no earlier change wrote that the snapshot left out is not
  # held either (`held_refs/3`). The snapshot leaves a row out by its key, so a change to a row of
  # a written key reads the drained removals whatever that row holds: a change routing dropped
  # leaves the row Postgres holds unlike the one written. A row the views before the move select
  # is the exception: the client holds it from before the move, and the move's query leaves it
  # out, so it is another row of that key, which a deferrable primary key allows.
  defp post_snapshot_refs(
         %ActiveMove{views_after_move: views},
         _shape_info,
         views,
         _written,
         nil
       ),
       do: views

  defp post_snapshot_refs(%ActiveMove{} = active_move, shape_info, written_views, written, skip) do
    [
      extra_refs: {held_refs(active_move, shape_info, skip), written_views},
      old_refs_once_written: written_views,
      written: written || %{},
      written_key?: written_key_fun(active_move, shape_info, written)
    ]
  end

  defp written_key_fun(%ActiveMove{views_before_move: before}, shape_info, written) do
    keys = written_keys(written)
    fn key, record -> MapSet.member?(keys, key) and not included?(shape_info, record, before) end
  end

  defp written_keys(written), do: (written || %{}) |> Map.keys() |> MapSet.new(&elem(&1, 0))

  # The views a change after the snapshot reads the row it replaces against, when no earlier
  # change wrote that row: the views after the move, which the snapshot sent it under, unless the
  # snapshot left it out for a removal of the move's own transaction. That row reads the views
  # before the move, which the move's query left it out of.
  defp held_refs(%ActiveMove{views_after_move: after_move}, _shape_info, skip)
       when is_nil(skip) or is_nil(elem(skip, 0)),
       do: after_move

  defp held_refs(%ActiveMove{} = active_move, shape_info, {any_row, _written_row, _keys}) do
    before = active_move.views_before_move
    after_move = active_move.views_after_move

    fn record ->
      if not included?(shape_info, record, before) and
           snapshot_skipped?(shape_info, record, after_move, any_row),
         do: before,
         else: after_move
    end
  end

  defp snapshot_skipped?(_shape_info, _record, _views, :all), do: true

  defp snapshot_skipped?(shape_info, record, views, patterns) do
    {_included?, tags, conditions} = row_metadata(shape_info, record, views)

    MoveBroadcast.matched_by?(tags, patterns) and
      MoveBroadcast.removed_by?(tags, conditions, patterns)
  end

  # The rows of a move-in's snapshot that the drained removals take out of the shape and Postgres
  # never selected, which the drain would delete right after: they stay out of the log. A row a
  # commit before the snapshot wrote, keyed in `written`, is as that commit left it, so it leaves
  # under any of the removals. Any other row is as the move's snapshot reads it, which Postgres
  # selected unless a removal that snapshot sees took it out, the move's own transaction's among
  # them: only those removals take it out of the snapshot, and the drain deletes the others after
  # it. A row leaves when the move-out controls of removals deactivate a position of each disjunct
  # its active conditions under the views after the move hold (`MoveBroadcast.removed_by?/3`), and
  # every row leaves when they take back each value the move brings in.
  defp skip_rows(%ActiveMove{kind: :move_out}, _shape_info, _drained_views, _removals, _written),
    do: nil

  defp skip_rows(%ActiveMove{} = active_move, shape_info, drained_views, removals, written) do
    keys = written_keys(written)

    with_move =
      Map.filter(removals, fn {_dep, removal} ->
        shares_txn?(active_move, removal) or seen_by_snapshot?(active_move, removal)
      end)

    any_row = removed_rows(active_move, shape_info, drained_views, with_move)

    written_row =
      if MapSet.size(keys) > 0,
        do: removed_rows(active_move, shape_info, drained_views, removals)

    if any_row || written_row, do: {any_row, written_row, keys}
  end

  defp removed_rows(_active_move, _shape_info, _drained_views, removals) when removals == %{},
    do: nil

  defp removed_rows(%ActiveMove{} = active_move, shape_info, drained_views, removals) do
    values =
      Map.new(removals, fn {dep_index, {values, _txids, _txids8}} -> {dep_index, values} end)

    entering = entering_values(active_move, shape_info)
    removed = values |> Map.get(active_move.dep_index, []) |> MapSet.new(&elem(&1, 0))

    if entering != [] and Enum.all?(entering, &MapSet.member?(removed, elem(&1, 0))),
      do: :all,
      else:
        MoveBroadcast.removal_patterns(
          shape_info,
          values,
          active_move.views_after_move,
          drained_views
        )
  end

  # Whether the removal's queue entry names a transaction of the move's. One that names none, as a
  # dependency's own splice reports its moves, is taken as later: its rows are sent, and the drain
  # deletes them after, as upstream does.
  defp shares_txn?(%ActiveMove{} = active_move, {_values, txids, txids8}) do
    if active_move.txids8 != [] and not Enum.empty?(txids8),
      do: Enum.any?(active_move.txids8, &(&1 in txids8)),
      else: Enum.any?(active_move.txids, &(&1 in txids))
  end

  # Whether the move's snapshot sees every transaction the removal's queue entry names: the query
  # read Postgres after the removal, which no longer selected the rows it took out. One that names a
  # transaction the snapshot does not see, even beside one it sees, is taken as later, since the
  # entry does not say which of its values each transaction removed. Both lists are checked: an
  # entry that nets removals keeps the 64-bit ids of those that had one and the 32-bit ids of all,
  # so a 32-bit id can name a transaction the 64-bit ids leave out.
  defp seen_by_snapshot?(%ActiveMove{snapshot: nil}, _removal), do: false

  defp seen_by_snapshot?(%ActiveMove{snapshot: snapshot}, {_values, txids, txids8}) do
    (not Enum.empty?(txids8) or not Enum.empty?(txids)) and
      Enum.all?(txids8, &Transaction.visible_in_snapshot?(%{full_xid: &1}, snapshot)) and
      Enum.all?(txids, &Transaction.visible_in_snapshot?(&1, snapshot))
  end

  # The values of the moved dependency whose move brings rows into the shape.
  defp entering_values(%ActiveMove{kind: :replace, values: {outs, ins}} = move, shape_info) do
    if shape_info.dnf_plan.dependency_polarities[move.dep_index] == :negated,
      do: outs,
      else: ins
  end

  defp entering_values(%ActiveMove{values: values}, _shape_info), do: values

  defp included?(%ShapeInfo{} = shape_info, record, views) do
    {included?, _tags, _conditions} = row_metadata(shape_info, record, views)
    included?
  end

  defp row_metadata(%ShapeInfo{shape: shape} = shape_info, record, views) do
    {:ok, included?, tags, conditions} =
      Shape.get_row_metadata(
        shape_info.dnf_plan,
        record,
        views,
        shape.where,
        shape_info.stack_id,
        shape_info.shape_handle,
        Shape.composite_tag_encoding(shape)
      )

    {included?, tags, conditions}
  end

  defp removals_applied(views, from_views, to_views, %ShapeInfo{} = shape_info) do
    Enum.reduce(shape_info.dnf_plan.dependency_polarities, views, fn {dep_index, polarity}, acc ->
      subquery_ref = RefResolver.ref_from_dep_index!(shape_info.ref_resolver, dep_index)

      Views.apply_removals(
        acc,
        subquery_ref,
        Map.fetch!(from_views, subquery_ref),
        Map.fetch!(to_views, subquery_ref),
        polarity
      )
    end)
  end

  defp convert_txns(txns, %ShapeInfo{} = shape_info, views) when is_map(views),
    do: convert_txns(txns, shape_info, extra_refs: {views, views})

  defp convert_txns(txns, %ShapeInfo{} = shape_info, refs) when is_list(refs) do
    TransactionConverter.convert_transactions(
      txns,
      shape_info.shape,
      [
        stack_id: shape_info.stack_id,
        shape_handle: shape_info.shape_handle,
        dnf_plan: shape_info.dnf_plan
      ] ++ refs
    )
  end

  defp maybe_append_move_in_snapshot(effects, %ActiveMove{kind: :move_out}, _skip_rows),
    do: effects

  defp maybe_append_move_in_snapshot(effects, %ActiveMove{} = active_move, skip_rows) do
    EffectList.append(effects, %Effects.AppendMoveInSnapshot{
      snapshot_name: active_move.move_in_snapshot_name,
      row_count: active_move.move_in_row_count,
      row_bytes: active_move.move_in_row_bytes,
      snapshot: active_move.snapshot,
      skip_rows: skip_rows
    })
  end
end
