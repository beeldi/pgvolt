defmodule Electric.Shapes.Consumer.EventHandler.WrittenKeyNarrowingTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Transaction
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.EventHandler
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Steady
  alias Electric.Shapes.Consumer.Subqueries.MoveBroadcast
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Shape

  @ref ["$sublink", "0"]
  @relation {"public", "employees"}
  @id %{name: "id", type: "int8", type_id: {20, 1}, pk_position: 0, not_null: true}
  @division %{name: "divisionId", type: "int8", type_id: {20, 1}}
  @name %{name: "name", type: "text", type_id: {25, 1}}
  @pk ["id"]
  @keyless ["id", "divisionId", "name"]

  for polarity <- [:positive, :negated] do
    @polarity polarity

    test "a routing gap can leave an unwritten version outside the old view (#{polarity})" do
      handler = new_handler(@polarity)
      source = employee(20, 4, "kept")
      written = employee(20, 4, "away")
      omitted = employee(20, 1, "away")
      returned = employee(20, 1, "kept")

      # The move's transaction also updates written -> omitted. Neither side matches
      # the old routing view. The snapshot sees omitted, then division 1 leaves the
      # shape again, so the snapshot must skip it.
      assert_splice(
        handler,
        [source],
        [update(source, written, @pk)],
        [omitted],
        1,
        [update(omitted, returned, @pk)],
        [returned]
      )
    end

    test "a before-view row can be absent after a routed write (#{polarity})" do
      handler = new_handler(@polarity)
      source = employee(20, 4, "kept")
      written = employee(20, 4, "away")
      hidden = employee(20, 1, "away")
      omitted = employee(20, 2, "away")
      returned = employee(20, 2, "kept")

      # Routing drops written -> hidden, but must deliver hidden -> omitted because
      # division 2 already matches. The queued exclusion suppresses that write and
      # the snapshot omits it because it matched the view before the move.
      assert_splice(
        handler,
        [source],
        [update(source, written, @pk), update(hidden, omitted, @pk)],
        [],
        2,
        [update(omitted, returned, @pk)],
        [returned]
      )
    end

    test "a deferred key can still hold a different before-view row (#{polarity})" do
      handler = new_handler(@polarity)
      held = employee(20, 2, "away")
      source = employee(10, 4, "kept")
      borrowed = employee(20, 4, "away")
      renamed = employee(20, 2, "kept")
      removed = employee(30, 2, "away")
      moved = employee(40, 1, "away")

      # The move's transaction borrows the key of held then moves borrowed to
      # an unused key at division 4. Routing drops that second update. The
      # drain excludes division 2, but held is still present at the post write.
      assert_splice(
        handler,
        [source, held, removed],
        [update(source, borrowed, @pk)],
        [moved],
        2,
        [update(held, renamed, @pk)],
        [renamed, moved]
      )
    end
  end

  test "deleting two identical absent keyless rows does not delete an absent key" do
    row = employee(20, 1, "away")

    # One copy predates the move. Routing has broadened by the time the next
    # transaction inserts the other copy. The queued exclusion skips both.
    assert_splice(
      new_handler(:positive, @keyless),
      [],
      [insert(row, @keyless)],
      [row, row],
      1,
      [delete(row, @keyless), delete(row, @keyless)],
      [],
      pre_xid: 104
    )
  end

  test "a keyless row held before the move survives its omitted identical copy" do
    held = employee(20, 2, "away")
    renamed = employee(20, 2, "kept")

    # The queued exclusion suppresses the insert. The first post-snapshot
    # change consumes that omitted copy, leaving the original copy to rename.
    assert_splice(
      new_handler(:positive, @keyless),
      [held],
      [insert(held, @keyless)],
      [],
      2,
      [delete(held, @keyless), update(held, renamed, @keyless)],
      [renamed]
    )
  end

  defp assert_splice(
         handler,
         initial_rows,
         pre_changes,
         snapshot_rows,
         removed_division,
         post_changes,
         expected_rows,
         opts \\ []
       ) do
    [dep_handle] = handler.shape_info.shape.shape_dependencies_handles

    {enter, leave} =
      case handler.shape_info.dnf_plan.dependency_polarities do
        %{0 => :positive} -> {:move_in, :move_out}
        %{0 => :negated} -> {:move_out, :move_in}
      end

    assert {:ok, %Buffering{} = buffering, _effects} =
             EventHandler.handle_event(handler, move(dep_handle, enter, 1, 103))

    events = [
      txn(Keyword.get(opts, :pre_xid, 103), pre_changes),
      {:pg_snapshot_known, {105, 105, []}},
      move(dep_handle, leave, removed_division, 106),
      txn(107, post_changes),
      {:query_move_in_complete, "move-in-snapshot", length(snapshot_rows), 0, lsn(105), nil}
    ]

    {state, effects} =
      Enum.reduce(events, {buffering, []}, fn event, {state, acc} ->
        assert {:ok, state, effects} = EventHandler.handle_event(state, event)
        {state, acc ++ effects}
      end)

    assert %Steady{} = state

    assert replay(buffering, effects, initial_rows, snapshot_rows) == MapSet.new(expected_rows)
  end

  # A key must exist before update/delete and be absent before insert. Replay
  # the snapshot skips and move controls as well as the row operations.
  defp replay(buffering, effects, initial_rows, snapshot_rows) do
    info = buffering.shape_info
    %{views_before_move: before, views_after_move: after_move} = buffering.active_move
    initial = Map.new(initial_rows, &snapshot_entry(&1, info, before))
    snapshot = Enum.map(snapshot_rows, &snapshot_entry(&1, info, after_move))
    log = client_log(effects, snapshot)

    log
    |> Enum.reduce(initial, fn
      {:insert, key, entry}, held ->
        refute Map.has_key?(held, key), "insert of held key #{key} in #{inspect(log)}"
        Map.put(held, key, entry)

      {:update, key, entry}, held ->
        assert Map.has_key?(held, key), "update of absent key #{key} in #{inspect(log)}"
        Map.put(held, key, entry)

      {:delete, key}, held ->
        assert Map.has_key?(held, key), "delete of absent key #{key} in #{inspect(log)}"
        Map.delete(held, key)

      {:move_in, patterns}, held ->
        refute Enum.any?(held, fn {_key, {_row, tags, _conditions}} ->
                 MoveBroadcast.matched_by?(tags, patterns)
               end)

        held

      {:move_out, patterns}, held ->
        Map.reject(held, fn {_key, {_row, tags, conditions}} ->
          MoveBroadcast.removed_by?(tags, conditions, patterns)
        end)
    end)
    |> Map.values()
    |> MapSet.new(&elem(&1, 0))
  end

  defp client_log(effects, snapshot_rows) do
    Enum.flat_map(effects, fn
      %Effects.AppendChanges{changes: changes} ->
        Enum.flat_map(changes, &operation/1)

      %Effects.AppendMoveInSnapshot{skip_rows: skip_rows} ->
        for {key, entry} <- snapshot_rows,
            not skipped?(skip_rows, key, entry),
            do: {:insert, key, entry}

      %Effects.AppendControl{message: %{headers: %{event: event, patterns: patterns}}} ->
        patterns =
          Enum.reduce(patterns, %{}, fn %{pos: pos, value: value}, acc ->
            Map.update(acc, pos, MapSet.new([value]), &MapSet.put(&1, value))
          end)

        case event do
          "move-in" -> [{:move_in, patterns}]
          "move-out" -> [{:move_out, patterns}]
        end

      _ ->
        []
    end)
  end

  defp skipped?(nil, _key, _entry), do: false

  defp skipped?({any, written, keys}, key, entry) do
    removed?(any, entry) or (MapSet.member?(keys, key) and removed?(written, entry))
  end

  defp removed?(nil, _entry), do: false
  defp removed?(:all, _entry), do: true

  defp removed?(patterns, {_row, tags, conditions}),
    do: MoveBroadcast.removed_by?(tags, conditions, patterns)

  defp operation(%Changes.NewRecord{} = change), do: [{:insert, change.key, entry(change)}]

  defp operation(%Changes.UpdatedRecord{old_key: nil} = change),
    do: [{:update, change.key, entry(change)}]

  defp operation(%Changes.UpdatedRecord{} = change),
    do: [{:delete, change.old_key}, {:insert, change.key, entry(change)}]

  defp operation(%Changes.DeletedRecord{key: key}), do: [{:delete, key}]

  defp entry(change), do: {change.record, change.move_tags, change.active_conditions}

  defp snapshot_entry(row, info, views) do
    assert {:ok, true, tags, conditions} =
             Shape.get_row_metadata(
               info.dnf_plan,
               row,
               views,
               info.shape.where,
               info.stack_id,
               info.shape_handle,
               Shape.composite_tag_encoding(info.shape)
             )

    {Changes.build_key(@relation, row, info.shape.root_pk), {row, tags, conditions}}
  end

  defp new_handler(polarity, pk \\ @pk) do
    id = if pk == @pk, do: @id, else: %{@id | pk_position: nil}

    inspector =
      Support.StubInspector.new(%{
        @relation => [id, @division, @name],
        {"public", "divisions"} => [@id, %{name: "active", type: "bool", type_id: {16, 1}}]
      })

    {op, divisions} = if polarity == :positive, do: {"IN", [2]}, else: {"NOT IN", [1, 4]}

    shape =
      Shape.new!("employees",
        where:
          ~s|name = 'kept' OR "divisionId" #{op} (SELECT id FROM public.divisions WHERE active = false)|,
        inspector: inspector
      )

    assert shape.root_pk == pk
    [dep_handle] = Enum.map(shape.shape_dependencies, &Shape.generate_id/1)
    shape = %{shape | shape_dependencies_handles: [dep_handle]}
    {:ok, dnf_plan} = DnfPlan.compile(shape)

    %Steady{
      shape_info: %ShapeInfo{
        shape: shape,
        stack_id: "stack-id",
        shape_handle: "shape-handle",
        dnf_plan: dnf_plan,
        ref_resolver: RefResolver.new(%{dep_handle => {0, @ref}}, %{0 => @ref}),
        buffer_max_transactions: 1000
      },
      views: %{@ref => MapSet.new(divisions)}
    }
  end

  defp employee(id, division, name),
    do: %{"id" => to_string(id), "divisionId" => to_string(division), "name" => name}

  defp insert(row, pk),
    do: %Changes.NewRecord{relation: @relation, record: row} |> Changes.fill_key(pk)

  defp delete(row, pk),
    do: %Changes.DeletedRecord{relation: @relation, old_record: row} |> Changes.fill_key(pk)

  defp update(old_row, row, pk) do
    Changes.UpdatedRecord.new(relation: @relation, old_record: old_row, record: row)
    |> Changes.fill_key(pk)
  end

  defp move(handle, kind, division, xid),
    do:
      {:materializer_changes, handle, %{kind => [{division, to_string(division)}], txids: [xid]}}

  defp txn(xid, changes) do
    %Transaction{
      xid: xid,
      changes: changes,
      num_changes: length(changes),
      lsn: lsn(xid),
      last_log_offset: LogOffset.new(lsn(xid), length(changes) - 1)
    }
  end

  defp lsn(value), do: Lsn.from_integer(value)
end
