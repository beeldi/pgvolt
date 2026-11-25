defmodule Electric.Shapes.Consumer.EventHandler.WrittenKeyHandOverTest do
  # After a move-in's snapshot, a change to a row of a key a change before the snapshot wrote reads
  # the drained removals, since the snapshot leaves that key's row out by key. These cases share a
  # key between two rows, through a deferrable primary key or identical rows of a table without one,
  # and replay the shape's log against what the client holds.
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

  test "re-keying a written row must leave an earlier holder of the deferred key updatable" do
    handler = new_handler(@pk, [2])
    held = employee(20, 2, "away")
    source = employee(10, 4, "kept")
    borrowed = employee(20, 4, "away")
    parked = employee(30, 4, "away")
    returned = %{parked | "name" => "kept"}
    renamed = %{held | "name" => "kept"}

    # xid 104 takes the held row's key and vacates it again before committing. The
    # router delivers source -> borrowed and drops borrowed -> parked. The snapshot
    # finds neither row newly selected by division 1. xid 107 takes the same key
    # again via parked -> borrowed, also dropped by routing. The delivered re-key
    # to returned consumes the tracked borrowed row before the update to held.
    effects =
      splice(
        handler,
        [update(source, borrowed, @pk)],
        [update(borrowed, returned, @pk), update(held, renamed, @pk)],
        2,
        []
      )

    assert replay(handler, effects, [source, held], []) == MapSet.new([returned, renamed])
  end

  test "deleting two identical keyless rows omitted from the snapshot emits no deletes" do
    handler = new_handler(@keyless, [])
    row = employee(20, 1, "away")

    # One copy predates the move. xid 104 inserts the other and the snapshot
    # reads both copies. xid 107 deletes both after the queued removal of
    # division 1. Only the first delete consumes a tracked write.
    effects =
      splice(
        handler,
        [insert(row, @keyless)],
        [delete(row, @keyless), delete(row, @keyless)],
        1,
        [
          row,
          row
        ]
      )

    assert replay(handler, effects, [], [row, row]) == MapSet.new()
  end

  test "a held keyless copy can change key after its identical copy is deleted" do
    handler = new_handler(@keyless, [2])
    held = employee(20, 2, "away")
    renamed = %{held | "name" => "kept"}

    # The move-in query excludes both copies since division 2 was already in
    # the view. The buffered insert is excluded by the queued removal.
    effects =
      splice(
        handler,
        [insert(held, @keyless)],
        [delete(held, @keyless), update(held, renamed, @keyless)],
        2,
        []
      )

    assert replay(handler, effects, [held], []) == MapSet.new([renamed])
  end

  defp splice(handler, pre_changes, post_changes, removed_division, snapshot_rows) do
    [dep_handle] = handler.shape_info.shape.shape_dependencies_handles

    assert {:ok, %Buffering{} = buffering, _effects} =
             EventHandler.handle_event(
               handler,
               {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], txids: [103]}}
             )

    events = [
      txn(104, pre_changes),
      {:pg_snapshot_known, {105, 105, []}},
      {:materializer_changes, dep_handle,
       %{move_out: [{removed_division, to_string(removed_division)}], txids: [106]}},
      txn(107, post_changes),
      {:query_move_in_complete, "move-in-snapshot", length(snapshot_rows), 0, lsn(105), nil}
    ]

    {handler, effects} =
      Enum.reduce(events, {buffering, []}, fn event, {state, acc} ->
        assert {:ok, state, effects} = EventHandler.handle_event(state, event)
        {state, acc ++ effects}
      end)

    assert %Steady{} = handler
    effects
  end

  defp replay(handler, effects, initial_rows, snapshot_rows) do
    shape_info = handler.shape_info
    snapshot_views = Map.update!(handler.views, @ref, &MapSet.put(&1, 1))

    initial =
      Map.new(initial_rows, &snapshot_entry(&1, shape_info, handler.views))

    snapshot_rows = Enum.map(snapshot_rows, &snapshot_entry(&1, shape_info, snapshot_views))
    log = client_log(effects, snapshot_rows)

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

  defp operation(%Changes.NewRecord{} = change),
    do: [{:insert, change.key, entry(change)}]

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

  defp new_handler(pk, divisions) do
    id = if pk == @pk, do: @id, else: %{@id | pk_position: nil}

    inspector =
      Support.StubInspector.new(%{
        @relation => [id, @division, @name],
        {"public", "divisions"} => [@id, %{name: "active", type: "bool", type_id: {16, 1}}]
      })

    shape =
      Shape.new!("employees",
        where:
          ~s|name = 'kept' OR "divisionId" IN (SELECT id FROM public.divisions WHERE active = false)|,
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
