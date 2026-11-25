defmodule Electric.Shapes.Consumer.EventHandler.QueuedRemovalRoutingGapTest do
  # The consumer side of `QueuedRemovalRoutingGapTest` in `test/integration`: a move-in whose
  # snapshot leaves a written key's row out never sends an update for that row after it, even when
  # the change after the snapshot replaces another row than the one written.
  #
  # Employee 20 is in the shape through `name = 'kept'` alone. One transaction (xid 104)
  # deactivates division 1, renames the employee to 'away', then moves them to division 1.
  # Routing reads the subquery index before the consumer starts division 1's move, so the second
  # update, (away, 4) -> (away, 1), matches neither side and is dropped: the consumer receives
  # only (kept, 4) -> (away, 4). Division 1's move-in takes its snapshot after xid 104. Division
  # 1 is activated again (xid 106), its removal queues behind the move, and xid 107 renames the
  # employee back to 'kept' after the snapshot. The move's query returns employee 20 as
  # (away, 1), the row Postgres holds at its snapshot.
  use ExUnit.Case, async: true

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Transaction
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.EventHandler
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Steady
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Shape

  @ref ["$sublink", "0"]

  @inspector Support.StubInspector.new(
               tables: ["divisions", "employees"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}, not_null: true},
                 %{name: "divisionId", type: "int8", pk_position: nil, type_id: {20, 1}},
                 %{name: "name", type: "text", pk_position: nil, type_id: {25, 1}},
                 %{name: "active", type: "bool", pk_position: nil, type_id: {16, 1}}
               ]
             )

  test "a move-in never sends an update for a row its snapshot left out" do
    handler = new_handler()

    assert {:ok, %Buffering{} = handler, _effects} =
             EventHandler.handle_event(
               handler,
               {:materializer_changes, dep_handle(handler), %{move_in: [{1, "1"}], txids: [104]}}
             )

    events = [
      # Routing dropped the transaction's second update of employee 20.
      txn(104, [employee_update(20, {4, "kept"}, {4, "away"})]),
      {:pg_snapshot_known, {105, 105, []}},
      {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [106]}},
      txn(107, [employee_update(20, {1, "away"}, {1, "kept"})]),
      {:query_move_in_complete, "move-in-snapshot", 1, 100, lsn(105), nil}
    ]

    {handler, effects} =
      Enum.reduce(events, {handler, []}, fn event, {handler, acc} ->
        assert {:ok, handler, effects} = EventHandler.handle_event(handler, event)
        {handler, acc ++ effects}
      end)

    assert %Steady{} = handler

    # The move's query returns employee 20 as Postgres holds it at the snapshot.
    snapshot_rows = [{key(20), %{"id" => "20", "divisionId" => "1", "name" => "away"}}]

    log = client_log(effects, snapshot_rows)

    Enum.reduce(log, MapSet.new([key(20)]), fn
      {:insert, key}, held ->
        if MapSet.member?(held, key), do: flunk("insert of a held row #{key} in #{inspect(log)}")
        MapSet.put(held, key)

      {op, key}, held when op in [:update, :delete] ->
        if not MapSet.member?(held, key),
          do: flunk("#{op} for a row the log does not hold: #{key} in #{inspect(log)}")

        if op == :delete, do: MapSet.delete(held, key), else: held
    end)
  end

  # The operations the shape's log carries, in order, with the snapshot's rows the splice keeps.
  defp client_log(effects, snapshot_rows) do
    Enum.flat_map(effects, fn
      %Effects.AppendChanges{changes: changes} ->
        for change <- changes, do: {operation(change), change.key}

      %Effects.AppendMoveInSnapshot{} = snapshot ->
        for {key, _row} <- snapshot_rows,
            not skipped?(Map.get(snapshot, :skip_rows), key),
            do: {:insert, key}

      _ ->
        []
    end)
  end

  # Only the forms this case reaches: no skip, or `:all` for the rows of written keys.
  defp skipped?(nil, _key), do: false
  defp skipped?({:all, _written_row, _keys}, _key), do: true
  defp skipped?({nil, :all, keys}, key), do: MapSet.member?(keys, key)
  defp skipped?({nil, nil, _keys}, _key), do: false
  defp skipped?(other, _key), do: flunk("unexpected skip_rows: #{inspect(other)}")

  defp operation(%Changes.NewRecord{}), do: :insert
  defp operation(%Changes.UpdatedRecord{}), do: :update
  defp operation(%Changes.DeletedRecord{}), do: :delete

  defp new_handler do
    shape =
      Shape.new!("employees",
        where:
          ~s|name = 'kept' OR "divisionId" IN (SELECT id FROM public.divisions WHERE active = false)|,
        inspector: @inspector
      )
      |> fill_handles()

    {:ok, dnf_plan} = DnfPlan.compile(shape)
    dep_handle = hd(shape.shape_dependencies_handles)

    %Steady{
      shape_info: %ShapeInfo{
        shape: shape,
        stack_id: "stack-id",
        shape_handle: "shape-handle",
        dnf_plan: dnf_plan,
        ref_resolver: RefResolver.new(%{dep_handle => {0, @ref}}, %{0 => @ref}),
        buffer_max_transactions: 1000
      },
      views: %{@ref => MapSet.new()}
    }
  end

  defp dep_handle(handler) do
    handler.shape_info.ref_resolver.handle_to_ref |> Map.keys() |> hd()
  end

  defp fill_handles(shape) do
    filled_deps = Enum.map(shape.shape_dependencies, &fill_handles/1)
    handles = Enum.map(filled_deps, &Shape.generate_id/1)
    %{shape | shape_dependencies: filled_deps, shape_dependencies_handles: handles}
  end

  defp employee_update(id, {old_division, old_name}, {division, name}) do
    Changes.UpdatedRecord.new(
      relation: {"public", "employees"},
      old_record: %{"id" => "#{id}", "divisionId" => "#{old_division}", "name" => old_name},
      record: %{"id" => "#{id}", "divisionId" => "#{division}", "name" => name}
    )
    |> Changes.fill_key(["id"])
  end

  defp key(id) do
    %Changes.NewRecord{relation: {"public", "employees"}, record: %{"id" => "#{id}"}}
    |> Changes.fill_key(["id"])
    |> Map.fetch!(:key)
  end

  defp txn(xid, changes) do
    %Transaction{
      xid: xid,
      changes: changes,
      num_changes: length(changes),
      lsn: lsn(xid),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(xid), max(length(changes) - 1, 0))
    }
  end

  defp lsn(value), do: Lsn.from_integer(value)
end
