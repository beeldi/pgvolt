defmodule Electric.Integration.QueuedRemovalRoutingGapTest do
  # A shape `<plain condition> OR "divisionId" IN (SELECT ...)` whose move-in a change routing
  # dropped precedes, with a removal of the moved value queued behind it. The snapshot leaves the
  # employee out by its key, so the change after it must not update a row the client lacks.
  #
  # The interleaving:
  # 1. Employee 20 is in the shape through the plain disjunct only: its division 4 is active.
  # 2. One transaction deactivates division 1, sets employee 20's name so the plain disjunct
  #    fails, then moves it to division 1. Routing reads the subquery index as it is before the
  #    consumer starts division 1's move, so the second update, from (away, 4) to (away, 1),
  #    matches neither side and never reaches the consumer. The move-in of division 1 starts;
  #    its query takes its snapshot here and is held.
  # 3. Division 1 is activated again: its removal queues behind the move.
  # 4. Employee 20 gets its name back: the change reaches the consumer and is buffered after the
  #    move's snapshot.
  # 5. The query resumes, its snapshot returns employee 20 as (away, 1), and the move splices.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer
  import Support.TestUtils, only: [wait_until: 1]

  alias Electric.Client
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.Message.ControlMessage
  alias Electric.Postgres.SnapshotQuery
  alias Electric.Replication.Changes
  alias Electric.Shapes.Consumer
  alias Electric.Shapes.Consumer.Effects

  @moduletag :tmp_dir

  describe "a change routing drops before a move-in whose value leaves behind it" do
    setup [:with_unique_db, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    @tag with_sql: [
           "CREATE TABLE divisions (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
           ~s|CREATE TABLE employees (id INT PRIMARY KEY, "divisionId" INT, name TEXT NOT NULL)|,
           "INSERT INTO divisions VALUES (1, true), (4, true)",
           "INSERT INTO employees VALUES (20, 4, 'kept')"
         ]
    test "never sends an update for an employee the client does not hold",
         %{client: client, db_conn: conn, stack_id: stack_id} do
      where =
        ~s|name = 'kept' OR "divisionId" IN (SELECT id FROM divisions WHERE active = false)|

      stream =
        Client.stream(client, ShapeDefinition.new!("employees", where: where), live: true)

      with_consumer stream do
        assert_insert(consumer, %{"id" => 20})
        assert %{handle: handle} = assert_up_to_date(consumer)
        name = Consumer.name(stack_id, handle)
        gate_first_move_query(name)

        assert {:ok, _} =
                 Postgrex.transaction(conn, fn tx ->
                   Postgrex.query!(tx, "UPDATE divisions SET active = false WHERE id = 1", [])
                   Postgrex.query!(tx, "UPDATE employees SET name = 'away' WHERE id = 20", [])

                   Postgrex.query!(
                     tx,
                     ~s|UPDATE employees SET "divisionId" = 1 WHERE id = 20|,
                     []
                   )
                 end)

        assert_receive {:move_query_paused, query}, 5_000

        Postgrex.query!(conn, "UPDATE divisions SET active = true WHERE id = 1", [])
        Postgrex.query!(conn, "UPDATE employees SET name = 'kept' WHERE id = 20", [])

        assert wait_until(fn ->
                 case :sys.get_state(name).event_handler do
                   %{active_move: %{buffered_txn_count: count}, queue: queue} ->
                     count >= 2 and queue.move_out != %{}

                   _ ->
                     false
                 end
               end)

        # Routing kept only the first update of the division-1 transaction for employee 20.
        %{active_move: %{buffered_txns: buffered}} = :sys.get_state(name).event_handler

        assert [[{%{"name" => "kept"}, %{"name" => "away", "divisionId" => "4"}}], _] =
                 buffered
                 |> Enum.reverse()
                 |> Enum.map(fn txn ->
                   for %Changes.UpdatedRecord{old_record: old, record: new} <- txn.changes,
                       do: {Map.take(old, ["name"]), Map.take(new, ["name", "divisionId"])}
                 end)

        send(query, :resume_move_query)

        assert wait_until(fn ->
                 case live_handler(name) do
                   nil -> true
                   %{active_move: _} -> false
                   %{queue: queue} -> queue.move_in == %{} and queue.move_out == %{}
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO employees VALUES (99, NULL, 'kept')", [])
        operations = operations_until_insert(consumer, 99)

        held =
          Enum.reduce(operations, MapSet.new([20]), fn
            :must_refetch, _held ->
              flunk("must-refetch in #{inspect(operations)}")

            {:insert, id}, held ->
              if MapSet.member?(held, id),
                do: flunk("insert for row that already exists: #{id} in #{inspect(operations)}")

              MapSet.put(held, id)

            {op, id}, held when op in [:update, :delete] ->
              if not MapSet.member?(held, id),
                do: flunk("#{op} for row that does not exist: #{id} in #{inspect(operations)}")

              if op == :delete, do: MapSet.delete(held, id), else: held
          end)

        assert MapSet.equal?(held, MapSet.new([20, 99]))
      end
    end
  end

  defp live_handler(pid) do
    :sys.get_state(pid).event_handler
  catch
    :exit, {:noproc, _} -> nil
    :exit, {:normal, _} -> nil
    :exit, {:shutdown, _} -> nil
    :exit, {{:shutdown, _}, _} -> nil
  end

  # Holds the first move-in query once its snapshot is taken, until `:resume_move_query`.
  defp gate_first_move_query(consumer) do
    test = self()
    gate = :atomics.new(1, [])

    pause = fn ->
      send(test, {:move_query_paused, self()})

      receive do
        :resume_move_query -> :ok
      end
    end

    Repatch.patch(SnapshotQuery, :execute_for_shape, [mode: :shared], fn pool,
                                                                         handle,
                                                                         shape,
                                                                         opts ->
      opts =
        if opts[:query_reason] == "move_in_query" and
             :atomics.compare_exchange(gate, 1, 0, 1) == :ok do
          Keyword.update!(opts, :query_fn, fn query ->
            fn conn, snapshot, lsn ->
              pause.()
              query.(conn, snapshot, lsn)
            end
          end)
        else
          opts
        end

      Repatch.real(SnapshotQuery.execute_for_shape(pool, handle, shape, opts))
    end)

    Repatch.patch(Effects, :query_move_in_async, [mode: :shared], fn supervisor,
                                                                     state,
                                                                     move,
                                                                     pid ->
      callers = Process.get(:"$callers", [])
      Process.put(:"$callers", [test | callers])

      try do
        Repatch.real(Effects.query_move_in_async(supervisor, state, move, pid))
      after
        Process.put(:"$callers", callers)
      end
    end)

    Repatch.allow(test, consumer)
  end

  defp operations_until_insert(consumer, marker, operations \\ []) do
    assert {:ok, [message]} =
             await_count(consumer, 1,
               match: fn
                 %ControlMessage{control: :must_refetch} -> true
                 %ChangeMessage{} -> true
                 _ -> false
               end
             )

    case message do
      %ControlMessage{control: :must_refetch} ->
        operations_until_insert(consumer, marker, [:must_refetch | operations])

      %ChangeMessage{headers: %{operation: :insert}, value: %{"id" => ^marker}} ->
        Enum.reverse([{:insert, marker} | operations])

      %ChangeMessage{headers: %{operation: operation}, value: %{"id" => id}} ->
        operations_until_insert(consumer, marker, [{operation, id} | operations])
    end
  end
end
