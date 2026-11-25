defmodule Electric.Integration.ReplicationFragmentRestartTest do
  # Replays the counterexamples of the model in fork/tla/replication-flush (bug-midtxn,
  # bug-held-exit): the shape log collector exits while the replication connection stays up. A
  # shape subsystem started again in place lost what its collector and consumers had not
  # committed, and Postgres resends that only to a new connection. Each test asserts what a client
  # sees once the service has delivered every transaction up to a later one, whether the subsystem
  # went on or the service stopped and was started again on its storage and slot.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils, only: [wait_until: 2]

  alias Electric.LsnTracker
  alias Electric.Plug.Router
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector

  @moduletag :tmp_dir

  setup :with_unique_db
  setup :with_basic_tables

  setup do
    %{replication_opts_overrides: [max_batch_size: 2, slot_temporary?: false]}
  end

  setup :with_complete_stack

  @shape_path "/v1/shape?table=items&where=" <> URI.encode_www_form("value = 'included'")

  test "a collector exit mid-transaction keeps the first fragment's change", ctx do
    %{db_conn: db_conn, stack_id: stack_id} = ctx
    {handle, offset} = shape_snapshot(ctx)

    collector = GenServer.whereis(ShapeLogCollector.name(stack_id))
    monitor = Process.monitor(collector)
    parent = self()

    # The first collector takes and answers the fragments before the commit; the commit fragment
    # is queued in its mailbox and the collector killed before it handles it, so the replication
    # client retries that fragment alone.
    Repatch.patch(ShapeLogCollector, :handle_event_async, [mode: :shared], fn
      %TransactionFragment{} = fragment, ^stack_id ->
        first_collector? = GenServer.whereis(ShapeLogCollector.name(stack_id)) == collector

        cond do
          first_collector? and is_nil(fragment.commit) ->
            ref = Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
            Process.put(:fragment_ref, ref)
            send(parent, {:fragment, fragment.has_begin?, values(fragment)})
            ref

          first_collector? ->
            # A pipelined client sends the commit before the collector has answered the earlier
            # fragments: wait until their changes have gone to the consumers.
            true =
              wait_until(
                fn -> answered?(self(), Process.get(:fragment_ref), collector) end,
                5_000
              )

            :ok = :sys.suspend(collector)
            ref = Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
            Process.exit(collector, :kill)
            send(parent, {:interrupted_commit, values(fragment)})
            ref

          true ->
            Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
        end

      event, id ->
        Repatch.real(ShapeLogCollector.handle_event_async(event, id))
    end)

    client = allow_replication_client(stack_id)

    Postgrex.query!(
      db_conn,
      "INSERT INTO items VALUES (gen_random_uuid(), 'included'), (gen_random_uuid(), 'excluded')",
      []
    )

    # The matching row went to the first collector in the transaction's first fragment, and the
    # commit fragment the client retries does not carry it.
    assert_receive {:fragment, true, ["included"]}, 5_000
    assert_receive {:interrupted_commit, commit_values}, 5_000
    refute "included" in commit_values
    assert_receive {:DOWN, ^monitor, :process, ^collector, :killed}, 5_000

    {ctx, _} = await_processed(ctx, client, later_transaction(db_conn))

    assert_change_or_refetch(ctx, handle, offset)
  end

  # The collector exits once it has answered the first fragment, and the commit fragment is
  # dispatched once the next collector is ready, if one starts, so no call of the client is in
  # flight when the collector exits.
  test "a collector exit the client does not see keeps the first fragment's change", ctx do
    %{db_conn: db_conn, stack_id: stack_id} = ctx
    {handle, offset} = shape_snapshot(ctx)

    collector = GenServer.whereis(ShapeLogCollector.name(stack_id))
    collector_monitor = Process.monitor(collector)
    parent = self()

    Repatch.patch(ShapeLogCollector, :handle_event_async, [mode: :shared], fn
      %TransactionFragment{} = fragment, ^stack_id ->
        first_collector? = GenServer.whereis(ShapeLogCollector.name(stack_id)) == collector

        cond do
          first_collector? and is_nil(fragment.commit) ->
            ref = Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
            Process.put(:fragment_ref, ref)
            send(parent, {:fragment, fragment.has_begin?, values(fragment)})
            ref

          first_collector? ->
            # The collector has answered the first fragment: the client read the answer and
            # dropped its monitor, or the answer waits in its mailbox and the client drops the
            # monitor, with the exit's DOWN, as it reads it. No call is left in flight.
            true =
              wait_until(
                fn -> answered?(self(), Process.get(:fragment_ref), collector) end,
                5_000
              )

            monitor = Process.monitor(collector)
            Process.exit(collector, :kill)
            assert_receive {:DOWN, ^monitor, :process, ^collector, :killed}, 5_000

            # A subsystem that starts again in place takes the commit fragment; one that stops
            # takes this process with it while it waits.
            if wait_until(fn -> next_collector_ready?(stack_id, collector) end, 15_000) do
              send(parent, {:commit_after_restart, values(fragment)})
            end

            Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))

          true ->
            Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
        end

      event, id ->
        Repatch.real(ShapeLogCollector.handle_event_async(event, id))
    end)

    client = allow_replication_client(stack_id)

    Postgrex.query!(
      db_conn,
      "INSERT INTO items VALUES (gen_random_uuid(), 'included'), (gen_random_uuid(), 'excluded')",
      []
    )

    assert_receive {:fragment, true, ["included"]}, 5_000
    assert_receive {:DOWN, ^collector_monitor, :process, ^collector, :killed}, 10_000

    case await_processed(ctx, client, later_transaction(db_conn)) do
      # The same client went on with the next collector: it sent that one the commit fragment,
      # which does not carry the matching row.
      {ctx, :went_on} ->
        assert_received {:commit_after_restart, commit_values}
        refute "included" in commit_values
        assert_change_or_refetch(ctx, handle, offset)

      {ctx, :restarted} ->
        refute_received {:commit_after_restart, _}
        assert_change_or_refetch(ctx, handle, offset)
    end
  end

  # The shape's consumer exits as a teardown would (killed) while the collector delivers a
  # transaction, the collector keeps its flush entry pinned, holds the shape for the stall check
  # and moves the processed position past the transaction, then exits before the check rules.
  test "a held consumer and a collector exit before the stall check keep the change", ctx do
    %{db_conn: db_conn, stack_id: stack_id} = ctx
    {handle, offset} = shape_snapshot(ctx)

    collector = GenServer.whereis(ShapeLogCollector.name(stack_id))
    consumer = Electric.Shapes.Consumer.whereis(stack_id, handle)
    assert is_pid(consumer)
    consumer_monitor = Process.monitor(consumer)
    parent = self()

    Repatch.patch(ShapeLogCollector, :handle_event_async, [mode: :shared], fn
      %TransactionFragment{commit: commit} = fragment, ^stack_id when not is_nil(commit) ->
        if Process.alive?(consumer) do
          :ok = :sys.suspend(consumer)
          ref = Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
          send(parent, {:delivering, fragment.lsn, ref})
          ref
        else
          Repatch.real(ShapeLogCollector.handle_event_async(fragment, stack_id))
        end

      event, id ->
        Repatch.real(ShapeLogCollector.handle_event_async(event, id))
    end)

    client = allow_replication_client(stack_id)

    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'included')", [])

    assert_receive {:delivering, lsn, ref}, 5_000

    # The event waits in the suspended consumer's mailbox, so the collector is awaiting it.
    assert wait_until(
             fn ->
               match?(
                 {:message_queue_len, n} when n > 0,
                 Process.info(consumer, :message_queue_len)
               )
             end,
             5_000
           )

    Process.exit(consumer, :kill)
    assert_receive {:DOWN, ^consumer_monitor, :process, ^consumer, :killed}, 5_000

    # The collector holds the shape for the stall check, pins the flush entry and moves the
    # processed position past the transaction, then answers the client.
    assert wait_until(fn -> answered?(client, ref, collector) end, 15_000)
    assert LsnTracker.get_last_processed_lsn(stack_id) == lsn

    collector_monitor = Process.monitor(collector)
    Process.exit(collector, :kill)
    assert_receive {:DOWN, ^collector_monitor, :process, ^collector, :killed}, 5_000

    {ctx, _} = await_processed(ctx, client, later_transaction(db_conn))

    assert_change_or_refetch(ctx, handle, offset)
  end

  defp shape_snapshot(ctx) do
    snapshot = Router.call(Plug.Test.conn(:get, @shape_path <> "&offset=-1"), router(ctx))
    assert snapshot.status == 200
    [handle] = Plug.Conn.get_resp_header(snapshot, "electric-handle")
    [offset] = Plug.Conn.get_resp_header(snapshot, "electric-offset")
    {handle, offset}
  end

  # The client resuming the shape gets the matching row, or is told to refetch and then gets it
  # in the new snapshot.
  defp assert_change_or_refetch(ctx, handle, offset) do
    response =
      Router.call(
        Plug.Test.conn(:get, @shape_path <> "&handle=#{handle}&offset=#{offset}"),
        router(ctx)
      )

    response =
      if response.status == 409,
        do: Router.call(Plug.Test.conn(:get, @shape_path <> "&offset=-1"), router(ctx)),
        else: response

    assert response.status == 200 and response.resp_body =~ "\"value\":\"included\"",
           "expected the matching change, got #{response.status} #{response.resp_body}"
  end

  defp router(ctx), do: Router.init(build_router_opts(ctx))

  defp allow_replication_client(stack_id) do
    replication_client = GenServer.whereis(ReplicationClient.name(stack_id))
    Repatch.allow(self(), replication_client)
    replication_client
  end

  # Whether the collector has answered the call `ref` that `client` made: the answer waits in the
  # client's mailbox, or the client has read it and holds no call to the collector open.
  defp answered?(client, ref, collector) do
    {:messages, messages} = Process.info(client, :messages)
    {:monitors, monitors} = Process.info(client, :monitors)
    Enum.any?(messages, &match?({^ref, _}, &1)) or {:process, collector} not in monitors
  end

  # Commits a transaction no shape matches and returns a position before its commit: once the
  # service has delivered that position, it has delivered every transaction committed before it.
  defp later_transaction(db_conn) do
    %{rows: [[position]]} = Postgrex.query!(db_conn, "SELECT pg_current_wal_lsn()::text", [])
    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'excluded')", [])
    Lsn.from_string(position)
  end

  # Waits until the running stack has delivered `position`, and returns `:went_on`. A stack that
  # stops on its own instead is started again on the same storage and slot, as what runs the
  # service would, and `:restarted` is returned once the new stack, with a new replication
  # client, has delivered it.
  defp await_processed(ctx, client, position) do
    stack_supervisor = ctx.stack_supervisor
    monitor = Process.monitor(stack_supervisor)

    case await_processed_or_down(ctx.stack_id, position, monitor, 20_000) do
      :processed ->
        Process.demonitor(monitor, [:flush])
        {ctx, :went_on}

      :stopped ->
        refute Process.alive?(client)
        ctx = Map.merge(ctx, brutally_restart_complete_stack(ctx))

        assert wait_until(fn -> processed?(ctx.stack_id, position) end, 20_000),
               "the restarted stack did not process #{position}"

        new_client = GenServer.whereis(ReplicationClient.name(ctx.stack_id))
        assert is_pid(new_client) and new_client != client
        {ctx, :restarted}
    end
  end

  defp await_processed_or_down(stack_id, position, monitor, time_left) do
    cond do
      processed?(stack_id, position) ->
        :processed

      time_left <= 0 ->
        flunk("the stack neither processed #{position} nor stopped")

      true ->
        receive do
          {:DOWN, ^monitor, :process, _pid, _reason} -> :stopped
        after
          50 -> await_processed_or_down(stack_id, position, monitor, time_left - 50)
        end
    end
  end

  # The collector's own position, which it moves once it has delivered a transaction to the
  # consumers. A boot that replays the transaction journal publishes the tracker's position
  # before the replay delivers, so the tracker cannot tell here.
  defp processed?(stack_id, position) do
    case ShapeLogCollector.last_processed_offset(stack_id) do
      {:ok, %LogOffset{tx_offset: tx_offset}} -> tx_offset >= Lsn.to_integer(position)
      _ -> false
    end
  rescue
    # No registry between the stop of one stack and the start of the next.
    ArgumentError -> false
  end

  defp next_collector_ready?(stack_id, collector) do
    case GenServer.whereis(ShapeLogCollector.name(stack_id)) do
      pid when is_pid(pid) and pid != collector ->
        Electric.StatusMonitor.status(stack_id).shape == :up

      _ ->
        false
    end
  end

  defp values(%TransactionFragment{changes: changes}),
    do: Enum.map(changes, &Map.get(&1.record, "value"))
end
