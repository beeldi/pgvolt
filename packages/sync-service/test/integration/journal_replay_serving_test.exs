defmodule Electric.Integration.JournalReplayServingTest do
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils, only: [activate_mocks_for_descendant_procs: 1, wait_until: 1]

  alias Electric.Connection.Manager
  alias Electric.LsnTracker
  alias Electric.Plug.Router
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TxnJournal
  alias Electric.ShapeCache
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes.Api
  alias Electric.Shapes.ConsumerRegistry
  alias Electric.StatusMonitor

  @moduletag :tmp_dir
  @shape_path "/v1/shape?table=items"

  setup [:with_unique_db, :with_basic_tables]

  setup do
    previous = Application.get_env(:electric, :ack_after_journal)
    Application.put_env(:electric, :ack_after_journal, true)
    on_exit(fn -> Application.put_env(:electric, :ack_after_journal, previous) end)

    %{replication_opts_overrides: [slot_temporary?: false]}
  end

  setup :with_complete_stack

  test "defers requests after lock acquisition and before the collector starts", ctx do
    assert_serving_waits_for_replay(ctx, :before_collector_start)
  end

  test "defers requests at active startup until the journal tail reaches the shape", ctx do
    assert_serving_waits_for_replay(ctx, :during_replay)
  end

  test "serves persisted shapes while another connection holds the lock over a journal tail",
       ctx do
    {handle, offset, lsn} = journal_a_row(ctx)

    restart_with_replay_paused(ctx, :before_collector_start, fn ->
      assert %{status: 202, resp_body: ~s({"status":"waiting"})} =
               get(ctx, "/v1/health")

      assert %{status: 200, resp_body: ~s({"status":"waiting"})} =
               get(ctx, "/v1/health?require=read_only")

      assert %{status: 200, resp_body: body} =
               snapshot = get_shape(ctx, "-1&handle=" <> handle, stack_ready_timeout: 0)

      assert [^handle] = Plug.Conn.get_resp_header(snapshot, "electric-handle")

      assert [
               %{"value" => %{"value" => "persisted"}},
               %{"headers" => %{"control" => "snapshot-end"}}
             ] = Jason.decode!(body)

      assert %{status: 200, resp_body: body} =
               get_shape(ctx, offset <> "&handle=" <> handle, stack_ready_timeout: 0)

      assert [%{"headers" => %{"control" => "up-to-date", "global_last_seen_lsn" => seen_lsn}}] =
               Jason.decode!(body)

      assert {:ok, persisted_offset} = LogOffset.from_string(offset)
      assert String.to_integer(seen_lsn) == persisted_offset.tx_offset
      assert String.to_integer(seen_lsn) < Lsn.to_integer(lsn)
    end)

    assert_replay_hold(ctx, :before_collector_start, handle, offset, lsn)
  end

  test "defers requests if a journal generation arrives while the stack waits for the lock",
       ctx do
    {handle, offset, lsn} = journal_a_row(ctx)
    supervisor_id = brutally_stop_complete_stack(ctx)
    base_path = Path.join(ctx.tmp_dir, ctx.stack_id)
    assert [_ | _] = TxnJournal.list_generations(base_path)
    [journal_dir | _] = TxnJournal.base_path_names()
    journal_path = Path.join(base_path, journal_dir)
    held_path = Path.join(ctx.tmp_dir, journal_dir)
    File.rename!(journal_path, held_path)

    admitted_request =
      start_with_lock_held(ctx, supervisor_id, :before_collector_start, fn ->
        assert [] == TxnJournal.list_generations(base_path)
        File.rename!(held_path, journal_path)

        api = build_router_opts(ctx)[:api]

        assert {:ok, %{read_only?: true} = request} =
                 Api.validate_params(api, %{table: "items", offset: offset, handle: handle})

        request
      end)

    assert_replay_hold(ctx, :before_collector_start, handle, offset, lsn, admitted_request)
  end

  # The client is ahead of the disk, so the request waits for the shape to reach its offset, and
  # the replay gets there with the tail still to come.
  test "a request admitted ahead of the disk while waiting for the lock reports no LSN past the journal tail",
       ctx do
    {ctx, handle, seen, tail} = journal_behind_writer(ctx)

    assert_replay_watermark(ctx, handle, seen, tail, fn ->
      get_shape(ctx, seen <> "&handle=" <> handle, long_poll_timeout: 8_000)
    end)
  end

  defp assert_serving_waits_for_replay(ctx, pause_at) do
    {handle, offset, lsn} = journal_a_row(ctx)
    restart_with_replay_paused(ctx, pause_at)
    assert_replay_hold(ctx, pause_at, handle, offset, lsn)
  end

  defp journal_a_row(ctx) do
    %{stack_id: stack_id, db_conn: db_conn} = ctx
    assert :ok = StatusMonitor.wait_until_active(stack_id, timeout: 5_000)
    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'persisted')", [])
    assert %{status: 200} = snapshot = get_shape(ctx, "-1")
    [handle] = Plug.Conn.get_resp_header(snapshot, "electric-handle")
    [offset] = Plug.Conn.get_resp_header(snapshot, "electric-offset")
    parent = self()

    # The client journals the commit and sends its slot ack even if delivery
    # to the collector never answers.
    Repatch.patch(ShapeLogCollector, :handle_event_async, [mode: :shared], fn
      %TransactionFragment{commit: commit, lsn: lsn}, ^stack_id when not is_nil(commit) ->
        send(parent, {:journaled, lsn})
        make_ref()

      event, id ->
        Repatch.real(ShapeLogCollector.handle_event_async(event, id))
    end)

    Repatch.allow(self(), GenServer.whereis(ReplicationClient.name(stack_id)))
    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'journaled')", [])
    assert_receive {:journaled, lsn}
    assert wait_until(fn -> slot_past?(db_conn, lsn) end)
    assert {_, %{base_path: base_path}} = Storage.for_stack(stack_id)
    assert [_ | _] = TxnJournal.list_generations(base_path)
    {handle, offset, lsn}
  end

  # A client has seen a change that the writer still buffers, and a later transaction is only in
  # the journal. After a stop, the replay delivers the buffered change again, then the tail.
  defp journal_behind_writer(ctx) do
    %{stack_id: stack_id, db_conn: db_conn} = ctx
    supervisor_id = brutally_stop_complete_stack(ctx)
    {mod, opts} = ctx.storage
    ctx = %{ctx | storage: {mod, Keyword.put(opts, :flush_period, 60_000)}}

    pid =
      start_stack_supervisor!(
        ctx,
        stack_id,
        ctx.persistent_kv,
        ctx.storage,
        ctx.stack_events_registry,
        ctx.publication_name,
        id: supervisor_id
      )

    ctx = %{ctx | stack_supervisor: pid}
    assert :ok = StatusMonitor.wait_until_active(stack_id, timeout: 5_000)

    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'initial')", [])
    assert %{status: 200} = snapshot = get_shape(ctx, "-1")
    [handle] = Plug.Conn.get_resp_header(snapshot, "electric-handle")
    [initial] = Plug.Conn.get_resp_header(snapshot, "electric-offset")

    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'buffered')", [])

    assert wait_until(fn ->
             response = get_shape(ctx, initial <> "&handle=" <> handle)

             if response.status == 200 and String.contains?(response.resp_body, "buffered") do
               send(self(), {:seen, Plug.Conn.get_resp_header(response, "electric-offset")})
             end
           end)

    assert_receive {:seen, [seen]}
    assert {:ok, seen_offset} = LogOffset.from_string(seen)
    assert {:ok, disk_offset} = ShapeCache.fetch_latest_offset(stack_id, handle, read_only?: true)
    assert LogOffset.compare(disk_offset, seen_offset) == :lt

    parent = self()

    Repatch.patch(ShapeLogCollector, :handle_event_async, [mode: :shared], fn
      %TransactionFragment{commit: commit, lsn: lsn}, ^stack_id when not is_nil(commit) ->
        send(parent, {:journaled, lsn})
        make_ref()

      event, id ->
        Repatch.real(ShapeLogCollector.handle_event_async(event, id))
    end)

    Repatch.allow(self(), GenServer.whereis(ReplicationClient.name(stack_id)))
    Postgrex.query!(db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'tail')", [])
    assert_receive {:journaled, tail}
    assert wait_until(fn -> slot_past?(db_conn, tail) end)

    {ctx, handle, seen, tail}
  end

  # The request is admitted read-only while another connection holds the lock, and answers once
  # the replay has handed the buffered change to the shape's consumer, before the tail.
  defp assert_replay_watermark(ctx, handle, seen, tail, request) do
    %{stack_id: stack_id} = ctx
    {:ok, %LogOffset{tx_offset: seen_lsn}} = LogOffset.from_string(seen)
    parent = self()
    activate_mocks_for_descendant_procs(ShapeLogCollector)

    Repatch.patch(ConsumerRegistry, :broadcast, [mode: :shared], fn events ->
      result = Repatch.real(ConsumerRegistry.broadcast(events))

      if Enum.any?(events, &delivers_commit?(&1, handle, seen_lsn)) do
        send(parent, {:replayed_buffered, self()})

        receive do
          :replay_tail -> :ok
        after
          30_000 -> :ok
        end
      end

      result
    end)

    supervisor_id = brutally_stop_complete_stack(ctx)
    registry = Electric.StackSupervisor.registry_name(stack_id)

    task =
      start_with_lock_held(ctx, supervisor_id, :before_collector_start, fn ->
        task = Task.async(request)

        assert wait_until(fn ->
                 Enum.any?(Registry.lookup(registry, handle), fn {pid, _} -> pid == task.pid end)
               end)

        task
      end)

    assert_receive {:replay_paused, manager}
    send(manager, :resume_replay)
    assert_receive {:replayed_buffered, collector}

    try do
      assert StatusMonitor.status(stack_id).shape == :starting
      assert %{status: 200, resp_body: body} = Task.await(task)

      assert [%{"headers" => %{"control" => "up-to-date", "global_last_seen_lsn" => reported}}] =
               Jason.decode!(body)

      assert String.to_integer(reported) == seen_lsn
      assert seen_lsn < Lsn.to_integer(tail)
    after
      send(collector, :replay_tail)
    end

    assert :ok = StatusMonitor.wait_until_active(stack_id, timeout: 5_000)
  end

  defp delivers_commit?(
         {handle, {:handle_event, %TransactionFragment{} = fragment, _}, _},
         handle,
         lsn
       ),
       do: not is_nil(fragment.commit) and fragment.last_log_offset.tx_offset == lsn

  defp delivers_commit?(_event, _handle, _lsn), do: false

  defp assert_replay_hold(ctx, pause_at, handle, offset, lsn, admitted_request \\ nil) do
    %{stack_id: stack_id} = ctx
    assert_receive {:replay_paused, paused_process}
    resume_from = offset <> "&handle=" <> handle

    {early_response, read_only_request} =
      try do
        if pause_at == :during_replay do
          assert wait_until(fn -> StatusMonitor.status(stack_id).conn == :up end)
        end

        assert %Lsn{} = seeded_lsn = LsnTracker.get_last_processed_lsn(stack_id)
        assert Lsn.compare(seeded_lsn, lsn) != :lt

        {get_shape(ctx, resume_from, stack_ready_timeout: 0),
         admitted_request && Api.load_shape_info(admitted_request)}
      after
        send(paused_process, :resume_replay)
      end

    assert_replayed_row(ctx, resume_from)

    assert early_response.status == 503,
           "expected the boot request to retry, got #{early_response.status} #{early_response.resp_body}"

    if admitted_request do
      assert {:ok, %{last_offset: %LogOffset{tx_offset: persisted_lsn}} = request} =
               read_only_request

      assert request.global_last_seen_lsn == persisted_lsn
      assert persisted_lsn < Lsn.to_integer(lsn)
    end
  end

  defp assert_replayed_row(ctx, resume_from) do
    %{stack_id: stack_id} = ctx
    assert :ok = StatusMonitor.wait_until_active(stack_id, timeout: 5_000)
    assert {:ok, _offset} = ShapeLogCollector.last_processed_offset(stack_id)

    assert %{status: 200, resp_body: body} = get_shape(ctx, resume_from)

    assert [%{"value" => %{"value" => "journaled"}}, %{"headers" => %{"control" => "up-to-date"}}] =
             Jason.decode!(body)
  end

  defp restart_with_replay_paused(ctx, pause_at, while_waiting \\ nil) do
    supervisor_id = brutally_stop_complete_stack(ctx)
    start_with_lock_held(ctx, supervisor_id, pause_at, while_waiting)
  end

  defp start_with_lock_held(ctx, supervisor_id, pause_at, while_waiting) do
    %{stack_id: stack_id, db_conn: db_conn} = ctx
    pause_replay(stack_id, pause_at)

    %{rows: [[slot_name]]} =
      Postgrex.query!(
        db_conn,
        "SELECT slot_name FROM pg_replication_slots WHERE database = current_database()",
        []
      )

    # Keep the connection manager behind the lock until the startup patch is installed in it.
    DBConnection.run(db_conn, fn conn ->
      Postgrex.query!(conn, "SELECT pg_advisory_lock(hashtext($1))", [slot_name])

      try do
        start_stack_supervisor!(
          ctx,
          stack_id,
          ctx.persistent_kv,
          ctx.storage,
          ctx.stack_events_registry,
          ctx.publication_name,
          id: supervisor_id,
          await_ready?: false
        )

        assert wait_until(fn -> GenServer.whereis(Manager.name(stack_id)) != nil end)
        Repatch.allow(self(), GenServer.whereis(Manager.name(stack_id)))

        if while_waiting do
          assert wait_until(fn ->
                   %{rows: [[waiting?]]} =
                     Postgrex.query!(
                       conn,
                       "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND objid = hashtext($1)::oid)",
                       [slot_name]
                     )

                   waiting?
                 end)

          owner = Electric.ShapeCache.ShapeStatusOwner.name(stack_id)
          :sys.get_state(owner)
          StatusMonitor.wait_for_messages_to_be_processed(stack_id)
          while_waiting.()
        end
      after
        Postgrex.query!(conn, "SELECT pg_advisory_unlock(hashtext($1))", [slot_name])
      end
    end)
  end

  defp pause_replay(stack_id, :before_collector_start) do
    parent = self()

    Repatch.patch(Manager, :handle_continue, [mode: :shared], fn
      :start_shapes_supervisor = step, %{stack_id: ^stack_id} = state ->
        wait_for_resume(parent)
        Repatch.real(Manager.handle_continue(step, state))

      step, state ->
        Repatch.real(Manager.handle_continue(step, state))
    end)
  end

  defp pause_replay(stack_id, :during_replay) do
    parent = self()
    activate_mocks_for_descendant_procs(ShapeLogCollector)

    Repatch.patch(ShapeLogCollector, :handle_continue, [mode: :shared], fn
      {:replay_journal, _slot_offset} = step, %{stack_id: ^stack_id} = state ->
        wait_for_resume(parent)
        Repatch.real(ShapeLogCollector.handle_continue(step, state))

      step, state ->
        Repatch.real(ShapeLogCollector.handle_continue(step, state))
    end)
  end

  defp wait_for_resume(parent) do
    send(parent, {:replay_paused, self()})
    assert_receive :resume_replay
  end

  defp slot_past?(db_conn, lsn) do
    %{rows: [[confirmed]]} =
      Postgrex.query!(
        db_conn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE database = current_database()",
        []
      )

    Lsn.compare(confirmed, lsn) == :gt
  end

  defp get_shape(ctx, offset, overrides \\ []) do
    get(ctx, @shape_path <> "&offset=" <> offset, overrides)
  end

  defp get(ctx, path, overrides \\ []) do
    Router.call(
      Plug.Test.conn(:get, path),
      Router.init(build_router_opts(ctx, overrides))
    )
  end
end
