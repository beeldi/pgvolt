defmodule Electric.Plug.RouterSlotPurgeTest do
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Plug.Test
  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  alias Electric.Plug.Router
  alias Electric.Postgres.ReplicationClient
  alias Electric.Postgres.ReplicationClient.SlotIncarnation
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.ShapeCache.ShapeStatusOwner
  alias Electric.StackSupervisor
  alias Electric.StatusMonitor

  @moduletag :tmp_dir
  @snapshot_path "/v1/shape?table=items&offset=-1"

  setup [:with_unique_db, :with_basic_tables]

  setup ctx do
    slot_name = "router_slot_purge_#{:erlang.phash2(ctx.test)}"

    %{
      slot_name: slot_name,
      replication_opts_overrides: [slot_name: slot_name, slot_temporary?: false]
    }
  end

  setup :with_complete_stack

  setup ctx do
    :ok = wait_until_active(ctx.stack_id)
    Postgrex.query!(ctx.db_conn, "INSERT INTO items VALUES (gen_random_uuid(), 'saved')", [])
    opts = Router.init(build_router_opts(ctx))
    snapshot = Router.call(conn("GET", @snapshot_path), opts)

    assert %{status: 200} = snapshot
    assert [%{"value" => %{"value" => "saved"}}, _] = Jason.decode!(snapshot.resp_body)
    [handle] = Plug.Conn.get_resp_header(snapshot, "electric-handle")

    assert Support.TestUtils.wait_until(fn ->
             :ok = WriteBuffer.flush_sync(ctx.stack_id)
             ShapeDb.snapshot_complete?(ctx.stack_id, handle)
           end)

    :ok = stop_supervised!(StackSupervisor)

    %{router_opts: opts, snapshot: snapshot}
  end

  test "serves the stored snapshot while waiting for the lock after a provisional intent",
       %{persistent_kv: kv, stack_id: stack_id, router_opts: opts} = ctx do
    take_lock(ctx)
    prepared = SlotIncarnation.prepare(kv, stack_id, ctx.slot_name, :required)

    start_waiting_stack(ctx)
    check_purge(stack_id)
    SlotIncarnation.settle(kv, stack_id, prepared, :existing)
    check_purge(stack_id)

    assert %{status: 202, resp_body: health} = Router.call(conn("GET", "/v1/health"), opts)
    assert Jason.decode!(health) == %{"status" => "waiting"}

    assert %{status: 200, resp_body: body} = Router.call(conn("GET", @snapshot_path), opts)
    assert Jason.decode!(body) == Jason.decode!(ctx.snapshot.resp_body)
  end

  test "stops serving the stored snapshot when the lock holder confirms a purge",
       %{persistent_kv: kv, stack_id: stack_id, router_opts: opts} = ctx do
    take_lock(ctx)
    prepared = SlotIncarnation.prepare(kv, stack_id, ctx.slot_name, :required)

    start_waiting_stack(ctx)

    assert %{status: 200, resp_body: body} = Router.call(conn("GET", @snapshot_path), opts)
    assert Jason.decode!(body) == Jason.decode!(ctx.snapshot.resp_body)

    SlotIncarnation.settle(kv, stack_id, prepared, :created)

    # The waiting instance reads the shared record once a second. A shape request waits for the
    # stack up to its readiness timeout before it answers 503.
    assert Support.TestUtils.wait_until(fn ->
             StatusMonitor.service_status(stack_id) == :starting
           end)

    assert %{status: 503} = response = Router.call(conn("GET", @snapshot_path), opts)
    assert [retry_after] = Plug.Conn.get_resp_header(response, "retry-after")
    assert String.to_integer(retry_after) in 5..10
    assert Plug.Conn.get_resp_header(response, "electric-handle") == []

    assert %{status: 202, resp_body: health} =
             Router.call(conn("GET", "/v1/health?require=read_only"), opts)

    assert Jason.decode!(health) == %{"status" => "starting"}
  end

  # The check reads the status the monitor last stored, which can be older than the lock: the
  # monitor handles the cast of the lock after the manager has refreshed the metadata. A close
  # handled after the lock outlasted the reopen of the manager that took it.
  test "leaves the metadata open when the monitor records the lock before the check's close",
       %{persistent_kv: kv, stack_id: stack_id} = ctx do
    take_lock(ctx)
    prepared = SlotIncarnation.prepare(kv, stack_id, ctx.slot_name, :required)
    start_waiting_stack(ctx)

    assert Support.TestUtils.wait_until(fn ->
             StatusMonitor.service_status(stack_id) == :waiting
           end)

    monitor = GenServer.whereis(StatusMonitor.name(stack_id))
    owner = GenServer.whereis(ShapeStatusOwner.name(stack_id))
    await_purge_check(owner)
    :ok = :sys.suspend(monitor)

    try do
      StatusMonitor.mark_pg_lock_acquired(stack_id, self())
      SlotIncarnation.settle(kv, stack_id, prepared, :created)
      send(owner, :check_purge)

      assert Support.TestUtils.wait_until(fn ->
               {:messages, messages} = Process.info(monitor, :messages)

               Enum.any?(
                 messages,
                 &match?({:"$gen_call", _, {:close_shape_metadata_unless_locked, _}}, &1)
               )
             end)
    after
      :sys.resume(monitor)
    end

    await_purge_check(owner)
    refute StatusMonitor.shape_metadata_closed?(stack_id)
  end

  # The manager refreshes the metadata in the owner once it takes the lock. A read of the shared
  # record that the check makes meanwhile can be slow on a shared volume, and the refresh does not
  # wait for it.
  test "takes the lock while the check's read of the shared record is slow",
       %{db_conn: db, stack_id: stack_id} = ctx do
    take_lock(ctx)
    start_waiting_stack(ctx)
    owner = GenServer.whereis(ShapeStatusOwner.name(stack_id))
    await_purge_check(owner)
    test_pid = self()

    # The owner runs the check in a task, and the task's callers are the owner's.
    :sys.replace_state(owner, fn state ->
      Process.put(:"$callers", [test_pid | Process.get(:"$callers", [])])
      state
    end)

    Repatch.patch(Electric.Timeline, :purge_confirmed?, [mode: :shared], fn opts ->
      send(test_pid, {:reading_record, self()})

      receive do
        :resume -> Repatch.real(Electric.Timeline.purge_confirmed?(opts))
      end
    end)

    send(owner, :check_purge)
    assert_receive {:reading_record, reader}

    try do
      assert true == scalar(db, "SELECT pg_advisory_unlock(hashtext($1))", [ctx.slot_name])
      assert :ok = wait_until_active(stack_id)
    after
      send(reader, :resume)
    end
  end

  # A check that outlived the owner reached the monitor by name once its read returned, and could
  # close an instance that had loaded its metadata since.
  test "stops a running check when the owner restarts", %{stack_id: stack_id} = ctx do
    take_lock(ctx)
    start_waiting_stack(ctx)
    owner = GenServer.whereis(ShapeStatusOwner.name(stack_id))
    await_purge_check(owner)
    test_pid = self()

    :sys.replace_state(owner, fn state ->
      Process.put(:"$callers", [test_pid | Process.get(:"$callers", [])])
      state
    end)

    Repatch.patch(Electric.Timeline, :purge_confirmed?, [mode: :shared], fn _opts ->
      send(test_pid, {:reading_record, self()})

      receive do
        :resume -> true
      end
    end)

    send(owner, :check_purge)
    assert_receive {:reading_record, reader}
    reader_ref = Process.monitor(reader)
    Process.exit(owner, :kill)

    assert Support.TestUtils.wait_until(fn ->
             GenServer.whereis(ShapeStatusOwner.name(stack_id)) not in [nil, owner] and
               StatusMonitor.service_status(stack_id) == :waiting
           end)

    send(reader, :resume)
    assert_receive {:DOWN, ^reader_ref, :process, ^reader, _}
    refute StatusMonitor.shape_metadata_closed?(stack_id)
  end

  test "answers requests and readiness waits while the shared record is slow to read",
       %{persistent_kv: kv, stack_id: stack_id, router_opts: opts} = ctx do
    take_lock(ctx)
    start_waiting_stack(ctx)
    check_purge(stack_id)

    # A suspended store holds every read of the record until it resumes.
    :ok = :sys.suspend(kv.pid)

    try do
      task =
        Task.async(fn ->
          {StatusMonitor.wait_until(stack_id, :active, timeout: 100),
           StatusMonitor.shape_metadata_closed?(stack_id),
           Router.call(conn("GET", "/v1/health"), opts).status,
           Router.call(conn("GET", @snapshot_path), opts).status}
        end)

      assert {{:error, _}, false, 202, 200} = Task.await(task, 2_000)
    after
      :sys.resume(kv.pid)
    end
  end

  test "keeps the saved handle after a restart stops before replication slot creation",
       %{db_conn: db, stack_id: stack_id, router_opts: opts} = ctx do
    flush_lsn = slot_flush_lsn(ctx)
    take_lock(ctx)

    parent = self()

    Repatch.patch(SlotIncarnation, :prepare, [mode: :shared], fn kv, id, slot, purge_on_create ->
      Repatch.real(SlotIncarnation.prepare(kv, id, slot, purge_on_create))
      send(parent, :slot_attempt_prepared)

      receive do
        {:EXIT, _, :shutdown} -> exit(:shutdown)
      end
    end)

    start_waiting_stack(ctx)
    Repatch.allow(parent, ReplicationClient.name(stack_id))
    assert true == scalar(db, "SELECT pg_advisory_unlock(hashtext($1))", [ctx.slot_name])
    assert_receive :slot_attempt_prepared
    :ok = stop_supervised!(StackSupervisor)
    assert slot_flush_lsn(ctx) == flush_lsn

    start_stack(ctx, await_ready?: true)
    :ok = wait_until_active(stack_id)

    [handle] = Plug.Conn.get_resp_header(ctx.snapshot, "electric-handle")
    [offset] = Plug.Conn.get_resp_header(ctx.snapshot, "electric-offset")
    path = "/v1/shape?table=items&handle=#{handle}&offset=#{offset}"
    assert Router.call(conn("GET", path), opts).status == 200
  end

  # Runs the owner's periodic check of the shared record now, and waits for it.
  defp check_purge(stack_id) do
    owner = GenServer.whereis(ShapeStatusOwner.name(stack_id))
    await_purge_check(owner)
    send(owner, :check_purge)
    await_purge_check(owner)
  end

  # Waits until no check of the shared record runs.
  defp await_purge_check(owner) do
    case Map.get(:sys.get_state(owner), :purge_check) do
      nil ->
        :ok

      {pid, _} ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}
        await_purge_check(owner)
    end
  end

  defp take_lock(%{db_conn: db, slot_name: slot_name}),
    do: Postgrex.query!(db, "SELECT pg_advisory_lock(hashtext($1))", [slot_name])

  defp slot_flush_lsn(%{db_conn: db, slot_name: slot_name}) do
    scalar(db, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", [
      slot_name
    ])
  end

  defp start_waiting_stack(ctx) do
    ref = StackSupervisor.subscribe_to_stack_events(ctx.stack_id)
    start_stack(ctx, await_ready?: false)
    assert_receive {:stack_status, ^ref, :waiting_for_connection_lock}
  end

  defp start_stack(ctx, opts) do
    start_stack_supervisor!(
      ctx,
      ctx.stack_id,
      ctx.persistent_kv,
      ctx.storage,
      ctx.stack_events_registry,
      ctx.publication_name,
      opts
    )
  end

  defp wait_until_active(stack_id) do
    StatusMonitor.wait_until_active(stack_id,
      timeout: ExUnit.configuration()[:assert_receive_timeout]
    )
  end
end
