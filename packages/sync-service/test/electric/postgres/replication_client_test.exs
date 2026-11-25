defmodule Electric.Postgres.ReplicationClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  import Support.ComponentSetup

  import Support.DbSetup, except: [with_publication: 1]
  import Support.DbStructureSetup

  alias Electric.Replication.LogOffset
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient

  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.ShapeLogCollector.AffectedColumns
  alias Electric.Replication.Changes.DeletedRecord
  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.UpdatedRecord
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.TransactionBuilder
  alias Electric.Replication.TxnJournal

  # Larger than average timeout for assertions that require
  # seeing changes back from the database, as it can be especially
  # slow on CI/Docker etc
  @assert_receive_db_timeout ExUnit.configuration()[:assert_receive_timeout]

  # Since Postgres 16 (bc971f4025) a walsender joins the condition variable that announces new
  # WAL only after reading the flush position, so a commit flushed in between is not streamed
  # until something else wakes it. A standby status update does: the client sends one on its
  # keepalive timer, every 15s by default, and a wait for streamed data asks for one sooner.
  @walsender_prompt_interval 500

  defmodule MockConnectionManager do
    def receive_casts(test_pid) do
      receive do
        message ->
          if response = process_message(message) do
            send(test_pid, response)
          end

          receive_casts(test_pid)
      end
    end

    defp process_message({:"$gen_cast", :replication_client_started}), do: nil
    defp process_message({:"$gen_cast", {:pg_info_obtained, _}}), do: nil
    defp process_message({:"$gen_cast", {:pg_system_identified, _}}), do: nil
    defp process_message({:"$gen_cast", :replication_client_lock_acquired}), do: :lock_acquired

    defp process_message({:"$gen_cast", {:replication_client_lock_acquisition_failed, err}}),
      do: {:lock_acquisition_failed, err}

    defp process_message({:"$gen_cast", :replication_client_created_new_slot}), do: nil

    defp process_message({:"$gen_cast", :replication_client_streamed_first_message}),
      do: {self(), :streaming_started}
  end

  # A logger handler that treats the process labelled `label` the way ExUnit's capture-log
  # handler treats every process that logs: it links a helper to that process and drains the
  # helper's exit only if the process traps exits when it logs. The helper exits once the
  # process traps exits, as the capture handler's task can when its caller runs at high priority.
  defmodule LinkedLogHelper do
    def log(_event, %{config: %{label: label, test_pid: test_pid}}) do
      if :proc_lib.get_label(self()) == label do
        logger = self()
        {:trap_exit, trapping_exits?} = Process.info(logger, :trap_exit)

        helper =
          spawn_link(fn ->
            await_trapping_exits(logger)
            send(test_pid, {:linked_log_helper_exited, self()})
          end)

        if trapping_exits?, do: receive(do: ({:EXIT, ^helper, _} -> :ok))
      end

      :ok
    end

    defp await_trapping_exits(pid) do
      if Process.info(pid, :trap_exit) == {:trap_exit, false} do
        Process.sleep(1)
        await_trapping_exits(pid)
      end
    end
  end

  defmodule MockTransactionProcessor do
    use GenServer, restart: :temporary

    alias Electric.Replication.Changes.TransactionFragment

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, test_pid, name: __MODULE__)
    end

    @impl true
    def init(test_pid) do
      {:ok, %{test_pid: test_pid, should_crash?: false, delay: 0, holding?: false, held: []}}
    end

    def handle_event(event) do
      GenServer.call(__MODULE__, {:handle_event, event}, :infinity)
    end

    def handle_event_async(event) do
      case GenServer.whereis(__MODULE__) do
        nil ->
          exit({:noproc, {__MODULE__, :handle_event_async, [event]}})

        pid ->
          monitor_ref = Process.monitor(pid)
          send(pid, {:"$gen_call", {self(), monitor_ref}, {:handle_event, event}})
          monitor_ref
      end
    end

    def toggle_crash(should_crash?) do
      GenServer.call(__MODULE__, {:toggle_crash, should_crash?})
    end

    def set_delay(delay_ms) do
      GenServer.call(__MODULE__, {:set_delay, delay_ms})
    end

    def hold_replies(holding?) do
      GenServer.call(__MODULE__, {:hold_replies, holding?})
    end

    def release_held do
      GenServer.call(__MODULE__, :release_held)
    end

    def release_one do
      GenServer.call(__MODULE__, :release_one)
    end

    @impl true
    def handle_call({:handle_event, %TransactionFragment{} = txn_fragment}, from, state) do
      if state.delay > 0, do: Process.sleep(state.delay)

      if state.should_crash? do
        raise "Interrupting transaction processing abnormally"
      end

      if state.holding? do
        send(state.test_pid, {:held, txn_fragment})
        {:noreply, %{state | held: state.held ++ [from]}}
      else
        send(state.test_pid, {:from_replication, txn_fragment})
        {:reply, :ok, state}
      end
    end

    def handle_call({:handle_event, %Relation{} = relation}, _from, state) do
      send(state.test_pid, {:from_replication, [relation]})
      {:reply, :ok, state}
    end

    def handle_call({:toggle_crash, should_crash?}, _from, state) do
      {:reply, :ok, %{state | should_crash?: should_crash?}}
    end

    def handle_call({:set_delay, delay_ms}, _from, state) do
      {:reply, :ok, %{state | delay: delay_ms}}
    end

    def handle_call({:hold_replies, holding?}, _from, state) do
      {:reply, :ok, %{state | holding?: holding?}}
    end

    def handle_call(:release_held, _from, state) do
      Enum.each(state.held, &GenServer.reply(&1, :ok))
      {:reply, :ok, %{state | held: []}}
    end

    def handle_call(:release_one, _from, %{held: [next | rest]} = state) do
      GenServer.reply(next, :ok)
      {:reply, :ok, %{state | held: rest}}
    end
  end

  # Stands in for the shape catalog's write buffer, and holds each drain until the test answers it.
  defmodule HeldWriteBuffer do
    use GenServer

    alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer

    def start_link({stack_id, test_pid}),
      do: GenServer.start_link(__MODULE__, test_pid, name: WriteBuffer.name(stack_id))

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:drain, _deadline}, from, test_pid) do
      send(test_pid, {:drain, from})
      {:noreply, test_pid}
    end
  end

  setup do
    # Spawn a dummy process to serve as the black hole for the messages that
    # ReplicationClient normally sends to Connection.Manager.
    pid = spawn_link(MockConnectionManager, :receive_casts, [self()])
    %{connection_manager: pid}
  end

  setup :with_stack_id_from_test
  setup :with_slot_name

  describe "full transaction provenance backpressure" do
    setup :with_lsn_tracker

    setup ctx do
      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "test",
          try_creating_publication?: false,
          slot_name: "test",
          connection_manager: ctx.connection_manager
        )

      %{
        state: %{
          state
          | step: :streaming,
            full_xid_source_positions?: true,
            received_wal: 100,
            flushed_wal: 90
        }
      }
    end

    test "cache miss pauses decoding while keepalives cannot acknowledge the pending Begin", %{
      state: state
    } do
      frame = <<?w, 0::64, 300::64, 0::64, ?B, 200::64, 0::64, 150::32>>
      assert {:noreply_and_pause, [], pending} = ReplicationClient.handle_data(frame, state)
      assert pending.message_converter.txn_fragment == nil
      assert pending.pending_provenance.xid == 150

      assert {:noreply, [<<?r, 101::64, 91::64, 91::64, _::64, 0>>], ^pending} =
               ReplicationClient.handle_info(:send_keepalive, pending)

      assert_receive {:fetch_provenance_anchor, _}
    end

    test "covered Begin is decoded immediately without a metadata request", %{state: state} do
      anchor =
        Electric.Postgres.FullTransactionId.anchor!(%{
          xmax: 200,
          catalog_xmin: 100,
          confirmed_lsn: 90,
          fence_lsn: 300
        })

      state = %{state | provenance_anchor: anchor}
      frame = <<?w, 0::64, 300::64, 0::64, ?B, 200::64, 0::64, 150::32>>
      assert {:noreply, result} = ReplicationClient.handle_data(frame, state)
      assert result.message_converter.txn_fragment.full_xid == 150
      assert result.provenance_task == nil
      assert result.pending_provenance == nil
      refute_receive {:fetch_provenance_anchor, _}
    end

    for journal? <- [false, true] do
      @tag journal: journal?
      test "downstream acknowledgement cannot resume unresolved provenance with journal=#{journal?}",
           %{state: state, journal: journal?} do
        event_ref = make_ref()

        state = %{
          state
          | pipeline_window: 3,
            pipeline_healthy?: true,
            ack_after_journal?: journal?,
            in_flight_count: 1,
            in_flight:
              :queue.from_list([
                {event_ref, %Electric.Replication.Changes.Relation{}, 1000,
                 System.monotonic_time(:millisecond)}
              ])
        }

        frame = <<?w, 0::64, 300::64, 0::64, ?B, 200::64, 0::64, 150::32>>
        assert {:noreply_and_pause, [], pending} = ReplicationClient.handle_data(frame, state)

        assert {:noreply, [], pending} =
                 ReplicationClient.handle_info({event_ref, :ok}, pending)

        assert pending.socket_paused?
        assert pending.in_flight_count == 0
        assert pending.message_converter.txn_fragment == nil

        anchor =
          Electric.Postgres.FullTransactionId.anchor!(%{
            xmax: 200,
            catalog_xmin: 100,
            confirmed_lsn: 90,
            fence_lsn: 300
          })

        ref = make_ref()

        pending = %{
          pending
          | provenance_task: %Task{ref: ref, pid: self(), owner: self(), mfa: nil}
        }

        assert {:noreply_and_resume, [], resolved} =
                 ReplicationClient.handle_info({ref, anchor}, pending)

        refute resolved.socket_paused?
        assert resolved.message_converter.txn_fragment.full_xid == 150
        assert_receive {:fetch_provenance_anchor, _}
      end
    end

    test "resolved provenance leaves a collapsed pipeline paused", %{state: state} do
      frame = <<?w, 0::64, 300::64, 0::64, ?B, 200::64, 0::64, 150::32>>
      assert {:noreply_and_pause, [], pending} = ReplicationClient.handle_data(frame, state)
      ref = make_ref()

      pending = %{
        pending
        | replay: :queue.new(),
          provenance_task: %Task{ref: ref, pid: self(), owner: self(), mfa: nil}
      }

      anchor =
        Electric.Postgres.FullTransactionId.anchor!(%{
          xmax: 200,
          catalog_xmin: 100,
          confirmed_lsn: 90,
          fence_lsn: 300
        })

      assert {:noreply, [], resolved} = ReplicationClient.handle_info({ref, anchor}, pending)
      assert resolved.socket_paused?
      assert resolved.message_converter.txn_fragment.full_xid == 150
      assert_receive {:fetch_provenance_anchor, _}
    end

    for journal? <- [false, true] do
      @tag journal: journal?
      test "exclusive keepalive cannot acknowledge the pending transaction with journal=#{journal?}",
           %{state: state, journal: journal?} do
        state = %{
          state
          | last_seen_txn_lsn: Lsn.from_integer(100),
            flush_up_to_date?: false,
            ack_after_journal?: journal?
        }

        assert {:noreply, [], state} =
                 ReplicationClient.handle_data(<<?k, 200::64, 0::64, 0>>, state)

        assert state.received_wal == 199
        frame = <<?w, 0::64, 300::64, 0::64, ?B, 200::64, 0::64, 150::32>>
        assert {:noreply_and_pause, [], state} = ReplicationClient.handle_data(frame, state)

        expected_flush = if journal?, do: 91, else: 200

        assert {:noreply, [<<?r, 200::64, ^expected_flush::64, ^expected_flush::64, _::64, 0>>],
                state} =
                 ReplicationClient.handle_info({:flush_boundary_updated, 100}, state)

        ref = make_ref()
        state = %{state | provenance_task: %Task{ref: ref, pid: self(), owner: self(), mfa: nil}}

        anchor =
          Electric.Postgres.FullTransactionId.anchor!(%{
            xmax: 200,
            catalog_xmin: 100,
            confirmed_lsn: 200,
            fence_lsn: 300
          })

        assert {:noreply_and_resume, [], result} =
                 ReplicationClient.handle_info({ref, anchor}, state)

        assert result.message_converter.txn_fragment.full_xid == 150
        assert_receive {:fetch_provenance_anchor, _}
      end
    end

    test "uncovered completion frontier stays paused when an asynchronous anchor needs retry", %{
      state: state
    } do
      frame = <<?w, 0::64, 300::64, 0::64, ?B, 200::64, 0::64, 150::32>>
      assert {:noreply_and_pause, [], state} = ReplicationClient.handle_data(frame, state)
      assert_receive {:fetch_provenance_anchor, _}
      ref = make_ref()
      state = %{state | provenance_task: %Task{ref: ref, pid: self(), owner: self(), mfa: nil}}

      anchor =
        Electric.Postgres.FullTransactionId.anchor!(%{
          xmax: 150,
          catalog_xmin: 100,
          confirmed_lsn: 90,
          fence_lsn: 300
        })

      assert {:noreply, [], result} = ReplicationClient.handle_info({ref, anchor}, state)
      assert result.pending_provenance.xid == 150
      assert result.message_converter.txn_fragment == nil
      assert_receive {:fetch_provenance_anchor, _}
    end

    test "disconnect cancels in-flight provenance and ignores old coalescing messages", %{
      state: state
    } do
      request = make_ref()

      task =
        Task.async(fn ->
          receive do
            :finish -> :ok
          end
        end)

      state = %{
        state
        | provenance_task: task,
          provenance_request: request,
          pending_provenance: {:first_message, "old source"},
          provenance_deadline: 123
      }

      assert {:noreply, disconnected} = ReplicationClient.handle_disconnect(state)
      refute Process.alive?(task.pid)
      assert disconnected.provenance_task == nil
      assert disconnected.pending_provenance == nil
      assert disconnected.provenance_deadline == nil

      assert {:noreply, ^disconnected} =
               ReplicationClient.handle_info({:fetch_provenance_anchor, request}, disconnected)

      refute Electric.StackConfig.lookup(state.stack_id, :full_xid_source_positions_ready)
    end
  end

  describe "full transaction provenance against real db" do
    @describetag stack_config_seed: [feature_flags: ["full_xid_source_positions"]]
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    setup ctx do
      start_full_xid_admin_pool(ctx)

      start_supervised!(
        {Task.Supervisor,
         name: Electric.ProcessRegistry.name(ctx.stack_id, Electric.StackTaskSupervisor)}
      )

      :ok
    end

    test "resolves committed source xid8 and retains raw xid compatibility",
         %{db_conn: conn} = ctx do
      start_client(ctx)
      assert Electric.StackConfig.lookup(ctx.stack_id, :full_xid_source_positions_ready)
      assert is_binary(Electric.StackConfig.lookup(ctx.stack_id, :full_xid_source_generation))

      {:ok, xid} =
        Postgrex.transaction(conn, fn connection ->
          %{rows: [[xid]]} = Postgrex.query!(connection, "SELECT pg_current_xact_id()::text", [])
          insert_item(connection, "full transaction")
          String.to_integer(xid)
        end)

      txn = receive_transaction_entity()
      assert txn.full_xid == xid
      assert txn.xid == Integer.mod(xid, 0x100000000)
    end

    @tag :tmp_dir
    @tag replication_opts_overrides: [pipeline_window: 3, ack_after_journal: true]
    test "pipelined journal ingestion retains full transaction IDs across reconnect", ctx do
      with_pure_file_storage(ctx)
      start_client(ctx)

      expected =
        Enum.map(1..20, fn index ->
          {:ok, xid} =
            Postgrex.transaction(ctx.db_conn, fn connection ->
              %{rows: [[xid]]} =
                Postgrex.query!(connection, "SELECT pg_current_xact_id()::text", [])

              insert_item(connection, "journal transaction #{index}")
              String.to_integer(xid)
            end)

          xid
        end)

      received = Enum.map(expected, fn _ -> receive_transaction_entity() end)
      assert Enum.map(received, & &1.full_xid) == expected

      {_, %{base_path: base_path}} =
        Electric.StackConfig.lookup(ctx.stack_id, Electric.ShapeCache.Storage)

      journaled =
        TxnJournal.stream_events!(base_path, TxnJournal.list_generations(base_path))
        |> Enum.flat_map(fn
          %TransactionFragment{commit: commit, full_xid: xid} when not is_nil(commit) -> [xid]
          _ -> []
        end)

      assert journaled == expected

      Support.TestUtils.wait_until(fn ->
        Lsn.compare(get_confirmed_flush_lsn(ctx.db_conn, ctx.slot_name), List.last(received).lsn) ==
          :gt
      end)

      :ok = stop_supervised(ReplicationClient)
      start_client(ctx)
      insert_item(ctx.db_conn, "after reconnect")
      assert receive_transaction_entity().full_xid > List.last(expected)
    end

    test "resolves an older data-only transaction committed after newer transactions",
         %{db_conn: conn} = ctx do
      start_client(ctx)

      long_conn =
        start_supervised!(
          {Postgrex,
           Keyword.put(
             Electric.Utils.deobfuscate_password(ctx.db_config),
             :types,
             PgInterop.Postgrex.Types
           )},
          id: :long_running_connection
        )

      Postgrex.query!(long_conn, "BEGIN", [])
      %{rows: [[old_xid]]} = Postgrex.query!(long_conn, "SELECT pg_current_xact_id()::text", [])
      insert_item(long_conn, "old transaction")

      Enum.each(1..12, fn index -> insert_item(conn, "new transaction #{index}") end)
      newer = Enum.map(1..12, fn _ -> receive_transaction_entity() end)
      assert Enum.all?(newer, &(&1.full_xid > String.to_integer(old_xid)))

      Postgrex.query!(long_conn, "COMMIT", [])
      assert receive_transaction_entity().full_xid == String.to_integer(old_xid)
    end

    test "reconnect replays unacknowledged transactions with the same full xid",
         %{db_conn: conn} = ctx do
      pid = start_client(ctx)
      insert_item(conn, "replayed full transaction")
      original = receive_transaction_entity()
      Process.unlink(pid)
      monitor = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}

      start_client(ctx, id: :reconnected_full_xid)
      replayed = receive_transaction_entity()
      assert replayed.full_xid == original.full_xid
      assert replayed.lsn == original.lsn
    end

    test "keeps the source generation across restarts until the slot is recreated", ctx do
      ctx = with_generation_kv(ctx)

      start_client(ctx)
      generation = source_generation(ctx)
      assert generation_database_oid(generation) == current_database_oid(ctx.db_conn)

      :ok = stop_supervised(ReplicationClient)
      start_client(ctx)
      assert source_generation(ctx) == generation

      :ok = stop_supervised(ReplicationClient)
      drop_slot!(ctx.db_conn, ctx.slot_name)
      start_client(ctx)
      assert source_generation(ctx) != generation
    end

    @tag timeout: 300_000
    test "changes the source generation when the database is recreated under its name", ctx do
      # A database of its own: the shared test pool cannot survive its database being dropped.
      db_config = Keyword.put(ctx.db_config, :database, "recreated_#{ctx.slot_name}")

      ctx =
        with_generation_kv(%{
          ctx
          | db_config: db_config,
            replication_opts: Keyword.put(ctx.replication_opts, :connection_opts, db_config)
        })

      on_exit(fn -> drop_source_database!(ctx) end)

      recreate_source_database!(ctx)
      :ok = stop_supervised(:full_xid_admin_pool)
      start_full_xid_admin_pool(ctx)
      start_client(ctx)
      generation = source_generation(ctx)
      oid = generation_database_oid(generation)

      :ok = stop_supervised(ReplicationClient)
      :ok = stop_supervised(:full_xid_admin_pool)
      recreate_source_database!(ctx)
      start_full_xid_admin_pool(ctx)
      start_client(ctx)

      assert source_generation(ctx) != generation
      assert generation_database_oid(source_generation(ctx)) != oid
    end
  end

  # DROP DATABASE waits for an immediate checkpoint, which outlasts the default query timeout on a
  # busy CI host; a cancelled drop leaves the database invalid.
  @database_ddl_timeout 120_000

  defp recreate_source_database!(ctx) do
    drop_source_database!(ctx)
    database = Keyword.fetch!(ctx.db_config, :database)

    with_connection(ctx, "postgres", fn conn ->
      Postgrex.query!(conn, ~s|CREATE DATABASE "#{database}"|, [], timeout: @database_ddl_timeout)
    end)

    with_connection(ctx, database, fn conn ->
      with_basic_tables(%{ctx | db_conn: conn})
      with_publication(%{ctx | db_conn: conn})
    end)
  end

  # DROP DATABASE drops the database's inactive slots itself, so the slot only has to be released.
  defp drop_source_database!(ctx) do
    database = Keyword.fetch!(ctx.db_config, :database)

    with_connection(ctx, "postgres", fn conn ->
      assert Support.TestUtils.wait_until(
               fn ->
                 Postgrex.query!(
                   conn,
                   "SELECT FROM pg_replication_slots WHERE slot_name = $1 AND active",
                   [ctx.slot_name]
                 ).num_rows == 0
               end,
               5_000
             )

      Postgrex.query!(conn, ~s|DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)|, [],
        timeout: @database_ddl_timeout
      )
    end)
  end

  defp with_connection(ctx, database, fun) do
    config =
      ctx.db_config |> Electric.Utils.deobfuscate_password() |> Keyword.put(:database, database)

    {:ok, conn} = Postgrex.start_link(config)

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
    end
  end

  defp with_generation_kv(ctx) do
    kv = Electric.PersistentKV.Memory.new!()
    %{ctx | replication_opts: Keyword.put(ctx.replication_opts, :persistent_kv, kv)}
  end

  defp source_generation(ctx),
    do: Electric.StackConfig.lookup(ctx.stack_id, :full_xid_source_generation)

  defp generation_database_oid(generation),
    do: generation |> Base.url_decode64!(padding: false) |> Jason.decode!() |> Enum.at(3)

  defp current_database_oid(conn) do
    %{rows: [[oid]]} =
      Postgrex.query!(
        conn,
        "SELECT oid::text FROM pg_database WHERE datname = current_database()",
        []
      )

    oid
  end

  # The walsender can outlive the stopped client for a moment, and an active slot cannot be dropped.
  defp drop_slot!(conn, slot_name) do
    query =
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1"

    assert Support.TestUtils.wait_until(
             fn -> match?({:ok, _}, Postgrex.query(conn, query, [slot_name])) end,
             5_000
           )
  end

  defp start_full_xid_admin_pool(ctx) do
    start_supervised!(
      Supervisor.child_spec(
        {Postgrex,
         Keyword.merge(Electric.Utils.deobfuscate_password(ctx.db_config),
           name: Electric.Connection.Manager.admin_pool(ctx.stack_id),
           types: PgInterop.Postgrex.Types
         )},
        id: :full_xid_admin_pool
      )
    )
  end

  describe "ReplicationClient init" do
    setup [:with_unique_db, :with_basic_tables, :with_status_monitor, :with_lsn_tracker]

    test "creates an empty publication on startup if requested",
         %{db_conn: conn, connection_manager: connection_manager, slot_name: slot_name} = ctx do
      replication_opts = [
        connection_opts: ctx.db_config,
        stack_id: ctx.stack_id,
        publication_name: ctx.slot_name,
        try_creating_publication?: true,
        slot_name: ctx.slot_name,
        handle_event: nil,
        connection_manager: connection_manager
      ]

      start_client(ctx, replication_opts: replication_opts)

      assert %{rows: [[^slot_name]]} =
               Postgrex.query!(conn, "SELECT pubname FROM pg_publication", [])

      assert %{rows: []} = Postgrex.query!(conn, "SELECT pubname FROM pg_publication_tables", [])
    end

    test "runs at high scheduler priority when ingest_priority_high is set",
         %{connection_manager: connection_manager} = ctx do
      put_electric_env(:ingest_priority_high, true)

      replication_opts = [
        connection_opts: ctx.db_config,
        stack_id: ctx.stack_id,
        publication_name: ctx.slot_name,
        try_creating_publication?: true,
        slot_name: ctx.slot_name,
        handle_event: nil,
        connection_manager: connection_manager
      ]

      pid = start_client(ctx, replication_opts: replication_opts)

      assert {:priority, :high} = Process.info(pid, :priority)
    end

    test "runs at normal scheduler priority by default",
         %{connection_manager: connection_manager} = ctx do
      # "By default" means with nothing configured anywhere: an option left out of the stack
      # falls through to release configuration, so the environment is pinned too.
      put_electric_env(:ingest_priority_high, false)

      replication_opts = [
        connection_opts: ctx.db_config,
        stack_id: ctx.stack_id,
        publication_name: ctx.slot_name,
        try_creating_publication?: true,
        slot_name: ctx.slot_name,
        handle_event: nil,
        connection_manager: connection_manager
      ]

      pid = start_client(ctx, replication_opts: replication_opts)

      assert {:priority, :normal} = Process.info(pid, :priority)
    end

    # The client logs from init/1 at high priority, and Postgrex traps exits only once it connects.
    # A capture-log task that exited after that reached the client as an exit message, which it
    # read once it streamed and stopped on, with reason :normal and nothing logged.
    test "an exit left behind by a log from init does not stop the client",
         %{connection_manager: connection_manager} = ctx do
      put_electric_env(:ingest_priority_high, true)

      :ok =
        :logger.add_handler(:linked_log_helper, LinkedLogHelper, %{
          level: :all,
          config: %{label: {:replication_client, ctx.stack_id}, test_pid: self()}
        })

      on_exit(fn -> :logger.remove_handler(:linked_log_helper) end)

      replication_opts = [
        connection_opts: ctx.db_config,
        stack_id: ctx.stack_id,
        publication_name: ctx.slot_name,
        try_creating_publication?: true,
        slot_name: ctx.slot_name,
        handle_event: nil,
        connection_manager: connection_manager
      ]

      pid = start_client(ctx, replication_opts: replication_opts)

      assert_receive {:linked_log_helper_exited, _helper}
      # read after any exit message that reached the client before it
      assert {_state, _data} = :sys.get_state(pid)
    end
  end

  describe "ReplicationClient against real db" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    test "calls a provided function when receiving it from the PG", %{db_conn: conn} = ctx do
      start_client(ctx)

      insert_item(conn, "test value")

      assert %NewRecord{record: %{"value" => "test value"}} = receive_tx_change()
    end

    test "logs a message when connected & replication has started", %{db_conn: conn} = ctx do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          start_client(ctx)

          insert_item(conn, "test value")

          assert %NewRecord{record: %{"value" => "test value"}} = receive_tx_change()
        end)

      log =~ "Started replication from postgres"
    end

    test "works with an existing publication", %{replication_opts: replication_opts} = ctx do
      replication_opts = Keyword.put(replication_opts, :try_creating_publication?, true)
      start_client(ctx, replication_opts: replication_opts)
    end

    test "restores the operations an existing publication stopped publishing and recreates the slot",
         %{db_conn: conn, slot_name: slot_name, replication_opts: replication_opts} = ctx do
      # The test process stands in for the connection manager, to see whether a new slot was made.
      replication_opts =
        Keyword.merge(replication_opts,
          try_creating_publication?: true,
          connection_manager: self()
        )

      boot = fn ->
        start_client(ctx, replication_opts: replication_opts, wait_for_start: false)

        # The slot is set up before streaming starts.
        assert_receive {:"$gen_cast", :replication_client_streamed_first_message},
                       @assert_receive_db_timeout

        :ok = stop_supervised(ReplicationClient)
      end

      boot.()
      assert_received {:"$gen_cast", :replication_client_created_new_slot}

      Postgrex.query!(conn, "ALTER PUBLICATION #{slot_name} SET (publish = 'insert, update')", [])

      boot.()
      assert_received {:"$gen_cast", :replication_client_created_new_slot}

      assert %{rows: [[true, true, true, true]]} =
               Postgrex.query!(
                 conn,
                 "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1",
                 [slot_name]
               )

      boot.()
      refute_received {:"$gen_cast", :replication_client_created_new_slot}
    end

    test "drops the slot before it restores the publication's operations",
         %{db_conn: conn, slot_name: slot_name, replication_opts: replication_opts} = ctx do
      # Were the slot dropped after the repair, Electric stopping in between would leave a sound
      # publication and the old slot: the next boot would reuse the slot and keep stale shapes.
      replication_opts = Keyword.put(replication_opts, :try_creating_publication?, true)
      start_client(ctx, replication_opts: replication_opts)
      :ok = stop_supervised(ReplicationClient)

      Postgrex.query!(conn, "ALTER PUBLICATION #{slot_name} SET (publish = 'insert, update')", [])
      Postgrex.query!(conn, "CREATE TABLE slots_at_repair (slots bigint)", [])

      Postgrex.query!(
        conn,
        """
        CREATE FUNCTION record_slots_at_repair() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF current_query() ILIKE '%SET (publish%' THEN
            INSERT INTO slots_at_repair
              SELECT count(*) FROM pg_replication_slots WHERE slot_name = '#{slot_name}';
          END IF;
        END $$
        """,
        []
      )

      Postgrex.query!(
        conn,
        "CREATE EVENT TRIGGER record_slots_at_repair ON ddl_command_start " <>
          "WHEN TAG IN ('ALTER PUBLICATION') EXECUTE FUNCTION record_slots_at_repair()",
        []
      )

      start_client(ctx, replication_opts: replication_opts)

      assert %{rows: [[0]]} = Postgrex.query!(conn, "SELECT slots FROM slots_at_repair", [])
    end

    test "works with an existing replication slot",
         %{db_conn: conn, slot_name: slot_name} = ctx do
      pid = start_client(ctx)

      assert %{
               "slot_name" => ^slot_name,
               "temporary" => false,
               "confirmed_flush_lsn" => flush_lsn
             } = fetch_slot_info(conn, slot_name)

      # Check that the slot remains even when the replication client goes down
      true = Process.unlink(pid)
      true = Process.exit(pid, :kill)

      assert %{"slot_name" => ^slot_name, "confirmed_flush_lsn" => ^flush_lsn} =
               fetch_slot_info(conn, slot_name)

      # Check that the replication client works when the replication slot already exists
      start_client(ctx)

      assert %{"slot_name" => ^slot_name, "confirmed_flush_lsn" => ^flush_lsn} =
               fetch_slot_info(conn, slot_name)
    end

    test "can replay already seen transaction", %{db_conn: conn} = ctx do
      pid = start_client(ctx)

      insert_item(conn, "test value")

      assert %NewRecord{
               record: %{"value" => "test value"},
               log_offset: %LogOffset{tx_offset: tx_lsn}
             } = receive_tx_change()

      insert_item(conn, "another value")
      assert %NewRecord{record: %{"value" => "another value"}} = receive_tx_change()

      # Verify that raising in the transaction callback crashes the connection process
      monitor = Process.monitor(pid)
      Process.unlink(pid)

      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      send(pid, {:flush_boundary_updated, tx_lsn})

      interrupt_val = "interrupt #{inspect(pid)}"
      insert_item(conn, interrupt_val)
      refute_receive {:from_replication, _}, 50
      Process.exit(pid, :some_reason)

      assert_receive {:DOWN, ^monitor, :process, ^pid, :some_reason}, @assert_receive_db_timeout

      assert_received :lock_acquired
      refute_received _

      # Now, when we restart the connection process, it replays transactions from the last
      # confirmed one
      start_client(ctx)

      assert %NewRecord{record: %{"value" => "another value"}} = receive_tx_change()
      assert %NewRecord{record: %{"value" => ^interrupt_val}} = receive_tx_change()

      assert_received :lock_acquired
      refute_receive _
    end

    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "holds processing of transaction until ready", %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)

      # should not process the transaction but also should not die
      insert_item(conn, "test value 1")
      refute_receive {:from_replication, _}, 50
      assert Process.alive?(client_pid)

      start_supervised({MockTransactionProcessor, self()})

      # once we start streaming we should see it processed
      assert %NewRecord{record: %{"value" => "test value 1"}} = receive_tx_change()

      # should have same behaviour mid-processing
      mock_ref = Process.monitor(MockTransactionProcessor)
      MockTransactionProcessor.toggle_crash(true)
      insert_item(conn, "test value 2")

      assert {%RuntimeError{message: "Interrupting transaction processing abnormally"}, _} =
               receive_exit_reason(mock_ref)

      assert Process.alive?(client_pid)

      # The crashed handler stays alive while it logs the exception, and the test supervisor
      # keeps its child entry until it handles the exit. Started any earlier, the replacement
      # fails as already started, which start_supervised/1 would not report.
      stop_supervised(MockTransactionProcessor)
      start_supervised!({MockTransactionProcessor, self()})
      assert %NewRecord{record: %{"value" => "test value 2"}} = receive_tx_change()
    end

    # An unavailable handler is retried every 50ms until the ten-minute budget runs out. Logging the
    # failure on every attempt turns one collector restart into ~12,000 identical error lines in the
    # log pipeline and the error tracker, each carrying the crash reason.
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "logs the unavailable handler once per second, not once per retry",
         %{db_conn: conn} = ctx do
      # Arrange — no MockTransactionProcessor, so every dispatch exits :noproc and reschedules.
      client_pid = start_client(ctx)

      # Act
      log =
        capture_log(fn ->
          insert_item(conn, "test value")
          Process.sleep(2_000)
        end)

      # Assert — the retry loop itself keeps running; only the logging is bounded.
      logged = log |> String.split("retry budget left") |> length() |> Kernel.-(1)
      assert logged >= 1
      assert logged <= 4
      assert Process.alive?(client_pid)
    end

    @tag database_settings: ["wal_sender_timeout='3s'"]
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "connection survives wal_sender_timeout when event handler is unavailable",
         %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)
      ref = Process.monitor(client_pid)

      # Insert data without MockTransactionProcessor started — events crash
      # with :noproc, triggering the retry loop. The gen_statem is free between
      # retries, allowing the keepalive timer to send StandbyStatusUpdate messages.
      insert_item(conn, "test value 1")

      # Wait ~2x wal_sender_timeout (3s). Without the keepalive fix, PG kills
      # the connection and the process crashes with "tcp send: closed".
      # The refute_receive is both the wait AND the assertion: if the process
      # dies during this window, the test fails immediately.
      refute_receive {:DOWN, ^ref, :process, ^client_pid, _}, 6_000

      # Start the handler — pending event should be retried and succeed.
      start_supervised({MockTransactionProcessor, self()})
      assert %NewRecord{record: %{"value" => "test value 1"}} = receive_tx_change()

      # Insert more data that requires a LIVE connection to receive new WAL.
      insert_item(conn, "test value 2")
      assert %NewRecord{record: %{"value" => "test value 2"}} = receive_tx_change()
    end

    @tag database_settings: ["wal_sender_timeout='3s'"]
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "connection survives wal_sender_timeout when event handler is slow",
         %{db_conn: conn} = ctx do
      start_supervised({MockTransactionProcessor, self()})
      MockTransactionProcessor.set_delay(6_000)

      client_pid = start_client(ctx)
      ref = Process.monitor(client_pid)

      # Insert data — the event handler will take 6s to respond (2x wal_sender_timeout).
      # Because apply_event dispatches via $gen_call, the gen_statem is free to send
      # keepalives. Without the async fix, the gen_statem would be blocked for 6s and
      # PG would kill the connection after 3s.
      insert_item(conn, "slow value")

      # The process must survive the full processing window.
      refute_receive {:DOWN, ^ref, :process, ^client_pid, _}, 8_000

      # The slow event should have been processed.
      assert %NewRecord{record: %{"value" => "slow value"}} = receive_tx_change()

      # Verify the connection is still live by sending more data with no delay.
      MockTransactionProcessor.set_delay(0)
      insert_item(conn, "fast value")
      assert %NewRecord{record: %{"value" => "fast value"}} = receive_tx_change()
    end

    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    @tag replication_opts_overrides: [pipeline_window: 3]
    test "pipelines transactions up to the window before pausing the socket",
         %{db_conn: conn} = ctx do
      # Arrange
      start_supervised({MockTransactionProcessor, self()})
      start_client(ctx)

      # The window only opens after the first successful ack.
      insert_item(conn, "warmup")
      assert %NewRecord{record: %{"value" => "warmup"}} = receive_tx_change()

      MockTransactionProcessor.hold_replies(true)

      insert_item(conn, "t2")
      insert_item(conn, "t3")
      insert_item(conn, "t4")
      insert_item(conn, "t5")

      # Three transactions are dispatched while none of them is acked — the stop-and-wait
      # client would deliver exactly one.
      assert %{changes: [%NewRecord{record: %{"value" => "t2"}}]} = receive_held()

      assert %{changes: [%NewRecord{record: %{"value" => "t3"}}]} = receive_held()

      assert %{changes: [%NewRecord{record: %{"value" => "t4"}}]} = receive_held()

      refute_receive {:held, _}, 100

      # Act
      MockTransactionProcessor.release_one()

      # Assert: the freed slot is what lets t5 through — held at the window, it never
      # arrives without the release, however long the wait.
      assert %{changes: [%NewRecord{record: %{"value" => "t5"}}]} = receive_held()

      MockTransactionProcessor.hold_replies(false)
      MockTransactionProcessor.release_held()

      # Acks drain FIFO and the resumed socket keeps streaming.
      insert_item(conn, "t6")
      assert %NewRecord{record: %{"value" => "t6"}} = receive_tx_change()
    end

    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    @tag replication_opts_overrides: [pipeline_window: 3]
    test "replays in-flight transactions in order after the event handler dies",
         %{db_conn: conn} = ctx do
      start_supervised({MockTransactionProcessor, self()})
      client_pid = start_client(ctx)

      insert_item(conn, "warmup")
      assert %NewRecord{record: %{"value" => "warmup"}} = receive_tx_change()

      MockTransactionProcessor.hold_replies(true)

      insert_item(conn, "t2")
      insert_item(conn, "t3")
      assert %TransactionFragment{} = receive_held()
      assert %TransactionFragment{} = receive_held()

      # Kill the handler with both transactions in flight: the client must collapse to
      # sequential replay and redeliver both, in WAL order, once a handler is back.
      mock_pid = GenServer.whereis(MockTransactionProcessor)
      Process.exit(mock_pid, :kill)

      # stop_supervised serializes through the test supervisor, so the killed child is
      # fully cleared before the respawn — start_supervised would otherwise race the
      # death processing and silently fail.
      stop_supervised(MockTransactionProcessor)
      start_supervised!({MockTransactionProcessor, self()})

      assert %NewRecord{record: %{"value" => "t2"}} = receive_tx_change()
      assert %NewRecord{record: %{"value" => "t3"}} = receive_tx_change()
      assert Process.alive?(client_pid)

      # The pipeline recovers: subsequent transactions flow normally.
      insert_item(conn, "t4")
      assert %NewRecord{record: %{"value" => "t4"}} = receive_tx_change()
    end

    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "aborts held processing of transaction on exit", %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)
      insert_item(conn, "test value 1")
      refute_receive {:from_replication, _}, 50
      ref = Process.monitor(client_pid)
      Process.unlink(client_pid)
      Process.exit(client_pid, :shutdown)
      assert_receive {:DOWN, ^ref, :process, ^client_pid, :shutdown}
    end

    # Regression test for https://github.com/electric-sql/electric/issues/1548
    test "fares well when multiple concurrent transactions are writing to WAL",
         %{db_conn: conn} = ctx do
      start_client(ctx)

      num_txn = 2
      num_ops = 8
      max_sleep = 20

      # Insert `num_txn` transactions, each in a separate process. Every transaction has
      # `num_ops` INSERTs with a random delay between each operation.
      # The end result is that INSERTs from different transactions get interleaved in
      # the WAL, challenging any assumptions in ReplicationClient about cross-transaction operation
      # ordering.
      Enum.each(1..num_txn, fn i ->
        tx_fun = fn conn ->
          pid_str = inspect(self())

          Enum.each(1..num_ops, fn j ->
            insert_item(conn, "#{i}-#{j} in process #{pid_str}")
            Process.sleep(:rand.uniform(max_sleep))
          end)
        end

        spawn_link(Postgrex, :transaction, [conn, tx_fun])
      end)

      # Receive every transaction sent by ReplicationClient to the test process.
      set =
        Enum.reduce(1..num_txn, MapSet.new(1..num_txn), fn _, set ->
          {_lsn, records} = receive_transaction()
          assert num_ops == length(records)

          [%NewRecord{record: %{"value" => val}} | _] = records
          {i, _} = Integer.parse(val)

          MapSet.delete(set, i)
        end)

      # Make sure there are no extraneous messages left.
      assert MapSet.size(set) == 0
      assert_received :lock_acquired
      refute_receive _
    end

    # Set the DB's display settings to something else than Electric.Postgres.display_settings
    @tag database_settings: [
           "DateStyle='Postgres, DMY'",
           "TimeZone='CET'",
           "extra_float_digits=-1",
           "bytea_output='escape'",
           "IntervalStyle='postgres'"
         ]
    @tag additional_fields:
           "date DATE, timestamptz TIMESTAMPTZ, float FLOAT8, bytea BYTEA, interval INTERVAL"
    test "returns data formatted according to display settings", %{db_conn: conn} = ctx do
      start_client(ctx)

      Postgrex.query!(
        conn,
        """
        INSERT INTO items (
          id, value, date, timestamptz, float, bytea, interval
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7
        )
        """,
        [
          Ecto.UUID.bingenerate(),
          "test value",
          ~D[2022-05-17],
          ~U[2022-01-12 00:01:00.00Z],
          1.234567890123456,
          <<0x5, 0x10, 0xFA>>,
          %Postgrex.Interval{
            days: 1,
            months: 0,
            # 12 hours, 59 minutes, 10 seconds
            secs: 46750,
            microsecs: 0
          }
        ]
      )

      # Check that the incoming data is formatted according to Electric.Postgres.display_settings
      assert %NewRecord{
               record: %{
                 "date" => "2022-05-17",
                 "timestamptz" => "2022-01-12 00:01:00+00",
                 "float" => "1.234567890123456",
                 "bytea" => "\\x0510fa",
                 "interval" => "P1DT12H59M10S"
               }
             } = receive_tx_change()
    end

    test "exits with irrecoverable slot error with large transactions", %{db_conn: conn} = ctx do
      pid =
        start_client(ctx,
          replication_opts: Keyword.put(ctx.replication_opts, :max_txn_size, 5000)
        )

      monitor = Process.monitor(pid)
      Process.unlink(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      insert_item(conn, gen_random_string(5001))

      # Verify that passing the txn size limit crashes the process

      assert {:irrecoverable_slot,
              {:exceeded_max_tx_size, "Collected transaction exceeds limit of 5000 bytes."}} =
               receive_exit_reason(monitor)
    end

    test "exits with irrecoverable slot error for invalid replica identity",
         %{db_conn: conn} = ctx do
      pid = start_client(ctx)

      monitor = Process.monitor(pid)
      Process.unlink(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      {_id, bin_uuid} = gen_uuid()

      Postgrex.query!(conn, "INSERT INTO items (id, value) VALUES ($1, $2)", [bin_uuid, "test"])
      assert %NewRecord{record: %{"value" => "test"}} = receive_tx_change()
      Postgrex.query!(conn, "UPDATE items SET value = $2 WHERE id = $1", [bin_uuid, "new"])

      # Verify that receiving updates without old values causes an exit
      assert {:irrecoverable_slot, {:replica_not_full, msg}} = receive_exit_reason(monitor)

      assert msg =~
               "Received an update from PG for public.items that did not have old data included in the message."
    end

    @tag with_empty_publication?: true
    test "flushes are advanced based on standby messages when publication is otherwise silent",
         %{db_conn: conn} = ctx do
      pid = start_client(ctx)

      Process.sleep(100)

      confirmed_flush_lsn = get_confirmed_flush_lsn(conn, ctx.slot_name)

      for _ <- 1..10 do
        insert_item(conn, "test value")
      end

      # The server's keepalives for these inserts ask for no reply: the client stores their
      # position and reports it in its next status update, which its keepalive timer sends
      # every 15s. Each poll sends one sooner.
      assert Support.TestUtils.wait_until(fn ->
               send(pid, :send_keepalive)
               new_confirmed_flush_lsn = get_confirmed_flush_lsn(conn, ctx.slot_name)
               Lsn.compare(confirmed_flush_lsn, new_confirmed_flush_lsn) == :lt
             end)
    end

    @tag with_empty_publication?: true
    test "flushes are not advancing if something isn't actually getting flushed",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      pid = start_client(ctx)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])

      assert {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      confirmed_flush_lsn1 = get_confirmed_flush_lsn(conn, ctx.slot_name)

      assert Lsn.to_integer(confirmed_flush_lsn1) < Lsn.to_integer(lsn1)

      for _ <- 1..10 do
        insert_item(conn, "test value")
      end

      send(pid, {:flush_boundary_updated, 100})

      # This is to pass the time instead of a sleep - if PG is responsive, it probably has processed the wal acknowledge too
      Postgrex.query!(conn, "SELECT * FROM items", [])

      # Still same LSN
      assert confirmed_flush_lsn1 ==
               get_confirmed_flush_lsn(conn, ctx.slot_name)
    end

    @tag with_empty_publication?: true
    test "flushes are advanced once confirmed, and advance beyond the last seen txn once up-to-date",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      pid = start_client(ctx)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])

      {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      confirmed_flush_lsn1 = get_confirmed_flush_lsn(conn, ctx.slot_name)

      assert Lsn.to_integer(confirmed_flush_lsn1) < Lsn.to_integer(lsn1)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (2)", [])

      {lsn2, %NewRecord{record: %{"id" => "2"}}} = receive_tx_change_with_lsn()

      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn1)})

      # Unrelated writes
      for _ <- 1..10, do: insert_item(conn, "test value")

      # The slot moves when the client's status update reaches Postgres; each poll asks for one
      # instead of waiting for the client's 15s keepalive.
      assert Support.TestUtils.wait_until(fn ->
               send(pid, :send_keepalive)
               Lsn.increment(lsn1, 1) == get_confirmed_flush_lsn(conn, ctx.slot_name)
             end)

      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn2)})

      # We should be using a higher LSN - one of the received ones - as "last seen", but in some race conditions
      # we might not have had a reason to respond to PG yet, so we're using `>=`
      assert Support.TestUtils.wait_until(fn ->
               send(pid, :send_keepalive)

               Lsn.to_integer(get_confirmed_flush_lsn(conn, ctx.slot_name)) >=
                 Lsn.to_integer(Lsn.increment(lsn2, 1))
             end)
    end

    @tag with_empty_publication?: true
    test "keepalives don't ack WAL past an unflushed txn on an already-streamed table",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      pid = start_client(ctx)

      # First insert into serial_ids in this walsender session. The txn carries
      # a Relation message which is dispatched as a standalone mid-transaction
      # event, so this txn incidentally clears flush_up_to_date?.
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])
      assert {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      # Confirm txn1 as flushed: lsn1 is the last seen txn, so the client
      # becomes fully caught up and flush_up_to_date? is re-armed.
      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn1)})

      # A second small insert into the now already-streamed table: no Relation
      # message and fewer changes than max_batch_size, so the whole txn is
      # decoded into a single commit-carrying fragment with no mid-transaction
      # event dispatch. Its flush is never confirmed.
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (2)", [])
      assert {lsn2, %NewRecord{record: %{"id" => "2"}}} = receive_tx_change_with_lsn()

      # Writes to a table outside the publication make the walsender skip
      # transactions and send keepalives with an advancing wal_end.
      for _ <- 1..10, do: insert_item(conn, "test value")

      # Wait long enough for those keepalives to arrive (their timing is up to
      # PG), then make the client report its ack state: re-notifying an
      # already-confirmed boundary — as happens whenever any shape reports a
      # flush — sends a standby status update carrying the current flushed_wal.
      confirmed =
        Enum.reduce_while(1..20, nil, fn _, _ ->
          Process.sleep(50)
          send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn1)})

          confirmed = get_confirmed_flush_lsn(conn, ctx.slot_name)

          if Lsn.compare(confirmed, lsn2) == :gt do
            {:halt, confirmed}
          else
            {:cont, confirmed}
          end
        end)

      # The unconfirmed txn2 must hold the flush boundary back: acknowledging
      # WAL past it means PG can drop WAL that no shape has durably stored.
      assert Lsn.compare(confirmed, lsn2) != :gt,
             "confirmed_flush_lsn #{confirmed} advanced past the unflushed txn at #{lsn2}"
    end

    @tag with_empty_publication?: true
    @tag database_settings: ["wal_sender_timeout='3s'"]
    test "keepalives keep advancing acks past txns that are confirmed during processing",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      # The confirming handler notifies the flush boundary from within event
      # processing, before the reply — like the ShapeLogCollector does for a
      # transaction that affects no shapes. Such transactions must not stall
      # the keepalive-driven slot advance. No further notifications arrive
      # after the reply, so the up-to-date state must be recomputed once the
      # transaction is acknowledged; the acks reach PG via the client's
      # periodic status updates (wal_sender_timeout/3, hence the 3s setting).
      replication_opts =
        Keyword.put(
          ctx.replication_opts,
          :handle_event,
          {__MODULE__, :test_handle_event_confirming_async, [self()]}
        )

      start_client(ctx, replication_opts: replication_opts)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])
      assert {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      # WAL produced outside the publication arrives as keepalives and must
      # keep being acknowledged past the already-confirmed transaction.
      for _ <- 1..10, do: insert_item(conn, "test value")

      confirmed =
        Enum.reduce_while(1..40, nil, fn _, _ ->
          Process.sleep(100)

          confirmed = get_confirmed_flush_lsn(conn, ctx.slot_name)

          if Lsn.to_integer(confirmed) > Lsn.to_integer(lsn1) + 1 do
            {:halt, confirmed}
          else
            {:cont, confirmed}
          end
        end)

      # If the up-to-date state were only recomputed when the notification
      # arrives, the ack would stall at exactly lsn1 + 1.
      assert Lsn.to_integer(confirmed) > Lsn.to_integer(lsn1) + 1,
             "confirmed_flush_lsn #{confirmed} stalled at the confirmed txn at #{lsn1}"
    end

    # Twin of "keepalives don't ack WAL past an unflushed txn" for a window larger than
    # 1, where the socket stays open with commits in flight. The recompute derives the
    # flag from the last ACKED commit, so without gating it re-arms while a later
    # dispatched commit is unacked and a keepalive acks straight past it.
    @tag with_empty_publication?: true
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    @tag replication_opts_overrides: [pipeline_window: 3]
    test "keepalives don't ack WAL past an in-flight txn with a window larger than 1",
         %{db_conn: conn} = ctx do
      # Arrange
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      start_supervised({MockTransactionProcessor, self()})
      pid = start_client(ctx)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])
      assert {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()
      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn1)})

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (2)", [])
      assert {lsn2, %NewRecord{record: %{"id" => "2"}}} = receive_tx_change_with_lsn()

      # Act
      MockTransactionProcessor.hold_replies(true)
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (3)", [])
      t3 = receive_held()
      lsn3 = Lsn.from_integer(t3.last_log_offset.tx_offset)

      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn2)})

      # Writes outside the publication make the walsender send keepalives with an
      # advancing wal_end.
      for _ <- 1..10, do: insert_item(conn, "test value")

      confirmed =
        Enum.reduce_while(1..20, nil, fn _, _ ->
          Process.sleep(50)
          send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn2)})
          confirmed = get_confirmed_flush_lsn(conn, ctx.slot_name)

          if Lsn.compare(confirmed, lsn3) == :gt do
            {:halt, confirmed}
          else
            {:cont, confirmed}
          end
        end)

      # Assert
      assert Lsn.compare(confirmed, lsn3) != :gt,
             "confirmed_flush_lsn #{confirmed} advanced past the in-flight txn at #{lsn3}"
    end
  end

  describe "replication heartbeat against real db" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    setup ctx do
      start_supervised!(
        Supervisor.child_spec(
          {Postgrex,
           Keyword.merge(Electric.Utils.deobfuscate_password(ctx.db_config),
             name: Electric.Connection.Manager.admin_pool(ctx.stack_id),
             types: PgInterop.Postgrex.Types
           )},
          id: :heartbeat_admin_pool
        )
      )

      start_supervised!(
        {Task.Supervisor,
         name: Electric.ProcessRegistry.name(ctx.stack_id, Electric.StackTaskSupervisor)}
      )

      :ok
    end

    @tag replication_opts_overrides: [replication_idle_timeout: 1_000]
    test "a quiet stream without heartbeats trips the idle timeout", ctx do
      pid = start_client(ctx)
      Process.unlink(pid)
      monitor = Process.monitor(pid)

      Process.sleep(1_100)
      send(pid, :check_if_idle)

      assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, {:connection_idle, _}}},
                     @assert_receive_db_timeout
    end

    @tag replication_opts_overrides: [
           replication_idle_timeout: 1_000,
           replication_heartbeat_interval: 100
         ]
    test "a quiet stream with heartbeats stays up past the idle timeout", ctx do
      pid = start_client(ctx)
      Process.unlink(pid)
      monitor = Process.monitor(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      Process.sleep(1_500)
      send(pid, :check_if_idle)

      refute_receive {:DOWN, ^monitor, :process, ^pid, _}, 500
    end

    @tag replication_opts_overrides: [replication_idle_timeout: 1_000]
    test "an idle drain outlives a stop of the stack's task supervisor", ctx do
      # Arrange
      start_supervised!({HeldWriteBuffer, {ctx.stack_id, self()}})
      pid = start_client(ctx)
      Process.unlink(pid)
      monitor = Process.monitor(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      Process.sleep(1_100)
      send(pid, :check_if_idle)
      assert_receive {:drain, drain}, @assert_receive_db_timeout

      # Act: a crash among the shape processes stops the task supervisor with them, while the
      # write buffer, supervised apart, goes on with the drain.
      {:via, _registry, task_supervisor} =
        Electric.ProcessRegistry.name(ctx.stack_id, Electric.StackTaskSupervisor)

      :ok = stop_supervised(task_supervisor)

      # Assert
      refute_receive {:DOWN, ^monitor, :process, ^pid, _}, 300
      GenServer.reply(drain, :ok)

      assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, {:connection_idle, _}}},
                     @assert_receive_db_timeout
    end

    # The idle window after the drain must outlast any delay before the next check.
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    @tag replication_opts_overrides: [replication_idle_timeout: @assert_receive_db_timeout]
    test "a transaction that arrives during the idle drain keeps the connection open",
         %{db_conn: conn} = ctx do
      # Arrange
      start_supervised!({HeldWriteBuffer, {ctx.stack_id, self()}})
      start_supervised!({MockTransactionProcessor, self()})
      pid = start_client(ctx)
      Process.unlink(pid)
      monitor = Process.monitor(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      Process.sleep(@assert_receive_db_timeout + 100)
      send(pid, :check_if_idle)
      assert_receive {:drain, drain}, @assert_receive_db_timeout

      # The collector has not acknowledged the transaction, which is what moves the idle clock.
      MockTransactionProcessor.hold_replies(true)
      insert_item(conn, "during the drain")
      assert %{changes: [%NewRecord{record: %{"value" => "during the drain"}}]} = receive_held()

      # Act: the next idle check comes once the client has seen the drain end, since a check that
      # comes while a drain runs does nothing.
      GenServer.reply(drain, :ok)
      assert Support.TestUtils.wait_until(fn -> idle_drain(pid) == nil end)
      send(pid, :check_if_idle)

      # Assert: the next idle check, with the transaction still unacknowledged, starts no drain.
      assert idle_drain(pid) == nil
      refute_receive {:drain, _}
      refute_received {:DOWN, ^monitor, :process, ^pid, _}
    end

    @tag replication_opts_overrides: [replication_heartbeat_interval: 100]
    test "messages from other producers reach no handler and leave the stream intact",
         %{db_conn: conn} = ctx do
      start_client(ctx)

      Postgrex.transaction(conn, fn connection ->
        Postgrex.query!(connection, "SELECT pg_logical_emit_message(true, 'other', 'inside')", [])
        insert_item(connection, "with a message")
      end)

      assert %NewRecord{record: %{"value" => "with a message"}} = receive_tx_change()

      Postgrex.query!(conn, "SELECT pg_logical_emit_message(true, 'other', 'alone')", [])
      assert {_lsn, []} = receive_transaction()

      Postgrex.query!(conn, "SELECT pg_logical_emit_message(false, 'other', 'outside')", [])
      insert_item(conn, "after a message")

      assert %NewRecord{record: %{"value" => "after a message"}} = receive_tx_change()
      refute_receive {:from_replication, _}, 300
    end

    @tag replication_opts_overrides: [replication_heartbeat_interval: 100]
    test "a heartbeat that cannot be written is logged and streaming goes on",
         %{db_conn: conn} = ctx do
      start_client(ctx)
      :ok = stop_supervised(:heartbeat_admin_pool)

      log =
        capture_log(fn ->
          Process.sleep(300)
          insert_item(conn, "without heartbeats")
          assert %NewRecord{record: %{"value" => "without heartbeats"}} = receive_tx_change()
        end)

      assert log =~ "Failed to emit a replication heartbeat"
    end
  end

  describe "ReplicationClient against real db (ack-after-journal)" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    # Journal-on twin of "flushes are not advancing if something isn't actually
    # getting flushed": the same stimulus — a streamed txn whose flush is never
    # confirmed — must advance the slot, because the ack is driven by the journal
    # fsync at decode time, not by shape flushes.
    @tag :tmp_dir
    @tag with_empty_publication?: true
    @tag replication_opts_overrides: [ack_after_journal: true]
    test "acks the slot at the journal boundary without any flush confirmation",
         %{db_conn: conn, tmp_dir: tmp_dir} = ctx do
      # Arrange
      Electric.StackConfig.put(
        ctx.stack_id,
        Electric.ShapeCache.Storage,
        {Electric.ShapeCache.PureFileStorage, %{base_path: tmp_dir}}
      )

      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      start_client(ctx)

      # Act
      streamed =
        Enum.map(1..3, fn id ->
          Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES ($1)", [id])
          id_str = to_string(id)
          assert {lsn, %NewRecord{record: %{"id" => ^id_str}}} = receive_tx_change_with_lsn()
          Lsn.to_integer(lsn)
        end)

      last_streamed = List.last(streamed)

      confirmed =
        Enum.reduce_while(1..40, nil, fn _, _ ->
          Process.sleep(50)
          confirmed = get_confirmed_flush_lsn(conn, ctx.slot_name)

          if Lsn.to_integer(confirmed) > last_streamed do
            {:halt, confirmed}
          else
            {:cont, confirmed}
          end
        end)

      # Assert
      assert Lsn.to_integer(confirmed) > last_streamed,
             "confirmed_flush_lsn #{confirmed} did not advance past the journaled txn at #{last_streamed}"

      # Assert: the ack is backed by the journal. Reading the generations back off disk is
      # the same path boot replay takes, so an unreadable frame fails here too.
      gens = TxnJournal.list_generations(tmp_dir)

      journaled =
        TxnJournal.stream_events!(tmp_dir, gens)
        |> Enum.flat_map(fn
          %TransactionFragment{commit: commit, last_log_offset: offset} when commit != nil ->
            [offset.tx_offset]

          _event ->
            []
        end)

      assert journaled == streamed

      # A keepalive over the drained pipeline may have widened the boundary past the last
      # commit with an idle marker; it must never sit below what was acked.
      assert TxnJournal.last_commit_tx_offset(tmp_dir, gens) >= last_streamed
      assert TxnJournal.last_commit_tx_offset(tmp_dir, gens) >= Lsn.to_integer(confirmed) - 1
    end

    # pg_stat_replication write/flush lag for this connection is the telemetry an operator
    # reads to judge this feature, and a flush position ahead of the write position makes it
    # nonsensical. The journal is both boundaries.
    @tag :tmp_dir
    @tag with_empty_publication?: true
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    @tag replication_opts_overrides: [ack_after_journal: true]
    test "never reports a flush position ahead of its own write position",
         %{db_conn: conn, tmp_dir: tmp_dir} = ctx do
      # Arrange
      Electric.StackConfig.put(
        ctx.stack_id,
        Electric.ShapeCache.Storage,
        {Electric.ShapeCache.PureFileStorage, %{base_path: tmp_dir}}
      )

      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      start_supervised({MockTransactionProcessor, self()})
      start_client(ctx)

      # Held replies keep the fan-out ack — the only other thing that advances the write
      # position — from running, so the journal is on its own.
      MockTransactionProcessor.hold_replies(true)

      # Act
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])
      assert %TransactionFragment{} = receive_held()

      # Assert
      {write_lsn, flush_lsn} =
        Enum.reduce_while(1..40, {nil, nil}, fn _, _ ->
          Process.sleep(50)

          case walsender_positions(conn, ctx.slot_name) do
            {nil, _} ->
              {:cont, {nil, nil}}

            {_write, flush} = positions ->
              if flush, do: {:halt, positions}, else: {:cont, positions}
          end
        end)

      assert write_lsn != nil, "the walsender never reported a position"

      assert Lsn.compare(write_lsn, flush_lsn) != :lt,
             "reported flush position #{flush_lsn} runs ahead of the write position #{write_lsn}"
    end

    # A collapse arms the replay queue, and every event decoded after it queues there
    # while strictly older events sit in the journal backlog. Draining replay first
    # would deliver the newer transaction ahead of them, and the collector drops
    # anything at or behind its last processed offset — losing them for good, since
    # the slot and the journal GC have already advanced past their commits.
    @tag :tmp_dir
    @tag with_empty_publication?: true
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    # The premise -- the window is closed while the first event is in flight -- holds only at
    # a window of one, so it is pinned here rather than inherited from the environment.
    @tag replication_opts_overrides: [ack_after_journal: true, pipeline_window: 1]
    test "dispatches the journal backlog in WAL order after a pipeline collapse",
         %{db_conn: conn, tmp_dir: tmp_dir} = ctx do
      # Arrange
      Electric.StackConfig.put(
        ctx.stack_id,
        Electric.ShapeCache.Storage,
        {Electric.ShapeCache.PureFileStorage, %{base_path: tmp_dir}}
      )

      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      start_supervised({MockTransactionProcessor, self()})
      start_client(ctx)

      MockTransactionProcessor.hold_replies(true)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])
      assert %TransactionFragment{} = receive_held()

      # The window is closed while the first event is in flight, so these land in the
      # journal backlog rather than being dispatched.
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (2)", [])
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (3)", [])
      refute_receive {:held, _}, 300

      # Act
      Process.exit(GenServer.whereis(MockTransactionProcessor), :kill)
      stop_supervised(MockTransactionProcessor)
      start_supervised!({MockTransactionProcessor, self()})
      MockTransactionProcessor.hold_replies(true)
      assert %TransactionFragment{} = receive_held()

      # Decoded after the collapse: strictly newer than everything in the backlog.
      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (4)", [])
      Process.sleep(300)

      MockTransactionProcessor.hold_replies(false)
      MockTransactionProcessor.release_held()

      # Assert
      assert %NewRecord{record: %{"id" => "2"}} = receive_tx_change()
      assert %NewRecord{record: %{"id" => "3"}} = receive_tx_change()
      assert %NewRecord{record: %{"id" => "4"}} = receive_tx_change()
    end

    # When the storage cannot host a journal the mode must disable itself
    # wholesale: half-journal flow control without journal durability would
    # advance the slot past WAL nothing has durably stored.
    @tag with_empty_publication?: true
    @tag replication_opts_overrides: [ack_after_journal: true]
    test "falls back to flush-confirmed acks when storage cannot host a journal",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      pid = start_client(ctx)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])

      assert {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      confirmed_flush_lsn1 = get_confirmed_flush_lsn(conn, ctx.slot_name)

      assert Lsn.to_integer(confirmed_flush_lsn1) < Lsn.to_integer(lsn1)

      for _ <- 1..10 do
        insert_item(conn, "test value")
      end

      send(pid, {:flush_boundary_updated, 100})

      # This is to pass the time instead of a sleep - if PG is responsive, it probably has processed the wal acknowledge too
      Postgrex.query!(conn, "SELECT * FROM items", [])

      # Still same LSN
      assert confirmed_flush_lsn1 ==
               get_confirmed_flush_lsn(conn, ctx.slot_name)
    end
  end

  # The idle drain the client runs, read from its state after every message sent to it so far.
  defp idle_drain(pid) do
    {_, %{state: {ReplicationClient, %ReplicationClient.State{idle_drain: idle_drain}}}} =
      :sys.get_state(pid)

    idle_drain
  end

  defp get_confirmed_flush_lsn(conn, slot_name) do
    %Postgrex.Result{rows: [[confirmed_flush_lsn]]} =
      Postgrex.query!(
        conn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
        [slot_name]
      )

    confirmed_flush_lsn
  end

  defp walsender_positions(conn, slot_name) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT s.write_lsn, s.flush_lsn
        FROM pg_stat_replication s
        JOIN pg_replication_slots r ON r.active_pid = s.pid
        WHERE r.slot_name = $1
        """,
        [slot_name]
      )

    case rows do
      [[write_lsn, flush_lsn]] -> {write_lsn, flush_lsn}
      [] -> {nil, nil}
    end
  end

  describe "ReplicationClient against real db (toast)" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    setup %{db_conn: conn} = ctx do
      Postgrex.query!(
        conn,
        "CREATE TABLE items2 (id UUID PRIMARY KEY, val1 TEXT, val2 TEXT, num INTEGER)",
        []
      )

      Postgrex.query!(conn, "ALTER TABLE items2 REPLICA IDENTITY FULL", [])

      start_client(ctx)

      :ok
    end

    test "detoasts column values in deletes", %{db_conn: conn} do
      {id, bin_uuid} = gen_uuid()
      long_string_1 = gen_random_string(2500)
      long_string_2 = gen_random_string(3000)

      Postgrex.query!(conn, "INSERT INTO items2 (id, val1, val2) VALUES ($1, $2, $3)", [
        bin_uuid,
        long_string_1,
        long_string_2
      ])

      assert %NewRecord{
               record: %{"id" => ^id, "val1" => ^long_string_1, "val2" => ^long_string_2},
               relation: {"public", "items2"}
             } = receive_tx_change()

      Postgrex.query!(conn, "DELETE FROM items2 WHERE id = $1", [bin_uuid])

      assert %DeletedRecord{
               old_record: %{"id" => ^id, "val1" => ^long_string_1, "val2" => ^long_string_2},
               relation: {"public", "items2"}
             } = receive_tx_change()
    end

    test "detoasts column values in updates", %{db_conn: conn} do
      {id, bin_uuid} = gen_uuid()
      long_string_1 = gen_random_string(2500)
      long_string_2 = gen_random_string(3000)

      Postgrex.query!(conn, "INSERT INTO items2 (id, val1, val2) VALUES ($1, $2, $3)", [
        bin_uuid,
        long_string_1,
        long_string_2
      ])

      assert %NewRecord{
               record: %{"id" => ^id, "val1" => ^long_string_1, "val2" => ^long_string_2},
               relation: {"public", "items2"}
             } = receive_tx_change()

      Postgrex.query!(conn, "UPDATE items2 SET num = 11 WHERE id = $1", [bin_uuid])

      assert %UpdatedRecord{
               record: %{
                 "id" => ^id,
                 "val1" => ^long_string_1,
                 "val2" => ^long_string_2,
                 "num" => "11"
               },
               changed_columns: changed_columns,
               relation: {"public", "items2"}
             } = receive_tx_change()

      assert MapSet.new(["num"]) == changed_columns
    end
  end

  describe "ReplicationClient against real db (relation stream)" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    setup ctx do
      start_client(ctx)
      :ok
    end

    test "column changes decoded together cost one classification, not one per statement",
         %{db_conn: conn} do
      # Arrange
      insert_item(conn, "before the migrations")
      registration = receive_relations_before_change()
      assert [%Relation{schema: "public", table: "items"}] = registration

      # Act
      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN note TEXT", [])
      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN rank INTEGER", [])
      insert_item(conn, "after the migrations")
      after_migrations = receive_relations_before_change()

      # Assert
      assert [{:new, _} | rest] = classify(registration ++ after_migrations)
      assert [{:changed, changed}] = Enum.reject(rest, &match?({:unchanged, _}, &1))
      assert MapSet.new(changed.affected_columns) == MapSet.new(["note", "rank"])
    end

    test "a write between two column changes pays a classification for each",
         %{db_conn: conn} do
      # Arrange
      insert_item(conn, "before the migrations")
      registration = receive_relations_before_change()

      # Act
      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN note TEXT", [])
      insert_item(conn, "between the migrations")
      between = receive_relations_before_change()

      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN rank INTEGER", [])
      insert_item(conn, "after the migrations")
      after_migrations = receive_relations_before_change()

      # Assert
      classified = classify(registration ++ between ++ after_migrations)

      assert [{:new, _}, {:changed, first}, {:changed, second}] =
               Enum.reject(classified, &match?({:unchanged, _}, &1))

      assert first.affected_columns == ["note"]
      assert second.affected_columns == ["rank"]
    end

    test "an index built on a live table is not classified as a change", %{db_conn: conn} do
      # Arrange
      insert_item(conn, "before the index")
      registration = receive_relations_before_change()

      # Act
      Postgrex.query!(conn, "CREATE INDEX items_value_idx ON items (value)", [])
      insert_item(conn, "after the index")
      after_index = receive_relations_before_change()

      # Assert
      assert [%Relation{table: "items"}] = after_index
      assert [{:new, _}, {:unchanged, _}] = classify(registration ++ after_index)
    end
  end

  describe "ReplicationClient lock handling" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    test "should acquire an advisory lock on startup", ctx do
      log =
        capture_log(fn ->
          start_client(ctx)
          assert_receive :lock_acquired
        end)

      # should have logged lock acquisition process
      lock_name = ctx.slot_name
      assert log =~ "Acquiring lock from postgres with name #{lock_name}"
      assert log =~ "Lock acquired from postgres with name #{lock_name}"

      # should have acquired an advisory lock on PG
      assert %Postgrex.Result{rows: [[false]]} =
               Postgrex.query!(
                 ctx.db_conn,
                 "SELECT pg_try_advisory_lock(hashtext('#{lock_name}'))",
                 []
               )
    end

    test "should wait if lock is already acquired", ctx do
      # grab lock with one connection
      start_client(ctx, id: :lock_client1)

      assert_receive :lock_acquired

      # try to grab the same lock using a different connection
      new_stack_id = ctx.stack_id <> "_new"
      start_link_supervised!({Electric.ProcessRegistry, stack_id: new_stack_id})

      start_link_supervised!(
        Supervisor.child_spec({Electric.StackConfig, stack_id: new_stack_id},
          id: :second_stack_config
        )
      )

      with_lsn_tracker(%{ctx | stack_id: new_stack_id})

      start_client(
        ctx,
        id: :lock_client2,
        stack_id: new_stack_id,
        replication_opts: Keyword.put(ctx.replication_opts, :stack_id, new_stack_id),
        wait_for_start: false
      )

      # should fail to grab it
      refute_receive {:lock_acquisition_failed, _}, 1000

      # should immediately grab it once previous lock is released
      stop_supervised!(:lock_client1)
      assert_receive :lock_acquired
      stop_supervised(:lock_client2)
    end

    test "should exit if timed out on connection ", ctx do
      client_pid = start_client(ctx, timeout: 1, wait_for_start: false)
      Process.unlink(client_pid)

      ref = Process.monitor(client_pid)

      assert_receive {:DOWN, ^ref, _, _, %DBConnection.ConnectionError{message: message}}

      assert message =~ "tcp"
    end
  end

  test "correctly responds to a status update request message from PG", ctx do
    state =
      ReplicationClient.State.new(
        stack_id: ctx.stack_id,
        handle_event: nil,
        publication_name: "",
        try_creating_publication?: false,
        slot_name: "",
        connection_manager: ctx.connection_manager
      )

    state = %{state | received_wal: lsn_to_wal("0/0"), flushed_wal: lsn_to_wal("0/0")}
    pg_wal = lsn_to_wal("0/10")

    assert {:noreply,
            [<<?r, received_wal::64, flushed_wal::64, flushed_wal::64, _time::64, 0::8>>], state} =
             ReplicationClient.handle_data(<<?k, pg_wal::64, 0::64, 1::8>>, state)

    assert state.received_wal == pg_wal - 1
    assert received_wal == state.received_wal + 1
    assert flushed_wal == state.flushed_wal + 1
  end

  defp with_publication(%{db_conn: conn, slot_name: slot_name} = ctx) do
    if Map.get(ctx, :with_empty_publication?, false) do
      Postgrex.query!(conn, "CREATE PUBLICATION #{slot_name}", [])
    else
      Postgrex.query!(conn, "CREATE PUBLICATION #{slot_name} FOR ALL TABLES", [])
    end

    :ok
  end

  defp with_replication_opts(ctx) do
    %{
      replication_opts:
        Keyword.merge(
          [
            connection_opts: ctx.db_config,
            stack_id: ctx.stack_id,
            publication_name: ctx.slot_name,
            try_creating_publication?: false,
            slot_name: ctx.slot_name,
            handle_event:
              Map.get(
                ctx,
                :handle_event,
                {__MODULE__, :test_handle_event_async, [self()]}
              ),
            connection_manager: ctx.connection_manager
          ],
          List.wrap(ctx[:replication_opts_overrides])
        )
    }
  end

  # Special handling for the items table to enable testing of various edge cases that depend on the result of transaction processing.
  def test_handle_event(
        %TransactionFragment{
          changes: [%NewRecord{relation: {"public", "items"}} = change]
        } = txn_fragment,
        test_pid
      ) do
    case Map.fetch!(change.record, "value") do
      "interrupt #PID" <> pid_str ->
        pid = pid_str |> String.to_charlist() |> :erlang.list_to_pid()

        if pid == self() do
          raise "Interrupting transaction processing abnormally"
        else
          send(test_pid, {:from_replication, txn_fragment})
          :ok
        end

      _ ->
        send(test_pid, {:from_replication, txn_fragment})
        :ok
    end
  end

  def test_handle_event(%TransactionFragment{} = txn_fragment, test_pid) do
    send(test_pid, {:from_replication, txn_fragment})
    :ok
  end

  def test_handle_event(%Relation{} = relation, test_pid) do
    send(test_pid, {:from_replication, [relation]})
    :ok
  end

  # Async variant for the $gen_call-based apply_event. Runs the handler inline
  # (in the gen_statem process) and queues the result as a {ref, result} message.
  # Since these test handlers are fast (no delay), this doesn't block keepalives.
  # Like test_handle_event_async, but confirms a committed transaction's flush
  # boundary from within event processing, before replying — mirroring the
  # ShapeLogCollector's behaviour for a transaction that affects no shapes.
  # Runs inside the replication client process, so send(self(), ...) puts the
  # notification in the client's mailbox ahead of the handler's reply.
  def test_handle_event_confirming_async(
        %TransactionFragment{commit: commit, lsn: lsn} = event,
        test_pid
      )
      when not is_nil(commit) do
    send(self(), {:flush_boundary_updated, Lsn.to_integer(lsn)})
    test_handle_event_async(event, test_pid)
  end

  def test_handle_event_confirming_async(event, test_pid),
    do: test_handle_event_async(event, test_pid)

  def test_handle_event_async(event, test_pid) do
    ref = make_ref()

    try do
      result = test_handle_event(event, test_pid)
      send(self(), {ref, result})
    catch
      kind, reason ->
        send(self(), {:DOWN, ref, :process, self(), {kind, reason, __STACKTRACE__}})
    end

    ref
  end

  defp gen_random_string(length) do
    Stream.repeatedly(fn -> :rand.uniform(125 - 32) + 32 end)
    |> Enum.take(length)
    |> List.to_string()
  end

  defp lsn_to_wal(lsn_str) when is_binary(lsn_str),
    do: lsn_str |> Lsn.from_string() |> Lsn.to_integer()

  defp fetch_slot_info(conn, target_slot_name) do
    %Postgrex.Result{columns: cols, rows: rows} =
      Postgrex.query!(conn, "SELECT * FROM pg_replication_slots", [])

    [row] = Enum.filter(rows, fn [slot_name | _] -> slot_name == target_slot_name end)

    Enum.zip(cols, row) |> Map.new()
  end

  defp insert_item(conn, val) do
    Postgrex.query!(conn, "INSERT INTO items (id, value) VALUES ($1, $2)", [
      Ecto.UUID.bingenerate(),
      val
    ])
  end

  defp gen_uuid do
    id = Ecto.UUID.generate()
    {:ok, bin_uuid} = Ecto.UUID.dump(id)
    {id, bin_uuid}
  end

  # Relation messages precede the changes they describe, so a transaction arriving
  # marks the end of the batch the preceding DDL produced.
  defp receive_relations_before_change(acc \\ [], waited \\ 0) do
    receive do
      {:from_replication, [%Relation{} = relation]} ->
        receive_relations_before_change([relation | acc])

      {:from_replication, %TransactionFragment{}} ->
        Enum.reverse(acc)
    after
      @walsender_prompt_interval ->
        waited =
          keep_waiting_for_stream(waited, "Expected a transaction to close the relation batch")

        receive_relations_before_change(acc, waited)
    end
  end

  defp classify(relations) do
    {:ok, initial_state} = AffectedColumns.init(%{id_to_table_info: %{}, table_to_id: %{}})

    {classified, _state} =
      Enum.map_reduce(relations, initial_state, fn relation, state ->
        {classification, transformed, state} =
          AffectedColumns.transform_relation(relation, state)

        {{classification, transformed}, state}
      end)

    classified
  end

  defp receive_tx_change do
    {_lsn, change} = receive_tx_change_with_lsn()
    change
  end

  defp receive_tx_change_with_lsn do
    {lsn, [change]} = receive_transaction()
    {lsn, change}
  end

  defp receive_transaction do
    txn = receive_transaction_entity()
    {txn.lsn, txn.changes}
  end

  defp receive_transaction_entity(builder \\ TransactionBuilder.new(), waited \\ 0) do
    receive do
      {:from_replication, %TransactionFragment{} = txn_fragment} ->
        case TransactionBuilder.build(txn_fragment, builder) do
          {[], builder} ->
            receive_transaction_entity(builder)

          {[txn], _builder} ->
            txn
        end

      # Discard Relation messages - they're not relevant for transaction tests
      {:from_replication, [%Relation{} | _]} ->
        receive_transaction_entity(builder)
    after
      @walsender_prompt_interval ->
        waited = keep_waiting_for_stream(waited, "Expected transaction")
        receive_transaction_entity(builder, waited)
    end
  end

  defp receive_held(waited \\ 0) do
    receive do
      {:held, txn_fragment} -> txn_fragment
    after
      @walsender_prompt_interval ->
        waited = keep_waiting_for_stream(waited, "Expected a held transaction")
        receive_held(waited)
    end
  end

  defp receive_exit_reason(monitor, waited \\ 0) do
    receive do
      {:DOWN, ^monitor, :process, _pid, reason} -> reason
    after
      @walsender_prompt_interval ->
        waited = keep_waiting_for_stream(waited, "Expected the client to exit")
        receive_exit_reason(monitor, waited)
    end
  end

  defp keep_waiting_for_stream(waited, failure) do
    waited = waited + @walsender_prompt_interval

    if waited >= @assert_receive_db_timeout, do: raise(failure)

    case Process.get({__MODULE__, :client}) do
      nil -> :ok
      client -> send(client, :send_keepalive)
    end

    waited
  end

  defp put_electric_env(key, value) do
    previous = Application.fetch_env(:electric, key)
    Application.put_env(:electric, key, value)

    on_exit(fn ->
      case previous do
        {:ok, previous} -> Application.put_env(:electric, key, previous)
        :error -> Application.delete_env(:electric, key)
      end
    end)
  end

  defp start_client(ctx, overrides \\ []) do
    ctx = Enum.into(overrides, ctx)

    client_pid =
      start_link_supervised!(%{
        id: ctx[:id] || ReplicationClient,
        start:
          {ReplicationClient, :start_link,
           [
             [
               stack_id: ctx.stack_id,
               replication_opts: ctx.replication_opts,
               timeout: Map.get(ctx, :timeout, nil)
             ]
           ]},
        restart: :temporary
      })

    Process.put({__MODULE__, :client}, client_pid)

    conn_mgr = ctx.connection_manager

    if Map.get(ctx, :wait_for_start, true) do
      assert_receive {^conn_mgr, :streaming_started}, @assert_receive_db_timeout
    end

    client_pid
  end
end
