defmodule Electric.Connection.ConnectionManagerTest do
  use ExUnit.Case
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup

  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.PersistentReplicationState
  alias Electric.Replication.PublicationManager.RelationTracker
  alias Electric.Connection
  alias Electric.StatusMonitor
  alias Electric.Postgres.Inspector.EtsInspector
  alias Electric.Timeline
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  @stub_inspector Support.StubInspector.new(
                    tables: [{1, {"public", "items"}}],
                    columns: [
                      %{
                        name: "id",
                        type: "int8",
                        type_id: {20, 1},
                        pk_position: 0,
                        is_generated: false
                      },
                      %{name: "value", type: "text", type_id: {25, 1}, is_generated: false}
                    ]
                  )

  setup [
    :with_unique_db,
    :with_stack_id_from_test,
    :with_status_monitor,
    :with_persistent_kv,
    :with_inspector,
    :with_slot_name,
    :with_in_memory_storage,
    :with_shape_status
  ]

  defp start_connection_manager(%{stack_id: stack_id} = ctx) do
    stack_events_registry = Electric.stack_events_registry()
    publication_name = "electric_conn_mgr_test_pub_#{:erlang.phash2(stack_id)}"
    connection_opts = Map.get(ctx, :pooled_connection_opts, ctx.db_config)

    replication_connection_opts = Map.get(ctx, :replication_connection_opts, ctx.db_config)

    replication_opts = [
      stack_id: stack_id,
      connection_opts: replication_connection_opts,
      persistent_kv: ctx.persistent_kv,
      slot_name: ctx.slot_name,
      publication_name: publication_name,
      try_creating_publication?: true,
      slot_temporary?: Map.get(ctx, :slot_temporary?, true),
      handle_event: nil,
      replication_idle_timeout: Map.get(ctx, :replication_idle_timeout, 0)
    ]

    connection_manager_opts = [
      stack_id: stack_id,
      connection_opts: connection_opts,
      replication_opts: replication_opts,
      pool_opts: [pool_size: 2],
      connection_backoff:
        Map.get(ctx, :connection_backoff, Connection.Manager.ConnectionBackoff.init(50, 50)),
      timeline_opts: [stack_id: stack_id, persistent_kv: ctx.persistent_kv],
      inspector: ctx.inspector,
      shape_cache_opts: [
        stack_id: stack_id
      ],
      tweaks: [],
      max_shapes: nil,
      persistent_kv: ctx.persistent_kv,
      stack_events_registry: stack_events_registry,
      lock_breaker_guard: ctx[:lock_breaker_guard],
      connection_status_check_interval: Map.get(ctx, :connection_status_check_interval, 10_000),
      manual_table_publishing?: Map.get(ctx, :manual_table_publishing?, false)
    ]

    core_sup =
      start_link_supervised!(
        {Electric.CoreSupervisor,
         stack_id: stack_id, connection_manager_opts: connection_manager_opts},
        # The test supervisor under which this one is started has `auto_shutdown` set to
        # `:never`, so we need to make sure the core supervisor is not a significant
        # child, otherwise we'd get the following error:
        #
        #     ** (RuntimeError) failed to start child with the spec {Electric.CoreSupervisor, [...]}.
        #      Reason: bad child specification, got: {:bad_combination, [auto_shutdown: :never, significant: true]}
        significant: false
      )

    Registry.register(stack_events_registry, {:stack_status, stack_id}, nil)

    %{conn_sup: core_sup, connection_opts: connection_opts, replication_opts: replication_opts}
  end

  defp unresponsive_port(_ctx) do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    [unresponsive_port: port]
  end

  describe "manual table publishing" do
    setup ctx do
      # The operator's publication, which does not publish TRUNCATE.
      publication_name = "electric_conn_mgr_test_pub_#{:erlang.phash2(ctx.stack_id)}"

      Postgrex.query!(
        ctx.db_conn,
        "CREATE PUBLICATION #{publication_name} WITH (publish = 'insert, update, delete')",
        []
      )

      # The configuration error stops the stack, which must not take the test process down.
      Process.flag(:trap_exit, true)

      %{manual_table_publishing?: true, publication_name: publication_name}
    end

    setup [:start_connection_manager]

    test "reports a publication missing an operation instead of repairing it", ctx do
      assert_receive {:stack_status, _ref, {:config_error, %{message: message}}}, 10_000
      assert message =~ "does not publish all required operations"

      assert %{rows: [[false]]} =
               Postgrex.query!(
                 ctx.db_conn,
                 "SELECT pubtruncate FROM pg_publication WHERE pubname = $1",
                 [ctx.publication_name]
               )
    end
  end

  describe "connection status check interval" do
    setup [:start_connection_manager]

    @tag connection_status_check_interval: 50
    test "is configurable via opts", %{stack_id: stack_id} do
      manager = stack_id |> Connection.Manager.name() |> GenServer.whereis()
      assert :sys.get_state(manager).connection_status_check_interval == 50
    end
  end

  describe "self-healing stuck slot creation" do
    @tag connection_status_check_interval: 50
    test "emits a one-time notice and keeps waiting when the function is not permitted",
         %{stack_id: stack_id} = ctx do
      parent = self()

      # Intercept only the pg_log_standby_snapshot() call (issued from the manager
      # process); pass every other Postgrex query through unchanged.
      Repatch.patch(Postgrex, :query, [mode: :shared], fn
        _pool, "SELECT pg_log_standby_snapshot()", _params, _opts ->
          {:error,
           %Postgrex.Error{
             postgres: %{
               code: :insufficient_privilege,
               severity: "ERROR",
               message: "permission denied"
             }
           }}

        pool, query, params, opts ->
          Repatch.real(Postgrex, :query, [pool, query, params, opts])
      end)

      # Allowance doesn't follow the supervision tree, so allow the manager process
      # to see the shared patch as soon as it has started.
      spawn_link(fn ->
        Stream.repeatedly(fn -> 0 end)
        |> Enum.reduce_while(0, fn _, _ ->
          case GenServer.whereis(Electric.Connection.Manager.name(stack_id)) do
            nil ->
              {:cont, 0}

            pid ->
              Repatch.allow(parent, pid)
              {:halt, 0}
          end
        end)
      end)

      # Hold an xid-bearing transaction open so CREATE_REPLICATION_SLOT blocks.
      # db_config carries an obfuscated password, so deobfuscate it like the test
      # support does before opening a raw connection.
      {:ok, blocker} = Postgrex.start_link(Electric.Utils.deobfuscate_password(ctx.db_config))
      Postgrex.query!(blocker, "BEGIN", [])
      Postgrex.query!(blocker, "SELECT txid_current()", [])

      start_connection_manager(ctx)

      # Connection bring-up + lock acquisition is the slow, load-sensitive part. Wait for
      # it via StatusMonitor (the source sits at conn: :starting while blocked on slot
      # creation) rather than a multi-second assert_receive, so the assertions below can
      # use short, default timeouts.
      wait_until_conn_starting(stack_id)

      assert_receive {:stack_status, _,
                      :replication_slot_creation_blocked_by_pending_transactions}

      # The notice fires once despite many timer ticks (interval = 50 ms).
      assert_receive {:stack_status, _,
                      {:replication_slot_unblock_unavailable,
                       %{
                         message:
                           "Replication slot creation is blocked by a pending transaction. " <>
                             "Electric might not be able to automatically unblock it" <> _
                       }}}

      refute_receive {:stack_status, _, {:replication_slot_unblock_unavailable, _}}, 400

      Postgrex.query!(blocker, "COMMIT", [])
    end
  end

  describe "status monitor" do
    setup [:start_connection_manager]

    test "reports status=waiting initially", %{stack_id: stack_id} do
      assert StatusMonitor.status(stack_id).conn == :waiting_on_lock
    end

    test "reports status=starting once the exclusive connection lock is acquired", %{
      stack_id: stack_id
    } do
      assert_receive {:stack_status, _, :waiting_for_connection_lock}
      assert_receive {:stack_status, _, :connection_lock_acquired}
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)
      assert StatusMonitor.status(stack_id).conn == :starting
    end

    test "reports status=active when all connection processes are running", %{stack_id: stack_id} do
      wait_until_active(stack_id)
    end

    test "backtracks the status when the replication client goes down", %{stack_id: stack_id} do
      wait_until_active(stack_id)

      monitor = monitor_replication_client(stack_id)

      :ok = GenServer.stop(Electric.Postgres.ReplicationClient.name(stack_id), :shutdown)

      assert_receive {:DOWN, ^monitor, :process, _pid, :shutdown}
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      status = StatusMonitor.status(stack_id)
      assert status.conn in [:waiting_on_lock, :starting]
    end

    test "resets the status when connection manager goes down", %{stack_id: stack_id} = ctx do
      wait_until_active(stack_id)

      # Start another lock process so that when ConnectionManager exits it is not able to restore its readiness immediately.
      new_stack_id = stack_id <> "_new"
      _registry = start_link_supervised!({Electric.ProcessRegistry, stack_id: new_stack_id})

      test_pid = self()

      start_supervised!({
        Task,
        fn ->
          DBConnection.run(ctx.db_conn, fn conn ->
            Postgrex.query!(
              conn,
              "SELECT pg_advisory_lock(hashtext('#{ctx.slot_name}'))",
              []
            )

            send(test_pid, :test_lock_acquired)

            Process.sleep(:infinity)
          end)
        end
      })

      monitor = monitor_replication_client(stack_id)

      :ok =
        Supervisor.terminate_child(
          Connection.Manager.Supervisor.name(stack_id),
          Connection.Manager
        )

      assert_receive {:DOWN, ^monitor, :process, _pid, :shutdown}
      assert_receive :test_lock_acquired
      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      assert StatusMonitor.status(stack_id).conn == :waiting_on_lock
    end

    test "backtracks the status when the shape log collector goes down", %{stack_id: stack_id} do
      wait_until_active(stack_id)

      :ok = GenServer.stop(ShapeLogCollector.name(stack_id), :shutdown)

      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      status = StatusMonitor.status(stack_id)

      # shape_metadata_ready survives (ShapeStatusOwner is alive), so shape is :read_only not :starting
      assert status.shape == :read_only
    end

    test "backtracks the status when the shape cache goes down", %{stack_id: stack_id} do
      wait_until_active(stack_id)

      # should backtrack the status by virtue of the shape log collector being shut down
      # by the replication supervisor
      monitor = Electric.Replication.ShapeLogCollector.monitor(stack_id)

      :ok = GenServer.stop(Electric.ShapeCache.name(stack_id), :shutdown)

      assert_receive {:DOWN, ^monitor, :process, _pid, :shutdown}

      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      status = StatusMonitor.status(stack_id)
      assert status.shape == :read_only
    end

    test "backtracks the status when the canary goes down", %{stack_id: stack_id} do
      wait_until_active(stack_id)

      # should backtrack the status by virtue of the shape log collector being shut down
      # by the replication supervisor
      monitor =
        stack_id
        |> Electric.Shapes.Supervisor.canary_name()
        |> GenServer.whereis()
        |> Process.monitor()

      :ok = GenServer.stop(Electric.Shapes.Supervisor.canary_name(stack_id), :shutdown)

      assert_receive {:DOWN, ^monitor, :process, _pid, :shutdown}

      StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      status = StatusMonitor.status(stack_id)
      assert status.shape == :read_only
    end
  end

  describe "process dependencies" do
    setup [:start_connection_manager]

    # https://github.com/electric-sql/electric/issues/3018
    test "handles status messages after shape cache exit", ctx do
      %{stack_id: stack_id} = ctx

      wait_until_active(stack_id)
      # The shape cache's exit stops the core, which the test process is linked to.
      Process.flag(:trap_exit, true)

      shape_cache_pid = stack_id |> Electric.ShapeCache.name() |> GenServer.whereis()
      assert Process.alive?(shape_cache_pid)

      manager_pid = GenServer.whereis(Electric.Connection.Manager.name(stack_id))
      ref = Process.monitor(manager_pid)

      Process.exit(shape_cache_pid, {:error, :reason})

      # The manager is stopped with the core rather than crashing on the status messages.
      assert_receive {:DOWN, ^ref, :process, ^manager_pid, :shutdown}, 5_000
    end

    # https://github.com/electric-sql/electric/issues/3630
    # A child started again in place would lose what it and its siblings had not committed, so
    # the first exit of any of them stops the shape subsystem, and the core with it.
    for child <- [
          Task.Supervisor,
          Electric.Replication.ShapeLogCollector.Supervisor,
          Electric.Replication.PublicationManager.Supervisor,
          Electric.Shapes.DynamicConsumerSupervisor,
          Electric.ShapeCache,
          Electric.ShapeCache.ExpiryManager,
          Electric.Replication.SchemaReconciler,
          Electric.Shapes.Supervisor.Canary
        ] do
      test "shuts the core down on the first exit of #{inspect(child)}", ctx do
        %{stack_id: stack_id, conn_sup: core_sup} = ctx

        wait_until_active(stack_id)
        Process.flag(:trap_exit, true)

        # A child is named by its id, or by its module when its id is its registered name.
        assert {_, pid, _, _} =
                 stack_id
                 |> Electric.Shapes.Supervisor.name()
                 |> Supervisor.which_children()
                 |> Enum.find(fn {id, _, _, modules} ->
                   id == unquote(child) or unquote(child) in List.wrap(modules)
                 end)

        Process.exit(pid, :kill)

        assert_receive {:EXIT, ^core_sup, :shutdown}, 5_000
      end
    end

    test "keeps the core up when the shape subsystem is stopped on purpose", ctx do
      %{stack_id: stack_id, conn_sup: core_sup} = ctx

      wait_until_active(stack_id)

      assert :ok = Electric.CoreSupervisor.stop_shapes_supervisor(stack_id: stack_id)
      assert Process.alive?(core_sup)
    end
  end

  # Both the persisted relation tracking and the inspector cache must be reset whenever
  # shapes are purged, otherwise stale pre-restart metadata would be read back even though
  # WAL continuity was lost. The purge runs through a single shared block in
  # Connection.Manager regardless of trigger, so we cover both triggers.
  describe "purging replication metadata on timeline change" do
    # A non-matching stored timeline forces a timeline change (and therefore a purge) on
    # the next connection.
    setup [
      :seed_stale_replication_metadata,
      :seed_a_dropped_broadcast_watermark,
      :store_mismatched_timeline,
      :start_connection_manager
    ]

    test "resets tracked relations and the inspector cache", ctx do
      assert_metadata_reset_after_active(ctx)
    end

    # What the drops of the watermark took away bounds the promotion check of the old stream: a
    # later check measures the new one, whose positions can be lower.
    test "forgets what the drops of the broadcast watermark took away", ctx do
      wait_until_active(ctx.stack_id)

      assert Electric.LsnTracker.dropped_high_water(ctx.stack_id) == nil
    end
  end

  describe "keeping replication metadata when a promotion continues the timeline" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :force_continued_timeline,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    test "keeps tracked relations and the inspector cache", ctx do
      %{stack_id: stack_id, persistent_kv: persistent_kv} = ctx

      wait_until_active(stack_id)

      assert_received {:timeline_checked, {_pg_id, "1"}, nil}

      assert %{table_to_id: %{{"public", "items"} => 1}} =
               PersistentReplicationState.get_tracked_relations(
                 stack_id: stack_id,
                 persistent_kv: persistent_kv
               )

      refute :ets.tab2list(ctx.pg_inspector_table) == []

      manager_pid = GenServer.whereis(Electric.Connection.Manager.name(stack_id))
      :ok = Electric.Connection.Manager.drop_replication_slot_on_stop(manager_pid)

      :ok =
        Supervisor.terminate_child(
          Connection.Manager.Supervisor.name(stack_id),
          Connection.Manager
        )
    end
  end

  # The timeline check runs before the purge. A manager that dies in the purge must still owe it
  # when it restarts, with a slot that survived and so does not purge on its own.
  describe "restarting in the middle of a timeline purge" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :disable_lock_breaker,
      :fail_the_first_storage_reset,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "purges again", ctx do
      assert_receive :storage_reset_failed
      assert_receive {:first_manager_down, {%RuntimeError{message: "the manager died" <> _}, _}}
      assert_purge_done(ctx)
    end
  end

  # A purge step that reports a failure leaves the purge owed. Going on stored the new timeline
  # over what the step left behind, and restarting the manager retried with no pause until the
  # supervisors above it gave up. The manager retries after a pause instead, and the shapes of the
  # old timeline are out of the catalog before the step, so no read serves them in between.
  for {step, module, function} <- [
        {"storage cleanup", Electric.Shapes.Supervisor, :reset_storage},
        {"replication metadata reset", Electric.Replication.PersistentReplicationState, :reset},
        {"inspector cache reset", Electric.Postgres.Inspector, :reset}
      ] do
    describe "a timeline purge whose #{step} fails" do
      @describetag failing_step: {module, function}

      setup [
        :seed_stale_replication_metadata,
        :store_mismatched_timeline,
        :create_existing_publication_and_slot,
        :seed_completed_shape,
        :disable_lock_breaker,
        :pause_purge_retries,
        :refuse_the_first_purge_step,
        :start_connection_manager
      ]

      @tag slot_temporary?: false
      @tag :capture_log
      test "retries the purge after a pause, the old shapes already out of the catalog", ctx do
        assert_receive {:purge_step_refused, manager, shape_listed?}
        refute shape_listed?
        trace_the_purge_retry(manager)

        retry = await_the_purge_retry(manager)
        fire_the_purge_retry(manager, retry)

        # Pinned: the same manager retries, no restart in between.
        assert_receive {:purge_step_retried, ^manager, {_pg_id, -1}}
        assert_purge_done(ctx, manager)
      end
    end
  end

  # The catalog reset fails on a full disk too. Its shapes are no longer served then, and the
  # storage cleanup runs anyway: the space it frees is what the retry needs.
  describe "a timeline purge whose catalog reset fails" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_catalog_reset,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "cleans the storage and retries the purge after a pause, serving no old shape", ctx do
      assert_receive {:purge_step_refused, manager, shape_listed?}
      refute shape_listed?
      trace_the_purge_retry(manager)
      assert_receive {:storage_reset, ^manager}

      retry = await_the_purge_retry(manager)
      refute Storage.snapshot_started?(ctx.shape_storage)

      fire_the_purge_retry(manager, retry)

      assert_receive {:purge_step_retried, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
    end
  end

  # The catalog's rows stay when its reset fails, and the storage cleanup removes their logs. A
  # retry that found the record gone took that for a first start: it stored the connected
  # timeline, purged nothing, and went on with catalog rows whose logs were gone.
  describe "a timeline purge whose catalog reset fails, then the record is gone" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_catalog_reset,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "purges again", ctx do
      assert_receive {:purge_step_refused, manager, _shape_listed?}
      trace_the_purge_retry(manager)
      assert_receive {:storage_reset, ^manager}

      retry = await_the_purge_retry(manager)
      assert ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
      remove_the_record(timeline_opts(ctx))

      fire_the_purge_retry(manager, retry)

      assert_receive {:purge_step_retried, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # The filesystem backend raises when a directory sync fails. The raise restarted the manager at
  # once, past the pause the purge retry takes, until its supervisors gave up.
  describe "a timeline purge whose replication metadata reset raises" do
    @describetag failing_step: {Electric.Replication.PersistentReplicationState, :reset},
                 failure: :raise

    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_purge_step,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "retries the purge after a pause", ctx do
      assert_receive {:purge_step_refused, manager, _shape_listed?}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      fire_the_purge_retry(manager, retry)

      assert_receive {:purge_step_retried, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
    end
  end

  # The first check of a changed timeline writes the owed purge, on a disk that can be full. A
  # write that raised restarted the manager at once, before any purge step or retry, until its
  # supervisors gave up. The manager retries after a pause instead, and purges nothing until the
  # purge is owed on disk: a restart in the middle of one would keep what it left.
  describe "a timeline check whose owed purge cannot be written" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "retries after a pause, nothing purged and no old shape served before", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      refute_received {:storage_reset, _, _}
      refute ShapeStatus.has_shape_handle?(stack_id, handle)
      assert ShapeDb.handle_exists?(stack_id, handle)
      assert Storage.snapshot_started?(ctx.shape_storage)

      assert Timeline.load_timeline(stack_id: stack_id, persistent_kv: ctx.persistent_kv) ==
               {0, 1}

      fire_the_purge_retry(manager, retry)

      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # A record the check cannot read is taken for a change, and the owed purge is written on a disk
  # that can fail it. The metadata is unloaded then, and the retry's own check can find the record
  # gone. The retry purged with no purge owed on disk, so a restart in the middle kept catalog rows
  # whose logs it had removed. It writes the owed purge first, and purges.
  describe "an unreadable timeline whose owed purge cannot be written, then is gone" do
    @describetag restore: :missing

    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "writes the owed purge before it purges", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      refute_received {:storage_reset, _, _}
      refute ShapeStatus.has_shape_handle?(stack_id, handle)

      fire_the_purge_retry(manager, retry)

      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # The retry's check can find the record readable again, as the connected timeline: no purge is
  # owed then, and the read that failed once cost a purge of every shape. The retry loads the
  # metadata again.
  describe "an unreadable timeline whose owed purge cannot be written, then reads again" do
    @describetag restore: :current

    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "loads the shapes again and purges nothing", ctx do
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, ctx.shape_handle)

      fire_the_purge_retry(manager, retry)

      assert_shapes_kept(ctx, manager)
    end
  end

  # A write of the owed purge can fail after its rename landed, as a directory sync does: the
  # retry's check then reads the purge as owed, though no write of it succeeded. A read of the
  # record made before the check can fail where the check's own read does not.
  describe "a timeline check whose owed purge lands and then fails" do
    @describetag restore: :written, fail_the_next_read: true

    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "writes it again before it purges", ctx do
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      assert {_pg_id, -1} = Timeline.load_timeline(timeline_opts(ctx))
      assert {:stored, {_, -1}} = next_record_event(manager)

      fire_the_purge_retry(manager, retry)

      assert {:stored, {_, -1}} = next_record_event(manager)
      assert {:reset, {_, -1}} = next_record_event(manager)
      assert_purge_done(ctx, manager)
    end
  end

  # The owner leaves the shape metadata unloaded while it cannot read the record, and the check
  # finds no change. The manager went on with no shape loaded over the catalog's. It has the owner
  # read the record again, and when that read fails too, purges the catalog, the purge owed on
  # disk before.
  describe "a refresh that leaves the shape metadata unloaded, the record unreadable again" do
    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :check_finds_no_change_in_the_first_manager,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "purges the catalog before it serves shapes", ctx do
      assert_receive {:storage_reset, manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # The record reads again by the check, as the connected timeline. A read that failed once cost a
  # purge of every shape, and a continued timeline the stream it kept. The owner loads the metadata
  # again.
  describe "a refresh that leaves the shape metadata unloaded, the record readable by the check" do
    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :restore_the_record_before_the_check,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "loads the shapes again and purges nothing", ctx do
      assert_receive {:timeline_restored, manager}
      assert_shapes_kept(ctx, manager)
    end
  end

  # The record is gone by the check, which took that for a first start and stored the connected
  # timeline before the owed purge: a restart in between loaded the catalog's shapes again, with
  # nothing owed. The purge is written first.
  describe "a refresh that leaves the shape metadata unloaded, the record gone by the check" do
    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :remove_the_record_after_the_refresh,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "writes the owed purge before any other timeline", ctx do
      assert_receive {:storage_reset, manager, {_pg_id, -1}}
      assert_received {:timeline_stored, ^manager, first_write}
      assert {_pg_id, -1} = first_write
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # The purge's last write, the connected timeline, can land and then fail, as a directory sync
  # does. With the metadata unloaded, the retry took the stored timeline for a reason to write the
  # owed purge again and purge again, on a disk that can fail that write as well.
  describe "a purge of unloaded shape metadata whose last timeline write lands and then fails" do
    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_final_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "goes on without purging again", ctx do
      assert_receive {:final_write_refused, manager}
      assert_received {:storage_reset, ^manager, {_pg_id, -1}}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      fire_the_purge_retry(manager, retry)

      assert_purge_done(ctx, manager)
      refute_received {:storage_reset, ^manager, _}
      assert_received {:timeline_stored, ^manager, {_pg_id, -1}}
      refute_received {:timeline_stored, ^manager, {_, -1}}
    end
  end

  # A new slot owes a purge of every shape as well. The retry of a purge whose last write landed and
  # failed found the purge for the new slot still owed, and wrote it and purged again.
  describe "a purge with a new slot whose last timeline write lands and then fails" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_final_write,
      :start_connection_manager
    ]

    @tag :capture_log
    test "goes on without purging again", ctx do
      assert_receive {:final_write_refused, manager}
      assert_received {:storage_reset, ^manager, {_pg_id, -1}}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      fire_the_purge_retry(manager, retry)

      assert_purge_done(ctx, manager)
      refute_received {:storage_reset, ^manager, _}
    end
  end

  # A slot created over a record that cannot be read writes no purge, and the record can read
  # again by the check, as the connected timeline. The purge ran on the slot alone with nothing
  # owed on disk: a restart in its middle found the slot and the timeline, and kept catalog rows
  # whose logs it had removed. The purge is written before it runs.
  describe "a slot created while the timeline cannot be read, the record readable by the check" do
    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :restore_the_record_before_the_check,
      :start_connection_manager
    ]

    @tag :capture_log
    test "writes the purge the new slot owes before it purges", ctx do
      assert_receive {:timeline_restored, manager}
      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # A slot created over a record that cannot be read owes the purge of the shapes from before it,
  # and the record can be gone by the check. The check took the missing record for a first start,
  # stored the connected timeline and kept the shapes across the new slot's gap.
  describe "a slot created while the timeline cannot be read, the record gone by the check" do
    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_items_table,
      :seed_completed_shape,
      :disable_lock_breaker,
      :remove_the_record_before_the_check,
      :start_connection_manager
    ]

    @tag :capture_log
    test "purges the shapes from before the slot", ctx do
      assert_receive {:timeline_removed, manager}
      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # A slot created while the record reads as missing can find it back by the check. The cast
  # took the missing record for a first start and set no purge, and the check found the record
  # unchanged: the manager kept the shapes across the new slot's gap.
  describe "a slot created while the timeline reads as missing, the record back by the check" do
    setup [
      :seed_stale_replication_metadata,
      :create_items_table,
      :seed_completed_shape,
      :disable_lock_breaker,
      :restore_the_record_before_the_check,
      :start_connection_manager
    ]

    @tag :capture_log
    test "purges the shapes from before the slot", ctx do
      assert_receive {:timeline_restored, manager}
      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # A manager that restarts finds the shape subsystem of the one before still running. Its refresh
  # cannot read the record, and the owner cleared the shape metadata under that subsystem: a
  # relation tracker that folded it then took the tables it had not read out of the publication.
  # The subsystem stopped next started again from the position its collector had processed, past
  # what the slot was acknowledged to, and dropped what Postgres resent that a writer stopped with
  # it had not kept. The manager stops the subsystem before it unloads the metadata, and the
  # collector it starts again replays from the slot.
  describe "a manager restart whose refresh leaves the shape metadata unloaded" do
    setup [
      :create_existing_publication_and_slot,
      :create_items_table,
      :publish_items_table,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "stops the shape subsystem first and starts it again from the slot", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      wait_until_active(stack_id)
      old_manager = GenServer.whereis(Connection.Manager.name(stack_id))
      assert {"public", "items"} in tracked_tables(stack_id)
      assert eventually(fn -> {"public", "items"} in published_tables(ctx) end)

      store_unreadable_timeline(ctx)
      processed = process_ahead_of_the_slot(ctx)
      report_the_unloads(ctx)
      report_the_replay(ctx)
      restore_the_record_in_the_check(ctx)
      allow_patches_in_the_next_manager(ctx, old_manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, old_manager)

      assert_receive {:metadata_unloaded, _unloader, running, serving}
      assert running == []
      assert serving == :starting
      wait_until_active(stack_id)

      assert {"public", "items"} in tracked_tables(stack_id)
      assert {"public", "items"} in published_tables(ctx)
      assert ShapeStatus.has_shape_handle?(stack_id, handle)
      assert_replays_from_the_slot(ctx, processed)
      stop_the_manager(stack_id)
    end
  end

  # The manager that takes over finds the record of another timeline, and the write of the purge
  # it owes fails: it stops the shape subsystem of the one before. Its retry reads the record as
  # the connected timeline and keeps the shapes. The subsystem started again from the position its
  # collector had processed, past what the slot was acknowledged to, and dropped what Postgres
  # resent that a writer stopped with it had not kept.
  describe "a manager restart whose owed purge cannot be written, then reads again" do
    setup [
      :create_existing_publication_and_slot,
      :create_items_table,
      :publish_items_table,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "starts the shape subsystem again from the slot", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      wait_until_active(stack_id)
      old_manager = GenServer.whereis(Connection.Manager.name(stack_id))

      store_mismatched_timeline(ctx)
      processed = process_ahead_of_the_slot(ctx)
      report_the_unloads(ctx)
      report_the_replay(ctx)
      refuse_the_next_marker_write(ctx)
      allow_patches_in_the_next_manager(ctx, old_manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, old_manager)

      assert_receive {:marker_write_refused, manager}
      assert_receive {:metadata_unloaded, ^manager, [], :starting}
      wait_until_active(stack_id)

      assert ShapeStatus.has_shape_handle?(stack_id, handle)
      assert {"public", "items"} in published_tables(ctx)
      assert_replays_from_the_slot(ctx, processed)
      assert GenServer.whereis(Connection.Manager.name(stack_id)) == manager
      stop_the_manager(stack_id)
    end
  end

  # The manager that stops the shape subsystem for the purge it owes can die while the core
  # supervisor stops it. The one that takes over found the old subsystem still running and took no
  # replay, and its start, queued behind the stop, started a collector from the position the old
  # one had processed.
  describe "a manager restart that dies while the shape subsystem stops" do
    setup [
      :create_existing_publication_and_slot,
      :create_items_table,
      :publish_items_table,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "starts the next shape subsystem from the slot", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      wait_until_active(stack_id)
      old_manager = GenServer.whereis(Connection.Manager.name(stack_id))

      store_mismatched_timeline(ctx)
      processed = process_ahead_of_the_slot(ctx)
      report_the_replay(ctx)
      refuse_the_next_marker_write(ctx)
      die_while_the_subsystem_stops(ctx)
      allow_patches_in_the_next_manager(ctx, old_manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, old_manager)

      assert_receive {:marker_write_refused, manager}
      assert_receive {:died_while_the_subsystem_stops, ^manager, core}
      assert eventually(fn -> queued_call?(core, :start_child) end)
      assert GenServer.whereis(Electric.Shapes.Supervisor.name(stack_id))
      :ok = :sys.resume(core)
      wait_until_active(stack_id)

      assert ShapeStatus.has_shape_handle?(stack_id, handle)
      assert_replays_from_the_slot(ctx, processed)
      stop_the_manager(stack_id)
    end
  end

  # A manager that finds another timeline and fails to write the purge it owes stops serving the
  # shapes, and can die before its retry: the record still names the timeline it found, with no
  # purge owed. The manager that takes over loads the shapes at its refresh, which opened serving
  # again before its own check found the change, and the old timeline's shapes were served.
  describe "a manager restart after a timeline check whose owed purge failed" do
    setup [
      :seed_stale_replication_metadata,
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "serves no old shape before its own check", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)
      await_the_purge_retry(manager)

      report_the_serving_at_the_check(ctx)
      allow_patches_in_the_next_manager(ctx, manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, manager)
      next_manager = await_manager_other_than(stack_id, manager)

      assert_receive {:checked, ^next_manager, serving, loaded?}
      assert loaded?
      assert serving == :starting

      assert_receive {:storage_reset, ^next_manager, {_pg_id, -1}}
      assert_purge_done(ctx, next_manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # A persistent slot created over a record that cannot be read writes no purge, and the record
  # reads again by the check, as the connected timeline. The check's write of the purge the slot
  # owes fails, and the manager dies before its retry. The next one found the slot in place and
  # the record unchanged: it served the old shapes again and kept them, short of the writes made
  # before the slot's consistent point.
  describe "a manager restart after the purge a new slot owes could not be written" do
    @describetag restore_before_check: true

    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "purges the shapes from before the slot", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      # A kept shape makes the stack fail to publish its table and stop, which must not take the
      # test down before it sees the purge missing.
      Process.flag(:trap_exit, true)
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)
      await_the_purge_retry(manager)

      allow_patches_in_the_next_manager(ctx, manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, manager)
      next_manager = await_manager_other_than(stack_id, manager)

      assert_receive {:storage_reset, ^next_manager, {_pg_id, -1}}
      assert_purge_done(ctx, next_manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  describe "a stack restart after the purge a recreated slot owes could not be written" do
    @describetag restore_before_check: true
    @describetag slot_temporary?: false

    setup [
      :create_existing_publication_and_slot,
      :create_items_table,
      :publish_items_table,
      :seed_completed_shape
    ]

    @tag :capture_log
    test "does not restore a shape from before the recreated slot", ctx do
      %{stack_id: stack_id, shape_handle: handle, db_conn: db, slot_name: slot_name} = ctx
      kv = Electric.PersistentKV.Filesystem.new!(root: Path.join(ctx.tmp_dir, "kv"))
      ctx = %{ctx | persistent_kv: kv}

      Postgrex.query!(db, "SELECT pg_drop_replication_slot($1)", [slot_name])
      Postgrex.query!(db, "INSERT INTO items VALUES (1, 'before slot creation')", [])
      %{rows: [[after_insert]]} = Postgrex.query!(db, "SELECT pg_current_wal_insert_lsn()", [])

      seed_stale_replication_metadata(ctx)
      store_unreadable_timeline(ctx)
      restart_the_owner_reading_the_timeline(ctx)
      ctx = Map.merge(ctx, pause_purge_retries(ctx))
      refuse_the_first_marker_write(ctx)
      start_connection_manager(ctx)

      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)
      await_the_purge_retry(manager)

      assert %{rows: [[true]]} =
               Postgrex.query!(
                 db,
                 "SELECT confirmed_flush_lsn >= $2 FROM pg_replication_slots WHERE slot_name = $1",
                 [slot_name, after_insert]
               )

      assert ShapeDb.handle_exists?(stack_id, handle)
      :ok = stop_supervised!(Electric.CoreSupervisor)
      :ok = stop_supervised!(ctx.shape_db)
      :ok = stop_supervised!(StatusMonitor)

      with_status_monitor(ctx)
      with_shape_db(ctx)
      restart_the_owner_reading_the_timeline(ctx)
      owner = GenServer.whereis(Electric.ShapeCache.ShapeStatusOwner.name(stack_id))
      :sys.get_state(owner)
      loaded_before_connection? = ShapeStatus.has_shape_handle?(stack_id, handle)

      start_connection_manager(ctx)
      wait_until_active(stack_id)

      refute ShapeDb.handle_exists?(stack_id, handle)
      refute loaded_before_connection?
    end
  end

  # A persistent slot created while the record reads as missing owes the purge once the check
  # finds the record, as the connected timeline. The check's write of that purge fails, and the
  # manager dies before its retry. The next one found the slot in place, the record unchanged and
  # nothing recorded with the monitor: it served the old shapes again and kept them.
  describe "a manager restart after the purge a slot created over a missing record owes could not be written" do
    @describetag restore_before_check: true

    setup [
      :seed_stale_replication_metadata,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "purges the shapes from before the slot", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      # A kept shape makes the stack fail to publish its table and stop, which must not take the
      # test down before it sees the purge missing.
      Process.flag(:trap_exit, true)
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)
      await_the_purge_retry(manager)

      allow_patches_in_the_next_manager(ctx, manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, manager)
      next_manager = await_manager_other_than(stack_id, manager)

      assert_receive {:storage_reset, ^next_manager, {_pg_id, -1}}
      assert_purge_done(ctx, next_manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # A first start finds no record and keeps what the catalog holds, though its new slot owes a
  # purge. The slot recorded that purge for a manager that takes over, which found the slot in
  # place and the connected timeline the first one stored before it died, and purged the shapes
  # the first one had kept.
  describe "a manager restart right after a first start stored its timeline" do
    setup [
      :create_items_table,
      :seed_completed_shape,
      :disable_lock_breaker,
      :die_after_the_first_check,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "keeps the shapes the first start kept", ctx do
      assert_receive {:died_after_the_check, manager, :no_previous_timeline}
      next_manager = await_manager_past_its_start(ctx.stack_id, manager, [:admin, :snapshot])
      assert_shapes_kept(ctx, next_manager)
    end
  end

  # A first start stores the connected timeline and keeps what the catalog holds. A write of it
  # whose rename landed and whose directory sync failed is retried, and the retry found the record
  # unchanged with the purge for the new slot still set: it wrote that purge and ran it.
  describe "a first start whose timeline write lands and then fails" do
    setup [
      :create_items_table,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_final_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "keeps the shapes at the retry", ctx do
      assert_receive {:final_write_refused, manager}
      trace_the_purge_retry(manager)

      retry = await_the_purge_retry(manager)
      fire_the_purge_retry(manager, retry)

      assert_shapes_kept(ctx, manager)
    end
  end

  # A new slot over a stored timeline records the purge it owes with the monitor before it writes
  # the marker, and the manager can die while that record waits in the monitor's queue. The one
  # that took over read the record at its start, before the monitor stored it: it found the slot
  # in place and the timeline unchanged, served the old shapes and cleared the record.
  describe "a manager restart while the purge a new slot owes waits for the monitor" do
    setup [
      :seed_stale_replication_metadata,
      :store_the_connected_timeline,
      :create_items_table,
      :seed_completed_shape,
      :disable_lock_breaker,
      :die_while_the_monitor_holds_the_slot_purge,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "purges the shapes from before the slot", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      assert_receive {:slot_purge_queued, killer, manager, monitor}
      lock_holder = queue_on_replication_lock(ctx)
      send(killer, :kill)

      # The next manager is past its start, and waits for the replication lock. Its question
      # waits behind the record in the monitor's queue, and is answered after it. The manager
      # that asks is past its pools, so it is the one that purges: one that started earlier can
      # still exit on a pool of the manager it replaced.
      await_manager_past_its_start(stack_id, manager)
      send(lock_holder, :release)
      assert eventually(fn -> queued_caller(monitor, :new_slot_purge_owed?) != nil end)
      next_manager = queued_caller(monitor, :new_slot_purge_owed?)
      :ok = :sys.resume(monitor)

      assert_purge_done(ctx, next_manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # The record can go missing once the write of the purge owed over an unreadable one has failed.
  # A manager that dies before its retry leaves the next one nothing in the record, and the
  # retained shapes in the catalog: that one took the missing record for a first start, stored
  # the connected timeline and kept them.
  describe "a manager restart after a failed owed purge over an unreadable record, then gone" do
    @describetag restore: :missing

    setup [
      :seed_stale_replication_metadata,
      :store_unreadable_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :disable_lock_breaker,
      :pause_purge_retries,
      :refuse_the_first_marker_write,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "writes the owed purge first", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      assert_receive {:marker_write_refused, manager}
      trace_the_purge_retry(manager)
      await_the_purge_retry(manager)
      assert_receive {:timeline_stored, ^manager, {_pg_id, -1}}

      allow_patches_in_the_next_manager(ctx, manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, manager)
      next_manager = await_manager_other_than(stack_id, manager)

      assert {:stored, {_pg_id, -1}} = next_record_event(next_manager)
      assert {:reset, {_pg_id, -1}} = next_record_event(next_manager)
      assert_purge_done(ctx, next_manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # A manager that takes over a running shape subsystem and finds another timeline purges at once.
  # The stack goes from up to read-only as the collector stops, and served the shapes it was about
  # to purge from the catalog it still held.
  describe "a manager restart that finds another timeline" do
    setup [
      :create_existing_publication_and_slot,
      :create_items_table,
      :publish_items_table,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "serves no old shape while it stops the shape subsystem", ctx do
      %{stack_id: stack_id, shape_handle: handle} = ctx
      wait_until_active(stack_id)
      old_manager = GenServer.whereis(Connection.Manager.name(stack_id))

      store_mismatched_timeline(ctx)
      report_the_stops(ctx)
      report_storage_resets(stack_id, ctx.persistent_kv)
      allow_patches_in_the_next_manager(ctx, old_manager)
      :ok = Electric.Connection.Restarter.restart_connection_subsystem(stack_id, old_manager)
      manager = await_manager_other_than(stack_id, old_manager)

      assert_receive {:subsystem_stopping, ^manager, serving, loaded?}
      assert loaded?
      assert serving == :starting

      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(stack_id, handle)
    end
  end

  # The check that finds another timeline writes the purge it owes. A manager that died between
  # that write and the stop of the shape subsystem left the shapes the owner had loaded served,
  # read-only, until another manager purged them. Serving closes before the write.
  describe "a timeline check that finds another timeline" do
    setup [
      :store_mismatched_timeline,
      :create_existing_publication_and_slot,
      :seed_completed_shape,
      :restart_the_owner_reading_the_timeline,
      :disable_lock_breaker,
      :report_the_serving_at_each_owed_purge,
      :allow_the_patches_in_the_first_manager,
      :start_connection_manager
    ]

    @tag slot_temporary?: false
    @tag :capture_log
    test "stops serving the old shapes before it writes the purge", ctx do
      assert_receive {:owed_purge_written, manager, closed?}
      assert closed?
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  # A new slot owes the purge of the catalog's shapes, which a check that finds no change does not
  # write: the manager writes it, and serving closes before that write too.
  describe "a new slot over shapes whose timeline the check finds unchanged" do
    setup [
      :seed_completed_shape,
      :report_the_serving_at_each_owed_purge,
      :check_finds_no_change_in_the_first_manager,
      :start_connection_manager
    ]

    @tag :capture_log
    test "stops serving the old shapes before it writes the purge", ctx do
      assert_receive {:owed_purge_written, manager, closed?}
      assert closed?
      assert_receive {:storage_reset, ^manager, {_pg_id, -1}}
      assert_purge_done(ctx, manager)
      refute ShapeDb.handle_exists?(ctx.stack_id, ctx.shape_handle)
    end
  end

  describe "purging replication metadata on new/temporary replication slot" do
    # The motivating path: the timeline is unchanged but the (temporary) replication slot
    # was created fresh, setting purge_all_shapes?. We stub the timeline check to report an
    # unchanged, non-initializing timeline so the purge is driven solely by slot creation.
    setup [:seed_stale_replication_metadata, :force_unchanged_timeline, :start_connection_manager]

    test "resets tracked relations and the inspector cache", ctx do
      assert_metadata_reset_after_active(ctx)
    end
  end

  describe "invalid pool configuration" do
    setup [:unresponsive_port]

    setup(ctx) do
      [
        pooled_connection_opts:
          Keyword.merge(ctx.db_config, port: ctx.unresponsive_port, timeout: 100)
      ]
    end

    setup [:start_connection_manager]

    test "failure to get pooled connection results in retries", ctx do
      %{stack_id: stack_id} = ctx

      ref = Process.monitor(GenServer.whereis(Electric.Connection.Manager.name(stack_id)))

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 1000
    end
  end

  describe "cleanup procedure" do
    setup [:start_connection_manager]

    @tag slot_temporary?: false
    test "handles dropping slot on termination", ctx do
      %{
        db_conn: db_conn,
        stack_id: stack_id,
        connection_opts: connection_opts,
        replication_opts: replication_opts
      } = ctx

      wait_until_active(stack_id)

      manager_pid = GenServer.whereis(Electric.Connection.Manager.name(stack_id))
      :ok = Electric.Connection.Manager.drop_replication_slot_on_stop(manager_pid)

      :ok =
        Supervisor.terminate_child(
          Connection.Manager.Supervisor.name(stack_id),
          Connection.Manager
        )

      # Ensure the replication slot has been dropped
      assert %{rows: []} =
               Postgrex.query!(
                 db_conn,
                 "SELECT slot_name FROM pg_replication_slots where database = $1",
                 [connection_opts[:database]]
               )

      # Ensure the publication has been dropped
      assert %{rows: []} =
               Postgrex.query!(
                 db_conn,
                 "SELECT pubname FROM pg_publication WHERE pubname = $1",
                 [replication_opts[:publication_name]]
               )
    end

    # The next holder of the lock validates the shape catalog as it finds it, and removes a shape
    # whose snapshot-complete mark the stopping instance still has queued.
    test "commits the queued shape catalog writes before it releases the lock", ctx do
      %{stack_id: stack_id} = ctx
      parent = self()

      wait_until_active(stack_id)
      queue_snapshot_complete_mark(stack_id)

      Repatch.patch(WriteBuffer, :drain, [mode: :shared], fn stack_id ->
        held_before = replication_lock_count(ctx)
        result = Repatch.real(WriteBuffer, :drain, [stack_id])
        send(parent, {:drain, held_before, replication_lock_count(ctx)})
        result
      end)

      Repatch.allow(parent, GenServer.whereis(Connection.Manager.name(stack_id)))

      :ok =
        Supervisor.terminate_child(
          Connection.Manager.Supervisor.name(stack_id),
          Connection.Manager
        )

      # Held when the drain starts, and still held once it has committed.
      assert_received {:drain, 1, 1}
      assert 0 = WriteBuffer.pending_operations_count(stack_id)
      assert {:ok, [1]} = snapshot_complete_in_file(ctx, "handle-0")
    end

    test "lets the lock go when the drain outlasts its wait", ctx do
      %{stack_id: stack_id} = ctx

      wait_until_active(stack_id)
      queue_snapshot_complete_mark(stack_id)

      buffer = GenServer.whereis(WriteBuffer.name(stack_id))
      :ok = :sys.suspend(buffer)

      Repatch.patch(WriteBuffer, :drain, [mode: :shared], fn stack_id ->
        Repatch.real(WriteBuffer, :drain, [stack_id, 100])
      end)

      Repatch.allow(self(), GenServer.whereis(Connection.Manager.name(stack_id)))

      try do
        log =
          ExUnit.CaptureLog.capture_log(fn ->
            :ok =
              Supervisor.terminate_child(
                Connection.Manager.Supervisor.name(stack_id),
                Connection.Manager
              )
          end)

        assert log =~
                 "Releasing the replication lock without committing the queued shape catalog writes"

        assert eventually(fn -> replication_lock_count(ctx) == 0 end)
      after
        :sys.resume(buffer)
      end

      # The buffer takes the drain it was sent once it runs again, and commits the queue: it
      # flushes only when asked, so nothing else does.
      assert eventually(fn -> WriteBuffer.pending_operations_count(stack_id) == 0 end)
      assert {:ok, [1]} = snapshot_complete_in_file(ctx, "handle-0")
    end

    test "goes on stopping when the drain raises", ctx do
      %{stack_id: stack_id} = ctx

      wait_until_active(stack_id)

      manager = GenServer.whereis(Connection.Manager.name(stack_id))
      %{replication_client_pid: client} = :sys.get_state(manager)
      client_monitor = Process.monitor(client)

      Repatch.patch(WriteBuffer, :drain, [mode: :shared], fn _stack_id ->
        raise ArgumentError, "unknown registry"
      end)

      Repatch.allow(self(), manager)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          :ok =
            Supervisor.terminate_child(
              Connection.Manager.Supervisor.name(stack_id),
              Connection.Manager
            )
        end)

      assert log =~
               "Releasing the replication lock without committing the queued shape catalog " <>
                 "writes: ** (ArgumentError) unknown registry"

      # The steps after the drain ran: the manager stopped the client itself. Had the raise ended
      # its `terminate/2`, the client would have gone down with the manager's crash.
      assert_receive {:DOWN, ^client_monitor, :process, ^client, :shutdown}, 1_000
    end

    # An idle client disconnects, which releases the lock, before the manager hears of it.
    @tag replication_idle_timeout: 1_000
    test "an idle replication client commits the queued writes before it lets the lock go",
         ctx do
      %{stack_id: stack_id} = ctx
      parent = self()

      Repatch.patch(WriteBuffer, :drain, [mode: :shared], fn stack_id ->
        send(parent, {:drain, self()})
        Repatch.real(WriteBuffer, :drain, [stack_id])
      end)

      %{manager: manager, client: client, buffer: buffer} = idle_replication_client(ctx)
      Repatch.allow(parent, manager)
      manager_monitor = Process.monitor(manager)
      client_monitor = Process.monitor(client)
      :ok = :sys.suspend(buffer)

      try do
        send(client, :check_if_idle)

        # The drain waits on the suspended buffer with the mark still queued, and the client
        # keeps its session, and the lock with it, until the drain returns.
        assert Support.TestUtils.wait_until(fn -> drain_waiting?(buffer) end)
        assert Process.alive?(client)
        assert 1 = replication_lock_count(ctx)
        assert 1 = WriteBuffer.pending_operations_count(stack_id)
      after
        :sys.resume(buffer)
      end

      assert_receive {:DOWN, ^client_monitor, :process, ^client,
                      {:shutdown, {:connection_idle, _}}},
                     5_000

      assert_receive {:DOWN, ^manager_monitor, :process, ^manager, _reason}, 5_000

      # The manager finds the client gone, and the lock with it.
      refute_received {:drain, ^manager}
      assert 0 = WriteBuffer.pending_operations_count(stack_id)
      assert {:ok, [1]} = snapshot_complete_in_file(ctx, "handle-0")
    end

    # Postgres ends a session that sends no status update for `wal_sender_timeout`, which releases
    # the lock before the drain has committed.
    @tag replication_idle_timeout: 1_000
    test "an idle replication client goes on answering Postgres while it drains", ctx do
      %{client: client, buffer: buffer} = idle_replication_client(ctx)
      client_monitor = Process.monitor(client)
      :ok = :sys.suspend(buffer)

      try do
        send(client, :check_if_idle)
        assert Support.TestUtils.wait_until(fn -> drain_waiting?(buffer) end)

        [[backend, reply_time]] = replication_replies(ctx)
        send(client, :send_keepalive)

        # The same session answers, later than before, and keeps the lock.
        assert Support.TestUtils.wait_until(fn ->
                 case replication_replies(ctx) do
                   [[^backend, %DateTime{} = later]] ->
                     is_nil(reply_time) or DateTime.after?(later, reply_time)

                   _ ->
                     false
                 end
               end)

        assert 1 = replication_lock_count(ctx)
      after
        :sys.resume(buffer)
      end

      assert_receive {:DOWN, ^client_monitor, :process, ^client,
                      {:shutdown, {:connection_idle, _}}}

      assert {:ok, [1]} = snapshot_complete_in_file(ctx, "handle-0")
    end
  end

  describe "shutdown" do
    setup [:unresponsive_port]

    test "manager blocked resolving a replication connection terminates cleanly", ctx do
      start_connection_manager(
        Map.merge(ctx, %{
          replication_connection_opts: Keyword.put(ctx.db_config, :port, ctx.unresponsive_port)
        })
      )

      {time, log} =
        :timer.tc(
          fn ->
            ExUnit.CaptureLog.capture_log(fn ->
              :ok = Supervisor.stop(Connection.Manager.Supervisor.name(ctx.stack_id))
            end)
          end,
          :millisecond
        )

      assert time < 1000
      refute log =~ "Electric.DBConnection unknown error"
    end

    test "manager blocked resolving a pool connection terminates cleanly", ctx do
      start_connection_manager(
        Map.merge(ctx, %{
          pooled_connection_opts: Keyword.put(ctx.db_config, :port, ctx.unresponsive_port)
        })
      )

      assert_receive {:stack_status, _, :connection_lock_acquired}

      {time, log} =
        :timer.tc(
          fn ->
            ExUnit.CaptureLog.capture_log(fn ->
              :ok = Supervisor.stop(Connection.Manager.Supervisor.name(ctx.stack_id))
            end)
          end,
          :millisecond
        )

      assert time < 1000
      refute log =~ "Electric.DBConnection unknown error"
    end
  end

  describe "pooled connection opts" do
    setup(ctx) do
      [replication_connection_opts: Keyword.put(ctx.db_config, :host, "unpooled.localhost")]
    end

    test "are used correctly", %{stack_id: stack_id} = ctx do
      %{replication_connection_opts: repl_opts, db_config: pooled_conn_opts} = ctx

      parent = self()

      refute repl_opts == pooled_conn_opts

      Repatch.patch(
        Connection.Manager.ConnectionResolver,
        :validate,
        [mode: :shared],
        fn _stack_id, conn_opts ->
          send(parent, {:validate, conn_opts})

          {:ok, conn_opts}
        end
      )

      # process allowance doesn't follow the supervision tree in this case
      spawn_link(fn ->
        Stream.repeatedly(fn -> 0 end)
        |> Enum.reduce_while(0, fn _, _ ->
          case GenServer.whereis(Electric.Connection.Manager.name(stack_id)) do
            nil ->
              {:cont, 0}

            pid ->
              Repatch.allow(parent, pid)
              {:halt, 0}
          end
        end)
      end)

      start_connection_manager(ctx)

      StatusMonitor.wait_until_active(stack_id,
        timeout: ExUnit.configuration()[:assert_receive_timeout]
      )

      # Assert pooled connection opts are validated separately from replication opts.
      # If both pool types used the same connection opts, this validation would be cached
      # from the admin pool's earlier :replication type validation and no message would arrive.
      assert_receive {:validate, ^pooled_conn_opts}
    end
  end

  describe "pool_sizes/1" do
    test "uses the given pool size for both if size is small" do
      # we need a pool size of at least 2
      assert %{admin: 1, snapshot: 1} =
               Electric.Connection.Manager.pool_sizes(1)

      assert %{admin: 1, snapshot: 1} =
               Electric.Connection.Manager.pool_sizes(2)

      assert %{admin: 1, snapshot: 3} =
               Electric.Connection.Manager.pool_sizes(4)

      assert %{admin: 1, snapshot: 5} =
               Electric.Connection.Manager.pool_sizes(6)

      assert %{admin: 2, snapshot: 8} =
               Electric.Connection.Manager.pool_sizes(10)
    end

    test "splits the pool between both roles for large enough sizes" do
      assert %{admin: 4, snapshot: 16} =
               Electric.Connection.Manager.pool_sizes(20)

      assert %{admin: 4, snapshot: 36} =
               Electric.Connection.Manager.pool_sizes(40)

      assert %{admin: 4, snapshot: 96} =
               Electric.Connection.Manager.pool_sizes(100)
    end
  end

  describe "admin pool before lock" do
    test "admin pool is available while waiting for lock", ctx do
      test_pid = self()

      # Hold the advisory lock so the manager stays in acquiring_lock state
      start_supervised!({
        Task,
        fn ->
          DBConnection.run(ctx.db_conn, fn conn ->
            Postgrex.query!(
              conn,
              "SELECT pg_advisory_lock(hashtext('#{ctx.slot_name}'))",
              []
            )

            send(test_pid, :lock_held)
            Process.sleep(:infinity)
          end)
        end
      })

      assert_receive :lock_held

      start_connection_manager(ctx)

      # Wait for the manager to reach the lock acquisition step
      assert_receive {:stack_status, _, :waiting_for_connection_lock}, 5000

      # The admin pool should be registered and accessible before the lock is acquired
      assert is_pid(GenServer.whereis(Connection.Manager.admin_pool(ctx.stack_id)))
    end
  end

  describe "lock_breaker" do
    test "breaks an abandoned lock when slot is inactive", ctx do
      test_pid = self()

      # Create an inactive replication slot so the lock breaker query finds it
      Postgrex.query!(
        ctx.db_conn,
        "SELECT pg_create_logical_replication_slot('#{ctx.slot_name}', 'pgoutput')",
        []
      )

      # Hold the advisory lock from a separate connection, simulating an abandoned lock
      start_supervised!({
        Task,
        fn ->
          DBConnection.run(ctx.db_conn, fn conn ->
            Postgrex.query!(
              conn,
              "SELECT pg_advisory_lock(hashtext('#{ctx.slot_name}'))",
              []
            )

            send(test_pid, :lock_held)
            Process.sleep(:infinity)
          end)
        end
      })

      assert_receive :lock_held

      start_connection_manager(ctx)

      # Wait for the manager to start waiting for the lock
      assert_receive {:stack_status, _, :waiting_for_connection_lock}, 5000

      manager_pid = GenServer.whereis(Connection.Manager.name(ctx.stack_id))

      # Wait for pg_info_obtained to set the backend PID
      %{replication_lock_timer: tref} = wait_for_pg_backend_pid(manager_pid)
      :erlang.cancel_timer(tref)

      # Manually trigger the lock breaker check
      send(manager_pid, {:timeout, tref, {:check_status, :replication_lock}})

      # The lock breaker should terminate the stuck backend, freeing the lock.
      # The lock breaker should terminate the stuck backend, freeing the lock.
      # The replication client should then acquire the lock and proceed.
      assert_receive {:stack_status, _, :connection_lock_acquired}, 5000
    end

    test "does not break the lock when slot is active", ctx do
      # Start a replication client that acquires the lock and makes the slot active.
      # This simulates a healthy Electric instance holding the lock.
      {:ok, replication_client_pid} =
        start_supervised(
          {Electric.Postgres.ReplicationClient,
           stack_id: ctx.stack_id,
           replication_opts: [
             connection_opts: ctx.db_config,
             stack_id: ctx.stack_id,
             publication_name: "test_pub_#{:erlang.phash2(ctx.stack_id)}",
             try_creating_publication?: true,
             slot_name: ctx.slot_name,
             handle_event: nil,
             connection_manager: self()
           ]}
        )

      assert_receive {:"$gen_cast", :replication_client_lock_acquired}, 5000
      assert_receive {:"$gen_cast", {:pg_info_obtained, %{pg_backend_pid: backend_pid}}}

      replication_client_monitor = Process.monitor(replication_client_pid)

      # Use the production lock breaker query to verify it doesn't break active slots
      database = ctx.db_config[:database]
      query = Connection.Manager.lock_breaker_query(ctx.slot_name, backend_pid, database)

      assert {:ok, %Postgrex.Result{num_rows: 0}} = Postgrex.query(ctx.db_conn, query, [])

      # The replication client should still be alive — the lock was not broken
      refute_receive {:DOWN, ^replication_client_monitor, :process, ^replication_client_pid,
                      _reason},
                     200

      assert Process.alive?(replication_client_pid)
    end

    test "skips lock breaker when guard returns false", ctx do
      test_pid = self()

      # Hold the advisory lock so the manager stays in acquiring_lock state
      start_supervised!({
        Task,
        fn ->
          DBConnection.run(ctx.db_conn, fn conn ->
            Postgrex.query!(
              conn,
              "SELECT pg_advisory_lock(hashtext('#{ctx.slot_name}'))",
              []
            )

            send(test_pid, :lock_held)
            Process.sleep(:infinity)
          end)
        end
      })

      assert_receive :lock_held

      start_connection_manager(Map.put(ctx, :lock_breaker_guard, fn -> false end))

      # Wait for lock acquisition to start
      assert_receive {:stack_status, _, :waiting_for_connection_lock}, 5000

      manager_pid = GenServer.whereis(Connection.Manager.name(ctx.stack_id))

      # Wait for pg_info_obtained to be processed (sets replication_pg_backend_pid)
      %{replication_lock_timer: tref} = wait_for_pg_backend_pid(manager_pid)
      assert not is_nil(tref)

      :erlang.cancel_timer(tref)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(manager_pid, {:timeout, tref, {:check_status, :replication_lock}})
          :pong = Connection.Manager.ping(manager_pid)
        end)

      assert log =~ "Lock breaker skipped"
    end

    test "allows lock breaker when no guard is set", ctx do
      test_pid = self()

      # Hold the advisory lock so the manager stays in acquiring_lock state
      start_supervised!({
        Task,
        fn ->
          DBConnection.run(ctx.db_conn, fn conn ->
            Postgrex.query!(
              conn,
              "SELECT pg_advisory_lock(hashtext('#{ctx.slot_name}'))",
              []
            )

            send(test_pid, :lock_held)
            Process.sleep(:infinity)
          end)
        end
      })

      assert_receive :lock_held

      start_connection_manager(ctx)

      # Wait for lock acquisition to start
      assert_receive {:stack_status, _, :waiting_for_connection_lock}, 5000

      manager_pid = GenServer.whereis(Connection.Manager.name(ctx.stack_id))

      # Wait for pg_info_obtained to be processed (sets replication_pg_backend_pid)
      %{replication_lock_timer: tref} = wait_for_pg_backend_pid(manager_pid)

      assert not is_nil(tref)

      :erlang.cancel_timer(tref)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(manager_pid, {:timeout, tref, {:check_status, :replication_lock}})
          :pong = Connection.Manager.ping(manager_pid)
        end)

      # Without a guard, the lock breaker should attempt to run (no "skipped" message)
      refute log =~ "Lock breaker skipped"

      # The lock is held by a plain connection with no matching inactive replication slot,
      # so the lock breaker should not terminate any backends
      refute log =~ "Terminated a stuck backend"
    end
  end

  defp seed_stale_replication_metadata(%{
         stack_id: stack_id,
         persistent_kv: persistent_kv,
         inspector: {EtsInspector, inspector_opts},
         pg_inspector_table: pg_inspector_table
       }) do
    # Populate the inspector cache (supported features needs no table) ...
    assert {:ok, _features} = EtsInspector.load_supported_features(inspector_opts)
    refute :ets.tab2list(pg_inspector_table) == []

    # ... and the persisted relation tracking.
    PersistentReplicationState.set_tracked_relations(
      %{table_to_id: %{{"public", "items"} => 1}, id_to_table_info: %{}},
      stack_id: stack_id,
      persistent_kv: persistent_kv
    )

    :ok
  end

  # A replay from the slot or a journal tail dropped the broadcast watermark before the check.
  defp seed_a_dropped_broadcast_watermark(%{stack_id: stack_id}) do
    :ok = Electric.LsnTracker.broadcast_last_seen_lsn(stack_id, 1000)
    :ok = Electric.LsnTracker.reset_last_broadcast_lsn(stack_id)
    999 = Electric.LsnTracker.dropped_high_water(stack_id)
    :ok
  end

  # Store a timeline with a Postgres ID that cannot match the real one, forcing a timeline
  # change (and therefore a full shape purge) on the next connection.
  defp store_mismatched_timeline(%{stack_id: stack_id, persistent_kv: persistent_kv}) do
    Timeline.store_timeline({0, 1}, stack_id: stack_id, persistent_kv: persistent_kv)
    :ok
  end

  # Make the timeline check report an unchanged, non-initializing timeline so that the only
  # thing that can drive a purge is the freshly-created (temporary) replication slot setting
  # purge_all_shapes?.
  defp force_unchanged_timeline(%{stack_id: stack_id}) do
    parent = self()

    Repatch.patch(Timeline, :check, [mode: :shared], fn _pg_timeline, _opts, _promotion ->
      :ok
    end)

    allow_patches_in_manager(stack_id, parent)
    :ok
  end

  # Make the timeline check read the change as a promotion that kept the stream, reporting the
  # evidence the manager handed it.
  defp force_continued_timeline(%{stack_id: stack_id}) do
    parent = self()

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, _opts, promotion ->
      send(parent, {:timeline_checked, pg_timeline, promotion})
      :timeline_continued
    end)

    allow_patches_in_manager(stack_id, parent)
    :ok
  end

  # Kill the first manager inside its purge, after the timeline check. Only that manager is
  # allowed the patch: the one its supervisor restarts resets the storage for real.
  defp fail_the_first_storage_reset(ctx) do
    patch_the_first_storage_reset(ctx, fn -> raise "the manager died in the purge" end)
  end

  # The first call of the purge step the test names reports a failure, as a refused rename
  # does, or raises the error a failed directory sync does with `failure: :raise`. It reports
  # whether the catalog still listed the seeded shape then, and waits for the test to trace the
  # retry timer. The second call reports the stored timeline before it goes on for real.
  defp refuse_the_first_purge_step(
         %{failing_step: {module, function}, stack_id: stack_id, shape_handle: handle} = ctx
       ) do
    parent = self()
    timeline_opts = [stack_id: stack_id, persistent_kv: ctx.persistent_kv]
    calls = :counters.new(1, [])

    Repatch.patch(module, function, [mode: :shared], fn opts ->
      :ok = :counters.add(calls, 1, 1)

      case :counters.get(calls, 1) do
        1 ->
          send(
            parent,
            {:purge_step_refused, self(), ShapeStatus.has_shape_handle?(stack_id, handle)}
          )

          await_the_retry_trace(parent)

          if ctx[:failure] == :raise,
            do: raise(File.Error, reason: :eio, action: "sync directory", path: "kv"),
            else: {:error, :eacces}

        2 ->
          send(parent, {:purge_step_retried, self(), Timeline.load_timeline(timeline_opts)})
          call_real(module, function, opts)

        _ ->
          call_real(module, function, opts)
      end
    end)

    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The catalog's statement fails as it does on a full disk, the first time only, once the test
  # traces the retry timer. The storage cleanup reports each call.
  defp refuse_the_first_catalog_reset(%{stack_id: stack_id, shape_handle: handle} = ctx) do
    parent = self()
    timeline_opts = [stack_id: stack_id, persistent_kv: ctx.persistent_kv]
    calls = :counters.new(1, [])

    Repatch.patch(ShapeDb.Sqlite3, :execute, [mode: :shared], fn
      conn, "DELETE FROM shapes" = sql ->
        :ok = :counters.add(calls, 1, 1)

        case :counters.get(calls, 1) do
          1 ->
            send(
              parent,
              {:purge_step_refused, self(), ShapeStatus.has_shape_handle?(stack_id, handle)}
            )

            await_the_retry_trace(parent)
            {:error, "database or disk is full"}

          2 ->
            send(parent, {:purge_step_retried, self(), Timeline.load_timeline(timeline_opts)})
            Repatch.real(ShapeDb.Sqlite3.execute(conn, sql))

          _ ->
            Repatch.real(ShapeDb.Sqlite3.execute(conn, sql))
        end

      conn, sql ->
        Repatch.real(ShapeDb.Sqlite3.execute(conn, sql))
    end)

    Repatch.patch(Electric.Shapes.Supervisor, :reset_storage, [mode: :shared], fn opts ->
      send(parent, {:storage_reset, self()})
      Repatch.real(Electric.Shapes.Supervisor.reset_storage(opts))
    end)

    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The first write of the owed purge fails as it does on a full disk, once the test traces the
  # retry timer. With `restore:` set, the record it failed over then reads again as the connected
  # timeline (`:current`), is gone (`:missing`), or holds the owed purge, the write landed
  # (`:written`). With `fail_the_next_read:` set, the manager's next `Timeline.load_timeline/1`
  # fails. With `restore_before_check:` set, each check first finds the record as the connected
  # timeline. Each write of the record is reported as `{:timeline_stored, manager, timeline}`, and
  # each storage cleanup with the stored timeline.
  defp refuse_the_first_marker_write(%{stack_id: stack_id} = ctx) do
    parent = self()
    calls = :counters.new(1, [])

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      Process.put(:checked_timeline, pg_timeline)

      if ctx[:restore_before_check],
        do: :ok = Repatch.real(Timeline.store_timeline(pg_timeline, opts))

      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)

    if ctx[:fail_the_next_read] do
      Repatch.patch(Timeline, :load_timeline, [mode: :shared], fn opts ->
        if Process.delete(:fail_the_next_read),
          do: :unreadable,
          else: Repatch.real(Timeline.load_timeline(opts))
      end)
    end

    Repatch.patch(Timeline, :store_timeline, [mode: :shared], fn
      {_pg_id, -1} = timeline, opts ->
        send(parent, {:timeline_stored, self(), timeline})
        :ok = :counters.add(calls, 1, 1)

        if :counters.get(calls, 1) == 1 do
          restore_the_record(ctx[:restore], timeline, opts)
          if ctx[:fail_the_next_read], do: Process.put(:fail_the_next_read, true)
          send(parent, {:marker_write_refused, self()})
          await_the_retry_trace(parent)
          {:error, :enospc}
        else
          Repatch.real(Timeline.store_timeline(timeline, opts))
        end

      timeline, opts ->
        send(parent, {:timeline_stored, self(), timeline})
        Repatch.real(Timeline.store_timeline(timeline, opts))
    end)

    report_storage_resets(stack_id, ctx.persistent_kv)
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  defp restore_the_record(nil, _marker, _opts), do: :ok

  defp restore_the_record(:current, _marker, opts),
    do: :ok = Repatch.real(Timeline.store_timeline(Process.get(:checked_timeline), opts))

  defp restore_the_record(:written, marker, opts),
    do: :ok = Repatch.real(Timeline.store_timeline(marker, opts))

  defp restore_the_record(:missing, _marker, opts), do: remove_the_record(opts)

  defp remove_the_record(opts) do
    %Electric.PersistentKV.Memory{pid: pid} = Keyword.fetch!(opts, :persistent_kv)
    key = "timeline_id_#{Keyword.fetch!(opts, :stack_id)}"
    Agent.update(pid, &Map.delete(&1, key))
  end

  defp report_storage_resets(stack_id, persistent_kv) do
    parent = self()
    timeline_opts = [stack_id: stack_id, persistent_kv: persistent_kv]

    Repatch.patch(Electric.Shapes.Supervisor, :reset_storage, [mode: :shared], fn opts ->
      # A read the test armed to fail is the manager's, not this one's.
      armed = Process.delete(:fail_the_next_read)
      timeline = Timeline.load_timeline(timeline_opts)
      if armed, do: Process.put(:fail_the_next_read, armed)
      send(parent, {:storage_reset, self(), timeline})
      Repatch.real(Electric.Shapes.Supervisor.reset_storage(opts))
    end)
  end

  # The suite's owner reads no timeline. This one reads the stack's, as the stack's owner does.
  defp restart_the_owner_reading_the_timeline(%{stack_id: stack_id} = ctx) do
    :ok = stop_supervised!(ctx.shape_status_owner)
    timeline_opts = [stack_id: stack_id, persistent_kv: ctx.persistent_kv]
    owner_opts = [stack_id: stack_id, timeline_opts: timeline_opts]

    start_supervised!(%{
      id: ctx.shape_status_owner,
      start: {Electric.ShapeCache.ShapeStatusOwner, :start_link, [owner_opts]},
      restart: :temporary
    })

    :ok
  end

  # Each write of an owed purge, the check's own included, reports whether serving was closed at
  # the time, as `{:owed_purge_written, manager, closed?}`.
  defp report_the_serving_at_each_owed_purge(%{stack_id: stack_id}) do
    parent = self()

    Repatch.patch(Timeline, :store_timeline, [mode: :shared], fn
      {_pg_id, -1} = timeline, opts ->
        closed? = StatusMonitor.shape_metadata_closed?(stack_id)
        send(parent, {:owed_purge_written, self(), closed?})
        Repatch.real(Timeline.store_timeline(timeline, opts))

      timeline, opts ->
        Repatch.real(Timeline.store_timeline(timeline, opts))
    end)

    :ok
  end

  defp allow_the_patches_in_the_first_manager(ctx) do
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The record reads again by the time the manager checks it, and the check finds no change.
  defp check_finds_no_change_in_the_first_manager(%{stack_id: stack_id} = ctx) do
    Repatch.patch(Timeline, :check, [mode: :shared], fn _pg_timeline, _opts, _promotion ->
      :ok
    end)

    report_storage_resets(stack_id, ctx.persistent_kv)
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The record reads again by the time the manager checks it, as the connected timeline.
  defp restore_the_record_before_the_check(%{stack_id: stack_id} = ctx) do
    parent = self()

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      :ok = Repatch.real(Timeline.store_timeline(pg_timeline, opts))
      send(parent, {:timeline_restored, self()})
      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)

    report_storage_resets(stack_id, ctx.persistent_kv)
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The record is gone by the time the manager checks it.
  defp remove_the_record_before_the_check(%{stack_id: stack_id} = ctx) do
    parent = self()

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      remove_the_record(opts)
      send(parent, {:timeline_removed, self()})
      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)

    report_storage_resets(stack_id, ctx.persistent_kv)
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The record is gone once the manager's first refresh has read it. Each write of the record is
  # reported, the check's own included.
  defp remove_the_record_after_the_refresh(%{stack_id: stack_id} = ctx) do
    parent = self()
    calls = :counters.new(1, [])

    Repatch.patch(Electric.ShapeCache.ShapeStatusOwner, :refresh, [mode: :shared], fn id ->
      result = Repatch.real(Electric.ShapeCache.ShapeStatusOwner.refresh(id))
      :ok = :counters.add(calls, 1, 1)
      if :counters.get(calls, 1) == 1, do: remove_the_record(timeline_opts(ctx))
      result
    end)

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)

    Repatch.patch(Timeline, :store_timeline, [mode: :shared], fn timeline, opts ->
      send(parent, {:timeline_stored, self(), timeline})
      Repatch.real(Timeline.store_timeline(timeline, opts))
    end)

    report_storage_resets(stack_id, ctx.persistent_kv)
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The first write of a timeline other than the owed purge lands and then fails, as a directory
  # sync does, once the test traces the retry timer. Each write of the record is reported, the
  # check's own included. A refresh after the manager's first reads the record no better, on the
  # same failing disk, and leaves the metadata unloaded.
  defp refuse_the_first_final_write(%{stack_id: stack_id} = ctx) do
    parent = self()
    calls = :counters.new(1, [])
    refreshes = :counters.new(1, [])

    Repatch.patch(Electric.ShapeCache.ShapeStatusOwner, :refresh, [mode: :shared], fn id ->
      :ok = :counters.add(refreshes, 1, 1)

      if :counters.get(refreshes, 1) == 1,
        do: Repatch.real(Electric.ShapeCache.ShapeStatusOwner.refresh(id)),
        else: :purge_owed
    end)

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)

    Repatch.patch(Timeline, :store_timeline, [mode: :shared], fn
      {_pg_id, -1} = timeline, opts ->
        send(parent, {:timeline_stored, self(), timeline})
        Repatch.real(Timeline.store_timeline(timeline, opts))

      timeline, opts ->
        send(parent, {:timeline_stored, self(), timeline})
        :ok = Repatch.real(Timeline.store_timeline(timeline, opts))
        :ok = :counters.add(calls, 1, 1)

        if :counters.get(calls, 1) == 1 do
          send(parent, {:final_write_refused, self()})
          await_the_retry_trace(parent)
          {:error, :eio}
        else
          :ok
        end
    end)

    report_storage_resets(stack_id, ctx.persistent_kv)
    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  defp timeline_opts(%{stack_id: stack_id, persistent_kv: persistent_kv}),
    do: [stack_id: stack_id, persistent_kv: persistent_kv]

  # The next write of the record or storage cleanup the manager reports, in the order it made them.
  defp next_record_event(manager) do
    receive do
      {:timeline_stored, ^manager, timeline} -> {:stored, timeline}
      {:storage_reset, ^manager, timeline} -> {:reset, timeline}
    after
      ExUnit.configuration()[:assert_receive_timeout] ->
        flunk("the manager reported no write of the record and no storage cleanup")
    end
  end

  defp tracked_tables(stack_id) do
    {_version, filters} = RelationTracker.fetch_current_filters!(stack_id: stack_id)
    for {_oid, relation} <- filters, do: relation
  end

  defp published_tables(%{db_conn: db_conn, replication_opts: replication_opts}) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        db_conn,
        "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1",
        [Keyword.fetch!(replication_opts, :publication_name)]
      )

    for [schema, table] <- rows, do: {schema, table}
  end

  # The collector took transactions past what the slot was acknowledged to, as it does before the
  # writers flush them: its position is put past the WAL's end.
  defp process_ahead_of_the_slot(%{stack_id: stack_id, db_conn: db_conn}) do
    %Postgrex.Result{rows: [[wal_end]]} =
      Postgrex.query!(db_conn, "SELECT pg_current_wal_lsn()", [])

    processed = Electric.Postgres.Lsn.to_integer(wal_end) + Bitwise.bsl(1, 32)
    :ok = Electric.LsnTracker.set_last_processed_lsn(stack_id, processed)
    processed
  end

  # The collector started again replays from the position the replacement connection read from
  # the slot, not from the position the one before it processed: the one before the slot's
  # confirmed flush, which nothing moves before the new connection streams.
  defp assert_replays_from_the_slot(%{stack_id: stack_id}, processed) do
    assert_receive {:replay_applied, applied, slot_position, confirmed}
    assert is_integer(slot_position) and slot_position > 0
    assert slot_position == confirmed - 1
    assert applied == slot_position
    assert slot_position < processed

    # The collector took the position applied, and has accepted nothing below it since.
    assert {:ok, %Electric.Replication.LogOffset{tx_offset: started_at}} =
             ShapeLogCollector.last_processed_offset(stack_id)

    assert started_at >= slot_position and started_at < processed
  end

  # Each replay applied before a shape subsystem starts is reported as
  # `{:replay_applied, processed, slot_position, confirmed}`: the processed position it leaves,
  # the slot position the replication client stored, and the slot's confirmed flush then. The one
  # the earlier connection stored is dropped first, so only a position the replacement connection
  # read can be applied.
  defp report_the_replay(%{stack_id: stack_id, db_conn: db_conn, slot_name: slot_name}) do
    parent = self()
    table = Electric.LsnTracker.stack_ref(stack_id)
    true = :ets.delete(table, :slot_flushed_lsn)
    # The core supervisor starts the shape subsystem, which applies the replay.
    Repatch.allow(self(), GenServer.whereis(Electric.CoreSupervisor.name(stack_id)))

    Repatch.patch(Electric.LsnTracker, :apply_replay_from_slot, [mode: :shared], fn id ->
      slot_position =
        case :ets.lookup(table, :slot_flushed_lsn) do
          [{:slot_flushed_lsn, lsn}] -> Electric.Postgres.Lsn.to_integer(lsn)
          [] -> nil
        end

      %Postgrex.Result{rows: [[confirmed]]} =
        Postgrex.query!(
          db_conn,
          "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
          [slot_name]
        )

      :ok = Repatch.real(Electric.LsnTracker.apply_replay_from_slot(id))
      applied = Electric.LsnTracker.get_last_processed_lsn(id)

      send(
        parent,
        {:replay_applied, Electric.Postgres.Lsn.to_integer(applied), slot_position,
         Electric.Postgres.Lsn.to_integer(confirmed)}
      )

      :ok
    end)
  end

  # Each unload of the shape metadata is reported as
  # `{:metadata_unloaded, process, running, serving}`, with the processes of the shape subsystem
  # still running then and the shape status the API admits requests on. The owner is allowed the patch,
  # as it unloaded the metadata at the refresh before.
  defp report_the_unloads(%{stack_id: stack_id}) do
    parent = self()

    subsystem = [
      Electric.Shapes.Supervisor.name(stack_id),
      RelationTracker.name(stack_id),
      ShapeLogCollector.name(stack_id)
    ]

    Repatch.patch(ShapeStatus, :unload, [mode: :shared], fn id ->
      send(
        parent,
        {:metadata_unloaded, self(), Enum.filter(subsystem, &GenServer.whereis/1),
         Electric.StatusMonitor.status(stack_id).shape}
      )

      Repatch.real(ShapeStatus.unload(id))
    end)

    Repatch.allow(self(), GenServer.whereis(Electric.ShapeCache.ShapeStatusOwner.name(stack_id)))
  end

  # The next write of the owed purge fails, as it does on a full disk, and the record it failed
  # over reads again as the connected timeline: `{:marker_write_refused, manager}`.
  defp refuse_the_next_marker_write(_ctx) do
    parent = self()
    calls = :counters.new(1, [])

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      Process.put(:checked_timeline, pg_timeline)
      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)

    Repatch.patch(Timeline, :store_timeline, [mode: :shared], fn
      {_pg_id, -1} = timeline, opts ->
        :ok = :counters.add(calls, 1, 1)

        if :counters.get(calls, 1) == 1 do
          restore_the_record(:current, timeline, opts)
          send(parent, {:marker_write_refused, self()})
          {:error, :enospc}
        else
          Repatch.real(Timeline.store_timeline(timeline, opts))
        end

      timeline, opts ->
        Repatch.real(Timeline.store_timeline(timeline, opts))
    end)

    :ok
  end

  # The manager dies as it stops the shape subsystem, the stop queued at the core supervisor, which
  # is suspended until the test resumes it: `{:died_while_the_subsystem_stops, manager, core}`.
  defp die_while_the_subsystem_stops(%{stack_id: stack_id}) do
    parent = self()
    core = GenServer.whereis(Electric.CoreSupervisor.name(stack_id))
    resume_when_the_test_ends(core)

    Repatch.patch(Electric.CoreSupervisor, :stop_shapes_supervisor, [mode: :shared], fn opts ->
      :ok = :sys.suspend(core)
      spawn(fn -> Electric.CoreSupervisor.stop_shapes_supervisor(opts) end)
      true = eventually(fn -> queued_call?(core, :terminate_child) end)
      send(parent, {:died_while_the_subsystem_stops, self(), core})
      Process.exit(self(), :kill)
    end)
  end

  # The first manager dies once its timeline check returns, which it reports with the result.
  defp die_after_the_first_check(ctx) do
    parent = self()

    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      result = Repatch.super(Timeline.check(pg_timeline, opts, promotion))
      send(parent, {:died_after_the_check, self(), result})
      Process.exit(self(), :kill)
    end)

    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The record a manager connected to the test database stores.
  defp store_the_connected_timeline(%{db_conn: db_conn} = ctx) do
    %Postgrex.Result{rows: [[pg_id, timeline_id]]} =
      Postgrex.query!(
        db_conn,
        """
        SELECT system_identifier::text, (SELECT timeline_id::text FROM pg_control_checkpoint())
        FROM pg_control_system()
        """,
        []
      )

    :ok = Timeline.store_timeline({pg_id, timeline_id}, timeline_opts(ctx))
  end

  # The first manager's record of the purge its new slot owes waits in the queue of the suspended
  # monitor. The test gets `{:slot_purge_queued, killer, manager, monitor}`, and sends the killer
  # `:kill` to end the manager there, as a timeout of its call would.
  defp die_while_the_monitor_holds_the_slot_purge(%{stack_id: stack_id} = ctx) do
    parent = self()
    monitor = GenServer.whereis(StatusMonitor.name(stack_id))
    resume_when_the_test_ends(monitor)

    Repatch.patch(StatusMonitor, :owe_new_slot_purge, [mode: :shared], fn
      id, true ->
        :ok = :sys.suspend(monitor)
        manager = self()

        spawn(fn ->
          true = eventually(fn -> queued_call?(monitor, :owe_new_slot_purge) end)
          send(parent, {:slot_purge_queued, self(), manager, monitor})

          receive do
            :kill -> Process.exit(manager, :kill)
          end
        end)

        Repatch.real(StatusMonitor.owe_new_slot_purge(id, true))

      id, owed? ->
        Repatch.real(StatusMonitor.owe_new_slot_purge(id, owed?))
    end)

    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The manager that took over, once it has started the pools named. One that started while a pool
  # of the manager it replaced still stopped exits on the pool's name, and the next one takes over.
  # The snapshot pool comes after the replication lock.
  defp await_manager_past_its_start(stack_id, old, pools \\ [:admin]) do
    next = await_manager_other_than(stack_id, old)

    try do
      :sys.get_state(next)
    catch
      :exit, _reason -> await_manager_past_its_start(stack_id, next, pools)
    else
      %{pool_pids: pool_pids} ->
        if Enum.all?(pools, &match?({pid, _ready?} when is_pid(pid), pool_pids[&1])) do
          next
        else
          Process.sleep(1)
          await_manager_past_its_start(stack_id, old, pools)
        end
    end
  end

  defp queued_call?(pid, request) do
    {:messages, messages} = Process.info(pid, :messages)

    Enum.any?(messages, fn
      {:"$gen_call", _from, call} when is_tuple(call) -> elem(call, 0) == request
      _other -> false
    end)
  end

  # The process whose call `request` waits in the queue of `pid`, or nil.
  defp queued_caller(pid, request) do
    {:messages, messages} = Process.info(pid, :messages)

    Enum.find_value(messages, fn
      {:"$gen_call", {caller, _tag}, ^request} -> caller
      _other -> nil
    end)
  end

  # ExUnit stops the test's supervisor, which waits for the suspended core supervisor, once the
  # test process exits and before it runs `on_exit/1`: an assertion that failed while the core was
  # suspended hung the run there.
  defp resume_when_the_test_ends(pid) do
    test = self()

    spawn(fn ->
      ref = Process.monitor(test)

      receive do
        {:DOWN, ^ref, :process, ^test, _reason} -> resume_if_alive(pid)
      end
    end)
  end

  defp resume_if_alive(pid) do
    :sys.resume(pid)
  catch
    :exit, _reason -> :ok
  end

  defp stop_the_manager(stack_id) do
    manager = GenServer.whereis(Connection.Manager.name(stack_id))
    :ok = Electric.Connection.Manager.drop_replication_slot_on_stop(manager)

    :ok =
      Supervisor.terminate_child(
        Connection.Manager.Supervisor.name(stack_id),
        Connection.Manager
      )
  end

  # Each stop of the shape subsystem is reported as `{:subsystem_stopping, process, serving,
  # loaded?}`, with the shape status the API admits requests on and whether the catalog holds the
  # seeded shape, as the stop starts.
  defp report_the_stops(%{stack_id: stack_id, shape_handle: handle}) do
    parent = self()

    Repatch.patch(Electric.CoreSupervisor, :stop_shapes_supervisor, [mode: :shared], fn opts ->
      send(
        parent,
        {:subsystem_stopping, self(), Electric.StatusMonitor.status(stack_id).shape,
         ShapeStatus.has_shape_handle?(stack_id, handle)}
      )

      Repatch.real(Electric.CoreSupervisor.stop_shapes_supervisor(opts))
    end)
  end

  # Each timeline check is reported as `{:checked, manager, serving, loaded?}`, with the shape
  # status the API admits requests on and whether the catalog holds the seeded shape, as the check
  # starts, once the status monitor has taken what was sent to it before, an opening the refresh
  # cast included.
  defp report_the_serving_at_the_check(%{stack_id: stack_id, shape_handle: handle}) do
    parent = self()

    Repatch.patch(Timeline, :check, [mode: :shared, force: true], fn pg_timeline,
                                                                     opts,
                                                                     promotion ->
      :ok = Electric.StatusMonitor.wait_for_messages_to_be_processed(stack_id)

      send(
        parent,
        {:checked, self(), Electric.StatusMonitor.status(stack_id).shape,
         ShapeStatus.has_shape_handle?(stack_id, handle)}
      )

      Repatch.real(Timeline.check(pg_timeline, opts, promotion))
    end)
  end

  # The record reads again by the time the manager checks it, as the connected timeline.
  defp restore_the_record_in_the_check(_ctx) do
    Repatch.patch(Timeline, :check, [mode: :shared], fn pg_timeline, opts, promotion ->
      :ok = Repatch.real(Timeline.store_timeline(pg_timeline, opts))
      Repatch.super(Timeline.check(pg_timeline, opts, promotion))
    end)
  end

  # The manager that replaces `old` is allowed the shared patches before it can take the
  # replication lock: a session queued on the lock before `old` goes down takes it first, and
  # holds it until then.
  defp allow_patches_in_the_next_manager(%{stack_id: stack_id} = ctx, old) do
    parent = self()
    lock_holder = queue_on_replication_lock(ctx)

    spawn_link(fn ->
      pid = await_manager_other_than(stack_id, old)
      Repatch.allow(parent, pid)
      send(lock_holder, :release)
    end)
  end

  defp queue_on_replication_lock(%{db_conn: db_conn, db_config: db_config, slot_name: slot_name}) do
    lock_holder =
      start_supervised!(
        {Task,
         fn ->
           {:ok, conn} = Postgrex.start_link(Electric.Utils.deobfuscate_password(db_config))
           Postgrex.query!(conn, "SELECT pg_advisory_lock(hashtext($1))", [slot_name])

           receive do
             :release ->
               Postgrex.query!(conn, "SELECT pg_advisory_unlock(hashtext($1))", [slot_name])
           end
         end},
        id: :replication_lock_queue
      )

    assert eventually(fn ->
             %Postgrex.Result{rows: [[waiting]]} =
               Postgrex.query!(
                 db_conn,
                 """
                 SELECT count(*) FROM pg_locks
                 WHERE locktype = 'advisory' AND NOT granted
                   AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                 """,
                 []
               )

             waiting > 0
           end)

    lock_holder
  end

  # A table the publication can take, and an inspector that gives the shape on it its oid.
  defp create_items_table(%{db_conn: db_conn}) do
    Postgrex.query!(db_conn, "CREATE TABLE items (id int8 PRIMARY KEY, value text)", [])
    %{rows: [[oid]]} = Postgrex.query!(db_conn, "SELECT 'public.items'::regclass::oid", [])

    %{
      shape_inspector:
        Support.StubInspector.new(
          tables: [{oid, {"public", "items"}}],
          columns: [
            %{name: "id", type: "int8", type_id: {20, 1}, pk_position: 0, is_generated: false},
            %{name: "value", type: "text", type_id: {25, 1}, is_generated: false}
          ]
        )
    }
  end

  defp await_manager_other_than(stack_id, old) do
    case GenServer.whereis(Connection.Manager.name(stack_id)) do
      pid when is_pid(pid) and pid != old ->
        pid

      _ ->
        Process.sleep(1)
        await_manager_other_than(stack_id, old)
    end
  end

  # A record the timeline check cannot decode.
  defp store_unreadable_timeline(%{stack_id: stack_id, persistent_kv: persistent_kv}) do
    :ok = Electric.PersistentKV.set(persistent_kv, "timeline_id_#{stack_id}", "not a timeline")
    :ok
  end

  # A retry after a pause long enough that only the test fires it.
  defp pause_purge_retries(_ctx),
    do: %{connection_backoff: Connection.Manager.ConnectionBackoff.init(60_000, 60_000)}

  # Called by a patch in the manager, before it reports the failure the test waits on. A test that
  # fails before it traces the retry ends the wait, so the manager can stop with the test.
  defp await_the_retry_trace(test_pid) do
    ref = Process.monitor(test_pid)

    receive do
      :retry_traced -> Process.demonitor(ref, [:flush])
      {:DOWN, ^ref, :process, _pid, _reason} -> :ok
    end
  end

  # From here on, the test receives every timer the manager starts, and the reference it returns.
  defp trace_the_purge_retry(manager) do
    :erlang.trace_pattern({:erlang, :start_timer, 3}, [{:_, [], [{:return_trace}]}], [:local])
    on_exit(fn -> :erlang.trace_pattern({:erlang, :start_timer, 3}, false, [:local]) end)
    1 = :erlang.trace(manager, true, [:call])
    send(manager, :retry_traced)
  end

  # The manager waits for the retry it scheduled, the timer still running. A manager that retried
  # at once reported the retry before this, and one that restarted to retry has no such timer.
  defp await_the_purge_retry(manager) do
    {tref, {:retry_connection, step} = retry} = await_the_retry_timer(manager)
    1 = :erlang.trace(manager, false, [:call])
    assert step == :start_shapes_supervisor

    # The reference the manager keeps is the one of the timer that carries the retry.
    assert %{connection_backoff: {_, ^tref}} = :sys.get_state(manager)
    assert is_integer(:erlang.read_timer(tref))
    refute_received {:purge_step_retried, _, _}
    {tref, retry}
  end

  # Trace messages come in the order the manager made the calls, so the return that follows the
  # retry's call is that timer's.
  defp await_the_retry_timer(manager) do
    timeout = ExUnit.configuration()[:assert_receive_timeout]

    receive do
      {:trace, ^manager, :call,
       {:erlang, :start_timer, [_time, ^manager, {:retry_connection, _} = retry]}} ->
        assert_receive {:trace, ^manager, :return_from, {:erlang, :start_timer, 3}, tref}
        {tref, retry}

      {:trace, ^manager, :call, {:erlang, :start_timer, _args}} ->
        await_the_retry_timer(manager)

      {:trace, ^manager, :return_from, {:erlang, :start_timer, 3}, _tref} ->
        await_the_retry_timer(manager)
    after
      timeout -> flunk("the manager scheduled no retry")
    end
  end

  # Delivers the message the manager scheduled, as its timer would have.
  defp fire_the_purge_retry(manager, {tref, retry}) do
    :erlang.cancel_timer(tref)
    send(manager, {:timeout, tref, retry})
  end

  defp call_real(Electric.Shapes.Supervisor, :reset_storage, opts),
    do: Repatch.real(Electric.Shapes.Supervisor.reset_storage(opts))

  defp call_real(Electric.Replication.PersistentReplicationState, :reset, opts),
    do: Repatch.real(Electric.Replication.PersistentReplicationState.reset(opts))

  defp call_real(Electric.Postgres.Inspector, :reset, inspector),
    do: Repatch.real(Electric.Postgres.Inspector.reset(inspector))

  defp seed_completed_shape(%{stack_id: stack_id, storage: storage} = ctx) do
    shape = Shape.new!("items", inspector: Map.get(ctx, :shape_inspector, @stub_inspector))
    {:ok, handle} = ShapeStatus.add_shape(stack_id, shape)
    :ok = ShapeStatus.mark_snapshot_complete(stack_id, handle)
    :ok = WriteBuffer.flush_sync(stack_id)
    assert ShapeStatus.has_shape_handle?(stack_id, handle)

    shape_storage = Storage.for_shape(handle, storage)
    {:ok, _} = Storage.start_link(shape_storage)
    :ok = Storage.mark_snapshot_as_started(shape_storage)
    assert Storage.snapshot_started?(shape_storage)

    %{shape_handle: handle, shape_storage: shape_storage}
  end

  # The lock breaker ends a session that holds the replication lock when the slot is inactive,
  # which the test's lock holder is.
  defp disable_lock_breaker(_ctx), do: %{lock_breaker_guard: fn -> false end}

  defp patch_the_first_storage_reset(ctx, result) do
    parent = self()

    Repatch.patch(Electric.Shapes.Supervisor, :reset_storage, [mode: :shared], fn _opts ->
      send(parent, :storage_reset_failed)
      result.()
    end)

    allow_patches_in_the_first_manager(ctx)
    :ok
  end

  # The manager needs the replication lock before it reaches its purge. Holding the lock until
  # the first manager is allowed the shared patch, and watched, keeps that manager from reaching
  # the purge unpatched. Its exit reason comes back as `{:first_manager_down, reason}`.
  defp allow_patches_in_the_first_manager(%{stack_id: stack_id} = ctx) do
    parent = self()
    lock_holder = hold_replication_lock(ctx)

    spawn_link(fn ->
      pid = await_manager(stack_id)
      Repatch.allow(parent, pid)
      ref = Process.monitor(pid)
      send(lock_holder, :release)

      receive do
        {:DOWN, ^ref, :process, ^pid, reason} -> send(parent, {:first_manager_down, reason})
      end
    end)
  end

  defp hold_replication_lock(%{db_conn: db_conn, slot_name: slot_name}) do
    parent = self()

    lock_holder =
      start_supervised!(
        {Task,
         fn ->
           DBConnection.run(db_conn, fn conn ->
             Postgrex.query!(conn, "SELECT pg_advisory_lock(hashtext($1))", [slot_name])
             send(parent, :replication_lock_held)

             receive do
               :release ->
                 Postgrex.query!(conn, "SELECT pg_advisory_unlock(hashtext($1))", [slot_name])
             end
           end)
         end},
        id: :replication_lock_holder
      )

    assert_receive :replication_lock_held
    lock_holder
  end

  defp await_manager(stack_id) do
    case GenServer.whereis(Connection.Manager.name(stack_id)) do
      nil ->
        Process.sleep(1)
        await_manager(stack_id)

      pid ->
        pid
    end
  end

  # A publication and a slot that already exist keep the slot-creation purge out of the picture:
  # a publication the client creates makes it recreate the slot.
  # A shape kept across a restart is on a table the publication already carries: the shapes on a
  # table it adds are from before the table was replicated, and its first pass removes them.
  defp publish_items_table(%{db_conn: db_conn, stack_id: stack_id}) do
    publication_name = "electric_conn_mgr_test_pub_#{:erlang.phash2(stack_id)}"
    Postgrex.query!(db_conn, ~s|ALTER PUBLICATION "#{publication_name}" ADD TABLE items|, [])
    :ok
  end

  defp create_existing_publication_and_slot(%{
         db_conn: db_conn,
         slot_name: slot_name,
         stack_id: stack_id
       }) do
    publication_name = "electric_conn_mgr_test_pub_#{:erlang.phash2(stack_id)}"
    Postgrex.query!(db_conn, ~s|CREATE PUBLICATION "#{publication_name}"|, [])

    Postgrex.query!(db_conn, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", [
      slot_name
    ])

    :ok
  end

  defp queue_snapshot_complete_mark(stack_id) do
    shape = Shape.new!("items", inspector: @stub_inspector)
    {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-0")
    :ok = WriteBuffer.flush_sync(stack_id)
    # The test's shape db flushes only when asked, so the mark stays queued.
    :ok = ShapeDb.mark_snapshot_complete(stack_id, "handle-0")
  end

  # The replication client's advisory lock, as Postgres grants it.
  defp replication_lock_count(ctx) do
    %{rows: [[count]]} =
      Postgrex.query!(
        ctx.db_conn,
        """
        SELECT count(*) FROM pg_locks
        WHERE locktype = 'advisory' AND granted AND objsubid = 1
          AND database = (SELECT oid FROM pg_database WHERE datname = $1)
          AND ((classid::bigint << 32) | objid::bigint) = hashtext($2)
        """,
        [ctx.db_config[:database], ctx.slot_name]
      )

    count
  end

  # A streaming replication client past its 1 s idle timeout, with a snapshot-complete mark queued.
  defp idle_replication_client(ctx) do
    wait_until_active(ctx.stack_id)
    queue_snapshot_complete_mark(ctx.stack_id)

    manager = GenServer.whereis(Connection.Manager.name(ctx.stack_id))
    # The client starts its idle clock with the first message of the stream.
    assert eventually(fn -> match?(%{current_step: :streaming}, :sys.get_state(manager)) end)
    %{replication_client_pid: client} = :sys.get_state(manager)
    Process.sleep(1_100)

    %{manager: manager, client: client, buffer: GenServer.whereis(WriteBuffer.name(ctx.stack_id))}
  end

  defp drain_waiting?(buffer) do
    {:messages, messages} = Process.info(buffer, :messages)
    Enum.any?(messages, &match?({:"$gen_call", _from, {:drain, _deadline}}, &1))
  end

  # The replication session's backend, and when it last sent Postgres a status update.
  defp replication_replies(ctx) do
    %{rows: rows} =
      Postgrex.query!(
        ctx.db_conn,
        """
        SELECT r.pid, r.reply_time FROM pg_stat_replication r
        JOIN pg_replication_slots s ON s.active_pid = r.pid
        WHERE s.slot_name = $1
        """,
        [ctx.slot_name]
      )

    rows
  end

  # Polls for a state another process reaches on its own time, such as a backend dropping its
  # locks once it has handled the termination it was sent.
  defp eventually(check, attempts \\ 50) do
    cond do
      check.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(100)
        eventually(check, attempts - 1)
    end
  end

  defp snapshot_complete_in_file(ctx, handle) do
    {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
    {:ok, reader} = ShapeDb.Sqlite3.open(path, [])

    try do
      ShapeDb.Connection.fetch_one(
        reader,
        "SELECT snapshot_complete FROM shapes WHERE handle = ?1",
        [{:blob, handle}]
      )
    after
      ShapeDb.Connection.close(reader)
    end
  end

  # Process allowance doesn't follow the supervision tree, so allow the manager process to
  # see the shared patch as soon as it has started.
  defp allow_patches_in_manager(stack_id, parent) do
    spawn_link(fn ->
      Stream.repeatedly(fn -> 0 end)
      |> Enum.reduce_while(0, fn _, _ ->
        case GenServer.whereis(Connection.Manager.name(stack_id)) do
          nil ->
            {:cont, 0}

          pid ->
            Repatch.allow(parent, pid)
            {:halt, 0}
        end
      end)
    end)
  end

  # Given the manager that started the purge, the purge is done by that manager, with no restart.
  defp assert_purge_done(
         %{stack_id: stack_id, persistent_kv: persistent_kv} = ctx,
         manager \\ nil
       ) do
    assert_metadata_reset_after_active(ctx)

    assert {_pg_id, timeline_id} =
             Timeline.load_timeline(stack_id: stack_id, persistent_kv: persistent_kv)

    refute timeline_id == -1

    manager_pid = GenServer.whereis(Electric.Connection.Manager.name(stack_id))
    if manager, do: assert(manager_pid == manager)
    :ok = Electric.Connection.Manager.drop_replication_slot_on_stop(manager_pid)

    :ok =
      Supervisor.terminate_child(
        Connection.Manager.Supervisor.name(stack_id),
        Connection.Manager
      )
  end

  # The manager that unloaded the shape metadata loaded it again, with no purge and no restart.
  defp assert_shapes_kept(%{stack_id: stack_id, shape_handle: handle} = ctx, manager) do
    wait_until_active(stack_id)
    refute_received {:storage_reset, _, _}
    assert ShapeStatus.has_shape_handle?(stack_id, handle)
    assert ShapeDb.handle_exists?(stack_id, handle)
    assert Storage.snapshot_started?(ctx.shape_storage)
    assert {_pg_id, timeline_id} = Timeline.load_timeline(timeline_opts(ctx))
    refute timeline_id == -1

    assert GenServer.whereis(Electric.Connection.Manager.name(stack_id)) == manager
    :ok = Electric.Connection.Manager.drop_replication_slot_on_stop(manager)

    :ok =
      Supervisor.terminate_child(
        Connection.Manager.Supervisor.name(stack_id),
        Connection.Manager
      )
  end

  defp assert_metadata_reset_after_active(%{
         stack_id: stack_id,
         persistent_kv: persistent_kv,
         pg_inspector_table: pg_inspector_table
       }) do
    wait_until_active(stack_id)

    assert :ets.tab2list(pg_inspector_table) == []

    assert %{table_to_id: %{}, id_to_table_info: %{}} =
             PersistentReplicationState.get_tracked_relations(
               stack_id: stack_id,
               persistent_kv: persistent_kv
             )
  end

  defp wait_until_active(stack_id) do
    assert_receive {:stack_status, _, :waiting_for_connection_lock}
    assert_receive {:stack_status, _, :connection_lock_acquired}
    assert_receive {:stack_status, _, :ready}

    StatusMonitor.wait_until_active(stack_id,
      timeout: ExUnit.configuration()[:assert_receive_timeout]
    )

    assert StatusMonitor.status(stack_id) == %{conn: :up, shape: :up}
  end

  defp monitor_replication_client(stack_id) do
    stack_id
    |> Electric.Postgres.ReplicationClient.name()
    |> GenServer.whereis()
    |> Process.monitor()
  end

  # Polls until the source has acquired its lock and reached the configuring stage
  # (conn: :starting). StatusMonitor.wait_until/3 only supports :read_only/:active,
  # which a slot-creation-blocked source never reaches, so we poll the status here.
  defp wait_until_conn_starting(stack_id, attempts \\ 200)

  defp wait_until_conn_starting(_stack_id, 0),
    do: flunk("source did not reach conn: :starting in time")

  defp wait_until_conn_starting(stack_id, attempts) do
    if StatusMonitor.status(stack_id).conn == :starting do
      :ok
    else
      Process.sleep(25)
      wait_until_conn_starting(stack_id, attempts - 1)
    end
  end

  defp wait_for_pg_backend_pid(manager_pid, attempts \\ 500) do
    state = :sys.get_state(manager_pid)

    if is_nil(state.replication_pg_backend_pid) and attempts > 0 do
      Process.sleep(10)
      wait_for_pg_backend_pid(manager_pid, attempts - 1)
    else
      state
    end
  end
end
