defmodule Electric.Replication.PublicationManagerTest do
  # This module tests the publication manager against a real database.
  #
  # Specifically, we verify that the publication in Postgres is updated correctly when a table
  # that's part of it is dropped or renamed in the database, and that the corresponding shapes
  # are cleaned up.

  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.TestUtils

  require Repatch
  alias Electric.Utils
  alias Electric.PersistentKV
  alias Electric.Replication.Changes
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.ShapeLogCollector.FlushTracker
  alias Electric.Postgres.Configuration
  alias Electric.Replication.PublicationManager
  alias Electric.ShapeCache
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Consumer
  alias Electric.SnapshotError

  @shape_handle_1 "pub_mgr_db_test_shape_handle_1"
  @shape_handle_2 "pub_mgr_db_test_shape_handle_2"
  @shape_handle_3 "pub_mgr_db_test_shape_handle_3"
  @where_clause_1 %Expr{query: "id = '1'", used_refs: %{["id"] => :text}}
  @where_clause_2 %Expr{query: "id = '2'", used_refs: %{["id"] => :text}}
  @where_clause_3 %Expr{query: "id = '3'", used_refs: %{["id"] => :text}}

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_in_memory_storage,
    :with_persistent_kv,
    :with_shape_status,
    :with_unique_db,
    :with_sql_execute,
    :with_publication_name,
    :with_publication,
    :with_basic_tables
  ]

  setup ctx do
    relation = {"public", "items"}
    relation_oid = lookup_relation_oid(ctx.pool, relation)

    %{
      relation: relation,
      relation_with_oid: {relation_oid, relation}
    }
  end

  setup ctx do
    handles =
      for where_clause <- Map.get(ctx, :existing_where_clauses, []) do
        shape = generate_shape(ctx.relation_with_oid, where_clause)
        {:ok, shape_handle} = Electric.ShapeCache.ShapeStatus.add_shape(ctx.stack_id, shape)
        shape_handle
      end

    # as a boot finds them: stored with their snapshot, and read once to rebuild the shape metadata
    if ctx[:shape_metadata_from_the_store] do
      for handle <- handles do
        :ok = Electric.ShapeCache.ShapeStatus.mark_snapshot_complete(ctx.stack_id, handle)
      end

      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = Electric.ShapeCache.ShapeStatus.initialize(ctx.stack_id)
    end

    :ok
  end

  # Reports each read of the stored shapes from here on, whichever process makes it: every
  # checkout of the store's connection is reported with what it is for.
  setup %{stack_id: stack_id, test: test} do
    parent = self()
    handler_id = "store-reads-#{inspect(test)}"

    :telemetry.attach(
      handler_id,
      [:electric, :shape_db, :pool, :checkout],
      fn _event, _measurements, metadata, _config ->
        if metadata.stack_id == stack_id and metadata.label in [:reduce_shapes, :list_shapes] do
          send(parent, {:read_of_the_stored_shapes, metadata.label})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  setup ctx do
    if fault = ctx[:barrier_write_fault] do
      {:ok, write_fault} = Agent.start_link(fn -> fault end)

      barrier_kv =
        PersistentKV.Serialized.new!(
          backend: ctx.persistent_kv,
          encoder: {__MODULE__, :encode_barrier_record, [write_fault, self()]}
        )

      %{barrier_kv: barrier_kv, write_fault: write_fault}
    else
      :ok
    end
  end

  setup ctx do
    activate_mocks_for_descendant_procs(PublicationManager.Configurator)
    task_supervisor = start_supervised!(Task.Supervisor)

    barrier_kv = Map.get(ctx, :barrier_kv, ctx.persistent_kv)
    configurator_pool = configurator_pool(ctx)

    %{publication_manager: {_, pub_mgr_opts}} =
      with_publication_manager(%{
        module: ctx.module,
        test: ctx.test,
        stack_id: ctx.stack_id,
        update_debounce_timeout: Access.get(ctx, :update_debounce_timeout, 0),
        publication_name: ctx.publication_name,
        pool: configurator_pool,
        persistent_kv: barrier_kv,
        database_timeline: Map.get(ctx, :database_timeline),
        manual_table_publishing?: Map.get(ctx, :manual_table_publishing?, false),
        admin_pool_size: Map.get(ctx, :admin_pool_size)
      })

    test_pid = self()

    Repatch.patch(
      Electric.ShapeCache.ShapeCleaner,
      :remove_shapes_for_relations,
      [mode: :shared],
      fn _stack_id, relations, _reason ->
        send(test_pid, {:remove_shapes_for_relations, relations})
      end
    )

    # The removal of the shapes on a table out of the publication, as far as the shape catalog
    # goes, and whether the table is in the publication at that point. It is over as it returns.
    Repatch.patch(
      Electric.ShapeCache.ShapeCleaner,
      :start_removing_shapes,
      [mode: :shared],
      fn stack_id, handles, _reason ->
        for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
        send(test_pid, {:removed, handles, fetch_pub_tables(ctx)})
        {:ok, completed_removal(ended_process())}
      end
    )

    # notify when publication is configured to avoid timing issues
    config_notification = "test"

    Repatch.allow(test_pid, PublicationManager.Configurator.name(ctx.stack_id))
    Repatch.allow(test_pid, PublicationManager.RelationTracker.name(ctx.stack_id))

    relation = {"public", "items"}
    relation_oid = lookup_relation_oid(ctx.pool, relation)

    %{
      pub_mgr_opts: pub_mgr_opts,
      relation: relation,
      relation_with_oid: {relation_oid, relation},
      task_supervisor: task_supervisor,
      config_notification: config_notification,
      configurator_pool: configurator_pool
    }
  end

  # The pool the configurator gets as its admin pool: the test's own, or, with a
  # `configurator_pool_size` tag, one of that many connections apart from the one the test's writers
  # use, as a stack's admin pool is apart from its snapshot pool.
  defp configurator_pool(%{configurator_pool_size: size} = ctx) do
    opts =
      ctx.db_config
      |> Electric.Utils.deobfuscate_password()
      |> Keyword.merge(
        backoff_type: :stop,
        max_restarts: 0,
        types: PgInterop.Postgrex.Types,
        pool_size: size
      )

    start_link_supervised!(Supervisor.child_spec({Postgrex, opts}, id: :configurator_pool))
  end

  defp configurator_pool(ctx), do: ctx.pool

  describe "wait_for_restore/1" do
    @tag existing_where_clauses: []
    test "immediately completes if nothing to restore", ctx do
      notify_alter_queries()
      assert :ok == PublicationManager.wait_for_restore(ctx.stack_id)
      assert_pub_tables(ctx, [])
      refute_receive {:alter_publication, _, _}
    end

    @tag existing_where_clauses: [@where_clause_1]
    test "restores existing shapes", ctx do
      assert :ok == PublicationManager.wait_for_restore(ctx.stack_id)
      assert_pub_tables(ctx, [ctx.relation])
    end

    # The store has one connection, held by a read until it has walked every shape: a read here
    # makes the collector's restore, which reads them too, wait for it.
    @tag existing_where_clauses: [@where_clause_1, @where_clause_2]
    test "restores existing shapes without reading the stored shapes", ctx do
      assert :ok == PublicationManager.wait_for_restore(ctx.stack_id)
      assert_pub_tables(ctx, [ctx.relation])
      refute_received {:read_of_the_stored_shapes, _}
    end

    # A boot reads the stored shapes once, in the collector's restore, which builds the routing
    # state the filters come from.
    @tag existing_where_clauses: [@where_clause_1], shape_metadata_from_the_store: true
    test "restores the shapes a boot finds in the store once the collector has read them", ctx do
      restore = Task.async(fn -> PublicationManager.wait_for_restore(ctx.stack_id) end)

      refute Task.yield(restore, 200)
      refute_received {:read_of_the_stored_shapes, _}

      # as the collector's restore does, from its read of the stored shapes
      routing_state =
        ShapeStatus.reduce_shapes(ctx.stack_id, ShapeStatus.new_routing_state(), fn
          {handle, shape}, routing_state ->
            ShapeStatus.add_to_routing_state(routing_state, handle, shape)
        end)

      :ok = ShapeStatus.restore_routing_state(ctx.stack_id, routing_state)

      assert :ok == Task.await(restore)
      assert_pub_tables(ctx, [ctx.relation])
    end
  end

  describe "add_shape/3" do
    test "adds a single relation", ctx do
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)

      assert_pub_tables(ctx, [ctx.relation])
    end

    test "accepts shape with generated columns if supported", ctx do
      supported_features = fetch_supported_features(ctx.db_conn)

      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)

      shape = %Electric.Shapes.Shape{
        shape
        | flags: Map.put(shape.flags, :selects_generated_columns, true)
      }

      if supported_features.supports_generated_column_replication do
        assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
        assert_pub_tables(ctx, [ctx.relation])
      else
        assert_raise Electric.DbConfigurationError,
                     ~r/does not publish generated columns/,
                     fn ->
                       PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
                     end
      end
    end

    test "ignores subsequent shapes for same handle", ctx do
      notify_alter_queries()
      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      shape2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape2)

      assert_pub_tables(ctx, [ctx.relation])
      assert_received {:alter_publication, _, _}
      refute_receive {:alter_publication, _, _}, 200
    end

    test "accepts multiple relations", ctx do
      Postgrex.query!(
        ctx.pool,
        "CREATE TABLE other_table (id UUID PRIMARY KEY, value TEXT NOT NULL)",
        []
      )

      alt_relation = {"public", "other_table"}
      alt_relation_oid = lookup_relation_oid(ctx.pool, alt_relation)

      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      shape2 = generate_shape({alt_relation_oid, alt_relation}, @where_clause_1)

      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
      assert_pub_tables(ctx, [ctx.relation])

      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape2)
      assert_pub_tables(ctx, [ctx.relation, alt_relation])
    end

    @tag update_debounce_timeout: 100
    test "doesn't update when adding same relation again", ctx do
      notify_alter_queries()
      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      shape2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      shape3 = generate_shape(ctx.relation_with_oid, @where_clause_3)

      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape2)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_3, shape3)

      assert_pub_tables(ctx, [ctx.relation])
      assert_received {:alter_publication, _, _}
      refute_receive {:alter_publication, _, _}, 200
    end

    test "keeps the table in the publication when shapes with different where clauses are added and removed",
         ctx do
      notify_alter_queries()
      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
      assert_receive {:alter_publication, _, _}
      assert_pub_tables(ctx, [ctx.relation])

      shape_2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape_2)
      refute_receive {:alter_publication, _, _}, 200
      assert_pub_tables(ctx, [ctx.relation])

      assert :ok == PublicationManager.remove_shape(ctx.stack_id, @shape_handle_2)
      refute_receive {:alter_publication, _, _}, 200
      assert_pub_tables(ctx, [ctx.relation])

      assert :ok == PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1)
      assert_receive {:alter_publication, _, _}
      assert_pub_tables(ctx, [])
    end

    test "should continue to fail with same error", ctx do
      Repatch.patch(
        Electric.Postgres.Configuration,
        :add_table_to_publication,
        [mode: :shared],
        fn _, _, _ -> {:error, %RuntimeError{message: "some error"}} end
      )

      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)

      assert_raise RuntimeError, "some error", fn ->
        PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
      end

      assert_raise RuntimeError, "some error", fn ->
        PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
      end
    end
  end

  describe "cast issuance" do
    @tag update_debounce_timeout: 0
    test "issues a single configure cast while a submission is in flight", ctx do
      test_pid = self()

      # Capture casts to the Configurator without forwarding them, so the
      # submission never produces a result and `committed` stays behind
      # `submitted` for the whole test — exactly the in-flight window.
      Repatch.patch(
        PublicationManager.Configurator,
        :configure_publication,
        [mode: :shared],
        fn _stack_id, filters -> send(test_pid, {:configure_cast, filters}) end
      )

      # Three shapes on the SAME relation (same oid) → a single relation
      # transition. Run them async because, with no result delivered, each
      # add_shape blocks as a waiter.
      for {handle, where} <- [
            {@shape_handle_1, @where_clause_1},
            {@shape_handle_2, @where_clause_2},
            {@shape_handle_3, @where_clause_3}
          ] do
        shape = generate_shape(ctx.relation_with_oid, where)
        run_async(fn -> PublicationManager.add_shape(ctx.stack_id, handle, shape) end)
      end

      # Exactly one cast, carrying the single-relation filter set.
      assert_receive {:configure_cast, {_revision, filters}}
      assert MapSet.size(filters) == 1
      tracker = ctx.pub_mgr_opts[:server]
      oid = elem(ctx.relation_with_oid, 0)
      assert wait_until(fn -> length(Map.get(:sys.get_state(tracker).waiters, oid, [])) == 3 end)
      refute_received {:configure_cast, _}
    end

    @tag update_debounce_timeout: 0
    test "re-issues a configure cast after a global configuration error", ctx do
      test_pid = self()

      Repatch.patch(
        PublicationManager.Configurator,
        :configure_publication,
        [mode: :shared],
        fn _stack_id, _filters -> send(test_pid, :configure_cast) end
      )

      relation_tracker = PublicationManager.RelationTracker.name(ctx.stack_id)

      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      run_async(fn -> PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1) end)

      # First submission is now in flight.
      assert_receive :configure_cast

      # Simulate the in-flight chain dying with a global configuration error.
      GenServer.cast(
        relation_tracker,
        {:configuration_error, {:error, %RuntimeError{message: "boom"}}}
      )

      # A subsequent add on the SAME relation (no change to prepared filters)
      # must re-arm the retry path and issue a fresh cast.
      shape2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      run_async(fn -> PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape2) end)

      assert_receive :configure_cast
    end

    @tag update_debounce_timeout: 0
    test "still issues a cast for a new relation while another submission is in flight", ctx do
      Postgrex.query!(
        ctx.pool,
        "CREATE TABLE other_table (id UUID PRIMARY KEY, value TEXT NOT NULL)",
        []
      )

      alt_relation = {"public", "other_table"}
      alt_relation_oid = lookup_relation_oid(ctx.pool, alt_relation)

      test_pid = self()

      Repatch.patch(
        PublicationManager.Configurator,
        :configure_publication,
        [mode: :shared],
        fn _stack_id, filters -> send(test_pid, {:configure_cast, filters}) end
      )

      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      run_async(fn -> PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1) end)

      # First relation's submission is in flight (no result delivered).
      assert_receive {:configure_cast, {first_revision, first_filters}}
      assert MapSet.size(first_filters) == 1

      # Adding a DIFFERENT relation changes `prepared`, so a fresh cast must
      # still be issued despite the in-flight submission.
      shape2 = generate_shape({alt_relation_oid, alt_relation}, @where_clause_1)
      run_async(fn -> PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape2) end)

      assert_receive {:configure_cast, {second_revision, second_filters}}
      assert MapSet.size(second_filters) == 2
      assert second_revision > first_revision
    end

    @tag update_debounce_timeout: 0
    test "re-issues a configure cast after a per-relation configuration error", ctx do
      test_pid = self()

      Repatch.patch(
        PublicationManager.Configurator,
        :configure_publication,
        [mode: :shared],
        fn _stack_id, _filters -> send(test_pid, :configure_cast) end
      )

      relation_tracker = PublicationManager.RelationTracker.name(ctx.stack_id)
      oid_rel = ctx.relation_with_oid

      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      run_async(fn -> PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1) end)

      # First submission is now in flight.
      assert_receive :configure_cast

      # Simulate the in-flight chain reporting a per-relation error for the
      # relation it was configuring.
      GenServer.cast(
        relation_tracker,
        {:relation_configuration_result, oid_rel, {:error, %RuntimeError{message: "boom"}}}
      )

      # A subsequent add on the SAME relation (no change to prepared filters)
      # must re-arm the retry path and issue a fresh cast.
      shape2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      run_async(fn -> PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape2) end)

      assert_receive :configure_cast
    end
  end

  describe "remove_shape/2" do
    test "removes single relation when last shape removed", ctx do
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
      assert_pub_tables(ctx, [ctx.relation])
      assert :ok == PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1)
      assert_pub_tables(ctx, [])
    end

    @tag update_debounce_timeout: 50
    test "subsequent additions should wait for reconfiguration", ctx do
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      assert :ok == PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
      assert_pub_tables(ctx, [ctx.relation])

      assert :ok == PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1)
      assert_pub_tables(ctx, [])

      test_pid = self()

      run_async(fn ->
        res = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
        send(test_pid, {:add_shape_result, res})
      end)

      refute_receive {:add_shape_result, _}, 20
      assert_pub_tables(ctx, [ctx.relation])
      assert_receive {:add_shape_result, :ok}
    end

    @tag update_debounce_timeout: 50
    test "deduplicates shape handle operations", ctx do
      notify_alter_queries()
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)

      task1 =
        Task.async(fn ->
          PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape)
        end)

      task2 =
        Task.async(fn -> PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1) end)

      task3 =
        Task.async(fn -> PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1) end)

      Task.await_many([task1, task2, task3])

      assert_pub_tables(ctx, [ctx.relation])
      assert_received {:alter_publication, _, _}
      refute_receive {:alter_publication, _, _}, 300
    end

    @tag update_debounce_timeout: 50
    test "reference counts relations to avoid premature removal", ctx do
      notify_alter_queries()
      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      shape2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      shape3 = generate_shape(ctx.relation_with_oid, @where_clause_3)

      task1 =
        Task.async(fn ->
          PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
        end)

      task2 =
        Task.async(fn ->
          PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape2)
        end)

      task3 =
        Task.async(fn ->
          PublicationManager.add_shape(ctx.stack_id, @shape_handle_3, shape3)
        end)

      Task.await_many([task1, task2, task3])
      assert_receive {:alter_publication, _, _}
      assert_pub_tables(ctx, [ctx.relation])

      # Remove one handle; relation should stay
      assert :ok == PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1)
      refute_receive {:alter_publication, _, _}
      assert_pub_tables(ctx, [ctx.relation])
    end
  end

  describe "concurrent operations" do
    @tag update_debounce_timeout: 100
    test "queues up requests to add same shape handle", ctx do
      notify_alter_queries()
      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)
      test_pid = self()

      run_async(fn ->
        :ok = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
        send(test_pid, :task1_done)
      end)

      run_async(fn ->
        :ok = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
        send(test_pid, :task2_done)
      end)

      refute_receive :task1_done, 50
      refute_receive {:alter_publication, _, _}, 0
      refute_receive :task2_done, 0

      assert_receive :task1_done
      assert_receive :task2_done
      assert_pub_tables(ctx, [ctx.relation])
      assert_received {:alter_publication, _, _}
      refute_received {:alter_publication, _, _}
    end

    @tag update_debounce_timeout: 100
    test "invalidates add requests if removed immediately", ctx do
      notify_alter_queries()
      shape1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      add_task =
        Task.async(fn ->
          assert_raise RuntimeError, "Shape removed before updating publication", fn ->
            PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape1)
          end
        end)

      remove_task =
        Task.async(fn ->
          Process.sleep(5)
          :ok = PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1)
        end)

      Task.await_many([add_task, remove_task])

      refute_receive {:alter_publication, _, _}, 200
    end
  end

  describe "publication misonfiguration" do
    setup ctx do
      test_pid = self()

      # The connection manager whose admin pool answers the check.
      start_supervised!(%{
        id: :manager,
        start:
          {Agent, :start_link,
           [fn -> nil end, [name: Electric.Connection.Manager.name(ctx.stack_id)]]}
      })

      Repatch.patch(
        Electric.Connection.Restarter,
        :restart_connection_subsystem,
        [mode: :shared],
        fn _, _ ->
          send(test_pid, :connection_subsystem_restarted)
          :ok
        end
      )
    end

    test "handles publication being deleted during operation", ctx do
      Postgrex.query!(ctx.pool, "DROP PUBLICATION #{ctx.publication_name};", [])

      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      assert_raise Electric.DbConfigurationError,
                   "Publication #{Utils.quote_name(ctx.publication_name)} not found in the database",
                   fn ->
                     PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
                   end

      assert_receive :connection_subsystem_restarted
      refute_receive {:remove_shapes_for_relations, _}
      assert [] == fetch_pub_tables(ctx)
    end

    test "handles publication not publishing all operations", ctx do
      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} SET (publish = 'insert, update');",
        []
      )

      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      assert_raise Electric.DbConfigurationError,
                   "Publication #{Utils.quote_name(ctx.publication_name)} does not " <>
                     "publish all required operations: INSERT, UPDATE, DELETE, TRUNCATE",
                   fn ->
                     PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
                   end

      assert_receive :connection_subsystem_restarted
      refute_receive {:remove_shapes_for_relations, _}
      assert [] == fetch_pub_tables(ctx)
    end

    # The check reads the publication before the snapshot the pass acts on. An operator who stops
    # it publishing deletes in between leaves its tables in place, and a shape served then would
    # miss every delete: the shape waits for a check that reads the change.
    @tag connection_opt_overrides: [pool_size: 3]
    test "fails a shape on a publication that stops publishing deletes after its check", ctx do
      publication = ctx.publication_name

      # The configurator's first pass, for no shape, would drop a table added before it ends. Added
      # after it, the table is one the shape's pass finds in place, and serves as it stands.
      :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{publication} ADD TABLE items", [])
      armed = :atomics.new(1, [])

      Repatch.patch(Configuration, :check_publication_status!, [mode: :shared], fn pool, name ->
        status = Repatch.real(Configuration.check_publication_status!(pool, name))

        if :atomics.compare_exchange(armed, 1, 1, 0) == :ok do
          Postgrex.query!(
            ctx.pool,
            "ALTER PUBLICATION #{publication} SET (publish = 'insert, update, truncate')",
            []
          )
        end

        status
      end)

      :atomics.put(armed, 1, 1)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert_raise Electric.DbConfigurationError,
                   "Publication #{Utils.quote_name(publication)} does not " <>
                     "publish all required operations: INSERT, UPDATE, DELETE, TRUNCATE",
                   fn -> PublicationManager.add_shape(ctx.stack_id, handle, shape) end

      assert :atomics.get(armed, 1) == 0
      assert_receive :connection_subsystem_restarted
      refute_received {:remove_shapes_for_relations, _}
    end

    test "handles publication not being owned", ctx do
      patch_queries_to_unprivileged()

      relation_with_oid = ctx.relation_with_oid
      shape_1 = generate_shape(relation_with_oid, @where_clause_1)

      assert_raise Electric.DbConfigurationError,
                   "Database table #{Utils.relation_to_sql(ctx.relation) |> Utils.quote_name()} is missing from " <>
                     "the publication #{Utils.quote_name(ctx.publication_name)} and " <>
                     "Electric lacks privileges to add it",
                   fn ->
                     PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
                   end

      assert_receive {:remove_shapes_for_relations, [^relation_with_oid]}
      assert [] == fetch_pub_tables(ctx)
    end
  end

  describe "publication misconfiguration found across a restart" do
    setup ctx do
      # The configurator's first check, which its start runs, ends before the stand-in connection
      # subsystem starts: a supervisor under the subsystem's name, holding a stand-in connection
      # manager under the manager's name.
      :sys.get_state(
        PublicationManager.Configurator.name(ctx.stack_id),
        ExUnit.configuration()[:assert_receive_timeout]
      )

      test_pid = self()

      manager = %{
        id: :manager,
        start:
          {Agent, :start_link,
           [fn -> nil end, [name: Electric.Connection.Manager.name(ctx.stack_id)]]}
      }

      subsystem =
        start_supervised!(%{
          id: :subsystem,
          type: :supervisor,
          start:
            {Supervisor, :start_link,
             [
               [manager],
               [
                 strategy: :one_for_all,
                 name: Electric.Connection.Manager.Supervisor.name(stack_id: ctx.stack_id)
               ]
             ]}
        })

      Repatch.patch(
        Electric.Connection.Restarter,
        :restart_connection_subsystem,
        [mode: :shared],
        fn _stack_id, manager ->
          send(test_pid, {:restart_requested, manager})
          :ok
        end
      )

      %{subsystem: subsystem}
    end

    test "checks again when the connection manager changes during a check finding none", ctx do
      # Arrange: the manager the first check began with stops while it runs, and its supervisor,
      # still running, starts another, so the answer may have come from either one's admin pool.
      patch_publication_checks(ctx, fn
        1 -> {:replace_manager, :not_found}
        _ -> {:keep_manager, :not_found}
      end)

      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      # Act
      assert_raise Electric.DbConfigurationError, fn ->
        PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
      end

      # Assert
      assert_receive {:checked, 1, later}
      assert_receive {:checked, 2, ^later}
      assert_receive {:restart_requested, ^later}
      refute_received {:restart_requested, _}
    end

    test "goes by the second check when the connection manager changes during the first", ctx do
      # Arrange: the publication is in place by the second check.
      patch_publication_checks(ctx, fn
        1 -> {:replace_manager, :not_found}
        _ -> {:keep_manager, :real}
      end)

      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      # Act
      result = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)

      # Assert
      assert :ok == result
      assert_receive {:checked, 2, _}
      refute_received {:restart_requested, _}
    end

    test "takes a publication found in place across a connection manager change", ctx do
      # Arrange
      patch_publication_checks(ctx, fn _ -> {:replace_manager, :real} end)
      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      # Act
      result = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)

      # Assert
      assert :ok == result
      assert_receive {:checked, 1, _}
      refute_received {:checked, 2, _}
      refute_received {:restart_requested, _}
    end

    test "answers with a retryable error when the connection manager changes again", ctx do
      # Arrange: the manager changes during each of the first three checks.
      patch_publication_checks(ctx, fn
        check when check <= 3 -> {:replace_manager, :not_found}
        _ -> {:keep_manager, :not_found}
      end)

      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      # Act
      error =
        assert_raise Electric.SnapshotError, fn ->
          PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
        end

      # Assert: a shape's snapshot keeps the shape for the next request, after one check again,
      # no third, and no restart.
      assert Electric.SnapshotError.retryable?(Electric.SnapshotError.from_error(error))
      assert_receive {:checked, 2, _}
      refute_received {:checked, 3, _}
      refute_received {:restart_requested, _}
    end

    test "answers with a retryable error when no connection manager runs after a change", ctx do
      # Arrange: the manager stops during the first check, and none runs at either end of the
      # second.
      patch_publication_checks(ctx, fn
        1 -> {:stop_manager, :not_found}
        _ -> {:keep_manager, :not_found}
      end)

      shape_1 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      # Act
      error =
        assert_raise Electric.SnapshotError, fn ->
          PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
        end

      # Assert
      assert Electric.SnapshotError.retryable?(Electric.SnapshotError.from_error(error))
      assert_receive {:checked, 2, nil}
      refute_received {:checked, 3, _}
      refute_received {:restart_requested, _}
    end
  end

  # Each publication check runs `step` with its number. It says whether the connection manager is
  # replaced or stopped while the check runs, or kept, and what the check finds: a status, or `:real` for what the
  # database holds. The test hears of each check, with the manager registered after it.
  defp patch_publication_checks(ctx, step) do
    test_pid = self()

    Repatch.patch(
      Electric.Postgres.Configuration,
      :check_publication_status!,
      [mode: :shared],
      fn conn, publication_name ->
        check = Process.get(:publication_checks, 0) + 1
        Process.put(:publication_checks, check)
        {manager_step, status} = step.(check)

        if manager_step in [:replace_manager, :stop_manager],
          do: :ok = Supervisor.terminate_child(ctx.subsystem, :manager)

        if manager_step == :replace_manager,
          do: {:ok, _} = Supervisor.restart_child(ctx.subsystem, :manager)

        manager = GenServer.whereis(Electric.Connection.Manager.name(ctx.stack_id))
        send(test_pid, {:checked, check, manager})

        if status == :real,
          do:
            Repatch.real(
              Electric.Postgres.Configuration.check_publication_status!(conn, publication_name)
            ),
          else: status
      end
    )
  end

  describe "insufficient table privilege" do
    setup ctx do
      relation_not_owned = {"public", "not_owned"}

      Postgrex.query!(ctx.pool, "CREATE TABLE not_owned (id SERIAL PRIMARY KEY);")
      Postgrex.query!(ctx.pool, "ALTER TABLE items OWNER TO unprivileged;")

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} OWNER TO unprivileged;"
      )

      patch_queries_to_unprivileged()

      %{
        relation_not_owned: relation_not_owned,
        relation_not_owned_with_oid:
          {lookup_relation_oid(ctx.pool, relation_not_owned), relation_not_owned}
      }
    end

    test "returns appropriate error when relation not owned", ctx do
      relation_not_owned_with_oid = ctx.relation_not_owned_with_oid
      shape_1 = generate_shape(relation_not_owned_with_oid, @where_clause_1)

      assert_raise Postgrex.Error, ~r/insufficient_privilege/, fn ->
        PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
      end

      assert_receive {:remove_shapes_for_relations, [^relation_not_owned_with_oid]}
      assert [] == fetch_pub_tables(ctx)
    end

    @tag update_debounce_timeout: 10
    test "should only fail relevant tables with insufficient privilege errors", ctx do
      %{relation_not_owned_with_oid: relation_not_owned_with_oid} = ctx
      shape_1 = generate_shape(relation_not_owned_with_oid, @where_clause_1)
      shape_2 = generate_shape(ctx.relation_with_oid, @where_clause_1)

      task =
        Task.async(fn ->
          assert_raise Postgrex.Error, ~r/insufficient_privilege/, fn ->
            PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape_1)
          end
        end)

      # this should succeed, even if the other one fails
      assert :ok = PublicationManager.add_shape(ctx.stack_id, @shape_handle_2, shape_2)

      Task.await(task)

      assert_receive {:remove_shapes_for_relations, [^relation_not_owned_with_oid]}
      assert [ctx.relation] == fetch_pub_tables(ctx)
    end
  end

  describe "a table dropped from the publication out of band" do
    setup ctx do
      test_pid = self()

      # What the shape catalog has yet to write to disk when the table goes back in.
      Repatch.patch(
        Configuration,
        :add_table_to_publication,
        [mode: :shared],
        fn pool, publication, filter ->
          send(test_pid, {:adding, WriteBuffer.pending_operations_count(ctx.stack_id)})
          Repatch.real(Configuration.add_table_to_publication(pool, publication, filter))
        end
      )

      Repatch.patch(
        Configuration,
        :configure_table_for_replication,
        [mode: :shared],
        fn pool, publication, filter ->
          send(test_pid, {:adding, WriteBuffer.pending_operations_count(ctx.stack_id)})
          Repatch.real(Configuration.configure_table_for_replication(pool, publication, filter))
        end
      )

      :ok
    end

    test "removes the shapes on it from before, on disk, then adds it back", ctx do
      shape_handle = add_shape_and_publish(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)

      assert_receive {:removed, [^shape_handle], []}
      assert_receive {:adding, 0}
      assert_pub_tables(ctx, [ctx.relation])
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, shape_handle)
    end

    test "removes the shapes on it from before when its replica identity went back too", ctx do
      shape_handle = add_shape_and_publish(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      sweep(ctx)

      assert_receive {:removed, [^shape_handle], []}
      assert_receive {:adding, 0}
      assert_pub_tables(ctx, [ctx.relation])

      assert %{rows: [["f"]]} =
               Postgrex.query!(
                 ctx.pool,
                 "SELECT relreplident::text FROM pg_class WHERE oid = $1",
                 [elem(ctx.relation_with_oid, 0)]
               )
    end

    # With no shape to remove there is no removal to flush, so the catalog writes of a shape still
    # taking its snapshot stay queued as they would without the addition.
    test "an addition with no shape to remove leaves the catalog's writes queued", ctx do
      add_shape_and_publish(ctx)
      :ok = WriteBuffer.pause_flush(ctx.stack_id)

      relation = {"public", "serial_ids"}
      shape = generate_shape({lookup_relation_oid(ctx.pool, relation), relation}, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
      assert_received {:adding, queued}
      assert queued > 0
    end

    # The write buffer marks a removal, then queues it, and a process stopped between the two
    # leaves the mark with no removal queued.
    test "queues the removal of a shape a stopped process marked and never queued", ctx do
      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      true =
        :ets.insert_new(
          WriteBuffer.shapes_table_name(ctx.stack_id),
          {{:tombstone, shape_handle}, WriteBuffer.op_key()}
        )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)

      assert_receive {:removed, [^shape_handle], []}
      assert_receive {:adding, 0}
      assert_pub_tables(ctx, [ctx.relation])
      refute WriteBuffer.is_tombstoned?(ctx.stack_id, shape_handle)
      refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
    end

    test "a shape waiting for the table retries when the removal is not on disk in time", ctx do
      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      block_catalog_writes(ctx)
      :ok = WriteBuffer.remove_shape(ctx.stack_id, shape_handle)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      error =
        assert_raise Electric.SnapshotError, fn ->
          PublicationManager.add_shape(ctx.stack_id, waiting, shape)
        end

      assert Electric.SnapshotError.retryable?(error)
      refute_received {:adding, _}
      assert_pub_tables(ctx, [])
    end

    # Its consumer suspends, and a pass can run before it waits again.
    test "keeps a new shape told to retry through the passes before it waits again", ctx do
      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      unblock = block_catalog_writes(ctx)
      :ok = WriteBuffer.remove_shape(ctx.stack_id, shape_handle)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, new_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert_raise Electric.SnapshotError, fn ->
        PublicationManager.add_shape(ctx.stack_id, new_handle, shape)
      end

      unblock.()
      sweep(ctx)

      assert_receive {:adding, 0}
      refute_received {:removed, [^new_handle], _}
      assert ShapeStatus.has_shape_handle?(ctx.stack_id, new_handle)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, new_handle, shape)
    end

    test "a removal not on disk in time holds back its own table only", ctx do
      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      block_catalog_writes(ctx)
      :ok = WriteBuffer.remove_shape(ctx.stack_id, shape_handle)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}

      relation = {"public", "serial_ids"}
      shape = generate_shape({lookup_relation_oid(ctx.pool, relation), relation}, @where_clause_1)
      {:ok, other_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert :ok = PublicationManager.add_shape(ctx.stack_id, other_handle, shape)
      assert_pub_tables(ctx, [relation])
    end

    # The removal stops the shapes' consumers, which can take longer than a shape waiting for the
    # table waits.
    test "a removal still running holds back the table, and the pass ends in time", ctx do
      test_pid = self()

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
          removal = spawn(fn -> receive(do: (:stopped -> :ok)) end)
          send(test_pid, {:removing, handles, removal})
          {:ok, completed_removal(removal)}
        end
      )

      shape_handle = add_shape_and_publish(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removing, [^shape_handle], removal}

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      error =
        assert_raise Electric.SnapshotError, fn ->
          PublicationManager.add_shape(ctx.stack_id, waiting, shape)
        end

      assert Electric.SnapshotError.retryable?(error)
      assert Process.alive?(removal)
      refute_received {:adding, _}
      assert_pub_tables(ctx, [])

      send(removal, :stopped)

      assert :ok = add_shape_once_admitted(ctx, waiting, shape)
      assert_received {:adding, 0}
      assert_pub_tables(ctx, [ctx.relation])
    end

    test "a removal still running holds back its own table only", ctx do
      test_pid = self()
      shape_handle = add_shape_and_publish(ctx)

      relation = {"public", "serial_ids"}
      other = {lookup_relation_oid(ctx.pool, relation), relation}
      shape = generate_shape(other, @where_clause_1)
      {:ok, other_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, other_handle, shape)
      assert_received {:adding, _}

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)

          if shape_handle in handles do
            removal = spawn(fn -> receive(do: (:stopped -> :ok)) end)
            send(test_pid, {:removing_items, removal})
            {:ok, completed_removal(removal)}
          else
            {:ok, completed_removal(ended_process())}
          end
        end
      )

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items, serial_ids",
        []
      )

      PublicationManager.Configurator.configure_publication(
        ctx.stack_id,
        {System.unique_integer([:positive, :monotonic]),
         MapSet.new([ctx.relation_with_oid, other])}
      )

      assert_receive {:removing_items, removal}
      assert_receive {:adding, _}, 5_000
      assert_pub_tables(ctx, [relation])
      assert Process.alive?(removal)

      send(removal, :stopped)
    end

    test "a table another session puts back waits for the removal still running", ctx do
      removal = drop_table_with_removal_running(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      assert_retries_until_removed(ctx, removal)
      refute_received {:adding, _}
    end

    test "a table another session puts back with its replica identity reset waits for the removal",
         ctx do
      removal = drop_table_with_removal_running(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])

      # The pass that held the table has ended, with whatever it ran after the reply.
      assert_retries_until_removed(ctx, removal, fn ->
        _ = :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))
        assert replica_identity(ctx) == "d"
      end)

      assert replica_identity(ctx) == "f"
    end

    # A removal task that stops early leaves the shape whole: its consumer, its storage and its
    # place in the log collector, as well as its row.
    test "starts again a removal that ended before it reached the shape", ctx do
      test_pid = self()
      calls = :counters.new(1, [])

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          :counters.add(calls, 1, 1)

          if :counters.get(calls, 1) > 1,
            do: for(handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle))

          send(test_pid, {:removing, handles})
          {:ok, completed_removal(ended_process())}
        end
      )

      shape_handle = add_shape_and_publish(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)

      assert_receive {:removing, [^shape_handle]}
      assert_receive {:removing, [^shape_handle]}
      assert_receive {:adding, 0}
      refute ShapeStatus.has_shape_handle?(ctx.stack_id, shape_handle)
      refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
    end

    test "queues again, within its deadline, the removal of a shape the write buffer gave up on",
         ctx do
      test_pid = self()
      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      dropped = :counters.new(1, [])

      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        if :counters.get(dropped, 1) == 1, do: send(test_pid, {:catalog_read, timeout})
        Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
      end)

      # As the buffer drops a write that keeps failing, the first removal goes with its mark.
      Repatch.patch(WriteBuffer, :remove_shape, [mode: :shared], fn stack_id, handle ->
        :ok = Repatch.real(WriteBuffer.remove_shape(stack_id, handle))

        if :counters.get(dropped, 1) == 0 do
          :counters.add(dropped, 1, 1)
          shapes_table = WriteBuffer.shapes_table_name(stack_id)

          :ets.match_delete(
            WriteBuffer.operations_table_name(stack_id),
            {:_, {:remove, handle}, :_, :_}
          )

          :ets.delete(shapes_table, {:tombstone, handle})
          :ets.update_counter(shapes_table, :count_remove, {2, -1, 0, 0})
        end

        :ok
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)

      assert_receive {:removed, [^shape_handle], []}
      assert_receive {:adding, 0}
      {:messages, messages} = Process.info(self(), :messages)
      reads = for {:catalog_read, timeout} <- messages, do: timeout
      assert reads != []
      assert Enum.all?(reads, &(&1 <= 3_000))
      assert_pub_tables(ctx, [ctx.relation])
      assert :counters.get(dropped, 1) == 1
      refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
    end

    @tag :catalog_insert_order
    test "keeps a removal owed when its buffered insert can commit after a SQLite read", ctx do
      parent = self()
      racing = :atomics.new(1, [])
      shape_handle = add_shape_and_publish(ctx)
      {:ok, shape} = ShapeStatus.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      Repatch.patch(WriteBuffer, :flush_sync, [mode: :shared], fn stack_id, timeout ->
        if :atomics.get(racing, 1) == 1,
          do: {:error, :lock_contention},
          else: Repatch.real(WriteBuffer.flush_sync(stack_id, timeout))
      end)

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, [^shape_handle], _reason ->
          :ok = ShapeStatus.remove_shape(stack_id, shape_handle)
          :ok = WriteBuffer.flush_sync(stack_id)
          {:ok, _} = ShapeDb.add_shape(stack_id, shape, shape_handle)
          :atomics.put(racing, 1, 1)
          {:ok, completed_removal(ended_process())}
        end
      )

      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        found? = Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))

        if label == :handle_exists? and found? == false and :atomics.get(racing, 1) == 1 do
          :ok = Repatch.real(WriteBuffer.flush_sync(stack_id, 5_000))
          send(parent, :insert_committed_after_absent_read)
        end

        found?
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      state = sweep_and_wait(ctx)

      assert Map.has_key?(state.removals_to_confirm, shape_handle),
             "the table was admitted after a false absence; " <>
               "the insert committed: #{inspect(received_insert_commit?())}"

      assert_pub_tables(ctx, [])
    end

    # The configurator restarts before the removal has reached its shape: the tracker still holds
    # it, and the replacement starts it again.
    test "a restart before the removal reaches its shape starts it again", ctx do
      test_pid = self()
      calls = :counters.new(1, [])

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          :counters.add(calls, 1, 1)

          if :counters.get(calls, 1) == 1 do
            removal = spawn(fn -> receive(do: (:stopped -> :ok)) end)
            send(test_pid, {:removing, handles, removal})
            {:ok, completed_removal(removal)}
          else
            for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
            send(test_pid, {:removing_again, handles})
            {:ok, completed_removal(ended_process())}
          end
        end
      )

      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removing, [^shape_handle], removal}
      _ = :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      restart_configurator(ctx)

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, waiting, shape)

      assert_received {:removing_again, [^shape_handle]}
      refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
      send(removal, :stopped)
    end

    # The configurator restarts with the removal queued and not on disk, after another session has
    # put the table back: the tracker still holds it, and the replacement holds the table.
    test "a table put back is held across a restart until its removals are on disk", ctx do
      shape_handle = add_shape_and_publish(ctx)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      release = block_catalog_writes(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      _ = :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      restart_configurator(ctx)

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      error =
        assert_raise Electric.SnapshotError, fn ->
          PublicationManager.add_shape(ctx.stack_id, waiting, shape)
        end

      assert Electric.SnapshotError.retryable?(error)
      assert WriteBuffer.is_tombstoned?(ctx.stack_id, shape_handle)

      release.()
      assert :ok = add_shape_once_admitted(ctx, waiting, shape)
      refute WriteBuffer.is_tombstoned?(ctx.stack_id, shape_handle)
      refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
    end

    # A table found out of the publication in manual mode is taken out of the ready set too. The
    # manual publishing role cannot drain the writers on it: the operator does, as they add it.
    @tag manual_table_publishing?: true
    test "in manual mode, a table the operator puts back waits for the removal still running",
         ctx do
      test_pid = self()
      relation_with_oid = ctx.relation_with_oid

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn _pool, _publication, {_oid, {schema, table}}, _expected ->
          {:error,
           {:lock_denied,
            %Postgrex.Error{
              postgres: %{
                code: :insufficient_privilege,
                pg_code: "42501",
                severity: "ERROR",
                message: "permission denied for table #{schema}.#{table}"
              }
            }}}
        end
      )

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
          removal = spawn(fn -> receive(do: (:stopped -> :ok)) end)
          send(test_pid, {:removing, handles, removal})
          {:ok, completed_removal(removal)}
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      shape = generate_shape(relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removing, [^shape_handle], removal}
      assert_receive {:remove_shapes_for_relations, [^relation_with_oid]}

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      assert_retries_until_removed(ctx, removal)
    end

    # Only a denial of the drain's `LOCK` says that this role cannot drain the table. A privilege
    # error met elsewhere in the drain holds the table, as any error does, rather than serve it.
    @tag manual_table_publishing?: true
    test "in manual mode, holds a table whose drain fails on a privilege other than the lock",
         ctx do
      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn _pool, _publication, _filter, _expected ->
          {:error,
           %Postgrex.Error{
             postgres: %{
               code: :insufficient_privilege,
               pg_code: "42501",
               severity: "ERROR",
               message: "permission denied"
             }
           }}
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, shape_handle, shape))
    end

    # The operator puts the table back after the removal's time has run out, so the pass that can
    # admit it has the least time, which a flush of the catalog writes can take all of.
    for {case_name, buffered?} <- [
          "still in the write buffer": true,
          "already on disk": false
        ] do
      @tag manual_table_publishing?: true
      @tag :expired_removal
      test "in manual mode, admits a table put back after its removal ran out of time, #{case_name}",
           ctx do
        shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
        release = block_catalog_writes(ctx)

        Postgrex.query!(
          ctx.pool,
          "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items",
          []
        )

        sweep(ctx)
        assert_receive {:removed, [^shape_handle], []}
        expire_removal(ctx, shape_handle)

        unless unquote(buffered?) do
          release.()
          :ok = WriteBuffer.flush_sync(ctx.stack_id)
        end

        test_pid = self()

        # Longer than the 50 ms a pass gives a removal whose time has run out.
        Repatch.patch(WriteBuffer, :flush_sync, [mode: :shared, force: true], fn stack_id, _ ->
          send(test_pid, :catalog_flushed)
          Process.sleep(200)
          Repatch.real(WriteBuffer.flush_sync(stack_id, 5_000))
        end)

        # The catalog's own lookup would take a buffered removal for done: read the file.
        assert shape_on_disk?(ctx.stack_id, shape_handle) == unquote(buffered?)
        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
        {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

        assert :ok = PublicationManager.add_shape(ctx.stack_id, waiting, shape)
        refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
        refute shape_on_disk?(ctx.stack_id, shape_handle)

        # A removal already on disk needs no flush: one would take the time of the read.
        unless unquote(buffered?), do: refute_received(:catalog_flushed)
      end
    end

    # No addition waits on a table the operator has left out, and the record of the removal of the
    # shapes on it goes once that removal is on disk, so that a restart does not start it again.
    @tag manual_table_publishing?: true
    @tag :unrelated_removal
    test "in manual mode, confirms the removal of the shapes on a table left out once on disk",
         ctx do
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      release = block_catalog_writes(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      _ = :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))
      assert Map.has_key?(removals_owed(ctx), shape_handle)
      assert MapSet.member?(persisted_removals_owed(ctx), shape_handle)

      release.()
      sweep(ctx)

      # The record a restart reads, not only the tracker's state.
      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      refute Map.has_key?(removals_owed(ctx), shape_handle)
      refute ShapeDb.handle_exists?(ctx.stack_id, shape_handle)
    end

    # The checkout's timeout bounds only the wait for a connection: a read that stalls once it has
    # one must not hold the configurator, which admits other tables meanwhile.
    @tag manual_table_publishing?: true
    @tag :stalled_unrelated_read
    test "in manual mode, admits another table while the catalog read of a removal stalls", ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      stalling = :atomics.new(1, [])

      # The removal reads the catalog too: the reads stall once it has reached its shape.
      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
          :ok = :atomics.put(stalling, 1, 1)
          send(test_pid, {:removed, handles, fetch_pub_tables(ctx)})
          {:ok, completed_removal(ended_process())}
        end
      )

      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        query =
          if label == :handle_exists? and :atomics.get(stalling, 1) == 1 do
            fn conn ->
              send(test_pid, {:catalog_read_stalled, self()})
              receive(do: (:resume -> query.(conn)))
            end
          else
            query
          end

        Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      assert_receive {:catalog_read_stalled, reader}

      Postgrex.query!(ctx.pool, "CREATE TABLE other_table (id UUID PRIMARY KEY)", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE other_table REPLICA IDENTITY FULL", [])

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE other_table",
        []
      )

      other = {"public", "other_table"}
      {added, _} = add_shape_async(ctx, {lookup_relation_oid(ctx.pool, other), other}, nil)
      assert {:ok, :ok} = Task.yield(added, 2_000)

      :ok = :atomics.put(stalling, 1, 0)
      {:messages, messages} = Process.info(self(), :messages)
      readers = [reader | for({:catalog_read_stalled, pid} <- messages, do: pid)]
      monitors = for pid <- readers, do: {pid, Process.monitor(pid)}
      for pid <- readers, do: send(pid, :resume)

      # The pass the sweep starts finds no read running.
      for {pid, monitor} <- monitors, do: assert_receive({:DOWN, ^monitor, :process, ^pid, _})

      sweep(ctx)

      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      refute shape_on_disk?(ctx.stack_id, shape_handle)
    end

    # A read left running keeps its catalog connection: a pass that started another each time
    # would take the whole read pool, and the lookups every other request needs with it.
    @tag manual_table_publishing?: true
    @tag :stalled_read_not_repeated
    test "in manual mode, starts no other catalog read while one stalls", ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      stalling = :atomics.new(1, [])

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn stack_id, handles, _reason ->
          for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
          :ok = :atomics.put(stalling, 1, 1)
          send(test_pid, {:removed, handles, fetch_pub_tables(ctx)})
          {:ok, completed_removal(ended_process())}
        end
      )

      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        # A read is counted as it asks for a connection: with one read worker, a second read
        # waits for it behind the stalled one.
        query =
          if label == :handle_exists? and :atomics.get(stalling, 1) == 1 do
            send(test_pid, {:catalog_read_started, self()})

            fn conn ->
              send(test_pid, {:catalog_read_stalled, self()})
              receive(do: (:resume -> query.(conn)))
            end
          else
            query
          end

        Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      assert_receive {:catalog_read_stalled, reader}
      assert_received {:catalog_read_started, ^reader}

      for _ <- 1..3, do: sweep_and_wait(ctx)
      refute_receive {:catalog_read_started, _}, 200
      assert Map.has_key?(removals_owed(ctx), shape_handle)

      :ok = :atomics.put(stalling, 1, 0)
      monitor = Process.monitor(reader)
      send(reader, :resume)
      assert_receive {:DOWN, ^monitor, :process, ^reader, _}
      sweep(ctx)

      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      refute shape_on_disk?(ctx.stack_id, shape_handle)
    end

    @tag manual_table_publishing?: true
    @tag :killed_catalog_read
    test "in manual mode, outlives a catalog read killed from outside", ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      killing = :atomics.new(1, [])
      configurator = GenServer.whereis(PublicationManager.Configurator.name(ctx.stack_id))
      monitor = Process.monitor(configurator)

      # The first read once the table waits on the removal is killed as it reads, while the pass
      # waits on it: a read linked to the configurator would take it down then.
      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        query =
          if label == :handle_exists? and :atomics.compare_exchange(killing, 1, 1, 0) == :ok do
            fn _conn ->
              waiting? = wait_until(fn -> waiting_on_catalog_read?(configurator) end)
              send(test_pid, {:catalog_read_killed, self(), waiting?})
              Process.exit(self(), :kill)
            end
          else
            query
          end

        Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
      end)

      # The removal is still in the write buffer as the table goes out, and on disk within its time
      # as the table comes back: the pass that admits it waits on the read for that time, where it
      # gives a removal no table waits on only 50 ms.
      release = block_catalog_writes(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      _ = :sys.get_state(configurator)
      release.()
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = :atomics.put(killing, 1, 1)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      # The pass reads the catalog again once the killed read is down, within the same wait.
      assert :ok = Task.await(admission_waiter(ctx, waiting, shape))
      assert_received {:catalog_read_killed, _reader, true}

      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      refute shape_on_disk?(ctx.stack_id, shape_handle)
      assert GenServer.whereis(PublicationManager.Configurator.name(ctx.stack_id)) == configurator
      refute_received {:DOWN, ^monitor, :process, _, _}
    end

    # A read that outlasts the time its pass gives it still answers: a store whose every read takes
    # longer than that would otherwise hold the table for good once the removal is on disk.
    @tag manual_table_publishing?: true
    @tag :late_catalog_read
    test "in manual mode, admits a table on the answer of a catalog read its pass gave up on",
         ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      release = block_catalog_writes(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      expire_removal(ctx, shape_handle)
      release.()
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      refute shape_on_disk?(ctx.stack_id, shape_handle)

      # Every read of the catalog now takes longer than the 50 ms a pass gives a removal whose
      # time has run out.
      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        query =
          if label == :handle_exists? do
            fn conn ->
              send(test_pid, {:catalog_read, self()})
              Process.sleep(150)
              query.(conn)
            end
          else
            query
          end

        Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, waiting, shape))

      assert_received {:catalog_read, _reader}

      # No other request comes in: the answer starts the pass that takes it.
      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      assert :ok = Task.await(admission_waiter(ctx, waiting, shape))
    end

    # A read that answers while a later pass runs has ended by the time that pass looks at its
    # removal, and its reply waits for the pass to end. The pass starts no read in its place, which
    # would leave the reply unmatched, and the reply, once taken in, admits the table.
    @tag manual_table_publishing?: true
    @tag :late_catalog_read
    test "in manual mode, takes the answer of a catalog read that ended during a later pass",
         ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      release = block_catalog_writes(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}
      expire_removal(ctx, shape_handle)
      release.()
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      refute shape_on_disk?(ctx.stack_id, shape_handle)

      # Every read of the catalog waits until the test lets it answer.
      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        query =
          if label == :handle_exists? do
            fn conn ->
              send(test_pid, {:catalog_read, self()})
              receive(do: (:answer -> query.(conn)))
            end
          else
            query
          end

        Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, waiting, shape))

      assert_receive {:catalog_read, reader}

      # The next pass, in its look at the publication, waits for the read to answer and end.
      armed = :atomics.new(1, [])

      Repatch.patch(Configuration, :check_publication_status!, [mode: :shared], fn pool, name ->
        if :atomics.compare_exchange(armed, 1, 1, 0) == :ok do
          send(test_pid, {:in_check, self()})
          receive(do: (:checked -> :ok))
        end

        Repatch.real(Configuration.check_publication_status!(pool, name))
      end)

      :atomics.put(armed, 1, 1)
      waiter = admission_waiter(ctx, waiting, shape)
      assert_receive {:in_check, configurator}
      monitor = Process.monitor(reader)
      send(reader, :answer)
      assert_receive {:DOWN, ^monitor, :process, ^reader, _reason}
      send(configurator, :checked)
      Task.await(waiter)

      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      await_table_committed(ctx)
      assert :ok = Task.await(admission_waiter(ctx, waiting, shape))
      refute_received {:catalog_read, _reader}
    end

    # A read killed while a later pass runs has its exit waiting for that pass to end, which takes
    # the read for running and starts none: a pass soon after reads again.
    @tag manual_table_publishing?: true
    @tag :late_catalog_read
    test "in manual mode, reads again soon after a catalog read is killed during a later pass",
         ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      left_running = leave_catalog_read_running(ctx, shape_handle)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, waiting, shape))

      reader = left_running.()
      armed = :atomics.new(1, [])

      Repatch.patch(Configuration, :check_publication_status!, [mode: :shared], fn pool, name ->
        if :atomics.compare_exchange(armed, 1, 1, 0) == :ok do
          send(test_pid, {:in_check, self()})
          receive(do: (:checked -> :ok))
        end

        Repatch.real(Configuration.check_publication_status!(pool, name))
      end)

      :atomics.put(armed, 1, 1)
      waiter = admission_waiter(ctx, waiting, shape)
      assert_receive {:in_check, configurator}
      Process.exit(reader, :kill)

      assert wait_until(fn ->
               {:messages, messages} = Process.info(configurator, :messages)
               Enum.any?(messages, &match?({:DOWN, _, :process, ^reader, :killed}, &1))
             end)

      send(configurator, :checked)
      assert_receive {:catalog_read, new_reader}
      Task.await(waiter)
      send(new_reader, :answer)

      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
      await_table_committed(ctx)
      assert :ok = Task.await(admission_waiter(ctx, waiting, shape))
    end

    # A read killed while no pass runs leaves its removal to a pass soon after, which reads again:
    # no shape waits on the table, so no request would start one.
    @tag manual_table_publishing?: true
    @tag :late_catalog_read
    test "in manual mode, reads again soon after a catalog read is killed between passes", ctx do
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      left_running = leave_catalog_read_running(ctx, shape_handle)
      sweep_and_wait(ctx)
      reader = left_running.()
      monitor = Process.monitor(reader)
      Process.exit(reader, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^reader, :killed}

      assert_receive {:catalog_read, new_reader}
      send(new_reader, :answer)
      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
    end

    # A read keeps its catalog connection past the configurator that started it: the one that takes
    # over starts no read while that one runs.
    @tag manual_table_publishing?: true
    @tag :stalled_read_not_repeated
    test "in manual mode, starts no catalog read while one a configurator before it left runs",
         ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      left_running = leave_catalog_read_running(ctx, shape_handle)
      sweep_and_wait(ctx)
      reader = left_running.()

      # The configurator that takes over starts the removal again. The shape is gone from the
      # catalog, and the read holds the catalog's only read connection, so this removal is over
      # without a look at the catalog.
      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn _stack_id, handles, _reason ->
          send(test_pid, {:removed, handles, []})
          {:ok, completed_removal(ended_process())}
        end
      )

      restart_configurator(ctx, &Process.exit(&1, :kill))

      for _ <- 1..3, do: sweep_and_wait(ctx)
      refute_receive {:catalog_read_started, _}, 200
      assert MapSet.member?(persisted_removals_owed(ctx), shape_handle)

      # No pass comes in manual mode: the end of the read is what starts the next.
      send(reader, :answer)
      assert_receive {:catalog_read, new_reader}
      send(new_reader, :answer)
      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
    end

    # The pass that finds the read in the slot queues the removal behind it, and the read ends
    # before the pass watches the slot. Looked up again, the slot was empty and nothing was watched.
    @tag manual_table_publishing?: true
    @tag :stalled_read_not_repeated
    test "in manual mode, a read that leaves the slot during the pass still wakes it", ctx do
      test_pid = self()
      shape_handle = add_shape_in_manual_mode(ctx, @where_clause_1)
      left_running = leave_catalog_read_running(ctx, shape_handle)
      sweep_and_wait(ctx)
      reader = left_running.()

      Repatch.patch(
        Electric.ShapeCache.ShapeCleaner,
        :start_removing_shapes,
        [mode: :shared, force: true],
        fn _stack_id, handles, _reason ->
          send(test_pid, {:removed, handles, []})
          {:ok, completed_removal(ended_process())}
        end
      )

      # The removal's look at the slot ends with this call, which the read's end then follows.
      Repatch.patch(
        PublicationManager.RelationTracker,
        :removals_confirmed,
        [mode: :shared],
        fn handles, opts ->
          if Process.alive?(reader) do
            monitor = Process.monitor(reader)
            send(reader, :answer)
            assert_receive {:DOWN, ^monitor, :process, ^reader, _}
          end

          Repatch.real(PublicationManager.RelationTracker.removals_confirmed(handles, opts))
        end
      )

      # The pass the restart starts is the one: a later pass would find the slot empty and read.
      restart_configurator(ctx, &Process.exit(&1, :kill))
      assert wait_until(fn -> not Process.alive?(reader) end)

      assert_receive {:catalog_read, new_reader}
      send(new_reader, :answer)
      assert wait_until(fn -> not MapSet.member?(persisted_removals_owed(ctx), shape_handle) end)
    end

    # A read that answered in a pass holds the slot until it has exited, and the next removal of the
    # same pass can find it there. That removal waits for the holder's end, which wakes the pass that
    # looks at it again. Here the first read hands the slot, as it answers, to a process that stands
    # for it until the test releases it.
    @tag manual_table_publishing?: true
    @tag :stalled_read_not_repeated
    test "in manual mode, a removal that finds the slot held by a read of its own pass still wakes it",
         ctx do
      test_pid = self()
      registry = Electric.ProcessRegistry.registry_name(ctx.stack_id)
      slot = {PublicationManager.Configurator.CatalogRead, nil}
      handle_1 = add_shape_in_manual_mode(ctx, @where_clause_1)
      shape_2 = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, handle_2} = ShapeStatus.add_shape(ctx.stack_id, shape_2)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, handle_2, shape_2)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      handles = MapSet.new([handle_1, handle_2])

      release = block_catalog_writes(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, removed, []}
      assert MapSet.new(removed) == handles
      expire_removal(ctx, handle_1)
      expire_removal(ctx, handle_2)
      release.()
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      refute shape_on_disk?(ctx.stack_id, handle_1)
      refute shape_on_disk?(ctx.stack_id, handle_2)

      holder =
        spawn(fn ->
          receive do
            {:take_slot, read} ->
              {:ok, _owner} = Registry.register(registry, slot, nil)
              send(read, :slot_taken)
          end

          receive(do: (:release -> :ok))
        end)

      reads = :atomics.new(1, [])

      Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                        label,
                                                                        query,
                                                                        timeout ->
        read = if label == :handle_exists?, do: :atomics.add_get(reads, 1, 1)
        if read, do: send(test_pid, {:catalog_read, read})

        if read == 1 do
          :ok = Registry.unregister(registry, slot)
          send(holder, {:take_slot, self()})
          receive(do: (:slot_taken -> :ok))
          # The first read answers as it hands the slot over, inside its pass's wait: an answer
          # after that wait would start a pass of its own, which finds the holder at its start.
          false
        else
          Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
        end
      end)

      revision = System.unique_integer([:positive, :monotonic])
      configure_revision(ctx, revision)
      assert_receive {:catalog_read, 1}

      assert wait_until(fn ->
               MapSet.size(MapSet.intersection(persisted_removals_owed(ctx), handles)) == 1
             end)

      # The pass that read for the first removal, and no pass after it, found the slot taken when
      # it came to the second, and waits for the read that holds it, not for a pass to come.
      assert %{
               checked_filter_revision: ^revision,
               removals_to_confirm: removals,
               slot_watch: watch
             } =
               :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))

      assert [{:behind, ^holder}] =
               for({handle, %{read: read}} <- removals, handle in handles, do: read)

      assert is_reference(watch)
      refute_receive {:catalog_read, 2}, 200

      # No pass comes in manual mode: the end of the holder is what starts the next.
      send(holder, :release)
      assert_receive {:catalog_read, 2}
      assert wait_until(fn -> MapSet.disjoint?(persisted_removals_owed(ctx), handles) end)
    end
  end

  describe "a table another session added while Electric was adding it" do
    # The other session's addition commits, and Electric's drain of the writers it left in flight
    # times out.
    setup ctx do
      Repatch.patch(
        Configuration,
        :add_table_to_publication,
        [mode: :shared],
        fn _conn, publication, _filter ->
          Postgrex.query!(ctx.db_conn, "ALTER PUBLICATION #{publication} ADD TABLE items", [])
          {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
        end
      )

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      :ok
    end

    @tag :owned_add_lock_timeout
    @tag connection_opt_overrides: [pool_size: 3]
    test "drains those writers before it serves the table", ctx do
      writer = start_writer(ctx)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, shape_handle, shape))

      assert_pub_tables(ctx, [ctx.relation])

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      adder =
        Task.async(fn -> PublicationManager.add_shape(ctx.stack_id, shape_handle, shape) end)

      assert_drain_waiting(ctx, writer, adder.pid, shape_handle)

      finish_writer(writer)
      assert :ok = Task.await(adder)
    end

    @tag :owned_add_lock_timeout
    @tag connection_opt_overrides: [pool_size: 3]
    test "drains those writers after a restart of the configurator", ctx do
      writer = start_writer(ctx)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, shape_handle, shape))

      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      old_pid = GenServer.whereis(configurator)
      ref = Process.monitor(old_pid)
      GenServer.stop(configurator)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, _}
      Repatch.allow(self(), wait_for_restart(configurator, old_pid))

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      adder =
        Task.async(fn -> PublicationManager.add_shape(ctx.stack_id, shape_handle, shape) end)

      assert_drain_waiting(ctx, writer, adder.pid, shape_handle)

      finish_writer(writer)
      assert :ok = Task.await(adder)
    end

    # A replica identity change takes a lock that drains them too.
    @tag :owned_add_lock_timeout
    @tag connection_opt_overrides: [pool_size: 2]
    test "owes no drain once a replica identity change has served the table", ctx do
      test_pid = self()

      Repatch.patch(
        Configuration,
        :configure_table_for_replication,
        [mode: :shared],
        fn _conn, publication, _filter ->
          Postgrex.query!(ctx.db_conn, "ALTER PUBLICATION #{publication} ADD TABLE items", [])
          {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
        end
      )

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          send(test_pid, :draining)

          Repatch.real(
            Configuration.wait_for_in_flight_writers_on_published_table(
              pool,
              publication,
              filter,
              expected
            )
          )
        end
      )

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, shape_handle, shape))

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
      refute_received :draining

      assert sweep_and_wait(ctx).drains == %{}
      refute_received :draining
    end

    # A drain verifies its table against the status its pass's check read. An update of another
    # table that finds the publication changed under it knows what a pass finding it changed knows:
    # the answer of a drain that verified the old status, queued as the update verifies, admits no
    # shape, and the check after the change answers the shape. An update Postgres refuses for a
    # publication it does not find knows it too, and so does one that finds only its own table
    # changed, by a migration that also drops the drained table and adds it back.
    for {found, change, error_type} <- [
          {"the publication changed", "SET (publish = 'insert, update, truncate')",
           :publication_missing_operations},
          {"the publication dropped", :drop, :publication_missing},
          {"its own table changed", :readd, nil}
        ] do
      @tag :owned_add_lock_timeout
      @tag change: change, error_type: error_type
      @tag connection_opt_overrides: [pool_size: 4]
      test "discards a completed drain queued before another table's update finds #{found}",
           ctx do
        parent = self()
        configurator = PublicationManager.Configurator.name(ctx.stack_id)
        drains = :atomics.new(1, [])

        # The connection manager whose admin pool answers the check that reads the change.
        start_supervised!(%{
          id: :manager,
          start:
            {Agent, :start_link,
             [fn -> nil end, [name: Electric.Connection.Manager.name(ctx.stack_id)]]}
        })

        Repatch.patch(
          Electric.Connection.Restarter,
          :restart_connection_subsystem,
          [mode: :shared],
          fn _, _ ->
            send(parent, :connection_subsystem_restarted)
            :ok
          end
        )

        Repatch.patch(
          Configuration,
          :wait_for_in_flight_writers_on_published_table,
          [mode: :shared],
          fn pool, publication, filter, expected ->
            first? = :atomics.add_get(drains, 1, 1) == 1

            result =
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected,
                :infinity
              )

            if first? do
              send(parent, {:drained, self(), result})
              assert_receive :report_drain
            end

            result
          end
        )

        shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
        {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

        assert {:error, %SnapshotError{type: :publication_admission_pending}} =
                 Task.await(admission_waiter(ctx, shape_handle, shape))

        {adder, _handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_2)
        assert_receive {:drained, drain, {:ok, membership}}
        %{drains: pending} = :sys.get_state(configurator)
        [{drain_ref, _}] = Map.to_list(pending)

        # The next pass adds another table: the drain reports, and the publication stops publishing
        # deletes, or is dropped, before that addition verifies it.
        Repatch.restore(Configuration, :add_table_to_publication, 3, mode: :shared)

        Repatch.patch(
          Configuration,
          :add_table_to_publication,
          [mode: :shared],
          fn conn, publication, filter ->
            send(drain, :report_drain)

            assert wait_until(fn ->
                     {:messages, messages} = Process.info(self(), :messages)
                     {drain_ref, {:ok, membership}} in messages
                   end)

            case ctx.change do
              :drop ->
                Postgrex.query!(ctx.db_conn, "DROP PUBLICATION #{publication}", [])

              :readd ->
                for sql <- [
                      "ALTER PUBLICATION #{publication} DROP TABLE items",
                      "ALTER PUBLICATION #{publication} ADD TABLE items",
                      "ALTER TABLE serial_ids REPLICA IDENTITY DEFAULT"
                    ],
                    do: Postgrex.query!(ctx.db_conn, sql, [])

              sql ->
                Postgrex.query!(ctx.db_conn, "ALTER PUBLICATION #{publication} #{sql}", [])
            end

            send(parent, :changed)
            Repatch.real(Configuration.add_table_to_publication(conn, publication, filter))
          end
        )

        other = {"public", "serial_ids"}
        Postgrex.query!(ctx.pool, "ALTER TABLE serial_ids REPLICA IDENTITY FULL", [])
        add_shape_async(ctx, {lookup_relation_oid(ctx.pool, other), other}, @where_clause_1)

        if ctx.change == :readd do
          # The configurator takes the drain's queued answer, unless it cancelled the drain, before
          # it answers this call.
          assert_receive :changed
          :sys.get_state(configurator)
          tracker = :sys.get_state(PublicationManager.RelationTracker.name(ctx.stack_id))

          refute match?(
                   {_, ^membership},
                   tracker.served_relations[elem(ctx.relation_with_oid, 0)]
                 )
        else
          error_type = ctx.error_type

          assert {:exit, {%Electric.DbConfigurationError{type: ^error_type}, _}} =
                   Task.yield(adder, 10_000)

          assert_receive :connection_subsystem_restarted
        end
      end
    end

    # An update that finds the publication replaced under it ends its pass. A drop after it, which
    # verifies nothing, would alter the replacement, which the pass never read: the drop waits for
    # the pass after the check that reads the replacement.
    @tag connection_opt_overrides: [pool_size: 4]
    test "runs no later action of a pass once an update finds the publication replaced", ctx do
      parent = self()
      replaced = :atomics.new(1, [])
      checked = :atomics.new(1, [])
      other = {"public", "serial_ids"}
      other = {lookup_relation_oid(ctx.pool, other), other}

      # The configurator's first pass, for no shape, is over before the table it drops is added.
      :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))
      Postgrex.query!(ctx.pool, "CREATE TABLE left_behind (id UUID PRIMARY KEY)", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE left_behind REPLICA IDENTITY FULL", [])
      Postgrex.query!(ctx.pool, "ALTER TABLE serial_ids REPLICA IDENTITY FULL", [])

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE left_behind",
        []
      )

      left = {"public", "left_behind"}
      left = {lookup_relation_oid(ctx.pool, left), left}
      # The pass adds the table a shape asks for before it drops the one none asks for.
      assert other < left

      # The addition finds the publication dropped and created again, with the table the pass
      # drops, under the same owner: it alters the replacement, and its check of the membership
      # finds another publication.
      Repatch.restore(Configuration, :add_table_to_publication, 3, mode: :shared)

      Repatch.patch(
        Configuration,
        :add_table_to_publication,
        [mode: :shared],
        fn conn, publication, filter ->
          if :atomics.compare_exchange(replaced, 1, 0, 1) == :ok do
            Postgrex.query!(ctx.db_conn, "DROP PUBLICATION #{publication}", [])

            Postgrex.query!(
              ctx.db_conn,
              "CREATE PUBLICATION #{publication} FOR TABLE left_behind",
              []
            )
          end

          Repatch.real(Configuration.add_table_to_publication(conn, publication, filter))
        end
      )

      Repatch.patch(
        Configuration,
        :check_publication_status!,
        [mode: :shared],
        fn pool, publication ->
          if :atomics.get(replaced, 1) == 1, do: :atomics.put(checked, 1, 1)
          Repatch.real(Configuration.check_publication_status!(pool, publication))
        end
      )

      Repatch.patch(
        Configuration,
        :drop_table_from_publication,
        [mode: :shared],
        fn conn, publication, filter ->
          send(parent, {:dropping, filter, :atomics.get(checked, 1) == 1})
          Repatch.real(Configuration.drop_table_from_publication(conn, publication, filter))
        end
      )

      shape = generate_shape(other, nil)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      adder = admission_waiter(ctx, handle, shape)
      assert_receive {:dropping, ^left, read_replacement?}, 5_000
      assert read_replacement?

      # The shape hears that its table waits for a fresh pass, which adds it to the replacement.
      assert {:error, %SnapshotError{type: :publication_admission_pending}} = Task.await(adder)
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
    end
  end

  describe "a table another role adds to a publication Electric may not alter" do
    # Electric runs in automatic publishing mode, and another role owns the publication.
    setup ctx do
      owned = :atomics.new(1, [])

      Repatch.patch(
        Configuration,
        :check_publication_status!,
        [mode: :shared],
        fn pool, publication ->
          case Repatch.real(Configuration.check_publication_status!(pool, publication)) do
            %{} = status -> %{status | can_alter_publication?: :atomics.get(owned, 1) == 1}
            not_found -> not_found
          end
        end
      )

      # The configurator's first pass, for no shape, may have checked the publication before the
      # patch, and would then drop a table the test adds.
      :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
      %{owned: owned}
    end

    @tag :waiter_guard
    @tag connection_opt_overrides: [pool_size: 3]
    test "is served once the writers in flight across its addition have finished", ctx do
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      assert_served_after_writer(ctx, writer, @where_clause_1)
    end

    # A migration can add the table and hand the publication to another role in one transaction.
    # Committed after the pass found the publication Electric's and before it read the tables,
    # the addition is not one Electric made, and its writers in flight are drained all the same.
    @tag connection_opt_overrides: [pool_size: 3]
    test "is drained when the migration that adds it hands the publication to another role",
         ctx do
      :atomics.put(ctx.owned, 1, 1)
      writer = start_writer(ctx)
      armed = :atomics.new(1, [])
      publication = ctx.publication_name

      Repatch.patch(
        Configuration,
        :check_publication_status!,
        [mode: :shared, force: true],
        fn pool, name ->
          status = Repatch.real(Configuration.check_publication_status!(pool, name))
          checked = %{status | can_alter_publication?: :atomics.get(ctx.owned, 1) == 1}

          if :atomics.compare_exchange(armed, 1, 1, 0) == :ok do
            {:ok, _} =
              Postgrex.transaction(ctx.pool, fn conn ->
                Postgrex.query!(conn, "ALTER PUBLICATION #{publication} ADD TABLE items", [])

                Postgrex.query!(
                  conn,
                  "ALTER PUBLICATION #{publication} OWNER TO unprivileged",
                  []
                )
              end)

            :atomics.put(ctx.owned, 1, 0)
          end

          checked
        end
      )

      :atomics.put(armed, 1, 1)
      assert_served_after_writer(ctx, writer, @where_clause_1)
      assert :atomics.get(armed, 1) == 0
    end

    # The same migration can commit after the pass read the catalog and before its addition, which
    # Postgres then refuses: the role no longer owns the publication. The table is in it, and the
    # admission waits for a fresh pass, which drains it.
    @tag connection_opt_overrides: [pool_size: 3]
    test "is drained when that migration commits before Electric's own addition", ctx do
      :atomics.put(ctx.owned, 1, 1)
      writer = start_writer(ctx)
      armed = :atomics.new(1, [])
      publication = ctx.publication_name

      Repatch.patch(
        Configuration,
        :add_table_to_publication,
        [mode: :shared],
        fn conn, name, filter ->
          if :atomics.compare_exchange(armed, 1, 1, 0) == :ok do
            {:ok, _} =
              Postgrex.transaction(ctx.pool, fn conn ->
                Postgrex.query!(conn, "ALTER PUBLICATION #{publication} ADD TABLE items", [])

                Postgrex.query!(
                  conn,
                  "ALTER PUBLICATION #{publication} OWNER TO unprivileged",
                  []
                )
              end)

            :atomics.put(ctx.owned, 1, 0)

            # The test's role is a superuser, whom Postgres lets alter any publication: this is
            # the refusal the role it stands for gets.
            {:error,
             %Postgrex.Error{
               postgres: %{
                 code: :insufficient_privilege,
                 pg_code: "42501",
                 message: "must be owner of publication #{publication}"
               }
             }}
          else
            Repatch.real(Configuration.add_table_to_publication(conn, name, filter))
          end
        end
      )

      :atomics.put(armed, 1, 1)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      assert :atomics.get(armed, 1) == 0
      refute_received {:remove_shapes_for_relations, _}

      retry = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, retry.pid, handle)
      finish_writer(writer)
      assert :ok = Task.await(retry)
    end

    # A pool of one connection has none to spare: a drain that held it until the writers on its
    # table finished would hold the checks of other tables, and every other user of the pool, as
    # long as the action timeout. Its first try waits for them briefly, and the connection is free
    # again once that try fails.
    @tag :single_connection_drain
    @tag admin_pool_size: 1
    @tag configurator_pool_size: 1
    @tag connection_opt_overrides: [pool_size: 3]
    test "with an admin pool of one, waits for the writers briefly and frees the connection",
         ctx do
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      # Well within the five seconds a drain on a larger pool waits for the lock.
      assert {:ok, {:error, %SnapshotError{type: :publication_admission_pending}}} =
               Task.yield(admission_waiter(ctx, handle, shape), 3_000)

      # The writer is still open, and the configurator's only connection, queued for the lock no
      # more, answers another user.
      refute drain_waiting?(ctx, writer)
      assert %{rows: [[1]]} = Postgrex.query!(ctx.configurator_pool, "SELECT 1", [])

      finish_writer(writer)
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
    end

    # A try that fails leaves the lock queue and lets in the writers queued behind it, so on a
    # table whose writers keep overlapping only a try that waits as long as they last drains it:
    # each failed try of a table doubles its next wait.
    @tag :single_connection_drain
    @tag admin_pool_size: 1
    @tag configurator_pool_size: 1
    @tag connection_opt_overrides: [pool_size: 6]
    test "with an admin pool of one, drains a table whose writers keep overlapping", ctx do
      report_drain_tries()
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      stop = :atomics.new(1, [])
      # Four writers 100 ms apart: one always has 300 ms or more left of its 400 ms window.
      writers = overlapping_writers(ctx, stop, 400, 4)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert wait_until(fn -> Task.await(admission_waiter(ctx, handle, shape)) == :ok end, 15_000)

      :atomics.put(stop, 1, 1)
      for writer <- writers, do: assert({:ok, :ok} = Task.yield(writer, 5_000))
      assert [100, 200 | _] = drain_tries()
    end

    # The last wait is the five seconds a drain on a larger pool waits: writers that each hold the
    # table for longer than a shorter one and keep overlapping are drained.
    @tag :single_connection_drain
    @tag admin_pool_size: 1
    @tag configurator_pool_size: 1
    @tag connection_opt_overrides: [pool_size: 14]
    test "with an admin pool of one, drains overlapping writers that each last 4.56 s", ctx do
      report_drain_tries()
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      stop = :atomics.new(1, [])
      # Twelve writers 380 ms apart: one always has 4.18 s or more left of its 4.56 s window.
      writers = overlapping_writers(ctx, stop, 4_560, 12)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert wait_until(
               fn -> Task.await(admission_waiter(ctx, handle, shape), 10_000) == :ok end,
               30_000
             )

      :atomics.put(stop, 1, 1)
      for writer <- writers, do: assert({:ok, :ok} = Task.yield(writer, 10_000))
      tries = drain_tries()
      assert Enum.take(tries, 6) == [100, 200, 400, 800, 1_600, 3_200]
      assert List.last(tries) == 5_000
    end

    # A try that fails otherwise than on its wait, on a lost connection say, says nothing of how
    # long the writers last; a table no longer owed a drain keeps no wait.
    @tag :single_connection_drain
    @tag admin_pool_size: 1
    @tag configurator_pool_size: 1
    test "with an admin pool of one, lengthens a table's wait only after one ran out", ctx do
      test_pid = self()
      tries = :atomics.new(1, [])
      configurator = PublicationManager.Configurator.name(ctx.stack_id)

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn _pool, _publication, _filter, _expected, lock_timeout ->
          send(test_pid, {:drain_try, lock_timeout})

          # The first try loses its connection, and the writers outlast every later one.
          if :atomics.add_get(tries, 1, 1) == 1 do
            {:error, %DBConnection.ConnectionError{message: "connection lost"}}
          else
            {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
          end
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      for _try <- 1..3 do
        assert {:error, %SnapshotError{type: :publication_admission_pending}} =
                 Task.await(admission_waiter(ctx, handle, shape))
      end

      assert Enum.take(drain_tries(), 3) == [100, 100, 200]
      assert map_size(:sys.get_state(configurator).drain_lock_timeouts) == 1

      # The table's shape goes, and with it the drain the table was owed.
      assert :ok = PublicationManager.remove_shape(ctx.stack_id, handle)
      assert wait_until(fn -> :sys.get_state(configurator).drain_lock_timeouts == %{} end)
    end

    # The earlier tries waited on the writers of the table's earlier membership: the table dropped
    # from the publication and added again starts over on the first wait.
    @tag :single_connection_drain
    @tag admin_pool_size: 1
    @tag configurator_pool_size: 1
    test "with an admin pool of one, starts a table added again on the first wait", ctx do
      test_pid = self()

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn _pool, _publication, _filter, {membership, _status}, lock_timeout ->
          send(test_pid, {:drain_try, lock_timeout, membership})
          {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      for _try <- 1..3 do
        assert {:error, %SnapshotError{type: :publication_admission_pending}} =
                 Task.await(admission_waiter(ctx, handle, shape))
      end

      assert_receive {:drain_try, 100, membership}
      assert_receive {:drain_try, 200, ^membership}
      assert_receive {:drain_try, 400, ^membership}

      {:ok, _} =
        Postgrex.transaction(ctx.pool, fn conn ->
          for change <- ["DROP", "ADD"] do
            Postgrex.query!(
              conn,
              "ALTER PUBLICATION #{ctx.publication_name} #{change} TABLE items",
              []
            )
          end
        end)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      assert {100, readded} = first_drain_try_off(membership)
      assert readded != membership
    end

    @tag :filter_revision
    @tag update_debounce_timeout: 60_000
    test "ignores a debounced filter snapshot overtaken by a drain recheck", ctx do
      parent = self()
      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      tracker = ctx.pub_mgr_opts[:server]
      drains = :atomics.new(1, [])

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          if :atomics.add_get(drains, 1, 1) == 1 do
            send(parent, {:draining, self()})
            assert_receive :recheck
            {:error, :publication_changed}
          else
            Repatch.real(
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected
              )
            )
          end
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      {items, _} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_1)
      oid = elem(ctx.relation_with_oid, 0)
      assert wait_until(fn -> Map.has_key?(:sys.get_state(tracker).waiters, oid) end)
      old = PublicationManager.RelationTracker.fetch_current_filters!(ctx.pub_mgr_opts)
      PublicationManager.Configurator.configure_publication(ctx.stack_id, old)
      %{scheduled_update_ref: first_timer} = :sys.get_state(configurator)

      send(
        GenServer.whereis(configurator),
        {:timeout, first_timer, :debounced_update_publication}
      )

      assert_receive {:draining, drain}

      sweep(ctx)

      %{scheduled_filters: obsolete, scheduled_update_ref: timer, drains: pending} =
        :sys.get_state(configurator)

      [{drain_ref, _}] = Map.to_list(pending)
      pid = GenServer.whereis(configurator)
      :ok = :sys.suspend(pid)

      send(drain, :recheck)

      assert wait_until(fn ->
               {:messages, messages} = Process.info(pid, :messages)
               {drain_ref, :publication_changed} in messages
             end)

      send(pid, {:timeout, timer, :debounced_update_publication})
      other = {"public", "serial_ids"}
      other_oid = lookup_relation_oid(ctx.pool, other)
      {added, _} = add_shape_async(ctx, {other_oid, other}, nil)
      assert wait_until(fn -> Map.has_key?(:sys.get_state(tracker).waiters, other_oid) end)
      :atomics.put(ctx.owned, 1, 1)
      :ok = :sys.resume(pid)

      assert [:ok, :ok] = Task.await_many([added, items])
      PublicationManager.Configurator.configure_publication(ctx.stack_id, obsolete)
      %{scheduled_update_ref: scheduled} = :sys.get_state(configurator)
      assert MapSet.new(fetch_pub_tables(ctx)) == MapSet.new([ctx.relation, other])
      assert is_nil(scheduled)
    end

    @tag :cancelled_filter_timer
    @tag update_debounce_timeout: 60_000
    test "an expired timer message cannot apply the filters of a newer timer", ctx do
      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      pid = GenServer.whereis(configurator)
      tracker = PublicationManager.RelationTracker

      PublicationManager.Configurator.configure_publication(
        ctx.stack_id,
        tracker.fetch_current_filters!(ctx.pub_mgr_opts)
      )

      %{scheduled_update_ref: expired} = :sys.get_state(pid)
      send(pid, {:timeout, expired, :debounced_update_publication})
      assert %{scheduled_update_ref: nil} = :sys.get_state(pid)

      PublicationManager.Configurator.configure_publication(
        ctx.stack_id,
        tracker.fetch_current_filters!(ctx.pub_mgr_opts)
      )

      %{scheduled_update_ref: current} = :sys.get_state(pid)
      assert is_reference(current)
      assert current != expired
      send(pid, {:timeout, expired, :debounced_update_publication})
      assert %{scheduled_update_ref: ^current} = :sys.get_state(pid)
    end

    @tag :obsolete_drain
    @tag connection_opt_overrides: [pool_size: 4]
    test "discards a completed drain queued before a check observes replacement membership",
         ctx do
      parent = self()
      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      drains = :atomics.new(1, [])

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          first? = :atomics.add_get(drains, 1, 1) == 1

          result =
            Configuration.wait_for_in_flight_writers_on_published_table(
              pool,
              publication,
              filter,
              expected,
              :infinity
            )

          if first? do
            send(parent, {:drained, self(), result})
            assert_receive :report_drain
          end

          result
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      {adder, handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_1)
      assert_receive {:drained, drain, {:ok, membership}}
      %{drains: pending} = :sys.get_state(configurator)
      [{drain_ref, _}] = Map.to_list(pending)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      Repatch.patch(
        Configuration,
        :determine_publication_relation_actions!,
        [mode: :shared],
        fn pool, publication, filters ->
          actions =
            Repatch.real(
              Configuration.determine_publication_relation_actions!(pool, publication, filters)
            )

          send(parent, {:checked_membership, self(), actions.memberships[ctx.relation_with_oid]})
          assert_receive :finish_check
          actions
        end
      )

      sweep(ctx)
      assert_receive {:checked_membership, checker, replacement}
      assert replacement != membership
      send(drain, :report_drain)

      assert wait_until(fn ->
               {:messages, messages} = Process.info(checker, :messages)
               {drain_ref, {:ok, membership}} in messages
             end)

      send(checker, :finish_check)
      assert_drain_waiting(ctx, writer, adder.pid, handle)
      finish_writer(writer)
      assert :ok = Task.await(adder)
    end

    # A drain verifies its table against the status its pass's check read. A pass whose snapshot
    # finds the publication changed since its check acts on nothing, and the answer of a drain that
    # verified the old status, queued as the pass reads, admits no shape: the check after the change
    # answers the shape. A query of the read after the status that fails, the probe for the tables
    # the dropped publication no longer names say, does not hide the change, nor does a connection
    # lost before the read's transaction ends.
    for {name, change, error_type, fault} <- [
          {"discards a completed drain queued before a pass finds the publication changed",
           "SET (publish = 'insert, update, truncate')", :publication_missing_operations, nil},
          {"discards a completed drain queued before a pass finds the publication dropped", :drop,
           :publication_missing, :probe},
          {"discards a completed drain queued before a pass loses the read that found a change",
           "SET (publish = 'insert, update, truncate')", :publication_missing_operations, :commit}
        ] do
      @tag change: change, error_type: error_type, fault: fault
      # The pool reconnects the session the lost connection case ends.
      @tag connection_opt_overrides:
             if(fault == :commit,
               do: [pool_size: 4, backoff_type: :exp, backoff_min: 10],
               else: [pool_size: 4]
             )
      test "#{name}", ctx do
        parent = self()
        configurator = PublicationManager.Configurator.name(ctx.stack_id)
        drains = :atomics.new(1, [])
        armed = :atomics.new(1, [])
        probe = :atomics.new(1, [])

        case ctx.fault do
          :probe ->
            Repatch.patch(
              Configuration,
              :existing_relation_oids!,
              [mode: :shared],
              fn conn, oids ->
                if oids != [] and :atomics.compare_exchange(probe, 1, 1, 0) == :ok do
                  raise %Postgrex.Error{
                    postgres: %{
                      code: :query_canceled,
                      pg_code: "57014",
                      severity: "ERROR",
                      message: "canceling statement due to statement timeout"
                    }
                  }
                end

                Repatch.real(Configuration.existing_relation_oids!(conn, oids))
              end
            )

          # The read's session ends once it has read the status, so the read's transaction ends on
          # a lost connection.
          :commit ->
            Repatch.patch(
              Configuration,
              :read_publication_status!,
              [mode: :shared],
              fn conn, publication ->
                status = Repatch.real(Configuration.read_publication_status!(conn, publication))

                if :atomics.compare_exchange(probe, 1, 1, 0) == :ok do
                  %{rows: [[pid]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])
                  Postgrex.query!(ctx.db_conn, "SELECT pg_terminate_backend($1, 5000)", [pid])
                end

                status
              end
            )

          nil ->
            :ok
        end

        # The connection manager whose admin pool answers the check that reads the change.
        start_supervised!(%{
          id: :manager,
          start:
            {Agent, :start_link,
             [fn -> nil end, [name: Electric.Connection.Manager.name(ctx.stack_id)]]}
        })

        Repatch.patch(
          Electric.Connection.Restarter,
          :restart_connection_subsystem,
          [mode: :shared],
          fn _, _ ->
            send(parent, :connection_subsystem_restarted)
            :ok
          end
        )

        Repatch.patch(
          Configuration,
          :wait_for_in_flight_writers_on_published_table,
          [mode: :shared],
          fn pool, publication, filter, expected ->
            first? = :atomics.add_get(drains, 1, 1) == 1

            result =
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected,
                :infinity
              )

            if first? do
              send(parent, {:drained, self(), result})
              assert_receive :report_drain
            end

            result
          end
        )

        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        {adder, _handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_1)
        assert_receive {:drained, drain, {:ok, membership}}
        %{drains: pending} = :sys.get_state(configurator)
        [{drain_ref, _}] = Map.to_list(pending)

        # The next pass's check has read the publication when its read is held, before the query
        # that takes the read's snapshot. The read's other queries, the probe among them, are not
        # under this patch's real call, which would run them unpatched.
        Repatch.patch(
          Configuration,
          :determine_publication_relation_actions!,
          [mode: :shared],
          fn conn, publication, filters ->
            if :atomics.compare_exchange(armed, 1, 1, 0) == :ok do
              send(parent, {:reading, self()})
              assert_receive :read
            end

            Repatch.real(
              Configuration.determine_publication_relation_actions!(conn, publication, filters)
            )
          end
        )

        :atomics.put(armed, 1, 1)
        sweep(ctx)
        assert_receive {:reading, reader}

        case ctx.change do
          :drop -> Postgrex.query!(ctx.pool, "DROP PUBLICATION #{ctx.publication_name}", [])
          sql -> Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} #{sql}", [])
        end

        send(drain, :report_drain)

        assert wait_until(fn ->
                 {:messages, messages} = Process.info(reader, :messages)
                 {drain_ref, {:ok, membership}} in messages
               end)

        :atomics.put(probe, 1, 1)
        send(reader, :read)

        error_type = ctx.error_type

        assert {:exit, {%Electric.DbConfigurationError{type: ^error_type}, _}} =
                 Task.yield(adder, 10_000)

        assert_receive :connection_subsystem_restarted
      end
    end

    # A drain that finds the publication changed under it knows what a pass finding it changed
    # knows: the answer of another drain, which verified its table against the old status, admits
    # no shape, though the check run on the report finds no connection, which keeps the drains. So
    # does a drain that finds only its own table changed, by a migration that drops both tables and
    # adds them back.
    for {found, change} <- [
          {"the publication changed", "SET (publish = 'insert, update, truncate')"},
          {"its own table changed", :readd}
        ] do
      @tag change: change
      @tag connection_opt_overrides: [pool_size: 4]
      test "discards a completed drain queued before another drain finds #{found}",
           ctx do
        parent = self()
        configurator = PublicationManager.Configurator.name(ctx.stack_id)
        held = :atomics.new(2, [])
        fail_check = :atomics.new(1, [])
        oid = elem(ctx.relation_with_oid, 0)
        other = {"public", "serial_ids"}
        other = {lookup_relation_oid(ctx.pool, other), other}

        # The connection manager whose admin pool answers the check that reads the change.
        start_supervised!(%{
          id: :manager,
          start:
            {Agent, :start_link,
             [fn -> nil end, [name: Electric.Connection.Manager.name(ctx.stack_id)]]}
        })

        Repatch.patch(
          Electric.Connection.Restarter,
          :restart_connection_subsystem,
          [mode: :shared],
          fn _, _ ->
            send(parent, :connection_subsystem_restarted)
            :ok
          end
        )

        # The first drain of `items` holds its answer, and the first of the other table waits before
        # it verifies.
        Repatch.patch(
          Configuration,
          :wait_for_in_flight_writers_on_published_table,
          [mode: :shared],
          fn pool, publication, filter, expected ->
            index = if filter == other, do: 2, else: 1
            first? = :atomics.add_get(held, index, 1) == 1

            if first? and filter == other do
              send(parent, {:verifying, self()})
              assert_receive :verify
            end

            result =
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected,
                :infinity
              )

            if first? and filter != other do
              send(parent, {:drained, self(), result})
              assert_receive :report_drain
            end

            result
          end
        )

        Postgrex.query!(ctx.pool, "ALTER TABLE serial_ids REPLICA IDENTITY FULL", [])

        Postgrex.query!(
          ctx.pool,
          "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items, serial_ids",
          []
        )

        {adder, _handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_1)
        assert_receive {:drained, drain, {:ok, membership}}
        add_shape_async(ctx, other, @where_clause_1)
        assert_receive {:verifying, verifier}
        assert map_size(:sys.get_state(configurator).drains) == 2

        # The publication stops publishing deletes, or both tables leave it and come back, after the
        # first drain verified it and before the other does, and the check that the other's report
        # runs finds no connection.
        case ctx.change do
          :readd ->
            for op <- ["DROP", "ADD"] do
              Postgrex.query!(
                ctx.pool,
                "ALTER PUBLICATION #{ctx.publication_name} #{op} TABLE items, serial_ids",
                []
              )
            end

          sql ->
            Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} #{sql}", [])
        end

        :atomics.put(fail_check, 1, 1)

        Repatch.patch(
          Configuration,
          :check_publication_status!,
          [mode: :shared, force: true],
          fn pool, publication ->
            if :atomics.compare_exchange(fail_check, 1, 1, 0) == :ok do
              send(parent, :check_failed)

              raise %DBConnection.ConnectionError{
                message: "connection not available and request was dropped from queue",
                reason: :queue_timeout
              }
            end

            case Repatch.real(Configuration.check_publication_status!(pool, publication)) do
              %{} = status -> %{status | can_alter_publication?: false}
              not_found -> not_found
            end
          end
        )

        send(verifier, :verify)
        assert_receive :check_failed

        assert {:exit, {%SnapshotError{type: :publication_admission_pending}, _}} =
                 Task.yield(adder, 10_000)

        # The first drain's answer comes once the shapes waiting have heard of the failed check.
        send(drain, :report_drain)
        assert wait_until(fn -> :sys.get_state(configurator).drains == %{} end)

        served =
          :sys.get_state(PublicationManager.RelationTracker.name(ctx.stack_id)).served_relations

        if ctx.change == :readd do
          refute match?({_, ^membership}, served[oid])
        else
          refute is_map_key(served, oid)
          assert_receive :connection_subsystem_restarted
        end
      end
    end

    for {change, error_type} <- [
          {:remove_membership, :relation_missing_from_publication},
          {:remove_membership_without_lock_privilege, :relation_missing_from_publication},
          {:reset_identity, :misconfigured_replica_identity},
          {:replace_relation, :schema_changed},
          {:drop_relation, :schema_changed}
        ] do
      @tag change: change
      test "rechecks #{change} at the drain lock", ctx do
        parent = self()

        Repatch.patch(
          Configuration,
          :wait_for_in_flight_writers_on_published_table,
          [mode: :shared],
          fn pool, publication, filter, expected ->
            send(parent, {:before_lock, self()})
            assert_receive :take_lock

            if ctx.change == :remove_membership_without_lock_privilege do
              {:error,
               {:lock_denied, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}}}
            else
              Repatch.real(
                Configuration.wait_for_in_flight_writers_on_published_table(
                  pool,
                  publication,
                  filter,
                  expected
                )
              )
            end
          end
        )

        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

        adder =
          Task.async(fn ->
            assert_raise Electric.DbConfigurationError, fn ->
              PublicationManager.add_shape(ctx.stack_id, handle, shape)
            end
          end)

        assert_receive {:before_lock, drain}

        case ctx.change do
          change when change in [:remove_membership, :remove_membership_without_lock_privilege] ->
            Postgrex.query!(
              ctx.pool,
              "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items",
              []
            )

          :reset_identity ->
            Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])

          :replace_relation ->
            Postgrex.query!(ctx.pool, "DROP TABLE items", [])
            Postgrex.query!(ctx.pool, "CREATE TABLE items (id uuid PRIMARY KEY)", [])
            Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])

            Postgrex.query!(
              ctx.pool,
              "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items",
              []
            )

          :drop_relation ->
            Postgrex.query!(ctx.pool, "DROP TABLE items", [])
        end

        send(drain, :take_lock)
        assert %Electric.DbConfigurationError{type: unquote(error_type)} = Task.await(adder)
      end
    end

    @tag connection_opt_overrides: [pool_size: 3]
    test "is drained again when that role adds it back", ctx do
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      sweep(ctx)
      assert_receive {:removed, [^shape_handle], []}

      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      # A pass that read the catalog before the addition answers every shape waiting on the table
      # when it ends, a shape asked for after the addition included. A pass that reads after it
      # ends behind them.
      sweep_and_wait(ctx)

      assert_served_after_writer(ctx, writer, @where_clause_2)
    end

    test "is held while its writers cannot be drained, and the shapes on it kept", ctx do
      drains = :atomics.new(1, [])

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          if :atomics.add_get(drains, 1, 1) == 1,
            do: {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")},
            else:
              Repatch.real(
                Configuration.wait_for_in_flight_writers_on_published_table(
                  pool,
                  publication,
                  filter,
                  expected
                )
              )
        end
      )

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      error =
        assert_raise Electric.SnapshotError, fn ->
          PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
        end

      assert Electric.SnapshotError.retryable?(error)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
      assert :atomics.get(drains, 1) == 2
      refute_received {:remove_shapes_for_relations, _}
    end

    @tag :drain_queue_order
    test "a waiting drain keeps its place ahead of retried drains that time out", ctx do
      parent = self()
      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      names = ~w(blocked_1 blocked_2 blocked_3 blocked_4 healthy)

      [b1, b2, b3, b4, healthy] =
        for name <- names do
          Postgrex.query!(ctx.pool, "CREATE TABLE #{name} (id UUID PRIMARY KEY)", [])
          Postgrex.query!(ctx.pool, "ALTER TABLE #{name} REPLICA IDENTITY FULL", [])

          Postgrex.query!(
            ctx.pool,
            "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE #{name}",
            []
          )

          rel = {"public", name}
          {lookup_relation_oid(ctx.pool, rel), rel}
        end

      blocked = [b1, b2, b3, b4]
      # A check lists its drains in term order: the healthy table, created last, comes last.
      assert Enum.all?(blocked, &(&1 < healthy))

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          send(parent, {:drain_started, filter, self()})

          if filter in blocked do
            receive do
              :time_out ->
                {_oid, rel} = filter

                {:error,
                 Electric.DbConfigurationError.table_lock_timeout(
                   Electric.Utils.relation_to_sql(rel)
                 )}
            end
          else
            Repatch.real(
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected
              )
            )
          end
        end
      )

      [{first, {handle, shape}}, _, _, _, {waiting, _}] =
        for filter <- [b1, b2, b3, b4, healthy] do
          shape = generate_shape(filter, nil)
          {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

          task =
            Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
              PublicationManager.add_shape(ctx.stack_id, handle, shape)
            end)

          assert wait_until(fn ->
                   Map.has_key?(
                     :sys.get_state(ctx.pub_mgr_opts[:server]).waiters,
                     elem(filter, 0)
                   )
                 end)

          {task, {handle, shape}}
        end

      drain_pids =
        for filter <- [b1, b2, b3], into: %{} do
          assert_receive {:drain_started, ^filter, pid}
          {filter, pid}
        end

      refute_receive {:drain_started, _, _}, 100

      # The first drain times out and its slot goes to the next queued drain, ahead of the healthy
      # table's.
      send(drain_pids[b1], :time_out)
      assert {:exit, _} = Task.yield(first, 5_000)
      assert_receive {:drain_started, ^b4, _}

      # The shape on the first table retries, and its check requeues that table's drain behind the
      # healthy table's, which has waited since the previous check.
      retried =
        Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
          PublicationManager.add_shape(ctx.stack_id, handle, shape)
        end)

      assert wait_until(fn ->
               length(:sys.get_state(configurator).queued_drains) == 2
             end)

      send(drain_pids[b2], :time_out)
      assert_receive {:drain_started, next, _}
      assert next == healthy
      assert {:ok, :ok} = Task.yield(waiting, 5_000)
      Task.shutdown(retried, :brutal_kill)
    end

    @tag :drain_cap
    test "leaves one admin connection free of drains" do
      assert PublicationManager.Configurator.max_concurrent_drains(nil) == 3

      for {admin_pool_size, drains} <- [{1, 1}, {2, 1}, {3, 2}, {4, 3}, {10, 3}] do
        assert PublicationManager.Configurator.max_concurrent_drains(admin_pool_size) == drains,
               "admin pool of #{admin_pool_size}"
      end
    end

    # A drain holds its connection while it waits for the writers on its table: drains that took
    # every connection would hold the checks of other tables, and every other user of the pool,
    # until their writers finished.
    @tag :drain_cap
    @tag admin_pool_size: 2
    @tag connection_opt_overrides: [pool_size: 2]
    test "checks another table while drains wait with an admin pool of two", ctx do
      parent = self()
      configurator = PublicationManager.Configurator.name(ctx.stack_id)

      [b1, b2, other] =
        for name <- ~w(blocked_1 blocked_2 other_table) do
          Postgrex.query!(ctx.pool, "CREATE TABLE #{name} (id UUID PRIMARY KEY)", [])
          Postgrex.query!(ctx.pool, "ALTER TABLE #{name} REPLICA IDENTITY FULL", [])

          Postgrex.query!(
            ctx.pool,
            "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE #{name}",
            []
          )

          rel = {"public", name}
          {lookup_relation_oid(ctx.pool, rel), rel}
        end

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          if filter in [b1, b2] do
            DBConnection.run(pool, fn _conn ->
              send(parent, {:drain_holding, filter, self()})
              receive(do: (:release -> :ok))
            end)
          end

          Repatch.real(
            Configuration.wait_for_in_flight_writers_on_published_table(
              pool,
              publication,
              filter,
              expected
            )
          )
        end
      )

      {blocked_1, _} = add_shape_async(ctx, b1, nil)
      assert_receive {:drain_holding, ^b1, holder_1}
      {blocked_2, _} = add_shape_async(ctx, b2, nil)
      tracker = PublicationManager.RelationTracker.name(ctx.stack_id)
      assert wait_until(fn -> Map.has_key?(:sys.get_state(tracker).waiters, elem(b2, 0)) end)

      # The other table's check, which queues its drain behind the second, has a connection.
      {checked, _} = add_shape_async(ctx, other, nil)
      assert wait_until(fn -> length(:sys.get_state(configurator).queued_drains) == 2 end, 1_000)
      refute_received {:drain_holding, _, _}

      send(holder_1, :release)
      assert {:ok, :ok} = Task.yield(blocked_1, 5_000)
      assert_receive {:drain_holding, ^b2, holder_2}
      send(holder_2, :release)
      assert {:ok, :ok} = Task.yield(blocked_2, 5_000)
      assert {:ok, :ok} = Task.yield(checked, 5_000)
    end

    # A check that finds no connection keeps the drains of the tables shapes still ask for. One
    # queued for a table whose last shape is gone would take the only drain slot, and its
    # connection, for no shape once the drain ahead of it ends.
    @tag :drain_cap
    @tag admin_pool_size: 2
    @tag connection_opt_overrides: [pool_size: 2]
    test "drops the queued drain of a table no shape asks for when a check loses its connection",
         ctx do
      parent = self()
      configurator = PublicationManager.Configurator.name(ctx.stack_id)

      [b1, b2] =
        for name <- ~w(blocked_1 blocked_2) do
          Postgrex.query!(ctx.pool, "CREATE TABLE #{name} (id UUID PRIMARY KEY)", [])
          Postgrex.query!(ctx.pool, "ALTER TABLE #{name} REPLICA IDENTITY FULL", [])

          Postgrex.query!(
            ctx.pool,
            "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE #{name}",
            []
          )

          rel = {"public", name}
          {lookup_relation_oid(ctx.pool, rel), rel}
        end

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          DBConnection.run(pool, fn _conn ->
            send(parent, {:drain_holding, filter, self()})
            receive(do: (:release -> :ok))
          end)

          Repatch.real(
            Configuration.wait_for_in_flight_writers_on_published_table(
              pool,
              publication,
              filter,
              expected
            )
          )
        end
      )

      shape_1 = generate_shape(b1, nil)
      {:ok, handle_1} = ShapeStatus.add_shape(ctx.stack_id, shape_1)
      blocked_1 = admission_waiter(ctx, handle_1, shape_1)
      assert_receive {:drain_holding, ^b1, holder_1}
      {blocked_2, handle_2} = add_shape_async(ctx, b2, nil)
      assert wait_until(fn -> length(:sys.get_state(configurator).queued_drains) == 1 end, 1_000)

      Repatch.patch(
        Configuration,
        :check_publication_status!,
        [mode: :shared, force: true],
        fn _, _ ->
          raise %DBConnection.ConnectionError{
            message: "connection not available and request was dropped from queue",
            reason: :queue_timeout
          }
        end
      )

      :ok = PublicationManager.remove_shape(ctx.stack_id, handle_2)
      Task.shutdown(blocked_2, :brutal_kill)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(blocked_1)

      assert %{drains: drains, queued_drains: []} = :sys.get_state(configurator)
      assert [{^b1, _membership, _status}] = for({_ref, {drain, _task}} <- drains, do: drain)

      send(holder_1, :release)
      assert wait_until(fn -> :sys.get_state(configurator).drains == %{} end)
      refute_receive {:drain_holding, ^b2, _holder}, 200
    end

    for {check, reason} <- [status: :error, membership: :queue_timeout, membership: :canceled] do
      @tag :transient_check
      @tag connection_opt_overrides: [pool_size: 4]
      test "retries admission when the #{check} check #{if reason == :canceled, do: "is canceled", else: "loses its connection"}",
           ctx do
        Repatch.patch(
          Configuration,
          :wait_for_in_flight_writers_on_published_table,
          [mode: :shared],
          fn pool, publication, filter, expected ->
            result =
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected,
                :infinity
              )

            # A check that finds no connection leaves the drain running. Hold its result, so that
            # it reports nothing while the test looks at what the check left owed, nor after.
            receive(do: (:report_drain -> result))
          end
        )

        writer = start_writer(ctx)
        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

        adder =
          Task.async(fn ->
            filter = PublicationManager.RelationTracker.publication_filter(shape)
            GenServer.call(ctx.pub_mgr_opts[:server], {:add_shape, handle, filter}, :infinity)
          end)

        assert_drain_waiting(ctx, writer, adder.pid, handle)

        # A canceled query, a `statement_timeout` say, is as transient as a lost connection.
        error =
          case unquote(reason) do
            :canceled ->
              %Postgrex.Error{
                postgres: %{
                  code: :query_canceled,
                  pg_code: "57014",
                  message: "canceling statement due to statement timeout"
                }
              }

            reason ->
              %DBConnection.ConnectionError{message: "connection unavailable", reason: reason}
          end

        case unquote(check) do
          :status ->
            Repatch.patch(
              Configuration,
              :check_publication_status!,
              [mode: :shared, force: true],
              fn _, _ -> raise error end
            )

          :membership ->
            Repatch.patch(
              Configuration,
              :determine_publication_relation_actions!,
              [mode: :shared],
              fn _, _, _ -> raise error end
            )
        end

        sweep(ctx)
        assert {:error, %SnapshotError{type: :publication_admission_pending}} = Task.await(adder)

        oid = elem(ctx.relation_with_oid, 0)

        assert MapSet.member?(
                 PublicationManager.RelationTracker.barriers_owed(ctx.pub_mgr_opts),
                 oid
               )

        refute_received {:remove_shapes_for_relations, _}
        finish_writer(writer)
      end
    end

    # A check that finds no connection says nothing of the table a drain waits on: on an admin pool
    # of one, the drain holds the connection the check queued for. The drain goes on, and the table
    # is served once it ends, with no second drain.
    @tag connection_opt_overrides: [pool_size: 4]
    test "keeps a drain running across a check that loses its connection", ctx do
      report_drain_tries()
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      adder = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, adder.pid, handle)

      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      oid = elem(ctx.relation_with_oid, 0)

      assert [{drain_ref, {{{^oid, _rel}, membership, _status}, %Task{pid: drain}}}] =
               Map.to_list(:sys.get_state(configurator).drains)

      Repatch.patch(
        Configuration,
        :check_publication_status!,
        [mode: :shared, force: true],
        fn _, _ ->
          raise %DBConnection.ConnectionError{
            message: "connection not available and request was dropped from queue",
            reason: :queue_timeout
          }
        end
      )

      sweep(ctx)
      assert {:error, %SnapshotError{type: :publication_admission_pending}} = Task.await(adder)
      assert %{^drain_ref => _} = :sys.get_state(configurator).drains
      assert Process.alive?(drain)
      assert drain_waiting?(ctx, writer)

      # The drain that went on is the one whose end serves the table.
      Repatch.restore(Configuration, :check_publication_status!, 2, mode: :shared)
      finish_writer(writer)
      assert wait_until(fn -> :sys.get_state(configurator).drains == %{} end)
      # The tracker has taken the drain's result, which the configurator sent it before that.
      assert %{served_relations: %{^oid => {_rel, ^membership}}} =
               :sys.get_state(PublicationManager.RelationTracker.name(ctx.stack_id))

      assert [_one] = drain_tries()
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
      assert [_one] = drain_tries()
    end

    # A check is asked for the filters of its time, and a shape can ask for a table while it runs:
    # the last shape on the table gone, the check asked for without it, and a new shape asking for
    # it before the check fails. The drain of that table goes on, with the wait it has run up.
    @tag connection_opt_overrides: [pool_size: 4]
    test "keeps a drain a shape asks for while a check made without it fails", ctx do
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      adder = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, adder.pid, handle)

      configurator = PublicationManager.Configurator.name(ctx.stack_id)

      assert [{drain_ref, {_drain, %Task{pid: drain}}}] =
               Map.to_list(:sys.get_state(configurator).drains)

      Repatch.patch(
        Configuration,
        :check_publication_status!,
        [mode: :shared, force: true],
        fn _, _ ->
          raise %DBConnection.ConnectionError{
            message: "connection not available and request was dropped from queue",
            reason: :queue_timeout
          }
        end
      )

      PublicationManager.Configurator.configure_publication(
        ctx.stack_id,
        {System.unique_integer([:positive, :monotonic]), MapSet.new()}
      )

      assert {:error, %SnapshotError{type: :publication_admission_pending}} = Task.await(adder)
      assert %{^drain_ref => _} = :sys.get_state(configurator).drains
      assert Process.alive?(drain)
      finish_writer(writer)
    end

    @tag connection_opt_overrides: [pool_size: 3]
    test "keeps a failed drain owed across ownership transfer and configurator restart", ctx do
      drains = :atomics.new(1, [])

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          if :atomics.add_get(drains, 1, 1) == 1 do
            {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
          else
            Repatch.real(
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected
              )
            )
          end
        end
      )

      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert_raise SnapshotError, fn ->
        PublicationManager.add_shape(ctx.stack_id, handle, shape)
      end

      :atomics.put(ctx.owned, 1, 1)
      restart_configurator(ctx)
      assert_served_after_writer(ctx, writer, @where_clause_2)
    end

    @tag :barrier_write_failure
    @tag barrier_write_fault: :file_error
    @tag connection_opt_overrides: [pool_size: 4]
    test "keeps a failed obligation write pending through an ownership transfer", ctx do
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      tracker = GenServer.whereis(ctx.pub_mgr_opts[:server])
      oid = elem(ctx.relation_with_oid, 0)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      assert_receive :barrier_write_failed
      :atomics.put(ctx.owned, 1, 1)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      assert_receive :barrier_write_failed

      assert MapSet.new([oid]) ==
               PublicationManager.RelationTracker.barriers_owed(ctx.pub_mgr_opts)

      assert GenServer.whereis(ctx.pub_mgr_opts[:server]) == tracker
      Agent.update(ctx.write_fault, fn _ -> :ok end)

      assert_receive {PersistentKV.Memory, {:set, _, record}}
      assert [nil, [^oid], [], false | _] = Jason.decode!(record)

      adder = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, adder.pid, handle)
      finish_writer(writer)
      assert :ok = Task.await(adder)
      refute_received {:remove_shapes_for_relations, _}
    end

    @tag :barrier_write_failure
    @tag :blocked_invalidation
    @tag :retained_membership_record
    @tag barrier_write_fault: :ok
    @tag connection_opt_overrides: [pool_size: 4]
    test "records stale handles before removing them after an observed absence", ctx do
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, stale} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, stale, shape)
      assert_receive {PersistentKV.Memory, {:set, key, _}}
      assert_receive {PersistentKV.Memory, {:set, ^key, served_record}}

      assert [nil, [], [[oid, "public", "items", _]] = served, false | _] =
               Jason.decode!(served_record)

      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      unblock = block_catalog_writes(ctx)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      Agent.update(ctx.write_fault, fn _ -> :enospc end)
      sweep(ctx)
      assert_receive :barrier_write_failed
      :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))

      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      :atomics.put(ctx.owned, 1, 1)

      refute_received {:removed, [^stale], _}
      assert ShapeStatus.has_shape_handle?(ctx.stack_id, stale)

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      Agent.update(ctx.write_fault, fn _ -> :ok end)
      assert_receive {PersistentKV.Memory, {:set, ^key, record}}
      assert [nil, [^oid], ^served, false, [^oid], [[^stale, [^oid]]]] = Jason.decode!(record)
      assert_receive {:removed, [^stale], [_]}

      assert %{drains: drains} = sweep_and_wait(ctx)
      assert drains == %{}, "expected the stale SQLite row to hold back the drain"
      refute drain_waiting?(ctx, writer)
      assert shape_on_disk?(ctx.stack_id, stale)

      unblock.()
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      adder = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, adder.pid, handle)
      refute shape_on_disk?(ctx.stack_id, stale)
      finish_writer(writer)
      assert :ok = Task.await(adder)
    end

    for {change, ddl} <- [
          renamed: "ALTER TABLE items RENAME TO renamed_items",
          dropped: "DROP TABLE items"
        ] do
      @tag :barrier_write_failure
      @tag :blocked_invalidation
      @tag barrier_write_fault: :enospc
      test "invalidates a #{change} relation while its barrier write is pending", ctx do
        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

        assert {:error, %SnapshotError{type: :publication_admission_pending}} =
                 Task.await(admission_waiter(ctx, handle, shape))

        assert_receive :barrier_write_failed
        relation = ctx.relation_with_oid
        Postgrex.query!(ctx.pool, unquote(ddl), [])
        sweep(ctx)

        assert_receive {:remove_shapes_for_relations, [^relation]}
      end
    end

    @tag :barrier_write_failure
    @tag :retry_barrier_clear
    @tag barrier_write_fault: :ok
    @tag connection_opt_overrides: [pool_size: 4]
    test "retries admission when the write clearing a completed drain fails", ctx do
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      :ok = ShapeStatus.mark_snapshot_complete(ctx.stack_id, handle)
      oid = elem(ctx.relation_with_oid, 0)
      tracker = GenServer.whereis(ctx.pub_mgr_opts[:server])

      adder = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, adder.pid, handle)
      assert_receive {PersistentKV.Memory, {:set, key, record}}
      assert [nil, [^oid], [], false | _] = Jason.decode!(record)

      Agent.update(ctx.write_fault, fn _ -> :enospc end)
      finish_writer(writer)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} = Task.await(adder)
      assert_receive :barrier_write_failed

      assert MapSet.new([oid]) ==
               PublicationManager.RelationTracker.barriers_owed(ctx.pub_mgr_opts)

      assert GenServer.whereis(ctx.pub_mgr_opts[:server]) == tracker

      parent = self()

      Repatch.patch(
        Configuration,
        :determine_publication_relation_actions!,
        [mode: :shared],
        fn pool, publication, filters ->
          actions =
            Repatch.real(
              Configuration.determine_publication_relation_actions!(pool, publication, filters)
            )

          send(parent, {:checked_membership, actions.memberships})
          actions
        end
      )

      relation = ctx.relation_with_oid
      Agent.update(ctx.write_fault, fn _ -> :ok end)
      assert_receive {PersistentKV.Memory, {:set, ^key, record}}
      assert [nil, [], [[^oid, "public", "items", _]], false | _] = Jason.decode!(record)
      assert_receive {:checked_membership, %{^relation => _}}
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
      assert ShapeStatus.has_shape_handle?(ctx.stack_id, handle)
      refute_received {:remove_shapes_for_relations, _}
    end

    @tag :barrier_recovery
    @tag :unreadable_untracked_barrier
    @tag barrier_write_fault: :ok
    @tag connection_opt_overrides: [pool_size: 4]
    test "drains a published table without restored shapes after an unreadable barrier record",
         ctx do
      tracker = PublicationManager.RelationTracker
      tracker.owe_barriers(MapSet.new([ctx.relation_with_oid]), ctx.pub_mgr_opts)
      assert_receive {PersistentKV.Memory, {:set, key, _}}
      :ok = PersistentKV.set(ctx.persistent_kv, key, "invalid JSON")

      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      :atomics.put(ctx.owned, 1, 1)

      name = tracker.name(ctx.stack_id)
      old_tracker = GenServer.whereis(name)
      GenServer.stop(name)
      wait_for_restart(name, old_tracker)
      assert :ok = tracker.wait_for_restore(ctx.stack_id)
      assert {_, filters} = tracker.fetch_current_filters!(ctx.pub_mgr_opts)
      assert MapSet.size(filters) == 0

      assert_served_after_writer(ctx, writer, @where_clause_1)
    end

    @tag :completion_write_sweep
    @tag barrier_write_fault: :ok
    test "keeps the restored shape across a sweep while recording its recovered drain", ctx do
      tracker = PublicationManager.RelationTracker
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      {completed, _} = admit_completed_shape(ctx)
      assert_receive {PersistentKV.Memory, {:set, key, _}}
      assert_receive {PersistentKV.Memory, {:set, ^key, _}}
      :ok = PersistentKV.set(ctx.persistent_kv, key, "invalid JSON")

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          assert {:ok, _} =
                   result =
                   Repatch.real(
                     Configuration.wait_for_in_flight_writers_on_published_table(
                       pool,
                       publication,
                       filter,
                       expected
                     )
                   )

          Agent.update(ctx.write_fault, fn _ -> :enospc end)
          result
        end
      )

      name = tracker.name(ctx.stack_id)
      old = GenServer.whereis(name)
      GenServer.stop(old)
      wait_for_restart(name, old)
      :ok = tracker.wait_for_restore(ctx.stack_id)
      assert_receive :barrier_write_failed

      assert %{drains: drains} = sweep_and_wait(ctx)
      assert drains == %{}
      refute_received {:removed, [^completed], _}
      assert shape_on_disk?(ctx.stack_id, completed)

      replacement = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, replacement)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, replacement))

      assert_receive {PersistentKV.Memory, {:set, ^key, "invalid JSON"}}
      assert_receive {PersistentKV.Memory, {:set, ^key, rebuilt}}
      assert [nil, [_], [], true | _] = Jason.decode!(rebuilt)

      Agent.update(ctx.write_fault, fn _ -> :ok end)
      assert_receive {PersistentKV.Memory, {:set, ^key, saved}}
      assert [nil, [], [[_, "public", "items", _]], true | _] = Jason.decode!(saved)
      assert :ok = Task.await(admission_waiter(ctx, handle, replacement))
      assert shape_on_disk?(ctx.stack_id, completed)
    end

    for replica_identity <- ["FULL", "DEFAULT"] do
      @tag :owned_add_recovery
      @tag barrier_write_fault: :ok
      @tag connection_opt_overrides: [pool_size: 4]
      test "records the owned addition's drain after record loss with replica identity #{replica_identity}",
           ctx do
        tracker = PublicationManager.RelationTracker
        relation = ctx.relation_with_oid
        {oid, {schema, table}} = relation
        tracker.owe_barriers(MapSet.new([relation]), ctx.pub_mgr_opts)
        assert_receive {PersistentKV.Memory, {:set, key, _}}
        :ok = PersistentKV.set(ctx.persistent_kv, key, "invalid JSON")

        :atomics.put(ctx.owned, 1, 1)

        Postgrex.query!(
          ctx.pool,
          "ALTER TABLE items REPLICA IDENTITY #{unquote(replica_identity)}",
          []
        )

        name = tracker.name(ctx.stack_id)
        old_tracker = GenServer.whereis(name)
        GenServer.stop(name)
        wait_for_restart(name, old_tracker)
        assert :ok = tracker.wait_for_restore(ctx.stack_id)
        assert {_, _, true, _} = tracker.barrier_state(ctx.pub_mgr_opts)

        writer = start_writer(ctx)
        shape = generate_shape(relation, @where_clause_1)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
        adder = admission_waiter(ctx, handle, shape)
        assert_drain_waiting(ctx, writer, adder.pid, handle)
        finish_writer(writer)
        assert :ok = Task.await(adder)

        writer = start_writer(ctx)
        assert %{drains: drains} = sweep_and_wait(ctx)
        assert drains == %{}, "expected the check to reuse the owned addition's drain"
        refute drain_waiting?(ctx, writer)

        shape = generate_shape(relation, @where_clause_2)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
        assert :ok = Task.await(admission_waiter(ctx, handle, shape))

        assert {owed, %{^relation => membership}, true, _} =
                 tracker.barrier_state(ctx.pub_mgr_opts)

        assert MapSet.size(owed) == 0
        assert {:ok, record} = PersistentKV.get(ctx.persistent_kv, key)
        assert [nil, [], [[^oid, ^schema, ^table, ^membership]], true | _] = Jason.decode!(record)
        finish_writer(writer)
      end
    end

    @tag :owned_repair_recovery
    @tag barrier_write_fault: :ok
    @tag connection_opt_overrides: [pool_size: 4]
    test "records the replica identity repair's drain after record loss", ctx do
      tracker = PublicationManager.RelationTracker
      relation = ctx.relation_with_oid
      {oid, {schema, table}} = relation

      tracker.owe_barriers(MapSet.new([relation]), ctx.pub_mgr_opts)
      assert_receive {PersistentKV.Memory, {:set, key, _}}
      :ok = PersistentKV.set(ctx.persistent_kv, key, "invalid JSON")

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      :atomics.put(ctx.owned, 1, 1)

      name = tracker.name(ctx.stack_id)
      old_tracker = GenServer.whereis(name)
      GenServer.stop(name)
      wait_for_restart(name, old_tracker)
      assert :ok = tracker.wait_for_restore(ctx.stack_id)
      assert {_, _, true, _} = tracker.barrier_state(ctx.pub_mgr_opts)

      writer = start_writer(ctx)
      shape = generate_shape(relation, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      adder = admission_waiter(ctx, handle, shape)
      assert_drain_waiting(ctx, writer, adder.pid, handle, "AccessExclusiveLock")
      finish_writer(writer)
      assert :ok = Task.await(adder)
      assert replica_identity(ctx) == "f"

      writer = start_writer(ctx)
      %{drains: drains} = sweep_and_wait(ctx)
      assert drains == %{}, "expected the check to reuse the replica identity repair's drain"
      shape = generate_shape(relation, @where_clause_2)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
      refute drain_waiting?(ctx, writer, nil)

      assert {owed, %{^relation => membership}, true, _} = tracker.barrier_state(ctx.pub_mgr_opts)
      assert MapSet.size(owed) == 0
      assert {:ok, record} = PersistentKV.get(ctx.persistent_kv, key)
      assert [nil, [], [[^oid, ^schema, ^table, ^membership]], true | _] = Jason.decode!(record)
      finish_writer(writer)
    end

    for action <- [:add_and_configure, :configure] do
      @tag replaced_owned_relation: action
      @tag configurator_pool_size: 2
      test "rolls back #{action} when the planned table is replaced",
           %{replaced_owned_relation: action} = ctx do
        relation = ctx.relation_with_oid
        {oid, name} = relation
        Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])

        if action == :configure do
          Postgrex.query!(
            ctx.pool,
            "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items",
            []
          )
        end

        :atomics.put(ctx.owned, 1, 1)
        parent = self()

        Repatch.patch(
          Configuration,
          :determine_publication_relation_actions!,
          [mode: :shared],
          fn pool, publication, filters ->
            actions =
              Repatch.real(
                Configuration.determine_publication_relation_actions!(pool, publication, filters)
              )

            if MapSet.member?(actions.to_configure_replica_identity, relation) do
              send(parent, {:planned_repair, self(), actions})
              assert_receive :run_planned_repair
            end

            actions
          end
        )

        shape = generate_shape(relation, @where_clause_1)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
        adder = admission_waiter(ctx, handle, shape)

        assert_receive {:planned_repair, configurator, actions}
        assert MapSet.member?(actions.to_add, relation) == (action == :add_and_configure)
        assert Map.has_key?(actions.memberships, relation) == (action == :configure)

        assert {:ok, _} =
                 Postgrex.transaction(ctx.pool, fn conn ->
                   Postgrex.query!(conn, "DROP TABLE items", [])

                   Postgrex.query!(
                     conn,
                     "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT)",
                     []
                   )
                 end)

        replacement_oid = lookup_relation_oid(ctx.pool, name)
        assert replacement_oid != oid
        send(configurator, :run_planned_repair)

        result = Task.await(adder)

        %{rows: rows} =
          Postgrex.query!(
            ctx.pool,
            """
            SELECT c.relreplident::text, pr.oid
            FROM pg_class c
            LEFT JOIN pg_publication_rel pr ON pr.prrelid = c.oid
            WHERE c.oid = $1
            """,
            [replacement_oid]
          )

        assert {{:error, %SnapshotError{type: :publication_admission_pending}}, [["d", nil]]} =
                 {result, rows}
      end
    end

    @tag :owned_repair_mismatch
    @tag configurator_pool_size: 2
    test "holds new shapes until a fresh check after a repair finds the membership gone", ctx do
      tracker = PublicationManager.RelationTracker
      relation = ctx.relation_with_oid
      {oid, _name} = relation

      :atomics.put(ctx.owned, 1, 1)
      shape = generate_shape(relation, @where_clause_1)
      {:ok, completed} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = Task.await(admission_waiter(ctx, completed, shape))
      :ok = ShapeStatus.mark_snapshot_complete(ctx.stack_id, completed)
      assert {_, %{^relation => membership}, false, _} = tracker.barrier_state(ctx.pub_mgr_opts)

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      hold_publication_checks(ctx)
      sweep(ctx)

      assert_receive {:publication_checked, 1, configurator, actions}
      assert MapSet.member?(actions.to_configure_replica_identity, relation)
      assert actions.memberships[relation] == membership

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      send(configurator, :continue_publication_check)

      assert_receive {:publication_checked, 2, ^configurator, actions}
      assert MapSet.member?(actions.to_add, relation)
      assert ShapeStatus.has_shape_handle?(ctx.stack_id, completed)
      assert {_, %{^relation => ^membership}, false, _} = tracker.barrier_state(ctx.pub_mgr_opts)

      shape = generate_shape(relation, @where_clause_2)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      request =
        :gen_server.send_request(
          ctx.pub_mgr_opts[:server],
          {:add_shape, handle, tracker.publication_filter(shape)}
        )

      assert %{waiters: %{^oid => [{_, ^handle}]}} = :sys.get_state(ctx.pub_mgr_opts[:server])

      send(configurator, :continue_publication_check)
      assert_receive {:removed, [^completed], []}
      assert {:reply, :ok} = :gen_server.receive_response(request, 5_000)
    end

    @tag :owned_repair_membership
    @tag configurator_pool_size: 2
    test "rolls back a repair when only the planned membership is replaced", ctx do
      tracker = PublicationManager.RelationTracker
      relation = ctx.relation_with_oid
      {oid, _name} = relation
      :atomics.put(ctx.owned, 1, 1)
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      hold_publication_checks(ctx)

      shape = generate_shape(relation, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      adder = admission_waiter(ctx, handle, shape)

      assert_receive {:publication_checked, 1, configurator, actions}
      assert MapSet.member?(actions.to_configure_replica_identity, relation)
      refute MapSet.member?(actions.to_add, relation)
      membership = Map.fetch!(actions.memberships, relation)

      assert {:ok, _} =
               Postgrex.transaction(ctx.pool, fn conn ->
                 Postgrex.query!(
                   conn,
                   "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items",
                   []
                 )

                 Postgrex.query!(
                   conn,
                   "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items",
                   []
                 )
               end)

      assert %{rows: [[replacement]]} =
               Postgrex.query!(
                 ctx.pool,
                 "SELECT oid FROM pg_publication_rel WHERE prrelid = $1",
                 [oid]
               )

      assert replacement != membership
      send(configurator, :continue_publication_check)

      assert {{:error, %SnapshotError{type: :publication_admission_pending}}, "d"} =
               {Task.await(adder), replica_identity(ctx)}

      assert_receive {:publication_checked, 2, ^configurator, actions}
      assert actions.memberships[relation] == replacement
      send(configurator, :continue_publication_check)

      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
      assert {_, %{^relation => ^replacement}, false, _} = tracker.barrier_state(ctx.pub_mgr_opts)
      assert replica_identity(ctx) == "f"
    end

    @tag :changed_membership_cleanup
    @tag configurator_pool_size: 2
    test "removes the old shape before repairing a membership replaced after verification", ctx do
      :atomics.put(ctx.owned, 1, 1)
      {completed, membership} = admit_completed_shape(ctx)
      relation = ctx.relation_with_oid
      tracker = PublicationManager.RelationTracker
      server = ctx.pub_mgr_opts[:server]
      release = block_catalog_writes(ctx)

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      hold_publication_checks(ctx, before: 2)
      sweep(ctx)

      assert_receive {:publication_checked, 1, configurator, actions}
      assert MapSet.member?(actions.to_configure_replica_identity, relation)
      assert actions.memberships[relation] == membership
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      send(configurator, :continue_publication_check)

      assert_receive {:checking_publication, 2, ^configurator}
      assert replica_identity(ctx) == "d"
      assert {_, %{^relation => ^membership}, false, _} = tracker.barrier_state(ctx.pub_mgr_opts)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      send(configurator, :continue_publication_check)

      assert_receive {:publication_checked, 2, ^configurator, actions}
      replacement = Map.fetch!(actions.memberships, relation)
      assert replacement != membership
      assert MapSet.member?(actions.to_configure_replica_identity, relation)

      shape = generate_shape(relation, @where_clause_2)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      request =
        :gen_server.send_request(server, {:add_shape, handle, tracker.publication_filter(shape)})

      {oid, _} = relation
      assert %{waiters: %{^oid => [{_, ^handle}]}} = :sys.get_state(server)

      send(configurator, :continue_publication_check)

      assert {:reply, {:error, %SnapshotError{type: :publication_admission_pending}}} =
               :gen_server.receive_response(
                 request,
                 ExUnit.configuration()[:assert_receive_timeout]
               )

      assert_received {:removed, [^completed], [_]}
      assert shape_on_disk?(ctx.stack_id, completed)
      assert replica_identity(ctx) == "d"

      release.()
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
      refute shape_on_disk?(ctx.stack_id, completed)
      assert {_, %{^relation => ^replacement}, false, _} = tracker.barrier_state(ctx.pub_mgr_opts)
    end

    for manual? <- [false, true] do
      @tag :changed_membership_cleanup
      @tag manual_table_publishing?: manual?
      @tag connection_opt_overrides: [pool_size: 4]
      test "rebuilds shapes after a drop and re-add between checks (manual: #{manual?})", ctx do
        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        {completed, membership} = admit_completed_shape(ctx)
        relation = ctx.relation_with_oid

        Postgrex.query!(
          ctx.pool,
          "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items",
          []
        )

        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
        writer = start_writer(ctx)
        sweep_and_wait(ctx)
        assert_received {:removed, [^completed], [_]}

        shape = generate_shape(relation, @where_clause_2)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
        adder = admission_waiter(ctx, handle, shape)
        assert_drain_waiting(ctx, writer, adder.pid, handle)
        refute shape_on_disk?(ctx.stack_id, completed)
        finish_writer(writer)
        assert :ok = Task.await(adder)

        assert {_, %{^relation => replacement}, false, _} =
                 PublicationManager.RelationTracker.barrier_state(ctx.pub_mgr_opts)

        assert replacement != membership
      end
    end

    @tag :changed_membership_cleanup
    test "rediscovers the old shape after a restart just after recording a changed membership",
         ctx do
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      {completed, _} = admit_completed_shape(ctx)
      assert shape_on_disk?(ctx.stack_id, completed)
      tracker = PublicationManager.RelationTracker
      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      relation = ctx.relation_with_oid
      parent = self()
      checks = :atomics.new(1, [])

      Repatch.patch(tracker, :owe_barriers, [mode: :shared], fn relations, opts ->
        result = Repatch.real(PublicationManager.RelationTracker.owe_barriers(relations, opts))

        if MapSet.member?(relations, relation) and :atomics.add_get(checks, 1, 1) == 1 do
          send(parent, {:barrier_recorded, self(), result})
          assert_receive :continue_after_recording_barrier
        end

        result
      end)

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      sweep(ctx)

      assert_receive {:barrier_recorded, pid, {owed, blocked}}
      assert MapSet.member?(owed, elem(relation, 0))
      assert MapSet.size(blocked) == 0
      assert ShapeStatus.has_shape_handle?(ctx.stack_id, completed)

      old_tracker = GenServer.whereis(tracker.name(ctx.stack_id))
      :ok = GenServer.stop(old_tracker)
      wait_for_restart(tracker.name(ctx.stack_id), old_tracker)
      :ok = tracker.wait_for_restore(ctx.stack_id)
      restart_configurator(ctx, fn ^pid -> Process.exit(pid, :kill) end)

      _ = :sys.get_state(configurator)
      assert_received {:removed, [^completed], [_]}
      refute shape_on_disk?(ctx.stack_id, completed)
    end

    @tag :promotion_membership_gap
    @tag database_timeline: ["1", "1"]
    @tag connection_opt_overrides: [pool_size: 4]
    test "rebuilds a shape kept across promotion if its publication membership changed", ctx do
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      {completed, _} = admit_completed_shape(ctx)
      :ok = stop_supervised(PublicationManager.name(ctx.stack_id))

      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

      # The connection manager keeps the routing state and catalog on a continuous promotion.
      %{publication_manager: {_, opts}} =
        with_publication_manager(
          %{ctx | database_timeline: ["1", "2"]}
          |> Map.put(:manual_table_publishing?, true)
        )

      ctx = %{ctx | pub_mgr_opts: opts}
      Repatch.allow(self(), PublicationManager.Configurator.name(ctx.stack_id))
      {adder, handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_2)
      assert_drain_waiting(ctx, writer, adder.pid, handle)
      refute shape_on_disk?(ctx.stack_id, completed)
      finish_writer(writer)
      assert :ok = Task.await(adder)
    end

    @tag :owned_update_pass
    @tag configurator_pool_size: 2
    test "finishes other repairs and processes a queued cast before replanning a mismatch", ctx do
      :atomics.put(ctx.owned, 1, 1)
      tracker = PublicationManager.RelationTracker
      configurator = PublicationManager.Configurator
      pid = GenServer.whereis(configurator.name(ctx.stack_id))
      server = ctx.pub_mgr_opts[:server]
      other = {"public", "serial_ids"}

      [first, second] =
        Enum.sort([{lookup_relation_oid(ctx.pool, other), other}, ctx.relation_with_oid])

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items, serial_ids",
        []
      )

      hold_publication_checks(ctx)

      parent = self()

      Repatch.patch(configurator, :handle_cast, [mode: :shared], fn
        {:update_publication, {0, _}} = cast, state ->
          send(parent, :queued_cast_processed)
          Repatch.real(PublicationManager.Configurator.handle_cast(cast, state))

        cast, state ->
          Repatch.real(PublicationManager.Configurator.handle_cast(cast, state))
      end)

      Repatch.patch(Configuration, :published_table_membership!, [mode: :shared], fn conn,
                                                                                     pub,
                                                                                     relation,
                                                                                     expected ->
        result =
          Repatch.real(Configuration.published_table_membership!(conn, pub, relation, expected))

        if relation == second, do: send(parent, :second_repair_verified)
        result
      end)

      :ok = :sys.suspend(pid)

      [first_request, second_request] =
        for relation <- [first, second] do
          shape = generate_shape(relation, nil)
          {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

          :gen_server.send_request(
            server,
            {:add_shape, handle, tracker.publication_filter(shape)}
          )
        end

      _ = tracker.fetch_current_filters!(ctx.pub_mgr_opts)
      :ok = :sys.resume(pid)

      assert_receive {:publication_checked, 1, ^pid, actions}
      assert actions.to_configure_replica_identity == MapSet.new([first, second])
      table = Utils.relation_to_sql(elem(first, 1))

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE #{table}",
        []
      )

      GenServer.cast(pid, {:update_publication, {0, MapSet.new()}})
      send(pid, :continue_publication_check)

      for expected <- [:second_repair_verified, :queued_cast_processed, :recheck] do
        assert_receive event
                       when event in [:second_repair_verified, :queued_cast_processed] or
                              (is_tuple(event) and elem(event, 0) == :publication_checked)

        case expected do
          :recheck -> assert {:publication_checked, 2, ^pid, _} = event
          _ -> assert event == expected
        end
      end

      timeout = ExUnit.configuration()[:assert_receive_timeout]
      assert {:reply, :ok} = :gen_server.receive_response(second_request, timeout)

      assert {:reply, {:error, %SnapshotError{type: :publication_admission_pending}}} =
               :gen_server.receive_response(first_request, timeout)

      send(pid, :continue_publication_check)
    end

    @tag :verification_backoff
    test "coalesces and caps mismatch retries and resets after a verified pass", ctx do
      :atomics.put(ctx.owned, 1, 1)
      _ = admit_completed_shape(ctx)
      configurator = GenServer.whereis(PublicationManager.Configurator.name(ctx.stack_id))
      mismatch = :atomics.new(1, [])
      :atomics.put(mismatch, 1, 1)

      Repatch.patch(Configuration, :published_table_membership!, [mode: :shared], fn conn,
                                                                                     pub,
                                                                                     filter,
                                                                                     expected ->
        if :atomics.get(mismatch, 1) == 1 do
          {:error, :publication_changed}
        else
          Repatch.real(Configuration.published_table_membership!(conn, pub, filter, expected))
        end
      end)

      session = :trace.session_create(:publication_recheck, self(), [])
      on_exit(fn -> :trace.session_destroy(session) end)
      callback = {PublicationManager.Configurator, :handle_continue, 2}
      pattern = [{[{:perform_relation_actions, [], :_, true}, :_], [], [{:return_trace}]}]
      :trace.function(session, callback, pattern, [:local])
      :trace.process(session, configurator, true, [:call])

      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      sweep(ctx)

      ref =
        Enum.reduce([100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000], nil, fn expected, ref ->
          if expected > 800 do
            Process.cancel_timer(ref)
            send(configurator, {:timeout, ref, :recheck_publication})
          end

          assert_receive {:trace, ^configurator, :call,
                          {_, :handle_continue, [{:perform_relation_actions, [], _, true}, state]}}

          assert Map.get(state, :recheck_delay) == expected
          assert_receive {:trace, ^configurator, :return_from, ^callback, {:noreply, next}}
          assert %{recheck_ref: ref, recheck_delay: updated} = next
          assert is_reference(ref) and updated == min(expected * 2, 5_000)
          ref
        end)

      # A sweep can consume the retry before its timer fires. A stale timer message is ignored.
      :atomics.put(mismatch, 1, 0)
      %{checked_filter_revision: revision} = sweep_and_wait(ctx)
      send(configurator, {:timeout, ref, :recheck_publication})

      assert %{recheck_ref: nil, recheck_delay: 100, checked_filter_revision: ^revision} =
               :sys.get_state(configurator)

      assert replica_identity(ctx) == "f"

      :atomics.put(mismatch, 1, 1)
      Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
      sweep(ctx)

      assert_receive {:trace, ^configurator, :call,
                      {_, :handle_continue, [{:perform_relation_actions, [], _, true}, state]}}

      assert %{recheck_delay: 100} = state

      assert_receive {:trace, ^configurator, :return_from, ^callback,
                      {:noreply, %{recheck_ref: new_ref}}}

      assert new_ref != ref
    end

    for catalog_check <- [:status, :membership] do
      @tag :verification_catalog_error
      test "preserves verification backoff through a transient #{catalog_check} error", ctx do
        :atomics.put(ctx.owned, 1, 1)
        _ = admit_completed_shape(ctx)
        configurator = PublicationManager.Configurator.name(ctx.stack_id)
        fault = :atomics.new(1, [])
        :atomics.put(fault, 1, 1)

        Repatch.patch(Configuration, :published_table_membership!, [mode: :shared], fn conn,
                                                                                       pub,
                                                                                       filter,
                                                                                       expected ->
          if :atomics.get(fault, 1) == 1,
            do: {:error, :publication_changed},
            else:
              Repatch.real(Configuration.published_table_membership!(conn, pub, filter, expected))
        end)

        case unquote(catalog_check) do
          :status ->
            Repatch.patch(
              Configuration,
              :check_publication_status!,
              [mode: :shared, force: true],
              fn pool, pub ->
                if :atomics.get(fault, 1) == 2, do: raise(DBConnection.ConnectionError)
                Repatch.real(Configuration.check_publication_status!(pool, pub))
              end
            )

          :membership ->
            Repatch.patch(
              Configuration,
              :determine_publication_relation_actions!,
              [mode: :shared],
              fn pool, pub, filters ->
                if :atomics.get(fault, 1) == 2, do: raise(DBConnection.ConnectionError)

                Repatch.real(
                  Configuration.determine_publication_relation_actions!(pool, pub, filters)
                )
              end
            )
        end

        :sys.replace_state(configurator, &%{&1 | recheck_delay: 1_600})
        Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
        assert %{recheck_ref: first, recheck_delay: 3_200} = sweep_and_wait(ctx)
        assert is_reference(first)
        Process.cancel_timer(first)

        :atomics.put(fault, 1, 2)
        assert %{recheck_ref: retry, recheck_delay: 5_000} = sweep_and_wait(ctx)
        assert is_reference(retry) and retry != first

        Process.cancel_timer(retry)
        pid = GenServer.whereis(configurator)
        send(pid, {:timeout, retry, :recheck_publication})
        assert %{recheck_ref: next, recheck_delay: 5_000} = :sys.get_state(pid)
        assert is_reference(next) and next != retry

        :atomics.put(fault, 1, 0)
        Process.cancel_timer(next)
        send(pid, {:timeout, next, :recheck_publication})
        assert %{recheck_ref: nil, recheck_delay: 100} = :sys.get_state(pid)
        assert replica_identity(ctx) == "f"
      end
    end

    for {code, sqlstate} <- [
          query_canceled: "57014",
          lock_not_available: "55P03",
          deadlock_detected: "40P01"
        ] do
      @tag owned_repair_transient: sqlstate
      @tag configurator_pool_size: 2
      test "revokes readiness after #{code} in repair verification",
           %{owned_repair_transient: sqlstate} = ctx do
        :atomics.put(ctx.owned, 1, 1)
        {completed, membership} = admit_completed_shape(ctx)
        relation = ctx.relation_with_oid
        {oid, _} = relation
        tracker = PublicationManager.RelationTracker
        server = ctx.pub_mgr_opts[:server]

        Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY DEFAULT", [])
        attempts = :atomics.new(1, [])
        hold_publication_checks(ctx)

        Repatch.patch(
          Configuration,
          :published_table_membership!,
          [mode: :shared],
          fn conn, publication, relation, expected ->
            result =
              Repatch.real(
                Configuration.published_table_membership!(conn, publication, relation, expected)
              )

            if :atomics.add_get(attempts, 1, 1) == 1 do
              assert {:ok, _membership} = result

              raise Postgrex.Error,
                postgres: %{code: sqlstate, severity: "ERROR", message: "verification failed"}
            end

            result
          end
        )

        sweep(ctx)
        assert_receive {:publication_checked, 1, configurator, actions}
        assert MapSet.member?(actions.to_configure_replica_identity, relation)
        assert actions.memberships[relation] == membership
        send(configurator, :continue_publication_check)

        _ = :sys.get_state(configurator)
        _ = tracker.barrier_state(ctx.pub_mgr_opts)
        assert replica_identity(ctx) == "d"
        refute_received {:remove_shapes_for_relations, _}
        assert shape_on_disk?(ctx.stack_id, completed)

        shape = generate_shape(relation, @where_clause_2)
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

        request =
          :gen_server.send_request(
            server,
            {:add_shape, handle, tracker.publication_filter(shape)}
          )

        _ = tracker.barrier_state(ctx.pub_mgr_opts)
        assert %{waiters: %{^oid => [{_, ^handle}]}} = :sys.get_state(server)
        assert_receive {:publication_checked, 2, ^configurator, _}

        send(configurator, :continue_publication_check)

        assert {:reply, :ok} =
                 :gen_server.receive_response(
                   request,
                   ExUnit.configuration()[:assert_receive_timeout]
                 )

        assert ShapeStatus.has_shape_handle?(ctx.stack_id, completed)
        assert replica_identity(ctx) == "f"
      end
    end

    @tag :owned_add_lock_timeout
    @tag connection_opt_overrides: [pool_size: 4]
    test "retries an owned addition when its writer lock times out", ctx do
      :atomics.put(ctx.owned, 1, 1)
      writer = start_writer(ctx)

      Repatch.patch(
        Configuration,
        :add_table_to_publication,
        [mode: :shared],
        fn conn, publication, relation ->
          Postgrex.query!(conn, "SET LOCAL lock_timeout = '100ms'", [])
          Repatch.real(Configuration.add_table_to_publication(conn, publication, relation))
        end
      )

      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      :sys.get_state(ctx.pub_mgr_opts[:server])
      refute_received {:remove_shapes_for_relations, _}
      assert [] == fetch_pub_tables(ctx)

      finish_writer(writer)
      assert :ok = Task.await(admission_waiter(ctx, handle, shape))
    end

    @tag :barrier_recovery
    @tag :restored_waiter_registration
    @tag connection_opt_overrides: [pool_size: 4]
    test "keeps a failed drain owed across ownership transfer and tracker restart", ctx do
      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn _, _, _, _ ->
          {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
        end
      )

      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert_raise SnapshotError, fn ->
        PublicationManager.add_shape(ctx.stack_id, handle, shape)
      end

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared, force: true],
        fn pool, publication, filter, expected ->
          Repatch.real(
            Configuration.wait_for_in_flight_writers_on_published_table(
              pool,
              publication,
              filter,
              expected
            )
          )
        end
      )

      :atomics.put(ctx.owned, 1, 1)
      tracker = PublicationManager.RelationTracker.name(ctx.stack_id)
      old_tracker = GenServer.whereis(tracker)
      GenServer.stop(tracker)
      wait_for_restart(tracker, old_tracker)

      shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      adder =
        Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
          receive(do: (:register_waiter -> :ok))
          filter = PublicationManager.RelationTracker.publication_filter(shape)
          GenServer.call(tracker, {:add_shape, handle, filter}, :infinity)
        end)

      checker =
        Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
          assert_receive :check_waiter
          assert_drain_waiting(ctx, writer, adder.pid, handle)
        end)

      checker_pid = checker.pid
      :erlang.trace(checker_pid, true, [:receive, {:tracer, self()}])
      send(checker_pid, :check_waiter)

      # The restored drain can already be blocked when the new caller starts registering.
      assert_receive {:trace, ^checker_pid, :receive, {_, {:ok, %{waiters: waiters}}}}
      assert waiters == %{}
      :erlang.trace(checker_pid, false, [:receive])
      send(adder.pid, :register_waiter)

      assert Task.await(checker)
      finish_writer(writer)
      assert :ok = Task.await(adder)
    end

    @tag :barrier_recovery
    test "forgets an owed barrier when its OID leaves the database", ctx do
      oid = elem(ctx.relation_with_oid, 0)
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert_raise Electric.DbConfigurationError, fn ->
        PublicationManager.add_shape(ctx.stack_id, handle, shape)
      end

      assert MapSet.member?(
               PublicationManager.RelationTracker.barriers_owed(ctx.pub_mgr_opts),
               oid
             )

      Postgrex.query!(ctx.pool, "DROP TABLE items", [])
      sweep(ctx)

      assert wait_until(fn ->
               not MapSet.member?(
                 PublicationManager.RelationTracker.barriers_owed(ctx.pub_mgr_opts),
                 oid
               )
             end)
    end

    @tag :own_drop
    @tag connection_opt_overrides: [pool_size: 3]
    test "checks again after Electric drops the previously drained table", ctx do
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
      assert :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)

      :atomics.put(ctx.owned, 1, 1)
      :ok = ShapeStatus.remove_shape(ctx.stack_id, handle)
      :ok = PublicationManager.remove_shape(ctx.stack_id, handle)
      assert wait_until(fn -> fetch_pub_tables(ctx) == [] end)

      oid = elem(ctx.relation_with_oid, 0)

      assert MapSet.member?(
               PublicationManager.RelationTracker.barriers_owed(ctx.pub_mgr_opts),
               oid
             )

      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      assert_served_after_writer(ctx, writer, @where_clause_2)
    end

    @tag connection_opt_overrides: [pool_size: 3]
    test "owes a drain for an absence observed under another owner", ctx do
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert_raise Electric.DbConfigurationError, fn ->
        PublicationManager.add_shape(ctx.stack_id, handle, shape)
      end

      :atomics.put(ctx.owned, 1, 1)
      writer = start_writer(ctx)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      assert_served_after_writer(ctx, writer, @where_clause_2)
    end

    @tag :owned_add_lock_timeout
    test "an external drain satisfies a previously recorded obligation once", ctx do
      :atomics.put(ctx.owned, 1, 1)

      Repatch.patch(Configuration, :add_table_to_publication, [mode: :shared], fn _, _, _ ->
        {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
      end)

      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
      {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

      assert {:error, %SnapshotError{type: :publication_admission_pending}} =
               Task.await(admission_waiter(ctx, handle, shape))

      drains = :atomics.new(1, [])

      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          if :atomics.add_get(drains, 1, 1) == 1 do
            Repatch.real(
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected
              )
            )
          else
            {:error, Electric.DbConfigurationError.table_lock_timeout("public.items")}
          end
        end
      )

      :atomics.put(ctx.owned, 1, 0)
      Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
      assert :ok = PublicationManager.add_shape(ctx.stack_id, handle, shape)
      assert :atomics.get(drains, 1) == 1
    end

    @tag connection_opt_overrides: [pool_size: 4]
    test "the first pass after a configurator restart admits an unblocked table independently",
         ctx do
      configurator = PublicationManager.Configurator.name(ctx.stack_id)
      :ok = :sys.suspend(configurator)
      Postgrex.query!(ctx.pool, "ALTER TABLE serial_ids REPLICA IDENTITY FULL", [])
      writer = start_writer(ctx)

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items, serial_ids",
        []
      )

      other_table = {"public", "serial_ids"}
      other_relation = {lookup_relation_oid(ctx.pool, other_table), other_table}
      {blocked, handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_1)
      {unblocked, _} = add_shape_async(ctx, other_relation, nil)
      tracker = PublicationManager.RelationTracker.name(ctx.stack_id)
      assert wait_until(fn -> map_size(:sys.get_state(tracker).waiters) == 2 end)

      restart_configurator(ctx, &Process.exit(&1, :kill))
      assert_drain_waiting(ctx, writer, blocked.pid, handle)
      assert :ok = Task.await(unblocked)
      assert drain_waiting?(ctx, writer)

      finish_writer(writer)
      assert :ok = Task.await(blocked)
    end

    @tag connection_opt_overrides: [pool_size: 4]
    test "a pending drain leaves later requests for another table free to proceed", ctx do
      Repatch.patch(
        Configuration,
        :wait_for_in_flight_writers_on_published_table,
        [mode: :shared],
        fn pool, publication, filter, expected ->
          Configuration.wait_for_in_flight_writers_on_published_table(
            pool,
            publication,
            filter,
            expected,
            10_000
          )
        end
      )

      Postgrex.query!(ctx.pool, "ALTER TABLE serial_ids REPLICA IDENTITY FULL", [])
      writer = start_writer(ctx)

      Postgrex.query!(
        ctx.pool,
        "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items, serial_ids",
        []
      )

      {blocked, handle} = add_shape_async(ctx, ctx.relation_with_oid, @where_clause_1)
      assert_drain_waiting(ctx, writer, blocked.pid, handle)

      other_table = {"public", "serial_ids"}
      other_relation = {lookup_relation_oid(ctx.pool, other_table), other_table}
      {unblocked, _} = add_shape_async(ctx, other_relation, nil)
      assert {:ok, :ok} = Task.yield(unblocked)
      assert drain_waiting?(ctx, writer)

      finish_writer(writer)
      assert :ok = Task.await(blocked)
    end
  end

  describe "publication admission of a restored shape" do
    setup [
      :with_pure_file_storage,
      :with_registry,
      :with_inspector,
      :with_lsn_tracker,
      :with_shape_cleaner
    ]

    for {test_name, drain_timeout} <- [
          {"keeps the completed snapshot when the admission call expires", 6_500},
          {"keeps the writer alive when a drain times out with a flush pending", 3_000}
        ] do
      @tag drain_timeout: drain_timeout
      @tag connection_opt_overrides: [
             pool_size: 4,
             parameters: [lock_timeout: "#{drain_timeout}ms"]
           ]
      test test_name, ctx do
        Repatch.patch(Configuration, :check_publication_status!, [mode: :shared], fn pool, name ->
          status = Repatch.real(Configuration.check_publication_status!(pool, name))
          %{status | can_alter_publication?: false}
        end)

        parent = self()

        # Keep the collector's flush obligation pending until after admission reports its error.
        activate_mocks_for_descendant_procs(Consumer)

        Repatch.patch(ShapeLogCollector, :notify_flushed, [mode: :shared], fn _, handle, offset ->
          send(parent, {:flush_ready, handle, offset})
          :ok
        end)

        Repatch.patch(
          Configuration,
          :wait_for_in_flight_writers_on_published_table,
          [mode: :shared],
          fn pool, publication, filter, expected ->
            result =
              Configuration.wait_for_in_flight_writers_on_published_table(
                pool,
                publication,
                filter,
                expected,
                10_000
              )

            send(parent, {:drain_result, result})
            result
          end
        )

        :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))
        Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
        Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])

        shape = generate_shape(ctx.relation_with_oid, nil, ["id", "value"])
        {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
        storage = Storage.for_shape(handle, ctx.storage)
        row = ~s({"value":{"id":"stored"}})
        disk_writer = Storage.init_writer!(storage, shape)
        :ok = Storage.set_pg_snapshot(%{xmin: 1, xmax: 2, xip_list: []}, storage)
        :ok = Electric.Shapes.mark_snapshot_started(storage, ctx.stack_id, handle)
        :ok = Electric.Shapes.make_new_snapshot!([row], storage, ctx.stack_id, handle)
        Storage.terminate(disk_writer)
        :ok = WriteBuffer.flush_sync(ctx.stack_id)

        with_shape_log_collector(ctx)
        collector = ShapeLogCollector.name(ctx.stack_id)
        :sys.get_state(collector)
        with_shape_cache(ctx)
        writer = start_writer(ctx)

        assert {:ok, consumer} = ShapeCache.start_consumer_for_handle(handle, ctx.stack_id)

        waiter =
          Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
            Consumer.await_snapshot_start(ctx.stack_id, handle)
          end)

        snapshotter = Consumer.Snapshotter.name(ctx.stack_id, handle)
        assert wait_until(fn -> GenServer.whereis(snapshotter) end)
        assert_drain_waiting(ctx, writer, GenServer.whereis(snapshotter), handle)

        fragment =
          complete_txn_fragment(10, 100, [
            %Changes.NewRecord{
              relation: ctx.relation,
              record: %{"id" => "new", "value" => "pending"},
              log_offset: LogOffset.new(100, 0)
            }
          ])

        assert :ok = ShapeLogCollector.handle_event(fragment, ctx.stack_id)
        assert FlushTracker.tracked?(:sys.get_state(collector).flush_tracker, handle)

        # The error comes once the snapshotter's own 5 s admission call times out, and that call
        # started before this await: the await's default 5 s leaves only the gap between the two.
        assert {:error, %SnapshotError{} = error} = Task.await(waiter, 15_000)

        if ctx.drain_timeout > 5_000 do
          assert error.type == :publication_admission_pending
          assert error.message == "Snapshot timed out while waiting for a table lock"
        end

        assert SnapshotError.retryable?(error)
        assert %Consumer.State{} = :sys.get_state(consumer)
        assert FlushTracker.tracked?(:sys.get_state(collector).flush_tracker, handle)
        assert ShapeDb.snapshot_complete?(ctx.stack_id, handle)

        assert_receive {:drain_result,
                        {:error, %Electric.DbConfigurationError{type: :table_lock_timeout}}}

        finish_writer(writer)

        assert wait_until(fn ->
                 case Consumer.await_snapshot_start(ctx.stack_id, handle) do
                   :started ->
                     true

                   {:error, error} ->
                     assert SnapshotError.retryable?(error)
                     false
                 end
               end)

        assert ^consumer = Consumer.whereis(ctx.stack_id, handle)

        assert_receive {:flush_ready, ^handle, offset}
        Repatch.real(ShapeLogCollector.notify_flushed(ctx.stack_id, handle, offset))

        assert wait_until(fn ->
                 not FlushTracker.tracked?(:sys.get_state(collector).flush_tracker, handle)
               end)

        assert [^row] =
                 LogOffset.before_all()
                 |> Storage.get_log_stream(LogOffset.last_before_real_offsets(), storage)
                 |> Enum.to_list()
      end
    end
  end

  # A shape on the table waits for the writer, and is served once it commits.
  defp assert_served_after_writer(ctx, writer, where_clause) do
    {adder, handle} = add_shape_async(ctx, ctx.relation_with_oid, where_clause)
    assert_drain_waiting(ctx, writer, adder.pid, handle)

    finish_writer(writer)
    assert :ok = Task.await(adder)
  end

  defp add_shape_async(ctx, relation, where_clause) do
    shape = generate_shape(relation, where_clause)
    {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

    task =
      Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
        PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
      end)

    {task, shape_handle}
  end

  defp admission_waiter(ctx, handle, shape) do
    Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
      filter = PublicationManager.RelationTracker.publication_filter(shape)
      GenServer.call(ctx.pub_mgr_opts[:server], {:add_shape, handle, filter}, :infinity)
    end)
  end

  # The drain the pass that confirms a removal starts reports to the tracker after that pass ends.
  # An admission asked before then starts a pass of its own, which owes the removal of the shape
  # the removal stub of the setup leaves tracked once more, and that stub reads the catalog inline
  # in the configurator, where a test's read patch can hold it.
  defp await_table_committed(ctx) do
    {oid, _relation} = ctx.relation_with_oid
    tracker = PublicationManager.RelationTracker.name(ctx.stack_id)

    assert wait_until(fn ->
             MapSet.member?(:sys.get_state(tracker).committed_relation_filters, oid)
           end)
  end

  defp admit_completed_shape(ctx) do
    relation = ctx.relation_with_oid
    tracker = PublicationManager.RelationTracker
    shape = generate_shape(relation, @where_clause_1)
    {:ok, handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    assert :ok = Task.await(admission_waiter(ctx, handle, shape))
    :ok = ShapeStatus.mark_snapshot_complete(ctx.stack_id, handle)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    state = tracker.barrier_state(ctx.pub_mgr_opts)
    assert %{^relation => membership} = elem(state, 1)
    assert elem(state, 2) == false
    {handle, membership}
  end

  # The pass waits in its catalog read, which holds a connection of the configurator's pool: a test
  # that changes the catalog meanwhile gives the configurator a pool of its own
  # (`configurator_pool_size`).
  defp hold_publication_checks(ctx, opts \\ []) do
    parent = self()
    checks = :atomics.new(1, [])

    Repatch.patch(
      Configuration,
      :determine_publication_relation_actions!,
      [mode: :shared],
      fn pool, publication, filters ->
        check =
          if MapSet.member?(filters, ctx.relation_with_oid),
            do: :atomics.add_get(checks, 1, 1)

        if check && check == opts[:before] do
          send(parent, {:checking_publication, check, self()})
          assert_receive :continue_publication_check
        end

        actions =
          Repatch.real(
            Configuration.determine_publication_relation_actions!(pool, publication, filters)
          )

        if check && check <= 2 do
          send(parent, {:publication_checked, check, self(), actions})
          assert_receive :continue_publication_check
        end

        actions
      end
    )
  end

  def encode_barrier_record(record, fault, parent) do
    case Agent.get(fault, & &1) do
      :ok ->
        Jason.encode(record)

      :file_error ->
        send(parent, :barrier_write_failed)
        raise File.Error, reason: :eacces, action: "sync directory", path: "."

      :enospc ->
        send(parent, :barrier_write_failed)
        {:error, :enospc}
    end
  end

  defp assert_drain_waiting(ctx, writer, caller, handle, mode \\ "ShareLock")
       when is_pid(caller) do
    assert wait_until(fn -> drain_waiting?(ctx, writer, mode) end),
           "expected a #{mode} waiting on the writer"

    tracker = PublicationManager.RelationTracker.name(ctx.stack_id)
    oid = elem(ctx.relation_with_oid, 0)

    assert wait_until(fn ->
             Enum.any?(Map.get(:sys.get_state(tracker).waiters, oid, []), fn
               {{^caller, _tag}, ^handle} -> true
               _ -> false
             end)
           end),
           "expected the shape's admission call to still be waiting"

    assert drain_waiting?(ctx, writer, mode)
  end

  defp drain_waiting?(ctx, writer, mode \\ "ShareLock") do
    oid = elem(ctx.relation_with_oid, 0)

    %{rows: [[waiting?]]} =
      Postgrex.query!(
        ctx.pool,
        """
        SELECT EXISTS (
          SELECT 1 FROM pg_locks
          WHERE relation = $1 AND ($3::text IS NULL OR mode = $3) AND NOT granted
            AND $2 = ANY(pg_blocking_pids(pid))
        )
        """,
        [oid, writer.backend, mode]
      )

    waiting?
  end

  # A transaction that holds the lock a write takes on the table and waits for `:commit`. It takes
  # no transaction id, which would hold back the creation of a replication slot by any other test
  # on the server for as long as the writer is kept open.
  defp start_writer(ctx) do
    test_pid = self()

    writer =
      Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
        Postgrex.transaction(ctx.pool, fn conn ->
          Postgrex.query!(conn, "LOCK TABLE items IN ROW EXCLUSIVE MODE", [])

          %{rows: [[backend]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])
          send(test_pid, {:written, backend})
          receive(do: (:commit -> :ok))
        end)
      end)

    assert_receive {:written, backend}
    %{task: writer, backend: backend}
  end

  # Writers that each hold the table in one window after another until `stop` is set, each starting
  # a `window / count` after the one before. Their windows keep to that schedule even when a lock
  # queued ahead of a writer holds it back, so once all of them hold the table one always has
  # `window - window / count` or more to run: a lock that waits less never gets the table. Returns
  # once each writer holds the table.
  defp overlapping_writers(ctx, stop, window, count) do
    start = System.monotonic_time(:millisecond)

    writers =
      for index <- 0..(count - 1) do
        overlapping_writer(ctx, stop, start + div(window * index, count), window, index)
      end

    for index <- 0..(count - 1), do: assert_receive({:writer_holding, ^index}, window + 5_000)
    writers
  end

  defp overlapping_writer(ctx, stop, start, window, index) do
    test_pid = self()

    Task.Supervisor.async_nolink(ctx.task_supervisor, fn ->
      start
      |> Stream.iterate(&(&1 + window))
      |> Enum.find(fn opening ->
        sleep_until(opening)

        # The lock a write takes, with no transaction id: a write's id would hold back the
        # creation of a replication slot by any other test on the server until it ends.
        Postgrex.transaction(ctx.pool, fn conn ->
          Postgrex.query!(conn, "LOCK TABLE items IN ROW EXCLUSIVE MODE", [])

          if opening == start, do: send(test_pid, {:writer_holding, index})
          sleep_until(opening + window)
        end)

        :atomics.get(stop, 1) == 1
      end)

      :ok
    end)
  end

  # The removal of the shape is on disk and its time has run out, the table is out of the
  # publication, and every read of the catalog from now on waits until the test sends it `:answer`,
  # reporting itself as it asks for a connection and once it has one. The function returned waits
  # for the read a pass leaves running and returns it.
  defp leave_catalog_read_running(ctx, shape_handle) do
    test_pid = self()
    release = block_catalog_writes(ctx)
    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
    sweep(ctx)
    assert_receive {:removed, [^shape_handle], []}
    expire_removal(ctx, shape_handle)
    release.()
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    refute shape_on_disk?(ctx.stack_id, shape_handle)

    Repatch.patch(ShapeDb.Connection, :checkout!, [mode: :shared], fn stack_id,
                                                                      label,
                                                                      query,
                                                                      timeout ->
      query =
        if label == :handle_exists? do
          send(test_pid, {:catalog_read_started, self()})

          fn conn ->
            send(test_pid, {:catalog_read, self()})
            receive(do: (:answer -> query.(conn)))
          end
        else
          query
        end

      Repatch.real(ShapeDb.Connection.checkout!(stack_id, label, query, timeout))
    end)

    fn ->
      assert_receive {:catalog_read, reader}
      assert_received {:catalog_read_started, ^reader}
      reader
    end
  end

  # Reports the wait of each try of a drain on a pool of one, in order.
  defp report_drain_tries do
    test_pid = self()

    Repatch.patch(
      Configuration,
      :wait_for_in_flight_writers_on_published_table,
      [mode: :shared],
      fn pool, publication, filter, expected, lock_timeout ->
        send(test_pid, {:drain_try, lock_timeout})

        Repatch.real(
          Configuration.wait_for_in_flight_writers_on_published_table(
            pool,
            publication,
            filter,
            expected,
            lock_timeout
          )
        )
      end
    )
  end

  defp drain_tries do
    {:messages, messages} = Process.info(self(), :messages)
    for {:drain_try, lock_timeout} <- messages, do: lock_timeout
  end

  # The wait and the membership of the first drain try on a membership other than the given one.
  defp first_drain_try_off(membership) do
    receive do
      {:drain_try, _lock_timeout, ^membership} -> first_drain_try_off(membership)
      {:drain_try, lock_timeout, other} -> {lock_timeout, other}
    after
      ExUnit.configuration()[:assert_receive_timeout] ->
        flunk("no drain try on another membership")
    end
  end

  defp sleep_until(time) do
    Process.sleep(max(time - System.monotonic_time(:millisecond), 0))
  end

  defp finish_writer(%{task: writer}) do
    send(writer.pid, :commit)
    assert {:ok, :ok} = Task.await(writer)
  end

  defp shape_on_disk?(stack_id, handle) do
    ShapeDb.Connection.checkout!(
      stack_id,
      :handle_exists?,
      &ShapeDb.Query.handle_exists?(&1, handle)
    )
  end

  # The operator has put the table in the publication; the shape's catalog entry is on disk.
  defp add_shape_in_manual_mode(ctx, where_clause) do
    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} ADD TABLE items", [])
    Postgrex.query!(ctx.pool, "ALTER TABLE items REPLICA IDENTITY FULL", [])
    shape = generate_shape(ctx.relation_with_oid, where_clause)
    {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    shape_handle
  end

  # As if the removal had started longer ago than the time it has to reach the disk.
  defp expire_removal(ctx, shape_handle) do
    expired = System.monotonic_time(:millisecond) - 1

    state =
      :sys.replace_state(PublicationManager.Configurator.name(ctx.stack_id), fn state ->
        update_in(state.removals_to_confirm[shape_handle], &%{&1 | deadline: expired})
      end)

    assert %{deadline: ^expired} = state.removals_to_confirm[shape_handle]
  end

  defp removals_owed(ctx),
    do: PublicationManager.RelationTracker.removals_owed(stack_id: ctx.stack_id)

  # The removals the stored barrier record still owes.
  defp persisted_removals_owed(ctx) do
    prefix = "#{ctx.stack_id}:publication_barriers:"

    for {key, record} <- PersistentKV.Memory.contents(ctx.persistent_kv),
        String.starts_with?(key, prefix),
        [_timeline, _owed, _served, _recheck?, _cleanup, removals] <- [stored_term(record)],
        [handle, _oids] <- removals,
        into: MapSet.new(),
        do: handle
  end

  # The record as the tracker wrote it, through the JSON encoder when a test installs one.
  defp stored_term(record) when is_binary(record), do: Jason.decode!(record)
  defp stored_term(record), do: record

  # The write buffer reaches the disk only once the returned function is called.
  defp block_catalog_writes(ctx) do
    blocked = :atomics.new(1, [])
    :ok = :atomics.put(blocked, 1, 1)
    :ok = WriteBuffer.pause_flush(ctx.stack_id)

    Repatch.patch(WriteBuffer, :flush_sync, [mode: :shared], fn stack_id, timeout ->
      if :atomics.get(blocked, 1) == 1,
        do: :ok,
        else: Repatch.real(WriteBuffer.flush_sync(stack_id, timeout))
    end)

    fn ->
      :ok = :atomics.put(blocked, 1, 0)
      :ok = WriteBuffer.resume_flush(ctx.stack_id)
    end
  end

  # The shapes on the table from before are still being removed when a pass finds it out, and that
  # pass has ended, holding the table.
  defp drop_table_with_removal_running(ctx) do
    test_pid = self()

    Repatch.patch(
      Electric.ShapeCache.ShapeCleaner,
      :start_removing_shapes,
      [mode: :shared, force: true],
      fn stack_id, handles, _reason ->
        for handle <- handles, do: ShapeStatus.remove_shape(stack_id, handle)
        removal = spawn(fn -> receive(do: (:stopped -> :ok)) end)
        send(test_pid, {:removing, handles, removal})
        {:ok, completed_removal(removal)}
      end
    )

    shape_handle = add_shape_and_publish(ctx)
    Postgrex.query!(ctx.pool, "ALTER PUBLICATION #{ctx.publication_name} DROP TABLE items", [])
    sweep(ctx)
    assert_receive {:removing, [^shape_handle], removal}
    _ = :sys.get_state(PublicationManager.Configurator.name(ctx.stack_id))
    assert_pub_tables(ctx, [])
    removal
  end

  defp assert_retries_until_removed(ctx, removal, while_held \\ fn -> :ok end) do
    shape = generate_shape(ctx.relation_with_oid, @where_clause_2)
    {:ok, waiting} = ShapeStatus.add_shape(ctx.stack_id, shape)

    error =
      assert_raise Electric.SnapshotError, fn ->
        PublicationManager.add_shape(ctx.stack_id, waiting, shape)
      end

    assert Electric.SnapshotError.retryable?(error)
    assert Process.alive?(removal)
    refute_received {:adding, _}
    while_held.()

    send(removal, :stopped)
    assert :ok = add_shape_once_admitted(ctx, waiting, shape)
  end

  # A removal that ran out of time in an earlier pass gets one recheck interval in the next, which
  # a slow runner can spend on the catalog flush and read alone: that pass holds the table again,
  # with a retryable error. A consumer waits that out and asks again, and so does this.
  defp add_shape_once_admitted(ctx, handle, shape),
    do: add_shape_until(ctx, handle, shape, System.monotonic_time(:millisecond) + 10_000)

  defp add_shape_until(ctx, handle, shape, deadline) do
    PublicationManager.add_shape(ctx.stack_id, handle, shape)
  rescue
    error in Electric.SnapshotError ->
      if Electric.SnapshotError.retryable?(error) and
           System.monotonic_time(:millisecond) < deadline do
        Process.sleep(50)
        add_shape_until(ctx, handle, shape, deadline)
      else
        reraise error, __STACKTRACE__
      end
  end

  defp replica_identity(ctx) do
    %{rows: [[identity]]} =
      Postgrex.query!(
        ctx.pool,
        "SELECT relreplident::text FROM pg_class WHERE oid = $1",
        [elem(ctx.relation_with_oid, 0)]
      )

    identity
  end

  # The replacement's first pass waits on the tracker until the test has allowed it its patches.
  defp restart_configurator(ctx, stop \\ &GenServer.stop/1) do
    tracker = GenServer.whereis(PublicationManager.RelationTracker.name(ctx.stack_id))
    configurator = PublicationManager.Configurator.name(ctx.stack_id)
    old_pid = GenServer.whereis(configurator)
    ref = Process.monitor(old_pid)
    :ok = :sys.suspend(tracker)
    stop.(old_pid)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, _}
    Repatch.allow(self(), wait_for_restart(configurator, old_pid))
    :ok = :sys.resume(tracker)
  end

  describe "component restarts" do
    test "handles relation tracker restart", ctx do
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)

      # Add the shape to ShapeStatus first - this is what allows restoration after restart
      {:ok, shape_handle} = Electric.ShapeCache.ShapeStatus.add_shape(ctx.stack_id, shape)

      # Add the shape to the publication manager
      assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
      assert_pub_tables(ctx, [ctx.relation])

      # Stop the RelationTracker - supervisor will restart it
      relation_tracker_name = PublicationManager.RelationTracker.name(ctx.stack_id)
      old_pid = GenServer.whereis(relation_tracker_name)
      ref = Process.monitor(old_pid)
      GenServer.stop(relation_tracker_name)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, _}

      # Wait for the supervisor to restart and re-register the RelationTracker
      # before issuing calls to it. Otherwise the via-Registry lookup made by
      # remove_shape/2 below can race with re-registration and fail with
      # :no_process.
      new_pid = wait_for_restart(relation_tracker_name, old_pid)
      assert new_pid != old_pid

      # Wait for the supervisor to restart and re-register the RelationTracker,
      # then for it to finish restoring filters from ShapeStatus.
      #
      # The assert_pub_tables below is NOT a sufficient barrier on its own: the
      # relation is already in the publication and is not removed during the
      # restart, so the assertion passes immediately - potentially before the
      # restarted process has re-registered. Calling remove_shape in that window
      # races with a "no process" exit, which is the flakiness this guards
      # against.
      assert wait_until(fn -> is_pid(GenServer.whereis(relation_tracker_name)) end)

      :ok =
        PublicationManager.wait_for_restore(ctx.stack_id,
          timeout: ExUnit.configuration()[:assert_receive_timeout]
        )

      # After restart, the publication manager should repopulate from ShapeStatus.
      # The publication should still have the relation.
      assert_pub_tables(ctx, [ctx.relation])

      # Verify we can still remove the shape after the restart
      PublicationManager.remove_shape(ctx.stack_id, shape_handle)
      assert_pub_tables(ctx, [])
    end

    # A shape stored before its table was renamed is found stale by the name it was stored with.
    # A boot submits one name per table, the first the relation tracker meets, and its read of
    # the stored shapes met them in handle order.
    test "a boot finds a renamed table stale while shapes on its new name remain", ctx do
      {oid, relation} = ctx.relation_with_oid

      store_shape_as!(
        ctx,
        "1-before-the-rename",
        generate_shape({oid, relation}, @where_clause_1)
      )

      Postgrex.query!(ctx.db_conn, "ALTER TABLE items RENAME TO renamed_items", [])

      for n <- 2..9 do
        where_clause = %Expr{query: "id = '#{n}'", used_refs: %{["id"] => :text}}
        shape = generate_shape({oid, {"public", "renamed_items"}}, where_clause)
        store_shape_as!(ctx, "#{n}-since-the-rename", shape)
      end

      # as a boot finds them: the relation tracker waits for the collector's read of them
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeStatus.initialize(ctx.stack_id)
      relation_tracker_name = PublicationManager.RelationTracker.name(ctx.stack_id)
      old_pid = GenServer.whereis(relation_tracker_name)
      GenServer.stop(relation_tracker_name)
      Repatch.allow(self(), wait_for_restart(relation_tracker_name, old_pid))

      routing_state =
        ShapeStatus.reduce_shapes(ctx.stack_id, ShapeStatus.new_routing_state(), fn
          {handle, shape}, routing_state ->
            ShapeStatus.add_to_routing_state(routing_state, handle, shape)
        end)

      :ok = ShapeStatus.restore_routing_state(ctx.stack_id, routing_state)

      assert_receive {:remove_shapes_for_relations, [{^oid, ^relation}]}
    end

    @tag update_debounce_timeout: 100
    test "handles configurator restart", ctx do
      notify_alter_queries()
      shape = generate_shape(ctx.relation_with_oid, @where_clause_1)

      configurator_name = PublicationManager.Configurator.name(ctx.stack_id)

      assert :ok = PublicationManager.add_shape(ctx.stack_id, @shape_handle_1, shape)
      assert_pub_tables(ctx, [ctx.relation])
      run_async(fn -> PublicationManager.remove_shape(ctx.stack_id, @shape_handle_1) end)
      GenServer.stop(configurator_name)
      assert_pub_tables(ctx, [])
    end
  end

  defp assert_pub_tables(
         ctx,
         expected_tables,
         timeout \\ ExUnit.configuration()[:assert_receive_timeout],
         start_time \\ :erlang.monotonic_time(:millisecond)
       ) do
    pub_tables = fetch_pub_tables(ctx)

    try do
      assert pub_tables == expected_tables
    rescue
      e in ExUnit.AssertionError ->
        current_time = :erlang.monotonic_time(:millisecond)

        if current_time - start_time < timeout do
          Process.sleep(10)
          assert_pub_tables(ctx, expected_tables, timeout, start_time)
        else
          reraise e, __STACKTRACE__
        end
    end
  end

  defp fetch_pub_tables(ctx), do: fetch_publication_tables(ctx.pool, ctx.publication_name)

  # The shape is the one the first addition is for, so it waits for it and stays.
  defp add_shape_and_publish(ctx) do
    shape = generate_shape(ctx.relation_with_oid, @where_clause_1)
    {:ok, shape_handle} = ShapeStatus.add_shape(ctx.stack_id, shape)

    assert :ok = PublicationManager.add_shape(ctx.stack_id, shape_handle, shape)
    assert_pub_tables(ctx, [ctx.relation])
    assert_received {:adding, _}
    refute_received {:removed, _, _}

    shape_handle
  end

  # Whether the configurator is in a pass that waits on a catalog read it started. The test helper
  # has Repatch recompile the configurator, which renames each function with a `REPATCH-` prefix.
  defp waiting_on_catalog_read?(configurator) do
    {:current_stacktrace, stacktrace} = Process.info(configurator, :current_stacktrace)
    configurator_module = PublicationManager.Configurator
    names = [:catalog_has_handle?, :"REPATCH-catalog_has_handle?"]

    Enum.any?(stacktrace, fn
      {^configurator_module, name, 3, _location} -> name in names
      _frame -> false
    end)
  end

  # Called from the configurator, where an assertion would only crash it.
  defp ended_process do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    receive(do: ({:DOWN, ^ref, :process, ^pid, _} -> pid))
  end

  defp completed_removal(pid) do
    completed = :atomics.new(1, [])
    :atomics.put(completed, 1, 1)
    {pid, completed}
  end

  defp received_insert_commit? do
    receive do
      :insert_committed_after_absent_read -> true
    after
      0 -> false
    end
  end

  # as the periodic sweep does
  defp sweep(ctx) do
    configure_revision(ctx, System.unique_integer([:positive, :monotonic]))
  end

  defp sweep_and_wait(ctx) do
    revision = System.unique_integer([:positive, :monotonic])
    configure_revision(ctx, revision)
    configurator = PublicationManager.Configurator.name(ctx.stack_id)

    assert wait_until(fn -> :sys.get_state(configurator).checked_filter_revision >= revision end)
    :sys.get_state(configurator)
  end

  defp configure_revision(ctx, revision) do
    PublicationManager.Configurator.configure_publication(
      ctx.stack_id,
      {revision, MapSet.new([ctx.relation_with_oid])}
    )
  end

  defp store_shape_as!(ctx, handle, shape) do
    Repatch.patch(Electric.Shapes.Shape, :generate_id, [force: true], fn shape ->
      {Electric.Shapes.Shape.hash(shape), handle}
    end)

    {:ok, ^handle} = ShapeStatus.add_shape(ctx.stack_id, shape)
    :ok = ShapeStatus.mark_snapshot_complete(ctx.stack_id, handle)
  end

  # Polls the process registry until `name` resolves to a live pid other than
  # `old_pid`, i.e. until the supervisor has restarted and re-registered it.
  defp wait_for_restart(
         name,
         old_pid,
         timeout \\ ExUnit.configuration()[:assert_receive_timeout],
         start_time \\ :erlang.monotonic_time(:millisecond)
       ) do
    case GenServer.whereis(name) do
      pid when is_pid(pid) and pid != old_pid ->
        pid

      _ ->
        if :erlang.monotonic_time(:millisecond) - start_time < timeout do
          Process.sleep(10)
          wait_for_restart(name, old_pid, timeout, start_time)
        else
          flunk("#{inspect(name)} was not restarted within #{timeout}ms")
        end
    end
  end

  defp notify_alter_queries(notify_pid \\ self()) do
    Repatch.patch(Postgrex, :query, [mode: :shared], fn conn, sql, params ->
      if String.starts_with?(sql, "ALTER TABLE") do
        send(notify_pid, {:alter_table, sql, params})
      end

      if String.starts_with?(sql, "ALTER PUBLICATION") do
        send(notify_pid, {:alter_publication, sql, params})
      end

      Repatch.real(Postgrex.query(conn, sql, params))
    end)
  end

  defp patch_queries_to_unprivileged() do
    Repatch.patch(Postgrex, :query!, [mode: :shared], fn conn, sql, params ->
      DBConnection.run(conn, fn conn ->
        Repatch.real(Postgrex.query(conn, "SET ROLE unprivileged", []))
        Repatch.real(Postgrex.query!(conn, sql, params))
      end)
    end)

    Repatch.patch(Postgrex, :query, [mode: :shared], fn conn, sql, params ->
      DBConnection.run(conn, fn conn ->
        Repatch.real(Postgrex.query(conn, "SET ROLE unprivileged", []))
        Repatch.real(Postgrex.query(conn, sql, params))
      end)
    end)
  end

  defp run_async(fun) do
    start_supervised!(
      Supervisor.child_spec(
        {Task, fun},
        id: make_ref()
      )
    )
  end
end
