defmodule Electric.Postgres.Inspector.EtsInspectorTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit
  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  alias Electric.PersistentKV
  alias Electric.Postgres.Inspector
  alias Electric.Postgres.Inspector.EtsInspector

  describe "load_relation_oid/2" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}
    setup :with_items_oid

    test "returns the relation id for a given relation", %{opts: opts} do
      assert {:ok, {oid, {"public", "items"}}} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert is_integer(oid)
    end

    test "caches the relation id for a given relation once accesses", %{opts: opts} do
      assert {:ok, {oid, {"public", "items"}}} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert is_integer(oid)

      Repatch.patch(Postgrex, :transaction, [mode: :shared], fn _, _ ->
        raise "should not be called again"
      end)

      Repatch.allow(self(), opts[:server])

      assert {:ok, {^oid, {"public", "items"}}} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)
    end

    test "returns a not found marker when the relation does not exist", %{opts: opts} do
      assert :table_not_found = EtsInspector.load_relation_oid({"public", "nonexistent"}, opts)
    end

    test "assumes passed-in relation respects casing", %{opts: opts} do
      assert :table_not_found = EtsInspector.load_relation_oid({"public", "ITEMS"}, opts)
    end

    test "forwards the DB errors without crashing the process", %{opts: opts} do
      Repatch.patch(Postgrex, :query, [mode: :shared], fn _, _, _ ->
        {:error, %DBConnection.ConnectionError{message: "expected error"}}
      end)

      Repatch.allow(self(), opts[:server])

      assert {:error, "expected error"} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)
    end
  end

  describe "load_relation_info/2" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}
    setup :with_items_oid

    test "returns relation info for a given relation", %{opts: opts, items_oid: items_oid} do
      assert {:ok,
              %{relation: {"public", "items"}, relation_id: ^items_oid, kind: :ordinary_table}} =
               EtsInspector.load_relation_info(items_oid, opts)
    end

    test "concurrent calls load value exactly once", %{
      opts: opts,
      items_oid: items_oid
    } do
      test_pid = self()

      Repatch.patch(Postgrex, :transaction, [mode: :shared], fn conn, fun ->
        send(test_pid, {:transaction, self()})
        Repatch.real(Postgrex.transaction(conn, fun))
      end)

      Repatch.allow(test_pid, opts[:server])

      task1 = Task.async(fn -> EtsInspector.load_relation_info(items_oid, opts) end)
      task2 = Task.async(fn -> EtsInspector.load_relation_info(items_oid, opts) end)

      assert {:ok,
              %{relation: {"public", "items"}, relation_id: ^items_oid, kind: :ordinary_table} =
                info} =
               Task.await(task1)

      assert {:ok, ^info} = Task.await(task2)

      # Non-parallel call should return value from cache
      assert {:ok, ^info} = EtsInspector.load_relation_info(items_oid, opts)

      server = GenServer.whereis(opts[:server])
      assert_received {:transaction, ^server}
      refute_received {:transaction, _}
    end

    test "returns a not found marker when the relation does not exist", %{opts: opts} do
      assert :table_not_found = EtsInspector.load_relation_info(1_234_567_890, opts)
    end

    test "forwards the DB errors without crashing the process", %{opts: opts} do
      Repatch.patch(Postgrex, :query, [mode: :shared], fn _, _, _ ->
        {:error, %DBConnection.ConnectionError{message: "expected error"}}
      end)

      Repatch.allow(self(), opts[:server])

      assert {:error, "expected error"} =
               EtsInspector.load_relation_info(1_234_567_890, opts)
    end

    @tag with_sql: [
           ~s|CREATE TABLE "just_normal_john" (a INT PRIMARY KEY)|
         ]
    test "returns blank children and parent for non-partitioned tables", %{
      opts: opts
    } do
      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "just_normal_john"}, opts)

      assert {:ok, %{relation: {"public", "just_normal_john"}, parent: nil, children: nil}} =
               EtsInspector.load_relation_info(oid, opts)
    end

    @tag with_sql: [
           ~s|CREATE SCHEMA other|,
           ~s|CREATE TABLE "partitioned_items" (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (b)|,
           ~s|CREATE TABLE "partitioned_items_100" PARTITION OF "partitioned_items" FOR VALUES FROM (0) TO (99)|,
           ~s|CREATE TABLE "partitioned_items_200" PARTITION OF "partitioned_items" FOR VALUES FROM (100) TO (199)|,
           ~s|CREATE TABLE other."partitioned_items_300" PARTITION OF "partitioned_items" FOR VALUES FROM (200) TO (299)|
         ]
    test "returns the partitioned table heirarchy", %{
      opts: opts
    } do
      partitions = [
        {"public", "partitioned_items_100"},
        {"public", "partitioned_items_200"},
        {"other", "partitioned_items_300"}
      ]

      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "partitioned_items"}, opts)

      assert {:ok,
              %{
                parent: nil,
                relation: {"public", "partitioned_items"},
                relation_id: _,
                kind: :partitioned_table,
                children: ^partitions
              }} = EtsInspector.load_relation_info(oid, opts)

      for relation <- partitions do
        assert {:ok, {oid, _}} =
                 EtsInspector.load_relation_oid(relation, opts)

        assert {:ok,
                %{
                  parent: {"public", "partitioned_items"},
                  relation: ^relation,
                  relation_id: ^oid,
                  kind: :ordinary_table,
                  children: nil
                }} = EtsInspector.load_relation_info(oid, opts)
      end
    end
  end

  describe "clean/2" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}
    setup :with_items_oid

    test "cleans up all information from ETS cache", %{
      inspector: {EtsInspector, opts},
      pg_inspector_table: pg_inspector_table
    } do
      assert :ets.tab2list(pg_inspector_table) == []

      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      refute :ets.tab2list(pg_inspector_table) == []
      assert EtsInspector.clean(oid, opts)
      assert :ets.tab2list(pg_inspector_table) == []
    end

    @tag with_sql: [
           ~s|CREATE TABLE "ITEMS" (a INT PRIMARY KEY)|
         ]
    test "cleans just the info for one relation", %{
      inspector: {EtsInspector, opts},
      pg_inspector_table: pg_inspector_table
    } do
      assert {:ok, {oid1, _}} = EtsInspector.load_relation_oid({"public", "items"}, opts)
      assert {:ok, {oid2, _}} = EtsInspector.load_relation_oid({"public", "ITEMS"}, opts)

      assert EtsInspector.clean(oid1, opts)

      assert :ets.lookup(pg_inspector_table, {:relation_to_oid, {"public", "items"}}) == []
      refute :ets.lookup(pg_inspector_table, {:relation_to_oid, {"public", "ITEMS"}}) == []

      assert :ets.lookup(pg_inspector_table, {:oid_info, oid1}) == []
      refute :ets.lookup(pg_inspector_table, {:oid_info, oid2}) == []
    end
  end

  describe "reset/1" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}
    setup :with_items_oid

    test "drops the entire ETS cache", %{
      opts: opts,
      pg_inspector_table: pg_inspector_table
    } do
      assert {:ok, {_oid, _}} = EtsInspector.load_relation_oid({"public", "items"}, opts)
      refute :ets.tab2list(pg_inspector_table) == []

      assert :ok = EtsInspector.reset(opts)
      assert :ets.tab2list(pg_inspector_table) == []
    end

    # A failed write left the old cache on disk for the next start to restore, and the caller,
    # told the reset worked, went on as if it had.
    @tag :tmp_dir
    @tag :capture_log
    test "reports a cleared cache it could not persist", %{tmp_dir: tmp_dir} = ctx do
      stop_supervised!(EtsInspector)
      kv = PersistentKV.Filesystem.new!(root: tmp_dir)
      # A directory where the cache's file goes fails its write, whoever runs the test.
      File.mkdir_p!(Path.join([tmp_dir, "#{ctx.stack_id}:ets_inspector_state", "taken"]))
      %{inspector: {EtsInspector, opts}} = with_inspector(%{ctx | persistent_kv: kv})

      assert {:error, _} = EtsInspector.reset(opts)
    end

    # The filesystem backend raises when the directory sync after the rename fails. The raise
    # ended the inspector, and the caller's call with it, where the caller retries a failure.
    @tag :tmp_dir
    @tag :capture_log
    test "reports a cleared cache whose directory sync fails, and goes on", ctx do
      stop_supervised!(EtsInspector)
      kv = PersistentKV.Filesystem.new!(root: ctx.tmp_dir)
      %{inspector: {EtsInspector, opts}} = with_inspector(%{ctx | persistent_kv: kv})
      server = GenServer.whereis(opts[:server])

      Repatch.patch(Electric.Utils, :sync_dir!, [mode: :shared], fn dir ->
        raise File.Error, reason: :eio, action: "sync directory", path: dir
      end)

      Repatch.allow(self(), server)

      assert {:error, %File.Error{reason: :eio}} = EtsInspector.reset(opts)
      assert GenServer.whereis(opts[:server]) == server
    end

    test "does not restore the cleared cache on a restart", %{opts: opts, db_conn: conn} = ctx do
      assert {:ok, {_oid, _}} = EtsInspector.load_relation_oid({"public", "items"}, opts)
      assert :ok = EtsInspector.reset(opts)

      stop_supervised!(EtsInspector)

      # Drop the table so that, if a stale cache were restored, the lookup would
      # succeed from cache rather than report the table as missing.
      Postgrex.query!(conn, "DROP TABLE items", [])

      %{inspector: {EtsInspector, opts}} = with_inspector(ctx)

      assert :table_not_found = EtsInspector.load_relation_oid({"public", "items"}, opts)
    end
  end

  describe "load_column_info/2" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}
    setup :with_items_oid

    test "returns column info for the table", %{opts: opts, items_oid: items_oid} do
      assert {:ok, [%{name: "id"}, %{name: "value"}]} =
               EtsInspector.load_column_info(items_oid, opts)
    end

    test "concurrent calls load value exactly once", %{
      opts: opts,
      items_oid: items_oid
    } do
      test_pid = self()

      Repatch.patch(Postgrex, :transaction, [mode: :shared], fn conn, fun ->
        send(test_pid, {:transaction, self()})
        Repatch.real(Postgrex.transaction(conn, fun))
      end)

      Repatch.allow(test_pid, opts[:server])

      task1 = Task.async(fn -> EtsInspector.load_column_info(items_oid, opts) end)
      task2 = Task.async(fn -> EtsInspector.load_column_info(items_oid, opts) end)

      assert {:ok, [%{name: "id"}, %{name: "value"}] = columns} = Task.await(task1)

      assert {:ok, ^columns} = Task.await(task2)

      # Non-parallel call should return value from cache
      assert {:ok, ^columns} = EtsInspector.load_column_info(items_oid, opts)

      server = GenServer.whereis(opts[:server])
      assert_received {:transaction, ^server}
      refute_received {:transaction, _}
    end

    test "returns a not found marker when the relation does not exist", %{opts: opts} do
      assert :table_not_found = EtsInspector.load_column_info(1_234_567_890, opts)
    end

    test "forwards the DB errors without crashing the process", %{opts: opts} do
      Repatch.patch(Postgrex, :query, [mode: :shared], fn _, _, _ ->
        {:error, %DBConnection.ConnectionError{message: "expected error"}}
      end)

      Repatch.allow(self(), opts[:server])

      assert {:error, "expected error"} =
               EtsInspector.load_column_info(1_234_567_890, opts)
    end

    @tag with_sql: [
           ~s|CREATE TABLE "partitioned_items" (a INT, b INT, c TEXT, PRIMARY KEY (a, b)) PARTITION BY RANGE (b)|
         ]
    test "can introspect partitioned tables", %{opts: opts} do
      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "partitioned_items"}, opts)

      assert {:ok, [%{name: "a"}, %{name: "b"}, %{name: "c"}]} =
               EtsInspector.load_column_info(oid, opts)
    end

    @tag with_sql: [
           ~s|CREATE TABLE "partitioned_items" (a INT, b INT, c TEXT, PRIMARY KEY (a, b)) PARTITION BY RANGE (b)|,
           ~s|CREATE TABLE "partitioned_items_100" PARTITION OF "partitioned_items" FOR VALUES FROM (0) TO (99)|
         ]
    test "can introspect partitions", %{opts: opts} do
      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "partitioned_items_100"}, opts)

      assert {:ok, [%{name: "a"}, %{name: "b"}, %{name: "c"}]} =
               EtsInspector.load_column_info(oid, opts)
    end

    @tag with_sql: [
           ~s|CREATE TYPE foo_enum AS ENUM ('a', 'b', 'c');|,
           ~s|CREATE TABLE "enum_table" (foo foo_enum)|
         ]
    test "can load enum types with type kind", %{opts: opts} do
      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "enum_table"}, opts)

      assert {:ok, [%{name: "foo", type_kind: :enum, type: "foo_enum"}]} =
               EtsInspector.load_column_info(oid, opts)
    end

    # An array column's own kind is 'b' -- the enum is the element -- so the element's kind
    # has to be carried separately or the enum-ness is lost on the way to the refs map.
    @tag with_sql: [
           ~s|CREATE TYPE bar_enum AS ENUM ('a', 'b', 'c');|,
           ~s|CREATE TABLE "enum_array_table" (foo bar_enum[])|
         ]
    test "keeps an array element's enum kind", %{opts: opts} do
      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "enum_array_table"}, opts)

      assert {:ok, [%{array_type: "bar_enum", array_element_kind: :enum} = column]} =
               EtsInspector.load_column_info(oid, opts)

      assert Inspector.columns_to_expr([column]) == %{["foo"] => {:array, {:enum, "bar_enum"}}}
    end

    @tag with_sql: [~s|CREATE TABLE "point_table" (p point)|]
    test "does not read a geometric type's element type as an array", %{opts: opts} do
      assert {:ok, {oid, _}} = EtsInspector.load_relation_oid({"public", "point_table"}, opts)

      assert {:ok, [%{type: "point", array_type: nil} = column]} =
               EtsInspector.load_column_info(oid, opts)

      assert Inspector.columns_to_expr([column]) == %{["p"] => :point}
    end

    @tag with_sql: [
           ~s|CREATE DOMAIN foo_domain AS text CHECK ( VALUE ~ 'test');|,
           ~s|CREATE TABLE "domain_table" (foo foo_domain)|
         ]
    test "can load domain types with type kind", %{opts: opts} do
      assert {:ok, {oid, _}} =
               EtsInspector.load_relation_oid({"public", "domain_table"}, opts)

      assert {:ok, [%{name: "foo", type_kind: :domain, type: "foo_domain"}]} =
               EtsInspector.load_column_info(oid, opts)
    end
  end

  describe "load_supported_features/1" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}

    setup ctx do
      %{pg_version: Support.TestUtils.fetch_pg_version(ctx.db_conn)}
    end

    test "returns supported features", %{opts: opts, pg_version: pg_version} do
      assert {:ok, features} = EtsInspector.load_supported_features(opts)

      if pg_version >= 180_000 do
        assert %{supports_generated_column_replication: true} == features
      else
        assert %{supports_generated_column_replication: false} == features
      end
    end

    test "concurrent calls load value exactly once", %{opts: opts} do
      test_pid = self()

      Repatch.patch(Postgrex, :query, [mode: :shared], fn conn, statement, params ->
        send(test_pid, {:query, self()})
        Repatch.real(Postgrex.query(conn, statement, params))
      end)

      Repatch.allow(test_pid, opts[:server])

      task1 = Task.async(fn -> EtsInspector.load_supported_features(opts) end)
      task2 = Task.async(fn -> EtsInspector.load_supported_features(opts) end)

      assert {:ok, %{supports_generated_column_replication: _val} = features} = Task.await(task1)

      assert {:ok, ^features} = Task.await(task2)

      # Non-parallel call should return value from cache
      assert {:ok, ^features} = EtsInspector.load_supported_features(opts)

      server = GenServer.whereis(opts[:server])
      assert_received {:query, ^server}
      refute_received {:query, _}
    end
  end

  describe "list_relations_with_stale_cache/1" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}

    test "returns nothing when there is no cache", %{opts: opts} do
      assert {:ok, []} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "doesn't return an unchanged relation", %{opts: opts} do
      assert {:ok, {_oid, _}} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:ok, []} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when a column is added", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN new_column TEXT", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when a column is dropped", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(conn, "ALTER TABLE items DROP COLUMN value", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when a column is renamed", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(conn, "ALTER TABLE items RENAME COLUMN value TO renamed_column", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when a column is changed", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(
        conn,
        "ALTER TABLE items ALTER COLUMN value TYPE INTEGER USING value::INTEGER",
        []
      )

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when table is dropped", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(conn, "DROP TABLE items", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when table is renamed", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(conn, "ALTER TABLE items RENAME TO renamed_items", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns a relation when table is recreated", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      Postgrex.query!(conn, "DROP TABLE items", [])
      Postgrex.query!(conn, "CREATE TABLE items (id INT PRIMARY KEY)", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "returns multiple relations if more than one is affected", %{opts: opts, db_conn: conn} do
      assert {:ok, oid_relation1} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:ok, oid_relation2} =
               EtsInspector.load_relation_oid({"public", "serial_ids"}, opts)

      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN new_column TEXT", [])
      Postgrex.query!(conn, "DROP TABLE serial_ids", [])

      assert {:ok, [^oid_relation1, ^oid_relation2]} =
               EtsInspector.list_relations_with_stale_cache(opts)
    end

    test "doesn't return a relation cached before the column info carried the collation",
         %{opts: opts} = ctx do
      assert {:ok, {oid, _}} = EtsInspector.load_relation_oid({"public", "items"}, opts)
      assert {:ok, columns} = EtsInspector.load_column_info(oid, opts)
      assert Enum.all?(columns, &Map.has_key?(&1, :deterministic_collation))

      opts = restart_with_cache_without_collation(ctx)

      assert {:ok, []} = EtsInspector.list_relations_with_stale_cache(opts)
      assert {:ok, ^columns} = EtsInspector.load_column_info(oid, opts)

      stop_supervised!(EtsInspector)
      %{inspector: {EtsInspector, opts}} = with_inspector(ctx)

      assert {:ok, ^columns} = EtsInspector.load_column_info(oid, opts)
    end

    test "returns a relation cached before the column info carried the collation when a column is added",
         %{opts: opts, db_conn: conn} = ctx do
      assert {:ok, oid_relation} = EtsInspector.load_relation_oid({"public", "items"}, opts)

      opts = restart_with_cache_without_collation(ctx)
      Postgrex.query!(conn, "ALTER TABLE items ADD COLUMN new_column TEXT", [])

      assert {:ok, [^oid_relation]} = EtsInspector.list_relations_with_stale_cache(opts)
    end
  end

  describe "persistance" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}

    test "loads back last seen state on a restart", %{opts: opts, db_conn: conn} = ctx do
      assert {:ok, {oid, _} = oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:ok, relation_info} = EtsInspector.load_relation_info(oid, opts)
      assert {:ok, columns} = EtsInspector.load_column_info(oid, opts)
      assert {:ok, features} = EtsInspector.load_supported_features(opts)
      stop_supervised!(EtsInspector)

      # Change the underlying relation to ensure we get a cached result -
      # there's another process responsible for cache invalidation on startup
      Postgrex.query!(conn, "DROP TABLE items", [])

      %{inspector: {EtsInspector, opts}} = with_inspector(ctx)

      assert {:ok, ^oid_relation} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:ok, ^relation_info} = EtsInspector.load_relation_info(oid, opts)
      assert {:ok, ^columns} = EtsInspector.load_column_info(oid, opts)
      assert {:ok, ^features} = EtsInspector.load_supported_features(opts)
    end

    test "returns supported features from persistence when DB is unavailable",
         %{opts: opts} = ctx do
      assert {:ok, features} = EtsInspector.load_supported_features(opts)
      stop_supervised!(EtsInspector)

      # Restart inspector without a DB pool — features should come from PersistentKV
      %{inspector: {EtsInspector, opts}} = with_inspector(ctx |> Map.put(:db_conn, :no_pool))

      assert {:ok, ^features} = EtsInspector.load_supported_features(opts)
    end

    test "doesn't load back last seen state on a restart if the storage format is old",
         %{
           opts: opts,
           persistent_kv: persistent_kv,
           db_conn: conn
         } = ctx do
      assert {:ok, {oid, relation}} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:ok, relation_info} = EtsInspector.load_relation_info(oid, opts)
      assert {:ok, columns} = EtsInspector.load_column_info(oid, opts)
      stop_supervised!(EtsInspector)
      # internal information about the persistence key, but that's for ease of testing
      persistence_key = "#{ctx.stack_id}:ets_inspector_state"

      # Prepare old data format, overwriting the existing state
      PersistentKV.set(
        persistent_kv,
        persistence_key,
        {
          # pg_info_table
          [
            {{"items", :table_to_relation}, relation_info},
            {{relation, :table_to_relation}, relation_info},
            {{relation, :columns}, columns}
          ],
          # pg_inspector_table
          [
            {{relation_info, :relation_to_table}, relation},
            {{relation_info, :relation_to_table}, "items"}
          ]
        }
      )

      # Change the underlying relation to ensure we get a cached result -
      # there's another process responsible for cache invalidation on startup
      Postgrex.query!(conn, "DROP TABLE items", [])

      %{inspector: {EtsInspector, opts}} = with_inspector(ctx)
      assert :table_not_found = EtsInspector.load_relation_oid({"public", "items"}, opts)
    end
  end

  describe "with complete lack of db pool" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_basic_tables, :with_sql_execute]

    setup ctx do
      %{inspector: {EtsInspector, opts}} = with_inspector(ctx |> Map.put(:db_conn, :no_pool))
      %{opts: opts}
    end

    test "returns error", %{opts: opts} do
      assert {:error, :connection_not_available} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:error, :connection_not_available} =
               EtsInspector.load_column_info(1234, opts)

      assert {:error, :connection_not_available} =
               EtsInspector.load_supported_features(opts)

      assert :error = EtsInspector.list_relations_with_stale_cache(opts)
    end
  end

  describe "with pool timeout" do
    setup {Support.DbSetup, :with_unique_db}
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_basic_tables]

    setup %{pooled_db_config: conn_opts} = ctx do
      conn_opts =
        conn_opts
        |> Keyword.merge(pool_size: 1, queue_target: 100, queue_interval: 500)
        |> Electric.Utils.deobfuscate_password()

      busy_pool =
        start_link_supervised!(Supervisor.child_spec({Postgrex, conn_opts}, id: :busy_pool))

      %{inspector: {EtsInspector, opts}} =
        inspector_ctx =
        with_inspector(Map.merge(ctx, %{db_conn: busy_pool}))

      Map.merge(inspector_ctx, %{opts: opts, busy_pool: busy_pool})
    end

    setup %{busy_pool: busy_pool} do
      test_pid = self()

      start_link_supervised!(
        Supervisor.child_spec(
          {Task,
           fn ->
             DBConnection.run(
               busy_pool,
               fn _conn ->
                 send(test_pid, :pool_busy)

                 receive do
                   _ -> :ok
                 end
               end,
               timeout: :infinity
             )
           end},
          restart: :temporary
        )
      )

      assert_receive :pool_busy
      :ok
    end

    test "returns error", %{opts: opts} do
      assert {:error, :connection_not_available} =
               EtsInspector.load_relation_oid({"public", "items"}, opts)

      assert {:error, :connection_not_available} =
               EtsInspector.load_column_info(1234, opts)

      assert :error = EtsInspector.list_relations_with_stale_cache(opts)
    end
  end

  describe "requests queued behind a lookup that leaves nothing in the cache" do
    setup :with_shared_db
    setup :in_transaction
    setup :with_stack_id_from_test
    setup [:with_persistent_kv, :with_inspector, :with_basic_tables, :with_sql_execute]
    setup %{inspector: {EtsInspector, opts}}, do: %{opts: opts}

    setup %{opts: opts} do
      test_pid = self()
      server = GenServer.whereis(opts[:server])

      Repatch.patch(Postgrex, :transaction, [mode: :shared], fn conn, fun ->
        send(test_pid, {:transaction, self()})

        case Process.get(:fail_lookups) do
          nil -> Repatch.real(Postgrex.transaction(conn, fun))
          error -> error
        end
      end)

      Repatch.allow(test_pid, server)

      %{server: server}
    end

    # Holds the inspector while `count` requests queue up, then lets it work through them.
    defp queue_requests(server, count, request) do
      :ok = :sys.suspend(server)
      tasks = for _ <- 1..count, do: Task.async(request)

      Support.TestUtils.wait_until(fn ->
        Process.info(server, :message_queue_len) == {:message_queue_len, count}
      end)

      :ok = :sys.resume(server)
      Enum.map(tasks, &Task.await/1)
    end

    defp lookups_by(server) do
      Stream.repeatedly(fn ->
        receive do
          {:transaction, ^server} -> :lookup
        after
          0 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> length()
    end

    test "share its answer for a missing table", %{opts: opts, server: server} do
      assert [:table_not_found, :table_not_found, :table_not_found, :table_not_found] =
               queue_requests(server, 4, fn ->
                 EtsInspector.load_relation_oid({"public", "nonexistent"}, opts)
               end)

      assert lookups_by(server) == 1
    end

    test "share its database error", %{opts: opts, server: server} do
      :sys.replace_state(server, fn state ->
        Process.put(:fail_lookups, {:error, :connection_not_available})
        state
      end)

      results =
        queue_requests(server, 4, fn -> EtsInspector.load_relation_info(1_234_567_890, opts) end)

      assert results == List.duplicate({:error, :connection_not_available}, 4)
      assert lookups_by(server) == 1
    end

    test "do not keep answers without bound while the queue never empties",
         %{opts: opts, server: server} do
      :sys.replace_state(server, fn state ->
        Process.put(:fail_lookups, {:error, :connection_not_available})
        state
      end)

      # 1,001 different missing relations, then the first one again. The inspector keeps at most
      # 1,000 answers, so the first one's is gone by then and that request asks again. The first
      # and the repeat are queued on their own, so they are first and last in the mailbox.
      :ok = :sys.suspend(server)

      tasks =
        for oids <- [[1], Enum.to_list(2..1_001), [1]], reduce: [] do
          tasks ->
            tasks =
              tasks ++
                for oid <- oids,
                    do: Task.async(fn -> EtsInspector.load_relation_info(oid, opts) end)

            Support.TestUtils.wait_until(fn ->
              Process.info(server, :message_queue_len) == {:message_queue_len, length(tasks)}
            end)

            tasks
        end

      :ok = :sys.resume(server)
      Enum.each(tasks, &Task.await/1)

      assert lookups_by(server) == 1_002
    end

    test "made after it ask the database again", %{opts: opts, server: server} do
      assert :table_not_found = EtsInspector.load_relation_oid({"public", "nonexistent"}, opts)
      assert :table_not_found = EtsInspector.load_relation_oid({"public", "nonexistent"}, opts)

      assert lookups_by(server) == 2
    end
  end

  defp restart_with_cache_without_collation(%{persistent_kv: kv, stack_id: stack_id} = ctx) do
    stop_supervised!(EtsInspector)
    key = "#{stack_id}:ets_inspector_state"
    {:ok, [version: 1, data: data]} = PersistentKV.get(kv, key)

    data =
      Enum.map(data, fn
        {{:oid_info, _} = info_key, info, columns} ->
          {info_key, info, Enum.map(columns, &Map.delete(&1, :deterministic_collation))}

        entry ->
          entry
      end)

    :ok = PersistentKV.set(kv, key, version: 1, data: data)
    %{inspector: {EtsInspector, opts}} = with_inspector(ctx)
    opts
  end

  defp with_items_oid(%{db_conn: conn}) do
    %{rows: [[oid]]} =
      Postgrex.query!(conn, "SELECT oid FROM pg_class WHERE relname = 'items'", [])

    %{items_oid: oid}
  end
end
