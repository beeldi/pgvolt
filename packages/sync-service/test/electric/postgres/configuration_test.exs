defmodule Electric.Postgres.ConfigurationTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  import Support.DbSetup
  import Support.TestUtils, only: [wait_until: 1]

  alias Electric.Postgres.Configuration
  alias Electric.Utils

  setup :with_unique_db
  setup :with_publication

  setup %{db_conn: conn} do
    Postgrex.query!(
      conn,
      """
      CREATE TABLE items (
        id UUID PRIMARY KEY,
        value TEXT NOT NULL,
        value_c VARCHAR(255)
      )
      """,
      []
    )

    :ok
  end

  describe "configure publication" do
    test "adds table to publication", %{pool: conn, publication_name: publication} do
      assert get_table_identity(conn, {"public", "items"}) == "d"
      assert list_tables_in_publication(conn, publication) == []
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}

      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)

      assert list_tables_in_publication(conn, publication) == [{"public", "items"}]

      # idempotent
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
    end

    @tag connection_opt_overrides: [pool_size: 2]
    test "adds table to publication only once in-flight writers have finished", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}
      test_pid = self()

      # A transaction that wrote to the table before it was added to the publication. Logical
      # decoding evaluates publication membership per change, so this write is never emitted by
      # the replication stream. It can only reach a shape via the initial snapshot, which is why
      # the addition must not commit until the writer has finished: a snapshot taken after that
      # point sees the write, while any later write is emitted by the stream.
      writer =
        Task.async(fn ->
          Postgrex.transaction(conn, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'pre-publication')",
              []
            )

            send(test_pid, :written)
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive :written

      adder =
        Task.async(fn ->
          Configuration.add_table_to_publication(conn, publication, oid_rel, :infinity)
        end)

      adder_ref = adder.ref
      refute_receive {^adder_ref, _}, 500

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)
      assert :ok = Task.await(adder)

      assert list_tables_in_publication(conn, publication) == [{"public", "items"}]
    end

    @tag connection_opt_overrides: [pool_size: 2]
    test "waits for in-flight writers when another session added the table first", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}
      test_pid = self()

      writer =
        Task.async(fn ->
          Postgrex.transaction(conn, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'pre-publication')",
              []
            )

            send(test_pid, :written)
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive :written

      # Added between Electric's check of the publication and its own addition, by a session that
      # waits for no writer: the write above reaches neither the stream nor a snapshot taken
      # before it commits.
      Postgrex.query!(conn, "ALTER PUBLICATION \"#{publication}\" ADD TABLE items", [])

      adder =
        Task.async(fn ->
          Configuration.add_table_to_publication(conn, publication, oid_rel, :infinity)
        end)

      adder_ref = adder.ref
      refute_receive {^adder_ref, _}, 500

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)
      assert :ok = Task.await(adder)
    end

    @tag connection_opt_overrides: [pool_size: 2, parameters: [lock_timeout: "100"]]
    test "fails to add table to publication if timing out on an in-flight writer", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}
      test_pid = self()

      # A server-side `lock_timeout` is used rather than the client-side transaction timeout, as
      # the latter tears the connection down at the deadline and races with the server's own
      # cancellation of the statement, making the resulting error type non-deterministic.
      writer =
        Task.async(fn ->
          Postgrex.transaction(conn, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'pre-publication')",
              []
            )

            send(test_pid, :written)
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive :written

      assert {:error, %Electric.DbConfigurationError{type: :table_lock_timeout}} =
               Configuration.add_table_to_publication(conn, publication, oid_rel)

      # the addition must have been rolled back along with the failed barrier
      assert list_tables_in_publication(conn, publication) == []

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)
    end

    @tag connection_opt_overrides: [pool_size: 3]
    test "drains in-flight writers before serving a table published out of band", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}
      test_pid = self()

      # Stand in for the manual publishing path: the table is already in the publication (there, the
      # operator added it by hand), so Electric's in-transaction barrier never ran. A writer open
      # across that addition must still be drained before Electric snapshots the table, or its
      # pre-addition rows reach neither the snapshot nor the replication stream.
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)

      assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)

      writer =
        Task.async(fn ->
          Postgrex.transaction(conn, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'pre-serving')",
              []
            )

            %{rows: [[backend]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])
            send(test_pid, {:written, backend})
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive {:written, backend}

      barrier =
        Task.async(fn ->
          Configuration.wait_for_in_flight_writers_on_published_table(
            conn,
            publication,
            oid_rel,
            expected,
            :infinity
          )
        end)

      assert wait_until(fn ->
               %{rows: [[blocked?]]} =
                 Postgrex.query!(
                   conn,
                   """
                   SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE relation = $1 AND mode = 'ShareLock' AND NOT granted
                       AND $2 = ANY(pg_blocking_pids(pid))
                   )
                   """,
                   [oid, backend]
                 )

               blocked?
             end)

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)
      assert {:ok, membership} = Task.await(barrier)
      assert is_integer(membership)

      assert list_tables_in_publication(conn, publication) == [{"public", "items"}]
    end

    @tag connection_opt_overrides: [pool_size: 2, parameters: [lock_timeout: "100"]]
    test "fails to serve a published table if timing out on an in-flight writer", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}
      test_pid = self()

      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)

      writer =
        Task.async(fn ->
          Postgrex.transaction(conn, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO items (id, value) VALUES (gen_random_uuid(), 'pre-serving')",
              []
            )

            send(test_pid, :written)
            receive(do: (:commit -> :ok))
          end)
        end)

      assert_receive :written

      assert {:error, %Electric.DbConfigurationError{type: :table_lock_timeout}} =
               Configuration.wait_for_in_flight_writers_on_published_table(
                 conn,
                 publication,
                 oid_rel,
                 expected
               )

      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)
    end

    # `LOCK` on a partitioned table takes a lock on each partition in turn, and each can wait: a
    # wait bounded lock by lock would let the drain run as long as all of them together.
    @tag connection_opt_overrides: [pool_size: 5]
    test "bounds the whole wait for the writers of a partitioned table", %{
      pool: conn,
      publication_name: publication
    } do
      Postgrex.query!(
        conn,
        """
        CREATE TABLE employees (id UUID NOT NULL, division_id INT NOT NULL)
        PARTITION BY LIST (division_id)
        """,
        []
      )

      for division <- 1..3 do
        Postgrex.query!(
          conn,
          "CREATE TABLE employees_#{division} PARTITION OF employees FOR VALUES IN (#{division})",
          []
        )
      end

      relation = {"public", "employees"}
      oid_rel = {get_table_oid(conn, relation), relation}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)
      test_pid = self()

      # A writer on each partition, each let go 200 ms after the drain starts waiting for its
      # partition: each lock of the drain waits 200 ms, and two of them together longer than 300.
      writers =
        Map.new(1..3, fn division ->
          writer =
            Task.async(fn ->
              # The lock a write takes, with no transaction id (`start_items_writer/1`).
              Postgrex.transaction(conn, fn conn ->
                Postgrex.query!(
                  conn,
                  "LOCK TABLE employees_#{division} IN ROW EXCLUSIVE MODE",
                  []
                )

                send(test_pid, {:written, division})
                receive(do: (:commit -> :ok))
              end)
            end)

          {"employees_#{division}", writer}
        end)

      for division <- 1..3, do: assert_receive({:written, ^division})

      # The settings the `LOCK` runs under, read on its connection just before it.
      Repatch.patch(Postgrex, :query, [mode: :shared], fn conn, statement, params, opts ->
        if is_binary(statement) and String.starts_with?(statement, "LOCK TABLE") do
          {:ok, %{rows: [settings]}} =
            Repatch.real(
              Postgrex.query(
                conn,
                "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
                [],
                []
              )
            )

          send(test_pid, {:lock_runs_under, settings})
        end

        Repatch.real(Postgrex.query(conn, statement, params, opts))
      end)

      drain =
        Task.async(fn ->
          Repatch.allow(test_pid, self())

          Configuration.wait_for_in_flight_writers_on_published_table(
            conn,
            publication,
            oid_rel,
            expected,
            300
          )
        end)

      released =
        Enum.reduce_while(1..3, MapSet.new(), fn _, released ->
          case partition_drain_waits_for(conn, drain, released) do
            nil ->
              {:halt, released}

            partition ->
              Process.sleep(200)
              send(writers[partition].pid, :commit)
              {:cont, MapSet.put(released, partition)}
          end
        end)

      assert MapSet.size(released) >= 2

      assert {:error, %Electric.DbConfigurationError{type: :table_lock_timeout}} =
               Task.await(drain)

      assert_received {:lock_runs_under, ["300ms", "0"]}

      for {partition, writer} <- writers do
        unless MapSet.member?(released, partition), do: send(writer.pid, :commit)
        assert {:ok, :ok} = Task.await(writer)
      end
    end

    # The connection's `statement_timeout` is stricter than the drain's wait: it bounds the wait.
    @tag connection_opt_overrides: [pool_size: 2, parameters: [statement_timeout: "200"]]
    test "keeps a stricter statement_timeout of the connection for the wait", %{
      pool: conn,
      publication_name: publication
    } do
      oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)
      writer = start_items_writer(conn)
      started = System.monotonic_time(:millisecond)

      assert {:error, %Electric.DbConfigurationError{type: :table_lock_timeout}} =
               Configuration.wait_for_in_flight_writers_on_published_table(
                 conn,
                 publication,
                 oid_rel,
                 expected,
                 5_000
               )

      assert System.monotonic_time(:millisecond) - started < 2_000
      send(writer.pid, :commit)
      assert {:ok, :ok} = Task.await(writer)
    end

    # The session set its own `statement_timeout`, looser than the drain's wait: the membership read
    # after the `LOCK` runs under its own second, or under the session's value where that is stricter,
    # not under the value the session started with.
    for {session_timeout, read_timeout} <- [{"7000", "1s"}, {"500", "500ms"}] do
      @tag connection_opt_overrides: [pool_size: 1]
      test "reads the membership in a second or the session's #{session_timeout} ms if stricter",
           %{
             pool: conn,
             publication_name: publication
           } do
        oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
        assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
        expected = expected_drain(conn, publication, oid_rel)
        assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)
        Postgrex.query!(conn, "SET statement_timeout = #{unquote(session_timeout)}", [])
        test_pid = self()

        Repatch.patch(Configuration, :published_table_membership!, fn conn,
                                                                      publication,
                                                                      filter,
                                                                      expected ->
          %{rows: [[setting]]} = Postgrex.query!(conn, "SHOW statement_timeout", [])
          send(test_pid, {:membership_read_under, setting})

          Repatch.real(
            Configuration.published_table_membership!(conn, publication, filter, expected)
          )
        end)

        assert {:ok, _membership} =
                 Configuration.wait_for_in_flight_writers_on_published_table(
                   conn,
                   publication,
                   oid_rel,
                   expected,
                   100
                 )

        assert_received {:membership_read_under, unquote(read_timeout)}
      end
    end

    # On a slow link every statement of the drain's transaction is slow, not only its wait for the
    # lock: each round trip has time of its own, outside the lock's wait and the read's. Six of
    # them at 1.3 s outlast the two budgets and one action timeout together.
    @tag connection_opt_overrides: [pool_size: 1]
    test "drains over a link whose round trips outlast the lock's wait and the read's", %{
      pool: conn,
      publication_name: publication
    } do
      oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)

      Repatch.patch(Postgrex, :query, fn conn, statement, params, opts ->
        Process.sleep(1_300)
        Repatch.real(Postgrex.query(conn, statement, params, opts))
      end)

      Repatch.patch(Postgrex, :query!, fn conn, statement, params, opts ->
        Process.sleep(1_300)
        Repatch.real(Postgrex.query!(conn, statement, params, opts))
      end)

      assert {:ok, _membership} =
               Configuration.wait_for_in_flight_writers_on_published_table(
                 conn,
                 publication,
                 oid_rel,
                 expected,
                 100
               )
    end

    # A role or a database can make REPEATABLE READ the transactions' default. The first statement
    # of the drain then took the snapshot its membership read used, and a table taken out of the
    # publication after it and before the `LOCK` was granted was read as still in it.
    @tag connection_opt_overrides: [pool_size: 1]
    test "reads the membership the drain's lock found under a repeatable read default", %{
      pool: conn,
      db_config: db_config,
      publication_name: publication
    } do
      oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)
      Postgrex.query!(conn, "SET default_transaction_isolation = 'repeatable read'", [])
      other = start_other_connection(db_config)
      drop = "ALTER PUBLICATION #{Utils.quote_name(publication)} DROP TABLE items"

      Repatch.patch(Postgrex, :query, fn conn, statement, params, opts ->
        if statement =~ "IN SHARE MODE", do: Postgrex.query!(other, drop, [])
        Repatch.real(Postgrex.query(conn, statement, params, opts))
      end)

      assert {:error, :publication_changed} =
               Configuration.wait_for_in_flight_writers_on_published_table(
                 conn,
                 publication,
                 oid_rel,
                 expected,
                 1_000
               )
    end

    # The same default under an update of a table Electric publishes, whose first statement takes
    # the snapshot before it waits for its lock.
    @tag connection_opt_overrides: [pool_size: 1]
    test "verifies an update's membership as committed after it under a repeatable read default",
         %{pool: conn, db_config: db_config, publication_name: publication} do
      oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)
      Postgrex.query!(conn, "SET default_transaction_isolation = 'repeatable read'", [])
      other = start_other_connection(db_config)
      drop = "ALTER PUBLICATION #{Utils.quote_name(publication)} DROP TABLE items"

      update_table = fn conn ->
        Postgrex.query!(conn, "SELECT 1", [])
        Postgrex.query!(other, drop, [])
        Configuration.set_table_replica_identity_full(conn, oid_rel)
      end

      assert {:error, :publication_changed} =
               Configuration.update_and_verify_table_membership(
                 conn,
                 publication,
                 oid_rel,
                 expected,
                 update_table
               )
    end

    # An update that fits the action timeout it always had, on a link where the membership read
    # after it is slow too.
    @tag connection_opt_overrides: [pool_size: 1]
    test "verifies an update that took most of the action timeout", %{
      pool: conn,
      publication_name: publication
    } do
      oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)

      Repatch.patch(Postgrex, :query!, fn conn, statement, params, opts ->
        if statement =~ "FROM pg_publication_rel pr", do: Process.sleep(1_500)
        Repatch.real(Postgrex.query!(conn, statement, params, opts))
      end)

      update_table = fn conn ->
        Process.sleep(4_000)
        Configuration.set_table_replica_identity_full(conn, oid_rel)
      end

      assert {:ok, _membership} =
               Configuration.update_and_verify_table_membership(
                 conn,
                 publication,
                 oid_rel,
                 expected,
                 update_table
               )

      assert get_table_identity(conn, {"public", "items"}) == "f"
    end

    # The minimal manual-publishing role may read the table and not lock it in SHARE mode. Its
    # denial is the `LOCK`'s own, which a caller tells from a privilege error before it.
    @tag connection_opt_overrides: [pool_size: 1]
    test "reports a LOCK the role may not take as denied", %{
      pool: conn,
      publication_name: publication
    } do
      oid_rel = {get_table_oid(conn, {"public", "items"}), {"public", "items"}}
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)
      expected = expected_drain(conn, publication, oid_rel)
      role = "reader_#{System.unique_integer([:positive])}"
      Postgrex.query!(conn, "CREATE ROLE #{role} NOLOGIN", [])
      Postgrex.query!(conn, "GRANT SELECT ON items TO #{role}", [])
      Postgrex.query!(conn, "SET ROLE #{role}", [])

      assert {:error, {:lock_denied, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}}} =
               Configuration.wait_for_in_flight_writers_on_published_table(
                 conn,
                 publication,
                 oid_rel,
                 expected,
                 100
               )

      Postgrex.query!(conn, "RESET ROLE", [])
      Postgrex.query!(conn, "REVOKE SELECT ON items FROM #{role}", [])
      Postgrex.query!(conn, "DROP ROLE #{role}", [])
    end

    for change <- [:membership, :publication, :operations, :generated_columns] do
      @tag :drain_catalog
      test "rejects a drain after the expected #{change} changes", %{
        pool: conn,
        publication_name: publication
      } do
        relation = {"public", "items"}
        filter = {get_table_oid(conn, relation), relation}
        :ok = Configuration.configure_table_for_replication(conn, publication, filter)
        {membership, status} = expected_drain(conn, publication, filter)
        publication_sql = Utils.quote_name(publication)

        expected =
          case unquote(change) do
            :membership ->
              Postgrex.query!(conn, "ALTER PUBLICATION #{publication_sql} DROP TABLE items", [])
              Postgrex.query!(conn, "ALTER PUBLICATION #{publication_sql} ADD TABLE items", [])
              {membership, status}

            :publication ->
              # A status check on the old publication and a membership check on its replacement.
              Postgrex.query!(conn, "DROP PUBLICATION #{publication_sql}", [])
              Postgrex.query!(conn, "CREATE PUBLICATION #{publication_sql} FOR TABLE items", [])
              {new_membership, _} = expected_drain(conn, publication, filter)
              {new_membership, status}

            :operations ->
              Postgrex.query!(
                conn,
                "ALTER PUBLICATION #{publication_sql} SET (publish = 'insert')",
                []
              )

              {membership, status}

            :generated_columns ->
              {membership,
               %{status | publishes_generated_columns?: not status.publishes_generated_columns?}}
          end

        assert {:error, :publication_changed} =
                 Configuration.wait_for_in_flight_writers_on_published_table(
                   conn,
                   publication,
                   filter,
                   expected
                 )
      end
    end

    test "sets REPLICA IDENTITY on the table", %{pool: conn} do
      assert get_table_identity(conn, {"public", "items"}) == "d"
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}

      assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)

      assert get_table_identity(conn, {"public", "items"}) == "f"

      # idempotent
      assert :ok = Configuration.set_table_replica_identity_full(conn, oid_rel)
    end

    test "configures table for replication in publication and replica identity", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}

      assert get_table_identity(conn, {"public", "items"}) == "d"
      assert list_tables_in_publication(conn, publication) == []

      assert :ok = Configuration.configure_table_for_replication(conn, publication, oid_rel)

      assert get_table_identity(conn, {"public", "items"}) == "f"
      assert list_tables_in_publication(conn, publication) == [{"public", "items"}]
    end

    test "drops table from publication", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}

      assert list_tables_in_publication(conn, publication) == []

      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel)

      assert list_tables_in_publication(conn, publication) == [{"public", "items"}]

      assert :ok = Configuration.drop_table_from_publication(conn, publication, oid_rel)

      assert list_tables_in_publication(conn, publication) == []

      # idempotent
      assert :ok = Configuration.drop_table_from_publication(conn, publication, oid_rel)
    end

    test "fails relation configuration when publication doesn't exist", %{pool: conn} do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}

      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} =
               Configuration.add_table_to_publication(conn, "nonexistent", oid_rel)

      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} =
               Configuration.configure_table_for_replication(conn, "nonexistent", oid_rel)

      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} =
               Configuration.drop_table_from_publication(conn, "nonexistent", oid_rel)
    end

    @tag connection_opt_overrides: [pool_size: 2]
    test "fails relation configuration if timing out on lock", %{
      pool: conn,
      publication_name: publication
    } do
      oid = get_table_oid(conn, {"public", "items"})
      oid_rel = {oid, {"public", "items"}}
      test_pid = self()

      # Need to unlink the connection pool as the test prevents any backoffs and retries for
      # connections that fail, and timeouts and cancellations will lead to the pool dying
      # and timing issues can cause the test to fail because of that
      Process.unlink(conn)

      start_supervised!(
        Supervisor.child_spec(
          {Task,
           fn ->
             Postgrex.transaction(
               conn,
               fn conn ->
                 Postgrex.query!(conn, "LOCK TABLE public.items IN ACCESS EXCLUSIVE MODE", [])
                 send(test_pid, :table_locked)
                 receive(do: (_ -> :ok))
               end
             )
           end},
          restart: :temporary
        )
      )

      assert_receive :table_locked

      assert {:error,
              %DBConnection.ConnectionError{
                message: "connection is closed because of an error, disconnect or timeout"
              }} =
               Configuration.add_table_to_publication(conn, publication, oid_rel, 50)
    end
  end

  @empty_set MapSet.new()
  describe "determine_publication_relation_actions!/3" do
    setup %{db_conn: conn} do
      Postgrex.query!(
        conn,
        "CREATE TABLE other_table_1 (id UUID PRIMARY KEY, value TEXT NOT NULL)",
        []
      )

      Postgrex.query!(
        conn,
        "CREATE TABLE other_table_2 (id UUID PRIMARY KEY, value TEXT NOT NULL)",
        []
      )

      Postgrex.query!(
        conn,
        "CREATE TABLE other_table_3 (id UUID PRIMARY KEY, value TEXT NOT NULL)",
        []
      )

      :ok
    end

    test "determines necessary actions to configure provided relations", %{
      pool: conn,
      publication_name: publication
    } do
      oid1 = get_table_oid(conn, {"public", "items"})
      oid2 = get_table_oid(conn, {"public", "other_table_1"})
      oid3 = get_table_oid(conn, {"public", "other_table_2"})
      oid4 = get_table_oid(conn, {"public", "other_table_3"})
      oid_rel1 = {oid1, {"public", "items"}}
      oid_rel2 = {oid2, {"public", "other_table_1"}}
      oid_rel3 = {oid3, {"public", "other_table_2"}}
      oid_rel4 = {oid4, {"public", "other_table_3"}}
      oid_rel5 = {999_999, {"public", "nonexistent_table"}}

      assert :ok = Configuration.configure_table_for_replication(conn, publication, oid_rel1)
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel2)
      assert :ok = Configuration.add_table_to_publication(conn, publication, oid_rel4)

      assert %{
               to_preserve: MapSet.new([oid_rel1]),
               to_add: MapSet.new([oid_rel3]),
               to_configure_replica_identity: MapSet.new([oid_rel2, oid_rel3]),
               to_drop: MapSet.new([oid_rel4]),
               to_invalidate: MapSet.new([oid_rel5])
             } ==
               conn
               |> Configuration.determine_publication_relation_actions!(
                 publication,
                 MapSet.new([oid_rel1, oid_rel2, oid_rel3, oid_rel5])
               )
               |> Map.delete(:memberships)
    end

    test "returns invalidated relations", %{
      pool: conn,
      publication_name: publication
    } do
      oid1 = get_table_oid(conn, {"public", "items"})
      oid2 = get_table_oid(conn, {"public", "other_table_1"})
      oid_rel1 = {oid1, {"public", "items"}}
      oid_rel2 = {oid2, {"public", "other_table_1"}}
      oid_rel1_renamed = {oid1, {"public", "items_old"}}

      assert :ok = Configuration.configure_table_for_replication(conn, publication, oid_rel1)

      # Rename one table and recreate the other
      Postgrex.query!(conn, "ALTER TABLE items RENAME TO items_old", [])
      Postgrex.query!(conn, "DROP TABLE public.other_table_1", [])

      Postgrex.query!(
        conn,
        "CREATE TABLE public.other_table_1 (id UUID PRIMARY KEY, value TEXT NOT NULL)",
        []
      )

      # Should return invalidated relations
      assert %{
               to_invalidate: MapSet.new([oid_rel1, oid_rel2]),
               to_add: @empty_set,
               to_configure_replica_identity: @empty_set,
               to_drop: MapSet.new([oid_rel1_renamed]),
               to_preserve: @empty_set
             } ==
               conn
               |> Configuration.determine_publication_relation_actions!(
                 publication,
                 MapSet.new([oid_rel1, oid_rel2])
               )
               |> Map.delete(:memberships)
    end
  end

  describe "read_in_one_snapshot/4" do
    # Each catalog query of a pass had a timeout to itself before the pass read them in one
    # snapshot. Queries that each take less than it still fit, however many there are.
    test "gives each of its queries the time one had alone", %{pool: conn} do
      assert :read =
               Configuration.read_in_one_snapshot(
                 conn,
                 4,
                 fn conn ->
                   for _ <- 1..4, do: Postgrex.query!(conn, "SELECT pg_sleep(0.15)", [])
                   :read
                 end,
                 200
               )
    end

    # A query that stalls ends at the time one had alone, on the server, where the read's deadline
    # would hold the connection for every round trip's time, past the query's sleep. The session
    # sets no `statement_timeout` of its own, so only the read's ends the query.
    @tag connection_opt_overrides: [pool_size: 1]
    test "ends a query that runs past the time one had alone", %{pool: conn} do
      Postgrex.query!(conn, "SET statement_timeout = 0", [])

      assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
               Configuration.read_in_one_snapshot(
                 conn,
                 4,
                 fn conn -> Postgrex.query!(conn, "SELECT pg_sleep(1)", []) end,
                 200
               )
    end

    # The session's own `statement_timeout`, stricter than a query's time, bounds each query.
    @tag connection_opt_overrides: [pool_size: 1]
    test "keeps a stricter statement_timeout of the session for each query", %{pool: conn} do
      Postgrex.query!(conn, "SET statement_timeout = 200", [])

      assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
               Configuration.read_in_one_snapshot(
                 conn,
                 1,
                 fn conn -> Postgrex.query!(conn, "SELECT pg_sleep(1)", []) end,
                 5_000
               )
    end
  end

  describe "check_publication_status!/2" do
    test "detects if publication is missing", %{pool: conn} do
      assert :not_found = Configuration.check_publication_status!(conn, "nonexistent")
    end

    test "detects if publication doesn't publish all operations", %{
      pool: conn,
      publication_name: publication
    } do
      Postgrex.query!(conn, "ALTER PUBLICATION \"#{publication}\" SET (publish = 'insert')", [])

      assert %{
               publishes_all_operations?: false
             } = Configuration.check_publication_status!(conn, publication)
    end

    test "detects if publication isn't owned by the current user", %{
      pool: conn,
      publication_name: publication
    } do
      role_name = "other_user_#{System.unique_integer([:positive])}"
      Postgrex.query!(conn, "CREATE ROLE #{role_name} NOLOGIN", [])
      Postgrex.query!(conn, "ALTER PUBLICATION \"#{publication}\" OWNER TO #{role_name}", [])

      assert %{
               can_alter_publication?: false
             } = Configuration.check_publication_status!(conn, publication)
    end

    test "detects if publication supports generated columns", %{
      pool: conn,
      publication_name: publication
    } do
      pg_version = Support.TestUtils.fetch_pg_version(conn)

      if pg_version < 180_000 do
        assert %{
                 publishes_generated_columns?: false
               } = Configuration.check_publication_status!(conn, publication)
      else
        Postgrex.query!(
          conn,
          "ALTER PUBLICATION \"#{publication}\" SET (publish_generated_columns = 'none')",
          []
        )

        assert %{
                 publishes_generated_columns?: false
               } = Configuration.check_publication_status!(conn, publication)

        Postgrex.query!(
          conn,
          "ALTER PUBLICATION \"#{publication}\" SET (publish_generated_columns = 'stored')",
          []
        )

        assert %{
                 publishes_generated_columns?: true
               } = Configuration.check_publication_status!(conn, publication)
      end
    end

    test "detects if publication exists, publishes all operations and is owned by the current user",
         %{pool: conn, publication_name: publication} do
      assert %{
               can_alter_publication?: true,
               publishes_all_operations?: true
             } = Configuration.check_publication_status!(conn, publication)
    end
  end

  describe "concurrent publication updates" do
    @tag slow: true
    @tag connection_opt_overrides: [pool_size: 50, queue_target: 10_000, queue_interval: 20_000]
    test "should not cause deadlocks", %{
      pool: conn,
      publication_name: publication
    } do
      num_relations = 10

      deadlock_oid_rels =
        for i <- 1..num_relations do
          table_name = "deadlock_table_#{i}"

          Postgrex.query!(
            conn,
            """
            CREATE TABLE #{table_name} (
              id UUID PRIMARY KEY,
              value TEXT NOT NULL
            )
            """,
            []
          )

          table_oid = get_table_oid(conn, {"public", table_name})
          {table_oid, {"public", table_name}}
        end
        |> MapSet.new()

      tasks =
        for _i <- 1..500 do
          Task.async(fn ->
            [oid_rel] = Enum.take_random(deadlock_oid_rels, 1)

            if Enum.random([true, false]) do
              Configuration.configure_table_for_replication(conn, publication, oid_rel, :infinity)
            else
              Configuration.drop_table_from_publication(conn, publication, oid_rel, :infinity)
            end
          end)
        end

      error_results =
        Task.await_many(tasks, 60_000)
        |> Enum.reject(&match?(:ok, &1))

      assert error_results == []
    end
  end

  defp get_table_identity(conn, {schema, table}) do
    %{rows: [[ident]]} =
      Postgrex.query!(
        conn,
        """
        SELECT relreplident
        FROM pg_class
        JOIN pg_namespace ON relnamespace = pg_namespace.oid
        WHERE relname = $2 AND nspname = $1
        """,
        [schema, table]
      )

    ident
  end

  # A transaction that has written to `items` and waits for `:commit`.
  defp start_items_writer(conn) do
    test_pid = self()

    writer =
      Task.async(fn ->
        # The lock a write takes, with no transaction id: a write's id would hold back the
        # creation of a replication slot by any other test on the server while it is open.
        Postgrex.transaction(conn, fn conn ->
          Postgrex.query!(conn, "LOCK TABLE items IN ROW EXCLUSIVE MODE", [])
          send(test_pid, :written)
          receive(do: (:commit -> :ok))
        end)
      end)

    assert_receive :written
    writer
  end

  # The partition the drain waits for a lock on, past the ones released, or nil once it has ended.
  defp partition_drain_waits_for(conn, drain, released) do
    %{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT c.relname FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
        WHERE l.mode = 'ShareLock' AND NOT l.granted AND c.relname LIKE 'employees\\_%'
        """,
        []
      )

    case Enum.reject(rows, fn [partition] -> MapSet.member?(released, partition) end) do
      [[partition] | _] ->
        partition

      [] ->
        if Process.alive?(drain.pid) do
          Process.sleep(10)
          partition_drain_waits_for(conn, drain, released)
        end
    end
  end

  defp start_other_connection(db_config) do
    start_supervised!({Postgrex, Electric.Utils.deobfuscate_password(db_config)},
      id: :other_connection
    )
  end

  defp expected_drain(conn, publication, filter) do
    status = Configuration.check_publication_status!(conn, publication)

    %{memberships: %{^filter => membership}} =
      Configuration.determine_publication_relation_actions!(
        conn,
        publication,
        MapSet.new([filter])
      )

    {membership, status}
  end

  defp get_table_oid(conn, {schema, table}) do
    %{rows: [[oid]]} =
      Postgrex.query!(
        conn,
        """
        SELECT pg_class.oid
        FROM pg_class
        JOIN pg_namespace ON relnamespace = pg_namespace.oid
        WHERE relname = $2 AND nspname = $1
        """,
        [schema, table]
      )

    oid
  end

  defp list_tables_in_publication(conn, publication) do
    Postgrex.query!(
      conn,
      "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY tablename",
      [publication]
    )
    |> Map.fetch!(:rows)
    |> Enum.map(&List.to_tuple/1)
  end
end
