defmodule Electric.Postgres.Configuration do
  @moduledoc """
  Module for functions that configure Postgres in some way using
  a provided connection.
  """
  require Logger
  alias Electric.DbConfigurationError
  alias Electric.Utils
  alias Electric.Replication.PublicationManager

  @type relation_filters :: PublicationManager.RelationTracker.relation_filters()

  @type publication_status :: %{
          oid: Electric.relation_id(),
          can_alter_publication?: boolean(),
          publishes_all_operations?: boolean(),
          publishes_generated_columns?: boolean()
        }

  @type relation_actions :: %{
          to_preserve: relation_filters(),
          to_add: relation_filters(),
          to_drop: relation_filters(),
          to_configure_replica_identity: relation_filters(),
          to_invalidate: relation_filters(),
          memberships: %{Electric.oid_relation() => Electric.relation_id()}
        }

  @typep changed_relation ::
           {
             Electric.relation_id(),
             Electric.relation(),
             Electric.relation_id() | nil,
             Electric.relation() | nil
           }

  @typep relation_with_replica :: {Electric.relation_id(), Electric.relation(), <<_::8>>}

  @default_action_timeout 5_000
  # Each catalog query of a pass had Postgrex's default to itself before the pass read them in one
  # snapshot, and has it there too: the read's deadline gives that time to each of its round trips,
  # the five around its queries included (`BEGIN`, its isolation level, the read of the
  # connection's own `statement_timeout`, the budget set from it, `COMMIT`).
  @catalog_query_timeout 15_000
  @catalog_read_round_trips 5
  # The time the membership read after a drain's `LOCK` has, on the server.
  @drain_read_timeout 1_000
  # A drain's checkout, which can queue behind another action on the pool, and the eight round
  # trips of its transaction: `BEGIN`, its isolation level, the read of the connection's own
  # `statement_timeout`, the budget set before the `LOCK`, the `LOCK`, the budget set before the
  # read, the read, `COMMIT`.
  @drain_round_trips 9
  # An update of a table Electric publishes had the action timeout for its checkout, `BEGIN`, its
  # statements and `COMMIT`. The isolation level set before it and the membership read after it are
  # two round trips more, with the second a drain's read has for each: an update that committed in
  # time on a slow link still does with them. The client's deadline is also what cancels a statement
  # still waiting for a lock, so the update gives up such a wait two seconds later than it did.
  @owned_update_timeout @default_action_timeout + 2 * @drain_read_timeout
  @publishes_generated_columns """
  CASE WHEN current_setting('server_version_num')::int >= 180000
      THEN (to_jsonb(p) ->> 'pubgencols') = 's'
      ELSE FALSE
  END
  """

  @doc """
  Check whether the publication with the given name exists, and return its status.

  The status includes whether the publication is owned, whether it publishes all operations,
  and whether it publishes generated columns (if supported by the Postgres version).
  """
  @spec check_publication_status!(Postgrex.conn(), String.t()) ::
          publication_status() | :not_found
  def check_publication_status!(conn, publication_name) do
    # note: we could do a separate query to get the PG version, unsure which is more efficient
    # but avoiding having to know the PG version in advance keeps things contained and simple
    query =
      """
      SELECT
        p.oid,
        pg_get_userbyid(p.pubowner) = current_role as can_alter_publication,
        pubinsert AND pubupdate AND pubdelete AND pubtruncate as publishes_all_operations,
        #{@publishes_generated_columns} AS publishes_generated_columns
      FROM pg_publication as p WHERE pubname = $1;
      """

    Postgrex.query!(conn, query, [publication_name])
    |> case do
      %Postgrex.Result{
        rows: [
          [oid, can_alter_publication, publishes_all_operations, publishes_generated_columns]
        ]
      } ->
        %{
          oid: oid,
          can_alter_publication?: can_alter_publication,
          publishes_all_operations?: publishes_all_operations,
          publishes_generated_columns?: publishes_generated_columns
        }

      %Postgrex.Result{num_rows: 0} ->
        :not_found
    end
  end

  @doc """
  The publication's status as `check_publication_status!/2` reads it, for a read of the catalog
  that is not the check: in a transaction, from its snapshot.
  """
  @spec read_publication_status!(Postgrex.conn(), String.t()) ::
          publication_status() | :not_found
  def read_publication_status!(conn, publication_name) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT
          p.oid,
          pg_get_userbyid(p.pubowner) = current_role,
          pubinsert AND pubupdate AND pubdelete AND pubtruncate,
          #{@publishes_generated_columns}
        FROM pg_publication AS p WHERE pubname = $1
        """,
        [publication_name]
      )

    case rows do
      [[oid, can_alter, all_operations, generated_columns]] ->
        %{
          oid: oid,
          can_alter_publication?: can_alter,
          publishes_all_operations?: all_operations,
          publishes_generated_columns?: generated_columns
        }

      [] ->
        :not_found
    end
  end

  @spec add_table_to_publication(Postgrex.conn(), String.t(), Electric.oid_relation(), timeout()) ::
          :ok | {:error, term()}
  def add_table_to_publication(
        conn,
        publication_name,
        oid_relation,
        timeout \\ @default_action_timeout
      ) do
    {_oid, relation} = oid_relation
    table = Utils.relation_to_sql(relation)

    Logger.debug("Adding #{table} to publication #{publication_name}")

    run_in_transaction(
      conn,
      fn conn ->
        case exec_alter_publication_for_table(conn, publication_name, :add, table) do
          :ok -> wait_for_in_flight_writers(conn, table)
          :noop -> wait_for_in_flight_writers_after_another_addition(conn, table)
          {:error, _} = error -> error
        end
      end,
      timeout
    )
  end

  # Logical decoding evaluates publication membership per change, against the catalog state at
  # the point in the WAL where the change was made. A transaction that wrote to the table
  # *before* the `ALTER PUBLICATION ... ADD TABLE` commits therefore never has those writes
  # emitted by the replication stream, even if it commits after. The only way for such writes to
  # reach a shape is through the initial snapshot, and the snapshot only includes them if the
  # transaction has committed by the time the snapshot is taken.
  #
  # `ALTER PUBLICATION ... ADD TABLE` only takes a SHARE UPDATE EXCLUSIVE lock on the table, which
  # doesn't conflict with writers, so on its own it gives no such guarantee. Taking a SHARE lock
  # here closes the gap: it conflicts with the ROW EXCLUSIVE lock every writer holds until its
  # transaction ends, so it is only granted once all in-flight writers have finished, and writers
  # that arrive while it's pending queue up behind it and end up writing after the addition has
  # committed, which is decoded normally. Since this runs inside the same transaction as the
  # addition, the addition doesn't commit - and shapes waiting on it don't start snapshotting -
  # until then.
  #
  # This mirrors what the `ALTER TABLE ... REPLICA IDENTITY FULL` in
  # `configure_table_for_replication/4` has always done implicitly via its stronger ACCESS
  # EXCLUSIVE lock. The wait is bounded by the transaction timeout, on expiry of which the whole
  # transaction, addition included, is rolled back and the configuration reported as failed.
  #
  # The automatic path calls this inside the transaction that added the table, while it holds the
  # SHARE UPDATE EXCLUSIVE lock from the `ALTER PUBLICATION`. That lock serialises concurrent
  # configurators on the same table, so no two of them can hold SHARE at the same time and then
  # deadlock on upgrading it to ACCESS EXCLUSIVE for the replica identity change.
  #
  # The path for a publication Electric may not alter, in manual publishing mode or owned by another
  # role, calls it through `wait_for_in_flight_writers_on_published_table/4`, in its own transaction,
  # for a table that role already added. No replica identity upgrade follows there - that role owns
  # it too - so the ACCESS EXCLUSIVE deadlock cannot arise.
  defp wait_for_in_flight_writers(conn, table, denied \\ & &1) do
    case Postgrex.query(conn, "LOCK TABLE #{table} IN SHARE MODE", []) do
      {:ok, _} ->
        :ok

      {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}} = error} ->
        Postgrex.rollback(conn, denied.(error))

      # `query_canceled` is what the transaction timeout produces, `lock_not_available` what a
      # `lock_timeout` configured on the database side does.
      # A failed `LOCK` leaves the transaction aborted, and the addition must not be committed
      # without the barrier in any case, so roll the whole transaction back.
      {:error, %Postgrex.Error{postgres: %{code: code}}}
      when code in [:query_canceled, :lock_not_available] ->
        Postgrex.rollback(conn, DbConfigurationError.table_lock_timeout(table))

      {:error, reason} ->
        Postgrex.rollback(conn, reason)
    end
  end

  # The table was added by someone else after the publication was checked, with nothing waiting
  # for the writers then in flight, so the barrier is still owed to the shapes waiting on this
  # addition. The lock the failed addition took went with its savepoint, and is taken again first
  # to keep the lock order of an addition.
  defp wait_for_in_flight_writers_after_another_addition(conn, table) do
    case Postgrex.query(conn, "LOCK TABLE #{table} IN SHARE UPDATE EXCLUSIVE MODE", []) do
      {:ok, _} ->
        wait_for_in_flight_writers(conn, table)

      {:error, %Postgrex.Error{postgres: %{code: code}}}
      when code in [:query_canceled, :lock_not_available] ->
        Postgrex.rollback(conn, DbConfigurationError.table_lock_timeout(table))

      {:error, reason} ->
        Postgrex.rollback(conn, reason)
    end
  end

  @doc """
  Drain in-flight writers on a table whose publication membership Electric did not establish itself,
  before Electric starts snapshotting it.

  Used when another session adds a table, including in manual publishing mode or when another role
  owns the publication. That session runs `ALTER PUBLICATION ... ADD TABLE`, so the barrier that
  `add_table_to_publication/4` takes never runs, and a writer open across that addition has its
  pre-addition rows in neither the initial snapshot nor the replication stream. Taking the same
  SHARE lock here, in its own transaction, waits for those writers to finish before the snapshot.

  This barrier needs a privilege the minimal manual-publishing role (SELECT + REPLICATION) does not
  have, so a caller must tolerate a `{:lock_denied, error}` failure, which only a denial of the
  `LOCK` itself returns, and rely on the operator taking the barrier in the same transaction as the
  addition instead - see the manual publishing guide.

  The `LOCK` waits up to `lock_timeout` milliseconds, the action timeout unless given, so that a
  writer still open fails the drain within it, with the connection kept. A stricter
  `statement_timeout` of the connection bounds it instead. The bound covers the whole statement, not
  each lock it takes: on a partitioned table the `LOCK` takes one per partition, each of which can
  wait. The membership read after it has a second of its own on the server, so a lock granted at the
  end of its wait still admits the table. The transaction's deadline adds the action timeout for
  each round trip, the checkout included, so the connection stays checked out past the `LOCK`'s
  wait through the read and the commit.
  """
  @spec wait_for_in_flight_writers_on_published_table(
          Postgrex.conn(),
          String.t(),
          Electric.oid_relation(),
          {Electric.relation_id(), publication_status()},
          pos_integer() | :infinity
        ) ::
          {:ok, Electric.relation_id()}
          | {:error, {:lock_denied, Postgrex.Error.t()}}
          | {:error, term()}
  def wait_for_in_flight_writers_on_published_table(
        conn,
        publication,
        oid_relation,
        expected,
        lock_timeout \\ @default_action_timeout
      ) do
    {_oid, relation} = oid_relation
    table = Utils.relation_to_sql(relation)
    Logger.debug("Waiting for in-flight writers on #{table} before serving it")

    run_in_transaction(
      conn,
      fn conn ->
        :ok = read_committed(conn)
        own = own_statement_timeout(conn, lock_timeout)

        # Only a denial of the `LOCK` itself says that this role cannot drain the table. One met
        # before it, while the timeout is set up, fails the drain as any other error does.
        :ok =
          with_statement_timeout(conn, own, lock_timeout, fn ->
            wait_for_in_flight_writers(conn, table, &{:lock_denied, &1})
          end)

        with_statement_timeout(conn, own, @drain_read_timeout, fn ->
          published_table_membership!(conn, publication, oid_relation, expected)
        end)
      end,
      drain_transaction_timeout(lock_timeout)
    )
  end

  # The server bounds the `LOCK` and the read. The client's deadline, which closes the connection
  # when it passes, gives each round trip the time an action has. A deadline any slow link can
  # outlast fails a drain as a lost connection does, and the next try, with the same deadline,
  # the same way: the table stays held with no writer left to wait for.
  defp drain_transaction_timeout(:infinity), do: :infinity

  defp drain_transaction_timeout(lock_timeout),
    do: lock_timeout + @drain_read_timeout + @drain_round_trips * @default_action_timeout

  # A `statement_timeout`, not a `lock_timeout`, which bounds each lock the statement waits for. Its
  # expiry is a `query_canceled` the server raises, which `wait_for_in_flight_writers/3` reports as
  # a lock timeout. The connection's own, read in milliseconds from `pg_settings`, stays when it is
  # stricter, whether it came from the server's configuration, the role or a `SET` on the session.
  defp own_statement_timeout(_conn, :infinity), do: nil

  defp own_statement_timeout(conn, _lock_timeout) do
    %{rows: [[setting]]} =
      Postgrex.query!(
        conn,
        "SELECT setting FROM pg_settings WHERE name = 'statement_timeout'",
        []
      )

    String.to_integer(setting)
  end

  # Each statement sets its own budget: a `SET LOCAL` lasts the transaction, so the one the `LOCK`
  # set would bound the read too.
  defp with_statement_timeout(_conn, nil, _timeout, fun), do: fun.()
  defp with_statement_timeout(_conn, _own, :infinity, fun), do: fun.()

  defp with_statement_timeout(conn, own, timeout, fun) when is_integer(timeout) and timeout > 0 do
    budget = if own > 0, do: min(own, timeout), else: timeout
    :ok = set_local_statement_timeout(conn, Integer.to_string(budget))
    fun.()
  end

  defp set_local_statement_timeout(conn, setting) do
    Postgrex.query!(conn, "SELECT set_config('statement_timeout', $1, true)", [setting])
    :ok
  end

  # A role or a database can make `REPEATABLE READ` its transactions' default, under which the
  # first statement takes the snapshot every later one reads. The membership read that follows a
  # lock's wait would then see the catalog as it was before the wait, and admit a table taken out
  # of the publication meanwhile. Under `READ COMMITTED` each statement reads what has committed
  # when it starts. The level is set before any other statement, which is when it can be.
  defp read_committed(conn) do
    Postgrex.query!(conn, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED", [])
    :ok
  end

  @doc false
  def update_and_verify_table_membership(conn, publication, oid_relation, expected, update_table) do
    run_in_transaction(
      conn,
      fn conn ->
        :ok = read_committed(conn)

        with :ok <- update_table.(conn),
             {:ok, _membership} = verified <-
               published_table_membership!(conn, publication, oid_relation, expected) do
          verified
        else
          {:error, reason} -> Postgrex.rollback(conn, reason)
        end
      end,
      @owned_update_timeout
    )
  end

  def published_table_membership!(
        conn,
        publication,
        {oid, {schema, name}},
        {membership, %{oid: publication_oid, publishes_generated_columns?: publishes_generated}}
      ) do
    case Postgrex.query!(
           conn,
           """
           SELECT pr.oid
           FROM pg_publication_rel pr
           JOIN pg_publication p ON p.oid = pr.prpubid
           JOIN pg_class c ON c.oid = pr.prrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE p.pubname = $1 AND c.oid = $2 AND n.nspname = $3 AND c.relname = $4
             AND c.relreplident = 'f'
             AND ($5::oid IS NULL OR pr.oid = $5) AND p.oid = $6
             AND p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate
             AND (#{@publishes_generated_columns}) = $7
           """,
           [publication, oid, schema, name, membership, publication_oid, publishes_generated]
         ) do
      %{rows: [[membership]]} -> {:ok, membership}
      %{rows: []} -> {:error, :publication_changed}
    end
  end

  @spec set_table_replica_identity_full(Postgrex.conn(), Electric.oid_relation(), timeout()) ::
          :ok | {:error, term()}
  def set_table_replica_identity_full(conn, oid_relation, timeout \\ @default_action_timeout) do
    {_oid, relation} = oid_relation
    table = Utils.relation_to_sql(relation)
    Logger.debug("Setting #{table} replica identity to FULL")
    run_in_transaction(conn, &exec_set_replica_identity_full(&1, table), timeout)
  end

  @spec configure_table_for_replication(
          Postgrex.conn(),
          String.t(),
          Electric.oid_relation(),
          timeout()
        ) ::
          :ok | {:error, term()}
  def configure_table_for_replication(
        conn,
        publication_name,
        oid_relation,
        timeout \\ @default_action_timeout
      ) do
    run_in_transaction(
      conn,
      fn conn ->
        with :ok <- add_table_to_publication(conn, publication_name, oid_relation),
             :ok <- set_table_replica_identity_full(conn, oid_relation) do
          :ok
        else
          # If the nested `add_table_to_publication/4` rolled back, the outer transaction is
          # already marked as failed and committing it would report a bare `{:error, :rollback}`,
          # losing the reason. Roll back explicitly to propagate it instead. For the errors the
          # nested calls recover from via savepoints there is nothing to commit anyway.
          {:error, reason} -> Postgrex.rollback(conn, reason)
        end
      end,
      timeout
    )
  end

  @spec drop_table_from_publication(
          Postgrex.conn(),
          String.t(),
          Electric.oid_relation(),
          timeout()
        ) ::
          :ok | {:error, term()}
  def drop_table_from_publication(
        conn,
        publication_name,
        oid_relation,
        timeout \\ @default_action_timeout
      ) do
    {_oid, relation} = oid_relation
    table = Utils.relation_to_sql(relation)
    Logger.debug("Removing #{table} from publication #{publication_name}")

    run_in_transaction(
      conn,
      fn conn ->
        case exec_alter_publication_for_table(conn, publication_name, :drop, table) do
          result when result in [:ok, :noop] -> :ok
          {:error, _} = error -> error
        end
      end,
      timeout
    )
  end

  defp publication_exists?(conn, publication_name) do
    %Postgrex.Result{num_rows: num_rows} =
      Postgrex.query!(conn, "SELECT 1 FROM pg_publication WHERE pubname = $1", [publication_name])

    num_rows > 0
  end

  # Returns `:noop` when the table was already in the requested state, i.e. the publication was
  # not altered. Note that in that case the savepoint rollback also releases the lock that
  # `ALTER PUBLICATION` took on the table.
  @spec exec_alter_publication_for_table(Postgrex.conn(), String.t(), :add | :drop, String.t()) ::
          :ok | :noop | {:error, term()}
  defp exec_alter_publication_for_table(conn, publication_name, op_atom, table) do
    op =
      case op_atom do
        :add -> "ADD"
        :drop -> "DROP"
      end

    publication_query =
      "ALTER PUBLICATION #{Utils.quote_name(publication_name)} #{op} TABLE #{table}"

    Postgrex.query!(conn, "SAVEPOINT before_publication", [])

    case Postgrex.query(conn, publication_query, []) do
      {:ok, _} ->
        Postgrex.query!(conn, "RELEASE SAVEPOINT before_publication", [])
        :ok

      {:error, reason} ->
        Postgrex.query!(conn, "ROLLBACK TO SAVEPOINT before_publication", [])
        Postgrex.query!(conn, "RELEASE SAVEPOINT before_publication", [])

        case reason do
          # undefined object can happen when removing a table that was already removed, and also
          # when the publication itself is missing. The message would tell them apart, but Postgres
          # translates it into the server's `lc_messages` language, so the catalog is asked instead.
          %{postgres: %{code: :undefined_object}} when op_atom == :drop ->
            if publication_exists?(conn, publication_name), do: :noop, else: {:error, reason}

          # duplicate object can happen when adding a table that was already added
          %{postgres: %{code: :duplicate_object}} when op_atom == :add ->
            :noop

          _ ->
            {:error, reason}
        end
    end
  end

  @spec exec_set_replica_identity_full(Postgrex.conn(), String.t()) :: :ok | {:error, term()}
  defp exec_set_replica_identity_full(conn, table) do
    Postgrex.query!(conn, "SAVEPOINT before_replica", [])

    case Postgrex.query(conn, "ALTER TABLE #{table} REPLICA IDENTITY FULL", []) do
      {:ok, _} ->
        Postgrex.query!(conn, "RELEASE SAVEPOINT before_replica", [])
        :ok

      {:error, reason} ->
        Postgrex.query!(conn, "ROLLBACK TO SAVEPOINT before_replica", [])
        Postgrex.query!(conn, "RELEASE SAVEPOINT before_replica", [])
        {:error, reason}
    end
  end

  @spec determine_publication_relation_actions!(Postgrex.conn(), String.t(), relation_filters()) ::
          relation_actions()
  def determine_publication_relation_actions!(conn, publication_name, expected_rels) do
    # New relations were configured using a schema read in a different transaction
    # (if at all, might have been from cache) so we need to check if any of
    # the relations were dropped/renamed since then
    to_invalidate =
      list_changed_relations!(conn, expected_rels |> Enum.to_list())
      |> Enum.map(&trim_changed_relation/1)
      |> MapSet.new()

    # Compare with current relations in the publication to determine what needs dropping/adding,
    # as well as reconfiguring because of misconfigured replica identity
    pub_tables = get_publication_memberships!(conn, publication_name)

    memberships =
      Map.new(pub_tables, fn {oid, rel, _, membership} -> {{oid, rel}, membership} end)

    publication_rels = MapSet.new(Map.keys(memberships))

    misconfigured_publication_rels =
      pub_tables
      |> Enum.reject(fn {_oid, _rel, replident, _membership} -> replident == "f" end)
      |> MapSet.new(fn {oid, rel, _, _} -> {oid, rel} end)

    correctly_configured_publication_rels =
      MapSet.difference(publication_rels, misconfigured_publication_rels)

    valid_expected = MapSet.difference(expected_rels, to_invalidate)

    to_preserve = MapSet.intersection(correctly_configured_publication_rels, valid_expected)

    to_reconfigure_replica_identity =
      MapSet.intersection(misconfigured_publication_rels, valid_expected)

    to_drop = MapSet.difference(publication_rels, valid_expected)
    to_add = MapSet.difference(valid_expected, publication_rels)

    to_configure_replica_identity =
      MapSet.union(
        to_reconfigure_replica_identity,
        get_replica_identities!(conn, to_add |> Enum.to_list())
        |> Enum.filter(fn {_oid, _rel, replident} -> replident != "f" end)
        |> Enum.map(&trim_relation_with_replica/1)
        |> MapSet.new()
      )

    %{
      to_preserve: to_preserve,
      to_add: to_add,
      to_drop: to_drop,
      to_configure_replica_identity: to_configure_replica_identity,
      to_invalidate: to_invalidate,
      memberships: memberships
    }
  end

  @spec get_publication_tables!(Postgrex.conn(), String.t()) :: list(relation_with_replica())
  def get_publication_tables!(conn, publication) do
    for {oid, relation, replica, _membership} <- get_publication_memberships!(conn, publication),
        do: {oid, relation, replica}
  end

  defp get_publication_memberships!(conn, publication) do
    # `pg_publication_tables` is too clever for us -- if you add a partitioned
    # table to the publication `pg_publication_tables` lists all the partitions
    # as members, not the actual partitioned table. `pg_publication_rel`
    # doesn't do this, it returns a direct list of the tables that were added
    # using `ALTER PUBLICATION` and doesn't expand a partitioned table into its
    # partitions.
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT
          pc.oid, (pn.nspname, pc.relname), pc.relreplident, ppr.oid
        FROM
          pg_publication_rel ppr
        JOIN
          pg_publication pp ON ppr.prpubid = pp.oid
        JOIN
          pg_class pc ON pc.oid = ppr.prrelid
        JOIN
          pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE
          pp.pubname = $1
        ORDER BY
          pn.nspname, pc.relname
        """,
        [publication]
      )

    Enum.map(rows, &List.to_tuple/1)
  end

  @spec existing_relation_oids!(Postgrex.conn(), [Electric.relation_id()]) ::
          MapSet.t(Electric.relation_id())
  def existing_relation_oids!(_conn, []), do: MapSet.new()

  def existing_relation_oids!(conn, oids) do
    %{rows: rows} =
      Postgrex.query!(conn, "SELECT oid FROM pg_class WHERE oid = ANY($1::oid[])", [oids])

    MapSet.new(rows, fn [oid] -> oid end)
  end

  @spec get_replica_identities!(Postgrex.conn(), list(Electric.oid_relation())) ::
          list(relation_with_replica())
  defp get_replica_identities!(_conn, []), do: []

  defp get_replica_identities!(conn, oid_relations) do
    oid_to_rel = Map.new(oid_relations, fn {oid, rel} -> {oid, {oid, rel}} end)
    oids = Map.keys(oid_to_rel)

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT
          pc.oid, pc.relreplident
        FROM
          pg_class pc
        WHERE
          pc.oid = ANY($1::oid[])
        """,
        [oids]
      )

    Enum.map(rows, fn [oid, replident] ->
      {_, rel} = Map.fetch!(oid_to_rel, oid)
      {oid, rel, replident}
    end)
  end

  @spec list_changed_relations!(Postgrex.conn(), list(Electric.oid_relation())) ::
          list(changed_relation())
  defp list_changed_relations!(_conn, []), do: []

  defp list_changed_relations!(conn, known_relations) do
    # We're checking whether the table has been renamed (same oid, different name) or
    # dropped (maybe same name exists, but different oid). If either is true, we need to update
    # the new filters and maybe notify existing shapes.
    {oids, relations} = Enum.unzip(known_relations)
    {schemas, tables} = Enum.unzip(relations)

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        WITH input_relations AS (
          SELECT
            UNNEST($1::oid[]) AS oid,
            UNNEST($2::text[]) AS input_nspname,
            UNNEST($3::text[]) AS input_relname
        )
        SELECT
          ir.oid, (ir.input_nspname, ir.input_relname) as input_relation, pc.oid, (pn.nspname, pc.relname)
        FROM input_relations ir
        LEFT JOIN pg_class pc ON pc.oid = ir.oid
        LEFT JOIN pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE pc.oid IS NULL OR (pc.relname != input_relname OR pn.nspname != input_nspname)
        """,
        [oids, schemas, tables]
      )

    Enum.map(rows, &List.to_tuple/1)
  end

  @doc """
  Runs `fun` on a connection in a read-only transaction, so that every query it makes reads one
  snapshot of the catalog. `fun` makes `queries` queries, and the transaction has `query_timeout`
  for each of its round trips. The server also ends each statement at `query_timeout`, or at the
  connection's own `statement_timeout` where that is stricter: one query that stalls fails the read
  there, as it did when it ran alone, and frees the connection, which on an admin pool of one the
  inspector shares, instead of holding it to the read's deadline. Returns what `fun` returns, or
  `{:error, error}` when a query fails.
  """
  @spec read_in_one_snapshot(
          Postgrex.conn(),
          pos_integer(),
          (Postgrex.conn() -> result),
          pos_integer()
        ) :: result | {:error, term()}
        when result: term()
  def read_in_one_snapshot(db_pool, queries, fun, query_timeout \\ @catalog_query_timeout) do
    run_in_transaction(
      db_pool,
      fn conn ->
        Postgrex.query!(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", [])
        :ok = set_catalog_query_budget(conn, query_timeout)
        fun.(conn)
      end,
      (queries + @catalog_read_round_trips) * query_timeout
    )
  end

  # The budget is read and set with `SHOW` and `SET`, not with the `SELECT`s a drain's are: neither
  # takes a snapshot, so the read's is still taken at `fun`'s first query and sees what committed
  # until then. The connection's own setting, from the server's configuration, the role or a `SET`
  # on the session, stays when it is stricter.
  defp set_catalog_query_budget(conn, query_timeout) do
    %{rows: [[setting]]} = Postgrex.query!(conn, "SHOW statement_timeout", [])
    own = shown_milliseconds(setting)
    budget = if own > 0, do: min(own, query_timeout), else: query_timeout
    Postgrex.query!(conn, "SET LOCAL statement_timeout = #{budget}", [])
    :ok
  end

  @shown_time_units %{
    "" => 1,
    "ms" => 1,
    "s" => 1_000,
    "min" => 60_000,
    "h" => 3_600_000,
    "d" => 86_400_000
  }

  # `SHOW` gives a time in the largest unit that divides it, and `0` with none.
  defp shown_milliseconds(setting) do
    {value, unit} = Integer.parse(setting)
    value * Map.fetch!(@shown_time_units, unit)
  end

  defp run_in_transaction(db_pool, fun, timeout) do
    run_handling_db_connection_errors(fn ->
      case Postgrex.transaction(db_pool, fun, timeout: timeout) do
        {:ok, result} ->
          result

        {:error, :rollback} ->
          {:error, %RuntimeError{message: "Transaction unexpectedly rolled back"}}

        {:error, err} ->
          {:error, err}
      end
    end)
  end

  def run_handling_db_connection_errors(fun) do
    fun.()
  rescue
    err -> {:error, err}
  catch
    :exit, {_, {DBConnection.Holder, :checkout, _}} ->
      {:error, %DBConnection.ConnectionError{message: "Database connection not available"}}
  end

  @spec trim_changed_relation(changed_relation()) :: Electric.oid_relation()
  defp trim_changed_relation({oid, relation, _new_oid, _renamed_relation}), do: {oid, relation}

  @spec trim_relation_with_replica(relation_with_replica()) :: Electric.oid_relation()
  defp trim_relation_with_replica({oid, relation, _replident}), do: {oid, relation}
end
