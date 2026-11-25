defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.Connection do
  @moduledoc false

  # alias Exqlite.Sqlite3
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Sqlite3
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Query
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.PoolRegistry
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Statistics
  alias Electric.BootStep
  alias Electric.Telemetry.OpenTelemetry

  require Logger

  @behaviour NimblePool

  @schema_version 4

  @migration_sqls [
    """
    CREATE TABLE shapes (
      handle BLOB NOT NULL,
      shape BLOB NOT NULL,
      comparable BLOB NOT NULL,
      hash INTEGER NOT NULL,
      snapshot_complete INTEGER NOT NULL DEFAULT 0
    ) STRICT
    """,
    """
    CREATE UNIQUE INDEX shapes_handle_idx ON shapes (handle)
    """,
    """
    CREATE UNIQUE INDEX shapes_comparable_idx ON shapes (comparable)
    """,
    """
    CREATE INDEX shapes_comparable_cover_idx ON shapes (comparable, handle)
    """,
    """
    CREATE INDEX shapes_handle_cover_idx ON shapes (handle, shape, hash, snapshot_complete)
    """,
    """
    CREATE INDEX shapes_snapshot_idx ON shapes (snapshot_complete, handle)
    """,
    """
    CREATE TABLE relations (
      handle BLOB NOT NULL,
      oid INTEGER NOT NULL,
      UNIQUE (handle, oid)
    ) STRICT
    """,
    """
    CREATE INDEX relation_oid_idx ON relations (oid, handle)
    """,
    """
    CREATE INDEX relation_handle_idx ON relations (handle)
    """,
    # keep a separate count of shapes to avoid some kind of o(log n) index scan
    """
    CREATE TABLE shape_count (
      id INTEGER NOT NULL,
      count INTEGER DEFAULT 0
    ) STRICT
    """,
    """
    INSERT INTO shape_count (id, count) VALUES (1, 0)
    """,
    "PRAGMA user_version=#{@schema_version}"
  ]

  # exqlite represents connections and statements as plain references.
  # esqlite wraps them in {:esqlite3, ref} and {:esqlite3_stmt, ref} records.
  # Both guards accept either form so the module works with either backend.
  defguardp is_raw_connection(conn)
            when is_reference(conn) or
                   (is_tuple(conn) and tuple_size(conn) == 2 and elem(conn, 0) == :esqlite3)

  defguardp is_prepared_statement(stmt)
            when is_reference(stmt) or
                   (is_tuple(stmt) and tuple_size(stmt) == 2 and elem(stmt, 0) == :esqlite3_stmt)

  defstruct [:conn, :mode, :stmts]

  def migrate(conn, opts) when is_raw_connection(conn) do
    # because we embed the storage version into the db path
    # we can only ever get 0 for un-migrated or the current storage
    # version.
    case db_version(conn) do
      # not been initialized
      {:ok, 0} ->
        Logger.notice("Migrating shape db to version #{@schema_version}")

        # One transaction, because `execute_all/2` otherwise applies these one statement at a time
        # and `PRAGMA user_version` is the last of them. A process killed partway leaves part of the
        # schema behind with the version still 0 -- a file that opens cleanly and passes
        # quick_check, so neither recovery branch fires, and every later boot replays the migration
        # into a `CREATE TABLE` for a table that is already there. Rolling back is what stops that
        # state existing. `user_version` is header state and commits with the schema; the journal
        # mode is set afterwards, since SQLite will not change it inside a transaction.
        with :ok <- execute_all(conn, ["BEGIN IMMEDIATE TRANSACTION"]),
             :ok <- execute_all(conn, @migration_sqls),
             :ok <- execute_all(conn, ["COMMIT"]),
             :ok <- execute_all(conn, [journal_mode_sql(opts)]) do
          {:ok, @schema_version}
        else
          error ->
            _ = execute(conn, "ROLLBACK")
            error
        end

      # The journal mode is stored in the file, so a database created under the other mode keeps
      # it unless it is set again here. Only a connection that owns the file migrates: the
      # migrator before the pool starts, or the single exclusive-mode connection.
      {:ok, @schema_version} ->
        with :ok <- execute_all(conn, [journal_mode_sql(opts)]) do
          {:ok, @schema_version}
        end

      # The path embeds the schema version, so any other version is a file that disagrees with its
      # own name. Say so rather than letting it arrive as a CaseClauseError from three frames up.
      {:ok, other} ->
        {:error, {:unexpected_schema_version, other}}

      error ->
        error
    end
  end

  defp journal_mode_sql(opts) do
    # Recommended journal_mode for SQLite over an NFS share is 'rollback mode', enabled
    # using `PRAGMA journal_mode = DELETE`
    # https://sqlite.org/useovernet.html
    # https://sqlite.org/pragma.html#pragma_journal_mode
    #
    # > Use SQLite in rollback mode. This means you can have multiple
    # > simultaneous readers or one writer, but not simultaneous readers and
    # > writers.
    #
    # Between this behaviour and our use of a single connection for both reads
    # and writes, there is no need for better locking to prevent db corruption
    # via simultaneous access.
    journal_mode =
      if Keyword.get(opts, :exclusive_mode, false) do
        "DELETE"
      else
        "WAL"
      end

    "PRAGMA journal_mode=#{journal_mode}"
  end

  # `PRAGMA quick_check` answers with one row per problem and only the single row `["ok"]` means
  # healthy, so a damaged database arrives here as a well-formed success: `{:ok, [message]}` for one
  # problem, `{:row, [message]}` for several -- `fetch_one/3` finds a second row where it expects
  # `:done` and hands its own step result straight back. A bare `with` passed both through unchanged
  # to `open_with_recovery`'s `case`, which matches only `:ok` and `{:error, _}`, so actually
  # detecting corruption raised CaseClauseError out of the Migrator's init and took the stack down
  # instead of running the recovery written for exactly this. Anything that is not the healthy row
  # is an error, including no row at all.
  defp integrity_check(conn) when is_raw_connection(conn) do
    try do
      with :ok <- quick_check(conn) do
        partial_migration_check(conn)
      end
    rescue
      e -> {:error, Exception.format(:error, e, __STACKTRACE__)}
    end
  end

  defp quick_check(conn) do
    case fetch_one(conn, "PRAGMA quick_check", []) do
      {:ok, ["ok"]} -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:integrity_check_failed, other}}
    end
  end

  # A db still at version 0 that already holds objects is a migration that died partway through.
  # `migrate/2`'s transaction stops new ones appearing, but one left by an older build -- or by a
  # power loss, since these connections run `synchronous=OFF` -- opens cleanly and passes
  # quick_check, and replaying the migration onto it dies on a `CREATE TABLE` for a table that is
  # already there, with no recovery. Version 0 means nothing in the file was ever committed as a
  # shape db, so let it be rebuilt. A read that fails, such as one that meets another connection's
  # lock, has not shown the file healthy, so it fails the check.
  defp partial_migration_check(conn) do
    with {:ok, 0} <- db_version(conn),
         {:ok, [objects]} when objects > 0 <-
           fetch_one(conn, "SELECT count(*) FROM sqlite_master", []) do
      {:error, {:partial_migration, objects}}
    else
      {:error, _} = error -> error
      _ -> :ok
    end
  end

  # https://sqlite.org/pragma.html#pragma_optimize
  # This pragma is usually a no-op or nearly so and is very fast. On the
  # occasions where it does need to run ANALYZE on one or more tables, it sets
  # a temporary analysis limit, valid for the duration of this pragma only,
  # that prevents the ANALYZE invocations from running for too long.
  def optimize(conn, mask \\ nil) when is_raw_connection(conn) do
    stmts =
      if is_nil(mask) do
        ["PRAGMA optimize"]
      else
        ["PRAGMA optimize=#{mask}"]
      end

    execute_all(conn, stmts)
  end

  def enable_extension(conn, _extension) when is_raw_connection(conn) do
    {:error, :unsupported}
    # with :ok <- Sqlite3.enable_load_extension(conn, true),
    #      {:ok, _} <- fetch_all(conn, "select load_extension(?)", [ExSqlean.path_for(extension)]) do
    #   :ok
    # end
  end

  @impl NimblePool
  def init_worker(pool_state) do
    with {:ok, conn} <- init_worker_for_pool(pool_state) do
      :ok = Statistics.worker_start(pool_state)
      {:ok, conn, pool_state}
    end
  end

  defp init_worker_for_pool(pool_state) do
    if Keyword.get(pool_state, :exclusive_mode, false) do
      init_worker_exclusive(pool_state)
    else
      init_worker_pooled(pool_state)
    end
  end

  defp init_worker_pooled(pool_state) do
    mode = Keyword.get(pool_state, :mode, :readwrite)

    with {:ok, conn} <- open(pool_state),
         stmts <- Query.prepare!(conn, mode) do
      {:ok, %__MODULE__{conn: conn, mode: mode, stmts: stmts}}
    end
  end

  # when opening in exclusive mode we support in-memory databases and so don't
  # perform any db initialisation in the Migration process, since it starts
  # before the pool and for an in-memory db *everything* has to be performed
  # over the same, single, connection since every connection is a separate db
  # in in-memory mode.
  defp init_worker_exclusive(pool_state) do
    mode = :readwrite

    with {:ok, conn} <- open(pool_state, integrity_check: true),
         {:ok, _version} <- migrate(conn, pool_state),
         stmts <- Query.prepare!(conn, mode) do
      {:ok, %__MODULE__{conn: conn, mode: mode, stmts: stmts}}
    end
  end

  @impl NimblePool
  def handle_enqueue({:checkout, label}, pool_state) do
    {:ok, {:checkout, label, now()}, pool_state}
  end

  @impl NimblePool
  def handle_checkout({:checkout, label, enqueued_at}, _from, conn, pool_state) do
    enqueued_duration_us = System.convert_time_unit(now() - enqueued_at, :native, :microsecond)

    OpenTelemetry.execute(
      [:electric, :shape_db, :pool, :checkout],
      %{queue_time_us: enqueued_duration_us},
      %{label: label, stack_id: Keyword.get(pool_state, :stack_id)}
    )

    {:ok, conn, conn, pool_state}
  end

  # handles checkouts that were not enqueued.
  def handle_checkout({:checkout, _label}, _from, conn, pool_state) do
    {:ok, conn, conn, pool_state}
  end

  @impl NimblePool
  def handle_checkin(client_state, _from, _worker_state, pool_state) do
    {:ok, client_state, pool_state}
  end

  @impl NimblePool
  def handle_ping(%__MODULE__{} = conn, pool_state) do
    if Keyword.get(pool_state, :exclusive_mode, false) do
      # keep the write connection alive in exclusive mode — closing it
      # would destroy an in-memory database
      {:ok, conn}
    else
      # the idle timeout is only enabled in non-exclusive mode, so we're free
      # to close all the connections, including write.
      {:remove, :idle}
    end
  end

  @impl NimblePool
  def terminate_worker(reason, conn, pool_state) do
    Logger.debug(fn ->
      ["Closing SQLite ", to_string(conn.mode), " connection for reason: ", inspect(reason)]
    end)

    _ = close(conn)
    :ok = Statistics.worker_stop(pool_state)

    {:ok, pool_state}
  end

  @max_recovery_attempts 2

  def open(pool_state, opts \\ []) do
    with {:ok, db_path} <- db_path(pool_state),
         {:ok, conn} <- open_with_recovery(db_path, pool_state, opts, @max_recovery_attempts) do
      configure_db(conn, pool_state)
    end
  end

  defp open_with_recovery(db_path, _pool_state, _opts, 0) do
    Logger.error("Unable to create database at #{db_path}")
    {:error, "failed to open #{db_path}"}
  end

  defp open_with_recovery(db_path, pool_state, opts, attempts_remaining) do
    case Sqlite3.open(db_path, open_opts(pool_state)) do
      {:ok, conn} ->
        if Keyword.get(opts, :integrity_check, false) do
          BootStep.measure("shape_db.read_ahead", fn -> read_ahead(db_path) end)

          case BootStep.measure("shape_db.integrity_check", fn -> integrity_check(conn) end) do
            :ok ->
              {:ok, conn}

            {:error, reason} ->
              close(conn)

              if lock_contention?(reason) do
                Logger.warning(
                  "Shape database #{db_path} is locked by another connection, keeping it: #{inspect(reason)}"
                )

                {:error, "#{db_path} is locked by another connection"}
              else
                Logger.warning("Database file corrupt #{db_path}: #{inspect(reason)}")

                with :ok <- delete_corrupt_db(db_path) do
                  open_with_recovery(db_path, pool_state, opts, attempts_remaining - 1)
                end
              end
          end
        else
          {:ok, conn}
        end

      {:error, reason} ->
        Logger.warning("Failed to open db #{db_path}: #{inspect(reason)}")

        with :ok <- delete_corrupt_db(db_path) do
          open_with_recovery(db_path, pool_state, opts, attempts_remaining - 1)
        end
    end
  end

  # `PRAGMA quick_check` visits every page in b-tree order, and on a cold page cache each visit is
  # its own synchronous 4 KB read, which takes minutes on a disk limited in reads per second. The
  # same disk reads a file front to back far faster. Reading the file through first leaves the
  # check, and every boot walk after it, on a warm cache; when the cache is already warm this is a
  # memory copy. The reads happen in a process of their own so that their buffers die with it.
  @read_ahead_chunk_bytes 4 * 1024 * 1024

  @doc false
  @spec read_ahead(String.t()) :: non_neg_integer()
  def read_ahead(db_path) do
    if File.regular?(db_path) do
      fn -> read_through(db_path) end |> Task.async() |> Task.await(:infinity)
    else
      # an in-memory database, or a file this open is about to create
      0
    end
  end

  defp read_through(db_path) do
    case :file.open(db_path, [:read, :raw, :binary]) do
      {:ok, file} ->
        try do
          _ = :file.advise(file, 0, 0, :sequential)
          read_to_end(file, 0)
        after
          :file.close(file)
        end

      # Not this function's failure to report: the open that follows reads the same file and
      # owns the recovery.
      {:error, _reason} ->
        0
    end
  end

  defp read_to_end(file, bytes_read) do
    case :file.read(file, @read_ahead_chunk_bytes) do
      {:ok, chunk} -> read_to_end(file, bytes_read + byte_size(chunk))
      _eof_or_error -> bytes_read
    end
  end

  # The check runs before this instance holds the replication lock, so another instance sharing
  # the storage can be committing to the same file. Its lock fails the check with SQLITE_BUSY or
  # SQLITE_LOCKED once the busy timeout runs out, which says nothing about the file's pages.
  @sqlite_busy 5
  @sqlite_locked 6

  @doc false
  # A statement that lost the lock reaches callers as the `{:error, code}` a `:ok =` match
  # refused, since every write here matches its result that way.
  def lock_contention?(%MatchError{term: {:error, code}}), do: lock_contention?(code)

  def lock_contention?(code) when is_integer(code),
    do: Bitwise.band(code, 0xFF) in [@sqlite_busy, @sqlite_locked]

  def lock_contention?(_reason), do: false

  @sqlite_corrupt 11
  @sqlite_notadb 26

  @doc false
  # A step that found the file's pages damaged, or a file that is not a database.
  def corruption?(code) when is_integer(code),
    do: Bitwise.band(code, 0xFF) in [@sqlite_corrupt, @sqlite_notadb]

  def corruption?(_reason), do: false

  defp delete_corrupt_db(db_path) do
    with dir = Path.dirname(db_path),
         {:ok, _} <- File.rm_rf(dir),
         :ok <- File.mkdir_p(dir) do
      :ok
    end
  end

  defp open_opts(pool_state) do
    case Keyword.get(pool_state, :mode, :readwrite) do
      :read ->
        # nomutex is safe because we're enforcing use by a single "thread" via the pool
        #
        # see: https://sqlite.org/threadsafe.html
        #
        # > The SQLITE_OPEN_NOMUTEX flag causes the database connection to be in the multi-thread mode
        #
        # > Multi-thread. In this mode, SQLite can be safely used by multiple
        # > threads provided that no single database connection nor any object
        # > derived from database connection, such as a prepared statement, is
        # > used in two or more threads at the same time.
        [mode: [:readonly, :nomutex]]

      _ ->
        []
    end
  end

  def close(%__MODULE__{conn: conn, stmts: stmts}) do
    # Need to release all the prepared statements on the connection or the
    # close doesn't actually release the resources.
    stmts
    |> Query.active_stmts()
    |> Enum.each(fn {_name, stmt} ->
      # For exqlite release/2 can return an error
      :ok = Sqlite3.release(conn, stmt)
    end)

    close(conn)
  end

  def close(conn) when is_raw_connection(conn) do
    Sqlite3.close(conn)
  end

  def checkout!(stack_id, label, function, timeout \\ 10_000, mode \\ :read) do
    NimblePool.checkout!(
      PoolRegistry.pool_name(stack_id, mode),
      {:checkout, label},
      fn _from, conn -> {function.(conn), conn} end,
      timeout
    )
  end

  def checkout_write!(stack_id, label, function, timeout \\ 10_000) do
    NimblePool.checkout!(
      PoolRegistry.pool_name(stack_id, :write),
      {:checkout, label},
      fn _from, conn ->
        {
          lost_lock_after_checkin(conn, fn -> transaction(conn, fn -> function.(conn) end) end),
          conn
        }
      end,
      timeout
    )
    |> case do
      {:lost_lock, exception, stacktrace} -> reraise exception, stacktrace
      {:ok, result} -> result
    end
  end

  # A raise inside the checkout makes the pool close the connection and open a new one, which in
  # exclusive mode reruns the boot integrity check on the only connection. A transaction that lost
  # the file lock to another process says nothing about the connection, and `transaction/2` has
  # rolled it back, so the connection goes back to the pool and the raise happens after.
  defp lost_lock_after_checkin(%__MODULE__{conn: conn}, fun) do
    {:ok, fun.()}
  rescue
    e ->
      if lock_contention?(e) and Sqlite3.autocommit?(conn),
        do: {:lost_lock, e, __STACKTRACE__},
        else: reraise(e, __STACKTRACE__)
  end

  def fetch_one(conn, sql, binds) when is_raw_connection(conn) and is_binary(sql) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         {:ok, row} <- fetch_one(conn, stmt, binds),
         :ok <- Sqlite3.release(conn, stmt) do
      {:ok, row}
    end
  end

  def fetch_one(conn, stmt, binds)
      when is_raw_connection(conn) and is_prepared_statement(stmt) do
    with :ok <- Sqlite3.bind(stmt, binds) do
      case Sqlite3.step(conn, stmt) do
        {:row, row} ->
          case Sqlite3.step(conn, stmt) do
            :done ->
              with :ok <- Sqlite3.reset(stmt), do: {:ok, row}

            # Left mid-step, as by a first step that failed, below.
            error ->
              _ = Sqlite3.reset(stmt)
              error
          end

        :done ->
          :ok = Sqlite3.reset(stmt)
          :error

        error ->
          # A statement left mid-step refuses its next bind, and resetting one whose step failed
          # returns that step's code again, which is already the result.
          _ = Sqlite3.reset(stmt)
          error
      end
    end
  end

  def fetch_all(conn, sql, binds) when is_raw_connection(conn) and is_binary(sql) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         {:ok, rows} <- fetch_all(conn, stmt, binds),
         :ok <- Sqlite3.release(conn, stmt) do
      {:ok, rows}
    end
  end

  def fetch_all(conn, stmt, binds)
      when is_raw_connection(conn) and is_prepared_statement(stmt) do
    with :ok <- Sqlite3.bind(stmt, binds) do
      case Sqlite3.fetch_all(conn, stmt) do
        {:ok, rows} ->
          with :ok <- Sqlite3.reset(stmt), do: {:ok, rows}

        error ->
          _ = Sqlite3.reset(stmt)
          error
      end
    end
  end

  def fetch_all(conn, sql, binds, mapper_fun) when is_raw_connection(conn) and is_binary(sql) do
    with {:ok, rows} <- fetch_all(conn, sql, binds) do
      {:ok, Enum.map(rows, mapper_fun)}
    end
  end

  def fetch_all(conn, stmt, binds, mapper_fun)
      when is_raw_connection(conn) and is_prepared_statement(stmt) do
    with {:ok, rows} <- fetch_all(conn, stmt, binds) do
      {:ok, Enum.map(rows, mapper_fun)}
    end
  end

  def modify(conn, stmt, binds)
      when is_raw_connection(conn) and is_prepared_statement(stmt) do
    with :ok <- Sqlite3.bind(stmt, binds) do
      case Sqlite3.step(conn, stmt) do
        :done ->
          with {:ok, changes} <- Sqlite3.changes(conn),
               :ok <- Sqlite3.reset(stmt),
               do: {:ok, changes}

        error ->
          _ = Sqlite3.reset(stmt)
          error
      end
    end
  end

  defp db_version(conn) do
    with {:ok, [version]} <- fetch_one(conn, "PRAGMA user_version", []) do
      {:ok, version}
    end
  end

  defp configure_db(conn, pool_state) do
    with :ok <- execute_all(conn, pragmas(pool_state)) do
      {:ok, conn}
    end
  end

  @pragma_defaults [
    # for our current deployment mode synchronous = OFF is enough (hand
    # data to kernel, don't fsync) but for oss deploys we should keep it at a
    # higher durability setting
    synchronous: "OFF",
    # Default to a beefy page cache size because we want decent read performance
    # Multiplied by 1024, because SQLite works in KiB but we configure in bytes.
    cache_size: 4096 * 1024
  ]

  def default!(pragma) do
    Keyword.fetch!(@pragma_defaults, pragma)
  end

  def defaults do
    @pragma_defaults
  end

  defp pragmas(pool_state) do
    [
      "PRAGMA synchronous=#{pragma_value(pool_state, :synchronous)}",
      # Divide by 1024 because we configure in bytes but SQLite works in KiB
      "PRAGMA cache_size=-#{div(pragma_value(pool_state, :cache_size), 1024)}"
    ]
  end

  defp pragma_value(opts, pragma) do
    Keyword.get(opts, pragma, default!(pragma))
  end

  def execute(conn, sql) when is_raw_connection(conn) and is_binary(sql) do
    Sqlite3.execute(conn, sql)
  end

  def execute_all(conn, sqls) when is_raw_connection(conn) and is_list(sqls) do
    with {:ok, _conn} <-
           Electric.Utils.reduce_while_ok(sqls, conn, fn sql, conn ->
             with :ok <- Sqlite3.execute(conn, sql) do
               {:ok, conn}
             end
           end) do
      :ok
    end
  end

  defp transaction(%__MODULE__{conn: conn}, fun) when is_function(fun, 0) do
    try do
      :ok = execute(conn, "BEGIN IMMEDIATE TRANSACTION")

      {result, commit} =
        case fun.() do
          :ok -> {:ok, true}
          {:ok, _} = result -> {result, true}
          error -> {error, false}
        end

      if commit,
        do: :ok = execute(conn, "COMMIT"),
        else: :ok = execute(conn, "ROLLBACK")

      result
    rescue
      e ->
        _ = execute(conn, "ROLLBACK")
        reraise e, __STACKTRACE__
    end
  end

  def stream_query(conn, stmt, row_mapper_fun)
      when is_raw_connection(conn) and is_prepared_statement(stmt) do
    Stream.resource(
      fn -> {:cont, conn, stmt} end,
      fn
        {:halt, conn, stmt} ->
          {:halt, {conn, stmt}}

        {:cont, conn, stmt} ->
          case Sqlite3.multi_step(conn, stmt) do
            {:rows, rows} ->
              {Stream.map(rows, row_mapper_fun), {:cont, conn, stmt}}

            {:done, []} ->
              {[], {:halt, conn, stmt}}

            {:done, rows} ->
              {Stream.map(rows, row_mapper_fun), {:halt, conn, stmt}}

            {:error, reason} ->
              raise Electric.ShapeCache.ShapeStatus.ShapeDb.Error, action: :read, error: reason
          end
      end,
      # Stream.resource hands after_fun whatever accumulator is current. Only normal
      # exhaustion narrows that to the 2-tuple above; a raise here, a raise in the
      # consumer's reducer, and an early halt all reach it holding the 3-tuple the
      # continuation threads, where a 2-tuple-only clause raises FunctionClauseError in
      # place of the real error and leaves the statement un-reset on a pooled connection.
      fn
        {_conn, stmt} -> Sqlite3.reset(stmt)
        {_tag, _conn, stmt} -> Sqlite3.reset(stmt)
      end
    )
  end

  def explain(stack_id) do
    checkout!(stack_id, :explain, fn %__MODULE__{} = conn ->
      Query.explain(conn, :read)
    end)

    checkout_write!(stack_id, :explain, fn %__MODULE__{} = conn ->
      Query.explain(conn, :write)
    end)
  end

  # used in testing and logging
  def db_path(pool_state) do
    # Manage compatibility by embedding all versions into the db name rather
    # than embed the values in the db itself and then have to manage schema
    # resets or migrations.
    # `System.otp_release()` is included because `:erlang.term_to_binary/2` has
    # limited guarantees about compatibilities between major versions of the
    # BEAM and its safer to just ignore data from a different release than
    # handle that.
    # Not sure of the format of `System.otp_release/0` - it may only ever
    # return the major version, i.e. `"28"`, but just to be sure let's
    # ignore everything that may come after the first numbers.
    [otp_version | _] = String.split(System.otp_release(), ~r/[^0-9]/)

    version =
      "v#{Electric.ShapeCache.ShapeStatus.version()}_#{@schema_version}@otp-#{otp_version}"

    with {:ok, storage_dir} <- Keyword.fetch(pool_state, :storage_dir) do
      case storage_dir do
        ":memory:" ->
          if Keyword.get(pool_state, :exclusive_mode, false) do
            Logger.notice("ShapeDb using in-memory database")
            {:ok, ":memory:"}
          else
            Logger.error(
              "Enable exclusive_mode (ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE=true) to use an in-memory database"
            )

            {:error, "Cannot use :memory: database without exclusive_mode"}
          end

        storage_dir ->
          path = Path.join(storage_dir, "meta/shape-db/#{version}.sqlite")

          with :ok <- File.mkdir_p(Path.dirname(path)) do
            {:ok, path}
          end
      end
    end
  end

  def db_path!(pool_state) do
    {:ok, path} = db_path(pool_state)
    path
  end

  @superseded_db_pattern ~r/^v.+\.sqlite(-\w+)?$/

  @doc """
  Deletes the shape catalogs written by builds using a different version triple.

  The path above embeds the shape status version, the schema version and the OTP release, so
  bumping any one of them puts a boot on an empty catalog -- and the first post-lock refresh answers
  an empty catalog by deleting every shape's storage. That makes the previous build's file
  worthless to the previous build too, since the shapes it names no longer have any: nothing will
  ever read it again. Nothing deleted it either, so each version bump left a full copy of the
  catalog on the volume permanently.

  The shape count goes into the log before the file does, because it is the size of the
  re-snapshot this boot has just committed the cluster to and is otherwise invisible until that
  load arrives at Postgres.

  A coordinated stack waits for active status before sweeping these files, so an instance
  still waiting for the replication lock leaves the previous catalog in place.
  """
  def discard_superseded_dbs(pool_state) do
    case db_path(pool_state) do
      {:ok, ":memory:"} ->
        :ok

      {:ok, db_path} ->
        dir = Path.dirname(db_path)
        current = Path.basename(db_path)

        # Two passes, and the order is load-bearing. Reading a catalog's shape count opens it, and
        # a WAL-mode db materialises `-shm` and `-wal` on open even read-only -- so any listing
        # taken before the counts is stale by the time the deletes run, and the sidecars come back
        # from under the sweep. Catalogs first, then re-list for whatever the counting resurrected.
        superseded_entries(dir, current, ".sqlite")
        |> Enum.each(&discard_superseded_catalog(dir, &1))

        superseded_entries(dir, current, nil)
        |> Enum.each(&remove_file(Path.join(dir, &1)))

        :ok

      {:error, _} = error ->
        error
    end
  end

  defp superseded_entries(dir, current, extname) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.filter(entries, fn entry ->
          entry != current and not String.starts_with?(entry, current <> "-") and
            Regex.match?(@superseded_db_pattern, entry) and
            (is_nil(extname) or Path.extname(entry) == extname)
        end)

      {:error, reason} ->
        Logger.warning(
          "Could not list #{dir} to discard superseded shape catalogs: #{inspect(reason)}"
        )

        []
    end
  end

  defp discard_superseded_catalog(dir, entry) do
    path = Path.join(dir, entry)

    Logger.warning(
      "Discarding the shape catalog left by a previous build: #{entry} " <>
        "#{superseded_db_contents(path)}. This build reads a differently versioned file, so " <>
        "every shape that catalog held is gone and will be re-snapshotted on demand."
    )

    remove_file(path)
  end

  defp remove_file(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("Could not delete #{path}: #{inspect(reason)}")
    end
  end

  defp superseded_db_contents(path) do
    size =
      case File.stat(path) do
        {:ok, %File.Stat{size: size}} -> "#{size} bytes"
        {:error, reason} -> "size unreadable (#{inspect(reason)})"
      end

    case superseded_shape_count(path) do
      {:ok, count} -> "(#{count} shapes, #{size})"
      :error -> "(#{size}, shape count unreadable)"
    end
  end

  # A foreign schema read by a build that by definition does not know it, so every step here is
  # allowed to fail: the count is reporting, and the delete it annotates is worth doing without it.
  defp superseded_shape_count(path) do
    case Sqlite3.open(path, mode: [:readonly, :nomutex]) do
      {:ok, conn} ->
        count =
          case fetch_one(conn, "SELECT count(*) FROM shapes", []) do
            {:ok, [count]} when is_integer(count) -> {:ok, count}
            _ -> :error
          end

        _ = close(conn)
        count

      {:error, _} ->
        :error
    end
  rescue
    _ -> :error
  end

  defp now, do: System.monotonic_time()
end
