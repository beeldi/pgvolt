defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.Migrator do
  use GenServer
  require Logger

  alias Electric.ShapeCache.ShapeStatus.ShapeDb

  @optimization_period :timer.minutes(60)

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl GenServer
  def init(args) do
    {:ok, stack_id} = Keyword.fetch(args, :stack_id)
    exclusive_mode = Keyword.get(args, :exclusive_mode, false)

    Process.set_label({:shape_db_migrator, stack_id})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    Logger.notice("Shape database file: #{inspect(ShapeDb.Connection.db_path!(args))}")

    # Discarding earlier builds' catalogs is safe only once this instance owns the volume — the
    # Postgres advisory lock the connection subsystem takes, which the supervisor starts *after*
    # this migrator. Discarding from init would let a booting instance delete a still-running
    # previous instance's catalog during a rolling deploy. When a stack coordinates ownership
    # (StatusMonitor running), defer the discard until it goes active (lock held); a standalone
    # ShapeDb boot with no StatusMonitor has no other owner, so discard synchronously as before.
    #
    # Kept outside `apply_migration/3` on purpose: exclusive mode has no migration to run here, but
    # it still writes a version-stamped file that supersedes the previous build's, and both deployed
    # environments run exclusive mode, so a discard reached only from the pooled branch never runs
    # where the volume actually fills up.
    with :ok <- apply_migration(stack_id, args, exclusive_mode) do
      if ownership_is_coordinated?(stack_id) do
        {:ok, schedule_optimize(stack_id), {:continue, {:discard_superseded_dbs, args}}}
      else
        with :ok <- ShapeDb.Connection.discard_superseded_dbs(args) do
          {:ok, schedule_optimize(stack_id), :hibernate}
        end
      end
    end
  end

  defp ownership_is_coordinated?(stack_id) do
    not is_nil(GenServer.whereis(Electric.StatusMonitor.name(stack_id)))
  end

  @impl GenServer
  def handle_continue({:discard_superseded_dbs, args}, stack_id) do
    timeout = Keyword.get(args, :discard_superseded_wait_timeout, :infinity)

    case Electric.StatusMonitor.wait_until_active(stack_id, timeout: timeout) do
      :ok ->
        case ShapeDb.Connection.discard_superseded_dbs(args) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning("Could not discard superseded shape catalogs: #{inspect(reason)}")
        end

      other ->
        Logger.notice(
          "Not discarding superseded shape catalogs; stack did not become active: #{inspect(other)}"
        )
    end

    {:noreply, stack_id, :hibernate}
  end

  defp apply_migration(_stack_id, _opts, true = _exclusive?) do
    # In exclusive  mode we *must* apply the migrations within the pool
    # connection initialization because we might be using a memory db.
    :ok
  end

  defp apply_migration(_stack_id, opts, false = _exclusive?) do
    with {:ok, conn} <- ShapeDb.Connection.open(opts, integrity_check: true),
         {:ok, _version} <- ShapeDb.Connection.migrate(conn, opts),
         # https://sqlite.org/pragma.html#pragma_optimize
         # Applications with long-lived database connections should run "PRAGMA
         # optimize=0x10002" when the database connection first opens
         :ok = ShapeDb.Connection.optimize(conn, "0x10002"),
         :ok = ShapeDb.Connection.close(conn) do
      :ok
    end
  end

  @impl GenServer
  def handle_info(:optimize, stack_id) do
    # `PRAGMA optimize` is advisory, so nothing here may escape: the supervisor above is
    # `:one_for_all` with `max_restarts: 0`, and a saturated write pool
    # used to take the whole stack down.
    #
    # Both clauses are load-bearing. `NimblePool.checkout!` signals its expired deadline with an
    # `exit`, which `rescue` cannot catch; `BEGIN IMMEDIATE` on a busy db reraises a MatchError,
    # which `catch` cannot.
    try do
      ShapeDb.Connection.checkout_write!(stack_id, :optimize, fn %{conn: conn} ->
        ShapeDb.Connection.optimize(conn)
      end)
    rescue
      e ->
        Logger.warning("Skipping scheduled PRAGMA optimize: #{Exception.message(e)}")
    catch
      :exit, reason ->
        Logger.warning(
          "Skipping scheduled PRAGMA optimize, write pool unavailable: #{inspect(reason)}"
        )
    else
      :ok ->
        :ok

      other ->
        Logger.warning("Scheduled PRAGMA optimize did not complete: #{inspect(other)}")
    end

    {:noreply, schedule_optimize(stack_id), :hibernate}
  end

  defp schedule_optimize(stack_id) do
    Process.send_after(self(), :optimize, @optimization_period)
    stack_id
  end
end
