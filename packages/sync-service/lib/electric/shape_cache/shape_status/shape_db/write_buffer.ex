defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer do
  @moduledoc """
  Buffers SQLite metadata writes using ETS for immediate return to callers,
  then batches and flushes to SQLite in the background.

  This prevents timeout cascades when many concurrent clients create shapes
  after a redeploy.

  ## Architecture

  Two ETS tables are used:

  1. **Operations table** (ordered_set) - queue of operations to flush to SQLite
  2. **Shapes table** (set) - buffered shapes, comparable index, and tombstones with namespaced keys

  ## How it works

  1. When a shape is added, insert into shapes table and queue :add operation
  2. When a shape is removed, insert tombstone and queue :remove operation
  3. GenServer polls every 50ms and flushes pending operations to SQLite
  4. After successful flush, clean up entries from shapes table

  ## Crash recovery

  If the system crashes, all in-flight writes in the operations table will be
  lost. On reboot clients of the in-flight shapes will receive `must-refetch`
  responses and the shapes will be re-inserted into the buffer.

  This will leave orphaned shape data in the storage implementation, as we are
  losing all references to the handle. We will need some background
  reconciliation process that culls orphaned storage data.

  ## Operations table format

  `{{monotonic_time, unique_int}, operation, flushing, attempts}`

  `attempts` counts failed flushes; an entry is dead-lettered once it reaches
  `@max_flush_attempts` so a poisoned batch cannot wedge the queue forever.

  ## Shapes table key formats

  - `{:pending_count, integer}` - count of shapes pending addition
  - `{{:shape, handle}, shape, comparable}` - shape data
  - `{{:comparable, comparable}, handle}` - reverse index for O(1) lookup
  - `{{:tombstone, handle}, timestamp}` - handles marked for deletion
  - `{:committed_removals, integer}` - flushes that committed a removal, see `committed_removals/1`
  """

  use GenServer

  require Logger

  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Query
  alias Electric.Telemetry.OpenTelemetry

  import Electric, only: [is_stack_id: 1]

  @poll_interval 100
  # keep this low-ish so that this process yields the write connection
  # to handle_for_shape_critical/2 reasonably often.
  @max_drain_per_cycle 100

  # A batch that keeps raising the same error is re-armed every poll; without a
  # terminal state it wedges the queue forever while later ops pile up until the
  # node OOMs. After this many failed flush attempts an entry is dead-lettered
  # (dropped with a distinct alarm) so the queue can drain.
  @max_flush_attempts 100

  # Safety ceiling on the pending-add backlog. Above it `add_shape` refuses new
  # work so callers get backpressure instead of the buffer growing without bound.
  # Removals and snapshot-completions are never refused so the queue can drain.
  @max_queue_size 100_000

  # How long the final flush keeps trying through contention before it gives the queue up.
  @final_flush_contention_ms 20_000

  @default_checkout_timeout 10_000

  # esqlite3 opens every connection with this busy timeout: a commit that meets another process's
  # lock waits that long before it fails.
  @busy_timeout_ms 2_000

  @doc false
  def max_flush_attempts, do: @max_flush_attempts

  # The supervisor's clock starts at the shutdown signal. A flush already running when it comes
  # can take a checkout and a busy timeout before `terminate/2` starts, and the last attempt of
  # the final flush as long again after its window: the shutdown timeout covers both.
  def child_spec(opts) do
    checkout_timeout = Keyword.get(opts, :write_checkout_timeout, @default_checkout_timeout)

    opts |> super() |> Map.put(:shutdown, final_flush_budget(checkout_timeout))
  end

  defp final_flush_budget(checkout_timeout),
    do: 2 * (checkout_timeout + @busy_timeout_ms) + @final_flush_contention_ms + @busy_timeout_ms

  def operations_table_name(stack_id), do: :"write_buffer_ops:#{stack_id}"
  def shapes_table_name(stack_id), do: :"write_buffer_shapes:#{stack_id}"

  def name(stack_id), do: Electric.ProcessRegistry.name(stack_id, __MODULE__)

  @doc "Returns a monotonic timestamp for ordering writes"
  def timestamp, do: System.monotonic_time()

  @doc "Returns a unique, ordered key for operations table entries"
  def op_key, do: {System.monotonic_time(), System.unique_integer([:positive, :monotonic])}

  def start_link(args) do
    stack_id = Keyword.fetch!(args, :stack_id)
    GenServer.start_link(__MODULE__, args, name: name(stack_id))
  end

  @doc """
  Synchronously flush all pending writes. Useful for testing and graceful shutdown.
  """
  def flush_sync(stack_id, timeout \\ 5000) when is_stack_id(stack_id) do
    GenServer.call(name(stack_id), :flush_sync, timeout)
  end

  @doc """
  Commits every queued write as the final flush does, retrying while another process holds the
  file. The holder of the replication lock calls it before it lets the lock go.
  """
  def drain(stack_id, timeout \\ final_flush_budget(@default_checkout_timeout))
      when is_stack_id(stack_id) do
    # The buffer starts no batch after the deadline, so the caller waits as long as the supervisor
    # gives the final flush: a batch running when the call arrives, the window, and the attempt
    # running at its end. A stalled write can still run past it; the caller then goes on.
    deadline = System.monotonic_time(:millisecond) + @final_flush_contention_ms
    GenServer.call(name(stack_id), {:drain, deadline}, timeout)
  end

  @doc """
  Disable flushing of buffered actions to db. Useful for testing
  """
  def pause_flush(stack_id) when is_stack_id(stack_id) do
    GenServer.call(name(stack_id), {:pause, true})
  end

  @doc """
  Re-enable flushing of buffered actions to db. Useful for testing
  """
  def resume_flush(stack_id) when is_stack_id(stack_id) do
    GenServer.call(name(stack_id), {:pause, false})
  end

  # Lookup functions

  @doc "Look up a shape by its handle. Returns {:ok, shape} or :not_found."
  def lookup_shape(stack_id, handle) do
    shapes_table = shapes_table_name(stack_id)

    case :ets.lookup(shapes_table, {:shape, handle}) do
      [{{:shape, ^handle}, shape, _comparable}] ->
        if :ets.member(shapes_table, {:tombstone, handle}) do
          :not_found
        else
          {:ok, shape}
        end

      [] ->
        :not_found
    end
  end

  @doc "Look up a handle by comparable shape binary. Returns {:ok, handle} or :not_found."
  def lookup_handle(stack_id, comparable_shape) do
    shapes_table = shapes_table_name(stack_id)

    case :ets.lookup(shapes_table, {:comparable, comparable_shape}) do
      [{{:comparable, ^comparable_shape}, handle}] ->
        if :ets.member(shapes_table, {:tombstone, handle}), do: :not_found, else: {:ok, handle}

      [] ->
        :not_found
    end
  end

  @doc "Check if a handle is in the tombstones (marked for deletion)"
  def is_tombstoned?(stack_id, handle) do
    :ets.member(shapes_table_name(stack_id), {:tombstone, handle})
  end

  @doc """
  Counts flushes that committed a removal. It moves before the flush clears the tombstones, so a
  reader that sees it change across a SQLite read may hold a row the flush deleted, with nothing
  left to say so, and has to read again.
  """
  def committed_removals(stack_id) do
    :ets.lookup_element(shapes_table_name(stack_id), :committed_removals, 2, 0)
  end

  @doc """
  Check if a handle exists in the buffer.
  Returns:
    - `false` if tombstoned (being deleted)
    - `true` if in shapes table (buffered, not yet in SQLite)
    - `:unknown` if not in buffer (may or may not exist in SQLite)

  Note: Checks tombstone last since it's the authoritative "delete in progress"
  signal, avoiding a race where tombstone is added between checks.
  """
  def has_handle?(stack_id, handle) do
    shapes_table = shapes_table_name(stack_id)
    has_shape = :ets.member(shapes_table, {:shape, handle})
    is_tombstoned = :ets.member(shapes_table, {:tombstone, handle})

    cond do
      is_tombstoned -> false
      has_shape -> true
      true -> :unknown
    end
  end

  @doc "Returns the number of pending operations in the buffer"
  def pending_operations_count(stack_id) do
    :ets.info(operations_table_name(stack_id), :size)
  rescue
    ArgumentError -> 0
  end

  @doc "Returns the handles with queued or in-flight catalog writes"
  def pending_shape_handles(stack_id) do
    stack_id
    |> operations_table_name()
    |> :ets.select([
      {{:_, {:add, :"$1", :_, :_, :_, :_}, :_, :_}, [], [:"$1"]},
      {{:_, {:remove, :"$1"}, :_, :_}, [], [:"$1"]},
      {{:_, {:snapshot_complete, :"$1"}, :_, :_}, [], [:"$1"]}
    ])
    |> MapSet.new()
  rescue
    ArgumentError -> :unavailable
  end

  @doc "Gives the change to the total count of shapes in the database once all buffered writes are applied"
  def pending_count_diff(stack_id) do
    :ets.lookup_element(shapes_table_name(stack_id), :count_add, 2) -
      :ets.lookup_element(shapes_table_name(stack_id), :count_remove, 2)
  end

  @doc "Returns all buffered shapes as a list of {handle, shape} tuples, excluding tombstoned handles, sorted by handle"
  def list_buffered_shapes(stack_id) do
    tombstones = tombstoned_handles(stack_id)

    shapes_table_name(stack_id)
    |> :ets.match({{:shape, :"$1"}, :"$2", :_})
    |> Enum.reject(fn [handle, _] -> MapSet.member?(tombstones, handle) end)
    |> Enum.map(fn [handle, shape] -> {handle, shape} end)
    |> Enum.sort_by(fn {handle, _} -> handle end)
  end

  @doc "Returns handles from the buffer that match any of the given relations (by OID)"
  def handles_for_relations(stack_id, relations) do
    if relations == [] do
      []
    else
      tombstones = tombstoned_handles(stack_id)
      oids_set = relations |> Enum.map(fn {oid, _relation} -> oid end) |> MapSet.new()

      operations_table_name(stack_id)
      |> :ets.match({:_, {:add, :"$1", :_, :_, :_, :"$2"}, :_, :_})
      |> Enum.filter(fn [handle, shape_relations] ->
        not MapSet.member?(tombstones, handle) and
          Enum.any?(shape_relations, fn {oid, _} -> MapSet.member?(oids_set, oid) end)
      end)
      |> Enum.map(fn [handle, _] -> handle end)
    end
  end

  @doc "Returns all tombstoned handles as a MapSet"
  def tombstoned_handles(stack_id) do
    shapes_table_name(stack_id)
    |> :ets.match({{:tombstone, :"$1"}, :_})
    |> MapSet.new(fn [h] -> h end)
  end

  @doc "Counts the additions the buffer holds: queued, or committed and not cleared yet"
  def buffered_addition_count(stack_id) do
    # From the operations, which only a commit or a drop clears: the `{:shape, handle}` entry of a
    # queued addition goes with a removal queued after it, and with an older addition or removal
    # of the same handle that commits or is dropped first.
    :ets.select_count(operations_table_name(stack_id), [
      {{:_, {:add, :_, :_, :_, :_, :_}, :_, :_}, [], [true]}
    ])
  end

  # Write functions

  @doc "Add a shape to the buffer"
  def add_shape(stack_id, handle, shape, comparable, hash, relations) do
    shapes_table = shapes_table_name(stack_id)
    ops_table = operations_table_name(stack_id)
    ts = op_key()

    cond do
      :ets.info(ops_table, :size) >= @max_queue_size ->
        {:error, :buffer_full}

      :ets.insert_new(shapes_table, [
        {{:shape, handle}, shape, comparable},
        {{:comparable, comparable}, handle}
      ]) ->
        :ets.update_counter(shapes_table, :count_add, 1)

        # Queue operation for SQLite (contains full data including hash and relations)
        true =
          :ets.insert(
            ops_table,
            {ts, {:add, handle, shape, comparable, hash, relations}, false, 0}
          )

        :ok

      true ->
        {:error, "duplicate shape #{handle} #{inspect(shape)}"}
    end
  end

  @doc "Mark a shape for removal"
  def remove_shape(stack_id, handle) do
    shapes_table = shapes_table_name(stack_id)
    ts = op_key()

    # insert_new is atomic - returns false if already tombstoned, preventing double-remove
    case :ets.insert_new(shapes_table, {{:tombstone, handle}, ts}) do
      true ->
        case :ets.lookup(shapes_table, {:shape, handle}) do
          [{{:shape, ^handle}, _shape, comparable}] ->
            :ets.delete(shapes_table, {:comparable, comparable})

          [] ->
            :ok
        end

        :ets.delete(shapes_table, {:shape, handle})
        :ets.update_counter(shapes_table, :count_remove, 1)

        # `queue_marked_removal/2` may have queued it since the mark, and counted it: one operation
        # and one count either way.
        ops_table = operations_table_name(stack_id)

        if not :ets.insert_new(ops_table, {ts, {:remove, handle}, false, 0}),
          do: :ets.update_counter(shapes_table, :count_remove, {2, -1, 0, 0})

        :ok

      false ->
        :ok
    end
  end

  @doc """
  Whether the handle is marked for removal, with its removal queued. `remove_shape/2` marks the
  handle, then queues the removal, and a process stopped between the two leaves a mark that no
  flush clears: its removal is queued here, under the mark's key, so that a process queuing it
  meanwhile leaves one operation and one count. The count goes first, as in `remove_shape/2`: a
  flush that commits the removal before it would leave the count one too high for good.
  """
  def queue_marked_removal(stack_id, handle) do
    shapes_table = shapes_table_name(stack_id)
    ops_table = operations_table_name(stack_id)

    case :ets.lookup(shapes_table, {:tombstone, handle}) do
      [{_key, ts}] ->
        if not :ets.member(ops_table, ts) do
          :ets.update_counter(shapes_table, :count_remove, 1)

          if not :ets.insert_new(ops_table, {ts, {:remove, handle}, false, 0}),
            do: :ets.update_counter(shapes_table, :count_remove, {2, -1, 0, 0})
        end

        true

      [] ->
        false
    end
  end

  @doc "Queue a snapshot_complete operation"
  def queue_snapshot_complete(stack_id, handle) do
    ops_table = operations_table_name(stack_id)
    ts = op_key()
    true = :ets.insert(ops_table, {ts, {:snapshot_complete, handle}, false, 0})
    :ok
  end

  @doc "Handles whose snapshot-complete mark is queued and not yet committed"
  def queued_snapshot_completions(stack_id) do
    stack_id
    |> operations_table_name()
    |> :ets.select([{{:_, {:snapshot_complete, :"$1"}, :_, :_}, [], [:"$1"]}])
    |> MapSet.new()
  end

  @doc "Clear all data from all tables"
  def clear(stack_id) do
    :ets.delete_all_objects(shapes_table_name(stack_id))
    :ets.delete_all_objects(operations_table_name(stack_id))
    initialize_counts(shapes_table_name(stack_id))
    :ok
  end

  @impl GenServer
  def init(opts) do
    # Trap exits to enable the terminate/2 callback, giving the buffer time
    # to flush to the db before terminating
    Process.flag(:trap_exit, true)

    stack_id = Keyword.fetch!(opts, :stack_id)
    manual_flush_only = Keyword.get(opts, :manual_flush_only, false)

    Process.set_label({:shape_db_write_buffer, stack_id})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    ops_table = operations_table_name(stack_id)
    shapes_table = shapes_table_name(stack_id)

    :ets.new(ops_table, [
      :named_table,
      :public,
      :ordered_set,
      write_concurrency: :auto,
      read_concurrency: true
    ])

    :ets.new(shapes_table, [
      :named_table,
      :public,
      :set,
      write_concurrency: :auto,
      read_concurrency: true
    ])

    initialize_counts(shapes_table)

    {:ok,
     schedule_poll(%{
       stack_id: stack_id,
       ops_table: ops_table,
       shapes_table: shapes_table,
       manual_flush_only: manual_flush_only,
       checkout_timeout: Keyword.get(opts, :write_checkout_timeout, @default_checkout_timeout),
       paused: false,
       drain_deadline: nil
     })}
  end

  defp initialize_counts(shapes_table) do
    :ets.insert(shapes_table, [{:count_add, 0}, {:count_remove, 0}])
  end

  @impl GenServer
  def handle_info(:poll, %{manual_flush_only: false} = state) do
    flush_until_empty(state)

    {:noreply, schedule_poll(state), :hibernate}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    # We're trapping exits to enable the terminate/2 callback, not to keep
    # this process alive
    {:stop, reason, state}
  end

  def handle_info(msg, state) do
    Logger.warning("Received unexpected message #{inspect(msg)}")
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:flush_sync, _from, state) do
    {:reply, flush_until_empty(%{state | paused: false}), state}
  end

  def handle_call({:drain, deadline}, _from, state) do
    {:reply, final_flush(%{state | paused: false, drain_deadline: deadline}, deadline), state}
  end

  def handle_call({:pause, paused?}, _from, state) do
    {:reply, :ok, %{state | paused: paused?}}
  end

  @impl GenServer
  def terminate(_reason, state) do
    final_flush(state, System.monotonic_time(:millisecond) + @final_flush_contention_ms)
  end

  # The queue goes with this process, so the last flush keeps trying while the contention lasts.
  # An instance sharing the storage holds the file for its whole boot integrity check, which can
  # run while the instance it replaces stops.
  defp final_flush(state, deadline) do
    case flush_until_empty(state) do
      {:error, reason} = error when reason in [:lock_contention, :checkout_timeout] ->
        if System.monotonic_time(:millisecond) < deadline,
          do: final_flush(state, deadline),
          else: error

      result ->
        result
    end
  end

  defp schedule_poll(%{manual_flush_only: true} = state) do
    state
  end

  defp schedule_poll(state) do
    Process.send_after(self(), :poll, @poll_interval)
    state
  end

  defp flush_until_empty(%{paused: true} = _state) do
    :ok
  end

  defp flush_until_empty(state) do
    case drain_status(state) do
      :flush ->
        state
        |> mark_and_collect_entries(@max_drain_per_cycle)
        |> flush_entries(state)

      :expired ->
        {:error, :drain_deadline}

      :empty ->
        :ok
    end
  end

  # A drain starts no batch after its deadline: its caller stops waiting soon after.
  defp drain_status(%{drain_deadline: deadline, ops_table: ops_table})
       when is_integer(deadline) do
    cond do
      System.monotonic_time(:millisecond) < deadline -> :flush
      :ets.info(ops_table, :size) > 0 -> :expired
      true -> :empty
    end
  end

  defp drain_status(_state), do: :flush

  defp flush_entries([], _state) do
    :ok
  end

  defp flush_entries(
         entries,
         %{
           ops_table: ops_table,
           shapes_table: shapes_table,
           stack_id: stack_id,
           checkout_timeout: checkout_timeout
         } = state
       ) do
    OpenTelemetry.with_span(
      "shape_db.write_buffer.flush",
      [entry_count: length(entries)],
      stack_id,
      fn ->
        case do_batch_write(stack_id, entries, checkout_timeout) do
          :ok ->
            if Enum.any?(entries, &match?({_ts, {:remove, _handle}, _attempts}, &1)) do
              :ets.update_counter(shapes_table, :committed_removals, 1, {:committed_removals, 0})
            end

            Enum.each(entries, fn {ts, op, _attempts} ->
              case op do
                {:add, handle, _shape, comparable, _hash, _relations} ->
                  :ets.delete(shapes_table, {:shape, handle})

                  # A removal frees the comparable, so another handle can claim it while this add is
                  # still queued. Deleting the entry by key would take that handle's index with it,
                  # and the lookups that then miss create a second handle for the same shape — two
                  # queued adds for one comparable, which the unique index rejects, and the batch
                  # carrying them can never drain.
                  :ets.select_delete(shapes_table, [
                    {{{:comparable, comparable}, handle}, [], [true]}
                  ])

                  :ets.update_counter(shapes_table, :count_add, {2, -1, 0, 0})
                  {:add, handle}

                {:remove, handle} ->
                  :ets.delete(shapes_table, {:tombstone, handle})
                  :ets.update_counter(shapes_table, :count_remove, {2, -1, 0, 0})
                  op

                _ ->
                  op
              end
              |> tap(&Logger.debug(fn -> ["ShapeDb: committed ", inspect(&1)] end))

              :ets.delete(ops_table, ts)
            end)

            flush_until_empty(state)

          # Nothing ran: the connection stayed busy past the checkout deadline. Readers share
          # that connection in exclusive mode, so a storm of shape requests or a bulk shape
          # cleanup can hold it for seconds. The batch waits for the next poll without
          # spending an attempt, so contention never dead-letters a write.
          {:error, :checkout_timeout} = error ->
            Logger.warning(
              "ShapeDb.WriteBuffer did not get the write connection within #{checkout_timeout}ms; " <>
                "#{length(entries)} operations stay queued for the next flush"
            )

            Enum.each(entries, fn {ts, _op, _attempts} ->
              :ets.update_element(ops_table, ts, {3, false})
            end)

            error

          # Rolled back: another process held the file past the busy timeout. With shared
          # storage, an instance waiting for the lock runs its boot integrity check as one read,
          # and in rollback-journal mode no commit lands until it ends. Like a busy connection,
          # that is contention, so it spends no attempt and never dead-letters a write.
          {:error, :lock_contention} = error ->
            Logger.warning(
              "ShapeDb.WriteBuffer could not commit while another process held the database " <>
                "lock; #{length(entries)} operations stay queued for the next flush"
            )

            Enum.each(entries, fn {ts, _op, _attempts} ->
              :ets.update_element(ops_table, ts, {3, false})
            end)

            error

          {:error, reason} = error ->
            Enum.each(entries, fn {ts, op, attempts} ->
              if attempts + 1 >= @max_flush_attempts do
                dead_letter(state, ts, op, reason)
              else
                # Re-arm for retry: clear the flushing flag and bump the attempt counter.
                :ets.update_element(ops_table, ts, [{3, false}, {4, attempts + 1}])
              end
            end)

            error
        end
      end
    )
  end

  # Terminal state for a persistently-failing entry. Dropping it — and the optimistic
  # buffer state it staged — lets the queue drain instead of re-arming the same doomed
  # batch forever. The alarm fires once per drop, not every poll.
  defp dead_letter(%{ops_table: ops_table, shapes_table: shapes_table}, ts, op, reason) do
    revert_buffered_op(shapes_table, op)
    :ets.delete(ops_table, ts)

    Logger.error(
      "ShapeDb.WriteBuffer dead-lettered a write operation after #{@max_flush_attempts} " <>
        "failed flush attempts and dropped it so the queue can drain: " <>
        "#{inspect(op)} (last error: #{inspect(reason)})"
    )
  end

  defp revert_buffered_op(shapes_table, {:add, handle, _shape, comparable, _hash, _relations}) do
    :ets.delete(shapes_table, {:shape, handle})

    :ets.select_delete(shapes_table, [
      {{{:comparable, comparable}, handle}, [], [true]}
    ])

    :ets.update_counter(shapes_table, :count_add, {2, -1, 0, 0})
    :ok
  end

  defp revert_buffered_op(shapes_table, {:remove, handle}) do
    :ets.delete(shapes_table, {:tombstone, handle})
    :ets.update_counter(shapes_table, :count_remove, {2, -1, 0, 0})
    :ok
  end

  defp revert_buffered_op(_shapes_table, _op), do: :ok

  defp mark_and_collect_entries(%{ops_table: ops_table}, limit) do
    # Select entries with flushing=false, carrying their attempt counter
    match_spec = [{{:"$1", :"$2", false, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}]

    case :ets.select(ops_table, match_spec, limit) do
      {results, _continuation} ->
        Enum.map(results, fn {ts, _op, _attempts} = entry ->
          :ets.update_element(ops_table, ts, {3, true})
          entry
        end)

      :"$end_of_table" ->
        []
    end
  end

  defp do_batch_write(stack_id, entries, checkout_timeout) do
    Connection.checkout_write!(
      stack_id,
      :batch_write,
      fn conn ->
        Enum.each(entries, fn
          {_ts, {:add, handle, shape, comparable, hash, relations}, _attempts} ->
            :ok = Query.add_shape(conn, handle, shape, comparable, hash, relations)

          {_ts, {:remove, handle}, _attempts} ->
            case Query.remove_shape(conn, handle) do
              {:error, {:enoshape, ^handle}} ->
                # tried to delete a shape that doesn't exist, in which case the
                # failure is ok, it's already deleted
                Logger.warning("Attempt to delete non-existent shape #{inspect(handle)}")

              :ok ->
                :ok

              error ->
                raise "Failed to remove shape: #{inspect(error)}"
            end

          {_ts, {:snapshot_complete, handle}, _attempts} ->
            # A failed update, such as one that meets another connection's lock, fails the batch
            # here, as a failed add does above, instead of being committed as done.
            :ok =
              with :error <- Query.mark_snapshot_complete(conn, handle) do
                # `Query.mark_snapshot_complete` only returns `:error` if the query
                # modified 0 rows, i.e. the shape does not exist. Rather than crash
                # just warn as we can continue in this scenario.
                Logger.warning("Unable to mark snapshot complete: #{handle} does not exist")
              end
        end)

        :ok
      end,
      checkout_timeout
    )
  rescue
    e ->
      if Connection.lock_contention?(e) do
        {:error, :lock_contention}
      else
        Logger.error(
          "WriteBuffer batch write failed: #{Exception.format(:error, e, __STACKTRACE__)}"
        )

        {:error, Exception.message(e)}
      end
  catch
    :exit, {:timeout, {NimblePool, :checkout, _args}} ->
      {:error, :checkout_timeout}
  end
end
