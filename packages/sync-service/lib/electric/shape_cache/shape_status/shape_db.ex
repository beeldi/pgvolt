defmodule Electric.ShapeCache.ShapeStatus.ShapeDb do
  @moduledoc """
  SQLite-backed persistent storage for shape metadata.

  The WriteBuffer provides buffering for writes to prevent timeout cascades.
  Only `handle_for_shape` and `shape_for_handle` need buffer awareness since
  they are entry points for new requests. Other functions are called after
  ShapeStatus has already updated its ETS cache.
  """

  alias Electric.Shapes.Shape
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Query
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Statistics

  import Electric, only: [is_stack_id: 1, is_shape_handle: 1]

  import Connection,
    only: [
      checkout!: 3,
      checkout!: 4,
      checkout_write!: 3,
      checkout_write!: 4
    ]

  @type shape_handle() :: Electric.shape_handle()
  @type stack_id() :: Electric.stack_id()

  defmodule Error do
    defexception [:message, :error]

    @impl true
    def exception(args) do
      action = Keyword.get(args, :action, :read)
      {:ok, error} = Keyword.fetch(args, :error)
      %__MODULE__{message: "ShapeDb #{action} failed: #{inspect(error)}", error: error}
    end
  end

  def add_shape(stack_id, %Shape{} = shape, shape_handle)
      when is_stack_id(stack_id) and is_shape_handle(shape_handle) do
    {comparable_shape, shape_hash} = Shape.comparable_hash(shape)
    relations = Shape.list_relations(shape)

    with :ok <-
           WriteBuffer.add_shape(
             stack_id,
             shape_handle,
             shape,
             comparable_shape,
             shape_hash,
             relations
           ) do
      {:ok, Shape.digest_comparable(comparable_shape)}
    end
  end

  def remove_shape(stack_id, shape_handle) when is_stack_id(stack_id) do
    if handle_exists?(stack_id, shape_handle) do
      WriteBuffer.remove_shape(stack_id, shape_handle)
    else
      {:error, {:enoshape, shape_handle}}
    end
  end

  def mark_snapshot_complete(stack_id, shape_handle) do
    if handle_exists?(stack_id, shape_handle) do
      WriteBuffer.queue_snapshot_complete(stack_id, shape_handle)
    else
      :error
    end
  end

  def reset(stack_id) when is_stack_id(stack_id) do
    WriteBuffer.clear(stack_id)
    checkout_write!(stack_id, :reset, &Query.reset/1)
  end

  @doc """
  Find a handle for a shape. Checks buffer first, then SQLite.
  Returns :error if the handle is tombstoned (being deleted).
  """
  def handle_for_shape(stack_id, %Shape{} = shape) when is_stack_id(stack_id) do
    checkout_fun = &checkout!(stack_id, :handle_for_shape, &1)
    handle_for_shape_inner(stack_id, shape, checkout_fun)
  end

  @doc """
  Find a handle for a shape using the write connection to guarantee consistency.
  """
  def handle_for_shape_critical(stack_id, %Shape{} = shape, timeout \\ 10_000)
      when is_stack_id(stack_id) do
    # The writer without a write transaction: a lookup has nothing to commit, and in exclusive
    # mode a commit waits for every read lock another process holds on the file.
    checkout_fun =
      &Connection.checkout!(stack_id, :handle_for_shape_critical, &1, timeout, :write)

    handle_for_shape_inner(stack_id, shape, checkout_fun)
  end

  defp handle_for_shape_inner(stack_id, %Shape{} = shape, checkout_fun) do
    {comparable_shape, _shape_hash} = Shape.comparable_hash(shape)

    case WriteBuffer.lookup_handle(stack_id, comparable_shape) do
      {:ok, handle} ->
        {:ok, handle}

      :not_found ->
        removals = WriteBuffer.committed_removals(stack_id)

        case checkout_fun.(&Query.handle_for_shape(&1, comparable_shape)) do
          {:ok, handle} ->
            cond do
              WriteBuffer.is_tombstoned?(stack_id, handle) ->
                :error

              WriteBuffer.committed_removals(stack_id) != removals ->
                handle_for_shape_inner(stack_id, shape, checkout_fun)

              true ->
                {:ok, handle}
            end

          :error ->
            :error
        end
    end
  end

  @doc """
  Find a shape by its handle. Checks buffer first, then SQLite.
  Returns :error if the handle is tombstoned (being deleted).
  """
  def shape_for_handle(stack_id, shape_handle) when is_stack_id(stack_id) do
    case WriteBuffer.lookup_shape(stack_id, shape_handle) do
      {:ok, shape} ->
        {:ok, shape}

      :not_found ->
        removals = WriteBuffer.committed_removals(stack_id)

        if WriteBuffer.is_tombstoned?(stack_id, shape_handle) do
          :error
        else
          case checkout!(stack_id, :shape_for_handle, &Query.shape_for_handle(&1, shape_handle)) do
            {:ok, shape} ->
              # Re-check tombstone after SQLite read to avoid race where
              # shape was tombstoned between the first check and SQLite query
              cond do
                WriteBuffer.is_tombstoned?(stack_id, shape_handle) ->
                  :error

                WriteBuffer.committed_removals(stack_id) != removals ->
                  shape_for_handle(stack_id, shape_handle)

                true ->
                  {:ok, shape}
              end

            :error ->
              :error
          end
        end
    end
  end

  def list_shapes(stack_id) when is_stack_id(stack_id) do
    buffered = WriteBuffer.list_buffered_shapes(stack_id)
    tombstones = WriteBuffer.tombstoned_handles(stack_id)

    case checkout!(stack_id, :list_shapes, &Query.list_shapes/1, boot_read_timeout()) do
      {:ok, sqlite_shapes} ->
        shapes =
          buffered
          |> Stream.concat(
            Stream.reject(sqlite_shapes, fn {handle, _shape} ->
              MapSet.member?(tombstones, handle)
            end)
          )
          # Deduplicate to handle race between buffer flush and SQLite read
          |> Enum.uniq_by(fn {handle, _} -> handle end)

        {:ok, shapes}

      error ->
        error
    end
  end

  @doc """
  The stored shapes after `after_handle` in handle order, at most `limit` of them, without the ones
  removed and not yet flushed, and the last handle the page read, `nil` past the last one. A shape
  still in the write buffer is in no page: it was added since the last flush.
  """
  def list_shapes_page_after(stack_id, after_handle, limit)
      when is_stack_id(stack_id) and is_binary(after_handle) and is_integer(limit) and limit > 0 do
    case checkout!(
           stack_id,
           :list_shapes_page_after,
           &Query.list_shapes_page_after(&1, after_handle, limit)
         ) do
      {:ok, rows} ->
        # Read after the page, so a removal queued while the page was read still drops its row.
        tombstones = WriteBuffer.tombstoned_handles(stack_id)
        last_handle = with {handle, _shape} <- List.last(rows), do: handle

        {:ok, Enum.reject(rows, fn {handle, _shape} -> MapSet.member?(tombstones, handle) end),
         last_handle}

      error ->
        error
    end
  end

  def list_shapes!(stack_id) when is_stack_id(stack_id) do
    stack_id |> list_shapes() |> raise_on_error!(:list_shapes)
  end

  def shape_handles_for_relations(stack_id, relations) when is_stack_id(stack_id) do
    buffered_handles = WriteBuffer.handles_for_relations(stack_id, relations)
    tombstones = WriteBuffer.tombstoned_handles(stack_id)

    case checkout!(
           stack_id,
           :shape_handles_for_relations,
           &Query.shape_handles_for_relations(&1, relations)
         ) do
      {:ok, sqlite_handles} ->
        filtered_sqlite = Enum.reject(sqlite_handles, &MapSet.member?(tombstones, &1))
        {:ok, Enum.uniq(buffered_handles ++ filtered_sqlite)}

      error ->
        error
    end
  end

  def shape_handles_for_relations!(stack_id, relations) when is_stack_id(stack_id) do
    stack_id
    |> shape_handles_for_relations(relations)
    |> raise_on_error!(:shape_handles_for_relations)
  end

  def reduce_shapes(stack_id, acc, reducer_fun) when is_function(reducer_fun, 2) do
    buffered = WriteBuffer.list_buffered_shapes(stack_id)
    tombstones = WriteBuffer.tombstoned_handles(stack_id)
    buffered_handles = MapSet.new(buffered, fn {handle, _shape} -> handle end)
    heap_floor = raise_heap_floor()

    try do
      checkout!(
        stack_id,
        :reduce_shapes,
        fn %Connection{} = conn ->
          conn
          |> Query.list_shape_stream()
          |> Stream.reject(fn {handle, _shape} ->
            MapSet.member?(tombstones, handle) or MapSet.member?(buffered_handles, handle)
          end)
          |> Enum.reduce(Enum.reduce(buffered, acc, reducer_fun), reducer_fun)
        end,
        boot_read_timeout()
      )
    after
      reset_heap_floor(heap_floor)
    end
  rescue
    error in Error -> {:error, error}
  end

  @doc """
  Folds over the stored shapes as `reduce_shapes/3` does, each one still encoded and with the oid
  of its root table: `{handle, oid, binary}`, the oid `nil` for a shape the store files under no
  table. A shape not yet written from the buffer comes decoded: `{handle, oid, %Shape{}}`.
  """
  def reduce_encoded_shapes(stack_id, acc, reducer_fun) when is_function(reducer_fun, 2) do
    buffered = WriteBuffer.list_buffered_shapes(stack_id)
    tombstones = WriteBuffer.tombstoned_handles(stack_id)
    buffered_handles = MapSet.new(buffered, fn {handle, _shape} -> handle end)

    checkout!(
      stack_id,
      :reduce_encoded_shapes,
      fn %Connection{} = conn ->
        acc =
          Enum.reduce(buffered, acc, fn {handle, shape}, acc ->
            reducer_fun.({handle, shape.root_table_id, shape}, acc)
          end)

        conn
        |> Query.list_encoded_shape_stream()
        |> Stream.reject(fn {handle, _oid, _shape} ->
          MapSet.member?(tombstones, handle) or MapSet.member?(buffered_handles, handle)
        end)
        |> Enum.reduce(acc, reducer_fun)
      end,
      boot_read_timeout()
    )
  rescue
    error in Error -> {:error, error}
  end

  # The walk decodes every stored shape on the heap of the process that asked for it, and the
  # boot restores that ask keep what they build on that same heap. A decoded shape is larger than
  # the young heap a process starts with, so the walk collected garbage once every other shape,
  # collections, each collection copying a share of the caller's growing state: in a synthetic
  # 100,000-shape benchmark, 44,800 collections took 26 s of a 41 s filter restore. With room for
  # a few hundred shapes between two collections it dropped to 376 collections and 1 s.
  @walk_heap_words 8 * 1024 * 1024

  defp raise_heap_floor do
    for flag <- [:min_heap_size, :min_bin_vheap_size] do
      previous = Process.flag(flag, @walk_heap_words)
      # a floor configured above this one stays
      if previous > @walk_heap_words, do: Process.flag(flag, previous)
      {flag, previous}
    end
  end

  defp reset_heap_floor(previous) do
    Enum.each(previous, fn {flag, words} -> Process.flag(flag, words) end)
  end

  # Only used during boot when buffer is empty
  def reduce_shape_meta(stack_id, acc, reducer_fun) when is_function(reducer_fun, 2) do
    checkout!(
      stack_id,
      :reduce_shape_meta,
      fn %Connection{} = conn ->
        conn
        |> Query.list_shape_meta_stream()
        |> Enum.reduce(acc, reducer_fun)
      end,
      boot_read_timeout()
    )
  end

  # Boot restores (shape meta, routing state, publication filters, log collector)
  # each hold a read connection for as long as it takes to walk every shape, and
  # in exclusive mode they all queue on the single write connection. The 10s
  # checkout default is a queueing deadline, not a query deadline, so at scale the
  # restores time each other out and the stack never converges.
  @doc false
  def boot_read_timeout, do: Electric.Config.get_env(:shape_db_boot_read_timeout)

  # Each attempt waits out the busy timeout (2 s) before it fails.
  @validation_timeout 60_000
  # As long as a write waits for the connection by default.
  @validation_last_checkout_timeout 10_000

  # May be slightly inaccurate during concurrent modifications due to
  # the `pending_count_diff` being updated after writes are in the database
  # meaning that changes may be counted twice for a (very) short period.
  # The boot path passes `boot_read_timeout()`: it queues behind restores that now hold the
  # connection for a whole shape walk instead of self-expiring at 10s. Steady-state callers -- the
  # 5s telemetry poller, the expiry manager -- keep the short default, where a slow count must fail
  # fast rather than block.
  def count_shapes(stack_id, timeout \\ 10_000) do
    case checkout!(stack_id, :count_shapes, &Query.count_shapes/1, timeout) do
      {:ok, sqlite_count} -> {:ok, sqlite_count + WriteBuffer.pending_count_diff(stack_id)}
      error -> error
    end
  end

  def count_shapes!(stack_id) do
    try do
      stack_id |> count_shapes() |> raise_on_error!(:count_shapes)
    rescue
      # the connection pool has its own registry, so attempting to checkout a
      # connection will raise an ArgumentError if that registry isn't running
      ArgumentError -> :error
    catch
      # connection pool has not started
      :exit, {:noproc, {NimblePool, :checkout, _args}} -> :error
    end
  end

  @doc false
  def handle_exists?(stack_id, shape_handle, timeout \\ 10_000) when is_stack_id(stack_id) do
    case WriteBuffer.has_handle?(stack_id, shape_handle) do
      true ->
        true

      false ->
        false

      :unknown ->
        case checkout!(stack_id, :handle_exists?, &read_handle_exists(&1, shape_handle), timeout) do
          {:error, code} -> raise Error, error: code, action: :handle_exists?
          exists? -> exists?
        end
    end
  end

  # A raise inside the checkout makes the pool close the connection and open a new one, which in
  # exclusive mode reruns the boot integrity check on the only connection, and a check that fails
  # on anything but a lock deletes the store. Only a read that found the file damaged says that of
  # the file, and raises inside. Any other failed step, one that met another connection's lock or
  # an I/O error SQLite recovers from, was reset, so the connection goes back to the pool and the
  # raise happens after.
  defp read_handle_exists(conn, shape_handle) do
    case Query.handle_exists?(conn, shape_handle) do
      {:error, code} = error ->
        if Connection.corruption?(code),
          do: raise(Error, error: code, action: :handle_exists?),
          else: error

      exists? ->
        exists?
    end
  end

  # Another process holding the file, such as an instance sharing the storage running its boot
  # integrity check, fails the flush and the validation's commit until it lets go.
  #
  # A shape whose snapshot-complete mark the flush left in the buffer reads as incomplete in the
  # catalog, so validation keeps every shape with a queued mark, whatever the flush returned,
  # unless its removal is queued too. Validation removes an invalid shape whose removal is queued
  # itself, and reports it like any other, so its storage is cleaned up even if the queued removal
  # never commits.
  #
  # The count is taken under the same connection: the catalog's rows plus each addition the buffer
  # still holds. A count of 0 on the first refresh clears every shape's storage, so the count is an
  # upper bound, 0 only when the catalog has no row and the buffer no addition. It keeps a shape
  # whose removal is queued, which may yet fail and leave the row, and never reads the buffer's
  # counters, which can still debit a removal the catalog has applied: the buffer takes a commit
  # off them only once it lets the connection go. An addition committed before validation's
  # checkout and not cleared from the buffer yet counts twice, or once if validation removed it.
  #
  # The commit is retried while the lock lasts, until `timeout` after validation started, flush
  # included, and then raises. An attempt waits for the connection until then, and at least 10 s.
  # Replication resumes only after validation, and the connection manager waits for it even to
  # shut down.
  def validate_existing_shapes(stack_id, timeout \\ @validation_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    # Must flush buffer before validating so we check persisted state. A flush can wait for the
    # connection longer than a call's default timeout. One that outlasts the whole bound leaves
    # what it has not committed queued, which validation allows for.
    try do
      WriteBuffer.flush_sync(stack_id, timeout)
    catch
      :exit, {:timeout, {GenServer, :call, _}} -> :ok
    end

    validate_persisted_shapes(stack_id, deadline)
  end

  defp validate_persisted_shapes(stack_id, deadline) do
    with {:ok, {removed_handles, count}} <-
           checkout_write!(
             stack_id,
             :validate_existing_shapes,
             fn %Connection{} = conn ->
               full_xid_enabled? =
                 "full_xid_source_positions" in Electric.StackConfig.lookup(
                   stack_id,
                   :feature_flags,
                   []
                 )

               with {:ok, handles} <- Query.select_invalid(conn, full_xid_enabled?) do
                 # Read under the write connection, which the buffer commits through.
                 kept =
                   MapSet.difference(
                     WriteBuffer.queued_snapshot_completions(stack_id),
                     WriteBuffer.tombstoned_handles(stack_id)
                   )

                 handles = Enum.reject(handles, &MapSet.member?(kept, &1))

                 Enum.each(handles, fn handle ->
                   :ok = Query.remove_shape(conn, handle)
                 end)

                 with {:ok, rows} <- Query.count_shapes(conn) do
                   {:ok, {handles, rows + WriteBuffer.buffered_addition_count(stack_id)}}
                 end
               end
             end,
             # a queueing deadline, which the work under the connection does not count against
             max(
               deadline - System.monotonic_time(:millisecond),
               @validation_last_checkout_timeout
             )
           ) do
      {:ok, removed_handles, count}
    end
  rescue
    e ->
      if Connection.lock_contention?(e) and System.monotonic_time(:millisecond) < deadline,
        do: validate_persisted_shapes(stack_id, deadline),
        else: reraise(e, __STACKTRACE__)
  end

  @doc "Handles whose snapshot-complete mark the write buffer has not committed yet"
  defdelegate queued_snapshot_completions(stack_id), to: WriteBuffer

  @doc "Handles whose removal the write buffer has not committed yet"
  defdelegate queued_removals(stack_id), to: WriteBuffer, as: :tombstoned_handles

  @doc """
  A token that stays the same for as long as no other connection commits to the store: SQLite's
  `PRAGMA data_version` on the write connection, with that connection. A write connection closed
  and opened again in between gives another token, whatever the store went through.
  """
  @spec data_version(stack_id()) :: {:ok, term()} | :error | {:error, term()}
  def data_version(stack_id) when is_stack_id(stack_id) do
    # Use the single writer for comparable versions, without opening a write transaction.
    Connection.checkout!(
      stack_id,
      :data_version,
      fn %Connection{conn: conn} ->
        with {:ok, [version]} <- Connection.fetch_one(conn, "PRAGMA data_version", []),
             do: {:ok, {conn, version}}
      end,
      boot_read_timeout(),
      :write
    )
  end

  def snapshot_complete?(stack_id, shape_handle) do
    checkout!(stack_id, :snapshot_complete?, &Query.snapshot_complete?(&1, shape_handle))
  end

  def explain(stack_id) do
    Connection.explain(stack_id)

    :ok
  end

  @doc "Returns the number of pending writes in the buffer"
  @spec pending_buffer_size(stack_id()) :: non_neg_integer()
  def pending_buffer_size(stack_id) when is_stack_id(stack_id) do
    WriteBuffer.pending_operations_count(stack_id)
  end

  defp raise_on_error!({:ok, result}, _action), do: result

  defp raise_on_error!({:error, reason}, action) do
    raise Error, error: reason, action: action
  end

  def statistics(stack_id) do
    Statistics.current(stack_id)
  end
end
