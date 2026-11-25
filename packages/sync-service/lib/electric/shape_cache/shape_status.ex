defmodule Electric.ShapeCache.ShapeStatus do
  @moduledoc """
  Keeps track of shape state.

  Can recover basic persisted shape metadata from shape storage to repopulate
  the in-memory cache.

  The shape cache then loads this and starts processes (storage and consumer)
  for each `{shape_handle, %Shape{}}` pair. These then use their attached storage
  to recover the status information for the shape (snapshot xmin and latest
  offset).

  The ETS metadata table name is part of the config because we need to be able
  to access the data in the ETS from anywhere, so there's an internal api,
  using the full state and an external api using just the table name.
  """
  alias Electric.BootStep
  alias Electric.Replication.PublicationManager.RelationTracker
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.Telemetry.OpenTelemetry

  import Electric, only: [is_stack_id: 1, is_shape_handle: 1]

  require Logger

  @type stack_id() :: Electric.stack_id()
  @type shape_handle() :: Electric.shape_handle()
  @type shape_counts() :: %{
          total: non_neg_integer(),
          indexed: non_neg_integer(),
          unindexed: non_neg_integer()
        }

  @typedoc """
  The routing state a read of the stored shapes builds, one shape at a time with
  `add_to_routing_state/3`, for `restore_routing_state/2` to install.

  Its rows wait in an ETS table owned by the reading process. In the read's accumulator, every
  full-sweep garbage collection of that process would copy them again, one row per shape read so
  far.
  """
  @opaque routing_state() ::
            {shape_counts(), :ets.tid(), %{Electric.relation_id() => Electric.relation()}}

  # MUST be updated when Shape.comparable/1 changes.
  @version 10

  # Tuple format: {handle, comparable_digest, snapshot_started, last_read_time, generation}
  @shape_last_used_time_pos 4
  @shape_counts_key :counts
  # Present while the routing state waits for the collector's restore to build it.
  @routing_state_pending_key :routing_state_pending
  # The store's version as `initialize/1` read it into the meta table, until `refresh/1` takes it.
  @meta_read_version_key :meta_read_version

  @spec version() :: pos_integer()
  def version, do: @version

  @doc """
  Loads completed shape metadata for read-only serving before taking the replication lock.

  The database path is dependent on both `@version` above and
  `ShapeDb.Connection`'s `@schema_version`.

  A change to either of those, or to the OTP release, will result in an empty
  database.

  With `load?: false` it creates the tables and loads nothing: the catalog holds shapes that a
  purge still has to remove, and none of them may be served.
  """
  @spec initialize(stack_id(), keyword()) :: :ok | {:error, term()}
  def initialize(stack_id, opts \\ [])

  def initialize(stack_id, load?: false) when is_stack_id(stack_id) do
    create_shape_meta_table(stack_id)
    Logger.notice("Left the shape metadata unloaded: a purge of every shape is owed")
    :ok
  end

  def initialize(stack_id, _opts) when is_stack_id(stack_id) do
    create_shape_meta_table(stack_id)

    # Before the store is read below: from here on, a commit by any other connection moves it.
    read_version = ShapeDb.data_version(stack_id)

    BootStep.measure("shape_status.initialize.populate_shape_meta_table", fn ->
      populate_shape_meta_table(stack_id, 0, completed_only?: true)
    end)

    completed_count = :ets.info(shape_meta_table(stack_id), :size)
    Logger.notice("Loaded shape metadata for #{completed_count} completed shapes")

    if completed_count > 0 do
      # The collector's restore builds the routing state after the lock. Loading it here would
      # decode every stored shape twice.
      mark_routing_state_pending(stack_id)

      with {:ok, version} <- read_version,
           do: :ets.insert(shape_counts_table(stack_id), {@meta_read_version_key, version})
    end

    :ok
  end

  @doc """
  Refresh the shape meta table from SQLite without recreating it.

  Used on lock acquisition to pick up any shapes that were created/deleted
  by a previous instance while we were in read-only mode, and to reclaim
  storage left behind by a removal that never finished.

  Runs both steps before it returns. `Electric.ShapeCache.ShapeStatusOwner` runs them apart
  with `refresh_shape_meta/1` and `reclaim_orphaned_shape_storage/2`, so that the boot does not
  wait for the reclaim.
  """
  @spec refresh(stack_id()) :: :ok | {:error, term()}
  def refresh(stack_id) when is_stack_id(stack_id) do
    with {:ok, valid_shape_count} <- refresh_shape_meta(stack_id),
         do: reclaim_orphaned_shape_storage(stack_id, valid_shape_count)
  end

  @doc """
  The part of `refresh/1` that the boot needs before replication starts: the meta table and the
  routing state brought in line with the store. Returns the count of valid shapes, which
  `reclaim_orphaned_shape_storage/2` takes.
  """
  @spec refresh_shape_meta(stack_id(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def refresh_shape_meta(stack_id, opts \\ []) when is_stack_id(stack_id) do
    # only the first refresh after `initialize/1` can find the meta table as that read left it
    read_version = take_meta_read_version(stack_id)

    {:ok, invalid_handles, valid_shape_count} =
      BootStep.measure("shape_status.refresh.validate_existing_shapes", fn ->
        ShapeDb.validate_existing_shapes(stack_id)
      end)

    Logger.notice(
      "Refreshing shape metadata: #{valid_shape_count} valid shapes, #{length(invalid_handles)} invalid"
    )

    with :ok <-
           clean_invalid_storage(
             stack_id,
             invalid_handles,
             valid_shape_count,
             Keyword.get(opts, :initial?, false)
           ) do
      if meta_table_current?(stack_id, read_version, invalid_handles) do
        Logger.notice("Shape metadata unchanged in the store since it was read, kept as read")
      else
        # Use a generation counter to avoid clearing the table (which would race
        # with concurrent readers). Upsert all current shapes with a new generation,
        # then delete any entries still on the old generation.
        generation = System.unique_integer([:positive, :monotonic])

        BootStep.measure("shape_status.refresh.populate_shape_meta_table", fn ->
          populate_shape_meta_table(stack_id, generation)
        end)

        :ets.select_delete(shape_meta_table(stack_id), [
          {{:_, :_, :_, :_, :"$1"}, [{:"/=", :"$1", generation}], [true]}
        ])
      end

      # After the meta table: the routing state is brought in line with the handles it now holds.
      with :ok <-
             BootStep.measure("shape_status.refresh.refresh_shape_routing_state", fn ->
               refresh_shape_routing_state(stack_id)
             end),
           do: {:ok, valid_shape_count}
    end
  end

  # The first refresh happens before consumers start. An empty catalog at that point
  # means no storage can be reused, including storage from an older catalog version.
  defp clean_invalid_storage(stack_id, _invalid_handles, 0, true) do
    BootStep.measure("shape_status.refresh.cleanup_empty_storage", fn ->
      stack_id
      |> Electric.ShapeCache.Storage.for_stack()
      |> Electric.ShapeCache.Storage.cleanup_all!()
    end)

    :ok
  end

  defp clean_invalid_storage(stack_id, invalid_handles, _valid_shape_count, true) do
    Electric.ShapeCache.ShapeCleaner.remove_shape_storage_async(stack_id, invalid_handles)
  end

  defp clean_invalid_storage(stack_id, invalid_handles, _valid_shape_count, false) do
    Electric.ShapeCache.ShapeCleaner.remove_detached_shapes_async(stack_id, invalid_handles)
  end

  defp take_meta_read_version(stack_id) do
    case :ets.take(shape_counts_table(stack_id), @meta_read_version_key) do
      [{@meta_read_version_key, version}] -> version
      [] -> nil
    end
  end

  # A completed shape that became invalid must be removed from the table. Incomplete shapes
  # were skipped, so deleting those rows alone doesn't require another catalog walk.
  defp meta_table_current?(stack_id, {_conn, _version} = read_version, invalid_handles) do
    routing_state_pending?(stack_id) and
      Enum.all?(invalid_handles, &(not has_shape_handle?(stack_id, &1))) and
      ShapeDb.data_version(stack_id) == {:ok, read_version}
  end

  defp meta_table_current?(_stack_id, _read_version, _invalid_handles), do: false

  @doc """
  Reclaims the storage of shape directories that have no metadata row, left behind by a removal
  that never finished. Takes the valid shape count that `refresh_shape_meta/1` returned.

  It lists every shape directory: with a large shape count, under a second to seconds as warm or
  cold as the directory cache, and far longer on a slow disk. It is safe while the
  shape subsystem runs: see `served_by_live_consumer?/2` and `known_to_authoritative_catalog?/2`.
  """
  @spec reclaim_orphaned_shape_storage(stack_id(), non_neg_integer()) :: :ok | {:error, term()}
  def reclaim_orphaned_shape_storage(stack_id, valid_shape_count) when is_stack_id(stack_id) do
    BootStep.measure("shape_status.refresh.reclaim_orphaned_shape_storage", fn ->
      reclaim_orphans_from_storage(stack_id, valid_shape_count)
    end)
  end

  # The first post-lock refresh handles an empty store synchronously, before writers start.
  defp reclaim_orphans_from_storage(_stack_id, 0), do: :ok

  defp reclaim_orphans_from_storage(stack_id, _valid_shape_count) do
    storage = Electric.ShapeCache.Storage.for_stack(stack_id)

    case Electric.ShapeCache.Storage.get_all_stored_shape_handles(storage) do
      {:ok, stored_handles} ->
        known_handles =
          shape_meta_table(stack_id)
          |> :ets.select([{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
          |> MapSet.new()

        stored_handles
        |> MapSet.difference(known_handles)
        |> MapSet.to_list()
        |> Enum.reject(&served_by_live_consumer?(stack_id, &1))
        |> reject_known_to_authoritative_catalog(stack_id)
        |> reclaim_orphans(stack_id)

      {:error, reason} ->
        Logger.error(
          "Could not enumerate stored shapes, orphaned shape storage not reclaimed: " <>
            inspect(reason)
        )

        :ok
    end
  end

  # A shape directory exists only because a consumer created it, so a live consumer means the
  # handle's row is in flight rather than missing. The reclaim runs while the boot goes on (see
  # `ShapeStatusOwner`), and a Connection.Manager restart reaches lock acquisition with the shape
  # subsystem still running (connection/manager/supervisor.ex:35), so absence from the meta table
  # -- rebuilt from SQLite alone, which the write buffer lags -- is not on its own evidence of a
  # leak. A shape created while the listing runs gets its meta row before its directory, and the
  # meta table is read after the listing, so a handle the listing finds and the table lacks was
  # removed or never had a row.
  defp served_by_live_consumer?(stack_id, shape_handle),
    do: not is_nil(Electric.Shapes.Consumer.whereis(stack_id, shape_handle))

  # The meta table is rebuilt from `reduce_shape_meta`, which reads persisted SQLite alone and
  # ignores the write buffer, so a shape whose create landed in the buffer after `refresh/1`'s
  # flush is missing from the table while its create is still in flight. `handle_exists?` consults
  # the buffer before SQLite, telling a shape genuinely removed from the catalog (buffer tombstone
  # or absent everywhere) apart from one missing only because this rebuild read a lagging snapshot;
  # reclaiming the latter's directory would strand a live shape with no data.
  #
  # The check waits for the store as long as the boot's own reads do: it runs while the boot's
  # restores hold the single connection of an exclusive-mode store for a whole walk, past the 10 s
  # default. A store still busy after that leaves the orphans to the next lock acquisition, rather
  # than take down the process that owns the shape tables, and the stack with it.
  defp reject_known_to_authoritative_catalog(shape_handles, stack_id) do
    Enum.reject(shape_handles, &known_to_authoritative_catalog?(stack_id, &1))
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} = reason ->
      Logger.warning(
        "Shape store busy, orphaned shape storage not reclaimed until the next lock acquisition: " <>
          inspect(reason)
      )

      []
  end

  defp known_to_authoritative_catalog?(stack_id, shape_handle),
    do: ShapeDb.handle_exists?(stack_id, shape_handle, ShapeDb.boot_read_timeout())

  defp reclaim_orphans([], _stack_id), do: :ok

  defp reclaim_orphans(orphan_handles, stack_id) do
    Logger.notice(
      "Reclaiming #{length(orphan_handles)} orphaned shape directories with no metadata row"
    )

    Electric.ShapeCache.ShapeCleaner.remove_shape_storage_async(stack_id, orphan_handles)
  end

  @spec add_shape(stack_id(), Shape.t()) :: {:ok, shape_handle()} | {:error, term()}
  def add_shape(stack_id, shape) when is_stack_id(stack_id) do
    OpenTelemetry.with_child_span("shape_status.add_shape", [], stack_id, fn ->
      {_, shape_handle} = Shape.generate_id(shape)
      indexed? = Filter.indexed_shape?(shape)
      {{oid, relation}, with_generated_cols?} = RelationTracker.publication_filter(shape)

      # Add the lookup last as it is the one that enables clients to find the shape
      with {:ok, shape_digest} <- ShapeDb.add_shape(stack_id, shape, shape_handle) do
        if :ets.insert_new(
             shape_meta_table(stack_id),
             # Generation 0 is safe here: add_shape only runs in active mode,
             # and refresh/1 (which sweeps by generation) only runs before active.
             # They are sequentially ordered by the Connection.Manager state machine.
             {shape_handle, shape_digest, false, nil, 0}
           ) do
          # the name first: a row the routing state holds can always find it
          root_table = root_table_ref(stack_id, oid, relation)

          :ets.insert(
            shape_indexability_table(stack_id),
            {shape_handle, indexed?, root_table, with_generated_cols?}
          )

          increment_shape_counts(stack_id, indexed?)
          {:ok, shape_handle}
        else
          {:error, "duplicate shape #{inspect(shape_handle)}: #{inspect(shape)}"}
        end
      end
    end)
  end

  @spec list_shapes(stack_id()) :: [{shape_handle(), Shape.t()}]
  def list_shapes(stack_id) when is_stack_id(stack_id) do
    OpenTelemetry.with_child_span("shape_status.list_shapes", [], stack_id, fn ->
      stack_id
      |> ShapeDb.list_shapes!()
      |> topological_sort()
    end)
  end

  @spec topological_sort([{shape_handle(), Shape.t()}]) :: [{shape_handle(), Shape.t()}]
  defp topological_sort(handles_and_shapes, acc \\ [], visited \\ MapSet.new())
  defp topological_sort([], acc, _visited), do: Enum.reverse(acc) |> List.flatten()

  defp topological_sort(handles_and_shapes, acc, visited) do
    {appendable, missing_deps} =
      Enum.split_with(handles_and_shapes, fn {_, shape} ->
        Enum.all?(shape.shape_dependencies_handles, &MapSet.member?(visited, &1))
      end)

    case appendable do
      # Nothing left can be ordered, so nothing ever will be: what remains depends on a handle no
      # shape in this store holds, or on a cycle. Restore skips such a shape and says so.
      [] ->
        Enum.reverse([handles_and_shapes | acc]) |> List.flatten()

      _ ->
        topological_sort(
          missing_deps,
          [appendable | acc],
          MapSet.union(MapSet.new(appendable, &elem(&1, 0)), visited)
        )
    end
  end

  def reduce_shapes(stack_id, acc, reducer_fun)
      when is_stack_id(stack_id) and is_function(reducer_fun, 2) do
    ShapeDb.reduce_shapes(stack_id, acc, reducer_fun)
  end

  @doc "See `ShapeDb.reduce_encoded_shapes/3`."
  def reduce_encoded_shapes(stack_id, acc, reducer_fun)
      when is_stack_id(stack_id) and is_function(reducer_fun, 2) do
    ShapeDb.reduce_encoded_shapes(stack_id, acc, reducer_fun)
  end

  @doc """
  Folds over the publication filter of every shape, as `RelationTracker.publication_filter/1`
  computes it.

  The routing state holds what it takes for each shape, worked out when the shape is added or when the collector's
  restore reads the store, so this reads neither the store nor its connection. During a boot, wait for that restore
  with `await_routing_state/1` first.

  On a table whose shapes carry two names, the shape with the lowest handle comes first.
  """
  @spec reduce_publication_filters(
          stack_id(),
          acc,
          ({shape_handle(), RelationTracker.publication_filter()}, acc -> acc)
        ) ::
          acc
        when acc: term()
  def reduce_publication_filters(stack_id, acc, reducer_fun)
      when is_stack_id(stack_id) and is_function(reducer_fun, 2) do
    names = shape_relation_names_table(stack_id)
    rows = shape_indexability_table(stack_id)

    reduce = fn
      {shape_handle, _indexed?, {_oid, _relation} = oid_relation, with_generated_cols?}, acc ->
        reducer_fun.({shape_handle, {oid_relation, with_generated_cols?}}, acc)

      {shape_handle, _indexed?, oid, with_generated_cols?}, acc ->
        case :ets.lookup(names, oid) do
          [{^oid, relation}] ->
            reducer_fun.({shape_handle, {{oid, relation}, with_generated_cols?}}, acc)

          # cleared by a reset since the row was read
          [] ->
            acc
        end
    end

    # The relation tracker keeps the first name it meets for a table, and the read of the stored
    # shapes it restored from met them in handle order. On a table renamed while shapes stored
    # under both names remain, that first name decides whether a restart finds the shapes stored
    # before the rename stale.
    case lowest_handle_rows_of_renamed_tables(rows) do
      [] ->
        :ets.foldl(reduce, acc, rows)

      firsts ->
        acc = Enum.reduce(firsts, acc, reduce)
        firsts = MapSet.new(firsts, &elem(&1, 0))

        :ets.foldl(
          fn row, acc ->
            if MapSet.member?(firsts, elem(row, 0)), do: acc, else: reduce.(row, acc)
          end,
          acc,
          rows
        )
    end
  end

  # A row names its table only when its shape named it otherwise than the table's first name:
  # cheap to look for, even with a large shape count.
  defp lowest_handle_rows_of_renamed_tables(rows) do
    case :ets.select(rows, [{{:_, :_, {:"$1", :_}, :_}, [], [:"$1"]}]) do
      [] ->
        []

      renamed ->
        renamed = Map.from_keys(renamed, true)

        :ets.foldl(
          fn {shape_handle, _indexed?, root_table, _with_generated_cols?} = row, firsts ->
            oid = root_table_oid(root_table)

            case firsts do
              _ when not is_map_key(renamed, oid) -> firsts
              %{^oid => {first, _, _, _}} when first < shape_handle -> firsts
              _ -> Map.put(firsts, oid, row)
            end
          end,
          %{},
          rows
        )
        |> Map.values()
    end
  end

  defp root_table_oid({oid, _relation}), do: oid
  defp root_table_oid(oid), do: oid

  @doc """
  Starts the routing state of a read of the stored shapes. See `restore_routing_state/2`.

  Only the calling process can install it, and its rows go once it is installed. Other processes
  can add to it through shares of it: see `share_routing_state/1`.
  """
  @spec new_routing_state() :: routing_state()
  def new_routing_state do
    rows = :ets.new(:shape_routing_rows, [:set, :public, write_concurrency: :auto])
    {empty_shape_counts(), rows, %{}}
  end

  @doc """
  A share of a routing state, for a read of the stored shapes spread over several processes: each
  one adds to a share of its own, and `join_routing_state/2` adds the shares back into the state.

  A table is named after the first shape added on it, so every shape on one table must go to the
  same share, in the order of the read, for the joined state to be the one a single read builds.
  """
  @spec share_routing_state(routing_state()) :: routing_state()
  def share_routing_state({_counts, rows, _names}), do: {empty_shape_counts(), rows, %{}}

  @spec join_routing_state(routing_state(), [routing_state()]) :: routing_state()
  def join_routing_state(state, shares) do
    Enum.reduce(shares, state, fn {share_counts, rows, share_names}, {counts, rows, names} ->
      {Map.merge(counts, share_counts, fn _key, count, share_count -> count + share_count end),
       rows, Map.merge(share_names, names)}
    end)
  end

  @spec add_to_routing_state(routing_state(), shape_handle(), Shape.t()) :: routing_state()
  def add_to_routing_state(state, shape_handle, %Shape{} = shape),
    do: add_to_routing_state(state, shape_handle, shape, Filter.indexed_shape?(shape))

  @doc """
  `add_to_routing_state/3` for a caller that knows already whether the filter indexes the shape,
  `Filter.indexed_shape?/1`.
  """
  @spec add_to_routing_state(routing_state(), shape_handle(), Shape.t(), boolean()) ::
          routing_state()
  def add_to_routing_state({counts, rows, names}, shape_handle, %Shape{} = shape, indexed?) do
    {{oid, relation}, with_generated_cols?} = RelationTracker.publication_filter(shape)
    names = Map.put_new(names, oid, relation)
    root_table = if Map.fetch!(names, oid) == relation, do: oid, else: {oid, relation}
    :ets.insert(rows, {shape_handle, indexed?, root_table, with_generated_cols?})

    {update_shape_counts(counts, indexed?, 1), rows, names}
  end

  # A table's name is kept once, as the first shape on it named it. A shape that names it
  # otherwise, as one added after a rename does, keeps its own name in its row: the relation
  # tracker restores each shape's filter under the name the shape was stored with, and that name
  # is how a restart finds a shape stored before the rename stale.
  defp root_table_ref(stack_id, oid, relation) do
    names = shape_relation_names_table(stack_id)

    if :ets.insert_new(names, {oid, relation}) or :ets.lookup(names, oid) == [{oid, relation}],
      do: oid,
      else: {oid, relation}
  end

  @doc """
  Installs the routing state the collector's restore builds from its read of the stored shapes.

  A boot reads the stored shapes once, in that restore, so `initialize/1` leaves the routing
  state pending for it. Once installed, `add_shape/2` and `remove_shape/2` keep it, and a restore
  that runs again, after the shape subsystem restarted, leaves it as it is.
  """
  @spec restore_routing_state(stack_id(), routing_state()) :: :ok
  def restore_routing_state(stack_id, {counts, rows, names}) when is_stack_id(stack_id) do
    if routing_state_pending?(stack_id) do
      # The counts first: from here on, a shape removed is counted down by whichever of
      # `remove_shape/2` and `forget_removed_shapes/2` takes its row.
      put_shape_counts(stack_id, counts)
      :ets.insert(shape_relation_names_table(stack_id), Map.to_list(names))
      :ets.insert(shape_indexability_table(stack_id), :ets.tab2list(rows))
      forget_removed_shapes(stack_id, rows)
      mark_routing_state_built(stack_id)
    end

    :ets.delete(rows)
    :ok
  end

  # A shape removed since the read had no row for `remove_shape/2` to take.
  defp forget_removed_shapes(stack_id, rows) do
    meta = shape_meta_table(stack_id)

    :ets.foldl(
      fn {shape_handle, _indexed?, _oid, _with_generated_cols?}, :ok ->
        if not :ets.member(meta, shape_handle) do
          decrement_shape_counts(stack_id, shape_cached_as_indexed?(stack_id, shape_handle))
        end

        :ok
      end,
      :ok,
      rows
    )
  end

  @doc """
  Waits for the routing state: at once unless a boot found shapes in the store, otherwise until
  the collector's restore installs it, or a reset empties the store.
  """
  @spec await_routing_state(stack_id()) :: :ok
  def await_routing_state(stack_id) when is_stack_id(stack_id) do
    table = shape_counts_table(stack_id)
    waiter = {:routing_state_waiter, self()}
    :ets.insert(table, {waiter})

    cond do
      routing_state_pending?(stack_id) -> receive_routing_state_built(stack_id)
      # still registered, so nothing is on its way
      :ets.take(table, waiter) != [] -> :ok
      true -> receive_routing_state_built(stack_id)
    end
  end

  defp receive_routing_state_built(stack_id) do
    receive do
      {:routing_state_built, ^stack_id} -> :ok
    end
  end

  @spec count_shapes(stack_id()) :: non_neg_integer()
  def count_shapes(stack_id) when is_stack_id(stack_id) do
    case shape_counts(stack_id) do
      %{total: total} -> total
      :error -> count_shapes_without_routing_state(stack_id)
    end
  end

  # Until the collector's restore installs the routing state, the meta table holds the handles it
  # will count. The store's own count would wait for that restore's read to give the store's
  # connection back.
  defp count_shapes_without_routing_state(stack_id) do
    if routing_state_pending?(stack_id),
      do: :ets.info(shape_meta_table(stack_id), :size),
      else: ShapeDb.count_shapes!(stack_id)
  end

  @spec shape_counts(stack_id()) :: shape_counts() | :error
  def shape_counts(stack_id) when is_stack_id(stack_id) do
    case :ets.lookup(shape_counts_table(stack_id), @shape_counts_key) do
      [{@shape_counts_key, total, indexed, unindexed}] ->
        %{total: total, indexed: indexed, unindexed: unindexed}

      [] ->
        :error
    end
  rescue
    ArgumentError -> :error
  end

  @spec list_shape_handles_for_relations(stack_id(), [Electric.oid_relation()]) :: [
          shape_handle()
        ]
  def list_shape_handles_for_relations(stack_id, relations) when is_stack_id(stack_id) do
    OpenTelemetry.with_child_span(
      "shape_status.list_shape_handles_for_relations",
      [],
      stack_id,
      fn ->
        ShapeDb.shape_handles_for_relations!(stack_id, relations)
      end
    )
  end

  @spec remove_shape(stack_id(), shape_handle()) :: :ok | {:error, term()}
  def remove_shape(stack_id, shape_handle) when is_stack_id(stack_id) do
    result = ShapeDb.remove_shape(stack_id, shape_handle)

    if result == :ok or result == {:error, {:enoshape, shape_handle}} do
      metadata = :ets.take(shape_meta_table(stack_id), shape_handle)

      case shape_cached_as_indexed?(stack_id, shape_handle) do
        nil -> :ok
        indexed? -> decrement_shape_counts(stack_id, indexed?)
      end

      if metadata == [], do: result, else: :ok
    else
      result
    end
  end

  # The in-memory tables go first: a read-only request serves only a shape they hold. A catalog
  # whose reset fails, on a full disk say, then serves none of the shapes it still lists, and the
  # failure is returned for the caller to retry.
  @spec reset(stack_id()) :: :ok | {:error, term()}
  def reset(stack_id) when is_stack_id(stack_id) do
    :ok = unload(stack_id)

    with :ok <- reset_catalog(stack_id) do
      put_shape_counts(stack_id, empty_shape_counts())
      mark_routing_state_built(stack_id)
    end
  end

  @doc """
  Empties the in-memory tables and leaves the catalog as it is. A read-only request serves only a
  shape they hold, so none of the catalog's shapes is served until a load or a reset.

  The routing state is left pending, as at boot: a relation tracker that restarts in between
  waits for the collector's restore, or a reset, where it would have restored an empty filter set
  and taken every table of the shapes kept out of the publication. The caller stops the shape
  subsystem first: a relation tracker that folded the tables as they were cleared took the ones it
  had not read yet out of the publication.
  """
  @spec unload(stack_id()) :: :ok
  def unload(stack_id) when is_stack_id(stack_id) do
    :ets.delete_all_objects(shape_meta_table(stack_id))
    :ets.delete_all_objects(shape_indexability_table(stack_id))
    :ets.delete_all_objects(shape_relation_names_table(stack_id))
    :ets.delete(shape_counts_table(stack_id), @meta_read_version_key)
    mark_routing_state_pending(stack_id)
    :ok
  end

  # A statement that fails comes back as an error, and a commit that fails as a raise.
  defp reset_catalog(stack_id) do
    ShapeDb.reset(stack_id)
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  @spec fetch_handle_by_shape(stack_id(), Shape.t()) :: {:ok, shape_handle()} | :error
  def fetch_handle_by_shape(stack_id, %Shape{} = shape) when is_stack_id(stack_id) do
    OpenTelemetry.with_child_span("shape_status.fetch_handle_by_shape", [], stack_id, fn ->
      ShapeDb.handle_for_shape(stack_id, shape)
    end)
  end

  @doc """
  Where as `fetch_handle_by_shape/2` *may* under high-write load return stale
  data -- not finding a shape that has been written -- due to SQLite's
  cross-connection durability when in WAL mode, where a connection reads
  against a snapshot of the data, this version does the lookup via the write
  connection, which is guaranteed to see all writes (SQLite connections can
  always see their own writes).

  This guarantees that will will return consistent restults at the cost of slower
  lookups as we're contending with access to the single write connection.
  """
  @spec fetch_handle_by_shape_critical(stack_id(), Shape.t()) :: {:ok, shape_handle()} | :error
  def fetch_handle_by_shape_critical(stack_id, %Shape{} = shape) when is_stack_id(stack_id) do
    OpenTelemetry.with_child_span(
      "shape_status.fetch_handle_by_shape_critical",
      [],
      stack_id,
      fn ->
        ShapeDb.handle_for_shape_critical(stack_id, shape)
      end
    )
  end

  @spec fetch_shape_by_handle(stack_id(), shape_handle()) :: {:ok, Shape.t()} | :error
  def fetch_shape_by_handle(stack_id, shape_handle)
      when is_stack_id(stack_id) and is_shape_handle(shape_handle) do
    OpenTelemetry.with_child_span("shape_status.fetch_shape_by_handle", [], stack_id, fn ->
      ShapeDb.shape_for_handle(stack_id, shape_handle)
    end)
  end

  @spec has_shape_handle?(stack_id(), shape_handle()) :: boolean()
  def has_shape_handle?(stack_id, shape_handle) do
    :ets.member(shape_meta_table(stack_id), shape_handle)
  end

  @spec shape_has_been_activated?(stack_id(), shape_handle()) :: boolean()
  def shape_has_been_activated?(stack_id, shape_handle) do
    last_used_timestamp =
      :ets.lookup_element(
        shape_meta_table(stack_id),
        shape_handle,
        @shape_last_used_time_pos,
        nil
      )

    not is_nil(last_used_timestamp)
  end

  @doc """
  Validate that a shape handle matches the shape definition by matching the shape's saved
  comparable digest against the provided shape's.

  This decides whether the caller's handle -- and so the log the caller is served -- may stand for
  the caller's shape definition, which makes it an identity comparison rather than a cache lookup:
  a false accept serves one shape's rows to a request carrying another shape's definition. It
  compares the collision-resistant digest for that reason, never `Shape.hash/1`, whose 2^27 range
  is small enough to collide on purpose.
  """
  @spec validate_shape_handle(stack_id(), shape_handle(), Shape.t()) :: :ok | :error
  def validate_shape_handle(stack_id, shape_handle, %Shape{} = shape)
      when is_stack_id(stack_id) do
    case :ets.lookup(shape_meta_table(stack_id), shape_handle) do
      [{^shape_handle, digest, _snapshot_started, _last_read, _gen}] ->
        if Shape.comparable_digest(shape) == digest, do: :ok, else: :error

      [] ->
        :error
    end
  end

  @spec mark_snapshot_started(stack_id(), shape_handle()) :: :ok | :error
  def mark_snapshot_started(stack_id, shape_handle) when is_stack_id(stack_id) do
    with true <- :ets.update_element(shape_meta_table(stack_id), shape_handle, {3, true}) do
      :ok
    else
      _ -> :error
    end
  rescue
    # The table goes with its owner while the stack restarts it, and every consumer with it. A
    # raise here, in a consumer told its snapshot started, would remove the shape by a cleanup
    # that outlives the restart. The table is rebuilt from the catalog, which the snapshot's
    # completion writes.
    error in ArgumentError ->
      if available?(stack_id), do: reraise(error, __STACKTRACE__), else: :error
  end

  def snapshot_started?(stack_id, shape_handle) do
    case :ets.lookup(shape_meta_table(stack_id), shape_handle) do
      [{^shape_handle, _hash, snapshot_started, _last_read, _gen}] -> snapshot_started
      [] -> false
    end
  end

  @spec mark_snapshot_complete(stack_id(), shape_handle()) :: :ok | :error
  def mark_snapshot_complete(stack_id, shape_handle) when is_stack_id(stack_id) do
    OpenTelemetry.with_child_span("shape_status.mark_snapshot_complete", [], stack_id, fn ->
      ShapeDb.mark_snapshot_complete(stack_id, shape_handle)
    end)
  end

  @doc """
  Updates the last read time for the given shape to the current time.
  """
  def update_last_read_time_to_now(stack_id, shape_handle) when is_stack_id(stack_id) do
    update_last_read_time(stack_id, shape_handle, System.monotonic_time())
  rescue
    # The table goes with its owner while the stack restarts it, and the shape cache stamps the
    # consumer it has just started then. A raise there would fail the routing of the change the
    # consumer was started for, and a table rebuilt from the catalog has no use for the stamp.
    error in ArgumentError ->
      if available?(stack_id), do: reraise(error, __STACKTRACE__), else: false
  end

  @doc """
  Sets the last read time for the given shape to the provided time.

  Used for tests, otherwise prefer `update_last_read_time_to_now/2`.
  """
  def update_last_read_time(stack_id, shape_handle, time) when is_stack_id(stack_id) do
    :ets.update_element(
      shape_meta_table(stack_id),
      shape_handle,
      {@shape_last_used_time_pos, time}
    )
  end

  def least_recently_used(_stack_id, 0) do
    {[], 0}
  end

  def least_recently_used(stack_id, shape_count) when is_stack_id(stack_id) do
    now = System.monotonic_time()
    table = shape_meta_table(stack_id)

    # Use :ets.foldl with gb_trees to efficiently maintain top N without copying
    # entire table into memory and without sorting on every iteration
    tree =
      :ets.foldl(
        fn
          # This shape has only just been created, so it's too young to be considered for
          # expiration.
          {_handle, _hash, _snapshot_started, nil, _gen}, tree ->
            tree

          {handle, _hash, _snapshot_started, last_read, _gen}, tree ->
            last_read_tuple = {last_read, handle}

            if :gb_trees.size(tree) < shape_count do
              # Insert into the tree until we reach the desired size
              :gb_trees.insert(last_read_tuple, true, tree)
            else
              # If entry being examined was used less recently than the
              # most recently used tracked entry in the tree so far, replace it
              {most_recent_tuple, _} = :gb_trees.largest(tree)

              if last_read_tuple < most_recent_tuple do
                tree = :gb_trees.delete(most_recent_tuple, tree)
                :gb_trees.insert(last_read_tuple, true, tree)
              else
                tree
              end
            end
        end,
        :gb_trees.empty(),
        table
      )

    # get a reversed iterator so the final handle list is in least- to
    # most-recently used order after the accumulator
    {handles, last_read} = lru_to_list(:gb_trees.iterator(tree, :reversed), [], nil)

    {handles, System.convert_time_unit(now - (last_read || now), :native, :second) / 60}
  end

  defp lru_to_list(iterator, handles, largest_last_read) do
    case :gb_trees.next(iterator) do
      {{last_read, handle}, _, iter} ->
        lru_to_list(iter, [handle | handles], largest_last_read || last_read)

      :none ->
        {handles, largest_last_read}
    end
  end

  @doc false
  # The table goes with the process that owns it, while the stack restarts it.
  @spec available?(stack_id()) :: boolean()
  def available?(stack_id), do: :ets.whereis(shape_meta_table(stack_id)) != :undefined

  @spec shape_meta_table(stack_id()) :: atom()
  defp shape_meta_table(stack_id),
    do: :"shape_meta_table:#{stack_id}"

  # The routing state, one row per shape: {handle, indexed?, root table, with_generated_cols?}.
  # The root table is its oid, or {oid, relation} for a shape that names it otherwise than the
  # table's row below.
  @spec shape_indexability_table(stack_id()) :: atom()
  defp shape_indexability_table(stack_id),
    do: :"shape_indexability_table:#{stack_id}"

  # The name of each root table the routing state refers to, {oid, relation}, as the first shape
  # on it named it: one row per table rather than a copy in every shape's row, which made the
  # routing state grow with the shape count rather than the table count.
  @spec shape_relation_names_table(stack_id()) :: atom()
  defp shape_relation_names_table(stack_id),
    do: :"shape_relation_names_table:#{stack_id}"

  @spec shape_counts_table(stack_id()) :: atom()
  defp shape_counts_table(stack_id),
    do: :"shape_counts_table:#{stack_id}"

  defp create_shape_meta_table(stack_id) do
    ensure_state_table(shape_meta_table(stack_id),
      read_concurrency: true,
      write_concurrency: :auto
    )

    ensure_state_table(shape_indexability_table(stack_id),
      read_concurrency: true,
      write_concurrency: :auto
    )

    ensure_state_table(shape_relation_names_table(stack_id), read_concurrency: true)

    ensure_state_table(shape_counts_table(stack_id),
      read_concurrency: true,
      write_concurrency: true
    )

    :ets.delete_all_objects(shape_meta_table(stack_id))
    :ets.delete_all_objects(shape_indexability_table(stack_id))
    :ets.delete(shape_counts_table(stack_id), @meta_read_version_key)
    put_shape_counts(stack_id, empty_shape_counts())
    mark_routing_state_built(stack_id)
  end

  defp mark_routing_state_pending(stack_id) do
    table = shape_counts_table(stack_id)
    :ets.insert(table, {@routing_state_pending_key})
    :ets.delete(table, @shape_counts_key)
  end

  defp mark_routing_state_built(stack_id) do
    table = shape_counts_table(stack_id)
    :ets.delete(table, @routing_state_pending_key)

    for [pid] <- :ets.match(table, {{:routing_state_waiter, :"$1"}}),
        :ets.take(table, {:routing_state_waiter, pid}) != [] do
      send(pid, {:routing_state_built, stack_id})
    end

    :ok
  end

  defp routing_state_pending?(stack_id) do
    :ets.member(shape_counts_table(stack_id), @routing_state_pending_key)
  rescue
    ArgumentError -> false
  end

  defp populate_shape_meta_table(stack_id, generation, opts \\ []) do
    start_time = System.monotonic_time()
    completed_only? = Keyword.get(opts, :completed_only?, false)

    # A refresh reads the catalog as the write buffer will leave it (see
    # `ShapeDb.validate_existing_shapes/2`). A shape whose removal is queued is gone. One whose
    # snapshot-complete mark is queued is complete: read as incomplete and without a consumer, it
    # is removed on its next request. Read before the walk, a write that commits meanwhile is in
    # one or the other; a removal queued once the walk has started is not, and the walk can read
    # its row back. A boot starts with none.
    {queued_removals, queued_completions} =
      if completed_only?,
        do: {MapSet.new(), MapSet.new()},
        else: {ShapeDb.queued_removals(stack_id), ShapeDb.queued_snapshot_completions(stack_id)}

    ShapeDb.reduce_shape_meta(
      stack_id,
      :ets.whereis(shape_meta_table(stack_id)),
      fn
        {_, _, false}, table when completed_only? ->
          table

        {handle, digest, snapshot_complete?}, table ->
          if MapSet.member?(queued_removals, handle) do
            table
          else
            snapshot_complete? = snapshot_complete? or MapSet.member?(queued_completions, handle)
            last_read = known_last_read(table, handle, start_time)
            true = :ets.insert(table, {handle, digest, snapshot_complete?, last_read, generation})
            table
          end
      end
    )

    :ok
  end

  # A refresh runs against a live table, where every entry already carries the read time its clients earned.
  # Overwriting those with one shared stamp costs the expiry tree the only signal it has: with the whole
  # store tied, `least_recently_used/2` falls back on the handle, whose leading component is a hash, and the
  # eviction picks names rather than idle shapes. A boot has nothing to keep — the table is empty and the
  # shared stamp stands — so only a takeover is repaired here.
  defp known_last_read(table, handle, default) do
    case :ets.lookup(table, handle) do
      [{^handle, _digest, _snapshot_started, last_read, _generation}] -> last_read
      [] -> default
    end
  end

  defp rebuild_shape_routing_state(stack_id) do
    {_counts, rows, _names} = routing_state = new_routing_state()

    try do
      case ShapeDb.reduce_shapes(stack_id, routing_state, fn {shape_handle, shape}, acc ->
             add_to_routing_state(acc, shape_handle, shape)
           end) do
        {:error, _reason} = error ->
          error

        {counts, ^rows, names} ->
          :ets.delete_all_objects(shape_indexability_table(stack_id))
          :ets.insert(shape_relation_names_table(stack_id), Map.to_list(names))
          true = :ets.insert(shape_indexability_table(stack_id), :ets.tab2list(rows))
          put_shape_counts(stack_id, counts)
          :ok
      end
    after
      # this runs in the long-lived ShapeStatusOwner, which a read that fails must not leave
      # holding the rows
      :ets.delete(rows)
    end
  end

  # Whether a shape is indexed follows from its definition, and a handle never comes to name
  # another definition, so what the boot worked out holds for every handle still stored. The meta
  # table has just been brought in line with the store. A handle it holds that the routing state
  # does not know was stored by another instance while this one waited for the lock, and only that
  # takes a read of the stored shapes, which decodes every one of them.
  defp refresh_shape_routing_state(stack_id) do
    # the collector's restore, still to come, builds it from the store as it stands then
    if routing_state_pending?(stack_id),
      do: :ok,
      else: refresh_built_shape_routing_state(stack_id)
  end

  defp refresh_built_shape_routing_state(stack_id) do
    meta = shape_meta_table(stack_id)
    indexability = shape_indexability_table(stack_id)

    unseen? =
      :ets.foldl(
        fn row, unseen? -> unseen? or not :ets.member(indexability, elem(row, 0)) end,
        false,
        meta
      )

    if unseen? do
      rebuild_shape_routing_state(stack_id)
    else
      {counts, gone} =
        :ets.foldl(
          fn {handle, indexed?, _oid, _with_generated_cols?}, {counts, gone} ->
            if :ets.member(meta, handle),
              do: {update_shape_counts(counts, indexed?, 1), gone},
              else: {counts, [handle | gone]}
          end,
          {empty_shape_counts(), []},
          indexability
        )

      Enum.each(gone, &:ets.delete(indexability, &1))
      put_shape_counts(stack_id, counts)
      :ok
    end
  end

  defp ensure_state_table(table_name, opts) do
    if :ets.whereis(table_name) == :undefined do
      :ets.new(table_name, [:named_table, :public, :set] ++ opts)
    end
  end

  defp empty_shape_counts do
    %{total: 0, indexed: 0, unindexed: 0}
  end

  defp put_shape_counts(stack_id, %{total: total, indexed: indexed, unindexed: unindexed}) do
    :ets.insert(shape_counts_table(stack_id), {@shape_counts_key, total, indexed, unindexed})
  end

  defp increment_shape_counts(stack_id, indexed?) do
    :ets.update_counter(
      shape_counts_table(stack_id),
      @shape_counts_key,
      [{2, 1}, {3, if(indexed?, do: 1, else: 0)}, {4, if(indexed?, do: 0, else: 1)}],
      {@shape_counts_key, 0, 0, 0}
    )
  end

  defp decrement_shape_counts(_stack_id, nil), do: :ok

  defp decrement_shape_counts(stack_id, indexed?) do
    :ets.update_counter(
      shape_counts_table(stack_id),
      @shape_counts_key,
      [{2, -1}, {3, if(indexed?, do: -1, else: 0)}, {4, if(indexed?, do: 0, else: -1)}],
      {@shape_counts_key, 0, 0, 0}
    )
  end

  defp shape_cached_as_indexed?(stack_id, shape_handle) do
    case :ets.take(shape_indexability_table(stack_id), shape_handle) do
      [{^shape_handle, indexed?, _oid, _with_generated_cols?}] -> indexed?
      [] -> nil
    end
  end

  defp update_shape_counts(%{total: total, indexed: indexed, unindexed: unindexed}, true, delta) do
    %{total: total + delta, indexed: indexed + delta, unindexed: unindexed}
  end

  defp update_shape_counts(%{total: total, indexed: indexed, unindexed: unindexed}, false, delta) do
    %{total: total + delta, indexed: indexed, unindexed: unindexed + delta}
  end
end
