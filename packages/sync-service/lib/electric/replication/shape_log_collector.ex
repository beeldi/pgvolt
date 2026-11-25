defmodule Electric.Replication.ShapeLogCollector do
  @moduledoc """
  The ShapeLogCollector is responsible for collecting and processing
  shape log operations and managing shape registrations.

  It consists of two main components: the processor and the RequestBatcher.

  The processor handles the processing of shape log operations
  and manages the shape matching index updates. When any txn comes from postgres,
  we need to store it into the log for this shape if and only if it has
  txid >= xmin of the snapshot.

  The RequestBatcher batches the registration and deregistration of shapes
  to avoid overwhelming the processor with frequent updates.
  """
  use GenServer

  alias Electric.BootStep
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.ShapeLogCollector.FlushTracker
  alias Electric.LsnTracker
  alias Electric.Replication.ShapeLogCollector.AffectedColumns
  alias Electric.Replication.ShapeLogCollector.Restore
  alias Electric.Postgres.Lsn
  alias Electric.Replication.PersistentReplicationState
  alias Electric.Postgres.Inspector
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Replication.Eval.Env
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.TxnJournal

  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes.DependencyLayers
  alias Electric.Shapes.EventRouter
  alias Electric.Shapes.FragmentAssembler
  alias Electric.Shapes.Partitions
  alias Electric.Shapes.RouterWorker
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.Shapes.ConsumerRegistry

  import Electric.Utils, only: [map_while_ok: 2, map_if_ok: 2]

  require Electric.Replication.LogOffset
  require Logger
  require TransactionFragment

  @schema NimbleOptions.new!(
            stack_id: [type: :string, required: true],
            inspector: [type: :mod_arg, required: true],
            persistent_kv: [type: :any, required: true],
            consumer_registry_opts: [type: :any],
            router_shards: [type: :pos_integer, required: false],
            shared_conversion: [type: :boolean, required: false],
            shared_payload_arena: [type: :boolean, required: false],
            payload_arena_reclaim_min_bytes: [type: :non_neg_integer, required: false],
            worker_prepare: [type: :boolean, required: false],
            shared_fragment_blocks: [type: :boolean, required: false],
            ack_after_journal: [type: :boolean, required: false],
            gather_stall_timeout: [type: :pos_integer, required: false]
          )

  @consumer_cleanup_reason Electric.ShapeCache.ShapeCleaner.consumer_cleanup_reason()

  # Exit reasons that read the same for a supervisor teardown and for a lost writer: the writer's
  # shape is kept and its flush entry pinned for the stall check (see handle_writer_down/3).
  @ambiguous_exit_reasons [:shutdown, :killed, :noproc]

  # How often to scan the FlushTracker for entries that have made no flush progress
  # past the grace period (see the :flush_stall_grace_period stack config value).
  @stall_check_interval 10_000
  @stall_check_interval_floor 1_000

  @max_router_shards 255

  defguardp is_ready_to_process(state)
            when is_map_key(state, :last_processed_offset) and
                   not is_nil(state.last_processed_offset)

  def start_link(opts) do
    with {:ok, opts} <- NimbleOptions.validate(Map.new(opts), @schema) do
      stack_id = opts[:stack_id]

      GenServer.start_link(__MODULE__, opts,
        name: name(stack_id),
        spawn_opt: Electric.StackConfig.spawn_opts(stack_id, :shape_log_collector)
      )
    end
  end

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  @doc """
  Marks the collector as ready to process operations from
  the replication stream.

  This is typically called after the initial shape registrations
  have been processed.
  """
  @spec mark_as_ready(Electric.stack_id()) :: :ok
  def mark_as_ready(stack_id) do
    # use an infinite timeout because the call can come in while the filters are building
    # the time taken to build the filters depends on the number of shapes so there is no
    # known upper bound for this after which we can say "this is taking too long"
    GenServer.call(name(stack_id), :mark_as_ready, :infinity)
  end

  @doc """
  The offset of the last transaction fragment the collector accepted, nil when it accepted none
  since it started or when no collector runs. An error means the collector did not answer, so
  what it holds is unknown.
  """
  @spec last_processed_offset(Electric.stack_id()) ::
          {:ok, LogOffset.t() | nil} | {:error, term()}
  def last_processed_offset(stack_id) do
    case GenServer.whereis(name(stack_id)) do
      nil -> {:ok, nil}
      pid -> {:ok, GenServer.call(pid, :last_processed_offset, 30_000)}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  @doc """
  Handles a replication log events.

  Should be called with operations received from the replication stream.

  Uuse `GenServer.call/2` here to make the event processing synchronous.

  This `call/3` has a timeout of `:infinity` because timeouts are
  handled at the storage layer, that is this function doesn't
  assume any aggregate max time for the shape consumers to actually commit
  the new txn to disk, instead the storage backend is responsible for
  determining how long a write should reasonably take and if that fails
  it should raise.
  """
  def handle_event(event, stack_id) do
    trace_context = OpenTelemetry.get_current_context()
    GenServer.call(name(stack_id), {:handle_event, event, trace_context}, :infinity)
  end

  @doc """
  Non-blocking variant of `handle_event/2`.

  Sends a `$gen_call` to the collector and returns a monitor reference.
  The caller receives `{monitor_ref, response}` when the event is processed,
  or `{:DOWN, monitor_ref, :process, pid, reason}` if the collector crashes.

  Uses the same `$gen_call` protocol as `GenServer.call` internally — the
  existing `handle_call` handles the request unchanged.
  """
  def handle_event_async(event, stack_id) do
    trace_context = OpenTelemetry.get_current_context()
    server = name(stack_id)

    case GenServer.whereis(server) do
      nil ->
        # The event stays out of the exit reason on purpose. The caller retries this dispatch every
        # 50ms for up to ten minutes and logs the reason on each attempt, so an embedded event means
        # one transaction's rows written to the log pipeline and the error
        # tracker thousands of times over for a single collector restart.
        exit({:noproc, {__MODULE__, :handle_event_async, [stack_id]}})

      pid ->
        monitor_ref = Process.monitor(pid)
        send(pid, {:"$gen_call", {self(), monitor_ref}, {:handle_event, event, trace_context}})
        monitor_ref
    end
  end

  @doc """
  Adds a shape to the shape matching index in the ShapeLogCollector
  used for matching and sending replication stream operations.
  """
  defdelegate add_shape(stack_id, shape_handle, shape, operation), to: __MODULE__.RequestBatcher

  @doc """
  Removes a shape from the shape matching index in the ShapeLogCollector.
  This call succeeds before the shape is actually removed from the index.
  """
  defdelegate remove_shape(stack_id, shape_handle), to: __MODULE__.RequestBatcher

  @doc """
  Handles batched shape registration updates from the RequestBatcher.
  """
  def handle_shape_registration_updates(stack_id, shapes_to_add, shapes_to_remove) do
    pid = name(stack_id) |> GenServer.whereis()
    call_ref = make_ref()

    GenServer.cast(
      pid,
      {:handle_shape_registration_updates, call_ref, shapes_to_add, shapes_to_remove}
    )

    call_ref
  end

  @doc """
  Notifies the ShapeLogCollector that a shape's data has been flushed
  up to a certain offset, used to mark the overall flush progress.

  Should be called by consumer processes after they flush data: the caller is the writer that
  flushed.
  """
  @spec notify_flushed(Electric.stack_id(), Electric.shape_handle(), LogOffset.t()) :: :ok
  def notify_flushed(stack_id, shape_handle, offset) do
    GenServer.cast(name(stack_id), {:writer_flushed, shape_handle, offset, self()})
  end

  @doc """
  `notify_flushed/3` for a flush made by `writer` rather than the caller.
  """
  @spec notify_flushed(Electric.stack_id(), Electric.shape_handle(), LogOffset.t(), pid()) :: :ok
  def notify_flushed(stack_id, shape_handle, offset, writer) do
    GenServer.cast(name(stack_id), {:writer_flushed, shape_handle, offset, writer})
  end

  @doc """
  Notifies the ShapeLogCollector that a shape's writer is alive and
  deliberately deferring its flush notifications (e.g. buffering transactions
  ahead of PG snapshot info or during a subquery move-in awaiting splice).

  Sent by a writer in answer to a `:verify_flush_progress` challenge from the
  stall check. Grants the shape's flush entry a fresh stall grace period so a
  healthy deferral is not mistaken for a wedged writer.
  """
  @spec notify_flush_deferred(Electric.stack_id(), Electric.shape_handle()) :: :ok
  def notify_flush_deferred(stack_id, shape_handle) do
    GenServer.cast(name(stack_id), {:writer_flush_deferred, shape_handle})
  end

  @doc """
  Returns the list of currently active shapes being tracked
  in the shape matching filters.
  """
  @spec active_shapes(Electric.stack_id()) :: MapSet.t(Electric.shape_handle())
  def active_shapes(stack_id) do
    GenServer.call(name(stack_id), :active_shapes)
  end

  @doc """
  Set process flags on the given ShapeLogCollector process.

  Accepts a list of flags to set, see `Process.flag/2` for valid settings.

  Doesn't crash if given an invalid flag or value - instead returns the list of
  invalid flags.

      iex> ShapeLogCollector.set_process_flags("my-stack-id", min_heap_size: 1024 * 1024, min_bin_vheap_size: 1024 * 1024)
      {:ok, settings: [min_heap_size: 1024 * 1024, min_bin_vheap_size: 1024 * 1024], invalid: []}
  """
  def set_process_flags(stack_id, flags) do
    GenServer.call(name(stack_id), {:set_process_flags, flags}, :infinity)
  end

  def get_process_flags(stack_id) do
    if pid = name(stack_id) |> GenServer.whereis() do
      {:garbage_collection, gc_flags} = :erlang.process_info(pid, :garbage_collection)
      {:priority, priority} = :erlang.process_info(pid, :priority)

      {:ok,
       [priority: priority] ++
         Keyword.take(gc_flags, [:min_bin_vheap_size, :min_heap_size, :fullsweep_after])}
    else
      :error
    end
  end

  @doc """
  Utility for tests, monitors the SLC process.
  """
  def monitor(stack_id) do
    stack_id
    |> name()
    |> GenServer.whereis()
    |> Process.monitor()
  end

  def init(opts) do
    activate_mocked_functions_from_test_process()

    stack_id = opts.stack_id

    Process.set_label({:shape_log_collector, stack_id})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    persistent_replication_data_opts = [
      stack_id: stack_id,
      persistent_kv: opts.persistent_kv
    ]

    {:ok, tracker_state} =
      persistent_replication_data_opts
      |> PersistentReplicationState.get_tracked_relations()
      |> AffectedColumns.init()

    {:ok, registry_state} =
      ConsumerRegistry.new(stack_id, Map.get(opts, :consumer_registry_opts, []))

    router_shards = router_shards(opts)
    ack_after_journal? = flag(opts, :ack_after_journal)

    shared_conversion? = flag(opts, :shared_conversion)
    if shared_conversion?, do: Logger.notice("Sharing one conversion per fan-out class")

    arena_enabled? = refinement(opts, :shared_payload_arena, shared_conversion?)

    fragment_blocks? =
      refinement(opts, :shared_fragment_blocks, shared_conversion?) and
        shared_fragment_blocks_supported?(stack_id)

    worker_prepare? = refinement(opts, :worker_prepare, shared_conversion?)

    if worker_prepare? and router_shards <= 1 do
      Logger.warning(
        "Worker prepare is on but router sharding is not; it does nothing below two shards"
      )
    end

    worker_prepare? = worker_prepare? and router_shards > 1

    if fragment_blocks?, do: Logger.notice("Framing class fragment runs once per fan-out class")

    route_memo? = flag(opts, :filter_routing_memo)
    if route_memo?, do: Logger.notice("Memoizing routing decisions per projected record key")

    arena_base_path = if arena_enabled?, do: payload_arena_base_path(stack_id)

    # Read by the storage stack's `required_version`, and set here -- during this process's init,
    # before it opens this boot's writer and before it restores any shape -- so the first shape to
    # open after the arena is enabled records the arena version instead of one keyed on a generation
    # file that does not exist yet.
    Electric.StackConfig.put(stack_id, :payload_arena_active?, not is_nil(arena_base_path))

    if arena_base_path do
      BootStep.measure("shape_log_collector.reclaim_arena_generations", fn ->
        reclaim_arena_generations(arena_base_path, flag(opts, :payload_arena_reclaim_min_bytes))
      end)
    end

    # Probed unconditionally: a journal left behind by a previous boot must be replayed
    # even when the flag is now off, because the slot was already acked past it. Gating
    # the probe on the flag turns disabling it into silent data loss.
    txn_journal_base = txn_journal_base_path(stack_id, ack_after_journal?)

    # The replication client disables ack-after-journal when the storage cannot host a journal
    # (ReplicationClient.ConnectionSetup.resolve_journal_support/1). The collector must reach the
    # same verdict from the same source, or it acks on the non-journal path in one process while
    # arming the boot durability guards below for a journal the client never wrote — and the next
    # boot then refuses to start. Leftover replay above already keyed off txn_journal_base, so the
    # downgrade only clears the flag the guards read.
    ack_after_journal? = ack_after_journal? and journal_capable_storage?(stack_id)

    # Read adjacent to the generation list and carried into the replay: the two answers are only
    # comparable when they name the same instant. This stack's own journal writer opens between
    # here and the replay, writing the marker before its first generation file, so a marker read
    # at replay time is a different instant and reports every generation lost on a first boot.
    journal_used? = !!txn_journal_base and TxnJournal.used?(txn_journal_base)

    journal_replay_gens =
      if txn_journal_base, do: TxnJournal.list_generations(txn_journal_base), else: []

    # The last-seen feed's promise — everything at or below a published position has
    # been fanned out — is void while journaled transactions await replay. Written
    # here, before the replication client can decode a keepalive, and recomputed each
    # incarnation: a crash mid-replay re-arms it, a consumed journal clears a stale
    # marker left in the surviving table. A surviving watermark predates the crash
    # that left this journal tail — it promises fan-out the restored shapes never
    # received, and it only ever advances — so it is dropped with the marker armed;
    # the replay's own per-commit broadcasts rebuild it from real deliveries.
    if journal_replay_gens == [] do
      LsnTracker.clear_journal_replay_pending(stack_id)
    else
      Electric.StatusMonitor.hold_shape_serving_for_journal_replay(stack_id)
      LsnTracker.mark_journal_replay_pending(stack_id)
      LsnTracker.reset_last_broadcast_lsn(stack_id)
    end

    workers =
      if router_shards > 1 do
        Logger.notice("Sharding shape routing across #{router_shards} router workers")
        if worker_prepare?, do: Logger.notice("Deferring class preparation to router workers")

        worker_opts =
          if worker_prepare? and arena_base_path,
            do: [payload_arena_base_path: arena_base_path],
            else: []

        worker_opts = Keyword.put(worker_opts, :fragment_blocks, fragment_blocks?)

        0..(router_shards - 1)
        |> Enum.map(fn index ->
          {:ok, pid} =
            RouterWorker.start_link(stack_id, index, router_shards, registry_state, worker_opts)

          pid
        end)
        |> List.to_tuple()
      end

    scatter_table =
      if workers, do: :ets.new(:router_scatter, [:public, read_concurrency: true])

    # In worker-prepare mode nothing is prepared on this process, so the collector holds no
    # arena writer — each worker owns its own generation instead.
    payload_arena =
      if arena_base_path && not worker_prepare?, do: PayloadArena.open_writer!(arena_base_path)

    replication_client_name = ReplicationClient.name(stack_id)

    state =
      Map.merge(opts, %{
        subscriptions: 0,
        persistent_replication_data_opts: persistent_replication_data_opts,
        tracked_relations: tracker_state,
        partitions: Partitions.new(Keyword.new(opts)),
        dependency_layers: DependencyLayers.new(),
        # A pending FlushTracker entry always has a live monitor watching the
        # writer pid responsible for completing it.
        writer_monitors: %{},
        # Shapes whose writer was challenged by the last stall check and has not
        # shown flush progress since (see :check_stalled_flushes).
        stall_suspects: MapSet.new(),
        # Per shape, the writer pid that last reported a flush the tracker was waiting for. That
        # pid no longer needs the first-flush allowance; any other pid writing the shape does (see
        # awaiting_first_flush?/3).
        warm_writers: %{},
        # Consecutive honored deferral answers per shape since its last real
        # flush progress; past the round limit the answers stop being honored.
        deferral_rounds: %{},
        # Shapes whose writer exited on a fragment of the open transaction without a verdict on
        # the shape (see finalize_publish/6); the commit pins their flush entries.
        undelivered_pins: MapSet.new(),
        workers: workers,
        scatter_table: scatter_table,
        shape_owners: %{},
        own_shape_count: 0,
        pending_gathers: :queue.new(),
        gather_stall_timeout:
          Map.get(opts, :gather_stall_timeout) ||
            Electric.StackConfig.lookup(
              stack_id,
              :gather_stall_timeout,
              Electric.Config.default(:gather_stall_timeout)
            ),
        pending_flush_notifications: [],
        worker_prepare?: worker_prepare?,
        ack_after_journal?: ack_after_journal?,
        txn_journal_base: txn_journal_base,
        journal_used?: journal_used?,
        journal_replay_gens: journal_replay_gens,
        journal_replay_end: nil,
        journal_gens_awaiting_durability: nil,
        journal_replaying?: false,
        journal_replayed?: false,
        event_router:
          opts
          |> Keyword.new()
          |> Keyword.put(:payload_arena, payload_arena)
          |> Keyword.put(:prepare_mode, if(worker_prepare?, do: :deferred, else: :inline))
          |> Keyword.put(:fragment_blocks, fragment_blocks?)
          |> Keyword.put(:filter_routing_memo, route_memo?)
          |> Keyword.put(:shared_conversion, shared_conversion?)
          |> EventRouter.new(),
        flush_tracker:
          FlushTracker.new(
            notify_fn: fn lsn ->
              case GenServer.whereis(replication_client_name) do
                nil -> :ok
                pid -> send(pid, {:flush_boundary_updated, lsn})
              end

              # In journal mode the consumer-durable boundary no longer drives the slot
              # ack (the journal boundary is always ahead; the client clamps with max) —
              # it becomes the journal's GC watermark, owned by the client's writer.
              if ack_after_journal? do
                case GenServer.whereis(replication_client_name) do
                  nil -> :ok
                  pid -> send(pid, {:journal_consumer_boundary, lsn})
                end
              end
            end
          ),
        registry_state: registry_state
      })

    schedule_stall_check(stall_grace_period(stack_id))

    {:ok, state, {:continue, :restore_shapes}}
  end

  # Shapes persisted before parse-time castability validation existed are restored from their
  # stored AST rather than re-parsed, so nothing already running is re-validated by that guard.
  # Found by the restore's own read of each shape: a read of the whole store for this report
  # alone decodes every stored shape once more.
  @doc false
  def uncastable_refs(%{where: %{used_refs: used_refs}}, env),
    do: Env.uncastable_refs(used_refs, env)

  def uncastable_refs(_shape, _env), do: []

  defp report_uncastable_filter(shape_handle, uncastable) do
    Logger.warning(
      "Restored a shape filtering on " <>
        Enum.map_join(uncastable, ", ", fn {[column], type} ->
          ~s|"#{column}" of type #{inspect(type)}|
        end) <>
        ", which Electric cannot parse from the replication stream: the shape matches no " <>
        "live change. Recreating it will report the reason.",
      shape_handle: shape_handle
    )
  end

  # Release configuration reaches the application env through config/runtime.exs in every
  # environment, dev and test included, so a stack option is the only layer above it.
  defp flag(opts, key), do: Electric.Config.get_env(key, Map.get(opts, key))

  # The arena, fragment blocks and worker prepare each decide how a shared conversion is
  # packaged, framed or parallelized. None of them has anything to act on while every shape
  # converts its own changes, so they follow the base flag rather than pretending to be on.
  defp refinement(opts, key, shared_conversion?) do
    case {flag(opts, key), shared_conversion?} do
      {true, false} ->
        Logger.warning("#{key} is on but shared conversion is off; it does nothing")
        false

      {enabled?, _} ->
        enabled?
    end
  end

  # Each router worker claims a payload-arena generation discriminator of `index + 1`, and
  # the arena packs that discriminator into the low byte of the generation id.
  defp router_shards(opts) do
    shards = flag(opts, :router_shards)

    if shards > @max_router_shards do
      raise ArgumentError,
            "ELECTRIC_ROUTER_SHARDS must be at most #{@max_router_shards}, got #{shards}"
    end

    shards
  end

  # Mirrors ReplicationClient.ConnectionSetup.resolve_journal_support/1: only PureFileStorage can
  # host a transaction journal, so ack-after-journal is impossible against any other storage.
  defp journal_capable_storage?(stack_id) do
    match?(
      {Electric.ShapeCache.PureFileStorage, _},
      Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage)
    )
  end

  defp txn_journal_base_path(stack_id, ack_after_journal?) do
    case {Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage), ack_after_journal?} do
      {{Electric.ShapeCache.PureFileStorage, %{base_path: base_path}}, true} ->
        Logger.notice("Transaction journal replay armed (ack-after-journal)")
        base_path

      {{Electric.ShapeCache.PureFileStorage, %{base_path: base_path}}, false} ->
        # Flag off: only leftovers matter, and they must still be drained.
        if TxnJournal.list_generations(base_path) != [] do
          Logger.notice(
            "Replaying a transaction journal left by a previous boot with ack-after-journal on"
          )

          base_path
        end

      {other, true} ->
        Logger.warning(
          "Ack-after-journal requested but storage does not support it: #{inspect(other)}"
        )

        nil

      {_other, false} ->
        nil
    end
  end

  defp payload_arena_base_path(stack_id) do
    case Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage) do
      {Electric.ShapeCache.PureFileStorage, %{base_path: base_path}} ->
        Logger.notice("Shared payload arena enabled")
        base_path

      other ->
        Logger.warning(
          "Shared payload arena requested but storage does not support it: #{inspect(other)}"
        )

        nil
    end
  end

  # This init is the one moment no writer holds a generation open, so it is where arena generations
  # are reclaimed. The scan of the surviving logs must be complete before a generation is dropped --
  # a missed ref would delete a live payload -- so a log this cannot read (a read error raises)
  # degrades to pruning only the empty generations, the reclaim that needs no scan.
  #
  # The scan lists the log directory of every stored shape, and that is a disk read per shape on a
  # cold volume: minutes of a restart with a large shape count, often with little to reclaim.
  # No writer is open yet, so every generation on disk was left by an earlier boot and their
  # size is the most the scan could give back. Below `min_bytes` the boot keeps them and prunes the
  # empty ones.
  defp reclaim_arena_generations(base_path, min_bytes) do
    case PayloadArena.stored_bytes(base_path) do
      bytes when bytes == 0 or bytes < min_bytes ->
        if bytes > 0 do
          Logger.notice(
            "Arena generation reclaim left for a later boot: #{bytes} bytes held by earlier " <>
              "generations, under the #{min_bytes} bytes that warrant a scan of every shape log"
          )
        end

        PayloadArena.prune_empty_generations!(base_path)

      _bytes ->
        keep = Electric.ShapeCache.PureFileStorage.referenced_arena_generations(base_path)
        PayloadArena.reclaim_generations!(base_path, keep)
    end
  rescue
    error ->
      Logger.warning("Arena generation reclaim skipped, pruning empty only: #{inspect(error)}")
      PayloadArena.prune_empty_generations!(base_path)
  end

  # Fragment blocks hand the storage backend a framed-block struct only pure file storage has a
  # clause for; every other backend would crash on the first framed change. Gate it at boot the
  # same way the arena is, so an unsupported backend silently falls back to per-shape conversion.
  defp shared_fragment_blocks_supported?(stack_id) do
    case Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage) do
      {Electric.ShapeCache.PureFileStorage, _opts} ->
        true

      other ->
        Logger.warning(
          "Shared fragment blocks requested but storage does not support them: #{inspect(other)}"
        )

        false
    end
  end

  def handle_continue({:replay_journal, slot_offset}, state) do
    state = replay_journal(state, slot_offset)
    offset = LogOffset.max(slot_offset, state.journal_replay_end || slot_offset)

    if state.txn_journal_base do
      Electric.StatusMonitor.mark_shape_log_collector_ready(state.stack_id, self())
    end

    {:noreply, Map.put(state, :last_processed_offset, offset)}
  end

  def handle_continue(:restore_shapes, state) do
    OpenTelemetry.with_span(
      "shape_log_collector.restore_shapes",
      [],
      state.stack_id,
      fn ->
        start = System.monotonic_time()

        BootStep.measure("shape_log_collector.invalidate_shapes_without_journal_provenance", fn ->
          invalidate_shapes_without_journal_provenance(state)
        end)

        # Restoring subquery shapes consistently across a restart is not yet
        # implemented, so for now they are dropped rather than restored and
        # clients re-request them. This is the first restore path in the shape
        # subsystem to read from ShapeStatus, and the shape consumers start after
        # it, so dropping them in this restore is the single point that keeps every
        # restore path from reinstating a subquery shape.
        #
        # The read that rebuilds the routing finds them too, since another read would decode
        # every stored shape again. A shape with dependencies stays out of the routing, and a
        # materializer goes in as a plain shape, for `prune_subquery_shapes/2` to take out once
        # the read is over.
        #
        # The same read builds the routing state ShapeStatus keeps, from every stored shape, as
        # `ShapeStatus.initialize/1` leaves it to this restore.
        #
        # The read is spread over several processes: see `Restore`.
        restored =
          BootStep.measure("shape_log_collector.restore_filters", fn ->
            Restore.run(state, ShapeStatus.new_routing_state())
          end)

        state = %{
          state
          | event_router: restored.router,
            dependency_layers:
              DependencyLayers.add_independent(state.dependency_layers, restored.independent),
            shape_owners: Map.merge(state.shape_owners, restored.owners),
            subscriptions: restored.count
        }

        {state, restored} = restore_deferred_shapes(state, restored)

        # Only the root tables of the shapes the restore keeps get partitions, and only once the
        # read is over: a materializer the restore drops must not fail it. The restore's processes
        # had those tables introspected while the read went on, so the inspector answers from its
        # cache here, and a table it could not introspect then is tried again here, where a failure
        # counts.
        partitions =
          BootStep.measure("shape_log_collector.restore_partitions", fn ->
            restored.relations
            |> Enum.sort()
            |> Enum.reduce(state.partitions, fn {table, shape_handles}, partitions ->
              case MapSet.difference(shape_handles, restored.subquery) do
                %MapSet{map: map} when map_size(map) == 0 -> partitions
                kept -> restore_partitions_for_table(partitions, table, kept)
              end
            end)
          end)

        # Reported once the read is over, for the same reason: not for a materializer it drops.
        for {_seq, shape_handle, refs} <- restored.uncastable,
            not MapSet.member?(restored.subquery, shape_handle),
            do: report_uncastable_filter(shape_handle, refs)

        state = %{
          state
          | partitions: partitions,
            own_shape_count: Enum.count(state.shape_owners, fn {_, owner} -> owner == :own end)
        }

        state =
          BootStep.measure("shape_log_collector.prune_subquery_shapes", fn ->
            prune_subquery_shapes(state, restored.subquery)
          end)

        # Only once the subquery shapes are dropped: the relation tracker reads the routing state
        # as soon as it is installed, and nothing tells it about that drop. The install counts
        # down the shapes removed since the read.
        BootStep.measure("shape_log_collector.restore_routing_state", fn ->
          ShapeStatus.restore_routing_state(state.stack_id, restored.routing_state)
        end)

        Logger.notice(
          "Restored filters for #{state.subscriptions} shapes in #{System.convert_time_unit(System.monotonic_time() - start, :native, :millisecond)}ms"
        )

        {:noreply, state}
      end
    )
  end

  # The shapes the restore leaves to the collector (see `Restore`), in the order of the read, each
  # added as the collector alone used to add every restored shape.
  defp restore_deferred_shapes(state, %{deferred: []} = restored), do: {state, restored}

  defp restore_deferred_shapes(state, restored) do
    env = Env.new()

    {state, restored} =
      Enum.reduce(restored.deferred, {state, restored}, fn
        {seq, shape_handle, shape, routed?}, {state, restored} ->
          restored =
            if routed?,
              do: restored,
              else: %{
                restored
                | routing_state:
                    ShapeStatus.add_to_routing_state(restored.routing_state, shape_handle, shape)
              }

          restore_deferred_shape(state, restored, {seq, shape_handle, shape}, env)
      end)

    {state, %{restored | uncastable: Enum.sort(restored.uncastable)}}
  end

  defp restore_deferred_shape(state, restored, {_seq, shape_handle, shape}, _env)
       when shape.shape_dependencies != [] do
    subquery = Enum.into([shape_handle | shape.shape_dependencies_handles], restored.subquery)
    {state, %{restored | subquery: subquery}}
  end

  defp restore_deferred_shape(state, restored, {seq, shape_handle, shape}, env) do
    if MapSet.member?(restored.subquery, shape_handle) do
      {state, restored}
    else
      restored =
        case uncastable_refs(shape, env) do
          [] -> restored
          refs -> %{restored | uncastable: [{seq, shape_handle, refs} | restored.uncastable]}
        end

      # Check dependencies first - if a parent shape failed to restore,
      # we should skip this shape (and its children will also be skipped)
      case DependencyLayers.add_dependency(state.dependency_layers, shape, shape_handle) do
        {:ok, layers} ->
          {event_router, owner} = add_shape_to_owner(state, shape_handle, shape)

          state = %{
            state
            | event_router: event_router,
              dependency_layers: layers,
              shape_owners: Map.put(state.shape_owners, shape_handle, owner),
              subscriptions: state.subscriptions + 1
          }

          relations =
            Map.update(
              restored.relations,
              {shape.root_table_id, shape.root_table},
              MapSet.new([shape_handle]),
              &MapSet.put(&1, shape_handle)
            )

          {state, %{restored | relations: relations}}

        {:error, {:missing_dependencies, missing_deps}} ->
          Logger.warning(
            "Skipping shape during restore: missing dependencies #{inspect(MapSet.to_list(missing_deps))}",
            shape_handle: shape_handle
          )

          {state, restored}
      end
    end
  end

  # Removes every shape involved in a subquery, the outer shape plus its dependency
  # materializers, from the shape metadata and from on-disk storage. No consumer runs for them
  # yet. The only routing entries to clear are those of the materializers the restore read
  # before the shape that names them.
  defp prune_subquery_shapes(state, handles) do
    alias Electric.ShapeCache.{ShapeStatus, Storage}

    if MapSet.size(handles) == 0 do
      state
    else
      Logger.notice(
        "Dropping #{MapSet.size(handles)} shape(s) involved in subqueries on restart; " <>
          "clients will re-request them from scratch"
      )

      storage = Storage.for_stack(state.stack_id)

      Enum.reduce(handles, state, fn handle, state ->
        state =
          case remove_subscription(state, handle) do
            {:ok, state} -> state
            # kept out of the routing by the restore
            {:error, _not_registered} -> state
          end

        case ShapeStatus.remove_shape(state.stack_id, handle) do
          :ok -> Storage.cleanup!(storage, handle)
          {:error, _reason} -> :ok
        end

        state
      end)
    end
  end

  # Restoring a shape requires introspecting its root table, which can fail
  # while the database connection pool is still coming up (or the database is
  # otherwise unhealthy) — exactly the situation we're likely to be in when
  # restarting after a crash. Skipping the shape is not an option: nothing
  # re-registers restored shapes later, so a skipped shape would silently stop
  # receiving updates.
  #
  # We deliberately retry in place rather than letting the error propagate and
  # be "handled upstream", because there is no upstream handler that recovers
  # gracefully here:
  #
  #   * Restore runs in `handle_continue`, not through the event path, so the
  #     replication client's pause/redeliver recovery for
  #     `:connection_not_available` does not apply.
  #   * `ShapeLogCollector.Supervisor` is `max_restarts: 0`, so a crash is not
  #     retried locally — it propagates straight to the `:one_for_all`
  #     `Shapes.Supervisor`, which stops the *entire* shape subsystem, and the
  #     service with it (see `Electric.CoreSupervisor`).
  #   * The failure is a transient pool-warmup race, so a crash would stop the
  #     service on a routine restart after a crash.
  #
  # Blocking in `handle_continue` is intentional: it keeps restore atomic with
  # respect to `mark_as_ready`/`handle_event`, so the collector never starts
  # serving before its partition state is rebuilt. The trade-off is that this
  # also delays the process's response to a shutdown signal, so the total wait
  # is bounded below the supervisor's default 5s shutdown timeout. If the error
  # still persists after that, we give up and crash with a descriptive error so
  # the wider connection-recovery machinery can take over.
  @restore_retry_delay_ms 100
  @restore_max_retries 40

  defp invalidate_shapes_without_journal_provenance(%{journal_replay_gens: []}), do: :ok

  # The journal is read before the shapes. Only a fragment without a full transaction ID can
  # invalidate a shape, and a journal written with full IDs holds none: the stored shapes are then
  # not read here at all. A journal written without them answers at its first fragment.
  defp invalidate_shapes_without_journal_provenance(state) do
    alias Electric.ShapeCache.{ShapeStatus, Storage}

    if journal_lacks_full_transaction_ids?(state) do
      handles =
        case ShapeStatus.reduce_shapes(state.stack_id, [], fn
               {handle, %{source_positions: :full_xid}}, handles -> [handle | handles]
               _, handles -> handles
             end) do
          {:error, error} -> raise error
          handles -> handles
        end

      if handles != [] do
        Logger.warning(
          "Invalidating #{length(handles)} full-provenance shapes: retained journal history lacks full transaction IDs"
        )

        storage = Storage.for_stack(state.stack_id)

        Enum.each(handles, fn handle ->
          :ok = ShapeStatus.remove_shape(state.stack_id, handle)
          Storage.cleanup!(storage, handle)
        end)
      end
    end

    :ok
  end

  defp journal_lacks_full_transaction_ids?(state) do
    Enum.any?(
      TxnJournal.stream_events!(state.txn_journal_base, state.journal_replay_gens),
      fn
        %TransactionFragment{} = fragment -> not is_integer(Map.get(fragment, :full_xid))
        _ -> false
      end
    )
  end

  defp restore_partitions_for_table(partitions, table, shape_handles, attempt \\ 1) do
    case Partitions.add_shapes(partitions, table, shape_handles) do
      {:ok, partitions} ->
        partitions

      {:error, reason} when attempt >= @restore_max_retries ->
        raise "Failed to restore partition info for #{describe_shapes(shape_handles)}: #{inspect(reason)}"

      {:error, reason} ->
        if attempt == 1 do
          Logger.warning(
            "Retrying shape restore: failed to introspect #{Electric.Utils.inspect_relation(elem(table, 1))}: #{inspect(reason)}",
            shape_handle: Enum.min(shape_handles)
          )
        end

        Process.sleep(@restore_retry_delay_ms)
        restore_partitions_for_table(partitions, table, shape_handles, attempt + 1)
    end
  end

  defp describe_shapes(shape_handles) do
    case MapSet.size(shape_handles) do
      1 -> "shape #{Enum.min(shape_handles)}"
      n -> "shape #{Enum.min(shape_handles)} and #{n - 1} other shapes on its table"
    end
  end

  def handle_call(:mark_as_ready, _from, state) do
    state = drain_pending_gathers(state)

    slot_offset =
      case LsnTracker.get_last_processed_lsn(state.stack_id) do
        %Lsn{} = lsn ->
          LogOffset.new(Lsn.to_integer(lsn), :infinity)

        nil ->
          raise "LsnTracker must be populated before marking shape_log_collector as ready"
      end

    if is_nil(state.txn_journal_base) do
      Electric.StatusMonitor.mark_shape_log_collector_ready(state.stack_id, self())
    end

    # The replay publishes to consumers, and starting a consumer is a call into
    # ShapeCache — the very process blocked on this call. Replaying from a continue lets
    # that caller return first while still running the whole replay before any other
    # message. last_processed_offset stays nil until the replay seeds it: a continue runs
    # ahead of every other message, so no event can observe the gap.
    {:reply, :ok, Map.put(state, :last_processed_offset, nil),
     {:continue, {:replay_journal, slot_offset}}}
  end

  def handle_call({:handle_event, _, _}, _from, state)
      when not is_ready_to_process(state) do
    {:reply, {:error, :not_ready}, state}
  end

  def handle_call({:handle_event, event, trace_context}, from, state) do
    OpenTelemetry.set_current_context(trace_context)

    case do_handle_event(event, from, state) do
      {:async, state} -> {:noreply, state}
      {response, state} -> {:reply, response, state}
    end
  end

  def handle_call(:last_processed_offset, _from, state) do
    {:reply, state[:last_processed_offset], state}
  end

  def handle_call(:active_shapes, _from, state) do
    {:reply, EventRouter.active_shapes(state.event_router), state}
  end

  def handle_call({:set_process_flags, flags}, _from, state) do
    {settings, invalid} =
      Enum.flat_map_reduce(flags, [], fn {flag, value}, invalid ->
        try do
          {[{flag, Process.flag(flag, value)}], invalid}
        rescue
          ArgumentError ->
            {[], [flag | invalid]}
        end
      end)

    {:reply, {:ok, [settings: settings, invalid: invalid]}, state}
  end

  def handle_cast({:writer_flushed, shape_id, offset, writer}, state) do
    {:noreply, apply_or_buffer_flush_notification(state, shape_id, offset, writer)}
  end

  def handle_cast({:writer_flush_deferred, shape_handle}, state) do
    rounds = Map.get(state.deferral_rounds, shape_handle, 0) + 1

    if rounds > max_deferral_rounds(state.stack_id) do
      # The writer has answered every challenge for the entire round budget with
      # zero flush progress. Stop honoring it: the entry stays un-touched and the
      # suspicion stays armed, so the next stall pass invalidates the shape and
      # unpins the boundary. A shape buffering ahead of a snapshot that never
      # completes would otherwise freeze the slot ack stack-wide.
      Logger.warning(
        "Writer for shape #{shape_handle} deferred flushes through #{rounds - 1} " <>
          "consecutive stall challenges; no longer honoring its deferral"
      )

      {:noreply, state}
    else
      state =
        state
        |> Map.update!(:flush_tracker, &FlushTracker.touch(&1, shape_handle, now_ms()))
        |> Map.update!(:deferral_rounds, &Map.put(&1, shape_handle, rounds))

      {:noreply, clear_stall_suspect(state, shape_handle)}
    end
  end

  def handle_cast(
        {:handle_shape_registration_updates, call_ref, shapes_to_add, shapes_to_remove},
        state
      ) do
    # Registration mutates routing, worker assemblers and the FlushTracker; in-flight
    # scattered fragments must finalize against the pre-mutation state first — a removal
    # interleaved with a pending finalize would re-register the removed shape in the
    # FlushTracker and wedge the flush boundary.
    state = drain_pending_gathers(state)

    OpenTelemetry.with_span(
      "shape_log_collector.handle_shape_registration_updates",
      [
        shapes_to_add_count: Enum.count(shapes_to_add),
        shapes_to_remove_count: Enum.count(shapes_to_remove)
      ],
      state.stack_id,
      fn ->
        {state, results} =
          shapes_to_remove
          |> Enum.reduce({state, %{}}, fn shape_handle, {state, results} ->
            case remove_subscription(state, shape_handle) do
              {:ok, state} -> {state, Map.put(results, shape_handle, :ok)}
              {:error, reason} -> {state, Map.put(results, shape_handle, {:error, reason})}
            end
          end)

        {state, results} =
          shapes_to_add
          |> Enum.reduce({state, results}, fn {shape_handle, shape}, {state, results} ->
            case Partitions.add_shape(state.partitions, shape_handle, shape) do
              {:ok, partitions} ->
                case DependencyLayers.add_dependency(
                       state.dependency_layers,
                       shape,
                       shape_handle
                     ) do
                  {:ok, dependency_layers} ->
                    {event_router, owner} = add_shape_to_owner(state, shape_handle, shape)

                    state =
                      %{
                        state
                        | partitions: partitions,
                          event_router: event_router,
                          shape_owners: Map.put(state.shape_owners, shape_handle, owner),
                          own_shape_count: state.own_shape_count + own_increment(owner),
                          dependency_layers: dependency_layers
                      }
                      |> Map.update!(:subscriptions, &(&1 + 1))
                      |> log_subscription_status()

                    {state, Map.put(results, shape_handle, :ok)}

                  {:error, {:missing_dependencies, missing_deps}} ->
                    Logger.warning(
                      "Shape cannot be added: missing dependencies #{inspect(MapSet.to_list(missing_deps))}",
                      shape_handle: shape_handle
                    )

                    {state, Map.put(results, shape_handle, {:error, :missing_dependencies})}
                end

              {:error, reason} ->
                {state, Map.put(results, shape_handle, {:error, reason})}
            end
          end)

        __MODULE__.RequestBatcher.handle_processor_update_response(
          state.stack_id,
          call_ref,
          results
        )

        {:noreply, state}
      end
    )
  end

  def handle_info({{:down, shape_handle}, ref, :process, _pid, reason}, state) do
    state =
      case Map.pop(state.writer_monitors, shape_handle) do
        {{_pid, ^ref}, writer_monitors} ->
          %{state | writer_monitors: writer_monitors}
          |> handle_writer_down(shape_handle, reason)

        {nil, _} ->
          state
      end

    {:noreply, state}
  end

  def handle_info(:check_stalled_flushes, state) do
    now = now_ms()

    check_stalled_gather_head!(state, now)

    grace_period = stall_grace_period(state.stack_id)
    schedule_stall_check(grace_period)

    stalled =
      FlushTracker.stalled_shapes(state.flush_tracker, now, grace_period)
      |> Enum.reject(&awaiting_first_flush?(state, &1, now))

    # Challenge-response: a stalled entry whose writer is still alive gets one
    # chance to prove it is deliberately deferring its flushes. First time a
    # shape shows up stalled, its monitored writer pid is sent a challenge; a
    # healthy deferring consumer answers with a notify_flush_deferred cast,
    # which touches the entry and clears the suspicion. Invalidated are only
    # the suspects still stalled with no progress since the previous check's
    # challenge, and shapes with no monitored writer left to challenge (their
    # writer is already dead — e.g. a :killed DOWN left the entry pinned).
    {challengeable, orphaned} =
      Enum.split_with(stalled, &is_map_key(state.writer_monitors, &1))

    {repeat_suspects, fresh_suspects} =
      Enum.split_with(challengeable, &MapSet.member?(state.stall_suspects, &1))

    Enum.each(fresh_suspects, fn shape_handle ->
      {pid, _ref} = Map.fetch!(state.writer_monitors, shape_handle)
      send(pid, :verify_flush_progress)
      Logger.info("Flush-stall challenge: #{stall_diagnostic(state, shape_handle, now)}")
    end)

    state = %{state | stall_suspects: MapSet.new(fresh_suspects)}

    case orphaned ++ repeat_suspects do
      [] ->
        {:noreply, state}

      stalled ->
        Logger.warning(
          "Writers for shapes #{inspect(stalled)} have made no flush progress in over " <>
            "#{grace_period}ms, removing the shapes to unpin the flush boundary"
        )

        Enum.each(stalled, fn shape_handle ->
          Logger.warning("Flush-stall removal: #{stall_diagnostic(state, shape_handle, now)}")
        end)

        OpenTelemetry.execute(
          [:electric, :flush_tracker, :stall_detected],
          %{count: length(stalled)},
          %{stack_id: state.stack_id}
        )

        Electric.ShapeCache.ShapeCleaner.remove_shapes_async(state.stack_id, stalled)

        # Touching re-arms the grace period: the unpinning happens via the removal
        # chain, and if that chain is lost the stall simply re-fires one grace
        # period later (shape removal is idempotent).
        flush_tracker =
          Enum.reduce(stalled, state.flush_tracker, &FlushTracker.touch(&2, &1, now))

        state = %{
          state
          | flush_tracker: flush_tracker,
            deferral_rounds: Map.drop(state.deferral_rounds, stalled)
        }

        {:noreply, state}
    end
  end

  # Worker acks for scattered-ahead fragments. Fragments finalize strictly in order:
  # only a completed queue head is finalized, so LsnTracker/FlushTracker updates and
  # caller replies keep the exact sequence of the blocking path.
  def handle_info({ref, {%MapSet{} = affected, undeliverable, delivered}}, state)
      when is_reference(ref) do
    {:noreply,
     state
     |> record_worker_ack(ref, affected, undeliverable, delivered)
     |> finalize_completed_gathers()}
  end

  # Preserve the default GenServer behaviour for unexpected messages.
  def handle_info(msg, state) do
    Logger.warning("#{inspect(__MODULE__)} received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  # ShapeCleaner is driving this removal: shape invalidation is already in flight,
  # so just unpin the flush entry.
  defp handle_writer_down(state, shape_handle, @consumer_cleanup_reason) do
    emit_writer_down_telemetry(state, :cleanup)
    forget_writer_flush_state(state, shape_handle)
  end

  # Assume supervisor teardown (deploy/stack shutdown): leave the entry pinned on
  # purpose so a mass shutdown never mass-invalidates shapes. :noproc belongs here
  # because it masks the real exit reason of a writer that died before we could
  # monitor it — a deploy-time :shutdown as easily as a crash. If the assumption
  # is wrong, the stall check self-heals one grace period later.
  defp handle_writer_down(state, shape_handle, reason)
       when reason in @ambiguous_exit_reasons do
    emit_writer_down_telemetry(state, :shutdown)

    if FlushTracker.tracked?(state.flush_tracker, shape_handle) do
      Logger.info(
        "Writer for shape #{shape_handle} exited with #{inspect(reason)} (assumed teardown), " <>
          "leaving its flush entry pinned for the stall check"
      )
    end

    cool_writer(state, shape_handle)
  end

  # Anything else is a crash ({:shutdown, :suspend} included: a suspending consumer
  # must have no pending flush entries, so a monitored suspend is a contract
  # violation). Unpin immediately and make sure the shape is invalidated — it must
  # not resume from storage that is behind the acked WAL.
  defp handle_writer_down(state, shape_handle, reason) do
    Logger.warning(
      "Writer for shape #{shape_handle} exited with #{inspect(reason)} before completing " <>
        "its flush, removing the shape"
    )

    emit_writer_down_telemetry(state, :crash)
    Electric.ShapeCache.ShapeCleaner.remove_shapes_async(state.stack_id, [shape_handle])
    forget_writer_flush_state(state, shape_handle)
  end

  # A removed shape's stall bookkeeping must go with its flush entry: a leftover
  # deferral count is a spent budget that would fast-forward a later same-handle
  # episode to invalidation, and a leftover suspicion re-arms against nothing.
  defp forget_writer_flush_state(state, shape_handle) do
    state
    |> Map.update!(:flush_tracker, &FlushTracker.handle_shape_removed(&1, shape_handle))
    |> Map.update!(:deferral_rounds, &Map.delete(&1, shape_handle))
    |> cool_writer(shape_handle)
    |> clear_stall_suspect(shape_handle)
  end

  # A writer that died has to earn the first-flush allowance again: the consumers a
  # reconnect wakes are exactly the population whose next first flush is slow.
  defp cool_writer(state, shape_handle) do
    Map.update!(state, :warm_writers, &Map.delete(&1, shape_handle))
  end

  # Only the writer the entry is monitored for is warmed. A commit can reach a fresh pid while
  # the flush of the one it replaces is still queued: the monitor has moved to the fresh pid by
  # the time that flush is applied, and the fresh pid has flushed nothing.
  defp warm_writer(state, shape_handle, writer, true) do
    case Map.fetch(state.writer_monitors, shape_handle) do
      {:ok, {^writer, _ref}} ->
        Map.update!(state, :warm_writers, &Map.put(&1, shape_handle, writer))

      _ ->
        state
    end
  end

  defp warm_writer(state, _shape_handle, _writer, false), do: state

  # An undeliverable reason from the registry is either a publish-level failure —
  # the shape is gone from ShapeStatus or was removed after failing to resume, so
  # removal is already in flight, same as a cleanup DOWN — or the exit reason the
  # broadcast's own monitor observed for the writer.
  defp undeliverable_down_reason({:publish, _}), do: @consumer_cleanup_reason
  defp undeliverable_down_reason(reason), do: reason

  defp emit_writer_down_telemetry(state, reason_class) do
    OpenTelemetry.execute(
      [:electric, :flush_tracker, :writer_down],
      %{count: 1},
      %{stack_id: state.stack_id, reason_class: reason_class}
    )
  end

  defp monitor_writer(state, shape_handle, pid) do
    case Map.fetch(state.writer_monitors, shape_handle) do
      {:ok, {^pid, _ref}} ->
        state

      {:ok, {_old_pid, _old_ref}} ->
        # A different pid now owns this shape's entry (resumed consumer): swap the
        # monitor over to it.
        state |> demonitor_writer(shape_handle) |> monitor_writer(shape_handle, pid)

      :error ->
        # Monitoring an already-dead pid yields an immediate :noproc DOWN, so a
        # writer that dies before this call is never lost: the DOWN is classified
        # as teardown and the stall check picks the entry up if that was wrong.
        ref = Process.monitor(pid, tag: {:down, shape_handle})
        %{state | writer_monitors: Map.put(state.writer_monitors, shape_handle, {pid, ref})}
    end
  end

  defp demonitor_writer(state, shape_handle) do
    case Map.pop(state.writer_monitors, shape_handle) do
      {{_pid, ref}, writer_monitors} ->
        Process.demonitor(ref, [:flush])
        %{state | writer_monitors: writer_monitors}

      {nil, _} ->
        state
    end
  end

  # Any flush progress answers an outstanding stall challenge: if the shape's
  # entry stalls again later, its writer must be challenged afresh rather than
  # invalidated as an unresponsive suspect. (The check interval is clamped to at
  # least 1s, so with a sub-second grace period a suspect could otherwise stall
  # again before the next check despite having answered in between.)
  defp clear_stall_suspect(state, shape_handle) do
    %{state | stall_suspects: MapSet.delete(state.stall_suspects, shape_handle)}
  end

  # One line per stalled handle with everything a stall post-mortem needs: the
  # entry's offsets and age, whether a writer monitor existed, and the registered
  # consumer's liveness at that instant. Entries removed with no monitor while
  # their consumer is alive and registered are the case this exists to catch —
  # without it a removal is indistinguishable from a plain wedged writer.
  defp stall_diagnostic(state, shape_handle, now) do
    entry =
      case FlushTracker.entry(state.flush_tracker, shape_handle) do
        {last_sent, last_flushed, last_progress_at} ->
          "last_sent=#{inspect(last_sent)} last_flushed=#{inspect(last_flushed)} " <>
            "stalled_for_ms=#{now - last_progress_at}"

        nil ->
          "entry=missing"
      end

    monitor =
      case Map.fetch(state.writer_monitors, shape_handle) do
        {:ok, {pid, _ref}} -> "monitored_pid=#{inspect(pid)}"
        :error -> "monitored=false"
      end

    consumer =
      case ConsumerRegistry.whereis(state.stack_id, shape_handle) do
        nil ->
          "registered=false"

        pid ->
          case Process.info(pid, [:current_function, :message_queue_len]) do
            [{:current_function, current_function}, {:message_queue_len, queue_len}] ->
              "registered_pid=#{inspect(pid)} alive=true " <>
                "current_function=#{inspect(current_function)} queue_len=#{queue_len}"

            nil ->
              "registered_pid=#{inspect(pid)} alive=false"
          end
      end

    deferrals = Map.get(state.deferral_rounds, shape_handle, 0)

    "#{shape_handle}: #{entry} #{monitor} #{consumer} deferral_rounds=#{deferrals}"
  end

  defp stall_grace_period(stack_id) do
    Electric.StackConfig.lookup(
      stack_id,
      :flush_stall_grace_period,
      Electric.Config.default(:flush_stall_grace_period)
    )
  end

  # A writer's first flush is the slow one — cold start with a large shape count, a replay
  # burst, or a reconnect waking many consumers at once — and a slow-but-healthy
  # writer answers no challenge by design. Without an allowance, a boot removes healthy shapes
  # exactly one grace-plus-scan after replay. Anchoring the allowance to the
  # collector's own start left every writer whose first flush came later — a reconnect an hour
  # into the run — with only the steady grace, which is the same mass invalidation moved in
  # time. It is granted per writer instead: until a writer has reported a flush, its entries
  # wait out the longer grace, and the wedged-writer backstop still fires once that expires.
  #
  # Warmth belongs to a pid, not to the shape: a consumer that suspends after a completed flush
  # leaves no monitor behind, and the pid that resumes the shape later has flushed nothing yet.
  #
  # A writer with no monitor left is dead and will never flush, so it gets no allowance — the
  # orphan path is the only thing that can still clear its entry.
  defp awaiting_first_flush?(state, shape_handle, now) do
    case Map.fetch(state.writer_monitors, shape_handle) do
      {:ok, {pid, _ref}} ->
        Map.get(state.warm_writers, shape_handle) != pid and
          first_flush_grace(state.stack_id) > stalled_for(state, shape_handle, now)

      :error ->
        false
    end
  end

  defp stalled_for(state, shape_handle, now) do
    case FlushTracker.entry(state.flush_tracker, shape_handle) do
      {_last_sent, _last_flushed, last_progress_at} -> now - last_progress_at
      nil -> 0
    end
  end

  defp first_flush_grace(stack_id) do
    Electric.StackConfig.lookup(
      stack_id,
      :flush_stall_boot_grace,
      Electric.Config.default(:flush_stall_boot_grace)
    )
  end

  defp max_deferral_rounds(stack_id) do
    Electric.StackConfig.lookup(
      stack_id,
      :flush_deferral_round_limit,
      Electric.Config.default(:flush_deferral_round_limit)
    )
  end

  # Check at the configured grace period when it is shorter than the default
  # interval, so a small grace period is enforced at matching granularity
  # (clamped below so a tiny value cannot turn the check into a busy loop).
  defp schedule_stall_check(grace_period) do
    grace_period
    |> min(@stall_check_interval)
    |> max(@stall_check_interval_floor)
    |> then(&Process.send_after(self(), :check_stalled_flushes, &1))
  end

  defp do_handle_event(%Relation{} = rel, _from, state) do
    # Relations mutate partition/relation state that pending fragments were routed
    # against; finalize them first and handle the relation on the blocking path.
    state = drain_pending_gathers(state)

    OpenTelemetry.with_span(
      "pg_txn.replication_client.relation_received",
      ["rel.id": rel.id, "rel.schema": rel.schema, "rel.table": rel.table],
      state.stack_id,
      fn ->
        Logger.info("Received relation #{inspect(rel.schema)}.#{inspect(rel.table)}")
        Logger.debug(fn -> "Relation received in ShapeLogCollector: #{inspect(rel)}" end)

        result = handle_relation(state, rel)
        OpenTelemetry.wipe_interval_timer()
        result
      end
    )
  end

  defp do_handle_event(%TransactionFragment{} = txn_fragment, from, state) do
    OpenTelemetry.with_span(
      "pg_txn.replication_client.transaction_received",
      [
        num_changes: txn_fragment.change_count,
        num_relations: MapSet.size(txn_fragment.affected_relations),
        xid: txn_fragment.xid,
        complete_transaction?: TransactionFragment.complete_transaction?(txn_fragment)
      ],
      state.stack_id,
      fn ->
        OpenTelemetry.start_interval(:"shape_log_collector.logging.duration_us")

        Logger.debug(
          fn ->
            "Received transaction fragment #{txn_fragment.xid} (#{txn_fragment.change_count} changes) from Postgres at #{txn_fragment.lsn}"
          end,
          received_transaction_xid: txn_fragment.xid,
          received_transaction_num_changes: txn_fragment.change_count,
          received_transaction_lsn: to_string(txn_fragment.lsn)
        )

        Logger.debug(fn ->
          "Txn fragment received in ShapeLogCollector: #{inspect(txn_fragment)}"
        end)

        result = handle_txn_fragment(state, txn_fragment, from)

        OpenTelemetry.stop_and_save_intervals(
          total_attribute: :"shape_log_collector.transaction.total_duration_us"
        )

        put_wall_clock_duration_if_commit(txn_fragment)

        result
      end
    )
  end

  defp put_wall_clock_duration_if_commit(%TransactionFragment{
         commit: %Changes.Commit{tx_started_at: tx_started_at}
       }) do
    OpenTelemetry.add_span_attributes(
      total_processing_time:
        System.convert_time_unit(System.monotonic_time() - tx_started_at, :native, :millisecond)
    )
  end

  defp put_wall_clock_duration_if_commit(_txn_fragment), do: :ok

  defp replay_journal(%{txn_journal_base: nil} = state, _slot_offset), do: state
  defp replay_journal(%{journal_replayed?: true} = state, _slot_offset), do: state

  # No generations is the one shape of this check that cannot be read from the journal itself:
  # every reader reaches a generation by name, so a directory that lost its entries is
  # indistinguishable from one that never had any, and the coverage check below never runs.
  # The marker is what separates them. It is written beside the journal directory the first
  # time a generation is opened, so it survives losing that directory and dies with a genuine
  # storage reset.
  #
  # Reaching here with the marker present means every generation is gone. Reclaim cannot produce
  # that: `gc!/2` never touches the live generation, and `reclaim_replayed_generations/2` keeps
  # the coverage holder when no commit-bearing generation carries it. So the journal held
  # transactions the slot was acked past, and they are not recoverable from it.
  defp replay_journal(%{journal_replay_gens: []} = state, _slot_offset) do
    if state.ack_after_journal? and state.journal_used? do
      raise """
      The transaction journal directory holds no generations, but this stack has written one \
      before. Every generation is gone, and the replication slot was acked past the \
      transactions they held, so the shapes behind it cannot recover them. Resync the shapes \
      from Postgres. Deleting the marker only silences this check: the gap it reports is in \
      the shape logs, not in the journal.
      """
    end

    state
  end

  # Boot-time journal replay: feed every journaled event through the normal blocking
  # pipeline before the collector is marked ready. Consumers skip what their storage
  # already holds; the slot resumes at its acked position, which the assert below pins
  # inside the journal's coverage. An introspection failure here raises — the shape
  # subsystem restarts and retries the replay, the same escalation restore_shapes uses.
  defp replay_journal(state, slot_offset) do
    gens = state.journal_replay_gens
    last_commit = TxnJournal.last_commit_tx_offset(state.txn_journal_base, gens)

    %{committed?: committed?, reclaimed: reclaimed} =
      TxnJournal.coverage_record(state.txn_journal_base)

    # What the journal can still account for: what its generations read back, plus what left them
    # only because the consumers were durable past it. A generation that streams back nothing
    # contributes nothing, which is a fact about coverage, not a reason to stop checking it.
    coverage = [last_commit, reclaimed] |> Enum.reject(&is_nil/1) |> Enum.max(fn -> nil end)

    # No coverage at all, from a journal that has synced a boundary and had none reclaimed, means
    # the generations holding it are gone or unreadable -- the fail-open opposite of the empty
    # directory guard, reached when the newest generation is present but reads back nothing. The
    # slot was acked to those boundaries, so Postgres will not resend them and nothing else can.
    if state.ack_after_journal? and coverage == nil and committed? do
      raise """
      The transaction journal reads back no boundary at all, but this stack has synced one into \
      it and nothing has reclaimed it. The generations holding the acknowledged transactions are \
      gone or unreadable, so the shapes behind the slot cannot recover them. Resync the shapes \
      from Postgres. Deleting the journal only silences this check: the gap it reports is in the \
      shape logs, not in the journal.
      """
    end

    # The slot position names the next byte Postgres will send, one past the commit the
    # client acked it at: the standby status update reports `flushed_wal + 1`. A journal
    # ending exactly one below the slot therefore covers it completely, and journal mode
    # holds the slot at that boundary rather than letting keepalives run it forward.
    #
    # Only journal mode makes that promise. With the mode off the flush boundary drives
    # the acks and the slot legitimately runs past a leftover journal awaiting reclaim,
    # so the same gap proves nothing there.
    if state.ack_after_journal? and coverage != nil and
         coverage < slot_offset.tx_offset - 1 do
      raise """
      The transaction journal ends at #{coverage} but the replication slot was \
      acknowledged to #{slot_offset.tx_offset}. Either the journal directory lost data \
      (shapes behind the slot cannot recover it), or ELECTRIC_ACK_AFTER_JOURNAL was \
      re-enabled after running disabled, which leaves the same gap. Both need the shapes \
      resynced from Postgres. Deleting the journal only silences this check: the gap it \
      reports is in the shape logs, not in the journal.
      """
    end

    Logger.notice("Replaying #{length(gens)} transaction journal generation(s)")

    # Replay runs before the continuation seeds last_processed_offset; nil keeps the
    # is_ready_to_process guard closed while letting put_last_processed_offset update it.
    state =
      state
      |> Map.put_new(:last_processed_offset, nil)
      |> Map.put(:journal_replaying?, true)

    state =
      TxnJournal.stream_events!(state.txn_journal_base, gens)
      |> Enum.reduce(state, &replay_event/2)

    LsnTracker.clear_journal_replay_pending(state.stack_id)

    # Replay delivery measures queueing, not wedging: a flush entry is stamped at its
    # first mid-replay delivery and never refreshed by later sends, so a replay longer
    # than the stall grace would leave every not-yet-flushed writer instantly
    # removable by the first post-boot stall scans. Their grace starts here instead.
    state = %{state | flush_tracker: FlushTracker.touch_all(state.flush_tracker, now_ms())}

    state = reclaim_replayed_generations(state, gens)

    %{
      state
      | journal_replaying?: false,
        journal_replayed?: true,
        journal_replay_end: state[:last_processed_offset] || slot_offset
    }
  end

  # Replay proved delivery, not durability, so what may be deleted here differs by
  # content. Marker-only generations fan out nothing: all but the one still carrying
  # the newest coverage go now, in either mode, bounding a quiet stack to a single
  # leftover generation instead of one per boot. Commit-bearing generations must
  # outlive their transactions' consumer flushes — deleting them at replay time
  # would turn a crash between fan-out and flush into a permanent, undetectable
  # gap — so with the mode on the client's writer adopts them (its consumer-boundary
  # GC reclaims them), and with the mode off they wait in state for the flush
  # boundary to pass their last commit. A stack that never flushes again simply
  # replays them again next boot; the coverage check no longer fires mode-off.
  defp reclaim_replayed_generations(state, gens) do
    summaries =
      Enum.map(gens, fn gen ->
        {gen, TxnJournal.generation_summary(state.txn_journal_base, gen)}
      end)

    coverage =
      summaries
      |> Enum.map(fn {_gen, %{coverage: coverage}} -> coverage end)
      |> Enum.reject(&is_nil/1)
      |> Enum.max(fn -> nil end)

    {commit_bearing, marker_only} =
      Enum.split_with(summaries, fn {_gen, %{last_commit: last_commit}} ->
        last_commit != nil
      end)

    coverage_held_by_commits? =
      Enum.any?(commit_bearing, fn {_gen, %{coverage: c}} -> c == coverage end)

    dead_marker_gens =
      marker_only
      |> Enum.reject(fn {_gen, %{coverage: c}} ->
        not coverage_held_by_commits? and c != nil and c == coverage
      end)
      |> Enum.map(fn {gen, _summary} -> gen end)

    # A generation that recorded no coverage still answers the boot guard's one question -- whether
    # the directory the marker names still holds entries. When no commit-bearing generation
    # survives to hold it and every marker-only generation here has none, the reject above spares
    # nothing, so emptying the directory would leave `journal_used?` true over an empty directory
    # and the next boot would read that as every generation lost. Keep the newest so the boundary
    # the marker names always has something behind it.
    dead_marker_gens =
      if commit_bearing == [] and dead_marker_gens != [] and
           length(dead_marker_gens) == length(marker_only) do
        List.delete(dead_marker_gens, Enum.max(dead_marker_gens))
      else
        dead_marker_gens
      end

    if dead_marker_gens != [] do
      Logger.notice(
        "Deleting #{length(dead_marker_gens)} replayed marker-only journal generation(s)"
      )

      TxnJournal.delete_generations!(state.txn_journal_base, dead_marker_gens)
    end

    cond do
      state.ack_after_journal? ->
        state

      commit_bearing == [] ->
        state

      true ->
        last_commit =
          commit_bearing
          |> Enum.map(fn {_gen, %{last_commit: last_commit}} -> last_commit end)
          |> Enum.max()

        %{
          state
          | journal_gens_awaiting_durability:
              {Enum.map(commit_bearing, fn {gen, _summary} -> gen end), last_commit}
        }
    end
  end

  @doc false
  def maybe_delete_durably_replayed_journal(%{journal_gens_awaiting_durability: nil} = state),
    do: state

  def maybe_delete_durably_replayed_journal(state) do
    {gens, last_commit} = state.journal_gens_awaiting_durability

    if FlushTracker.consumer_durable_boundary(state.flush_tracker) >= last_commit do
      Logger.notice(
        "Deleting #{length(gens)} replayed journal generation(s): consumers are " <>
          "durable past their last commit and no journal writer runs in this mode"
      )

      # Persist reclaimed coverage before the delete: a crash in between drops this boundary, so
      # re-enabling journal mode later reads the deleted generations as loss.
      TxnJournal.record_coverage!(state.txn_journal_base, reclaimed: last_commit)

      TxnJournal.delete_generations!(state.txn_journal_base, gens)

      %{state | journal_gens_awaiting_durability: nil}
    else
      state
    end
  end

  defp replay_event(%Relation{} = rel, state) do
    {:ok, state} = handle_relation(state, rel)
    state
  end

  # Idle-boundary markers widen the journal's coverage for the boot check above;
  # they carry no changes to fan out.
  defp replay_event({:idle_slot_boundary, _tx_offset}, state), do: state

  defp replay_event(%TransactionFragment{} = fragment, state) do
    fragment = fragment |> Map.put_new(:full_xid, nil) |> refresh_replayed_commit_clock()

    case handle_txn_fragment(state, fragment, :journal_replay) do
      {:ok, state} ->
        state

      {other, _state} ->
        raise "Transaction journal replay failed at #{inspect(fragment.last_log_offset)}: #{inspect(other)}"
    end
  end

  # tx_started_at is a monotonic timestamp from the boot that journaled the fragment;
  # replayed under a new boot it would poison the wall-clock telemetry it feeds.
  defp refresh_replayed_commit_clock(
         %TransactionFragment{commit: %Changes.Commit{} = commit} = fragment
       ),
       do: %{fragment | commit: %{commit | tx_started_at: System.monotonic_time()}}

  defp refresh_replayed_commit_clock(fragment), do: fragment

  # If we've already processed a txn_fragment, then drop it without processing. A duplicate
  # of a fragment still in the scatter-ahead pipeline (a caller replaying after a collapse)
  # must not double-book its commit in the FlushTracker, so finalize the pipeline first.
  defp handle_txn_fragment(
         %{last_processed_offset: last_processed_offset} = state,
         txn_fragment,
         _from
       )
       when LogOffset.is_log_offset_lte(txn_fragment.last_log_offset, last_processed_offset) do
    state = drain_pending_gathers(state)

    Logger.debug(fn ->
      "Dropping transaction fragment as last_log_offset #{txn_fragment.last_log_offset} not greater than last processed #{last_processed_offset}"
    end)

    OpenTelemetry.add_span_attributes("txn.is_dropped": true)

    flush_tracker =
      if txn_fragment.commit do
        FlushTracker.handle_txn_fragment(state.flush_tracker, txn_fragment, [], now_ms())
      else
        state.flush_tracker
      end

    {:ok, %{state | flush_tracker: flush_tracker}}
  end

  defp handle_txn_fragment(
         %{last_processed_offset: last_processed_offset},
         %TransactionFragment{
           changes: [%{log_offset: first_log_offset} | _],
           last_log_offset: last_log_offset
         },
         _from
       )
       when LogOffset.is_log_offset_lte(first_log_offset, last_processed_offset) and
              LogOffset.is_log_offset_lt(last_processed_offset, last_log_offset) do
    raise """
    Received TransactionFragment that has already been partially processed.

    This scenario is not currently supported. It could occur if the
    batch size was changed while restarting the replication client.

    First log offset: #{inspect(first_log_offset)}
    last processed offset: #{inspect(last_processed_offset)}
    last log offset: #{inspect(last_log_offset)}
    """
  end

  defp handle_txn_fragment(state, txn_fragment, from) do
    OpenTelemetry.add_span_attributes("txn.is_dropped": false)

    OpenTelemetry.start_interval(:"shape_log_collector.fill_keys_in_txn.duration_us")

    case fill_keys(txn_fragment, state) do
      {:ok, txn_fragment} ->
        OpenTelemetry.start_interval(:"partitions.handle_transaction.duration_us")

        {partitions, txn_fragment} =
          Partitions.handle_txn_fragment(state.partitions, txn_fragment)

        state =
          state
          |> Map.put(:partitions, partitions)
          |> put_last_processed_offset(txn_fragment)

        if scatter_ahead_eligible?(state) do
          {:async, publish_scatter_ahead(state, txn_fragment, from)}
        else
          {:ok, publish(state, txn_fragment)}
        end

      {:error, reason} ->
        Logger.warning(
          "Failed to introspect relations affected by transaction #{txn_fragment.xid}: #{inspect(reason)}. Replication is paused until introspection succeeds"
        )

        {normalize_inspector_error(reason), state}
    end
  end

  # Scatter-ahead applies only when every shape is worker-owned: with no dependency
  # shapes there is no layered own-publish to order behind the gather and no subquery
  # predicate reading materializer state, so routing the next fragment while workers
  # still process the previous one is pure. One dependency shape disables it and every
  # fragment takes the blocking path — registration changes drain the pipeline first,
  # so the two modes never overlap.
  # Journal replay has no live caller to receive a deferred reply, so it always publishes
  # on the blocking path.
  defp scatter_ahead_eligible?(%{journal_replaying?: true}), do: false
  defp scatter_ahead_eligible?(%{workers: workers, own_shape_count: 0}), do: workers != nil
  defp scatter_ahead_eligible?(_state), do: false

  defp publish(state, event) do
    OpenTelemetry.start_interval(:"shape_log_collector.event_routing.duration_us")

    context = OpenTelemetry.get_current_context()

    # Routing happens here, once, for every shape — including the shapes the shard workers
    # publish. Workers hold no filter: they receive the pre-routed decision list and slice it
    # to the shapes they own, so sharding duplicates no routing or encoding work. Routing
    # before any consumer processes the event also keeps subquery predicates reading
    # pre-publish materializer state, exactly as the single-process path does. The gather is
    # a barrier, so by the time the collector publishes its layered (dependency-bearing)
    # shapes every parent has been fully acknowledged — the same parents-before-dependents
    # order as the single-process path.
    {events_by_handle, worker_payload, state} = route_and_assemble_own(state, event)

    own_affected = Map.keys(events_by_handle) |> MapSet.new()

    {worker_affected, worker_undeliverable, worker_delivered} =
      scatter_gather(state, worker_payload, context)

    affected_shapes = MapSet.union(own_affected, worker_affected)
    affected_shape_count = MapSet.size(affected_shapes)

    OpenTelemetry.add_span_attributes(
      "shape_log_collector.affected_shape_count": affected_shape_count
    )

    OpenTelemetry.execute(
      [:electric, :shape_log_collector, :transaction],
      %{affected_shape_count: affected_shape_count},
      %{stack_id: state.stack_id}
    )

    OpenTelemetry.start_interval(:"shape_log_collector.publish.duration_us")

    # The layered publish covers only collector-owned shapes (events_by_handle holds no
    # worker slices — the shard workers published theirs during the gather). Worker
    # undeliverables and delivered pids seed the accumulators so the classification and
    # writer monitoring below cover both paths.
    {undeliverable, delivered_pids} =
      for layer <- DependencyLayers.get_for_handles(state.dependency_layers, own_affected),
          reduce: {worker_undeliverable, worker_delivered} do
        {undeliverable_acc, delivered_acc} ->
          # Each publish is synchronous, so layers will be processed in order
          layer_events =
            Map.new(layer, fn handle ->
              {handle, {:handle_event, Map.fetch!(events_by_handle, handle), context}}
            end)

          {layer_undeliverable, layer_delivered} =
            ConsumerRegistry.publish(layer_events, state.registry_state)

          {Map.merge(undeliverable_acc, layer_undeliverable),
           Map.merge(delivered_acc, layer_delivered)}
      end

    OpenTelemetry.start_interval(:"shape_log_collector.set_last_processed_lsn.duration_us")

    finalize_publish(
      state,
      event,
      processed_lsn(state),
      affected_shapes,
      undeliverable,
      delivered_pids
    )
  end

  # Journal replay starts with no processed offset — Postgres streams its relation messages
  # before the first transaction, so the journal does too, and a relation carries none.
  defp processed_lsn(state) do
    case Map.get(state, :last_processed_offset) do
      nil -> nil
      offset -> Lsn.from_integer(offset.tx_offset)
    end
  end

  # Everything after the fan-out, shared by the blocking publish and the scatter-ahead
  # finalization: durable progress, undeliverable classification, and the commit
  # bookkeeping. The two differ only in where the LSN and the accumulators come from.
  defp finalize_publish(state, event, lsn, affected_shapes, undeliverable, delivered_pids) do
    delivered_shapes =
      MapSet.difference(affected_shapes, undeliverable |> Map.keys() |> MapSet.new())

    down_reasons =
      Map.new(undeliverable, fn {handle, reason} ->
        {handle, undeliverable_down_reason(reason)}
      end)

    # A shape whose writer crashed on this event lacks it, tracked or not, and must not outlive
    # the position this event moves the flush boundary to. Invalidate it before anything below
    # can move the boundary, not after: an asynchronous removal leaves a window in which the
    # slot is acked past an event the shape never received.
    case for({handle, reason} <- down_reasons, crash_exit?(reason), do: handle) do
      [] ->
        :ok

      crashed ->
        Electric.ShapeCache.ShapeCleaner.remove_shapes_waiting(
          state.stack_id,
          crashed,
          state.gather_stall_timeout
        )
    end

    # An undeliverable shape may still hold a pending flush entry from an
    # earlier commit. Run the failure through the same classification as a
    # writer DOWN instead of blindly unpinning: a crash still invalidates the
    # shape, and an ambiguous reason leaves the entry pinned for the stall
    # check rather than disarming that backstop.
    state =
      Enum.reduce(undeliverable, state, fn {shape_handle, _reason}, state ->
        state = demonitor_writer(state, shape_handle)

        if FlushTracker.tracked?(state.flush_tracker, shape_handle) do
          handle_writer_down(state, shape_handle, Map.fetch!(down_reasons, shape_handle))
        else
          state
        end
      end)

    # A shape whose writer exited in a way that may be a teardown is kept, so the boundary has
    # to wait for the stall check to rule on it. That holds whether or not it had a flush
    # pending: an untracked shape gets an entry at this commit, and a tracked one has its sent
    # position raised to it, so a late flush of an earlier commit cannot complete the entry.
    # A fragment that is not the commit carries its shapes to the commit.
    held =
      for {handle, reason} <- down_reasons,
          reason in @ambiguous_exit_reasons,
          into: state.undelivered_pins,
          do: handle

    # This tracker is served as `global_last_seen_lsn`, which promises "no further messages <= T".
    # Only a completed transaction fragment names such a T. A `commit: nil` fragment cannot: storage
    # withholds it until `signal_txn_commit!/2` moves the committed boundary, and the rest of the
    # transaction then arrives at that same LSN, past where a reader who trusted the promise has
    # moved. A Relation carries no offset of its own — `lsn` here is the preceding in-flight
    # fragment's position, so advancing on a relation would publish an uncommitted transaction's
    # final LSN as up-to-date. Relations change no durable position, so skipping them is safe. The
    # crashed consumers above are invalidated first, so the frontier only goes public once the
    # shapes that cannot serve it are out of the way.
    if lsn && completed_transaction?(event),
      do: LsnTracker.set_last_processed_lsn(state.stack_id, lsn)

    case event do
      %TransactionFragment{commit: commit} when not is_nil(commit) ->
        LsnTracker.broadcast_last_seen_lsn(state.stack_id, lsn)

        flush_tracker =
          FlushTracker.handle_txn_fragment(
            state.flush_tracker,
            event,
            MapSet.union(delivered_shapes, held),
            now_ms()
          )

        # Every delivered shape still tracked after this commit gets a monitor on
        # the pid that actually received it — not just newly tracked shapes. The
        # suspend-retry path in ConsumerRegistry.publish/2 can deliver a commit
        # for an already-tracked shape to a fresh consumer pid while the previous
        # pid's completing flush cast is still in our mailbox; monitor_writer
        # swaps the monitor over and flushes the old pid's queued DOWN, so the
        # suspended predecessor's exit is never misread as a crash.
        Enum.reduce(
          delivered_pids,
          %{state | flush_tracker: flush_tracker, undelivered_pins: MapSet.new()},
          fn {shape_handle, pid}, state ->
            if FlushTracker.tracked?(state.flush_tracker, shape_handle) do
              monitor_writer(state, shape_handle, pid)
            else
              state
            end
          end
        )

      _ ->
        %{state | undelivered_pins: held}
    end
  end

  # A writer exit that is neither a ShapeCleaner-driven removal (already in flight) nor ambiguous
  # between a teardown and a crash: the shape cannot be trusted to resume from its storage.
  defp crash_exit?(reason),
    do: reason != @consumer_cleanup_reason and reason not in @ambiguous_exit_reasons

  defp completed_transaction?(%TransactionFragment{commit: commit}), do: not is_nil(commit)
  defp completed_transaction?(_event), do: false

  # The scatter-ahead path: route and scatter now, finalize when the workers ack. The
  # caller's reply is deferred, so the replication client can pipeline the next fragment
  # into this process while the workers and consumers are still fanning this one out —
  # the routing of fragment k+1 overlaps the publish of fragment k instead of waiting
  # behind the gather barrier.
  defp publish_scatter_ahead(state, %TransactionFragment{} = fragment, from) do
    OpenTelemetry.start_interval(:"shape_log_collector.event_routing.duration_us")

    context = OpenTelemetry.get_current_context()

    {own_events, payload, state} = route_and_assemble_own(state, fragment)

    # Eligibility guarantees no own shapes; a non-empty own map here means the
    # invariant broke and finalize would silently drop shapes.
    true = own_events == %{}

    key = :erlang.unique_integer()
    true = :ets.insert(state.scatter_table, {key, payload})

    refs =
      for pid <- Tuple.to_list(state.workers),
          do: RouterWorker.scatter(pid, {state.scatter_table, key}, context)

    entry = %{
      fragment: fragment,
      from: from,
      key: key,
      refs: MapSet.new(refs),
      ref_workers: refs |> Enum.with_index() |> Map.new(),
      scattered_at: now_ms(),
      affected: MapSet.new(),
      undeliverable: %{},
      delivered: %{}
    }

    Map.update!(state, :pending_gathers, &:queue.in(entry, &1))
  end

  defp record_worker_ack(state, ref, affected, undeliverable, delivered) do
    entries = :queue.to_list(state.pending_gathers)

    # An ack no pending entry claims would leave its fragment waiting forever — wedging
    # the pipeline silently is worse than crashing and re-streaming from the slot.
    true = Enum.any?(entries, &MapSet.member?(&1.refs, ref))

    entries =
      entries
      |> Enum.map(fn entry ->
        if MapSet.member?(entry.refs, ref) do
          %{
            entry
            | refs: MapSet.delete(entry.refs, ref),
              affected: MapSet.union(entry.affected, affected),
              undeliverable: Map.merge(entry.undeliverable, undeliverable),
              delivered: Map.merge(entry.delivered, delivered)
          }
        else
          entry
        end
      end)

    %{state | pending_gathers: :queue.from_list(entries)}
  end

  defp finalize_completed_gathers(state) do
    case :queue.out(state.pending_gathers) do
      {{:value, %{refs: refs} = entry}, rest} ->
        if MapSet.size(refs) == 0 do
          %{state | pending_gathers: rest}
          |> finalize_gather_entry(entry)
          |> finalize_completed_gathers()
        else
          state
        end

      {:empty, _} ->
        drop_stray_flush_notifications(state)
    end
  end

  # The deferred tail of publish/2: everything after the gather, minus the own layered
  # publish (empty by eligibility). Runs strictly in fragment order.
  defp finalize_gather_entry(state, %{fragment: fragment} = entry) do
    :ets.delete(state.scatter_table, entry.key)

    affected_shape_count = MapSet.size(entry.affected)

    OpenTelemetry.execute(
      [:electric, :shape_log_collector, :transaction],
      %{affected_shape_count: affected_shape_count},
      %{stack_id: state.stack_id}
    )

    # state.last_processed_offset may already be ahead of this fragment — durable
    # progress must track the fragment being finalized, not the newest one routed.
    state =
      finalize_publish(
        state,
        fragment,
        Lsn.from_integer(fragment.last_log_offset.tx_offset),
        entry.affected,
        entry.undeliverable,
        entry.delivered
      )

    GenServer.reply(entry.from, :ok)

    apply_buffered_flush_notifications(state)
  end

  # Blocks until every scattered-ahead fragment has finalized. Used by the paths that
  # mutate state the pending fragments were routed against (relations, shape
  # registration) and by the duplicate-fragment drop path.
  defp drain_pending_gathers(state) do
    case :queue.peek(state.pending_gathers) do
      :empty ->
        state

      {:value, _} ->
        # The `after` window restarts on every ack, so it only bounds inter-ack
        # idleness; a worker acking non-head entries at a trickle would reset it
        # forever while the head ages unbounded. The head-age check enforces the
        # same absolute deadline the periodic pass applies outside drains.
        check_stalled_gather_head!(state, now_ms())

        receive do
          {ref, {%MapSet{} = affected, undeliverable, delivered}} when is_reference(ref) ->
            state
            |> record_worker_ack(ref, affected, undeliverable, delivered)
            |> finalize_completed_gathers()
            |> drain_pending_gathers()
        after
          state.gather_stall_timeout ->
            pending = :queue.to_list(state.pending_gathers)

            head_xid =
              case pending do
                [%{fragment: fragment} | _] -> Map.get(fragment, :xid)
                _ -> nil
              end

            raise "Scatter gather stalled: #{length(pending)} pending fragment(s) received " <>
                    "no router-worker ack for #{state.gather_stall_timeout}ms (head xid: " <>
                    "#{inspect(head_xid)}). Crashing to re-stream from the slot rather " <>
                    "than wedging the pipeline silently."
        end
    end
  end

  # The async scatter-gather path has no deadline of its own: a router worker that
  # stays alive but never acks pins the queue head — and the entire pipeline
  # backpressured behind it — invisibly and indefinitely. Symmetric with the blocking drain's
  # raise above; runs on the :check_stalled_flushes cadence.
  defp check_stalled_gather_head!(state, now) do
    case :queue.peek(state.pending_gathers) do
      {:value, %{scattered_at: scattered_at} = entry} ->
        age = now - scattered_at

        if age > state.gather_stall_timeout do
          outstanding =
            entry.refs
            |> Enum.map(&Map.get(entry.ref_workers, &1))
            |> Enum.sort()

          raise "Async scatter gather stalled: the queue head received no ack from " <>
                  "router worker(s) #{inspect(outstanding)} for #{age}ms (head xid: " <>
                  "#{inspect(Map.get(entry.fragment, :xid))}). Crashing to re-stream " <>
                  "from the slot rather than wedging the pipeline silently."
        end

        :ok

      _ ->
        :ok
    end
  end

  # A consumer can flush and notify before the fragment that sent it the data has
  # finalized — the FlushTracker would treat the shape as untracked and silently drop
  # the notification, wedging the flush boundary. Buffer those until the registration
  # catches up; anything still untracked once the pipeline is empty is a genuine stray
  # (removed shape) and is dropped, matching the tracker's own behaviour.
  defp apply_or_buffer_flush_notification(state, shape_id, offset, writer) do
    if :queue.is_empty(state.pending_gathers) or
         FlushTracker.tracked?(state.flush_tracker, shape_id) do
      apply_flush_notification(state, shape_id, offset, writer)
    else
      Map.update!(state, :pending_flush_notifications, &[{shape_id, offset, writer} | &1])
    end
  end

  defp apply_flush_notification(state, shape_id, offset, writer) do
    # Only a flush the tracker was waiting for shows a live writer. A notification for a shape it no
    # longer tracks is a stray from a removed shape, and recording it would keep a handle nobody
    # clears.
    tracked_before? = FlushTracker.tracked?(state.flush_tracker, shape_id)

    state =
      state
      |> Map.update!(
        :flush_tracker,
        &FlushTracker.handle_flush_notification(&1, shape_id, offset, now_ms())
      )
      |> Map.update!(:deferral_rounds, &Map.delete(&1, shape_id))
      |> warm_writer(shape_id, writer, tracked_before?)

    # A flush that completes the entry removes it from the tracker; its writer no
    # longer needs watching.
    state =
      if FlushTracker.tracked?(state.flush_tracker, shape_id),
        do: state,
        else: demonitor_writer(state, shape_id)

    state
    |> clear_stall_suspect(shape_id)
    |> maybe_delete_durably_replayed_journal()
  end

  defp apply_buffered_flush_notifications(%{pending_flush_notifications: []} = state), do: state

  defp apply_buffered_flush_notifications(state) do
    {state, kept} =
      state.pending_flush_notifications
      |> Enum.reverse()
      |> Enum.reduce({%{state | pending_flush_notifications: []}, []}, fn
        {shape_id, offset, writer} = notification, {state, kept} ->
          if FlushTracker.tracked?(state.flush_tracker, shape_id) do
            {apply_flush_notification(state, shape_id, offset, writer), kept}
          else
            {state, [notification | kept]}
          end
      end)

    # kept was built oldest-to-newest with prepends, so it is already newest-first —
    # the same convention the cast handler stores with.
    %{state | pending_flush_notifications: kept}
  end

  defp drop_stray_flush_notifications(%{pending_flush_notifications: []} = state), do: state

  defp drop_stray_flush_notifications(state) do
    state = apply_buffered_flush_notifications(state)
    %{state | pending_flush_notifications: []}
  end

  defp handle_relation(state, rel) do
    {relation_status, updated_rel, tracker_state} =
      AffectedColumns.transform_relation(rel, state.tracked_relations)

    # PG omits details from the relation message (nullability in particular) and sends one even
    # when nothing visibly changed, so any relation we process can mean the cached schema is
    # stale. Clean unconditionally: gating on `rel == updated_rel` skipped the :changed case,
    # whose cache clean then depended on a downstream consumer's clean_table -- and that never
    # runs when the changed table has no active consumer, leaving the stale entry until the
    # periodic SchemaReconciler.
    Inspector.clean(updated_rel.id, state.inspector)

    case Partitions.handle_relation(state.partitions, updated_rel) do
      {:ok, partitions} ->
        state = %{state | partitions: partitions}

        with {:ok, state} <- publish_relation(state, updated_rel, relation_status) do
          :ok =
            PersistentReplicationState.set_tracked_relations(
              tracker_state,
              state.persistent_replication_data_opts
            )

          {:ok, %{state | tracked_relations: tracker_state}}
        end

      {:error, reason} ->
        Logger.warning(
          "Failed to introspect relation #{Electric.Utils.inspect_relation({updated_rel.schema, updated_rel.table})}: #{inspect(reason)}. Replication is paused until introspection succeeds"
        )

        {normalize_inspector_error(reason), state}
    end
  end

  defp normalize_inspector_error(:connection_not_available),
    do: {:error, :connection_not_available}

  # Introspection failed for a reason other than the connection pool being
  # down, e.g. the database returned an error to the catalog query. Reply with
  # the one error the replication client knows how to recover from: it pauses
  # the stream and redelivers the event, giving the introspection another
  # chance instead of crashing the collector.
  defp normalize_inspector_error(_reason), do: {:error, :connection_not_available}

  defp publish_relation(state, rel, :unchanged) do
    OpenTelemetry.add_span_attributes("rel.is_dropped": true)

    Logger.debug(fn ->
      "Dropping unchanged relation message for #{inspect(rel.schema)}.#{inspect(rel.table)}"
    end)

    {:ok, state}
  end

  defp publish_relation(state, rel, _relation_status) do
    case state do
      %{subscriptions: 0} ->
        OpenTelemetry.add_span_attributes("rel.is_dropped": true)

        Logger.debug(fn ->
          "Dropping relation message for #{inspect(rel.schema)}.#{inspect(rel.table)}: no active consumers"
        end)

        {:ok, state}

      _ ->
        OpenTelemetry.add_span_attributes("rel.is_dropped": false)

        # relation changes will also start consumers if they're not running
        {:ok, publish(state, rel)}
    end
  end

  defp remove_subscription(%{subscriptions: count} = state, shape_handle) do
    OpenTelemetry.with_span(
      "shape_log_collector.remove_shape",
      [shape_handle: shape_handle],
      state.stack_id,
      fn ->
        if Map.has_key?(state.shape_owners, shape_handle) do
          Logger.debug("Deleting shape #{shape_handle}")

          OpenTelemetry.start_interval(:"unsubscribe_shape.remove_subscription.duration_us")

          OpenTelemetry.start_interval(:"unsubscribe_shape.remove_from_event_router.duration_us")

          owner = Map.fetch!(state.shape_owners, shape_handle)
          event_router = remove_shape_from_owner(state, owner, shape_handle)

          OpenTelemetry.start_interval(:"unsubscribe_shape.remove_from_partitions.duration_us")
          partitions = Partitions.remove_shape(state.partitions, shape_handle)

          OpenTelemetry.start_interval(:"unsubscribe_shape.demonitor_writer.duration_us")
          state = demonitor_writer(state, shape_handle)

          OpenTelemetry.start_interval(:"unsubscribe_shape.remove_from_flush_tracker.duration_us")
          flush_tracker = FlushTracker.handle_shape_removed(state.flush_tracker, shape_handle)

          OpenTelemetry.start_interval(
            :"unsubscribe_shape.remove_from_dependency_layers.duration_us"
          )

          dependency_layers =
            DependencyLayers.remove_dependency(state.dependency_layers, shape_handle)

          Electric.Shapes.ConsumerRegistry.remove_consumer(shape_handle, state.registry_state)

          OpenTelemetry.stop_and_save_intervals(
            total_attribute: "unsubscribe_shape.total_duration_us"
          )

          {:ok,
           %{
             state
             | subscriptions: count - 1,
               event_router: event_router,
               partitions: partitions,
               dependency_layers: dependency_layers,
               shape_owners: Map.delete(state.shape_owners, shape_handle),
               own_shape_count: state.own_shape_count - own_increment(owner),
               flush_tracker: flush_tracker,
               deferral_rounds: Map.delete(state.deferral_rounds, shape_handle),
               undelivered_pins: MapSet.delete(state.undelivered_pins, shape_handle)
           }
           |> cool_writer(shape_handle)
           |> clear_stall_suspect(shape_handle)
           |> log_subscription_status()}
        else
          # This may happen as we attempt to remove a shape multiple times
          # depending on the source of the delete, on the understanding that
          # removal is idempotent.
          #
          # A create cancelled before it registered here still started (and registered) its
          # consumer, so its ConsumerRegistry entry outlives the shape unless we reclaim it on
          # the removal that never had a subscription to tear down. The delete is idempotent.
          Electric.Shapes.ConsumerRegistry.remove_consumer(shape_handle, state.registry_state)
          {:error, "shape #{shape_handle} not registered"}
        end
      end
    )
  end

  defp log_subscription_status(%{subscriptions: active} = state) do
    Logger.debug(fn ->
      "#{active} consumers of replication stream"
    end)

    state
  end

  # Route once, assemble the collector-owned slice, and build the worker payload. With
  # sharding off every shape is collector-owned, so this is exactly the single-process
  # pipeline; with sharding on the workers assemble their slices from these same decisions.
  defp route_and_assemble_own(state, %TransactionFragment{} = fragment) do
    {decisions, event_router} =
      EventRouter.route_transaction_fragment(state.event_router, fragment)

    {own_decisions, sharded_decisions} = split_decisions(state, decisions)

    {own_events, assembler} =
      FragmentAssembler.assemble(event_router.assembler, fragment, own_decisions)

    state = %{state | event_router: %{event_router | assembler: assembler}}

    # The payload carries the changes inside the decisions; stripping them from the fragment
    # halves what the workers copy out of the scatter table.
    {own_events, {:fragment, %{fragment | changes: []}, sharded_decisions}, state}
  end

  defp route_and_assemble_own(state, %Relation{} = relation) do
    {shape_ids, event_router} = EventRouter.route_relation(state.event_router, relation)
    {own_ids, sharded_ids} = split_shape_ids(state, shape_ids)
    state = %{state | event_router: event_router}
    {Map.new(own_ids, &{&1, relation}), {:relation, relation, sharded_ids}, state}
  end

  defp split_decisions(%{workers: nil}, decisions), do: {decisions, []}
  defp split_decisions(%{own_shape_count: 0}, decisions), do: {[], decisions}

  defp split_decisions(%{shape_owners: owners}, decisions) do
    decisions
    |> Enum.reduce({[], []}, fn {change, prepared_groups, unprepared_ids}, {own, sharded} ->
      {own_groups, sharded_groups} =
        Enum.reduce(prepared_groups, {[], []}, fn {prepared_change, shape_ids},
                                                  {own_groups, sharded_groups} ->
          {own_ids, sharded_ids} = Enum.split_with(shape_ids, &(Map.get(owners, &1) == :own))

          {prepend_group(own_groups, prepared_change, own_ids),
           prepend_group(sharded_groups, prepared_change, sharded_ids)}
        end)

      {own_plain, sharded_plain} =
        Enum.split_with(unprepared_ids, &(Map.get(owners, &1) == :own))

      {prepend_decision(own, change, own_groups, own_plain),
       prepend_decision(sharded, change, sharded_groups, sharded_plain)}
    end)
    |> then(fn {own, sharded} -> {Enum.reverse(own), Enum.reverse(sharded)} end)
  end

  defp prepend_group(groups, _prepared_change, []), do: groups

  defp prepend_group(groups, prepared_change, shape_ids),
    do: [{prepared_change, shape_ids} | groups]

  defp prepend_decision(decisions, _change, [], []), do: decisions

  defp prepend_decision(decisions, change, prepared_groups, unprepared_ids),
    do: [{change, prepared_groups, unprepared_ids} | decisions]

  defp split_shape_ids(%{workers: nil}, shape_ids), do: {shape_ids, []}
  defp split_shape_ids(%{own_shape_count: 0}, shape_ids), do: {[], shape_ids}

  defp split_shape_ids(%{shape_owners: owners}, shape_ids),
    do: Enum.split_with(shape_ids, &(Map.get(owners, &1) == :own))

  # Fragments are always scattered when sharding is on, even with no sharded decisions:
  # workers track transaction participation, and a commit-only fragment must still fan
  # commits out to their in-transaction shapes.
  defp scatter_gather(%{workers: nil}, _payload, _context), do: {MapSet.new(), %{}, %{}}

  defp scatter_gather(state, payload, context) do
    key = :erlang.unique_integer()
    true = :ets.insert(state.scatter_table, {key, payload})

    refs =
      for pid <- Tuple.to_list(state.workers),
          do: RouterWorker.scatter(pid, {state.scatter_table, key}, context)

    result = RouterWorker.gather(refs, state.gather_stall_timeout)
    :ets.delete(state.scatter_table, key)
    result
  end

  # Dependency-free shapes are published by a shard worker when sharding is enabled; shapes
  # with dependencies stay on the collector's own layered path. Every shape goes into the
  # collector's router either way — routing is central, only assembly and publish shard.
  # Both dependency fields are checked: a restored subquery shape can carry plans before its
  # dependency handles are known.
  defp add_shape_to_owner(state, shape_handle, shape) do
    do_add_shape_to_owner(mark_unblockable_dependencies(state, shape), shape_handle, shape)
  end

  # An outer shape's materializer subscribes to its dependency shapes' consumers and walks
  # their individual changes, so those consumers can never receive framed blocks. Marks are
  # applied before the shape registers (and before any subsequent fragment routes), and ride
  # the same FIFO channel as scatters on the workers.
  defp mark_unblockable_dependencies(state, %{shape_dependencies_handles: []}), do: state

  defp mark_unblockable_dependencies(state, %{shape_dependencies_handles: handles} = _shape) do
    if state.workers do
      state.workers
      |> Tuple.to_list()
      |> Enum.each(&RouterWorker.mark_unblockable(&1, handles))
    end

    %{state | event_router: EventRouter.mark_unblockable(state.event_router, handles)}
  end

  defp do_add_shape_to_owner(%{workers: workers} = state, shape_handle, shape)
       when workers != nil and shape.shape_dependencies == [] and
              shape.shape_dependencies_handles == [] do
    index = shard_shape(workers, state.worker_prepare?, shape_handle, shape)
    {EventRouter.add_shape(state.event_router, shape_handle, shape), index}
  end

  defp do_add_shape_to_owner(state, shape_handle, shape) do
    {EventRouter.add_shape(state.event_router, shape_handle, shape), :own}
  end

  @doc false
  # The shard worker of a shape without dependencies.
  def shard_shape(workers, worker_prepare?, shape_handle, shape) do
    index = :erlang.phash2(shape_handle, tuple_size(workers))

    # Deferred preparation converts on the worker, which therefore needs the shape struct.
    # The cast shares the scattered events' collector→worker channel, so registration always
    # lands before any fragment that routes to this shape. A restore that casts from other
    # processes syncs them with the worker before it returns (see `Restore`).
    if worker_prepare?,
      do: RouterWorker.add_shape(elem(workers, index), shape_handle, without_where_eval(shape))

    index
  end

  # `Shape.convert_routed_change/2` reads `where` only as the nil/not-nil distinction that picks a
  # clause: routing has already settled membership, so the worker never evaluates the predicate.
  # The cast above copies whatever it is handed into the worker's heap, and for a long IN list
  # the parsed tree is the bulk of that copy -- the same bytes per listed value that
  # `Filter.add_shape/3` drops before it stores the shape. The clause text is a refc binary and is
  # shared rather than copied, so dropping the tree is the whole saving.
  defp without_where_eval(%{where: %Expr{} = where} = shape),
    do: %{shape | where: %{where | eval: nil}}

  defp without_where_eval(shape), do: shape

  defp own_increment(:own), do: 1
  defp own_increment(_index), do: 0

  defp remove_shape_from_owner(%{workers: workers} = state, index, shape_handle)
       when is_integer(index) do
    :ok = RouterWorker.remove_shape(elem(workers, index), shape_handle)
    EventRouter.remove_shape(state.event_router, shape_handle)
  end

  defp remove_shape_from_owner(state, :own, shape_handle) do
    EventRouter.remove_shape(state.event_router, shape_handle)
  end

  defp put_last_processed_offset(state, %TransactionFragment{last_log_offset: last_log_offset}),
    do: %{state | last_processed_offset: last_log_offset}

  if Mix.env() == :test do
    def activate_mocked_functions_from_test_process do
      Support.TestUtils.activate_mocked_functions_for_module(__MODULE__)
    end
  else
    def activate_mocked_functions_from_test_process, do: :noop
  end

  defp fill_keys(batch, state) do
    with {:ok, pk_cols_of_relations} <- pk_cols_of_relations(batch, state) do
      batch =
        Map.update!(batch, :changes, fn changes ->
          Enum.map(
            changes,
            &Changes.fill_key(&1, pk_cols_of_relations[Map.get(&1, :relation)])
          )
        end)

      {:ok, batch}
    end
  end

  defp pk_cols_of_relations(batch, state) do
    batch.affected_relations
    |> map_while_ok(fn relation ->
      with {:ok, pk_cols} <- pk_cols_of_relation(relation, state) do
        {:ok, {relation, pk_cols}}
      end
    end)
    |> map_if_ok(&Map.new/1)
  end

  defp pk_cols_of_relation(relation, state) do
    with {:ok, {oid, _}} <- Inspector.load_relation_oid(relation, state.inspector),
         {:ok, info} <- Inspector.load_column_info(oid, state.inspector) do
      {:ok, Inspector.get_pk_cols(info)}
    else
      :table_not_found ->
        # The table was dropped (or renamed) after these changes were written
        # to the WAL, so its primary key can no longer be introspected. Key
        # the changes on the full record — the same fallback used for tables
        # without a primary key — and leave handling of the dropped table to
        # the affected shapes further down the stack.
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
