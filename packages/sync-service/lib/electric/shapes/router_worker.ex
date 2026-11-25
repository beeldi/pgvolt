defmodule Electric.Shapes.RouterWorker do
  @moduledoc """
  One shard of the ShapeLogCollector's publish fan-out.

  The collector routes each event once (memoized filter) and hands the decision list over
  via a public ETS table; each worker slices the decisions to the shapes it owns —
  `phash2(shape_handle, shard_count) == index` over the dependency-free shapes — assembles
  their fragments with the same `FragmentAssembler` code the unsharded path runs, publishes
  to its consumers, and runs its own reply barrier.

  Class conversions come in two modes. Inline (default): the collector prepares once per
  fan-out class and the worker's slice carries ready `PreparedChange`s — no duplicated
  per-row work, but the encode serializes on the collector. Deferred: decisions carry
  `{:prepare, shape_ids}` markers and each worker converts/encodes for its own slice from
  its own shape registry — the encode duplicates across K workers but runs in parallel,
  taking the dominant cost off the routing spine. With a payload arena, each worker owns its
  own arena generation and syncs it before publishing a commit fragment, so any ref its
  consumers can flush is already durable.

  Shapes with dependencies never land here: the collector keeps them on its serial, layered
  publish path so parents are fully acknowledged before dependents.

  Workers are started from and linked to the collector: a worker crash takes the collector
  down with it, the same blast radius as a crash in the single-process path.
  """
  use GenServer

  require Logger

  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.Shapes.ConsumerRegistry
  alias Electric.Shapes.EventRouter
  alias Electric.Shapes.FragmentAssembler
  alias Electric.Telemetry.OpenTelemetry

  def start_link(stack_id, index, shard_count, registry_state, opts \\ []) do
    GenServer.start_link(__MODULE__, {stack_id, index, shard_count, registry_state, opts})
  end

  @doc """
  Drop a removed shape's transaction bookkeeping so it stops receiving commit-only fragments.
  """
  def remove_shape(pid, shape_handle) do
    GenServer.call(pid, {:remove_shape, shape_handle}, :infinity)
  end

  @doc """
  Register a shape this worker owns, for deferred class preparation. Sent on the same
  collector→worker channel as scattered events, so a shape is always registered before any
  fragment that routes to it arrives.
  """
  def add_shape(pid, shape_handle, shape) do
    GenServer.cast(pid, {:add_shape, shape_handle, shape})
  end

  @doc """
  Returns once the worker has handled every message the caller sent it before. A shape another
  process added for the caller, as the collector's restore does, is only sure to reach the worker
  before the caller's next message once that process has synced.
  """
  def sync(pid) do
    GenServer.call(pid, :sync, :infinity)
  end

  @doc """
  Exclude shapes from framed-block membership on this worker's assembler. Rides the same
  FIFO channel as scatters, so the mark always lands before any later fragment.
  """
  def mark_unblockable(pid, shape_handles) do
    GenServer.cast(pid, {:mark_unblockable, shape_handles})
  end

  @doc """
  Asynchronously assembles and publishes an event on the worker.

  The event is handed over as `{table, key}` in a public ETS table rather than in the
  message: inserting once costs the collector one copy instead of K, and the per-worker copy
  happens on lookup, in parallel (prepared log items are refc binaries, shared, not copied).

  Returns a ref; the worker sends `{ref, {affected_shapes, undeliverable, delivered_pids}}`
  to the caller once every consumer in its slice has acknowledged. Scatter to all workers
  first, then gather, so the slices run concurrently.
  """
  def scatter(pid, {_table, _key} = event_ref, context) do
    ref = make_ref()
    send(pid, {:route_and_publish, {self(), ref}, event_ref, context})
    ref
  end

  @doc """
  Collect every worker's ack for one scatter, or crash if the scatter never completes.

  A worker that dies takes the collector with it through their link, but one that stays
  alive and never acks is invisible: this wait is what the whole replication pipeline is
  backpressured behind. The deadline is absolute across all the refs, not per-ref, so K
  workers acking one timeout apart cannot stretch it to K × timeout.
  """
  def gather(refs, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    refs
    |> Enum.reduce({MapSet.new(), %{}, %{}}, fn ref, {affected, undeliverable, delivered} ->
      receive do
        {^ref, {worker_affected, worker_undeliverable, worker_delivered}} ->
          {MapSet.union(affected, worker_affected),
           Map.merge(undeliverable, worker_undeliverable), Map.merge(delivered, worker_delivered)}
      after
        max(deadline - System.monotonic_time(:millisecond), 0) ->
          raise "Scatter gather stalled: a router worker returned no ack for #{timeout}ms. " <>
                  "Crashing to re-stream from the slot rather than wedging the pipeline silently."
      end
    end)
  end

  def init({stack_id, index, shard_count, registry_state, opts}) do
    # Same per-process test mocks as the collector: consumer starts happen from this process
    # via ConsumerRegistry.publish, exactly as they do from the collector when unsharded.
    Electric.Replication.ShapeLogCollector.activate_mocked_functions_from_test_process()

    Process.set_label({:router_worker, stack_id, index})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    payload_arena =
      case Keyword.get(opts, :payload_arena_base_path) do
        nil -> nil
        base_path -> PayloadArena.open_writer!(base_path, index + 1)
      end

    {:ok,
     %{
       stack_id: stack_id,
       index: index,
       shard_count: shard_count,
       assembler: FragmentAssembler.new(blocks?: Keyword.get(opts, :fragment_blocks, false)),
       registry_state: registry_state,
       shapes: %{},
       payload_arena: payload_arena
     }}
  end

  def handle_call(:sync, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:remove_shape, shape_handle}, _from, state) do
    {:reply, :ok,
     %{
       state
       | assembler: FragmentAssembler.remove_shape(state.assembler, shape_handle),
         shapes: Map.delete(state.shapes, shape_handle)
     }}
  end

  def handle_cast({:add_shape, shape_handle, shape}, state) do
    {:noreply, %{state | shapes: Map.put(state.shapes, shape_handle, shape)}}
  end

  def handle_cast({:mark_unblockable, shape_handles}, state) do
    {:noreply,
     %{state | assembler: FragmentAssembler.mark_unblockable(state.assembler, shape_handles)}}
  end

  def handle_info({:route_and_publish, {caller, ref}, {table, key}, context}, state) do
    OpenTelemetry.set_current_context(context)

    [{^key, payload}] = :ets.lookup(table, key)

    {events_by_handle, state} =
      case payload do
        {:fragment, fragment, decisions} ->
          {sliced, state} = slice_decisions(decisions, fragment, state)
          state = maybe_sync_arena(state, fragment)

          {fragments, assembler} = FragmentAssembler.assemble(state.assembler, fragment, sliced)
          {fragments, %{state | assembler: assembler}}

        {:relation, relation, shape_ids} ->
          {shape_ids |> Enum.filter(&mine?(&1, state)) |> Map.new(&{&1, relation}), state}
      end

    events =
      Map.new(events_by_handle, fn {handle, shape_event} ->
        {handle, {:handle_event, shape_event, context}}
      end)

    {undeliverable, delivered} = ConsumerRegistry.publish(events, state.registry_state)

    affected = events_by_handle |> Map.keys() |> MapSet.new()
    send(caller, {ref, {affected, undeliverable, delivered}})

    {:noreply, state}
  end

  # The worker runs ConsumerRegistry.publish, which gen_calls consumers and monitors them from
  # this process, so unsolicited replies can land here. Without this clause they are a
  # FunctionClauseError, and the worker is start_link'ed from the collector whose supervisor is
  # max_restarts: 0 -- one stray message would stop the whole shape subsystem. The collector
  # keeps the same clause for the same reason.
  def handle_info(msg, state) do
    Logger.warning("#{inspect(__MODULE__)} received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp slice_decisions(decisions, transaction, state) do
    {sliced, state} =
      Enum.map_reduce(decisions, state, fn {change, prepared_groups, unprepared_ids}, state ->
        {groups, extra_unprepared, state} =
          Enum.reduce(prepared_groups, {[], [], state}, fn group, acc ->
            slice_group(group, change, transaction, acc)
          end)

        unprepared = Enum.filter(unprepared_ids, &mine?(&1, state)) ++ extra_unprepared

        {{change, groups, unprepared}, state}
      end)

    {Enum.reject(sliced, fn {_change, groups, unprepared_ids} ->
       groups == [] and unprepared_ids == []
     end), state}
  end

  # Deferred class conversion: prepare from any member of this worker's slice — every shape
  # in a class produces the identical result, which is what makes the class a class. A
  # failed preparation falls back to per-shape conversion in the consumers, exactly like the
  # collector's inline fallback.
  defp slice_group({:prepare, shape_ids}, change, transaction, {groups, unprepared, state}) do
    case Enum.filter(shape_ids, &mine?(&1, state)) do
      [] ->
        {groups, unprepared, state}

      [representative | _] = mine ->
        # A shape this worker has no struct for is a failed preparation like any other, and the
        # fallback below is correctness-preserving: the consumer converts the change itself and
        # produces the identical result. Fetching it with `!` instead made the mismatch fatal, and
        # fatal here is not local -- the workers are `start_link`ed from the collector, whose own
        # supervisor is `max_restarts: 0`, so one missing key stops the entire shape subsystem.
        #
        # Nothing produces that mismatch today: only shapes with no dependencies are registered
        # here, and only shapes with no dependencies and no tag structure get a fan-out class at
        # all, so class membership implies registration. But the two conditions are written
        # independently, in two modules, with nothing tying them together.
        with {:ok, shape} <- Map.fetch(state.shapes, representative),
             {prepared_change, payload_arena} <-
               EventRouter.prepare_change_for_shape(
                 shape,
                 change,
                 transaction,
                 state.payload_arena
               ) do
          {[{prepared_change, mine} | groups], unprepared,
           %{state | payload_arena: payload_arena}}
        else
          _ -> {groups, mine ++ unprepared, state}
        end
    end
  end

  defp slice_group(
         {prepared_change, shape_ids},
         _change,
         _transaction,
         {groups, unprepared, state}
       ) do
    case Enum.filter(shape_ids, &mine?(&1, state)) do
      [] -> {groups, unprepared, state}
      mine -> {[{prepared_change, mine} | groups], unprepared, state}
    end
  end

  # Any ref this worker's consumers can ever flush must be backed by durable arena bytes, so
  # the worker's arena generation is datasynced at every commit fragment BEFORE publishing.
  # Mid-transaction refs are covered by recovery exactly as on the collector's inline path.
  defp maybe_sync_arena(%{payload_arena: nil} = state, _fragment), do: state

  defp maybe_sync_arena(state, fragment) do
    case fragment do
      %{commit: nil} -> state
      _ -> %{state | payload_arena: PayloadArena.sync!(state.payload_arena)}
    end
  end

  defp mine?(shape_handle, %{index: index, shard_count: shard_count}) do
    :erlang.phash2(shape_handle, shard_count) == index
  end
end
