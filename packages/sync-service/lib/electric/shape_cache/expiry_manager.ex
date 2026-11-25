defmodule Electric.ShapeCache.ExpiryManager do
  use GenServer

  alias Electric.ShapeCache.ShapeStatus
  alias Electric.StatusMonitor
  alias Electric.Telemetry.OpenTelemetry

  require Logger

  @schema NimbleOptions.new!(
            max_shapes: [type: {:or, [:non_neg_integer, nil]}, default: nil],
            # Removal is a serial fold that holds this process and the shape-db pool for the length of
            # the list, and a long list is enough to make checkouts start
            # failing. A store that has had a limit all along only ever exceeds it by a few shapes,
            # but the first run after a limit is applied to a store already grown past one asks for
            # the entire excess at once. Draining it over several periods costs time nobody is
            # waiting on. Between batches the manager does not wait out the period: while the store
            # is still over its limit and a batch made progress, it queues the next one behind
            # whatever is already in its mailbox.
            max_expired_per_run: [type: :pos_integer, default: 1_000],
            period: [type: :non_neg_integer, default: 60_000],
            stack_id: [type: :string, required: true]
          )

  def name(stack_ref) do
    Electric.ProcessRegistry.name(stack_ref, __MODULE__)
  end

  def start_link(opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @schema) do
      GenServer.start_link(__MODULE__, opts, name: name(opts))
    end
  end

  def init(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    Process.set_label({:shape_expiry_manager, stack_id})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    state =
      %{
        stack_id: stack_id,
        max_shapes: Keyword.fetch!(opts, :max_shapes),
        max_expired_per_run: Keyword.fetch!(opts, :max_expired_per_run),
        period: Keyword.fetch!(opts, :period),
        continuing?: false
      }

    case state.max_shapes do
      nil ->
        Logger.warning(
          "Shape expiry is disabled: no shape ceiling is configured, so the store grows without " <>
            "bound. Nothing reclaims a shape no client has asked for since, and the same unset " <>
            "ceiling sizes the consumer partitions, so a store that grows large is also the one " <>
            "sized as though it had not."
        )

      _ ->
        schedule_next_check(state)
    end

    {:ok, state}
  end

  defp schedule_next_check(state) do
    Process.send_after(self(), :maybe_expire_shapes, state.period)
  end

  def handle_info(:maybe_expire_shapes, state) do
    # Schedule before removing: removal stops each consumer serially and one slow batch can hold
    # this process for minutes. Arming the next check first keeps the cadence tied to the period
    # instead of the period plus however long the removal took.
    schedule_next_check(state)
    {:noreply, expire_and_continue(state)}
  end

  def handle_info(:continue_expiring_shapes, state) do
    {:noreply, expire_and_continue(%{state | continuing?: false})}
  end

  # One batch is capped, so a store that is far over its limit, or that grows faster than a batch
  # per period, would stay over it for as long as the manager idled out each period. While it is
  # still over after a batch that removed something, the next batch is queued straight away. A
  # batch that removed nothing waits for the period, so the manager cannot spin on candidates it
  # is unable to expire.
  defp expire_and_continue(state) do
    case maybe_expire_shapes(state) do
      :over_limit when not state.continuing? ->
        send(self(), :continue_expiring_shapes)
        %{state | continuing?: true}

      _ ->
        state
    end
  end

  defp maybe_expire_shapes(%{max_shapes: nil}), do: :ok

  defp maybe_expire_shapes(%{max_shapes: max_shapes} = state) do
    case StatusMonitor.status(state.stack_id) do
      %{shape: :up} ->
        shape_count = shape_count(state)

        if shape_count > max_shapes do
          removed = expire_shapes(shape_count, state)
          if removed > 0 and shape_count(state) > max_shapes, do: :over_limit, else: :ok
        end

      status ->
        # We do not expire shapes if the stack is not active since this may mean that
        # shapes have not fully restored yet and we don't want to expire while restoring
        # as this may cause race conditions.
        Logger.debug("Expiry check skipped due to inactive stack: #{inspect(status)}")
    end
  end

  defp expire_shapes(shape_count, state) do
    number_to_expire = min(shape_count - state.max_shapes, state.max_expired_per_run)
    {handles_to_expire, min_age} = least_recently_used(state, number_to_expire)

    Logger.info(
      "Expiring shapes as the number of shapes has exceeded the limit",
      number_to_expire: number_to_expire,
      max_shapes: state.max_shapes
    )

    OpenTelemetry.with_span(
      "expiry_manager.expire_shapes",
      [
        max_shapes: state.max_shapes,
        shape_count: shape_count,
        number_to_expire: number_to_expire,
        elapsed_minutes_since_use: min_age
      ],
      fn ->
        Electric.ShapeCache.ShapeCleaner.remove_shapes(state.stack_id, handles_to_expire)
      end
    )

    # Shapes created while the batch ran hold the count where it was, so progress is what this
    # batch took out, not how far the count moved.
    Enum.count(handles_to_expire, &(not ShapeStatus.has_shape_handle?(state.stack_id, &1)))
  end

  defp least_recently_used(%{stack_id: stack_id}, number_to_expire) do
    ShapeStatus.least_recently_used(stack_id, number_to_expire)
  end

  defp shape_count(%{stack_id: stack_id}) do
    ShapeStatus.count_shapes(stack_id)
  end
end
