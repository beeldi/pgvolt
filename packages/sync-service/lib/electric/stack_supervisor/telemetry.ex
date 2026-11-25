defmodule Electric.StackSupervisor.Telemetry do
  # Prometheus histogram buckets. The shared reporter defaults every non-byte distribution to a
  # 0.01..1 second ladder, so these millisecond-lag and fan-out-count series would land entirely
  # in +Inf and no percentile or le-based query could work on the Prometheus endpoint. Cover
  # the expected ranges instead: single-digit ms up to a 5-minute staleness, and fan-out from one
  # shape into the thousands.
  @lag_ms_buckets [
    1,
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    30_000,
    60_000,
    300_000
  ]
  @fanout_buckets [1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000]

  def configure(config) do
    # Set shared OpenTelemetry span attributes for the given stack. They are stored in
    # persistent_term so it doesn't matter which process this function is called from.
    telemetry_span_attrs = Access.get(config, :telemetry_span_attrs, %{})

    if is_map(telemetry_span_attrs) and map_size(telemetry_span_attrs) > 0 do
      Electric.Telemetry.OpenTelemetry.set_stack_span_attrs(
        config.stack_id,
        telemetry_span_attrs
      )
    end

    child_spec(config)
  end

  @doc false
  # used in tests
  def default_periodic_measurements(%{stack_id: stack_id} = config) do
    [
      {__MODULE__, :count_shapes, [stack_id]},
      {__MODULE__, :report_write_buffer_size, [stack_id]},
      {__MODULE__, :report_retained_wal_size, [stack_id, config.replication_opts[:slot_name]]},
      {__MODULE__, :report_disk_usage, [stack_id]},
      {__MODULE__, :report_shape_db_stats, [stack_id]}
    ]
  end

  @doc """
  The curated subset of stack-level metrics exported to the Prometheus `/metrics` endpoint.

  These are emitted by the periodic measurements above (and, for `receive_lag`, by the
  replication client) and are already exported to OTel/StatsD/Call-Home via
  `ElectricTelemetry.StackTelemetry`. They are passed to `ApplicationTelemetry` as
  `additional_prometheus_metrics` so they reach the single shared Prometheus aggregator
  without being double-reported through the other reporters.

  Note: these definitions carry no `stack_id` tag, so they assume a single stack per
  instance. With multiple stacks the series would collide in the shared aggregator.
  """
  def prometheus_metrics do
    [
      # number of defined shapes
      Telemetry.Metrics.last_value("electric.shapes.total_shapes.count"),
      # number of active shapes
      Telemetry.Metrics.last_value("electric.shapes.active_shapes.count"),
      # retained WAL size (bytes Postgres is keeping for Electric's replication slot)
      Telemetry.Metrics.last_value("electric.postgres.replication.slot_retained_wal_size",
        unit: :byte
      ),
      # replication lag, byte-based: how far behind Postgres' WAL Electric is
      Telemetry.Metrics.last_value("electric.postgres.replication.slot_confirmed_flush_lsn_lag",
        unit: :byte
      ),
      Telemetry.Metrics.counter("electric.postgres.full_transaction_id.anchor.count"),
      # replication lag, time-based: wall-clock staleness of received transactions
      Telemetry.Metrics.distribution(
        "electric.postgres.replication.transaction_received.receive_lag",
        unit: :millisecond,
        reporter_options: [buckets: @lag_ms_buckets]
      ),
      # HTTP response status-code counts for the shape endpoint (200s, 409s, 50xs, ...).
      # The event also carries :known_error and :live; we tag by :status only so the series
      # are a plain per-code count.
      Telemetry.Metrics.counter("electric.plug.serve_shape.requests.count",
        event_name: [:electric, :plug, :serve_shape],
        measurement: :count,
        tags: [:status]
      ),
      # admission control: current in-flight concurrency per kind
      Telemetry.Metrics.last_value("electric.admission_control.acquire.current", tags: [:kind]),
      # admission control: number of rejected requests per kind
      Telemetry.Metrics.sum("electric.admission_control.reject.count", tags: [:kind]),
      # shape-db write buffer depth. Reading a high backlog needs this series, and it is only
      # otherwise defined in `default_metrics_from_periodic_measurements/0`, which reaches only the
      # StatsD/OTel reporters. Listed on both channels deliberately: the duplicate
      # the moduledoc warns about is cheaper than a series the Prometheus endpoint cannot show.
      Telemetry.Metrics.last_value("electric.shape_db.write_buffer.pending_writes.count"),
      Telemetry.Metrics.last_value("electric.shape_db.sqlite.connections"),
      # admission control: permit swaps, and swaps refused because the target bucket was full.
      # The refusal is an overload signal in its own right.
      Telemetry.Metrics.sum("electric.admission_control.swap.count", tags: [:from, :to]),
      Telemetry.Metrics.sum("electric.admission_control.swap_rejected.count", tags: [:from, :to]),
      # admission control: permits a dead request process still held, given back when it died.
      # Each one was a slot lost to every other request until then.
      Telemetry.Metrics.sum("electric.admission_control.reclaim.initial"),
      Telemetry.Metrics.sum("electric.admission_control.reclaim.existing"),
      # shape responses killed because their client stopped reading. Untagged: the shape
      # handle is unbounded, and the warning log names it.
      Telemetry.Metrics.sum("electric.plug.serve_shape.reaped.count"),
      # the replication socket has been paused longer than the retry budget, i.e. an ack nobody is
      # going to send. This is the only detector for a slot held open by such a pause, and the
      # check that watches for it reports rather than acts, so the report has to arrive somewhere.
      Telemetry.Metrics.sum("electric.flush_tracker.stall_detected.count"),
      # a shape writer went down; `reason_class` separates a deliberate stop from a crash
      Telemetry.Metrics.counter("electric.flush_tracker.writer_down.count",
        tags: [:reason_class]
      ),
      # bytes on the shape storage volume. Nothing shrinks the volume and nothing caps WAL
      # retention, so this is the one series that shows the ratchet before the volume does.
      Telemetry.Metrics.last_value("electric.storage.used.bytes", unit: :byte),
      # how many shapes one transaction fanned out to -- the cost driver behind the routing work
      Telemetry.Metrics.distribution(
        "electric.shape_log_collector.transaction.affected_shape_count",
        reporter_options: [buckets: @fanout_buckets]
      ),
      # wall-clock staleness between a transaction committing and its rows reaching shape storage
      Telemetry.Metrics.distribution("electric.storage.transaction_stored.replication_lag",
        unit: :millisecond,
        reporter_options: [buckets: @lag_ms_buckets]
      ),
      # rows dropped from a shape because their predicate could not be evaluated. Any non-zero
      # value here is silent data loss: the snapshot holds the row and its live changes do not
      # arrive. Untagged deliberately -- the refs that failed are unbounded cardinality, and the
      # question this answers is "is it happening at all", which the warning log then localises.
      Telemetry.Metrics.sum("electric.shape.unevaluable_row_excluded.count",
        event_name: [:electric, :shape, :unevaluable_row_excluded],
        measurement: :count
      )
    ]
  end

  def count_shapes(stack_id, _telemetry_opts) do
    # Emit active_shapes first so that a failure in shape_counts (e.g. shape cache not yet
    # started during stack startup) doesn't drop this metric for the tick.
    Electric.Telemetry.OpenTelemetry.execute(
      [:electric, :shapes, :active_shapes],
      %{count: Electric.Shapes.ConsumerRegistry.active_consumer_count(stack_id)},
      %{stack_id: stack_id}
    )

    case Electric.ShapeCache.shape_counts(stack_id) do
      %{total: num_shapes, indexed: indexed_shapes, unindexed: unindexed_shapes} ->
        Electric.Telemetry.OpenTelemetry.execute(
          [:electric, :shapes, :total_shapes],
          %{count: num_shapes, count_indexed: indexed_shapes, count_unindexed: unindexed_shapes},
          %{stack_id: stack_id}
        )

      # Not counted yet: at boot, the collector's restore counts the stored shapes, and an
      # instance waiting for the lock has not restored them.
      :error ->
        :ok
    end
  end

  def report_write_buffer_size(stack_id, _telemetry_opts) do
    alias Electric.ShapeCache.ShapeStatus.ShapeDb

    pending_count = ShapeDb.pending_buffer_size(stack_id)

    Electric.Telemetry.OpenTelemetry.execute(
      [:electric, :shape_db, :write_buffer, :pending_writes],
      %{count: pending_count},
      %{stack_id: stack_id}
    )
  end

  @min_signed_int8 -2 ** 63
  @retained_wal_size_query """
  SELECT
    (pg_current_wal_lsn() - '0/0' + #{@min_signed_int8})::int8 AS pg_wal_offset,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::int8 AS retained_wal_size,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::int8 AS confirmed_flush_lsn_lag
  FROM
    pg_replication_slots
  WHERE
    slot_name = $1
  """

  @doc false
  @spec report_retained_wal_size(Electric.stack_id(), binary(), map()) :: :ok
  def report_retained_wal_size(stack_id, slot_name, _telemetry_opts) do
    %Postgrex.Result{rows: [[pg_wal_offset, retained_wal_size, confirmed_flush_lsn_lag]]} =
      Postgrex.query!(
        Electric.Connection.Manager.admin_pool(stack_id),
        @retained_wal_size_query,
        [slot_name],
        timeout: 3_000,
        deadline: 3_000
      )

    # The query above can return `-1` for `confirmed_flush_lsn_lag` which means that Electric
    # is caught up with Postgres' replication stream.
    # This is a confusing stat if we're measuring in bytes, so use 0 as the bottom limit.

    Electric.Telemetry.OpenTelemetry.execute(
      [:electric, :postgres, :replication],
      %{
        # The absolute value of pg_current_wal_lsn() doesn't convey any useful info but by
        # plotting its rate of change we can see how fast the WAL is growing.
        #
        # We shift the absolute value of pg_current_wal_lsn() by -2**63 in the query above
        # to make sure it fits inside the signed 64-bit integer type expected by the
        # OpenTelemetry Protocol,
        pg_wal_offset: pg_wal_offset,
        slot_retained_wal_size: retained_wal_size,
        slot_confirmed_flush_lsn_lag: max(0, confirmed_flush_lsn_lag)
      },
      %{stack_id: stack_id}
    )
  end

  if Code.ensure_loaded?(ElectricTelemetry.DiskUsage) do
    def report_disk_usage(stack_id, _telemetry_opts) do
      case ElectricTelemetry.DiskUsage.current(stack_id) do
        {:ok, usage_bytes, measurement_duration} ->
          Electric.Telemetry.OpenTelemetry.execute(
            [:electric, :storage, :used],
            %{bytes: usage_bytes, measurement_duration: measurement_duration},
            %{stack_id: stack_id}
          )

        :pending ->
          :ok
      end
    end
  else
    def report_disk_usage(_stack_id, _telemetry_opts) do
      :ok
    end
  end

  def report_shape_db_stats(stack_id, _telemetry_opts) do
    case Electric.ShapeCache.ShapeStatus.ShapeDb.statistics(stack_id) do
      {:ok, stats} ->
        Electric.Telemetry.OpenTelemetry.execute(
          [:electric, :shape_db, :sqlite],
          stats,
          %{stack_id: stack_id}
        )

      _ ->
        :ok
    end
  end

  def child_spec(%{stack_telemetry: stack_telemetry}), do: stack_telemetry

  if Code.ensure_loaded?(ElectricTelemetry.StackTelemetry) do
    def child_spec(config) when is_map(config) do
      otel_opts_base = Keyword.get(config.telemetry_opts, :otel_opts, [])
      existing_resource = Keyword.get(otel_opts_base, :resource, %{})

      otel_opts =
        Keyword.put(
          otel_opts_base,
          :resource,
          Map.put_new(existing_resource, :stack_id, config.stack_id)
        )

      telemetry_opts =
        config.telemetry_opts
        |> Keyword.put(:stack_id, config.stack_id)
        |> Keyword.put(:storage_dir, config.storage_dir)
        |> Keyword.put(:otel_opts, otel_opts)
        # Always enable default periodic measurements in addition to the user-provided ones
        |> Keyword.update(
          :periodic_measurements,
          default_periodic_measurements(config),
          &(default_periodic_measurements(config) ++ &1)
        )
        # Add metrics for the default periodic measurements regardless of whether the
        # measurements themselves are occuring.
        |> Keyword.update(
          :additional_metrics,
          default_metrics_from_periodic_measurements(),
          &(default_metrics_from_periodic_measurements() ++ &1)
        )

      {ElectricTelemetry.StackTelemetry, telemetry_opts}
    end

    defp default_metrics_from_periodic_measurements do
      [
        Telemetry.Metrics.last_value("electric.shapes.total_shapes.count"),
        Telemetry.Metrics.last_value("electric.shapes.total_shapes.count_indexed"),
        Telemetry.Metrics.last_value("electric.shapes.total_shapes.count_unindexed"),
        Telemetry.Metrics.last_value("electric.shapes.active_shapes.count"),
        Telemetry.Metrics.last_value("electric.shape_db.write_buffer.pending_writes.count"),
        Telemetry.Metrics.last_value("electric.postgres.replication.pg_wal_offset"),
        Telemetry.Metrics.last_value("electric.postgres.replication.slot_retained_wal_size",
          unit: :byte
        ),
        Telemetry.Metrics.last_value("electric.postgres.replication.slot_confirmed_flush_lsn_lag",
          unit: :byte
        ),
        Telemetry.Metrics.last_value("electric.shape_db.sqlite.connections")
      ]
    end
  else
    def child_spec(_), do: nil
  end
end
