if Electric.telemetry_enabled?() and Code.ensure_loaded?(ElectricTelemetry.Reporters.Prometheus) do
  defmodule Electric.StackSupervisor.TelemetryTest do
    # Not async: the Prometheus reporter attaches a global :telemetry handler for
    # [:electric, :plug, :serve_shape], so concurrent async tests that emit that
    # event (via the real serve_shape plug) leak into this reporter's per-status
    # counters and make the exact-count assertions below non-deterministic.
    use ExUnit.Case, async: false

    alias Electric.StackSupervisor.Telemetry

    describe "prometheus_metrics/0" do
      test "the curated stack metrics are scrapable via the Prometheus reporter" do
        name = :"test_prometheus_#{System.unique_integer([:positive])}"

        # Build the core through the real reporter so distribution buckets are applied, exactly
        # as ApplicationTelemetry does for the /metrics endpoint.
        {mod, opts} =
          ElectricTelemetry.Reporters.Prometheus.child_spec(
            %{reporters: %{prometheus?: true}},
            name: name,
            metrics: Telemetry.prometheus_metrics()
          )

        start_supervised!({mod, opts})

        # The core attaches its telemetry handlers asynchronously (via a `:setup` message).
        # A scrape is a synchronous call to the registry, so it drains that message and
        # guarantees the handlers are attached before we emit events below.
        _ = TelemetryMetricsPrometheus.Core.scrape(name)

        stack_id = "test-stack"

        :telemetry.execute(
          [:electric, :shapes, :total_shapes],
          %{count: 7, count_indexed: 4, count_unindexed: 3},
          %{stack_id: stack_id}
        )

        :telemetry.execute(
          [:electric, :shapes, :active_shapes],
          %{count: 3},
          %{stack_id: stack_id}
        )

        :telemetry.execute(
          [:electric, :postgres, :replication],
          %{pg_wal_offset: 1, slot_retained_wal_size: 1234, slot_confirmed_flush_lsn_lag: 56},
          %{stack_id: stack_id}
        )

        :telemetry.execute(
          [:electric, :postgres, :replication, :transaction_received],
          %{receive_lag: 12, bytes: 0, count: 1, operations: 1},
          %{stack_id: stack_id}
        )

        scrape = TelemetryMetricsPrometheus.Core.scrape(name)

        # defined shapes
        assert scrape =~ "electric_shapes_total_shapes_count 7"
        # active shapes
        assert scrape =~ "electric_shapes_active_shapes_count 3"
        # retained WAL size
        assert scrape =~ "electric_postgres_replication_slot_retained_wal_size 1234"
        # byte-based replication lag
        assert scrape =~ "electric_postgres_replication_slot_confirmed_flush_lsn_lag 56"
        # time-based replication lag (histogram)
        assert scrape =~ "electric_postgres_replication_transaction_received_receive_lag"
      end

      test "millisecond-lag and fan-out histograms use ranges that capture real observations" do
        name = :"test_prometheus_buckets_#{System.unique_integer([:positive])}"

        {mod, opts} =
          ElectricTelemetry.Reporters.Prometheus.child_spec(
            %{reporters: %{prometheus?: true}},
            name: name,
            metrics: Telemetry.prometheus_metrics()
          )

        start_supervised!({mod, opts})
        _ = TelemetryMetricsPrometheus.Core.scrape(name)

        stack_id = "test-stack"

        # A 250 ms lag and a 111-shape fan-out both overflow the reporter's default 0.01..1 second
        # bucket ladder into +Inf, which is the bug: no percentile or le-based query can work.
        :telemetry.execute(
          [:electric, :postgres, :replication, :transaction_received],
          %{receive_lag: 250, bytes: 0, count: 1, operations: 1},
          %{stack_id: stack_id}
        )

        :telemetry.execute(
          [:electric, :storage, :transaction_stored],
          %{replication_lag: 250},
          %{stack_id: stack_id}
        )

        :telemetry.execute(
          [:electric, :shape_log_collector, :transaction],
          %{affected_shape_count: 111},
          %{stack_id: stack_id}
        )

        scrape = TelemetryMetricsPrometheus.Core.scrape(name)

        # Bucket boundaries that exist only under the millisecond / fan-out ladders, never the
        # default seconds ladder whose largest finite bucket is le="1.0".
        assert scrape =~ ~r/receive_lag_bucket\{le="300000(\.0)?"\}/
        assert scrape =~ ~r/transaction_stored_replication_lag_bucket\{le="300000(\.0)?"\}/
        assert scrape =~ ~r/affected_shape_count_bucket\{le="5000(\.0)?"\}/

        # Each observation is captured below +Inf: the 250 ms lags land at or under the le="250"
        # boundary and the 111 fan-out under le="250", where the default ladder counted zero.
        assert scrape =~ ~r/receive_lag_bucket\{le="250(\.0)?"\} 1/
        assert scrape =~ ~r/transaction_stored_replication_lag_bucket\{le="250(\.0)?"\} 1/
        assert scrape =~ ~r/affected_shape_count_bucket\{le="250(\.0)?"\} 1/
      end

      test "per-status HTTP request counts are scrapable" do
        name = :"test_prometheus_status_#{System.unique_integer([:positive])}"

        {mod, opts} =
          ElectricTelemetry.Reporters.Prometheus.child_spec(
            %{reporters: %{prometheus?: true}},
            name: name,
            metrics: Telemetry.prometheus_metrics()
          )

        start_supervised!({mod, opts})
        _ = TelemetryMetricsPrometheus.Core.scrape(name)

        for status <- [200, 200, 409] do
          :telemetry.execute(
            [:electric, :plug, :serve_shape],
            %{count: 1},
            %{status: status, known_error: false, live: false, stack_id: "test-stack"}
          )
        end

        scrape = TelemetryMetricsPrometheus.Core.scrape(name)

        assert scrape =~ ~r/electric_plug_serve_shape_requests_count\{status="200"\} 2/
        assert scrape =~ ~r/electric_plug_serve_shape_requests_count\{status="409"\} 1/
      end

      test "admission control metrics are scrapable" do
        name = :"test_prometheus_admission_#{System.unique_integer([:positive])}"

        {mod, opts} =
          ElectricTelemetry.Reporters.Prometheus.child_spec(
            %{reporters: %{prometheus?: true}},
            name: name,
            metrics: Telemetry.prometheus_metrics()
          )

        start_supervised!({mod, opts})
        _ = TelemetryMetricsPrometheus.Core.scrape(name)

        :telemetry.execute(
          [:electric, :admission_control, :acquire],
          %{count: 1, current: 5, limit: 10},
          %{stack_id: "test-stack", kind: :shape}
        )

        for _ <- 1..2 do
          :telemetry.execute(
            [:electric, :admission_control, :reject],
            %{count: 1, limit: 10},
            %{stack_id: "test-stack", reason: :overloaded, kind: :shape, current: 11}
          )
        end

        scrape = TelemetryMetricsPrometheus.Core.scrape(name)

        # current concurrency gauge
        assert scrape =~ ~r/electric_admission_control_acquire_current\{kind="shape"\} 5/
        # rejection count
        assert scrape =~ ~r/electric_admission_control_reject_count\{kind="shape"\} 2/
      end
    end
  end
end
