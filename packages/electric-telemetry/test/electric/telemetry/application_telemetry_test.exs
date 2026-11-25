defmodule ElectricTelemetry.ApplicationTelemetryTest do
  use ExUnit.Case, async: true

  alias ElectricTelemetry.ApplicationTelemetry

  describe "additional_prometheus_metrics" do
    setup do
      {:ok, opts} =
        ElectricTelemetry.validate_options(
          instance_id: "test-instance",
          version: "1.0.0",
          reporters: [prometheus?: true, statsd_host: "localhost", otel_metrics?: true],
          additional_prometheus_metrics: [Telemetry.Metrics.last_value("test.custom.gauge")]
        )

      {:ok, {_flags, children}} = ApplicationTelemetry.init(opts)
      %{children: children}
    end

    test "are exported to the Prometheus reporter", %{children: children} do
      metrics = reporter_metrics(children, :prometheus_metrics)
      assert Enum.any?(metrics, &(&1.name == [:test, :custom, :gauge]))
    end

    test "are not exported to the OTel or StatsD reporters", %{children: children} do
      refute Enum.any?(
               reporter_metrics(children, OtelMetricExporter),
               &(&1.name == [:test, :custom, :gauge])
             )

      refute Enum.any?(
               reporter_metrics(children, TelemetryMetricsStatsd),
               &(&1.name == [:test, :custom, :gauge])
             )
    end
  end

  # `Supervisor.init/2` normalises child specs into maps, nesting the reporter's start args
  # (which carry `:metrics`) inside `:start`. Reporters are identified by their child spec id.
  defp reporter_metrics(children, id) do
    Enum.find_value(children, [], fn
      %{id: ^id, start: {_m, _f, [opts]}} when is_list(opts) -> Keyword.get(opts, :metrics, [])
      _ -> false
    end)
  end

  describe "cpu_utilization_metrics" do
    test "builds one gauge per logical processor when the count is known" do
      metrics = ApplicationTelemetry.cpu_utilization_metrics(4)

      assert length(metrics) == 4

      assert Enum.map(metrics, & &1.name) == [
               [:system, :cpu, :utilization, :core_0],
               [:system, :cpu, :utilization, :core_1],
               [:system, :cpu, :utilization, :core_2],
               [:system, :cpu, :utilization, :core_3]
             ]
    end

    test "yields no gauges when the VM cannot report the processor count" do
      assert ApplicationTelemetry.cpu_utilization_metrics(:unknown) == []
    end
  end

  describe "get_system_memory_usage" do
    test "returns calculated memory stats" do
      case :os.type() do
        {:unix, :darwin} ->
          assert %{
                   total_memory: _,
                   available_memory: _,
                   free_memory: _,
                   used_memory: _,
                   resident_memory: _
                 } = ApplicationTelemetry.get_system_memory_usage(%{})

        _ ->
          assert %{
                   total_memory: _,
                   available_memory: _,
                   buffered_memory: _,
                   cached_memory: _,
                   free_memory: _,
                   used_memory: _,
                   resident_memory: _,
                   total_swap: _,
                   free_swap: _,
                   used_swap: _
                 } = ApplicationTelemetry.get_system_memory_usage(%{})
      end
    end
  end
end
