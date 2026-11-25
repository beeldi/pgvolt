defmodule Electric.TelemetryExportCoverageTest do
  @moduledoc """
  An event is only observable where a reporter carries it. With `ELECTRIC_PROMETHEUS_PORT` set and
  neither `ELECTRIC_STATSD_HOST` nor an OTel endpoint, `prometheus_metrics/0` is the whole surface
  visible. Anything defined only in `StackTelemetry.metrics/1` is emitted, exported, and collected
  by nobody there.

  Two guards: the signals needed for diagnosis must be on the Prometheus
  list, and no newly emitted event may be absent from every reporter.
  """
  use ExUnit.Case, async: true

  alias Electric.StackSupervisor.Telemetry

  @must_reach_prometheus [
    [:electric, :flush_tracker, :stall_detected],
    [:electric, :flush_tracker, :writer_down],
    [:electric, :storage, :used],
    [:electric, :shape, :unevaluable_row_excluded],
    [:electric, :shape_log_collector, :transaction],
    [:electric, :postgres, :full_transaction_id, :anchor],
    # A high backlog can only be read from this one.
    [:electric, :shape_db, :write_buffer, :pending_writes]
  ]

  # Emitted with a non-ASCII metric name (`queue_time_μs`), which Prometheus cannot express; it
  # needs renaming upstream before it can be exported, and a bad name risks the whole endpoint.
  @not_on_prometheus_yet [[:electric, :shape_db, :pool, :checkout]]

  test "the signals needed for diagnosis are on the Prometheus endpoint" do
    # Arrange
    exported = MapSet.new(Telemetry.prometheus_metrics(), & &1.event_name)

    # Assert
    for event <- @must_reach_prometheus do
      assert MapSet.member?(exported, event),
             "#{inspect(event)} is emitted but not on the Prometheus list, and with only the Prometheus " <>
               "reporter it would report into nothing"
    end
  end

  test "no emitted event is absent from every reporter" do
    # Arrange
    exported =
      MapSet.new(
        Enum.map(Telemetry.prometheus_metrics(), & &1.event_name) ++ stack_telemetry_event_names()
      )

    # Act
    unexported =
      emitted_events()
      |> Enum.reject(&MapSet.member?(exported, &1))
      |> Enum.reject(&(&1 in @not_on_prometheus_yet))
      |> Enum.sort()

    # Assert
    assert unexported == [],
           "emitted and carried by no reporter, so it cannot be observed anywhere: " <>
             inspect(unexported)
  end

  # Read from source rather than calling the module: `ElectricTelemetry` is a sibling package that
  # is not loaded in this app's test environment, and the question here is what the checked-in
  # definitions cover, which the source answers directly.
  @stack_telemetry "../electric-telemetry/lib/electric/telemetry/stack_telemetry.ex"

  defp stack_telemetry_event_names do
    Regex.scan(
      ~r/(?:distribution|counter|sum|last_value)\(\s*"([^"]+)"([^)]*)\)/,
      File.read!(@stack_telemetry)
    )
    |> Enum.map(fn [_, name, rest] ->
      case Regex.run(~r/event_name:\s*\[([^\]]*)\]/, rest) do
        [_, inner] -> parse_event(inner)
        # `Telemetry.Metrics` derives the event from the metric name: every segment but the last.
        nil -> name |> String.split(".") |> Enum.drop(-1) |> Enum.map(&String.to_atom/1)
      end
    end)
  end

  defp parse_event(inner) do
    inner
    |> String.split(",")
    |> Enum.map(&(&1 |> String.trim() |> String.trim_leading(":") |> String.to_atom()))
  end

  defp emitted_events do
    Path.wildcard("lib/electric/**/*.ex")
    |> Enum.flat_map(fn path ->
      Regex.scan(
        ~r/(?::telemetry\.execute|OpenTelemetry\.execute)\(\s*\[([^\]]*)\]/,
        File.read!(path)
      )
      |> Enum.map(fn [_, inner] -> parse_event(inner) end)
    end)
    |> Enum.uniq()
  end
end
