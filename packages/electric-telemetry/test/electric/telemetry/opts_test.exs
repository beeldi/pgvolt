defmodule ElectricTelemetry.OptsTest do
  use ExUnit.Case, async: true

  @required_opts [instance_id: "test", version: "0.0.0"]

  defp validate(overrides) do
    opts = Keyword.merge(@required_opts, overrides)
    ElectricTelemetry.validate_options(opts)
  end

  describe "disk usage scan" do
    test "runs every minute by default" do
      assert {:ok, opts} = validate(stack_id: "stack", storage_dir: "/tmp/storage")
      assert opts.intervals_and_thresholds.disk_usage_scan_period == 60_000

      assert [{ElectricTelemetry.DiskUsage, child_opts}] =
               ElectricTelemetry.StackTelemetry.disk_usage_child_specs(opts)

      assert Keyword.fetch!(child_opts, :update_period) == 60_000
    end

    test "uses the configured period" do
      assert {:ok, opts} =
               validate(
                 stack_id: "stack",
                 storage_dir: "/tmp/storage",
                 intervals_and_thresholds: [disk_usage_scan_period: 600_000]
               )

      assert [{ElectricTelemetry.DiskUsage, child_opts}] =
               ElectricTelemetry.StackTelemetry.disk_usage_child_specs(opts)

      assert Keyword.fetch!(child_opts, :update_period) == 600_000
    end

    test "is off with a period of 0" do
      assert {:ok, opts} =
               validate(
                 stack_id: "stack",
                 storage_dir: "/tmp/storage",
                 intervals_and_thresholds: [disk_usage_scan_period: 0]
               )

      assert [] = ElectricTelemetry.StackTelemetry.disk_usage_child_specs(opts)
    end
  end

  describe "top_process_limit validation" do
    test "accepts {:count, N}" do
      assert {:ok, %{intervals_and_thresholds: %{top_process_limit: {:count, 10}}}} =
               validate(intervals_and_thresholds: [top_process_limit: {:count, 10}])
    end

    test "accepts {:mem_percent, N} for values 1..100" do
      assert {:ok, %{intervals_and_thresholds: %{top_process_limit: {:mem_percent, 1}}}} =
               validate(intervals_and_thresholds: [top_process_limit: {:mem_percent, 1}])

      assert {:ok, %{intervals_and_thresholds: %{top_process_limit: {:mem_percent, 100}}}} =
               validate(intervals_and_thresholds: [top_process_limit: {:mem_percent, 100}])
    end

    test "defaults to {:count, 5}" do
      assert {:ok, %{intervals_and_thresholds: %{top_process_limit: {:count, 5}}}} = validate([])
    end

    test "rejects {:mem_percent, 0}" do
      assert {:error, %NimbleOptions.ValidationError{}} =
               validate(intervals_and_thresholds: [top_process_limit: {:mem_percent, 0}])
    end

    test "rejects {:mem_percent, 101}" do
      assert {:error, %NimbleOptions.ValidationError{}} =
               validate(intervals_and_thresholds: [top_process_limit: {:mem_percent, 101}])
    end

    test "rejects {:count, 0}" do
      assert {:error, %NimbleOptions.ValidationError{}} =
               validate(intervals_and_thresholds: [top_process_limit: {:count, 0}])
    end

    test "rejects plain integer" do
      assert {:error, %NimbleOptions.ValidationError{}} =
               validate(intervals_and_thresholds: [top_process_limit: 5])
    end
  end
end
