defmodule Electric.Telemetry.OpenTelemetryTest do
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.Telemetry.Sampler

  use ExUnit.Case, async: true
  use Repatch.ExUnit

  @stack_id "the_stack_id"

  defp report_span_creation do
    test_pid = self()

    Repatch.patch(:otel_tracer, :with_span, fn tracer, name, opts, fun ->
      send(test_pid, :span_created)
      Repatch.real(:otel_tracer, :with_span, [tracer, name, opts, fun])
    end)
  end

  test "baggage is propagated to new processes when the context is carried over" do
    OpenTelemetry.with_span("test", %{}, @stack_id, fn ->
      OpenTelemetry.set_in_baggage("some_key", "some_value")

      context = OpenTelemetry.get_current_context()

      Task.async(fn ->
        OpenTelemetry.set_current_context(context)

        assert OpenTelemetry.get_from_baggage("some_key") == "some_value"
      end)
      |> Task.await()
    end)
  end

  describe "with_span/4" do
    test "creates a OTEL span" do
      report_span_creation()

      OpenTelemetry.with_span("test_span", %{}, @stack_id, fn ->
        :some_code
      end)

      assert_received :span_created
    end

    test "does not create an OTEL span if the samler does not include it" do
      report_span_creation()
      Repatch.patch(Sampler, :include_span?, fn _ -> false end)

      OpenTelemetry.with_span("test_span", %{}, @stack_id, fn ->
        :some_code
      end)

      refute_received :span_created
    end

    test "calls :telemetry.span/3 even if the samler does not include it" do
      pid = self()

      :telemetry.attach(
        pid,
        [:electric, :test_span, :start],
        fn _, _, _, _ -> send(pid, :span_started) end,
        nil
      )

      Repatch.patch(Sampler, :include_span?, fn _ -> false end)

      OpenTelemetry.with_span("test_span", %{}, @stack_id, fn ->
        :some_code
      end)

      assert_receive :span_started
    end
  end

  describe "with_child_span/4" do
    test "creates a span if there is a parent span" do
      OpenTelemetry.with_span("parent_span", %{}, @stack_id, fn ->
        report_span_creation()

        OpenTelemetry.with_child_span("child_span", %{}, @stack_id, fn ->
          :some_code
        end)

        assert_received :span_created
      end)
    end

    test "does not create a span if there is not a parent span" do
      report_span_creation()

      OpenTelemetry.with_child_span("child_span", %{}, @stack_id, fn ->
        :some_code
      end)

      refute_received :span_created
    end

    test "calls :telemetry.span/3 even if there is not a parent span" do
      pid = self()

      :telemetry.attach(
        pid,
        [:electric, :child_span, :start],
        fn _, _, _, _ -> send(pid, :span_started) end,
        nil
      )

      OpenTelemetry.with_child_span("child_span", %{}, @stack_id, fn ->
        :some_code
      end)

      assert_receive :span_started
    end

    test "does not create a span if the sampler does not include the child span" do
      OpenTelemetry.with_span("parent_span", %{}, @stack_id, fn ->
        report_span_creation()

        Repatch.patch(Sampler, :include_span?, fn _ -> false end)

        OpenTelemetry.with_child_span("child_span", %{}, @stack_id, fn ->
          :some_code
        end)

        refute_received :span_created
      end)
    end
  end

  describe "with_flattened_span/2" do
    test "records duration and prefixed memory attributes on the current span" do
      test_pid = self()

      Repatch.patch(:otel_span, :set_attributes, fn ctx, attrs ->
        send(test_pid, {:set_attributes, attrs})
        Repatch.real(:otel_span, :set_attributes, [ctx, attrs])
      end)

      result =
        OpenTelemetry.with_span("test_span", %{}, @stack_id, fn ->
          OpenTelemetry.with_flattened_span("inner_op", fn -> :some_result end)
        end)

      assert result == :some_result

      assert_received {:set_attributes,
                       %{
                         "inner_op.duration_ms" => duration_ms,
                         "inner_op.memory.start.process_bytes" => start_process_bytes,
                         "inner_op.memory.start.binary_bytes" => _,
                         "inner_op.memory.end.process_bytes" => _,
                         "inner_op.memory.end.binary_bytes" => _
                       }}

      assert is_float(duration_ms) and duration_ms >= 0
      assert is_integer(start_process_bytes) and start_process_bytes > 0
    end

    test "still records attributes when the function raises" do
      test_pid = self()

      Repatch.patch(:otel_span, :set_attributes, fn ctx, attrs ->
        send(test_pid, {:set_attributes, attrs})
        Repatch.real(:otel_span, :set_attributes, [ctx, attrs])
      end)

      assert_raise RuntimeError, "boom", fn ->
        OpenTelemetry.with_span("test_span", %{}, @stack_id, fn ->
          OpenTelemetry.with_flattened_span("inner_op", fn -> raise "boom" end)
        end)
      end

      assert_received {:set_attributes, %{"inner_op.duration_ms" => _}}
    end

    test "only calls the function when outside any span context" do
      test_pid = self()

      Repatch.patch(:otel_span, :set_attributes, fn ctx, attrs ->
        send(test_pid, {:set_attributes, attrs})
        Repatch.real(:otel_span, :set_attributes, [ctx, attrs])
      end)

      assert OpenTelemetry.with_flattened_span("inner_op", fn -> :some_result end) ==
               :some_result

      refute_received {:set_attributes, _}
    end

    test "only calls the function under a span that is not recording" do
      test_pid = self()

      Repatch.patch(:otel_span, :set_attributes, fn ctx, attrs ->
        send(test_pid, {:set_attributes, attrs})
        Repatch.real(:otel_span, :set_attributes, [ctx, attrs])
      end)

      # What a sampler that drops the trace leaves current: a span context that records nothing.
      :otel_tracer.set_current_span(:otel_tracer.from_remote_span(1, 1, 0))

      assert OpenTelemetry.with_flattened_span("inner_op", fn -> :some_result end) ==
               :some_result

      refute_received {:set_attributes, _}
    end
  end

  describe "process_memory_attributes/1" do
    test "returns process and binary memory for :start phase" do
      attrs = OpenTelemetry.process_memory_attributes(:start)

      assert %{
               "memory.start.process_bytes" => process_bytes,
               "memory.start.binary_bytes" => binary_bytes
             } = attrs

      assert is_integer(process_bytes) and process_bytes > 0
      assert is_integer(binary_bytes) and binary_bytes >= 0
    end

    test "returns process and binary memory for :end phase" do
      attrs = OpenTelemetry.process_memory_attributes(:end)

      assert %{
               "memory.end.process_bytes" => process_bytes,
               "memory.end.binary_bytes" => binary_bytes
             } = attrs

      assert is_integer(process_bytes) and process_bytes > 0
      assert is_integer(binary_bytes) and binary_bytes >= 0
    end

    test "rejects phase values other than :start or :end" do
      # apply/3 hides the intentionally invalid arguments from the type checker.
      assert_raise FunctionClauseError, fn ->
        apply(OpenTelemetry, :process_memory_attributes, [nil])
      end

      assert_raise FunctionClauseError, fn ->
        apply(OpenTelemetry, :process_memory_attributes, ["start"])
      end
    end
  end

  describe "add_process_memory_attributes/1" do
    test "forwards memory attributes through set_attributes on the current span" do
      test_pid = self()

      Repatch.patch(:otel_span, :set_attributes, fn ctx, attrs ->
        send(test_pid, {:set_attributes, attrs})
        Repatch.real(:otel_span, :set_attributes, [ctx, attrs])
      end)

      OpenTelemetry.with_span("test_span", %{}, @stack_id, fn ->
        OpenTelemetry.add_process_memory_attributes(:start)
      end)

      assert_received {:set_attributes,
                       %{
                         "memory.start.process_bytes" => process_bytes,
                         "memory.start.binary_bytes" => binary_bytes
                       }}

      assert is_integer(process_bytes) and process_bytes > 0
      assert is_integer(binary_bytes) and binary_bytes >= 0
    end

    test "is a no-op outside any span context" do
      # No surrounding with_span — should short-circuit and skip Process.info
      assert OpenTelemetry.add_process_memory_attributes(:start) == false
    end

    test "is a no-op under a span that is not recording" do
      test_pid = self()

      Repatch.patch(:otel_span, :set_attributes, fn ctx, attrs ->
        send(test_pid, {:set_attributes, attrs})
        Repatch.real(:otel_span, :set_attributes, [ctx, attrs])
      end)

      # What a sampler that drops the trace leaves current: a span context that records nothing.
      :otel_tracer.set_current_span(:otel_tracer.from_remote_span(1, 1, 0))

      assert OpenTelemetry.add_process_memory_attributes(:start) == false
      refute_received {:set_attributes, _}
    end
  end
end
