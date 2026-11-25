# The W3C trace context propagator is configured by the :opentelemetry SDK application,
# which is only a dependency when building for the telemetry target (MIX_TARGET=application,
# the configuration CI runs the test suite with).
if Electric.telemetry_enabled?() do
  defmodule Electric.Plug.TraceContextPlugTest do
    use ExUnit.Case, async: false

    alias Electric.Plug.TraceContextPlug

    @trace_id_hex "0af7651916cd43dd8448eb211c80319c"
    @parent_span_id_hex "b7ad6b7169203331"

    defp call(headers) do
      headers
      |> Enum.reduce(Plug.Test.conn(:get, "/v1/shape"), fn {key, value}, conn ->
        Plug.Conn.put_req_header(conn, key, value)
      end)
      |> TraceContextPlug.call([])
    end

    defp traceparent(flags), do: "00-#{@trace_id_hex}-#{@parent_span_id_hex}-#{flags}"

    test "no traceparent: conn passes through with no trace context" do
      conn = call([])
      assert TraceContextPlug.trace_context(conn) == nil
    end

    test "tracestate without traceparent is ignored" do
      conn = call([{"tracestate", "electric=rate:20"}])
      assert TraceContextPlug.trace_context(conn) == nil
    end

    test "sampled traceparent with rate hint" do
      conn = call([{"traceparent", traceparent("01")}, {"tracestate", "electric=rate:20"}])

      assert %{sample_rate_hint: 20} = TraceContextPlug.trace_context(conn)

      # The extracted remote parent is installed as the current span for this process.
      assert :otel_tracer.current_span_ctx() != :undefined
    end

    test "unsampled traceparent still parses the rate hint" do
      conn = call([{"traceparent", traceparent("00")}, {"tracestate", "electric=rate:20"}])

      assert %{sample_rate_hint: 20} = TraceContextPlug.trace_context(conn)
    end

    test "traceparent without tracestate: no rate hint" do
      conn = call([{"traceparent", traceparent("01")}])

      assert %{sample_rate_hint: nil} = TraceContextPlug.trace_context(conn)
    end

    test "electric member is found among other tracestate members" do
      conn =
        call([
          {"traceparent", traceparent("01")},
          {"tracestate", "congo=t61rcWkgMzE,electric=rate:42,rojo=00f067aa0ba902b7"}
        ])

      assert %{sample_rate_hint: 42} = TraceContextPlug.trace_context(conn)
    end

    test "whitespace around tracestate members is tolerated" do
      conn =
        call([
          {"traceparent", traceparent("01")},
          {"tracestate", "congo=t61rcWkgMzE , electric=rate:7"}
        ])

      assert %{sample_rate_hint: 7} = TraceContextPlug.trace_context(conn)
    end

    test "rate of exactly 1 is accepted" do
      conn = call([{"traceparent", traceparent("01")}, {"tracestate", "electric=rate:1"}])
      assert %{sample_rate_hint: 1} = TraceContextPlug.trace_context(conn)
    end

    test "the largest signed 64-bit rate is accepted" do
      conn =
        call([
          {"traceparent", traceparent("01")},
          {"tracestate", "electric=rate:9223372036854775807"}
        ])

      assert %{sample_rate_hint: 9_223_372_036_854_775_807} =
               TraceContextPlug.trace_context(conn)
    end

    test "applies the configured sample rates without dropping the remote parent" do
      key = :otel_trusted_sample_rates
      previous = Application.fetch_env(:electric, key)
      Application.put_env(:electric, key, MapSet.new([10, 100]))

      on_exit(fn ->
        case previous do
          {:ok, value} -> Application.put_env(:electric, key, value)
          :error -> Application.delete_env(:electric, key)
        end
      end)

      listed = call([{"traceparent", traceparent("01")}, {"tracestate", "electric=rate:100"}])
      assert TraceContextPlug.sample_rate_attrs(listed, 200) == %{"SampleRate" => 100}
      assert TraceContextPlug.sample_rate_attrs(listed, 503) == %{"SampleRate" => 1}

      unlisted = call([{"traceparent", traceparent("01")}, {"tracestate", "electric=rate:20"}])
      assert %{sample_rate_hint: nil} = TraceContextPlug.trace_context(unlisted)
      assert TraceContextPlug.sample_rate_attrs(unlisted, 200) == %{}
      assert TraceContextPlug.sample_rate_attrs(unlisted, 503) == %{}
      assert :otel_tracer.current_span_ctx() != :undefined
    end

    for tracestate <- [
          # rate < 1
          "electric=rate:0",
          "electric=rate:-5",
          # more than an OpenTelemetry integer attribute holds
          "electric=rate:9223372036854775808",
          "electric=rate:1000000000000000000000000000000",
          # non-integer / trailing garbage
          "electric=rate:1.5",
          "electric=rate:20x",
          "electric=rate:abc",
          "electric=rate:",
          # wrong member format
          "electric=ratio:20",
          "electric=20",
          # no electric member at all
          "congo=t61rcWkgMzE",
          "notelectric=rate:20"
        ] do
      test "invalid or missing hint in #{inspect(tracestate)} is ignored" do
        conn =
          call([{"traceparent", traceparent("01")}, {"tracestate", unquote(tracestate)}])

        assert %{sample_rate_hint: nil} = TraceContextPlug.trace_context(conn)
      end
    end

    describe "sample_rate_attrs/2" do
      test "uses the hinted rate for non-5xx statuses" do
        conn = call([{"traceparent", traceparent("01")}, {"tracestate", "electric=rate:20"}])

        assert TraceContextPlug.sample_rate_attrs(conn, 200) == %{"SampleRate" => 20}
        assert TraceContextPlug.sample_rate_attrs(conn, 304) == %{"SampleRate" => 20}
        assert TraceContextPlug.sample_rate_attrs(conn, 404) == %{"SampleRate" => 20}
        assert TraceContextPlug.sample_rate_attrs(conn, 499) == %{"SampleRate" => 20}
        # Status not yet known: default to the success-path rate.
        assert TraceContextPlug.sample_rate_attrs(conn, nil) == %{"SampleRate" => 20}
      end

      test "5xx statuses override the hint with rate 1" do
        conn = call([{"traceparent", traceparent("01")}, {"tracestate", "electric=rate:20"}])

        assert TraceContextPlug.sample_rate_attrs(conn, 500) == %{"SampleRate" => 1}
        assert TraceContextPlug.sample_rate_attrs(conn, 503) == %{"SampleRate" => 1}
      end

      test "returns no attributes without a rate hint" do
        conn = call([{"traceparent", traceparent("01")}])
        assert TraceContextPlug.sample_rate_attrs(conn, 200) == %{}
        assert TraceContextPlug.sample_rate_attrs(conn, 500) == %{}
      end

      test "returns no attributes without a remote parent" do
        conn = call([])
        assert TraceContextPlug.sample_rate_attrs(conn, 500) == %{}
      end
    end
  end
end
