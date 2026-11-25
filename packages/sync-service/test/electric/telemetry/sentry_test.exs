if Electric.telemetry_enabled?() and Code.ensure_loaded?(Sentry.LoggerHandler) do
  defmodule Electric.Telemetry.SentryTest do
    # async: false because :logger handler state is VM-global — unique ids per
    # test avoid name collisions but not the add/remove race across processes.
    use ExUnit.Case, async: false

    alias Electric.Telemetry.Sentry, as: ElectricSentry

    setup do
      id = :"sentry_test_#{System.unique_integer([:positive])}"
      on_exit(fn -> _ = :logger.remove_handler(id) end)
      {:ok, handler_id: id}
    end

    defp handler_config!(id) do
      {:ok, %{config: config}} = :logger.get_handler_config(id)
      config
    end

    @tag :request_value_redaction
    test "keeps request values out of the Sentry request context" do
      Application.put_env(:electric, :telemetry_redact_request_values?, true)
      on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)
      query = "where=query-marker&secret=query-credential"

      conn =
        Plug.Test.conn(:post, "/?" <> query, %{"subset" => %{"where" => "body-marker"}})
        |> Plug.Conn.put_req_header("referer", "http://www.example.com/?" <> query)
        |> Plug.Conn.put_req_header(
          "x-original-url",
          "http://www.example.com/?where=forwarded-marker"
        )

      Electric.Plug.Router.call(conn, Electric.Plug.Router.init(stack_id: "test-stack"))
      context = Sentry.Context.get_all().request

      assert %{
               url: "http://www.example.com/",
               query_string: "",
               env: %{"http.query_string.present" => true, "http.query_string.bytes" => bytes}
             } = context

      assert context.data == %{}
      assert bytes == byte_size(query)
      refute inspect(context) =~ "query-marker"
      refute inspect(context) =~ "body-marker"
      refute inspect(context) =~ "query-credential"
      refute inspect(context) =~ "forwarded-marker"
    end

    @tag :request_value_redaction
    test "removes request values from the Sentry exception payload" do
      Application.put_env(:electric, :telemetry_redact_request_values?, true)
      on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)

      exception = %Postgrex.Error{message: "database error quoting body-marker"}

      event = %Sentry.Event{
        event_id: "test",
        timestamp: 0,
        source: :plug,
        request: %Sentry.Interfaces.Request{
          method: "POST",
          url: "http://www.example.com/?where=query-marker",
          query_string: "where=query-marker",
          data: %{"where" => "body-marker"}
        },
        original_exception: exception,
        exception: [
          %Sentry.Interfaces.Exception{type: "Postgrex.Error", value: exception.message}
        ]
      }

      scrubbed = ElectricSentry.scrub_request_credentials(event)
      assert [%{type: "Postgrex.Error", value: "Postgrex.Error"}] = scrubbed.exception
      assert %{url: "http://www.example.com/", query_string: nil, data: nil} = scrubbed.request
      refute inspect(scrubbed) =~ "body-marker"
      refute inspect(scrubbed) =~ "query-marker"
    end

    @tag :request_value_redaction
    test "removes function arguments from Sentry request stack frames" do
      Application.put_env(:electric, :telemetry_redact_request_values?, true)
      on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)

      stacktrace = [{__MODULE__, :serve_request, ["argument-marker"], []}]

      for details <- [[exception: %RuntimeError{message: "test"}], [message: "test"]] do
        event =
          Sentry.Event.create_event(details ++ [event_source: :plug, stacktrace: stacktrace])

        scrubbed =
          event |> ElectricSentry.scrub_request_credentials() |> inspect(limit: :infinity)

        assert scrubbed =~ "serve_request/1"
        refute scrubbed =~ "argument-marker"
      end
    end

    describe "add_logger_handler/2" do
      test "installs Sentry.LoggerHandler with default config", %{handler_id: id} do
        assert :ok = ElectricSentry.add_logger_handler(id)

        {:ok, handler} = :logger.get_handler_config(id)
        assert handler.module == Sentry.LoggerHandler

        assert %{metadata: :all, capture_log_messages: true, level: :error} =
                 handler_config!(id)
      end

      test "merges caller-supplied options into the handler config",
           %{handler_id: id} do
        assert :ok =
                 ElectricSentry.add_logger_handler(id,
                   discard_threshold: 2000,
                   sync_threshold: nil
                 )

        assert %{
                 metadata: :all,
                 capture_log_messages: true,
                 level: :error,
                 discard_threshold: 2000,
                 sync_threshold: nil
               } = handler_config!(id)
      end

      test "caller-supplied options override defaults", %{handler_id: id} do
        assert :ok = ElectricSentry.add_logger_handler(id, level: :warning)

        assert %{level: :warning} = handler_config!(id)
      end

      test "uses the default handler id when called with no arguments" do
        default_id = ElectricSentry.default_handler_id()
        _ = :logger.remove_handler(default_id)
        on_exit(fn -> _ = :logger.remove_handler(default_id) end)

        assert :ok = ElectricSentry.add_logger_handler()

        {:ok, handler} = :logger.get_handler_config(default_id)
        assert handler.module == Sentry.LoggerHandler
      end
    end

    describe "scrub_request_credentials/1" do
      test "drops the conn Bandit logs with a crash, whose header and query carry the secret" do
        conn =
          Plug.Test.conn("GET", "/v1/shape/multiplex?secret=query-secret-value")
          |> Plug.Conn.put_req_header("authorization", "Bearer header-secret-value")

        event =
          Sentry.Event.create_event(
            message: "** (RuntimeError) boom",
            extra: %{logger_metadata: %{conn: conn, request_id: "request-1"}}
          )

        scrubbed = ElectricSentry.scrub_request_credentials(event)

        refute inspect(scrubbed) =~ "header-secret-value"
        refute inspect(scrubbed) =~ "query-secret-value"
        assert %{request_id: "request-1"} = scrubbed.extra.logger_metadata
      end

      @tag :request_value_redaction
      test "drops the logged conn with request value redaction on, from an event no request built" do
        Application.put_env(:electric, :telemetry_redact_request_values?, true)
        on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)

        conn =
          Plug.Test.conn("GET", "/v1/shape/multiplex?secret=query-secret-value")
          |> Plug.Conn.put_req_header("authorization", "Bearer header-secret-value")

        event =
          Sentry.Event.create_event(
            message: "** (RuntimeError) boom",
            extra: %{logger_metadata: %{conn: conn, request_id: "request-1"}}
          )

        scrubbed = ElectricSentry.scrub_request_credentials(event)

        refute inspect(scrubbed) =~ "header-secret-value"
        refute inspect(scrubbed) =~ "query-secret-value"
        assert %{request_id: "request-1"} = scrubbed.extra.logger_metadata
      end
    end
  end
end
