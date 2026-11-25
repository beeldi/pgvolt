defmodule Electric.Telemetry.Sentry do
  use Electric.Telemetry

  @default_handler_id :electric_sentry_handler

  @typedoc """
  Extra entries for the `Sentry.LoggerHandler` config map (e.g.
  `:discard_threshold`, `:sync_threshold`). Merged over the defaults at
  install time.
  """
  @type handler_opts :: [{atom(), term()}]

  def default_handler_id, do: @default_handler_id

  @spec add_logger_handler(atom(), handler_opts()) :: :ok | {:error, term()}
  @spec add_logger_handler(atom()) :: :ok | {:error, term()}
  @spec add_logger_handler() :: :ok | {:error, term()}
  def add_logger_handler(id \\ @default_handler_id, opts \\ [])

  with_telemetry Sentry.LoggerHandler do
    @default_config %{metadata: :all, capture_log_messages: true, level: :error}

    def add_logger_handler(id, opts) do
      config = Map.merge(@default_config, Map.new(opts))
      :logger.add_handler(id, Sentry.LoggerHandler, %{config: config})
    end
  else
    def add_logger_handler(_id, _opts), do: :ok
  end

  @spec set_tags_context(keyword()) :: :ok

  with_telemetry Sentry.Context do
    def set_tags_context(tags) do
      Sentry.Context.set_tags_context(Map.new(tags))
    end
  else
    def set_tags_context(_tags), do: :ok
  end

  @doc false
  with_telemetry Sentry.PlugContext do
    alias Electric.Telemetry.RequestValues

    def put_request_context(conn, opts) do
      if RequestValues.enabled?() do
        context_conn = %{
          conn
          | query_string: "",
            query_params: %{},
            params: %{},
            req_headers:
              Enum.reject(conn.req_headers, fn {name, _} -> RequestValues.url_header?(name) end)
        }

        context = Sentry.PlugContext.build_request_interface_data(context_conn, opts)
        query = if conn.query_string != "", do: conn.query_string

        context
        |> update_in([:env], &Map.merge(&1, RequestValues.summary("http.query_string", query)))
        |> Sentry.Context.set_request_context()

        conn
      else
        Sentry.PlugContext.call(conn, opts)
      end
    end
  else
    def put_request_context(conn, _opts), do: conn
  end

  @doc """
  `:before_send` callback that scrubs request URLs and query strings. When
  request value redaction is enabled, it also removes request data and
  free-form error details from request events.

  Bandit reports a request that crashes with its whole `Plug.Conn` as logger
  metadata, and the logger handler keeps every metadata key, so the event's
  `logger_metadata` drops `:conn`: its headers and query string hold the secret.
  """
  @spec scrub_request_credentials(term()) :: term()

  with_telemetry Sentry.Event do
    alias Electric.Plug.Utils
    alias Electric.Telemetry.RequestValues

    def scrub_request_credentials(%Sentry.Event{} = event) do
      event =
        if RequestValues.enabled?() do
          event
          |> Map.update!(:request, &redact_request/1)
          |> redact_request_error()
        else
          Map.update!(event, :request, &scrub_request/1)
        end

      drop_logged_conn(event)
    end

    def scrub_request_credentials(event), do: event

    defp drop_logged_conn(
           %Sentry.Event{extra: %{logger_metadata: %{conn: _} = metadata} = extra} = event
         ),
         do: %{event | extra: %{extra | logger_metadata: Map.delete(metadata, :conn)}}

    defp drop_logged_conn(event), do: event

    defp scrub_request(%_{} = request) do
      %{
        request
        | url: scrub_url(request.url, &Utils.redact_query_string/1),
          query_string: scrub_query_string(request.query_string)
      }
    end

    defp scrub_request(request), do: request

    defp redact_request(%_{} = request) do
      %{
        request
        | url: scrub_url(request.url, fn _ -> nil end),
          query_string: nil,
          data: nil,
          headers:
            if(request.headers,
              do: Map.reject(request.headers, fn {name, _} -> RequestValues.url_header?(name) end)
            )
      }
    end

    defp redact_request(request), do: request

    defp redact_request_error(%{source: source, request: request} = event) do
      if source == :plug or match?(%{method: method} when is_binary(method), request) do
        %{
          event
          | exception:
              Enum.map(event.exception, fn exception ->
                %{redact_stacktrace(exception) | value: exception.type}
              end),
            threads: if(event.threads, do: Enum.map(event.threads, &redact_stacktrace/1)),
            original_exception: nil,
            message:
              if(event.message, do: %Sentry.Interfaces.Message{formatted: "Request error"}),
            extra: %{},
            breadcrumbs: []
        }
      else
        event
      end
    end

    defp redact_stacktrace(%{stacktrace: %{frames: frames} = stacktrace} = item)
         when is_list(frames) do
      %{item | stacktrace: %{stacktrace | frames: Enum.map(frames, &%{&1 | vars: nil})}}
    end

    defp redact_stacktrace(item), do: item

    defp scrub_url(url, scrub) when is_binary(url) do
      uri = URI.parse(url)
      URI.to_string(%{uri | query: scrub.(uri.query)})
    end

    defp scrub_url(url, _scrub), do: url

    defp scrub_query_string(query_string) when is_binary(query_string),
      do: Utils.redact_query_string(query_string)

    defp scrub_query_string(query_string), do: query_string
  else
    def scrub_request_credentials(event), do: event
  end
end
