defmodule Electric.Client.Fetch.HTTP do
  @default_timeout 5 * 60
  @default_max_response_bytes 256 * 1024 * 1024
  @schema NimbleOptions.new!(
            timeout: [
              type: {:or, [:pos_integer, {:in, [:infinity]}]},
              doc: """
              Request timeout in seconds or `:infinity` for no timeout.

              The client will keep trying the remote Electric server until it reaches
              this timeout.
              """,
              default: @default_timeout,
              type_spec: quote(do: pos_integer() | :infinity)
            ],
            is_transient_fun: [
              type: {:fun, 1},
              default: &Electric.Client.Fetch.HTTP.transient_response?/1,
              doc: """
              Function that determines if a server response represents a transient error and should be retried.

              Defaults to identical behaviour to `Req`, and retries any
              response with an HTTP 408/429/500/502/503/504 status.
              """
            ],
            headers: [
              type: {:or, [{:map, :string, :string}, {:list, {:tuple, [:string, :string]}}]},
              doc: """
              Additional headers to add to every request.

              This can be a list of tuples, `[{"my-header", "my-header-value"}]` or a map.
              """,
              default: [],
              type_spec: quote(do: [{binary(), binary()}] | %{binary() => binary()})
            ],
            request: [
              type: :keyword_list,
              doc: """
              Options to include in `Req.new/1` for every request.
              """,
              default: []
            ],
            max_response_bytes: [
              type: :pos_integer,
              doc: """
              Largest response body to accept, in bytes, counted as received and again after
              decompression.

              A longer response fails the request with
              `Electric.Client.Fetch.HTTP.ResponseTooLargeError`, without a retry.
              """,
              default: @default_max_response_bytes,
              type_spec: quote(do: pos_integer())
            ]
          )

  @moduledoc """
  Client `Electric.Client.Fetch` implementation for HTTP requests to an
  external Electric API server.

  This is the default backend when creating an Electric client using
  `Electric.Client.new/1`.

  You can configure aspects of its behaviour by passing options when in the
  call to `Electric.Client.new/1`

      Electric.Client.new(
        base_url: "http://localhost:3000",
        fetch:
          {Electric.Client.Fetch.HTTP,
            timeout: 3600,
            request: [headers: [{"authorize", "Bearer xxxtoken"}]}
      )


  ## Retries

  Transient failures are retried with exponential backoff and full jitter, up to 32 seconds
  between attempts. If the server's response carries a `Retry-After` header (delta-seconds or
  an HTTP-date) the client waits at least that long, up to a limit of one hour. A response
  without the header, or with one the client can't read, keeps the client's own backoff.

  ## Options

  #{NimbleOptions.docs(@schema)}
  """

  alias Electric.Client.Fetch

  require Logger

  @behaviour Electric.Client.Fetch

  defmodule ResponseTooLargeError do
    @moduledoc """
    A response body is longer than the `:max_response_bytes` option of
    `Electric.Client.Fetch.HTTP` allows.
    """

    defexception [:max_bytes]

    @impl true
    def message(%__MODULE__{max_bytes: max_bytes}) do
      "response body is longer than the limit of #{max_bytes} bytes"
    end
  end

  @body_over_limit :electric_body_over_limit

  @impl Electric.Client.Fetch
  def validate_opts(opts) do
    NimbleOptions.validate(opts, @schema)
  end

  @impl Electric.Client.Fetch
  def fetch(%Fetch.Request{authenticated: true} = request, opts) do
    request
    |> build_request(opts)
    |> request()
  end

  @doc false
  def build_request(%Fetch.Request{authenticated: true} = request, opts) do
    request_opts = Keyword.get(opts, :request, [])
    {retry_delay, request_opts} = Keyword.pop(request_opts, :retry_delay, &retry_delay/1)

    retry_delay_fun =
      case retry_delay do
        fun1 when is_function(fun1, 1) -> fun1
        delay when is_integer(delay) and delay > 0 -> fn _ -> delay end
      end

    is_transient_fun =
      Keyword.get(opts, :is_transient_fun, &Electric.Client.Fetch.HTTP.transient_response?/1)

    timeout = Keyword.get(opts, :timeout, @default_timeout)
    max_response_bytes = Keyword.get(opts, :max_response_bytes, @default_max_response_bytes)
    {pool, request_opts} = Keyword.pop(request_opts, :finch, nil)

    connect_options =
      if pool do
        [finch: pool]
      else
        [connect_options: [protocols: [:http2, :http1]]]
      end

    [
      method: request.method,
      url: Fetch.Request.url(request),
      headers: merge_headers(request.headers, Keyword.get(opts, :headers, [])),
      retry: &retry(&1, &2, retry_delay_fun, is_transient_fun, timeout),
      # turn off req's retry logging and replace with ours
      retry_log_level: false,
      # :infinity actually means this number of retries, which equates to ~10 years
      max_retries: 10_512_000,
      # we use long polling with a timeout of 20s so we don't want Req to error before
      # Electric has returned something
      receive_timeout: 60_000
    ]
    |> Keyword.merge(connect_options)
    |> Req.new()
    |> merge_options(request_opts)
    |> Req.Request.put_private(:electric_start_request, now())
    |> limit_response_size(max_response_bytes)
  end

  # Without `into` Req holds the whole body before anything can look at its size. With it, Req
  # neither asks for compression nor inflates, so `compressed: true` is honoured here, for gzip
  # alone since that is what can be inflated in bounded steps.
  defp limit_response_size(%Req.Request{} = request, max_bytes) do
    request = Req.merge(request, into: &collect_body(&1, &2, max_bytes))

    %{
      request
      | response_steps:
          Keyword.replace!(
            request.response_steps,
            :decompress_body,
            &decompress_body_within_limit(&1, max_bytes)
          )
    }
    |> accept_gzip()
  end

  defp accept_gzip(%Req.Request{} = request) do
    if Req.Request.get_option(request, :compressed, false) do
      Req.Request.put_new_header(request, "accept-encoding", "gzip")
    else
      request
    end
  end

  defp collect_body({:data, data}, {request, response}, max_bytes) do
    body = response.body <> data

    if byte_size(body) > max_bytes do
      {:halt, {request, Req.Response.put_private(%{response | body: ""}, @body_over_limit, true)}}
    else
      {:cont, {request, %{response | body: body}}}
    end
  end

  defp decompress_body_within_limit({request, response}, max_bytes) do
    cond do
      Req.Response.get_private(response, @body_over_limit, false) ->
        {request, %ResponseTooLargeError{max_bytes: max_bytes}}

      gzip_encoded?(request, response) and is_binary(response.body) and response.body != "" ->
        inflate_response(request, response, max_bytes)

      true ->
        {request, response}
    end
  end

  defp gzip_encoded?(request, response) do
    Req.Request.get_option(request, :compressed, false) and request.options[:raw] != true and
      content_codings(response) in [["gzip"], ["x-gzip"]]
  end

  defp content_codings(response) do
    response
    |> Req.Response.get_header("content-encoding")
    |> Enum.flat_map(&String.split(&1, ",", trim: true))
    |> Enum.map(&(&1 |> String.trim() |> String.downcase()))
  end

  defp inflate_response(request, response, max_bytes) do
    case inflate_within_limit(response.body, max_bytes) do
      {:ok, body} ->
        response =
          %{response | body: body}
          |> Req.Response.delete_header("content-encoding")
          |> Req.Response.delete_header("content-length")

        {request, response}

      :too_large ->
        {request, %ResponseTooLargeError{max_bytes: max_bytes}}

      {:error, reason} ->
        {request, %Req.DecompressError{format: :gzip, data: response.body, reason: reason}}
    end
  end

  defp inflate_within_limit(body, max_bytes) do
    zlib = :zlib.open()

    try do
      # 31 selects the gzip wrapper; `:reset` reads concatenated members, as `:zlib.gunzip/1` does
      :zlib.inflateInit(zlib, 31, :reset)
      inflate_steps(zlib, :zlib.safeInflate(zlib, body), [], 0, max_bytes)
    rescue
      error in ErlangError -> {:error, error.original}
    after
      :zlib.close(zlib)
    end
  end

  defp inflate_steps(zlib, {status, output}, inflated, size, max_bytes) do
    size = size + IO.iodata_length(output)

    cond do
      size > max_bytes ->
        :too_large

      status == :continue ->
        inflate_steps(zlib, :zlib.safeInflate(zlib, []), [inflated, output], size, max_bytes)

      true ->
        :zlib.inflateEnd(zlib)
        {:ok, IO.iodata_to_binary([inflated, output])}
    end
  end

  defp request(request) do
    now = DateTime.utc_now()
    request |> Req.request() |> wrap_resp(now)
  end

  defp wrap_resp({:ok, %Req.Response{} = resp}, timestamp) do
    %{status: status, headers: headers, body: body} = resp
    {:ok, Fetch.Response.decode!(status, headers, body, timestamp)}
  end

  defp wrap_resp({:error, _} = error, _timestamp) do
    error
  end

  defp merge_options(%Req.Request{} = request, options) do
    %{request | options: Map.merge(request.options, Map.new(options), &resolve_merge_options/3)}
  end

  defp resolve_merge_options(_key, left, right)
       when is_list(left) and (is_list(right) or is_map(right)) do
    Keyword.merge(left, Enum.to_list(right))
  end

  defp resolve_merge_options(_key, left, right)
       when is_map(left) and (is_list(right) or is_map(right)) do
    Map.merge(left, Map.new(right))
  end

  defp resolve_merge_options(_key, _left, right) do
    right
  end

  defp merge_headers(request_headers, opts_headers) do
    Enum.concat(Enum.to_list(request_headers), Enum.to_list(opts_headers))
  end

  defp retry(
         %Req.Request{} = request,
         response_or_error,
         retry_delay_fun,
         is_transient_fun,
         :infinity
       ) do
    if transient?(response_or_error, is_transient_fun) do
      delay_ms = request_delay(request, response_or_error, retry_delay_fun)
      log_retry(response_or_error, retry_count(request), delay_ms, "")

      {:delay, delay_ms}
    end
  end

  defp retry(
         %Req.Request{} = request,
         response_or_error,
         retry_delay_fun,
         is_transient_fun,
         max_age
       )
       when is_integer(max_age) do
    start_time = Req.Request.get_private(request, :electric_start_request)
    age = now() - start_time
    delay_ms = request_delay(request, response_or_error, retry_delay_fun)

    # using the :transient retry methodology here, retrying even POSTs,
    # because our server's endpoints are idempotent by design
    if transient?(response_or_error, is_transient_fun) && age + delay_ms / 1000 <= max_age do
      log_retry(
        response_or_error,
        retry_count(request),
        delay_ms,
        " #{max_age - age}s remaining."
      )

      {:delay, delay_ms}
    else
      false
    end
  end

  defp log_retry(response_or_error, retry_count, delay_ms, timeout_message) do
    Logger.warning(fn ->
      case response_or_error do
        %{__exception__: true} = exception ->
          [
            "retry: got exception: (",
            inspect(exception.__struct__),
            ") ",
            Exception.message(exception)
          ]

        response ->
          ["retry: got response with status #{response.status}, "]
      end
    end)

    Logger.warning(fn ->
      [
        "retry: transient error. attempt #{retry_count + 1}, will retry in #{delay_ms}ms.",
        timeout_message
      ]
    end)
  end

  defp retry_count(%Req.Request{} = request),
    do: Req.Request.get_private(request, :req_retry_count, 0)

  # The server's `Retry-After` is a floor under the client's own backoff.
  defp request_delay(%Req.Request{} = request, response_or_error, retry_delay_fun) do
    backoff_ms =
      request
      |> retry_count()
      |> then(retry_delay_fun)

    max(retry_after_ms(response_or_error), backoff_ms)
  end

  # A server asking for more, or sending a date far ahead, must not park the client for days.
  # The request `timeout` still ends the retries first if it is shorter.
  @max_retry_after_ms 3_600_000

  @doc false
  # Returns the delay in milliseconds asked for by the `Retry-After` header of a response, or `0`
  # if there is no such header, it can't be parsed or it asks for no delay. Accepts both the
  # delta-seconds form (`"120"`) and the HTTP-date form (`"Wed, 21 Oct 2026 07:28:00 GMT"`).
  @spec retry_after_ms(Req.Response.t() | Exception.t(), DateTime.t()) :: non_neg_integer()
  def retry_after_ms(response_or_error, now \\ DateTime.utc_now())

  def retry_after_ms(%Req.Response{} = response, %DateTime{} = now) do
    case Req.Response.get_header(response, "retry-after") do
      [value | _] -> parse_retry_after(value, now)
      [] -> 0
    end
  end

  def retry_after_ms(_error, _now), do: 0

  defp parse_retry_after(value, now) do
    value = String.trim(value)

    case Integer.parse(value) do
      {seconds, ""} when seconds > 0 ->
        min(seconds * 1000, @max_retry_after_ms)

      {_seconds, ""} ->
        0

      _not_a_number ->
        case parse_http_date(value) do
          {:ok, date} ->
            date |> DateTime.diff(now, :millisecond) |> max(0) |> min(@max_retry_after_ms)

          :error ->
            0
        end
    end
  end

  @months ~w(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)

  # Only the IMF-fixdate form ("Sun, 06 Nov 1994 08:49:37 GMT"). RFC 9110's two obsolete
  # forms count as unparseable, so the client keeps its own backoff.
  defp parse_http_date(value) do
    with [_, day, month, year, hour, minute, second] <-
           Regex.run(
             ~r/\A[A-Z][a-z]{2}, (\d{2}) ([A-Z][a-z]{2}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT\z/,
             value
           ),
         month_number when is_integer(month_number) <- Enum.find_index(@months, &(&1 == month)),
         {:ok, date} <-
           Date.new(String.to_integer(year), month_number + 1, String.to_integer(day)),
         {:ok, time} <-
           Time.new(
             String.to_integer(hour),
             String.to_integer(minute),
             String.to_integer(second)
           ) do
      DateTime.new(date, time, "Etc/UTC")
    else
      _ -> :error
    end
  end

  defp retry_delay(n) do
    # Full jitter strategy (AWS recommended), minimum 1ms:
    #   random_between(1, min(cap, base * 2^n))
    # See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    delay = min(32_000, Integer.pow(2, n) * 1000)
    :rand.uniform(delay)
  end

  defp now, do: System.monotonic_time(:second)

  @transient_status [408, 429, 500, 502, 503, 504]

  @transient_http1_framing_reasons [
    :invalid_chunk_size,
    :missing_crlf_after_chunk,
    :invalid_trailer_header,
    :invalid_status_line
  ]

  @doc """
  List of HTTP status codes that represent a retryable error.
  """
  def transient_status_codes, do: @transient_status

  @doc """
  Test the given `Req.Response` against the list of transient error status
  codes.

  Returns `true` if the response has a status code in this list and so the
  request is retryable.
  """
  @spec transient_response?(Req.Response.t(), [pos_integer(), ...]) :: boolean()
  def transient_response?(response, status_codes \\ @transient_status)

  def transient_response?(%Req.Response{status: status}, status_codes) do
    status in status_codes
  end

  defp transient?(%Req.Response{} = response, is_transient_fun) do
    is_transient_fun.(response)
  end

  defp transient?(%Req.TransportError{reason: reason}, _is_transient_fun)
       when reason in [:timeout, :econnrefused, :closed] do
    true
  end

  defp transient?(%Req.HTTPError{protocol: :http2, reason: :unprocessed}, _is_transient_fun) do
    true
  end

  # Finch starts a pool on the first request to a host; a request racing that start finds the
  # pool's supervisor registered before its connection, and gets this error instead of waiting.
  defp transient?(%Req.HTTPError{reason: :pool_not_available}, _is_transient_fun) do
    true
  end

  defp transient?(%Req.HTTPError{protocol: :http1, reason: reason}, _is_transient_fun)
       when reason in @transient_http1_framing_reasons do
    true
  end

  defp transient?(%{__exception__: true}, _is_transient_fun) do
    false
  end
end
