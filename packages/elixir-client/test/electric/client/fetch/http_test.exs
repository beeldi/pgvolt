defmodule Electric.Client.Fetch.HTTPTest do
  use ExUnit.Case, async: true

  alias Electric.Client.Fetch

  describe "validate_opts/1" do
    test "pass valid headers" do
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(headers: [{"this", "that"}])
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(headers: %{"this" => "that"})
    end

    test "reject invalid headers" do
      assert {:error, _} = Fetch.HTTP.validate_opts(headers: "here")
    end

    test "pass valid timeout" do
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(timeout: 123)
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(timeout: :infinity)
    end

    test "reject invalid timeout" do
      assert {:error, _} = Fetch.HTTP.validate_opts(timeout: -123)
      assert {:error, _} = Fetch.HTTP.validate_opts(timeout: 0)
      assert {:error, _} = Fetch.HTTP.validate_opts(timeout: :something)
    end

    test "pass valid request opts" do
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(request: [doesnt: "matter"])
    end

    test "reject invalid opts" do
      assert {:error, _} = Fetch.HTTP.validate_opts(request: :ouch)
    end

    test "pass valid is_transient_fun" do
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(is_transient_fun: fn _ -> false end)
    end

    test "reject invalid is_transient_fun" do
      assert {:error, _} = Fetch.HTTP.validate_opts(is_transient_fun: fn _, _ -> false end)
      assert {:error, _} = Fetch.HTTP.validate_opts(is_transient_fun: nil)
    end
  end

  describe "max_response_bytes" do
    setup do
      bypass = Bypass.open()

      request = %Fetch.Request{
        endpoint: URI.new!("http://localhost:#{bypass.port}/v1/shape"),
        authenticated: true
      }

      [bypass: bypass, request: request]
    end

    defp respond(bypass, body, headers \\ [], counter \\ nil) do
      Bypass.expect(bypass, "GET", "/v1/shape", fn conn ->
        if counter, do: :counters.add(counter, 1, 1)

        headers
        |> Enum.reduce(conn, fn {name, value}, conn ->
          Plug.Conn.put_resp_header(conn, name, value)
        end)
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(200, body)
      end)
    end

    defp compressed_within(max_bytes) do
      [max_response_bytes: max_bytes, request: [compressed: true]]
    end

    defp json_of_size(size) do
      ~s([{"value":"#{String.duplicate("a", size - 14)}"}])
    end

    test "pass valid limit" do
      assert {:ok, _opts} = Fetch.HTTP.validate_opts(max_response_bytes: 1024)
    end

    test "reject invalid limit" do
      assert {:error, _} = Fetch.HTTP.validate_opts(max_response_bytes: 0)
      assert {:error, _} = Fetch.HTTP.validate_opts(max_response_bytes: :infinity)
    end

    test "returns a body of exactly the limit", %{bypass: bypass, request: request} do
      respond(bypass, json_of_size(1024))

      assert {:ok, %Fetch.Response{status: 200, body: [%{"value" => value}]}} =
               Fetch.HTTP.fetch(request, max_response_bytes: 1024)

      assert byte_size(value) == 1010
    end

    test "fails a body over the limit", %{bypass: bypass, request: request} do
      respond(bypass, json_of_size(1025))

      assert {:error, %Fetch.HTTP.ResponseTooLargeError{max_bytes: 1024}} =
               Fetch.HTTP.fetch(request, max_response_bytes: 1024)
    end

    test "does not retry a body over the limit", %{bypass: bypass, request: request} do
      counter = :counters.new(1, [])
      respond(bypass, json_of_size(2048), [], counter)

      assert {:error, %Fetch.HTTP.ResponseTooLargeError{}} =
               Fetch.HTTP.fetch(request, max_response_bytes: 1024)

      assert :counters.get(counter, 1) == 1
    end

    test "does not ask for compression by default", %{bypass: bypass, request: request} do
      Bypass.expect(bypass, "GET", "/v1/shape", fn conn ->
        assert Plug.Conn.get_req_header(conn, "accept-encoding") == []

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(200, ~s([{"id":"1"}]))
      end)

      assert {:ok, %Fetch.Response{body: [%{"id" => "1"}]}} =
               Fetch.HTTP.fetch(request, max_response_bytes: 1024)
    end

    test "asks for gzip and decodes a gzip body", %{bypass: bypass, request: request} do
      Bypass.expect(bypass, "GET", "/v1/shape", fn conn ->
        assert Plug.Conn.get_req_header(conn, "accept-encoding") == ["gzip"]

        conn
        |> Plug.Conn.put_resp_header("content-encoding", "gzip")
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(200, :zlib.gzip(~s([{"id":"1"}])))
      end)

      assert {:ok, %Fetch.Response{body: [%{"id" => "1"}]}} =
               Fetch.HTTP.fetch(request, compressed_within(1024))
    end

    test "decodes a gzip body whatever the case of its content coding", %{
      bypass: bypass,
      request: request
    } do
      Bypass.expect(bypass, "GET", "/v1/shape", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("content-encoding", "GZip")
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(200, :zlib.gzip(~s([{"id":"1"}])))
      end)

      assert {:ok, %Fetch.Response{body: [%{"id" => "1"}]}} =
               Fetch.HTTP.fetch(request, compressed_within(1024))
    end

    test "fails a small gzip body that inflates past the limit", %{
      bypass: bypass,
      request: request
    } do
      compressed = :zlib.gzip(json_of_size(1024 * 1024))
      assert byte_size(compressed) < 8 * 1024
      respond(bypass, compressed, [{"content-encoding", "gzip"}])

      assert {:error, %Fetch.HTTP.ResponseTooLargeError{max_bytes: 8192}} =
               Fetch.HTTP.fetch(request, compressed_within(8192))
    end

    test "fails a truncated gzip body", %{bypass: bypass, request: request} do
      compressed = :zlib.gzip(json_of_size(4096))
      respond(bypass, binary_part(compressed, 0, 16), [{"content-encoding", "gzip"}])

      assert {:error, %Req.DecompressError{format: :gzip}} =
               Fetch.HTTP.fetch(request, compressed_within(8192))
    end
  end

  describe "build_request/2" do
    test "merges connect_options" do
      assert %Req.Request{options: options} =
               %Fetch.Request{
                 endpoint: URI.new!("http://localhost:3000/v1/shape"),
                 authenticated: true,
                 headers: %{"h1" => "v1", "h2" => "v2"}
               }
               |> Fetch.HTTP.build_request(
                 request: [
                   retry: true,
                   retry_log_level: false,
                   connect_options: [proxy: {"http", "123.123.123.123", 8080, []}]
                 ]
               )

      assert %{
               retry: true,
               retry_log_level: false,
               connect_options: [
                 protocols: [:http2, :http1],
                 proxy: {"http", "123.123.123.123", 8080, []}
               ]
             } = options
    end

    test "merges headers" do
      assert %Req.Request{headers: headers} =
               %Fetch.Request{
                 endpoint: URI.new!("http://localhost:3000/v1/shape"),
                 authenticated: true,
                 headers: %{"h1" => "v1", "h2" => "v2"}
               }
               |> Fetch.HTTP.build_request(
                 headers: [{"h1", "v1_2"}, {"h3", "v3"}, {"h4", "v4_1"}, {"h4", "v4_2"}]
               )

      assert %{
               "h1" => ["v1", "v1_2"],
               "h2" => ["v2"],
               "h3" => ["v3"],
               "h4" => ["v4_1", "v4_2"]
             } = headers
    end
  end

  describe "retry classification for transport errors" do
    setup do
      request =
        %Fetch.Request{
          endpoint: URI.new!("http://localhost:3000/v1/shape"),
          authenticated: true
        }
        |> Fetch.HTTP.build_request(request: [retry_delay: 1])

      %{request: request}
    end

    for reason <- [
          :invalid_chunk_size,
          :missing_crlf_after_chunk,
          :invalid_trailer_header,
          :invalid_status_line
        ] do
      test "retries HTTP/1 framing error #{reason} on a fresh connection", %{request: request} do
        attempts = :counters.new(1, [])

        adapter = fn req ->
          :counters.add(attempts, 1, 1)

          case :counters.get(attempts, 1) do
            1 -> {req, %Req.HTTPError{protocol: :http1, reason: unquote(reason)}}
            _ -> {req, Req.Response.new(status: 200, body: "")}
          end
        end

        assert {:ok, %Req.Response{status: 200}} = Req.request(%{request | adapter: adapter})
        assert :counters.get(attempts, 1) == 2
      end
    end

    for protocol <- [:http2, :http1] do
      test "retries a request that raced the start of its #{protocol} pool", %{request: request} do
        attempts = :counters.new(1, [])

        adapter = fn req ->
          :counters.add(attempts, 1, 1)

          case :counters.get(attempts, 1) do
            1 -> {req, %Req.HTTPError{protocol: unquote(protocol), reason: :pool_not_available}}
            _ -> {req, Req.Response.new(status: 200, body: "")}
          end
        end

        assert {:ok, %Req.Response{status: 200}} = Req.request(%{request | adapter: adapter})
        assert :counters.get(attempts, 1) == 2
      end
    end

    test "does not retry non-framing HTTP/1 protocol errors", %{request: request} do
      attempts = :counters.new(1, [])

      adapter = fn req ->
        :counters.add(attempts, 1, 1)
        {req, %Req.HTTPError{protocol: :http1, reason: :more_than_one_content_length_header}}
      end

      assert {:error, %Req.HTTPError{reason: :more_than_one_content_length_header}} =
               Req.request(%{request | adapter: adapter})

      assert :counters.get(attempts, 1) == 1
    end
  end

  describe "Retry-After" do
    defp build(opts \\ []) do
      {timeout, opts} = Keyword.pop(opts, :timeout, 300)
      {retry_delay, _opts} = Keyword.pop(opts, :retry_delay, 1)

      %Fetch.Request{
        endpoint: URI.new!("http://localhost:3000/v1/shape"),
        authenticated: true
      }
      |> Fetch.HTTP.build_request(timeout: timeout, request: [retry_delay: retry_delay])
    end

    # what the retry callback tells Req to do with this response
    defp retry_decision(request, response) do
      request.options.retry.(request, response)
    end

    defp response(status, retry_after) do
      Req.Response.new(status: status, headers: [{"retry-after", retry_after}])
    end

    test "waits as long as a 503 asks for" do
      assert {:delay, 7_000} = retry_decision(build(), response(503, "7"))
    end

    test "waits as long as other retryable statuses ask for" do
      for status <- Fetch.HTTP.transient_status_codes() do
        assert {:delay, 3_000} = retry_decision(build(), response(status, "3"))
      end
    end

    test "keeps its own backoff when the server asks for less" do
      assert {:delay, 5_000} = retry_decision(build(retry_delay: 5_000), response(503, "2"))
    end

    test "keeps its own backoff when there is no Retry-After" do
      assert {:delay, 1} = retry_decision(build(), Req.Response.new(status: 503))

      assert {:delay, 5_000} =
               retry_decision(build(retry_delay: 5_000), Req.Response.new(status: 503))
    end

    test "keeps its own backoff when Retry-After can't be used" do
      for value <- ["", "soon", "-5", "0", "1.5", "5 seconds", "Wed, 32 Oct 2026 07:28:00 GMT"] do
        assert {:delay, 1} = retry_decision(build(), response(503, value)),
               "Retry-After: #{inspect(value)}"
      end
    end

    test "waits at most an hour, however much the server asks for" do
      request = build(timeout: :infinity)

      assert {:delay, 3_600_000} = retry_decision(request, response(503, "86400"))
      assert {:delay, 3_600_000} = retry_decision(request, response(503, "99999999999999999999"))
    end

    test "gives up instead of waiting past the request timeout" do
      assert retry_decision(build(timeout: 30), response(503, "60")) == false
      assert {:delay, 10_000} = retry_decision(build(timeout: 30), response(503, "10"))
    end

    test "does not retry a response that is not transient" do
      refute retry_decision(build(), response(404, "5"))
    end

    test "sleeps for the time the server asked for before the next attempt" do
      attempts = :counters.new(1, [])

      adapter = fn req ->
        :counters.add(attempts, 1, 1)

        case :counters.get(attempts, 1) do
          1 -> {req, response(503, "1")}
          _ -> {req, Req.Response.new(status: 200, body: "")}
        end
      end

      started = System.monotonic_time(:millisecond)

      assert {:ok, %Req.Response{status: 200}} = Req.request(%{build() | adapter: adapter})

      assert System.monotonic_time(:millisecond) - started >= 1_000
      assert :counters.get(attempts, 1) == 2
    end

    test "retry_after_ms/2 reads delta-seconds and HTTP-dates" do
      now = ~U[2026-10-21 07:27:00Z]

      assert Fetch.HTTP.retry_after_ms(response(503, "120"), now) == 120_000
      assert Fetch.HTTP.retry_after_ms(response(503, " 120 "), now) == 120_000

      assert Fetch.HTTP.retry_after_ms(response(503, "Wed, 21 Oct 2026 07:28:00 GMT"), now) ==
               60_000

      assert Fetch.HTTP.retry_after_ms(response(503, "Wed, 21 Oct 2026 07:27:00 GMT"), now) == 0
      # a date in the past, e.g. from clock skew, asks for no delay
      assert Fetch.HTTP.retry_after_ms(response(503, "Wed, 21 Oct 2026 07:00:00 GMT"), now) == 0

      assert Fetch.HTTP.retry_after_ms(response(503, "Wed, 21 Oct 2027 07:27:00 GMT"), now) ==
               3_600_000
    end

    test "retry_after_ms/2 is zero without a usable header" do
      now = ~U[2026-10-21 07:27:00Z]

      assert Fetch.HTTP.retry_after_ms(Req.Response.new(status: 503), now) == 0
      assert Fetch.HTTP.retry_after_ms(response(503, "Wed, 21 Foo 2026 07:28:00 GMT"), now) == 0
      assert Fetch.HTTP.retry_after_ms(%Req.TransportError{reason: :timeout}, now) == 0
    end
  end
end
