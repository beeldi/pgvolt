defmodule Electric.Plug.UtilsTest do
  alias Electric.Plug.Utils
  alias OpenTelemetry.SemConv, as: SC

  use ExUnit.Case, async: true
  doctest Utils, import: true

  describe "parse_columns_param/1" do
    test "keeps commas inside quoted identifiers and splits the rest" do
      assert Utils.parse_columns_param(~S|"a,b",c|) == {:ok, ["a,b", "c"]}
    end

    test "parses a large comma-separated list in linear, not quadratic, time" do
      # 50k unquoted columns. The previous per-comma regex lookahead re-scanned to end-of-string at
      # every comma, so an input this size burned tens of seconds; the linear split returns in ms.
      columns = Enum.map_join(1..50_000, ",", &"c#{&1}")

      task = Task.async(fn -> Utils.parse_columns_param(columns) end)

      case Task.yield(task, 5_000) || Task.shutdown(task) do
        {:ok, {:ok, parsed}} ->
          assert length(parsed) == 50_000
          assert List.first(parsed) == "c1"

        other ->
          flunk("parse_columns_param did not finish in 5s (quadratic split?): #{inspect(other)}")
      end
    end
  end

  describe "redact_query_string/1" do
    test "redacts sensitive query params while preserving order and flags" do
      assert Utils.redact_query_string(
               "table=users&secret=hunter2&flag&Token=abc123&offset=-1&token"
             ) ==
               "table=users&secret=[REDACTED]&flag&Token=[REDACTED]&offset=-1&token"
    end

    test "redacts empty sensitive values and encoded sensitive keys" do
      assert Utils.redact_query_string(
               "secret=&api_secret=legacy&access%5Ftoken=abc&api_key=123&password=swordfish"
             ) ==
               "secret=[REDACTED]&api_secret=[REDACTED]&access%5Ftoken=[REDACTED]&api_key=[REDACTED]&password=[REDACTED]"
    end
  end

  describe "common_open_telemetry_attrs/1" do
    test "redacts query string, url.full and per-param attributes consistently" do
      conn =
        Plug.Test.conn(
          "GET",
          "/v1/shape?table=users&secret=hunter2&api_secret=legacy&Token=abc123&flag&offset=-1&api_key=123"
        )
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:plug_request_id, "req-123")

      attrs = Utils.common_open_telemetry_attrs(conn)

      assert attrs["http.query_string"] ==
               "table=users&secret=[REDACTED]&api_secret=[REDACTED]&Token=[REDACTED]&flag&offset=-1&api_key=[REDACTED]"

      assert attrs[SC.URLAttributes.url_full()] ==
               "http://www.example.com/v1/shape?table=users&secret=[REDACTED]&api_secret=[REDACTED]&Token=[REDACTED]&flag&offset=-1&api_key=[REDACTED]"

      assert attrs["http.query_param.secret"] == "[REDACTED]"
      assert attrs["http.query_param.api_secret"] == "[REDACTED]"
      assert attrs["http.query_param.table"] == "users"
      assert attrs["http.query_param.offset"] == "-1"

      # The query string above still redacts every sensitive value, but names outside the
      # telemetry allowlist never become attribute keys: Token/api_key are attacker-supplied and a
      # bare flag is not a shape param.
      refute Map.has_key?(attrs, "http.query_param.Token")
      refute Map.has_key?(attrs, "http.query_param.api_key")
      refute Map.has_key?(attrs, "http.query_param.flag")
    end

    test "includes documented top-level subset body params alongside query params" do
      conn =
        Plug.Test.conn(:post, "/v1/shape?table=bar")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "where" => "value ILIKE $1",
          "params" => %{"1" => "%2"},
          "limit" => 1,
          "offset" => 0,
          "tags" => ["a", 2, nil],
          "table" => "items",
          "skip_me" => nil
        })

      attrs = Utils.common_open_telemetry_attrs(conn)

      assert attrs["http.query_param.table"] == "bar"
      assert attrs["http.body_param.subset.where"] == "value ILIKE $1"
      assert attrs["http.body_param.subset.params"] == ~s(%{"1" => "%2"})
      assert attrs["http.body_param.subset.limit"] == 1
      assert attrs["http.body_param.subset.offset"] == 0
      refute Map.has_key?(attrs, "http.body_param.table")
      refute Map.has_key?(attrs, "http.body_param.subset.tags")
      refute Map.has_key?(attrs, "http.body_param.skip_me")
    end

    test "treats top-level subset body params as subset telemetry attrs" do
      conn =
        Plug.Test.conn(:post, "/v1/shape")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "where" => "value ILIKE $1",
          "params" => %{"1" => "%2"},
          "limit" => 1,
          "offset" => 0
        })

      attrs = Utils.common_open_telemetry_attrs(conn)

      assert attrs["http.body_param.subset.where"] == "value ILIKE $1"
      assert attrs["http.body_param.subset.params"] == ~s(%{"1" => "%2"})
      assert attrs["http.body_param.subset.limit"] == 1
      assert attrs["http.body_param.subset.offset"] == 0
      refute Map.has_key?(attrs, "http.body_param.where")
    end

    test "truncates large body param strings to 2000 graphemes and keeps valid UTF-8" do
      long_value = String.duplicate("😀", 20_000)

      conn =
        Plug.Test.conn(:post, "/v1/shape")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "where" => long_value
        })

      attrs = Utils.common_open_telemetry_attrs(conn)
      truncated = attrs["http.body_param.subset.where"]

      assert String.valid?(truncated)
      assert String.length(truncated) == 2_000
      assert byte_size(truncated) < byte_size(long_value)
      assert String.starts_with?(long_value, truncated)
    end

    test "drops invalid leading bytes when truncating body param strings" do
      long_value = <<255>> <> String.duplicate("a", 3_000)

      conn =
        Plug.Test.conn(:post, "/v1/shape")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "where" => long_value
        })

      attrs = Utils.common_open_telemetry_attrs(conn)
      truncated = attrs["http.body_param.subset.where"]

      assert String.valid?(truncated)
      assert truncated == String.duplicate("a", 2_000)
    end

    test "drops invalid bytes from body param strings that do not need truncation" do
      value = "prefix" <> <<255>> <> "suffix"

      conn =
        Plug.Test.conn(:post, "/v1/shape")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "where" => value
        })

      attrs = Utils.common_open_telemetry_attrs(conn)
      sanitized = attrs["http.body_param.subset.where"]

      assert String.valid?(sanitized)
      assert sanitized == "prefixsuffix"
    end

    test "records subset params as a single truncated string attr" do
      long_param = String.duplicate("a", 3_000)

      conn =
        Plug.Test.conn(:post, "/v1/shape")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "params" => %{"1" => long_param}
        })

      attrs = Utils.common_open_telemetry_attrs(conn)
      params_attr = attrs["http.body_param.subset.params"]

      assert String.valid?(params_attr)
      assert String.length(params_attr) <= 2_000
      assert String.starts_with?(params_attr, ~s(%{"1" => "aaaa))
    end

    test "ignores non-subset top-level POST body params" do
      conn =
        Plug.Test.conn(:post, "/v1/shape")
        |> Plug.Conn.fetch_query_params()
        |> Plug.Conn.assign(:body_params, %{
          "table" => "items",
          "columns" => ["id"]
        })

      attrs = Utils.common_open_telemetry_attrs(conn)

      refute Map.has_key?(attrs, "http.body_param.table")
      refute Map.has_key?(attrs, "http.body_param.subset.params")

      refute Enum.any?(Map.keys(attrs), fn key ->
               is_binary(key) and String.starts_with?(key, "http.body_param.")
             end)
    end

    test "bounds request header attributes to the telemetry allowlist" do
      attacker_headers = for i <- 1..200, do: {"x-attacker-#{i}", "v"}

      conn =
        %{
          Plug.Test.conn("GET", "/v1/shape")
          | req_headers: [{"user-agent", "curl"} | attacker_headers]
        }
        |> Plug.Conn.fetch_query_params()

      attrs = Utils.common_open_telemetry_attrs(conn)

      request_header_keys =
        attrs
        |> Map.keys()
        |> Enum.filter(&(is_binary(&1) and String.starts_with?(&1, "http.request.header.")))

      assert attrs["http.request.header.user-agent"] == "curl"
      refute Map.has_key?(attrs, "http.request.header.x-attacker-1")
      assert request_header_keys == ["http.request.header.user-agent"]
    end

    test "bounds query-param attributes to the telemetry allowlist" do
      attacker_params = for i <- 1..200, do: "attacker_#{i}=v"
      query_string = Enum.join(["table=foo" | attacker_params], "&")

      conn =
        Plug.Test.conn("GET", "/v1/shape?" <> query_string)
        |> Plug.Conn.fetch_query_params()

      attrs = Utils.common_open_telemetry_attrs(conn)

      query_param_keys =
        attrs
        |> Map.keys()
        |> Enum.filter(&(is_binary(&1) and String.starts_with?(&1, "http.query_param.")))

      assert attrs["http.query_param.table"] == "foo"
      refute Map.has_key?(attrs, "http.query_param.attacker_1")
      assert query_param_keys == ["http.query_param.table"]
    end
  end

  describe "CORSHeaderPlug" do
    test "reflects a single Origin and advertises Vary on origin and the credential" do
      conn =
        Plug.Test.conn("GET", "/v1/shape")
        |> Plug.Conn.put_req_header("origin", "https://a.example")

      conn = Utils.CORSHeaderPlug.call(conn, %{methods: ["GET"]})

      assert Plug.Conn.get_resp_header(conn, "access-control-allow-origin") == [
               "https://a.example"
             ]

      assert MapSet.member?(vary_fields(conn), "origin")
      assert MapSet.member?(vary_fields(conn), "authorization")
    end

    # A cacheable (`cache-control: public`) response carries per-principal rows when auth lives in a
    # header. Unless the response advertises the credential header in `Vary`, a shared cache keys on the
    # URL alone and serves one principal the other's cached rows. Two requests differing only by the
    # `authorization` header must both name it in `Vary` so a compliant cache keeps them apart.
    test "varies on the credential so a shared cache cannot serve one principal's rows to another" do
      base =
        Plug.Test.conn("GET", "/v1/shape")
        |> Plug.Conn.put_req_header("origin", "https://a.example")

      alice = Plug.Conn.put_req_header(base, "authorization", "Bearer alice")
      bob = Plug.Conn.put_req_header(base, "authorization", "Bearer bob")

      alice = Utils.CORSHeaderPlug.call(alice, %{methods: ["GET"]})
      bob = Utils.CORSHeaderPlug.call(bob, %{methods: ["GET"]})

      for cred_header <-
            ~w(authorization cookie x-api-key x-auth-token x-electric-secret electric-secret) do
        assert MapSet.member?(vary_fields(alice), cred_header)
        assert MapSet.member?(vary_fields(bob), cred_header)
      end
    end

    test "falls back to * when no Origin is present" do
      conn = Plug.Test.conn("GET", "/v1/shape")

      conn = Utils.CORSHeaderPlug.call(conn, %{methods: ["GET"]})

      assert Plug.Conn.get_resp_header(conn, "access-control-allow-origin") == ["*"]
    end

    test "does not crash when the request carries duplicate Origin headers" do
      conn = %{
        Plug.Test.conn("GET", "/v1/shape")
        | req_headers: [{"origin", "https://a.example"}, {"origin", "https://b.example"}]
      }

      conn = Utils.CORSHeaderPlug.call(conn, %{methods: ["GET"]})

      assert Plug.Conn.get_resp_header(conn, "access-control-allow-origin") == ["*"]
    end
  end

  describe "get_next_interval_timestamp/2" do
    test "returns expected interval" do
      long_poll_timeout_ms = 20000
      long_poll_timeout_sec = div(long_poll_timeout_ms, 1000)
      # Calculate the expected next interval
      now = DateTime.utc_now()
      oct9th2024 = DateTime.from_naive!(~N[2024-10-09 00:00:00], "Etc/UTC")
      diff_in_seconds = DateTime.diff(now, oct9th2024, :second)
      expected_interval = ceil(diff_in_seconds / long_poll_timeout_sec) * long_poll_timeout_sec

      assert Utils.get_next_interval_timestamp(long_poll_timeout_ms) ==
               expected_interval
    end

    test "returns expected inteval with different timeout" do
      long_poll_timeout_ms = 30000
      long_poll_timeout_sec = div(long_poll_timeout_ms, 1000)

      # Calculate the expected next interval
      now = DateTime.utc_now()
      oct9th2024 = DateTime.from_naive!(~N[2024-10-09 00:00:00], "Etc/UTC")
      diff_in_seconds = DateTime.diff(now, oct9th2024, :second)
      expected_interval = ceil(diff_in_seconds / long_poll_timeout_sec) * long_poll_timeout_sec

      assert Utils.get_next_interval_timestamp(long_poll_timeout_ms) ==
               expected_interval
    end

    test "returns expected interval with different timeout and cursor collision" do
      long_poll_timeout_ms = 30000
      long_poll_timeout_sec = div(long_poll_timeout_ms, 1000)

      # Calculate the expected next interval
      now = DateTime.utc_now()
      oct9th2024 = DateTime.from_naive!(~N[2024-10-09 00:00:00], "Etc/UTC")
      diff_in_seconds = DateTime.diff(now, oct9th2024, :second)
      expected_interval = ceil(diff_in_seconds / long_poll_timeout_sec) * long_poll_timeout_sec

      assert Utils.get_next_interval_timestamp(
               long_poll_timeout_ms,
               "#{expected_interval}"
             ) != expected_interval
    end

    test "never returns a cursor smaller than prev_interval" do
      # Reproduces a 2-cycle observed in production: when a client polls with
      # a jittered cursor (bucket + delta) and the wall clock has not yet
      # crossed that jittered value, the function used to return the plain
      # bucket value — i.e. a cursor smaller than the one the client sent.
      # Combined with `Cache-Control: public, max-age=<sse_timeout-1>` on the
      # response, two cached entries would end up pointing at each other and
      # the client would bounce between them at line rate.
      long_poll_timeout_ms = 20_000
      long_poll_timeout_sec = div(long_poll_timeout_ms, 1000)

      now = DateTime.utc_now()
      oct9th2024 = DateTime.from_naive!(~N[2024-10-09 00:00:00], "Etc/UTC")
      diff_in_seconds = DateTime.diff(now, oct9th2024, :second)
      current_bucket = ceil(diff_in_seconds / long_poll_timeout_sec) * long_poll_timeout_sec

      # Simulate a jittered prev cursor that sits ahead of the current bucket
      # for every possible delta the jitter branch can produce (1..3_600).
      for delta <- 1..3_600 do
        prev = current_bucket + delta

        result =
          Utils.get_next_interval_timestamp(long_poll_timeout_ms, "#{prev}")

        assert result > prev,
               "cursor went backward: prev=#{prev}, result=#{result}, " <>
                 "current_bucket=#{current_bucket}"
      end
    end

    test "keeps moving the cursor when the timeout is below the bucket resolution" do
      # A sub-second timeout has no whole-second bucket to derive an interval from, and the cursor was
      # a constant there. A client handed back the value it just sent reads the response as a replay of
      # the previous one, so the guarantee above has to survive a timeout an operator can set without
      # touching code.
      for long_poll_timeout_ms <- [0, 1, 999] do
        assert Utils.get_next_interval_timestamp(long_poll_timeout_ms, "500") > 500
        assert Utils.get_next_interval_timestamp(long_poll_timeout_ms, nil) > 0
      end
    end
  end

  defp vary_fields(conn) do
    conn
    |> Plug.Conn.get_resp_header("vary")
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.map(&(&1 |> String.trim() |> String.downcase()))
    |> MapSet.new()
  end
end
