defmodule Electric.ConfigTest do
  use ExUnit.Case, async: false

  doctest Electric.Config, import: true

  describe "parse_dashboard_listen_address!/2" do
    test "defaults to IPv4 loopback so an enabled dashboard is not exposed on a public interface" do
      assert Electric.Config.parse_dashboard_listen_address!(nil, false) == {127, 0, 0, 1}
    end

    test "defaults to IPv6 loopback when IPv6 is selected" do
      assert Electric.Config.parse_dashboard_listen_address!(nil, true) ==
               {0, 0, 0, 0, 0, 0, 0, 1}
    end

    test "treats a blank address as unset and falls back to loopback" do
      assert Electric.Config.parse_dashboard_listen_address!("", false) == {127, 0, 0, 1}
    end

    test "binds to an explicit address only when one is provided" do
      assert Electric.Config.parse_dashboard_listen_address!("0.0.0.0", false) == {0, 0, 0, 0}
      assert Electric.Config.parse_dashboard_listen_address!("10.0.0.5", false) == {10, 0, 0, 5}
    end

    test "raises on an unparseable address rather than silently binding a default" do
      assert_raise Dotenvy.Error, fn ->
        Electric.Config.parse_dashboard_listen_address!("not-an-ip", false)
      end
    end
  end

  describe "validate_security_config!/2" do
    test "raises error when neither secret nor insecure mode are set" do
      assert_raise RuntimeError,
                   ~r/You must set ELECTRIC_SECRET unless ELECTRIC_INSECURE=true. Setting ELECTRIC_INSECURE=true risks exposing your database, only use insecure mode in development or you've otherwise secured the Electric API/,
                   fn ->
                     Electric.Config.validate_security_config!(nil, false)
                   end
    end

    test "raises error when both secret and insecure mode are set" do
      assert_raise RuntimeError,
                   ~r/You cannot set both ELECTRIC_SECRET and ELECTRIC_INSECURE=true/,
                   fn ->
                     Electric.Config.validate_security_config!("secret", true)
                   end
    end

    test "accepts valid secure configuration" do
      assert :ok = Electric.Config.validate_security_config!("secret", false)
    end

    test "accepts valid insecure configuration" do
      assert :ok = Electric.Config.validate_security_config!(nil, true)
    end
  end

  test "runtime config wires an optional positive subset snapshot row cap" do
    read_config = runtime_option("ELECTRIC_SUBSET_MAX_ROWS", :subset_max_rows)

    assert read_config.("2") == 2
    assert %{subset_max_rows: 2} = Electric.Application.api(subset_max_rows: 2)
    assert read_config.(nil) == nil

    # The value runtime.exs read reaches the API without an override.
    original = Application.fetch_env(:electric, :subset_max_rows)

    on_exit(fn ->
      case original do
        {:ok, value} -> Application.put_env(:electric, :subset_max_rows, value)
        :error -> Application.delete_env(:electric, :subset_max_rows)
      end
    end)

    Application.put_env(:electric, :subset_max_rows, 3)
    assert %{subset_max_rows: 3} = Electric.Application.api([])

    for invalid <- ["0", "-1", "1.5", "9223372036854775807"] do
      assert_raise RuntimeError, ~r/ELECTRIC_SUBSET_MAX_ROWS/, fn -> read_config.(invalid) end
    end
  end

  defp runtime_option(variable, key) do
    original = System.get_env(variable)
    on_exit(fn -> set_env(variable, original) end)

    fn value ->
      set_env(variable, value)
      config = Config.Reader.read!("config/runtime.exs", env: :test, target: Mix.target())
      get_in(config, [:electric, key])
    end
  end

  defp set_env(key, nil), do: System.delete_env(key)
  defp set_env(key, value), do: System.put_env(key, value)

  describe "defaults" do
    # want to know what happens without our runtime environment
    # if any of the configuration options are missing defaults
    # then these configuration functions will raise
    setup do
      initial_config = Application.get_all_env(:electric)

      for {key, _} <- initial_config do
        Application.delete_env(:electric, key)
      end

      on_exit(fn ->
        Application.put_all_env([{:electric, initial_config}])
      end)

      [initial_config: initial_config]
    end

    test "api_server/0" do
      Electric.Application.api_server()
    end

    test "api_server/1 maps tcp_read_timeout to a top-level ThousandIsland option" do
      [{Bandit, bandit_opts}] = Electric.Application.api_server(tcp_read_timeout: 180_000)

      assert bandit_opts[:thousand_island_options][:read_timeout] == 180_000

      [{Bandit, default_opts}] = Electric.Application.api_server([])
      refute Keyword.has_key?(default_opts[:thousand_island_options], :read_timeout)
    end

    test "api_server/1 applies conn_max_requests to HTTP/1 connections only" do
      [{Bandit, bandit_opts}] = Electric.Application.api_server(tweaks: [conn_max_requests: 50])

      assert bandit_opts[:http_1_options][:max_requests] == 50
      refute Keyword.has_key?(bandit_opts[:http_2_options], :max_requests)

      [{Bandit, default_opts}] = Electric.Application.api_server([])
      refute Keyword.has_key?(default_opts[:http_1_options], :max_requests)
      refute Keyword.has_key?(default_opts[:http_2_options], :max_requests)
    end

    test "api_server/1 applies max_request_line_length to HTTP/1 connections" do
      [{Bandit, bandit_opts}] =
        Electric.Application.api_server(tweaks: [max_request_line_length: 65_536])

      assert bandit_opts[:http_1_options][:max_request_line_length] == 65_536

      [{Bandit, default_opts}] = Electric.Application.api_server([])
      refute Keyword.has_key?(default_opts[:http_1_options], :max_request_line_length)
    end

    @tag :request_value_redaction
    test "api_server/1 suppresses Bandit's formatted exception log when redacting request values" do
      [{Bandit, default_opts}] = Electric.Application.api_server([])
      refute Keyword.has_key?(default_opts, :http_options)

      Application.put_env(:electric, :telemetry_redact_request_values?, true)
      on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)
      [{Bandit, bandit_opts}] = Electric.Application.api_server([])
      assert bandit_opts[:http_options][:log_exceptions_with_status_codes] == []
    end

    test "api_server/1 limits the HTTP/2 reset-stream rate by default" do
      [{Bandit, default_opts}] = Electric.Application.api_server([])

      assert Keyword.fetch!(default_opts[:http_2_options], :max_reset_stream_rate) ==
               {10_000, 10_000}
    end

    test "api_server/1 applies http2_max_reset_stream_rate when configured" do
      [{Bandit, bandit_opts}] =
        Electric.Application.api_server(tweaks: [http2_max_reset_stream_rate: {500, 10_000}])

      assert bandit_opts[:http_2_options][:max_reset_stream_rate] == {500, 10_000}

      [{Bandit, disabled_opts}] =
        Electric.Application.api_server(tweaks: [http2_max_reset_stream_rate: :disabled])

      assert Keyword.fetch!(disabled_opts[:http_2_options], :max_reset_stream_rate) == nil
    end

    test "configuration/1", ctx do
      Electric.Application.configuration(
        Keyword.take(ctx.initial_config, [:replication_connection_opts])
      )
    end

    test "configuration/1 exposes the curated stack metrics to Prometheus", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert config[:telemetry_opts][:additional_prometheus_metrics] ==
               Electric.StackSupervisor.Telemetry.prometheus_metrics()
    end

    test "api/0" do
      Electric.Application.api()
    end

    test "api_plug_opts/0" do
      Electric.Application.api_plug_opts()
    end
  end

  describe "parse_top_process_limit/1" do
    import Electric.Config, only: [parse_top_process_limit: 1]

    test "parses count:<N>" do
      assert {:ok, {:count, 5}} = parse_top_process_limit("count:5")
      assert {:ok, {:count, 100}} = parse_top_process_limit("count:100")
    end

    test "parses mem_percent:<N>" do
      assert {:ok, {:mem_percent, 1}} = parse_top_process_limit("mem_percent:1")
      assert {:ok, {:mem_percent, 50}} = parse_top_process_limit("mem_percent:50")
      assert {:ok, {:mem_percent, 100}} = parse_top_process_limit("mem_percent:100")
    end

    test "rejects count:0 and negative values" do
      assert {:error, "count value must be a positive integer, got: 0"} =
               parse_top_process_limit("count:0")

      assert {:error, "count value must be a positive integer, got: -1"} =
               parse_top_process_limit("count:-1")
    end

    test "rejects mem_percent out of 1..100 range" do
      assert {:error, "mem_percent value must be between 1 and 100, got: 0"} =
               parse_top_process_limit("mem_percent:0")

      assert {:error, "mem_percent value must be between 1 and 100, got: 101"} =
               parse_top_process_limit("mem_percent:101")
    end

    test "rejects non-integer values" do
      assert {:error, "count value must be a positive integer, got: abc"} =
               parse_top_process_limit("count:abc")

      assert {:error, "mem_percent value must be between 1 and 100, got: 3.5"} =
               parse_top_process_limit("mem_percent:3.5")
    end

    test "rejects unknown format" do
      assert {:error, msg} = parse_top_process_limit("foo")
      assert msg =~ "invalid top process limit"
      assert msg =~ "Expected format: count:<N> or mem_percent:<N>"
    end
  end

  describe "parse_http2_max_reset_stream_rate/1" do
    import Electric.Config, only: [parse_http2_max_reset_stream_rate: 1]

    test "parses <count>/<duration>" do
      assert {:ok, {500, 10_000}} = parse_http2_max_reset_stream_rate("500/10s")
      assert {:ok, {50, 1_000}} = parse_http2_max_reset_stream_rate("50/1000ms")
      assert {:ok, {1, 60_000}} = parse_http2_max_reset_stream_rate("1/1m")
    end

    test "accepts disabled" do
      assert {:ok, :disabled} = parse_http2_max_reset_stream_rate("disabled")
      assert {:ok, :disabled} = parse_http2_max_reset_stream_rate("DISABLED")
    end

    test "rejects invalid input" do
      for input <- ["", "500", "0/10s", "-5/10s", "abc/10s", "500/abc", "500/0s", "500/10s/1"] do
        assert {:error, msg} = parse_http2_max_reset_stream_rate(input)
        assert msg =~ "invalid HTTP/2 reset stream rate"
      end
    end
  end

  describe "validations" do
    test "configuring deprecated FileStorage raises" do
      assert_raise RuntimeError, ~r/FileStorage storage is deprecated/, fn ->
        Electric.Application.configuration(
          storage: {Electric.ShapeCache.FileStorage, storage_dir: "./persistent"}
        )
      end
    end
  end

  describe "tweaks propagation" do
    setup do
      initial_config = Application.get_all_env(:electric)

      for {key, _} <- initial_config do
        Application.delete_env(:electric, key)
      end

      on_exit(fn ->
        Application.put_all_env([{:electric, initial_config}])
      end)

      [initial_config: initial_config]
    end

    test "configuration/1 is accepted by the StackSupervisor options schema", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert {:ok, _} =
               NimbleOptions.validate(Map.new(config), Electric.StackSupervisor.opts_schema())
    end

    test "consumer_gc_heap_threshold opt is threaded into tweaks", ctx do
      threshold = 209_715_200

      config =
        Electric.Application.configuration(
          Keyword.merge(
            Keyword.take(ctx.initial_config, [:replication_connection_opts]),
            consumer_gc_heap_threshold: threshold
          )
        )

      assert Keyword.fetch!(config[:tweaks], :consumer_gc_heap_threshold) == threshold
    end

    test "subquery_buffer_max_transactions set in the app env reaches the tweaks", ctx do
      # What config/runtime.exs does with ELECTRIC_SUBQUERY_BUFFER_MAX_TRANSACTIONS.
      Application.put_env(:electric, :subquery_buffer_max_transactions, 3)

      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert Keyword.fetch!(config[:tweaks], :subquery_buffer_max_transactions) == 3
    end

    test "subquery_buffer_max_transactions defaults to 1000 in tweaks", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert Keyword.fetch!(config[:tweaks], :subquery_buffer_max_transactions) == 1000
    end

    test "consumer_gc_heap_threshold defaults to nil in tweaks", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert Keyword.fetch!(config[:tweaks], :consumer_gc_heap_threshold) == nil
    end

    test "txn_buffer_max_changes opt is threaded into tweaks", ctx do
      config =
        Electric.Application.configuration(
          Keyword.merge(
            Keyword.take(ctx.initial_config, [:replication_connection_opts]),
            txn_buffer_max_changes: 10
          )
        )

      assert Keyword.fetch!(config[:tweaks], :txn_buffer_max_changes) == 10
    end

    test "txn_buffer_max_changes has a default in tweaks", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert Keyword.fetch!(config[:tweaks], :txn_buffer_max_changes) == 250_000
    end
  end

  describe "transaction size cap" do
    setup do
      initial_config = Application.get_all_env(:electric)

      for {key, _} <- initial_config do
        Application.delete_env(:electric, key)
      end

      on_exit(fn ->
        Application.put_all_env([{:electric, initial_config}])
      end)

      [initial_config: initial_config]
    end

    # Going over the cap drops the replication slot and purges every shape, so a large
    # transaction must not trip it unless an operator asked for one.
    test "is off by default", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert Keyword.fetch!(config[:replication_opts], :max_txn_size) == nil
    end

    test "is passed through when set", ctx do
      config =
        Electric.Application.configuration(
          Keyword.merge(
            Keyword.take(ctx.initial_config, [:replication_connection_opts]),
            max_txn_size: 5000
          )
        )

      assert Keyword.fetch!(config[:replication_opts], :max_txn_size) == 5000
    end
  end

  describe "disk usage scan period" do
    setup do
      initial_config = Application.get_all_env(:electric)

      for {key, _} <- initial_config do
        Application.delete_env(:electric, key)
      end

      on_exit(fn ->
        Application.put_all_env([{:electric, initial_config}])
      end)

      [initial_config: initial_config]
    end

    test "is threaded into the telemetry options", ctx do
      config =
        Electric.Application.configuration(
          Keyword.merge(
            Keyword.take(ctx.initial_config, [:replication_connection_opts]),
            disk_usage_scan_period: 0
          )
        )

      assert Keyword.fetch!(
               config[:telemetry_opts][:intervals_and_thresholds],
               :disk_usage_scan_period
             ) ==
               0
    end
  end

  describe "replication heartbeat" do
    setup do
      initial_config = Application.get_all_env(:electric)

      for {key, _} <- initial_config do
        Application.delete_env(:electric, key)
      end

      on_exit(fn ->
        Application.put_all_env([{:electric, initial_config}])
      end)

      [initial_config: initial_config]
    end

    test "is off by default", ctx do
      config =
        Electric.Application.configuration(
          Keyword.take(ctx.initial_config, [:replication_connection_opts])
        )

      assert Keyword.fetch!(config[:replication_opts], :replication_heartbeat_interval) == 0
    end

    test "reaches the replication options", ctx do
      config =
        Electric.Application.configuration(
          Keyword.merge(
            Keyword.take(ctx.initial_config, [:replication_connection_opts]),
            replication_idle_timeout: 600_000,
            replication_heartbeat_interval: 150_000
          )
        )

      assert Keyword.fetch!(config[:replication_opts], :replication_heartbeat_interval) ==
               150_000
    end

    test "refuses an interval over half the idle timeout", ctx do
      assert_raise RuntimeError, ~r/ELECTRIC_REPLICATION_HEARTBEAT_INTERVAL/, fn ->
        Electric.Application.configuration(
          Keyword.merge(
            Keyword.take(ctx.initial_config, [:replication_connection_opts]),
            replication_idle_timeout: 600_000,
            replication_heartbeat_interval: 300_001
          )
        )
      end
    end
  end

  describe "parse_insecure!/1" do
    test "resolves recognised and unrecognised non-true values to secure" do
      for value <- ["no", "No", "0", "off", "false", "", "  ", "disabled", "garbage"] do
        refute Electric.Config.parse_insecure!(value), "expected #{inspect(value)} to be secure"
      end
    end

    test "resolves explicit true spellings to insecure" do
      for value <- ["true", "TRUE", "True", " true ", "1", "yes", "on"] do
        assert Electric.Config.parse_insecure!(value), "expected #{inspect(value)} to be insecure"
      end
    end
  end

  describe "parse_postgresql_uri/1 sslmode" do
    test "accepts verify-ca and verify-full" do
      assert {:ok, opts} =
               Electric.Config.parse_postgresql_uri(
                 "postgres://user@localhost/db?sslmode=verify-full"
               )

      assert opts[:sslmode] == :"verify-full"

      assert {:ok, opts} =
               Electric.Config.parse_postgresql_uri(
                 "postgres://user@localhost/db?sslmode=verify-ca"
               )

      assert opts[:sslmode] == :"verify-ca"
    end

    test "names each ignored query option in a warning, and never its value" do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, opts} =
                   Electric.Config.parse_postgresql_uri(
                     "postgres://user@localhost/db?sslmod=require&password=hunter2"
                   )

          refute Keyword.has_key?(opts, :sslmode)
        end)

      assert log =~ "[warning]"
      assert log =~ ~s("sslmod")
      assert log =~ ~s("password")
      refute log =~ "hunter2"
      refute log =~ "require"
    end

    test "does not warn when sslmode is the only query option" do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, [_ | _]} =
                   Electric.Config.parse_postgresql_uri(
                     "postgres://user@localhost/db?sslmode=require"
                   )
        end)

      refute log =~ "Ignoring unsupported options"
    end

    test "still rejects an unknown sslmode" do
      assert {:error, message} =
               Electric.Config.parse_postgresql_uri(
                 "postgres://user@localhost/db?sslmode=yesplease"
               )

      assert message =~ ~s(invalid "sslmode" value)
    end
  end
end
