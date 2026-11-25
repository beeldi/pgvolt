defmodule Electric.Config.Defaults do
  @moduledoc false

  # we want the default storage and kv implementations to honour the
  # `:storage_dir` configuration setting so we need to use runtime-evaluated
  # functions to get them. Since you can't embed anoymous functions these
  # functions are used instead.

  @doc false
  def storage(opts \\ []) do
    storage_dir = Keyword.get_lazy(opts, :storage_dir, fn -> storage_dir("shapes") end)
    {Electric.ShapeCache.PureFileStorage, storage_dir: storage_dir}
  end

  @doc false
  def persistent_kv(opts \\ []) do
    storage_dir = Keyword.get_lazy(opts, :storage_dir, fn -> storage_dir("state") end)
    {Electric.PersistentKV.Filesystem, :new!, root: storage_dir}
  end

  defp storage_dir(sub_dir) do
    Path.join(storage_dir(), sub_dir)
  end

  defp storage_dir do
    Electric.Config.get_env(:storage_dir)
  end

  def process_registry_partitions do
    System.schedulers_online()
  end
end

defmodule Electric.Config do
  require Logger

  @type instance_id :: String.t()

  @known_feature_flags ~w[full_xid_source_positions]
  @default_storage_dir "./persistent"

  @defaults [
    ## Database
    provided_database_id: "single_stack",
    db_pool_size: 20,
    # TCP-level liveness detection for database connections. All nil by
    # default, leaving the OS defaults in place. See
    # Electric.Connection.Manager.ConnectionResolver for what these do.
    db_tcp_keepalive_idle: nil,
    db_tcp_keepalive_interval: nil,
    db_tcp_keepalive_count: nil,
    db_tcp_user_timeout: nil,
    replication_stream_id: "default",
    replication_slot_temporary?: false,
    replication_slot_temporary_random_name?: false,
    # No cap: transactions reach the shapes in fragments of `max_batch_size` changes, and
    # going over a cap drops the slot and purges every shape. Shapes that hold changes in
    # memory have their own limit, `txn_buffer_max_changes`.
    max_txn_size: nil,
    max_batch_size: 100,
    # Scaling down on idle is disabled by default
    replication_idle_timeout: 0,
    # Replication heartbeats are disabled by default
    replication_heartbeat_interval: 0,
    manual_table_publishing?: false,
    ## HTTP API
    # set enable_http_api: false to turn off the HTTP server totally
    enable_http_api: true,
    long_poll_timeout: 20_000,
    http_api_num_acceptors: nil,
    tcp_send_timeout: :timer.seconds(30),
    # Socket read/keep-alive timeout (ThousandIsland's read_timeout). Bandit reaps
    # keep-alive connections that have been idle for this long. When Electric runs
    # behind a connection-pooling proxy (ALB, nginx, etc.), this MUST exceed the
    # proxy's idle timeout — otherwise the proxy races Bandit's unannounced close
    # when reusing a pooled connection and surfaces the reset as a 502.
    # nil keeps ThousandIsland's default (60s).
    tcp_read_timeout: nil,
    cache_max_age: 60,
    cache_stale_age: 60 * 5,
    chunk_bytes_threshold: Electric.ShapeCache.LogChunker.default_chunk_size_threshold(),
    compaction_period: :timer.minutes(10),
    allow_shape_deletion?: false,
    service_port: 3000,
    listen_on_ipv6?: false,
    stack_ready_timeout: 5_000,
    send_cache_headers?: true,
    max_shapes: nil,
    consumer_partitions: nil,
    # This value should be tuned for the hardware it's running on.
    max_concurrent_requests: %{initial: 300, existing: 10_000},
    ## Storage
    storage_dir: @default_storage_dir,
    storage: &Electric.Config.Defaults.storage/0,
    persistent_kv: &Electric.Config.Defaults.persistent_kv/0,
    cleanup_interval_ms: 10_000,
    ## Telemetry
    instance_id: nil,
    prometheus_port: nil,
    telemetry_statsd_host: nil,
    telemetry_redact_request_values?: false,
    otel_sampling_ratio: 0.01,
    otel_trusted_sample_rates: nil,
    metrics_sampling_ratio: 1,
    # When true, the OTel spans of empty/up-to-date shape-GET responses are tail-dropped
    # at export time to cut trace volume. See Electric.Telemetry.EmptyResponseSampler.
    drop_empty_response_spans?: false,
    ## Memory
    # After this duration of inactivity, consumer processes will hibernate
    # to allow garbage collection
    shape_hibernate_after: :timer.seconds(30),
    # If enabled, terminate (suspend) consumer processes after hibernating.
    # This frees memory more aggressively than hibernation alone.
    shape_enable_suspend?: false,
    # After hibernating, wait this duration before suspending (terminating).
    # Only applies when shape_enable_suspend? is true.
    shape_suspend_after: :timer.minutes(10),
    # How long a pending flush entry may sit without flush progress before its
    # shape is invalidated to unpin the stack-wide flush boundary. The storage
    # contract already says writes slower than this should raise.
    flush_stall_grace_period: :timer.minutes(1),
    flush_stall_boot_grace: :timer.minutes(10),
    # How many consecutive stall challenges a deliberately deferring writer may
    # answer before its word stops being taken. A shape buffering ahead of a
    # snapshot that never completes would otherwise pin the stack-wide flush
    # boundary — and the replication slot ack — indefinitely while reporting
    # itself healthy on every challenge.
    flush_deferral_round_limit: 10,
    # How long a scatter-gather may wait on router-worker acks before the collector
    # crashes to re-stream from the slot instead of wedging silently. Generous on
    # purpose: the gather spans consumer publishes, and the first fragment after a
    # restart cold-starts every affected consumer serially, so with a large
    # shape count a healthy post-boot gather can take minutes.
    gather_stall_timeout: :timer.minutes(10),
    # How long a broadcast waits for a consumer's reply before killing it as
    # unresponsive; the DOWN classifies it as crashed and the shape is invalidated
    # rather than pinning the pipeline. Must stay well below gather_stall_timeout.
    broadcast_reply_grace: :timer.minutes(1),
    # Sets max_requests for Bandit HTTP/1 handler processes:
    # https://hexdocs.pm/bandit/Bandit.html#t:http_1_options/0
    # "The maximum number of requests to serve in a single HTTP/1
    # connection before closing the connection"
    conn_max_requests: 50,
    # Longest HTTP/1 request line Bandit accepts, in bytes; nil keeps Bandit's 10,000. A shape
    # whose `where` or `params` make the URL longer gets a 414.
    # https://hexdocs.pm/bandit/Bandit.html#t:http_1_options/0
    max_request_line_length: nil,
    # Sets fullsweep_after for Bandit handler processes.
    # Bandit reuses handler processes across requests (capped by conn_max_requests).
    # These processes can accumulate garbage on the old heap since BEAM's generational
    # GC only does fullsweep after a certain number of minor collections. This setting
    # forces a fullsweep after N minor collections to reclaim that memory.
    # See https://www.erlang.org/doc/apps/erts/erlang.html#spawn_opt/4
    handler_fullsweep_after: nil,
    # Sets max_reset_stream_rate for Bandit HTTP/2 connections: the number of client
    # RST_STREAM frames tolerated per connection within a time window before Bandit
    # tears down the whole connection with ENHANCE_YOUR_CALM. Bandit defaults to
    # {500, 10_000}. Higher here: tearing down a connection kills every in-flight
    # stream on it, and behind a proxy that multiplexes many end clients onto a few
    # upstream connections, ordinary client-side cancellations of long-polls count
    # against the limit.
    http2_max_reset_stream_rate: {10_000, 10_000},
    ## Performance tweaks
    publication_alter_debounce_ms: 0,
    # allow for configuring per-process `Process.spawn_opt()`. In the form
    # %{process_id :: atom() => [Process.spawn_opt()]}
    # See `Process.flag/2`
    #
    # e.g. %{shape_log_collector: [min_heap_size: 1024 * 1024, min_bin_vheap_size: 1024 * 1024]}
    process_spawn_opts: %{},
    # Heap-size threshold (in BYTES) above which a consumer runs :erlang.garbage_collect()
    # after processing a transaction fragment.
    consumer_gc_heap_threshold: nil,
    # Terminate a shape response serve when a single socket write makes no
    # progress for this long (ms). Catches serves to clients that stop
    # accepting data, which otherwise pin memory and a file descriptor
    # indefinitely. 0 disables reaping.
    stalled_serve_timeout: :timer.seconds(60),
    ## Misc
    process_registry_partitions: &Electric.Config.Defaults.process_registry_partitions/0,
    feature_flags: [],
    publication_refresh_period: 60_000,
    schema_reconciler_period: 60_000,
    snapshot_timeout_to_first_data: :timer.seconds(30),
    shape_db_exclusive_mode: false,
    shape_db_storage_dir: @default_storage_dir,
    # TODO: fix defaults to synchronous=NORMAL and shape_db_cache_size=2048
    shape_db_synchronous:
      Electric.ShapeCache.ShapeStatus.ShapeDb.Connection.default!(:synchronous),
    shape_db_cache_size: Electric.ShapeCache.ShapeStatus.ShapeDb.Connection.default!(:cache_size),
    shape_db_boot_read_timeout: 300_000,
    exclude_spans: MapSet.new(),
    live_dashboard_port: nil,
    subquery_buffer_max_transactions: 1000,
    subset_max_rows: nil,
    # Changes a shape may hold in memory: those of one transaction before its commit, for a
    # shape writing whole transactions (a subquery shape or one of its inner shapes), and
    # those it buffers while waiting for its snapshot, for any shape. Past it the shape is
    # removed, as on a truncate. nil means no limit.
    txn_buffer_max_changes: 250_000,
    router_shards: 1,
    replication_pipeline_window: 1,
    shared_conversion: false,
    shared_payload_arena: false,
    # A boot reclaims the payload arena only once earlier generations hold this much: the reclaim
    # scans the log directory of every stored shape before the stack can go active.
    payload_arena_reclaim_min_bytes: 1024 * 1024 * 1024,
    worker_prepare: false,
    shared_fragment_blocks: false,
    ack_after_journal: false,
    keep_shapes_on_promotion: false,
    ingest_priority_high: false,
    filter_routing_memo: false
  ]

  @installation_id_key "electric_installation_id"

  def default(key) do
    case Keyword.fetch!(@defaults, key) do
      fun when is_function(fun, 0) -> fun.()
      value -> value
    end
  end

  @doc false
  @spec ensure_instance_id() :: instance_id()
  # the instance id needs to be consistent across calls, so we do need to have
  # a value in the config, even if it's not configured by the user.
  def ensure_instance_id do
    case Application.get_env(:electric, :instance_id) do
      nil ->
        instance_id = generate_instance_id()

        Logger.notice("Setting electric instance_id: #{instance_id}")
        Application.put_env(:electric, :instance_id, instance_id)

        instance_id

      id when is_binary(id) ->
        id
    end
  end

  defp generate_instance_id do
    Electric.Utils.uuid4()
  end

  # the installation id is persisted to disk to remain the same between restarts of the sync service
  @spec persist_installation_id(term, binary) :: instance_id()
  def persist_installation_id(persistent_kv, instance_id) when is_binary(instance_id) do
    case Electric.PersistentKV.get(persistent_kv, @installation_id_key) do
      {:ok, id} when is_binary(id) ->
        id

      {:error, :not_found} ->
        :ok = Electric.PersistentKV.set(persistent_kv, @installation_id_key, instance_id)
        instance_id

      {:error, reason} ->
        Logger.warning(
          "Could not read the stored installation id (#{inspect(reason)}), generating a new one."
        )

        :ok = Electric.PersistentKV.set(persistent_kv, @installation_id_key, instance_id)
        instance_id
    end
  end

  @spec installation_id!(term) :: binary | no_return
  def installation_id!(kv) do
    case Electric.PersistentKV.get(kv, @installation_id_key) do
      {:ok, id} when is_binary(id) -> id
      {:error, :not_found} -> raise "Electric's installation_id not set"
      {:error, reason} -> raise "Electric's installation_id is unreadable: #{inspect(reason)}"
    end
  end

  @doc """
  The minimum allowed time before Electric can close database connections due to the
  replication stream inactivity.

  This is to prevent churn where connection and replication supervisors would restart too frequently.

  The scale-to-zero feature of managed providers like Neon takes on the order of minutes before
  deciding that an idle database can be scaled down.
  """
  @spec min_replication_idle_timeout() :: pos_integer
  def min_replication_idle_timeout, do: 30_000

  def min_replication_idle_timeout_in_seconds, do: div(min_replication_idle_timeout(), 1000)

  @spec get_env(Application.key()) :: Application.value()
  def get_env(key) do
    # handle the case where the config value was set in runtime.exs but to
    # `nil` because of a missing env var. This allows us to just use `nil`
    # as the default config values in runtime.exs so avoiding hard-coding
    # defaults all over the place.
    case Application.get_env(:electric, key) do
      nil -> default(key)
      value -> value
    end
  end

  @doc """
  `get_env/1` with a caller-supplied override, for settings a stack can be started with.

  `nil` means "not overridden" and falls through; an explicit `false` wins, which a
  `||` chain cannot express.
  """
  @spec get_env(Application.key(), Application.value()) :: Application.value()
  def get_env(key, nil), do: get_env(key)
  def get_env(_key, override), do: override

  def get_env_lazy(key, fun) when is_function(fun, 0) do
    case Application.fetch_env(:electric, key) do
      {:ok, nil} -> fun.()
      {:ok, value} -> value
      :error -> fun.()
    end
  end

  @spec fetch_env!(Application.key()) :: Application.value()
  def fetch_env!(key) do
    Application.fetch_env!(:electric, key)
  end

  def persistent_kv do
    with {m, f, a} <- get_env(:persistent_kv) do
      apply(m, f, [a])
    end
  end

  @doc ~S"""
  Parse a PostgreSQL URI into a keyword list.

  ## Examples

      iex> parse_postgresql_uri("postgresql://postgres:password@example.com/app-db") |> deobfuscate()
      {:ok, [
        hostname: "example.com",
        port: 5432,
        database: "app-db",
        username: "postgres",
        password: "password",
      ]}

      iex> parse_postgresql_uri("postgresql://electric@192.168.111.33:81/__shadow")
      {:ok, [
        hostname: "192.168.111.33",
        port: 81,
        database: "__shadow",
        username: "electric"
      ]}

      iex> parse_postgresql_uri("postgresql://pg@[2001:db8::1234]:4321")
      {:ok, [
        hostname: "2001:db8::1234",
        port: 4321,
        database: "pg",
        username: "pg"
      ]}

      iex> parse_postgresql_uri("postgresql://user@localhost:5433/")
      {:ok, [
        hostname: "localhost",
        port: 5433,
        database: "user",
        username: "user"
      ]}

      iex> parse_postgresql_uri("postgresql://user%2Btesting%40gmail.com:weird%2Fpassword@localhost:5433/my%2Bdb%2Bname") |> deobfuscate()
      {:ok, [
        hostname: "localhost",
        port: 5433,
        database: "my+db+name",
        username: "user+testing@gmail.com",
        password: "weird/password"
      ]}

      iex> parse_postgresql_uri("postgres://super_user@localhost:7801/postgres?sslmode=disable")
      {:ok, [
        hostname: "localhost",
        port: 7801,
        database: "postgres",
        username: "super_user",
        sslmode: :disable
      ]}

      iex> parse_postgresql_uri("postgres://super_user@localhost:7801/postgres?sslmode=require")
      {:ok, [
        hostname: "localhost",
        port: 7801,
        database: "postgres",
        username: "super_user",
        sslmode: :require
      ]}

      iex> parse_postgresql_uri("postgres://super_user@localhost:7801/postgres?sslmode=yesplease")
      {:error, "invalid \"sslmode\" value: \"yesplease\""}

      iex> parse_postgresql_uri("postgrex://localhost")
      {:error, "invalid URL scheme: \"postgrex\""}

      iex> parse_postgresql_uri("postgresql://localhost")
      {:error, "invalid or missing username"}

      iex> parse_postgresql_uri("postgresql://:@localhost")
      {:error, "invalid or missing username"}

      iex> parse_postgresql_uri("postgresql://:password@localhost")
      {:error, "invalid or missing username"}

      iex> parse_postgresql_uri("postgresql://user:password")
      {:error, "invalid or missing username"}

      iex> parse_postgresql_uri("postgresql://user:password@")
      {:error, "missing host"}

      iex> parse_postgresql_uri("postgresql://user@localhost:5433/mydb?opts=-c%20synchronous_commit%3Doff&foo=bar")
      {:ok, [
        hostname: "localhost",
        port: 5433,
        database: "mydb",
        username: "user"
      ]}

      iex> parse_postgresql_uri("postgres://user:pass@localhost:5432/db?uselibpqcompat=true") |> deobfuscate()
      {:ok, [
        hostname: "localhost",
        port: 5432,
        database: "db",
        username: "user",
        password: "pass"
      ]}

      iex> parse_postgresql_uri("postgres://user:pass@localhost:5432/db?uselibpqcompat=true&sslmode=require") |> deobfuscate()
      {:ok, [
        hostname: "localhost",
        port: 5432,
        database: "db",
        username: "user",
        password: "pass",
        sslmode: :require
      ]}

      iex> parse_postgresql_uri("postgresql://electric@localhost/db?replication=database")
      {:error, "unsupported \"replication\" query option. Electric opens both a replication connection and regular connections to Postgres as needed"}

      iex> parse_postgresql_uri("postgresql://electric@localhost/db?replication=off")
      {:error, "unsupported \"replication\" query option. Electric opens both a replication connection and regular connections to Postgres as needed"}
  """
  @spec parse_postgresql_uri(binary) :: {:ok, keyword} | {:error, binary}
  def parse_postgresql_uri(uri_str) do
    %URI{scheme: scheme, host: host, port: port, path: path, userinfo: userinfo, query: query} =
      URI.parse(uri_str)

    with :ok <- validate_url_scheme(scheme),
         :ok <- validate_url_host(host),
         {:ok, {username, password}} <- parse_url_userinfo(userinfo),
         {:ok, options} <- parse_url_query(query) do
      conn_params =
        Enum.reject(
          [
            hostname: host,
            port: port || 5432,
            database: parse_database(path, username) |> URI.decode(),
            username: URI.decode(username),
            password: if(password, do: password |> URI.decode() |> Electric.Utils.wrap_in_fun())
          ] ++ options,
          fn {_key, val} -> is_nil(val) end
        )

      {:ok, conn_params}
    end
  end

  def parse_postgresql_uri!(uri_str) do
    case parse_postgresql_uri(uri_str) do
      {:ok, results} -> results
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  defp validate_url_scheme(scheme) when scheme in ["postgres", "postgresql"], do: :ok
  defp validate_url_scheme(scheme), do: {:error, "invalid URL scheme: #{inspect(scheme)}"}

  defp validate_url_host(str) do
    if is_binary(str) and String.trim(str) != "" do
      :ok
    else
      {:error, "missing host"}
    end
  end

  defp parse_url_userinfo(str) do
    with false <- is_nil(str),
         {:ok, {username, password}} <- split_userinfo(str),
         false <- String.trim(username) == "" do
      {:ok, {username, password}}
    else
      _ -> {:error, "invalid or missing username"}
    end
  end

  defp split_userinfo(str) do
    case String.split(str, ":") do
      [username] -> {:ok, {username, nil}}
      [username, password] -> {:ok, {username, password}}
      _ -> :error
    end
  end

  defp parse_url_query(nil), do: {:ok, []}

  defp parse_url_query(query_str) do
    params = URI.decode_query(query_str)

    with :ok <- validate_no_replication_param(params) do
      warn_ignored_query_options(params)
      parse_sslmode(params)
    end
  end

  # Only `sslmode` is read from the query. Another key is accepted and dropped, which hides a
  # misspelt option, so each is named. Values stay out of the log: a query can carry a secret.
  defp warn_ignored_query_options(params) do
    case params |> Map.keys() |> List.delete("sslmode") |> Enum.sort() do
      [] ->
        :ok

      ignored ->
        Logger.warning(
          "Ignoring unsupported options in the database URL query: " <>
            Enum.map_join(ignored, ", ", &inspect/1) <>
            ". Only \"sslmode\" is read from it."
        )
    end
  end

  defp validate_no_replication_param(%{"replication" => _}) do
    {:error,
     "unsupported \"replication\" query option. Electric opens both a replication connection and regular connections to Postgres as needed"}
  end

  defp validate_no_replication_param(_), do: :ok

  defp parse_sslmode(%{"sslmode" => sslmode})
       when sslmode in ~w[disable allow prefer require] do
    {:ok, sslmode: String.to_existing_atom(sslmode)}
  end

  defp parse_sslmode(%{"sslmode" => "verify-ca"}), do: {:ok, sslmode: :"verify-ca"}

  defp parse_sslmode(%{"sslmode" => "verify-full"}), do: {:ok, sslmode: :"verify-full"}

  defp parse_sslmode(%{"sslmode" => sslmode}) do
    {:error, "invalid \"sslmode\" value: #{inspect(sslmode)}"}
  end

  defp parse_sslmode(_), do: {:ok, []}

  defp parse_database(nil, username), do: username
  defp parse_database("/", username), do: username
  defp parse_database("/" <> dbname, _username), do: dbname

  @log_levels ~w[emergency alert critical error warning warn notice info debug]
  @public_log_levels ~w[error warning info debug]

  @spec parse_log_level(binary) :: {:ok, Logger.level()} | {:error, binary}
  def parse_log_level(str) when str in @log_levels do
    {:ok, String.to_existing_atom(str)}
  end

  def parse_log_level(str) do
    {:error, "invalid log level: #{inspect(str)}. Must be one of #{inspect(@public_log_levels)}"}
  end

  def parse_log_level!(str) when str in @log_levels, do: String.to_existing_atom(str)

  def parse_log_level!(_str) do
    raise Dotenvy.Error, message: "Must be one of #{inspect(@public_log_levels)}"
  end

  @doc """
  Resolve the IP address the (unauthenticated) LiveDashboard endpoint binds to.

  The dashboard exposes VM internals with no authentication, so it binds to loopback unless an
  operator explicitly opts into another address via `ELECTRIC_LIVE_DASHBOARD_ADDRESS`. `ipv6?`
  selects the loopback family used when no address is given.

  ## Examples

      iex> parse_dashboard_listen_address(nil, false)
      {:ok, {127, 0, 0, 1}}

      iex> parse_dashboard_listen_address("", true)
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}}

      iex> parse_dashboard_listen_address("10.1.2.3", false)
      {:ok, {10, 1, 2, 3}}

      iex> parse_dashboard_listen_address("not-an-ip", false)
      {:error, ~S|invalid IP address: "not-an-ip"|}
  """
  @spec parse_dashboard_listen_address(String.t() | nil, boolean()) ::
          {:ok, :inet.ip_address()} | {:error, binary()}
  def parse_dashboard_listen_address(address, ipv6?) when address in [nil, ""],
    do: {:ok, dashboard_loopback(ipv6?)}

  def parse_dashboard_listen_address(address, _ipv6?) when is_binary(address) do
    case :inet.parse_address(String.to_charlist(address)) do
      {:ok, ip} -> {:ok, ip}
      {:error, _reason} -> {:error, "invalid IP address: #{inspect(address)}"}
    end
  end

  defp dashboard_loopback(false), do: {127, 0, 0, 1}
  defp dashboard_loopback(true), do: {0, 0, 0, 0, 0, 0, 0, 1}

  @spec parse_dashboard_listen_address!(String.t() | nil, boolean()) :: :inet.ip_address()
  def parse_dashboard_listen_address!(address, ipv6?) do
    case parse_dashboard_listen_address(address, ipv6?) do
      {:ok, ip} -> ip
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  @time_units ~w[ms msec s sec m min]

  @spec parse_human_readable_time(binary | nil) :: {:ok, pos_integer} | {:error, binary}

  def parse_human_readable_time(str) do
    with {num, suffix} <- Float.parse(str),
         true <- num > 0,
         suffix = String.trim(suffix),
         true <- suffix == "" or suffix in @time_units do
      {:ok, trunc(num * time_multiplier(suffix))}
    else
      _ -> {:error, "invalid time unit: #{inspect(str)}. Must be one of #{inspect(@time_units)}"}
    end
  end

  defp time_multiplier(""), do: 1
  defp time_multiplier(millisecond) when millisecond in ["ms", "msec"], do: 1
  defp time_multiplier(second) when second in ["s", "sec"], do: 1000
  defp time_multiplier(minute) when minute in ["m", "min"], do: 1000 * 60

  def parse_human_readable_time!(str) do
    case parse_human_readable_time(str) do
      {:ok, result} -> result
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  @doc """
  Parse human-readable memory/storage size string into bytes.

  ## Examples

    iex> parse_human_readable_size("1GiB")
    {:ok, #{1024 * 1024 * 1024}}

    iex> parse_human_readable_size("2.23GB")
    {:ok, 2_230_000_000}

    iex> parse_human_readable_size("256MiB")
    {:ok, #{256 * 1024 * 1024}}

    iex> parse_human_readable_size("377MB")
    {:ok, 377_000_000}

    iex> parse_human_readable_size("430KiB")
    {:ok, #{430 * 1024}}

    iex> parse_human_readable_size("142888KB")
    {:ok, 142_888_000}

    iex> parse_human_readable_size("123456789")
    {:ok, 123_456_789}

    iex> parse_human_readable_size("")
    {:error, ~S'invalid size unit: "". Must be one of ["KB", "KiB", "MB", "MiB", "GB", "GiB"]'}

    iex> parse_human_readable_size("foo")
    {:error, ~S'invalid size unit: "foo". Must be one of ["KB", "KiB", "MB", "MiB", "GB", "GiB"]'}
  """
  @spec parse_human_readable_size(binary) :: {:ok, pos_integer} | {:error, binary}

  @size_units ~w[KB KiB MB MiB GB GiB]

  def parse_human_readable_size(str) do
    with {num, suffix} <- Float.parse(str),
         true <- num > 0,
         suffix = String.trim(suffix),
         true <- suffix == "" or suffix in @size_units do
      {:ok, trunc(num * size_multiplier(suffix))}
    else
      _ -> {:error, "invalid size unit: #{inspect(str)}. Must be one of #{inspect(@size_units)}"}
    end
  end

  defp size_multiplier(""), do: 1
  defp size_multiplier("KB"), do: 1_000
  defp size_multiplier("KiB"), do: 1024
  defp size_multiplier("MB"), do: 1_000_000
  defp size_multiplier("MiB"), do: 1024 * 1024
  defp size_multiplier("GB"), do: 1_000_000_000
  defp size_multiplier("GiB"), do: 1024 * 1024 * 1024

  def parse_human_readable_size!(str) do
    case parse_human_readable_size(str) do
      {:ok, result} -> result
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  @doc """
  Parse a top process limit string into a tagged tuple.

  ## Examples

    iex> parse_top_process_limit("count:5")
    {:ok, {:count, 5}}

    iex> parse_top_process_limit("mem_percent:80")
    {:ok, {:mem_percent, 80}}

    iex> parse_top_process_limit("mem_percent:0")
    {:error, "mem_percent value must be between 1 and 100, got: 0"}

    iex> parse_top_process_limit("count:0")
    {:error, "count value must be a positive integer, got: 0"}

    iex> parse_top_process_limit("foo")
    {:error, ~S'invalid top process limit: "foo". Expected format: count:<N> or mem_percent:<N>'}
  """
  @spec parse_top_process_limit(binary) ::
          {:ok, {:count, pos_integer()} | {:mem_percent, 1..100}} | {:error, binary}
  def parse_top_process_limit(str) do
    case String.split(str, ":", parts: 2) do
      ["count", n] ->
        parse_process_count(n)

      ["mem_percent", n] ->
        parse_mem_percent(n)

      _ ->
        {:error,
         "invalid top process limit: #{inspect(str)}. Expected format: count:<N> or mem_percent:<N>"}
    end
  end

  def parse_top_process_limit!(str) do
    case parse_top_process_limit(str) do
      {:ok, result} -> result
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  @doc """
  Parse an HTTP/2 reset-stream rate limit of the form `<count>/<duration>` into the
  `{count, period_ms}` tuple Bandit expects, or `disabled` into `:disabled`.

  ## Examples

    iex> parse_http2_max_reset_stream_rate("500/10s")
    {:ok, {500, 10000}}

    iex> parse_http2_max_reset_stream_rate("disabled")
    {:ok, :disabled}

    iex> parse_http2_max_reset_stream_rate("500")
    {:error, ~S'invalid HTTP/2 reset stream rate: "500". Expected format: <count>/<duration> (e.g. 500/10s) or disabled'}
  """
  @spec parse_http2_max_reset_stream_rate(binary) ::
          {:ok, {pos_integer, pos_integer} | :disabled} | {:error, binary}
  def parse_http2_max_reset_stream_rate(str) do
    with false <- String.downcase(str) == "disabled",
         [count_str, period_str] <- String.split(str, "/"),
         {count, ""} when count > 0 <- Integer.parse(count_str),
         {:ok, period_ms} <- parse_human_readable_time(period_str) do
      {:ok, {count, period_ms}}
    else
      true ->
        {:ok, :disabled}

      _ ->
        {:error,
         "invalid HTTP/2 reset stream rate: #{inspect(str)}. " <>
           "Expected format: <count>/<duration> (e.g. 500/10s) or disabled"}
    end
  end

  def parse_http2_max_reset_stream_rate!(str) do
    case parse_http2_max_reset_stream_rate(str) do
      {:ok, result} -> result
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  @doc """
  Parse the deprecated ELECTRIC_TELEMETRY_TOP_PROCESS_COUNT value (a plain integer)
  into a `{:count, N}` tuple.

  ## Examples

    iex> parse_legacy_top_process_count("10")
    {:ok, {:count, 10}}

    iex> parse_legacy_top_process_count("0")
    {:error, "count value must be a positive integer, got: 0"}
  """
  @spec parse_legacy_top_process_count(binary) ::
          {:ok, {:count, pos_integer()}} | {:error, binary}
  def parse_legacy_top_process_count(str), do: parse_process_count(str)

  def parse_legacy_top_process_count!(str) do
    case parse_legacy_top_process_count(str) do
      {:ok, result} -> result
      {:error, message} -> raise Dotenvy.Error, message: message
    end
  end

  defp parse_process_count(str) do
    case Integer.parse(str) do
      {val, ""} when val > 0 -> {:ok, {:count, val}}
      _ -> {:error, "count value must be a positive integer, got: #{str}"}
    end
  end

  defp parse_mem_percent(str) do
    case Integer.parse(str) do
      {val, ""} when val >= 1 and val <= 100 -> {:ok, {:mem_percent, val}}
      _ -> {:error, "mem_percent value must be between 1 and 100, got: #{str}"}
    end
  end

  def validate_security_config!(secret, insecure) do
    cond do
      insecure && secret != nil ->
        raise "You cannot set both ELECTRIC_SECRET and ELECTRIC_INSECURE=true"

      !insecure && secret == nil ->
        raise "You must set ELECTRIC_SECRET unless ELECTRIC_INSECURE=true. Setting ELECTRIC_INSECURE=true risks exposing your database, only use insecure mode in development or you've otherwise secured the Electric API"

      # An unset variable expanding to an empty string is the ordinary way this happens in a shell or
      # a manifest. Blank is not nil, so it used to pass as a configured secret: the service came
      # up reporting itself secured, logged nothing unusual, and `?secret=` then matched it exactly.
      # Refuse to boot rather than advertise protection that is not there.
      !insecure && is_binary(secret) && String.trim(secret) == "" ->
        raise "ELECTRIC_SECRET is set but blank. A blank secret is not a secret - it leaves the API open to anyone who sends `?secret=`. Set a real value, or set ELECTRIC_INSECURE=true if you genuinely intend to run without authentication."

      true ->
        if insecure do
          Logger.warning(
            "Electric is running in insecure mode - this risks exposing your database - only use insecure mode in development or if you've otherwise secured the Electric API."
          )
        end

        :ok
    end
  end

  @insecure_true_values ~w[true 1 yes on]

  @doc """
  Decode the `ELECTRIC_INSECURE` flag with fail-closed semantics.

  Only an explicit, recognised true spelling turns authentication off. Anything else - unset,
  blank, a recognised false spelling, or an unrecognised value - resolves to secure, so a typo
  or a shell-expanded empty string can never leave the API unauthenticated.

  ## Examples

      iex> parse_insecure!("true")
      true

      iex> parse_insecure!("YES")
      true

      iex> parse_insecure!("no")
      false

      iex> parse_insecure!("off")
      false

      iex> parse_insecure!("")
      false

      iex> parse_insecure!("disabled")
      false
  """
  @spec parse_insecure!(binary) :: boolean
  def parse_insecure!(value) when is_binary(value) do
    String.downcase(String.trim(value)) in @insecure_true_values
  end

  # Leave room for the extra row used to detect overflow within PostgreSQL's bigint LIMIT.
  @max_subset_rows 2 ** 63 - 2
  @subset_max_rows_error "must be an integer between 1 and #{@max_subset_rows}"

  def parse_subset_max_rows!(value) do
    with {rows, ""} <- Integer.parse(value),
         {:ok, rows} <- validate_subset_max_rows(rows) do
      rows
    else
      _ -> raise Dotenvy.Error, message: "ELECTRIC_SUBSET_MAX_ROWS #{@subset_max_rows_error}"
    end
  end

  def validate_subset_max_rows(nil), do: {:ok, nil}

  def validate_subset_max_rows(rows) when is_integer(rows) and rows in 1..@max_subset_rows,
    do: {:ok, rows}

  def validate_subset_max_rows(_), do: {:error, @subset_max_rows_error}

  @valid_spawn_opts ~w[min_bin_vheap_size min_heap_size priority fullsweep_after message_queue_data]

  @doc """
  Parse `spawn_opts` from environment variable to keyword list suitable for passing to `GenServer.start_link/2`

  ## Examples

      iex> parse_spawn_opts!(~S({"shape_log_collector":{"min_heap_size":234,"min_bin_vheap_size":123,"message_queue_data":"on_heap","priority":"high","fullsweep_after":104}}))
      %{shape_log_collector: [fullsweep_after: 104, message_queue_data: :on_heap, min_bin_vheap_size: 123, min_heap_size: 234, priority: :high]}

      iex> parse_spawn_opts!(~S({"shape_log_collector":{"monkey":123,"message_queue_data":"on_fire","min_bin_vheap_size":-1,"priority":"high"}}))
      %{shape_log_collector: [priority: :high]}

      iex> parse_spawn_opts!("")
      %{}

      iex> parse_spawn_opts!("{}")
      %{}
  """
  def parse_spawn_opts!("") do
    %{}
  end

  def parse_spawn_opts!(str) do
    str
    |> Jason.decode!()
    |> then(fn
      opts when is_map(opts) ->
        for {process_name, process_opts} <- opts, is_map(process_opts), into: %{} do
          opts =
            for {opt_key, opt_val} <- process_opts,
                opt_key in @valid_spawn_opts,
                key = String.to_atom(opt_key),
                val = validate_spawn_opt(key, opt_val) do
              {key, val}
            end

          {String.to_atom(process_name), opts}
        end

      _invalid ->
        raise ArgumentError, message: "Invalid spawn opts: #{inspect(str)}"
    end)
    |> tap(fn process_spawn_opts ->
      if map_size(process_spawn_opts) > 0 do
        Logger.notice("Process spawn opts: #{inspect(process_spawn_opts)}")
      end
    end)
  end

  defp validate_spawn_opt(:min_bin_vheap_size, val) when is_integer(val) and val >= 0, do: val
  defp validate_spawn_opt(:min_heap_size, val) when is_integer(val) and val >= 0, do: val

  defp validate_spawn_opt(:priority, val) when val in ["low", "normal", "high"],
    do: String.to_atom(val)

  defp validate_spawn_opt(:fullsweep_after, val) when is_integer(val) and val >= 0, do: val

  defp validate_spawn_opt(:message_queue_data, val) when val in ["off_heap", "on_heap"],
    do: String.to_atom(val)

  defp validate_spawn_opt(_, _), do: nil

  @doc false
  # helper function for use in doc tests
  def deobfuscate({:ok, connection_opts}),
    do: {:ok, Electric.Utils.deobfuscate_password(connection_opts)}

  def deobfuscate(other), do: other

  @doc ~S"""
  Parse a comma-separated string into a MapSet of trimmed values.

  ## Examples

      iex> parse_comma_separated_set!("foo,bar,baz")
      MapSet.new(["bar", "baz", "foo"])

      iex> parse_comma_separated_set!(" foo , bar , baz ")
      MapSet.new(["bar", "baz", "foo"])

      iex> parse_comma_separated_set!("single")
      MapSet.new(["single"])

      iex> parse_comma_separated_set!(",,,")
      MapSet.new()

      iex> parse_comma_separated_set!("")
      MapSet.new()
  """
  def parse_comma_separated_set!(str) do
    str |> String.split(",", trim: true) |> Enum.map(&String.trim/1) |> MapSet.new()
  end

  def parse_otel_sample_rate(str) do
    case Integer.parse(str) do
      {rate, ""} when rate >= 1 and rate <= 0x7FFF_FFFF_FFFF_FFFF -> rate
      _ -> nil
    end
  end

  def parse_otel_trusted_sample_rates!(str) do
    case String.trim(str) do
      "" ->
        MapSet.new()

      rates ->
        rates
        |> String.split(",")
        |> MapSet.new(fn entry ->
          parse_otel_sample_rate(String.trim(entry)) ||
            raise Dotenvy.Error,
                  "expected a comma-separated list of positive signed 64-bit sample rates"
        end)
    end
  end

  def parse_feature_flags(str) do
    str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.split_with(&(&1 in @known_feature_flags))
    |> case do
      {known, []} ->
        known

      {known, unknown} ->
        # Log an error but don't raise so that deployments can proceeed without
        # removal of old flags and new flags can be added before deployment
        Logger.error(
          "Unknown feature flags specified: #{inspect(unknown)}. Known feature flags: #{inspect(@known_feature_flags)}"
        )

        known
    end
  end
end
