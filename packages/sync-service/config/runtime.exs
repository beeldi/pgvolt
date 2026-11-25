import Config
import Dotenvy

config :elixir, :time_zone_database, Tz.TimeZoneDatabase

if config_env() in [:dev, :test] do
  source!([".env.#{config_env()}", ".env.#{config_env()}.local", System.get_env()])
else
  source!([System.get_env()])
end

test_log_level =
  if config_env() == :test,
    do: env!("ELECTRIC_TEST_LOG_LEVEL", &Electric.Config.parse_log_level!/1, :error)

log_level =
  env!("ELECTRIC_LOG_LEVEL", &Electric.Config.parse_log_level!/1, test_log_level) || :info

config :logger, level: log_level

config :logger, :default_formatter,
  # Doubled line breaks serve as long message boundaries
  format: "\n$time $metadata[$level] $message\n",
  metadata: [:pid, :shape_handle, :request_id],
  colors: [enabled: env!("ELECTRIC_LOG_COLORS", :boolean!, true)]

# The default logger_std_h handler writes application logs to stdout. Cap its
# OLP mailbox so error bursts shed stdout logs instead of blocking Logger
# callers (sync mode) or letting the queue grow unbounded.
config :logger, :default_handler,
  config: %{sync_mode_qlen: 2000, drop_mode_qlen: 2000, flush_qlen: 5000}

# Enable this to get **very noisy** but useful messages from BEAM about
# processes being started, stopped and crashes.
# https://www.erlang.org/doc/apps/sasl/error_logging#sasl-reports
sasl? = env!("ELECTRIC_LOG_OTP_REPORTS", :boolean, false)

config :logger,
  handle_otp_reports: true,
  handle_sasl_reports: sasl?

if config_env() == :test do
  config :logger, :default_handler, level: test_log_level
end

service_name = env!("ELECTRIC_SERVICE_NAME", :string, "electric")
instance_id = env!("ELECTRIC_INSTANCE_ID", :string, Electric.Utils.uuid4())

replication_database_url_config = env!("DATABASE_URL", &Electric.Config.parse_postgresql_uri!/1)

# TODO: Remove this in a minor version bump
old_pooled_database_url_config =
  env!(
    "ELECTRIC_QUERY_DATABASE_URL",
    &Electric.Config.parse_postgresql_uri!/1,
    replication_database_url_config
  )

pooled_database_url_config =
  env!(
    "ELECTRIC_POOLED_DATABASE_URL",
    &Electric.Config.parse_postgresql_uri!/1,
    old_pooled_database_url_config
  )

database_ipv6_config = env!("ELECTRIC_DATABASE_USE_IPV6", :boolean, false)
database_cacertfile = env!("ELECTRIC_DATABASE_CA_CERTIFICATE_FILE", :string, nil)

if replication_database_url_config[:sslmode] == :disable and not is_nil(database_cacertfile) do
  raise Dotenvy.Error,
    message:
      "When ELECTRIC_DATABASE_CA_CERTIFICATE_FILE is set, " <>
        "sslmode must be omitted or set to a value other than 'disable'"
end

extra_conn_opts =
  Enum.reject(
    [ipv6: database_ipv6_config, cacertfile: database_cacertfile],
    fn {_, val} -> is_nil(val) end
  )

config :electric,
  replication_connection_opts: replication_database_url_config ++ extra_conn_opts,
  query_connection_opts: pooled_database_url_config ++ extra_conn_opts

enable_integration_testing? = env!("ELECTRIC_ENABLE_INTEGRATION_TESTING", :boolean, nil)
cache_max_age = env!("ELECTRIC_CACHE_MAX_AGE", :integer, nil)
cache_stale_age = env!("ELECTRIC_CACHE_STALE_AGE", :integer, nil)
statsd_host = env!("ELECTRIC_STATSD_HOST", :string?, nil)

chunk_bytes_threshold = env!("ELECTRIC_SHAPE_CHUNK_BYTES_THRESHOLD", :integer, nil)
compaction_period = env!("ELECTRIC_COMPACTION_PERIOD", :integer, nil)

storage_dir = env!("ELECTRIC_STORAGE_DIR", :string, "./persistent")

shape_path = Path.join(storage_dir, "./shapes")
persistent_state_path = Path.join(storage_dir, "./state")

persistent_kv_spec =
  env!(
    "ELECTRIC_PERSISTENT_STATE",
    fn storage ->
      case String.downcase(storage) do
        "memory" ->
          {Electric.PersistentKV.Memory, :new!, []}

        "file" ->
          {Electric.PersistentKV.Filesystem, :new!, root: persistent_state_path}

        _ ->
          raise Dotenvy.Error, message: "ELECTRIC_PERSISTENT_STATE must be one of: MEMORY, FILE"
      end
    end,
    nil
  )

if persistent_kv_spec do
  {m, f, a} = persistent_kv_spec
  persistent_kv = apply(m, f, [a])
  Electric.Config.persist_installation_id(persistent_kv, instance_id)
end

storage_spec =
  env!(
    "ELECTRIC_STORAGE",
    fn storage ->
      case String.downcase(storage) do
        "memory" ->
          {Electric.ShapeCache.InMemoryStorage, []}

        legacy_file when legacy_file in ["file", "legacy_file"] ->
          raise RuntimeError,
            message:
              "#{inspect(legacy_file)} storage is deprecated. Please change to \"fast_file\""

        "fast_file" ->
          {Electric.ShapeCache.PureFileStorage, storage_dir: shape_path}

        _ ->
          raise Dotenvy.Error, message: "storage must be one of: MEMORY, FAST_FILE, LEGACY_FILE"
      end
    end,
    nil
  )

replication_stream_id =
  env!(
    "ELECTRIC_REPLICATION_STREAM_ID",
    fn replication_stream_id ->
      {:ok, parsed_id} =
        replication_stream_id
        |> Electric.Postgres.Identifiers.parse_unquoted_identifier()

      parsed_id
    end,
    nil
  )

prometheus_port = env!("ELECTRIC_PROMETHEUS_PORT", :integer, nil)
live_dashboard_port = env!("ELECTRIC_LIVE_DASHBOARD_PORT", :integer, nil)

shape_hibernate_after =
  env!("ELECTRIC_SHAPE_HIBERNATE_AFTER", &Electric.Config.parse_human_readable_time!/1, nil)

shape_enable_suspend? = env!("ELECTRIC_SHAPE_SUSPEND_CONSUMER", :boolean, nil)

shape_suspend_after =
  env!("ELECTRIC_SHAPE_SUSPEND_AFTER", &Electric.Config.parse_human_readable_time!/1, nil)

flush_stall_grace_period =
  env!("ELECTRIC_FLUSH_STALL_GRACE_PERIOD", &Electric.Config.parse_human_readable_time!/1, nil)

flush_stall_boot_grace =
  env!("ELECTRIC_FLUSH_STALL_BOOT_GRACE", &Electric.Config.parse_human_readable_time!/1, nil)

flush_deferral_round_limit = env!("ELECTRIC_FLUSH_DEFERRAL_ROUND_LIMIT", :integer, nil)

gather_stall_timeout =
  env!("ELECTRIC_GATHER_STALL_TIMEOUT", &Electric.Config.parse_human_readable_time!/1, nil)

system_metrics_poll_interval =
  env!(
    "ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL",
    &Electric.Config.parse_human_readable_time!/1,
    nil
  )

otel_export_period =
  env!(
    "ELECTRIC_OTEL_EXPORT_PERIOD",
    &Electric.Config.parse_human_readable_time!/1,
    nil
  )

stack_telemetry_init_delay =
  env!(
    "ELECTRIC_STACK_TELEMETRY_INIT_DELAY",
    &Electric.Config.parse_human_readable_time!/1,
    nil
  )

# 0 turns the storage directory walk, and with it the storage-used metric, off.
disk_usage_scan_period =
  env!(
    "ELECTRIC_DISK_USAGE_SCAN_PERIOD",
    fn
      "0" -> 0
      str -> Electric.Config.parse_human_readable_time!(str)
    end,
    nil
  )

# The provided database id is relevant if you had used v0.8 and want to keep the storage
# instead of having hanging files. We use a provided value as stack id, but nothing else.
provided_database_id = env!("ELECTRIC_DATABASE_ID", :string, nil)

# Handle authentication configuration
insecure = env!("ELECTRIC_INSECURE", &Electric.Config.parse_insecure!/1, false)
secret = env!("ELECTRIC_SECRET", :string, nil)

if config_env() != :test do
  Electric.Config.validate_security_config!(secret, insecure)
end

max_concurrent_requests =
  case env!("ELECTRIC_MAX_CONCURRENT_REQUESTS", :string, nil) do
    nil ->
      nil

    json when is_binary(json) ->
      Jason.decode!(json, keys: :atoms)
  end

config :electric,
  provided_database_id: provided_database_id,
  allow_shape_deletion?: enable_integration_testing?,
  cache_max_age: cache_max_age,
  cache_stale_age: cache_stale_age,
  chunk_bytes_threshold: chunk_bytes_threshold,
  compaction_period: compaction_period,
  max_shapes:
    env!("ELECTRIC_MAX_SHAPES", :integer, nil) ||
      env!("ELECTRIC_EXPERIMENTAL_MAX_SHAPES", :integer, nil),
  consumer_partitions: env!("ELECTRIC_CONSUMER_PARTITIONS", :integer, nil),
  max_concurrent_requests: max_concurrent_requests,
  long_poll_timeout: env!("ELECTRIC_LONG_POLL_TIMEOUT", :integer, nil),
  # Used in telemetry
  instance_id: instance_id,
  system_metrics_poll_interval: system_metrics_poll_interval,
  stack_telemetry_init_delay: stack_telemetry_init_delay,
  disk_usage_scan_period: disk_usage_scan_period,
  otel_export_period: otel_export_period,
  otel_sampling_ratio: env!("ELECTRIC_OTEL_SAMPLING_RATIO", :float, nil),
  otel_trusted_sample_rates:
    env!(
      "ELECTRIC_OTEL_TRUSTED_SAMPLE_RATES",
      &Electric.Config.parse_otel_trusted_sample_rates!/1,
      nil
    ),
  metrics_sampling_ratio: env!("ELECTRIC_METRICS_SAMPLING_RATIO", :float, nil),
  drop_empty_response_spans?: env!("ELECTRIC_DROP_EMPTY_RESPONSE_SPANS", :boolean, nil),
  telemetry_top_process_limit:
    env!("ELECTRIC_TELEMETRY_TOP_PROCESS_LIMIT", &Electric.Config.parse_top_process_limit!/1, nil) ||
      env!(
        "ELECTRIC_TELEMETRY_TOP_PROCESS_COUNT",
        fn str ->
          IO.warn(
            "ELECTRIC_TELEMETRY_TOP_PROCESS_COUNT is deprecated, " <>
              "use ELECTRIC_TELEMETRY_TOP_PROCESS_LIMIT=count:#{str} instead"
          )

          Electric.Config.parse_legacy_top_process_count!(str)
        end,
        nil
      ),
  telemetry_long_gc_threshold: env!("ELECTRIC_TELEMETRY_LONG_GC_THRESHOLD", :integer, nil),
  telemetry_long_schedule_threshold:
    env!("ELECTRIC_TELEMETRY_LONG_SCHEDULE_THRESHOLD", :integer, nil),
  telemetry_long_message_queue_enable_threshold:
    env!("ELECTRIC_TELEMETRY_LONG_MESSAGE_QUEUE_ENABLE_THRESHOLD", :integer, nil),
  telemetry_long_message_queue_disable_threshold:
    env!("ELECTRIC_TELEMETRY_LONG_MESSAGE_QUEUE_DISABLE_THRESHOLD", :integer, nil),
  telemetry_statsd_host: statsd_host,
  telemetry_redact_request_values?:
    env!("ELECTRIC_TELEMETRY_REDACT_REQUEST_VALUES", :boolean, nil),
  prometheus_port: prometheus_port,
  live_dashboard_port: live_dashboard_port,
  db_pool_size: env!("ELECTRIC_DB_POOL_SIZE", :integer, nil),
  router_shards: env!("ELECTRIC_ROUTER_SHARDS", :integer, nil),
  replication_pipeline_window: env!("ELECTRIC_REPLICATION_PIPELINE_WINDOW", :integer, nil),
  shared_conversion: env!("ELECTRIC_SHARED_CONVERSION", :boolean, nil),
  shared_payload_arena: env!("ELECTRIC_SHARED_PAYLOAD_ARENA", :boolean, nil),
  payload_arena_reclaim_min_bytes:
    env!("ELECTRIC_PAYLOAD_ARENA_RECLAIM_MIN_BYTES", :integer, nil),
  worker_prepare: env!("ELECTRIC_WORKER_PREPARE", :boolean, nil),
  shared_fragment_blocks: env!("ELECTRIC_SHARED_FRAGMENT_BLOCKS", :boolean, nil),
  ack_after_journal: env!("ELECTRIC_ACK_AFTER_JOURNAL", :boolean, nil),
  keep_shapes_on_promotion: env!("ELECTRIC_KEEP_SHAPES_ON_PROMOTION", :boolean, nil),
  ingest_priority_high: env!("ELECTRIC_INGEST_PRIORITY_HIGH", :boolean, nil),
  filter_routing_memo: env!("ELECTRIC_FILTER_ROUTING_MEMO", :boolean, nil),
  db_tcp_keepalive_idle:
    env!(
      "ELECTRIC_DATABASE_TCP_KEEPALIVE_IDLE",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  db_tcp_keepalive_interval:
    env!(
      "ELECTRIC_DATABASE_TCP_KEEPALIVE_INTERVAL",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  db_tcp_keepalive_count: env!("ELECTRIC_DATABASE_TCP_KEEPALIVE_COUNT", :integer, nil),
  db_tcp_user_timeout:
    env!(
      "ELECTRIC_DATABASE_TCP_USER_TIMEOUT",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  replication_stream_id: replication_stream_id,
  replication_slot_temporary?: env!("CLEANUP_REPLICATION_SLOTS_ON_SHUTDOWN", :boolean, nil),
  replication_slot_temporary_random_name?:
    env!("ELECTRIC_TEMPORARY_REPLICATION_SLOT_USE_RANDOM_NAME", :boolean, nil),
  # The ELECTRIC_EXPERIMENTAL_MAX_TXN_SIZE is undocumented and will be removed in future versions.
  max_txn_size: env!("ELECTRIC_EXPERIMENTAL_MAX_TXN_SIZE", :integer, nil),
  # The ELECTRIC_EXPERIMENTAL_MAX_BATCH_SIZE is undocumented and used for testing only.
  max_batch_size: env!("ELECTRIC_EXPERIMENTAL_MAX_BATCH_SIZE", :integer, nil),
  service_port: env!("ELECTRIC_PORT", :integer, nil),
  subquery_buffer_max_transactions:
    env!("ELECTRIC_SUBQUERY_BUFFER_MAX_TRANSACTIONS", :integer, nil),
  subset_max_rows:
    env!("ELECTRIC_SUBSET_MAX_ROWS", &Electric.Config.parse_subset_max_rows!/1, nil),
  txn_buffer_max_changes: env!("ELECTRIC_TXN_BUFFER_MAX_CHANGES", :integer, nil),
  shape_hibernate_after: shape_hibernate_after,
  shape_enable_suspend?: shape_enable_suspend?,
  shape_suspend_after: shape_suspend_after,
  flush_stall_grace_period: flush_stall_grace_period,
  flush_stall_boot_grace: flush_stall_boot_grace,
  flush_deferral_round_limit: flush_deferral_round_limit,
  gather_stall_timeout: gather_stall_timeout,
  storage_dir: storage_dir,
  storage: storage_spec,
  cleanup_interval_ms:
    env!("ELECTRIC_CLEANUP_INTERVAL_MS", &Electric.Config.parse_human_readable_time!/1, nil),
  profile_where_clauses?: env!("ELECTRIC_PROFILE_WHERE_CLAUSES", :boolean, false),
  persistent_kv: persistent_kv_spec,
  listen_on_ipv6?: env!("ELECTRIC_LISTEN_ON_IPV6", :boolean, nil),
  secret: secret,
  publication_alter_debounce_ms:
    env!(
      "ELECTRIC_PUBLICATION_ALTER_DEBOUNCE_TIME",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  process_registry_partitions: env!("ELECTRIC_TWEAKS_PROCESS_REGISTRY_PARTITIONS", :integer, nil),
  process_spawn_opts:
    env!("ELECTRIC_PROCESS_SPAWN_OPTS", &Electric.Config.parse_spawn_opts!/1, %{}),
  consumer_gc_heap_threshold: env!("ELECTRIC_CONSUMER_GC_HEAP_THRESHOLD", :integer, nil),
  stalled_serve_timeout: env!("ELECTRIC_STALLED_SERVE_TIMEOUT", :integer, nil),
  http_api_num_acceptors: env!("ELECTRIC_TWEAKS_HTTP_API_NUM_ACCEPTORS", :integer, 100),
  conn_max_requests: env!("ELECTRIC_TWEAKS_CONN_MAX_REQUESTS", :integer, nil),
  max_request_line_length: env!("ELECTRIC_TWEAKS_MAX_REQUEST_LINE_LENGTH", :integer, nil),
  handler_fullsweep_after: env!("ELECTRIC_TWEAKS_HANDLER_FULLSWEEP_AFTER", :integer, nil),
  http2_max_reset_stream_rate:
    env!(
      "ELECTRIC_TWEAKS_HTTP2_MAX_RESET_STREAM_RATE",
      &Electric.Config.parse_http2_max_reset_stream_rate!/1,
      nil
    ),
  tcp_send_timeout:
    env!("ELECTRIC_TCP_SEND_TIMEOUT", &Electric.Config.parse_human_readable_time!/1, nil),
  tcp_read_timeout:
    env!("ELECTRIC_TCP_READ_TIMEOUT", &Electric.Config.parse_human_readable_time!/1, nil),
  feature_flags: env!("ELECTRIC_FEATURE_FLAGS", &Electric.Config.parse_feature_flags/1, nil),
  manual_table_publishing?: env!("ELECTRIC_MANUAL_TABLE_PUBLISHING", :boolean, nil),
  publication_refresh_period:
    env!(
      "ELECTRIC_TWEAKS_PUBLICATION_REFRESH_PERIOD",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  schema_reconciler_period:
    env!(
      "ELECTRIC_TWEAKS_SCHEMA_RECONCILER_PERIOD",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  replication_idle_timeout:
    env!(
      "ELECTRIC_REPLICATION_IDLE_TIMEOUT",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  replication_heartbeat_interval:
    env!(
      "ELECTRIC_REPLICATION_HEARTBEAT_INTERVAL",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  shape_db_exclusive_mode: env!("ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE", :boolean, false),
  shape_db_storage_dir: env!("ELECTRIC_SHAPE_DB_STORAGE_DIR", :string, nil),
  shape_db_synchronous: env!("ELECTRIC_SHAPE_DB_SYNCHRONOUS", :string, nil),
  shape_db_cache_size:
    env!("ELECTRIC_SHAPE_DB_CACHE_SIZE", &Electric.Config.parse_human_readable_size!/1, nil),
  shape_db_boot_read_timeout:
    env!(
      "ELECTRIC_SHAPE_DB_BOOT_READ_TIMEOUT",
      &Electric.Config.parse_human_readable_time!/1,
      nil
    ),
  exclude_spans:
    env!("ELECTRIC_EXCLUDE_SPANS", &Electric.Config.parse_comma_separated_set!/1, nil)

if Electric.telemetry_enabled?() do
  # Disable the default telemetry_poller process since we start our own in
  # `ElectricTelemetry.{ApplicationTelemetry, StackTelemetry}`.
  config :telemetry_poller, default: false

  config :sentry,
    environment_name: config_env(),
    client: Electric.Telemetry.SentryReqHTTPClient,
    before_send: {Electric.Telemetry.Sentry, :scrub_request_credentials}

  if sentry_dsn = env!("SENTRY_DSN", :string, nil) do
    config :sentry, dsn: sentry_dsn
  end

  otlp_endpoint = env!("ELECTRIC_OTLP_ENDPOINT", :string, nil)
  otel_debug? = env!("ELECTRIC_OTEL_DEBUG", :boolean, false)

  if otlp_endpoint do
    # Shortcut config for Honeycomb.io:
    # users may set the optional ELECTRIC_HNY_API_KEY and ELECTRIC_HNY_DATASET environment variables
    # and specify the Honeycomb URL in ELECTRIC_OTLP_ENDPOINT to export traces directly to
    # Honeycomb, without the need to run an OpenTelemetry Collector.
    honeycomb_api_key = env!("ELECTRIC_HNY_API_KEY", :string, nil)
    honeycomb_dataset = env!("ELECTRIC_HNY_DATASET", :string, nil)

    otlp_headers =
      Enum.reject(
        [
          {"x-honeycomb-team", honeycomb_api_key},
          {"x-honeycomb-dataset", honeycomb_dataset}
        ],
        fn {_, val} -> is_nil(val) end
      )

    resource = %{
      service: %{name: service_name, version: Electric.version()},
      instance: %{id: instance_id}
    }

    # We must populate otel_metric_exporter's app env to configure its LogHandler
    # and provide base config for the metrics export.
    config :otel_metric_exporter,
      otlp_endpoint: otlp_endpoint,
      otlp_headers: Map.new(otlp_headers),
      # The `name` resource attribute will be inherited by both metric and log events. This is
      # an artifact of otel_metric_exporter's implementation. With some more effort, we could
      # allow setting different names for the two types of events.
      resource: Map.put(resource, :name, "metrics")

    Electric.Telemetry.OpenTelemetry.Config.configure(
      otlp_endpoint: otlp_endpoint,
      otlp_headers: otlp_headers,
      otel_resource: resource,
      otel_debug?: otel_debug?
    )

    config :electric, :logger, [
      {:handler, :otel_log_handler, OtelMetricExporter.LogHandler,
       %{
         config: %{
           resource: %{name: "logs"},
           # Cap the OLP mailbox so log bursts shed events instead of letting
           # the queue grow unbounded. `sync_mode_qlen` is intentionally omitted:
           # OtelMetricExporter.LogHandler forces `sync_mode_qlen == drop_mode_qlen`,
           # so setting it is a no-op.
           drop_mode_qlen: 2000,
           flush_qlen: 5000,
           metadata_map: %{
             request_id: "http.request_id",
             stack_id: "source_id",
             shape_handle: "shape.handle",
             received_transaction_xid: "received.transaction.xid",
             received_transaction_num_changes: "received.transaction.num_changes",
             received_transaction_lsn: "received.transaction.lsn",
             publication_alter_drop_tables: "publication.alter.drop_tables",
             publication_alter_add_tables: "publication.alter.add_tables",
             publication_alter_set_tables: "publication.alter.set_tables",
             number_to_expire: "shape.expiry.number_to_expire",
             max_shapes: "shape.expiry.max_shapes"
           }
         }
       }}
    ]
  else
    # Disable opentelemetry_exporter.
    #
    # Without any explicit config, opentelemetry starts some resource detectors and initializes
    # otel_batch_processor which then tries to communicate with a remote OTLP server
    # (localhost:4318 by default) periodically.
    #
    # We don't want any of that unless OpenTelemetry export is explicitly enabled.
    config :opentelemetry, processors: []
  end
end

# Phoenix LiveDashboard Endpoint Configuration
#
# WARNING: The dashboard is completely unauthenticated and exposes internal
# system state (VM metrics, process info, ETS tables, etc.). It binds to loopback
# by default so enabling it never publishes that state on a public interface by
# accident. Set ELECTRIC_LIVE_DASHBOARD_ADDRESS to bind elsewhere only once the
# port is firewalled or otherwise restricted to trusted networks.
if live_dashboard_port do
  dashboard_ip =
    Electric.Config.parse_dashboard_listen_address!(
      env!("ELECTRIC_LIVE_DASHBOARD_ADDRESS", :string, nil),
      env!("ELECTRIC_LISTEN_ON_IPV6", :boolean, false)
    )

  config :electric, Electric.LiveDashboard.Endpoint,
    adapter: Bandit.PhoenixAdapter,
    http: [
      port: live_dashboard_port,
      ip: dashboard_ip
    ],
    server: true,
    render_errors: [
      formats: [html: Electric.LiveDashboard.ErrorView],
      layout: false
    ],
    live_view: [signing_salt: "r5zw+GcXjt3wP3Z/snFRqQ5uH2cm8Vb7ldc8t0POZdo="],
    secret_key_base: Base.encode64(:crypto.strong_rand_bytes(48)),
    pubsub_server: Electric.PubSub
end
