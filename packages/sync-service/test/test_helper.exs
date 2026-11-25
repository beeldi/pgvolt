# The process registry is implicitly used by processes in the dev, prod and test environments alike.
#
# Explicitly start the process registry here since the OTP application does not start a
# supervision tree in the test environment.
# Registry.start_link(name: Electric.Application.process_registry(), keys: :unique)

ExUnit.configure(formatters: [JUnitFormatter, ExUnit.CLIFormatter])

# assert_receive_timeout bounds how long a message may take to arrive, which is a property of
# scheduler contention rather than of anything the suite asserts — and it costs nothing when
# the message shows up promptly, because the wait ends on delivery. At 400ms a coverage-
# instrumented CI leg running ~2500 async tests loses one of them to delivery latency most
# runs, always a different one. Raising it removes that class of false red and only delays
# reporting a genuine missing message. refute_receive_timeout is deliberately left alone: that
# one is paid in full on every passing assertion.
ExUnit.start(
  assert_receive_timeout: String.to_integer(System.get_env("ASSERT_RECEIVE_TIMEOUT") || "5000"),
  exclude:
    [:slow, :oracle, :performance, :memory] ++
      if(System.get_env("PUBLIC_DNS_UNAVAILABLE") in ["1", "true"], do: [:public_dns], else: []),
  capture_log: true
)

# Start electric_client application directly, bypassing OTP's dependency resolution.
# This avoids a circular dependency: electric_client has :electric as an optional dep,
# which gets added to its applications list when compiled in sync-service context,
# causing a deadlock when OTP tries to start both applications.
{:ok, _} = Electric.Client.Application.start(:normal, [])

skip_repatch_prewarm? = System.get_env("SKIP_REPATCH_PREWARM") == "true"

# Repatch in async tests has lazy recompilation issues, so as a temporary fix
# we force recompilation in the setup. The issue is tracked here:
# https://github.com/hissssst/repatch/issues/2
#
# Repatch does slow down execution so if needed this prewarm can be skipped
# by setting the SKIP_REPATCH_PREWARM environment variable to "true".
# Skipping the prewarm is useful for the postgres oracle tests where performance
# is critical for high load.
#
# Only the recompile list is skippable: `Repatch.setup/1` creates the ETS tables every patch
# reads, so not calling it makes `Repatch.patch/4` raise for the whole lane.
prewarmed_modules =
  if skip_repatch_prewarm? do
    []
  else
    [
      # IMPORTANT: When adding a new Repatch.patch(..., mode: :shared, ...) in any
      # test file, add the target module to this list. Omitting it causes rare async
      # test failures because Repatch recompiles modules on first patch, which
      # destroys Erlang trace patterns, invalidates ETS table references, and breaks
      # anonymous function closures in concurrent tests. Pre-warming here triggers
      # the recompilation once at startup so subsequent patches only update ETS hooks.
      Postgrex,
      Plug.Conn,
      # Patched locally, and run by every async test that deletes a shape.
      Electric.AsyncDeleter,
      Electric.StatusMonitor,
      Electric.Telemetry.Sampler,
      Electric.Connection.Manager,
      Electric.Connection.Restarter,
      Electric.Postgres.Configuration,
      Electric.Postgres.Inspector,
      Electric.Postgres.ReplicationClient.SlotIncarnation,
      Electric.Postgres.SnapshotQuery,
      Electric.Replication.PersistentReplicationState,
      Electric.Replication.PublicationManager,
      Electric.Replication.PublicationManager.Configurator,
      Electric.Replication.PublicationManager.RelationTracker,
      Electric.Replication.ShapeLogCollector,
      Electric.ShapeCache,
      Electric.ShapeCache.PureFileStorage,
      Electric.ShapeCache.ShapeCleaner,
      Electric.ShapeCache.ShapeStatus,
      Electric.ShapeCache.ShapeStatus.ShapeDb,
      Electric.ShapeCache.ShapeStatus.ShapeDb.Connection,
      # Patched locally, but run by every async test with a shape db: a call made while Repatch
      # compiles it loads the original back, and Repatch's retry then purges the module, which
      # kills every process still running the old code.
      Electric.ShapeCache.ShapeStatus.ShapeDb.Sqlite3,
      Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer,
      # Patched locally, and run by every async test that opens a shape's files.
      Electric.ShapeCache.PureFileStorage.FileInfo,
      Electric.ShapeCache.ShapeStatusOwner,
      # Patched by async tests too, and read by every async test that stores a shape log.
      Electric.ShapeCache.PureFileStorage.LogFile,
      Electric.ShapeCache.Storage,
      Electric.Shapes.Consumer.Effects,
      Electric.Shapes.Consumer.Snapshotter,
      Electric.Shapes.ConsumerRegistry,
      Electric.Shapes.DynamicConsumerSupervisor,
      Electric.Shapes.Shape,
      Electric.Shapes.Supervisor,
      Electric.Timeline,
      Electric.Utils,
      :otel_tracer
    ]
  end

# History off: it logs every call any process makes into a patched module, nothing prunes the
# entries of processes other than the test's own, and each test's cleanup scans the whole log.
# A test that needs to know a call happened patches the function to report it.
Repatch.setup(recompile: prewarmed_modules, enable_history: false)
