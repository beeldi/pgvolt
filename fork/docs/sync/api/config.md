---
title: Sync service
description: >-
  Configuration options for the Electric sync engine.
outline: deep
---

<script setup>
import EnvVarConfig from '../../../src/components/EnvVarConfig.vue'
</script>

# Sync service configuration

This page documents the config options for [self-hosting](https://electric-sql.com/docs/sync/guides/deployment) the [Electric sync engine](https://electric-sql.com/sync/). It mirrors upstream's VitePress reference with this fork's additions and changed defaults inline.

> [!Warning] Advanced only
> You don't need to worry about this if you're using [Electric Cloud](https://electric-sql.com/cloud/).
>
> Also, the only required configuration options are `DATABASE_URL` and `ELECTRIC_SECRET`.

## Configuration

The sync engine is an [Elixir](https://elixir-lang.org) application in `packages/sync-service` (upstream publishes Docker images at [electricsql/electric](https://hub.docker.com/r/electricsql/electric); build from this repository to get the fork's additions documented below).

Configuration options can be provided as environment variables, e.g.:

```shell
docker run \
    -e "DATABASE_URL=postgresql://..." \
    -e "ELECTRIC_DB_POOL_SIZE=10" \
    -p 3000:3000 \
    electricsql/electric
```

These are passed into the application via [packages/sync-service/config/runtime.exs](../../../../packages/sync-service/config/runtime.exs).

## Database

### DATABASE_URL

<EnvVarConfig
    name="DATABASE_URL"
    required={true}
    example="postgresql://user:password@example.com:54321/electric">

Postgres connection string. Used to connect to the Postgres database.

The connection string must be in the [libpg Connection URI format](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS) of `postgresql://[userspec@][hostspec][/dbname][?sslmode=<sslmode>]`.

The `userspec` section of the connection string specifies the database user that Electric connects to Postgres as. They must have the `REPLICATION` role.

For a secure connection, set the `sslmode` query parameter to `require`.

Only `sslmode` is read from the query. Any other key is ignored, and each ignored key is named in a warning at startup, without its value.

This fork also accepts `verify-ca` and `verify-full`. Both verify the server's certificate chain against `ELECTRIC_DATABASE_CA_CERTIFICATE_FILE`, or against the system's trusted roots when no file is set. `verify-full` also checks that the certificate matches the hostname. A CA file turns verification on for the other modes too, as it does for `psql`. A failed SSL connection falls back to an unencrypted one only when `sslmode` is unset, `allow` or `prefer` and no CA file is set, and that fallback is always logged.

</EnvVarConfig>

### ELECTRIC_POOLED_DATABASE_URL

<EnvVarConfig
    name="ELECTRIC_POOLED_DATABASE_URL"
    defaultValue="DATABASE_URL"
    example="postgresql://user:password@example-pooled.com:54321/electric">

Postgres connection string. Used to connect to the Postgres database for anything but the replication, will default to the same as `DATABASE_URL` if not provided.

The connection string must be in the [libpg Connection URI format](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS) of `postgresql://[userspec@][hostspec][/dbname][?sslmode=<sslmode>]`.

The `userspec` section of the connection string specifies the database user that Electric connects to Postgres as. This can point to a connection pooler and does not need a `REPLICATION` role as it does not handle the replication.

This should point to the same instance as the main database URL, as Electric relies on transaction information for consistency reasons.

For a secure connection, set the `sslmode` query parameter to `require`.

Only `sslmode` is read from the query. Any other key is ignored, and each ignored key is named in a warning at startup, without its value.

This used to be called `ELECTRIC_QUERY_DATABASE_URL`, but that name is deprecated and will be removed in a future release.

</EnvVarConfig>

### ELECTRIC_DATABASE_USE_IPV6

<EnvVarConfig
    name="ELECTRIC_DATABASE_USE_IPV6"
    defaultValue="false"
    example="true">

Set to `true` to prioritise connecting to the database over IPv6. Electric will fall back to an IPv4 DNS lookup if the IPv6 lookup fails.

</EnvVarConfig>

### ELECTRIC_DB_POOL_SIZE

<EnvVarConfig
    name="ELECTRIC_DB_POOL_SIZE"
    defaultValue="20"
    example="10">

How many connections Electric opens as a pool for handling shape queries.

</EnvVarConfig>

### ELECTRIC_DATABASE_TCP_KEEPALIVE_IDLE

<EnvVarConfig
    name="ELECTRIC_DATABASE_TCP_KEEPALIVE_IDLE"
    optional="true"
    example="30s">

Enable TCP keepalive on database connections and set how long a connection may sit idle before the kernel starts sending probes.

By default Electric leaves the operating system's settings in place. Setting this lets an idle replication connection detect the loss and reconnect sooner.

See also `ELECTRIC_DATABASE_TCP_USER_TIMEOUT`, which bounds detection while data is actually in flight.

</EnvVarConfig>

### ELECTRIC_DATABASE_TCP_KEEPALIVE_INTERVAL

<EnvVarConfig
    name="ELECTRIC_DATABASE_TCP_KEEPALIVE_INTERVAL"
    optional="true"
    example="10s">

How long to wait between individual TCP keepalive probes. Enables keepalive if set on its own.

</EnvVarConfig>

### ELECTRIC_DATABASE_TCP_KEEPALIVE_COUNT

<EnvVarConfig
    name="ELECTRIC_DATABASE_TCP_KEEPALIVE_COUNT"
    optional="true"
    example="3">

How many unanswered TCP keepalive probes before the connection is dropped. Enables keepalive if set on its own.

</EnvVarConfig>

### ELECTRIC_DATABASE_TCP_USER_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_DATABASE_TCP_USER_TIMEOUT"
    optional="true"
    example="60s">

The maximum time data may remain unacknowledged before the connection is dropped (Linux `TCP_USER_TIMEOUT`).

This complements the keepalive settings above. Keepalive probes only run while a connection is idle, whereas this bounds how long Electric keeps retransmitting a write to a peer that has stopped responding.

</EnvVarConfig>

### ELECTRIC_DATABASE_CA_CERTIFICATE_FILE

<EnvVarConfig
    name="ELECTRIC_DATABASE_CA_CERTIFICATE_FILE"
    optional="true"
    example="/root/.postgresql/root.crt">

The path on local disk to a file containing trusted certificate(s) that Electric will use to verify the database server identity.

Trusted certificates are those that have been signed by trusted certificate authorities (CA); they are also known as root certificates. Every operating system and most web browsers include a bundle of well-known root certificates (aka CA store). You can instruct Electric to use the default bundle provided by your OS by specifying an absolute path to it. [This page](https://neon.com/docs/connect/connect-securely#location-of-system-root-certificates) from Neon lists the typical locations for different operating systems.

Some managed Postgres providers such as Supabase and DigitalOcean use a self-signed root certificate that won't be found in OS-specific CA stores. If you're using one of those, download the trusted certificate from the provider's website and put it somewhere on your local disk where Electric can access it.

**Certificate verification and `sslmode`**

This fork accepts `sslmode=verify-ca` and `sslmode=verify-full` in `DATABASE_URL`, as described under `DATABASE_URL`. When you configure Electric with a trusted certificate file, every connection that uses TLS verifies the server identity, whatever its `sslmode`, and Electric refuses to open it if the verification does not succeed. A connection with `sslmode=disable` does not use TLS: `ELECTRIC_POOLED_DATABASE_URL` may still set it when a certificate file is configured.

Note, however, that setting `sslmode=disable` in `DATABASE_URL` and enabling certificate verification at the same time will result in a startup error.

</EnvVarConfig>

### ELECTRIC_REPLICATION_STREAM_ID

<EnvVarConfig
    name="ELECTRIC_REPLICATION_STREAM_ID"
    defaultValue="default"
    example="my-app">

Suffix for the logical replication publication and slot name.

</EnvVarConfig>

### CLEANUP_REPLICATION_SLOTS_ON_SHUTDOWN

<EnvVarConfig
    name="CLEANUP_REPLICATION_SLOTS_ON_SHUTDOWN"
    defaultValue="false"
    example="true">

When set to `true`, Electric creates a [temporary replication slot](https://www.postgresql.org/docs/current/protocol-replication.html) that is automatically dropped when the database connection closes. This is useful for ephemeral deployments where each container has its own storage and replication slots don't need to persist across restarts.

> [!Warning] Unclean shutdowns cause shape rotations
> If Electric crashes or loses its database connection (e.g., during a network partition), the temporary slot is lost. The next instance starts with a fresh slot and clients connected to old shapes will receive `409` (must-refetch) responses, requiring a full resync.

See the [Upgrading guide](https://electric-sql.com/docs/sync/guides/upgrading#temporary-replication-slots) for more context on using temporary slots.

</EnvVarConfig>

### ELECTRIC_TEMPORARY_REPLICATION_SLOT_USE_RANDOM_NAME

<EnvVarConfig
    name="ELECTRIC_TEMPORARY_REPLICATION_SLOT_USE_RANDOM_NAME"
    defaultValue="false"
    example="true">

When used with [`CLEANUP_REPLICATION_SLOTS_ON_SHUTDOWN=true`](#cleanup_replication_slots_on_shutdown), generates a random replication slot name and uses the same suffix for the automatically managed publication. Concurrent instances can then manage their own publications independently. An explicitly configured publication name and the stream-based default under [manual table publishing](#electric_manual_table_publishing) are preserved.

Electric drops its generated publication on clean shutdown. After an abrupt stop, a generated publication can remain and can be removed manually once the instance has stopped. Electric creates the publication before the slot, so an absent slot alone cannot identify an abandoned publication.

Has no effect unless `CLEANUP_REPLICATION_SLOTS_ON_SHUTDOWN` is also set to `true`.

</EnvVarConfig>

### ELECTRIC_REPLICATION_IDLE_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_REPLICATION_IDLE_TIMEOUT"
    defaultValue="0"
    example="5min">

After seeing no activity on the logical replication stream for this long, Electric will close all of its database connections. This allows the database server to scale-to-zero on supported providers.

While Electric is in the scaled-down mode, an incoming shape request will cause it to reopen database connections and restart the logical replication stream. The request itself will be held until it can be processed as usual to return a proper response.

The default value is 0, meaning the connection scaling down is disabled and Electric will keep its database connections open permanently.

**Important note on WAL growth**

Avoid setting this timeout if your database sees constant or frequent writes.

When Electric isn't streaming from the database, its replication slot is inactive. Postgres will continue to retain WAL files needed for the slot, since they are required to resume replication later. Over time, this can cause storage growth proportional to the volume of writes on the primary database, regardless of whether those writes target tables for which Electric has active shapes or not.

Once Electric reconnects and replication catches up, Postgres will automatically discard the no-longer-needed WAL segments. However, if the inactivity period is too long, the accumulated WAL may exceed available disk space, potentially interrupting database operations.

</EnvVarConfig>

### ELECTRIC_REPLICATION_HEARTBEAT_INTERVAL

<EnvVarConfig
    name="ELECTRIC_REPLICATION_HEARTBEAT_INTERVAL"
    optional="true"
    example="150s">

How often Electric writes a heartbeat into the logical replication stream: a non-transactional logical decoding message, `pg_logical_emit_message(false, 'electric_heartbeat', '')`, sent through the admin connection pool and read back on Electric's own slot. Electric then asks Postgres for logical decoding messages when it starts replication, and any message it reads counts as stream activity, whoever wrote it.

Set it together with `ELECTRIC_REPLICATION_IDLE_TIMEOUT` to turn the idle timeout into a stall check. A quiet database then never looks idle, because the heartbeat keeps arriving; a stream that stopped reading its socket still trips the timeout and reconnects on the next request. The interval must be at most half of the idle timeout.

Each heartbeat adds one small WAL record. Other logical replication clients receive it only if they enable logical decoding messages (`messages = 'true'`); verify that any other subscriber consuming logical messages ignores the `electric_heartbeat` prefix.

Unset by default, meaning no heartbeat and no logical decoding messages requested. When set, it must be a time greater than zero.

</EnvVarConfig>

### ELECTRIC_MANUAL_TABLE_PUBLISHING

<EnvVarConfig
    name="ELECTRIC_MANUAL_TABLE_PUBLISHING"
    defaultValue="false"
    example="true">

Set to `true` to disable automatic addition/removal of database tables from the publication in Postgres.

In order to receive realtime updates as soon as they are committed in Postgres, Electric maintains a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) inside the database and automatically adds tables to it for which shape subscriptions are established. This only works if Electric's database role owns the table or is granted the [group role](https://www.postgresql.org/docs/current/role-membership.html#ROLE-MEMBERSHIP) that owns the table.

If your permissions policies prevent Electric from using a role that can alter application tables, set this setting to `true` and manually add each table to the publication. See the [Manual Mode Setup guide](../guides/postgres-permissions.md#manual-mode-setup) for complete setup instructions, or use the following minimal steps:

```sql
BEGIN;
ALTER PUBLICATION electric_publication_default ADD TABLE <my table>;
ALTER TABLE <my table> REPLICA IDENTITY FULL;
COMMIT;
```

before requesting a new shape for that table.

</EnvVarConfig>

## Electric

### ELECTRIC_SUBSET_MAX_ROWS

<EnvVarConfig
    name="ELECTRIC_SUBSET_MAX_ROWS"
    example="10000">

Experimental cap on the rows a subset snapshot may return. Unset means there is no cap. Set a
positive integer up to `9223372036854775806`. Requests with a higher `limit` return `400`. Requests
without a `limit` return `400` if the result would exceed the cap.

</EnvVarConfig>

### ELECTRIC_SECRET

<EnvVarConfig
    name="ELECTRIC_SECRET"
    required={true}
    example="1U6ItbhoQb4kGUU5wXBLbxvNf">

Secret for shape requests to the [HTTP API](https://electric-sql.com/docs/sync/api/http). This is required unless `ELECTRIC_INSECURE` is set to `true`.
By default, the Electric API is public and authorises all shape requests against this secret.
More details are available in the [security guide](https://electric-sql.com/docs/sync/guides/security).

In this fork, a value that is empty or only whitespace stops the service at boot instead of being accepted as a secret. The secret is compared in constant time, is accepted in an `authorization: Bearer <secret>` header on `/v1/shape` and its subpaths (taking precedence over `?secret=` and `?api_secret=`), and every path under `/v1/shape` requires it.

</EnvVarConfig>

### ELECTRIC_INSECURE

<EnvVarConfig
    name="ELECTRIC_INSECURE"
    defaultValue="false"
    example="true">

When set to `true`, runs Electric in insecure mode and does not require an `ELECTRIC_SECRET`.
Use with caution.
API requests are unprotected and may risk exposing your database.
Good for development environments.
If used in production, make sure to [lock down access](https://electric-sql.com/docs/sync/guides/security#network-security) to Electric.

In this fork only `true`, `1`, `yes` and `on`, in any case, turn insecure mode on. Any other value, `off` and `no` included, keeps authentication on and requires `ELECTRIC_SECRET`.

</EnvVarConfig>

### ELECTRIC_INSTANCE_ID

<EnvVarConfig
    name="ELECTRIC_INSTANCE_ID"
    defaultValue="Electric.Utils.uuid4()"
    example="some-unique-instance-identifier">

A unique identifier for the Electric instance. Defaults to a randomly generated UUID.

</EnvVarConfig>

### ELECTRIC_SERVICE_NAME

<EnvVarConfig
    name="ELECTRIC_SERVICE_NAME"
    defaultValue="electric"
    example="my-electric-service">

Name of the electric service. Used as a resource name in OTEL traces and metrics.

</EnvVarConfig>

### ELECTRIC_LISTEN_ON_IPV6

<EnvVarConfig
    name="ELECTRIC_LISTEN_ON_IPV6"
    defaultValue="false"
    example="true">

By default, Electric binds to IPv4. Enable this to listen on IPv6 addresses as well.

</EnvVarConfig>

### ELECTRIC_TCP_SEND_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_TCP_SEND_TIMEOUT"
    defaultValue="30s"
    example="60s">
Timeout for sending a response chunk back to the client. Defaults to 30 seconds.

Slow response processing on the client or bandwidth restristrictions can cause TCP backpressure leading to the error message:

```
Error while streaming response: :timeout
```

This environment variable increases this timeout.

</EnvVarConfig>

### ELECTRIC_TCP_READ_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_TCP_READ_TIMEOUT"
    defaultValue="60s"
    example="180s">
Socket read timeout, which doubles as the HTTP keep-alive idle timeout: Electric closes a connection that has been waiting this long for its next request. Defaults to 60 seconds.

When Electric runs behind a connection-pooling reverse proxy or load balancer (AWS ALB, nginx, etc.), set this **above** the proxy's idle timeout. If the proxy's timeout is longer, the proxy can reuse a pooled connection at the same moment Electric closes it, and the resulting TCP reset surfaces to clients as an intermittent `502 Bad Gateway`. For example, behind an AWS ALB with the default 60-second idle timeout, set `ELECTRIC_TCP_READ_TIMEOUT=120s`.

</EnvVarConfig>

### ELECTRIC_STALLED_SERVE_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_STALLED_SERVE_TIMEOUT"
    defaultValue="60000"
    example="120000">

Added by this fork. Default: `60000` (60 seconds). How long, in milliseconds, a shape response may wait for its client to accept the next piece of the body (at most 256 KiB) before Electric terminates it. A client that stops reading would otherwise hold the response's memory and a file descriptor for as long as its connection lives. A terminated client resumes from its last offset. Each termination logs a warning with the shape handle and is counted as `electric_plug_serve_shape_reaped_count` on the Prometheus endpoint. `0` turns it off.

</EnvVarConfig>

### ELECTRIC_TWEAKS_HTTP2_MAX_RESET_STREAM_RATE

<EnvVarConfig
    name="ELECTRIC_TWEAKS_HTTP2_MAX_RESET_STREAM_RATE"
    defaultValue="10000/10s"
    example="500/10s">

Default: `10000/10s`. Maximum number of HTTP/2 stream resets (`RST_STREAM` frames) a single client connection may send within a time window, in the form `COUNT/DURATION` (for example `500/10s`), or `disabled`. When a connection exceeds the limit, Electric closes the whole connection and every request still in flight on it is dropped.

The default is higher than the HTTP server's own `500/10s`. Behind a reverse proxy that multiplexes many end clients onto a few upstream HTTP/2 connections, ordinary client-side cancellations of live requests count against the limit, and tripping it disconnects every other client sharing that connection. Set `disabled` only when something in front of Electric limits resets itself.

</EnvVarConfig>

### ELECTRIC_TWEAKS_MAX_REQUEST_LINE_LENGTH

<EnvVarConfig
    name="ELECTRIC_TWEAKS_MAX_REQUEST_LINE_LENGTH"
    optional="true"
    example="65536">

Added by this fork. The longest HTTP/1 request line Electric accepts, in bytes. Unset, Bandit's default of 10,000 bytes applies. Raise it when shapes with a long `where` clause or many `params` would otherwise have to be split.

</EnvVarConfig>

### ELECTRIC_SHAPE_CHUNK_BYTES_THRESHOLD

<EnvVarConfig
    name="ELECTRIC_SHAPE_CHUNK_BYTES_THRESHOLD"
    defaultValue="10485760"
    example="20971520">

Limit the maximum size of a shape log response, to ensure they are cached by
upstream caches. Defaults to 10MB (10 _ 1024 _ 1024).

See [#1581](https://github.com/electric-sql/electric/issues/1581) for context.

</EnvVarConfig>

### ELECTRIC_PORT

<EnvVarConfig
    name="ELECTRIC_PORT"
    defaultValue="3000"
    example="8080">

Port that the [HTTP API](https://electric-sql.com/docs/sync/api/http) is exposed on.

</EnvVarConfig>

### ELECTRIC_SHAPE_SUSPEND_CONSUMER

<EnvVarConfig
    name="ELECTRIC_SHAPE_SUSPEND_CONSUMER"
    defaultValue="false"
    example="true">

Whether to terminate idle shape consumer processes after `ELECTRIC_SHAPE_HIBERNATE_AFTER` seconds. This saves on memory at the cost of slightly higher CPU usage. When receiving a transaction that contains changes matching a given shape, a consumer process is started to handle the update. If more transactions matching the shape appear within the time defined by `ELECTRIC_SHAPE_HIBERNATE_AFTER` then the consumer will remain active, if not it will be terminated.

If set to `false` the consumer processes will [hibernate](https://www.erlang.org/doc/apps/erts/erlang#hibernate/3) instead of terminating, meaning they still occupy some memory but are inactive until passed transaction operations to process.

If you enable this feature then you should configure `ELECTRIC_SHAPE_HIBERNATE_AFTER` to match the usage patterns of your application to avoid unnecessary process churn.

</EnvVarConfig>

### ELECTRIC_SHAPE_HIBERNATE_AFTER

<EnvVarConfig
    name="ELECTRIC_SHAPE_HIBERNATE_AFTER"
    defaultValue="30s"
    example="5000ms">

The amount of time a consumer process remains active without receiving transaction operations before either [hibernating](https://www.erlang.org/doc/apps/erts/erlang#hibernate/3) or terminating (if `ELECTRIC_SHAPE_SUSPEND_CONSUMER` is `true`).

</EnvVarConfig>

### ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE"
    defaultValue="false"
    example="true">

Run the SQLite database holding active shape data over a single read-write connection, rather than a single writer and multiple reader connections. This avoids corruption issues when holding shape data on an NFS share (such as [AWS EFS](https://aws.amazon.com/efs/)). Set this to `true` and `ELECTRIC_SHAPE_DB_STORAGE_DIR=:memory:` to use an in-memory database.

</EnvVarConfig>

### ELECTRIC_SHAPE_DB_STORAGE_DIR

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_STORAGE_DIR"
    defaultValue="$ELECTRIC_STORAGE_DIR"
    example="/var/db/electric">

The base path for the shapes SQLite database. Set this to a local, non-networked drive, for consistency and performance reasons when hosting shape data on a network volume, such as [AWS EFS](https://aws.amazon.com/efs/). This is an alternative to `ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE` when a single read-write connection does not provide enough performance. Note that if the `ELECTRIC_SHAPE_DB_STORAGE_DIR` is ephemeral, e.g. instance specific, then on a re-deployment the shape log data in `$ELECTRIC_STORAGE_DIR` (hosted on EFS) will be ignored by the system, which will start empty.

Enable `ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE` and set `ELECTRIC_SHAPE_DB_STORAGE_DIR=:memory:` to use an ephemeral in-memory shape database.

</EnvVarConfig>

### ELECTRIC_SHAPE_DB_SYNCHRONOUS

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_SYNCHRONOUS"
    defaultValue="OFF"
    example="NORMAL">

The value of the [`synchronous` PRAGMA](https://sqlite.org/pragma.html#pragma_synchronous) set on every connection to the shape db.


</EnvVarConfig>

### ELECTRIC_SHAPE_DB_CACHE_SIZE

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_CACHE_SIZE"
    defaultValue="4096KiB"
    example="8MiB">

The value of the [`cache_size` PRAGMA](https://sqlite.org/pragma.html#pragma_cache_size) set on every connection to the shape db. Higher values will result in more memory usage but improved performance. Accepts values in bytes, or with a suffix such as "1024KB", "500KiB", "5MB", "8MiB", etc.

</EnvVarConfig>

### ELECTRIC_SHAPE_DB_ENABLE_STATS

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_ENABLE_STATS"
    defaultValue="false"
    example="true">

Set `ELECTRIC_SHAPE_DB_ENABLE_STATS=true` to enable collection of statistics for the SQLite shape db system.

</EnvVarConfig>

### ELECTRIC_SHAPE_DB_ENABLE_MEMORY_STATS

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_ENABLE_MEMORY_STATS"
    defaultValue="false"
    example="true">

Set `ELECTRIC_SHAPE_DB_ENABLE_STATS=true` and `ELECTRIC_SHAPE_DB_ENABLE_MEMORY_STATS=true` to enable collection of memory usage statistics for the SQLite shape db system. Requires the loading of a pre-compiled extension so may cause issues on some architectures and is disabled by default for stability reasons.

</EnvVarConfig>

### ELECTRIC_SHAPE_DB_BOOT_READ_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_SHAPE_DB_BOOT_READ_TIMEOUT"
    defaultValue="300s"
    example="10min">

Added by this fork. Default: `300s`. How long a boot-time read of the shape store may wait for a connection: the reads that list, count and fold every stored shape. Other reads keep the 10-second default, which a store with many shapes can outrun and fail the boot.

Accepts a number of milliseconds, or a number followed by `ms`, `s`, `sec`, `m` or `min`.

</EnvVarConfig>

### ELECTRIC_MAX_CONCURRENT_REQUESTS

<EnvVarConfig
    name="ELECTRIC_MAX_CONCURRENT_REQUESTS"
    defaultValue='{"initial": 300, "existing": 10000}'
    example='{"initial": 500, "existing": 30000}'>

Maximum number of concurrent HTTP requests Electric will serve, as a JSON object with two keys:

- `initial` &mdash; limit for initial sync requests (requests with `offset=-1`). Default: **300**.
- `existing` &mdash; limit for ongoing requests (live long-polls and catch-up requests). Default: **10000**.

When the limit is exceeded, Electric responds with `503` and a `Retry-After` header. See the [troubleshooting guide](https://electric-sql.com/docs/sync/guides/troubleshooting#_503-mdash-concurrent-request-limit-exceeded) for details.

Each `live=true` long-poll request holds the connection open for up to 20 seconds, so the effective limit is determined by the number of concurrent shape subscriptions across all connected clients. Putting a [CDN with request collapsing](https://electric-sql.com/docs/sync/api/http#collapsing-live-requests) in front of Electric is the recommended way to handle high connection counts.

</EnvVarConfig>

### ELECTRIC_MAX_SHAPES

<EnvVarConfig
    name="ELECTRIC_MAX_SHAPES"
    optional="true"
    example="10000">

Maximum number of shapes that Electric will serve concurrently. When unset (the default), there is no limit on the number of shapes.

When set, Electric periodically evicts the least recently used shapes to stay within the limit.

Clients subscribed to an expired shape will receive a `409 Conflict` response, prompting them to create a new shape subscription with a fresh initial sync.

This replaces the previous `ELECTRIC_EXPERIMENTAL_MAX_SHAPES` environment variable, which is deprecated and will be removed in a future release.

</EnvVarConfig>

### ELECTRIC_CONSUMER_PARTITIONS

<EnvVarConfig
    name="ELECTRIC_CONSUMER_PARTITIONS"
    optional="true"
    example="128">

Consumer processes are partitioned across some number of supervisors to improve launch and shutdown time. If `ELECTRIC_MAX_SHAPES` is set the number of partitions will scale to fit this maximum but if not, it defaults to the number of CPU cores. If you want to improve shutdown performance without setting an upper limit on the number of shapes, then set this to roughly your expected number of shapes / 4000.

</EnvVarConfig>

## Feature Flags

Feature flags enable staged rollouts of new capabilities that are not yet enabled by default in production.

### ELECTRIC_FEATURE_FLAGS

<EnvVarConfig
    name="ELECTRIC_FEATURE_FLAGS"
    defaultValue="">

Comma-separated list of feature flags to enable. This fork has one:

- `full_xid_source_positions` lets shape requests ask for full transaction provenance with `source_positions=full_xid`. Without the flag such a request gets `426`. See [full transaction provenance](../../../../packages/sync-service/README.md#full-transaction-provenance-experimental) in the sync service README.

</EnvVarConfig>

## Caching

### ELECTRIC_CACHE_MAX_AGE

<EnvVarConfig
    name="ELECTRIC_CACHE_MAX_AGE"
    defaultValue="60"
    example="5">

Default `max-age` for the cache headers of the HTTP API.

</EnvVarConfig>

### ELECTRIC_CACHE_STALE_AGE

<EnvVarConfig
    name="ELECTRIC_CACHE_STALE_AGE"
    defaultValue="300"
    example="5">

Default `stale-age` for the cache headers of the HTTP API.

</EnvVarConfig>

## Storage

### ELECTRIC_PERSISTENT_STATE

<EnvVarConfig
    name="ELECTRIC_PERSISTENT_STATE"
    defaultValue="FILE"
    example="MEMORY">

Where to store shape metadata. Defaults to storing on the filesystem.
If provided must be one of `MEMORY` or `FILE`.

</EnvVarConfig>

### ELECTRIC_STORAGE

<EnvVarConfig
    name="ELECTRIC_STORAGE"
    defaultValue="FAST_FILE"
    example="MEMORY">

Where to store shape logs. Defaults to storing on the filesystem.
If provided must be one of `MEMORY` or `FAST_FILE`.

</EnvVarConfig>

### ELECTRIC_STORAGE_DIR

<EnvVarConfig
    name="ELECTRIC_STORAGE_DIR"
    defaultValue="./persistent"
    example="/var/example">

Path to root folder for storing data on the filesystem.

This must be persistent storage that survives restarts. See [Optimizing for disk](https://electric-sql.com/docs/sync/guides/deployment#optimizing-for-disk) in the deployment guide for what to run it on.

</EnvVarConfig>

### ELECTRIC_COMPACTION_PERIOD

<EnvVarConfig
    name="ELECTRIC_COMPACTION_PERIOD"
    defaultValue="600000"
    example="3600000">

Added by this fork. Default: `600000` (10 minutes). The period, in milliseconds, between two compactions of a shape's log. Each run is scheduled at this period plus or minus half of it, and compacts only chunks already synced to disk, leaving the rest to a later run. It applies only to shapes requested with `experimental_compaction=true`.

</EnvVarConfig>

## Replication pipeline

Added by this fork. These options bound how long the pipeline waits on a shape that stopped making progress, so that one stuck shape cannot hold back the replication slot for every other shape.

### ELECTRIC_FLUSH_STALL_BOOT_GRACE

<EnvVarConfig
    name="ELECTRIC_FLUSH_STALL_BOOT_GRACE"
    defaultValue="10min"
    example="20min">

Default: `10min`. How long a shape's writer may go without flushing before its first flush, at boot or after it restarts. Writers start one after another behind the restore, so the steady grace of `ELECTRIC_FLUSH_STALL_GRACE_PERIOD` (1 minute) would remove shapes that are only queued. Once a writer has flushed, the steady grace applies.

Accepts the same formats as `ELECTRIC_SHAPE_DB_BOOT_READ_TIMEOUT`.

</EnvVarConfig>

### ELECTRIC_FLUSH_DEFERRAL_ROUND_LIMIT

<EnvVarConfig
    name="ELECTRIC_FLUSH_DEFERRAL_ROUND_LIMIT"
    defaultValue="10"
    example="20">

Default: `10`. How many stall challenges in a row a writer may answer by asking for more time without flushing anything. After that, the next stall check removes its shape, and clients of that shape refetch it.

</EnvVarConfig>

### ELECTRIC_GATHER_STALL_TIMEOUT

<EnvVarConfig
    name="ELECTRIC_GATHER_STALL_TIMEOUT"
    defaultValue="10min"
    example="20min">

Default: `10min`. How long the log collector waits for the router workers to acknowledge a transaction, when `ELECTRIC_ROUTER_SHARDS` is above 1, and, with any shard count, for a busy shape catalog when it removes the shape of a consumer that crashed during delivery. Past it, the collector restarts and streams again from the replication slot instead of waiting forever.

</EnvVarConfig>

### ELECTRIC_TXN_BUFFER_MAX_CHANGES

<EnvVarConfig
    name="ELECTRIC_TXN_BUFFER_MAX_CHANGES"
    defaultValue="250000"
    example="1000000">

Default: `250000`. How many changes of one transaction a shape may hold in memory: a shape waiting for its snapshot, or a subquery shape or one of its inner shapes. Past it the shape is removed and its clients refetch it, as after a truncate, while the other shapes keep going. Upstream holds them without a limit.

</EnvVarConfig>

## Performance switches

Added by this fork. Each switch is off when unset (`false`, or `1` for numeric concurrency options), and with all of them off the replication pipeline behaves as upstream's does. CI runs the sync service test suite, the oracle tests and part of the integration tests with all of them on as well as off. They are experimental: their on-disk effects may change between releases.

`ELECTRIC_SHARED_CONVERSION` is the base the arena, fragment blocks and worker prepare build on. Each of those logs a warning and stays off without it. The arena, fragment blocks and the journal need the default file storage (`ELECTRIC_STORAGE=FAST_FILE`), and log a warning and stay off with any other.

### ELECTRIC_SHARED_CONVERSION

<EnvVarConfig
    name="ELECTRIC_SHARED_CONVERSION"
    defaultValue="false"
    example="true">

Default: `false`. Converts an insert or a delete once for all the shapes that would convert it the same way: shapes on the same table, with the same columns and modes, no subquery and no move tags. A change is shared only when routing matched it exactly.

</EnvVarConfig>

### ELECTRIC_SHARED_PAYLOAD_ARENA

<EnvVarConfig
    name="ELECTRIC_SHARED_PAYLOAD_ARENA"
    defaultValue="false"
    example="true">

Default: `false`. Writes each shared change once, to `payload-arena` under the storage directory, and stores a checksummed reference to it in each shape's log instead of a copy. Requires `ELECTRIC_SHARED_CONVERSION`.

Shapes opened while the arena is on, or while arena files remain on disk, are stored in on-disk format version 3. This fork reads every version. Upstream Electric reads only version 1: moving back to it drops those shapes, and their clients resync them.

</EnvVarConfig>

### ELECTRIC_PAYLOAD_ARENA_RECLAIM_MIN_BYTES

<EnvVarConfig
    name="ELECTRIC_PAYLOAD_ARENA_RECLAIM_MIN_BYTES"
    defaultValue="1073741824"
    example="0">

Default: `1073741824` (1 GiB). Used only with `ELECTRIC_SHARED_PAYLOAD_ARENA`. A boot reclaims the arena generations left by earlier boots only once they hold this many bytes, and under it removes only the empty ones. The reclaim scans the log directory of every stored shape before the stack goes active, which takes minutes with a large shape count. `0` reclaims at every boot.

</EnvVarConfig>

### ELECTRIC_SHARED_FRAGMENT_BLOCKS

<EnvVarConfig
    name="ELECTRIC_SHARED_FRAGMENT_BLOCKS"
    defaultValue="false"
    example="true">

Default: `false`. Frames a run of three or more consecutive changes that go to the same set of shapes once, and hands the block to each of them. Requires `ELECTRIC_SHARED_CONVERSION`.

</EnvVarConfig>

### ELECTRIC_ROUTER_SHARDS

<EnvVarConfig
    name="ELECTRIC_ROUTER_SHARDS"
    defaultValue="1"
    example="4">

Default: `1`. Routes changes to shapes across this many router workers instead of in the log collector alone. Each shape is placed on one worker by its handle. Shapes with subquery dependencies stay on the collector. At most 255: a larger value stops the stack at start.

</EnvVarConfig>

### ELECTRIC_WORKER_PREPARE

<EnvVarConfig
    name="ELECTRIC_WORKER_PREPARE"
    defaultValue="false"
    example="true">

Default: `false`. Moves the preparation of shared changes from the log collector onto the router workers. Requires `ELECTRIC_SHARED_CONVERSION` and `ELECTRIC_ROUTER_SHARDS` of 2 or more, and logs a warning and stays off otherwise.

</EnvVarConfig>

### ELECTRIC_FILTER_ROUTING_MEMO

<EnvVarConfig
    name="ELECTRIC_FILTER_ROUTING_MEMO"
    defaultValue="false"
    example="true">

Default: `false`. Remembers which shapes a row matched, keyed by the table and the values of the columns the shapes filter on. It applies only to tables whose shapes all filter on plain columns, without subqueries, and never stores a match routing could not decide exactly. The memo is cleared at 8,192 entries or 32 MiB.

</EnvVarConfig>

### ELECTRIC_ACK_AFTER_JOURNAL

<EnvVarConfig
    name="ELECTRIC_ACK_AFTER_JOURNAL"
    defaultValue="false"
    example="true">

Default: `false`. Writes each transaction to a journal, `txn-journal` under the storage directory, syncs it at commit, and acknowledges the replication slot once the journal holds the transaction, before it reaches the shapes. While waiting for the PostgreSQL advisory lock, the service serves existing shapes read-only with each shape's persisted watermark. After acquiring the lock, it waits for any outstanding journal replay before admitting requests on the local writer path, and a request admitted earlier reports the shape's own offset as its watermark until the replay ends. Requests that reach the stack readiness timeout during replay receive a retryable `503`.

- Turning it off is safe: a journal left by an earlier boot is still replayed.
- Turning it back on after running with it off can stop the boot with an error that asks for the shapes to be resynced, when a journal from the earlier period is still on disk.
- Upstream Electric never reads the journal. Turn the switch off and restart once on this fork before moving back to upstream.

</EnvVarConfig>

### ELECTRIC_KEEP_SHAPES_ON_PROMOTION

<EnvVarConfig
    name="ELECTRIC_KEEP_SHAPES_ON_PROMOTION"
    defaultValue="false"
    example="true">

Default: `false`. Requires `ELECTRIC_ACK_AFTER_JOURNAL`. Keeps shapes when PostgreSQL moves to a new timeline by promoting a replica rather than by rewind, as a managed database's failover or maintenance takeover does. Electric reads the new timeline's history and keeps the shapes only when the stored timeline is its ancestor and everything Electric took from the replication stream comes before the switch point. In any other case it purges the shapes, as upstream does, and logs why. Full transaction provenance keeps its source generation across such a promotion.

</EnvVarConfig>

### ELECTRIC_REPLICATION_PIPELINE_WINDOW

<EnvVarConfig
    name="ELECTRIC_REPLICATION_PIPELINE_WINDOW"
    defaultValue="1"
    example="4">

Default: `1`. How many decoded replication events may be in flight from the replication client to the log collector at once. `1` keeps upstream's one-at-a-time behaviour. A value below 1 stops the replication client at start.

</EnvVarConfig>

### ELECTRIC_INGEST_PRIORITY_HIGH

<EnvVarConfig
    name="ELECTRIC_INGEST_PRIORITY_HIGH"
    defaultValue="false"
    example="true">

Default: `false`. Runs the replication client at the Erlang VM's `high` scheduling priority, so reading the replication stream is not starved by request handling under load.

</EnvVarConfig>

## Telemetry

These environment variables allow configuration of metric and trace export for visibility into performance of the Electric instance.

### ELECTRIC_OTLP_ENDPOINT

<EnvVarConfig
    name="ELECTRIC_OTLP_ENDPOINT"
    optional="true"
    example="https://example.com">

Set an [OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/) endpoint URL
to enable telemetry.

</EnvVarConfig>

### ELECTRIC_OTEL_DEBUG

<EnvVarConfig
    name="ELECTRIC_OTEL_DEBUG"
    defaultValue="false"
    example="true">

Debug tracing by printing spans to stdout, without batching.

</EnvVarConfig>

### ELECTRIC_TELEMETRY_REDACT_REQUEST_VALUES (experimental)

<EnvVarConfig
    name="ELECTRIC_TELEMETRY_REDACT_REQUEST_VALUES"
    defaultValue="false"
    example="true">

Default: `false`. Set to `true` to replace query strings, predicates, bind parameters and subset values in traces with presence and byte-size attributes. String sizes are measured in bytes. The size of a decoded map, list or other non-string value is its Erlang external term size. Request URLs in traces and Sentry omit the query string. Request process labels and debug logs show the query string size. Request exceptions retain their type in traces and Sentry. The Sentry request context drops the `Referer`, `X-Forwarded-Uri`, `X-Original-Uri`, `X-Original-Url` and `X-Rewrite-Url` headers, in addition to the ones Sentry already scrubs. Any other header is kept. Logs and crash reports outside the request, snapshot and streaming paths are not redacted, for example those of a failed move-in query or of a catalog write that exhausts its retries. With the option unset, the service uses its existing credential redaction.

</EnvVarConfig>

### ELECTRIC_OTEL_TRUSTED_SAMPLE_RATES (experimental)

<EnvVarConfig
    name="ELECTRIC_OTEL_TRUSTED_SAMPLE_RATES"
    optional="true"
    example="10,100">

Restricts the sample rate accepted from the `electric` member of incoming `tracestate` headers to a comma-separated list of positive signed 64-bit integers. An unlisted rate is ignored and the remote trace context is still used. An empty list ignores all rate hints. When unset, every valid rate is accepted. Invalid list entries prevent the service from starting.

</EnvVarConfig>

### ELECTRIC_HNY_API_KEY

<EnvVarConfig
    name="ELECTRIC_HNY_API_KEY"
    optional="true"
    example="your-api-key">

[Honeycomb.io](https://www.honeycomb.io) api key. Specify along with `ELECTRIC_HNY_DATASET` to
export traces directly to Honeycomb, without the need to run an OpenTelemetry Collector.

</EnvVarConfig>

### ELECTRIC_HNY_DATASET

<EnvVarConfig
    name="ELECTRIC_HNY_DATASET"
    optional="true"
    example="your-dataset-name">

Name of your Honeycomb Dataset.

</EnvVarConfig>

### ELECTRIC_PROMETHEUS_PORT

<EnvVarConfig
    name="ELECTRIC_PROMETHEUS_PORT"
    optional="true"
    example="9090">

Expose a prometheus reporter for telemetry data on the specified port.

The `/metrics` endpoint exposed on this port **must be scraped regularly** (for example by a Prometheus server). Distribution metrics accumulate in memory between scrapes, so enabling this port without a scraper actively collecting from it causes unbounded memory growth that can eventually crash the service. Leave it unset if nothing is scraping it &mdash; for example, if you only use OpenTelemetry.

</EnvVarConfig>

### ELECTRIC_STATSD_HOST

<EnvVarConfig
    name="ELECTRIC_STATSD_HOST"
    optional="true"
    example="https://example.com">

Enable sending telemetry data to a StatsD reporting endpoint.

</EnvVarConfig>

### SENTRY_DSN

<EnvVarConfig
    name="SENTRY_DSN"
    optional="true"
    example="https://key@o0.ingest.sentry.io/0">

Set a [Sentry](https://sentry.io) DSN to enable error tracking via the Sentry Elixir SDK. When configured, Electric will automatically capture errors and report them to your Sentry project.

This requires Electric to be built with telemetry enabled (which is the case for the official Docker images).

</EnvVarConfig>

### ELECTRIC_DISK_USAGE_SCAN_PERIOD

<EnvVarConfig
    name="ELECTRIC_DISK_USAGE_SCAN_PERIOD"
    defaultValue="1min"
    example="10min">

Added by this fork. Default: `1min`. How often the storage directory is walked for the storage-used metric. The next walk also waits at least ten times as long as the last one took, so a large shape store is not walked without a pause. `0` turns the walk, and the metric, off.

</EnvVarConfig>

## Logging

### ELECTRIC_LOG_LEVEL

<EnvVarConfig
    name="ELECTRIC_LOG_LEVEL"
    optional="true"
    example="debug">

Verbosity of Electric's log output.

Available levels, in the order of increasing verbosity:

- `error`
- `warning`
- `info`
- `debug`

</EnvVarConfig>

### ELECTRIC_LOG_COLORS

<EnvVarConfig
    name="ELECTRIC_LOG_COLORS"
    optional="true"
    example="false">

Enable or disable ANSI coloring of Electric's log output.

By default, coloring is enabled when Electric's stdout is connected to a terminal. This may be undesirable in certain runtime environments, such as AWS which displays ANSI color codes using escape sequences and may incorrectly split log entries into multiple lines.

</EnvVarConfig>

### ELECTRIC_LOG_OTP_REPORTS

<EnvVarConfig
    name="ELECTRIC_LOG_OTP_REPORTS"
    defaultValue="false"
    example="true">

Enable [OTP SASL](https://www.erlang.org/doc/apps/sasl/sasl_app.html) reporting at runtime.

</EnvVarConfig>

## Operations

### ELECTRIC_LIVE_DASHBOARD_PORT

<EnvVarConfig
    name="ELECTRIC_LIVE_DASHBOARD_PORT"
    optional="true"
    example="4000">

Starts the [Phoenix LiveDashboard](https://github.com/phoenixframework/phoenix_live_dashboard) on this port. The dashboard has no authentication and shows the VM's internal state. When unset, it is not started.

</EnvVarConfig>

### ELECTRIC_LIVE_DASHBOARD_ADDRESS

<EnvVarConfig
    name="ELECTRIC_LIVE_DASHBOARD_ADDRESS"
    defaultValue="127.0.0.1"
    example="0.0.0.0">

Added by this fork. The address the dashboard listens on. By default it listens on loopback only: `127.0.0.1`, or `::1` when `ELECTRIC_LISTEN_ON_IPV6` is `true`. Upstream listens on every interface. Set another address only once the port is firewalled or otherwise restricted to trusted networks.

</EnvVarConfig>

### RELEASE_DISTRIBUTION

<EnvVarConfig
    name="RELEASE_DISTRIBUTION"
    defaultValue="none"
    example="sname">

Changed by this fork. Default: `none`. Erlang distribution is off by default, so the release opens no `epmd` or distribution port. Upstream's release starts both. Set `sname` or `name` to turn it back on, for example to run `bin/electric rpc` or `bin/electric remote` (`/app/bin/entrypoint` in the container image) against a running node.

</EnvVarConfig>
