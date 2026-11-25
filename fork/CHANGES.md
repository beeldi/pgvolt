# PgVolt: changes from upstream Electric

PgVolt is based on upstream Electric's `main` at the 1.8.0 release of the sync service
(`6efc8a7d3`), and merges later upstream changes as described in the
[README](../README.md#merging-upstream). This page summarizes the scope of the fork's changes at
a glance, lists what behaves differently from upstream without setting anything, and details the
fixes and improvements by area.

The options the fork adds are in the [configuration reference](docs/sync/api/config.md). The
opt-in additions to the HTTP API are in the [sync service README](../packages/sync-service/README.md).

## At a glance

On top of upstream `6efc8a7d3` (v1.8.0), this fork maintains four packages (`@core/sync-service`,
`@electric-sql/client`, `@core/elixir-client`, and `@core/electric-telemetry`). Rows below are
thematic views rather than disjoint totals (some changes span more than one row). All protocol
additions and performance switches are opt-in and off by default.

| Category                            |                               Count | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------------------------------- | ----------------------------------: | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Security & hardening**            |                25+ fixes & defaults | Authentication and config validation, TLS `verify-ca`/`verify-full`, strict request-parameter and cache-key isolation, credential redaction, SHA-256 handle digests, query/response/memory bounds, compaction isolation, and least-privilege container/network defaults (see [Before you switch](#before-you-switch), [Access and exposure](#access-and-exposure), and [SECURITY.md](../SECURITY.md)).                                                                                                    |
| **Fixes & improvements**            |                                 121 | Itemized under [Fixes and improvements](#fixes-and-improvements): `23` where-clause and subquery parity, `24` storage and compaction durability, `23` replication and restart recovery, `12` shape catalog, `8` change routing, `4` HTTP API, `4` logging/telemetry, `2` release image, `15` TypeScript client, `6` Elixir client.                                                                                                                                                                        |
| **Default behaviour differences**   |                                  47 | Always-on changes documented under [Default behaviour that differs from upstream](#default-behaviour-that-differs-from-upstream), plus `17` migration notes (`12` upstream-to-fork, `5` fork-to-upstream) under [Before you switch](#before-you-switch).                                                                                                                                                                                                                                                  |
| **Performance & scale**             |             Always-on + 10 switches | Always-on speedups preventing WAL stalls, boot OOMs, and deploy hangs (`23.3 ms` to `6.0 µs` multi-tenant filter routing, `4.2 s` to `17 ms` wide fan-out, `489.3 MB` to `2.9 MB` boot fold RAM, `4x` smaller compressed catalog, `>10x` cold-cache open, `>3x` parallel restore, `14 min` to `1.1 s` graceful shutdown), plus `10` opt-in high-scale and failover switches (`6-8x` faster write-storm WAL catch-up, zero-resync replica promotion; see [Performance and scale](#performance-and-scale)). |
| **New configuration options**       |                                  26 | New `ELECTRIC_*` options added in `runtime.exs` and documented in [fork/docs/sync/api/config.md](docs/sync/api/config.md) (`10` opt-in switches, `1` arena-reclaim threshold, and `15` operational, resilience, and security options), alongside newly documented `ELECTRIC_LIVE_DASHBOARD_PORT` and changed default `RELEASE_DISTRIBUTION=none`.                                                                                                                                                         |
| **Experimental HTTP API additions** |                                   7 | Opt-in endpoints and parameters documented in [packages/sync-service/README.md](../packages/sync-service/README.md): subset snapshot row cap, full transaction provenance, live-request multiplexing, locale-projected columns, covered LSN wait, shape generation reclaim, and read-only health probes (`/v1/health?require=read_only`).                                                                                                                                                                 |
| **Verification added**              | 162 test files, formal verification | `152` test files (plus `1` fixture module) in `sync-service`, `6` in `typescript-client`, `2` in `elixir-client`, `1` in `electric-telemetry`, `1` Lux suite in `integration-tests`, plus TLC-checked TLA+ formal verification (`ReplicationFlush.tla`, `SubqueryMoves.tla`) under [fork/tla](tla/README.md).                                                                                                                                                                                             |

## Performance and scale

Measurements below come from synthetic benchmarks comparing upstream `6efc8a7d3` (v1.8.0) against
PgVolt (reproducible filter benchmarks live in
[packages/sync-service/dev/shape_filter_cost.exs](../packages/sync-service/dev/shape_filter_cost.exs)).
Always-on improvements require no configuration; opt-in switches are off by default and documented in
[fork/docs/sync/api/config.md](docs/sync/api/config.md#performance-switches).

| Area                                    | Synthetic benchmark                                    |                          Upstream |                                      PgVolt | Mode            |
| --------------------------------------- | ------------------------------------------------------ | --------------------------------: | ------------------------------------------: | --------------- |
| **Array-overlap routing (`&&`)**        | 20,000 shapes, 2,000/group (`ARRAY_ACCESS=1`)          |                          6,858 µs |                        **5.7 µs** (~1,200x) | Always on       |
| **`IN` + disjunctive-residual routing** | 20,000 shapes, 2,000/group (`CLAUSE=or_residual`)      |                         23,261 µs |                        **6.0 µs** (~3,800x) | Always on       |
| **Inclusion-index build (`&&`)**        | 20,000 shapes, 2,000/group (constant-size node rows)   |                           271.9 s |                            **3.9 s** (~70x) | Always on       |
| **Filter registration & index build**   | 40,000 shapes (`ordered_set` relation index)           |            146.4 s / 7,322 µs add |             **11.7 s / 111 µs** (12.5x/66x) | Always on       |
| **Broadcast fan-out reply collection**  | 8,000 shapes touched by one transaction                |                             4.2 s |                           **17 ms** (~247x) | Always on       |
| **Relation-event routing**              | 8,000 shapes across 89 tables (unreferenced / garbage) |             42.3 ms / 39.2 Mwords |              **0.7 µs / 0.5 Mwords** (~78x) | Always on       |
| **Boot catalog fold peak memory**       | 4,000 shapes (`reduce_shapes` vs `list_shapes`)        |                          489.3 MB |                     **2.9 MB** (~168x less) | Always on       |
| **Router per-clause memory**            | Storing clause text instead of duplicated parsed AST   |                  1,295 B / clause |                    **472 B** (~64% smaller) | Always on       |
| **Shape catalog size & `quick_check`**  | 100k shapes (blob / vacuumed file / integrity check)   | 10.5 KB / 2,304 MiB / 0.44-0.59 s |    **1.4 KB / 568 MiB / 0.11-0.12 s** (~4x) | Always on       |
| **Cold-cache boot open & walks**        | Sequential read-ahead / index metadata / `File.ls`     |                                1x |               **>10x / ~13x / ~10x** faster | Always on       |
| **Parallel & paged boot restore**       | 100k shapes, busy schedulers (SQLite read / restore)   |                  2.35-2.74 s / 1x |                **0.68-0.98 s / >3x** faster | Always on       |
| **Where-clause restore analysis**       | 100k shapes, lazy `AND` right-side analysis            |                    916 reductions |                  **214 reductions** (~4.3x) | Always on       |
| **Walk heap sizing**                    | Large bench store (filter restore / routing rebuild)   |                                1x |                       **~2x / ~40% faster** | Always on       |
| **Log read-until-offset & large rows**  | 100k tail rows / single 2 MiB row                      |                   385 ms / 119 ms | **0.27 ms / 0.8 ms** (up to 257x less copy) | Always on       |
| **Graceful shutdown under live load**   | 200 concurrent long-polling clients                    |                             849 s |                           **1.1 s** (~770x) | Always on       |
| **Snapshot query round trips**          | Postgres statements per new shape / subset / move-in   |                     8 round trips |                      **2 round trips** (4x) | Always on       |
| **Write-storm slot catch-up**           | Synthetic high-fan-out write storm (2 CPUs)            |                            49.5 s |                           **6-8 s** (~6-8x) | Opt-in switches |
| **Slot lag under live clients**         | Synthetic storm under concurrent live-client load      |                           62-73 s |                         **11-12 s** (~5.7x) | Opt-in switches |
| **Durability & arena storm drain**      | Batched syncs + shared payload arena / fragment blocks |                                1x |                            **~1.9x** faster | Opt-in switches |

### Always-on performance improvements

- **Flat, O(1)-scaling routing:** Array-overlap (`&&`) clauses decompose into the inclusion index,
  whose nodes track a child count instead of a sorted child-value list; `AND` distributes over `IN`
  and `OR` even alongside inert `OR` residuals; relation events index shapes in an `ordered_set`
  keyed by `{bucket, shape_id}` instead of scanning every shape or paying quadratic ETS bag
  inserts; unindexed shape clauses live as one ETS row each; and broadcast replies collect in
  arrival order instead of rescanning the collector mailbox.
- **Fast boot, restore, and compact catalog:** Shapes are stored compressed (`term_to_binary`
  `compressed: 1`, backwards- and forwards-compatible), cutting SQLite catalog size and boot
  integrity checks by ~4x; sequential read-ahead eliminates random cold-cache 4 KB page reads;
  metadata and incomplete-snapshot checks read covering indexes; boot restores stream shapes in
  100-row pages across one worker per scheduler with pre-sized heaps, single-pass lazy clause
  analysis, and parallel table introspection; `File.ls` directory listing replaces `Path.wildcard`
  and runs after replication starts; and `reduce_shapes/3` streams one shape at a time instead of
  materializing the full catalog in RAM.
- **Read-path and shutdown efficiency:** Snapshot queries fold all six display `SET` statements into
  `set_config(...)` inside the `pg_current_snapshot()` query (`8` round trips down to `2`); log
  reads stop at the target offset and read multi-block rows in one call; journal restart prep scans
  generations once (`114x` to `1,367x` faster across `50` to `1,600` generations); and graceful
  shutdown answers parked long-polls and SSE streams immediately instead of waiting out acceptor
  timeouts.

### Opt-in performance & failover switches

All `10` switches default to off and are exercised off and on in CI:

- **Shared fan-out pipeline:** `ELECTRIC_SHARED_CONVERSION` (convert inserts/deletes once per
  fan-out class), `ELECTRIC_SHARED_PAYLOAD_ARENA` (write shared payloads once to a checksummed
  arena instead of duplicating per shape), `ELECTRIC_SHARED_FRAGMENT_BLOCKS` (frame runs of 3+
  shared changes once), `ELECTRIC_ROUTER_SHARDS` (parallelize routing across worker shards),
  `ELECTRIC_WORKER_PREPARE` (offload shared preparation to shard workers), and
  `ELECTRIC_FILTER_ROUTING_MEMO` (memoize column-pure routing decisions).
- **Decoupled ingest & zero-resync failover:** `ELECTRIC_ACK_AFTER_JOURNAL` (journal transactions
  at decode and acknowledge the replication slot at journal-fsync speed),
  `ELECTRIC_REPLICATION_PIPELINE_WINDOW` (pipeline multiple decoded events in flight),
  `ELECTRIC_INGEST_PRIORITY_HIGH` (prevent live HTTP serving from starving WAL ingest), and
  `ELECTRIC_KEEP_SHAPES_ON_PROMOTION` (preserve cached shapes and full-provenance source
  generations across Postgres replica promotions and failovers).

## Before you switch

From upstream to this fork:

- The runner image is based on distroless `cc-debian12` without `curl` or a built-in Docker
  `HEALTHCHECK`; configure health checks from your orchestrator against `/v1/health`.
- The image runs as user and group `65534:65534` instead of root. The storage volume must be
  writable by that user: a volume upstream's image wrote as root needs a `chown` first.
- Erlang distribution is off. `bin/electric remote` and `bin/electric rpc` (`/app/bin/entrypoint`
  in the container image) need `RELEASE_DISTRIBUTION=sname`.
- A blank `ELECTRIC_SECRET` stops the boot. `ELECTRIC_INSECURE` turns authentication off only for
  `true`, `1`, `yes` or `on`, so a value upstream read as true, such as `enabled`, now requires a
  secret.
- The LiveDashboard listens on loopback. Set `ELECTRIC_LIVE_DASHBOARD_ADDRESS` to reach it from
  another host.
- A `POST /v1/shape` body supplies subset parameters only. `table`, `where`, `columns` and the
  other shape parameters in a body are ignored and must be in the URL.
- A subset field (`where`, `order_by`, `limit`, `offset` or `params`) set in both the URL and the
  body, or in both the flat and nested body, gets `400`. Duplicate JSON member names in the body
  also get `400`, as does a key repeated in subset `params` given as a JSON string.
- A shape request that gives a shape parameter twice gets `400`, on `GET`, `POST` and `DELETE`:
  the same name repeated, `subset__x` together with `subset[x]`, `params` together with
  `params[1]`, or a spelling that nginx or PHP would read as that parameter (another case, an
  undecoded name, a leading space, `.` for `_`). A `DELETE` gets the same for its `handle`.
  Upstream keeps the last value.
- A filesystem that cannot sync a directory stops the service, since metadata writes now sync
  the directory after each rename.
- An exit of a shape subsystem process stops the service, whereas upstream starts the subsystem
  again in place. Run it under something that restarts it, such as a container restart policy;
  an application that embeds it restarts its stack itself. A shape restore that gives up on its
  retries, as when the database stays unavailable through a boot, stops the service the same
  way.
- The 250 MiB default transaction size cap is off by default, since transactions reach shapes in
  small fragments; set `ELECTRIC_EXPERIMENTAL_MAX_TXN_SIZE` explicitly if you want a cap.
- The TypeScript and Elixir clients fail a response longer than 256 MiB. Set `maxResponseBytes` or
  `max_response_bytes` where a single response can be larger.

From this fork back to upstream:

- Shapes written with `ELECTRIC_SHARED_PAYLOAD_ARENA` on are in on-disk format version 3, which
  upstream does not read. Upstream drops them and their clients resync.
- With `ELECTRIC_ACK_AFTER_JOURNAL` on, turn it off and restart once on this fork first.
  Upstream never reads the journal.
- Shapes requested with `source_positions=full_xid` need a fresh storage directory, as the
  [sync service README](../packages/sync-service/README.md#full-transaction-provenance-experimental)
  explains.
- A shape this fork creates with a composite subquery condition, such as
  `(a, b) IN (SELECT a, b FROM parent)`, tags its rows with length-prefixed inputs, which
  upstream does not compute. Upstream, like this fork, drops every shape with subqueries at
  boot, so their clients resync with upstream's tags.
- A shape this fork creates with `=`, `<>`, `@>`, `<@` or `&&` between two arrays stores a
  comparison that earlier fork versions and upstream cannot run, so they exclude every row it
  tests. Delete such shapes, or use a fresh storage directory, before moving back.
- No other move back has been tested. A fresh storage directory always works, at the cost of
  resyncing every shape.

## Default behaviour that differs from upstream

### Access and exposure

- The secret is compared in constant time, is accepted in an `authorization: Bearer <secret>`
  header on `/v1/shape` and its subpaths (taking precedence over `?secret=` and `?api_secret=`),
  and also guards the fork's `/v1/shape/multiplex`.
- The database connection accepts `sslmode=verify-ca` and `verify-full`, and logs every fallback
  to an unencrypted connection.
- Responses carry
  `Vary: origin, authorization, cookie, x-api-key, x-auth-token, x-electric-secret, electric-secret`,
  so a shared cache keeps apart the responses of principals that authenticate in a header.
- Traces export only allowlisted request headers and query parameters, with credentials
  redacted. Error reports and process labels scrub credentials from the request URL.
- A shape handle is validated against a SHA-256 digest of the shape's definition instead of a
  27-bit hash.
- A where clause with more than 1,000 expression positions is refused, and the limit on
  disjuncts is checked while they are built rather than after.
- A where clause nested more than 100 levels deep is refused.
- `@>`, `<@` and `&&` against an array of more than 256 elements are evaluated per shape
  instead of indexed, whereas indexing them would cost quadratic time.

### Where clauses

- A where clause on a column whose values Electric cannot parse from the replication stream is
  refused when the shape is created. Upstream accepted it and dropped the live changes to its
  rows.
- `COLLATE "C"` and `COLLATE "POSIX"`, unqualified or in `pg_catalog`, are accepted. Electric
  compares text byte by byte, which is what they mean. Other collations, a `C` in another schema
  among them, are refused with an explicit error.
- A cast with a type modifier, such as `varchar(2)`, `timestamp(0)` or `interval '...' day`, is
  refused. Electric would compare the value before Postgres rounds or cuts it. A precision of 6
  on a time or a timestamp changes no value and is accepted.
- Numeric literals of every form Postgres accepts are parsed: integers past int4, exponents,
  `.5`, `1.`, digit separators and radix prefixes. Integers are typed int4, int8 or numeric as
  Postgres types them. Upstream answered some of these with a 500.
- More literal forms are accepted with their Postgres meaning: boolean spellings such as `'yes'`
  and `'1'`, numbers padded with whitespace, `'12:30'::time`, day-time intervals such as
  `'04:05:06'`, `ago`, and `<` and `>` between intervals.
- The connections that run snapshots and move-in queries set `standard_conforming_strings = on`,
  and generated SQL writes a string constant that holds a backslash as `E'...'`. Neither depends
  on the database's setting any more.

### HTTP API

- An offset with a sign, such as `+0_0`, gets `400`. `-1` stays valid.
- An offset that is still past the end of the shape log after the out-of-bounds wait gets a
  `409` must-refetch with the current handle instead of a `400`, which clients treat as final.
- Every `retry-after` on a `503` is drawn from base to twice the base: 10 becomes 10 to 20
  seconds, 5 becomes 5 to 10, 1 becomes 1 or 2. Admission control sends 5 to 10 instead of 6 to 10.
- The `electric-schema` header marks only primary-key columns `not_null`. A client caches the
  schema, and a later `DROP NOT NULL` would make it reject rows.
- Existing shapes are served read-only whenever their metadata is loaded and the node cannot
  write yet, not only while it waits for the lock. Creating or deleting a shape still waits.
- `/v1/health?require=read_only` answers `200` as soon as existing shapes are served read-only
  (keeping the status text), while `/v1/health` without the parameter keeps upstream's `202` until
  the stack is active.
- An HTTP/2 connection may send 10,000 stream resets in 10 seconds before it is closed
  (`ELECTRIC_TWEAKS_HTTP2_MAX_RESET_STREAM_RATE`). Upstream 1.8.0 uses Bandit's default of 500 in
  10 seconds (later upstream disabled the limit by default).
- A request naming a known handle counts against the `existing` admission limit only if the handle
  is of the requested definition and its offset reads that shape. Upstream goes by the handle alone.
- A shape response whose client accepts no data for `ELECTRIC_STALLED_SERVE_TIMEOUT` (60 seconds)
  is terminated, and the client resumes from its last offset. Upstream keeps it open for as long
  as the connection lives.

### Storage and shapes

- A composite subquery shape length-prefixes each field of its tag input, so a move-out removes
  only the rows of its own key pair. Upstream joins the fields with no separator, so two text
  pairs that concatenate to the same string share a tag.
- A shape log is synced to disk at transaction commit, on the flush timer, or once 8 MiB are
  unsynced, instead of at every 64 KiB write. The slot is acknowledged only up to synced bytes.
  A reopen keeps the unsynced bytes that reached the file only when the machine has not restarted
  since they were written; after a restart it resumes from the synced offset, the source replays
  the rest, and the writer syncs a record of the new boot before it appends.
- A shape's synced offset, storage version and log generation names, and the persistent key-value
  store, are written with `O_SYNC`, and the directory is synced after the rename. A shape's first
  synced commit also syncs every directory from the storage directory's parent down to its log,
  and leaves to the host a parent the service may neither open nor write. Reopening a shape that
  has a synced offset syncs the files and directories of the log generation its metadata names,
  and the two files that name it, once per generation, and then its metadata directory. A
  compaction syncs each generation it writes, and the directory naming its files, before it writes
  the name that points at it.
- Compaction covers only chunks at or below the shape's last synced offset. Upstream compacts any
  complete chunk, which can hold a transaction not yet committed or synced.
- Shape catalogs left by builds with another catalog version or OTP release are deleted once
  the stack is active, each with its shape count in the log.
- One expiry batch removes at most 1,000 shapes. While the store is still over its limit and a
  batch made progress, the next batch follows straight away instead of after the period.
- A restored shape whose snapshot fails with a retryable error before streaming any row is kept
  and waits for the database, instead of being deleted. A new shape is deleted, as upstream
  deletes it.
- A shape is removed (so clients refetch under a new handle) if its writer reopens on unreadable
  or unsupported files, a missing or invalid version file (including a non-integer numeric value
  such as `1.0`), missing snapshot markers (unless the snapshot end is already cached), or missing
  storage files for an already-restored or cached shape. Upstream rebuilds it under the same
  handle, letting resuming clients miss changes between the two snapshots (or failing on the old
  snapshot's first chunk when only the start marker is missing). With file storage, a shape
  removed concurrently while an incoming change restores its consumer is no longer recreated under
  its old handle.
- The shape catalog's write buffer gives up on an operation after 100 failed attempts, and new
  shapes are refused while 100,000 operations are queued.
- Compaction keeps the previous generation of a shape's log until the next compaction, and
  removes older ones.

### Replication pipeline

- The service acknowledges one byte below a keepalive's `wal_end` and treats the slot's
  confirmed flush position the same way, since Postgres reports both as exclusive boundaries.
- A shape whose consumer has not answered a broadcast within a minute is removed. Upstream waited
  without a limit, holding up every shape.
- A writer that keeps asking for more time without flushing is cut off after
  `ELECTRIC_FLUSH_DEFERRAL_ROUND_LIMIT` rounds, and its shape removed.
- Until a writer's first flush, the stall check waits `ELECTRIC_FLUSH_STALL_BOOT_GRACE`
  (10 minutes) instead of the steady grace of 1 minute.
- Boot-time reads of the shape catalog wait up to `ELECTRIC_SHAPE_DB_BOOT_READ_TIMEOUT`
  (300 seconds) instead of 10.
- A shape that holds more than `ELECTRIC_TXN_BUFFER_MAX_CHANGES` (250,000) changes of one
  transaction in memory, while it waits for its snapshot or as a subquery shape, is removed and
  its clients refetch it. Upstream holds them without a limit.
- When a table leaves the publication, existing shapes on that table are removed and persisted to
  disk before the table is served again, regardless of who re-adds it: Electric, another session,
  or the operator in manual publishing mode. A restart of the publication configurator keeps that
  wait. Their clients refetch them, since changes written while the table was out never reached
  them. A shape that has not started its first snapshot on that table is kept. Upstream keeps them
  when it adds the table back itself, and does not wait for their removal when another session or
  the operator does.

### Telemetry

- Millisecond lag histograms in Prometheus use buckets from 1 ms to 300 s, and fan-out
  histograms get buckets that fit them.
- Prometheus also exports the series that went only to StatsD or OpenTelemetry: the shape
  catalog's write buffer and connections, admission control, flush stalls, storage bytes,
  fan-out and storage lag.
- With no `ELECTRIC_MAX_SHAPES` set, the service warns at start that shapes never expire.
- The storage directory walk behind the storage-used metric waits at least ten times as long as
  its last run took before the next one, and `ELECTRIC_DISK_USAGE_SCAN_PERIOD=0` turns it off.
- The `root_table` label of `electric.shape.response_size.bytes` names the root table of the shape
  the request resolved to, as `schema.table`, instead of the `table` parameter as the request
  spelled it.

### Clients

- TypeScript: an int, float or bool column receiving an array literal throws
  `ParserArrayShapeError` instead of coercing it, and integers in `json` and `jsonb` past 2^53
  parse to `BigInt`, except in a document nested too deep to revive, where they stay numbers.
- Elixir: two requests share a response only when they also have the same authenticator and
  method.

## Fixes and improvements

### Where-clause evaluation

Electric evaluates live changes itself, while Postgres evaluates the snapshot. These fixes make
the two agree, so that a live change is not delivered to a shape whose snapshot excludes it, or
dropped from one that includes it.

- `NOT IN (SELECT ...)` excludes the row when the subquery returns a NULL, on the indexed path
  too.
- A negated condition that evaluates to NULL, or fails, no longer counts as a match.
- `x <op> ANY (array)` and `ALL` treat a NULL element as unknown.
- A value Electric cannot parse no longer fails the whole predicate: `IS [NOT] NULL` can test it
  and every other operator refuses it. Each excluded row is counted in
  `electric.shape.unevaluable_row_excluded.count`.
- Where clauses on `json`, `jsonb` and `bytea` columns are evaluated instead of failing on every
  change. Only null tests apply to these types, and to arrays of them: `=`, `<>`, `@>`, `<@` and
  `&&` between such arrays are refused. A subset's where clause, which only Postgres evaluates,
  keeps them.
- `LIKE` and `NOT LIKE` match multi-line text and honour escapes.
- Backslash escapes in array literals and replicated `text[]` values are honoured, and an
  unquoted element holding a `"` is refused as Postgres refuses it.
- An array with fewer subscripts than dimensions returns NULL.
- `'12:30 PM'::time` is 12:30, not 00:30, and `'12:30 AM'` is 00:30.
- Intervals compare by their total span, so `'1 day' = '24 hours'`.
- An array's element type no longer depends on the order of its elements.
- `bool::text` gives `true` and `false`, and `upper` and `lower` map case per code point.
- An array whose lower bounds are not 1, which Postgres writes as `[0:1]={...}` after an
  assignment such as `arr[0] = x`, is read with its bounds: `=` and `<>` compare them, subscripts
  count from them, and `@>`, `<@`, `&&` and `ANY` ignore them. A where clause constant with such
  bounds fails with an explicit error.
- An unquoted identifier followed by a newline is refused.
- Moving a row into or out of a subquery shape matches it on the value Postgres sent instead of
  a re-rendered one, which differed for timestamps, numerics and arrays and crashed on some
  types.
- `point` columns are no longer read as arrays of floats, and arrays of an enum keep their enum
  type.
- A subset request on a shape whose where clause holds an `OR` applies the subset to every
  branch.
- `=`, `<>`, `@>`, `<@` and `&&` between two arrays compare their elements as the element type
  does: a float `-0` equals `0`, a numeric `-0.00` is zero, and two spellings of one time or
  interval are equal. A shape created before this change keeps the old comparison until it is
  recreated.
- A subquery keyed on a `numeric` compares the key exactly, including `NaN` and digits past
  the sixteenth.
- A subquery whose column holds a value Electric cannot parse, such as a float `NaN` or
  `Infinity`, a date or a timestamp at `infinity` or before the common era, a time of `24:00:00`
  or an interval at `infinity`, compares it by the text Postgres writes for it, instead of
  stopping the shape.
- A subquery over a table when a single transaction reuses a deferrable primary key stops its view
  with a warning that names no key, and the shapes over it are invalidated so their clients
  refetch. Previously, the view crashed with the key in its report or silently overwrote the
  remaining row's value, because the log's delete carries only the key and cannot tell which of
  the two rows left.
- A subquery compares a float `-0` and `0`, intervals of one span written another way
  (`1 day` and `24:00:00`), `char(n)` values padded to two lengths, and `jsonb` values whose
  numbers have another scale (`{"rate": 1.0}` and `{"rate": 1}`) as one key. When such a key
  moves, Postgres names the tags of the rows it moves, as for a `numeric`.
- A composite subquery pairs the row on its left with its select list in the order the list
  names its columns. `("divisionId", "firmId") IN (SELECT id, "firmId" FROM divisions)` compared
  `("divisionId", "firmId")` with `("firmId", id)`, sorted by name, so the shape held rows Postgres
  does not select, or got a `400` when the paired columns had different types. Such a shape gets
  a new handle, and its clients refetch.

### Routing changes to shapes

- A row value the index cannot read, such as `NaN`, `infinity` or `24:00:00`, routes the change to
  the shapes indexed on that field only, instead of every shape on the table, and no longer logs
  a stack trace per change.
- An `AND` over an `OR` or an `IN` list is split into up to 1,024 indexed branches, and
  `col && ARRAY[...]` is indexed, so such shapes leave the per-change scan. Each branch holds a
  copy of the rest of the clause, and a split whose own branches would hold more than 16,384
  nodes of copies is not made. Splits made one inside another, as each term of a long `AND`
  chain ending in an `IN` list makes, count only their own copies. Deciding on a split analyses
  each part of the clause once, however it nests.
- Index keys have one canonical form: numbers compare by value, and timestamps by a fixed
  precision.
- `@>` routes rows whose matching elements sit in nested arrays.
- Containment routing bounds the work for long row arrays while keeping exact matches, and never
  matches a NULL array element.
- A schema change evaluates only the shapes of its table.
- The router stores a shape's where clause as text and parses it again on removal, which lowers
  its memory per shape.
- Replies to a broadcast are collected in the order they arrive.

### Storage

- A failed sync stops the shape's writer instead of acknowledging bytes that never reached the
  disk.
- A read error while streaming a log raises instead of ending the stream early, so indexes and
  compactions are no longer built from a truncated log.
- Damage found while serving a read, a log shorter than its index, and a snapshot chunk that
  never got its end marker are classed as data loss: the shape is removed and the next request
  rebuilds it. Each of these used to fail every retry.
- A log shorter than the offset it recorded as durable is rebuilt instead of served short.
- A restart keeps the transactions a shape's log wrote after its last sync, and syncs them before
  acknowledging any. Upstream trims them, though readers may already have been served them. A log
  that a machine crash left short of them, or whose record of them it tore, goes back to its last
  synced offset.
- Rows past the committed offset are not served: a chunk that straddles the bound is read up to
  it, and the advertised cursor is clamped to it.
- Two shapes compacting at once no longer share scratch files, which could mix their rows.
- A compaction merges each key's entries in offset order. Upstream sorts its key index by key
  alone, so a key updated since the previous compaction came out of the next one with its older
  compacted value, and the updates since were lost.
- Compaction file names always increase, so two compactions in one millisecond, or a clock that
  steps back, can no longer truncate the live log.
- A compaction takes a name past every generation on disk and never replaces a file, so one left
  running by a killed writer can no longer rewrite the generation a later one published, and the
  cleanup keeps the generation published before the current one and every later file.
- A writer keeps its state across a compaction, with its cached chunk positions moved to the new
  log, and readers get the new log's name and positions together. Upstream restarts the writer
  from a blank state and pairs the new name with the old positions, so reads miss rows and a
  restart can drop acknowledged ones.
- A writer reopened after a kill during a compaction's switch appends to the generation a restart
  reads, and finishes a switch the kill left between its two names. Upstream keeps that latest
  log, which still holds the compacted rows, and the next compaction merges them twice. A reopen
  that finds a file of its compacted generation missing, cut short of its index, or ending short
  of its boundary, rebuilds the shape instead of serving the offsets below the compaction empty or
  short. So does a reopen whose compaction boundary is past the shape's synced offset.
- A compaction's switch checks its copy of the log tail and names the new generations only once
  both are synced. Upstream ignores the copy's result, so a disk that fills up mid-copy publishes
  a short log.
- A read from disk takes a shape's latest log name before its compaction boundary, so a read
  during a compaction's switch no longer skips the rows the compaction moved.
- A read whose log generation two compactions removed between its name lookup and its chunk
  lookup reads the names again and goes on in the newer generations. Upstream, which removes the
  superseded log at the first compaction, ends the response there with no error, short of the
  offset it advertised.
- A compaction cuts at the newest complete chunk at or below the last synced offset, past the ones
  it keeps. Upstream takes only the chunk the keep count names, and with none to keep, fails on
  the open chunk at the end of the log.
- A commit right after a compaction gets its own flush timer. Upstream leaves the old timer set,
  and the commit stays buffered and unsynced until a later write flushes it.
- A compaction that fails clears its in-progress flag, so the shape compacts again later.
- Superseded compacted generations are deleted. Upstream's cleanup did not remove them.
- A volume too full to create the trash directory no longer stops the stack, and deletes no
  longer report success when the directory is missing.
- A reopen that rebuilds a shape whose files it cannot read fails when it cannot move the old
  files to the trash. Upstream ignores the failure and initialises the new shape over them.
- Shape directories with no catalog entry are reclaimed when the node takes the lock.
- A failed reclaim no longer leaves the shape holding back the replication slot.
- The persistent key-value store reports a truncated or empty file as corrupted instead of
  returning its bytes.

### Shape catalog

- The catalog is read as a stream at boot, one shape at a time.
- A failed hourly `PRAGMA optimize` is logged and skipped. Under load it used to stop the stack.
- Damage found by `PRAGMA quick_check` triggers the rebuild instead of crashing the boot.
- A schema migration runs in one transaction, so one killed halfway no longer leaves a catalog
  that fails every boot.
- Refreshing shape metadata keeps each shape's last-read time, so expiry removes idle shapes
  rather than shapes that are still read.
- A lookup that raced a removal reads again, so it never returns a deleted handle.
- Flushing a buffered addition deletes only its own lookup entry, which could create a duplicate
  handle.
- A registration timeout invalidates the shape once, through the usual path.
- Shapes whose dependencies are missing or circular are skipped at restore instead of hanging
  the boot.
- Invalid shapes are removed by stopping their consumer first, never by deleting storage under a
  live one.
- A removed shape keeps the reason it was removed, and requests report it.
- The expiry check schedules its next run before it removes shapes, so it keeps its period.

### Replication and restarts

- An exit of the shape log collector, the shape cache or another process of the shape subsystem
  stops the service, and the next start reads the replication slot again. Upstream starts the
  subsystem again in place while the replication connection stays up, and Postgres resends
  nothing to that connection: a transaction whose first fragment the collector had routed, or a
  shape the collector held after its consumer exited, lost its change, and the shape served
  up to date without it.
- Snapshot visibility is correct after transaction ID wraparound. Transactions in flight when a
  snapshot was taken were dropped from the shape's log.
- A recreated replication slot purges old shapes even if the stack restarts before it can record
  the purge in the timeline. Opening an existing slot leaves no new purge intent. Instances
  waiting for the lock can serve stored shapes read-only during a provisional slot creation.
  A timeline record that cannot be read also triggers a purge.
- Random temporary slots use a publication with the same random suffix when Electric chooses
  and manages the publication. Electric drops that publication on clean shutdown.
- Standbys load completed shapes for early reads and wait for the replication lock before
  validating the catalog or clearing storage for an empty catalog.
- A shape catalog that another connection holds locked during the boot integrity check is kept,
  and the open fails. It was deleted as corrupt.
- A standby that finds lost data while serving a read reports it and leaves the shape to the
  instance that holds the lock. It removed the shape from the shared catalog and storage.
- With `ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE`, a standby's boot integrity check holds a read lock on
  the shared catalog, blocking commits until it finishes. The running instance now keeps catalog
  writes queued (instead of dropping each after 100 failed flushes), keeps its connection open
  rather than reopening and rechecking it, and continues creating shapes. Its final shutdown flush
  retries through that lock for up to 20 s, and the write buffer's shutdown timeout (5 s upstream)
  is raised to 46 s by default to cover that window and two checkouts.
- Catalog validation after acquiring the replication lock, and the refresh after it, read the
  catalog as the queued writes will leave it: a shape with a queued completion stays complete
  (instead of being removed by validation), and one with a queued removal stays out (instead of
  being put back by the refresh). Validation still removes an invalid shape whose removal is
  queued and counts it once, bounding its count by committed rows plus queued additions (upstream
  subtracts queued removals, which can still fail, and reads lagging buffer counters). Validation
  retries its commit through the lock for up to 60 s from start (flush included) before raising.
- An instance tries to commit its queued shape catalog writes before releasing the replication
  lock on stop or idle disconnect, retrying through another process's lock on the catalog for up
  to 20 s. Upstream releases the lock first, so the next holder's validation removed shapes whose
  snapshot-complete mark had not landed yet. The drain is best effort: another instance taking the
  lock next keeps such a shape only if its mark committed before validation, which a write error,
  the 20 s timeout, or an early session end can prevent. Before an idle disconnect, the
  replication client waits for the drain while continuing to answer Postgres keepalives, and stays
  connected if replication data arrives meanwhile; upstream disconnects at once.
- Concurrent connection-restart requests are deduplicated by target connection manager PID: a
  request stops that manager's supervisor only while that manager is still running, and returns
  without restarting once it has stopped (upstream stopped the supervisor once per piled-up
  request, eventually hitting the supervisor restart limit). Likewise, when the publication
  configurator finds the publication missing during a check, it rechecks if the connection manager
  changed or was absent at either end and returns a retryable connection error if that holds for
  both checks, keeping waiting shapes alive for their first snapshot instead of restarting and
  removing them. A shape restored with its snapshot on disk is kept too, with its consumer alive
  to flush writes while publication admission retries.
- The publication is refreshed on a timer re-armed after every pass. The old idle timeout never
  fired on a busy stack, and stopped for good in some states.
- With manual publishing, or a publication another role owns, Electric waits for in-flight
  writers before admitting each table added by that role and rechecks its publication membership
  under the lock. A failed wait keeps the table pending across restarts until a check succeeds.
  Barrier records are scoped to the publication and database system. A failed write keeps
  affected tables pending until the record is durable. After recovery from an unreadable record,
  each published table without a recorded drain must drain before admission in either publishing
  mode, including tables with no restored shapes. Electric records the drain it performs when
  adding a table or repairing its replica identity, so later admissions can reuse it for the
  same membership. In either publishing mode, a check that finds a table's recorded membership
  changed removes the old shapes and confirms their removal on disk before admitting the table
  again, including when the table was dropped and re-added between checks. A mismatch during an
  addition or repair holds new shapes until a fresh check after the current pass. A transient
  cancellation, lock timeout or deadlock leaves shapes in place while admission retries. While
  admission is held, the consumer stays alive so the next request can wait for the initial
  snapshot. If Electric lacks the table privileges to take the lock, the operator must take it
  when adding the table.
- When another session adds a table between Electric's check of the publication and its own
  addition, the shapes waiting for the table take their snapshot only once writers already in
  flight have finished, on a later check too if that wait timed out or the configurator
  restarted. Upstream answers them at once.
- A message that reaches a paused replication connection is queued instead of dropped.
- A `Begin` inside an open transaction is refused, and unsupported messages are skipped.
- A shape counts as caught up once its flush reaches the change sent to it, which ended false
  stall removals.
- The replication client logs an error every five minutes while it has been paused longer than
  its retry budget, a state Postgres cannot detect because keepalives continue.
- While the log collector is down, the retry log leaves out the transaction's rows and is
  written at most once a second.
- A snapshot or move-in query that dies in any way reports the failure to its shape, which used
  to wait until it timed out.
- Requests that reach a shape before it has started are answered once it starts.
- A status wait always answers, even when the status monitor is not running or stops.
- Waiters answered by the relation tracker are no longer kept in memory.

### HTTP API

- A where clause holding `%{...}` in a quoted identifier, and `params` given as an object or
  array, get `400` instead of `500`.
- A request with two `Origin` headers gets `*` instead of crashing.
- The `columns` parameter is split in linear time.
- With a long-poll timeout under one second, the live cursor still moves past the client's.

### Logging and telemetry

- Flush-stall challenges and removals log each shape's offsets, stall age, writer and deferral
  count.
- At restore, shapes filtering on a type Electric cannot parse are logged, since they match no
  live change.
- Debug logs say which schema change removed a table's shapes.
- `electric-telemetry`: CPU metrics no longer crash with no processor reported, a failed disk
  reading is skipped, the binary memory report honours `:mem_percent`, and the message-queue
  monitor forgets dead processes.

### Release image

- All three Dockerfile base images (builder, `runtime-packages`, and distroless `cc-debian12`
  runner) are pinned by digest, targeting Erlang/OTP 29.1.3 and Elixir 1.20.5 without `curl`,
  `apt`, `dpkg`, or `perl`.
- Scheduler threads get a 32 MB stack (`+sss 4096` in `rel/vm.args.eex`), which the SQL parser
  needs for the most deeply nested where clause its input limit admits.

### TypeScript client

- The stream offset rolls back when a response body fails to parse.
- A warm subset snapshot no longer moves the stream's offset.
- Snapshot requests are cancelled with the stream.
- `unsubscribeAll` stops the stream, and `Shape.value` rejects on an error already stored.
- Every terminal error reaches the subscriber.
- The request loop no longer recurses.
- Snapshot positions are compared per key.
- The array parser no longer repeats the last element, and parses every element as nullable. An
  array with explicit bounds throws `ParserArrayBoundsError`.
- A `json` or `jsonb` value nested a few thousand levels deep is parsed instead of stopping the
  stream, except on Firefox when it holds an integer past 2^53.
- The live-request watchdog also covers reading the body of a long poll.
- A read hit on the expired-shapes cache updates eviction order in memory without rewriting
  browser storage.
- A column named like an inherited object member, such as `constructor`, stays an ordinary field
  of a mapped row.
- The expired-shapes cache and the up-to-date tracker store a digest of each shape key in place of
  its URL, and remove what earlier versions stored.
- `FetchError` and `MissingHeadersError` report a URL with the protocol parameters only.
- `maxResponseBytes` (256 MiB by default) limits what the client holds of a response. A longer
  body fails with `ResponseTooLargeError`. Over SSE, messages held for an up-to-date or for slow
  subscribers count towards it, and passing it closes the connection. An error body is read up to
  1 MiB.

### Elixir client

- `delete_shape` sends the shape's definition.
- A reset clears the schema and the value mapper, and a resumed stream rebuilds its value mapper
  from the stored schema.
- Array columns pass through the value mapper.
- Request logs redact credentials in headers, parameters and the endpoint.
- HTTP/1 framing errors are retried.
- `max_response_bytes` (256 MiB by default) limits a response body as received and after gzip
  decompression, and fails the request with `ResponseTooLargeError` without a retry.
