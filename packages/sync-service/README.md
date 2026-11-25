# Electric

Electric Sync is a lightweight data synchronization service designed to sync small subsets of your Postgres data into various environments and applications, such as web and mobile apps, development environments, edge services, and local AI systems.

### Key Use Cases:

- **Web & Mobile Apps**: Replaces traditional data fetching by syncing necessary data directly into your apps.
- **Development Environments**: Synchronizes data into embedded databases like PGlite, facilitating local testing.
- **Edge Workers & Services**: Maintains low-latency data caches for edge services.
- **Local AI Systems**: Syncs data into AI systems utilizing pgvector for efficient, local data handling.

### How it Works:

Electric Sync is powered by an Elixir-based application that connects to your Postgres database via a `DATABASE_URL`. It consumes the logical replication stream and exposes an HTTP API for replicating data subsets, or "Shapes," to local environments.

For a quick setup and examples, refer to the [Quickstart guide](https://electric-sql.com/docs/quickstart).

## Running as a Standalone HTTP Endpoint

Run Postgres:

```sh
docker compose -f dev/docker-compose.yml create
docker compose -f dev/docker-compose.yml start
```

Source the `.env.dev` somehow, e.g.:

```sh
set -a; source .env.dev; set +a
```

Run the Elixir app:

```sh
mix deps.get
iex -S mix
```

## Subset snapshot row cap (experimental)

Set `ELECTRIC_SUBSET_MAX_ROWS` to a positive integer to cap subset snapshots at that many rows.
A request with a higher `limit` returns `400`. For a request without a `limit`, Electric checks
for one row beyond the cap. If it finds that row, it returns `400` and asks the caller to add a
`limit` at or below the cap. A `limit` requires an `order_by`.

The cap is off when the option is unset. The protocol may change.

## Full transaction provenance (experimental)

> Experimental and opt-in. This is an addition of this fork, not part of upstream Electric, and
> its protocol may change between releases.

Enable `full_xid_source_positions` in `ELECTRIC_FEATURE_FLAGS` before deploying clients
that merge overlapping shapes. Clients request `source_positions=full_xid` and must
require `electric-source-positions: mvcc-full-xid-v1` and `electric-source-generation`
on successful data responses. A generation change invalidates the entire local
membership cache, including snapshots and persisted WAL positions. Shape handles
include this mode and generation; legacy handles cannot resume a full-provenance shape.

The generation identifies one continuous source stream: the PostgreSQL system
identifier, the timeline the stream started on, database name and OID, and a slot
incarnation that Electric rotates in its persistent state each time it creates the
replication slot. A promotion that Electric reads as continuing the stream (see
below) keeps the timeline the stream started on, and so keeps the generation.
Recreating the database under the same name, creating a new replication slot, or losing
persistent state all change the generation. Like shape logs, it
cannot detect a slot recreated by someone else while Electric is offline.

WAL changes add `headers.txids8`, an array of decimal strings containing PostgreSQL
full transaction identifiers. Existing `headers.txids` retains its original 32-bit
values. Snapshot rows use their following `snapshot-end` MVCC metadata, whose
`xmin`, `xmax`, and `xip_list` are already full-width decimal strings. Compaction is
disabled for this mode because compacted rows do not retain source provenance.
The global up-to-date LSN is not a row version or a per-shape consumption barrier.

`HEAD /v1/shape?table=...&offset=-1&source_positions=full_xid` without a handle,
live mode, or subset is a generation probe. It authenticates and validates the
shape parameters, then returns the capability and current generation with
`Cache-Control: no-store`, without creating a shape or reading its snapshot.
It returns no shape handle or offset. Legacy HEAD requests retain their existing behavior.

The decoder resolves epochs before persisting changes. It validates the active
replication slot against the replication connection's system identifier,
timeline, database name and OID, and backend PID. A first SQL statement captures
a WAL fence; a second READ COMMITTED statement obtains the full snapshot and
retained slot interval.
Only unacknowledged transactions inside that interval and at or before the fence
can reuse the anchor. Transactions at the snapshot's completion frontier wait for
a fresh anchor. This remains safe across quiet streams and transaction-ID wrap;
elapsed time never establishes an epoch. Cache misses pause socket reads and
coalesce for 5 ms before one asynchronous lookup, while keepalive and durable-flush
messages remain responsive. An unresolved frontier fails after 30 seconds without
acknowledging that transaction.

PostgreSQL's confirmed-flush and keepalive positions are exclusive boundaries: a
commit record starting exactly there is still eligible for decoding. Incoming
boundaries are normalized before the existing inclusive-position ACK encoder.
Replay detection uses the connection's initial confirmed boundary, never its
changing flush position. Snapshot filtering also uses the full transaction ID,
including when an otherwise quiet shape crosses a transaction-ID wrap.

On restart, retained transaction journals without full transaction IDs invalidate
existing full-provenance shape handles before replay. Legacy shapes keep their
handles and replay normally. Clients rebuild invalidated shapes from a fresh
snapshot; this can recur while slow consumers retain an older journal backlog.
Historical transaction epochs are never inferred from current database state.
Do not downgrade to a binary that predates full-provenance shape identities while
reusing its shape storage; use a fresh storage directory for that downgrade.

Deploy the fork with the feature disabled first, then enable it and verify readiness
before releasing clients that require the capability. Old clients continue using
legacy handles and ignore the additive headers. The capability is unavailable
until source validation succeeds. Rollback of the fork requires rolling back the
new clients first; do not reinterpret old 32-bit transaction IDs as full identifiers.

The database role needs the existing logical replication permissions plus access to
`pg_replication_slots`, `pg_current_snapshot()`, `pg_current_wal_lsn()`,
`pg_walfile_name(pg_lsn)`, and `pg_control_system()`. These accesses were verified
with an otherwise unprivileged replication role on PostgreSQL 18. Where operators
have revoked default function privileges, restore only these required privileges
before enabling the flag. The admin and replication connections must target the
same writable primary. A mismatched source fails validation instead of advertising
the capability.

## Keeping shapes across a promotion

Electric purges every shape when the PostgreSQL timeline changes, because a new
timeline usually means a point-in-time recovery that rewound the database. A
promotion also starts a new timeline without rewinding anything: a managed
PostgreSQL service does one at a maintenance takeover, and so does an HA failover
onto a replica that had received every write.

Set `ELECTRIC_KEEP_SHAPES_ON_PROMOTION=true` to keep shapes across such a
promotion. On a new timeline, Electric then reads its history with
`TIMELINE_HISTORY` and keeps shapes only when all of these hold:

- the system identifier is the one stored;
- the stored timeline is an ancestor of the new one;
- the highest position Electric took from the stream, across the shape logs, its
  transaction journal and what it holds in memory, is before the point where the
  new timeline branched off the stored one;
- Electric did not create the replication slot since.

Any other case, or any doubt, purges as before, with a notice naming the rule
that failed. Replayed transactions are dropped as on any reconnection: the slot
on the new primary may resume from an older position than the one Electric
acknowledged.

The check needs `ELECTRIC_ACK_AFTER_JOURNAL=true`: clients read transactions
before the shape logs keep them, and without the journal nothing on disk records
those, so every timeline change purges, as does one before the journal has
recorded anything.
It cannot see a snapshot query that read rows the new timeline lost, so enable it
only where a promotion keeps every committed write, as a planned switchover does. Rolling back to a binary
without this setting after it kept shapes purges once: the older binary cannot
read the timeline record and treats it as a change.

## Publication membership changes

If a check finds that a table's recorded publication membership has changed, Electric removes
the old shapes and confirms their removal on disk before admitting the table again. This applies
in automatic and manual publishing modes, including when a table is dropped and re-added
between checks. Clients rebuild the shapes from fresh snapshots.

## Embedding into another Elixir Application

Include `:electric` into your dependencies:

    # mix.exs
    defp deps do
      [
      {:electric, ">= 1.0.0-beta.18"}
      ]
    end

Add the Postgres db connection configuration to your application's config.
Electric accepts the same configuration format as
[Ecto](https://hexdocs.pm/ecto/Ecto.html) (and
[Postgrex](https://hexdocs.pm/postgrex/Postgrex.html#start_link/1)) so you can
reuse that configuration if you want:

    # config/*.exs
    database_config = [
      database: "ecto_simple",
      username: "postgres",
      password: "postgres",
      hostname: "localhost"
    ]

    config :my_app, Repo, database_config

    config :electric, replication_connection_opts: database_config

Or if you're getting your db connection from an environment variable, then you
can use
[`Electric.Config.parse_postgresql_uri!/1`](https://hexdocs.pm/electric/Electric.Config.html#parse_postgresql_uri!/1):

    # config/*.exs
    {:ok, database_config} =
      System.fetch_env!("DATABASE_URL")
      |> Electric.Config.parse_postgresql_uri()

    config :electric, replication_connection_opts: database_config

The Electric app will start up along with the rest of your Elixir app.

Beyond the required database connection configuration there are a lot of other
optional configuration parameters. See the [`Electric` docs for more
information](https://hexdocs.pm/electric/Electric.html).

## Live-request multiplexing (experimental)

> Experimental and opt-in. This is an addition of this fork, not part of upstream Electric, and
> its protocol may change between releases.

Electric exposes an authenticated WebSocket upgrade at
`GET /v1/shape/multiplex` for trusted proxies that need to park many silent
live waits without retaining one HTTP request process per wait. This endpoint
uses the `electric.shape-multiplex.v1` WebSocket subprotocol. It is only
available on the active Electric instance; read-only instances reject the
upgrade.

The client sends JSON text frames to add or remove logical waits:

```json
{"type":"watch","id":"request-1","handle":"...","offset":"12_0","cursor":"123","shape":{"table":"items","where":"value = 'a'"}}
{"type":"watch","id":"request-2","handle":"...","offset":"12_0","cursor":"123","shape":{"table":"items"},"timeout_ms":5000}
{"type":"unwatch","id":"request-1"}
```

`cursor` is the raw value of the previous `electric-cursor` header, or `null`
when there is no previous value. `shape` is required: it carries the
shape-definition query parameters (`table`, `where`, `columns`, `replica`,
`params`, ...) the proxy sends on the HTTP request for that handle, and is
parsed the same way. A watch whose handle is not the shape of that definition
is answered with a `shape_mismatch` error and never armed, so holding a handle
is not enough to learn whether its shape changed. A definition whose values the
shape endpoint would refuse is answered `invalid_shape`, and so is one carrying a
subset (`subset`, `subset__*` or `subset[...]`): the shape endpoint answers a
subset request at once with its rows, so there is nothing to wait for. Parameter-spelling checks performed on HTTP query strings do not apply to keys inside
`shape`: a malformed key such as `subset[order_by]x` is decoded as an ordinary query key
and ignored as a subset field.
`shape_unavailable` (retryable) means it could not be checked yet. The optional `timeout_ms` shortens the wait
below the configured long-poll timeout, never beyond it. Electric may acknowledge an armed wait with
`{"type":"ready","id":"request-1"}`. It then sends one terminal frame:

```json
{"type":"wake","id":"request-1","reason":"changes"}
{"type":"wake","id":"request-1","reason":"rotation"}
{"type":"no_change","id":"request-1","response":{"status":200,"headers":{},"body":[]}}
{"type":"error","id":"request-1","code":"...","message":"...","retryable":true}
```

Wake frames deliberately carry no shape rows. The proxy must issue the normal
shape HTTP request after a wake. A `no_change` frame contains the lowercase
HTTP response headers and JSON body that the proxy should return to the
unchanged shape client. The server uses its configured long-poll timeout, or
the smaller `timeout_ms`, and removes every wait after a terminal frame.

Embedded deployments can call `Electric.Plug.ShapeMultiplexPlug` directly. An
optional `:availability_guard` zero-arity function can enforce tenant ownership;
it must return `:ok` while the socket is valid and is rechecked periodically.

## Locale-projected columns (experimental)

> Experimental and opt-in. This is an addition of this fork, not part of upstream Electric, and
> its protocol may change between releases.

A shape can serve a locale-keyed column in a single language. Postgres holds the projection:
for a localized column `name`, the table has one stored generated column per language, named
`<column>_<locale>`, for example `name_EN`.

A shape request opts in with two parameters:

- `locale` is the language to serve. It must match `[A-Za-z0-9_-]{1,35}`.
- `localized_columns` lists the columns to project. It follows the identifier rules of
  `columns`: a camelCase name has to be quoted, or it folds to lowercase and stays unprojected.

For each listed column that the shape selects and that has a generated `<column>_<locale>`
column, the snapshot and the live stream serve the generated column's value under the base
column's name, with the generated column's type in the response schema. The generated columns
backing a projected column are left out of the response. A listed column with no generated
column for the requested locale is served as stored.

The locale and the projected columns are part of the shape's identity, so two languages never
share a handle. A shape that declares no locale keeps the identity it had. Where clauses and
row keys always read the stored column, never the projection.

A request that sets `queryable_columns` projects only through generated columns listed there:
one left off the list counts as missing, and its column is served as stored. A proxy that pins
`columns` alone to limit what a client reads must also pin or drop `locale` and
`localized_columns`. Forwarded from the client, they let a request serve any stored generated
column named `<column>_<suffix>` under the name of a selected column.

## Covered LSN wait (experimental)

> Experimental and opt-in. This is an addition of this fork, not part of upstream Electric, and
> its protocol may change between releases.

A client that has just committed a write can ask a shape request whether the shape holds that
write. It adds `wait_for_lsn=<lsn>`, the commit's LSN as a decimal integer, to its shape requests.
The up-to-date message of the response then carries `headers.covered_lsn`, a decimal LSN: every
transaction at or below it that changes the shape is in this response or earlier in the shape's
log. Once `covered_lsn` reaches the commit's LSN, the client holds the commit's rows for the shape.
`global_last_seen_lsn` makes no such promise: it can name a transaction the response does not
hold yet.

A writer can take `pg_current_wal_flush_lsn()` minus one, read after its commit returns: with
`synchronous_commit` on, the commit record is flushed before the commit returns, so its LSN is
below the flush position.

`covered_lsn` is the position just before the last one Electric published: either a commit it
fanned out to the shapes or the WAL end of a server keepalive. On an idle database the keepalive
that follows a commit names the end of that commit's record, so a wait for the commit does not
depend on later traffic. Electric vouches for it only when this instance runs the shape's
consumer, the consumer holds every transaction it was handed, and the shape's offset has not moved
since the request read it. Otherwise the response says `covered_lsn` 0: on an instance that only
reads what another one synced to disk, while the consumer buffers transactions behind its
snapshot, or while a truncate removes the shape. A response that reaches the end of a transaction
in the log covers that transaction too.

A live request with `wait_for_lsn` returns the shape's new changes as usual. It also returns
without a change as soon as `covered_lsn` reaches `wait_for_lsn` while the shape has no change to
serve, and at the long-poll timeout with the `covered_lsn` reached so far.

- `wait_for_lsn` must be an integer from 0 to 2^64 - 1, and cannot be empty, combined with
  `live_sse=true`, or used on a shape with subqueries, whose move-ins hold transactions back after
  they were fanned out. Each error answers 400.
- A multiplex watch whose `shape` carries `wait_for_lsn` is refused with `invalid_shape`. A proxy
  sends such a live request over HTTP.
- A request without `wait_for_lsn` gets upstream's messages, without `covered_lsn`.

## Shape generation reclaim (experimental)

> Experimental and opt-in. This is an addition of this fork, not part of upstream Electric, and
> its protocol may change between releases.

A caller or proxy that appends a trailing generation marker `AND '<token_prefix><N>' = '<token_prefix><N>'`
to every where clause and increments `N` to rotate shapes leaves earlier generations in the store
until expiry runs. `POST /v1/admin/shape-generations/reclaim` removes those superseded shapes one
page at a time.

Admin routes require `ELECTRIC_SECRET` in an `authorization: Bearer <secret>` header and return
`401` when `ELECTRIC_INSECURE` is enabled (`/v1/shape` also accepts `authorization: Bearer <secret>`,
taking precedence over query parameters).

The request body is JSON:

```json
{"current_generation": 5, "newest_collectable_generation": 4, "budget_ms": 30000, "after": "", "token_prefix": "shape-rotation:db"}
```

- `current_generation` and `newest_collectable_generation` are positive integers with
  `newest_collectable_generation < current_generation`.
- `budget_ms` bounds the removal work for this call (1 to 120,000 ms).
- `after` is the previous response's `next_after` cursor, or `""` for the first page. Send
  `next_after` back until it returns `null` (also returned when `outcome` is `"aborted"`).
- `token_prefix` is optional (default `"shape-rotation:db"`): 1 to 64 ASCII identifier characters
  (`A-Z`, `a-z`, `0-9`, `:`, `.`, `_`, `/`, `-`) placed immediately before the integer generation `N`.

If any matched shape on the page compares the `current_generation` token anywhere in its where
clause or has a running materializer (another shape depends on it), the call returns `409` with
the live handles and removes nothing on that page.

## Read-only health probe (experimental)

> Experimental and opt-in. This is an addition of this fork, not part of upstream Electric, and
> its protocol may change between releases.

`GET /v1/health?require=read_only` returns `200` as soon as the stack can serve existing shapes in
read-only mode, instead of `202` until it becomes fully active. The JSON `status` text (`waiting`
or `starting`) is unchanged, and `/v1/health` without the query parameter keeps upstream behaviour.
