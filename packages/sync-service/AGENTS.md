# packages/sync-service

Read with the root `AGENTS.md`. Applies to `packages/sync-service/**` and `integration-tests/**`.

The service tails Postgres logical replication and serves shapes, which are subsets of tables, over HTTP.
It is read-path only: it never accepts a write.

## Map

| path                                        | what                                                              |
| ------------------------------------------- | ----------------------------------------------------------------- |
| `lib/electric/plug/`                        | HTTP API: shape requests, health                                  |
| `lib/electric/shapes/`                      | shape definitions, their lifecycle and consumers, routing filter  |
| `lib/electric/replication/`                 | logical replication: WAL in, changes out to the shapes they match |
| `lib/electric/shape_cache/`                 | per-shape storage, snapshot and log serving, cleanup              |
| `lib/electric/postgres/`, `lib/pg_interop/` | connections, identifiers, types                                   |
| `lib/electric/telemetry/`                   | metrics and traces                                                |
| `config/runtime.exs`                        | every environment option, validated                               |
| `dev/`                                      | Docker Compose: Postgres on 54321 and 54322, pgbouncer on 65432   |

A request resolves or creates a shape, then streams its snapshot and its log. The replication pipeline
routes each change to the shapes it matches.

## Commands

Run from `packages/sync-service`.

```sh
mix deps.get
mix start_dev      # the services in ./dev; mix stop_dev removes them and their volumes
mix test test/electric/shapes/shape_test.exs
mix test --include slow
mix format
iex -S mix         # run the service against the dev Postgres
```

## Tests

- Pick the level that proves the change: a module test, a router test (`router_test.exs`,
  `low_privilege_router_test.exs`) for the service as a whole, `test/integration/` when client logic is
  involved, Lux for restarts and failures of the whole stack.
- `mix test` excludes `:slow`, `:oracle`, `:performance` and `:memory`. CI includes `:slow` and runs
  `:oracle` and `:performance` as their own jobs. No CI job runs `:memory`: run the ones that cover what
  you changed, and say which.
- One `file:line` argument makes every path in that `mix test` command a location filter, and a location
  filter beats the default excludes: the `:memory` and `:performance` tests in those files run too. Keep
  line-targeted runs and whole-file runs in separate commands.
- The `:memory` scale tests outgrow a small container. `shape_db_boot_scale_test.exs` needs more than
  9 GB of memory at its default 60,000 shapes, and the shape-store tests fill a 1 GB `tmp/`. Lower
  `BOOT_SCALE_SHAPE_COUNT` on a smaller machine and say so. `:enospc` or SQLite error 13 there means the
  disk is full, not that the code is wrong.
- A test that waits for a message, a task or a polled condition uses the suite's default receive timeout
  (`ASSERT_RECEIVE_TIMEOUT`, 5 s): no shorter budget, no fixed sleep. A loaded CI runner overruns both.
  `refute_receive` keeps its short default, since every passing run pays it in full.
- A module patched with `Repatch.patch(..., mode: :shared, ...)` joins the prewarm list in
  `test/test_helper.exs`. Without it Repatch recompiles the module on the first patch and unrelated async
  tests fail, rarely.
- Prefer one structural assert with pattern matching over several asserts on single fields.

## Switches

- Feature flags are listed in `@known_feature_flags` in `lib/electric/config.ex` and set through
  `ELECTRIC_FEATURE_FLAGS`. A flag that is off leaves the protocol as upstream's.
- The performance switches are read in `config/runtime.exs` (`ELECTRIC_ROUTER_SHARDS`,
  `ELECTRIC_SHARED_CONVERSION` and the rest of that block) and default to off in `lib/electric/config.ex`.
  A new one is added to every job that sets them in `.github/workflows/sync_service_tests.yml` and
  `integration_tests.yml`, or CI never runs it.

## Contracts

- `Electric.Application.api/1`, `api_plug_opts/1` and `configuration/1` are `@doc false` but public: the
  Elixir client's embedded mode and Phoenix.Sync call them.
- The body of `/v1/health` is a public contract. Lux tests assert it verbatim; do not change it to carry
  internal state.
- The HTTP API is open unless something in front of it authenticates. `ELECTRIC_INSECURE=true` is for
  development; production sets `ELECTRIC_SECRET`. Review a change to `lib/electric/plug/` with that in
  mind.
