## 0.1.0-alpha.8

Highlights:
- Multi-tenant sessions: tenant strategy/resolver now flows through document sessions, events, projections, subscriptions, and snapshotter; metrics export tags per-tenant counters.
- Tenant-aware CLI: `projections`, `subscriptions`, `docs`, and `snapshots` accept `--tenant` / `--all-tenants`, reusing the same resolver wiring as the runtime.
- Schema drift guardrail: new `health schema` command surfaces migrations-in-plan mode per tenant and flags drift before jobs run.
- Background workers: projection daemon and snapshot compaction respect tenant selection and set `search_path` locally.

Docs:
- README upgrade guide covering single-tenant → schema-per-tenant rollout, health checks, and CLI examples for `health schema`.

Tests:
- Added integration coverage for tenant-aware subscription CLI commands; suite remains green (`cargo test`).

Tooling:
- `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` clean.

## 0.1.0-alpha.7

Highlights:
- Added `Store::session()` that returns a session preconfigured from store defaults; `document_session()` deprecated.
- `StoreBuilder::session_defaults` and `.session_advisory_locks` let you configure session metadata at startup (no runtime mutation required).
- Document sessions now merge idempotency keys into existing default headers and examples/README updated with the session defaults workflow.
- New example `session_defaults` demonstrates configuring defaults and using `store.session()`.

Tooling:
- `cargo clippy --all-targets -- -D warnings` clean.

## 0.1.0-alpha.6

Highlights:
- Metrics export: Prometheus-style metrics at `/metrics` on the health server (docs reads/writes/conflicts, projection events processed, subscription deliveries/pending, snapshotter candidates/max_gap).
- Subscriptions scalability: per-group lag in CLI, backpressure tuning with `--max-in-flight`, persistent `subscription_groups.max_in_flight`, and throttle when pending exceeds limit; batch delivery tracing.
- Projection daemon long-run mode: `projections run-loop` with optional NOTIFY wakeups and Ctrl-C shutdown; simple health endpoint.
- Tenants CLI: create/sync/list schemas for schema-per-tenant.
- Document partial updates: `docs.patch` / `docs.patch_fields` with nested `jsonb_set`; README examples.
- Docs index advisor CLI: suggest GIN and expression indexes (DDL hints).

Tests:
- Added tests for snapshotter, docs patch, projection run loop flag, plus existing suites remain green.

## 0.1.0-alpha.5

Highlights:
- Snapshotter: background compaction API (`Snapshotter`, `AggregateFolder`), CLI (`snapshots compact-once`, `run-until-idle`), metrics/tracing, README example.
- Document repo DX: optimistic concurrency (`put`, `update`), versioned `get`, soft delete column and DSL knobs (`include_deleted`, `only_deleted`), CLI for docs.
- Stream aliases: `stream_aliases` table, `Store::resolve_stream_alias`, CLI `streams resolve`.
- Projection: metrics command (`projections metrics`).

Tests:
- Snapshotter test for long streams, docs repo e2e, idempotency conflict, consumer groups and manual ack, stream aliases.

## 0.1.0-alpha.4

Highlights:
- Subscriptions: consumer groups with per-group checkpoints and leasing; manual ack mode with explicit checkpointing; added `global_seq` to `EventEnvelope`.
- CLI: `subscriptions tail --group` for grouped consumers.
- Docs: README examples for consumer groups and manual ack.

Tests:
- Added group leasing test (only one consumer in group receives).
- Added manual ack test (checkpoint advances only on ack).

## 0.1.0-alpha.3

Highlights:
- Subscriptions: polling API with filters, checkpoints, pause/resume, optional LISTEN/NOTIFY wakeups; CLI admin (create/list/status/pause/resume/reset/tail).
- Aggregates enrichment: Events AppendBuilder (headers/ids, batching) and pre-commit validator hook; snapshots load/commit helpers.
- DX: Store builder, unified append_with, typed queries.

Docs:
- README updates for Store builder, append_with, subscriptions, aggregates enrichment.

Tests:
- Integration tests for subscriptions (delivery, pause/resume, filters), aggregates repo and snapshots.

## 0.1.0-alpha.2

Highlights:
- Schema manager CLI feature flag (`--features cli`), keep library lean by default.
- Projection runtime:
  - Daemon schema scoping (`SET LOCAL search_path`).
  - Exponential backoff with persisted attempts; DLQ on failures.
  - Advisory locks for leases; optional advisory locks for `append_stream`.
  - Typed queries, builder API, `run_until_idle()` convenience.
  - Tracing spans for observability.
- Documents: `INSERT ... ON CONFLICT DO UPDATE` upsert.
- Error: `#[non_exhaustive]`, `UnknownProjection` variant.

Docs:
- README updates: CLI feature flag usage, builder pattern, advisory locks, defaults guide.

Tests:
- Added `projection_runtime` integration test covering pause/resume and ticks.

Breaking changes:
- None intended; CLI gated behind feature; new APIs additive.
