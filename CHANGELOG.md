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
