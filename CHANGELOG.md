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
