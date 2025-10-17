# Migrations

Rillflow stores its schema migrations in `sql/`. Apply them with your preferred Postgres migration tooling or the `sqlx-cli`.

## Applying with `sqlx`

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/rillflow_dev
cargo sqlx migrate run

# optional: run integration tests (requires Docker)
cargo test --test integration_postgres
```

## First Migration

The `sql/0001_init.sql` migration creates:

- `docs`: JSONB document store with optimistic version column and optional `tenant_id` for conjoined tenancy
- `events`: append-only event store with global and stream sequence numbers
  - Columns include `headers jsonb`, `causation_id uuid`, `correlation_id uuid`, and `created_at`.
- `projections`: checkpoint table for projection processors (adds `tenant_id` in conjoined mode)
- `snapshots`: snapshotting table for aggregates (`stream_id uuid pk, version int, body jsonb, created_at timestamptz`).
- `stream_aliases`: human-friendly stream aliases (includes `tenant_id` in conjoined mode).
- `subscriptions`: consumer checkpoints (`tenant_id` support for conjoined mode).
- `subscription_groups`: per-group checkpoints (`tenant_id` support for conjoined mode).
- `subscription_group_leases`: cooperative leases (`tenant_id` support for conjoined mode).
- `subscription_dlq`: dead-letter queue entries (`tenant_id` support for conjoined mode).

> **Upgrading to conjoined tenancy**
>
> When you enable conjoined tenancy on an existing database, populate the new `tenant_id`
> columns on `subscriptions`, `subscription_groups`, `subscription_group_leases`, and
> `subscription_dlq` before relying on them for isolation. The schema planner will add the
> columns automatically; you can backfill them with an `update` statement that tags each row
> with the appropriate tenant identifier.

Rillflow's schema manager (CLI) can also create projection runtime support tables:

- `projection_control`: per-projection pause flags and backoff windows
- `projection_leases`: cooperative leases to avoid double processing in multi-worker setups
- `projection_dlq`: dead-letter queue of failed events for operator review

Ensure your application runs this migration before executing any Rillflow APIs.

### Indexing Guidance

The core migration only creates a GIN index on the `doc` column. This keeps the base schema portable while enabling broad JSONB containment lookups. For production workloads you should add expression indexes tailored to the fields you query most often. Examples:

```
-- Case-insensitive email lookups
create index concurrently if not exists docs_email_idx
    on docs ((lower(doc->>'email')));

-- Numeric range scans on `doc->>'created_at'` stored as timestamptz
create index concurrently if not exists docs_created_at_idx
    on docs (((doc->>'created_at')::timestamptz));

-- Array membership checks
create index concurrently if not exists docs_tags_idx
    on docs using gin ((doc->'tags'));
```

Apply these via your own migration files after running the base schema.
