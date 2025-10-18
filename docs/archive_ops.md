# Archive Operations Guide

This document explains how to roll out and operate Rillflow's hot/cold event archiving when running against PostgreSQL.

## Prerequisites

- Apply the migrations `sql/0003_event_archiving.sql` and `sql/0005_event_partitioning.sql`
  (and keep them in sync for future upgrades).
- Ensure application binaries are built from a release that contains the archive CLI/metrics (vNEXT or later).
- PostgreSQL role needs permissions to create triggers and write to `rf_streams`.
- Optional: Prometheus scraping of the `/metrics` endpoint for archive counters.

## Migration Checklist

```bash
cargo sqlx migrate run --database-url "$DATABASE_URL"
```

Key changes from `0003` + `0005`:

- `rf_streams` tracks archival metadata (`archived_at`, `archived_by`, `archive_reason`,
  `retention_class`).
- `events_archive` gains audit metadata and the hot→cold redirect trigger
  (`rf_events_redirect_archived`).
- `events` becomes a partitioned table: top-level partitions split `hot` vs `cold`, each with
  range partitions on `bucket_month`. Default partitions (`events_hot_default`,
  `events_cold_default`) ensure writes keep flowing.

To verify:

```sql
select child.relname
from pg_inherits
join pg_class parent on parent.oid = inhparent
join pg_class child on child.oid = inhrelid
join pg_namespace pn on pn.oid = parent.relnamespace
where pn.nspname = current_schema()
  and parent.relname = 'events';
```

Down migrations revert the redirect trigger (`0003 ... .down.sql`) and the partitioned layout
(`0005_event_partitioning.down.sql`). Only run them if you intend to disable archiving or return to
the pre-partitioned schema.

## Configuration Quick Start

- Dual-table backend (default):

  ```rust
  let store = Store::builder(&url)
      .archive_backend(ArchiveBackend::DualTable)
      .archive_redirect_enabled(true)       // send archived streams to events_archive
      .include_archived_by_default(false)   // hot reads stay small
      .build()
      .await?;
  ```

- Partition backend (uses the `events.retention_class` column introduced in `0004`):

  ```rust
  let partitioned = Store::builder(&url)
      .archive_backend(ArchiveBackend::Partitioned)
      .include_archived_by_default(false)
      .build()
      .await?;
  ```

  Partitioned deployments should create LIST partitions on `events.retention_class` (and optional
  RANGE partitions on `created_at`) once data has been tagged `hot`/`cold`. The runtime keeps hot
  workloads filtered via `retention_class = 'hot'` while `with_archived()` and rebuild paths scan
  both partitions.

- Toggle redirect at runtime: `store.set_archive_redirect(true/false).await?` (only meaningful for
  the dual-table backend).
- Per-query opt-in: `store.events().with_archived()` unions cold data on demand.

## CLI Reference

```bash
# Show counts/state by schema (hot vs cold streams, table sizes, redirect flag)
cargo run --bin rillflow -- archive status --database-url "$DATABASE_URL"

# Mark a stream archived
cargo run --bin rillflow -- archive mark <stream-id> \
  --reason "customer closed account" \
  --database-url "$DATABASE_URL"

# Move events older than 2024-01-01 (supports --dry-run, --batch-size, --stream)
cargo run --bin rillflow -- archive move \
  --before 2024-01-01T00:00:00Z \
  --batch-size 500 \
  --database-url "$DATABASE_URL"

# Reactivate a stream (hot writes resume)
cargo run --bin rillflow -- archive reactivate <stream-id> --database-url "$DATABASE_URL"

# Toggle redirect trigger
cargo run --bin rillflow -- archive redirect --enable true --database-url "$DATABASE_URL"

# Create a monthly partition (partitioned backend)
cargo run --bin rillflow -- archive partition \
  --retention hot \
  --month 2025-01-01 \
  --database-url "$DATABASE_URL"
```

Tips:
- Use `--dry-run` with `archive move` to inspect candidate streams without modifying data.
- Provide `--stream <uuid>` to migrate a single stream; omit to iterate batches across all tenants.
- For conjoined tenancy, include `--tenant <name>` to scope operations.
- For partition maintenance, supply `--tenant <name>` when targeting a tenant schema.

## Metrics & Logging

Prometheus counters/gauges (available per-tenant if tenancy is enabled):

- `archive_streams_marked_total`
- `archive_streams_reactivated_total`
- `archive_events_moved_total`
- `archive_move_batches_total`
- `archive_move_duration_seconds`
- `archive_events_hot_bytes`

Each CLI action logs structured entries (`stream marked archived`, `archive move batch`, etc.) with stream/tenant identifiers. Scrape `/metrics` or tail logs to build alerts (e.g., unexpected spikes in archived streams or zero mover throughput).

## Operational Guidance

- Start with redirect **disabled** in production. Run the mover (dry-run first) to segment existing cold data, then enable redirect once confidence is high.
- Schedule mover jobs during low-traffic windows. Use smaller `--batch-size` values if you observe lock contention.
- Monitor archive metrics after rollout; alert when hot streams grow beyond expected thresholds, mover duration spikes, or mover counters stay flat.
- Reactivation is rare; when performed it resets `archived_at` and returns the stream to the hot tier—double-check downstream projections before resuming writes.
- Back up `events/archive` tables before large migrations; mover operations are ordinary SQL DML and can be wrapped in your backup/restore strategy.

### Partition Maintenance

- Default partitions (`events_hot_default`, `events_cold_default`) accept writes even if a dedicated
  monthly partition is missing. Create range partitions ahead of time with `archive partition` to
  keep default partitions small and to move cold data onto cheaper tablespaces.
- Monthly partitions are named `events_hot_YYYY_MM` / `events_cold_YYYY_MM`. Dropping an exhausted
  partition is a fast metadata operation (after ensuring no active data remains).
- When cloning tenant schemas manually, run `sql/0005_event_partitioning.sql` (or invoke
  `archive partition`) inside the new schema to provision the partition tree.

## Testing & Validation

Integration tests (`cargo test --test integration_postgres`) cover both the dual-table and
partitioned backends via Testcontainers. Run them locally (Docker required) and in CI to catch
regressions.

### Performance Validation

- Hot-only regression: seed 50–100k events, run a projection replay, and capture baseline timings
  with `ArchiveBackend::DualTable` (redirect disabled) or `ArchiveBackend::Partitioned` with
  `include_archived=false`. Record the `projection_fetch_batch` metric and query duration logs.
- Mixed workload regression: archive half the streams using `archive move --before <ts>` and rerun
  the same projection. Ensure batch latency stays within ±5% of the hot-only baseline.
- Archive mover: run `archive move --before <future>` in production-like conditions; monitor
  `archive_move_duration_seconds` and `archive_move_batches_total` along with database locks. Tweak
  `--batch-size` if duration climbs or lock contention appears.
- Document results (baseline throughput, acceptable deltas) and keep them with release notes so
  operators can compare after upgrades.

For manual smoke tests:

1. Insert a known stream with events.
2. `archive mark` the stream and `archive move --dry-run` to confirm visibility.
3. Execute `archive move --before <future>` and verify `events_archive`.
4. `archive reactivate` and append another event; verify it lands back in `events`.
