# Rillflow

Rillflow is a lightweight document and event store for Rust applications, backed by PostgreSQL. It provides a JSONB document store, append-only event streams with optimistic concurrency, projection scaffolding, and developer tracing breadcrumbs that can export to Mermaid diagrams.

## Quickstart

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
createdb rillflow_dev
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/rillflow_dev
cargo sqlx migrate run
cargo run --example quickstart
```

### Integration Tests (requires Docker)

```bash
docker --version    # ensure Docker daemon is running
cargo test --test integration_postgres
```

### CLI

- Schema
```bash
cargo run --bin rillflow -- schema-plan --database-url "$DATABASE_URL" --schema public
cargo run --bin rillflow -- schema-sync  --database-url "$DATABASE_URL" --schema public
# Tenants (schema-per-tenant)
cargo run --bin rillflow -- tenants ensure acme --database-url "$DATABASE_URL"
cargo run --bin rillflow -- tenants status acme --database-url "$DATABASE_URL"
cargo run --bin rillflow -- tenants list        --database-url "$DATABASE_URL"
cargo run --bin rillflow -- tenants archive acme --output backups/acme --database-url "$DATABASE_URL"
cargo run --bin rillflow -- tenants drop acme --force --database-url "$DATABASE_URL"
```

- Projections admin
```bash
cargo run --bin rillflow -- projections list --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections status my_projection --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections pause my_projection --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections resume my_projection --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections reset-checkpoint my_projection 0 --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections rebuild my_projection --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections run-once --database-url "$DATABASE_URL"            # tick all
cargo run --bin rillflow -- projections run-once --name my_projection --database-url "$DATABASE_URL"
# run until idle (all or one)
cargo run --bin rillflow -- projections run-until-idle --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections run-until-idle --name my_projection --database-url "$DATABASE_URL"

# DLQ admin
cargo run --bin rillflow -- projections dlq-list my_projection --database-url "$DATABASE_URL" --limit 100
cargo run --bin rillflow -- projections dlq-requeue my_projection --id 123 --database-url "$DATABASE_URL"
cargo run --bin rillflow -- projections dlq-delete my_projection --id 123 --database-url "$DATABASE_URL"
# metrics
cargo run --bin rillflow -- projections metrics my_projection --database-url "$DATABASE_URL"
# run loop (long-running)
cargo run --bin rillflow -- projections run-loop --use-notify true --health-bind 0.0.0.0:8080 --database-url "$DATABASE_URL"
# metrics endpoint (served on the same bind as health)
curl -s http://127.0.0.1:8080/metrics | head -50
```

- Streams
```bash
cargo run --bin rillflow -- streams resolve orders:42 --database-url "$DATABASE_URL"
```

- Documents admin
```bash
# read JSON
cargo run --bin rillflow -- docs get <uuid> --database-url "$DATABASE_URL"
# soft-delete / restore
cargo run --bin rillflow -- docs soft-delete <uuid> --database-url "$DATABASE_URL"
cargo run --bin rillflow -- docs restore <uuid> --database-url "$DATABASE_URL"
# index advisor (DDL suggestions)
cargo run --bin rillflow -- docs index-advisor --gin --field email --database-url "$DATABASE_URL"
```

- Snapshots
```bash
cargo run --bin rillflow -- snapshots compact-once --threshold 200 --batch 200 --database-url "$DATABASE_URL" --schema public
cargo run --bin rillflow -- snapshots run-until-idle --threshold 200 --batch 200 --database-url "$DATABASE_URL" --schema public
```


### Tenants health check

```bash
# Drift detection across all known tenants
cargo run --features cli --bin rillflow -- health schema --all-tenants \
  --database-url "$DATABASE_URL"

# Single tenant check
cargo run --features cli --bin rillflow -- health schema --tenant acme \
  --database-url "$DATABASE_URL"
```

Feature flag: the CLI is gated behind the `cli` feature. Enable it when building/running:

```bash
cargo run --features cli --bin rillflow -- schema-plan --database-url "$DATABASE_URL"
```

## Features

- JSONB document store with optimistic versioning
- Document session unit-of-work with identity map + staged writes
- LINQ-like document query DSL (filters, sorting, paging, projections)
- Composable compiled queries for cached predicates and reuse
- Event streams with expected-version checks
  - Envelopes: headers, causation_id, correlation_id, created_at (API: read envelopes, append with headers)
- Projection replay and checkpointing helpers
- Projection runtime (daemon-ready primitives): per-projection checkpoints, leases, DLQ; CLI admin
- Developer tracing breadcrumbs with Mermaid export (dev-only)
- Integration test harness using Testcontainers (Docker required)

### Store builder

```rust
use std::time::Duration;
let store = rillflow::Store::builder(std::env::var("DATABASE_URL")?)
    .max_connections(20)
    .connect_timeout(Duration::from_secs(5))
    .build()
    .await?;
```

### Append with options (headers/causation/correlation)

```rust
use rillflow::{Event, Expected};
use serde_json::json;

let opts = rillflow::events::AppendOptions {
    headers: Some(json!({"req_id": "r-123"})),
    causation_id: None,
    correlation_id: None,
};
store
  .events()
  .append_with(stream_id, Expected::Any, vec![Event::new("E1", &json!({}))], &opts)
  .await?;
```

Idempotent appends:

```rust
store
  .events()
  .builder(stream_id)
  .idempotency_key("req-123")
  .push(Event::new("OrderPlaced", &json!({"order_id": 42})))
  .send()
  .await?;
// a second call with the same idempotency key will return Error::IdempotencyConflict
```

### Subscriptions (polling)

Create a subscription with filters, then tail events.

```bash
cargo run --features cli --bin rillflow -- subscriptions create s1 --event-type Ping --start-from 0
cargo run --features cli --bin rillflow -- subscriptions tail s1 --limit 10
```

Programmatic:

```rust
use rillflow::subscriptions::{Subscriptions, SubscriptionFilter, SubscriptionOptions};
let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
let filter = SubscriptionFilter { event_types: Some(vec!["Ping".into()]), ..Default::default() };
let opts = SubscriptionOptions { start_from: 0, ..Default::default() };
let (_handle, mut rx) = subs.subscribe("s1", filter, opts).await?;
while let Some(env) = rx.recv().await {
    println!("{} {} {}", env.stream_id, env.stream_seq, env.typ);
}
```

Consumer groups (checkpoint + leasing per group):

```rust
use rillflow::subscriptions::{Subscriptions, SubscriptionFilter, SubscriptionOptions};

let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
let filter = SubscriptionFilter { event_types: Some(vec!["Ping".into()]), ..Default::default() };
let mut opts = SubscriptionOptions { start_from: 0, ..Default::default() };
opts.group = Some("workers-a".to_string());
let (_h, mut rx) = subs.subscribe("orders", filter, opts).await?;
```

CLI tail with group:

```bash
cargo run --features cli --bin rillflow -- subscriptions tail orders --group workers-a --limit 10 --database-url "$DATABASE_URL"

# group admin
cargo run --features cli --bin rillflow -- subscriptions groups orders --database-url "$DATABASE_URL"
cargo run --features cli --bin rillflow -- subscriptions group-status orders --group workers-a --database-url "$DATABASE_URL"
# outputs include last_seq, head and lag per group for quick capacity checks

# tune backpressure per group
cargo run --features cli --bin rillflow -- subscriptions set-group-max-in-flight orders --group workers-a --value 500 --database-url "$DATABASE_URL"
cargo run --features cli --bin rillflow -- subscriptions set-group-max-in-flight orders --group workers-a --database-url "$DATABASE_URL"   # unset
```

Manual ack mode (explicit checkpointing):

```rust
use rillflow::subscriptions::{Subscriptions, SubscriptionFilter, SubscriptionOptions, AckMode};

let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
let filter = SubscriptionFilter { event_types: Some(vec!["Ping".into()]), ..Default::default() };
let mut opts = SubscriptionOptions { start_from: 0, ..Default::default() };
opts.ack_mode = AckMode::Manual; // disable auto checkpointing
let (handle, mut rx) = subs.subscribe("s1", filter, opts).await?;

while let Some(env) = rx.recv().await {
    // ... process ...
    handle.ack(env.global_seq).await?; // checkpoint when done
}
```

Transactional ack (exactly-once-ish):

```rust
use sqlx::{Postgres, Transaction};
use rillflow::subscriptions::{AckMode, SubscriptionOptions};

let mut opts = SubscriptionOptions { start_from: 0, ..Default::default() };
opts.ack_mode = AckMode::Manual; // we will ack inside our DB tx
let (handle, mut rx) = subs.subscribe("orders", filter, opts).await?;

while let Some(env) = rx.recv().await {
  let mut tx: Transaction<'_, Postgres> = store.pool().begin().await?;
  // 1) apply side effects in the same transaction
  // ... your writes using &mut *tx ...
  // 2) ack the subscription checkpoint in the same transaction
  handle.ack_in_tx(&mut tx, env.global_seq).await?;
  tx.commit().await?;
}
```

### Aggregates

Fold streams into domain state with a simple trait and repository.

```rust
use rillflow::{Aggregate, AggregateRepository, Event};

struct Counter { n: i32 }
impl Aggregate for Counter {
    fn new() -> Self { Self { n: 0 } }
    fn apply(&mut self, e: &rillflow::EventEnvelope) { if e.typ == "Inc" { self.n += 1; } }
    fn version(&self) -> i32 { self.n }
}

let repo = AggregateRepository::new(store.events());
let id = uuid::Uuid::new_v4();
repo.commit(id, rillflow::Expected::Any, vec![Event::new("Inc", &())]).await?;
let agg: Counter = repo.load(id).await?;
```

### Stream Aliases

Resolve a human-friendly alias to a `Uuid`, creating it on first use.

```rust
let id = store.resolve_stream_alias("orders:42").await?;
store
  .events()
  .append_stream(id, rillflow::Expected::Any, vec![Event::new("OrderPlaced", &serde_json::json!({}))])
  .await?;
```

Append builder and validator hook:

```rust
use serde_json::json;

// Fluent append with headers/ids and batching
store
  .events()
  .builder(id)
  .headers(json!({"req_id": "abc-123"}))
  .push(Event::new("Inc", &json!({})))
  .expected(rillflow::Expected::Any)
  .send()
  .await?;

// Optional pre-commit validator (receives aggregate state as JSON)
fn validate(state: &serde_json::Value) -> rillflow::Result<()> {
  // example: reject negative counters
  if state.get("n").and_then(|v| v.as_i64()).unwrap_or(0) < 0 { return Err(rillflow::Error::VersionConflict); }
  Ok(())
}

let repo = rillflow::AggregateRepository::new(store.events())
  .with_validator(validate);
```

### Snapshots

Persist aggregate state every N events to speed up loads.

```rust
// write snapshot every 100 events
repo.commit_and_snapshot(id, &agg, vec![Event::new("Inc", &())], 100).await?;
// fast load using snapshot + tail
let agg: Counter = repo.load_with_snapshot(id).await?;
```

Programmatic snapshotter (background compaction):

```rust
use std::sync::Arc;
use rillflow::snapshotter::{AggregateFolder, Snapshotter, SnapshotterConfig};

// For an aggregate `Counter` implementing Aggregate + Serialize
let repo = rillflow::AggregateRepository::new(store.events());
let folder = AggregateFolder::<Counter>::new(repo);
let snap = Snapshotter::new(store.pool().clone(), Arc::new(folder), SnapshotterConfig { threshold_events: 200, batch_size: 200, ..Default::default() });
snap.run_until_idle().await?;
```

### Document Queries

```rust
use rillflow::{
    Store,
    query::{Predicate, SortDirection},
};

#[derive(serde::Deserialize)]
struct Customer {
    email: String,
    status: String,
}

async fn active_customers(store: &Store) -> rillflow::Result<Vec<Customer>> {
    store
        .docs()
        .query::<Customer>()
        .filter(Predicate::eq("status", "active"))
        .order_by("email", SortDirection::Asc)
        .page(1, 25)
        .fetch_all()
        .await
}
```

### Document Repository (OCC and soft delete)

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Customer { email: String, tier: String }

let id = uuid::Uuid::new_v4();

// put returns new version (starts at 1)
let v1 = store.docs().put(&id, &Customer { email: "a@x".into(), tier: "free".into() }, None).await?;

// get returns (doc, version)
let (cust, ver) = store.docs().get::<Customer>(&id).await?.unwrap();

// update with optimistic concurrency
let v2 = store.docs().update::<Customer, _>(&id, ver, |c| c.tier = "pro".into()).await?;

// soft delete / restore (programmatic or via CLI)
sqlx::query("update docs set deleted_at = now() where id = $1").bind(id).execute(store.pool()).await?;
sqlx::query("update docs set deleted_at = null where id = $1").bind(id).execute(store.pool()).await?;
```

Partial updates (jsonb_set):

```rust
// set a single field
store.docs().patch(&id, Some(2), "profile.name", &serde_json::json!("Alicia")).await?;

// set multiple fields in one statement
store.docs().patch_fields(
  &id,
  Some(3),
  &[("profile.age", serde_json::json!(31)), ("extra.flag", serde_json::json!(true))]
).await?;
```

See `MIGRATIONS.md` for guidance on adding workload-specific JSONB indexes for query performance.
### Projections Runtime (daemon primitives)

Minimal runtime to process events into read models with leases, backoff, DLQ and admin CLI.

Programmatic usage:

```rust
use std::sync::Arc;
use rillflow::{Store, SchemaConfig};
use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};
use rillflow::projections::ProjectionHandler;

struct MyProjection;

#[async_trait::async_trait]
impl ProjectionHandler for MyProjection {
    async fn apply(
        &self,
        event_type: &str,
        body: &serde_json::Value,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> rillflow::Result<()> {
        // mutate your read model using tx
        Ok(())
    }
}

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let store = Store::connect(&std::env::var("DATABASE_URL")?).await?;
    store.schema().sync(&SchemaConfig::single_tenant()).await?; // ensure tables

    let mut daemon = ProjectionDaemon::new(store.pool().clone(), ProjectionWorkerConfig::default());
    daemon.register("my_projection", Arc::new(MyProjection));
    let _ = daemon.tick_once("my_projection").await?; // or loop/timer
    Ok(())
}
```

See runnable example: `examples/projection_run_once.rs`.

Builder usage and idle runner:

```rust
use std::sync::Arc;
use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};

let daemon = ProjectionDaemon::builder(store.pool().clone())
    .schema("public")
    .batch_size(500)
    .register("my_projection", Arc::new(MyProjection))
    .build();

daemon.run_until_idle().await?;

// Long-running loop with graceful shutdown and optional NOTIFY wakeups
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
let stop = Arc::new(AtomicBool::new(false));
let stop2 = stop.clone();
tokio::spawn(async move {
  let _ = tokio::signal::ctrl_c().await;
  stop2.store(true, Ordering::Relaxed);
});
daemon.run_loop(true, stop).await?; // true = use LISTEN/NOTIFY (channel defaults to rillflow_events)
```

Advisory locks (optional) for append:

```rust
store
  .events()
  .with_advisory_locks()
  .append_stream(stream_id, rillflow::Expected::Any, vec![evt])
  .await?;
```

### Choosing your defaults

- Single-tenant vs multi-tenant: use `SchemaConfig::single_tenant()` for `public`, or `TenancyMode::SchemaPerTenant` to create per-tenant schemas via the CLI/API.
- Projection schema: set `ProjectionDaemon::builder(...).schema("app")` if you don’t use `public`.
- Advisory locks: keep projection lease locks on (default). Enable append advisory locks only if you see writer contention on streams.
- Indexes: start with the default GIN on `doc`, then add expression indexes for hot paths (emails, timestamps, numeric ranges). See `MIGRATIONS.md` for examples.
- Examples: see `examples/projection_run_once.rs` for a runnable projection demo end-to-end.


#### Compiled Queries

```rust
use rillflow::{
    Store,
    query::{CompiledQuery, DocumentQueryContext, Predicate, SortDirection},
};

struct VipCustomers;

impl CompiledQuery<serde_json::Value> for VipCustomers {
    fn configure(&self, ctx: &mut DocumentQueryContext) {
        ctx.filter(Predicate::eq("active", true))
            .filter(Predicate::contains("tags", serde_json::json!(["vip"])))
            .order_by("first_name", SortDirection::Asc)
            .select_fields(&[("email", "email"), ("status", "active")]);
    }
}

async fn load_vips(store: &Store) -> rillflow::Result<Vec<serde_json::Value>> {
    store.docs().execute_compiled(VipCustomers).await
}
```

### Document Sessions & Aggregates

```rust
use rillflow::{Aggregate, AggregateRepository, DocumentSession};
use uuid::Uuid;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Customer { id: Uuid, email: String, tier: String }

#[derive(Clone, Default, serde::Serialize)]
struct VisitCounter { total: i32 }

impl Aggregate for VisitCounter {
    fn new() -> Self { Self { total: 0 } }
    fn apply(&mut self, env: &rillflow::EventEnvelope) {
        if env.typ == "CustomerVisited" { self.total += 1; }
    }
    fn version(&self) -> i32 { self.total }
}

let store = Store::builder(&url)
    .session_defaults(AppendOptions {
        headers: Some(serde_json::json!({"source": "api"})),
        causation_id: None,
        correlation_id: None,
    })
    .session_advisory_locks(true)
    .build()
    .await?;

// Store is cheap to clone and share. Configure defaults up front so every clone
// produces sessions with consistent metadata.

let mut session = store.session();
let customer = Customer { id, email: "new@example.com".into(), tier: "starter".into() };
session.store(id, &customer)?;
session.enqueue_event(
    id,
    rillflow::Expected::Any,
    rillflow::Event::new("CustomerRegistered", &customer),
)?;
let repo = AggregateRepository::new(store.events());
let mut aggregates = session.aggregates(&repo);
aggregates.commit(
    id,
    rillflow::Expected::Any,
    vec![rillflow::Event::new("CustomerVisited", &serde_json::json!({}))],
)?;
aggregates.commit_and_snapshot(
    id,
    &VisitCounter::default(),
    vec![rillflow::Event::new("CustomerVisited", &serde_json::json!({}))],
    2,
)?;
session.save_changes().await?;
```

### Multi-Tenancy (Schema Per Tenant)

```rust
use rillflow::{Store, store::TenantStrategy};

let store = Store::builder(&url)
    .tenant_strategy(TenantStrategy::SchemaPerTenant)
    .build()
    .await?;

store.ensure_tenant("acme").await?;
store.ensure_tenant("globex").await?;

// ensure_tenant() is idempotent and guarded by a Postgres advisory lock so
// concurrent app instances won't double-run migrations. Once a tenant is
// provisioned it is cached in-process for fast session spins.

let mut acme = store.session();
acme.context_mut().tenant = Some("acme".into());
acme.store(customer_id, &customer_body)?;
acme.save_changes().await?;

let mut globex = store.session();
globex.context_mut().tenant = Some("globex".into());
assert!(globex.load::<Customer>(&customer_id).await?.is_none());

// If you forget to provision a tenant before calling save_changes(), the
// session will fail fast with rillflow::Error::TenantNotFound("tenant_acme").
```

#### Upgrading from single-tenant to schema-per-tenant

1. **Prerequisites**
   - Ensure the base schema (`public` by default) is fully migrated (`schema-sync`).
   - Take a database backup.
   - Identify the tenant identifiers you plan to introduce (e.g. `acme`, `globex`).

2. **Run schema provisioning**
   - Create the per-tenant schemas using the CLI or API:
     ```bash
     cargo run --features cli --bin rillflow -- tenants ensure acme --database-url "$DATABASE_URL"
     cargo run --features cli --bin rillflow -- tenants ensure globex --database-url "$DATABASE_URL"
     ```

3. **Backfill data**
   - For each tenant, copy existing documents/events into the new schema (custom SQL/backfill job).
   - Use the snapshots/export helpers if you plan to archive or migrate historical tenants.

4. **Validate drift**
   - Run the health command to make sure each tenant schema matches the latest migrations:
     ```bash
     cargo run --features cli --bin rillflow -- health schema --all-tenants --database-url "$DATABASE_URL"
     ```
   - Fix any drift before enabling the new strategy.

5. **Enable schema-per-tenant**
   - Update your `Store::builder` to call `.tenant_strategy(TenantStrategy::SchemaPerTenant)` and provide a resolver (per-request tenant lookup).
   - Restart application nodes; watch logs for `tenant required` errors.

6. **Post-migration checks**
   - Verify metrics/traces include tenant labels.
   - Run smoke tests against each tenant (documents/events/projections).
   - Optionally remove the legacy single-tenant data once confirmed.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
