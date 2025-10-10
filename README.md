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
```

Feature flag: the CLI is gated behind the `cli` feature. Enable it when building/running:

```bash
cargo run --features cli --bin rillflow -- schema-plan --database-url "$DATABASE_URL"
```

## Features

- JSONB document store with optimistic versioning
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

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
