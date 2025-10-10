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
- Projection replay and checkpointing helpers
- Projection runtime (daemon-ready primitives): per-projection checkpoints, leases, DLQ; CLI admin
- Developer tracing breadcrumbs with Mermaid export (dev-only)
- Integration test harness using Testcontainers (Docker required)

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
