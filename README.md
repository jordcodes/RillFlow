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

## Features

- JSONB document store with optimistic versioning
- LINQ-like document query DSL (filters, sorting, paging, projections)
- Event streams with expected-version checks
- Projection replay and checkpointing helpers
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

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

