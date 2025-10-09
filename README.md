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

## Features

- JSONB document store with optimistic versioning
- Event streams with expected-version checks
- Projection replay and checkpointing helpers
- Developer tracing breadcrumbs with Mermaid export (dev-only)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

