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

- `docs`: JSONB document store with optimistic version column
- `events`: append-only event store with global and stream sequence numbers
- `projections`: checkpoint table for projection processors

Ensure your application runs this migration before executing any Rillflow APIs.

