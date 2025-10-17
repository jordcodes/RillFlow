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

The core migration only creates a GIN index on the `doc` column. This keeps the base schema portable while enabling broad JSONB containment lookups. For production workloads you have two indexing options:

#### Option 1: Expression Indexes (Manual)

Create indexes directly on JSONB expressions:

```sql
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

#### Option 2: Duplicated Fields (Recommended for High Performance)

For frequently queried fields, use **duplicated fields** to extract JSONB data into native PostgreSQL columns with automatic synchronization. This provides 10-100x faster queries than expression indexes:

```rust
use rillflow::schema::{DuplicatedField, DuplicatedFieldType, IndexType, SchemaConfig};

let config = SchemaConfig::single_tenant()
    .add_duplicated_field(
        DuplicatedField::new("email", "d_email", DuplicatedFieldType::Text)
            .with_indexed(true)
            .with_transform("lower({value})")  // Case-insensitive
    )
    .add_duplicated_field(
        DuplicatedField::new("age", "d_age", DuplicatedFieldType::Integer)
            .with_indexed(true)
    )
    .add_duplicated_field(
        DuplicatedField::new("created_at", "d_created_at", DuplicatedFieldType::Timestamptz)
            .with_indexed(true)
    );

store.schema().sync(&config).await?;
```

**How Duplicated Fields Work:**

1. **Schema Sync**: Adds columns to `docs` table (e.g., `d_email text`, `d_age integer`)
2. **Automatic Triggers**: PostgreSQL triggers keep duplicated columns in sync with `doc` JSONB on INSERT/UPDATE
3. **Query Rewriting**: Rillflow's query DSL automatically uses duplicated columns when available
4. **Index Creation**: Optionally creates indexes on duplicated columns for maximum performance

**Example: Before and After**

*Without duplicated fields:*
```sql
-- Uses GIN index on full JSONB (slower)
SELECT * FROM docs WHERE doc->>'email' = 'user@example.com';
```

*With duplicated fields:*
```sql
-- Uses BTree index on text column (10-100x faster)
SELECT * FROM docs WHERE d_email = 'user@example.com';
```

**Supported Types:**
- `Text`, `Integer`, `BigInt`, `Numeric`, `Boolean`, `Timestamptz`, `Uuid`, `Jsonb`

**Transforms:**
- Apply functions like `lower({value})` for case-insensitive text matching
- Use `{value}` as placeholder for the extracted value

**Nested Paths:**
```rust
DuplicatedField::new("profile.age", "d_profile_age", DuplicatedFieldType::Integer)
```

**Backfilling Existing Data:**

When adding duplicated fields to an existing database, the trigger only fires on new INSERT/UPDATE operations. To backfill existing rows:

```sql
-- Force trigger execution on all rows
UPDATE docs SET doc = doc WHERE d_email IS NULL;
```

Or use a batched approach for large tables:

```sql
DO $$
DECLARE
    batch_size INT := 1000;
    updated INT;
BEGIN
    LOOP
        UPDATE docs SET doc = doc 
        WHERE d_email IS NULL 
        AND id IN (SELECT id FROM docs WHERE d_email IS NULL LIMIT batch_size);
        
        GET DIAGNOSTICS updated = ROW_COUNT;
        EXIT WHEN updated = 0;
        
        RAISE NOTICE 'Updated % rows', updated;
        COMMIT;
    END LOOP;
END $$;
```

Apply these via your own migration files after running the base schema.
