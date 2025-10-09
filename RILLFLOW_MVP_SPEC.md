# Rillflow — MVP Build Spec (for Cursor)

**Goal:** Ship a minimal but working Rust crate that provides:
1) JSONB **document store** on Postgres (get/upsert/delete by id, optimistic concurrency).  
2) **Event store** with append-only streams and expected-version checks.  
3) **Projection scaffolding** (checkpoint table + a basic replay loop).  
4) Dev-only **trace breadcrumbs** with a Mermaid export (no UI yet).

Status target: `v0.1.0-alpha`. License: MIT OR Apache-2.0.

---

## Non‑Goals (for MVP)
- No fancy query builder. ID lookup + a couple of JSON path helpers only.
- No multi-tenancy or RLS.
- No compiled queries.
- No dashboard UI (text/mermaid export only).
- No snapshots (can land in 0.2).

---

## Project Layout
Create a new repo/workspace named **`rillflow`**.

```
rillflow/
├─ Cargo.toml
├─ README.md
├─ LICENSE-MIT
├─ LICENSE-APACHE
├─ MIGRATIONS.md
├─ sql/
│  └─ 0001_init.sql
├─ src/
│  ├─ lib.rs
│  ├─ error.rs
│  ├─ store.rs
│  ├─ documents.rs
│  ├─ events.rs
│  ├─ projections.rs
│  └─ tracing.rs
├─ examples/
│  └─ quickstart.rs
└─ tests/
   └─ integration_postgres.rs
```

---

## Cargo.toml (starter)
```toml
[package]
name = "rillflow"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
description = "Rillflow — a lightweight document + event store for Rust, powered by Postgres."
license = "MIT OR Apache-2.0"
repository = "https://github.com/jordcodes/rillflow"
documentation = "https://docs.rs/rillflow"
readme = "README.md"
keywords = ["postgres", "event-sourcing", "jsonb", "cqrs", "database"]
categories = ["database", "asynchronous", "data-structures"]

[features]
default = ["postgres"]
postgres = []
tracing-dashboard = []

[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
sqlx = { version = "0.7", features = ["runtime-tokio-rustls", "postgres", "uuid", "json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
thiserror = "1"
tracing = "0.1"

[dev-dependencies]
testcontainers = "0.22"
anyhow = "1"
```

---

## Database Schema (sql/0001_init.sql)
```sql
-- documents
create table if not exists docs (
  id uuid primary key,
  doc jsonb not null,
  version int not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists docs_gin on docs using gin (doc);

-- events
create table if not exists events (
  global_seq bigserial primary key,
  stream_id uuid not null,
  stream_seq int not null,
  event_type text not null,
  body jsonb not null,
  created_at timestamptz not null default now(),
  unique(stream_id, stream_seq)
);

-- projection checkpoints
create table if not exists projections (
  name text primary key,
  last_seq bigint not null default 0,
  updated_at timestamptz not null default now()
);
```

---

## Public API (MVP)
```rust
// lib.rs re-exports
pub struct Store;
impl Store {
  pub async fn connect(url: &str) -> Result<Self>;
  pub fn docs(&self) -> Documents;
  pub fn events(&self) -> Events;
  pub fn projections(&self) -> Projections;
}

// documents
pub struct Documents;
impl Documents {
  pub async fn upsert<T: Serialize>(&self, id: &Uuid, doc: &T) -> Result<()>;
  pub async fn get<T: for<'de> Deserialize<'de>>(&self, id: &Uuid) -> Result<Option<T>>;
  pub async fn delete(&self, id: &Uuid) -> Result<()>;
}

// events
pub enum Expected { Any, NoStream, Exact(i32) }
pub struct Event { pub typ: String, pub body: Value }
impl Event { pub fn new<T: Serialize>(typ: impl Into<String>, body: &T) -> Self; }

pub struct Events;
impl Events {
  pub async fn append_stream(&self, stream_id: Uuid, expected: Expected, events: Vec<Event>) -> Result<()>;
  pub async fn read_stream(&self, stream_id: Uuid) -> Result<Vec<(i32, Event)>>;
}

// projections
pub struct Projections;
impl Projections {
  pub async fn replay(&self, name: &str, handler: impl ProjectionHandler) -> Result<()>;
}

// tracing (dev-only breadcrumbs)
pub fn record_step(kind: &str, component: &str, data: &serde_json::Value);
pub fn mermaid(trace_id: &str) -> String;
```

---

## Implementation Order (Cursor Tasks)

### 1) Bootstrapping
- [ ] Create crate with files above.
- [ ] Implement `error.rs` using `thiserror` with variants: `Db`, `Serde`, `VersionConflict`.
- [ ] Implement `Store` with `sqlx::PgPool` inside and `connect()`.

### 2) Documents
- [ ] Implement `Documents::upsert` with optimistic bump (`version = version + 1`).
- [ ] Implement `Documents::get` (deserialize to `T`).
- [ ] Implement `Documents::delete`.
- [ ] Add a JSON path helper (non-indexed MVP): `get_field(id, path) -> Option<Value>`.

**Acceptance:**
- Upsert a struct → Get returns same struct.
- `version` increments on each upsert.
- Delete removes row.

### 3) Events
- [ ] Implement `Expected` checks by reading `max(stream_seq)` in tx.
- [ ] Insert each event with incremental `stream_seq`.
- [ ] Implement `read_stream` returning ordered events.

**Acceptance:**
- `Expected::NoStream` fails when stream exists.
- `Expected::Exact(n)` guards against race.
- Events returned ordered by `stream_seq`.

### 4) Projections (basic)
- [ ] Implement `Projections::replay(name, handler)`:
  - load `last_seq` from `projections`,
  - scan rows `global_seq > last_seq` ordered asc,
  - call `handler.apply(event, &tx)` for each,
  - commit docs/read models + update `last_seq` in the same tx periodically (batch size 100).
- [ ] Provide a `ProjectionHandler` trait with an async `apply(event, &mut Tx) -> Result<()>`.

**Acceptance:**
- Creating a simple “appointments view” that counts events updates after replay.
- Re-running is idempotent.

### 5) Dev-only Trace Breadcrumbs
- [ ] Add `record_step(kind, component, data)`; behind `cfg(debug_assertions)` it pushes to a bounded `VecDeque` keyed by a `trace_id` (propagate via argument for MVP).
- [ ] Add `mermaid(trace_id)` that renders a basic sequence diagram from recorded steps.
- [ ] Add example: after an endpoint-like function runs, print Mermaid to stdout.

**Acceptance:**
- Running the example prints a valid Mermaid diagram you can paste into Markdown previews.

### 6) Example + Docs
- [ ] `examples/quickstart.rs` showing docs upsert + event append + projection replay + mermaid print.
- [ ] `README.md` quickstart using that example.

### 7) Tests
- [ ] `tests/integration_postgres.rs` using **testcontainers** to run Postgres, execute `sql/0001_init.sql`, then assert:
  - upsert/get roundtrip,
  - expected-version conflict,
  - projection replay updates a counter.

---

## Code Stubs

### src/error.rs
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("version conflict")]
    VersionConflict,
}
pub type Result<T> = std::result::Result<T, Error>;
```

### src/store.rs
```rust
use crate::{documents::Documents, events::Events, projections::Projections, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }
    pub fn docs(&self) -> Documents { Documents { pool: self.pool.clone() } }
    pub fn events(&self) -> Events { Events { pool: self.pool.clone() } }
    pub fn projections(&self) -> Projections { Projections { pool: self.pool.clone() } }
    pub fn pool(&self) -> &PgPool { &self.pool }
}
```

### src/documents.rs
```rust
use crate::Result;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;
use uuid::Uuid;

pub struct Documents { pub(crate) pool: PgPool }

impl Documents {
    pub async fn upsert<T: Serialize>(&self, id: &Uuid, doc: &T) -> Result<()> {
        let json = serde_json::to_value(doc)?;
        sqlx::query!(
            r#"
            insert into docs (id, doc, version)
            values ($1, $2, 0)
            on conflict (id) do update
              set doc = excluded.doc,
                  version = docs.version + 1,
                  updated_at = now()
            "#,
            id, json
        ).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get<T: DeserializeOwned>(&self, id: &Uuid) -> Result<Option<T>> {
        let row = sqlx::query_scalar::<_, Option<serde_json::Value>>(
            "select doc from docs where id = $1"
        ).bind(id).fetch_one(&self.pool).await?;
        Ok(match row {
            Some(v) => Some(serde_json::from_value(v)?),
            None => None
        })
    }

    pub async fn delete(&self, id: &Uuid) -> Result<()> {
        sqlx::query!("delete from docs where id = $1", id).execute(&self.pool).await?;
        Ok(())
    }
}
```

### src/events.rs
```rust
use crate::{Error, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum Expected { Any, NoStream, Exact(i32) }

pub struct Event { pub typ: String, pub body: Value }
impl Event {
    pub fn new<T: Serialize>(typ: impl Into<String>, body: &T) -> Self {
        Self { typ: typ.into(), body: serde_json::to_value(body).unwrap() }
    }
}

pub struct Events { pub(crate) pool: PgPool }

impl Events {
    pub async fn append_stream(&self, stream_id: Uuid, expected: Expected, events: Vec<Event>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let current = sqlx::query_scalar::<_, Option<i32>>(
            "select max(stream_seq) from events where stream_id = $1"
        ).bind(stream_id).fetch_one(&mut *tx).await?.unwrap_or(0);

        match expected {
            Expected::Any => {}
            Expected::NoStream if current != 0 => return Err(Error::VersionConflict),
            Expected::Exact(v) if v != current => return Err(Error::VersionConflict),
            _ => {}
        }

        let mut seq = current;
        for e in events {
            seq += 1;
            sqlx::query!(
                "insert into events (stream_id, stream_seq, event_type, body) values ($1,$2,$3,$4)",
                stream_id, seq, e.typ, e.body
            ).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn read_stream(&self, stream_id: Uuid) -> Result<Vec<(i32, Event)>> {
        let rows = sqlx::query!(
            "select stream_seq, event_type, body from events where stream_id = $1 order by stream_seq asc",
            stream_id
        ).fetch_all(&self.pool).await?;

        Ok(rows.into_iter().map(|r| {
            (r.stream_seq, Event { typ: r.event_type, body: r.body })
        }).collect())
    }
}
```

### src/projections.rs
```rust
use crate::Result;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};

pub struct Projections { pub(crate) pool: PgPool }

#[async_trait::async_trait]
pub trait ProjectionHandler: Send + Sync {
    async fn apply(&self, event_type: &str, body: &Value, tx: &mut Transaction<'_, Postgres>) -> Result<()>;
}

impl Projections {
    pub async fn replay<H: ProjectionHandler>(&self, name: &str, handler: &H) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let last_seq: i64 = sqlx::query_scalar(
            "select last_seq from projections where name = $1"
        ).bind(name).fetch_optional(&mut *tx).await?.unwrap_or(0);

        let rows = sqlx::query!(
            "select global_seq, event_type, body from events where global_seq > $1 order by global_seq asc",
            last_seq
        ).fetch_all(&mut *tx).await?;

        let mut new_last = last_seq;
        for r in rows {
            handler.apply(&r.event_type, &r.body, &mut tx).await?;
            new_last = r.global_seq;
        }

        sqlx::query!(
            r#"
            insert into projections(name, last_seq) values ($1,$2)
            on conflict (name) do update set last_seq = excluded.last_seq, updated_at = now()
            "#,
            name, new_last
        ).execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }
}
```

### src/tracing.rs (dev-only stubs)
```rust
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use serde_json::json;

#[derive(Clone, Default)]
pub struct TraceSink {
    inner: Arc<Mutex<HashMap<String, VecDeque<(String, String, serde_json::Value)>>>>,
    cap: usize,
}

impl TraceSink {
    pub fn new(cap: usize) -> Self { Self { inner: Default::default(), cap } }
    pub fn record(&self, trace_id: &str, kind: &str, component: &str, data: serde_json::Value) {
        #[cfg(debug_assertions)]
        {
            let mut m = self.inner.lock().unwrap();
            let q = m.entry(trace_id.to_string()).or_default();
            if q.len() >= self.cap { let _ = q.pop_front(); }
            q.push_back((kind.to_string(), component.to_string(), data));
        }
    }
    pub fn mermaid(&self, trace_id: &str) -> String {
        let mut s = String::from("sequenceDiagram\n");
        let lanes = ["HTTP","CMD","ES","PR"];
        for l in lanes.iter() { s.push_str(&format!("  participant {} as {}\n", l, l)); }
        let m = self.inner.lock().unwrap();
        if let Some(q) = m.get(trace_id) {
            for (k,c,d) in q.iter() {
                let msg = json!({"k":k, "c":c, "d":d}).to_string();
                match k.as_str() {
                    "HTTP_IN" => s.push_str(&format!("  HTTP->>CMD: {}\n", msg)),
                    "APPEND"  => s.push_str(&format!("  CMD->>ES: {}\n", msg)),
                    "PROJECT" => s.push_str(&format!("  ES-->>PR: {}\n", msg)),
                    _ => s.push_str(&format!("  CMD->>PR: {}\n", msg)),
                }
            }
        }
        s
    }
}
```

### src/lib.rs (skeleton)
```rust
//! Rillflow — document + event store for Rust, powered by Postgres.

pub mod store;
pub mod documents;
pub mod events;
pub mod projections;
pub mod tracing;
mod error;

pub use error::{Error, Result};
pub use events::{Event, Expected};
pub use store::Store;

pub mod prelude {
    pub use crate::{Event, Expected, Result, Store};
}
```

---

## Example (examples/quickstart.rs)
```rust
use rillflow::{Store, Event, Expected, Result};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("DATABASE_URL").unwrap_or("postgres://postgres:postgres@localhost:5432/postgres".into());
    let store = Store::connect(&url).await?;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Customer { id: Uuid, email: String }

    let c = Customer { id: Uuid::new_v4(), email: "alice@example.com".into() };
    store.docs().upsert(&c.id, &c).await?;
    store.events().append_stream(c.id, Expected::Any, vec![ Event::new("CustomerRegistered", &c) ]).await?;

    println!("ok");
    Ok(())
}
```

---

## Integration Test (tests/integration_postgres.rs)
```rust
use anyhow::Result;
use testcontainers::{runners::AsyncRunner, images::postgres::Postgres};
use rillflow::{Store, Event, Expected};
use uuid::Uuid;
use std::fs;

#[tokio::test]
async fn roundtrip() -> Result<()> {
    let node = Postgres::default().start().await?;
    let url = node.get_connection_string();
    let store = Store::connect(&url).await?;

    // run migrations
    let ddl = fs::read_to_string("sql/0001_init.sql")?;
    sqlx::query(&ddl).execute(store.pool()).await?;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Customer { id: Uuid, email: String }
    let id = Uuid::new_v4();
    let c = Customer { id, email: "a@b.com".into() };

    store.docs().upsert(&id, &c).await?;
    let got: Option<Customer> = store.docs().get(&id).await?;
    assert!(got.is_some());

    store.events().append_stream(id, Expected::Any, vec![Event::new("CustomerRegistered", &c)]).await?;
    let items = store.events().read_stream(id).await?;
    assert_eq!(items.len(), 1);

    Ok(())
}
```

---

## Dev Workflow (Cursor Tasks)
- [ ] Set `DATABASE_URL` in `.env` (for local dev).
- [ ] Add `justfile` (optional) with tasks: `dev`, `fmt`, `clippy`, `test`.
- [ ] `cargo fmt && cargo clippy -- -D warnings` clean.
- [ ] `cargo test -q` green locally.
- [ ] Update `README.md` with Quickstart and status.
- [ ] Commit with message: `feat: rillflow v0.1.0-alpha MVP (docs, events, projections stub)`.

---

## Release Checklist
- [ ] `cargo publish --dry-run`
- [ ] Check crate size; exclude `/sql` in package if not needed or include intentionally via `include` in Cargo.toml.
- [ ] Tag `v0.1.0-alpha` and push.
- [ ] Open GH issues for: snapshots, JSON path indexes, daemon worker, dev trace UI.
