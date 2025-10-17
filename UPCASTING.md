# Event Upcasting Guide

This guide explains how to adopt the new upcasting infrastructure introduced in Rillflow
`0.1.0-alpha.8`.

## Why Upcasting?

Event schemas evolve. Without a translation layer, legacy events become unreadable once the code
moves on. Upcasting lets you transparently convert historical envelopes into the newest shape just
before consumers process them.

## Getting Started

1. **Create upcaster types** implementing either [`Upcaster`](src/upcasting/mod.rs) for synchronous
   transformations or [`AsyncUpcaster`](src/upcasting/mod.rs) when database/network access is
   required.
2. **Register upcasters** with an [`UpcasterRegistry`](src/upcasting/mod.rs) during application
   startup.
3. **Attach the registry** using `Store::builder(&url).upcasters(registry)` so the store injects it
   into `Events`, `AggregateRepository`, and projection infrastructure automatically.

### Builder & Macro Helpers

For trivial upgrades you can skip custom structs and lean on the fluent builder or macros:

```rust
use rillflow::upcasting::{UpcasterBuilder, AsyncUpcasterBuilder};

let sync = UpcasterBuilder::new("OrderPlaced")
    .from_version(1)
    .to("OrderPlaced", 2)
    .handler(|body| {
        let mut next = body.clone();
        next["currency"] = serde_json::json!("USD");
        Ok(next)
    })
    .build()?;

let async_transformation = AsyncUpcasterBuilder::new("OrderPlaced")
    .from_version(2)
    .to("OrderPlaced", 3)
    .handler(|mut value, pool| async move {
        let label: String = sqlx::query_scalar("select upper($1)")
            .bind(value["customer_id"].as_str().unwrap_or_default())
            .fetch_one(pool)
            .await?;
        value["customer_label"] = serde_json::json!(label);
        Ok(value)
    })
    .build()?;

registry.register(sync);
registry.register_async(async_transformation);
```

Alternatively the new `sync_upcaster!` and `async_upcaster!` macros wrap the same builders in a more
compact syntax.

### Derive Support

When transformations live in standalone functions, the `#[derive(rillflow::Upcaster)]` macro can
generate the trait implementation:

```rust
fn promote_currency(body: &serde_json::Value) -> rillflow::Result<serde_json::Value> {
    let mut updated = body.clone();
    updated["currency"] = serde_json::json!("USD");
    Ok(updated)
}

#[derive(rillflow::Upcaster)]
#[upcaster(
    from_type = "OrderPlaced",
    from_version = 1,
    to_type = "OrderPlaced",
    to_version = 2,
    transform = "crate::promote_currency"
)]
struct OrderPlacedV1ToV2;

let mut registry = UpcasterRegistry::new();
registry.register(OrderPlacedV1ToV2);
```

The `#[upcaster(...)]` attribute accepts `from_type`, `from_version`, `to_type`,
`to_version`, and a function `transform` path returning `rillflow::Result<serde_json::Value>`.

### Example

```rust
use async_trait::async_trait;
use rillflow::{
    upcasting::{Upcaster, AsyncUpcaster, UpcasterRegistry},
    Store,
};

struct OrderPlacedV1ToV2;

impl Upcaster for OrderPlacedV1ToV2 {
    fn from_type(&self) -> &str { "OrderPlaced" }
    fn from_version(&self) -> i32 { 1 }
    fn to_type(&self) -> &str { "OrderPlaced" }
    fn to_version(&self) -> i32 { 2 }
    fn upcast(&self, body: &serde_json::Value) -> rillflow::Result<serde_json::Value> {
        let mut updated = body.clone();
        updated["currency"] = serde_json::json!("USD");
        Ok(updated)
    }
}

struct OrderPlacedV2ToV3;

#[async_trait]
impl AsyncUpcaster for OrderPlacedV2ToV3 {
    fn from_type(&self) -> &str { "OrderPlaced" }
    fn from_version(&self) -> i32 { 2 }
    fn to_type(&self) -> &str { "OrderPlaced" }
    fn to_version(&self) -> i32 { 3 }

    async fn upcast(
        &self,
        body: &serde_json::Value,
        pool: &sqlx::PgPool,
    ) -> rillflow::Result<serde_json::Value> {
        let mut updated = body.clone();
        let id = body["customer_id"].as_str().unwrap_or_default();
        let label: String = sqlx::query_scalar("select upper($1::text)")
            .bind(id)
            .fetch_one(pool)
            .await?;
        updated["customer_label"] = serde_json::json!(label);
        Ok(updated)
    }
}

let mut registry = UpcasterRegistry::new();
registry.register(OrderPlacedV1ToV2);
registry.register_async(OrderPlacedV2ToV3);

let store = Store::builder(&database_url)
    .upcasters(registry)
    .build()
    .await?;
```

The registry is honored throughout:

- `Store::events()` when reading stream envelopes.
- `AggregateRepository::load` so aggregates see the modern shape.
- `Projections` and `ProjectionDaemon` before handlers execute.

## Migration Guide

1. **Identify legacy registrations.** Locate uses of the deprecated
   `events::register_upcaster`. Note the source/target type and version.
2. **Move logic into structs.** Replace bare closures with concrete types implementing
   `Upcaster`/`AsyncUpcaster`.
3. **Build a registry.** Instantiate an `UpcasterRegistry`, register the new types, and hand it to
   `Store::builder().upcasters(registry)`.
4. **Remove globals.** Delete the old `register_upcaster` calls once all upcasters are in the
   registry.
5. **Verify behaviour.** Run the new `tests/upcasting.rs` integration test or the example to ensure
   multi-hop chains and async lookups function as expected.

## Performance Notes

Upcasting happens only on read paths. For high-throughput workloads, consider:

- Keeping transformations lightweight (prefer sync transformations when possible).
- Hoisting expensive lookups behind async upcasters and batching downstream consumers.
- Benchmarking with the criterion harness under `benches/upcasting.rs` to measure impact.

## CLI Support

The CLI provides helpers for inspecting registries:

```bash
cargo run --bin rillflow -- upcasters list
cargo run --bin rillflow -- upcasters path \
  --from-type OrderPlaced --from-version 1 \
  --to-type OrderPlaced --to-version 4
```

## Further Reading

- [examples/upcasting.rs](examples/upcasting.rs) – runnable sample.
- [tests/upcasting.rs](tests/upcasting.rs) – integration coverage with Postgres via testcontainers.
