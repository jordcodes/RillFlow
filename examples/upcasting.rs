//! Demonstrates registering sync + async upcasters with Rillflow.

use async_trait::async_trait;
use rillflow::{
    Event, Expected, Store,
    upcasting::{AsyncUpcaster, Upcaster, UpcasterRegistry},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").expect("set DATABASE_URL pointing to your Postgres");

    let mut registry = UpcasterRegistry::new();
    registry.register(OrderPlacedV1ToV2);
    registry.register_async(OrderPlacedV2ToV3);

    let store = Store::builder(&database_url)
        .upcasters(registry)
        .build()
        .await?;

    let stream_id = Uuid::new_v4();
    store
        .events()
        .append_stream(
            stream_id,
            Expected::Any,
            vec![Event::new(
                "OrderPlaced",
                &json!({"order_id": 42, "customer_id": "abc123"}),
            )],
        )
        .await?;

    let envelope = store
        .events()
        .read_stream_envelopes(stream_id)
        .await?
        .pop()
        .expect("event inserted");

    println!("type={} version={}", envelope.typ, envelope.event_version);
    println!("{}", serde_json::to_string_pretty(&envelope.body)?);

    Ok(())
}

#[derive(Clone)]
struct OrderPlacedV1ToV2;

impl Upcaster for OrderPlacedV1ToV2 {
    fn from_type(&self) -> &str {
        "OrderPlaced"
    }
    fn from_version(&self) -> i32 {
        1
    }
    fn to_type(&self) -> &str {
        "OrderPlaced"
    }
    fn to_version(&self) -> i32 {
        2
    }
    fn upcast(&self, body: &Value) -> rillflow::Result<Value> {
        let mut updated = body.clone();
        updated["currency"] = json!("USD");
        Ok(updated)
    }
}

struct OrderPlacedV2ToV3;

#[async_trait]
impl AsyncUpcaster for OrderPlacedV2ToV3 {
    fn from_type(&self) -> &str {
        "OrderPlaced"
    }
    fn from_version(&self) -> i32 {
        2
    }
    fn to_type(&self) -> &str {
        "OrderPlaced"
    }
    fn to_version(&self) -> i32 {
        3
    }
    async fn upcast(&self, body: &Value, pool: &PgPool) -> rillflow::Result<Value> {
        let mut updated = body.clone();
        let id = body["customer_id"].as_str().unwrap_or_default();
        let label: String = sqlx::query_scalar("select upper($1::text)")
            .bind(id)
            .fetch_one(pool)
            .await?;
        updated["customer_label"] = json!(label);
        Ok(updated)
    }
}
