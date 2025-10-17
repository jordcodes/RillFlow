use anyhow::Result;
use async_trait::async_trait;
use rillflow::{
    Aggregate, AggregateRepository, Event, EventEnvelope, Expected, Store,
    upcasting::{AsyncUpcaster, Upcaster, UpcasterRegistry},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[tokio::test]
async fn registry_applies_sync_and_async_upcasters() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let mut registry = UpcasterRegistry::new();
    registry.register(OrderPlacedV1ToV2);
    registry.register_async(OrderPlacedV2ToV3);

    let store = Store::builder(&url).upcasters(registry).build().await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let stream_id = Uuid::new_v4();
    store
        .events()
        .append_stream(
            stream_id,
            Expected::Any,
            vec![Event::new(
                "OrderPlaced",
                &json!({
                    "order_id": 42,
                    "customer_id": "abc123"
                }),
            )],
        )
        .await?;

    let mut envelopes = store.events().read_stream_envelopes(stream_id).await?;
    assert_eq!(envelopes.len(), 1);
    let envelope = envelopes.pop().expect("single envelope");
    assert_eq!(envelope.event_version, 3);
    assert_eq!(envelope.typ, "OrderPlaced");
    assert_eq!(envelope.body["currency"], "USD");
    assert_eq!(envelope.body["customer_label"], "ABC123");

    let repo = AggregateRepository::new(store.events());
    let aggregate: OrderSummary = repo.load(stream_id).await?;
    assert_eq!(aggregate.currency.as_deref(), Some("USD"));
    assert_eq!(aggregate.customer_label.as_deref(), Some("ABC123"));
    assert_eq!(aggregate.version(), 1);

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
        let customer_id = body
            .get("customer_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let label: String = sqlx::query_scalar("select upper($1::text)")
            .bind(customer_id)
            .fetch_one(pool)
            .await?;
        updated["customer_label"] = json!(label);
        Ok(updated)
    }
}

#[derive(Clone, Default)]
struct OrderSummary {
    currency: Option<String>,
    customer_label: Option<String>,
    version: i32,
}

impl Aggregate for OrderSummary {
    fn new() -> Self {
        Self::default()
    }

    fn apply(&mut self, envelope: &EventEnvelope) {
        self.currency = envelope
            .body
            .get("currency")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        self.customer_label = envelope
            .body
            .get("customer_label")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        self.version = envelope.stream_seq;
    }

    fn version(&self) -> i32 {
        self.version
    }
}
