use anyhow::Result;
use rillflow::upcasting::UpcasterRegistry;
use serde_json::{Value, json};
use uuid::Uuid;

fn transform(body: &Value) -> rillflow::Result<Value> {
    let mut updated = body.clone();
    updated["flag"] = json!(true);
    Ok(updated)
}

#[derive(rillflow::Upcaster)]
#[upcaster(
    from_type = "OrderPlaced",
    from_version = 1,
    to_type = "OrderPlaced",
    to_version = 2,
    transform = "crate::transform"
)]
struct OrderPlacedV1ToV2;

#[test]
fn derive_generates_upcaster_trait_impl() -> Result<()> {
    let mut registry = UpcasterRegistry::new();
    registry.register(OrderPlacedV1ToV2);

    let mut envelope = rillflow::events::EventEnvelope {
        global_seq: 1,
        stream_id: Uuid::nil(),
        stream_seq: 1,
        typ: "OrderPlaced".into(),
        body: json!({"order_id": 42}),
        headers: Value::Null,
        causation_id: None,
        correlation_id: None,
        event_version: 1,
        tenant_id: None,
        user_id: None,
        created_at: chrono::Utc::now(),
    };

    registry.upcast_sync(&mut envelope)?;
    assert_eq!(envelope.event_version, 2);
    assert_eq!(envelope.body["flag"], true);
    Ok(())
}
