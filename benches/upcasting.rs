use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rillflow::upcasting::{Upcaster, UpcasterRegistry};
use serde_json::{Value, json};
use uuid::Uuid;

fn build_registry() -> UpcasterRegistry {
    let mut registry = UpcasterRegistry::new();
    registry.register(OrderPlacedV1ToV2);
    registry.register(OrderPlacedV2ToV3);
    registry.register(OrderPlacedV3ToV4);
    registry
}

fn bench_upcasting(c: &mut Criterion) {
    let registry = build_registry();
    let mut group = c.benchmark_group("upcasting");
    group.throughput(Throughput::Elements(1));

    group.bench_function(BenchmarkId::new("sync_chain", 3), |b| {
        b.iter(|| {
            let mut envelope = make_envelope(1);
            registry.upcast_sync(&mut envelope).unwrap();
            assert_eq!(envelope.event_version, 4);
            envelope
        })
    });

    group.finish();
}

fn make_envelope(version: i32) -> rillflow::events::EventEnvelope {
    rillflow::events::EventEnvelope {
        global_seq: 1,
        stream_id: Uuid::nil(),
        stream_seq: 1,
        typ: "OrderPlaced".to_string(),
        body: json!({
            "order_id": 42,
            "customer_id": "abc123"
        }),
        headers: Value::Null,
        causation_id: None,
        correlation_id: None,
        event_version: version,
        tenant_id: None,
        user_id: None,
        created_at: chrono::Utc::now(),
    }
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

#[derive(Clone)]
struct OrderPlacedV2ToV3;

impl Upcaster for OrderPlacedV2ToV3 {
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
    fn upcast(&self, body: &Value) -> rillflow::Result<Value> {
        let mut updated = body.clone();
        updated["customer_id"] =
            json!(format!("{}-v2", body["customer_id"].as_str().unwrap_or("")));
        Ok(updated)
    }
}

#[derive(Clone)]
struct OrderPlacedV3ToV4;

impl Upcaster for OrderPlacedV3ToV4 {
    fn from_type(&self) -> &str {
        "OrderPlaced"
    }
    fn from_version(&self) -> i32 {
        3
    }
    fn to_type(&self) -> &str {
        "OrderPlaced"
    }
    fn to_version(&self) -> i32 {
        4
    }
    fn upcast(&self, body: &Value) -> rillflow::Result<Value> {
        let mut updated = body.clone();
        updated["status"] = json!("processed");
        Ok(updated)
    }
}

criterion_group!(benches, bench_upcasting);
criterion_main!(benches);
