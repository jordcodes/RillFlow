use anyhow::Result;
use rillflow::{Aggregate, AggregateRepository, Event, Expected, Store};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[derive(Default, serde::Serialize)]
struct CounterAgg {
    count: i32,
}

impl Aggregate for CounterAgg {
    fn new() -> Self {
        Self { count: 0 }
    }
    fn apply(&mut self, env: &rillflow::EventEnvelope) {
        if env.typ == "Inc" {
            self.count += 1;
        }
    }
    fn version(&self) -> i32 {
        self.count
    }
    fn hydrate(&mut self, snap: &serde_json::Value) {
        if let Some(c) = snap.get("count").and_then(|v| v.as_i64()) {
            self.count = c as i32;
        }
    }
}

#[tokio::test]
async fn commit_and_load_with_snapshot() -> Result<()> {
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

    let store = Store::connect(&url).await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let repo = AggregateRepository::new(store.events());
    let stream = Uuid::new_v4();

    // commit 4 increments, snapshot every 2 -> snapshot at 2 and 4
    for _ in 0..4 {
        let agg: CounterAgg = repo.load(stream).await.unwrap_or_default();
        repo.commit_and_snapshot(stream, &agg, vec![Event::new("Inc", &json!({}))], 2)
            .await?;
    }

    let loaded: CounterAgg = repo.load_with_snapshot(stream).await?;
    assert_eq!(loaded.count, 4);

    Ok(())
}
