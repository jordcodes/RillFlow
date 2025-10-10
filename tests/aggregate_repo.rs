use anyhow::Result;
use rillflow::{Aggregate, AggregateRepository, Event, Expected, Store};
use serde_json::json;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    GenericImage, ImageExt,
    runners::AsyncRunner,
};
use uuid::Uuid;

#[derive(Default)]
struct CounterAgg { count: i32 }

impl Aggregate for CounterAgg {
    fn new() -> Self { Self { count: 0 } }
    fn apply(&mut self, env: &rillflow::EventEnvelope) {
        if env.typ == "Inc" { self.count += 1; }
    }
    fn version(&self) -> i32 { self.count }
}

#[tokio::test]
async fn load_and_commit_counter() -> Result<()> {
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

    // commit three increments
    repo.commit(stream, Expected::Any, vec![
        Event::new("Inc", &json!({})),
        Event::new("Inc", &json!({})),
        Event::new("Inc", &json!({})),
    ]).await?;

    let agg: CounterAgg = repo.load(stream).await?;
    assert_eq!(agg.count, 3);

    // commit one more using aggregate version
    repo.commit_for_aggregate(stream, &agg, vec![Event::new("Inc", &json!({}))]).await?;
    let agg2: CounterAgg = repo.load(stream).await?;
    assert_eq!(agg2.count, 4);

    Ok(())
}
