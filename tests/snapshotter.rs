use anyhow::Result;
use rillflow::{
    Aggregate, AggregateRepository, Event, Expected, Store,
    snapshotter::{AggregateFolder, Snapshotter, SnapshotterConfig},
};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[derive(Default, serde::Serialize)]
struct Counter(i32);

impl Aggregate for Counter {
    fn new() -> Self {
        Self(0)
    }
    fn apply(&mut self, env: &rillflow::EventEnvelope) {
        if env.typ == "Inc" {
            self.0 += 1;
        }
    }
    fn version(&self) -> i32 {
        self.0
    }
}

#[tokio::test]
async fn snapshotter_writes_snapshots_for_long_streams() -> Result<()> {
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

    let stream = Uuid::new_v4();
    let events: Vec<_> = (0..150).map(|_| Event::new("Inc", &json!({}))).collect();
    store
        .events()
        .append_stream(stream, Expected::Any, events)
        .await?;

    let repo = AggregateRepository::new(store.events());
    let folder = AggregateFolder::<Counter>::new(repo);
    let snapshotter = Snapshotter::new(
        store.pool().clone(),
        std::sync::Arc::new(folder),
        SnapshotterConfig {
            threshold_events: 100,
            ..Default::default()
        },
    );
    let n = snapshotter.tick_once().await?;
    assert!(n >= 1);

    let (ver,): (i32,) = sqlx::query_as("select version from snapshots where stream_id = $1")
        .bind(stream)
        .fetch_one(store.pool())
        .await?;
    assert!(ver >= 100);

    Ok(())
}
