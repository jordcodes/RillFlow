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

#[tokio::test]
async fn conjoined_tenant_filtering_in_snapshotter() -> Result<()> {
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

    // Set up store with conjoined tenancy
    let store = Store::builder(&url)
        .tenant_strategy(rillflow::store::TenantStrategy::conjoined(
            rillflow::schema::TenantColumn::text("tenant_id"),
        ))
        .tenant_resolver(|| Some("tenant_a".to_string()))
        .build()
        .await?;

    // Sync schema with tenant column
    let schema_config = rillflow::SchemaConfig {
        base_schema: "public".into(),
        tenancy_mode: rillflow::schema::TenancyMode::Conjoined {
            column: rillflow::schema::TenantColumn::text("tenant_id"),
        },
    };
    store.schema().sync(&schema_config).await?;

    // Append 150 events for tenant_a stream (exceeds threshold of 100)
    let stream_a = Uuid::new_v4();
    let events_a: Vec<_> = (0..150).map(|_| Event::new("Inc", &json!({}))).collect();
    store
        .events()
        .with_tenant("tenant_a")
        .append_stream(stream_a, Expected::Any, events_a)
        .await?;

    // Append 150 events for tenant_b stream (exceeds threshold of 100)
    let stream_b = Uuid::new_v4();
    let events_b: Vec<_> = (0..150).map(|_| Event::new("Inc", &json!({}))).collect();
    store
        .events()
        .with_tenant("tenant_b")
        .append_stream(stream_b, Expected::Any, events_b)
        .await?;

    // Create snapshotter for tenant_a
    let repo_a = AggregateRepository::new(store.events().with_tenant("tenant_a"));
    let folder_a = AggregateFolder::<Counter>::new(repo_a);
    let config_a = SnapshotterConfig {
        threshold_events: 100,
        tenant_strategy: rillflow::store::TenantStrategy::conjoined(
            rillflow::schema::TenantColumn::text("tenant_id"),
        ),
        tenant_resolver: Some(std::sync::Arc::new(|| Some("tenant_a".to_string()))),
        ..Default::default()
    };
    let snapshotter_a = Snapshotter::new(
        store.pool().clone(),
        std::sync::Arc::new(folder_a),
        config_a,
    );

    // Create snapshotter for tenant_b
    let repo_b = AggregateRepository::new(store.events().with_tenant("tenant_b"));
    let folder_b = AggregateFolder::<Counter>::new(repo_b);
    let config_b = SnapshotterConfig {
        threshold_events: 100,
        tenant_strategy: rillflow::store::TenantStrategy::conjoined(
            rillflow::schema::TenantColumn::text("tenant_id"),
        ),
        tenant_resolver: Some(std::sync::Arc::new(|| Some("tenant_b".to_string()))),
        ..Default::default()
    };
    let snapshotter_b = Snapshotter::new(
        store.pool().clone(),
        std::sync::Arc::new(folder_b),
        config_b,
    );

    // Tick tenant_a snapshotter - should only see tenant_a streams
    let n_a = snapshotter_a.tick_once().await?;
    assert_eq!(
        n_a, 1,
        "tenant_a snapshotter should find 1 candidate stream"
    );

    // Tick tenant_b snapshotter - should only see tenant_b streams
    let n_b = snapshotter_b.tick_once().await?;
    assert_eq!(
        n_b, 1,
        "tenant_b snapshotter should find 1 candidate stream"
    );

    // Verify tenant_a snapshot exists
    let ver_a: Option<i32> =
        sqlx::query_scalar("select version from snapshots where stream_id = $1")
            .bind(stream_a)
            .fetch_optional(store.pool())
            .await?;
    assert!(
        ver_a.is_some() && ver_a.unwrap() >= 100,
        "tenant_a snapshot should exist"
    );

    // Verify tenant_b snapshot exists
    let ver_b: Option<i32> =
        sqlx::query_scalar("select version from snapshots where stream_id = $1")
            .bind(stream_b)
            .fetch_optional(store.pool())
            .await?;
    assert!(
        ver_b.is_some() && ver_b.unwrap() >= 100,
        "tenant_b snapshot should exist"
    );

    // Verify metrics are tenant-scoped
    let metrics_a = snapshotter_a.metrics().await?;
    assert_eq!(
        metrics_a.candidates, 0,
        "tenant_a should have no more candidates after snapshot"
    );

    let metrics_b = snapshotter_b.metrics().await?;
    assert_eq!(
        metrics_b.candidates, 0,
        "tenant_b should have no more candidates after snapshot"
    );

    Ok(())
}
