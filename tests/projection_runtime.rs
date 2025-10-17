use anyhow::Result;
use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};
use rillflow::projections::ProjectionHandler;
use rillflow::testing::{ensure_counters_table, migrate_core_schema};
use rillflow::{Expected, SchemaConfig, Store, events::Event};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

struct CounterProjection;

#[async_trait::async_trait]
impl ProjectionHandler for CounterProjection {
    async fn apply(
        &self,
        _event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> rillflow::Result<()> {
        let id: Uuid = serde_json::from_value(body.get("id").cloned().unwrap())?;
        sqlx::query(
            r#"
            insert into counters(id, count) values ($1, 1)
            on conflict (id) do update set count = counters.count + 1
            "#,
        )
        .bind(id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn projection_tick_and_pause_resume() -> Result<()> {
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
    // ensure schema including projection runtime tables
    store.schema().sync(&SchemaConfig::single_tenant()).await?;

    migrate_core_schema(store.pool()).await?;
    ensure_counters_table(store.pool()).await?;

    let stream = Uuid::new_v4();
    let body = serde_json::json!({"id": stream});
    store
        .events()
        .append_stream(
            stream,
            Expected::Any,
            vec![Event::new("Ping", &body), Event::new("Ping", &body)],
        )
        .await?;

    let mut daemon = ProjectionDaemon::new(store.pool().clone(), ProjectionWorkerConfig::default());
    daemon.register("counter", Arc::new(CounterProjection));

    // tick once should process both events
    let _ = daemon.tick_once("counter").await?;

    let count: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream)
        .fetch_optional(store.pool())
        .await?;
    assert_eq!(count, Some(2));

    // pause then tick — should be Paused, count unchanged
    daemon.pause("counter", None).await?;
    let res = daemon.tick_once("counter").await?;
    assert!(matches!(
        res,
        rillflow::projection_runtime::TickResult::Paused
    ));

    // resume and append one more event; tick processes it
    daemon.resume("counter", None).await?;
    store
        .events()
        .append_stream(stream, Expected::Any, vec![Event::new("Ping", &body)])
        .await?;
    let _ = daemon.tick_once("counter").await?;
    let count2: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream)
        .fetch_optional(store.pool())
        .await?;
    assert_eq!(count2, Some(3));

    Ok(())
}

#[tokio::test]
async fn conjoined_tenant_filtering_in_projections() -> Result<()> {
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
        duplicated_fields: Vec::new(),
    };
    store.schema().sync(&schema_config).await?;
    ensure_counters_table(store.pool()).await?;

    // Append events for tenant_a
    let stream_a = Uuid::new_v4();
    let body_a = serde_json::json!({"id": stream_a});
    store
        .events()
        .with_tenant("tenant_a")
        .append_stream(
            stream_a,
            Expected::Any,
            vec![Event::new("Ping", &body_a), Event::new("Ping", &body_a)],
        )
        .await?;

    // Append events for tenant_b
    let stream_b = Uuid::new_v4();
    let body_b = serde_json::json!({"id": stream_b});
    store
        .events()
        .with_tenant("tenant_b")
        .append_stream(
            stream_b,
            Expected::Any,
            vec![
                Event::new("Ping", &body_b),
                Event::new("Ping", &body_b),
                Event::new("Ping", &body_b),
            ],
        )
        .await?;

    // Create projection daemon for tenant_a
    let config_a = ProjectionWorkerConfig {
        tenant_strategy: rillflow::store::TenantStrategy::conjoined(
            rillflow::schema::TenantColumn::text("tenant_id"),
        ),
        tenant_resolver: Some(std::sync::Arc::new(|| Some("tenant_a".to_string()))),
        ..Default::default()
    };
    let mut daemon_a = ProjectionDaemon::new(store.pool().clone(), config_a);
    daemon_a.register("counter_a", Arc::new(CounterProjection));

    // Create projection daemon for tenant_b
    let config_b = ProjectionWorkerConfig {
        tenant_strategy: rillflow::store::TenantStrategy::conjoined(
            rillflow::schema::TenantColumn::text("tenant_id"),
        ),
        tenant_resolver: Some(std::sync::Arc::new(|| Some("tenant_b".to_string()))),
        ..Default::default()
    };
    let mut daemon_b = ProjectionDaemon::new(store.pool().clone(), config_b);
    daemon_b.register("counter_b", Arc::new(CounterProjection));

    // Tick both projections
    daemon_a.tick_once("counter_a").await?;
    daemon_b.tick_once("counter_b").await?;

    // Verify tenant_a projection only processed tenant_a events
    let count_a: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream_a)
        .fetch_optional(store.pool())
        .await?;
    assert_eq!(count_a, Some(2), "tenant_a should have 2 events");

    // Verify tenant_b projection only processed tenant_b events
    let count_b: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream_b)
        .fetch_optional(store.pool())
        .await?;
    assert_eq!(count_b, Some(3), "tenant_b should have 3 events");

    // Verify no cross-tenant pollution
    let cross_check_a: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream_b)
        .fetch_optional(store.pool())
        .await?;
    // stream_b should NOT have been processed by daemon_a
    assert_eq!(
        cross_check_a,
        Some(3),
        "should only be processed by tenant_b"
    );

    let cross_check_b: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream_a)
        .fetch_optional(store.pool())
        .await?;
    // stream_a should NOT have been processed by daemon_b
    assert_eq!(
        cross_check_b,
        Some(2),
        "should only be processed by tenant_a"
    );

    Ok(())
}
