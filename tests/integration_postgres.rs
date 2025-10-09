use anyhow::Result;
use rillflow::{Event, Expected, Store, projections::ProjectionHandler};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
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
        _body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> rillflow::Result<()> {
        sqlx::query(
            r#"
            insert into counters (id, count)
            values ($1, 1)
            on conflict (id) do update set count = counters.count + 1
            "#,
        )
        .bind(Uuid::nil())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn roundtrip() -> Result<()> {
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
    rillflow::testing::ensure_counters_table(store.pool()).await?;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Customer {
        id: Uuid,
        email: String,
    }

    let id = Uuid::new_v4();
    let customer = Customer {
        id,
        email: "a@b.com".into(),
    };

    store.docs().upsert(&id, &customer).await?;
    let fetched: Option<Customer> = store.docs().get(&id).await?;
    assert!(fetched.is_some());

    store
        .events()
        .append_stream(
            id,
            Expected::Any,
            vec![Event::new("CustomerRegistered", &customer)],
        )
        .await?;

    let items = store.events().read_stream(id).await?;
    assert_eq!(items.len(), 1);

    let projections = store.projections();
    projections.replay("counter", &CounterProjection).await?;

    let count: i32 = sqlx::query_scalar("select count from counters where id = $1")
        .bind(Uuid::nil())
        .fetch_one(store.pool())
        .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
async fn expected_version_conflict() -> Result<()> {
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

    let stream_id = Uuid::new_v4();
    let event = Event::new("TestEvent", &serde_json::json!({"k": 1}));

    store
        .events()
        .append_stream(stream_id, Expected::Any, vec![event.clone()])
        .await?;

    let err = store
        .events()
        .append_stream(stream_id, Expected::Exact(0), vec![event])
        .await
        .expect_err("mismatched expected version should fail");

    assert!(matches!(err, rillflow::Error::VersionConflict));

    Ok(())
}
