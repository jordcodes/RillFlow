use anyhow::Result;
use rillflow::events::register_inline_projection;
use rillflow::projections::ProjectionHandler;
use rillflow::{Event, Expected, Store};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

struct InlineCounter;

#[async_trait::async_trait]
impl ProjectionHandler for InlineCounter {
    async fn apply(
        &self,
        _event_type: &str,
        body: &serde_json::Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> rillflow::Result<()> {
        let key = body["k"].as_str().unwrap_or("default");
        sqlx::query("create table if not exists inline_counts(k text primary key, n int not null default 0)")
            .execute(&mut **tx).await?;
        sqlx::query("insert into inline_counts(k,n) values($1,1) on conflict(k) do update set n = inline_counts.n + 1")
            .bind(key)
            .execute(&mut **tx).await?;
        Ok(())
    }
}

#[tokio::test]
async fn inline_projection_applies_and_rolls_back_on_error() -> Result<()> {
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

    register_inline_projection(Arc::new(InlineCounter));

    let stream = Uuid::new_v4();
    // Apply inline
    store
        .events()
        .enable_inline_projections()
        .append_stream(
            stream,
            Expected::Any,
            vec![Event::new("Inc", &json!({"k":"a"}))],
        )
        .await?;

    let n: i32 = sqlx::query_scalar("select n from inline_counts where k = 'a'")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(n, 1);

    // Now simulate erroring handler to force rollback
    struct Failing;
    #[async_trait::async_trait]
    impl ProjectionHandler for Failing {
        async fn apply(
            &self,
            _event_type: &str,
            _body: &serde_json::Value,
            _tx: &mut Transaction<'_, Postgres>,
        ) -> rillflow::Result<()> {
            Err(rillflow::Error::UnknownProjection("fail".into()))
        }
    }
    register_inline_projection(Arc::new(Failing));

    let res = store
        .events()
        .enable_inline_projections()
        .append_stream(
            stream,
            Expected::Any,
            vec![Event::new("Inc", &json!({"k":"a"}))],
        )
        .await;
    assert!(res.is_err());

    // Count remains unchanged due to rollback
    let n2: i32 = sqlx::query_scalar("select n from inline_counts where k = 'a'")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(n2, 1);
    Ok(())
}
