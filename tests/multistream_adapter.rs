use anyhow::Result;
use rillflow::events::register_inline_projection;
use rillflow::projections::{MultiStreamAdapter, MultiStreamProjection};
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

struct CountAB;

#[async_trait::async_trait]
impl MultiStreamProjection for CountAB {
    fn event_types(&self) -> &'static [&'static str] {
        &["A", "B"]
    }

    async fn apply_multi(
        &self,
        event_type: &str,
        _body: &serde_json::Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> rillflow::Result<()> {
        sqlx::query(
            "create table if not exists ms_counts(t text primary key, n int not null default 0)",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "insert into ms_counts(t,n) values($1,1) on conflict(t) do update set n = ms_counts.n + 1",
        )
        .bind(event_type)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn multistream_adapter_filters_and_applies() -> Result<()> {
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

    register_inline_projection(Arc::new(MultiStreamAdapter(Arc::new(CountAB))));

    let stream = Uuid::new_v4();
    store
        .events()
        .enable_inline_projections()
        .append_stream(
            stream,
            Expected::Any,
            vec![
                Event::new("A", &json!({})),
                Event::new("B", &json!({})),
                Event::new("C", &json!({})), // should be ignored
                Event::new("A", &json!({})),
            ],
        )
        .await?;

    let (a, b, c): (i32, i32, Option<i32>) = sqlx::query_as(
        "select
            coalesce((select n from ms_counts where t = 'A'),0) as a,
            coalesce((select n from ms_counts where t = 'B'),0) as b,
            (select n from ms_counts where t = 'C') as c",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(a, 2);
    assert_eq!(b, 1);
    assert!(c.is_none());
    Ok(())
}
