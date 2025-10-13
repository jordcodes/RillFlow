use anyhow::Result;
use rillflow::events::register_inline_projection;
use rillflow::projections::CustomProjection;
use rillflow::{Event, Expected, Store};
use serde_json::json;
use std::sync::Arc;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[tokio::test]
async fn custom_projection_executes_arbitrary_sql() -> Result<()> {
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

    // Create target table
    sqlx::query("create table if not exists cp_targets(k text primary key, v text not null)")
        .execute(store.pool())
        .await?;

    let proj = CustomProjection::new()
        .on(
            &["Set"],
            "insert into cp_targets(k,v) values($1,$2) on conflict(k) do update set v = excluded.v",
        )
        .args(&["key", "val"]);
    register_inline_projection(Arc::new(proj));

    let stream = Uuid::new_v4();
    store
        .events()
        .enable_inline_projections()
        .append_stream(
            stream,
            Expected::Any,
            vec![
                Event::new("Set", &json!({"key":"a","val":"1"})),
                Event::new("Set", &json!({"key":"a","val":"2"})),
            ],
        )
        .await?;

    let v: String = sqlx::query_scalar("select v from cp_targets where k = 'a'")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(v, "2");
    Ok(())
}
