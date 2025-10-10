use anyhow::Result;
use rillflow::{Error, Event, Store};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[tokio::test]
async fn append_idempotency_conflict() -> Result<()> {
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
    // first append with idempotency key
    store
        .events()
        .builder(stream)
        .idempotency_key("k-123")
        .push(Event::new("Ping", &json!({})))
        .send()
        .await?;

    // second append with the same key should yield IdempotencyConflict
    let err = store
        .events()
        .builder(stream)
        .idempotency_key("k-123")
        .push(Event::new("Ping", &json!({})))
        .send()
        .await
        .expect_err("expected idempotency conflict");

    // ensure it maps to our typed error
    match err {
        Error::IdempotencyConflict => {}
        other => panic!("wrong error: {other:?}"),
    }

    Ok(())
}
