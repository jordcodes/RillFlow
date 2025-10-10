use anyhow::Result;
use rillflow::subscriptions::{SubscriptionFilter, SubscriptionOptions, Subscriptions};
use rillflow::{Event, Expected, Store};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[tokio::test]
async fn subscribe_and_receive_events() -> Result<()> {
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

    // create subscription
    let subs = Subscriptions::new(store.pool().clone());
    let filter = SubscriptionFilter {
        event_types: Some(vec!["Ping".to_string()]),
        stream_ids: None,
        stream_prefix: None,
    };
    let opts = SubscriptionOptions {
        batch_size: 100,
        poll_interval: Duration::from_millis(100),
        start_from: 0,
        channel_capacity: 16,
    };
    let (_handle, mut rx) = subs.subscribe("s1", filter, opts).await?;

    // append events
    let stream = Uuid::new_v4();
    store
        .events()
        .append_stream(stream, Expected::Any, vec![Event::new("Ping", &json!({}))])
        .await?;

    // expect delivery
    let env = timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("expected event");
    assert_eq!(env.typ, "Ping");
    assert_eq!(env.stream_id, stream);

    Ok(())
}
