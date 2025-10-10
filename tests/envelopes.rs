use anyhow::Result;
use rillflow::{Event, Expected, Store};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[tokio::test]
async fn append_and_read_event_envelopes() -> Result<()> {
    // start ephemeral Postgres
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

    // create base schema
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    // stream 1: explicit headers + causation/correlation ids
    let s1 = Uuid::new_v4();
    let body1 = json!({"k": 1});
    let headers = json!({"req_id": "r-123", "part": 7});
    let causation = Some(Uuid::new_v4());
    let correlation = Some(Uuid::new_v4());

    store
        .events()
        .append_stream_with_headers(
            s1,
            Expected::Any,
            vec![Event::new("E1", &body1)],
            &headers,
            causation,
            correlation,
        )
        .await?;

    let envs = store.events().read_stream_envelopes(s1).await?;
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].typ, "E1");
    assert_eq!(envs[0].body, body1);
    assert_eq!(envs[0].headers, headers);
    assert_eq!(envs[0].causation_id, causation);
    assert_eq!(envs[0].correlation_id, correlation);

    // stream 2: default path (no headers/ids)
    let s2 = Uuid::new_v4();
    let body2 = json!({"k": 2});
    store
        .events()
        .append_stream(s2, Expected::Any, vec![Event::new("E2", &body2)])
        .await?;
    let envs2 = store.events().read_stream_envelopes(s2).await?;
    assert_eq!(envs2.len(), 1);
    assert_eq!(envs2[0].headers, json!({}));
    assert!(envs2[0].causation_id.is_none());
    assert!(envs2[0].correlation_id.is_none());

    Ok(())
}
