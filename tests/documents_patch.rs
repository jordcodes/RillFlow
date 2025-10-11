use anyhow::Result;
use rillflow::Store;
use serde::{Deserialize, Serialize};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Profile {
    name: String,
    age: u32,
}

#[tokio::test]
async fn patch_single_and_multiple_fields() -> Result<()> {
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

    let id = Uuid::new_v4();
    let v1 = store
        .docs()
        .put(&id, &json!({"profile": {"name": "Alice", "age": 30}}), None)
        .await?;
    assert_eq!(v1, 1);

    // single field patch
    let v2 = store
        .docs()
        .patch(&id, Some(1), "profile.name", &json!("Alicia"))
        .await?;
    assert_eq!(v2, 2);
    let (doc, ver) = store.docs().get::<serde_json::Value>(&id).await?.unwrap();
    assert_eq!(doc["profile"]["name"], "Alicia");
    assert_eq!(ver, 2);

    // multi-field patch
    let v3 = store
        .docs()
        .patch_fields(
            &id,
            Some(2),
            &[("profile.age", json!(31)), ("extra.flag", json!(true))],
        )
        .await?;
    assert_eq!(v3, 3);
    let (doc2, ver2) = store.docs().get::<serde_json::Value>(&id).await?.unwrap();
    assert_eq!(doc2["profile"]["age"], 31);
    assert_eq!(doc2["extra"]["flag"], true);
    assert_eq!(ver2, 3);

    Ok(())
}
