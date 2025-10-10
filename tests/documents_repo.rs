use anyhow::Result;
use rillflow::{Error, Store};
use serde::{Deserialize, Serialize};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Customer {
    email: String,
    tier: String,
}

#[tokio::test]
async fn put_get_update_soft_delete_restore() -> Result<()> {
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
        .put(
            &id,
            &Customer {
                email: "a@x".into(),
                tier: "free".into(),
            },
            None,
        )
        .await?;
    assert_eq!(v1, 1);

    let got = store.docs().get::<Customer>(&id).await?.unwrap();
    assert_eq!(got.0.email, "a@x");
    assert_eq!(got.1, 1);

    let v2 = store
        .docs()
        .update::<Customer, _>(&id, 1, |c| c.tier = "pro".into())
        .await?;
    assert_eq!(v2, 2);
    let got2 = store.docs().get::<Customer>(&id).await?.unwrap();
    assert_eq!(got2.0.tier, "pro");
    assert_eq!(got2.1, 2);

    // version conflict
    let err = store
        .docs()
        .put(
            &id,
            &Customer {
                email: "a@x".into(),
                tier: "plus".into(),
            },
            Some(1),
        )
        .await
        .expect_err("expect conflict");
    match err {
        Error::DocVersionConflict => {}
        other => panic!("wrong error: {other:?}"),
    }

    // soft delete
    sqlx::query("update docs set deleted_at = now() where id = $1")
        .bind(id)
        .execute(store.pool())
        .await?;
    assert!(store.docs().get::<Customer>(&id).await?.is_none());

    // restore
    sqlx::query("update docs set deleted_at = null where id = $1")
        .bind(id)
        .execute(store.pool())
        .await?;
    let got3 = store.docs().get::<Customer>(&id).await?.unwrap();
    assert_eq!(got3.0.email, "a@x");

    Ok(())
}
