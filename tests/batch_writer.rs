use anyhow::Result;
use rillflow::Store;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Doc { id: Uuid, v: i32 }

#[tokio::test]
async fn batch_writer_upsert_update_delete() -> Result<()> {
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

    let mut batch = store.batch();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    batch.upsert(id1, &Doc { id: id1, v: 1 })?;
    batch.upsert(id2, &Doc { id: id2, v: 2 })?;
    let out = batch.flush().await?;
    assert_eq!(out.upserts, 2);

    // update id1 with expected version 1
    let ver1: i32 = sqlx::query_scalar("select version from docs where id = $1")
        .bind(id1)
        .fetch_one(store.pool())
        .await?;
    assert_eq!(ver1, 1);
    let mut batch2 = store.batch();
    batch2.update_expected(id1, &Doc { id: id1, v: 3 }, 1)?;
    let out2 = batch2.flush().await?;
    assert_eq!(out2.updates, 1);

    // delete id2
    let mut batch3 = store.batch();
    batch3.delete(id2);
    let out3 = batch3.flush().await?;
    assert_eq!(out3.deletes, 1);

    // validate
    let ids: Vec<Uuid> = sqlx::query_scalar("select id from docs")
        .fetch_all(store.pool())
        .await?;
    let set: HashSet<Uuid> = ids.into_iter().collect();
    assert!(set.contains(&id1));
    assert!(!set.contains(&id2));
    Ok(())
}
