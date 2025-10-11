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
struct Doc {
    id: Uuid,
    title: String,
    amount: i32,
}

#[tokio::test]
async fn full_text_search_and_batch_ops() -> Result<()> {
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

    // Batch upsert
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let docs = vec![
        (
            id1,
            Doc {
                id: id1,
                title: "Alice in Rustland".into(),
                amount: 10,
            },
        ),
        (
            id2,
            Doc {
                id: id2,
                title: "Bob the Builder".into(),
                amount: 20,
            },
        ),
    ];
    let n = store.docs().bulk_upsert(&docs).await?;
    assert_eq!(n, 2);

    // FTS search for "Alice"
    let mut ctx = rillflow::query::DocumentQueryContext::new();
    ctx.full_text_search("Alice");
    let (pool, mut builder) = ctx.build_query(store.pool().clone());
    let query = builder.build_query_as::<(serde_json::Value,)>();
    let rows = query.fetch_all(&pool).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0["title"], json!("Alice in Rustland"));

    // Complex predicates: regex and between
    let results: Vec<serde_json::Value> = store
        .docs()
        .query::<serde_json::Value>()
        .filter(rillflow::query::Predicate::iregex("title", "builder"))
        .filter(rillflow::query::Predicate::between("amount", 15.0, 25.0))
        .fetch_all()
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["title"], json!("Bob the Builder"));

    // Bulk update expected
    let updated = vec![
        (
            id1,
            Doc {
                id: id1,
                title: "Alice Returns".into(),
                amount: 30,
            },
            1,
        ),
        (
            id2,
            Doc {
                id: id2,
                title: "Bob the Builder".into(),
                amount: 40,
            },
            1,
        ),
    ];
    let (ok, conflicts) = store.docs().bulk_update_expected(&updated).await?;
    assert_eq!(ok, 2);
    assert_eq!(conflicts, 0);

    // Bulk delete
    let deleted = store.docs().bulk_delete(&[id1]).await?;
    assert_eq!(deleted, 1);

    Ok(())
}
