use anyhow::Result;
use rillflow::Store;
use rillflow::query::{CompiledQuery, DocumentQueryContext};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

struct SumByUser;

impl CompiledQuery<serde_json::Value> for SumByUser {
    fn configure(&self, ctx: &mut DocumentQueryContext) {
        // Aggregate total amount per user name via include
        ctx.include("users", ["user_id"].as_ref(), "id", "u")
            .group_by_from("u", ["name"].as_ref())
            .sum(["amount"].as_ref(), "total");
    }
}

#[tokio::test]
async fn aggregates_with_include_sum() -> Result<()> {
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

    sqlx::query("create table if not exists users (id uuid primary key, doc jsonb not null)")
        .execute(store.pool())
        .await?;

    let ua = Uuid::new_v4();
    let ub = Uuid::new_v4();
    sqlx::query("insert into users(id, doc) values ($1,$2), ($3,$4)")
        .bind(ua)
        .bind(json!({"id": ua, "name": "Alice"}))
        .bind(ub)
        .bind(json!({"id": ub, "name": "Bob"}))
        .execute(store.pool())
        .await?;

    // orders
    let o1 = Uuid::new_v4();
    let o2 = Uuid::new_v4();
    let o3 = Uuid::new_v4();
    let o4 = Uuid::new_v4();
    store
        .docs()
        .upsert(&o1, &json!({"id": o1, "user_id": ua, "amount": 10}))
        .await?;
    store
        .docs()
        .upsert(&o2, &json!({"id": o2, "user_id": ua, "amount": 7}))
        .await?;
    store
        .docs()
        .upsert(&o3, &json!({"id": o3, "user_id": ub, "amount": 5}))
        .await?;
    store
        .docs()
        .upsert(&o4, &json!({"id": o4, "user_id": ub, "amount": 8}))
        .await?;

    let rows: Vec<serde_json::Value> = store.docs().execute_compiled(SumByUser).await?;

    // rows are json objects with only aggregate fields like { total: <numeric> }
    // Collect totals and assert the multiset equals {17, 13}
    let mut totals: Vec<f64> = rows
        .into_iter()
        .map(|r| r.get("total").and_then(|v| v.as_f64()).unwrap())
        .collect();
    totals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(totals, vec![13.0, 17.0]);

    Ok(())
}
