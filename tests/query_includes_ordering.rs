use anyhow::Result;
use rillflow::Store;
use rillflow::query::{CompiledQuery, DocumentQueryContext, SortDirection};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

struct OrdersWithUserByName;

impl CompiledQuery<serde_json::Value> for OrdersWithUserByName {
    fn configure(&self, ctx: &mut DocumentQueryContext) {
        ctx.include("users", ["user_id"].as_ref(), "id", "u")
            .select_from("u", "user", "name")
            .select_field("amount", "amount")
            .order_by_from("u", ["name"].as_ref(), SortDirection::Asc)
            .order_by_number(["amount"].as_ref(), SortDirection::Desc);
    }
}

#[tokio::test]
async fn query_with_include_and_source_ordering() -> Result<()> {
    // spin up postgres
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

    // join target with a jsonb `doc` column
    sqlx::query("create table if not exists users (id uuid primary key, doc jsonb not null)")
        .execute(store.pool())
        .await?;

    // insert users
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();
    sqlx::query("insert into users(id, doc) values ($1,$2), ($3,$4)")
        .bind(u1)
        .bind(json!({"id": u1, "name": "Alice"}))
        .bind(u2)
        .bind(json!({"id": u2, "name": "Bob"}))
        .execute(store.pool())
        .await?;

    // insert orders into docs (user_id references users.id)
    let o1 = Uuid::new_v4();
    let o2 = Uuid::new_v4();
    let o3 = Uuid::new_v4();
    store
        .docs()
        .upsert(
            &o1,
            &json!({"id": o1, "user_id": u1, "amount": 10, "note": "x"}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o2,
            &json!({"id": o2, "user_id": u2, "amount": 20, "note": "y"}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o3,
            &json!({"id": o3, "user_id": u1, "amount": 5, "note": "z"}),
        )
        .await?;

    let rows: Vec<serde_json::Value> = store.docs().execute_compiled(OrdersWithUserByName).await?;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["user"], "Alice");
    assert_eq!(rows[0]["amount"], 10);
    assert_eq!(rows[1]["user"], "Alice");
    assert_eq!(rows[1]["amount"], 5);
    assert_eq!(rows[2]["user"], "Bob");
    assert_eq!(rows[2]["amount"], 20);

    Ok(())
}
