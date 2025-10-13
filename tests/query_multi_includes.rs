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

struct OrdersWithUserAndVendor;

impl CompiledQuery<serde_json::Value> for OrdersWithUserAndVendor {
    fn configure(&self, ctx: &mut DocumentQueryContext) {
        ctx.include("users", ["user_id"].as_ref(), "id", "u")
            .include("vendors", ["vendor_id"].as_ref(), "id", "v")
            .select_from("u", "user", "name")
            .select_from("v", "vendor", "name")
            .select_field("amount", "amount")
            .order_by_from("u", ["name"].as_ref(), SortDirection::Asc)
            .order_by_from("v", ["name"].as_ref(), SortDirection::Asc)
            .order_by_number(["amount"].as_ref(), SortDirection::Desc);
    }
}

#[tokio::test]
async fn multi_include_select_and_order() -> Result<()> {
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

    // join targets with jsonb `doc`
    sqlx::query("create table if not exists users (id uuid primary key, doc jsonb not null)")
        .execute(store.pool())
        .await?;
    sqlx::query("create table if not exists vendors (id uuid primary key, doc jsonb not null)")
        .execute(store.pool())
        .await?;

    let ua = Uuid::new_v4();
    let ub = Uuid::new_v4();
    let va = Uuid::new_v4();
    let vb = Uuid::new_v4();

    sqlx::query("insert into users(id, doc) values ($1,$2), ($3,$4)")
        .bind(ua)
        .bind(json!({"id": ua, "name": "Alice"}))
        .bind(ub)
        .bind(json!({"id": ub, "name": "Bob"}))
        .execute(store.pool())
        .await?;

    sqlx::query("insert into vendors(id, doc) values ($1,$2), ($3,$4)")
        .bind(va)
        .bind(json!({"id": va, "name": "Acme"}))
        .bind(vb)
        .bind(json!({"id": vb, "name": "Zen"}))
        .execute(store.pool())
        .await?;

    // orders
    let o1 = Uuid::new_v4(); // Alice + Acme amount 15
    let o2 = Uuid::new_v4(); // Alice + Zen amount 30
    let o3 = Uuid::new_v4(); // Bob + Acme amount 25
    store
        .docs()
        .upsert(
            &o1,
            &json!({"id": o1, "user_id": ua, "vendor_id": va, "amount": 15}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o2,
            &json!({"id": o2, "user_id": ua, "vendor_id": vb, "amount": 30}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o3,
            &json!({"id": o3, "user_id": ub, "vendor_id": va, "amount": 25}),
        )
        .await?;

    let rows: Vec<serde_json::Value> = store
        .docs()
        .execute_compiled(OrdersWithUserAndVendor)
        .await?;

    // Expect name asc (Alice before Bob), then vendor asc within same user, then amount desc within same vendor
    // So: Alice/Acme(15), Alice/Zen(30), then Bob/Acme(25)
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["user"], "Alice");
    assert_eq!(rows[0]["vendor"], "Acme");
    assert_eq!(rows[0]["amount"], 15);
    assert_eq!(rows[1]["user"], "Alice");
    assert_eq!(rows[1]["vendor"], "Zen");
    assert_eq!(rows[1]["amount"], 30);
    assert_eq!(rows[2]["user"], "Bob");
    assert_eq!(rows[2]["vendor"], "Acme");
    assert_eq!(rows[2]["amount"], 25);

    Ok(())
}
