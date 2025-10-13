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

#[derive(Clone, Copy)]
struct OrdersByUserNamePaged {
    page: u32,
    per_page: u32,
}

impl CompiledQuery<serde_json::Value> for OrdersByUserNamePaged {
    fn configure(&self, ctx: &mut DocumentQueryContext) {
        ctx.include("users", ["user_id"].as_ref(), "id", "u")
            .select_from("u", "user", "name")
            .select_field("amount", "amount")
            // filter amount between 5 and 50
            .filter(rillflow::query::Predicate::between(
                ["amount"].as_ref(),
                5.0,
                50.0,
            ))
            .order_by_from("u", ["name"].as_ref(), SortDirection::Asc)
            .order_by_number(["amount"].as_ref(), SortDirection::Desc)
            .page(self.page, self.per_page);
    }
}

#[tokio::test]
async fn include_with_filters_and_pagination() -> Result<()> {
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
    let ua = Uuid::new_v4();
    let ub = Uuid::new_v4();
    sqlx::query("insert into users(id, doc) values ($1,$2), ($3,$4)")
        .bind(ua)
        .bind(json!({"id": ua, "name": "Alice"}))
        .bind(ub)
        .bind(json!({"id": ub, "name": "Bob"}))
        .execute(store.pool())
        .await?;

    // insert orders
    let o1 = Uuid::new_v4();
    let o2 = Uuid::new_v4();
    let o3 = Uuid::new_v4();
    let o4 = Uuid::new_v4();
    store
        .docs()
        .upsert(
            &o1,
            &json!({"id": o1, "user_id": ua, "amount": 10, "note": "x"}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o2,
            &json!({"id": o2, "user_id": ub, "amount": 20, "note": "y"}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o3,
            &json!({"id": o3, "user_id": ua, "amount": 5, "note": "z"}),
        )
        .await?;
    store
        .docs()
        .upsert(
            &o4,
            &json!({"id": o4, "user_id": ua, "amount": 55, "note": "w"}),
        )
        .await?;

    // page 1 (per_page=2) should include Alice(10) then Alice(5); amount 55 excluded by between filter
    let page1: Vec<serde_json::Value> = store
        .docs()
        .execute_compiled(OrdersByUserNamePaged {
            page: 1,
            per_page: 2,
        })
        .await?;
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0]["user"], "Alice");
    assert_eq!(page1[0]["amount"], 10);
    assert_eq!(page1[1]["user"], "Alice");
    assert_eq!(page1[1]["amount"], 5);

    // page 2 should contain the remaining Bob(20)
    let page2: Vec<serde_json::Value> = store
        .docs()
        .execute_compiled(OrdersByUserNamePaged {
            page: 2,
            per_page: 2,
        })
        .await?;
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0]["user"], "Bob");
    assert_eq!(page2[0]["amount"], 20);

    Ok(())
}
