use anyhow::Result;
use rillflow::projections::ViewProjection;
use rillflow::{Event, Expected, Store};
use serde_json::json;
use std::sync::Arc;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

#[tokio::test]
async fn view_projection_counts_and_sums() -> Result<()> {
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

    let proj = ViewProjection::new("order_totals")
        .group_by("customer_id", "customer")
        .count("cnt")
        .sum("amount", "total");

    let stream = Uuid::new_v4();
    store
        .events()
        .enable_inline_projections()
        .append_stream(
            stream,
            Expected::Any,
            vec![
                Event::new("OrderPlaced", &json!({"customer":"a","amount": 10})),
                Event::new("OrderPlaced", &json!({"customer":"a","amount": 20})),
                Event::new("OrderPlaced", &json!({"customer":"b","amount": 5})),
            ],
        )
        .await?;

    // Apply the projection via inline by registering it as a handler
    rillflow::events::register_inline_projection(Arc::new(proj));

    // Append more to trigger
    store
        .events()
        .enable_inline_projections()
        .append_stream(
            stream,
            Expected::Any,
            vec![Event::new(
                "OrderPlaced",
                &json!({"customer":"a","amount": 7}),
            )],
        )
        .await?;

    let (cnt_a, total_a): (i64, f64) =
        sqlx::query_as("select cnt, total::float8 from order_totals where customer_id = 'a'")
            .fetch_one(store.pool())
            .await?;
    assert!(cnt_a >= 1);
    assert!(total_a >= 7.0);
    Ok(())
}
