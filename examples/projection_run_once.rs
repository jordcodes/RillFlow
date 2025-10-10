use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};
use rillflow::projections::ProjectionHandler;
use rillflow::testing::ensure_counters_table;
use rillflow::{Expected, SchemaConfig, Store, events::Event};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

struct CounterProjection;

#[async_trait::async_trait]
impl ProjectionHandler for CounterProjection {
    async fn apply(
        &self,
        _event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> rillflow::Result<()> {
        let id: Uuid = serde_json::from_value(body.get("id").cloned().unwrap())?;
        sqlx::query(
            r#"
            insert into counters(id, count) values ($1, 1)
            on conflict (id) do update set count = counters.count + 1
            "#,
        )
        .bind(id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let store = Store::connect(&url).await?;

    // Ensure base schema
    store.schema().sync(&SchemaConfig::single_tenant()).await?;
    ensure_counters_table(store.pool()).await?;

    // Append sample events
    let stream = Uuid::new_v4();
    let body = serde_json::json!({"id": stream});
    store
        .events()
        .append_stream(
            stream,
            Expected::Any,
            vec![Event::new("Ping", &body), Event::new("Ping", &body)],
        )
        .await?;

    // Run a single tick
    let mut daemon = ProjectionDaemon::new(store.pool().clone(), ProjectionWorkerConfig::default());
    daemon.register("counter", Arc::new(CounterProjection));
    let res = daemon.tick_once("counter").await?;
    println!("tick result: {:?}", res);

    // Verify count
    let count: Option<i32> = sqlx::query_scalar("select count from counters where id = $1")
        .bind(stream)
        .fetch_optional(store.pool())
        .await?;
    println!("projected count: {:?}", count);

    Ok(())
}
