use anyhow::Result;
use rillflow::Store;
use std::time::Duration;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn live_cache_ttl_and_invalidate() -> Result<()> {
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
    sqlx::query("create table if not exists counters(n int not null)").execute(store.pool()).await?;
    sqlx::query("insert into counters(n) values(0)").execute(store.pool()).await?;

    let live = store.live().with_ttl(Duration::from_millis(300));

    let key = "counter";
    let sql = "select n::int8 from counters limit 1"; // cast to int8 for i64 decode

    let a = live.query_scalar_i64_cached(key, sql).await?;
    assert_eq!(a, 0);

    // bump the value in DB, cached result should still be 0 until TTL expires
    sqlx::query("update counters set n = n + 1").execute(store.pool()).await?;
    let b = live.query_scalar_i64_cached(key, sql).await?;
    assert_eq!(b, 0);

    // wait for TTL then it should refresh
    tokio::time::sleep(Duration::from_millis(400)).await;
    let c = live.query_scalar_i64_cached(key, sql).await?;
    assert_eq!(c, 1);

    // invalidate and bump again; next read should see new value immediately
    live.invalidate(key);
    sqlx::query("update counters set n = n + 1").execute(store.pool()).await?;
    let d = live.query_scalar_i64_cached(key, sql).await?;
    assert_eq!(d, 2);
    Ok(())
}
