use anyhow::Result;
use rillflow::{
    Store,
    projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

#[tokio::test]
async fn run_loop_stops_on_flag() -> Result<()> {
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
    // no need to sync schema since we won't tick anything

    let daemon = ProjectionDaemon::new(store.pool().clone(), ProjectionWorkerConfig::default());
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let jh = tokio::spawn(async move { daemon.run_loop(false, stop2).await });
    // let it start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    stop.store(true, Ordering::Relaxed);
    jh.await??;
    Ok(())
}
