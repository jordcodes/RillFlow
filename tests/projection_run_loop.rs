use anyhow::{Result, bail};
use rillflow::testing::{ensure_counters_table, migrate_core_schema};
use rillflow::{
    Expected, SchemaConfig, Store,
    events::Event,
    projection_runtime::{
        DaemonClusterConfig, HotColdConfig, ProjectionDaemon, ProjectionWorkerConfig,
    },
    projections::ProjectionHandler,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{sync::Notify, time::timeout};
use uuid::Uuid;

struct DaemonProc {
    id: Uuid,
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<rillflow::Result<()>>,
}

struct BlockingProjection {
    should_block: Arc<AtomicBool>,
    block_started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl ProjectionHandler for BlockingProjection {
    async fn apply(
        &self,
        _event_type: &str,
        body: &serde_json::Value,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> rillflow::Result<()> {
        let id: Uuid = serde_json::from_value(body.get("id").cloned().unwrap())?;
        let block = body.get("block").and_then(|v| v.as_bool()).unwrap_or(false);

        if block
            && self
                .should_block
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.block_started.notify_waiters();
            self.release.notified().await;
        }

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
    let jh = tokio::spawn(async move { daemon.run_loop(false, stop2, None).await });
    // let it start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    stop.store(true, Ordering::Relaxed);
    jh.await??;
    Ok(())
}

#[tokio::test]
async fn hot_node_failover_promotes_standby() -> Result<()> {
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
    store.schema().sync(&SchemaConfig::single_tenant()).await?;

    let cluster_name = format!("test-cluster-{}", Uuid::new_v4());
    let timing = HotColdConfig::new(
        cluster_name.clone(),
        Duration::from_millis(1_000),
        Duration::from_millis(200),
        Duration::from_millis(0),
        0,
    );

    let hot_id = Uuid::new_v4();
    let hot_config = ProjectionWorkerConfig {
        cluster: DaemonClusterConfig::HotCold(timing.clone()),
        node_name: "node-hot".to_string(),
        daemon_id: Some(hot_id),
        lease_ttl: Duration::from_millis(200),
        ..ProjectionWorkerConfig::default()
    };
    let hot_daemon = ProjectionDaemon::new(store.pool().clone(), hot_config);
    let hot_stop = Arc::new(AtomicBool::new(false));
    let hot_stop_task = hot_stop.clone();
    let hot_handle =
        tokio::spawn(async move { hot_daemon.run_loop(true, hot_stop_task, None).await });

    wait_for_hot(store.pool(), &cluster_name, hot_id, "hot bootstrap").await?;

    wait_for_hot(store.pool(), &cluster_name, hot_id, "hot node").await?;

    let cold_id = Uuid::new_v4();
    let cold_config = ProjectionWorkerConfig {
        cluster: DaemonClusterConfig::HotCold(timing.clone()),
        node_name: "node-cold".to_string(),
        daemon_id: Some(cold_id),
        lease_ttl: Duration::from_millis(200),
        ..ProjectionWorkerConfig::default()
    };
    let cold_daemon = ProjectionDaemon::new(store.pool().clone(), cold_config);
    let cold_stop = Arc::new(AtomicBool::new(false));
    let cold_stop_task = cold_stop.clone();
    let cold_handle =
        tokio::spawn(async move { cold_daemon.run_loop(true, cold_stop_task, None).await });

    wait_for_role(store.pool(), &cluster_name, cold_id, "cold").await?;

    hot_stop.store(true, Ordering::Relaxed);
    let failover_start = std::time::Instant::now();
    hot_handle.await??;

    wait_for_hot(store.pool(), &cluster_name, cold_id, "standby promotion").await?;
    let failover_elapsed = failover_start.elapsed();
    assert!(
        failover_elapsed < Duration::from_millis(2_000),
        "failover took {:?}, expected under 2s",
        failover_elapsed
    );

    cold_stop.store(true, Ordering::Relaxed);
    cold_handle.await??;

    Ok(())
}

#[tokio::test]
async fn mid_batch_failover_resumes_without_duplicates() -> Result<()> {
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
    store.schema().sync(&SchemaConfig::single_tenant()).await?;
    migrate_core_schema(store.pool()).await?;
    ensure_counters_table(store.pool()).await?;

    let cluster_name = format!("test-cluster-{}", Uuid::new_v4());
    let timing = HotColdConfig::new(
        cluster_name.clone(),
        Duration::from_millis(1_000),
        Duration::from_millis(300),
        Duration::from_millis(0),
        0,
    );

    let should_block = Arc::new(AtomicBool::new(true));
    let block_started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let projection = BlockingProjection {
        should_block: should_block.clone(),
        block_started: block_started.clone(),
        release: release.clone(),
    };

    let hot_id = Uuid::new_v4();
    let hot_config = ProjectionWorkerConfig {
        cluster: DaemonClusterConfig::HotCold(timing.clone()),
        node_name: "node-hot".to_string(),
        daemon_id: Some(hot_id),
        lease_ttl: Duration::from_millis(300),
        ..ProjectionWorkerConfig::default()
    };
    let mut hot_daemon = ProjectionDaemon::new(store.pool().clone(), hot_config);
    hot_daemon.register("blocking", Arc::new(projection));
    let hot_stop = Arc::new(AtomicBool::new(false));
    let hot_stop_task = hot_stop.clone();
    let hot_handle =
        tokio::spawn(async move { hot_daemon.run_loop(true, hot_stop_task, None).await });

    wait_for_hot(store.pool(), &cluster_name, hot_id, "hot bootstrap").await?;

    let cold_id = Uuid::new_v4();
    let cold_config = ProjectionWorkerConfig {
        cluster: DaemonClusterConfig::HotCold(timing.clone()),
        node_name: "node-cold".to_string(),
        daemon_id: Some(cold_id),
        lease_ttl: Duration::from_millis(300),
        ..ProjectionWorkerConfig::default()
    };
    let mut cold_daemon = ProjectionDaemon::new(store.pool().clone(), cold_config);
    cold_daemon.register(
        "blocking",
        Arc::new(BlockingProjection {
            should_block: should_block.clone(),
            block_started: block_started.clone(),
            release: release.clone(),
        }),
    );
    let cold_stop = Arc::new(AtomicBool::new(false));
    let cold_stop_task = cold_stop.clone();
    let cold_handle =
        tokio::spawn(async move { cold_daemon.run_loop(true, cold_stop_task, None).await });

    wait_for_role(store.pool(), &cluster_name, cold_id, "cold").await?;
    wait_for_hot(store.pool(), &cluster_name, hot_id, "initial leader").await?;

    let stream = Uuid::new_v4();
    let body_block = json!({"id": stream, "block": true});
    let body_follow = json!({"id": stream, "block": false});
    store
        .events()
        .append_stream(
            stream,
            Expected::Any,
            vec![
                Event::new("Ping", &body_block),
                Event::new("Ping", &body_follow),
            ],
        )
        .await?;

    tokio::time::timeout(Duration::from_secs(30), block_started.notified())
        .await
        .expect("leader did not start processing blocking event");

    hot_handle.abort();
    let _ = hot_handle.await;

    wait_for_hot(store.pool(), &cluster_name, cold_id, "post-failover leader").await?;

    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        loop {
            let count: Option<i64> = sqlx::query_scalar("select count from counters where id = $1")
                .bind(stream)
                .fetch_optional(store.pool())
                .await?
                .map(|v: i32| v as i64);
            if count == Some(2) {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
    .unwrap();

    cold_stop.store(true, Ordering::Relaxed);
    cold_handle.await??;

    Ok(())
}

#[tokio::test]
async fn rapid_failover_stress_executes_all_events() -> Result<()> {
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
    store.schema().sync(&SchemaConfig::single_tenant()).await?;
    migrate_core_schema(store.pool()).await?;
    ensure_counters_table(store.pool()).await?;

    let cluster_name = format!("stress-cluster-{}", Uuid::new_v4());
    let timing = HotColdConfig::new(
        cluster_name.clone(),
        Duration::from_millis(1_000),
        Duration::from_millis(300),
        Duration::from_millis(0),
        0,
    );

    let should_block = Arc::new(AtomicBool::new(false));
    let block_started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let base_config = ProjectionWorkerConfig {
        cluster: DaemonClusterConfig::HotCold(timing.clone()),
        lease_ttl: Duration::from_millis(300),
        ..ProjectionWorkerConfig::default()
    };

    let pool = store.pool().clone();
    let spawn_daemon = |name: &str, config: &ProjectionWorkerConfig| -> DaemonProc {
        let id = Uuid::new_v4();
        let mut cfg = config.clone();
        cfg.daemon_id = Some(id);
        cfg.node_name = name.to_string();
        let mut daemon = ProjectionDaemon::new(pool.clone(), cfg);
        daemon.register(
            "blocking",
            Arc::new(BlockingProjection {
                should_block: should_block.clone(),
                block_started: block_started.clone(),
                release: release.clone(),
            }),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let handle = tokio::spawn(async move { daemon.run_loop(true, stop_clone, None).await });
        DaemonProc { id, stop, handle }
    };

    let mut daemons = vec![
        spawn_daemon("node-a", &base_config),
        spawn_daemon("node-b", &base_config),
        spawn_daemon("node-c", &base_config),
    ];

    timeout(Duration::from_secs(5), async {
        loop {
            if current_hot(store.pool(), &cluster_name).await?.is_some() {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("initial leader never elected")?;

    let mut expected_total = 0i64;
    for iter in 0..3 {
        should_block.store(true, Ordering::SeqCst);
        let stream = Uuid::new_v4();
        let body_block = json!({"id": stream, "block": true, "iteration": iter});
        let body_follow = json!({"id": stream, "block": false, "iteration": iter});
        store
            .events()
            .append_stream(
                stream,
                Expected::Any,
                vec![
                    Event::new("Ping", &body_block),
                    Event::new("Ping", &body_follow),
                    Event::new("Ping", &body_follow),
                ],
            )
            .await?;

        expected_total += 3;

        timeout(Duration::from_secs(30), block_started.notified())
            .await
            .expect("blocking handler never started");

        let hot_id = current_hot(store.pool(), &cluster_name)
            .await?
            .expect("cluster missing hot node");

        if let Some(pos) = daemons.iter().position(|d| d.id == hot_id) {
            let proc = daemons.remove(pos);
            proc.handle.abort();
            let _ = proc.handle.await;
        }

        let _new_hot = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(id) = current_hot(store.pool(), &cluster_name).await? {
                    if id != hot_id {
                        return Ok::<Uuid, sqlx::Error>(id);
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("cluster never promoted new leader")?;

        release.notify_waiters();

        timeout(Duration::from_secs(5), async {
            loop {
                let total: i64 = sqlx::query_scalar("select coalesce(sum(count), 0) from counters")
                    .fetch_one(store.pool())
                    .await?;
                if total == expected_total {
                    return Ok::<(), sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("counter total did not reach expected value")?;

        let new_name = format!("node-rejoin-{}", iter);
        daemons.push(spawn_daemon(&new_name, &base_config));
        let new_id = daemons.last().unwrap().id;
        wait_for_role(store.pool(), &cluster_name, new_id, "cold").await?;
    }

    for proc in daemons {
        proc.stop.store(true, Ordering::Relaxed);
        let _ = proc.handle.await??;
    }

    let final_total: i64 = sqlx::query_scalar("select coalesce(sum(count), 0) from counters")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(final_total, expected_total);

    Ok(())
}

async fn wait_for_hot(
    pool: &sqlx::PgPool,
    cluster: &str,
    expected: Uuid,
    context: &str,
) -> Result<()> {
    match tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(id) = current_hot(pool, cluster).await? {
                if id == expected {
                    return Ok::<(), sqlx::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => bail!("timed out waiting for {context} to hold hot lease"),
    }
}

async fn wait_for_role(
    pool: &sqlx::PgPool,
    cluster: &str,
    daemon: Uuid,
    expected: &str,
) -> Result<()> {
    match tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(role) = current_role(pool, cluster, daemon).await? {
                if role == expected {
                    return Ok::<(), sqlx::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => bail!("timed out waiting for daemon {daemon} to assume role {expected}"),
    }
}

async fn current_hot(pool: &sqlx::PgPool, cluster: &str) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "select daemon_id from rf_daemon_nodes where cluster = $1 and role = 'hot' limit 1",
    )
    .bind(cluster)
    .fetch_optional(pool)
    .await
}

async fn current_role(
    pool: &sqlx::PgPool,
    cluster: &str,
    daemon: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        "select role from rf_daemon_nodes where cluster = $1 and daemon_id = $2 limit 1",
    )
    .bind(cluster)
    .bind(daemon)
    .fetch_optional(pool)
    .await
}
