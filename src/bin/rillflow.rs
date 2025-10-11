use clap::{ArgAction, Parser, Subcommand};
use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};
use rillflow::subscriptions::{SubscriptionFilter, SubscriptionOptions, Subscriptions};
use rillflow::{SchemaConfig, Store, TenancyMode, TenantSchema};
use serde_json::Value as JsonValue;
use sqlx::Row;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "rillflow", version, about = "Rillflow CLI")]
struct Cli {
    /// Postgres connection string. Falls back to DATABASE_URL.
    #[arg(long)]
    database_url: Option<String>,

    /// Base schema to manage (default: public)
    #[arg(long, default_value = "public")]
    schema: String,

    /// Additional tenant schemas (repeatable)
    #[arg(long = "tenant-schema", action = ArgAction::Append)]
    tenant_schemas: Vec<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show planned DDL changes without applying
    SchemaPlan,

    /// Apply DDL changes (create schemas/tables/indexes as needed)
    SchemaSync,

    /// Projection admin commands
    #[command(subcommand)]
    Projections(ProjectionsCmd),

    /// Subscriptions admin commands
    #[command(subcommand)]
    Subscriptions(SubscriptionsCmd),

    /// Stream alias helpers
    #[command(subcommand)]
    Streams(StreamsCmd),

    /// Documents admin commands
    #[command(subcommand)]
    Docs(DocsCmd),

    /// Snapshots compaction
    #[command(subcommand)]
    Snapshots(SnapshotsCmd),
}

#[derive(Subcommand, Debug)]
enum ProjectionsCmd {
    /// List projections and their status
    List,
    /// Show status of a single projection
    Status { name: String },
    /// Pause a projection
    Pause { name: String },
    /// Resume a projection
    Resume { name: String },
    /// Reset checkpoint to a specific sequence
    ResetCheckpoint { name: String, seq: i64 },
    /// Rebuild (reset to 0 and clear DLQ)
    Rebuild { name: String },
    /// Run a single processing tick for one projection (by name) or all registered if omitted
    RunOnce { name: Option<String> },
    /// Run until idle (no projection has work)
    RunUntilIdle { name: Option<String> },
    /// Dead Letter Queue: list recent failures
    DlqList {
        name: String,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Dead Letter Queue: requeue one item by id (sets checkpoint to id's seq - 1)
    DlqRequeue { name: String, id: i64 },
    /// Dead Letter Queue: delete one item by id
    DlqDelete { name: String, id: i64 },
    /// Show basic metrics (lag, last_seq, dlq)
    Metrics { name: String },
    /// Run long-lived projection loop
    RunLoop {
        /// Use LISTEN/NOTIFY to wake immediately on new events
        #[arg(long, default_value_t = true)]
        use_notify: bool,
        /// Optional health HTTP bind, e.g. 0.0.0.0:8080
        #[arg(long)]
        health_bind: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum SubscriptionsCmd {
    /// Create or update a subscription checkpoint and filter
    Create {
        name: String,
        #[arg(long, action = ArgAction::Append)]
        event_type: Vec<String>,
        #[arg(long, action = ArgAction::Append)]
        stream_id: Vec<String>,
        #[arg(long, default_value_t = 0)]
        start_from: i64,
    },
    /// List subscriptions and checkpoints
    List,
    /// Show a subscription status
    Status { name: String },
    /// Pause a subscription
    Pause { name: String },
    /// Resume a subscription
    Resume { name: String },
    /// Reset checkpoint to a specific sequence
    Reset { name: String, seq: i64 },
    /// Tail a subscription (prints incoming events)
    Tail {
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Optional consumer group name
        #[arg(long)]
        group: Option<String>,
    },
    /// Group admin: list groups for a subscription
    Groups { name: String },
    /// Group admin: show a group status (checkpoint and lease)
    GroupStatus { name: String, group: String },
}

#[derive(Subcommand, Debug)]
enum StreamsCmd {
    /// Resolve or create a stream id for an alias
    Resolve { alias: String },
}

#[derive(Subcommand, Debug)]
enum DocsCmd {
    /// Get a document by id
    Get { id: String },
    /// Soft-delete a document (sets deleted_at)
    SoftDelete { id: String },
    /// Restore a soft-deleted document (clears deleted_at)
    Restore { id: String },
}

#[derive(Subcommand, Debug)]
enum SnapshotsCmd {
    /// Write snapshots for streams where head - snapshot.version >= threshold (version-only body)
    CompactOnce {
        #[arg(long, default_value_t = 100)]
        threshold: i32,
        #[arg(long, default_value_t = 100)]
        batch: i64,
    },
    /// Repeatedly compact until no more streams exceed the threshold
    RunUntilIdle {
        #[arg(long, default_value_t = 100)]
        threshold: i32,
        #[arg(long, default_value_t = 100)]
        batch: i64,
    },
    /// Show snapshotter metrics (candidate streams and max gap)
    Metrics {
        #[arg(long, default_value_t = 100)]
        threshold: i32,
    },
}

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let cli = Cli::parse();

    let url = match cli
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok())
    {
        Some(u) => u,
        None => {
            eprintln!("error: --database-url or env DATABASE_URL is required");
            std::process::exit(2);
        }
    };

    let tenancy_mode = if cli.tenant_schemas.is_empty() {
        TenancyMode::SingleTenant
    } else {
        TenancyMode::SchemaPerTenant {
            tenants: cli
                .tenant_schemas
                .iter()
                .map(|s| TenantSchema::new(s.trim()))
                .collect(),
        }
    };

    let config = SchemaConfig {
        base_schema: cli.schema,
        tenancy_mode,
    };

    let store = Store::connect(&url).await?;
    let mgr = store.schema();

    match cli.command {
        Commands::SchemaPlan => {
            let plan = mgr.plan(&config).await?;
            print_plan(&plan);
        }
        Commands::SchemaSync => {
            let plan = mgr.sync(&config).await?;
            if plan.is_empty() {
                println!("No changes needed.");
            } else {
                println!("Applied changes:");
                print_plan(&plan);
            }
        }
        Commands::Projections(cmd) => {
            let daemon =
                ProjectionDaemon::new(store.pool().clone(), ProjectionWorkerConfig::default());
            match cmd {
                ProjectionsCmd::List => {
                    let list = daemon.list().await?;
                    for s in list {
                        println!(
                            "{}  last_seq={}  paused={}  leased_by={:?}  lease_until={:?}  backoff_until={:?}  dlq_count={}",
                            s.name,
                            s.last_seq,
                            s.paused,
                            s.leased_by,
                            s.lease_until,
                            s.backoff_until,
                            s.dlq_count
                        );
                    }
                }
                ProjectionsCmd::Status { name } => {
                    let s = daemon.status(&name).await?;
                    println!(
                        "{}  last_seq={}  paused={}  leased_by={:?}  lease_until={:?}  backoff_until={:?}  dlq_count={}",
                        s.name,
                        s.last_seq,
                        s.paused,
                        s.leased_by,
                        s.lease_until,
                        s.backoff_until,
                        s.dlq_count
                    );
                }
                ProjectionsCmd::Pause { name } => {
                    daemon.pause(&name).await?;
                    println!("paused {}", { name });
                }
                ProjectionsCmd::Resume { name } => {
                    daemon.resume(&name).await?;
                    println!("resumed {}", { name });
                }
                ProjectionsCmd::ResetCheckpoint { name, seq } => {
                    daemon.reset_checkpoint(&name, seq).await?;
                    println!("reset {} to {}", name, seq);
                }
                ProjectionsCmd::Rebuild { name } => {
                    daemon.rebuild(&name).await?;
                    println!("rebuild scheduled for {}", name);
                }
                ProjectionsCmd::RunOnce { name } => {
                    if let Some(n) = name {
                        let res = daemon.tick_once(&n).await?;
                        println!("{}: {:?}", n, res);
                    } else {
                        daemon.tick_all_once().await?;
                        println!("tick-all executed");
                    }
                }
                ProjectionsCmd::RunUntilIdle { name } => {
                    if let Some(n) = name {
                        // run only this projection until idle
                        loop {
                            let res = daemon.tick_once(&n).await?;
                            match res {
                                rillflow::projection_runtime::TickResult::Processed { count }
                                    if count > 0 => {}
                                _ => break,
                            }
                        }
                        println!("{}: idle", n);
                    } else {
                        daemon.run_until_idle().await?;
                        println!("all projections idle");
                    }
                }
                ProjectionsCmd::DlqList { name, limit } => {
                    let items = daemon.dlq_list(&name, limit).await?;
                    for i in items {
                        println!(
                            "id={} seq={} type={} failed_at={} error={}",
                            i.id, i.seq, i.event_type, i.failed_at, i.error
                        );
                    }
                }
                ProjectionsCmd::DlqRequeue { name, id } => {
                    daemon.dlq_requeue(&name, id).await?;
                    println!("requeued {}:{}", name, id);
                }
                ProjectionsCmd::DlqDelete { name, id } => {
                    daemon.dlq_delete(&name, id).await?;
                    println!("deleted {}:{}", name, id);
                }
                ProjectionsCmd::Metrics { name } => {
                    let m = daemon.metrics(&name).await?;
                    println!(
                        "{} last_seq={} head_seq={} lag={} dlq_count={}",
                        m.name, m.last_seq, m.head_seq, m.lag, m.dlq_count
                    );
                }
                ProjectionsCmd::RunLoop {
                    use_notify,
                    health_bind,
                } => {
                    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let stop2 = stop.clone();
                    tokio::spawn(async move {
                        let _ = tokio::signal::ctrl_c().await;
                        stop2.store(true, std::sync::atomic::Ordering::Relaxed);
                    });

                    if let Some(addr) = health_bind {
                        let stop_health = stop.clone();
                        tokio::spawn(async move {
                            if let Err(err) = serve_health(addr, stop_health).await {
                                eprintln!("health server error: {err}");
                            }
                        });
                    }

                    daemon.run_loop(use_notify, stop).await?;
                }
            }
        }
        Commands::Subscriptions(cmd) => {
            let subs = Subscriptions::new(store.pool().clone());
            match cmd {
                SubscriptionsCmd::Create {
                    name,
                    event_type,
                    stream_id,
                    start_from,
                } => {
                    let ids: Vec<Uuid> = stream_id
                        .into_iter()
                        .filter_map(|s| Uuid::parse_str(&s).ok())
                        .collect();
                    let filter = SubscriptionFilter {
                        event_types: if event_type.is_empty() {
                            None
                        } else {
                            Some(event_type)
                        },
                        stream_ids: if ids.is_empty() { None } else { Some(ids) },
                        stream_prefix: None,
                    };
                    subs.create_or_update(&name, &filter, start_from).await?;
                    println!("subscription '{}' upserted (from={})", name, start_from);
                }
                SubscriptionsCmd::List => {
                    let rows = sqlx::query(
                        "select name, last_seq, paused, backoff_until, filter from subscriptions order by name",
                    )
                    .fetch_all(store.pool())
                    .await?;
                    for r in rows {
                        let name: String = r.get("name");
                        let last_seq: i64 = r.get::<Option<i64>, _>("last_seq").unwrap_or(0);
                        let paused: bool = r.get::<Option<bool>, _>("paused").unwrap_or(false);
                        let backoff_until =
                            r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("backoff_until");
                        let filter = r
                            .get::<Option<serde_json::Value>, _>("filter")
                            .unwrap_or(serde_json::json!({}));
                        println!(
                            "{} last_seq={} paused={} backoff_until={:?} filter={}",
                            name, last_seq, paused, backoff_until, filter,
                        );
                    }
                }
                SubscriptionsCmd::Status { name } => {
                    let r = sqlx::query(
                        "select name, last_seq, paused, backoff_until, filter from subscriptions where name = $1",
                    )
                    .bind(&name)
                    .fetch_optional(store.pool())
                    .await?;
                    if let Some(r) = r {
                        let last_seq: i64 = r.get::<Option<i64>, _>("last_seq").unwrap_or(0);
                        let paused: bool = r.get::<Option<bool>, _>("paused").unwrap_or(false);
                        let backoff_until =
                            r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("backoff_until");
                        let filter = r
                            .get::<Option<serde_json::Value>, _>("filter")
                            .unwrap_or(serde_json::json!({}));
                        println!(
                            "{} last_seq={} paused={} backoff_until={:?} filter={}",
                            name, last_seq, paused, backoff_until, filter,
                        );
                    } else {
                        println!("subscription '{}' not found", name);
                    }
                }
                SubscriptionsCmd::Pause { name } => {
                    sqlx::query(
                        "insert into subscriptions(name, paused) values($1,true) on conflict (name) do update set paused=true, updated_at=now()",
                    )
                    .bind(&name)
                    .execute(store.pool())
                    .await?;
                    println!("paused {}", name);
                }
                SubscriptionsCmd::Resume { name } => {
                    sqlx::query(
                        "update subscriptions set paused=false, backoff_until=null, updated_at=now() where name=$1",
                    )
                    .bind(&name)
                    .execute(store.pool())
                    .await?;
                    println!("resumed {}", name);
                }
                SubscriptionsCmd::Reset { name, seq } => {
                    sqlx::query(
                        "update subscriptions set last_seq=$2, updated_at=now() where name=$1",
                    )
                    .bind(&name)
                    .bind(seq)
                    .execute(store.pool())
                    .await?;
                    println!("reset {} to {}", name, seq);
                }
                SubscriptionsCmd::Tail { name, limit, group } => {
                    // load filter
                    let rec =
                        sqlx::query("select filter, last_seq from subscriptions where name=$1")
                            .bind(&name)
                            .fetch_one(store.pool())
                            .await?;
                    let filter: SubscriptionFilter = rec
                        .get::<Option<serde_json::Value>, _>("filter")
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default();
                    let last_seq: i64 = rec.get::<Option<i64>, _>("last_seq").unwrap_or(0);
                    let opts = SubscriptionOptions {
                        start_from: last_seq,
                        group,
                        ..Default::default()
                    };
                    let (_h, mut rx) = subs.subscribe(&name, filter, opts).await?;
                    let mut n = 0usize;
                    while let Some(env) = rx.recv().await {
                        println!("{} {} {}", env.stream_id, env.stream_seq, env.typ);
                        n += 1;
                        if n >= limit {
                            break;
                        }
                    }
                }
                SubscriptionsCmd::Groups { name } => {
                    let rows = sqlx::query(
                        "select grp, last_seq from subscription_groups where name=$1 order by grp",
                    )
                    .bind(&name)
                    .fetch_all(store.pool())
                    .await?;
                    for r in rows {
                        let grp: String = r.get("grp");
                        let last_seq: i64 = r.get::<Option<i64>, _>("last_seq").unwrap_or(0);
                        println!("{}:{} last_seq={}", name, grp, last_seq);
                    }
                }
                SubscriptionsCmd::GroupStatus { name, group } => {
                    let last_seq: i64 = sqlx::query_scalar(
                        "select last_seq from subscription_groups where name=$1 and grp=$2",
                    )
                    .bind(&name)
                    .bind(&group)
                    .fetch_optional(store.pool())
                    .await?
                    .unwrap_or(0);
                    let leased_by: Option<String> = sqlx::query_scalar(
                        "select leased_by from subscription_group_leases where name=$1 and grp=$2",
                    )
                    .bind(&name)
                    .bind(&group)
                    .fetch_optional(store.pool())
                    .await?;
                    let lease_until: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
                        "select lease_until from subscription_group_leases where name=$1 and grp=$2",
                    )
                    .bind(&name)
                    .bind(&group)
                    .fetch_optional(store.pool())
                    .await?;
                    println!(
                        "{}:{} last_seq={} leased_by={:?} lease_until={:?}",
                        name, group, last_seq, leased_by, lease_until
                    );
                }
            }
        }
        Commands::Streams(cmd) => match cmd {
            StreamsCmd::Resolve { alias } => {
                let id = store.resolve_stream_alias(&alias).await?;
                println!("{} -> {}", alias, id);
            }
        },
        Commands::Docs(cmd) => match cmd {
            DocsCmd::Get { id } => {
                let id = Uuid::parse_str(&id)?;
                let doc = store.docs().get::<JsonValue>(&id).await?;
                match doc {
                    Some((v, ver)) => println!("version={} doc={}", ver, v),
                    None => println!("not found"),
                }
            }
            DocsCmd::SoftDelete { id } => {
                let id = Uuid::parse_str(&id)?;
                sqlx::query("update docs set deleted_at = now() where id = $1")
                    .bind(id)
                    .execute(store.pool())
                    .await?;
                println!("soft-deleted {}", id);
            }
            DocsCmd::Restore { id } => {
                let id = Uuid::parse_str(&id)?;
                sqlx::query("update docs set deleted_at = null where id = $1")
                    .bind(id)
                    .execute(store.pool())
                    .await?;
                println!("restored {}", id);
            }
        },
        Commands::Snapshots(cmd) => match cmd {
            SnapshotsCmd::CompactOnce { threshold, batch } => {
                let n = compact_snapshots_once(store.pool(), &cli.schema, threshold, batch).await?;
                println!("compacted {} stream(s)", n);
            }
            SnapshotsCmd::RunUntilIdle { threshold, batch } => {
                let mut total = 0u64;
                loop {
                    let n =
                        compact_snapshots_once(store.pool(), &cli.schema, threshold, batch).await?;
                    total += n as u64;
                    if n == 0 {
                        break;
                    }
                }
                println!("compacted total {} stream(s)", total);
            }
            SnapshotsCmd::Metrics { threshold } => {
                let candidates: i64 = sqlx::query_scalar(
                    r#"
                     select count(1) from (
                       select e.stream_id
                         from events e
                         left join snapshots s on s.stream_id = e.stream_id
                        group by e.stream_id, s.version
                       having max(e.stream_seq) - coalesce(s.version, 0) >= $1
                     ) t
                     "#,
                )
                .bind(threshold)
                .fetch_one(store.pool())
                .await?;

                let max_gap: Option<i32> = sqlx::query_scalar(
                    r#"
                     select max(max_seq - coalesce(s.version, 0)) as gap from (
                       select e.stream_id, max(e.stream_seq) as max_seq
                         from events e
                        group by e.stream_id
                     ) h
                     left join snapshots s on s.stream_id = h.stream_id
                     "#,
                )
                .fetch_one(store.pool())
                .await?;

                println!(
                    "threshold={} candidates={} max_gap={}",
                    threshold,
                    candidates,
                    max_gap.unwrap_or(0).max(0)
                );
            }
        },
    }

    Ok(())
}

fn print_plan(plan: &rillflow::SchemaPlan) {
    if !plan.warnings().is_empty() {
        eprintln!("Warnings ({}):", plan.warnings().len());
        for w in plan.warnings() {
            eprintln!("  - {}", w);
        }
    }

    if plan.actions().is_empty() {
        println!("No pending DDL actions.");
        return;
    }

    println!("DDL actions ({}):", plan.actions().len());
    for (i, action) in plan.actions().iter().enumerate() {
        println!("{}. {}", i + 1, action.description());
        println!("{}\n", action.sql());
    }
}

async fn serve_health(
    addr: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> rillflow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind(&addr).await?;
    loop {
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let (mut sock, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let mut _buf = [0u8; 1024];
        let _ = sock.read(&_buf).await;
        let resp = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\ncontent-type: text/plain\r\n\r\nok";
        let _ = sock.write_all(resp).await;
    }
}

async fn compact_snapshots_once(
    pool: &sqlx::PgPool,
    schema: &str,
    threshold: i32,
    batch: i64,
) -> rillflow::Result<u32> {
    let set_search_path = format!("set local search_path to {}", quote_ident(schema));
    let mut tx = pool.begin().await?;
    sqlx::query(&set_search_path).execute(&mut *tx).await?;
    let rows: Vec<(uuid::Uuid, i32)> = sqlx::query_as(
        r#"
        select e.stream_id,
               max(e.stream_seq) as head
          from events e
          left join snapshots s on s.stream_id = e.stream_id
         group by e.stream_id, s.version
        having max(e.stream_seq) - coalesce(s.version, 0) >= $1
         limit $2
        "#,
    )
    .bind(threshold)
    .bind(batch)
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        tx.commit().await?;
        return Ok(0);
    }

    for (stream_id, head) in rows {
        sqlx::query(
            r#"insert into snapshots(stream_id, version, body)
                values ($1, $2, '{}'::jsonb)
                on conflict (stream_id) do update set version = excluded.version, body = excluded.body, created_at = now()"#,
        )
        .bind(stream_id)
        .bind(head)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(rows.len() as u32)
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
