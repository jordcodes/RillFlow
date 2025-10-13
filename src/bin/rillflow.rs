async fn list_tenants(store: &Store) -> rillflow::Result<()> {
    let rows = sqlx::query_scalar::<_, String>(
        "select schema_name from information_schema.schemata where schema_name like 'tenant_%' order by schema_name",
    )
    .fetch_all(store.pool())
    .await?;
    for s in rows {
        println!("{}", s);
    }
    Ok(())
}

async fn tenant_status(store: &Store, name: &str) -> rillflow::Result<()> {
    let schema = rillflow::store::tenant_schema_name(name);
    let exists: bool = sqlx::query_scalar(
        "select exists (select 1 from information_schema.schemata where schema_name = $1)",
    )
    .bind(&schema)
    .fetch_one(store.pool())
    .await?;
    if !exists {
        println!("tenant '{}' (schema {}) does not exist", name, schema);
        return Ok(());
    }

    println!("tenant '{}' (schema {}) exists", name, schema);

    let plan = store
        .schema()
        .plan(&SchemaConfig::with_base_schema(schema.clone()))
        .await?;
    if plan.is_empty() {
        println!("  schema is up to date");
    } else {
        println!("  pending DDL actions: {}", plan.actions().len());
    }
    Ok(())
}

async fn drop_tenant(store: &Store, name: &str) -> rillflow::Result<()> {
    store.drop_tenant(name).await
}

async fn archive_tenant(
    store: &Store,
    name: &str,
    output: &str,
    include_snapshots: bool,
) -> rillflow::Result<()> {
    if !store.tenant_exists(name).await? {
        println!("tenant '{}' does not exist, nothing to archive", name);
        return Ok(());
    }
    let schema = rillflow::store::tenant_schema_name(name);
    let docs_out = format!("{}/docs.json", output);
    export_table(store, &schema, "docs", &docs_out, include_snapshots).await?;
    let events_out = format!("{}/events.json", output);
    export_table(store, &schema, "events", &events_out, include_snapshots).await?;
    if include_snapshots {
        let snaps_out = format!("{}/snapshots.json", output);
        export_table(store, &schema, "snapshots", &snaps_out, true).await?;
    }
    Ok(())
}

async fn export_table(
    store: &Store,
    schema: &str,
    table: &str,
    path: &str,
    include_meta: bool,
) -> rillflow::Result<()> {
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    let qualified = format!(
        "{}.{}",
        rillflow::schema::quote_ident(schema),
        rillflow::schema::quote_ident(table)
    );
    let rows = sqlx::query(&format!("select row_to_json(t) from {} t", qualified))
        .fetch_all(store.pool())
        .await?;
    fs::create_dir_all(
        std::path::Path::new(path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new(".")),
    )
    .await?;
    let mut file = fs::File::create(path).await?;
    for row in rows {
        let value: serde_json::Value = row.get(0);
        let line = serde_json::to_string(&value)?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }

    if include_meta && table == "events" {
        let checkpoints = sqlx::query(&format!(
            "select row_to_json(t) from {}.projections t",
            rillflow::schema::quote_ident(schema)
        ))
        .fetch_all(store.pool())
        .await?;
        if !checkpoints.is_empty() {
            let path = format!(
                "{}/projections.json",
                std::path::Path::new(path).parent().unwrap().display()
            );
            let mut meta = fs::File::create(&path).await?;
            for row in checkpoints {
                let value: serde_json::Value = row.get(0);
                let line = serde_json::to_string(&value)?;
                meta.write_all(line.as_bytes()).await?;
                meta.write_all(b"\n").await?;
            }
        }
    }

    Ok(())
}
use clap::{ArgAction, Parser, Subcommand};
use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};
use rillflow::subscriptions::{SubscriptionFilter, SubscriptionOptions, Subscriptions};
use rillflow::{SchemaConfig, Store, TenancyMode, TenantSchema, TenantStrategy};
use serde_json::Value as JsonValue;
use sqlx::Row;
use std::sync::Arc;
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

    /// Slow query logging threshold in milliseconds (default 500)
    #[arg(long, default_value_t = 500)]
    slow_query_ms: u64,

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

    /// Tenants (schema-per-tenant) helpers
    #[command(subcommand)]
    Tenants(TenantsCmd),

    /// Validate tenant schemas are current
    #[command(subcommand)]
    Health(HealthCmd),
}

#[derive(Subcommand, Debug)]
enum ProjectionsCmd {
    /// List projections and their status
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
    },
    /// Show status of a single projection
    Status {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Pause a projection
    Pause {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Resume a projection
    Resume {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Reset checkpoint to a specific sequence
    ResetCheckpoint {
        name: String,
        seq: i64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Rebuild (reset to 0 and clear DLQ)
    Rebuild {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Run a single processing tick for one projection (by name) or all registered if omitted
    RunOnce {
        name: Option<String>,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
    },
    /// Run until idle (no projection has work)
    RunUntilIdle {
        name: Option<String>,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
    },
    /// Dead Letter Queue: list recent failures
    DlqList {
        name: String,
        #[arg(long, default_value_t = 50)]
        limit: i64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Dead Letter Queue: requeue one item by id (sets checkpoint to id's seq - 1)
    DlqRequeue {
        name: String,
        id: i64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Dead Letter Queue: delete one item by id
    DlqDelete {
        name: String,
        id: i64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Show basic metrics (lag, last_seq, dlq)
    Metrics {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Run long-lived projection loop
    RunLoop {
        /// Use LISTEN/NOTIFY to wake immediately on new events
        #[arg(long, default_value_t = true)]
        use_notify: bool,
        /// Optional health HTTP bind, e.g. 0.0.0.0:8080
        #[arg(long)]
        health_bind: Option<String>,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
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
        #[arg(long)]
        tenant: Option<String>,
    },
    /// List subscriptions and checkpoints
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
    },
    /// Show a subscription status
    Status {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Pause a subscription
    Pause {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Resume a subscription
    Resume {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Reset checkpoint to a specific sequence
    Reset {
        name: String,
        seq: i64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Tail a subscription (prints incoming events)
    Tail {
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Optional consumer group name
        #[arg(long)]
        group: Option<String>,
        /// Optional max in-flight items to bound per-batch delivery
        #[arg(long)]
        max_in_flight: Option<usize>,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Group admin: list groups for a subscription
    Groups {
        name: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Group admin: show a group status (checkpoint and lease)
    GroupStatus {
        name: String,
        group: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Set or unset max in-flight per group
    SetGroupMaxInFlight {
        name: String,
        group: String,
        value: Option<i32>,
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum StreamsCmd {
    /// Resolve or create a stream id for an alias
    Resolve { alias: String },
}

#[derive(Subcommand, Debug)]
enum DocsCmd {
    /// Get a document by id
    Get {
        id: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// EXPLAIN/ANALYZE an ad-hoc SQL (against docs table) for troubleshooting
    ExplainSql {
        /// Raw SQL to EXPLAIN (must be a SELECT)
        sql: String,
        /// Use EXPLAIN ANALYZE (executes query)
        #[arg(long, default_value_t = false)]
        analyze: bool,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Soft-delete a document (sets deleted_at)
    SoftDelete {
        id: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Restore a soft-deleted document (clears deleted_at)
    Restore {
        id: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Index advisor: prints suggested DDL for common patterns
    IndexAdvisor {
        /// Suggest GIN index on full doc (useful default)
        #[arg(long, default_value_t = false)]
        gin: bool,
        /// Suggest expression indexes for specific fields (repeatable)
        #[arg(long = "field", action = ArgAction::Append)]
        fields: Vec<String>,
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum SnapshotsCmd {
    /// Write snapshots for streams where head - snapshot.version >= threshold (version-only body)
    CompactOnce {
        #[arg(long, default_value_t = 100)]
        threshold: i32,
        #[arg(long, default_value_t = 100)]
        batch: i64,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
    },
    /// Repeatedly compact until no more streams exceed the threshold
    RunUntilIdle {
        #[arg(long, default_value_t = 100)]
        threshold: i32,
        #[arg(long, default_value_t = 100)]
        batch: i64,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
    },
    /// Show snapshotter metrics (candidate streams and max gap)
    Metrics {
        #[arg(long, default_value_t = 100)]
        threshold: i32,
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum TenantsCmd {
    /// Ensure (create + migrate) a tenant schema using Store::ensure_tenant
    Ensure { name: String },
    /// Sync (plan+apply) core tables for an existing tenant schema
    Sync { name: String },
    /// List known tenant schemas
    List,
    /// Show schema-level status for a tenant
    Status { name: String },
    /// Archive a tenant schema to disk (exports docs/events JSON)
    Archive {
        name: String,
        #[arg(long)]
        output: String,
        #[arg(long, default_value_t = false)]
        include_snapshots: bool,
    },
    /// Drop a tenant schema (dangerous)
    Drop {
        name: String,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum HealthCmd {
    /// Check schema drift for selected tenants
    Schema {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long, default_value_t = false)]
        all_tenants: bool,
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

    let mut builder = Store::builder(&url);
    if matches!(tenancy_mode, TenancyMode::SchemaPerTenant { .. }) {
        builder = builder.tenant_strategy(TenantStrategy::SchemaPerTenant);
    }
    let store = builder.build().await?;
    rillflow::metrics::set_slow_query_threshold(std::time::Duration::from_millis(
        cli.slow_query_ms,
    ));
    let mgr = store.schema();

    let tenant_helper = TenantHelper::new(
        store.tenant_strategy(),
        store.tenant_resolver().cloned(),
        store.tenant_strategy() == TenantStrategy::SchemaPerTenant,
    );

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
            let daemon = tenant_helper.projection_daemon(store.pool().clone());
            match cmd {
                ProjectionsCmd::List {
                    tenant,
                    all_tenants,
                } => {
                    let list = tenant_helper
                        .with_selection(tenant, all_tenants, |sel| async {
                            let mut rows = Vec::new();
                            for ctx in sel {
                                rows.extend(
                                    daemon
                                        .list(ctx.tenant_label.as_deref())
                                        .await?
                                        .into_iter()
                                        .map(move |mut s| {
                                            if let Some(label) = &ctx.tenant_label {
                                                s.name = format!("{}@{}", s.name, label);
                                            }
                                            Ok(s)
                                        })
                                        .collect::<Result<Vec<_>>>()?,
                                );
                            }
                            Ok(rows)
                        })
                        .await?;
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
                ProjectionsCmd::Status { name, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    let s = daemon.status(&name, ctx.tenant_label.as_deref()).await?;
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
                ProjectionsCmd::Pause { name, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    daemon.pause(&name, ctx.tenant_label.as_deref()).await?;
                    println!("paused {}", { name });
                }
                ProjectionsCmd::Resume { name, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    daemon.resume(&name, ctx.tenant_label.as_deref()).await?;
                    println!("resumed {}", { name });
                }
                ProjectionsCmd::ResetCheckpoint { name, seq, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    daemon
                        .reset_checkpoint(&name, seq, ctx.tenant_label.as_deref())
                        .await?;
                    println!("reset {} to {}", name, seq);
                }
                ProjectionsCmd::Rebuild { name, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    daemon.rebuild(&name, ctx.tenant_label.as_deref()).await?;
                    println!("rebuild scheduled for {}", name);
                }
                ProjectionsCmd::RunOnce {
                    name,
                    tenant,
                    all_tenants,
                } => {
                    tenant_helper
                        .with_selection(tenant, all_tenants, |sel| async {
                            if let Some(name) = name.clone() {
                                for ctx in sel {
                                    let res = daemon
                                        .tick_once(&name, ctx.tenant_label.as_deref())
                                        .await?;
                                    println!("{}{}: {:?}", name, ctx.tenant_suffix(), res);
                                }
                            } else {
                                for ctx in sel {
                                    daemon.tick_all_once(ctx.tenant_label.as_deref()).await?;
                                }
                                println!("tick-all executed");
                            }
                            Ok(())
                        })
                        .await?;
                }
                ProjectionsCmd::RunUntilIdle {
                    name,
                    tenant,
                    all_tenants,
                } => {
                    tenant_helper
                        .with_selection(tenant, all_tenants, |sel| async {
                            if let Some(n) = name.clone() {
                                for ctx in sel {
                                    loop {
                                        let res = daemon
                                            .tick_once(&n, ctx.tenant_label.as_deref())
                                            .await?;
                                        match res {
                                            rillflow::projection_runtime::TickResult::Processed {
                                                count,
                                            } if count > 0 => {}
                                            _ => break,
                                        }
                                    }
                                    println!("{}{}: idle", n, ctx.tenant_suffix());
                                }
                            } else {
                                for ctx in sel {
                                    daemon.run_until_idle(ctx.tenant_label.as_deref()).await?;
                                }
                                println!("all projections idle");
                            }
                            Ok(())
                        })
                        .await?;
                }
                ProjectionsCmd::DlqList {
                    name,
                    limit,
                    tenant,
                } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    let items = daemon
                        .dlq_list(&name, limit, ctx.tenant_label.as_deref())
                        .await?;
                    for i in items {
                        println!(
                            "id={} seq={} type={} failed_at={} error={}",
                            i.id, i.seq, i.event_type, i.failed_at, i.error
                        );
                    }
                }
                ProjectionsCmd::DlqRequeue { name, id, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    daemon
                        .dlq_requeue(&name, id, ctx.tenant_label.as_deref())
                        .await?;
                    println!("requeued {}:{}", name, id);
                }
                ProjectionsCmd::DlqDelete { name, id, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    daemon
                        .dlq_delete(&name, id, ctx.tenant_label.as_deref())
                        .await?;
                    println!("deleted {}:{}", name, id);
                }
                ProjectionsCmd::Metrics { name, tenant } => {
                    let (ctx, name) = tenant_helper.select_single(tenant.as_deref(), &name)?;
                    let m = daemon.metrics(&name, ctx.tenant_label.as_deref()).await?;
                    println!(
                        "{} last_seq={} head_seq={} lag={} dlq_count={}",
                        m.name, m.last_seq, m.head_seq, m.lag, m.dlq_count
                    );
                }
                ProjectionsCmd::RunLoop {
                    use_notify,
                    health_bind,
                    tenant,
                    all_tenants,
                } => {
                    tenant_helper
                        .with_selection(tenant, all_tenants, |sel| async move {
                            for ctx in sel {
                                let stop =
                                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                                let stop2 = stop.clone();
                                tokio::spawn(async move {
                                    let _ = tokio::signal::ctrl_c().await;
                                    stop2.store(true, std::sync::atomic::Ordering::Relaxed);
                                });

                                if let Some(addr) = &health_bind {
                                    let stop_health = stop.clone();
                                    let addr = addr.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) = serve_health(addr, stop_health).await {
                                            eprintln!("health server error: {err}");
                                        }
                                    });
                                }

                                daemon
                                    .run_loop(use_notify, stop, ctx.tenant_label.as_deref())
                                    .await?;
                            }
                            Ok(())
                        })
                        .await?;
                }
            }
        }
        Commands::Subscriptions(cmd) => {
            let subs = tenant_helper.subscriptions(store.pool().clone());
            match cmd {
                SubscriptionsCmd::Create {
                    name,
                    event_type,
                    stream_id,
                    start_from,
                    tenant,
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
                SubscriptionsCmd::List {
                    tenant,
                    all_tenants,
                } => {
                    tenant_helper
                        .with_selection(tenant, all_tenants, |sel| async {
                            for ctx in sel {
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
                                        "{}{} last_seq={} paused={} backoff_until={:?} filter={}",
                                        name,
                                        ctx.tenant_suffix(),
                                        last_seq,
                                        paused,
                                        backoff_until,
                                        filter,
                                    );
                                }
                            }
                            Ok(())
                        })
                        .await?;
                }
                SubscriptionsCmd::Status { name, tenant } => {
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
                SubscriptionsCmd::Pause { name, tenant } => {
                    sqlx::query(
                        "insert into subscriptions(name, paused) values($1,true) on conflict (name) do update set paused=true, updated_at=now()",
                    )
                    .bind(&name)
                    .execute(store.pool())
                    .await?;
                    println!("paused {}", name);
                }
                SubscriptionsCmd::Resume { name, tenant } => {
                    sqlx::query(
                        "update subscriptions set paused=false, backoff_until=null, updated_at=now() where name=$1",
                    )
                    .bind(&name)
                    .execute(store.pool())
                    .await?;
                    println!("resumed {}", name);
                }
                SubscriptionsCmd::Reset { name, seq, tenant } => {
                    sqlx::query(
                        "update subscriptions set last_seq=$2, updated_at=now() where name=$1",
                    )
                    .bind(&name)
                    .bind(seq)
                    .execute(store.pool())
                    .await?;
                    println!("reset {} to {}", name, seq);
                }
                SubscriptionsCmd::Tail {
                    name,
                    limit,
                    group,
                    max_in_flight,
                    tenant,
                } => {
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
                        max_in_flight: max_in_flight.unwrap_or(1024),
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
                SubscriptionsCmd::Groups { name, tenant } => {
                    let rows = sqlx::query(
                        "select g.grp, g.last_seq, coalesce(h.head, 0) as head
                           from subscription_groups g
                           left join (
                                select stream_id, max(global_seq) as head from events group by stream_id
                           ) h on true
                          where g.name=$1
                          order by g.grp",
                    )
                    .bind(&name)
                    .fetch_all(store.pool())
                    .await?;
                    for r in rows {
                        let grp: String = r.get("grp");
                        let last_seq: i64 = r.get::<Option<i64>, _>("last_seq").unwrap_or(0);
                        let head: i64 = r.get::<Option<i64>, _>("head").unwrap_or(0);
                        let lag = (head - last_seq).max(0);
                        println!(
                            "{}:{} last_seq={} head={} lag={}",
                            name, grp, last_seq, head, lag
                        );
                    }
                }
                SubscriptionsCmd::GroupStatus {
                    name,
                    group,
                    tenant,
                } => {
                    let last_seq: i64 = sqlx::query_scalar(
                        "select last_seq from subscription_groups where name=$1 and grp=$2",
                    )
                    .bind(&name)
                    .bind(&group)
                    .fetch_optional(store.pool())
                    .await?
                    .unwrap_or(0);
                    let head: i64 =
                        sqlx::query_scalar("select coalesce(max(global_seq), 0) from events")
                            .fetch_one(store.pool())
                            .await?;
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
                    let lag = (head - last_seq).max(0);
                    println!(
                        "{}:{} last_seq={} head={} lag={} leased_by={:?} lease_until={:?}",
                        name, group, last_seq, head, lag, leased_by, lease_until
                    );
                }
                SubscriptionsCmd::SetGroupMaxInFlight {
                    name,
                    group,
                    value,
                    tenant,
                } => {
                    if let Some(v) = value {
                        sqlx::query(
                            "insert into subscription_groups(name, grp, max_in_flight) values($1,$2,$3)
                             on conflict(name,grp) do update set max_in_flight = excluded.max_in_flight, updated_at = now()",
                        )
                        .bind(&name)
                        .bind(&group)
                        .bind(v)
                        .execute(store.pool())
                        .await?;
                        println!("{}:{} max_in_flight set to {}", name, group, v);
                    } else {
                        sqlx::query(
                            "update subscription_groups set max_in_flight = null, updated_at = now() where name=$1 and grp=$2",
                        )
                        .bind(&name)
                        .bind(&group)
                        .execute(store.pool())
                        .await?;
                        println!("{}:{} max_in_flight unset", name, group);
                    }
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
            DocsCmd::Get { id, tenant } => {
                let id = Uuid::parse_str(&id)?;
                let doc = store.docs().get::<JsonValue>(&id).await?;
                match doc {
                    Some((v, ver)) => println!("version={} doc={}", ver, v),
                    None => println!("not found"),
                }
            }
            DocsCmd::SoftDelete { id, tenant } => {
                let id = Uuid::parse_str(&id)?;
                sqlx::query("update docs set deleted_at = now() where id = $1")
                    .bind(id)
                    .execute(store.pool())
                    .await?;
                println!("soft-deleted {}", id);
            }
            DocsCmd::Restore { id, tenant } => {
                let id = Uuid::parse_str(&id)?;
                sqlx::query("update docs set deleted_at = null where id = $1")
                    .bind(id)
                    .execute(store.pool())
                    .await?;
                println!("restored {}", id);
            }
            DocsCmd::IndexAdvisor {
                gin,
                fields,
                tenant,
            } => {
                if gin {
                    println!(
                        "-- full-doc GIN (skip if already applied)\ncreate index if not exists docs_gin on {} using gin (doc);",
                        quote_ident(&cli.schema)
                    );
                }
                for f in fields {
                    let idx = format!("docs_{}_expr_idx", f.replace('"', "").to_lowercase());
                    println!(
                        "-- expression index for text field {}\ncreate index if not exists {} on {} ((lower(doc->>'{}')));",
                        f,
                        quote_ident(&idx),
                        quote_ident(&cli.schema),
                        f
                    );
                }
            }
            DocsCmd::ExplainSql {
                sql,
                analyze,
                tenant,
            } => {
                let mut conn = store.pool().acquire().await?;
                if let Some(t) = tenant.as_deref() {
                    let stmt = format!(
                        "set search_path to {}, public",
                        rillflow::schema::quote_ident(&rillflow::store::tenant_schema_name(t))
                    );
                    sqlx::query(&stmt).execute(&mut *conn).await?;
                }
                let explained = if analyze {
                    format!("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT) {}", sql)
                } else {
                    format!("EXPLAIN (VERBOSE, FORMAT TEXT) {}", sql)
                };
                let rows: Vec<String> =
                    sqlx::query_scalar(&explained).fetch_all(&mut *conn).await?;
                for line in rows {
                    println!("{}", line);
                }
            }
        },
        Commands::Snapshots(cmd) => match cmd {
            SnapshotsCmd::CompactOnce {
                threshold,
                batch,
                tenant,
                all_tenants,
            } => {
                let n = tenant_helper
                    .with_selection(tenant, all_tenants, |sel| async {
                        let mut total = 0u64;
                        for ctx in sel {
                            let schema = ctx.schema_or(&cli.schema);
                            total += compact_snapshots_once(store.pool(), &schema, threshold, batch)
                                .await? as u64;
                        }
                        Ok(total as u32)
                    })
                    .await?;
                println!("compacted {} stream(s)", n);
            }
            SnapshotsCmd::RunUntilIdle {
                threshold,
                batch,
                tenant,
                all_tenants,
            } => {
                tenant_helper
                    .with_selection(tenant, all_tenants, |sel| async {
                        for ctx in sel {
                            loop {
                                let schema = ctx.schema_or(&cli.schema);
                                let n =
                                    compact_snapshots_once(store.pool(), &schema, threshold, batch)
                                        .await?;
                                if n == 0 {
                                    break;
                                }
                            }
                        }
                        Ok(())
                    })
                    .await?;
                println!("compacted all streams to threshold");
            }
            SnapshotsCmd::Metrics { threshold, tenant } => {
                let schema = tenant
                    .as_ref()
                    .map(|t| tenant_helper.require_schema(t))
                    .transpose()?;
                let schema = schema.as_deref().unwrap_or(&cli.schema);
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
                    "schema={} threshold={} candidates={} max_gap={}",
                    schema,
                    threshold,
                    candidates,
                    max_gap.unwrap_or(0).max(0)
                );
            }
        },
        Commands::Tenants(cmd) => match cmd {
            TenantsCmd::Ensure { name } => {
                store.ensure_tenant(&name).await?;
                println!("tenant '{}' ensured", name);
            }
            TenantsCmd::Sync { name } => {
                let cfg = SchemaConfig::with_base_schema(name.clone());
                let plan = mgr.sync(&cfg).await?;
                if plan.is_empty() {
                    println!("No changes needed.");
                } else {
                    print_plan(&plan);
                }
            }
            TenantsCmd::List => {
                list_tenants(&store).await?;
            }
            TenantsCmd::Status { name } => {
                tenant_status(&store, &name).await?;
            }
            TenantsCmd::Archive {
                name,
                output,
                include_snapshots,
            } => {
                archive_tenant(&store, &name, &output, include_snapshots).await?;
                println!("tenant '{}' archived to {}", name, output);
            }
            TenantsCmd::Drop { name, force } => {
                if !force {
                    eprintln!(
                        "Refusing to drop tenant '{}'. Re-run with --force if you are absolutely sure.",
                        name
                    );
                    std::process::exit(3);
                }
                store.drop_tenant(&name).await?;
                println!("tenant '{}' dropped", name);
            }
        },
        Commands::Health(HealthCmd::Schema {
            tenant,
            all_tenants,
        }) => {
            tenant_helper
                .with_selection(tenant, all_tenants, |sel| async {
                    for ctx in sel {
                        let schema = ctx.schema_or(&cli.schema);
                        let config = SchemaConfig {
                            base_schema: schema.clone(),
                            tenancy_mode,
                        };
                        let plan = store.schema().plan(&config).await?;
                        if plan.is_empty() {
                            println!("schema '{}' up to date", schema);
                        } else {
                            eprintln!("schema '{}' drift detected:", schema);
                            print_plan(&plan);
                        }
                    }
                    Ok(())
                })
                .await?;
        }
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
        let n = sock.read(&_buf).await.unwrap_or(0);
        let req = std::str::from_utf8(&_buf[..n]).unwrap_or("");
        let (status, body) = if req.starts_with("GET /metrics ") {
            ("200 OK", rillflow::metrics::render_prometheus())
        } else {
            ("200 OK", "ok".to_string())
        };
        let headers = format!(
            "HTTP/1.1 {}\r\ncontent-length: {}\r\ncontent-type: text/plain\r\n\r\n",
            status,
            body.len()
        );
        let _ = sock.write_all(headers.as_bytes()).await;
        let _ = sock.write_all(body.as_bytes()).await;
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

#[derive(Clone)]
struct TenantHelper {
    strategy: TenantStrategy,
    resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    schema_per_tenant: bool,
}

#[derive(Clone)]
struct TenantContext {
    tenant_label: Option<String>,
}

impl TenantContext {
    fn tenant_suffix(&self) -> String {
        self.tenant_label
            .as_ref()
            .map(|t| format!("@{}", t))
            .unwrap_or_default()
    }

    fn schema_or<'a>(&self, default: &'a str) -> String {
        if let Some(label) = &self.tenant_label {
            tenant_schema_name(label)
        } else {
            default.to_string()
        }
    }
}

impl TenantHelper {
    fn new(
        strategy: TenantStrategy,
        resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
        schema_per_tenant: bool,
    ) -> Self {
        Self {
            strategy,
            resolver,
            schema_per_tenant,
        }
    }

    fn projection_daemon(&self, pool: PgPool) -> ProjectionDaemon {
        let mut config = ProjectionWorkerConfig::default();
        config.tenant_strategy = self.strategy;
        config.tenant_resolver = self.resolver.clone();
        ProjectionDaemon::new(pool, config)
    }

    fn subscriptions(&self, pool: PgPool) -> Subscriptions {
        let mut subs = Subscriptions::new(pool);
        subs.tenant_strategy = self.strategy;
        subs.tenant_resolver = self.resolver.clone();
        subs
    }

    fn select_single<'a>(
        &'a self,
        tenant: Option<&str>,
        name: &str,
    ) -> rillflow::Result<(TenantContext, String)> {
        let ctx = match (tenant, self.strategy) {
            (Some(t), TenantStrategy::SchemaPerTenant) => TenantContext {
                tenant_label: Some(t.to_string()),
            },
            (None, TenantStrategy::SchemaPerTenant) => TenantContext {
                tenant_label: self
                    .resolver
                    .as_ref()
                    .and_then(|r| (r)())
                    .ok_or(rillflow::Error::TenantRequired)?,
                tenant_label: None,
            },
            _ => TenantContext { tenant_label: None },
        };
        Ok((ctx, name.to_string()))
    }

    async fn with_selection<F, Fut, T>(
        &self,
        tenant: Option<String>,
        all_tenants: bool,
        mut f: F,
    ) -> rillflow::Result<T>
    where
        F: FnMut(Vec<TenantContext>) -> Fut,
        Fut: std::future::Future<Output = rillflow::Result<T>>,
    {
        let contexts = if all_tenants && self.schema_per_tenant {
            let tenants = self.list_tenants().await?;
            tenants
                .into_iter()
                .map(|t| TenantContext {
                    tenant_label: Some(t),
                })
                .collect()
        } else if let Some(t) = tenant {
            vec![TenantContext {
                tenant_label: Some(t),
            }]
        } else {
            vec![TenantContext { tenant_label: None }]
        };
        f(contexts).await
    }

    async fn list_tenants(&self) -> rillflow::Result<Vec<String>> {
        // For now rely on resolver; extension point for reading from DB later
        if let Some(resolver) = &self.resolver {
            if let Some(tenant) = resolver() {
                return Ok(vec![tenant]);
            }
        }
        Ok(vec![])
    }

    fn require_schema(&self, tenant: &str) -> rillflow::Result<String> {
        if !self.schema_per_tenant {
            return Ok(tenant.to_string());
        }
        Ok(tenant_schema_name(tenant))
    }
}
