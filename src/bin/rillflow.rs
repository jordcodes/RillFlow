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

#[derive(sqlx::FromRow)]
struct DaemonNodeStatusRow {
    cluster: String,
    daemon_id: Uuid,
    node_name: String,
    role: String,
    heartbeat_at: chrono::DateTime<chrono::Utc>,
    lease_until: chrono::DateTime<chrono::Utc>,
    lease_token: Option<Uuid>,
    min_cold_standbys: i32,
}

struct HotColdNodeStatus {
    daemon_id: Uuid,
    node_name: String,
    role: String,
    heartbeat_age: String,
    lease_remaining: String,
    lease_token: String,
    min_cold_standbys: i32,
}

struct HotColdClusterStatus {
    cluster: String,
    hot_nodes: u64,
    standby_nodes: u64,
    required_standbys: u32,
    nodes: Vec<HotColdNodeStatus>,
}

async fn hotcold_cluster_status(
    pool: &sqlx::PgPool,
    schema: &str,
    cluster: Option<&str>,
) -> rillflow::Result<Vec<HotColdClusterStatus>> {
    let mut conn = pool.acquire().await?;
    let set_search_path = format!("set search_path to {}", quote_ident(schema));
    sqlx::query(&set_search_path).execute(&mut *conn).await?;

    let rows: Vec<DaemonNodeStatusRow> = if let Some(cluster) = cluster {
        sqlx::query_as(
            "select cluster, daemon_id, node_name, role, heartbeat_at, lease_until, lease_token, min_cold_standbys
             from rf_daemon_nodes
             where cluster = $1
             order by role desc, lease_until desc, node_name",
        )
        .bind(cluster)
        .fetch_all(&mut *conn)
        .await?
    } else {
        sqlx::query_as(
            "select cluster, daemon_id, node_name, role, heartbeat_at, lease_until, lease_token, min_cold_standbys
             from rf_daemon_nodes
             order by cluster, role desc, lease_until desc, node_name",
        )
        .fetch_all(&mut *conn)
        .await?
    };

    let now = chrono::Utc::now();
    let mut grouped: BTreeMap<String, Vec<DaemonNodeStatusRow>> = BTreeMap::new();
    for row in rows {
        grouped.entry(row.cluster.clone()).or_default().push(row);
    }

    let mut clusters = Vec::new();
    for (cluster_name, nodes) in grouped {
        let hot_nodes = nodes.iter().filter(|n| n.role == "hot").count() as u64;
        let total_nodes = nodes.len() as u64;
        let standby_nodes = total_nodes.saturating_sub(hot_nodes);
        let required_standbys = nodes
            .iter()
            .map(|n| n.min_cold_standbys.max(0) as u32)
            .max()
            .unwrap_or(0);

        let node_statuses = nodes
            .into_iter()
            .map(|row| HotColdNodeStatus {
                daemon_id: row.daemon_id,
                node_name: row.node_name,
                role: row.role,
                heartbeat_age: format_duration(now.signed_duration_since(row.heartbeat_at)),
                lease_remaining: format_duration(row.lease_until.signed_duration_since(now)),
                lease_token: row
                    .lease_token
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                min_cold_standbys: row.min_cold_standbys,
            })
            .collect();

        clusters.push(HotColdClusterStatus {
            cluster: cluster_name,
            hot_nodes,
            standby_nodes,
            required_standbys,
            nodes: node_statuses,
        });
    }

    Ok(clusters)
}

fn format_duration(duration: chrono::Duration) -> String {
    let millis = duration.num_milliseconds();
    let sign = if millis < 0 { "-" } else { "" };
    let abs_millis = millis.abs();
    if abs_millis >= 60_000 {
        let minutes = abs_millis / 60_000;
        let seconds = (abs_millis % 60_000) / 1000;
        format!("{}{}m{}s", sign, minutes, seconds)
    } else if abs_millis >= 1_000 {
        let secs = abs_millis as f64 / 1_000.0;
        format!("{}{:0.3}s", sign, secs)
    } else {
        format!("{}{}ms", sign, abs_millis)
    }
}

fn promotion_channel_name(cluster: &str) -> String {
    let mut sanitized = String::with_capacity(cluster.len());
    for ch in cluster.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else if ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        sanitized.push_str("cluster");
    }
    let mut channel = format!("rillflow_daemon_{}", sanitized);
    if channel.len() > 63 {
        channel.truncate(63);
    }
    channel
}

async fn promote_daemon_node(
    pool: &sqlx::PgPool,
    schema: &str,
    cluster: &str,
    daemon_id: &Uuid,
) -> rillflow::Result<()> {
    let mut conn = pool.acquire().await?;
    let set_search_path = format!("set search_path to {}", quote_ident(schema));
    sqlx::query(&set_search_path).execute(&mut *conn).await?;

    let mut tx = conn.begin().await?;
    sqlx::query(
        "update rf_daemon_nodes set role = 'cold', lease_token = null, updated_at = now() where cluster = $1 and role = 'hot'",
    )
    .bind(cluster)
    .execute(&mut *tx)
    .await?;

    let now = chrono::Utc::now();
    let lease_until = now + chrono::Duration::seconds(30);
    let lease_token = Uuid::new_v4();
    let rows = sqlx::query(
        "update rf_daemon_nodes set role = 'hot', lease_token = $3, heartbeat_at = $4, lease_until = $5, updated_at = now() where cluster = $1 and daemon_id = $2",
    )
    .bind(cluster)
    .bind(daemon_id)
    .bind(lease_token)
    .bind(now)
    .bind(lease_until)
    .execute(&mut *tx)
    .await?;

    if rows.rows_affected() == 0 {
        tx.rollback().await?;
        anyhow::bail!("no matching node found to promote");
    }

    let channel = promotion_channel_name(cluster);
    sqlx::query("select pg_notify($1, $2)")
        .bind(&channel)
        .bind(format!("promoted:{}", daemon_id))
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

async fn demote_daemon_node(
    pool: &sqlx::PgPool,
    schema: &str,
    cluster: &str,
    daemon_id: &Uuid,
) -> rillflow::Result<()> {
    let mut conn = pool.acquire().await?;
    let set_search_path = format!("set search_path to {}", quote_ident(schema));
    sqlx::query(&set_search_path).execute(&mut *conn).await?;

    let mut tx = conn.begin().await?;
    let rows = sqlx::query(
        "update rf_daemon_nodes set role = 'cold', lease_token = null, updated_at = now() where cluster = $1 and daemon_id = $2",
    )
    .bind(cluster)
    .bind(daemon_id)
    .execute(&mut *tx)
    .await?;

    if rows.rows_affected() == 0 {
        tx.rollback().await?;
        anyhow::bail!("node is not registered in cluster");
    }

    let channel = promotion_channel_name(cluster);
    sqlx::query("select pg_notify($1, $2)")
        .bind(&channel)
        .bind(format!("demoted:{}", daemon_id))
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

async fn list_upcasters(registry: Option<Arc<UpcasterRegistry>>) -> Result<()> {
    if let Some(registry) = registry {
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for entry in registry.describe() {
            grouped
                .entry(entry.from_type.clone())
                .or_default()
                .push(format!(
                    "v{} -> {} v{} ({})",
                    entry.from_version, entry.to_type, entry.to_version, entry.kind
                ));
        }

        if grouped.is_empty() {
            println!("No upcasters registered");
        } else {
            for (typ, transitions) in grouped {
                println!("{}", typ);
                for line in transitions {
                    println!("  {}", line);
                }
            }
        }
    } else {
        println!("No upcaster registry configured for this store");
    }
    Ok(())
}

async fn print_upcaster_path(
    registry: Option<Arc<UpcasterRegistry>>,
    from: (&str, i32),
    to: (&str, i32),
) -> Result<()> {
    if let Some(registry) = registry {
        match registry.find_path(from, to) {
            Some(path) => {
                println!("Transformation path:");
                for (typ, version) in path {
                    println!("  {} v{}", typ, version);
                }
            }
            None => println!("No transformation path found"),
        }
    } else {
        println!("No upcaster registry configured for this store");
    }
    Ok(())
}
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use rillflow::projection_runtime::{
    DaemonClusterConfig, HotColdConfig, ProjectionDaemon, ProjectionWorkerConfig,
};
use rillflow::subscriptions::{SubscriptionFilter, SubscriptionOptions, Subscriptions};
use rillflow::{
    SchemaConfig, Store, TenancyMode, TenantSchema, TenantStrategy,
    events::ArchiveBackend,
    metrics::{
        record_archive_mark, record_archive_move, record_archive_moved, record_archive_reactivate,
        set_archive_hot_bytes, set_slow_query_explain, set_slow_query_threshold,
    },
    upcasting::UpcasterRegistry,
};
use serde_json::Value as JsonValue;
use sqlx::{Executor, PgPool, Postgres, QueryBuilder, Row};
use std::time::Instant;
use std::{collections::BTreeMap, sync::Arc};
use tracing::info;
use tracing::info;
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
    /// Also capture EXPLAIN (FORMAT TEXT) for slow queries
    #[arg(long, default_value_t = false)]
    slow_query_explain: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show planned DDL changes without applying
    SchemaPlan,

    /// Apply DDL changes (create schemas/tables/indexes as needed)
    SchemaSync,

    /// Roll back core schema (apply sql/0001_init.down.sql) for the selected schema
    SchemaDown {
        /// Required to execute destructive rollback
        #[arg(long)]
        force: bool,
    },

    /// Projection admin commands
    #[command(subcommand)]
    Projections(ProjectionsCmd),

    /// Upcasting helpers
    #[command(subcommand)]
    Upcasters(UpcastersCmd),

    /// Subscriptions admin commands
    #[command(subcommand)]
    Subscriptions(SubscriptionsCmd),

    /// Stream alias helpers
    #[command(subcommand)]
    Streams(StreamsCmd),

    /// Archive lifecycle (hot/cold) helpers
    #[command(subcommand)]
    Archive(ArchiveCmd),

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
    /// Show hot/cold daemon cluster membership
    ClusterStatus {
        /// Optional cluster name filter
        #[arg(long)]
        cluster: Option<String>,
    },
    /// Force a cold node to promote to hot (best-effort)
    Promote { cluster: String, node: String },
    /// Demote the current hot node (best-effort)
    Demote {
        cluster: String,
        node: Option<String>,
    },
    /// Run long-lived projection loop
    RunLoop {
        /// Use LISTEN/NOTIFY to wake immediately on new events
        #[arg(long, default_value_t = true)]
        use_notify: bool,
        /// Cluster coordination mode (`single` or `hotcold`)
        #[arg(long, value_enum, default_value_t = DaemonClusterModeArg::Single)]
        cluster_mode: DaemonClusterModeArg,
        /// Cluster name when using hot/cold mode
        #[arg(long)]
        cluster_name: Option<String>,
        /// Heartbeat interval in seconds for hot/cold mode
        #[arg(long, default_value_t = 5)]
        heartbeat_secs: u64,
        /// Lease TTL in seconds for hot/cold mode
        #[arg(long, default_value_t = 30)]
        lease_ttl_secs: u64,
        /// Additional grace period (seconds) before declaring the leader dead
        #[arg(long, default_value_t = 5)]
        lease_grace_secs: u64,
        /// Minimum number of cold standbys to expect in the cluster
        #[arg(long, default_value_t = 1)]
        min_cold_standbys: u32,
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
enum UpcastersCmd {
    /// List all registered upcasters grouped by source type
    List,
    /// Print the transformation path from one type/version to another
    Path {
        #[arg(long)]
        from_type: String,
        #[arg(long)]
        from_version: i32,
        #[arg(long)]
        to_type: String,
        #[arg(long)]
        to_version: i32,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ArchiveRetention {
    Hot,
    Cold,
}

impl ArchiveRetention {
    fn as_str(&self) -> &'static str {
        match self {
            ArchiveRetention::Hot => "hot",
            ArchiveRetention::Cold => "cold",
        }
    }
}

#[derive(Subcommand, Debug)]
enum ArchiveCmd {
    /// Show archive status (stream counts, table sizes)
    Status {
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Mark a stream as archived
    Mark {
        stream: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        by: Option<String>,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Reactivate a stream (allow hot writes)
    Reactivate {
        stream: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Move eligible events from hot to cold storage
    Move {
        #[arg(long)]
        before: chrono::DateTime<chrono::Utc>,
        #[arg(long, default_value_t = 100)]
        batch_size: i64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        stream: Option<String>,
    },
    /// Toggle redirect trigger (enable/disable hot->cold routing)
    Redirect {
        #[arg(long)]
        enable: Option<bool>,
    },
    /// Create a monthly partition (partitioned backend only)
    Partition {
        #[arg(long, value_enum, default_value_t = ArchiveRetention::Hot)]
        retention: ArchiveRetention,
        #[arg(long, value_parser = clap::value_parser!(NaiveDate))]
        month: NaiveDate,
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum DocsCmd {
    /// Get a document by id
    Get {
        id: String,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Verify required indexes exist and print missing index warnings
    Verify {
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
    /// Duplicated fields management commands
    #[command(subcommand)]
    DuplicatedFields(DuplicatedFieldsCmd),
}

#[derive(Subcommand, Debug)]
enum DuplicatedFieldsCmd {
    /// List currently configured duplicated fields
    List {
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Suggest adding a duplicated field (prints DDL preview)
    Suggest {
        /// JSONB path (e.g., "email", "profile.age")
        field: String,
        /// Column name (e.g., "d_email")
        #[arg(long)]
        column: String,
        /// PostgreSQL type (text, integer, bigint, numeric, boolean, timestamptz, uuid, jsonb)
        #[arg(long)]
        field_type: String,
        /// Whether to create an index on this column
        #[arg(long, default_value_t = true)]
        indexed: bool,
        /// Index type (btree, hash, gin, gist)
        #[arg(long, default_value = "btree")]
        index_type: String,
        /// Transform expression (e.g., "lower({value})")
        #[arg(long)]
        transform: Option<String>,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Backfill existing data into duplicated columns
    Backfill {
        /// Column name to backfill (e.g., "d_email")
        column: String,
        /// Batch size for updates
        #[arg(long, default_value_t = 1000)]
        batch_size: i64,
        /// Dry run (show what would be done)
        #[arg(long, default_value_t = false)]
        dry_run: bool,
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum DaemonClusterModeArg {
    Single,
    Hotcold,
}

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    // Initialize OTEL if configured
    let _ = rillflow::tracing::init_otlp_from_env();
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
        duplicated_fields: Vec::new(),
    };

    let mut builder = Store::builder(&url);
    if matches!(tenancy_mode, TenancyMode::SchemaPerTenant { .. }) {
        builder = builder.tenant_strategy(TenantStrategy::SchemaPerTenant);
    }
    let store = builder.build().await?;
    set_slow_query_threshold(std::time::Duration::from_millis(cli.slow_query_ms));
    set_slow_query_explain(cli.slow_query_explain);
    let mgr = store.schema();

    let store_tenant_strategy = store.tenant_strategy();
    let tenant_helper = TenantHelper::new(
        store_tenant_strategy.clone(),
        store.tenant_resolver().cloned(),
        matches!(
            store_tenant_strategy,
            TenantStrategy::SchemaPerTenant | TenantStrategy::Conjoined { .. }
        ),
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
        Commands::SchemaDown { force } => {
            if !force {
                eprintln!(
                    "This will DROP objects in schema '{}'. Re-run with --force to proceed.",
                    &config.base_schema
                );
                std::process::exit(3);
            }
            let path = "sql/0001_init.down.sql";
            let ddl = std::fs::read_to_string(path).map_err(|e| rillflow::Error::Context {
                context: format!("failed to read {}", path),
                source: Box::new(e.into()),
            })?;
            let mut tx = store.pool().begin().await?;
            let set_search_path = format!(
                "set local search_path to {}",
                quote_ident(&config.base_schema)
            );
            sqlx::query(&set_search_path).execute(&mut *tx).await?;
            for stmt in split_sql_dollar_safe(&ddl) {
                sqlx::query(stmt).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            println!(
                "Rolled back core schema objects in schema '{}'.",
                &config.base_schema
            );
        }
        Commands::Projections(cmd) => {
            let daemon =
                tenant_helper.projection_daemon(store.pool().clone(), store.upcaster_registry());
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
                ProjectionsCmd::ClusterStatus { cluster } => {
                    let clusters = hotcold_cluster_status(
                        store.pool(),
                        &config.base_schema,
                        cluster.as_deref(),
                    )
                    .await?;
                    if clusters.is_empty() {
                        let suffix = cluster
                            .as_ref()
                            .map(|c| format!(" for cluster '{}'", c))
                            .unwrap_or_default();
                        println!("no daemon nodes registered{}", suffix);
                    } else {
                        for cs in clusters {
                            println!(
                                "cluster={} hot_nodes={} standby_nodes={} required_standbys={}",
                                cs.cluster, cs.hot_nodes, cs.standby_nodes, cs.required_standbys
                            );
                            for node in cs.nodes {
                                println!(
                                    "  node={} id={} role={} heartbeat_age={} lease_remaining={} token={} min_cold_standbys={}",
                                    node.node_name,
                                    node.daemon_id,
                                    node.role,
                                    node.heartbeat_age,
                                    node.lease_remaining,
                                    node.lease_token,
                                    node.min_cold_standbys
                                );
                            }
                        }
                    }
                }
                ProjectionsCmd::Promote { cluster, node } => {
                    let clusters =
                        hotcold_cluster_status(store.pool(), &config.base_schema, Some(&cluster))
                            .await?;
                    let target = clusters
                        .into_iter()
                        .flat_map(|cs| cs.nodes.into_iter().map(move |n| (cs.cluster.clone(), n)))
                        .find(|(_, n)| n.node_name == node)
                        .ok_or_else(|| {
                            anyhow::anyhow!("node '{}' not found in cluster '{}'", node, cluster)
                        })?;
                    promote_daemon_node(
                        store.pool(),
                        &config.base_schema,
                        &target.0,
                        &target.1.daemon_id,
                    )
                    .await?;
                    println!("promotion requested for {} in cluster {}", node, target.0);
                }
                ProjectionsCmd::Demote { cluster, node } => {
                    let clusters =
                        hotcold_cluster_status(store.pool(), &config.base_schema, Some(&cluster))
                            .await?;
                    let target = if let Some(node_name) = node {
                        clusters
                            .into_iter()
                            .flat_map(|cs| {
                                cs.nodes.into_iter().map(move |n| (cs.cluster.clone(), n))
                            })
                            .find(|(_, n)| n.node_name == node_name)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "node '{}' not found in cluster '{}'",
                                    node_name,
                                    cluster
                                )
                            })?
                    } else {
                        clusters
                            .into_iter()
                            .flat_map(|cs| {
                                cs.nodes.into_iter().map(move |n| (cs.cluster.clone(), n))
                            })
                            .find(|(_, n)| n.role == "hot")
                            .ok_or_else(|| {
                                anyhow::anyhow!("no hot node found in cluster '{}'", cluster)
                            })?
                    };
                    demote_daemon_node(
                        store.pool(),
                        &config.base_schema,
                        &target.0,
                        &target.1.daemon_id,
                    )
                    .await?;
                    println!(
                        "demotion requested for {} in cluster {}",
                        target.1.node_name, target.0
                    );
                }
                ProjectionsCmd::RunLoop {
                    use_notify,
                    cluster_mode,
                    cluster_name,
                    heartbeat_secs,
                    lease_ttl_secs,
                    lease_grace_secs,
                    min_cold_standbys,
                    health_bind,
                    tenant,
                    all_tenants,
                } => {
                    let lease_ttl =
                        std::time::Duration::from_secs(std::cmp::max(lease_ttl_secs, 1));
                    let cluster_config = match cluster_mode {
                        DaemonClusterModeArg::Single => DaemonClusterConfig::Single,
                        DaemonClusterModeArg::Hotcold => {
                            let name = cluster_name.clone().unwrap_or_else(|| {
                                eprintln!(
                                    "error: --cluster-name is required when --cluster-mode=hotcold"
                                );
                                std::process::exit(2);
                            });
                            let heartbeat =
                                std::time::Duration::from_secs(std::cmp::max(heartbeat_secs, 1));
                            let lease_grace = std::time::Duration::from_secs(lease_grace_secs);
                            DaemonClusterConfig::HotCold(HotColdConfig::new(
                                name,
                                heartbeat,
                                lease_ttl,
                                lease_grace,
                                std::cmp::max(min_cold_standbys, 1),
                            ))
                        }
                    };

                    if let DaemonClusterConfig::HotCold(ref cfg) = cluster_config {
                        if cfg.lease_ttl <= cfg.heartbeat_interval {
                            eprintln!(
                                "warning: lease-ttl-secs ({}) should be greater than heartbeat-secs ({})",
                                lease_ttl_secs, heartbeat_secs
                            );
                        }
                    }

                    let helper = tenant_helper.clone();
                    let pool = store.pool().clone();
                    let upcasters = store.upcaster_registry();
                    let node_name_arg = node_name.clone();
                    let daemon_id_arg = daemon_id;
                    tenant_helper
                        .with_selection(tenant, all_tenants, move |sel| {
                            let helper = helper.clone();
                            let pool = pool.clone();
                            let upcasters = upcasters.clone();
                            let cluster_config = cluster_config.clone();
                            let health_bind = health_bind.clone();
                            let node_name_arg = node_name_arg.clone();
                            async move {
                                for ctx in sel {
                                    let mut config = ProjectionWorkerConfig::default();
                                    config.cluster = cluster_config.clone();
                                    config.lease_ttl = lease_ttl;
                                    if let Some(name) = node_name_arg.clone() {
                                        config.node_name = name;
                                    }
                                    if let Some(id) = daemon_id_arg {
                                        config.daemon_id = Some(id);
                                    }
                                    let daemon = helper.projection_daemon_with_config(
                                        pool.clone(),
                                        upcasters.clone(),
                                        config,
                                    );

                                    let stop = std::sync::Arc::new(
                                        std::sync::atomic::AtomicBool::new(false),
                                    );
                                    let stop2 = stop.clone();
                                    tokio::spawn(async move {
                                        let _ = tokio::signal::ctrl_c().await;
                                        stop2.store(true, std::sync::atomic::Ordering::Relaxed);
                                    });

                                    if let Some(addr) = &health_bind {
                                        let stop_health = stop.clone();
                                        let addr = addr.clone();
                                        tokio::spawn(async move {
                                            if let Err(err) = serve_health(addr, stop_health).await
                                            {
                                                eprintln!("health server error: {err}");
                                            }
                                        });
                                    }

                                    daemon
                                        .run_loop(use_notify, stop, ctx.tenant_label.as_deref())
                                        .await?;
                                }
                                Ok(())
                            }
                        })
                        .await?;
                }
            }
        }
        Commands::Upcasters(cmd) => match cmd {
            UpcastersCmd::List => upcasters::list(store.upcaster_registry()).await?,
            UpcastersCmd::Path {
                from_type,
                from_version,
                to_type,
                to_version,
            } => {
                upcasters::path(
                    store.upcaster_registry(),
                    (&from_type, from_version),
                    (&to_type, to_version),
                )
                .await?;
            }
        },
        Commands::Subscriptions(cmd) => {
            let subs = tenant_helper.subscriptions(
                store.pool().clone(),
                store.archive_backend(),
                store.include_archived_by_default(),
            );
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
                    let global_head = store.events().head_sequence().await?;
                    let rows = sqlx::query(
                        "select grp, last_seq, max_in_flight from subscription_groups where name=$1 order by grp",
                    )
                    .bind(&name)
                    .fetch_all(store.pool())
                    .await?;
                    for r in rows {
                        let grp: String = r.get("grp");
                        let last_seq: i64 = r.get::<Option<i64>, _>("last_seq").unwrap_or(0);
                        let lag = (global_head - last_seq).max(0);
                        println!(
                            "{}:{} last_seq={} head={} lag={}",
                            name, grp, last_seq, global_head, lag
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
                    let head: i64 = store.events().head_sequence().await?;
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
        Commands::Archive(cmd) => match cmd {
            ArchiveCmd::Status { tenant } => {
                tenant_helper
                    .with_selection(tenant, false, |sel| async {
                        for ctx in sel {
                            let schema = ctx.schema_or(&cli.schema);
                            archive_status(&store, &schema, ctx.tenant_label.as_deref()).await?;
                        }
                        Ok(())
                    })
                    .await?;
            }
            ArchiveCmd::Mark {
                stream,
                reason,
                by,
                tenant,
            } => {
                let (ctx, _) = tenant_helper.select_single(tenant.as_deref(), &stream)?;
                let schema = ctx.schema_or(&cli.schema);
                archive_mark(
                    &store,
                    &schema,
                    ctx.tenant_label.as_deref(),
                    &stream,
                    reason,
                    by,
                )
                .await?;
            }
            ArchiveCmd::Reactivate { stream, tenant } => {
                let (ctx, _) = tenant_helper.select_single(tenant.as_deref(), &stream)?;
                let schema = ctx.schema_or(&cli.schema);
                archive_reactivate(&store, &schema, ctx.tenant_label.as_deref(), &stream).await?;
            }
            ArchiveCmd::Move {
                before,
                batch_size,
                dry_run,
                tenant,
                stream,
            } => {
                if let Some(stream) = stream.clone() {
                    let (ctx, _) = tenant_helper.select_single(tenant.as_deref(), &stream)?;
                    let schema = ctx.schema_or(&cli.schema);
                    archive_move(
                        &store,
                        &schema,
                        ctx.tenant_label.as_deref(),
                        Some(&stream),
                        before,
                        batch_size,
                        dry_run,
                    )
                    .await?;
                } else {
                    tenant_helper
                        .with_selection(tenant, false, |sel| async {
                            for ctx in sel {
                                let schema = ctx.schema_or(&cli.schema);
                                archive_move(
                                    &store,
                                    &schema,
                                    ctx.tenant_label.as_deref(),
                                    None,
                                    before,
                                    batch_size,
                                    dry_run,
                                )
                                .await?;
                            }
                            Ok(())
                        })
                        .await?;
                }
            }
            ArchiveCmd::Partition {
                retention,
                month,
                tenant,
            } => {
                tenant_helper
                    .with_selection(tenant, false, |sel| async {
                        for ctx in sel {
                            let schema = ctx.schema_or(&cli.schema);
                            archive_partition_create(&store, &schema, retention, month).await?;
                        }
                        Ok(())
                    })
                    .await?;
            }
            ArchiveCmd::Redirect { enable } => {
                if let Some(flag) = enable {
                    store.set_archive_redirect(flag).await?;
                    println!(
                        "archive redirect {}",
                        if flag { "enabled" } else { "disabled" }
                    );
                } else {
                    let current = store.archive_redirect_enabled().await?;
                    println!(
                        "archive redirect is currently {}",
                        if current { "enabled" } else { "disabled" }
                    );
                }
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
            DocsCmd::DuplicatedFields(dup_cmd) => match dup_cmd {
                DuplicatedFieldsCmd::List { tenant } => {
                    use rillflow::schema::quote_ident;
                    let schema = tenant
                        .as_deref()
                        .map(|t| rillflow::store::tenant_schema_name(t))
                        .unwrap_or_else(|| "public".to_string());

                    // Query information_schema to find columns that look like duplicated fields
                    let rows: Vec<(String, String, String)> = sqlx::query_as(
                        "SELECT column_name, data_type, is_nullable
                         FROM information_schema.columns
                         WHERE table_schema = $1
                           AND table_name = 'docs'
                           AND column_name LIKE 'd_%'
                         ORDER BY ordinal_position",
                    )
                    .bind(&schema)
                    .fetch_all(store.pool())
                    .await?;

                    if rows.is_empty() {
                        println!("No duplicated fields found in schema '{}'", schema);
                        println!(
                            "\nTo add duplicated fields, use the 'suggest' command or configure them in code:"
                        );
                        println!(
                            "  rillflow docs duplicated-fields suggest <field> --column <name> --field-type <type>"
                        );
                    } else {
                        println!("Duplicated fields in schema '{}':", schema);
                        println!("{:<20} {:<15} {:<10}", "Column", "Type", "Nullable");
                        println!("{}", "-".repeat(50));
                        for (col, typ, nullable) in rows {
                            println!("{:<20} {:<15} {:<10}", col, typ, nullable);
                        }
                    }
                }
                DuplicatedFieldsCmd::Suggest {
                    field,
                    column,
                    field_type,
                    indexed,
                    index_type,
                    transform,
                    tenant,
                } => {
                    use rillflow::schema::{DuplicatedField, DuplicatedFieldType, IndexType};

                    let schema = tenant
                        .as_deref()
                        .map(|t| rillflow::store::tenant_schema_name(t))
                        .unwrap_or_else(|| "public".to_string());

                    // Parse field type
                    let pg_type = match field_type.to_lowercase().as_str() {
                        "text" => DuplicatedFieldType::Text,
                        "integer" | "int" => DuplicatedFieldType::Integer,
                        "bigint" => DuplicatedFieldType::BigInt,
                        "numeric" | "decimal" => DuplicatedFieldType::Numeric,
                        "boolean" | "bool" => DuplicatedFieldType::Boolean,
                        "timestamptz" | "timestamp" => DuplicatedFieldType::Timestamptz,
                        "uuid" => DuplicatedFieldType::Uuid,
                        "jsonb" => DuplicatedFieldType::Jsonb,
                        _ => {
                            eprintln!("Error: Unknown field type '{}'", field_type);
                            eprintln!(
                                "Supported types: text, integer, bigint, numeric, boolean, timestamptz, uuid, jsonb"
                            );
                            return Ok(());
                        }
                    };

                    let idx_type = match index_type.to_lowercase().as_str() {
                        "btree" => IndexType::BTree,
                        "hash" => IndexType::Hash,
                        "gin" => IndexType::Gin,
                        "gist" => IndexType::Gist,
                        _ => {
                            eprintln!("Error: Unknown index type '{}'", index_type);
                            eprintln!("Supported types: btree, hash, gin, gist");
                            return Ok(());
                        }
                    };

                    let mut dup_field =
                        DuplicatedField::new(field.clone(), column.clone(), pg_type)
                            .with_indexed(*indexed)
                            .with_index_type(idx_type);

                    if let Some(ref t) = transform {
                        dup_field = dup_field.with_transform(t.clone());
                    }

                    println!(
                        "Suggested configuration for duplicated field '{}' -> '{}':",
                        field, column
                    );
                    println!("\n// Rust code:");
                    println!(
                        "use rillflow::schema::{{DuplicatedField, DuplicatedFieldType, IndexType, SchemaConfig}};"
                    );
                    println!();
                    println!("let config = SchemaConfig::single_tenant()");
                    print!(
                        "    .add_duplicated_field(\n        DuplicatedField::new(\"{}\", \"{}\", DuplicatedFieldType::{:?})",
                        field, column, pg_type
                    );
                    if *indexed {
                        print!("\n            .with_indexed(true)");
                        if idx_type != IndexType::BTree {
                            print!("\n            .with_index_type(IndexType::{:?})", idx_type);
                        }
                    }
                    if let Some(ref t) = transform {
                        print!("\n            .with_transform(\"{}\")", t);
                    }
                    println!("\n    );");
                    println!();
                    println!("store.schema().sync(&config).await?;");
                    println!("\n// Extraction SQL that will be used:");
                    println!("{}", dup_field.extraction_sql());
                }
                DuplicatedFieldsCmd::Backfill {
                    column,
                    batch_size,
                    dry_run,
                    tenant,
                } => {
                    use rillflow::schema::quote_ident;
                    let schema = tenant
                        .as_deref()
                        .map(|t| rillflow::store::tenant_schema_name(t))
                        .unwrap_or_else(|| "public".to_string());

                    // Check if column exists
                    let exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = $1 AND table_name = 'docs' AND column_name = $2
                        )",
                    )
                    .bind(&schema)
                    .bind(&column)
                    .fetch_one(store.pool())
                    .await?;

                    if !exists {
                        eprintln!(
                            "Error: Column '{}' does not exist in {}.docs",
                            column, schema
                        );
                        eprintln!(
                            "Add the duplicated field to your SchemaConfig and run schema-sync first."
                        );
                        return Ok(());
                    }

                    // Count rows that need backfilling
                    let count: i64 = sqlx::query_scalar(&format!(
                        "SELECT COUNT(*) FROM {}.docs WHERE {} IS NULL",
                        quote_ident(&schema),
                        quote_ident(&column)
                    ))
                    .fetch_one(store.pool())
                    .await?;

                    if count == 0 {
                        println!(
                            "All rows already have '{}' populated. No backfill needed.",
                            column
                        );
                        return Ok(());
                    }

                    println!("Found {} rows where '{}' is NULL", count, column);

                    if *dry_run {
                        println!("\n[DRY RUN] Would backfill in batches of {}", batch_size);
                        println!("Run without --dry-run to execute the backfill.");
                        return Ok(());
                    }

                    println!("Starting backfill in batches of {}...", batch_size);

                    let mut total_updated = 0i64;
                    loop {
                        let updated: u64 = sqlx::query(&format!(
                            "UPDATE {schema}.docs SET doc = doc
                             WHERE {col} IS NULL
                             AND id IN (SELECT id FROM {schema}.docs WHERE {col} IS NULL LIMIT $1)",
                            schema = quote_ident(&schema),
                            col = quote_ident(&column)
                        ))
                        .bind(batch_size)
                        .execute(store.pool())
                        .await?
                        .rows_affected();

                        total_updated += updated as i64;

                        if updated == 0 {
                            break;
                        }

                        println!("  Updated {} rows (total: {})", updated, total_updated);
                    }

                    println!("\nBackfill complete! Updated {} rows total.", total_updated);
                }
            },
            DocsCmd::Verify { tenant } => {
                let mut conn = store.pool().acquire().await?;
                if let Some(t) = tenant.as_deref() {
                    let stmt = format!(
                        "set search_path to {}, public",
                        rillflow::schema::quote_ident(&rillflow::store::tenant_schema_name(t))
                    );
                    sqlx::query(&stmt).execute(&mut *conn).await?;
                }
                // Check docs table exists
                let exists: bool = sqlx::query_scalar(
                    "select exists (select 1 from information_schema.tables where table_name = 'docs')",
                )
                .fetch_one(&mut *conn)
                .await?;
                if !exists {
                    println!("docs table not found in current schema");
                    return Ok(());
                }
                // Check GIN on doc
                let has_gin_doc: bool = sqlx::query_scalar(
                    "select exists (
                        select 1 from pg_indexes
                        where tablename = 'docs'
                          and indexdef ilike '% using gin %doc%'
                    )",
                )
                .fetch_one(&mut *conn)
                .await?;
                if !has_gin_doc {
                    println!(
                        "missing index: create index if not exists docs_gin on docs using gin (doc);"
                    );
                } else {
                    println!("ok: GIN index on docs.doc present");
                }
                // Check docs_fulltext column and index
                let has_fulltext_col: bool = sqlx::query_scalar(
                    "select exists (
                        select 1 from information_schema.columns
                         where table_name = 'docs' and column_name = 'docs_fulltext'
                    )",
                )
                .fetch_one(&mut *conn)
                .await?;
                if has_fulltext_col {
                    let has_fulltext_idx: bool = sqlx::query_scalar(
                        "select exists (
                            select 1 from pg_indexes
                             where tablename = 'docs' and indexdef ilike '% using gin %docs_fulltext%'
                        )",
                    )
                    .fetch_one(&mut *conn)
                    .await?;
                    if !has_fulltext_idx {
                        println!(
                            "missing index: create index if not exists docs_fulltext_idx on docs using gin (docs_fulltext);"
                        );
                    } else {
                        println!("ok: GIN index on docs.docs_fulltext present");
                    }
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
                            total += compact_snapshots_once(&store, &schema, threshold, batch)
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
                                let n = compact_snapshots_once(&store, &schema, threshold, batch)
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
                let use_partitioned =
                    matches!(store.archive_backend(), ArchiveBackend::Partitioned);
                let include_cold = if use_partitioned {
                    store.include_archived_by_default()
                } else {
                    store.include_archived_by_default()
                        && matches!(store.archive_backend(), ArchiveBackend::DualTable)
                };

                let mut tx = store.pool().begin().await?;
                let set_search_path = format!("set local search_path to {}", quote_ident(schema));
                sqlx::query(&set_search_path).execute(&mut *tx).await?;

                let mut qb_candidates = QueryBuilder::<Postgres>::new(
                    "with combined as (select stream_id, stream_seq from events e where 1=1",
                );
                if use_partitioned {
                    if !include_cold {
                        qb_candidates.push(" and e.retention_class = 'hot'");
                    }
                } else if include_cold {
                    qb_candidates.push(
                        " union all select stream_id, stream_seq from events_archive ea where 1=1",
                    );
                }
                qb_candidates.push(
                    ") select count(1) from (select c.stream_id from combined c left join snapshots s on s.stream_id = c.stream_id",
                );
                qb_candidates.push(
                    " group by c.stream_id, s.version having max(c.stream_seq) - coalesce(s.version, 0) >= ",
                );
                qb_candidates.push_bind(threshold);
                qb_candidates.push(") as candidates");

                let candidates: i64 = qb_candidates
                    .build_query_scalar()
                    .fetch_one(&mut *tx)
                    .await?;

                let mut qb_gap = QueryBuilder::<Postgres>::new(
                    "with combined as (select stream_id, stream_seq from events e where 1=1",
                );
                if use_partitioned {
                    if !include_cold {
                        qb_gap.push(" and e.retention_class = 'hot'");
                    }
                } else if include_cold {
                    qb_gap.push(
                        " union all select stream_id, stream_seq from events_archive ea where 1=1",
                    );
                }
                qb_gap.push(
                    ") select max(h.max_seq - coalesce(s.version, 0)) as gap from (select c.stream_id, max(c.stream_seq) as max_seq from combined c group by c.stream_id) h left join snapshots s on s.stream_id = h.stream_id",
                );

                let max_gap: Option<i32> = qb_gap.build_query_scalar().fetch_one(&mut *tx).await?;
                tx.commit().await?;

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
                            duplicated_fields: Vec::new(),
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
        let (status, body, content_type) = if req.starts_with("GET /metrics ") {
            (
                "200 OK",
                rillflow::metrics::render_prometheus(),
                "text/plain",
            )
        } else {
            let clusters = rillflow::metrics::snapshot_daemon_clusters();
            let now = chrono::Utc::now();
            let hotcold: Vec<serde_json::Value> = clusters
                .into_iter()
                .map(|cluster| {
                    let nodes: Vec<serde_json::Value> = cluster
                        .nodes
                        .into_iter()
                        .map(|node| {
                            let heartbeat_ms = now
                                .signed_duration_since(node.last_heartbeat)
                                .num_milliseconds();
                            let lease_remaining_ms = node
                                .lease_until
                                .signed_duration_since(now)
                                .num_milliseconds();
                            serde_json::json!({
                                "node": node.node,
                                "is_hot": node.is_hot,
                                "last_heartbeat": node.last_heartbeat,
                                "heartbeat_age_ms": heartbeat_ms,
                                "lease_until": node.lease_until,
                                "lease_remaining_ms": lease_remaining_ms,
                                "min_cold_standbys": node.min_cold_standbys,
                            })
                        })
                        .collect();
                    serde_json::json!({
                        "cluster": cluster.cluster,
                        "total_nodes": cluster.total_nodes,
                        "hot_nodes": cluster.hot_nodes,
                        "standby_nodes": cluster.standby_nodes,
                        "required_standbys": cluster.required_standbys,
                        "healthy": cluster.hot_nodes == 1 && cluster.standby_nodes as u32 >= cluster.required_standbys,
                        "nodes": nodes,
                    })
                })
                .collect();
            let payload = serde_json::json!({
                "status": "ok",
                "hotcold": hotcold,
            });
            (
                "200 OK",
                serde_json::to_string(&payload)
                    .unwrap_or_else(|_| "{\"status\":\"ok\"}".to_string()),
                "application/json",
            )
        };
        let headers = format!(
            "HTTP/1.1 {}\r\ncontent-length: {}\r\ncontent-type: {}\r\n\r\n",
            status,
            body.len(),
            content_type
        );
        let _ = sock.write_all(headers.as_bytes()).await;
        let _ = sock.write_all(body.as_bytes()).await;
    }
}

async fn compact_snapshots_once(
    store: &Store,
    schema: &str,
    threshold: i32,
    batch: i64,
) -> rillflow::Result<u32> {
    let set_search_path = format!("set local search_path to {}", quote_ident(schema));
    let use_partitioned = matches!(store.archive_backend(), ArchiveBackend::Partitioned);
    let include_cold = if use_partitioned {
        store.include_archived_by_default()
    } else {
        store.include_archived_by_default()
            && matches!(store.archive_backend(), ArchiveBackend::DualTable)
    };

    let mut tx = store.pool().begin().await?;
    sqlx::query(&set_search_path).execute(&mut *tx).await?;
    #[allow(clippy::type_complexity)]
    let mut qb = QueryBuilder::<Postgres>::new(
        "with combined as (select stream_id, stream_seq from events e where 1=1",
    );
    if use_partitioned {
        if !include_cold {
            qb.push(" and e.retention_class = 'hot'");
        }
    } else if include_cold {
        qb.push(" union all select stream_id, stream_seq from events_archive ea where 1=1");
    }
    qb.push(
        ") select c.stream_id, max(c.stream_seq) as head from combined c left join snapshots s on s.stream_id = c.stream_id",
    );
    qb.push(
        " group by c.stream_id, s.version having max(c.stream_seq) - coalesce(s.version, 0) >= ",
    );
    qb.push_bind(threshold);
    qb.push(" order by head desc limit ");
    qb.push_bind(batch);

    let rows: Vec<(uuid::Uuid, i32)> = qb.build_query_as().fetch_all(&mut *tx).await?;

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

async fn archive_status(store: &Store, schema: &str, tenant: Option<&str>) -> rillflow::Result<()> {
    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;

    let counts_res =
        sqlx::query("select retention_class, count(*) from rf_streams group by retention_class")
            .fetch_all(&mut *tx)
            .await;

    let counts = match counts_res {
        Ok(rows) => rows,
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P01") => {
            println!(
                "[schema={}] rf_streams not found (run migration sql/0003_event_archiving.sql)",
                schema
            );
            tx.rollback().await.ok();
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    let mut archived = 0i64;
    let mut hot = 0i64;
    for row in counts {
        let class: String = row.get(0);
        let count: i64 = row.get(1);
        match class.as_str() {
            "cold" => archived += count,
            _ => hot += count,
        }
    }

    let tenant_label = tenant.unwrap_or("<all>");
    println!(
        "Archive status (schema={}, tenant={}):",
        schema, tenant_label
    );
    println!("  hot streams   : {}", hot);
    println!("  cold streams  : {}", archived);

    let escaped_schema = schema.replace('"', "\"\"");
    let hot_rel = format!("\"{}\".\"events\"", escaped_schema);
    let cold_rel = format!("\"{}\".\"events_archive\"", escaped_schema);
    let use_partitioned = matches!(store.archive_backend(), ArchiveBackend::Partitioned);

    let hot_bytes: i64 =
        sqlx::query_scalar("select coalesce(pg_total_relation_size($1::regclass), 0)")
            .bind(&hot_rel)
            .fetch_one(store.pool())
            .await
            .unwrap_or(0);

    let cold_bytes: i64 = if use_partitioned {
        0
    } else {
        sqlx::query_scalar("select coalesce(pg_total_relation_size($1::regclass), 0)")
            .bind(&cold_rel)
            .fetch_one(store.pool())
            .await
            .unwrap_or(0)
    };

    println!("  events bytes  : {} B", hot_bytes);
    println!("  archive bytes : {} B", cold_bytes);

    let redirect = store.archive_redirect_enabled().await.unwrap_or(false);
    println!(
        "  redirect      : {}",
        if redirect { "enabled" } else { "disabled" }
    );
    set_archive_hot_bytes(hot_bytes as u64);
    tx.commit().await?;
    Ok(())
}

async fn archive_mark(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    stream_ref: &str,
    reason: Option<String>,
    by: Option<String>,
) -> rillflow::Result<()> {
    let stream_id = resolve_stream_id(store, stream_ref).await?;
    let actor = by
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "unknown".to_string());
    let reason = reason.unwrap_or_else(|| "manual mark".to_string());
    let retention = "cold";
    let actor_clone = actor.clone();
    let reason_clone = reason.clone();
    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;
    sqlx::query(
        "insert into rf_streams (stream_id, tenant_id, archived_at, archived_by, archive_reason, retention_class)\
         values ($1,$2,$3,$4,$5,$6)\
         on conflict (stream_id) do update set archived_at = EXCLUDED.archived_at, archived_by = EXCLUDED.archived_by, archive_reason = EXCLUDED.archive_reason, retention_class = EXCLUDED.retention_class, updated_at = now()",
    )
    .bind(stream_id)
    .bind(tenant)
    .bind(Some(Utc::now()))
    .bind(&actor_clone)
    .bind(&reason_clone)
    .bind(retention)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    info!(
        stream_id = %stream_id,
        tenant = tenant.unwrap_or("<default>"),
        archived_by = %actor_clone,
        reason = %reason_clone,
        "stream marked archived"
    );
    record_archive_mark(tenant);
    println!("stream {} marked as archived ({})", stream_id, retention);
    Ok(())
}

async fn archive_reactivate(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    stream_ref: &str,
) -> rillflow::Result<()> {
    let stream_id = resolve_stream_id(store, stream_ref).await?;
    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;
    sqlx::query(
        "insert into rf_streams (stream_id, tenant_id, archived_at, archived_by, archive_reason, retention_class)\
         values ($1,$2,NULL,NULL,NULL,'hot')\
         on conflict (stream_id) do update set archived_at = NULL, archived_by = NULL, archive_reason = NULL, retention_class = 'hot', updated_at = now()",
    )
    .bind(stream_id)
    .bind(tenant)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    info!(stream_id = %stream_id, tenant = tenant.unwrap_or("<default>"), "stream reactivated");
    record_archive_reactivate(tenant);
    println!("stream {} reactivated", stream_id);
    Ok(())
}

async fn archive_move(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    stream_ref: Option<&str>,
    before: chrono::DateTime<chrono::Utc>,
    batch_size: i64,
    dry_run: bool,
) -> rillflow::Result<()> {
    let tenant_column = match store.tenant_strategy() {
        TenantStrategy::Conjoined { ref column } => {
            if tenant.is_none() {
                return Err(rillflow::Error::TenantRequired);
            }
            Some(column.name.clone())
        }
        _ => None,
    };

    let tenant_col_ref = tenant_column.as_deref();

    if let Some(stream) = stream_ref {
        process_stream_move(
            store,
            schema,
            tenant,
            tenant_col_ref,
            stream,
            before,
            dry_run,
        )
        .await?;
        return Ok(());
    }

    let mut total_streams = 0u64;
    let mut total_events = 0u64;
    let started = Instant::now();

    loop {
        let candidates =
            fetch_candidate_streams(store, schema, tenant, tenant_col_ref, before, batch_size)
                .await?;
        if candidates.is_empty() {
            break;
        }

        for stream_id in candidates {
            if dry_run {
                let count = count_events(
                    store,
                    schema,
                    tenant,
                    tenant_col_ref,
                    stream_id,
                    Some(before),
                )
                .await?;
                if count > 0 {
                    println!(
                        "[dry-run] stream {} has {} event(s) before {}",
                        stream_id, count, before
                    );
                }
                continue;
            }

            let mut events = store.events();
            if let Some(t) = tenant {
                events = events.with_tenant(t.to_string());
            }

            let moved = events.archive_stream_before(stream_id, before).await? as u64;
            if moved > 0 {
                total_streams += 1;
                total_events += moved;

                let remaining =
                    count_events(store, schema, tenant, tenant_col_ref, stream_id, None).await?;
                if remaining == 0 {
                    upsert_stream_metadata(
                        store,
                        schema,
                        tenant,
                        stream_id,
                        Some(Utc::now()),
                        Some("archive_move"),
                        Some("auto"),
                        "cold",
                    )
                    .await?;
                }
                record_archive_moved(tenant, moved);
                info!(
                    stream_id = %stream_id,
                    tenant = tenant.unwrap_or("<default>"),
                    moved_events = moved,
                    before = %before,
                    "archive move batch"
                );
            }
        }
    }

    if dry_run {
        println!("dry-run complete");
    } else {
        println!(
            "moved {} event(s) across {} stream(s)",
            total_events, total_streams
        );
        info!(
            tenant = tenant.unwrap_or("<default>"),
            events = total_events,
            streams = total_streams,
            "archive move complete"
        );
        record_archive_move(started.elapsed());
    }
    Ok(())
}

async fn archive_partition_create(
    store: &Store,
    schema: &str,
    retention: ArchiveRetention,
    month: NaiveDate,
) -> rillflow::Result<()> {
    if !matches!(store.archive_backend(), ArchiveBackend::Partitioned) {
        return Err(rillflow::Error::QueryError {
            query: "archive partition".into(),
            context: "ArchiveBackend::Partitioned required".into(),
        });
    }
    if month.day() != 1 {
        return Err(rillflow::Error::QueryError {
            query: "archive partition".into(),
            context: "--month must use the first day (YYYY-MM-01)".into(),
        });
    }
    let next = if month.month() == 12 {
        NaiveDate::from_ymd_opt(month.year() + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(month.year(), month.month() + 1, 1)
    }
    .expect("valid month rollover");

    let partition = format!("events_{}_{}", retention.as_str(), month.format("%Y_%m"));
    let parent = match retention {
        ArchiveRetention::Hot => "events_hot",
        ArchiveRetention::Cold => "events_cold",
    };

    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;
    let stmt = format!(
        "do $$\nbegin\n    if to_regclass('{partition}') is null then\n        execute 'create table {partition} partition of {parent} for values from (''{start}'') to (''{end}'')';\n    end if;\nend;\n$$;",
        partition = partition,
        parent = parent,
        start = month.format("%Y-%m-%d"),
        end = next.format("%Y-%m-%d"),
    );
    sqlx::query(&stmt).execute(&mut *tx).await?;
    tx.commit().await?;
    println!(
        "partition {} covers [{} , {} )",
        partition,
        month.format("%Y-%m-%d"),
        next.format("%Y-%m-%d")
    );
    Ok(())
}

async fn fetch_candidate_streams(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    tenant_column: Option<&str>,
    before: chrono::DateTime<chrono::Utc>,
    limit: i64,
) -> rillflow::Result<Vec<Uuid>> {
    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;
    let use_partitioned = matches!(store.archive_backend(), ArchiveBackend::Partitioned);
    let mut qb =
        QueryBuilder::<Postgres>::new("select distinct stream_id from events where created_at < ");
    qb.push_bind(before);
    if let (Some(col), Some(val)) = (tenant_column, tenant) {
        qb.push(" and ");
        qb.push(schema::quote_ident(col));
        qb.push(" = ");
        qb.push_bind(val);
    }
    if use_partitioned {
        qb.push(" and retention_class = 'hot'");
    }
    qb.push(" order by stream_id asc limit ");
    qb.push_bind(limit);
    let ids = qb.build_query_scalar::<Uuid>().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(ids)
}

async fn process_stream_move(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    tenant_column: Option<&str>,
    stream: &str,
    before: chrono::DateTime<chrono::Utc>,
    dry_run: bool,
) -> rillflow::Result<()> {
    let stream_id = resolve_stream_id(store, stream).await?;
    if dry_run {
        let count = count_events(
            store,
            schema,
            tenant,
            tenant_column,
            stream_id,
            Some(before),
        )
        .await?;
        println!(
            "[dry-run] stream {} would move {} event(s) before {}",
            stream_id, count, before
        );
        return Ok(());
    }

    let mut events = store.events();
    if let Some(t) = tenant {
        events = events.with_tenant(t.to_string());
    }
    let moved = events.archive_stream_before(stream_id, before).await?;
    println!(
        "stream {} moved {} event(s) before {}",
        stream_id, moved, before
    );

    let remaining = count_events(store, schema, tenant, tenant_column, stream_id, None).await?;
    if remaining == 0 {
        upsert_stream_metadata(
            store,
            schema,
            tenant,
            stream_id,
            Some(Utc::now()),
            Some("archive_move"),
            Some("auto"),
            "cold",
        )
        .await?;
    }
    Ok(())
}

async fn count_events(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    tenant_column: Option<&str>,
    stream_id: Uuid,
    before: Option<chrono::DateTime<chrono::Utc>>,
) -> rillflow::Result<i64> {
    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;
    let mut qb = QueryBuilder::<Postgres>::new("select count(*) from events where stream_id = ");
    qb.push_bind(stream_id);
    if let (Some(col), Some(val)) = (tenant_column, tenant) {
        qb.push(" and ");
        qb.push(schema::quote_ident(col));
        qb.push(" = ");
        qb.push_bind(val);
    }
    if matches!(store.archive_backend(), ArchiveBackend::Partitioned) {
        if let Some(before) = before {
            qb.push(" and created_at < ");
            qb.push_bind(before);
        }
        qb.push(" and retention_class = 'hot'");
    } else if let Some(before) = before {
        qb.push(" and created_at < ");
        qb.push_bind(before);
    }
    let count = qb.build_query_scalar::<i64>().fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(count)
}

async fn upsert_stream_metadata(
    store: &Store,
    schema: &str,
    tenant: Option<&str>,
    stream_id: Uuid,
    archived_at: Option<chrono::DateTime<chrono::Utc>>,
    archived_by: Option<&str>,
    archive_reason: Option<&str>,
    retention_class: &str,
) -> rillflow::Result<()> {
    let mut tx = store.pool().begin().await?;
    set_search_path(&mut tx, schema).await?;
    sqlx::query(
        "insert into rf_streams (stream_id, tenant_id, archived_at, archived_by, archive_reason, retention_class)\
         values ($1,$2,$3,$4,$5,$6)\
         on conflict (stream_id) do update set archived_at = EXCLUDED.archived_at, archived_by = EXCLUDED.archived_by, archive_reason = EXCLUDED.archive_reason, retention_class = EXCLUDED.retention_class, updated_at = now()",
    )
    .bind(stream_id)
    .bind(tenant)
    .bind(archived_at)
    .bind(archived_by)
    .bind(archive_reason)
    .bind(retention_class)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn set_search_path<'a>(
    exec: &mut (impl Executor<'a, Database = Postgres>),
    schema: &str,
) -> rillflow::Result<()> {
    let stmt = format!("set local search_path to {}", quote_ident(schema));
    sqlx::query(&stmt).execute(exec).await?;
    Ok(())
}

async fn resolve_stream_id(store: &Store, input: &str) -> rillflow::Result<Uuid> {
    if let Ok(id) = Uuid::parse_str(input) {
        Ok(id)
    } else {
        store.resolve_stream_alias(input).await
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn split_sql_dollar_safe(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let bytes = input.as_bytes();
    let mut in_dollar = false;
    while i < bytes.len() {
        if !in_dollar && bytes[i] == b';' {
            let stmt = input[start..i].trim();
            if !stmt.is_empty() {
                parts.push(stmt);
            }
            i += 1;
            start = i;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'$' && bytes[i + 1] == b'$' {
            in_dollar = !in_dollar;
            i += 2;
            continue;
        }
        i += 1;
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

#[derive(Clone)]
struct TenantHelper {
    strategy: TenantStrategy,
    resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    tenant_aware: bool,
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
        tenant_aware: bool,
    ) -> Self {
        Self {
            strategy,
            resolver,
            tenant_aware,
        }
    }

    fn projection_daemon(
        &self,
        pool: PgPool,
        upcasters: Option<Arc<UpcasterRegistry>>,
    ) -> ProjectionDaemon {
        let config = ProjectionWorkerConfig::default();
        self.projection_daemon_with_config(pool, upcasters, config)
    }

    fn projection_daemon_with_config(
        &self,
        pool: PgPool,
        upcasters: Option<Arc<UpcasterRegistry>>,
        mut config: ProjectionWorkerConfig,
    ) -> ProjectionDaemon {
        config.tenant_strategy = self.strategy.clone();
        config.tenant_resolver = self.resolver.clone();
        let daemon = ProjectionDaemon::new(pool, config);
        if let Some(registry) = upcasters {
            daemon.with_upcasters(registry)
        } else {
            daemon
        }
    }

    fn subscriptions(
        &self,
        pool: PgPool,
        backend: ArchiveBackend,
        include_archived: bool,
    ) -> Subscriptions {
        Subscriptions::new_with_strategy(
            pool,
            self.strategy.clone(),
            self.resolver.clone(),
            backend,
            include_archived,
        )
    }

    fn select_single<'a>(
        &'a self,
        tenant: Option<&str>,
        name: &str,
    ) -> rillflow::Result<(TenantContext, String)> {
        let ctx = match (tenant, &self.strategy) {
            (Some(t), TenantStrategy::SchemaPerTenant) => TenantContext {
                tenant_label: Some(t.to_string()),
            },
            (None, TenantStrategy::SchemaPerTenant) => TenantContext {
                tenant_label: self
                    .resolver
                    .as_ref()
                    .and_then(|r| (r)())
                    .ok_or(rillflow::Error::TenantRequired)?,
            },
            (Some(t), TenantStrategy::Conjoined { .. }) => TenantContext {
                tenant_label: Some(t.to_string()),
            },
            (None, TenantStrategy::Conjoined { .. }) => TenantContext {
                tenant_label: self
                    .resolver
                    .as_ref()
                    .and_then(|r| (r)())
                    .ok_or(rillflow::Error::TenantRequired)?,
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
        let contexts = if all_tenants && self.tenant_aware {
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
        } else if self.tenant_aware {
            vec![TenantContext {
                tenant_label: self
                    .resolver
                    .as_ref()
                    .and_then(|r| (r)())
                    .ok_or(rillflow::Error::TenantRequired)?,
            }]
        } else {
            vec![TenantContext { tenant_label: None }]
        };
        f(contexts).await
    }

    async fn list_tenants(&self) -> rillflow::Result<Vec<String>> {
        // For now rely on resolver; extension point for reading from DB later
        if let Some(resolver) = &self.resolver {
            if let Some(tenant) = (resolver)() {
                return Ok(vec![tenant]);
            }
        }
        Ok(vec![])
    }

    fn require_schema(&self, tenant: &str) -> rillflow::Result<String> {
        if matches!(self.strategy, TenantStrategy::SchemaPerTenant) {
            Ok(tenant_schema_name(tenant))
        } else {
            Ok(tenant.to_string())
        }
    }
}
