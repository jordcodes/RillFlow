use std::env;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::{
    Error, Result, SessionContext,
    events::{EventEnvelope, Events},
    projections::ProjectionHandler,
    schema,
    store::{TenantStrategy, tenant_schema_name},
    upcasting::UpcasterRegistry,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, types::Json};
use tokio::sync::RwLock;
use tracing::{info, instrument, warn};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum DaemonClusterConfig {
    Single,
    HotCold(HotColdConfig),
}

impl Default for DaemonClusterConfig {
    fn default() -> Self {
        Self::Single
    }
}

#[derive(Clone, Debug)]
pub struct HotColdConfig {
    pub cluster: String,
    pub heartbeat_interval: Duration,
    pub lease_ttl: Duration,
    pub lease_grace: Duration,
    pub min_cold_standbys: u32,
    pub promotion_channel: String,
}

impl HotColdConfig {
    pub fn new(
        cluster: impl Into<String>,
        heartbeat_interval: Duration,
        lease_ttl: Duration,
        lease_grace: Duration,
        min_cold_standbys: u32,
    ) -> Self {
        let cluster = cluster.into();
        Self {
            promotion_channel: default_promotion_channel(&cluster),
            cluster,
            heartbeat_interval,
            lease_ttl,
            lease_grace,
            min_cold_standbys,
        }
    }
}

#[derive(Clone, Debug)]
enum DaemonNodeRole {
    Hot,
    Cold,
}

#[derive(Clone, Copy, Debug)]
struct HotColdTiming {
    heartbeat_interval: chrono::Duration,
    lease_ttl: chrono::Duration,
    lease_grace: chrono::Duration,
}

impl HotColdTiming {
    fn new(heartbeat_interval: Duration, lease_ttl: Duration, lease_grace: Duration) -> Self {
        Self {
            heartbeat_interval: chrono::Duration::from_std(heartbeat_interval)
                .unwrap_or_else(|_| chrono::Duration::seconds(5)),
            lease_ttl: chrono::Duration::from_std(lease_ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(30)),
            lease_grace: chrono::Duration::from_std(lease_grace)
                .unwrap_or_else(|_| chrono::Duration::seconds(0)),
        }
    }

    fn from_config(cfg: &HotColdConfig) -> Self {
        Self::new(cfg.heartbeat_interval, cfg.lease_ttl, cfg.lease_grace)
    }

    fn heartbeat_due(&self, last_heartbeat: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        now.signed_duration_since(last_heartbeat) >= self.heartbeat_interval
    }

    fn lease_deadline(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        now + self.lease_ttl
    }

    fn lease_expired(&self, lease_until: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        lease_until + self.lease_grace <= now
    }

    fn lease_ttl(&self) -> chrono::Duration {
        self.lease_ttl
    }

    fn lease_grace(&self) -> chrono::Duration {
        self.lease_grace
    }
}

impl DaemonNodeRole {
    fn as_str(&self) -> &'static str {
        match self {
            DaemonNodeRole::Hot => "hot",
            DaemonNodeRole::Cold => "cold",
        }
    }
}

#[derive(Clone, Debug)]
struct DaemonNodeState {
    daemon_id: Uuid,
    node_name: String,
    cluster: String,
    role: DaemonNodeRole,
    lease_until: DateTime<Utc>,
    last_heartbeat: DateTime<Utc>,
    lease_ttl: Duration,
    lease_token: Option<Uuid>,
    min_cold_standbys: u32,
}

fn default_node_name() -> String {
    if let Ok(name) = env::var("RILLFLOW_NODE_NAME") {
        if !name.trim().is_empty() {
            return name;
        }
    }

    for key in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(name) = env::var(key) {
            if !name.trim().is_empty() {
                return name;
            }
        }
    }

    format!("rillflow-{}", std::process::id())
}

fn default_promotion_channel(cluster: &str) -> String {
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

#[derive(Clone)]
pub struct ProjectionWorkerConfig {
    pub batch_size: i64,
    pub max_retries: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    pub lease_ttl: Duration,
    pub schema: String,
    pub notify_channel: Option<String>,
    pub tenant_strategy: TenantStrategy,
    pub tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    pub cluster: DaemonClusterConfig,
    pub node_name: String,
    pub daemon_id: Option<Uuid>,
}

impl Default for ProjectionWorkerConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            max_retries: 3,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            lease_ttl: Duration::from_secs(30),
            schema: "public".to_string(),
            notify_channel: Some("rillflow_events".to_string()),
            tenant_strategy: TenantStrategy::Single,
            tenant_resolver: None,
            cluster: DaemonClusterConfig::default(),
            node_name: default_node_name(),
            daemon_id: None,
        }
    }
}

impl ProjectionWorkerConfig {
    pub fn default_node_name() -> String {
        default_node_name()
    }
}

#[derive(Clone)]
pub struct ProjectionRegistration {
    pub name: String,
    pub handler: Arc<dyn ProjectionHandler>,
}

pub struct ProjectionDaemon {
    pool: PgPool,
    config: ProjectionWorkerConfig,
    registrations: Vec<ProjectionRegistration>,
    upcaster_registry: Option<Arc<UpcasterRegistry>>,
    daemon_id: Uuid,
    node_name: String,
    node_state: RwLock<Option<DaemonNodeState>>,
    lease_lost: Arc<AtomicBool>,
}

impl ProjectionDaemon {
    pub fn new(pool: PgPool, config: ProjectionWorkerConfig) -> Self {
        let daemon_id = config.daemon_id.unwrap_or_else(Uuid::new_v4);
        let node_name = config.node_name.clone();
        Self {
            pool,
            config,
            registrations: Vec::new(),
            upcaster_registry: None,
            daemon_id,
            node_name,
            node_state: RwLock::new(None),
            lease_lost: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn builder(pool: PgPool) -> ProjectionDaemonBuilder {
        ProjectionDaemonBuilder {
            pool,
            config: ProjectionWorkerConfig::default(),
            registrations: Vec::new(),
            upcaster_registry: None,
            lease_lost: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_upcasters(mut self, registry: Arc<UpcasterRegistry>) -> Self {
        self.upcaster_registry = Some(registry);
        self
    }

    pub fn register(&mut self, name: impl Into<String>, handler: Arc<dyn ProjectionHandler>) {
        self.registrations.push(ProjectionRegistration {
            name: name.into(),
            handler,
        });
    }

    fn record_hotcold_metrics(&self, state: &DaemonNodeState) {
        if matches!(self.config.cluster, DaemonClusterConfig::HotCold(_)) {
            crate::metrics::record_daemon_node_state(
                &state.cluster,
                &state.node_name,
                matches!(state.role, DaemonNodeRole::Hot),
                state.last_heartbeat,
                state.lease_until,
                state.min_cold_standbys,
            );
        }
    }

    fn clear_hotcold_metrics(&self) {
        if let DaemonClusterConfig::HotCold(cfg) = &self.config.cluster {
            crate::metrics::clear_daemon_node_state(&cfg.cluster, &self.node_name);
        }
    }

    async fn ensure_node_bootstrapped(&self) -> Result<()> {
        match &self.config.cluster {
            DaemonClusterConfig::Single => Ok(()),
            DaemonClusterConfig::HotCold(cfg) => {
                if self.node_state.read().await.is_some() {
                    return Ok(());
                }
                let state = self.register_hotcold_node(cfg).await?;
                *self.node_state.write().await = Some(state);
                Ok(())
            }
        }
    }

    async fn register_hotcold_node(&self, cfg: &HotColdConfig) -> Result<DaemonNodeState> {
        let now = Utc::now();
        let timing = HotColdTiming::from_config(cfg);
        let lease_until = timing.lease_deadline(now);
        let role = DaemonNodeRole::Cold;

        sqlx::query(
            r#"
            insert into rf_daemon_nodes(daemon_id, cluster, node_name, role, heartbeat_at, lease_until, lease_token, min_cold_standbys)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            on conflict (cluster, daemon_id) do update
              set node_name = excluded.node_name,
                  role = excluded.role,
                  heartbeat_at = excluded.heartbeat_at,
                  lease_until = excluded.lease_until,
                  lease_token = excluded.lease_token,
                  min_cold_standbys = excluded.min_cold_standbys,
                  updated_at = now()
            "#,
        )
        .bind(self.daemon_id)
        .bind(&cfg.cluster)
        .bind(&self.node_name)
        .bind(role.as_str())
        .bind(now)
        .bind(lease_until)
        .bind(Option::<Uuid>::None)
        .bind(cfg.min_cold_standbys as i32)
        .execute(&self.pool)
        .await?;

        if let Some((leader_id, leader_name, leader_lease)) =
            sqlx::query_as::<_, (Uuid, String, DateTime<Utc>)>(
                "select daemon_id, node_name, lease_until
                 from rf_daemon_nodes
                 where cluster = $1 and role = 'hot'
                 order by lease_until desc
                 limit 1",
            )
            .bind(&cfg.cluster)
            .fetch_optional(&self.pool)
            .await?
        {
            info!(
                cluster = %cfg.cluster,
                leader_id = %leader_id,
                leader = %leader_name,
                lease_until = %leader_lease,
                "detected existing hot node"
            );
        }

        info!(
            cluster = %cfg.cluster,
            daemon_id = %self.daemon_id,
            node = %self.node_name,
            lease_until = %lease_until,
            "registered daemon node in hot/cold cluster"
        );

        self.lease_lost.store(false, Ordering::Relaxed);

        let state = DaemonNodeState {
            daemon_id: self.daemon_id,
            node_name: self.node_name.clone(),
            cluster: cfg.cluster.clone(),
            role,
            lease_until,
            last_heartbeat: now,
            lease_ttl: cfg.lease_ttl,
            lease_token: None,
            min_cold_standbys: cfg.min_cold_standbys,
        };
        self.record_hotcold_metrics(&state);
        Ok(state)
    }

    async fn queue_cluster_notify(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        channel: &str,
        payload: &str,
    ) -> Result<()> {
        sqlx::query("select pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn relinquish_hot_role(&self) -> Result<()> {
        let cfg = match &self.config.cluster {
            DaemonClusterConfig::HotCold(cfg) => cfg,
            DaemonClusterConfig::Single => return Ok(()),
        };

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "update rf_daemon_nodes set role = 'cold', lease_token = null, min_cold_standbys = $3, updated_at = now() where cluster = $1 and daemon_id = $2 and role = 'hot'",
        )
        .bind(&cfg.cluster)
        .bind(self.daemon_id)
        .bind(cfg.min_cold_standbys as i32)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.queue_cluster_notify(
                &mut tx,
                &cfg.promotion_channel,
                &format!("demoted:{}", self.daemon_id),
            )
            .await?;
            let mut guard = self.node_state.write().await;
            if let Some(state) = guard.as_mut() {
                let now = Utc::now();
                state.role = DaemonNodeRole::Cold;
                state.lease_token = None;
                state.lease_until = now;
                state.last_heartbeat = now;
                self.record_hotcold_metrics(state);
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn try_acquire_leadership(&self, cfg: &HotColdConfig) -> Result<()> {
        let timing = HotColdTiming::new(
            cfg.heartbeat_interval,
            self.config.lease_ttl,
            cfg.lease_grace,
        );
        // Avoid unnecessary work if we already have a healthy lease.
        if let Some(state) = self.node_state.read().await.as_ref() {
            if matches!(state.role, DaemonNodeRole::Hot) {
                if state.lease_until + timing.lease_grace() > Utc::now() {
                    return Ok(());
                }
            }
        }

        let now = Utc::now();
        let lease_extend = timing.lease_ttl();

        let mut tx = self.pool.begin().await?;
        let lock_acquired: bool =
            sqlx::query_scalar("select pg_try_advisory_xact_lock(hashtext($1)::bigint)")
                .bind(&cfg.cluster)
                .fetch_one(&mut *tx)
                .await?;

        if !lock_acquired {
            tx.rollback().await?;
            return Ok(());
        }

        let current_hot = sqlx::query_as::<_, (Uuid, DateTime<Utc>)>(
            "select daemon_id, lease_until from rf_daemon_nodes where cluster = $1 and role = 'hot' for update",
        )
        .bind(&cfg.cluster)
        .fetch_optional(&mut *tx)
        .await?;

        let mut promote = false;
        if let Some((leader_id, lease_until)) = current_hot {
            if leader_id == self.daemon_id {
                promote = true;
            } else if timing.lease_expired(lease_until, now) {
                sqlx::query(
                    "update rf_daemon_nodes set role = 'cold', min_cold_standbys = $3, updated_at = now() where cluster = $1 and daemon_id = $2",
                )
                .bind(&cfg.cluster)
                .bind(leader_id)
                .bind(cfg.min_cold_standbys as i32)
                .execute(&mut *tx)
                .await?;
                self.queue_cluster_notify(
                    &mut tx,
                    &cfg.promotion_channel,
                    &format!("lease_expired:{}", leader_id),
                )
                .await?;
                promote = true;
            }
        } else {
            promote = true;
        }

        let new_lease = now + lease_extend;
        let mut new_token: Option<Uuid> = None;
        if promote {
            let lease_token = Uuid::new_v4();
            sqlx::query(
                r#"
                insert into rf_daemon_nodes(daemon_id, cluster, node_name, role, heartbeat_at, lease_until, lease_token, min_cold_standbys)
                values ($1, $2, $3, 'hot', now(), $4, $5, $6)
                on conflict (cluster, daemon_id) do update
                  set role = 'hot',
                      node_name = excluded.node_name,
                      heartbeat_at = excluded.heartbeat_at,
                      lease_until = excluded.lease_until,
                      lease_token = excluded.lease_token,
                      min_cold_standbys = excluded.min_cold_standbys,
                      updated_at = now()
                "#,
            )
            .bind(self.daemon_id)
            .bind(&cfg.cluster)
            .bind(&self.node_name)
            .bind(new_lease)
            .bind(lease_token)
            .bind(cfg.min_cold_standbys as i32)
            .execute(&mut *tx)
            .await?;
            new_token = Some(lease_token);
            self.queue_cluster_notify(
                &mut tx,
                &cfg.promotion_channel,
                &format!("promoted:{}", self.daemon_id),
            )
            .await?;
        } else {
            sqlx::query(
                "update rf_daemon_nodes set role = 'cold', node_name = $3, lease_token = null, min_cold_standbys = $4, updated_at = now() where cluster = $1 and daemon_id = $2",
            )
            .bind(&cfg.cluster)
            .bind(self.daemon_id)
            .bind(&self.node_name)
            .bind(cfg.min_cold_standbys as i32)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        let mut guard = self.node_state.write().await;
        let state = guard.get_or_insert_with(|| DaemonNodeState {
            daemon_id: self.daemon_id,
            node_name: self.node_name.clone(),
            cluster: cfg.cluster.clone(),
            role: DaemonNodeRole::Cold,
            lease_until: new_lease,
            last_heartbeat: now,
            lease_ttl: self.config.lease_ttl,
            lease_token: None,
            min_cold_standbys: cfg.min_cold_standbys,
        });
        state.role = if promote {
            DaemonNodeRole::Hot
        } else {
            DaemonNodeRole::Cold
        };
        state.last_heartbeat = now;
        if promote {
            state.lease_until = new_lease;
        }
        state.lease_token = new_token;
        state.min_cold_standbys = cfg.min_cold_standbys;
        if promote {
            self.lease_lost.store(false, Ordering::Relaxed);
        }
        self.record_hotcold_metrics(state);
        Ok(())
    }

    async fn heartbeat_if_due(&self, cfg: &HotColdConfig) -> Result<()> {
        let mut guard = self.node_state.write().await;
        let state = match guard.as_mut() {
            Some(state) => state,
            None => return Ok(()),
        };

        let timing = HotColdTiming::new(cfg.heartbeat_interval, state.lease_ttl, cfg.lease_grace);
        let now = Utc::now();
        if !timing.heartbeat_due(state.last_heartbeat, now) {
            return Ok(());
        }

        let lease_until = if matches!(state.role, DaemonNodeRole::Hot) {
            timing.lease_deadline(now)
        } else {
            state.lease_until
        };

        let rows = if let Some(token) = state.lease_token {
            sqlx::query(
                "update rf_daemon_nodes set heartbeat_at = now(), lease_until = $3, node_name = $4, min_cold_standbys = $6, updated_at = now() where cluster = $1 and daemon_id = $2 and lease_token = $5",
            )
            .bind(&state.cluster)
            .bind(state.daemon_id)
            .bind(lease_until)
            .bind(&state.node_name)
            .bind(token)
            .bind(state.min_cold_standbys as i32)
            .execute(&self.pool)
            .await?
        } else {
            sqlx::query(
                "update rf_daemon_nodes set heartbeat_at = now(), lease_until = $3, node_name = $4, min_cold_standbys = $5, updated_at = now() where cluster = $1 and daemon_id = $2",
            )
            .bind(&state.cluster)
            .bind(state.daemon_id)
            .bind(lease_until)
            .bind(&state.node_name)
            .bind(state.min_cold_standbys as i32)
            .execute(&self.pool)
            .await?
        };

        if rows.rows_affected() == 0 {
            drop(guard);
            let new_state = self.register_hotcold_node(cfg).await?;
            let mut guard = self.node_state.write().await;
            *guard = Some(new_state);
            warn!(
                daemon_id = %self.daemon_id,
                node = %self.node_name,
                cluster = %cfg.cluster,
                "heartbeat update failed; re-registered node"
            );
        } else {
            state.last_heartbeat = now;
            if matches!(state.role, DaemonNodeRole::Hot) {
                state.lease_until = lease_until;
            }
            self.record_hotcold_metrics(state);
        }
        Ok(())
    }

    async fn verify_hot_leadership(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        cfg: &HotColdConfig,
    ) -> Result<bool> {
        let timing = HotColdTiming::new(
            cfg.heartbeat_interval,
            self.config.lease_ttl,
            cfg.lease_grace,
        );
        let now = Utc::now();
        let current_token = {
            let guard = self.node_state.read().await;
            guard.as_ref().and_then(|state| state.lease_token)
        };
        let was_hot = self.is_hot_cached().await;

        let record = sqlx::query_as::<_, (String, DateTime<Utc>, Option<Uuid>)>(
            "select role, lease_until, lease_token from rf_daemon_nodes where cluster = $1 and daemon_id = $2 for update",
        )
        .bind(&cfg.cluster)
        .bind(self.daemon_id)
        .fetch_optional(&mut **tx)
        .await?;

        let mut new_role = DaemonNodeRole::Cold;
        let mut lease_until = now;
        let mut still_hot = false;
        let mut new_token: Option<Uuid> = None;

        match record {
            Some((role, lease, token)) => {
                lease_until = lease;
                if role == "hot" && !timing.lease_expired(lease, now) {
                    match (token, current_token) {
                        (Some(db_token), Some(cur_token)) if db_token == cur_token => {
                            new_role = DaemonNodeRole::Hot;
                            still_hot = true;
                            new_token = Some(db_token);
                        }
                        (Some(db_token), None) => {
                            new_role = DaemonNodeRole::Hot;
                            still_hot = true;
                            new_token = Some(db_token);
                        }
                        _ => {
                            sqlx::query(
                                "update rf_daemon_nodes set role = 'cold', lease_token = null, min_cold_standbys = $3, updated_at = now() where cluster = $1 and daemon_id = $2",
                            )
                            .bind(&cfg.cluster)
                            .bind(self.daemon_id)
                            .bind(cfg.min_cold_standbys as i32)
                            .execute(&mut **tx)
                            .await?;
                            self.queue_cluster_notify(
                                tx,
                                &cfg.promotion_channel,
                                &format!("demoted:{}", self.daemon_id),
                            )
                            .await?;
                            warn!(
                                cluster = %cfg.cluster,
                                daemon_id = %self.daemon_id,
                                "lease token mismatch; demoting to cold"
                            );
                            new_token = None;
                        }
                    }
                } else if role == "hot" {
                    sqlx::query(
                        "update rf_daemon_nodes set role = 'cold', lease_token = null, min_cold_standbys = $3, updated_at = now() where cluster = $1 and daemon_id = $2",
                    )
                    .bind(&cfg.cluster)
                    .bind(self.daemon_id)
                    .bind(cfg.min_cold_standbys as i32)
                    .execute(&mut **tx)
                    .await?;
                    self.queue_cluster_notify(
                        tx,
                        &cfg.promotion_channel,
                        &format!("demoted:{}", self.daemon_id),
                    )
                    .await?;
                    warn!(
                        cluster = %cfg.cluster,
                        daemon_id = %self.daemon_id,
                        lease_until = %lease,
                        "lost hot leadership due to expired lease"
                    );
                    new_token = None;
                }
            }
            None => {
                warn!(
                    cluster = %cfg.cluster,
                    daemon_id = %self.daemon_id,
                    "node row missing during leadership check; treating as cold"
                );
                new_token = None;
            }
        }

        {
            let mut guard = self.node_state.write().await;
            if let Some(state) = guard.as_mut() {
                state.role = new_role;
                state.lease_until = lease_until;
                state.last_heartbeat = now;
                state.lease_token = new_token;
                self.record_hotcold_metrics(state);
            }
        }

        if was_hot && !still_hot {
            self.lease_lost.store(true, Ordering::Relaxed);
        }

        Ok(still_hot)
    }

    async fn tick_cluster_state(&self) -> Result<bool> {
        match &self.config.cluster {
            DaemonClusterConfig::Single => Ok(true),
            DaemonClusterConfig::HotCold(cfg) => {
                let was_hot = self.is_hot_cached().await;
                self.ensure_node_bootstrapped().await?;
                self.try_acquire_leadership(cfg).await?;
                self.heartbeat_if_due(cfg).await?;
                let is_hot = self.is_hot_cached().await;
                if was_hot && !is_hot {
                    self.lease_lost.store(true, Ordering::Relaxed);
                }
                Ok(is_hot)
            }
        }
    }

    async fn is_hot_cached(&self) -> bool {
        match self.config.cluster {
            DaemonClusterConfig::Single => true,
            DaemonClusterConfig::HotCold(_) => self
                .node_state
                .read()
                .await
                .as_ref()
                .map(|state| matches!(state.role, DaemonNodeRole::Hot))
                .unwrap_or(false),
        }
    }

    async fn current_lease_context(&self) -> Option<(String, Uuid)> {
        match self.config.cluster {
            DaemonClusterConfig::HotCold(_) => {
                let guard = self.node_state.read().await;
                guard.as_ref().and_then(|state| {
                    if matches!(state.role, DaemonNodeRole::Hot) {
                        state
                            .lease_token
                            .map(|token| (state.cluster.clone(), token))
                    } else {
                        None
                    }
                })
            }
            DaemonClusterConfig::Single => None,
        }
    }

    async fn note_lease_loss(&self, reason: &str) {
        {
            let mut guard = self.node_state.write().await;
            if let Some(state) = guard.as_mut() {
                state.role = DaemonNodeRole::Cold;
                state.lease_token = None;
            }
        }
        self.lease_lost.store(true, Ordering::Relaxed);
        warn!(
            daemon_id = %self.daemon_id,
            node = %self.node_name,
            cluster = match &self.config.cluster {
                DaemonClusterConfig::HotCold(cfg) => cfg.cluster.as_str(),
                DaemonClusterConfig::Single => "single",
            },
            "hot lease lost: {reason}"
        );
    }

    fn tenant_column(&self) -> Option<&str> {
        match &self.config.tenant_strategy {
            TenantStrategy::Conjoined { column } => Some(column.name.as_str()),
            _ => None,
        }
    }

    fn resolve_conjoined_tenant(&self, context: &SessionContext) -> Result<Option<String>> {
        match &self.config.tenant_strategy {
            TenantStrategy::Conjoined { .. } => {
                if let Some(ref t) = context.tenant {
                    return Ok(Some(t.clone()));
                }
                if let Some(resolver) = &self.config.tenant_resolver {
                    if let Some(t) = (resolver)() {
                        return Ok(Some(t));
                    }
                }
                Err(Error::TenantRequired)
            }
            _ => Ok(None),
        }
    }

    fn push_tenant_filter<'a>(
        &self,
        qb: &mut QueryBuilder<'a, Postgres>,
        tenant: Option<&'a String>,
    ) {
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant) {
            qb.push(format!("{} = ", schema::quote_ident(column)));
            qb.push_bind(value);
            qb.push(" and ");
        }
    }

    fn resolve_schema(&self, context: &SessionContext) -> Result<Option<String>> {
        match self.config.tenant_strategy {
            TenantStrategy::Single => Ok(Some(self.config.schema.clone())),
            TenantStrategy::Conjoined { .. } => {
                if context.tenant.is_some() {
                    Ok(Some(self.config.schema.clone()))
                } else if let Some(resolver) = &self.config.tenant_resolver {
                    if (resolver)().is_some() {
                        Ok(Some(self.config.schema.clone()))
                    } else {
                        Err(crate::Error::TenantRequired)
                    }
                } else {
                    Err(crate::Error::TenantRequired)
                }
            }
            TenantStrategy::SchemaPerTenant => {
                if let Some(ref tenant) = context.tenant {
                    Ok(Some(tenant_schema_name(tenant)))
                } else if let Some(resolver) = &self.config.tenant_resolver {
                    match resolver() {
                        Some(tenant) => Ok(Some(tenant_schema_name(&tenant))),
                        None => Err(crate::Error::TenantRequired),
                    }
                } else {
                    Err(crate::Error::TenantRequired)
                }
            }
        }
    }

    /// Long-running loop: ticks all projections, optionally LISTENs for event notifications to wake up.
    /// Stops when the provided stop flag becomes true.
    #[instrument(skip_all, fields(schema = %self.config.schema))]
    pub async fn run_loop(
        &self,
        use_notify: bool,
        stop_flag: Arc<std::sync::atomic::AtomicBool>,
        tenant: Option<&str>,
    ) -> Result<()> {
        self.ensure_node_bootstrapped().await?;

        let _ = tenant;

        let event_channel = if use_notify {
            self.config.notify_channel.clone()
        } else {
            None
        };
        let cluster_channel = match &self.config.cluster {
            DaemonClusterConfig::HotCold(cfg) => Some(cfg.promotion_channel.clone()),
            DaemonClusterConfig::Single => None,
        };

        let mut listen_channels = Vec::new();
        if let Some(ref chan) = event_channel {
            listen_channels.push(chan.clone());
        }
        if let Some(ref chan) = cluster_channel {
            listen_channels.push(chan.clone());
        }

        let mut listener = if listen_channels.is_empty() {
            None
        } else {
            match sqlx::postgres::PgListener::connect_with(&self.pool).await {
                Ok(mut l) => {
                    for ch in &listen_channels {
                        let _ = l.listen(ch).await;
                    }
                    Some(l)
                }
                Err(_) => None,
            }
        };

        loop {
            if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                self.relinquish_hot_role().await?;
                self.clear_hotcold_metrics();
                return Ok(());
            }
            if self.lease_lost.load(Ordering::Relaxed) {
                self.relinquish_hot_role().await?;
                self.clear_hotcold_metrics();
                info!(
                    daemon_id = %self.daemon_id,
                    node = %self.node_name,
                    "run loop stopping after lease loss"
                );
                return Ok(());
            }

            let is_hot = self.tick_cluster_state().await?;
            if self.lease_lost.load(Ordering::Relaxed) {
                self.relinquish_hot_role().await?;
                info!(
                    daemon_id = %self.daemon_id,
                    node = %self.node_name,
                    "lease lost during tick; exiting loop"
                );
                self.relinquish_hot_role().await?;
                self.clear_hotcold_metrics();
                return Ok(());
            }
            if !is_hot {
                let wait = match &self.config.cluster {
                    DaemonClusterConfig::HotCold(cfg) => cfg.heartbeat_interval,
                    DaemonClusterConfig::Single => Duration::from_millis(250),
                };
                if let Some(l) = &mut listener {
                    let cluster_chan = cluster_channel.as_deref().unwrap_or("");
                    let event_chan = event_channel.as_deref().unwrap_or("");
                    let _ = tokio::time::timeout(wait, async {
                        loop {
                            match l.recv().await {
                                Ok(notification) => {
                                    let channel = notification.channel();
                                    if channel == cluster_chan || channel == event_chan {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    })
                    .await;
                } else {
                    tokio::time::sleep(wait).await;
                }
                continue;
            }

            let mut did_work = false;
            for r in &self.registrations {
                if matches!(self.config.cluster, DaemonClusterConfig::HotCold(_))
                    && !self.is_hot_cached().await
                {
                    break;
                }
                match self.tick_once(&r.name).await? {
                    TickResult::Processed { count } if count > 0 => did_work = true,
                    _ => {}
                }
            }
            if !did_work {
                if let Some(l) = &mut listener {
                    let _ = tokio::time::timeout(Duration::from_secs(5), l.recv()).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    /// Execute one processing tick for the given projection name (if registered).
    #[instrument(skip_all, fields(projection = %name, schema = %self.config.schema))]
    pub async fn tick_once(&self, name: &str) -> Result<TickResult> {
        let reg = self
            .registrations
            .iter()
            .find(|r| r.name == name)
            .ok_or_else(|| Error::UnknownProjection(name.to_string()))?;

        if self.is_paused(name).await? {
            return Ok(TickResult::Paused);
        }

        if let Some(until) = self.backoff_until(name).await? {
            // still in backoff; do nothing
            let now = chrono::Utc::now();
            if until > now {
                return Ok(TickResult::Backoff);
            }
        }

        if !self.acquire_lease(name).await? {
            return Ok(TickResult::LeasedByOther);
        }

        let mut tx = self.pool.begin().await?;
        if let DaemonClusterConfig::HotCold(ref cfg) = self.config.cluster {
            if !self.verify_hot_leadership(&mut tx, cfg).await? {
                tx.rollback().await?;
                return Ok(TickResult::LeasedByOther);
            }
        }
        let lease_ctx = self.current_lease_context().await;
        let context = SessionContext::default();
        if let Some(schema) = self.resolve_schema(&context)? {
            let set_search_path = format!("set local search_path to {}", quote_ident(&schema));
            sqlx::query(&set_search_path).execute(&mut *tx).await?;
        }
        let last_seq: i64 = sqlx::query_scalar("select last_seq from projections where name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);

        let q_start = std::time::Instant::now();

        // Build query with optional tenant filter for conjoined mode
        let tenant_value = self.resolve_conjoined_tenant(&context)?;
        let tenant_ref = tenant_value.as_ref();
        let mut qb = QueryBuilder::<Postgres>::new(
            "select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id, created_at from events where ",
        );
        self.push_tenant_filter(&mut qb, tenant_ref);
        qb.push("global_seq > ");
        qb.push_bind(last_seq);
        qb.push(" order by global_seq asc limit ");
        qb.push_bind(self.config.batch_size);

        let rows: Vec<(
            i64,
            Uuid,
            i32,
            String,
            Value,
            Value,
            Option<Uuid>,
            Option<Uuid>,
            i32,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
        )> = qb.build_query_as().fetch_all(&mut *tx).await?;
        crate::metrics::record_query_duration("projection_fetch_batch", q_start.elapsed());

        if rows.is_empty() {
            // refresh lease (touch) and exit
            self.refresh_lease(name).await?;
            tx.commit().await?;
            return Ok(TickResult::Idle);
        }

        let mut new_last = last_seq;
        let mut processed: usize = 0;
        let batch_apply_start = std::time::Instant::now();
        for (
            seq,
            stream_id,
            stream_seq,
            typ,
            body,
            headers,
            causation_id,
            correlation_id,
            event_version,
            user_id,
            created_at,
        ) in rows
        {
            // apply within the same transaction to keep read model + checkpoint atomic
            let mut envelope = EventEnvelope {
                global_seq: seq,
                stream_id,
                stream_seq,
                typ,
                body,
                headers,
                causation_id,
                correlation_id,
                event_version,
                tenant_id: tenant_value.clone(),
                user_id,
                created_at,
            };

            if let Some(registry) = &self.upcaster_registry {
                registry
                    .upcast_with_pool(&mut envelope, Some(&self.pool))
                    .await?;
            } else {
                Events::apply_legacy_upcasters(&mut envelope);
            }

            if let Err(err) = reg
                .handler
                .apply(&envelope.typ, &envelope.body, &mut tx)
                .await
            {
                // record to DLQ, set backoff, advance checkpoint to skip poison pill for now
                warn!(
                    error = %err,
                    seq = envelope.global_seq,
                    event_type = %envelope.typ,
                    "projection handler failed; sending to DLQ and backing off"
                );
                self.insert_dlq(
                    &mut tx,
                    name,
                    envelope.global_seq,
                    &envelope.typ,
                    &envelope.body,
                    &format!("{}", err),
                )
                .await?;
                self.increment_attempts_and_set_backoff(name).await?;
                new_last = envelope.global_seq;
                break;
            }

            new_last = envelope.global_seq;
            processed += 1;
            crate::metrics::record_proj_events_processed(context.tenant.as_deref(), 1);
        }

        let cp_start = std::time::Instant::now();
        if let Err(err) = self
            .persist_checkpoint(&mut tx, name, new_last, lease_ctx.clone())
            .await
        {
            if matches!(err, Error::LeaseMismatch) {
                if let Some((cluster, _)) = &lease_ctx {
                    self.note_lease_loss("checkpoint lease token mismatch")
                        .await;
                    let _ = sqlx::query(
                        "update rf_daemon_nodes set role = 'cold', lease_token = null, updated_at = now() where cluster = $1 and daemon_id = $2",
                    )
                    .bind(cluster)
                    .bind(self.daemon_id)
                    .execute(&mut *tx)
                    .await;
                }
                tx.rollback().await?;
                return Ok(TickResult::LeasedByOther);
            } else {
                return Err(err);
            }
        }
        crate::metrics::record_query_duration("projection_persist_checkpoint", cp_start.elapsed());
        tx.commit().await?;
        self.refresh_lease(name).await?;

        let batch_elapsed = batch_apply_start.elapsed();
        info!(
            processed = processed,
            last_seq = new_last,
            elapsed_ms = batch_elapsed.as_millis() as u64,
            "tick processed batch"
        );
        crate::metrics::record_projection_batch(name, processed as u64, batch_elapsed);
        Ok(TickResult::Processed {
            count: processed as u32,
        })
    }

    pub async fn tick_all_once(&self) -> Result<()> {
        if self.lease_lost.load(Ordering::Relaxed) {
            return Ok(());
        }
        if matches!(self.config.cluster, DaemonClusterConfig::HotCold(_))
            && !self.is_hot_cached().await
        {
            return Ok(());
        }
        for r in &self.registrations {
            let res = self.tick_once(&r.name).await?;
            if matches!(self.config.cluster, DaemonClusterConfig::HotCold(_))
                && !self.is_hot_cached().await
            {
                if !matches!(res, TickResult::LeasedByOther) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Repeatedly tick until no projections have work remaining.
    pub async fn run_until_idle(&self) -> Result<()> {
        if self.lease_lost.load(Ordering::Relaxed) {
            return Ok(());
        }
        if matches!(self.config.cluster, DaemonClusterConfig::HotCold(_))
            && !self.is_hot_cached().await
        {
            return Ok(());
        }
        loop {
            let before = chrono::Utc::now();
            let mut did_work = false;
            for r in &self.registrations {
                match self.tick_once(&r.name).await? {
                    TickResult::Processed { count } if count > 0 => did_work = true,
                    _ => {}
                }
                if matches!(self.config.cluster, DaemonClusterConfig::HotCold(_))
                    && !self.is_hot_cached().await
                {
                    return Ok(());
                }
            }
            if !did_work {
                return Ok(());
            }
            // small guard to avoid hot-looping
            let elapsed = (chrono::Utc::now() - before).num_milliseconds();
            if elapsed < 5 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    }

    // --- Admin APIs ---

    pub async fn list(&self, tenant: Option<&str>) -> Result<Vec<ProjectionStatus>> {
        // Prefer registered names; if none, fall back to names found in DB.
        let mut names: Vec<String> = if self.registrations.is_empty() {
            self.discover_names().await?
        } else {
            self.registrations.iter().map(|r| r.name.clone()).collect()
        };
        names.sort();
        names.dedup();

        let mut out = Vec::new();
        for name in names {
            out.push(self.status(&name, tenant).await?);
        }
        Ok(out)
    }

    /// List DLQ items for a projection (most recent first)
    pub async fn dlq_list(
        &self,
        name: &str,
        limit: i64,
        tenant: Option<&str>,
    ) -> Result<Vec<ProjectionDlqItem>> {
        let mut tx = self.pool.begin().await?;
        if let Some(schema) = tenant.map(tenant_schema_name) {
            let stmt = format!("set local search_path to {}", quote_ident(&schema));
            sqlx::query(&stmt).execute(&mut *tx).await?;
        } else {
            let stmt = format!(
                "set local search_path to {}",
                quote_ident(&self.config.schema)
            );
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }
        let rows: Vec<(i64, i64, String, chrono::DateTime<chrono::Utc>, String)> = sqlx::query_as(
            r#"select id, global_seq, event_type, failed_at, error
                from projection_dlq where name = $1
                order by id desc limit $2"#,
        )
        .bind(name)
        .bind(limit.max(1))
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows
            .into_iter()
            .map(|(id, seq, typ, failed_at, error)| ProjectionDlqItem {
                id,
                seq,
                event_type: typ,
                failed_at,
                error,
            })
            .collect())
    }

    /// Delete a single DLQ item
    pub async fn dlq_delete(&self, name: &str, id: i64, tenant: Option<&str>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        if let Some(schema) = tenant.map(tenant_schema_name) {
            let stmt = format!("set local search_path to {}", quote_ident(&schema));
            sqlx::query(&stmt).execute(&mut *tx).await?;
        } else {
            let stmt = format!(
                "set local search_path to {}",
                quote_ident(&self.config.schema)
            );
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }
        sqlx::query("delete from projection_dlq where name = $1 and id = $2")
            .bind(name)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Requeue a DLQ item by setting checkpoint to seq-1 and deleting the DLQ row
    pub async fn dlq_requeue(&self, name: &str, id: i64, tenant: Option<&str>) -> Result<()> {
        let schema_stmt = if let Some(schema) = tenant.map(tenant_schema_name) {
            format!("set local search_path to {}", quote_ident(&schema))
        } else {
            format!(
                "set local search_path to {}",
                quote_ident(&self.config.schema)
            )
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query(&schema_stmt).execute(&mut *tx).await?;
        let rec: Option<i64> =
            sqlx::query_scalar("select global_seq from projection_dlq where name = $1 and id = $2")
                .bind(name)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(seq) = rec {
            let reset_to = seq.saturating_sub(1);
            self.reset_checkpoint(name, reset_to, tenant).await?;
            sqlx::query("delete from projection_dlq where name = $1 and id = $2")
                .bind(name)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn status(&self, name: &str, tenant: Option<&str>) -> Result<ProjectionStatus> {
        let schema = tenant
            .map(tenant_schema_name)
            .unwrap_or_else(|| self.config.schema.clone());
        let stmt = format!("set local search_path to {}", quote_ident(&schema));
        let mut tx = self.pool.begin().await?;
        sqlx::query(&stmt).execute(&mut *tx).await?;

        let last_seq: i64 = sqlx::query_scalar("select last_seq from projections where name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);

        let paused: bool =
            sqlx::query_scalar("select paused from projection_control where name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?
                .unwrap_or(false);

        let backoff_until: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("select backoff_until from projection_control where name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?;

        let leased_by: Option<String> =
            sqlx::query_scalar("select leased_by from projection_leases where name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?;

        let lease_until: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("select lease_until from projection_leases where name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?;

        let dlq_count: i64 =
            sqlx::query_scalar("select count(1) from projection_dlq where name = $1")
                .bind(name)
                .fetch_one(&mut *tx)
                .await?;

        tx.commit().await?;

        Ok(ProjectionStatus {
            name: name.to_string(),
            last_seq,
            paused,
            backoff_until,
            leased_by,
            lease_until,
            dlq_count,
        })
    }

    /// Compute simple metrics: last checkpoint, head of events, lag and DLQ size.
    pub async fn metrics(&self, name: &str, tenant: Option<&str>) -> Result<ProjectionMetrics> {
        let schema = tenant
            .map(tenant_schema_name)
            .unwrap_or_else(|| self.config.schema.clone());
        let stmt = format!("set local search_path to {}", quote_ident(&schema));
        let mut tx = self.pool.begin().await?;
        sqlx::query(&stmt).execute(&mut *tx).await?;

        let last_seq: i64 = sqlx::query_scalar("select last_seq from projections where name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);

        let head_seq: i64 = sqlx::query_scalar("select coalesce(max(global_seq), 0) from events")
            .fetch_one(&mut *tx)
            .await?;

        let dlq_count: i64 =
            sqlx::query_scalar("select count(1) from projection_dlq where name = $1")
                .bind(name)
                .fetch_one(&mut *tx)
                .await?;

        tx.commit().await?;

        Ok(ProjectionMetrics {
            name: name.to_string(),
            last_seq,
            head_seq,
            lag: (head_seq - last_seq).max(0),
            dlq_count,
        })
    }

    #[instrument(skip_all, fields(projection = %name))]
    pub async fn pause(&self, name: &str, tenant: Option<&str>) -> Result<()> {
        let schema = tenant
            .map(tenant_schema_name)
            .unwrap_or_else(|| self.config.schema.clone());
        let stmt = format!("set local search_path to {}", quote_ident(&schema));
        let mut tx = self.pool.begin().await?;
        sqlx::query(&stmt).execute(&mut *tx).await?;
        sqlx::query(
            r#"insert into projection_control(name, paused) values($1, true)
                on conflict (name) do update set paused = true, updated_at = now()"#,
        )
        .bind(name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument(skip_all, fields(projection = %name))]
    pub async fn resume(&self, name: &str, tenant: Option<&str>) -> Result<()> {
        let schema = tenant
            .map(tenant_schema_name)
            .unwrap_or_else(|| self.config.schema.clone());
        let stmt = format!("set local search_path to {}", quote_ident(&schema));
        let mut tx = self.pool.begin().await?;
        sqlx::query(&stmt).execute(&mut *tx).await?;
        sqlx::query(
            r#"insert into projection_control(name, paused, backoff_until) values($1, false, null)
                on conflict (name) do update set paused = false, backoff_until = null, updated_at = now()"#,
        )
        .bind(name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument(skip_all, fields(projection = %name, seq = seq))]
    pub async fn reset_checkpoint(&self, name: &str, seq: i64, tenant: Option<&str>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        if let Some(schema) = tenant.map(tenant_schema_name) {
            let stmt = format!("set local search_path to {}", quote_ident(&schema));
            sqlx::query(&stmt).execute(&mut *tx).await?;
        } else {
            let stmt = format!(
                "set local search_path to {}",
                quote_ident(&self.config.schema)
            );
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }
        self.persist_checkpoint(&mut tx, name, seq, None).await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument(skip_all, fields(projection = %name))]
    pub async fn rebuild(&self, name: &str, tenant: Option<&str>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        if let Some(schema) = tenant.map(tenant_schema_name) {
            let stmt = format!("set local search_path to {}", quote_ident(&schema));
            sqlx::query(&stmt).execute(&mut *tx).await?;
        } else {
            let stmt = format!(
                "set local search_path to {}",
                quote_ident(&self.config.schema)
            );
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }
        self.persist_checkpoint(&mut tx, name, 0, None).await?;
        sqlx::query("delete from projection_dlq where name = $1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    // --- internals ---

    async fn is_paused(&self, name: &str) -> Result<bool> {
        let paused =
            sqlx::query_scalar::<_, bool>("select paused from projection_control where name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or(false);
        Ok(paused)
    }

    async fn backoff_until(&self, name: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        let ts = sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
            "select backoff_until from projection_control where name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(ts.flatten())
    }

    async fn increment_attempts_and_set_backoff(&self, name: &str) -> Result<()> {
        let attempts: i64 = sqlx::query_scalar(
            r#"insert into projection_control(name, attempts)
                   values ($1, 1)
                 on conflict (name) do update
                   set attempts = projection_control.attempts + 1,
                       updated_at = now()
                 returning attempts"#,
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await?;

        let exp = attempts.saturating_sub(1).clamp(0, 16) as u32;
        let mut delay = self.config.base_backoff * (1u32 << exp);
        if delay > self.config.max_backoff {
            delay = self.config.max_backoff;
        }
        let until = chrono::Utc::now() + chrono::Duration::from_std(delay).unwrap();
        sqlx::query(
            r#"update projection_control set backoff_until = $2, updated_at = now() where name = $1"#,
        )
        .bind(name)
        .bind(until)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn acquire_lease(&self, name: &str) -> Result<bool> {
        let id = format!("pid-{}", std::process::id());
        let ttl = chrono::Utc::now() + chrono::Duration::from_std(self.config.lease_ttl).unwrap();

        // Optional advisory lock per projection name to reduce stampedes
        let _ = sqlx::query("select pg_try_advisory_xact_lock(hashtext($1)::bigint)")
            .bind(name)
            .execute(&self.pool)
            .await?;

        // Try to insert or update if expired
        let rows = sqlx::query(
            r#"
            insert into projection_leases(name, leased_by, lease_until)
            values ($1, $2, $3)
            on conflict (name) do update
              set leased_by = excluded.leased_by,
                  lease_until = excluded.lease_until,
                  updated_at = now()
              where projection_leases.lease_until <= now()
                 or projection_leases.leased_by = excluded.leased_by
            "#,
        )
        .bind(name)
        .bind(&id)
        .bind(ttl)
        .execute(&self.pool)
        .await?;

        if rows.rows_affected() > 0 {
            return Ok(true);
        }

        // If a row exists and not expired, we don't own it
        Ok(false)
    }

    async fn refresh_lease(&self, name: &str) -> Result<()> {
        let ttl = chrono::Utc::now() + chrono::Duration::from_std(self.config.lease_ttl).unwrap();
        sqlx::query(
            r#"update projection_leases set lease_until = $2, updated_at = now() where name = $1"#,
        )
        .bind(name)
        .bind(ttl)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"update projection_control set attempts = 0, backoff_until = null, updated_at = now() where name = $1"#,
        )
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn insert_dlq(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        name: &str,
        seq: i64,
        typ: &str,
        body: &Value,
        err_msg: &str,
    ) -> Result<()> {
        sqlx::query(
            "insert into projection_dlq(name, global_seq, event_type, body, error) values ($1,$2,$3,$4,$5)",
        )
        .bind(name)
        .bind(seq)
        .bind(typ)
        .bind(Json(body))
        .bind(err_msg)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn persist_checkpoint(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        name: &str,
        last_seq: i64,
        lease_ctx: Option<(String, Uuid)>,
    ) -> Result<()> {
        if let Some((cluster, token)) = lease_ctx {
            let valid = sqlx::query_scalar::<_, Option<Uuid>>(
                "select lease_token from rf_daemon_nodes where cluster = $1 and daemon_id = $2",
            )
            .bind(&cluster)
            .bind(self.daemon_id)
            .fetch_optional(&mut **tx)
            .await?
            .flatten();

            if valid != Some(token) {
                return Err(Error::LeaseMismatch);
            }
        }

        sqlx::query(
            r#"
            insert into projections(name, last_seq) values ($1,$2)
            on conflict (name) do update set last_seq = excluded.last_seq, updated_at = now()
            "#,
        )
        .bind(name)
        .bind(last_seq)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn discover_names(&self) -> Result<Vec<String>> {
        // union of names present anywhere
        let mut names: Vec<String> = Vec::new();
        for sql in [
            "select name from projections",
            "select name from projection_control",
            "select name from projection_leases",
            "select distinct name from projection_dlq",
        ] {
            let mut rows: Vec<String> = sqlx::query_scalar(sql).fetch_all(&self.pool).await?;
            names.append(&mut rows);
        }
        names.sort();
        names.dedup();
        Ok(names)
    }
}

pub struct ProjectionDaemonBuilder {
    pool: PgPool,
    config: ProjectionWorkerConfig,
    registrations: Vec<ProjectionRegistration>,
    upcaster_registry: Option<Arc<UpcasterRegistry>>,
    lease_lost: Arc<AtomicBool>,
}

impl ProjectionDaemonBuilder {
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.config.schema = schema.into();
        self
    }
    pub fn tenant_strategy(mut self, strategy: TenantStrategy) -> Self {
        self.config.tenant_strategy = strategy;
        self
    }
    pub fn tenant_resolver<F>(mut self, resolver: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        self.config.tenant_resolver = Some(Arc::new(resolver));
        self
    }
    pub fn batch_size(mut self, size: i64) -> Self {
        self.config.batch_size = size.max(1);
        self
    }
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.config.lease_ttl = ttl;
        self
    }
    pub fn cluster(mut self, cluster: DaemonClusterConfig) -> Self {
        self.config.cluster = cluster;
        self
    }
    pub fn backoff(mut self, base: Duration, max: Duration) -> Self {
        self.config.base_backoff = base;
        self.config.max_backoff = max;
        self
    }
    pub fn register(
        mut self,
        name: impl Into<String>,
        handler: Arc<dyn ProjectionHandler>,
    ) -> Self {
        self.registrations.push(ProjectionRegistration {
            name: name.into(),
            handler,
        });
        self
    }
    pub fn upcasters(mut self, registry: Arc<UpcasterRegistry>) -> Self {
        self.upcaster_registry = Some(registry);
        self
    }
    pub fn build(self) -> ProjectionDaemon {
        let mut daemon = ProjectionDaemon::new(self.pool, self.config);
        if let Some(registry) = self.upcaster_registry {
            daemon = daemon.with_upcasters(registry);
        }
        daemon.lease_lost = self.lease_lost;
        for r in self.registrations {
            daemon.registrations.push(r);
        }
        daemon
    }
}

#[derive(Clone, Debug)]
pub struct ProjectionStatus {
    pub name: String,
    pub last_seq: i64,
    pub paused: bool,
    pub backoff_until: Option<chrono::DateTime<chrono::Utc>>,
    pub leased_by: Option<String>,
    pub lease_until: Option<chrono::DateTime<chrono::Utc>>,
    pub dlq_count: i64,
}

#[derive(Clone, Debug)]
pub struct ProjectionMetrics {
    pub name: String,
    pub last_seq: i64,
    pub head_seq: i64,
    pub lag: i64,
    pub dlq_count: i64,
}

#[derive(Clone, Debug)]
pub enum TickResult {
    Idle,
    Paused,
    Backoff,
    LeasedByOther,
    Processed { count: u32 },
}

#[derive(Clone, Debug)]
pub struct ProjectionDlqItem {
    pub id: i64,
    pub seq: i64,
    pub event_type: String,
    pub failed_at: chrono::DateTime<chrono::Utc>,
    pub error: String,
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

#[cfg(test)]
mod hot_cold_timing_tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, TimeZone};
    use std::time::Duration;

    #[test]
    fn heartbeat_due_requires_elapsed_interval() {
        let timing = HotColdTiming::new(
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_secs(2),
        );
        let base = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        assert!(!timing.heartbeat_due(base, base + ChronoDuration::seconds(4)));
        assert!(timing.heartbeat_due(base, base + ChronoDuration::seconds(5)));
        assert!(timing.heartbeat_due(base, base + ChronoDuration::seconds(6)));
    }

    #[test]
    fn lease_deadline_adds_ttl() {
        let timing = HotColdTiming::new(
            Duration::from_secs(5),
            Duration::from_secs(42),
            Duration::from_secs(0),
        );
        let now = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let expected = now + ChronoDuration::seconds(42);
        assert_eq!(timing.lease_deadline(now), expected);
    }

    #[test]
    fn lease_expired_honors_grace_period() {
        let timing = HotColdTiming::new(
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_secs(3),
        );
        let lease_until = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 30).unwrap();

        assert!(!timing.lease_expired(lease_until, lease_until + ChronoDuration::seconds(2)));
        assert!(timing.lease_expired(lease_until, lease_until + ChronoDuration::seconds(3)));
        assert!(timing.lease_expired(lease_until, lease_until + ChronoDuration::seconds(10)));
    }
}
