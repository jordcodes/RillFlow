use std::sync::Arc;
use std::time::Duration;

use crate::{
    Error, Result, SessionContext,
    projections::ProjectionHandler,
    store::{TenantStrategy, tenant_schema_name},
};
use serde_json::Value;
use sqlx::{PgPool, types::Json};
use tracing::{info, instrument, warn};

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
        }
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
}

impl ProjectionDaemon {
    pub fn new(pool: PgPool, config: ProjectionWorkerConfig) -> Self {
        Self {
            pool,
            config,
            registrations: Vec::new(),
        }
    }

    pub fn builder(pool: PgPool) -> ProjectionDaemonBuilder {
        ProjectionDaemonBuilder {
            pool,
            config: ProjectionWorkerConfig::default(),
            registrations: Vec::new(),
        }
    }

    pub fn register(&mut self, name: impl Into<String>, handler: Arc<dyn ProjectionHandler>) {
        self.registrations.push(ProjectionRegistration {
            name: name.into(),
            handler,
        });
    }

    fn resolve_schema(&self, context: &SessionContext) -> Result<Option<String>> {
        match self.config.tenant_strategy {
            TenantStrategy::Single => Ok(Some(self.config.schema.clone())),
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
    ) -> Result<()> {
        let mut listener = if use_notify {
            match sqlx::postgres::PgListener::connect_with(&self.pool).await {
                Ok(mut l) => {
                    if let Some(chan) = &self.config.notify_channel {
                        let _ = l.listen(chan).await;
                    }
                    Some(l)
                }
                Err(_) => None,
            }
        } else {
            None
        };

        loop {
            if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }
            let mut did_work = false;
            for r in &self.registrations {
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
        let rows: Vec<(i64, String, Value)> = sqlx::query_as(
            "select global_seq, event_type, body from events where global_seq > $1 order by global_seq asc limit $2",
        )
        .bind(last_seq)
        .bind(self.config.batch_size)
        .fetch_all(&mut *tx)
        .await?;
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
        for (seq, typ, body) in rows {
            // apply within the same transaction to keep read model + checkpoint atomic
            if let Err(err) = reg.handler.apply(&typ, &body, &mut tx).await {
                // record to DLQ, set backoff, advance checkpoint to skip poison pill for now
                warn!(error = %err, seq = seq, event_type = %typ, "projection handler failed; sending to DLQ and backing off");
                self.insert_dlq(&mut tx, name, seq, &typ, &body, &format!("{}", err))
                    .await?;
                self.increment_attempts_and_set_backoff(name).await?;
                new_last = seq;
                break;
            }

            new_last = seq;
            processed += 1;
            crate::metrics::record_proj_events_processed(context.tenant.as_deref(), 1);
        }

        let cp_start = std::time::Instant::now();
        self.persist_checkpoint(&mut tx, name, new_last).await?;
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
        for r in &self.registrations {
            let _ = self.tick_once(&r.name).await?;
        }
        Ok(())
    }

    /// Repeatedly tick until no projections have work remaining.
    pub async fn run_until_idle(&self) -> Result<()> {
        loop {
            let before = chrono::Utc::now();
            let mut did_work = false;
            for r in &self.registrations {
                match self.tick_once(&r.name).await? {
                    TickResult::Processed { count } if count > 0 => did_work = true,
                    _ => {}
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
        self.persist_checkpoint(&mut tx, name, seq).await?;
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
        self.persist_checkpoint(&mut tx, name, 0).await?;
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
    ) -> Result<()> {
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
    pub fn build(self) -> ProjectionDaemon {
        let mut daemon = ProjectionDaemon::new(self.pool, self.config);
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
