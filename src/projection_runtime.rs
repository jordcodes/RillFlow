use std::sync::Arc;
use std::time::Duration;

use crate::{Result, projections::ProjectionHandler};
use serde_json::Value;
use sqlx::{PgPool, types::Json};

#[derive(Clone, Debug)]
pub struct ProjectionWorkerConfig {
    pub batch_size: i64,
    pub max_retries: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    pub lease_ttl: Duration,
    pub schema: String,
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

    pub fn register(&mut self, name: impl Into<String>, handler: Arc<dyn ProjectionHandler>) {
        self.registrations.push(ProjectionRegistration {
            name: name.into(),
            handler,
        });
    }

    /// Execute one processing tick for the given projection name (if registered).
    pub async fn tick_once(&self, name: &str) -> Result<TickResult> {
        let reg = self
            .registrations
            .iter()
            .find(|r| r.name == name)
            .ok_or_else(|| {
                sqlx::Error::Protocol(format!("unknown projection `{}`", name).into())
            })?;

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
        // Scope all table references to configured schema (search_path is local to this tx)
        let set_search_path = format!(
            "set local search_path to {}",
            quote_ident(&self.config.schema)
        );
        sqlx::query(&set_search_path).execute(&mut *tx).await?;
        let last_seq: i64 = sqlx::query_scalar("select last_seq from projections where name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);

        let rows: Vec<(i64, String, Value)> = sqlx::query_as(
            "select global_seq, event_type, body from events where global_seq > $1 order by global_seq asc limit $2",
        )
        .bind(last_seq)
        .bind(self.config.batch_size)
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            // refresh lease (touch) and exit
            self.refresh_lease(name).await?;
            tx.commit().await?;
            return Ok(TickResult::Idle);
        }

        let mut new_last = last_seq;
        let mut processed: usize = 0;
        for (seq, typ, body) in rows {
            // apply within the same transaction to keep read model + checkpoint atomic
            if let Err(err) = reg.handler.apply(&typ, &body, &mut tx).await {
                // record to DLQ, set backoff, advance checkpoint to skip poison pill for now
                self.insert_dlq(&mut tx, name, seq, &typ, &body, &format!("{}", err))
                    .await?;
                self.set_backoff(name, 1).await?;
                new_last = seq;
                break;
            }

            new_last = seq;
            processed += 1;
        }

        self.persist_checkpoint(&mut tx, name, new_last).await?;
        tx.commit().await?;
        self.refresh_lease(name).await?;

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

    // --- Admin APIs ---

    pub async fn list(&self) -> Result<Vec<ProjectionStatus>> {
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
            out.push(self.status(&name).await?);
        }
        Ok(out)
    }

    pub async fn status(&self, name: &str) -> Result<ProjectionStatus> {
        let last_seq: Option<i64> =
            sqlx::query_scalar("select last_seq from projections where name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;

        let paused: bool =
            sqlx::query_scalar("select paused from projection_control where name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or(false);

        let backoff_until: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("select backoff_until from projection_control where name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;

        let leased_by: Option<String> =
            sqlx::query_scalar("select leased_by from projection_leases where name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;

        let lease_until: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("select lease_until from projection_leases where name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;

        let dlq_count: i64 =
            sqlx::query_scalar("select count(1) from projection_dlq where name = $1")
                .bind(name)
                .fetch_one(&self.pool)
                .await?;

        Ok(ProjectionStatus {
            name: name.to_string(),
            last_seq: last_seq.unwrap_or(0),
            paused,
            backoff_until,
            leased_by,
            lease_until,
            dlq_count,
        })
    }

    pub async fn pause(&self, name: &str) -> Result<()> {
        sqlx::query(
            r#"insert into projection_control(name, paused) values($1, true)
                on conflict (name) do update set paused = true, updated_at = now()"#,
        )
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn resume(&self, name: &str) -> Result<()> {
        sqlx::query(
            r#"insert into projection_control(name, paused, backoff_until) values($1, false, null)
                on conflict (name) do update set paused = false, backoff_until = null, updated_at = now()"#,
        )
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_checkpoint(&self, name: &str, seq: i64) -> Result<()> {
        self.persist_checkpoint(&mut self.pool.begin().await?, name, seq)
            .await
    }

    pub async fn rebuild(&self, name: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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

    async fn set_backoff(&self, name: &str, attempts: u32) -> Result<()> {
        let mut delay = self.config.base_backoff * attempts;
        if delay > self.config.max_backoff {
            delay = self.config.max_backoff;
        }
        let until = chrono::Utc::now() + chrono::Duration::from_std(delay).unwrap();
        sqlx::query(
            r#"insert into projection_control(name, backoff_until)
                values ($1, $2)
                on conflict (name) do update set backoff_until = excluded.backoff_until, updated_at = now()"#,
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
pub enum TickResult {
    Idle,
    Paused,
    Backoff,
    LeasedByOther,
    Processed { count: u32 },
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
