use std::sync::Arc;

use crate::{Aggregate, AggregateRepository, Result};
use serde_json::Value;
use sqlx::PgPool;
use tracing::{info, instrument};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct SnapshotterConfig {
    pub threshold_events: i32,
    pub batch_size: i64,
    pub schema: String,
}

impl Default for SnapshotterConfig {
    fn default() -> Self {
        Self {
            threshold_events: 100,
            batch_size: 100,
            schema: "public".to_string(),
        }
    }
}

#[async_trait::async_trait]
pub trait SnapshotFolder: Send + Sync {
    async fn fold(&self, stream_id: Uuid) -> Result<(i32, Value)>;
}

pub struct AggregateFolder<A: Aggregate + serde::Serialize + Send + Sync + 'static> {
    repo: AggregateRepository,
    _phantom: std::marker::PhantomData<A>,
}

impl<A: Aggregate + serde::Serialize + Send + Sync + 'static> AggregateFolder<A> {
    pub fn new(repo: AggregateRepository) -> Self {
        Self {
            repo,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<A: Aggregate + serde::Serialize + Send + Sync + 'static> SnapshotFolder
    for AggregateFolder<A>
{
    async fn fold(&self, stream_id: Uuid) -> Result<(i32, Value)> {
        let agg: A = self.repo.load(stream_id).await?;
        let version = agg.version();
        let body = serde_json::to_value(&agg)?;
        Ok((version, body))
    }
}

pub struct Snapshotter<F: SnapshotFolder> {
    pool: PgPool,
    folder: Arc<F>,
    config: SnapshotterConfig,
}

impl<F: SnapshotFolder> Snapshotter<F> {
    pub fn new(pool: PgPool, folder: Arc<F>, config: SnapshotterConfig) -> Self {
        Self {
            pool,
            folder,
            config,
        }
    }

    /// Find streams exceeding the snapshot threshold and write snapshots for a batch.
    #[instrument(skip_all, fields(schema = %self.config.schema, threshold = self.config.threshold_events, batch = self.config.batch_size))]
    pub async fn tick_once(&self) -> Result<u32> {
        let mut tx = self.pool.begin().await?;
        let set_search_path = format!(
            "set local search_path to {}",
            quote_ident(&self.config.schema)
        );
        sqlx::query(&set_search_path).execute(&mut *tx).await?;

        let rows: Vec<(Uuid, i32, i32)> = sqlx::query_as(
            r#"
            select e.stream_id,
                   max(e.stream_seq) as head,
                   coalesce(s.version, 0) as snap
              from events e
              left join snapshots s on s.stream_id = e.stream_id
             group by e.stream_id, s.version
            having max(e.stream_seq) - coalesce(s.version, 0) >= $1
             limit $2
            "#,
        )
        .bind(self.config.threshold_events)
        .bind(self.config.batch_size)
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let mut processed = 0u32;
        for (stream_id, head, snap_ver) in rows {
            // outside tx to avoid long-held transaction during folding
            tx.commit().await?;
            let (version, body) = self.folder.fold(stream_id).await?;
            sqlx::query(
                r#"insert into snapshots(stream_id, version, body)
                    values ($1, $2, $3)
                    on conflict (stream_id) do update set version = excluded.version, body = excluded.body, created_at = now()"#,
            )
            .bind(stream_id)
            .bind(version)
            .bind(body)
            .execute(&self.pool)
            .await?;
            processed += 1;
            info!(stream_id = %stream_id, head = head, snap_version = snap_ver, new_version = version, "snapshot written");
            // re-open a short transaction for search_path on next loop
            tx = self.pool.begin().await?;
            sqlx::query(&set_search_path).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(processed)
    }

    #[instrument(skip_all, fields(schema = %self.config.schema))]
    pub async fn run_until_idle(&self) -> Result<()> {
        loop {
            let n = self.tick_once().await?;
            if n == 0 {
                return Ok(());
            }
        }
    }

    pub async fn metrics(&self) -> Result<SnapshotterMetrics> {
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
        .bind(self.config.threshold_events)
        .fetch_one(&self.pool)
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
        .fetch_one(&self.pool)
        .await?;

        let out = SnapshotterMetrics {
            candidates,
            max_gap: max_gap.unwrap_or(0).max(0),
        };
        crate::metrics::metrics()
            .snapshot_candidates_gauge
            .store(out.candidates as u64, std::sync::atomic::Ordering::Relaxed);
        crate::metrics::metrics()
            .snapshot_max_gap_gauge
            .store(out.max_gap as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(out)
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

#[derive(Clone, Debug)]
pub struct SnapshotterMetrics {
    pub candidates: i64,
    pub max_gap: i32,
}
