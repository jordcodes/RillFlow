use std::sync::Arc;

use crate::{Aggregate, AggregateRepository, Result};
use serde_json::Value;
use sqlx::PgPool;
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
        for (stream_id, _head, _snap) in rows {
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
            // re-open a short transaction for search_path on next loop
            tx = self.pool.begin().await?;
            sqlx::query(&set_search_path).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(processed)
    }

    pub async fn run_until_idle(&self) -> Result<()> {
        loop {
            let n = self.tick_once().await?;
            if n == 0 {
                return Ok(());
            }
        }
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
