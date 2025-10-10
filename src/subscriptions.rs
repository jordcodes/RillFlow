use crate::{Result, events::EventEnvelope};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgRow};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, sleep};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SubscriptionFilter {
    pub event_types: Option<Vec<String>>, // match event_type
    pub stream_ids: Option<Vec<Uuid>>,    // match specific streams
    pub stream_prefix: Option<String>,    // match stream_id::text like prefix%
}

#[derive(Clone, Debug)]
pub struct SubscriptionOptions {
    pub batch_size: i64,
    pub poll_interval: Duration,
    pub start_from: i64,
    pub channel_capacity: usize,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            batch_size: 500,
            poll_interval: Duration::from_millis(250),
            start_from: 0,
            channel_capacity: 1024,
        }
    }
}

pub struct Subscriptions {
    pool: PgPool,
}

impl Subscriptions {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create or update a subscription checkpoint and filter.
    pub async fn create_or_update(
        &self,
        name: &str,
        filter: &SubscriptionFilter,
        start_from: i64,
    ) -> Result<()> {
        let filter_json = serde_json::to_value(filter)?;
        sqlx::query(
            r#"insert into subscriptions(name, last_seq, filter)
                values ($1, $2, $3)
                on conflict (name) do update set filter = excluded.filter, updated_at = now()"#,
        )
        .bind(name)
        .bind(start_from)
        .bind(filter_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Start a polling subscription returning a bounded channel receiver of envelopes.
    pub async fn subscribe(
        &self,
        name: &str,
        filter: SubscriptionFilter,
        opts: SubscriptionOptions,
    ) -> Result<(SubscriptionHandle, mpsc::Receiver<EventEnvelope>)> {
        self.create_or_update(name, &filter, opts.start_from)
            .await?;

        let (tx, rx) = mpsc::channel::<EventEnvelope>(opts.channel_capacity);
        let pool = self.pool.clone();
        let name_s = name.to_string();
        tokio::spawn(async move {
            let mut cursor = opts.start_from;
            let mut last_checkpoint = Instant::now();
            loop {
                // read pause/backoff
                if let Ok(Some(paused)) = sqlx::query_scalar::<_, Option<bool>>(
                    "select paused from subscriptions where name = $1",
                )
                .bind(&name_s)
                .fetch_one(&pool)
                .await
                {
                    if paused {
                        sleep(opts.poll_interval).await;
                        continue;
                    }
                }

                // Build dynamic filter via CASE checks; pass arrays or nulls and rely on ANY/LIKE.

                let rows: Vec<PgRow> = sqlx::query(
                    r#"
                    with f as (
                      select $2::text[] as types,
                             $3::uuid[] as ids,
                             $4::text as prefix
                    )
                    select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, created_at
                      from events, f
                     where global_seq > $1
                       and ($2::text[] is null or event_type = any(f.types))
                       and ($3::uuid[] is null or stream_id = any(f.ids))
                       and ($4::text is null or stream_id::text like (f.prefix || '%'))
                     order by global_seq asc
                     limit $5
                    "#,
                )
                .bind(cursor)
                .bind(filter.event_types.clone())
                .bind(filter.stream_ids.clone())
                .bind(filter.stream_prefix.clone())
                .bind(opts.batch_size)
                .fetch_all(&pool)
                .await
                .unwrap_or_default();

                if rows.is_empty() {
                    sleep(opts.poll_interval).await;
                    continue;
                }

                for row in rows {
                    let env = EventEnvelope {
                        stream_id: row.get("stream_id"),
                        stream_seq: row.get("stream_seq"),
                        typ: row.get("event_type"),
                        body: row.get("body"),
                        headers: row.get("headers"),
                        causation_id: row.get("causation_id"),
                        correlation_id: row.get("correlation_id"),
                        created_at: row.get("created_at"),
                    };
                    cursor = row.get::<i64, _>("global_seq");
                    if tx.send(env).await.is_err() {
                        return; // receiver dropped
                    }
                }

                if last_checkpoint.elapsed() >= Duration::from_secs(1) {
                    let _ = sqlx::query(
                        "update subscriptions set last_seq = $2, updated_at = now() where name = $1",
                    )
                    .bind(&name_s)
                    .bind(cursor)
                    .execute(&pool)
                    .await;
                    last_checkpoint = Instant::now();
                }
            }
        });

        Ok((
            SubscriptionHandle {
                pool: self.pool.clone(),
                name: name.to_string(),
            },
            rx,
        ))
    }
}

pub struct SubscriptionHandle {
    pool: PgPool,
    name: String,
}

impl SubscriptionHandle {
    pub async fn checkpoint(&self, to_seq: i64) -> Result<()> {
        sqlx::query("update subscriptions set last_seq = $2, updated_at = now() where name = $1")
            .bind(&self.name)
            .bind(to_seq)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn pause(&self) -> Result<()> {
        sqlx::query(
            "insert into subscriptions(name, paused) values($1, true) on conflict (name) do update set paused = true, updated_at = now()",
        )
        .bind(&self.name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        sqlx::query(
            "update subscriptions set paused = false, backoff_until = null, updated_at = now() where name = $1",
        )
        .bind(&self.name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// Helper trait bound for typed bind workaround (no-op placeholder)
pub trait PostgresBind {}
impl PostgresBind for i64 {}
