use crate::{Result, events::EventEnvelope};
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, PgPool, Postgres, Row, Transaction, postgres::PgRow};
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
    pub notify_channel: Option<String>,
    pub group: Option<String>,
    pub lease_ttl: Duration,
    pub ack_mode: AckMode,
    pub max_in_flight: usize,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            batch_size: 500,
            poll_interval: Duration::from_millis(250),
            start_from: 0,
            channel_capacity: 1024,
            notify_channel: Some("rillflow_events".to_string()),
            group: None,
            lease_ttl: Duration::from_secs(30),
            ack_mode: AckMode::Auto,
            max_in_flight: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AckMode {
    Auto,
    Manual,
}

pub struct Subscriptions {
    pool: PgPool,
    schema: String,
}

impl Subscriptions {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: "public".to_string(),
        }
    }

    pub fn new_with_schema(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
        }
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
        let schema = self.schema.clone();
        let name_s = name.to_string();
        let handle_group = opts.group.clone();
        tokio::spawn(async move {
            let mut cursor = opts.start_from;
            // Acquire a dedicated connection and set search_path for consistent schema scoping
            let mut conn = match pool.acquire().await {
                Ok(c) => c,
                Err(_) => return,
            };
            let set_path = format!("set search_path to \"{}\"", schema);
            let _ = sqlx::query(&set_path).execute(&mut *conn).await;
            // Start optional listener for NOTIFY wakeups
            let mut listener = if let Some(chan) = &opts.notify_channel {
                match sqlx::postgres::PgListener::connect_with(&pool).await {
                    Ok(mut l) => {
                        let _ = l.listen(chan).await;
                        Some(l)
                    }
                    Err(_) => None,
                }
            } else {
                None
            };
            let mut last_checkpoint = Instant::now();
            let mut last_lease_refresh = Instant::now();
            // stable unique worker id for lease ownership within this process
            let worker_id = format!("pid-{}-{}", std::process::id(), Uuid::new_v4());
            // If running in group mode, initialize cursor from group checkpoint if available
            if let Some(grp) = &opts.group {
                if let Ok(Some(seq)) = sqlx::query_scalar::<_, Option<i64>>(
                    "select last_seq from subscription_groups where name=$1 and grp=$2",
                )
                .bind(&name_s)
                .bind(grp)
                .fetch_one(&mut *conn)
                .await
                {
                    cursor = seq;
                }
            }
            loop {
                // read pause/backoff
                if let Ok(Some(paused)) = sqlx::query_scalar::<_, Option<bool>>(
                    "select paused from subscriptions where name = $1",
                )
                .bind(&name_s)
                .fetch_one(&mut *conn)
                .await
                {
                    if paused {
                        sleep(opts.poll_interval).await;
                        continue;
                    }
                }

                // Acquire per-group lease if group mode is enabled
                if let Some(grp) = &opts.group {
                    let now = chrono::Utc::now();
                    let ttl = now + chrono::Duration::from_std(opts.lease_ttl).unwrap();
                    let id = &worker_id;
                    // Use an explicit transaction + advisory xact lock to serialize ownership decisions
                    let key = format!("{}:{}", name_s, grp);
                    let mut ltx = match conn.begin().await {
                        Ok(t) => t,
                        Err(_) => {
                            sleep(opts.poll_interval).await;
                            continue;
                        }
                    };
                    let got_lock: Option<bool> = sqlx::query_scalar(
                        "select pg_try_advisory_xact_lock(hashtext($1)::bigint)",
                    )
                    .bind(&key)
                    .fetch_optional(&mut *ltx)
                    .await
                    .ok()
                    .flatten();
                    if got_lock != Some(true) {
                        let _ = ltx.rollback().await;
                        sleep(opts.poll_interval).await;
                        continue;
                    }

                    // Read existing lease under the lock
                    let existing: Option<(String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                        "select leased_by, lease_until from subscription_group_leases where name=$1 and grp=$2 for update",
                    )
                    .bind(&name_s)
                    .bind(grp)
                    .fetch_optional(&mut *ltx)
                    .await
                    .ok()
                    .flatten();

                    let can_take = match existing {
                        None => true,
                        Some((ref who, _until)) if id == who => true,
                        Some((_who, until)) if until <= now => true,
                        _ => false,
                    };

                    if !can_take {
                        let _ = ltx.rollback().await;
                        sleep(opts.poll_interval).await;
                        continue;
                    }

                    // Upsert our lease
                    let _ = sqlx::query(
                        r#"
                        insert into subscription_group_leases(name, grp, leased_by, lease_until)
                        values ($1,$2,$3,$4)
                        on conflict (name, grp) do update set leased_by=excluded.leased_by, lease_until=excluded.lease_until, updated_at=now()
                        "#,
                    )
                    .bind(&name_s)
                    .bind(grp)
                        .bind(id)
                    .bind(ttl)
                    .execute(&mut *ltx)
                    .await;
                    let _ = ltx.commit().await;
                }

                // Build dynamic filter via CASE checks; pass arrays or nulls and rely on ANY/LIKE.

                let limit = std::cmp::min(opts.batch_size, opts.max_in_flight as i64);
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
                .bind(limit)
                .fetch_all(&mut *conn)
                .await
                .unwrap_or_default();

                if rows.is_empty() {
                    if let Some(l) = &mut listener {
                        // wait for a notify with a timeout fallback
                        let _ = tokio::time::timeout(opts.poll_interval, l.recv()).await;
                    } else {
                        sleep(opts.poll_interval).await;
                    }
                    continue;
                }

                let mut last_seq_in_batch = cursor;
                for row in rows {
                    let env = EventEnvelope {
                        global_seq: row.get("global_seq"),
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
                    last_seq_in_batch = cursor;
                    if tx.send(env).await.is_err() {
                        return; // receiver dropped
                    }
                }

                // In group mode with auto ack, checkpoint immediately to minimize duplicate work
                if let Some(grp) = &opts.group {
                    if matches!(opts.ack_mode, AckMode::Auto) {
                        let _ = sqlx::query(
                            "insert into subscription_groups(name, grp, last_seq) values($1,$2,$3)
                             on conflict(name,grp) do update set last_seq = excluded.last_seq, updated_at = now()",
                        )
                        .bind(&name_s)
                        .bind(grp)
                        .bind(last_seq_in_batch)
                        .execute(&mut *conn)
                        .await;
                    }
                }

                if last_checkpoint.elapsed() >= Duration::from_secs(1) {
                    if let Some(grp) = &opts.group {
                        if matches!(opts.ack_mode, AckMode::Auto) {
                            let _ = sqlx::query(
                                "insert into subscription_groups(name, grp, last_seq) values($1,$2,$3)
                                 on conflict(name,grp) do update set last_seq = excluded.last_seq, updated_at = now()",
                            )
                            .bind(&name_s)
                            .bind(grp)
                            .bind(cursor)
                            .execute(&mut *conn)
                            .await;
                        }
                    } else if matches!(opts.ack_mode, AckMode::Auto) {
                        let _ = sqlx::query(
                            "update subscriptions set last_seq = $2, updated_at = now() where name = $1",
                        )
                        .bind(&name_s)
                        .bind(cursor)
                        .execute(&mut *conn)
                        .await;
                    }
                    last_checkpoint = Instant::now();
                }

                if last_lease_refresh.elapsed() >= Duration::from_secs(5) {
                    if let Some(grp) = &opts.group {
                        let ttl = chrono::Utc::now()
                            + chrono::Duration::from_std(opts.lease_ttl).unwrap();
                        let _ = sqlx::query(
                            "update subscription_group_leases set lease_until=$3, updated_at=now() where name=$1 and grp=$2",
                        )
                        .bind(&name_s)
                        .bind(grp)
                        .bind(ttl)
                        .execute(&mut *conn)
                        .await;
                    }
                    last_lease_refresh = Instant::now();
                }
            }
        });

        Ok((
            SubscriptionHandle {
                pool: self.pool.clone(),
                name: name.to_string(),
                group: handle_group,
            },
            rx,
        ))
    }
}

pub struct SubscriptionHandle {
    pool: PgPool,
    name: String,
    group: Option<String>,
}

impl SubscriptionHandle {
    pub async fn checkpoint(&self, to_seq: i64) -> Result<()> {
        match &self.group {
            Some(grp) => {
                sqlx::query(
                    "insert into subscription_groups(name, grp, last_seq) values($1,$2,$3)
                     on conflict(name,grp) do update set last_seq = excluded.last_seq, updated_at = now()",
                )
                .bind(&self.name)
                .bind(grp)
                .bind(to_seq)
                .execute(&self.pool)
                .await?;
            }
            None => {
                sqlx::query(
                    "update subscriptions set last_seq = $2, updated_at = now() where name = $1",
                )
                .bind(&self.name)
                .bind(to_seq)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn ack(&self, to_seq: i64) -> Result<()> {
        self.checkpoint(to_seq).await
    }

    pub async fn ack_in_tx(&self, tx: &mut Transaction<'_, Postgres>, to_seq: i64) -> Result<()> {
        match &self.group {
            Some(grp) => {
                sqlx::query(
                    "insert into subscription_groups(name, grp, last_seq) values($1,$2,$3)
                     on conflict(name,grp) do update set last_seq = excluded.last_seq, updated_at = now()",
                )
                .bind(&self.name)
                .bind(grp)
                .bind(to_seq)
                .execute(&mut **tx)
                .await?;
            }
            None => {
                sqlx::query(
                    "update subscriptions set last_seq = $2, updated_at = now() where name = $1",
                )
                .bind(&self.name)
                .bind(to_seq)
                .execute(&mut **tx)
                .await?;
            }
        }
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
