use crate::{Error, Result, metrics};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum Expected {
    Any,
    NoStream,
    Exact(i32),
}

#[derive(Clone, Debug)]
pub struct Event {
    pub typ: String,
    pub body: Value,
}

impl Event {
    pub fn new<T: Serialize>(typ: impl Into<String>, body: &T) -> Self {
        Self {
            typ: typ.into(),
            body: serde_json::to_value(body).expect("failed to serialize event body"),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct AppendOptions {
    pub headers: Option<Value>,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct Events {
    pub(crate) pool: PgPool,
    pub(crate) use_advisory_lock: bool,
}

#[derive(Clone, Debug)]
pub struct EventEnvelope {
    pub global_seq: i64,
    pub stream_id: Uuid,
    pub stream_seq: i32,
    pub typ: String,
    pub body: Value,
    pub headers: Value,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub event_version: i32,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Events {
    pub fn with_advisory_locks(mut self) -> Self {
        self.use_advisory_lock = true;
        self
    }

    pub async fn append_stream(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
    ) -> Result<()> {
        self.append_with(stream_id, expected, events, &AppendOptions::default())
            .await
    }

    pub async fn append_stream_with_headers(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
        headers: &Value,
        causation_id: Option<Uuid>,
        correlation_id: Option<Uuid>,
    ) -> Result<()> {
        let opts = AppendOptions {
            headers: Some(headers.clone()),
            causation_id,
            correlation_id,
        };
        self.append_with(stream_id, expected, events, &opts).await
    }

    pub async fn read_stream(&self, stream_id: Uuid) -> Result<Vec<(i32, Event)>> {
        let rows: Vec<(i32, String, Value)> = sqlx::query_as(
            "select stream_seq, event_type, body from events where stream_id = $1 order by stream_seq asc",
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(seq, typ, body)| (seq, Event { typ, body }))
            .collect())
    }

    pub async fn read_stream_envelopes(&self, stream_id: Uuid) -> Result<Vec<EventEnvelope>> {
        #[allow(clippy::type_complexity)]
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
            Option<String>,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(
            r#"
            with combined as (
              select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, tenant_id, user_id, created_at
              from events where stream_id = $1
              union all
              select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, tenant_id, user_id, created_at
              from events_archive where stream_id = $1
            )
            select * from combined order by stream_seq asc
            "#,
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    global_seq,
                    stream_id,
                    stream_seq,
                    typ,
                    mut body,
                    headers,
                    causation_id,
                    correlation_id,
                    mut event_version,
                    tenant_id,
                    user_id,
                    created_at,
                )| {
                    // Apply upcasters if registered, chaining from current version.
                    let mut next = event_version;
                    while let Some(up) = UPCASTERS
                        .get_or_init(Default::default)
                        .lock()
                        .ok()
                        .and_then(|m| m.get(&(typ.clone(), next)).cloned())
                    {
                        match (up)(body.clone()) {
                            Ok(new_body) => {
                                body = new_body;
                                next += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    EventEnvelope {
                        global_seq,
                        stream_id,
                        stream_seq,
                        typ,
                        body,
                        headers,
                        causation_id,
                        correlation_id,
                        event_version: next,
                        tenant_id,
                        user_id,
                        created_at,
                    }
                },
            )
            .collect())
    }

    pub async fn append_with(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
        opts: &AppendOptions,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let res = self
            .append_with_tx_internal(&mut tx, stream_id, expected, &events, opts)
            .await;
        match res {
            Ok(()) => {
                tx.commit().await?;
                metrics::record_event_appends(None, events.len() as u64);
                Ok(())
            }
            Err(err) => {
                metrics::record_event_conflict(None);
                Err(err)
            }
        }
    }

    async fn append_with_tx_internal(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: Uuid,
        expected: Expected,
        events: &[Event],
        opts: &AppendOptions,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        if self.use_advisory_lock {
            let key = stream_id.to_string();
            sqlx::query("select pg_advisory_xact_lock(hashtext($1)::bigint)")
                .bind(&key)
                .execute(&mut **tx)
                .await?;
        }

        let current: i32 = sqlx::query_scalar::<_, Option<i32>>(
            "select max(stream_seq) from events where stream_id = $1",
        )
        .bind(stream_id)
        .fetch_one(&mut **tx)
        .await?
        .unwrap_or(0);

        match expected {
            Expected::Any => {}
            Expected::NoStream if current != 0 => return Err(Error::VersionConflict),
            Expected::Exact(value) if value != current => return Err(Error::VersionConflict),
            _ => {}
        }

        let mut seq = current;
        let headers = opts
            .headers
            .clone()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        for event in events {
            seq += 1;
            let res = sqlx::query(
                r#"insert into events (stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, tenant_id, user_id)
                    values ($1, $2, $3, $4, $5, $6, $7, 1, null, current_user)"#,
            )
            .bind(stream_id)
            .bind(seq)
            .bind(&event.typ)
            .bind(&event.body)
            .bind(&headers)
            .bind(opts.causation_id)
            .bind(opts.correlation_id)
            .execute(&mut **tx)
            .await;

            if let Err(e) = res {
                if let sqlx::Error::Database(db_err) = &e {
                    let msg = db_err.message().to_lowercase();
                    if msg.contains("events_idemp_key_uq")
                        || msg.contains("unique") && msg.contains("idempotency_key")
                    {
                        return Err(Error::IdempotencyConflict);
                    }
                }
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub(crate) async fn append_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: Uuid,
        expected: Expected,
        events: &[Event],
        opts: &AppendOptions,
    ) -> Result<()> {
        self.append_with_tx_internal(tx, stream_id, expected, events, opts)
            .await
    }

    pub fn builder(&self, stream_id: Uuid) -> AppendBuilder {
        AppendBuilder {
            events: Vec::new(),
            opts: AppendOptions::default(),
            expected: Expected::Any,
            stream_id,
            inner: self.clone(),
        }
    }

    pub async fn archive_stream_before(
        &self,
        stream_id: Uuid,
        before: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        // Move rows to archive
        let moved = sqlx::query(
            r#"
            insert into events_archive (global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, tenant_id, user_id, created_at)
            select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, tenant_id, user_id, created_at
            from events where stream_id = $1 and created_at < $2
            on conflict (global_seq) do nothing
            "#,
        )
        .bind(stream_id)
        .bind(before)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        // Delete moved rows from live
        let _ = sqlx::query("delete from events where stream_id = $1 and created_at < $2")
            .bind(stream_id)
            .bind(before)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(moved)
    }

    pub async fn append_tombstone(&self, stream_id: Uuid, reason: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let current: i32 = sqlx::query_scalar::<_, Option<i32>>(
            "select max(stream_seq) from events where stream_id = $1",
        )
        .bind(stream_id)
        .fetch_one(&mut *tx)
        .await?
        .unwrap_or(0);
        let next = current + 1;
        sqlx::query(
            r#"insert into events (stream_id, stream_seq, event_type, body, headers, is_tombstone, event_version, user_id)
                values ($1,$2,$3,$4,$5,true,1,current_user)"#,
        )
        .bind(stream_id)
        .bind(next)
        .bind("$tombstone")
        .bind(serde_json::json!({"reason": reason}))
        .bind(serde_json::json!({}))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

type UpcasterFn = Arc<dyn Fn(Value) -> Result<Value> + Send + Sync + 'static>;
static UPCASTERS: OnceLock<Mutex<HashMap<(String, i32), UpcasterFn>>> = OnceLock::new();

/// Register an event upcaster that transforms a given event type from a specific version to the next version.
pub fn register_upcaster(event_type: &str, from_version: i32, upcaster: UpcasterFn) {
    let map = UPCASTERS.get_or_init(Default::default);
    if let Ok(mut m) = map.lock() {
        m.insert((event_type.to_string(), from_version), upcaster);
    }
}

pub struct AppendBuilder {
    inner: Events,
    stream_id: Uuid,
    expected: Expected,
    opts: AppendOptions,
    events: Vec<Event>,
}

impl AppendBuilder {
    pub fn expected(mut self, expected: Expected) -> Self {
        self.expected = expected;
        self
    }

    pub fn headers(mut self, headers: serde_json::Value) -> Self {
        self.opts.headers = Some(headers);
        self
    }

    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        let mut h = self
            .opts
            .headers
            .take()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        if let Value::Object(ref mut m) = h {
            m.insert("idempotency_key".to_string(), Value::String(key.into()));
        }
        self.opts.headers = Some(h);
        self
    }

    pub fn causation_id(mut self, id: Uuid) -> Self {
        self.opts.causation_id = Some(id);
        self
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.opts.correlation_id = Some(id);
        self
    }

    pub fn push(mut self, event: Event) -> Self {
        self.events.push(event);
        self
    }

    pub fn extend<I: IntoIterator<Item = Event>>(mut self, iter: I) -> Self {
        self.events.extend(iter);
        self
    }

    pub async fn send(self) -> Result<()> {
        self.inner
            .append_with(self.stream_id, self.expected, self.events, &self.opts)
            .await
    }
}
