use crate::{Error, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
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

pub struct Events {
    pub(crate) pool: PgPool,
    pub(crate) use_advisory_lock: bool,
}

#[derive(Clone, Debug)]
pub struct EventEnvelope {
    pub stream_id: Uuid,
    pub stream_seq: i32,
    pub typ: String,
    pub body: Value,
    pub headers: Value,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
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
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        if self.use_advisory_lock {
            // Serialize writers per stream using a transaction-scoped advisory lock
            let key = stream_id.to_string();
            sqlx::query("select pg_advisory_xact_lock(hashtext($1)::bigint)")
                .bind(&key)
                .execute(&mut *tx)
                .await?;
        }

        let current: i32 = sqlx::query_scalar::<_, Option<i32>>(
            "select max(stream_seq) from events where stream_id = $1",
        )
        .bind(stream_id)
        .fetch_one(&mut *tx)
        .await?
        .unwrap_or(0);

        match expected {
            Expected::Any => {}
            Expected::NoStream if current != 0 => return Err(Error::VersionConflict),
            Expected::Exact(value) if value != current => return Err(Error::VersionConflict),
            _ => {}
        }

        let mut seq = current;
        for event in events {
            seq += 1;
            sqlx::query(
                "insert into events (stream_id, stream_seq, event_type, body) values ($1, $2, $3, $4)"
            )
            .bind(stream_id)
            .bind(seq)
            .bind(&event.typ)
            .bind(&event.body)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
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
        if events.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let current: i32 = sqlx::query_scalar::<_, Option<i32>>(
            "select max(stream_seq) from events where stream_id = $1",
        )
        .bind(stream_id)
        .fetch_one(&mut *tx)
        .await?
        .unwrap_or(0);

        match expected {
            Expected::Any => {}
            Expected::NoStream if current != 0 => return Err(Error::VersionConflict),
            Expected::Exact(value) if value != current => return Err(Error::VersionConflict),
            _ => {}
        }

        let mut seq = current;
        for event in events {
            seq += 1;
            sqlx::query(
                r#"insert into events (stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id)
                    values ($1, $2, $3, $4, $5, $6, $7)"#,
            )
            .bind(stream_id)
            .bind(seq)
            .bind(&event.typ)
            .bind(&event.body)
            .bind(headers)
            .bind(causation_id)
            .bind(correlation_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn read_stream(&self, stream_id: Uuid) -> Result<Vec<(i32, Event)>> {
        let rows = sqlx::query(
            "select stream_seq, event_type, body from events where stream_id = $1 order by stream_seq asc"
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<i32, _>("stream_seq"),
                    Event {
                        typ: row.get::<String, _>("event_type"),
                        body: row.get::<Value, _>("body"),
                    },
                )
            })
            .collect())
    }

    pub async fn read_stream_envelopes(&self, stream_id: Uuid) -> Result<Vec<EventEnvelope>> {
        let rows = sqlx::query(
            r#"select stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, created_at
                from events where stream_id = $1 order by stream_seq asc"#,
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| EventEnvelope {
                stream_id: row.get::<Uuid, _>("stream_id"),
                stream_seq: row.get::<i32, _>("stream_seq"),
                typ: row.get::<String, _>("event_type"),
                body: row.get::<Value, _>("body"),
                headers: row.get::<Value, _>("headers"),
                causation_id: row.get::<Option<Uuid>, _>("causation_id"),
                correlation_id: row.get::<Option<Uuid>, _>("correlation_id"),
                created_at: row.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
            })
            .collect())
    }
}
