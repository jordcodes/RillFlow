use crate::{
    Result,
    documents::DocumentSession,
    events::{AppendOptions, Event, EventEnvelope, Events, Expected},
};
use serde_json::{Map as JsonMap, Value};
use uuid::Uuid;

/// Minimal aggregate trait folded from event envelopes.
pub trait Aggregate: Sized {
    /// Construct a new empty aggregate.
    fn new() -> Self;
    /// Apply a single event envelope to mutate internal state.
    fn apply(&mut self, envelope: &EventEnvelope);
    /// Current aggregate version (number of applied stream events).
    fn version(&self) -> i32;
    /// Hydrate state from a snapshot body (optional; default no-op).
    fn hydrate(&mut self, _snapshot: &serde_json::Value) {}
}

/// Repository for loading and committing event-sourced aggregates.
pub struct AggregateRepository {
    pub(crate) events: Events,
    pub(crate) validator: Option<fn(&serde_json::Value) -> crate::Result<()>>,
}

pub struct AggregateSession<'a> {
    repo: &'a AggregateRepository,
    session: &'a mut DocumentSession,
}

impl<'a> AggregateSession<'a> {
    pub(crate) fn new(repo: &'a AggregateRepository, session: &'a mut DocumentSession) -> Self {
        Self { repo, session }
    }

    pub async fn load<A: Aggregate>(&self, stream_id: Uuid) -> Result<A> {
        self.repo.load(stream_id).await
    }

    pub async fn load_with_snapshot<A: Aggregate>(&self, stream_id: Uuid) -> Result<A> {
        self.repo.load_with_snapshot(stream_id).await
    }

    pub fn commit(
        &mut self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
    ) -> Result<()> {
        self.session
            .enqueue_aggregate(stream_id, expected, events, None)
    }

    pub fn commit_with_options(
        &mut self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
        overrides: AppendOptions,
    ) -> Result<()> {
        self.session
            .enqueue_aggregate(stream_id, expected, events, Some(overrides))
    }

    pub fn commit_for<A: Aggregate>(
        &mut self,
        stream_id: Uuid,
        aggregate: &A,
        events: Vec<Event>,
    ) -> Result<()> {
        self.commit(stream_id, Expected::Exact(aggregate.version()), events)
    }

    pub fn commit_and_snapshot<A: Aggregate + serde::Serialize + Clone>(
        &mut self,
        stream_id: Uuid,
        aggregate: &A,
        events: Vec<Event>,
        snapshot_every: i32,
    ) -> Result<()> {
        let event_count = events.len() as i32;
        if event_count == 0 {
            return Ok(());
        }

        let expected = Expected::Exact(aggregate.version());

        let should_snapshot = snapshot_every > 0;
        let mut new_state = if should_snapshot || self.repo.validator.is_some() {
            Some(aggregate.clone())
        } else {
            None
        };

        if let Some(state) = new_state.as_mut() {
            let mut seq = aggregate.version();
            for event in &events {
                seq += 1;
                let envelope = EventEnvelope {
                    global_seq: 0,
                    stream_id,
                    stream_seq: seq,
                    typ: event.typ.clone(),
                    body: event.body.clone(),
                    headers: Value::Object(JsonMap::new()),
                    causation_id: None,
                    correlation_id: None,
                    event_version: 1,
                    tenant_id: None,
                    user_id: None,
                    created_at: chrono::Utc::now(),
                };
                state.apply(&envelope);
            }
            if let Some(v) = self.repo.validator {
                v(&serde_json::to_value(state)?)?;
            }
        }

        let event_len = events.len() as i32;
        let new_version = aggregate.version() + event_len;

        self.session
            .enqueue_aggregate(stream_id, expected, events, None)?;

        if should_snapshot && new_version % snapshot_every == 0 {
            let body = serde_json::to_value(new_state.expect("snapshot state"))?;
            self.session.enqueue_snapshot(stream_id, new_version, body);
        }

        Ok(())
    }
}

impl AggregateRepository {
    pub fn new(events: Events) -> Self {
        Self {
            events,
            validator: None,
        }
    }

    pub fn events(&self) -> &Events {
        &self.events
    }

    pub fn with_validator(
        mut self,
        validator: fn(&serde_json::Value) -> crate::Result<()>,
    ) -> Self {
        self.validator = Some(validator);
        self
    }

    /// Load an aggregate by folding its stream envelopes in order.
    pub async fn load<A: Aggregate>(&self, stream_id: Uuid) -> Result<A> {
        let envelopes = self.events.read_stream_envelopes(stream_id).await?;
        let mut agg = A::new();
        for env in &envelopes {
            agg.apply(env);
        }
        Ok(agg)
    }

    /// Commit new events with explicit expected version.
    pub async fn commit(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
    ) -> Result<()> {
        self.events.append_stream(stream_id, expected, events).await
    }

    /// Commit using the aggregate's current version as Expected::Exact(version).
    pub async fn commit_for_aggregate<A: Aggregate>(
        &self,
        stream_id: Uuid,
        aggregate: &A,
        events: Vec<Event>,
    ) -> Result<()> {
        self.commit(stream_id, Expected::Exact(aggregate.version()), events)
            .await
    }
}

impl AggregateRepository {
    /// Load an aggregate using a snapshot when available, then tail events.
    pub async fn load_with_snapshot<A: Aggregate>(&self, stream_id: Uuid) -> Result<A> {
        let snap: Option<(i32, serde_json::Value)> =
            sqlx::query_as("select version, body from snapshots where stream_id = $1")
                .bind(stream_id)
                .fetch_optional(&self.events.pool)
                .await?;

        let mut agg = A::new();
        let mut from_version = 0;
        if let Some((version, body)) = snap {
            agg.hydrate(&body);
            from_version = version;
        }

        let envelopes = self.events.read_stream_envelopes(stream_id).await?;
        for env in envelopes
            .into_iter()
            .filter(|e| e.stream_seq > from_version)
        {
            agg.apply(&env);
        }
        Ok(agg)
    }

    /// Commit events and optionally write a snapshot when threshold divides version.
    pub async fn commit_and_snapshot<A: Aggregate + serde::Serialize>(
        &self,
        stream_id: Uuid,
        aggregate: &A,
        events: Vec<Event>,
        snapshot_every: i32,
    ) -> Result<()> {
        let new_version = aggregate.version() + events.len() as i32;
        if let Some(v) = self.validator {
            let snapshot = serde_json::to_value(aggregate)?;
            v(&snapshot)?;
        }
        self.commit_for_aggregate(stream_id, aggregate, events)
            .await?;
        if new_version % snapshot_every == 0 {
            // Load latest state to snapshot the post-commit aggregate
            let latest: A = self.load(stream_id).await?;
            let body = serde_json::to_value(&latest)?;
            sqlx::query(
                r#"insert into snapshots(stream_id, version, body)
                    values ($1, $2, $3)
                    on conflict (stream_id) do update set version = excluded.version, body = excluded.body, created_at = now()"#,
            )
            .bind(stream_id)
            .bind(new_version)
            .bind(&body)
            .execute(&self.events.pool)
            .await?;
        }
        Ok(())
    }
}
