use crate::{
    Result,
    events::{Event, EventEnvelope, Events, Expected},
};
use uuid::Uuid;

/// Minimal aggregate trait folded from event envelopes.
pub trait Aggregate: Sized {
    /// Construct a new empty aggregate.
    fn new() -> Self;
    /// Apply a single event envelope to mutate internal state.
    fn apply(&mut self, envelope: &EventEnvelope);
    /// Current aggregate version (number of applied stream events).
    fn version(&self) -> i32;
}

/// Repository for loading and committing event-sourced aggregates.
pub struct AggregateRepository {
    events: Events,
}

impl AggregateRepository {
    pub fn new(events: Events) -> Self {
        Self { events }
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
