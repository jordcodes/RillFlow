//! Rillflow — document + event store for Rust, powered by Postgres.

pub mod aggregates;
pub mod context;
pub mod documents;
mod error;
pub mod events;
pub mod metrics;
pub mod projection_runtime;
pub mod projections;
pub mod query;
pub mod schema;
pub mod snapshotter;
pub mod store;
pub mod subscriptions;
pub mod testing;
pub mod tracing;

pub use aggregates::{Aggregate, AggregateRepository, AggregateSession};
pub use context::{SessionContext, SessionContextBuilder};
pub use error::{Error, Result};
pub use events::EventEnvelope;
pub use events::{Event, Expected};
pub use schema::{SchemaConfig, SchemaManager, SchemaPlan, TenancyMode, TenantSchema};
pub use store::SessionBuilder;
pub use store::Store;

pub use documents::DocumentSession;

pub mod prelude {
    pub use crate::events::AppendOptions;
    pub use crate::store::SessionBuilder;
    pub use crate::{Event, Expected, Result, Store};
}
