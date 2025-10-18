//! Rillflow — document + event store for Rust, powered by Postgres.

pub mod aggregates;
pub mod batch;
pub mod context;
pub mod documents;
mod error;
pub mod events;
pub mod live;
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
pub mod upcasting;

pub use aggregates::{Aggregate, AggregateRepository, AggregateSession};
pub use context::{SessionContext, SessionContextBuilder};
pub use documents::DocumentSession;
pub use error::{Error, Result};
pub use events::{AppendOptions, ArchiveBackend, Event, EventEnvelope, Expected};
pub use schema::{
    SchemaConfig, SchemaManager, SchemaPlan, TenancyMode, TenantColumn, TenantColumnType,
    TenantSchema,
};
pub use store::SessionBuilder;
pub use store::Store;
pub use upcasting::{
    AsyncUpcaster, AsyncUpcasterBuilder, ClosureAsyncUpcaster, ClosureUpcaster, Upcaster,
    UpcasterBuilder, UpcasterDescriptor, UpcasterKind, UpcasterRegistry,
};
pub use upcasting_derive::Upcaster;

pub mod prelude {
    pub use crate::events::{AppendOptions, ArchiveBackend};
    pub use crate::store::SessionBuilder;
    pub use crate::{
        Event, Expected, Result, Store,
        upcasting::{AsyncUpcaster, Upcaster, UpcasterDescriptor, UpcasterKind, UpcasterRegistry},
    };
}
