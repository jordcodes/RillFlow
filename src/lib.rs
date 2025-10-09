//! Rillflow — document + event store for Rust, powered by Postgres.

pub mod documents;
mod error;
pub mod events;
pub mod projections;
pub mod query;
pub mod store;
pub mod testing;
pub mod tracing;

pub use error::{Error, Result};
pub use events::{Event, Expected};
pub use store::Store;

pub mod prelude {
    pub use crate::{Event, Expected, Result, Store};
}
