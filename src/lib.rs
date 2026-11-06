//! Durable, physically isolated tenant databases. See `docs/format.md` for the
//! persistence contract and `docs/api.md` for query and lookup semantics.
pub mod backup;
pub mod client;
pub mod codec;
pub mod dsl;
pub mod error;
pub mod http;
pub mod identity;
mod json;
pub mod model;
pub mod query;
mod read;
pub use read::{ReadCancellation, ReadOptions, TenantReader};
pub mod replication;
pub mod resources;
pub mod storage;
pub mod telemetry;
pub mod tenant;

pub use error::{Error, Result};
pub use model::*;
pub use tenant::dataset_reads::{PublishedQuery, PublishedReadBatch};
pub use tenant::datasets::PublishedLookup as PublishedDataset;
pub use tenant::datasets::{Publication, PublishedLookup, PublishedResult};
pub use tenant::{RetentionPolicy, RetryContext, Store, TenantHandle};
