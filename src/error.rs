use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("version conflict")]
    VersionConflict,
    #[error("idempotency conflict")]
    IdempotencyConflict,
    #[error("unknown projection: {0}")]
    UnknownProjection(String),
    #[error("document not found")]
    DocNotFound,
    #[error("document version conflict")]
    DocVersionConflict,
    #[error("tenant required for this operation")]
    TenantRequired,
    #[error("tenant {0} is not provisioned")]
    TenantNotFound(String),
}

pub type Result<T> = std::result::Result<T, Error>;
