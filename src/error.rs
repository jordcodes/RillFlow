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
    #[error("constraint violation on `{constraint}`: {detail}")]
    ConstraintViolation { constraint: String, detail: String },
    #[error("query error: {context}")]
    QueryError { query: String, context: String },
    #[error("projection `{projection}` failed at event {event_seq}: {source}")]
    ProjectionError {
        projection: String,
        event_seq: i64,
        #[source]
        source: Box<Error>,
    },
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<Error>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[allow(dead_code)]
pub trait WithContext<T> {
    fn context(self, msg: impl Into<String>) -> Result<T>;
}

impl<T> WithContext<T> for Result<T> {
    fn context(self, msg: impl Into<String>) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: msg.into(),
            source: Box::new(e),
        })
    }
}
