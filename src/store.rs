use crate::{
    Result,
    documents::{DocumentSession, Documents},
    events::{AppendOptions, Events},
    projections::Projections,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    session_defaults: AppendOptions,
    session_advisory_locks: bool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self {
            pool,
            session_defaults: AppendOptions::default(),
            session_advisory_locks: false,
        })
    }

    pub fn builder(url: impl Into<String>) -> StoreBuilder {
        StoreBuilder::new(url)
    }

    pub fn docs(&self) -> Documents {
        Documents {
            pool: self.pool.clone(),
        }
    }

    pub fn document_session(&self) -> DocumentSession {
        self.session_builder().build()
    }

    pub fn session_builder(&self) -> SessionBuilder {
        SessionBuilder {
            store: self.clone(),
            defaults: self.session_defaults.clone(),
            use_advisory_lock: self.session_advisory_locks,
        }
    }

    pub fn session_defaults(&self) -> (&AppendOptions, bool) {
        (&self.session_defaults, self.session_advisory_locks)
    }

    pub fn set_session_defaults(&mut self, defaults: AppendOptions, advisory_locks: bool) {
        self.session_defaults = defaults;
        self.session_advisory_locks = advisory_locks;
    }

    pub fn with_session_defaults(mut self, defaults: AppendOptions, advisory_locks: bool) -> Self {
        self.set_session_defaults(defaults, advisory_locks);
        self
    }

    pub fn events(&self) -> Events {
        Events {
            pool: self.pool.clone(),
            use_advisory_lock: false,
        }
    }

    pub fn projections(&self) -> Projections {
        Projections {
            pool: self.pool.clone(),
        }
    }

    pub fn schema(&self) -> crate::schema::SchemaManager {
        crate::schema::SchemaManager::new(self.pool.clone())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Resolve a human alias to a stream_id UUID, creating if missing.
    pub async fn resolve_stream_alias(&self, alias: &str) -> crate::Result<uuid::Uuid> {
        if let Some(id) = sqlx::query_scalar::<_, uuid::Uuid>(
            "select stream_id from stream_aliases where alias=$1",
        )
        .bind(alias)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(id);
        }
        let id = uuid::Uuid::new_v4();
        sqlx::query(
            "insert into stream_aliases(alias, stream_id) values($1,$2) on conflict (alias) do nothing",
        )
        .bind(alias)
        .bind(id)
        .execute(&self.pool)
        .await?;
        let resolved = sqlx::query_scalar::<_, uuid::Uuid>(
            "select stream_id from stream_aliases where alias=$1",
        )
        .bind(alias)
        .fetch_one(&self.pool)
        .await?;
        Ok(resolved)
    }
}

pub struct StoreBuilder {
    url: String,
    max_connections: Option<u32>,
    connect_timeout: Option<Duration>,
}

/// Builder for `DocumentSession` instances with preconfigured defaults.
pub struct SessionBuilder {
    store: Store,
    defaults: AppendOptions,
    use_advisory_lock: bool,
}

impl StoreBuilder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: None,
            connect_timeout: None,
        }
    }

    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = Some(max.max(1));
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub async fn build(self) -> Result<Store> {
        let mut opts = PgPoolOptions::new();
        if let Some(max) = self.max_connections {
            opts = opts.max_connections(max);
        }
        if let Some(t) = self.connect_timeout {
            opts = opts.acquire_timeout(t);
        }
        let pool = opts.connect(&self.url).await?;
        Ok(Store {
            pool,
            session_defaults: AppendOptions::default(),
            session_advisory_locks: false,
        })
    }
}

impl SessionBuilder {
    /// Replace default headers applied to every staged event.
    pub fn headers(mut self, headers: JsonValue) -> Self {
        self.defaults.headers = Some(headers);
        self
    }

    /// Merge additional headers into the existing defaults.
    pub fn merge_headers(mut self, headers: JsonValue) -> Self {
        self.defaults.headers = crate::documents::DocumentSession::merge_headers(
            &self.defaults.headers,
            &Some(headers),
        );
        self
    }

    /// Set a default causation id for staged events.
    pub fn causation_id(mut self, id: Option<uuid::Uuid>) -> Self {
        self.defaults.causation_id = id;
        self
    }

    /// Set a default correlation id for staged events.
    pub fn correlation_id(mut self, id: Option<uuid::Uuid>) -> Self {
        self.defaults.correlation_id = id;
        self
    }

    /// Provide an idempotency key header applied to subsequent staged events.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        let mut map = match self.defaults.headers.take() {
            Some(JsonValue::Object(m)) => m,
            _ => JsonMap::new(),
        };
        map.insert("idempotency_key".to_string(), JsonValue::String(key.into()));
        self.defaults.headers = Some(JsonValue::Object(map));
        self
    }

    /// Enable or disable advisory locks when the session flushes events.
    pub fn advisory_locks(mut self, enable: bool) -> Self {
        self.use_advisory_lock = enable;
        self
    }

    /// Build a new `DocumentSession` applying the configured defaults.
    pub fn build(self) -> DocumentSession {
        let SessionBuilder {
            store,
            defaults,
            use_advisory_lock,
        } = self;

        let mut events = store.events();
        events.use_advisory_lock = use_advisory_lock;

        let mut session = DocumentSession::new(store.pool.clone(), events);
        if let Some(headers) = defaults.headers.clone() {
            session.merge_event_headers(headers);
        }
        session.set_event_causation_id(defaults.causation_id);
        session.set_event_correlation_id(defaults.correlation_id);
        if let Some(key) = defaults
            .headers
            .as_ref()
            .and_then(|h| h.get("idempotency_key"))
            .and_then(|v| v.as_str())
        {
            session.set_event_idempotency_key(key);
        }
        session
    }
}
