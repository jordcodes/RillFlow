use crate::{Result, documents::Documents, events::Events, projections::Projections};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub fn builder(url: impl Into<String>) -> StoreBuilder {
        StoreBuilder::new(url)
    }

    pub fn docs(&self) -> Documents {
        Documents {
            pool: self.pool.clone(),
        }
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
        Ok(Store { pool })
    }
}
