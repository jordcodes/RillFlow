use crate::{Result, documents::Documents, events::Events, projections::Projections};
use sqlx::PgPool;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
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
}
