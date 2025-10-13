use crate::Result;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

static CACHE: OnceLock<Mutex<HashMap<String, (Instant, Value)>>> = OnceLock::new();

#[derive(Clone)]
pub struct Live {
    pool: PgPool,
    ttl: Duration,
}

impl Live {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            ttl: Duration::from_secs(1),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub async fn query_scalar_i64_cached(&self, key: &str, sql: &str) -> Result<i64> {
        // cache lookup
        if let Ok(map) = CACHE.get_or_init(Default::default).lock() {
            if let Some((ts, val)) = map.get(key) {
                if ts.elapsed() < self.ttl {
                    if let Some(n) = val.as_i64() {
                        return Ok(n);
                    }
                }
            }
        }

        // execute query
        let n: i64 = sqlx::query_scalar(sql).fetch_one(&self.pool).await?;

        // store
        if let Ok(mut map) = CACHE.get_or_init(Default::default).lock() {
            map.insert(key.to_string(), (Instant::now(), Value::from(n)));
        }
        Ok(n)
    }

    pub fn invalidate(&self, key: &str) {
        if let Ok(mut map) = CACHE.get_or_init(Default::default).lock() {
            map.remove(key);
        }
    }
}
