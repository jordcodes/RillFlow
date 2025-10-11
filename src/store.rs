use crate::{
    Result,
    context::{SessionContext, SessionContextBuilder},
    documents::{DocumentSession, Documents},
    events::{AppendOptions, Events},
    projections::Projections,
    schema::{SchemaConfig, TenancyMode, TenantSchema},
    subscriptions::Subscriptions,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::str::FromStr;
use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::time::sleep;

type TenantResolver = Arc<dyn Fn() -> Option<String> + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantStrategy {
    Single,
    SchemaPerTenant,
}

pub(crate) fn tenant_schema_name(tenant: &str) -> String {
    let mut normalized = String::with_capacity(tenant.len());
    for ch in tenant.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        normalized.push('_');
    }
    format!("tenant_{}", normalized)
}

fn tenant_lock_key(schema: &str) -> i64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in schema.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    // Clamp to positive i64 so advisory locks stay within valid range.
    (hash & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    session_defaults: AppendOptions,
    session_advisory_locks: bool,
    session_context: SessionContext,
    tenant_strategy: TenantStrategy,
    ensured_tenants: Arc<RwLock<HashSet<String>>>,
    tenant_resolver: Option<TenantResolver>,
    enforce_tenant: bool,
    prepared_statement_cache_size: Option<usize>,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self {
            pool,
            session_defaults: AppendOptions::default(),
            session_advisory_locks: false,
            session_context: SessionContext::default(),
            tenant_strategy: TenantStrategy::Single,
            ensured_tenants: Arc::new(RwLock::new(HashSet::new())),
            tenant_resolver: None,
            enforce_tenant: false,
            prepared_statement_cache_size: None,
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

    /// Obtain a `DocumentSession` using the store's current session defaults.
    pub fn session(&self) -> DocumentSession {
        self.session_builder().build()
    }

    #[deprecated(note = "Use Store::session() instead")] // maintain backward compatibility
    pub fn document_session(&self) -> DocumentSession {
        self.session()
    }

    pub fn session_builder(&self) -> SessionBuilder {
        SessionBuilder {
            store: self.clone(),
            defaults: self.session_defaults.clone(),
            use_advisory_lock: self.session_advisory_locks,
            context: self.session_context.clone(),
            tenant_strategy: self.tenant_strategy,
            ensured_tenants: self.ensured_tenants.clone(),
            tenant_resolver: self.tenant_resolver.clone(),
            enforce_tenant: self.enforce_tenant,
        }
    }

    /// Current session defaults (headers + advisory lock flag).
    /// Defaults are applied to `store.session()` and `store.session_builder()`.
    pub fn session_defaults(&self) -> (&AppendOptions, bool) {
        (&self.session_defaults, self.session_advisory_locks)
    }

    /// Override session defaults for new sessions created after this call.
    /// Prefer configuring defaults through [`StoreBuilder::session_defaults`] at startup so
    /// clones of `Store` share the same settings without requiring runtime mutation.
    pub fn set_session_defaults(&mut self, defaults: AppendOptions, advisory_locks: bool) {
        self.session_defaults = defaults;
        self.session_advisory_locks = advisory_locks;
    }

    /// Consume the builder while setting session defaults before creating the store.
    pub fn with_session_defaults(mut self, defaults: AppendOptions, advisory_locks: bool) -> Self {
        self.set_session_defaults(defaults, advisory_locks);
        self
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.session_context
    }

    pub async fn ensure_tenant(&self, tenant: &str) -> Result<()> {
        match self.tenant_strategy {
            TenantStrategy::Single => Ok(()),
            TenantStrategy::SchemaPerTenant => {
                let schema = tenant_schema_name(tenant);

                if self
                    .ensured_tenants
                    .read()
                    .expect("tenant cache poisoned")
                    .contains(&schema)
                {
                    return Ok(());
                }

                let lock_key = tenant_lock_key(&schema);
                let mut conn = self.pool.acquire().await?;
                let mut backoff = Duration::from_millis(50);

                loop {
                    let acquired: bool = sqlx::query_scalar("select pg_try_advisory_lock($1)")
                        .bind(lock_key)
                        .fetch_one(&mut *conn)
                        .await?;

                    if acquired {
                        break;
                    }

                    drop(conn);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                    conn = self.pool.acquire().await?;
                }

                let sync_result = async {
                    let config = SchemaConfig {
                        base_schema: "public".into(),
                        tenancy_mode: TenancyMode::SchemaPerTenant {
                            tenants: vec![TenantSchema::new(&schema)],
                        },
                    };
                    self.schema().sync(&config).await
                }
                .await;

                let unlock_result = sqlx::query("select pg_advisory_unlock($1)")
                    .bind(lock_key)
                    .execute(&mut *conn)
                    .await;

                drop(conn);

                sync_result?;
                unlock_result?;

                self.ensured_tenants
                    .write()
                    .expect("tenant cache poisoned")
                    .insert(schema);

                Ok(())
            }
        }
    }

    pub async fn drop_tenant(&self, tenant: &str) -> Result<()> {
        match self.tenant_strategy {
            TenantStrategy::Single => Err(crate::Error::TenantNotFound(tenant.to_string())),
            TenantStrategy::SchemaPerTenant => {
                let schema = tenant_schema_name(tenant);
                let stmt = format!(
                    "drop schema if exists {} cascade",
                    crate::schema::quote_ident(&schema)
                );
                sqlx::query(&stmt).execute(&self.pool).await?;
                self.forget_tenant_schema(&schema);
                Ok(())
            }
        }
    }

    pub async fn tenant_exists(&self, tenant: &str) -> Result<bool> {
        let schema = tenant_schema_name(tenant);
        let exists: bool = sqlx::query_scalar(
            "select exists (select 1 from information_schema.schemata where schema_name = $1)",
        )
        .bind(&schema)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub fn tenant_strategy(&self) -> TenantStrategy {
        self.tenant_strategy
    }

    pub fn tenant_resolver(&self) -> Option<&TenantResolver> {
        self.tenant_resolver.as_ref()
    }

    pub fn forget_tenant_schema(&self, schema: &str) {
        if let Ok(mut cache) = self.ensured_tenants.write() {
            cache.remove(schema);
        }
    }

    pub fn forget_tenant(&self, tenant: &str) {
        let schema = tenant_schema_name(tenant);
        self.forget_tenant_schema(&schema);
    }

    pub fn events(&self) -> Events {
        Events {
            pool: self.pool.clone(),
            use_advisory_lock: false,
        }
    }

    pub fn projections(&self) -> Projections {
        Projections::new(
            self.pool.clone(),
            self.tenant_strategy,
            self.tenant_resolver.clone(),
        )
    }

    pub fn subscriptions(&self) -> Subscriptions {
        Subscriptions::new_with_strategy(
            self.pool.clone(),
            self.tenant_strategy,
            self.tenant_resolver.clone(),
        )
    }

    pub fn schema(&self) -> crate::schema::SchemaManager {
        crate::schema::SchemaManager::new(self.pool.clone())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Lightweight liveness check for the connection pool.
    pub async fn pool_health(&self) -> crate::Result<PoolHealth> {
        let one: i32 = sqlx::query_scalar("select 1").fetch_one(&self.pool).await?;
        Ok(PoolHealth { ok: one == 1 })
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
    session_defaults: AppendOptions,
    session_advisory_locks: bool,
    session_context_builder: SessionContextBuilder,
    tenant_strategy: TenantStrategy,
    tenant_resolver: Option<TenantResolver>,
    enforce_tenant: bool,
    prepared_statement_cache_size: Option<usize>,
}

/// Builder for `DocumentSession` instances with preconfigured defaults.
pub struct SessionBuilder {
    store: Store,
    defaults: AppendOptions,
    use_advisory_lock: bool,
    context: SessionContext,
    tenant_strategy: TenantStrategy,
    ensured_tenants: Arc<RwLock<HashSet<String>>>,
    tenant_resolver: Option<TenantResolver>,
    enforce_tenant: bool,
}

impl StoreBuilder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: None,
            connect_timeout: None,
            session_defaults: AppendOptions::default(),
            session_advisory_locks: false,
            session_context_builder: SessionContext::builder(),
            tenant_strategy: TenantStrategy::Single,
            tenant_resolver: None,
            enforce_tenant: true,
            prepared_statement_cache_size: None,
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

    pub fn session_defaults(mut self, defaults: AppendOptions) -> Self {
        self.session_defaults = defaults;
        self
    }

    pub fn session_advisory_locks(mut self, enable: bool) -> Self {
        self.session_advisory_locks = enable;
        self
    }

    pub fn session_context(mut self, builder: SessionContextBuilder) -> Self {
        self.session_context_builder = builder;
        self
    }

    pub fn tenant_resolver<F>(mut self, resolver: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        self.tenant_resolver = Some(Arc::new(resolver));
        self
    }

    pub fn tenant_strategy(mut self, strategy: TenantStrategy) -> Self {
        self.tenant_strategy = strategy;
        if matches!(self.tenant_strategy, TenantStrategy::SchemaPerTenant) {
            self.enforce_tenant = true;
        }
        self
    }

    /// Hint for prepared statement cache size. Actual behavior depends on driver.
    pub fn prepared_statement_cache_size(mut self, size: usize) -> Self {
        self.prepared_statement_cache_size = Some(size.max(1));
        self
    }

    /// Allow creating sessions without a resolver or explicit tenant (system jobs only).
    pub fn allow_missing_tenant(mut self) -> Self {
        self.enforce_tenant = false;
        self
    }

    pub async fn build(self) -> Result<Store> {
        if matches!(self.tenant_strategy, TenantStrategy::SchemaPerTenant)
            && self.tenant_resolver.is_none()
            && self.enforce_tenant
        {
            panic!(
                "StoreBuilder requires tenant_resolver or allow_missing_tenant() when TenantStrategy::SchemaPerTenant is configured."
            );
        }

        let mut opts = PgPoolOptions::new();
        if let Some(max) = self.max_connections {
            opts = opts.max_connections(max);
        }
        if let Some(t) = self.connect_timeout {
            opts = opts.acquire_timeout(t);
        }
        // Configure prepared statement cache if requested
        let pool = if let Some(cap) = self.prepared_statement_cache_size {
            let mut connect_opts = PgConnectOptions::from_str(&self.url)
                .map_err(|e| sqlx::Error::Configuration(Box::new(e)))?;
            connect_opts = connect_opts.statement_cache_capacity(cap);
            opts.connect_with(connect_opts).await?
        } else {
            opts.connect(&self.url).await?
        };
        Ok(Store {
            pool,
            session_defaults: self.session_defaults,
            session_advisory_locks: self.session_advisory_locks,
            session_context: self.session_context_builder.build(),
            tenant_strategy: self.tenant_strategy,
            ensured_tenants: Arc::new(RwLock::new(HashSet::new())),
            tenant_resolver: self.tenant_resolver.clone(),
            enforce_tenant: self.enforce_tenant,
            prepared_statement_cache_size: self.prepared_statement_cache_size,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PoolHealth {
    pub ok: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PoolMetrics {
    pub total: i64,
    pub active: i64,
    pub idle: i64,
}

impl Store {
    /// Query Postgres for per-database connection counts (active/idle) and update gauges.
    pub async fn pool_metrics(&self) -> crate::Result<PoolMetrics> {
        let row: (i64, i64) = sqlx::query_as(
            r#"select
                    count(*) filter (where state = 'active') as active,
                    count(*) filter (where state = 'idle')   as idle
                from pg_stat_activity
                where datname = current_database()
                  and usename = current_user"#,
        )
        .fetch_one(&self.pool)
        .await?;
        let (active, idle) = row;
        let total = active + idle;
        crate::metrics::record_pool_gauges(total as u64, active as u64, idle as u64);
        Ok(PoolMetrics {
            total,
            active,
            idle,
        })
    }

    /// Convenience: update gauges and return liveness.
    pub async fn update_pool_metrics(&self) -> crate::Result<PoolMetrics> {
        self.pool_metrics().await
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
        let resolver_present = self.tenant_resolver.is_some();

        let mut events = self.store.events();
        events.use_advisory_lock = self.use_advisory_lock;

        let mut session = DocumentSession::new(self.store.pool.clone(), events, self.context);
        session.set_tenant_strategy(self.tenant_strategy);
        session.set_tenant_cache(self.ensured_tenants);
        session.set_tenant_resolver(self.tenant_resolver);

        if self.enforce_tenant
            && matches!(self.tenant_strategy, TenantStrategy::SchemaPerTenant)
            && session.context().tenant.is_none()
            && !resolver_present
        {
            panic!(
                "DocumentSession requires tenant context or tenant_resolver (call allow_missing_tenant for system jobs) when using TenantStrategy::SchemaPerTenant."
            );
        }

        if let Some(headers) = self.defaults.headers.clone() {
            session.merge_event_headers(headers);
        }
        session.set_event_causation_id(self.defaults.causation_id);
        session.set_event_correlation_id(self.defaults.correlation_id);
        if let Some(key) = self
            .defaults
            .headers
            .as_ref()
            .and_then(|h| h.get("idempotency_key"))
            .and_then(|v| v.as_str())
        {
            session.set_event_idempotency_key(key);
        }
        session
    }

    /// Provide a tenant resolver closure (e.g. per-request metadata) for schema-per-tenant sessions.
    pub fn tenant_resolver<F>(mut self, resolver: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        self.tenant_resolver = Some(Arc::new(resolver));
        self
    }

    /// Allow creating a session without an immediate tenant (system jobs only).
    pub fn allow_missing_tenant(mut self) -> Self {
        self.enforce_tenant = false;
        self
    }
}
