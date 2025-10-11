use crate::{
    Error, Result,
    aggregates::{AggregateRepository, AggregateSession},
    events::{AppendOptions, Event, Events, Expected},
    metrics,
    query::{CompiledQuery, DocumentQuery, DocumentQueryContext},
    schema,
    store::{TenantStrategy, tenant_schema_name},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{Acquire, PgPool, Postgres, QueryBuilder, Transaction, types::Json};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub struct Documents {
    pub(crate) pool: PgPool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DocumentMetadata {
    pub id: Uuid,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_by: Option<String>,
    pub last_modified_by: Option<String>,
}

impl Documents {
    pub async fn upsert<T: Serialize>(&self, id: &Uuid, doc: &T) -> Result<i32> {
        let json = serde_json::to_value(doc)?;
        let version: i32 = sqlx::query_scalar(
            r#"
            with up as (
                insert into docs (id, doc, version)
                values ($1, $2, 1)
                on conflict (id) do update
                  set doc = excluded.doc,
                      version = docs.version + 1,
                      updated_at = now()
                returning version
            ) select version from up
            "#,
        )
        .bind(id)
        .bind(&json)
        .fetch_one(&self.pool)
        .await?;
        Ok(version)
    }

    pub async fn get<T: DeserializeOwned>(&self, id: &Uuid) -> Result<Option<(T, i32)>> {
        let row: Option<(Value, i32, Option<chrono::DateTime<chrono::Utc>>)> =
            sqlx::query_as("select doc, version, deleted_at from docs where id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        if let Some((value, version, deleted_at)) = row {
            if deleted_at.is_some() {
                return Ok(None);
            }
            let doc: T = serde_json::from_value(value)?;
            metrics::record_doc_read(Some("public"));
            Ok(Some((doc, version)))
        } else {
            Ok(None)
        }
    }

    pub async fn get_with_metadata<T: DeserializeOwned>(&self, id: &Uuid) -> Result<Option<(T, DocumentMetadata)>> {
        let row: Option<(Value, i32, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>, Option<chrono::DateTime<chrono::Utc>>, Option<String>, Option<String>)> =
            sqlx::query_as(
                "select doc, version, created_at, updated_at, deleted_at, created_by, last_modified_by from docs where id = $1",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some((value, version, created_at, updated_at, deleted_at, created_by, last_modified_by)) => {
                if deleted_at.is_some() {
                    return Ok(None);
                }
                let doc: T = serde_json::from_value(value)?;
                let meta = DocumentMetadata {
                    id: *id,
                    version,
                    created_at,
                    updated_at,
                    deleted_at,
                    created_by,
                    last_modified_by,
                };
                Ok(Some((doc, meta)))
            }
            None => Ok(None),
        }
    }

    pub async fn put<T: Serialize>(
        &self,
        id: &Uuid,
        doc: &T,
        expected: Option<i32>,
    ) -> Result<i32> {
        let json = serde_json::to_value(doc)?;
        match expected {
            Some(ver) => {
                let rows = sqlx::query(
                    r#"update docs set doc=$2, version=version+1, updated_at=now()
                        where id=$1 and version=$3 and deleted_at is null"#,
                )
                .bind(id)
                .bind(&json)
                .bind(ver)
                .execute(&self.pool)
                .await?;
                if rows.rows_affected() == 1 {
                    metrics::record_doc_write(None, 1);
                    Ok(ver + 1)
                } else {
                    metrics::record_doc_conflict(Some("public"));
                    Err(Error::DocVersionConflict)
                }
            }
            None => {
                // upsert semantics when no expected
                self.upsert(id, doc).await
            }
        }
    }

    pub async fn update<T, F>(&self, id: &Uuid, expected: i32, mutator: F) -> Result<i32>
    where
        T: DeserializeOwned + Serialize,
        F: FnOnce(&mut T),
    {
        let mut tx = self.pool.begin().await?;
        let row: Option<(Value, i32)> =
            sqlx::query_as("select doc, version from docs where id = $1 for update")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        let (mut doc, ver) = match row {
            Some((v, ver)) => (serde_json::from_value::<T>(v)?, ver),
            None => return Err(Error::DocNotFound),
        };
        if ver != expected {
            return Err(Error::DocVersionConflict);
        }
        mutator(&mut doc);
        let json = serde_json::to_value(&doc)?;
        let rows = sqlx::query(
            r#"update docs set doc=$2, version=version+1, updated_at=now()
                where id=$1 and version=$3 and deleted_at is null"#,
        )
        .bind(id)
        .bind(&json)
        .bind(ver)
        .execute(&mut *tx)
        .await?;
        if rows.rows_affected() != 1 {
            return Err(Error::DocVersionConflict);
        }
        tx.commit().await?;
        Ok(ver + 1)
    }

    /// Partially update fields using jsonb_set. Each path is dot/bracket notation (e.g. "profile.name").
    /// Returns the new version. If `expected` is Some, enforces OCC with that version.
    pub async fn patch_fields(
        &self,
        id: &Uuid,
        expected: Option<i32>,
        patches: &[(&str, Value)],
    ) -> Result<i32> {
        if patches.is_empty() {
            // No-op; return current version
            let ver: Option<i32> = sqlx::query_scalar("select version from docs where id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
            return ver.ok_or(Error::DocNotFound);
        }

        // Build nested jsonb_set(jsonb_set(...)) over doc
        // Ensure parent objects exist before setting deep fields (for PG < 16 jsonb_set)
        use std::collections::HashSet;
        let mut parent_paths: HashSet<Vec<String>> = HashSet::new();
        for (path, _) in patches.iter() {
            let parts = crate::query::JsonPath::from(*path).parts().to_vec();
            if parts.len() > 1 {
                parent_paths.insert(parts[..parts.len() - 1].to_vec());
            }
        }

        let mut qb = QueryBuilder::<Postgres>::new("update docs set doc = ");
        let total_ops = parent_paths.len() + patches.len();
        for _ in 0..total_ops {
            qb.push("jsonb_set(");
        }
        qb.push("doc");
        // Parent creation ops first
        for parts in &parent_paths {
            qb.push(", ");
            qb.push_bind(parts);
            qb.push(", ");
            qb.push_bind(Json(serde_json::json!({})));
            qb.push(", true)");
        }
        // Actual value patches
        for (path, value) in patches.iter() {
            let parts: Vec<String> = crate::query::JsonPath::from(*path).parts().to_vec();
            qb.push(", ");
            qb.push_bind(parts);
            qb.push(", ");
            qb.push_bind(Json(value.clone()));
            qb.push(", true)");
        }
        qb.push(", version = version + 1, updated_at = now() where id = ");
        qb.push_bind(id);
        if let Some(ver) = expected {
            qb.push(" and version = ");
            qb.push_bind(ver);
            qb.push(" and deleted_at is null");
        } else {
            qb.push(" and deleted_at is null");
        }
        qb.push(" returning version");

        let query = qb.build_query_as::<(i32,)>();
        if let Some((new_ver,)) = query.fetch_optional(&self.pool).await? {
            metrics::record_doc_write(Some("public"), 1);
            Ok(new_ver)
        } else if expected.is_some() {
            metrics::record_doc_conflict(Some("public"));
            Err(Error::DocVersionConflict)
        } else {
            Err(Error::DocNotFound)
        }
    }

    /// Convenience: patch a single field path.
    pub async fn patch(
        &self,
        id: &Uuid,
        expected: Option<i32>,
        path: &str,
        value: &Value,
    ) -> Result<i32> {
        self.patch_fields(id, expected, &[(path, value.clone())])
            .await
    }

    pub async fn get_field(&self, id: &Uuid, path: &str) -> Result<Option<Value>> {
        let parts: Vec<String> = path.split('.').map(|segment| segment.to_owned()).collect();

        let value: Option<Option<Value>> =
            sqlx::query_scalar("select doc #> $2 as field from docs where id = $1")
                .bind(id)
                .bind(parts)
                .fetch_optional(&self.pool)
                .await?;

        Ok(value.flatten())
    }

    pub fn query<T>(&self) -> DocumentQuery<T> {
        DocumentQuery::new(self.pool.clone())
    }

    pub async fn execute_compiled<Q, R>(&self, query: Q) -> Result<Vec<R>>
    where
        Q: CompiledQuery<R>,
        R: serde::de::DeserializeOwned,
    {
        let mut ctx = DocumentQueryContext::new();
        query.configure(&mut ctx);
        let (pool, mut builder) = ctx.into_spec().build_query(self.pool.clone());
        let query = builder.build_query_as::<(serde_json::Value,)>();
        let rows = query.fetch_all(&pool).await?;
        rows.into_iter()
            .map(|(value,)| serde_json::from_value(value).map_err(Into::into))
            .collect()
    }

    pub fn session(&self) -> DocumentSession {
        DocumentSession::new(
            self.pool.clone(),
            Events {
                pool: self.pool.clone(),
                use_advisory_lock: false,
            },
            crate::context::SessionContext::default(),
        )
    }
}

#[derive(Clone, Debug)]
struct IdentityEntry {
    value: Option<Value>,
    version: Option<i32>,
    dirty: bool,
}

impl IdentityEntry {
    fn new() -> Self {
        Self {
            value: None,
            version: None,
            dirty: false,
        }
    }
}

#[derive(Clone, Debug)]
struct StagedOperation {
    id: Uuid,
    action: SessionAction,
}

#[derive(Clone, Debug)]
enum SessionAction {
    Upsert { value: Value, expected: Option<i32> },
    Delete { expected: Option<i32> },
}

#[derive(Clone, Debug)]
struct SessionEventOp {
    stream_id: Uuid,
    expected: Expected,
    events: Vec<Event>,
    options: AppendOptions,
}

#[derive(Clone, Debug)]
struct SessionSnapshotOp {
    stream_id: Uuid,
    version: i32,
    body: Value,
}

enum IdentityMutation {
    Upsert {
        id: Uuid,
        value: Value,
        version: i32,
    },
    Delete {
        id: Uuid,
    },
}

/// State-tracking unit-of-work for coordinating document changes.
pub struct DocumentSession {
    pool: PgPool,
    identity: HashMap<Uuid, IdentityEntry>,
    staged: Vec<StagedOperation>,
    events_api: Events,
    event_ops: Vec<SessionEventOp>,
    snapshot_ops: Vec<SessionSnapshotOp>,
    context: crate::context::SessionContext,
    tenant_strategy: TenantStrategy,
    ensured_tenants: Option<Arc<RwLock<HashSet<String>>>>,
    tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
}

impl DocumentSession {
    pub fn aggregates<'a>(&'a mut self, repo: &'a AggregateRepository) -> AggregateSession<'a> {
        AggregateSession::new(repo, self)
    }

    pub(crate) fn set_tenant_strategy(&mut self, strategy: TenantStrategy) {
        self.tenant_strategy = strategy;
    }

    pub(crate) fn set_tenant_cache(&mut self, cache: Arc<RwLock<HashSet<String>>>) {
        self.ensured_tenants = Some(cache);
    }

    pub(crate) fn set_tenant_resolver(
        &mut self,
        resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    ) {
        self.tenant_resolver = resolver;
    }

    pub(crate) fn new(
        pool: PgPool,
        events_api: Events,
        context: crate::context::SessionContext,
    ) -> Self {
        Self {
            pool,
            identity: HashMap::new(),
            staged: Vec::new(),
            events_api,
            event_ops: Vec::new(),
            snapshot_ops: Vec::new(),
            context,
            tenant_strategy: TenantStrategy::Single,
            ensured_tenants: None,
            tenant_resolver: None,
        }
    }

    fn tenant_schema(&mut self) -> Result<Option<String>> {
        match self.tenant_strategy {
            TenantStrategy::Single => Ok(None),
            TenantStrategy::SchemaPerTenant => {
                if let Some(ref tenant) = self.context.tenant {
                    Ok(Some(tenant_schema_name(tenant)))
                } else if let Some(resolver) = &self.tenant_resolver {
                    if let Some(tenant) = (resolver)() {
                        self.context.tenant = Some(tenant.clone());
                        Ok(Some(tenant_schema_name(&tenant)))
                    } else {
                        Err(Error::TenantRequired)
                    }
                } else {
                    Err(Error::TenantRequired)
                }
            }
        }
    }

    fn remove_staged(&mut self, id: &Uuid) {
        self.staged.retain(|op| &op.id != id);
    }

    fn current_expected(&self, id: &Uuid) -> Option<i32> {
        self.identity.get(id).and_then(|entry| entry.version)
    }

    fn mark_loaded(&mut self, id: Uuid, value: Value, version: i32) {
        let entry = self.identity.entry(id).or_insert_with(IdentityEntry::new);
        entry.value = Some(value);
        entry.version = Some(version);
        entry.dirty = false;
    }

    /// Load a document into the session identity map, returning a typed struct if present.
    pub async fn load<T: DeserializeOwned>(&mut self, id: &Uuid) -> Result<Option<T>> {
        if let Some(entry) = self.identity.get(id) {
            if let Some(value) = &entry.value {
                let doc = serde_json::from_value::<T>(value.clone())?;
                return Ok(Some(doc));
            }
            if entry.dirty {
                return Ok(None);
            }
        }

        let schema = self.tenant_schema()?;
        let mut conn = self.pool.acquire().await?;
        let row = if let Some(ref schema_name) = schema {
            let mut tx = conn.begin().await?;
            Self::set_local_search_path_tx(&mut tx, schema_name).await?;
            let row: Option<(Value, i32, Option<chrono::DateTime<chrono::Utc>>)> =
                sqlx::query_as("select doc, version, deleted_at from docs where id = $1")
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await?;
            tx.commit().await?;
            row
        } else {
            sqlx::query_as("select doc, version, deleted_at from docs where id = $1")
                .bind(id)
                .fetch_optional(&mut *conn)
                .await?
        };

        match row {
            Some((value, version, deleted_at)) => {
                if deleted_at.is_some() {
                    let entry = self.identity.entry(*id).or_insert_with(IdentityEntry::new);
                    entry.value = None;
                    entry.version = Some(version);
                    entry.dirty = false;
                    return Ok(None);
                }
                metrics::record_doc_read(self.context.tenant.as_deref());
                let doc_json = value.clone();
                self.mark_loaded(*id, value, version);
                let doc: T = serde_json::from_value(doc_json)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Stage a document for insertion or update, using the session's known version for concurrency.
    pub fn store<T: Serialize>(&mut self, id: Uuid, doc: &T) -> Result<()> {
        let expected = self.current_expected(&id);
        self.store_with_expected(id, doc, expected)
    }

    /// Stage a document with an explicit expected version (None for upsert semantics).
    pub fn store_with_expected<T: Serialize>(
        &mut self,
        id: Uuid,
        doc: &T,
        expected: Option<i32>,
    ) -> Result<()> {
        let value = serde_json::to_value(doc)?;
        self.remove_staged(&id);
        self.staged.push(StagedOperation {
            id,
            action: SessionAction::Upsert {
                value: value.clone(),
                expected,
            },
        });

        let entry = self.identity.entry(id).or_insert_with(IdentityEntry::new);
        entry.value = Some(value);
        entry.version = expected;
        entry.dirty = true;
        Ok(())
    }

    /// Stage a delete using the session's known version if available.
    pub fn delete(&mut self, id: Uuid) {
        let expected = self.current_expected(&id);
        self.delete_with_expected(id, expected);
    }

    /// Stage a delete with an explicit expected version.
    pub fn delete_with_expected(&mut self, id: Uuid, expected: Option<i32>) {
        self.remove_staged(&id);
        self.staged.push(StagedOperation {
            id,
            action: SessionAction::Delete { expected },
        });

        let entry = self.identity.entry(id).or_insert_with(IdentityEntry::new);
        entry.value = None;
        entry.version = expected;
        entry.dirty = true;
    }

    /// Merge default headers for staged event appends.
    pub fn merge_event_headers(&mut self, headers: Value) -> &mut Self {
        self.context.merge_headers(headers);
        self
    }

    /// Replace default headers for staged event appends.
    pub fn set_event_headers(&mut self, headers: Option<Value>) -> &mut Self {
        self.context.headers.clear();
        if let Some(Value::Object(map)) = headers {
            self.context.headers = map;
        }
        self
    }

    /// Set default causation id used for staged event appends.
    pub fn set_event_causation_id(&mut self, id: Option<Uuid>) -> &mut Self {
        self.context.causation_id = id;
        self
    }

    /// Set default correlation id used for staged event appends.
    pub fn set_event_correlation_id(&mut self, id: Option<Uuid>) -> &mut Self {
        self.context.correlation_id = id;
        self
    }

    pub fn context(&self) -> &crate::context::SessionContext {
        &self.context
    }

    pub fn context_mut(&mut self) -> &mut crate::context::SessionContext {
        &mut self.context
    }

    /// Ensure an idempotency key header is present on staged event appends.
    pub fn set_event_idempotency_key(&mut self, key: impl Into<String>) -> &mut Self {
        self.context
            .headers
            .insert("idempotency_key".to_string(), Value::String(key.into()));
        self
    }

    pub fn enqueue_event(
        &mut self,
        stream_id: Uuid,
        expected: Expected,
        event: Event,
    ) -> Result<()> {
        self.enqueue_events(stream_id, expected, vec![event])
    }

    pub fn enqueue_events<I>(
        &mut self,
        stream_id: Uuid,
        expected: Expected,
        events: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Event>,
    {
        let overrides = AppendOptions::default();
        self.append_events_with(stream_id, expected, events, overrides)
    }

    pub fn enable_event_advisory_locks(&mut self) -> &mut Self {
        self.events_api.use_advisory_lock = true;
        self
    }

    pub fn disable_event_advisory_locks(&mut self) -> &mut Self {
        self.events_api.use_advisory_lock = false;
        self
    }

    /// Stage events to be appended when `save_changes` succeeds, using defaults.
    pub fn append_events<I>(&mut self, stream_id: Uuid, expected: Expected, events: I) -> Result<()>
    where
        I: IntoIterator<Item = Event>,
    {
        self.enqueue_events(stream_id, expected, events)
    }

    /// Stage events with per-call override options (headers/ids).
    pub fn append_events_with<I>(
        &mut self,
        stream_id: Uuid,
        expected: Expected,
        events: I,
        overrides: AppendOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Event>,
    {
        let events_vec: Vec<Event> = events.into_iter().collect();
        if events_vec.is_empty() {
            return Ok(());
        }

        let defaults = AppendOptions {
            headers: Some(Value::Object(self.context.headers.clone())),
            causation_id: self.context.causation_id,
            correlation_id: self.context.correlation_id,
        };
        let options = Self::combine_options(&defaults, &overrides);
        self.event_ops.push(SessionEventOp {
            stream_id,
            expected,
            events: events_vec,
            options,
        });
        Ok(())
    }

    pub fn enqueue_aggregate(
        &mut self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
        overrides: Option<AppendOptions>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let overrides = overrides.unwrap_or_default();
        let defaults = AppendOptions {
            headers: Some(Value::Object(self.context.headers.clone())),
            causation_id: self.context.causation_id,
            correlation_id: self.context.correlation_id,
        };
        let options = Self::combine_options(&defaults, &overrides);
        self.event_ops.push(SessionEventOp {
            stream_id,
            expected,
            events,
            options,
        });
        Ok(())
    }

    pub fn enqueue_snapshot(&mut self, stream_id: Uuid, version: i32, body: Value) {
        self.snapshot_ops.push(SessionSnapshotOp {
            stream_id,
            version,
            body,
        });
    }

    /// Persist staged changes inside a single database transaction.
    pub async fn save_changes(&mut self) -> Result<()> {
        let schema = self.tenant_schema()?;
        if let (Some(schema_name), Some(cache)) = (&schema, &self.ensured_tenants) {
            if !cache
                .read()
                .expect("tenant cache poisoned")
                .contains(schema_name)
            {
                return Err(Error::TenantNotFound(schema_name.clone()));
            }
        }
        let mut tx = self.pool.begin().await?;

        if let Some(ref schema_name) = schema {
            Self::set_local_search_path_tx(&mut tx, schema_name).await?;
        }
        let mut mutations = Vec::with_capacity(self.staged.len());
        let mut write_count = 0u64;

        let staged_docs: Vec<_> = self.staged.drain(..).collect();
        for op in staged_docs {
            match op.action {
                SessionAction::Upsert { value, expected } => {
                    let version = if let Some(expected) = expected {
                        let rec: Option<(i32,)> = sqlx::query_as(
                            r#"update docs
                               set doc = $2,
                                   version = version + 1,
                                   updated_at = now()
                               where id = $1 and version = $3 and deleted_at is null
                               returning version"#,
                        )
                        .bind(op.id)
                        .bind(&value)
                        .bind(expected)
                        .fetch_optional(&mut *tx)
                        .await?;

                        match rec {
                            Some((ver,)) => ver,
                            None => {
                                metrics::record_doc_conflict(self.context.tenant.as_deref());
                                return Err(Error::DocVersionConflict);
                            }
                        }
                    } else {
                        sqlx::query_scalar(
                            r#"with up as (
                                   insert into docs (id, doc, version)
                                   values ($1, $2, 1)
                                   on conflict (id) do update
                                     set doc = excluded.doc,
                                         version = docs.version + 1,
                                         updated_at = now()
                                   returning version)
                               select version from up"#,
                        )
                        .bind(op.id)
                        .bind(&value)
                        .fetch_one(&mut *tx)
                        .await?
                    };

                    mutations.push(IdentityMutation::Upsert {
                        id: op.id,
                        value,
                        version,
                    });
                    write_count += 1;
                }
                SessionAction::Delete { expected } => {
                    if let Some(expected) = expected {
                        let result = sqlx::query(
                            "delete from docs where id = $1 and version = $2 and deleted_at is null",
                        )
                        .bind(op.id)
                        .bind(expected)
                        .execute(&mut *tx)
                        .await?;
                        if result.rows_affected() == 0 {
                            metrics::record_doc_conflict(self.context.tenant.as_deref());
                            return Err(Error::DocVersionConflict);
                        }
                    } else {
                        sqlx::query("delete from docs where id = $1")
                            .bind(op.id)
                            .execute(&mut *tx)
                            .await?;
                    }
                    mutations.push(IdentityMutation::Delete { id: op.id });
                }
            }
        }

        let mut event_ops = std::mem::take(&mut self.event_ops);
        let mut snapshot_ops = std::mem::take(&mut self.snapshot_ops);
        let mut events_written = 0u64;
        if !event_ops.is_empty() {
            for op in &event_ops {
                let result = self
                    .events_api
                    .append_with_tx(&mut tx, op.stream_id, op.expected, &op.events, &op.options)
                    .await;
                if let Err(err) = result {
                    if matches!(err, Error::VersionConflict | Error::IdempotencyConflict) {
                        metrics::record_event_conflict(self.context.tenant.as_deref());
                    }
                    self.event_ops = event_ops;
                    self.snapshot_ops = snapshot_ops;
                    return Err(err);
                }
                events_written += op.events.len() as u64;
            }
        }

        if !snapshot_ops.is_empty() {
            for op in &snapshot_ops {
                if let Err(err) = sqlx::query(
                    r#"insert into snapshots(stream_id, version, body)
                        values ($1, $2, $3)
                        on conflict (stream_id) do update set version = excluded.version, body = excluded.body, created_at = now()"#,
                )
                .bind(op.stream_id)
                .bind(op.version)
                .bind(&op.body)
                .execute(&mut *tx)
                .await
                {
                    self.event_ops = event_ops;
                    self.snapshot_ops = snapshot_ops;
                    return Err(err.into());
                }
            }
        }

        tx.commit().await?;

        if write_count > 0 {
            metrics::record_doc_write(self.context.tenant.as_deref(), write_count);
        }
        if events_written > 0 {
            metrics::record_event_appends(self.context.tenant.as_deref(), events_written);
        }

        for mutation in mutations {
            match mutation {
                IdentityMutation::Upsert { id, value, version } => {
                    let entry = self.identity.entry(id).or_insert_with(IdentityEntry::new);
                    entry.value = Some(value);
                    entry.version = Some(version);
                    entry.dirty = false;
                }
                IdentityMutation::Delete { id } => {
                    self.identity.remove(&id);
                }
            }
        }

        event_ops.clear();
        snapshot_ops.clear();
        Ok(())
    }

    /// Clear cached identity entries and staged operations without touching the database.
    pub fn clear(&mut self) {
        self.identity.clear();
        self.staged.clear();
        self.event_ops.clear();
        self.snapshot_ops.clear();
        self.context = crate::context::SessionContext::default();
    }

    pub(crate) fn combine_options(
        defaults: &AppendOptions,
        overrides: &AppendOptions,
    ) -> AppendOptions {
        AppendOptions {
            headers: Self::merge_headers(&defaults.headers, &overrides.headers),
            causation_id: overrides.causation_id.or(defaults.causation_id),
            correlation_id: overrides.correlation_id.or(defaults.correlation_id),
        }
    }

    pub(crate) fn merge_headers(base: &Option<Value>, overrides: &Option<Value>) -> Option<Value> {
        match (base, overrides) {
            (_, Some(Value::Object(override_map))) => {
                if let Some(Value::Object(base_map)) = base {
                    let mut merged = base_map.clone();
                    for (k, v) in override_map {
                        merged.insert(k.clone(), v.clone());
                    }
                    Some(Value::Object(merged))
                } else {
                    Some(Value::Object(override_map.clone()))
                }
            }
            (_, Some(value)) => Some(value.clone()),
            (Some(value), None) => Some(value.clone()),
            (None, None) => None,
        }
    }

    async fn set_local_search_path_tx(
        tx: &mut Transaction<'_, Postgres>,
        schema_name: &str,
    ) -> Result<()> {
        let stmt = format!(
            "set local search_path to {}, public",
            schema::quote_ident(schema_name)
        );
        sqlx::query(&stmt).execute(&mut **tx).await?;
        Ok(())
    }
}
