use crate::projections::ProjectionHandler;
use crate::{Error, Result, metrics, schema, store::TenantStrategy};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum Expected {
    Any,
    NoStream,
    Exact(i32),
}

#[derive(Clone, Debug)]
pub struct Event {
    pub typ: String,
    pub body: Value,
}

impl Event {
    pub fn new<T: Serialize>(typ: impl Into<String>, body: &T) -> Self {
        Self {
            typ: typ.into(),
            body: serde_json::to_value(body).expect("failed to serialize event body"),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct AppendOptions {
    pub headers: Option<Value>,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct Events {
    pub(crate) pool: PgPool,
    pub(crate) use_advisory_lock: bool,
    pub(crate) apply_inline: bool,
    tenant_strategy: TenantStrategy,
    tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    tenant: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EventEnvelope {
    pub global_seq: i64,
    pub stream_id: Uuid,
    pub stream_seq: i32,
    pub typ: String,
    pub body: Value,
    pub headers: Value,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub event_version: i32,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Events {
    pub(crate) fn new(
        pool: PgPool,
        tenant_strategy: TenantStrategy,
        tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
        tenant: Option<String>,
    ) -> Self {
        Self {
            pool,
            use_advisory_lock: false,
            apply_inline: false,
            tenant_strategy,
            tenant_resolver,
            tenant,
        }
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub(crate) fn set_tenant(&mut self, tenant: Option<String>) {
        self.tenant = tenant;
    }

    pub fn with_advisory_locks(mut self) -> Self {
        self.use_advisory_lock = true;
        self
    }

    pub fn enable_inline_projections(mut self) -> Self {
        self.apply_inline = true;
        self
    }

    fn tenant_column(&self) -> Option<&str> {
        match &self.tenant_strategy {
            TenantStrategy::Conjoined { column } => Some(column.name.as_str()),
            _ => None,
        }
    }

    fn resolve_conjoined_tenant(&self) -> Result<Option<String>> {
        match &self.tenant_strategy {
            TenantStrategy::Conjoined { .. } => {
                if let Some(t) = &self.tenant {
                    return Ok(Some(t.clone()));
                }
                if let Some(resolver) = &self.tenant_resolver {
                    if let Some(t) = (resolver)() {
                        return Ok(Some(t));
                    }
                }
                Err(Error::TenantRequired)
            }
            _ => Ok(None),
        }
    }

    fn metrics_label(&self) -> Option<&str> {
        if matches!(self.tenant_strategy, TenantStrategy::Conjoined { .. }) {
            self.tenant.as_deref()
        } else {
            None
        }
    }

    fn push_tenant_filter<'a>(
        &self,
        qb: &mut QueryBuilder<'a, Postgres>,
        tenant: Option<&'a String>,
    ) {
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant) {
            qb.push(format!("{} = ", schema::quote_ident(column)));
            qb.push_bind(value);
            qb.push(" and ");
        }
    }

    pub async fn append_stream(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
    ) -> Result<()> {
        self.append_with(stream_id, expected, events, &AppendOptions::default())
            .await
    }

    pub async fn append_stream_with_headers(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
        headers: &Value,
        causation_id: Option<Uuid>,
        correlation_id: Option<Uuid>,
    ) -> Result<()> {
        let opts = AppendOptions {
            headers: Some(headers.clone()),
            causation_id,
            correlation_id,
        };
        self.append_with(stream_id, expected, events, &opts).await
    }

    pub async fn read_stream(&self, stream_id: Uuid) -> Result<Vec<(i32, Event)>> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let mut qb =
            QueryBuilder::<Postgres>::new("select stream_seq, event_type, body from events where ");
        self.push_tenant_filter(&mut qb, tenant_value.as_ref());
        qb.push("stream_id = ");
        qb.push_bind(stream_id);
        qb.push(" order by stream_seq asc");
        let rows: Vec<(i32, String, Value)> = qb.build_query_as().fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(|(seq, typ, body)| (seq, Event { typ, body }))
            .collect())
    }

    pub async fn read_stream_envelopes(&self, stream_id: Uuid) -> Result<Vec<EventEnvelope>> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();
        #[allow(clippy::type_complexity)]
        let mut qb = QueryBuilder::<Postgres>::new("with combined as (");
        qb.push("select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ");
        if let Some(column) = self.tenant_column() {
            qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
        } else {
            qb.push("null::text as tenant_id, ");
        }
        qb.push("user_id, created_at from events where ");
        self.push_tenant_filter(&mut qb, tenant_ref);
        qb.push("stream_id = ");
        qb.push_bind(stream_id);
        qb.push(" union all select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ");
        if let Some(column) = self.tenant_column() {
            qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
        } else {
            qb.push("null::text as tenant_id, ");
        }
        qb.push("user_id, created_at from events_archive where ");
        self.push_tenant_filter(&mut qb, tenant_ref);
        qb.push("stream_id = ");
        qb.push_bind(stream_id);
        qb.push(") select * from combined order by stream_seq asc");
        let rows: Vec<(
            i64,
            Uuid,
            i32,
            String,
            Value,
            Value,
            Option<Uuid>,
            Option<Uuid>,
            i32,
            Option<String>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
        )> = qb.build_query_as().fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    global_seq,
                    stream_id,
                    stream_seq,
                    typ,
                    mut body,
                    headers,
                    causation_id,
                    correlation_id,
                    event_version,
                    tenant_id,
                    user_id,
                    created_at,
                )| {
                    // Apply upcasters if registered, chaining from current version.
                    let mut next = event_version;
                    while let Some(up) = UPCASTERS
                        .get_or_init(Default::default)
                        .lock()
                        .ok()
                        .and_then(|m| m.get(&(typ.clone(), next)).cloned())
                    {
                        match (up)(body.clone()) {
                            Ok(new_body) => {
                                body = new_body;
                                next += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    EventEnvelope {
                        global_seq,
                        stream_id,
                        stream_seq,
                        typ,
                        body,
                        headers,
                        causation_id,
                        correlation_id,
                        event_version: next,
                        tenant_id,
                        user_id,
                        created_at,
                    }
                },
            )
            .collect())
    }

    pub async fn append_with(
        &self,
        stream_id: Uuid,
        expected: Expected,
        events: Vec<Event>,
        opts: &AppendOptions,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();
        let metrics_label = tenant_ref
            .map(|s| s.as_str())
            .or_else(|| self.metrics_label());
        let mut tx = self.pool.begin().await?;
        let res = self
            .append_with_tx_internal(&mut tx, stream_id, expected, &events, opts, tenant_ref)
            .await;
        match res {
            Ok(()) => {
                tx.commit().await?;
                metrics::record_event_appends(metrics_label, events.len() as u64);
                Ok(())
            }
            Err(err) => {
                metrics::record_event_conflict(metrics_label);
                Err(err)
            }
        }
    }

    async fn append_with_tx_internal(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: Uuid,
        expected: Expected,
        events: &[Event],
        opts: &AppendOptions,
        tenant: Option<&String>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        if self.use_advisory_lock {
            let key = stream_id.to_string();
            sqlx::query("select pg_advisory_xact_lock(hashtext($1)::bigint)")
                .bind(&key)
                .execute(&mut **tx)
                .await?;
        }

        if self.tenant_column().is_some() && tenant.is_none() {
            return Err(Error::TenantRequired);
        }

        let mut seq_query =
            QueryBuilder::<Postgres>::new("select max(stream_seq) from events where ");
        self.push_tenant_filter(&mut seq_query, tenant);
        seq_query.push("stream_id = ");
        seq_query.push_bind(stream_id);
        let current: i32 = seq_query
            .build_query_scalar::<Option<i32>>()
            .fetch_one(&mut **tx)
            .await?
            .unwrap_or(0);

        match expected {
            Expected::Any => {}
            Expected::NoStream if current != 0 => return Err(Error::VersionConflict),
            Expected::Exact(value) if value != current => return Err(Error::VersionConflict),
            _ => {}
        }

        let mut seq = current;
        let headers = opts
            .headers
            .clone()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        for event in events {
            // JSON Schema validation if a schema exists
            if let Some((schema_value, ver)) = sqlx::query_as::<_, (Value, i32)>(
                "select schema, version from event_schemas where event_type = $1 order by version desc limit 1",
            )
            .bind(&event.typ)
            .fetch_optional(&mut **tx)
            .await?
            {
                // validate against latest schema
                let compiled = match jsonschema::validator_for(&schema_value) {
                    Ok(c) => c,
                    Err(e) => {
                        return Err(Error::QueryError { query: "compile event schema".into(), context: e.to_string() });
                    }
                };
                let result = compiled.validate(&event.body);
                if let Err(error) = result {
                    return Err(Error::QueryError { query: "event schema validation".into(), context: error.to_string() });
                }
                let _v = ver; // reserved for future use (recording)
            }

            seq += 1;
            let mut insert = QueryBuilder::<Postgres>::new("insert into events (");
            if let (Some(column), Some(value)) = (self.tenant_column(), tenant) {
                insert.push(format!("{}, ", schema::quote_ident(column)));
                insert.push("stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id) values (");
                insert.push_bind(value);
                insert.push(", ");
            } else {
                insert.push(
                    "stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id) values (",
                );
            }
            insert.push_bind(stream_id);
            insert.push(", ");
            insert.push_bind(seq);
            insert.push(", ");
            insert.push_bind(&event.typ);
            insert.push(", ");
            insert.push_bind(&event.body);
            insert.push(", ");
            insert.push_bind(&headers);
            insert.push(", ");
            insert.push_bind(opts.causation_id);
            insert.push(", ");
            insert.push_bind(opts.correlation_id);
            insert.push(", 1, current_user)");

            let res = insert.build().execute(&mut **tx).await;

            if let Err(e) = res {
                if let sqlx::Error::Database(db_err) = &e {
                    let msg = db_err.message().to_lowercase();
                    if msg.contains("events_idemp_key_uq")
                        || msg.contains("unique") && msg.contains("idempotency_key")
                    {
                        return Err(Error::IdempotencyConflict);
                    }
                }
                return Err(e.into());
            }

            // Apply inline projections inside the same transaction for immediate consistency
            if self.apply_inline {
                if let Ok(handlers) = INLINE_HANDLERS.get_or_init(Default::default).lock() {
                    for h in handlers.iter() {
                        h.apply(&event.typ, &event.body, tx).await?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn append_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: Uuid,
        expected: Expected,
        events: &[Event],
        opts: &AppendOptions,
    ) -> Result<()> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        self.append_with_tx_internal(tx, stream_id, expected, events, opts, tenant_value.as_ref())
            .await
    }

    pub fn builder(&self, stream_id: Uuid) -> AppendBuilder {
        AppendBuilder {
            events: Vec::new(),
            opts: AppendOptions::default(),
            expected: Expected::Any,
            stream_id,
            inner: self.clone(),
        }
    }

    pub async fn archive_stream_before(
        &self,
        stream_id: Uuid,
        before: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();
        let mut tx = self.pool.begin().await?;
        // Move rows to archive
        let mut insert = QueryBuilder::<Postgres>::new("insert into events_archive (");
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant_ref) {
            insert.push(format!("{}, ", schema::quote_ident(column)));
            insert.push("global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id, created_at) select ");
            insert.push_bind(value);
            insert.push(", global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id, created_at from events where ");
        } else {
            insert.push(
                "global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id, created_at) select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id, created_at from events where ",
            );
        }
        self.push_tenant_filter(&mut insert, tenant_ref);
        insert.push("stream_id = ");
        insert.push_bind(stream_id);
        insert.push(" and created_at < ");
        insert.push_bind(before);
        insert.push(" on conflict (global_seq) do nothing");
        let moved = insert.build().execute(&mut *tx).await?.rows_affected();

        // Delete moved rows from live
        let mut delete = QueryBuilder::<Postgres>::new("delete from events where ");
        self.push_tenant_filter(&mut delete, tenant_ref);
        delete.push("stream_id = ");
        delete.push_bind(stream_id);
        delete.push(" and created_at < ");
        delete.push_bind(before);
        delete.build().execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(moved)
    }

    pub async fn append_tombstone(&self, stream_id: Uuid, reason: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();
        if self.tenant_column().is_some() && tenant_ref.is_none() {
            return Err(Error::TenantRequired);
        }
        let mut qb = QueryBuilder::<Postgres>::new("select max(stream_seq) from events where ");
        self.push_tenant_filter(&mut qb, tenant_ref);
        qb.push("stream_id = ");
        qb.push_bind(stream_id);
        let current: i32 = qb
            .build_query_scalar::<Option<i32>>()
            .fetch_one(&mut *tx)
            .await?
            .unwrap_or(0);
        let next = current + 1;
        let mut insert = QueryBuilder::<Postgres>::new("insert into events (");
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant_ref) {
            insert.push(format!("{}, ", schema::quote_ident(column)));
            insert.push("stream_id, stream_seq, event_type, body, headers, is_tombstone, event_version, user_id) values (");
            insert.push_bind(value);
            insert.push(", ");
        } else {
            insert.push(
                "stream_id, stream_seq, event_type, body, headers, is_tombstone, event_version, user_id) values (",
            );
        }
        insert.push_bind(stream_id);
        insert.push(", ");
        insert.push_bind(next);
        insert.push(", ");
        insert.push_bind("$tombstone");
        insert.push(", ");
        insert.push_bind(serde_json::json!({"reason": reason}));
        insert.push(", ");
        insert.push_bind(serde_json::json!({}));
        insert.push(", true, 1, current_user)");
        insert.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

type UpcasterFn = Arc<dyn Fn(Value) -> Result<Value> + Send + Sync + 'static>;
static UPCASTERS: OnceLock<Mutex<HashMap<(String, i32), UpcasterFn>>> = OnceLock::new();

/// Register an event upcaster that transforms a given event type from a specific version to the next version.
pub fn register_upcaster(event_type: &str, from_version: i32, upcaster: UpcasterFn) {
    let map = UPCASTERS.get_or_init(Default::default);
    if let Ok(mut m) = map.lock() {
        m.insert((event_type.to_string(), from_version), upcaster);
    }
}

static INLINE_HANDLERS: OnceLock<Mutex<Vec<Arc<dyn ProjectionHandler + Send + Sync>>>> =
    OnceLock::new();

pub fn register_inline_projection(handler: Arc<dyn ProjectionHandler + Send + Sync>) {
    let list = INLINE_HANDLERS.get_or_init(Default::default);
    if let Ok(mut v) = list.lock() {
        v.push(handler);
    }
}

pub struct AppendBuilder {
    inner: Events,
    stream_id: Uuid,
    expected: Expected,
    opts: AppendOptions,
    events: Vec<Event>,
}

impl AppendBuilder {
    pub fn expected(mut self, expected: Expected) -> Self {
        self.expected = expected;
        self
    }

    pub fn headers(mut self, headers: serde_json::Value) -> Self {
        self.opts.headers = Some(headers);
        self
    }

    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        let mut h = self
            .opts
            .headers
            .take()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        if let Value::Object(ref mut m) = h {
            m.insert("idempotency_key".to_string(), Value::String(key.into()));
        }
        self.opts.headers = Some(h);
        self
    }

    pub fn causation_id(mut self, id: Uuid) -> Self {
        self.opts.causation_id = Some(id);
        self
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.opts.correlation_id = Some(id);
        self
    }

    pub fn push(mut self, event: Event) -> Self {
        self.events.push(event);
        self
    }

    pub fn extend<I: IntoIterator<Item = Event>>(mut self, iter: I) -> Self {
        self.events.extend(iter);
        self
    }

    pub async fn send(self) -> Result<()> {
        self.inner
            .append_with(self.stream_id, self.expected, self.events, &self.opts)
            .await
    }
}
