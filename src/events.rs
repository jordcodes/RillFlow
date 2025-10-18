use crate::projections::ProjectionHandler;
use crate::{Error, Result, metrics, schema, store::TenantStrategy, upcasting::UpcasterRegistry};
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
    pub allow_archived_stream: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchiveBackend {
    DualTable,
    Partitioned,
}

#[derive(Clone, Copy, Debug)]
pub struct ArchiveSettings {
    pub backend: ArchiveBackend,
    pub include_archived: bool,
}

impl Default for ArchiveSettings {
    fn default() -> Self {
        Self {
            backend: ArchiveBackend::DualTable,
            include_archived: false,
        }
    }
}

#[derive(Debug, Clone)]
struct StreamState {
    archived_at: Option<chrono::DateTime<chrono::Utc>>,
    retention_class: String,
}

impl StreamState {
    fn hot() -> Self {
        Self {
            archived_at: None,
            retention_class: "hot".to_string(),
        }
    }

    fn is_archived(&self) -> bool {
        self.archived_at.is_some() || self.retention_class.eq_ignore_ascii_case("cold")
    }
}

#[derive(Clone)]
pub struct Events {
    pub(crate) pool: PgPool,
    pub(crate) use_advisory_lock: bool,
    pub(crate) apply_inline: bool,
    pub(crate) tenant_strategy: TenantStrategy,
    pub(crate) tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    pub(crate) tenant: Option<String>,
    pub(crate) upcaster_registry: Option<Arc<UpcasterRegistry>>,
    archive_backend: ArchiveBackend,
    include_archived: bool,
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
    fn using_partitioned_backend(&self) -> bool {
        matches!(self.archive_backend, ArchiveBackend::Partitioned)
    }

    pub(crate) fn new(
        pool: PgPool,
        tenant_strategy: TenantStrategy,
        tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
        tenant: Option<String>,
        archive_settings: ArchiveSettings,
    ) -> Self {
        Self {
            pool,
            use_advisory_lock: false,
            apply_inline: false,
            tenant_strategy,
            tenant_resolver,
            tenant,
            upcaster_registry: None,
            archive_backend: archive_settings.backend,
            include_archived: archive_settings.include_archived,
        }
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub fn with_upcasters(mut self, registry: Arc<UpcasterRegistry>) -> Self {
        self.upcaster_registry = Some(registry);
        self
    }

    pub fn archive_backend(&self) -> ArchiveBackend {
        self.archive_backend
    }

    pub fn include_archived(mut self, include: bool) -> Self {
        self.include_archived = include;
        self
    }

    pub fn with_archived(self) -> Self {
        self.include_archived(true)
    }

    pub fn hot_only(mut self) -> Self {
        self.include_archived = false;
        self
    }

    pub fn set_include_archived(&mut self, include: bool) {
        self.include_archived = include;
    }

    pub(crate) fn set_upcasters(&mut self, registry: Option<Arc<UpcasterRegistry>>) {
        self.upcaster_registry = registry;
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

    pub async fn fetch_after_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        last_seq: i64,
        limit: i64,
    ) -> Result<Vec<EventEnvelope>> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();

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
        )> = if self.using_partitioned_backend() {
            let mut qb = QueryBuilder::<Postgres>::new(
                "select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
            );
            if let Some(column) = self.tenant_column() {
                qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
            } else {
                qb.push("null::text as tenant_id, ");
            }
            qb.push("user_id, created_at from events where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            if !self.include_archived {
                qb.push("retention_class = 'hot' and ");
            }
            qb.push("global_seq > ");
            qb.push_bind(last_seq);
            qb.push(" order by global_seq asc limit ");
            qb.push_bind(limit);
            qb.build_query_as().fetch_all(&mut **tx).await?
        } else if self.include_archived {
            let mut qb = QueryBuilder::<Postgres>::new(
                "with combined as (select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
            );
            if let Some(column) = self.tenant_column() {
                qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
            } else {
                qb.push("null::text as tenant_id, ");
            }
            qb.push("user_id, created_at from events where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("global_seq > ");
            qb.push_bind(last_seq);
            qb.push(
                " union all select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
            );
            if let Some(column) = self.tenant_column() {
                qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
            } else {
                qb.push("null::text as tenant_id, ");
            }
            qb.push("user_id, created_at from events_archive where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("global_seq > ");
            qb.push_bind(last_seq);
            qb.push(
                ") select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, tenant_id, user_id, created_at from combined order by global_seq asc limit ",
            );
            qb.push_bind(limit);
            qb.build_query_as().fetch_all(&mut **tx).await?
        } else {
            let mut qb = QueryBuilder::<Postgres>::new(
                "select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
            );
            if let Some(column) = self.tenant_column() {
                qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
            } else {
                qb.push("null::text as tenant_id, ");
            }
            qb.push("user_id, created_at from events where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("global_seq > ");
            qb.push_bind(last_seq);
            qb.push(" order by global_seq asc limit ");
            qb.push_bind(limit);
            qb.build_query_as().fetch_all(&mut **tx).await?
        };

        let mut envelopes: Vec<EventEnvelope> = rows
            .into_iter()
            .map(
                |(
                    global_seq,
                    stream_id,
                    stream_seq,
                    typ,
                    body,
                    headers,
                    causation_id,
                    correlation_id,
                    event_version,
                    tenant_id,
                    user_id,
                    created_at,
                )| EventEnvelope {
                    global_seq,
                    stream_id,
                    stream_seq,
                    typ,
                    body,
                    headers,
                    causation_id,
                    correlation_id,
                    event_version,
                    tenant_id,
                    user_id,
                    created_at,
                },
            )
            .collect();

        if let Some(registry) = &self.upcaster_registry {
            for envelope in &mut envelopes {
                registry
                    .upcast_with_pool(envelope, Some(&self.pool))
                    .await?;
            }
        } else {
            for envelope in &mut envelopes {
                Self::apply_legacy_upcasters(envelope);
            }
        }

        if let Some(value) = tenant_value {
            for envelope in &mut envelopes {
                if envelope.tenant_id.is_none() {
                    envelope.tenant_id = Some(value.clone());
                }
            }
        }

        Ok(envelopes)
    }

    pub async fn head_sequence_tx(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();

        let head: i64 = if self.using_partitioned_backend() {
            let mut qb =
                QueryBuilder::<Postgres>::new("select coalesce(max(global_seq), 0) from events");
            if tenant_ref.is_some() || !self.include_archived {
                qb.push(" where ");
                self.push_tenant_filter(&mut qb, tenant_ref);
                if !self.include_archived {
                    qb.push("retention_class = 'hot' and ");
                }
                qb.push("true");
            }
            qb.build_query_scalar::<i64>().fetch_one(&mut **tx).await?
        } else if self.include_archived && matches!(self.archive_backend, ArchiveBackend::DualTable)
        {
            let mut qb = QueryBuilder::<Postgres>::new(
                "select coalesce(max(global_seq), 0) from (select global_seq from events where ",
            );
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("true union all select global_seq from events_archive where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("true) as combined");
            qb.build_query_scalar::<i64>().fetch_one(&mut **tx).await?
        } else {
            let mut qb =
                QueryBuilder::<Postgres>::new("select coalesce(max(global_seq), 0) from events");
            if tenant_ref.is_some() {
                qb.push(" where ");
                self.push_tenant_filter(&mut qb, tenant_ref);
                qb.push("true");
            }
            qb.build_query_scalar::<i64>().fetch_one(&mut **tx).await?
        };

        Ok(head)
    }

    pub async fn head_sequence(&self) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let head = self.head_sequence_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(head)
    }

    pub async fn load_stream_envelopes(
        &self,
        stream_id: Uuid,
        after: Option<i64>,
        limit: Option<i64>,
        ascending: bool,
    ) -> Result<Vec<EventEnvelope>> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();

        let mut envelopes: Vec<EventEnvelope> = if self.using_partitioned_backend() {
            let mut qb = QueryBuilder::<Postgres>::new(
                "select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
            );
            if let Some(column) = self.tenant_column() {
                qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
            } else {
                qb.push("null::text as tenant_id, ");
            }
            qb.push("user_id, created_at from events where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("stream_id = ");
            qb.push_bind(stream_id);
            if let Some(seq) = after {
                qb.push(" and global_seq > ");
                qb.push_bind(seq);
            }
            if !self.include_archived {
                qb.push(" and retention_class = 'hot'");
            }
            qb.push(" order by stream_seq ");
            qb.push(if ascending { "asc" } else { "desc" });
            if let Some(limit) = limit {
                qb.push(" limit ");
                qb.push_bind(limit);
            }

            qb.build_query_as().fetch_all(&self.pool).await?
        } else {
            #[allow(clippy::type_complexity)]
            let mut qb = QueryBuilder::<Postgres>::new("with combined as (");
            qb.push(
                "select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
            );
            if let Some(column) = self.tenant_column() {
                qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
            } else {
                qb.push("null::text as tenant_id, ");
            }
            qb.push("user_id, created_at from events where ");
            self.push_tenant_filter(&mut qb, tenant_ref);
            qb.push("stream_id = ");
            qb.push_bind(stream_id);
            if let Some(seq) = after {
                qb.push(" and global_seq > ");
                qb.push_bind(seq);
            }
            if self.include_archived
                && matches!(self.archive_backend, ArchiveBackend::DualTable)
            {
                qb.push(
                    " union all select global_seq, stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, ",
                );
                if let Some(column) = self.tenant_column() {
                    qb.push(format!("{} as tenant_id, ", schema::quote_ident(column)));
                } else {
                    qb.push("null::text as tenant_id, ");
                }
                qb.push("user_id, created_at from events_archive where ");
                self.push_tenant_filter(&mut qb, tenant_ref);
                qb.push("stream_id = ");
                qb.push_bind(stream_id);
                if let Some(seq) = after {
                    qb.push(" and global_seq > ");
                    qb.push_bind(seq);
                }
            }
            qb.push(") select * from combined order by stream_seq ");
            qb.push(if ascending { "asc" } else { "desc" });
            if let Some(limit) = limit {
                qb.push(" limit ");
                qb.push_bind(limit);
            }

            qb.build_query_as().fetch_all(&self.pool).await?
        }
            .into_iter()
            .map(
                |(
                    global_seq,
                    stream_id,
                    stream_seq,
                    typ,
                    body,
                    headers,
                    causation_id,
                    correlation_id,
                    event_version,
                    tenant_id,
                    user_id,
                    created_at,
                )| EventEnvelope {
                    global_seq,
                    stream_id,
                    stream_seq,
                    typ,
                    body,
                    headers,
                    causation_id,
                    correlation_id,
                    event_version,
                    tenant_id,
                    user_id,
                    created_at,
                },
            )
            .collect();

        if let Some(value) = tenant_value {
            for envelope in &mut envelopes {
                if envelope.tenant_id.is_none() {
                    envelope.tenant_id = Some(value.clone());
                }
            }
        }

        Ok(envelopes)
    }

    async fn ensure_stream_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: Uuid,
        tenant: Option<&String>,
        allow_archived: bool,
    ) -> Result<StreamState> {
        let row = sqlx::query_as::<
            _,
            (
                Option<chrono::DateTime<chrono::Utc>>,
                String,
                Option<String>,
            ),
        >(
            "select archived_at, retention_class, tenant_id from rf_streams where stream_id = $1",
        )
        .bind(stream_id)
        .fetch_optional(&mut **tx)
        .await?;

        if let Some((archived_at, retention_class, tenant_id)) = row {
            if tenant_id.is_none() {
                if let Some(t) = tenant {
                    sqlx::query(
                        "update rf_streams set tenant_id = $2, updated_at = now() where stream_id = $1 and tenant_id is null",
                    )
                    .bind(stream_id)
                    .bind(t)
                    .execute(&mut **tx)
                    .await?;
                }
            }

            let state = StreamState {
                archived_at,
                retention_class,
            };
            if state.is_archived() && !allow_archived {
                return Err(Error::StreamArchived { stream_id });
            }
            return Ok(state);
        }

        sqlx::query(
            "insert into rf_streams (stream_id, tenant_id) values ($1, $2) on conflict (stream_id) do nothing",
        )
        .bind(stream_id)
        .bind(tenant.map(|t| t.as_str()))
        .execute(&mut **tx)
        .await?;

        Ok(StreamState::hot())
    }

    pub(crate) fn apply_legacy_upcasters(envelope: &mut EventEnvelope) {
        loop {
            let maybe = {
                let guard = UPCASTERS.get_or_init(Default::default);
                guard.lock().ok().and_then(|map| {
                    map.get(&(envelope.typ.clone(), envelope.event_version))
                        .cloned()
                })
            };

            let Some(upcaster) = maybe else {
                break;
            };

            match (upcaster)(envelope.body.clone()) {
                Ok(new_body) => {
                    envelope.body = new_body;
                    envelope.event_version += 1;
                }
                Err(_) => break,
            }
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
            ..AppendOptions::default()
        };
        self.append_with(stream_id, expected, events, &opts).await
    }

    pub async fn read_stream(&self, stream_id: Uuid) -> Result<Vec<(i32, Event)>> {
        let envelopes = self
            .load_stream_envelopes(stream_id, None, None, true)
            .await?;

        Ok(envelopes
            .into_iter()
            .map(|env| {
                (
                    env.stream_seq,
                    Event {
                        typ: env.typ,
                        body: env.body,
                    },
                )
            })
            .collect())
    }

    pub async fn read_stream_envelopes(&self, stream_id: Uuid) -> Result<Vec<EventEnvelope>> {
        let mut envelopes = self
            .load_stream_envelopes(stream_id, None, None, true)
            .await?;

        if let Some(registry) = &self.upcaster_registry {
            for envelope in &mut envelopes {
                registry
                    .upcast_with_pool(envelope, Some(&self.pool))
                    .await?;
            }
        } else {
            for envelope in &mut envelopes {
                Self::apply_legacy_upcasters(envelope);
            }
        }

        Ok(envelopes)
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

        let stream_state = self
            .ensure_stream_state(tx, stream_id, tenant, opts.allow_archived_stream)
            .await?;

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
        let use_partitioned = self.using_partitioned_backend();
        let retention_class = if use_partitioned && stream_state.is_archived() {
            "cold"
        } else {
            "hot"
        };

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
                if use_partitioned {
                    insert.push("retention_class, ");
                }
                insert.push("stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id) values (");
                insert.push_bind(value);
                insert.push(", ");
            } else {
                if use_partitioned {
                    insert.push("retention_class, ");
                }
                insert.push(
                    "stream_id, stream_seq, event_type, body, headers, causation_id, correlation_id, event_version, user_id) values (",
                );
            }
            if use_partitioned {
                insert.push_bind(retention_class);
                insert.push(", ");
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
                let handlers = INLINE_HANDLERS
                    .get_or_init(Default::default)
                    .lock()
                    .map(|guard| guard.iter().cloned().collect::<Vec<_>>())
                    .ok();
                if let Some(handlers) = handlers {
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
        let moved = if self.using_partitioned_backend() {
            let mut update =
                QueryBuilder::<Postgres>::new("update events set retention_class = 'cold' where ");
            self.push_tenant_filter(&mut update, tenant_ref);
            update.push("stream_id = ");
            update.push_bind(stream_id);
            update.push(" and created_at < ");
            update.push_bind(before);
            update.push(" and retention_class <> 'cold'");
            update.build().execute(&mut *tx).await?.rows_affected()
        } else {
            // Move rows to archive table
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

            let mut delete = QueryBuilder::<Postgres>::new("delete from events where ");
            self.push_tenant_filter(&mut delete, tenant_ref);
            delete.push("stream_id = ");
            delete.push_bind(stream_id);
            delete.push(" and created_at < ");
            delete.push_bind(before);
            delete.build().execute(&mut *tx).await?;
            moved
        };

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
        let stream_state = self
            .ensure_stream_state(&mut tx, stream_id, tenant_ref, false)
            .await?;
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
        let use_partitioned = self.using_partitioned_backend();
        let retention_class = if use_partitioned && stream_state.is_archived() {
            "cold"
        } else {
            "hot"
        };

        let mut insert = QueryBuilder::<Postgres>::new("insert into events (");
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant_ref) {
            insert.push(format!("{}, ", schema::quote_ident(column)));
            if use_partitioned {
                insert.push("retention_class, ");
            }
            insert.push("stream_id, stream_seq, event_type, body, headers, is_tombstone, event_version, user_id) values (");
            insert.push_bind(value);
            insert.push(", ");
        } else {
            if use_partitioned {
                insert.push("retention_class, ");
            }
            insert.push(
                "stream_id, stream_seq, event_type, body, headers, is_tombstone, event_version, user_id) values (",
            );
        }
        if use_partitioned {
            insert.push_bind(retention_class);
            insert.push(", ");
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

/// Legacy global upcaster registration.
///
/// Prefer building a [`UpcasterRegistry`](crate::upcasting::UpcasterRegistry) and wiring it through
/// [`Store::builder().upcasters`](crate::store::StoreBuilder::upcasters). This helper will be removed in a future release once dependent crates migrate.
#[deprecated(note = "Use UpcasterRegistry with Store::builder().upcasters(...) instead")]
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

    pub fn allow_archived_stream(mut self, allow: bool) -> Self {
        self.opts.allow_archived_stream = allow;
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
