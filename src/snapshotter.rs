use std::sync::Arc;

use crate::{
    Aggregate, AggregateRepository, Result,
    events::ArchiveBackend,
    schema,
    store::{TenantStrategy, tenant_schema_name},
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tracing::{info, instrument};
use uuid::Uuid;

#[derive(Clone)]
pub struct SnapshotterConfig {
    pub threshold_events: i32,
    pub batch_size: i64,
    pub schema: Option<String>,
    pub tenant_strategy: TenantStrategy,
    pub tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    pub archive_backend: ArchiveBackend,
    pub include_archived: bool,
}

impl Default for SnapshotterConfig {
    fn default() -> Self {
        Self {
            threshold_events: 100,
            batch_size: 100,
            schema: None,
            tenant_strategy: TenantStrategy::Single,
            tenant_resolver: None,
            archive_backend: ArchiveBackend::DualTable,
            include_archived: false,
        }
    }
}

#[async_trait::async_trait]
pub trait SnapshotFolder: Send + Sync {
    async fn fold(&self, stream_id: Uuid) -> Result<(i32, Value)>;
}

pub struct AggregateFolder<A: Aggregate + serde::Serialize + Send + Sync + 'static> {
    repo: AggregateRepository,
    _phantom: std::marker::PhantomData<A>,
}

impl<A: Aggregate + serde::Serialize + Send + Sync + 'static> AggregateFolder<A> {
    pub fn new(repo: AggregateRepository) -> Self {
        Self {
            repo,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<A: Aggregate + serde::Serialize + Send + Sync + 'static> SnapshotFolder
    for AggregateFolder<A>
{
    async fn fold(&self, stream_id: Uuid) -> Result<(i32, Value)> {
        let agg: A = self.repo.load(stream_id).await?;
        let version = agg.version();
        let body = serde_json::to_value(&agg)?;
        Ok((version, body))
    }
}

pub struct Snapshotter<F: SnapshotFolder> {
    pool: PgPool,
    folder: Arc<F>,
    config: SnapshotterConfig,
}

impl<F: SnapshotFolder> Snapshotter<F> {
    pub fn new(pool: PgPool, folder: Arc<F>, config: SnapshotterConfig) -> Self {
        Self {
            pool,
            folder,
            config,
        }
    }

    fn schema_label(&self) -> String {
        match self.config.schema.as_ref() {
            Some(s) => s.clone(),
            None => match self.config.tenant_strategy {
                TenantStrategy::Single => "public".to_string(),
                TenantStrategy::SchemaPerTenant => "<resolver>".to_string(),
                TenantStrategy::Conjoined { .. } => "public".to_string(),
            },
        }
    }

    fn tenant_column(&self) -> Option<&str> {
        match &self.config.tenant_strategy {
            TenantStrategy::Conjoined { column } => Some(column.name.as_str()),
            _ => None,
        }
    }

    fn resolve_conjoined_tenant(&self) -> Result<Option<String>> {
        match &self.config.tenant_strategy {
            TenantStrategy::Conjoined { .. } => {
                if let Some(resolver) = &self.config.tenant_resolver {
                    if let Some(t) = (resolver)() {
                        return Ok(Some(t));
                    }
                }
                Err(crate::Error::TenantRequired)
            }
            _ => Ok(None),
        }
    }

    fn push_tenant_filter_with_alias<'a>(
        &self,
        qb: &mut QueryBuilder<'a, Postgres>,
        alias: &str,
        tenant: Option<&'a String>,
    ) {
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant) {
            qb.push(alias);
            qb.push(schema::quote_ident(column));
            qb.push(" = ");
            qb.push_bind(value);
            qb.push(" and ");
        }
    }

    fn resolve_schema(&self) -> Result<Option<String>> {
        match self.config.tenant_strategy {
            TenantStrategy::Single => Ok(self.config.schema.clone()),
            TenantStrategy::Conjoined { .. } => Ok(self.config.schema.clone()),
            TenantStrategy::SchemaPerTenant => {
                if let Some(ref explicit) = self.config.schema {
                    return Ok(Some(explicit.clone()));
                }
                if let Some(ref resolver) = self.config.tenant_resolver {
                    if let Some(tenant) = (resolver)() {
                        return Ok(Some(tenant_schema_name(&tenant)));
                    }
                }
                Err(crate::Error::TenantRequired)
            }
        }
    }

    fn include_cold(&self) -> bool {
        if matches!(self.config.archive_backend, ArchiveBackend::Partitioned) {
            self.config.include_archived
        } else {
            self.config.include_archived
                && matches!(self.config.archive_backend, ArchiveBackend::DualTable)
        }
    }

    /// Find streams exceeding the snapshot threshold and write snapshots for a batch.
    #[instrument(skip_all, fields(schema = %self.schema_label(), threshold = self.config.threshold_events, batch = self.config.batch_size))]
    pub async fn tick_once(&self) -> Result<u32> {
        let mut tx = self.pool.begin().await?;
        if let Some(schema) = self.resolve_schema()? {
            let stmt = format!("set local search_path to {}", quote_ident(&schema));
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }

        // Build query with optional tenant filter for conjoined mode
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();
        let include_cold = self.include_cold();
        let use_partitioned = matches!(self.config.archive_backend, ArchiveBackend::Partitioned);

        let mut qb = QueryBuilder::<Postgres>::new(
            "with combined as (select stream_id, stream_seq from events e where ",
        );
        self.push_tenant_filter_with_alias(&mut qb, "e.", tenant_ref);
        qb.push("1=1");
        if use_partitioned {
            if !include_cold {
                qb.push(" and e.retention_class = 'hot'");
            }
        } else if include_cold {
            qb.push(" union all select stream_id, stream_seq from events_archive ea where ");
            self.push_tenant_filter_with_alias(&mut qb, "ea.", tenant_ref);
            qb.push("1=1");
        }
        qb.push(") select c.stream_id, max(c.stream_seq) as head, coalesce(s.version, 0) as snap from combined c left join snapshots s on s.stream_id = c.stream_id");
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant_ref) {
            qb.push(" and s.");
            qb.push(schema::quote_ident(column));
            qb.push(" = ");
            qb.push_bind(value);
        }
        qb.push(" group by c.stream_id, s.version having max(c.stream_seq) - coalesce(s.version, 0) >= ");
        qb.push_bind(self.config.threshold_events);
        qb.push(" order by head desc limit ");
        qb.push_bind(self.config.batch_size);

        let rows: Vec<(Uuid, i32, i32)> = qb.build_query_as().fetch_all(&mut *tx).await?;

        if rows.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let mut processed = 0u32;
        for (stream_id, head, snap_ver) in rows {
            // outside tx to avoid long-held transaction during folding
            tx.commit().await?;
            let (version, body) = self.folder.fold(stream_id).await?;

            if let (Some(column), Some(tenant)) = (self.tenant_column(), tenant_ref) {
                let mut qb = QueryBuilder::<Postgres>::new("insert into snapshots(");
                qb.push(schema::quote_ident(column));
                qb.push(", stream_id, version, body) values (");
                qb.push_bind(tenant);
                qb.push(", ");
                qb.push_bind(stream_id);
                qb.push(", ");
                qb.push_bind(version);
                qb.push(", ");
                qb.push_bind(body);
                qb.push(") on conflict (");
                qb.push(schema::quote_ident(column));
                qb.push(", stream_id) do update set version = excluded.version, body = excluded.body, created_at = now()");
                qb.build().execute(&self.pool).await?;
            } else {
                sqlx::query(
                    r#"insert into snapshots(stream_id, version, body)
                        values ($1, $2, $3)
                        on conflict (stream_id) do update set version = excluded.version, body = excluded.body, created_at = now()"#,
                )
                .bind(stream_id)
                .bind(version)
                .bind(body)
                .execute(&self.pool)
                .await?;
            }
            processed += 1;
            info!(stream_id = %stream_id, head = head, snap_version = snap_ver, new_version = version, "snapshot written");
            // re-open a short transaction for search_path on next loop
            tx = self.pool.begin().await?;
            if let Some(schema) = self.resolve_schema()? {
                let stmt = format!("set local search_path to {}", quote_ident(&schema));
                sqlx::query(&stmt).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(processed)
    }

    #[instrument(skip_all, fields(schema = %self.schema_label()))]
    pub async fn run_until_idle(&self) -> Result<()> {
        loop {
            let n = self.tick_once().await?;
            if n == 0 {
                return Ok(());
            }
        }
    }

    pub async fn metrics(&self) -> Result<SnapshotterMetrics> {
        let tenant_value = self.resolve_conjoined_tenant()?;
        let tenant_ref = tenant_value.as_ref();

        // Build candidates query with tenant filter
        let include_cold = self.include_cold();
        let use_partitioned = matches!(self.config.archive_backend, ArchiveBackend::Partitioned);
        let mut qb_candidates = QueryBuilder::<Postgres>::new(
            "with combined as (select stream_id, stream_seq from events e where ",
        );
        self.push_tenant_filter_with_alias(&mut qb_candidates, "e.", tenant_ref);
        qb_candidates.push("1=1");
        if use_partitioned {
            if !include_cold {
                qb_candidates.push(" and e.retention_class = 'hot'");
            }
        } else if include_cold {
            qb_candidates
                .push(" union all select stream_id, stream_seq from events_archive ea where ");
            self.push_tenant_filter_with_alias(&mut qb_candidates, "ea.", tenant_ref);
            qb_candidates.push("1=1");
        }
        qb_candidates.push(") select count(1) from (select c.stream_id from combined c left join snapshots s on s.stream_id = c.stream_id");
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant_ref) {
            qb_candidates.push(" and s.");
            qb_candidates.push(schema::quote_ident(column));
            qb_candidates.push(" = ");
            qb_candidates.push_bind(value);
        }
        qb_candidates.push(" group by c.stream_id, s.version having max(c.stream_seq) - coalesce(s.version, 0) >= ");
        qb_candidates.push_bind(self.config.threshold_events);
        qb_candidates.push(") as candidates");

        let candidates: i64 = qb_candidates
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await?;

        // Build max gap query with tenant filter
        let mut qb_gap = QueryBuilder::<Postgres>::new(
            "with combined as (select stream_id, stream_seq from events e where ",
        );
        self.push_tenant_filter_with_alias(&mut qb_gap, "e.", tenant_ref);
        qb_gap.push("1=1");
        if use_partitioned {
            if !include_cold {
                qb_gap.push(" and e.retention_class = 'hot'");
            }
        } else if include_cold {
            qb_gap.push(" union all select stream_id, stream_seq from events_archive ea where ");
            self.push_tenant_filter_with_alias(&mut qb_gap, "ea.", tenant_ref);
            qb_gap.push("1=1");
        }
        qb_gap.push(
            ") select max(h.max_seq - coalesce(s.version, 0)) as gap from (select c.stream_id, max(c.stream_seq) as max_seq from combined c group by c.stream_id) h left join snapshots s on s.stream_id = h.stream_id",
        );
        if let (Some(column), Some(value)) = (self.tenant_column(), tenant_ref) {
            qb_gap.push(" and s.");
            qb_gap.push(schema::quote_ident(column));
            qb_gap.push(" = ");
            qb_gap.push_bind(value);
        }

        let max_gap: Option<i32> = qb_gap.build_query_scalar().fetch_one(&self.pool).await?;

        let out = SnapshotterMetrics {
            candidates,
            max_gap: max_gap.unwrap_or(0).max(0),
        };
        let tenant_label = self
            .config
            .schema
            .as_ref()
            .map(|s| s.trim_start_matches("tenant_").to_string())
            .or_else(|| {
                if matches!(
                    self.config.tenant_strategy,
                    TenantStrategy::SchemaPerTenant | TenantStrategy::Conjoined { .. }
                ) {
                    Some("<resolver>".to_string())
                } else {
                    None
                }
            });
        crate::metrics::record_snapshot_candidates(
            tenant_label.as_deref(),
            out.candidates.max(0) as u64,
        );
        crate::metrics::record_snapshot_max_gap(tenant_label.as_deref(), out.max_gap.max(0) as u64);
        Ok(out)
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

#[derive(Clone, Debug)]
pub struct SnapshotterMetrics {
    pub candidates: i64,
    pub max_gap: i32,
}
