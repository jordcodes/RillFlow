use crate::{
    Result,
    context::SessionContext,
    schema,
    store::{TenantStrategy, tenant_schema_name},
};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};

use std::sync::Arc;

pub struct Projections {
    pool: PgPool,
    tenant_strategy: TenantStrategy,
    tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
}

impl Projections {
    pub(crate) fn new(
        pool: PgPool,
        tenant_strategy: TenantStrategy,
        tenant_resolver: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    ) -> Self {
        Self {
            pool,
            tenant_strategy,
            tenant_resolver,
        }
    }
}

#[async_trait]
pub trait ProjectionHandler: Send + Sync {
    async fn apply(
        &self,
        event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()>;
}

impl Projections {
    fn resolve_schema(&self, context: &SessionContext) -> Result<Option<String>> {
        match self.tenant_strategy {
            TenantStrategy::Single => Ok(None),
            TenantStrategy::Conjoined { .. } => {
                if context.tenant.is_some() {
                    Ok(None)
                } else if let Some(resolver) = &self.tenant_resolver {
                    if (resolver)().is_some() {
                        Ok(None)
                    } else {
                        Err(crate::Error::TenantRequired)
                    }
                } else {
                    Err(crate::Error::TenantRequired)
                }
            }
            TenantStrategy::SchemaPerTenant => {
                if let Some(ref tenant) = context.tenant {
                    Ok(Some(tenant_schema_name(tenant)))
                } else if let Some(resolver) = &self.tenant_resolver {
                    if let Some(tenant) = (resolver)() {
                        Ok(Some(tenant_schema_name(&tenant)))
                    } else {
                        Err(crate::Error::TenantRequired)
                    }
                } else {
                    Err(crate::Error::TenantRequired)
                }
            }
        }
    }

    async fn load_last_seq(tx: &mut Transaction<'_, Postgres>, name: &str) -> Result<i64> {
        let last_seq: Option<i64> =
            sqlx::query_scalar("select last_seq from projections where name = $1")
                .bind(name)
                .fetch_optional(&mut **tx)
                .await?;

        Ok(last_seq.unwrap_or(0))
    }

    async fn persist_checkpoint(
        tx: &mut Transaction<'_, Postgres>,
        name: &str,
        last_seq: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into projections (name, last_seq)
            values ($1, $2)
            on conflict (name) do update
              set last_seq = excluded.last_seq,
                  updated_at = now()
            "#,
        )
        .bind(name)
        .bind(last_seq)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn replay<H>(&self, name: &str, handler: &H) -> Result<()>
    where
        H: ProjectionHandler,
    {
        let mut tx = self.pool.begin().await?;
        let context = SessionContext::default();
        let schema = self.resolve_schema(&context)?;
        let schema_tenant = schema
            .as_ref()
            .and_then(|s| s.strip_prefix("tenant_").map(str::to_string));
        if let Some(ref schema_name) = schema {
            Self::set_local_search_path(&mut tx, schema_name).await?;
        }
        let last_seq = Self::load_last_seq(&mut tx, name).await?;

        let rows = sqlx::query(
            "select global_seq, event_type, body from events where global_seq > $1 order by global_seq asc"
        )
        .bind(last_seq)
        .fetch_all(&mut *tx)
        .await?;

        let mut new_last = last_seq;

        for row in rows {
            let event_type: String = row.get("event_type");
            let body: Value = row.get("body");
            let global_seq: i64 = row.get("global_seq");

            handler.apply(&event_type, &body, &mut tx).await?;
            crate::metrics::record_proj_events_processed(schema_tenant.as_deref(), 1);
            new_last = global_seq;
        }

        Self::persist_checkpoint(&mut tx, name, new_last).await?;

        tx.commit().await?;
        Ok(())
    }

    async fn set_local_search_path(
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

// ---------------------
// ViewProjection (declarative aggregation)

#[derive(Clone, Debug)]
pub enum ViewAgg {
    Count { alias: String },
    Sum { path: String, alias: String },
}

#[derive(Clone, Debug)]
pub struct ViewProjection {
    table: String,
    key_alias: String,
    key_path: String,
    aggregates: Vec<ViewAgg>,
}

impl ViewProjection {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            key_alias: "agg_key".to_string(),
            key_path: "id".to_string(),
            aggregates: Vec::new(),
        }
    }

    pub fn group_by(mut self, alias: &str, json_path: &str) -> Self {
        self.key_alias = alias.to_string();
        self.key_path = json_path.to_string();
        self
    }

    pub fn count(mut self, alias: &str) -> Self {
        self.aggregates.push(ViewAgg::Count {
            alias: alias.to_string(),
        });
        self
    }

    pub fn sum(mut self, json_path: &str, alias: &str) -> Self {
        self.aggregates.push(ViewAgg::Sum {
            path: json_path.to_string(),
            alias: alias.to_string(),
        });
        self
    }

    fn ensure_table_sql(&self) -> String {
        // Build a simple table with key column and numeric aggregate columns
        let mut cols = String::new();
        for agg in &self.aggregates {
            match agg {
                ViewAgg::Count { alias } => {
                    cols.push_str(&format!(", \"{}\" bigint not null default 0", alias));
                }
                ViewAgg::Sum { alias, .. } => {
                    cols.push_str(&format!(", \"{}\" numeric not null default 0", alias));
                }
            }
        }
        format!(
            "create table if not exists {} (\"{}\" text primary key{} )",
            self.table, self.key_alias, cols
        )
    }

    fn upsert_sql(&self) -> String {
        // Build an upsert statement incrementing counters
        let mut insert_cols = vec![format!("\"{}\"", self.key_alias)];
        let mut insert_vals = vec!["$1".to_string()];
        let mut updates = Vec::new();
        let mut idx = 2;
        for agg in &self.aggregates {
            match agg {
                ViewAgg::Count { alias } => {
                    insert_cols.push(format!("\"{}\"", alias));
                    insert_vals.push("1".to_string());
                    updates.push(format!(
                        "\"{alias}\" = {table}.\"{alias}\" + EXCLUDED.\"{alias}\"",
                        table = self.table
                    ));
                }
                ViewAgg::Sum { alias, .. } => {
                    insert_cols.push(format!("\"{}\"", alias));
                    insert_vals.push(format!("${}", idx));
                    updates.push(format!(
                        "\"{alias}\" = {table}.\"{alias}\" + EXCLUDED.\"{alias}\"",
                        table = self.table
                    ));
                    idx += 1;
                }
            }
        }
        format!(
            "insert into {table} ({cols}) values ({vals}) \
             on conflict(\"{key}\") do update set {updates}",
            table = self.table,
            cols = insert_cols.join(", "),
            vals = insert_vals.join(", "),
            key = self.key_alias,
            updates = updates.join(", ")
        )
    }
}

#[async_trait]
impl ProjectionHandler for ViewProjection {
    async fn apply(
        &self,
        _event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        // ensure table exists
        let create = self.ensure_table_sql();
        sqlx::query(&create).execute(&mut **tx).await?;

        // Extract key
        let key = json_path_get_text(body, &self.key_path).unwrap_or_default();

        // Build parameters for sum columns
        let mut params: Vec<sqlx::types::Json<Value>> = Vec::new();
        for agg in &self.aggregates {
            if let ViewAgg::Sum { path, .. } = agg {
                let val = json_path_get_number(body, path).unwrap_or(0.0);
                params.push(sqlx::types::Json(Value::from(val)));
            }
        }

        // Execute upsert
        let sql = self.upsert_sql();
        let mut q = sqlx::query(&sql).bind(&key);
        for p in params {
            // coerce numeric via CAST in SQL; here bind as json and cast in statement via EXCLUDED
            // For simplicity, bind numeric as text via to_string().
            let v = p.0.as_f64().unwrap_or(0.0);
            q = q.bind(v);
        }
        q.execute(&mut **tx).await?;
        Ok(())
    }
}

fn json_path_get_text(value: &Value, path: &str) -> Option<String> {
    let mut cur = value;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(arr) => arr.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur.as_str().unwrap_or(&cur.to_string()).to_string())
}

fn json_path_get_number(value: &Value, path: &str) -> Option<f64> {
    let mut cur = value;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(arr) => arr.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    cur.as_f64().or_else(|| cur.as_i64().map(|v| v as f64))
}

// ---------------------
// MultiStreamProjection adapter

#[async_trait]
pub trait MultiStreamProjection: Send + Sync {
    fn event_types(&self) -> &'static [&'static str];
    async fn apply_multi(
        &self,
        event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()>;
}

// ---------------------
// CustomProjection (arbitrary SQL per event type)

#[derive(Clone, Debug)]
pub struct CustomStep {
    pub event_types: Vec<String>,
    pub sql: String,
    pub arg_paths: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct CustomProjection {
    steps: Vec<CustomStep>,
}

impl CustomProjection {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn on(mut self, event_types: &[&str], sql: impl Into<String>) -> Self {
        self.steps.push(CustomStep {
            event_types: event_types.iter().map(|s| s.to_string()).collect(),
            sql: sql.into(),
            arg_paths: Vec::new(),
        });
        self
    }

    pub fn args(mut self, paths: &[&str]) -> Self {
        if let Some(last) = self.steps.last_mut() {
            last.arg_paths = paths.iter().map(|s| s.to_string()).collect();
        }
        self
    }
}

#[async_trait]
impl ProjectionHandler for CustomProjection {
    async fn apply(
        &self,
        event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        for step in &self.steps {
            if step.event_types.iter().any(|t| t == event_type) {
                let mut q = sqlx::query(&step.sql);
                for p in &step.arg_paths {
                    // Bind as text for broad compatibility; use casts in SQL if needed
                    let v = json_path_get_text(body, p).unwrap_or_default();
                    q = q.bind(v);
                }
                q.execute(&mut **tx).await?;
            }
        }
        Ok(())
    }
}

pub struct MultiStreamAdapter<T: MultiStreamProjection>(pub Arc<T>);

#[async_trait]
impl<T: MultiStreamProjection> ProjectionHandler for MultiStreamAdapter<T> {
    async fn apply(
        &self,
        event_type: &str,
        body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        if self.0.event_types().contains(&event_type) {
            self.0.apply_multi(event_type, body, tx).await
        } else {
            Ok(())
        }
    }
}
