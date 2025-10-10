use std::collections::HashSet;

use crate::Result;
use indoc::formatdoc;
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub struct SchemaManager {
    pool: PgPool,
}

impl SchemaManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn plan(&self, config: &SchemaConfig) -> Result<SchemaPlan> {
        let mut plan = SchemaPlan::default();
        let existing_schemas = self.existing_schemas().await?;

        let base_schema = config.base_schema.trim();
        self.plan_for_schema(&mut plan, base_schema, &existing_schemas)
            .await?;

        if let TenancyMode::SchemaPerTenant { tenants } = &config.tenancy_mode {
            for tenant in tenants {
                let schema = tenant.schema.trim();
                self.plan_for_schema(&mut plan, schema, &existing_schemas)
                    .await?;
            }
        }

        Ok(plan)
    }

    pub async fn apply(&self, plan: &SchemaPlan) -> Result<()> {
        if plan.actions.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        for action in &plan.actions {
            sqlx::query(action.sql()).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn sync(&self, config: &SchemaConfig) -> Result<SchemaPlan> {
        let plan = self.plan(config).await?;
        if !plan.is_empty() {
            self.apply(&plan).await?;
        }
        Ok(plan)
    }

    async fn plan_core_schema(
        &self,
        plan: &mut SchemaPlan,
        schema: &str,
        schema_exists: bool,
    ) -> Result<()> {
        if !schema_exists {
            plan.push_action(
                format!("create schema {}", quote_ident(schema)),
                formatdoc!(
                    "create schema if not exists {schema}",
                    schema = quote_ident(schema),
                ),
            );
        }

        let existing_tables = if schema_exists {
            self.existing_tables(schema).await?
        } else {
            HashSet::new()
        };

        ensure_table(plan, schema, &existing_tables, "docs", build_docs_table_sql);
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "events",
            build_events_table_sql,
        );
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "projections",
            build_projections_table_sql,
        );

        let existing_indexes = if schema_exists {
            self.existing_indexes(schema).await?
        } else {
            HashSet::new()
        };

        ensure_index(
            plan,
            schema,
            &existing_indexes,
            "docs_gin",
            build_docs_index_sql,
        );

        // Projection runtime tables (control, leases, DLQ)
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "projection_control",
            build_projection_control_table_sql,
        );
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "projection_leases",
            build_projection_leases_table_sql,
        );
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "projection_dlq",
            build_projection_dlq_table_sql,
        );

        Ok(())
    }

    async fn existing_schemas(&self) -> Result<HashSet<String>> {
        let rows =
            sqlx::query_scalar::<_, String>("select schema_name from information_schema.schemata")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().collect())
    }

    async fn plan_for_schema(
        &self,
        plan: &mut SchemaPlan,
        schema: &str,
        existing_schemas: &HashSet<String>,
    ) -> Result<()> {
        let schema = schema.trim();

        if schema.is_empty() {
            plan.push_warning("schema name is empty; skipping".to_string());
            return Ok(());
        }

        if plan.has_schema(schema) {
            plan.push_warning(format!(
                "schema `{}` already planned; skipping duplicate entry",
                schema
            ));
            return Ok(());
        }

        let exists = existing_schemas.contains(schema);
        plan.mark_schema(schema);
        self.plan_core_schema(plan, schema, exists).await
    }

    async fn existing_tables(&self, schema: &str) -> Result<HashSet<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "select table_name from information_schema.tables where table_schema = $1",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }

    async fn existing_indexes(&self, schema: &str) -> Result<HashSet<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "select indexname from pg_indexes where schemaname = $1",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }
}

#[derive(Clone, Debug)]
pub struct SchemaConfig {
    pub base_schema: String,
    pub tenancy_mode: TenancyMode,
}

impl SchemaConfig {
    pub fn single_tenant() -> Self {
        Self::default()
    }

    pub fn with_base_schema(schema: impl Into<String>) -> Self {
        Self {
            base_schema: schema.into(),
            ..Self::default()
        }
    }
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            base_schema: "public".to_string(),
            tenancy_mode: TenancyMode::SingleTenant,
        }
    }
}

#[derive(Clone, Debug)]
pub enum TenancyMode {
    SingleTenant,
    SchemaPerTenant { tenants: Vec<TenantSchema> },
}

impl TenancyMode {
    pub fn schema_per_tenant<I>(tenants: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<TenantSchema>,
    {
        let tenants = tenants.into_iter().map(Into::into).collect();
        Self::SchemaPerTenant { tenants }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantSchema {
    pub schema: String,
}

impl TenantSchema {
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }
}

impl From<&str> for TenantSchema {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TenantSchema {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SchemaPlan {
    actions: Vec<SchemaAction>,
    warnings: Vec<String>,
    seen_schemas: HashSet<String>,
}

impl SchemaPlan {
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn actions(&self) -> &[SchemaAction] {
        &self.actions
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn push_action(&mut self, description: String, sql: String) {
        self.actions.push(SchemaAction { description, sql });
    }

    fn push_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    fn mark_schema(&mut self, schema: &str) {
        self.seen_schemas.insert(schema.to_lowercase());
    }

    fn has_schema(&self, schema: &str) -> bool {
        self.seen_schemas.contains(&schema.to_lowercase())
    }
}

#[derive(Clone, Debug)]
pub struct SchemaAction {
    description: String,
    sql: String,
}

impl SchemaAction {
    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

fn ensure_table<F>(
    plan: &mut SchemaPlan,
    schema: &str,
    existing_tables: &HashSet<String>,
    table: &str,
    build_sql: F,
) where
    F: Fn(&str) -> String,
{
    if !existing_tables.contains(table) {
        plan.push_action(
            format!("create table {}", qualified_name(schema, table)),
            build_sql(schema),
        );
    }
}

fn ensure_index<F>(
    plan: &mut SchemaPlan,
    schema: &str,
    existing_indexes: &HashSet<String>,
    index: &str,
    build_sql: F,
) where
    F: Fn(&str) -> String,
{
    if !existing_indexes.contains(index) {
        plan.push_action(
            format!("create index {}", qualified_name(schema, index)),
            build_sql(schema),
        );
    }
}

fn build_docs_table_sql(schema: &str) -> String {
    formatdoc!(
        "
        create table if not exists {table} (
            id uuid primary key,
            doc jsonb not null,
            version int not null default 0,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "docs"),
    )
}

fn build_events_table_sql(schema: &str) -> String {
    formatdoc!(
        "
        create table if not exists {table} (
            global_seq bigserial primary key,
            stream_id uuid not null,
            stream_seq int not null,
            event_type text not null,
            body jsonb not null,
            headers jsonb not null default '{{}}'::jsonb,
            causation_id uuid null,
            correlation_id uuid null,
            created_at timestamptz not null default now(),
            unique (stream_id, stream_seq)
        )
        ",
        table = qualified_name(schema, "events"),
    )
}

fn build_projections_table_sql(schema: &str) -> String {
    formatdoc!(
        "
        create table if not exists {table} (
            name text primary key,
            last_seq bigint not null default 0,
            updated_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "projections"),
    )
}

fn build_docs_index_sql(schema: &str) -> String {
    formatdoc!(
        "
        create index if not exists {index} on {table}
            using gin (doc)
        ",
        index = quote_ident("docs_gin"),
        table = qualified_name(schema, "docs"),
    )
}

fn build_projection_control_table_sql(schema: &str) -> String {
    formatdoc!(
        "
        create table if not exists {table} (
            name text primary key,
            paused boolean not null default false,
            attempts int not null default 0,
            backoff_until timestamptz null,
            updated_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "projection_control"),
    )
}

fn build_projection_leases_table_sql(schema: &str) -> String {
    formatdoc!(
        "
        create table if not exists {table} (
            name text primary key,
            leased_by text not null,
            lease_until timestamptz not null,
            updated_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "projection_leases"),
    )
}

fn build_projection_dlq_table_sql(schema: &str) -> String {
    formatdoc!(
        "
        create table if not exists {table} (
            id bigserial primary key,
            name text not null,
            global_seq bigint not null,
            event_type text not null,
            body jsonb not null,
            error text not null,
            failed_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "projection_dlq"),
    )
}

fn qualified_name(schema: &str, ident: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(ident))
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified() {
        assert_eq!(qualified_name("public", "docs"), "\"public\".\"docs\"");
    }

    #[test]
    fn quote_handles_quotes() {
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }
}
