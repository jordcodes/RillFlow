use std::collections::HashSet;

use crate::Result;
use indoc::formatdoc;
use sqlx::PgPool;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantColumnType {
    Text,
    Uuid,
}

impl TenantColumnType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            TenantColumnType::Text => "text",
            TenantColumnType::Uuid => "uuid",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantColumn {
    pub name: String,
    pub data_type: TenantColumnType,
}

impl TenantColumn {
    pub fn new(name: impl Into<String>, data_type: TenantColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    pub fn text(name: impl Into<String>) -> Self {
        Self::new(name, TenantColumnType::Text)
    }

    pub fn uuid(name: impl Into<String>) -> Self {
        Self::new(name, TenantColumnType::Uuid)
    }
}

impl Default for TenantColumn {
    fn default() -> Self {
        Self::uuid("tenant_id")
    }
}

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
        let tenant_column = match &config.tenancy_mode {
            TenancyMode::Conjoined { column } => Some(column),
            _ => None,
        };
        self.plan_for_schema(&mut plan, base_schema, &existing_schemas, tenant_column)
            .await?;

        if let TenancyMode::SchemaPerTenant { tenants } = &config.tenancy_mode {
            for tenant in tenants {
                let schema = tenant.schema.trim();
                self.plan_for_schema(&mut plan, schema, &existing_schemas, None)
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
        tenant_column: Option<&TenantColumn>,
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

        let docs_exists = existing_tables.contains("docs");
        let docs_history_exists = existing_tables.contains("docs_history");
        let events_exists = existing_tables.contains("events");
        let events_archive_exists = existing_tables.contains("events_archive");
        let projections_exists = existing_tables.contains("projections");
        let snapshots_exists = existing_tables.contains("snapshots");
        let stream_aliases_exists = existing_tables.contains("stream_aliases");
        let subscriptions_exists = existing_tables.contains("subscriptions");
        let subscription_groups_exists = existing_tables.contains("subscription_groups");
        let subscription_group_leases_exists =
            existing_tables.contains("subscription_group_leases");
        let subscription_dlq_exists = existing_tables.contains("subscription_dlq");

        ensure_table(plan, schema, &existing_tables, "docs", |s| {
            build_docs_table_sql(s, tenant_column)
        });
        if docs_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "docs", column)
                    .await?;
            }
        }
        ensure_table(plan, schema, &existing_tables, "docs_history", |s| {
            build_docs_history_table_sql(s, tenant_column)
        });
        if docs_history_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "docs_history", column)
                    .await?;
            }
        }
        ensure_table(plan, schema, &existing_tables, "events", |s| {
            build_events_table_sql(s, tenant_column)
        });
        if events_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "events", column)
                    .await?;
            }
        }
        ensure_table(plan, schema, &existing_tables, "events_archive", |s| {
            build_events_archive_table_sql(s, tenant_column)
        });
        if events_archive_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "events_archive", column)
                    .await?;
            }
        }
        ensure_table(plan, schema, &existing_tables, "projections", |s| {
            build_projections_table_sql(s, tenant_column)
        });
        if projections_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "projections", column)
                    .await?;
            }
        }

        // event schema registry
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "event_schemas",
            build_event_schemas_table_sql,
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

        // Full-text search index
        ensure_index(
            plan,
            schema,
            &existing_indexes,
            "docs_fts_idx",
            build_docs_fts_index_sql,
        );

        // FTS function and triggers
        plan.push_action(
            format!(
                "create function {}.rf_docs_search_update()",
                quote_ident(schema)
            ),
            build_docs_fts_fn_sql(schema),
        );
        plan.push_action(
            format!(
                "create triggers for {}.docs full-text search",
                quote_ident(schema)
            ),
            build_docs_fts_triggers_sql(schema),
        );

        // History function and triggers
        plan.push_action(
            format!("create function {}.rf_docs_history()", quote_ident(schema)),
            build_docs_history_fn_sql(schema),
        );
        plan.push_action(
            format!("create triggers for {}.docs history", quote_ident(schema)),
            build_docs_history_triggers_sql(schema),
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

        // Snapshots support
        ensure_table(plan, schema, &existing_tables, "snapshots", |s| {
            build_snapshots_table_sql(s, tenant_column)
        });
        if snapshots_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "snapshots", column)
                    .await?;
            }
        }

        // Stream aliases
        ensure_table(plan, schema, &existing_tables, "stream_aliases", |s| {
            build_stream_aliases_table_sql(s, tenant_column)
        });
        if stream_aliases_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "stream_aliases", column)
                    .await?;
            }
        }

        // Subscription consumer groups (per-group checkpoints and leases)
        ensure_table(plan, schema, &existing_tables, "subscriptions", |s| {
            build_subscriptions_table_sql(s, tenant_column)
        });
        if subscriptions_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "subscriptions", column)
                    .await?;
            }
        }
        ensure_table(plan, schema, &existing_tables, "subscription_groups", |s| {
            build_subscription_groups_table_sql(s, tenant_column)
        });
        if subscription_groups_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "subscription_groups", column)
                    .await?;
            }
        }
        ensure_table(
            plan,
            schema,
            &existing_tables,
            "subscription_group_leases",
            |s| build_subscription_group_leases_table_sql(s, tenant_column),
        );
        if subscription_group_leases_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "subscription_group_leases", column)
                    .await?;
            }
        }
        ensure_table(plan, schema, &existing_tables, "subscription_dlq", |s| {
            build_subscription_dlq_table_sql(s, tenant_column)
        });
        if subscription_dlq_exists {
            if let Some(column) = tenant_column {
                self.ensure_tenant_column(plan, schema, "subscription_dlq", column)
                    .await?;
            }
        }

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
        tenant_column: Option<&TenantColumn>,
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
        self.plan_core_schema(plan, schema, exists, tenant_column)
            .await
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

    async fn existing_columns(&self, schema: &str, table: &str) -> Result<HashSet<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "select column_name from information_schema.columns where table_schema = $1 and table_name = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }

    async fn ensure_tenant_column(
        &self,
        plan: &mut SchemaPlan,
        schema: &str,
        table: &str,
        column: &TenantColumn,
    ) -> Result<()> {
        let columns = self.existing_columns(schema, table).await?;
        if columns.contains(&column.name) {
            return Ok(());
        }

        let table_name = qualified_name(schema, table);
        let action = format!(
            "alter table {} add column if not exists {} {}",
            table_name,
            quote_ident(&column.name),
            column.data_type.as_sql()
        );
        plan.push_action(
            format!("add {} column to {}", quote_ident(&column.name), table_name),
            action,
        );
        Ok(())
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

    pub fn conjoined_with(column: TenantColumn) -> Self {
        Self {
            base_schema: "public".to_string(),
            tenancy_mode: TenancyMode::conjoined(column),
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
    Conjoined { column: TenantColumn },
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

    pub fn conjoined(column: TenantColumn) -> Self {
        Self::Conjoined { column }
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

fn build_docs_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        "
        create table if not exists {table} (
{tenant_column}
            id uuid primary key,
            doc jsonb not null,
            version int not null default 0,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now(),
            deleted_at timestamptz null,
            created_by text null,
            last_modified_by text null,
            docs_search tsvector null
        )
        ",
        table = qualified_name(schema, "docs"),
        tenant_column = tenant_column_sql,
    )
}

fn build_docs_history_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        "
        create table if not exists {table} (
            hist_id bigserial primary key,
{tenant_column}
            id uuid not null,
            version int not null,
            doc jsonb not null,
            modified_at timestamptz not null default now(),
            modified_by text null,
            op text not null
        )
        ",
        table = qualified_name(schema, "docs_history"),
        tenant_column = tenant_column_sql,
    )
}

fn build_events_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        "
        create table if not exists {table} (
{tenant_column}
            global_seq bigserial primary key,
            stream_id uuid not null,
            stream_seq int not null,
            event_type text not null,
            body jsonb not null,
            headers jsonb not null default '{{}}'::jsonb,
            causation_id uuid null,
            correlation_id uuid null,
            event_version int not null default 1,
            user_id text null,
            is_tombstone boolean not null default false,
            created_at timestamptz not null default now(),
            unique (stream_id, stream_seq)
        )
        ",
        table = qualified_name(schema, "events"),
        tenant_column = tenant_column_sql,
    )
}

fn build_events_archive_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        "
        create table if not exists {table} (
{tenant_column}
            global_seq bigint not null,
            stream_id uuid not null,
            stream_seq int not null,
            event_type text not null,
            body jsonb not null,
            headers jsonb not null default '{{}}'::jsonb,
            causation_id uuid null,
            correlation_id uuid null,
            event_version int not null default 1,
            user_id text null,
            is_tombstone boolean not null default false,
            created_at timestamptz not null,
            primary key (global_seq)
        )
        ",
        table = qualified_name(schema, "events_archive"),
        tenant_column = tenant_column_sql,
    )
}

fn build_projections_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        "
        create table if not exists {table} (
{tenant_column}
            name text primary key,
            last_seq bigint not null default 0,
            updated_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "projections"),
        tenant_column = tenant_column_sql,
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

fn build_docs_fts_index_sql(schema: &str) -> String {
    formatdoc!(
        "
        create index if not exists {index} on {table}
            using gin (docs_search)
        ",
        index = quote_ident("docs_fts_idx"),
        table = qualified_name(schema, "docs"),
    )
}

fn build_docs_history_fn_sql(schema: &str) -> String {
    formatdoc!(
        r#"
        create or replace function {schema}.rf_docs_history() returns trigger as $$
        begin
          if TG_OP = 'UPDATE' then
            insert into {schema}.docs_history(id, version, doc, modified_at, modified_by, op)
            values (OLD.id, OLD.version, OLD.doc, now(), current_user, 'UPDATE');
            return NEW;
          elsif TG_OP = 'DELETE' then
            insert into {schema}.docs_history(id, version, doc, modified_at, modified_by, op)
            values (OLD.id, OLD.version, OLD.doc, now(), current_user, 'DELETE');
            return OLD;
          else
            return NEW;
          end if;
        end;
        $$ language plpgsql;
        "#,
        schema = quote_ident(schema),
    )
}

fn build_docs_history_triggers_sql(schema: &str) -> String {
    formatdoc!(
        r#"
        do $$
        begin
          if not exists (
            select 1 from pg_trigger t
            join pg_class c on c.oid = t.tgrelid
            join pg_namespace n on n.oid = c.relnamespace
            where t.tgname = 'rf_docs_history_update' and c.relname = 'docs' and n.nspname = {schema_lit}
          ) then
            execute 'create trigger rf_docs_history_update after update on {tbl} for each row execute function {fn}()';
          end if;
          if not exists (
            select 1 from pg_trigger t
            join pg_class c on c.oid = t.tgrelid
            join pg_namespace n on n.oid = c.relnamespace
            where t.tgname = 'rf_docs_history_delete' and c.relname = 'docs' and n.nspname = {schema_lit}
          ) then
            execute 'create trigger rf_docs_history_delete after delete on {tbl} for each row execute function {fn}()';
          end if;
        end$$;
        "#,
        schema_lit = format!("'{}'", schema),
        tbl = qualified_name(schema, "docs"),
        fn = format!("{}.{}", quote_ident(schema), quote_ident("rf_docs_history")),
    )
}

fn build_docs_fts_fn_sql(schema: &str) -> String {
    formatdoc!(
        r#"
        create or replace function {schema}.rf_docs_search_update() returns trigger as $$
        begin
          new.docs_search := jsonb_to_tsvector('english', new.doc, '["all"]');
          return new;
        end;
        $$ language plpgsql;
        "#,
        schema = quote_ident(schema),
    )
}

fn build_docs_fts_triggers_sql(schema: &str) -> String {
    let tbl = qualified_name(schema, "docs");
    let fnq = format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident("rf_docs_search_update")
    );
    formatdoc!(
        r#"
        do $$
        begin
          if not exists (
            select 1 from pg_trigger t
            join pg_class c on c.oid = t.tgrelid
            join pg_namespace n on n.oid = c.relnamespace
            where t.tgname = 'rf_docs_fts_biu' and c.relname = 'docs' and n.nspname = {schema_lit}
          ) then
            execute 'create trigger rf_docs_fts_biu before insert or update of doc on {tbl} for each row execute function {fnq}()';
          end if;
        end$$;
        "#,
        schema_lit = format!("'{}'", schema),
        tbl = tbl,
        fnq = fnq,
    )
}

fn build_snapshots_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        "
        create table if not exists {table} (
{tenant_column}
            stream_id uuid primary key,
            version int not null,
            body jsonb not null,
            created_at timestamptz not null default now()
        )
        ",
        table = qualified_name(schema, "snapshots"),
        tenant_column = tenant_column_sql,
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

fn build_event_schemas_table_sql(schema: &str) -> String {
    formatdoc!(
        r#"
        create table if not exists {table} (
            event_type text not null,
            version int not null,
            schema jsonb not null,
            created_at timestamptz not null default now(),
            primary key(event_type, version)
        )
        "#,
        table = qualified_name(schema, "event_schemas"),
    )
}

fn build_stream_aliases_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        r#"
        create table if not exists {table} (
{tenant_column}
            alias text primary key,
            stream_id uuid not null,
            created_at timestamptz not null default now()
        )
        "#,
        table = qualified_name(schema, "stream_aliases"),
        tenant_column = tenant_column_sql,
    )
}

fn build_subscriptions_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    let primary_key = tenant_column
        .map(|col| format!("primary key ({}, name)", quote_ident(&col.name)))
        .unwrap_or_else(|| "primary key (name)".to_string());
    formatdoc!(
        r#"
        create table if not exists {table} (
{tenant_column}
            name text not null,
            last_seq bigint not null default 0,
            filter jsonb not null default '{{}}'::jsonb,
            paused boolean not null default false,
            backoff_until timestamptz null,
            updated_at timestamptz not null default now(),
            {primary_key}
        )
        "#,
        table = qualified_name(schema, "subscriptions"),
        tenant_column = tenant_column_sql,
        primary_key = primary_key,
    )
}

fn build_subscription_groups_table_sql(
    schema: &str,
    tenant_column: Option<&TenantColumn>,
) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    let primary_key = tenant_column
        .map(|col| format!("primary key ({}, name, grp)", quote_ident(&col.name)))
        .unwrap_or_else(|| "primary key (name, grp)".to_string());
    formatdoc!(
        r#"
        create table if not exists {table} (
{tenant_column}
            name text not null,
            grp text not null,
            last_seq bigint not null default 0,
            paused boolean not null default false,
            backoff_until timestamptz null,
            max_in_flight int null,
            updated_at timestamptz not null default now(),
            {primary_key}
        )
        "#,
        table = qualified_name(schema, "subscription_groups"),
        tenant_column = tenant_column_sql,
        primary_key = primary_key,
    )
}

fn build_subscription_group_leases_table_sql(
    schema: &str,
    tenant_column: Option<&TenantColumn>,
) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    let primary_key = tenant_column
        .map(|col| format!("primary key ({}, name, grp)", quote_ident(&col.name)))
        .unwrap_or_else(|| "primary key (name, grp)".to_string());
    formatdoc!(
        r#"
        create table if not exists {table} (
{tenant_column}
            name text not null,
            grp text not null,
            leased_by text not null,
            lease_until timestamptz not null,
            updated_at timestamptz not null default now(),
            {primary_key}
        )
        "#,
        table = qualified_name(schema, "subscription_group_leases"),
        tenant_column = tenant_column_sql,
        primary_key = primary_key,
    )
}

fn build_subscription_dlq_table_sql(schema: &str, tenant_column: Option<&TenantColumn>) -> String {
    let tenant_column_sql = tenant_column
        .map(|col| {
            format!(
                "            {} {} null,\n",
                quote_ident(&col.name),
                col.data_type.as_sql()
            )
        })
        .unwrap_or_default();
    formatdoc!(
        r#"
        create table if not exists {table} (
{tenant_column}
            id bigserial primary key,
            name text not null,
            global_seq bigint not null,
            event_type text not null,
            body jsonb not null,
            error text not null,
            failed_at timestamptz not null default now()
        )
        "#,
        table = qualified_name(schema, "subscription_dlq"),
        tenant_column = tenant_column_sql,
    )
}

pub fn qualified_name(schema: &str, ident: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(ident))
}

pub fn quote_ident(value: &str) -> String {
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
