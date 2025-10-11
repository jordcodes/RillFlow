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
