use crate::{
    Result,
    query::{CompiledQuery, DocumentQuery, DocumentQueryContext},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

pub struct Documents {
    pub(crate) pool: PgPool,
}

impl Documents {
    pub async fn upsert<T: Serialize>(&self, id: &Uuid, doc: &T) -> Result<()> {
        let json = serde_json::to_value(doc)?;
        sqlx::query(
            r#"
            insert into docs (id, doc, version)
            values ($1, $2, 0)
            on conflict (id) do update
              set doc = excluded.doc,
                  version = docs.version + 1,
                  updated_at = now()
            "#,
        )
        .bind(id)
        .bind(&json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get<T: DeserializeOwned>(&self, id: &Uuid) -> Result<Option<T>> {
        let value: Option<Value> = sqlx::query_scalar("select doc from docs where id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(value.map(serde_json::from_value).transpose()?)
    }

    pub async fn delete(&self, id: &Uuid) -> Result<()> {
        sqlx::query("delete from docs where id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
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
}
