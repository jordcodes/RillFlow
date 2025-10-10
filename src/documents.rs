use crate::{
    Error, Result,
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
            Ok(Some((doc, version)))
        } else {
            Ok(None)
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
                    Ok(ver + 1)
                } else {
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
