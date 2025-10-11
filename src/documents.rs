use crate::{
    Error, Result,
    query::{CompiledQuery, DocumentQuery, DocumentQueryContext},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, types::Json};
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
            crate::metrics::metrics()
                .doc_reads_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                    crate::metrics::metrics()
                        .doc_writes_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(ver + 1)
                } else {
                    crate::metrics::metrics()
                        .doc_conflicts_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            crate::metrics::metrics()
                .doc_writes_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(new_ver)
        } else if expected.is_some() {
            crate::metrics::metrics()
                .doc_conflicts_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
}
