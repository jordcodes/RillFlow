use crate::{Result, query::DocumentQuery};
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
        let mut tx = self.pool.begin().await?;

        let current_version: Option<i32> =
            sqlx::query_scalar("select version from docs where id = $1 for update")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;

        if current_version.is_some() {
            sqlx::query(
                "update docs set doc = $2, version = version + 1, updated_at = now() where id = $1",
            )
            .bind(id)
            .bind(&json)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query("insert into docs (id, doc, version) values ($1, $2, 0)")
                .bind(id)
                .bind(&json)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
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
}
