use crate::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};

pub struct Projections {
    pub(crate) pool: PgPool,
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
    pub async fn replay<H>(&self, name: &str, handler: &H) -> Result<()>
    where
        H: ProjectionHandler,
    {
        let mut tx = self.pool.begin().await?;

        let last_seq: Option<i64> =
            sqlx::query_scalar("select last_seq from projections where name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?;

        let last_seq = last_seq.unwrap_or(0);

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
            new_last = global_seq;
        }

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
        .bind(new_last)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
