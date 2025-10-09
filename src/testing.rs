use sqlx::{Pool, Postgres};

use crate::Result;

pub async fn migrate_core_schema(pool: &Pool<Postgres>) -> Result<()> {
    let ddl = std::fs::read_to_string("sql/0001_init.sql")?;
    for stmt in ddl
        .split(';')
        .map(str::trim)
        .filter(|stmt| !stmt.is_empty())
    {
        sqlx::query(stmt).execute(pool).await?;
    }
    Ok(())
}

pub async fn ensure_counters_table(pool: &Pool<Postgres>) -> Result<()> {
    sqlx::query(
        "create table if not exists counters (id uuid primary key, count int not null default 0)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

