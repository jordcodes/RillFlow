use sqlx::{Pool, Postgres};

use crate::Result;

pub async fn migrate_core_schema(pool: &Pool<Postgres>) -> Result<()> {
    let ddl = std::fs::read_to_string("sql/0001_init.sql")?;
    // Split SQL by semicolons, but ignore semicolons inside $$ ... $$ blocks
    let mut stmts: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut chars = ddl.chars().peekable();
    let mut in_dollar = false;
    while let Some(ch) = chars.next() {
        if ch == '$' {
            if let Some('$') = chars.peek().copied() {
                // toggle dollar-quoted region
                in_dollar = !in_dollar;
                buf.push('$');
                buf.push('$');
                chars.next();
                continue;
            }
        }
        if ch == ';' && !in_dollar {
            let stmt = buf.trim();
            if !stmt.is_empty() {
                stmts.push(stmt.to_string());
            }
            buf.clear();
        } else {
            buf.push(ch);
        }
    }
    let tail = buf.trim();
    if !tail.is_empty() {
        stmts.push(tail.to_string());
    }

    for stmt in stmts {
        sqlx::query(&stmt).execute(pool).await?;
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
