use sqlx::{Pool, Postgres};
use std::path::Path;

use crate::Result;

async fn run_sql_file(pool: &Pool<Postgres>, path: &str) -> Result<()> {
    if !Path::new(path).exists() {
        return Ok(());
    }

    let ddl = std::fs::read_to_string(path)?;
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
        eprintln!("executing migration stmt: {}", stmt);
        if let Err(err) = sqlx::query(&stmt).execute(pool).await {
            if let Some(db_err) = err.as_database_error() {
                eprintln!("database error: {}", db_err.message());
            } else {
                eprintln!("error: {err:?}");
            }
            return Err(err.into());
        }
    }
    Ok(())
}

pub async fn migrate_core_schema(pool: &Pool<Postgres>) -> Result<()> {
    for file in [
        "sql/0001_init.sql",
        "sql/0002_hotcold_daemon.sql",
        "sql/0003_event_archiving.sql",
        "sql/0004_event_archiving_partitioning.sql",
        "sql/0005_event_partitioning.sql",
    ] {
        run_sql_file(pool, file).await?;
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
