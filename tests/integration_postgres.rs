use anyhow::Result;
use chrono::Utc;
use rillflow::events::{AppendOptions, ArchiveBackend};
use rillflow::{
    Error, Event, Expected, Store,
    projections::ProjectionHandler,
    query::{CompiledQuery, DocumentQueryContext, Predicate, SortDirection},
};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

struct CounterProjection;

#[async_trait::async_trait]
impl ProjectionHandler for CounterProjection {
    async fn apply(
        &self,
        _event_type: &str,
        _body: &Value,
        tx: &mut Transaction<'_, Postgres>,
    ) -> rillflow::Result<()> {
        sqlx::query(
            r#"
            insert into counters (id, count)
            values ($1, 1)
            on conflict (id) do update set count = counters.count + 1
            "#,
        )
        .bind(Uuid::nil())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn roundtrip() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::connect(&url).await?;

    rillflow::testing::migrate_core_schema(store.pool()).await?;
    rillflow::testing::ensure_counters_table(store.pool()).await?;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Customer {
        id: Uuid,
        email: String,
    }

    let id = Uuid::new_v4();
    let customer = Customer {
        id,
        email: "a@b.com".into(),
    };

    let mut session = store.session();
    session.store(id, &customer)?;
    session.enqueue_event(
        id,
        Expected::Any,
        Event::new("CustomerRegistered", &customer),
    )?;
    session.save_changes().await?;

    let fetched = store.docs().get::<Customer>(&id).await?;
    assert!(fetched.is_some());

    let items = store.events().read_stream(id).await?;
    assert_eq!(items.len(), 1);

    let projections = store.projections();
    projections.replay("counter", &CounterProjection).await?;

    let count: i32 = sqlx::query_scalar("select count from counters where id = $1")
        .bind(Uuid::nil())
        .fetch_one(store.pool())
        .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
async fn document_query_api_covers_indexed_and_non_indexed_fields() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::connect(&url).await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct Customer {
        id: Uuid,
        email: String,
        first_name: String,
        last_name: String,
        age: u32,
        tags: Vec<String>,
        active: bool,
        balance: f64,
    }

    let base = vec![
        Customer {
            id: Uuid::new_v4(),
            email: "ava@example.com".into(),
            first_name: "Ava".into(),
            last_name: "Stone".into(),
            age: 28,
            tags: vec!["vip".into(), "beta".into()],
            active: true,
            balance: 120.5,
        },
        Customer {
            id: Uuid::new_v4(),
            email: "li@example.com".into(),
            first_name: "Li".into(),
            last_name: "Wei".into(),
            age: 35,
            tags: vec!["standard".into()],
            active: true,
            balance: 80.0,
        },
        Customer {
            id: Uuid::new_v4(),
            email: "sam@example.com".into(),
            first_name: "Sam".into(),
            last_name: "Taylor".into(),
            age: 42,
            tags: vec!["vip".into(), "newsletter".into()],
            active: false,
            balance: -15.75,
        },
    ];

    for customer in &base {
        store.docs().upsert(&customer.id, customer).await?;
    }

    // Simulate consumer-provided index for email lookups.
    sqlx::query("create index if not exists docs_email_idx on docs ((lower(doc->>'email')))")
        .execute(store.pool())
        .await?;

    let vip_customers = store
        .docs()
        .query::<serde_json::Value>()
        .filter(Predicate::eq("active", true))
        .filter(Predicate::contains("tags", serde_json::json!(["vip"])))
        .order_by_number("balance", SortDirection::Desc)
        .select_fields(&[
            ("email", "email"),
            ("vip", "active"),
            ("balance", "balance"),
        ])
        .fetch_all()
        .await?;

    assert_eq!(vip_customers.len(), 1);
    assert_eq!(vip_customers[0]["email"], "ava@example.com");

    let mut extra = Vec::new();
    for i in 0..30 {
        extra.push(Customer {
            id: Uuid::new_v4(),
            email: format!("user{i}@example.com"),
            first_name: format!("User{i}"),
            last_name: "Example".into(),
            age: 25 + i,
            tags: vec!["bulk".into()],
            active: i % 2 == 0,
            balance: 60.0 + i as f64,
        });
    }

    for customer in &extra {
        store.docs().upsert(&customer.id, customer).await?;
    }

    let paged = store
        .docs()
        .query::<Customer>()
        .filter(Predicate::gt("age", 30.0))
        .order_by("last_name", SortDirection::Asc)
        .page(2, 5)
        .fetch_all()
        .await?;

    assert_eq!(paged.len(), 5);
    assert!(paged.iter().all(|c| c.age > 30));

    struct VipCustomers;

    impl CompiledQuery<serde_json::Value> for VipCustomers {
        fn configure(&self, ctx: &mut DocumentQueryContext) {
            ctx.filter(Predicate::eq("active", true))
                .filter(Predicate::contains("tags", serde_json::json!(["vip"])))
                .order_by("first_name", SortDirection::Asc)
                .select_fields(&[("email", "email"), ("status", "active")]);
        }
    }

    let compiled = store.docs().execute_compiled(VipCustomers).await?;
    assert_eq!(compiled.len(), 1);
    assert_eq!(compiled[0]["email"], "ava@example.com");

    Ok(())
}

#[tokio::test]
async fn expected_version_conflict() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::connect(&url).await?;

    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let stream_id = Uuid::new_v4();
    let event = Event::new("TestEvent", &serde_json::json!({"k": 1}));

    store
        .events()
        .append_stream(stream_id, Expected::Any, vec![event.clone()])
        .await?;

    let err = store
        .events()
        .append_stream(stream_id, Expected::Exact(0), vec![event])
        .await
        .expect_err("mismatched expected version should fail");

    assert!(matches!(err, rillflow::Error::VersionConflict));

    Ok(())
}

#[tokio::test]
async fn archived_stream_blocks_writes() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::connect(&url).await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let stream_id = Uuid::new_v4();
    let initial = Event::new("ExampleCreated", &json!({"n": 1}));

    store
        .events()
        .append_stream(stream_id, Expected::Any, vec![initial.clone()])
        .await?;

    let retention: Option<String> =
        sqlx::query_scalar("select retention_class from rf_streams where stream_id = $1")
            .bind(stream_id)
            .fetch_optional(store.pool())
            .await?;
    assert_eq!(retention.as_deref(), Some("hot"));

    sqlx::query(
        "update rf_streams set archived_at = now(), retention_class = 'cold' where stream_id = $1",
    )
    .bind(stream_id)
    .execute(store.pool())
    .await?;

    let err = store
        .events()
        .append_stream(stream_id, Expected::Any, vec![initial.clone()])
        .await
        .expect_err("archived stream should reject appends");
    assert!(matches!(err, Error::StreamArchived { .. }));

    let mut override_opts = AppendOptions::default();
    override_opts.allow_archived_stream = true;
    store
        .events()
        .append_with(
            stream_id,
            Expected::Any,
            vec![Event::new("Backfill", &json!({"n": 2}))],
            &override_opts,
        )
        .await?;

    let total = store
        .events()
        .with_archived()
        .read_stream(stream_id)
        .await?
        .len();
    assert_eq!(total, 2);

    Ok(())
}

#[tokio::test]
async fn archived_reads_require_opt_in() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::connect(&url).await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let stream_id = Uuid::new_v4();
    let event = Event::new("ExampleCreated", &json!({"n": 1}));

    store
        .events()
        .append_stream(stream_id, Expected::Any, vec![event.clone()])
        .await?;

    store
        .events()
        .archive_stream_before(stream_id, Utc::now() + chrono::Duration::seconds(1))
        .await?;

    let hot_only = store.events().read_stream(stream_id).await?;
    assert!(hot_only.is_empty());

    let archived = store
        .events()
        .with_archived()
        .read_stream(stream_id)
        .await?;
    assert_eq!(archived.len(), 1);

    Ok(())
}

#[tokio::test]
async fn archive_mark_move_reactivate_flow() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::connect(&url).await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let stream_id = Uuid::new_v4();
    let events: Vec<_> = (0..5)
        .map(|i| Event::new("Demo", &json!({ "n": i })))
        .collect();
    store
        .events()
        .append_stream(stream_id, Expected::Any, events)
        .await?;

    // Mark stream archived via rf_streams metadata.
    sqlx::query(
        "insert into rf_streams (stream_id, archived_at, archived_by, archive_reason, retention_class)\
         values ($1, now(), $2, $3, 'cold')\
         on conflict (stream_id) do update set archived_at = excluded.archived_at, archived_by = excluded.archived_by, archive_reason = excluded.archive_reason, retention_class = excluded.retention_class",
    )
    .bind(stream_id)
    .bind("tester")
    .bind("integration test")
    .execute(store.pool())
    .await?;

    // Move events to archive table.
    let moved = store
        .events()
        .archive_stream_before(stream_id, Utc::now() + chrono::Duration::seconds(1))
        .await?;
    assert_eq!(moved, 5);

    let in_archive: i64 =
        sqlx::query_scalar("select count(*) from events_archive where stream_id = $1")
            .bind(stream_id)
            .fetch_one(store.pool())
            .await?;
    assert_eq!(in_archive, 5);

    // Reactivate stream and write again.
    sqlx::query(
        "update rf_streams set archived_at = null, archived_by = null, archive_reason = null, retention_class = 'hot' where stream_id = $1",
    )
    .bind(stream_id)
    .execute(store.pool())
    .await?;

    store
        .events()
        .append_stream(
            stream_id,
            Expected::Any,
            vec![Event::new("HotResume", &json!({ "n": 99 }))],
        )
        .await?;

    let current = store
        .events()
        .with_archived()
        .read_stream(stream_id)
        .await?;
    assert_eq!(current.len(), 6);

    Ok(())
}

#[tokio::test]
async fn partitioned_backend_routes_events() -> Result<()> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");

    let container = image.start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let store = Store::builder(&url)
        .archive_backend(ArchiveBackend::Partitioned)
        .build()
        .await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let stream_id = Uuid::new_v4();
    store
        .events()
        .append_stream(
            stream_id,
            Expected::Any,
            vec![Event::new("HotEvent", &json!({"i": 1}))],
        )
        .await?;

    let initial_retention: Option<String> =
        sqlx::query_scalar("select retention_class from events where stream_id = $1")
            .bind(stream_id)
            .fetch_one(store.pool())
            .await
            .ok();
    assert_eq!(initial_retention.as_deref(), Some("hot"));

    store
        .events()
        .archive_stream_before(stream_id, Utc::now() + chrono::Duration::seconds(1))
        .await?;

    let hot_only = store.events().read_stream(stream_id).await?;
    assert!(hot_only.is_empty());

    let archived = store
        .events()
        .with_archived()
        .read_stream(stream_id)
        .await?;
    assert_eq!(archived.len(), 1);

    sqlx::query(
        "update rf_streams set archived_at = now(), retention_class = 'cold' where stream_id = $1",
    )
    .bind(stream_id)
    .execute(store.pool())
    .await?;

    let err = store
        .events()
        .append_stream(
            stream_id,
            Expected::Any,
            vec![Event::new("Blocked", &json!({"i": 2}))],
        )
        .await
        .expect_err("archived stream should block hot appends");
    assert!(matches!(err, Error::StreamArchived { .. }));

    let mut opts = AppendOptions::default();
    opts.allow_archived_stream = true;
    store
        .events()
        .append_with(
            stream_id,
            Expected::Any,
            vec![Event::new("ColdBackfill", &json!({"i": 3}))],
            &opts,
        )
        .await?;

    let classes: Vec<String> = sqlx::query_scalar(
        "select retention_class from events where stream_id = $1 order by stream_seq",
    )
    .bind(stream_id)
    .fetch_all(store.pool())
    .await?;
    assert_eq!(classes.last().map(String::as_str), Some("cold"));

    let partitions: Vec<String> = sqlx::query_scalar(
        "select child.relname\n         from pg_inherits\n         join pg_class parent on parent.oid = inhparent\n         join pg_class child on child.oid = inhrelid\n         join pg_namespace pn on pn.oid = parent.relnamespace\n         where pn.nspname = current_schema() and parent.relname = 'events'",
    )
    .fetch_all(store.pool())
    .await?;
    assert!(partitions.iter().any(|name| name.contains("events_hot")));
    assert!(partitions.iter().any(|name| name.contains("events_cold")));

    Ok(())
}
