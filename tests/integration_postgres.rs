use anyhow::Result;
use rillflow::{
    Event, Expected, Store,
    projections::ProjectionHandler,
    query::{CompiledQuery, DocumentQueryContext, Predicate, SortDirection},
};
use serde_json::Value;
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
    session.append_events(
        id,
        Expected::Any,
        vec![Event::new("CustomerRegistered", &customer)],
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
