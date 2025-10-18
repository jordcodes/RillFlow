use anyhow::Result;
use rillflow::events::AppendOptions;
use rillflow::{Aggregate, AggregateRepository, Error, Event, Expected, Store};
use serde::{Deserialize, Serialize};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Customer {
    email: String,
    tier: String,
}

#[tokio::test]
async fn put_get_update_soft_delete_restore() -> Result<()> {
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
        .session_defaults(AppendOptions {
            headers: Some(json!({"source": "test"})),
            ..AppendOptions::default()
        })
        .session_advisory_locks(true)
        .build()
        .await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let id = Uuid::new_v4();
    let v1 = store
        .docs()
        .put(
            &id,
            &Customer {
                email: "a@x".into(),
                tier: "free".into(),
            },
            None,
        )
        .await?;
    assert_eq!(v1, 1);

    let got = store.docs().get::<Customer>(&id).await?.unwrap();
    assert_eq!(got.0.email, "a@x");
    assert_eq!(got.1, 1);

    let v2 = store
        .docs()
        .update::<Customer, _>(&id, 1, |c| c.tier = "pro".into())
        .await?;
    assert_eq!(v2, 2);
    let got2 = store.docs().get::<Customer>(&id).await?.unwrap();
    assert_eq!(got2.0.tier, "pro");
    assert_eq!(got2.1, 2);

    // version conflict
    let err = store
        .docs()
        .put(
            &id,
            &Customer {
                email: "a@x".into(),
                tier: "plus".into(),
            },
            Some(1),
        )
        .await
        .expect_err("expect conflict");
    match err {
        Error::DocVersionConflict => {}
        other => panic!("wrong error: {other:?}"),
    }

    // soft delete
    sqlx::query("update docs set deleted_at = now() where id = $1")
        .bind(id)
        .execute(store.pool())
        .await?;
    assert!(store.docs().get::<Customer>(&id).await?.is_none());

    // restore
    sqlx::query("update docs set deleted_at = null where id = $1")
        .bind(id)
        .execute(store.pool())
        .await?;
    let got3 = store.docs().get::<Customer>(&id).await?.unwrap();
    assert_eq!(got3.0.email, "a@x");

    Ok(())
}

#[tokio::test]
async fn session_load_store_delete_roundtrip() -> Result<()> {
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
        .session_defaults(AppendOptions {
            headers: Some(json!({"source": "test"})),
            ..AppendOptions::default()
        })
        .session_advisory_locks(true)
        .build()
        .await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Profile {
        email: String,
        tier: String,
    }

    let id = Uuid::new_v4();
    let mut session = store.session();

    // create new doc via session store + save
    session.store(
        id,
        &Profile {
            email: "beta@example.com".into(),
            tier: "starter".into(),
        },
    )?;
    session.save_changes().await?;

    // identity map serves cached copy without hitting DB
    let loaded = session.load::<Profile>(&id).await?.unwrap();
    assert_eq!(loaded.tier, "starter");

    // update tracked doc (uses version 1 underneath)
    session.store(
        id,
        &Profile {
            email: "beta@example.com".into(),
            tier: "plus".into(),
        },
    )?;
    session.set_event_idempotency_key("req-123");
    session.enqueue_event(
        id,
        Expected::Any,
        Event::new("CustomerUpgraded", &json!({"tier": "plus"})),
    )?;
    session.save_changes().await?;

    // second session observes persisted version 2
    let mut session2 = store.session();
    let fresh = session2.load::<Profile>(&id).await?.unwrap();
    assert_eq!(fresh.tier, "plus");

    // first session bumps version to 3
    session.store(
        id,
        &Profile {
            email: "beta@example.com".into(),
            tier: "platinum".into(),
        },
    )?;
    session.set_event_idempotency_key("req-124");
    session.enqueue_event(
        id,
        Expected::Exact(1),
        Event::new("CustomerTierChanged", &json!({"tier": "platinum"})),
    )?;
    session.save_changes().await?;

    // stale session attempts update -> version conflict
    session2.store(
        id,
        &Profile {
            email: "beta@example.com".into(),
            tier: "gold".into(),
        },
    )?;
    session2.merge_event_headers(json!({"source": "test"}));
    session2.set_event_idempotency_key("req-125");
    session2.enqueue_event(
        id,
        Expected::Exact(2),
        Event::new("CustomerTierChanged", &json!({"tier": "gold"})),
    )?;
    let err = session2
        .save_changes()
        .await
        .expect_err("stale session must error");
    assert!(matches!(err, Error::DocVersionConflict));

    // delete should remove from DB and the session cache
    session.delete(id);
    session.save_changes().await?;

    assert!(session.load::<Profile>(&id).await?.is_none());
    let persisted = store.docs().get::<Profile>(&id).await?;
    assert!(persisted.is_none());

    let envelopes = store.events().read_stream_envelopes(id).await?;
    assert_eq!(envelopes.len(), 2);
    assert_eq!(envelopes[0].typ, "CustomerUpgraded");
    assert_eq!(envelopes[0].headers["idempotency_key"], "req-123");
    assert_eq!(envelopes[0].headers["source"], "test");
    assert_eq!(envelopes[1].typ, "CustomerTierChanged");
    assert_eq!(envelopes[1].headers["idempotency_key"], "req-124");
    assert_eq!(envelopes[1].headers["source"], "test");

    Ok(())
}

#[derive(Default, Clone, serde::Serialize)]
struct CounterAggregate {
    n: i32,
}

impl Aggregate for CounterAggregate {
    fn new() -> Self {
        Self { n: 0 }
    }

    fn apply(&mut self, env: &rillflow::EventEnvelope) {
        if env.typ == "Increment" {
            self.n += 1;
        }
    }

    fn version(&self) -> i32 {
        self.n
    }
}

#[tokio::test]
async fn session_aggregate_commit_and_snapshot() -> Result<()> {
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
        .session_defaults(AppendOptions {
            headers: Some(json!({"source": "test"})),
            ..AppendOptions::default()
        })
        .session_advisory_locks(true)
        .build()
        .await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let repo = AggregateRepository::new(store.events());

    let stream_id = Uuid::new_v4();
    let mut session = store.session();
    let mut aggregates = session.aggregates(&repo);

    aggregates.commit(
        stream_id,
        Expected::NoStream,
        vec![Event::new("Increment", &json!({}))],
    )?;
    session.save_changes().await?;

    let envelopes = store.events().read_stream_envelopes(stream_id).await?;
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].typ, "Increment");

    let mut session = store.session();
    let mut aggregates = session.aggregates(&repo);
    let agg_state: CounterAggregate = repo.load(stream_id).await?;
    assert_eq!(agg_state.n, 1);

    aggregates.commit_for(
        stream_id,
        &agg_state,
        vec![Event::new("Increment", &json!({}))],
    )?;
    session.save_changes().await?;

    let envelopes = store.events().read_stream_envelopes(stream_id).await?;
    assert_eq!(envelopes.len(), 2);

    let mut session = store.session();
    let mut aggregates = session.aggregates(&repo);
    let agg_state: CounterAggregate = repo.load(stream_id).await?;
    aggregates.commit_and_snapshot(
        stream_id,
        &agg_state,
        vec![Event::new("Increment", &json!({}))],
        3,
    )?;
    session.save_changes().await?;

    let snapshot: Option<(i32, serde_json::Value)> =
        sqlx::query_as("select version, body from snapshots where stream_id = $1")
            .bind(stream_id)
            .fetch_optional(store.pool())
            .await?;
    let (version, body) = snapshot.expect("snapshot row");
    assert_eq!(version, 3);
    assert_eq!(body["n"], 3);

    Ok(())
}

#[tokio::test]
async fn session_multi_tenant_isolation() -> Result<()> {
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
        .tenant_strategy(rillflow::store::TenantStrategy::SchemaPerTenant)
        .tenant_resolver(|| Some("acme".into()))
        .build()
        .await?;

    store.ensure_tenant("acme").await?;
    store.ensure_tenant("globex").await?;

    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let customer_id = Uuid::new_v4();

    let mut session_acme = store.session();
    session_acme.context_mut().tenant = Some("acme".into());
    session_acme.store(
        customer_id,
        &Customer {
            email: "acme@example.com".into(),
            tier: "starter".into(),
        },
    )?;
    session_acme.save_changes().await?;

    let mut session_globex = store
        .session_builder()
        .tenant_resolver(|| Some("globex".to_string()))
        .build();
    let acme_doc = session_globex.load::<Customer>(&customer_id).await?;
    assert!(acme_doc.is_none(), "globex session must not see acme data");

    session_globex.store(
        customer_id,
        &Customer {
            email: "globex@example.com".into(),
            tier: "pro".into(),
        },
    )?;
    session_globex.save_changes().await?;

    let mut acme_check = store.session();
    acme_check.context_mut().tenant = Some("acme".into());
    let acme_doc = acme_check.load::<Customer>(&customer_id).await?.unwrap();
    assert_eq!(acme_doc.email, "acme@example.com");

    let mut globex_check = store.session();
    globex_check.context_mut().tenant = Some("globex".into());
    let globex_doc = globex_check.load::<Customer>(&customer_id).await?.unwrap();
    assert_eq!(globex_doc.email, "globex@example.com");

    Ok(())
}

#[tokio::test]
async fn session_requires_tenant_when_schema_per_tenant() -> Result<()> {
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

    let result = std::panic::catch_unwind(|| {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let _ = Store::builder(&url)
                .tenant_strategy(rillflow::store::TenantStrategy::SchemaPerTenant)
                .build()
                .await
                .unwrap();
        });
    });

    assert!(
        result.is_err(),
        "store build should panic without tenant resolver"
    );
    Ok(())
}

#[tokio::test]
async fn session_allow_missing_tenant_ok() -> Result<()> {
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
        .tenant_strategy(rillflow::store::TenantStrategy::SchemaPerTenant)
        .allow_missing_tenant()
        .build()
        .await?;

    store.ensure_tenant("acme").await?;

    let mut session = store
        .session_builder()
        .tenant_resolver(|| Some("acme".to_string()))
        .allow_missing_tenant()
        .build();
    session.store(
        Uuid::new_v4(),
        &Customer {
            email: "missing@example.com".into(),
            tier: "trial".into(),
        },
    )?;
    session.save_changes().await?;

    Ok(())
}

#[tokio::test]
async fn session_search_path_is_local() -> Result<()> {
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
        .tenant_strategy(rillflow::store::TenantStrategy::SchemaPerTenant)
        .allow_missing_tenant()
        .tenant_resolver(|| Some("acme".to_string()))
        .build()
        .await?;

    store.ensure_tenant("acme").await?;

    {
        let mut session = store.session();
        session.context_mut().tenant = Some("acme".into());
        session.save_changes().await?; // no-op flush
    }

    let search_path: String = sqlx::query_scalar("select current_setting('search_path')")
        .fetch_one(store.pool())
        .await?;

    assert!(
        search_path.contains("public"),
        "search_path should include public after session"
    );
    assert!(
        !search_path.contains("tenant_acme"),
        "tenant schema should not leak after session"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_ensure_tenant_concurrency() -> Result<()> {
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
        .tenant_strategy(rillflow::store::TenantStrategy::SchemaPerTenant)
        .allow_missing_tenant()
        .build()
        .await?;

    let tenant = "acme_concurrent";
    let iterations = 16;
    let mut handles = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let store = store.clone();
        let tenant = tenant.to_string();
        handles.push(tokio::spawn(
            async move { store.ensure_tenant(&tenant).await },
        ));
    }

    for handle in handles {
        handle.await.expect("task join").expect("ensure tenant");
    }

    let schema_name = format!(
        "tenant_{}",
        tenant.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_', "_")
    );
    let schemas: i64 = sqlx::query_scalar(
        "select count(*) from information_schema.schemata where schema_name = $1",
    )
    .bind(&schema_name)
    .fetch_one(store.pool())
    .await?;
    assert_eq!(schemas, 1, "tenant schema must exist once");

    teardown_tenant(&store, tenant).await?;
    Ok(())
}

async fn teardown_tenant(store: &Store, tenant: &str) -> Result<()> {
    store.drop_tenant(tenant).await?;
    Ok(())
}
