use anyhow::Result;
use rillflow::{Error, Event, Expected, Store};
use serde::{Deserialize, Serialize};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
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

    let store = Store::connect(&url).await?;
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

    let store = Store::connect(&url).await?;
    rillflow::testing::migrate_core_schema(store.pool()).await?;

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Profile {
        email: String,
        tier: String,
    }

    let id = Uuid::new_v4();
    let mut session = store
        .session_builder()
        .merge_headers(json!({"source": "test"}))
        .advisory_locks(true)
        .build();

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
    session.append_events(
        id,
        Expected::Any,
        vec![Event::new("CustomerUpgraded", &json!({"tier": "plus"}))],
    )?;
    session.save_changes().await?;

    // second session observes persisted version 2
    let mut session2 = store.document_session();
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
    session.append_events(
        id,
        Expected::Exact(1),
        vec![Event::new(
            "CustomerTierChanged",
            &json!({"tier": "platinum"}),
        )],
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
    session2.append_events(
        id,
        Expected::Exact(2),
        vec![Event::new("CustomerTierChanged", &json!({"tier": "gold"}))],
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

    Ok(())
}
