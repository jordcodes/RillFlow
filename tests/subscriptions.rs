use anyhow::Result;
use rillflow::subscriptions::{AckMode, SubscriptionFilter, SubscriptionOptions, Subscriptions};
use rillflow::{Event, Expected, Store};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{Duration, timeout};
use uuid::Uuid;
#[tokio::test]
async fn manual_ack_updates_checkpoint() -> Result<()> {
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

    let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
    let filter = SubscriptionFilter {
        event_types: Some(vec!["Ping".into()]),
        ..Default::default()
    };
    let mut opts = SubscriptionOptions {
        start_from: 0,
        ..Default::default()
    };
    opts.ack_mode = AckMode::Manual;
    let (handle, mut rx) = subs.subscribe("ack1", filter, opts).await?;

    let stream = Uuid::new_v4();
    store
        .events()
        .append_stream(stream, Expected::Any, vec![Event::new("Ping", &json!({}))])
        .await?;

    let env = timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("event");
    // checkpoint should not auto-advance in manual mode
    let last_seq: i64 =
        sqlx::query_scalar::<_, Option<i64>>("select last_seq from subscriptions where name=$1")
            .bind("ack1")
            .fetch_one(store.pool())
            .await?
            .unwrap_or(0);
    assert_eq!(last_seq, 0);

    // Manually ack using handle
    handle.ack(env.global_seq).await?;
    let last_seq2: i64 =
        sqlx::query_scalar::<_, Option<i64>>("select last_seq from subscriptions where name=$1")
            .bind("ack1")
            .fetch_one(store.pool())
            .await?
            .unwrap_or(0);
    assert_eq!(last_seq2, env.global_seq);

    Ok(())
}

#[tokio::test]
async fn conjoined_tenant_filtering_in_subscriptions() -> Result<()> {
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

    // Set up store with conjoined tenancy
    let store = Store::builder(&url)
        .tenant_strategy(rillflow::store::TenantStrategy::conjoined(
            rillflow::schema::TenantColumn::text("tenant_id"),
        ))
        .tenant_resolver(|| Some("tenant_a".to_string()))
        .build()
        .await?;

    // Sync schema with tenant column
    let schema_config = rillflow::SchemaConfig {
        base_schema: "public".into(),
        tenancy_mode: rillflow::schema::TenancyMode::Conjoined {
            column: rillflow::schema::TenantColumn::text("tenant_id"),
        },
    };
    store.schema().sync(&schema_config).await?;

    // Append events for tenant_a
    let stream_a = Uuid::new_v4();
    store
        .events()
        .with_tenant("tenant_a")
        .append_stream(
            stream_a,
            Expected::Any,
            vec![
                Event::new("TenantA_Event1", &json!({"msg": "a1"})),
                Event::new("TenantA_Event2", &json!({"msg": "a2"})),
            ],
        )
        .await?;

    // Append events for tenant_b
    let stream_b = Uuid::new_v4();
    store
        .events()
        .with_tenant("tenant_b")
        .append_stream(
            stream_b,
            Expected::Any,
            vec![
                Event::new("TenantB_Event1", &json!({"msg": "b1"})),
                Event::new("TenantB_Event2", &json!({"msg": "b2"})),
            ],
        )
        .await?;

    // Create subscription for tenant_a
    let subs_a = store.subscriptions().with_tenant("tenant_a");
    let filter = SubscriptionFilter::default();
    let opts = SubscriptionOptions {
        start_from: 0,
        poll_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let (_handle, mut rx_a) = subs_a
        .subscribe("sub_tenant_a", filter.clone(), opts.clone())
        .await?;

    // Create subscription for tenant_b
    let subs_b = store.subscriptions().with_tenant("tenant_b");
    let (_handle, mut rx_b) = subs_b.subscribe("sub_tenant_b", filter, opts).await?;

    // Tenant A should only receive tenant A events
    let env1 = timeout(Duration::from_secs(5), rx_a.recv())
        .await?
        .expect("tenant_a event 1");
    assert_eq!(env1.typ, "TenantA_Event1");
    assert_eq!(env1.tenant_id, Some("tenant_a".to_string()));

    let env2 = timeout(Duration::from_secs(5), rx_a.recv())
        .await?
        .expect("tenant_a event 2");
    assert_eq!(env2.typ, "TenantA_Event2");
    assert_eq!(env2.tenant_id, Some("tenant_a".to_string()));

    // Tenant B should only receive tenant B events
    let env3 = timeout(Duration::from_secs(5), rx_b.recv())
        .await?
        .expect("tenant_b event 1");
    assert_eq!(env3.typ, "TenantB_Event1");
    assert_eq!(env3.tenant_id, Some("tenant_b".to_string()));

    let env4 = timeout(Duration::from_secs(5), rx_b.recv())
        .await?
        .expect("tenant_b event 2");
    assert_eq!(env4.typ, "TenantB_Event2");
    assert_eq!(env4.tenant_id, Some("tenant_b".to_string()));

    // Verify no cross-tenant leakage (should timeout)
    let no_more_a = timeout(Duration::from_millis(500), rx_a.recv()).await;
    assert!(
        no_more_a.is_err(),
        "tenant_a should not receive tenant_b events"
    );

    let no_more_b = timeout(Duration::from_millis(500), rx_b.recv()).await;
    assert!(
        no_more_b.is_err(),
        "tenant_b should not receive tenant_a events"
    );

    Ok(())
}

#[tokio::test]
async fn subscribe_and_receive_events() -> Result<()> {
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

    // create subscription
    let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
    let filter = SubscriptionFilter {
        event_types: Some(vec!["Ping".to_string()]),
        stream_ids: None,
        stream_prefix: None,
    };
    let opts = SubscriptionOptions {
        batch_size: 100,
        poll_interval: Duration::from_millis(100),
        start_from: 0,
        channel_capacity: 16,
        notify_channel: None,
        group: None,
        lease_ttl: Duration::from_secs(30),
        ack_mode: rillflow::subscriptions::AckMode::Auto,
        max_in_flight: 1024,
    };
    let (_handle, mut rx) = subs.subscribe("s1", filter, opts).await?;

    // append events
    let stream = Uuid::new_v4();
    store
        .events()
        .append_stream(stream, Expected::Any, vec![Event::new("Ping", &json!({}))])
        .await?;

    // expect delivery
    let env = timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("expected event");
    assert_eq!(env.typ, "Ping");
    assert_eq!(env.stream_id, stream);

    Ok(())
}

#[tokio::test]
async fn pause_and_resume_subscription() -> Result<()> {
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

    let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
    let filter = SubscriptionFilter {
        event_types: Some(vec!["Ping".into()]),
        ..Default::default()
    };
    let opts = SubscriptionOptions {
        start_from: 0,
        notify_channel: None,
        ..Default::default()
    };
    let (handle, mut rx) = subs.subscribe("s2", filter, opts).await?;

    // pause should stop delivery
    handle.pause().await?;
    let stream = Uuid::new_v4();
    store
        .events()
        .append_stream(stream, Expected::Any, vec![Event::new("Ping", &json!({}))])
        .await?;
    let r = timeout(Duration::from_millis(500), rx.recv()).await; // expect timeout
    assert!(r.is_err() || r.unwrap().is_none());

    // resume and expect now
    handle.resume().await?;
    let env = timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("event after resume");
    assert_eq!(env.stream_id, stream);
    Ok(())
}

#[tokio::test]
async fn filters_combo_event_type_and_stream_id() -> Result<()> {
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

    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    // events across two streams and types
    store
        .events()
        .append_stream(
            s1,
            Expected::Any,
            vec![
                Event::new("Ping", &json!({})),
                Event::new("Pong", &json!({})),
            ],
        )
        .await?;
    store
        .events()
        .append_stream(s2, Expected::Any, vec![Event::new("Ping", &json!({}))])
        .await?;

    let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
    let filter = SubscriptionFilter {
        event_types: Some(vec!["Ping".into()]),
        stream_ids: Some(vec![s1]),
        stream_prefix: None,
    };
    let opts = SubscriptionOptions {
        start_from: 0,
        poll_interval: Duration::from_millis(50),
        notify_channel: None,
        ..Default::default()
    };
    let (_handle, mut rx) = subs.subscribe("s3", filter, opts).await?;

    // Should only receive Ping from s1, not Pong from s1 or Ping from s2
    let first = timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("expected event");
    assert_eq!(first.typ, "Ping");
    assert_eq!(first.stream_id, s1);

    // No more events should match
    let second = timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(second.is_err() || second.unwrap().is_none());

    Ok(())
}

#[tokio::test]
async fn consumer_group_leasing_only_one_receiver() -> Result<()> {
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

    let subs = Subscriptions::new_with_schema(store.pool().clone(), "public");
    let filter = SubscriptionFilter {
        event_types: Some(vec!["Ping".into()]),
        ..Default::default()
    };
    let mut opts = SubscriptionOptions {
        start_from: 0,
        notify_channel: None,
        ..Default::default()
    };
    opts.poll_interval = Duration::from_millis(50);
    opts.group = Some("g1".to_string());

    let (_h1, mut rx1) = subs.subscribe("sg1", filter.clone(), opts.clone()).await?;
    let (_h2, mut rx2) = subs.subscribe("sg1", filter.clone(), opts.clone()).await?;

    // give both consumers a moment to initialize
    tokio::time::sleep(Duration::from_millis(150)).await;

    let stream = Uuid::new_v4();
    store
        .events()
        .append_stream(stream, Expected::Any, vec![Event::new("Ping", &json!({}))])
        .await?;

    // Exactly one of the receivers should get the event; the other should time out
    let r1 = timeout(Duration::from_secs(5), rx1.recv()).await;
    let r2 = timeout(Duration::from_secs(5), rx2.recv()).await;
    let got1 = r1.ok().flatten().is_some();
    let got2 = r2.ok().flatten().is_some();
    assert!(
        got1 ^ got2,
        "exactly one consumer in the group should receive the event"
    );

    Ok(())
}
