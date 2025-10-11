use rillflow::{Store, events::AppendOptions};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Customer {
    id: Uuid,
    email: String,
    tier: String,
}

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into());

    let store = Store::builder(&url)
        .session_defaults(AppendOptions {
            headers: Some(json!({"source": "session-example"})),
            causation_id: None,
            correlation_id: None,
        })
        .session_advisory_locks(true)
        .build()
        .await?;

    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let customer_id = Uuid::new_v4();

    let mut session = store.session();
    let customer = Customer {
        id: customer_id,
        email: "alice@example.com".into(),
        tier: "starter".into(),
    };
    session.store(customer_id, &customer)?;
    session.append_events(
        customer_id,
        rillflow::Expected::Any,
        vec![rillflow::Event::new("CustomerRegistered", &customer)],
    )?;
    session.save_changes().await?;

    let upgraded = Customer {
        id: customer_id,
        email: "alice@example.com".into(),
        tier: "pro".into(),
    };
    let mut session = store.session();
    session.store(customer_id, &upgraded)?;
    session.set_event_idempotency_key("req-upgrade-1");
    session.append_events(
        customer_id,
        rillflow::Expected::Any,
        vec![rillflow::Event::new(
            "CustomerTierChanged",
            &json!({"tier": "pro"}),
        )],
    )?;
    session.save_changes().await?;

    let body = store
        .events()
        .read_stream_envelopes(customer_id)
        .await?
        .into_iter()
        .map(|env| (env.stream_seq, env.typ, env.headers))
        .collect::<Vec<_>>();

    println!("events: {:?}", body);

    Ok(())
}
