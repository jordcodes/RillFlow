use rillflow::{
    Aggregate, AggregateRepository, SessionContext, Store, events::AppendOptions,
    store::TenantStrategy,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Customer {
    id: Uuid,
    email: String,
    tier: String,
}

#[derive(Clone, Debug, Default, Serialize)]
struct VisitCounter {
    total: i32,
}

impl Aggregate for VisitCounter {
    fn new() -> Self {
        Self { total: 0 }
    }

    fn apply(&mut self, env: &rillflow::EventEnvelope) {
        if env.typ == "CustomerVisited" {
            self.total += 1;
        }
    }

    fn version(&self) -> i32 {
        self.total
    }
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
        .session_context(
            SessionContext::builder()
                .tenant("default")
                .headers(json!({"environment": "dev"})),
        )
        .tenant_strategy(TenantStrategy::SchemaPerTenant)
        .session_advisory_locks(true)
        .build()
        .await?;

    store.ensure_tenant("default").await?;
    store.ensure_tenant("acme").await?;

    rillflow::testing::migrate_core_schema(store.pool()).await?;

    let customer_id = Uuid::new_v4();

    let mut session = store.session();
    let customer = Customer {
        id: customer_id,
        email: "alice@example.com".into(),
        tier: "starter".into(),
    };
    session.store(customer_id, &customer)?;
    session.enqueue_event(
        customer_id,
        rillflow::Expected::Any,
        rillflow::Event::new("CustomerRegistered", &customer),
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
    session
        .context_mut()
        .merge_headers(json!({"upgraded": true}));
    session.context_mut().correlation_id = Some(Uuid::new_v4());
    session.enqueue_event(
        customer_id,
        rillflow::Expected::Any,
        rillflow::Event::new("CustomerTierChanged", &json!({"tier": "pro"})),
    )?;
    session.save_changes().await?;

    let mut acme_session = store.session();
    acme_session.context_mut().tenant = Some("acme".into());
    acme_session.store(customer_id, &customer)?;
    acme_session.save_changes().await?;

    let mut session = store.session();
    session.context_mut().tenant = Some("default".into());
    let repo = AggregateRepository::new(store.events());
    let mut aggregates = session.aggregates(&repo);
    let visits_stream = customer_id;

    aggregates.commit(
        visits_stream,
        rillflow::Expected::Any,
        vec![rillflow::Event::new("CustomerVisited", &json!({}))],
    )?;
    aggregates.commit_and_snapshot(
        visits_stream,
        &VisitCounter::default(),
        vec![rillflow::Event::new("CustomerVisited", &json!({}))],
        2,
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
