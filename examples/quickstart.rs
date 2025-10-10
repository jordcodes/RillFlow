use rillflow::{Event, Expected, Store};
use uuid::Uuid;

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into());

    let store = Store::connect(&url).await?;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Customer {
        id: Uuid,
        email: String,
    }

    let customer = Customer {
        id: Uuid::new_v4(),
        email: "alice@example.com".into(),
    };

    store.docs().upsert(&customer.id, &customer).await?;
    let fetched = store.docs().get::<Customer>(&customer.id).await?;
    println!("Fetched: {fetched:?}");

    store
        .events()
        .append_stream(
            customer.id,
            Expected::Any,
            vec![Event::new("CustomerRegistered", &customer)],
        )
        .await?;

    let events = store.events().read_stream(customer.id).await?;
    println!("Events: {events:#?}");

    Ok(())
}
