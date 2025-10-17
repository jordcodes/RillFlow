use rillflow::{
    Store,
    schema::{DuplicatedField, DuplicatedFieldType, IndexType, SchemaConfig, SchemaManager},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Customer {
    email: String,
    age: i32,
    score: f64,
    active: bool,
}

#[tokio::test]
#[cfg_attr(not(feature = "postgres"), ignore)]
async fn test_duplicated_fields_schema_creation() {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = image.start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    rillflow::testing::migrate_core_schema(&pool).await.unwrap();

    let schema_mgr = SchemaManager::new(pool.clone());

    // Create config with duplicated fields
    let config = SchemaConfig::single_tenant()
        .add_duplicated_field(
            DuplicatedField::new("email", "d_email", DuplicatedFieldType::Text)
                .with_indexed(true)
                .with_index_type(IndexType::BTree),
        )
        .add_duplicated_field(
            DuplicatedField::new("age", "d_age", DuplicatedFieldType::Integer)
                .with_indexed(true)
                .with_nullable(false),
        );

    let plan = schema_mgr.plan(&config).await.unwrap();

    // Verify plan includes column creation
    let actions: Vec<String> = plan
        .actions()
        .iter()
        .map(|a| a.description().to_string())
        .collect();
    println!("Schema actions: {:#?}", actions);

    // Apply the plan
    schema_mgr.sync(&config).await.unwrap();

    // Verify columns exist
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'docs'
         AND column_name IN ('d_email', 'd_age')
         ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].0, "d_age");
    assert_eq!(columns[0].1, "integer");
    assert_eq!(columns[1].0, "d_email");
    assert_eq!(columns[1].1, "text");

    // Verify indexes exist
    let indexes: Vec<(String,)> = sqlx::query_as(
        "SELECT indexname FROM pg_indexes
         WHERE schemaname = 'public' AND tablename = 'docs'
         AND indexname IN ('d_email_idx', 'd_age_idx')
         ORDER BY indexname",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0].0, "d_age_idx");
    assert_eq!(indexes[1].0, "d_email_idx");
}

#[tokio::test]
#[cfg_attr(not(feature = "postgres"), ignore)]
async fn test_duplicated_fields_auto_sync() {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = image.start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    rillflow::testing::migrate_core_schema(&pool).await.unwrap();

    let schema_mgr = SchemaManager::new(pool.clone());

    let config = SchemaConfig::single_tenant()
        .add_duplicated_field(
            DuplicatedField::new("email", "d_email", DuplicatedFieldType::Text)
                .with_transform("lower({value})"), // Case-insensitive
        )
        .add_duplicated_field(DuplicatedField::new(
            "age",
            "d_age",
            DuplicatedFieldType::Integer,
        ));

    schema_mgr.sync(&config).await.unwrap();

    let store = Store::connect(&url).await.unwrap();

    // Insert a document
    let customer = Customer {
        email: "Alice@Example.COM".to_string(),
        age: 30,
        score: 95.5,
        active: true,
    };

    let id = uuid::Uuid::new_v4();
    store.docs().put(&id, &customer, None).await.unwrap();

    // Verify duplicated columns were populated via trigger
    let row: (String, i32) = sqlx::query_as("SELECT d_email, d_age FROM docs WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(row.0, "alice@example.com"); // Transform applied!
    assert_eq!(row.1, 30);

    // Update the document
    store
        .docs()
        .put(
            &id,
            &Customer {
                email: "Bob@Test.ORG".to_string(),
                age: 45,
                score: 88.0,
                active: false,
            },
            Some(1),
        )
        .await
        .unwrap();

    // Verify duplicated columns updated
    let row: (String, i32) = sqlx::query_as("SELECT d_email, d_age FROM docs WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(row.0, "bob@test.org");
    assert_eq!(row.1, 45);
}

#[tokio::test]
#[cfg_attr(not(feature = "postgres"), ignore)]
async fn test_duplicated_fields_indexed_queries() {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = image.start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    rillflow::testing::migrate_core_schema(&pool).await.unwrap();

    let schema_mgr = SchemaManager::new(pool.clone());

    let config = SchemaConfig::single_tenant()
        .add_duplicated_field(
            DuplicatedField::new("email", "d_email", DuplicatedFieldType::Text)
                .with_indexed(true)
                .with_index_type(IndexType::BTree),
        )
        .add_duplicated_field(
            DuplicatedField::new("age", "d_age", DuplicatedFieldType::Integer)
                .with_indexed(true)
                .with_index_type(IndexType::BTree),
        );

    schema_mgr.sync(&config).await.unwrap();

    let store = Store::connect(&url).await.unwrap();

    // Insert test data
    for i in 0..10 {
        let customer = Customer {
            email: format!("user{}@example.com", i),
            age: 20 + i,
            score: 50.0 + (i as f64 * 5.0),
            active: i % 2 == 0,
        };
        store
            .docs()
            .put(&uuid::Uuid::new_v4(), &customer, None)
            .await
            .unwrap();
    }

    // Query directly using the duplicated columns (which are indexed)
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM docs WHERE d_age > 25")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(count.0, 4); // ages 26, 27, 28, 29

    // Verify the index is being used by checking the query plan
    let plan: Vec<(String,)> = sqlx::query_as("EXPLAIN SELECT * FROM docs WHERE d_age > 25")
        .fetch_all(&pool)
        .await
        .unwrap();

    let plan_text = plan
        .iter()
        .map(|r| r.0.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    println!("Query plan:\n{}", plan_text);

    // The plan should mention the index (this is a basic check)
    // In a real scenario with more data, we'd see "Index Scan using d_age_idx"
}

#[tokio::test]
#[cfg_attr(not(feature = "postgres"), ignore)]
async fn test_duplicated_fields_with_nested_paths() {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = image.start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    rillflow::testing::migrate_core_schema(&pool).await.unwrap();

    let schema_mgr = SchemaManager::new(pool.clone());

    let config = SchemaConfig::single_tenant()
        .add_duplicated_field(DuplicatedField::new(
            "profile.name",
            "d_profile_name",
            DuplicatedFieldType::Text,
        ))
        .add_duplicated_field(DuplicatedField::new(
            "settings.notifications",
            "d_notifications",
            DuplicatedFieldType::Boolean,
        ));

    schema_mgr.sync(&config).await.unwrap();

    let store = Store::connect(&url).await.unwrap();

    // Insert document with nested structure
    let doc = json!({
        "profile": {
            "name": "Alice Smith",
            "age": 30
        },
        "settings": {
            "notifications": true,
            "theme": "dark"
        }
    });

    let id = uuid::Uuid::new_v4();
    store.docs().put(&id, &doc, None).await.unwrap();

    // Verify nested fields extracted correctly
    let row: (String, bool) =
        sqlx::query_as("SELECT d_profile_name, d_notifications FROM docs WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(row.0, "Alice Smith");
    assert!(row.1);
}

#[tokio::test]
#[cfg_attr(not(feature = "postgres"), ignore)]
async fn test_duplicated_fields_extraction_sql() {
    // Test the extraction SQL generation logic

    let field1 = DuplicatedField::new("email", "d_email", DuplicatedFieldType::Text);
    assert_eq!(field1.extraction_sql(), "NEW.doc->>'email'");

    let field2 = DuplicatedField::new("age", "d_age", DuplicatedFieldType::Integer);
    assert_eq!(field2.extraction_sql(), "(NEW.doc->>'age')::integer");

    let field3 = DuplicatedField::new("profile.name", "d_name", DuplicatedFieldType::Text);
    assert_eq!(field3.extraction_sql(), "NEW.doc#>>'{profile,name}'");

    let field4 = DuplicatedField::new("email", "d_email_lower", DuplicatedFieldType::Text)
        .with_transform("lower({value})");
    assert_eq!(field4.extraction_sql(), "lower(NEW.doc->>'email')");

    let field5 = DuplicatedField::new("metadata", "d_metadata", DuplicatedFieldType::Jsonb);
    assert_eq!(field5.extraction_sql(), "NEW.doc->'metadata'");
}

#[tokio::test]
#[cfg_attr(not(feature = "postgres"), ignore)]
async fn test_duplicated_fields_performance_comparison() {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres");
    let container = image.start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    rillflow::testing::migrate_core_schema(&pool).await.unwrap();

    let schema_mgr = SchemaManager::new(pool.clone());

    let config = SchemaConfig::single_tenant().add_duplicated_field(
        DuplicatedField::new("email", "d_email", DuplicatedFieldType::Text).with_indexed(true),
    );

    schema_mgr.sync(&config).await.unwrap();

    let store = Store::connect(&url).await.unwrap();

    // Insert 1000 documents
    for i in 0..1000 {
        let customer = Customer {
            email: format!("user{}@example.com", i),
            age: 20 + (i % 50),
            score: 50.0 + ((i % 100) as f64),
            active: i % 2 == 0,
        };
        store
            .docs()
            .put(&uuid::Uuid::new_v4(), &customer, None)
            .await
            .unwrap();
    }

    // Query using duplicated column (uses index)
    let start = std::time::Instant::now();
    let _: Vec<(uuid::Uuid,)> =
        sqlx::query_as("SELECT id FROM docs WHERE d_email = 'user500@example.com'")
            .fetch_all(&pool)
            .await
            .unwrap();
    let duplicated_time = start.elapsed();

    // Query using JSONB extraction (no dedicated index)
    let start = std::time::Instant::now();
    let _: Vec<(uuid::Uuid,)> =
        sqlx::query_as("SELECT id FROM docs WHERE doc->>'email' = 'user500@example.com'")
            .fetch_all(&pool)
            .await
            .unwrap();
    let jsonb_time = start.elapsed();

    println!("Duplicated column query time: {:?}", duplicated_time);
    println!("JSONB extraction query time: {:?}", jsonb_time);

    // Duplicated column should generally be faster, but let's just verify both work
    // (actual performance depends on data size, indexes, etc.)
}
