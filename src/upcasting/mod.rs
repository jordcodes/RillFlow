use crate::events::EventEnvelope;
use crate::{Error, Result};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type EventKey = (String, i32);

/// Trait for transforming events from one version/type to another.
pub trait Upcaster: Send + Sync {
    /// Source event type.
    fn from_type(&self) -> &str;
    /// Source event version. Use 0 to match all versions of a type.
    fn from_version(&self) -> i32;
    /// Target event type.
    fn to_type(&self) -> &str;
    /// Target event version.
    fn to_version(&self) -> i32;
    /// Transform the event body into the new representation.
    fn upcast(&self, body: &Value) -> Result<Value>;
}

/// Async variant for upcasters that require I/O (e.g., database lookups).
#[async_trait]
pub trait AsyncUpcaster: Send + Sync {
    fn from_type(&self) -> &str;
    fn from_version(&self) -> i32;
    fn to_type(&self) -> &str;
    fn to_version(&self) -> i32;

    async fn upcast(&self, body: &Value, pool: &PgPool) -> Result<Value>;
}

#[derive(Default)]
pub struct UpcasterRegistry {
    sync_upcasters: HashMap<EventKey, Arc<dyn Upcaster>>,
    async_upcasters: HashMap<EventKey, Arc<dyn AsyncUpcaster>>,
    transformation_graph: TransformationGraph,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpcasterKind {
    Sync,
    Async,
}

impl fmt::Display for UpcasterKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpcasterKind::Sync => write!(f, "sync"),
            UpcasterKind::Async => write!(f, "async"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpcasterDescriptor {
    pub from_type: String,
    pub from_version: i32,
    pub to_type: String,
    pub to_version: i32,
    pub kind: UpcasterKind,
}

impl UpcasterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a synchronous upcaster.
    pub fn register<U>(&mut self, upcaster: U)
    where
        U: Upcaster + 'static,
    {
        let from_type = upcaster.from_type().to_string();
        let from_version = upcaster.from_version();
        let to_type = upcaster.to_type().to_string();
        let to_version = upcaster.to_version();

        self.sync_upcasters
            .insert((from_type.clone(), from_version), Arc::new(upcaster));
        self.transformation_graph
            .add_edge((from_type, from_version), (to_type, to_version));
    }

    /// Register an asynchronous upcaster.
    pub fn register_async<U>(&mut self, upcaster: U)
    where
        U: AsyncUpcaster + 'static,
    {
        let from_type = upcaster.from_type().to_string();
        let from_version = upcaster.from_version();
        let to_type = upcaster.to_type().to_string();
        let to_version = upcaster.to_version();

        self.async_upcasters
            .insert((from_type.clone(), from_version), Arc::new(upcaster));
        self.transformation_graph
            .add_edge((from_type, from_version), (to_type, to_version));
    }

    /// Returns true when an async upcaster exists for the given type/version.
    pub fn has_async_upcaster(&self, event_type: &str, version: i32) -> bool {
        self.async_upcasters
            .contains_key(&(event_type.to_string(), version))
            || (version != 0
                && self
                    .async_upcasters
                    .contains_key(&(event_type.to_string(), 0)))
    }

    pub fn describe(&self) -> Vec<UpcasterDescriptor> {
        let mut entries: Vec<UpcasterDescriptor> = self
            .sync_upcasters
            .values()
            .map(|upcaster| UpcasterDescriptor {
                from_type: upcaster.from_type().to_string(),
                from_version: upcaster.from_version(),
                to_type: upcaster.to_type().to_string(),
                to_version: upcaster.to_version(),
                kind: UpcasterKind::Sync,
            })
            .collect();

        entries.extend(
            self.async_upcasters
                .values()
                .map(|upcaster| UpcasterDescriptor {
                    from_type: upcaster.from_type().to_string(),
                    from_version: upcaster.from_version(),
                    to_type: upcaster.to_type().to_string(),
                    to_version: upcaster.to_version(),
                    kind: UpcasterKind::Async,
                }),
        );

        entries.sort_by(|a, b| {
            a.from_type
                .cmp(&b.from_type)
                .then(a.from_version.cmp(&b.from_version))
                .then(a.kind.cmp(&b.kind))
                .then(a.to_version.cmp(&b.to_version))
        });

        entries
    }

    /// Attempt to find a transformation path between two versions/types.
    pub fn find_path(&self, from: (&str, i32), to: (&str, i32)) -> Option<Vec<(String, i32)>> {
        self.transformation_graph
            .find_path((from.0.to_string(), from.1), (to.0.to_string(), to.1))
    }

    /// Apply upcasting using sync upcasters only.
    pub fn upcast_sync(&self, envelope: &mut EventEnvelope) -> Result<()> {
        let mut visited = HashSet::new();

        loop {
            let key = (envelope.typ.clone(), envelope.event_version);
            if !visited.insert(key.clone()) {
                return Err(Error::UpcastingCycle {
                    event_type: key.0,
                    version: key.1,
                });
            }

            if let Some(upcaster) = self.sync_upcasters.get(&key).or_else(|| {
                if key.1 != 0 {
                    self.sync_upcasters.get(&(key.0.clone(), 0))
                } else {
                    None
                }
            }) {
                envelope.body = upcaster.upcast(&envelope.body)?;
                envelope.typ = upcaster.to_type().to_string();
                envelope.event_version = upcaster.to_version();
                continue;
            }

            if self.async_upcasters.contains_key(&key)
                || (key.1 != 0 && self.async_upcasters.contains_key(&(key.0.clone(), 0)))
            {
                return Err(Error::UpcastingPoolRequired {
                    event_type: key.0,
                    version: key.1,
                });
            }

            break;
        }

        Ok(())
    }

    /// Apply upcasting allowing async transformations when a pool is supplied.
    pub async fn upcast_with_pool(
        &self,
        envelope: &mut EventEnvelope,
        pool: Option<&PgPool>,
    ) -> Result<()> {
        self.apply_async(envelope, pool).await
    }

    async fn apply_async(&self, envelope: &mut EventEnvelope, pool: Option<&PgPool>) -> Result<()> {
        let mut visited = HashSet::new();
        loop {
            let key = (envelope.typ.clone(), envelope.event_version);
            if !visited.insert(key.clone()) {
                return Err(Error::UpcastingCycle {
                    event_type: key.0,
                    version: key.1,
                });
            }

            if let Some(upcaster) = self.sync_upcasters.get(&key) {
                envelope.body = upcaster.upcast(&envelope.body)?;
                envelope.typ = upcaster.to_type().to_string();
                envelope.event_version = upcaster.to_version();
                continue;
            }

            if let Some(upcaster) = self.async_upcasters.get(&key).or_else(|| {
                if key.1 != 0 {
                    self.async_upcasters.get(&(key.0.clone(), 0))
                } else {
                    None
                }
            }) {
                let pool = pool.ok_or_else(|| Error::UpcastingPoolRequired {
                    event_type: key.0,
                    version: key.1,
                })?;
                envelope.body = upcaster.upcast(&envelope.body, pool).await?;
                envelope.typ = upcaster.to_type().to_string();
                envelope.event_version = upcaster.to_version();
                continue;
            }

            break;
        }

        Ok(())
    }
}

/// Builder for constructing simple closure-based upcasters without defining a concrete type.
pub struct UpcasterBuilder {
    from_type: String,
    from_version: i32,
    to_type: String,
    to_version: i32,
    handler: Option<Box<dyn Fn(&Value) -> Result<Value> + Send + Sync>>,
}

impl UpcasterBuilder {
    pub fn new(from_type: impl Into<String>) -> Self {
        Self {
            from_type: from_type.into(),
            from_version: 0,
            to_type: String::new(),
            to_version: 0,
            handler: None,
        }
    }

    pub fn from_version(mut self, version: i32) -> Self {
        self.from_version = version;
        self
    }

    pub fn to(mut self, to_type: impl Into<String>, version: i32) -> Self {
        self.to_type = to_type.into();
        self.to_version = version;
        self
    }

    pub fn handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&Value) -> Result<Value> + Send + Sync + 'static,
    {
        self.handler = Some(Box::new(handler));
        self
    }

    pub fn build(self) -> Result<ClosureUpcaster> {
        let handler = self.handler.ok_or_else(|| Error::QueryError {
            query: "upcaster builder".into(),
            context: "missing handler".into(),
        })?;

        Ok(ClosureUpcaster {
            from_type: self.from_type,
            from_version: self.from_version,
            to_type: self.to_type,
            to_version: self.to_version,
            handler,
        })
    }
}

/// Builder for async upcasters backed by closures.
pub struct AsyncUpcasterBuilder {
    from_type: String,
    from_version: i32,
    to_type: String,
    to_version: i32,
    handler: Option<
        Box<
            dyn Fn(Value, &PgPool) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>>
                + Send
                + Sync,
        >,
    >,
}

impl AsyncUpcasterBuilder {
    pub fn new(from_type: impl Into<String>) -> Self {
        Self {
            from_type: from_type.into(),
            from_version: 0,
            to_type: String::new(),
            to_version: 0,
            handler: None,
        }
    }

    pub fn from_version(mut self, version: i32) -> Self {
        self.from_version = version;
        self
    }

    pub fn to(mut self, to_type: impl Into<String>, version: i32) -> Self {
        self.to_type = to_type.into();
        self.to_version = version;
        self
    }

    pub fn handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Value, &PgPool) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.handler = Some(Box::new(move |value: Value, pool| {
            Box::pin(handler(value, pool))
        }));
        self
    }

    pub fn build(self) -> Result<ClosureAsyncUpcaster> {
        let handler = self.handler.ok_or_else(|| Error::QueryError {
            query: "async upcaster builder".into(),
            context: "missing handler".into(),
        })?;

        Ok(ClosureAsyncUpcaster {
            from_type: self.from_type,
            from_version: self.from_version,
            to_type: self.to_type,
            to_version: self.to_version,
            handler,
        })
    }
}

pub struct ClosureUpcaster {
    from_type: String,
    from_version: i32,
    to_type: String,
    to_version: i32,
    handler: Box<dyn Fn(&Value) -> Result<Value> + Send + Sync>,
}

impl Upcaster for ClosureUpcaster {
    fn from_type(&self) -> &str {
        &self.from_type
    }

    fn from_version(&self) -> i32 {
        self.from_version
    }

    fn to_type(&self) -> &str {
        &self.to_type
    }

    fn to_version(&self) -> i32 {
        self.to_version
    }

    fn upcast(&self, body: &Value) -> Result<Value> {
        (self.handler)(body)
    }
}

pub struct ClosureAsyncUpcaster {
    from_type: String,
    from_version: i32,
    to_type: String,
    to_version: i32,
    handler: Box<
        dyn Fn(Value, &PgPool) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>> + Send + Sync,
    >,
}

/// Macro helper for constructing synchronous upcasters with minimal boilerplate.
#[macro_export]
macro_rules! sync_upcaster {
    (from $from_ty:expr, $from_ver:expr => $to_ty:expr, $to_ver:expr, |$val:ident| $body:block) => {{
        $crate::upcasting::UpcasterBuilder::new($from_ty)
            .from_version($from_ver)
            .to($to_ty, $to_ver)
            .handler(|value| {
                let mut $val = value.clone();
                $body
            })
            .build()
            .expect("invalid upcaster definition")
    }};
}

/// Macro helper for constructing async upcasters backed by an async block.
#[macro_export]
macro_rules! async_upcaster {
    (from $from_ty:expr, $from_ver:expr => $to_ty:expr, $to_ver:expr, |$val:ident, $pool:ident| $body:block) => {{
        $crate::upcasting::AsyncUpcasterBuilder::new($from_ty)
            .from_version($from_ver)
            .to($to_ty, $to_ver)
            .handler(|value, pool| {
                let pool = pool.clone();
                async move {
                    let mut $val = value;
                    let $pool = pool;
                    $body
                }
            })
            .build()
            .expect("invalid async upcaster definition")
    }};
}

#[async_trait]
impl AsyncUpcaster for ClosureAsyncUpcaster {
    fn from_type(&self) -> &str {
        &self.from_type
    }

    fn from_version(&self) -> i32 {
        self.from_version
    }

    fn to_type(&self) -> &str {
        &self.to_type
    }

    fn to_version(&self) -> i32 {
        self.to_version
    }

    async fn upcast(&self, body: &Value, pool: &PgPool) -> Result<Value> {
        (self.handler)(body.clone(), pool).await
    }
}

#[derive(Default)]
struct TransformationGraph {
    edges: HashMap<EventKey, Vec<EventKey>>,
}

impl TransformationGraph {
    fn add_edge(&mut self, from: EventKey, to: EventKey) {
        self.edges.entry(from).or_default().push(to);
    }

    fn find_path(&self, from: EventKey, to: EventKey) -> Option<Vec<EventKey>> {
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parents: HashMap<EventKey, EventKey> = HashMap::new();

        queue.push_back(from.clone());
        visited.insert(from.clone());

        while let Some(current) = queue.pop_front() {
            if current == to {
                let mut path = vec![current.clone()];
                let mut node = current;
                while let Some(parent) = parents.get(&node) {
                    path.push(parent.clone());
                    node = parent.clone();
                }
                path.reverse();
                return Some(path);
            }

            if let Some(neighbors) = self.edges.get(&current) {
                for neighbor in neighbors {
                    if visited.insert(neighbor.clone()) {
                        parents.insert(neighbor.clone(), current.clone());
                        queue.push_back(neighbor.clone());
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::{Value, json};

    #[derive(Clone)]
    struct V1ToV2;

    impl Upcaster for V1ToV2 {
        fn from_type(&self) -> &str {
            "OrderPlaced"
        }

        fn from_version(&self) -> i32 {
            1
        }

        fn to_type(&self) -> &str {
            "OrderPlaced"
        }

        fn to_version(&self) -> i32 {
            2
        }

        fn upcast(&self, body: &Value) -> Result<Value> {
            let mut updated = body.clone();
            updated["currency"] = json!("USD");
            Ok(updated)
        }
    }

    #[test]
    fn test_upcast_sync_applies_transformation() {
        let mut registry = UpcasterRegistry::new();
        registry.register(V1ToV2);

        let mut envelope = EventEnvelope {
            global_seq: 1,
            stream_id: uuid::Uuid::nil(),
            stream_seq: 1,
            typ: "OrderPlaced".to_string(),
            body: json!({"order_id": 1}),
            headers: Value::Null,
            causation_id: None,
            correlation_id: None,
            event_version: 1,
            tenant_id: None,
            user_id: None,
            created_at: chrono::Utc::now(),
        };

        registry.upcast_sync(&mut envelope).unwrap();

        assert_eq!(envelope.event_version, 2);
        assert_eq!(envelope.body["currency"], "USD");
    }

    #[test]
    fn test_find_path_returns_multi_hop() {
        struct V2ToV3;
        impl Upcaster for V2ToV3 {
            fn from_type(&self) -> &str {
                "OrderPlaced"
            }
            fn from_version(&self) -> i32 {
                2
            }
            fn to_type(&self) -> &str {
                "OrderPlaced"
            }
            fn to_version(&self) -> i32 {
                3
            }
            fn upcast(&self, body: &Value) -> Result<Value> {
                Ok(body.clone())
            }
        }

        let mut registry = UpcasterRegistry::new();
        registry.register(V1ToV2);
        registry.register(V2ToV3);

        let path = registry
            .find_path(("OrderPlaced", 1), ("OrderPlaced", 3))
            .unwrap();

        assert_eq!(
            path,
            vec![
                ("OrderPlaced".to_string(), 1),
                ("OrderPlaced".to_string(), 2),
                ("OrderPlaced".to_string(), 3)
            ]
        );
    }

    #[test]
    fn test_cycle_detection_errors() {
        #[derive(Clone)]
        struct LoopCaster;
        impl Upcaster for LoopCaster {
            fn from_type(&self) -> &str {
                "Loop"
            }
            fn from_version(&self) -> i32 {
                1
            }
            fn to_type(&self) -> &str {
                "Loop"
            }
            fn to_version(&self) -> i32 {
                1
            }
            fn upcast(&self, body: &Value) -> Result<Value> {
                Ok(body.clone())
            }
        }

        let mut registry = UpcasterRegistry::new();
        registry.register(LoopCaster);

        let mut envelope = EventEnvelope {
            global_seq: 1,
            stream_id: uuid::Uuid::nil(),
            stream_seq: 1,
            typ: "Loop".to_string(),
            body: json!({"value": 1}),
            headers: Value::Null,
            causation_id: None,
            correlation_id: None,
            event_version: 1,
            tenant_id: None,
            user_id: None,
            created_at: chrono::Utc::now(),
        };

        let err = registry.upcast_sync(&mut envelope).unwrap_err();
        assert!(matches!(
            err,
            Error::UpcastingCycle {
                event_type: _,
                version: _
            }
        ));
    }

    struct AsyncV2ToV3;

    #[async_trait]
    impl AsyncUpcaster for AsyncV2ToV3 {
        fn from_type(&self) -> &str {
            "OrderPlaced"
        }

        fn from_version(&self) -> i32 {
            2
        }

        fn to_type(&self) -> &str {
            "OrderPlaced"
        }

        fn to_version(&self) -> i32 {
            3
        }

        async fn upcast(&self, body: &Value, _pool: &PgPool) -> Result<Value> {
            let mut updated = body.clone();
            updated["enriched"] = json!(true);
            Ok(updated)
        }
    }

    #[tokio::test]
    async fn test_async_upcaster_requires_pool_and_applies() {
        let mut registry = UpcasterRegistry::new();
        registry.register(V1ToV2);
        registry.register_async(AsyncV2ToV3);

        let mut envelope = EventEnvelope {
            global_seq: 1,
            stream_id: uuid::Uuid::nil(),
            stream_seq: 1,
            typ: "OrderPlaced".to_string(),
            body: json!({"order_id": 1}),
            headers: Value::Null,
            causation_id: None,
            correlation_id: None,
            event_version: 1,
            tenant_id: None,
            user_id: None,
            created_at: chrono::Utc::now(),
        };

        let mut envelope_sync = envelope.clone();
        let err = registry.upcast_sync(&mut envelope_sync).unwrap_err();
        assert!(matches!(
            err,
            Error::UpcastingPoolRequired {
                event_type: _,
                version: _
            }
        ));

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://postgres@localhost/postgres")
            .expect("create lazy pool");

        registry
            .upcast_with_pool(&mut envelope, Some(&pool))
            .await
            .unwrap();

        assert_eq!(envelope.event_version, 3);
        assert_eq!(envelope.body["currency"], "USD");
        assert_eq!(envelope.body["enriched"], true);
    }

    #[test]
    fn builder_constructs_sync_upcaster() {
        let upcaster = UpcasterBuilder::new("OrderPlaced")
            .from_version(1)
            .to("OrderPlaced", 2)
            .handler(|body| {
                let mut updated = body.clone();
                updated["flag"] = json!(true);
                Ok(updated)
            })
            .build()
            .unwrap();

        let mut envelope = EventEnvelope {
            global_seq: 1,
            stream_id: uuid::Uuid::nil(),
            stream_seq: 1,
            typ: "OrderPlaced".into(),
            body: json!({"order_id": 1}),
            headers: Value::Null,
            causation_id: None,
            correlation_id: None,
            event_version: 1,
            tenant_id: None,
            user_id: None,
            created_at: chrono::Utc::now(),
        };

        let mut registry = UpcasterRegistry::new();
        registry.register(upcaster);
        registry.upcast_sync(&mut envelope).unwrap();
        assert_eq!(envelope.event_version, 2);
        assert_eq!(envelope.body["flag"], true);
    }

    #[test]
    fn macro_constructs_upcasters() {
        let sync = crate::sync_upcaster!(from "OrderPlaced", 1 => "OrderPlaced", 2, |order| {
            order["flag"] = json!(true);
            Ok(order)
        });
        let mut registry = UpcasterRegistry::new();
        registry.register(sync);

        let mut envelope = EventEnvelope {
            global_seq: 1,
            stream_id: uuid::Uuid::nil(),
            stream_seq: 1,
            typ: "OrderPlaced".into(),
            body: json!({"order_id": 5}),
            headers: Value::Null,
            causation_id: None,
            correlation_id: None,
            event_version: 1,
            tenant_id: None,
            user_id: None,
            created_at: chrono::Utc::now(),
        };

        registry.upcast_sync(&mut envelope).unwrap();
        assert_eq!(envelope.event_version, 2);
        assert_eq!(envelope.body["flag"], true);

        let async_upcaster = crate::async_upcaster!(from "OrderPlaced", 2 => "OrderPlaced", 3, |event, _pool| {
            event["tag"] = json!("macro");
            Ok(event)
        });

        assert_eq!(AsyncUpcaster::from_version(&async_upcaster), 2);
        assert_eq!(AsyncUpcaster::to_version(&async_upcaster), 3);
    }

    #[test]
    fn describe_returns_registered_entries() {
        let mut registry = UpcasterRegistry::new();
        registry.register(V1ToV2);
        registry.register_async(AsyncV2ToV3);

        let descriptors = registry.describe();
        assert_eq!(descriptors.len(), 2);
        assert!(descriptors.iter().any(|d| d.kind == UpcasterKind::Sync));
        assert!(descriptors.iter().any(|d| d.kind == UpcasterKind::Async));
    }
}
