use serde_json::{Map as JsonMap, Value};
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
pub struct SessionContext {
    pub tenant: Option<String>,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub headers: JsonMap<String, Value>,
}

impl SessionContext {
    pub fn builder() -> SessionContextBuilder {
        SessionContextBuilder::default()
    }

    pub fn merge_headers(&mut self, headers: Value) {
        if let Value::Object(new) = headers {
            for (k, v) in new {
                self.headers.insert(k, v);
            }
        }
    }
}

#[derive(Default)]
pub struct SessionContextBuilder {
    tenant: Option<String>,
    causation_id: Option<Uuid>,
    correlation_id: Option<Uuid>,
    headers: JsonMap<String, Value>,
}

impl SessionContextBuilder {
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.correlation_id = Some(id);
        self
    }

    pub fn causation_id(mut self, id: Uuid) -> Self {
        self.causation_id = Some(id);
        self
    }

    pub fn headers(mut self, headers: Value) -> Self {
        if let Value::Object(map) = headers {
            self.headers = map;
        }
        self
    }

    pub fn build(self) -> SessionContext {
        SessionContext {
            tenant: self.tenant,
            causation_id: self.causation_id,
            correlation_id: self.correlation_id,
            headers: self.headers,
        }
    }
}
