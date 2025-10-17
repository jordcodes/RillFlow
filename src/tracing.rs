use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[cfg(feature = "otel")]
pub fn init_otlp_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace as sdktrace;
    use opentelemetry_sdk::{self, Resource};
    use tracing_subscriber::prelude::*;

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return Ok(());
    }

    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "rillflow".to_string());
    let tracer =
        opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic())
            .with_trace_config(sdktrace::config().with_resource(Resource::new(vec![
                KeyValue::new("service.name", service_name),
            ])))
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(env_filter))
        .with(tracing_subscriber::fmt::layer().json())
        .with(otel_layer)
        .try_init()
        .ok();
    Ok(())
}

#[cfg(not(feature = "otel"))]
pub fn init_otlp_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Ok(())
}

type TraceEntry = (String, String, serde_json::Value);
type TraceBuffer = HashMap<String, VecDeque<TraceEntry>>;

#[derive(Clone, Default)]
pub struct TraceSink {
    inner: Arc<Mutex<TraceBuffer>>,
    cap: usize,
}

impl TraceSink {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::default(),
            cap,
        }
    }

    pub fn record(&self, trace_id: &str, kind: &str, component: &str, data: serde_json::Value) {
        #[cfg(debug_assertions)]
        {
            let mut map = self.inner.lock().expect("trace sink poisoned");
            let queue = map.entry(trace_id.to_string()).or_default();
            if queue.len() >= self.cap {
                queue.pop_front();
            }
            queue.push_back((kind.to_string(), component.to_string(), data));
        }
    }

    pub fn mermaid(&self, trace_id: &str) -> String {
        let mut diagram = String::from("sequenceDiagram\n");
        let lanes = ["HTTP", "CMD", "ES", "PR"];
        for lane in lanes {
            diagram.push_str(&format!("  participant {lane} as {lane}\n"));
        }

        let map = self.inner.lock().expect("trace sink poisoned");
        if let Some(queue) = map.get(trace_id) {
            for (kind, component, data) in queue.iter() {
                let payload =
                    json!({ "kind": kind, "component": component, "data": data }).to_string();
                let line = match kind.as_str() {
                    "HTTP_IN" => format!("  HTTP->>CMD: {payload}\n"),
                    "APPEND" => format!("  CMD->>ES: {payload}\n"),
                    "PROJECT" => format!("  ES-->>PR: {payload}\n"),
                    _ => format!("  CMD->>PR: {payload}\n"),
                };
                diagram.push_str(&line);
            }
        }

        diagram
    }
}

#[cfg(test)]
mod tests {
    use super::TraceSink;
    use serde_json::json;

    #[test]
    fn mermaid_formats_sequence() {
        let sink = TraceSink::new(4);

        sink.record("t1", "HTTP_IN", "api", json!({"path": "/customers"}));
        sink.record("t1", "APPEND", "store", json!({"stream": "cust"}));
        sink.record("t1", "PROJECT", "projection", json!({"name": "read_model"}));

        let mermaid = sink.mermaid("t1");

        assert!(mermaid.contains("sequenceDiagram"));
        assert!(mermaid.contains("participant HTTP as HTTP"));
        assert!(mermaid.contains("HTTP->>CMD"));
        assert!(mermaid.contains("CMD->>ES"));
        assert!(mermaid.contains("ES-->>PR"));
        assert!(mermaid.contains("\"path\":\"/customers\""));
    }
}
