use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct TraceSink {
    inner: Arc<Mutex<HashMap<String, VecDeque<(String, String, serde_json::Value)>>>>,
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

