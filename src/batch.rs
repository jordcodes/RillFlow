use crate::{Result, documents::Documents};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

pub struct BatchWriter {
    documents: Documents,
    // upserts without OCC
    upserts: HashMap<Uuid, Value>,
    // updates with OCC (expected version)
    updates_expected: Vec<(Uuid, Value, i32)>,
    // deletes
    deletes: Vec<Uuid>,
}

impl BatchWriter {
    pub fn new(documents: Documents) -> Self {
        Self {
            documents,
            upserts: HashMap::new(),
            updates_expected: Vec::new(),
            deletes: Vec::new(),
        }
    }

    pub fn upsert<T: Serialize>(&mut self, id: Uuid, doc: &T) -> crate::Result<()> {
        let v = serde_json::to_value(doc)?;
        self.upserts.insert(id, v);
        Ok(())
    }

    pub fn update_expected<T: Serialize>(
        &mut self,
        id: Uuid,
        doc: &T,
        expected: i32,
    ) -> crate::Result<()> {
        let v = serde_json::to_value(doc)?;
        self.updates_expected.push((id, v, expected));
        Ok(())
    }

    pub fn delete(&mut self, id: Uuid) {
        self.deletes.push(id);
    }

    pub async fn flush(&mut self) -> Result<FlushOutcome> {
        let docs = self.documents.clone();
        let mut outcome = FlushOutcome::default();

        if !self.upserts.is_empty() {
            let items: Vec<(Uuid, Value)> = self.upserts.drain().collect();
            let affected = docs.bulk_upsert(&items).await?;
            outcome.upserts = affected;
        }

        if !self.updates_expected.is_empty() {
            let items = std::mem::take(&mut self.updates_expected);
            let (updated, conflicts) = docs.bulk_update_expected(&items).await?;
            outcome.updates = updated;
            outcome.conflicts = conflicts;
        }

        if !self.deletes.is_empty() {
            let ids = std::mem::take(&mut self.deletes);
            outcome.deletes = docs.bulk_delete(&ids).await? as usize;
        }

        Ok(outcome)
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FlushOutcome {
    pub upserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub conflicts: usize,
}
