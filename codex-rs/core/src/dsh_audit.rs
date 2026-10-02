//! Turn-owned callback for facts that the DSH host must durably acknowledge.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use anyhow::Result;
use futures::future::BoxFuture;
use serde_json::Value;

type Append = dyn Fn(Value) -> BoxFuture<'static, Result<()>> + Send + Sync;

/// One DSH Session turn and its required audit transport, independent of app-server internals.
pub struct DshAuditBinding {
    session_id: String,
    turn: u64,
    append: Arc<Append>,
}

impl std::fmt::Debug for DshAuditBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DshAuditBinding")
            .field("session_id", &self.session_id)
            .field("turn", &self.turn)
            .finish_non_exhaustive()
    }
}

impl DshAuditBinding {
    /// Create a binding whose callback returns only after DSH has acknowledged the fact.
    pub fn new(
        session_id: String,
        turn: u64,
        append: impl Fn(Value) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            session_id,
            turn,
            append: Arc::new(append),
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn turn(&self) -> u64 {
        self.turn
    }

    /// Await the host's durable acknowledgment before dependent work proceeds.
    pub async fn append(&self, fact: Value) -> Result<()> {
        (self.append)(fact).await
    }
}

/// Temporary handoff from one app-server request to one newly admitted Core turn.
#[derive(Debug, Default)]
pub struct DshAuditBindingRegistry {
    pending: Mutex<HashMap<u64, Arc<DshAuditBinding>>>,
}

impl DshAuditBindingRegistry {
    pub fn insert(&self, binding: DshAuditBinding) -> Result<()> {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if pending.contains_key(&binding.turn) {
            anyhow::bail!("DSH audit binding already exists for turn {}", binding.turn);
        }
        pending.insert(binding.turn, Arc::new(binding));
        Ok(())
    }

    /// Consume the binding once; a later turn cannot inherit it.
    pub fn take(&self, turn: u64) -> Option<Arc<DshAuditBinding>> {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&turn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn binding_is_single_use_and_keeps_the_host_ack() {
        let registry = DshAuditBindingRegistry::default();
        registry
            .insert(DshAuditBinding::new("session-a".to_string(), 3, |fact| {
                Box::pin(async move {
                    assert_eq!(fact, json!({ "kind": "model/request" }));
                    Ok(())
                })
            }))
            .expect("insert binding");
        let binding = registry.take(3).expect("binding for turn 3");
        assert_eq!(binding.session_id(), "session-a");
        assert_eq!(binding.turn(), 3);
        binding
            .append(json!({ "kind": "model/request" }))
            .await
            .expect("host ACK");
        assert!(registry.take(3).is_none());
        assert!(registry.take(4).is_none());
    }
}
