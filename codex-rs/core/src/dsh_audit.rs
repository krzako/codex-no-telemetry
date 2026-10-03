//! Turn-owned callback for facts that the DSH host must durably acknowledge.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use anyhow::Result;
use codex_api::ResponseEvent;
use codex_protocol::models::ResponseItem;
use futures::future::BoxFuture;
use serde_json::Value;
use serde_json::json;

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

    pub(crate) async fn model_request(
        &self,
        attempt_id: &str,
        purpose: &'static str,
        request: Value,
    ) -> Result<()> {
        self.append(json!({
            "eventId": format!("dsh-model-request-{attempt_id}"),
            "turn": self.turn,
            "kind": "model/request",
            "attemptId": attempt_id,
            "purpose": purpose,
            "stage": "model-visible",
            "apiFormat": "responses",
            "request": { "kind": "inline", "value": request },
        }))
        .await
    }
}

/// Ordered parsed response facts for one already-audited model request.
#[derive(Debug)]
pub(crate) struct DshModelAttempt {
    binding: Arc<DshAuditBinding>,
    attempt_id: String,
    purpose: &'static str,
    next_index: u64,
}

impl DshModelAttempt {
    pub(crate) fn new(
        binding: Arc<DshAuditBinding>,
        attempt_id: String,
        purpose: &'static str,
    ) -> Self {
        Self {
            binding,
            attempt_id,
            purpose,
            next_index: 0,
        }
    }

    pub(crate) async fn stream_event(&mut self, event: &ResponseEvent) -> Result<()> {
        let index = self.next_index;
        let chunk = serde_json::to_value(event)?;
        self.binding
            .append(json!({
                "eventId": format!("dsh-model-stream-{}-{index}", self.attempt_id),
                "turn": self.binding.turn,
                "kind": "model/stream",
                "attemptId": &self.attempt_id,
                "purpose": self.purpose,
                "stage": "codex",
                "index": index,
                "chunk": { "kind": "inline", "value": chunk },
            }))
            .await?;
        self.next_index += 1;
        Ok(())
    }

    pub(crate) async fn completed(
        &self,
        event: &ResponseEvent,
        output_items: &[ResponseItem],
    ) -> Result<()> {
        let result = json!({
            "terminal": serde_json::to_value(event)?,
            "outputItems": output_items,
        });
        self.binding
            .append(json!({
                "eventId": format!("dsh-model-result-{}", self.attempt_id),
                "turn": self.binding.turn,
                "kind": "model/result",
                "attemptId": &self.attempt_id,
                "purpose": self.purpose,
                "stage": "codex",
                "result": { "kind": "inline", "value": result },
            }))
            .await
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

    #[tokio::test]
    async fn model_request_preserves_the_full_payload_and_attempt_identity() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let facts = Arc::clone(&recorded);
        let binding = DshAuditBinding::new("session-a".to_string(), 7, move |fact| {
            let facts = Arc::clone(&facts);
            Box::pin(async move {
                facts
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(fact);
                Ok(())
            })
        });
        let request = json!({ "model": "custom", "instructions": "keep me", "input": [] });
        binding
            .model_request("123", "compaction", request.clone())
            .await
            .expect("host ACK");
        assert_eq!(
            *recorded.lock().unwrap_or_else(PoisonError::into_inner),
            vec![json!({
                "eventId": "dsh-model-request-123",
                "turn": 7,
                "kind": "model/request",
                "attemptId": "123",
                "purpose": "compaction",
                "stage": "model-visible",
                "apiFormat": "responses",
                "request": { "kind": "inline", "value": request },
            })]
        );
    }
}
