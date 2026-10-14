//! Turn-owned callback for facts that the DSH host must durably acknowledge.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use anyhow::Result;
use codex_api::ResponseEvent;
use codex_protocol::models::ResponseItem;
use futures::future::BoxFuture;
use serde_json::Value;
use serde_json::json;
use tokio::sync::Mutex as AsyncMutex;

type Append = dyn Fn(Value) -> BoxFuture<'static, Result<()>> + Send + Sync;

/// One DSH Session turn and its required audit transport, independent of app-server internals.
pub struct DshAuditBinding {
    session_id: String,
    turn: u64,
    context_config: Option<Value>,
    append: Arc<Append>,
    append_gate: AsyncMutex<()>,
    failed: AtomicBool,
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
            context_config: None,
            append: Arc::new(append),
            append_gate: AsyncMutex::new(()),
            failed: AtomicBool::new(false),
        }
    }

    /// Bind the credential-free configuration whose `context/config` fact DSH already acknowledged.
    pub fn with_context_config(
        session_id: String,
        turn: u64,
        context_config: Value,
        append: impl Fn(Value) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
    ) -> Self {
        let mut binding = Self::new(session_id, turn, append);
        binding.context_config = Some(context_config);
        binding
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn turn(&self) -> u64 {
        self.turn
    }

    /// The accepted configuration of this turn, if its app-server preflight supplied one.
    pub fn context_config(&self) -> Option<&Value> {
        self.context_config.as_ref()
    }

    pub(crate) fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    /// Await the host's durable acknowledgment before dependent work proceeds.
    pub async fn append(&self, fact: Value) -> Result<()> {
        let _guard = self.append_gate.lock().await;
        if self.failed.load(Ordering::Acquire) {
            anyhow::bail!("DSH audit binding failed earlier in this turn");
        }
        match (self.append)(fact).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error)
            }
        }
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

    pub(crate) async fn failure(
        &self,
        code: &'static str,
        message: &str,
        output_items: &[ResponseItem],
        upstream_request_id: Option<&str>,
    ) -> Result<()> {
        self.binding
            .append(json!({
                "eventId": format!("dsh-model-failure-{}", self.attempt_id),
                "turn": self.binding.turn,
                "kind": "model/failure",
                "attemptId": &self.attempt_id,
                "purpose": self.purpose,
                "stage": "codex",
                "code": code,
                "message": message,
                "details": { "kind": "inline", "value": {
                    "outputItems": output_items,
                    "upstreamRequestId": upstream_request_id,
                } },
            }))
            .await
    }
}

/// Temporary handoff from one app-server request to one newly admitted Core turn.
#[derive(Debug, Default)]
pub struct DshAuditBindingRegistry {
    pending: Mutex<HashMap<u64, Arc<DshAuditBinding>>>,
    failed: AtomicBool,
}

impl DshAuditBindingRegistry {
    pub fn insert(&self, binding: DshAuditBinding) -> Result<()> {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if self.failed.load(Ordering::Acquire) {
            anyhow::bail!("DSH thread persistence failed; further turns are blocked");
        }
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

    pub(crate) fn fail(&self) {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        self.failed.store(true, Ordering::Release);
        for binding in pending.drain().map(|(_, binding)| binding) {
            binding.fail();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

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

    #[test]
    fn accepted_context_config_is_immutable_and_isolated_by_turn() {
        let registry = DshAuditBindingRegistry::default();
        let mut accepted = json!({ "model": "first", "instructions": "keep" });
        registry
            .insert(DshAuditBinding::with_context_config(
                "session-a".to_string(),
                1,
                accepted.clone(),
                |_| Box::pin(async { Ok(()) }),
            ))
            .expect("first turn");
        accepted["model"] = json!("changed in caller");
        registry
            .insert(DshAuditBinding::with_context_config(
                "session-a".to_string(),
                2,
                json!({ "model": "second" }),
                |_| Box::pin(async { Ok(()) }),
            ))
            .expect("second turn");
        assert_eq!(
            registry.take(1).expect("first binding").context_config(),
            Some(&json!({ "model": "first", "instructions": "keep" }))
        );
        assert_eq!(
            registry.take(2).expect("second binding").context_config(),
            Some(&json!({ "model": "second" }))
        );
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

    #[tokio::test]
    async fn missing_ack_poisoning_blocks_later_model_work() {
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let binding = DshAuditBinding::new("session-a".into(), 7, move |_fact| {
            let callback_calls = Arc::clone(&callback_calls);
            Box::pin(async move {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("storage unavailable")
            })
        });

        assert!(
            binding
                .append(json!({ "kind": "model/failure" }))
                .await
                .is_err()
        );
        assert!(
            binding
                .model_request("attempt-2", "inference", json!({ "input": [] }))
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
