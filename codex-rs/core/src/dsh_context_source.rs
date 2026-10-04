//! Consistent source data for a portable DSH checkpoint, not an importable snapshot itself.

use crate::CodexThread;
use crate::context_manager::ContextManager;
use crate::session::session::Session;
use codex_history::ResponseItemEnvelope;
use codex_history::RetainedContext;
use codex_history::RolloutItem;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use sha2::Digest;
use sha2::Sha256;

/// The live model window and durable rollout observed across one persistence barrier.
/// Callers must only capture this after a turn has become quiescent; a concurrent change is rejected.
#[derive(Debug)]
pub struct DshContextSource {
    pub effective_history: Vec<ResponseItemEnvelope>,
    pub retained_context: RetainedContext,
    pub history_version: u64,
    pub rollout: Vec<RolloutItem>,
    pub rollout_sha256: String,
}

impl CodexThread {
    /// Flush the rollout, then capture the effective history and its durable provenance.
    pub async fn dsh_context_source(&self) -> ThreadStoreResult<DshContextSource> {
        self.session.dsh_context_source().await
    }
}

impl Session {
    pub(crate) async fn dsh_context_source(&self) -> ThreadStoreResult<DshContextSource> {
        let before = self.clone_history().await;
        self.flush_rollout()
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to flush DSH context rollout: {error}"),
            })?;
        let stored = self
            .live_thread_for_persistence("capture DSH context")
            .map_err(|error| ThreadStoreError::Internal {
                message: error.to_string(),
            })?
            .load_history(/*include_archived*/ false)
            .await?;
        let after = self.clone_history().await;
        capture_source(before, after, stored.items)
    }
}

fn capture_source(
    before: ContextManager,
    after: ContextManager,
    rollout: Vec<RolloutItem>,
) -> ThreadStoreResult<DshContextSource> {
    if before.history_version() != after.history_version()
        || before.annotated_items() != after.annotated_items()
        || before.retained_context() != after.retained_context()
    {
        return Err(ThreadStoreError::Internal {
            message: "DSH context changed during durable checkpoint capture".to_string(),
        });
    }
    let bytes = serde_json::to_vec(&rollout).map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to serialize DSH context rollout: {error}"),
    })?;
    Ok(DshContextSource {
        effective_history: after.annotated_items().to_vec(),
        retained_context: after.retained_context().clone(),
        history_version: after.history_version(),
        rollout,
        rollout_sha256: format!("{:x}", Sha256::digest(bytes)),
    })
}

#[cfg(test)]
#[path = "dsh_context_source_tests.rs"]
mod tests;
