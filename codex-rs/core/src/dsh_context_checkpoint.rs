//! Required DSH checkpoint ACK before a Codex turn can report completion.

use crate::DshContextSnapshotV2;
use crate::dsh_audit::DshAuditBinding;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use serde_json::json;

impl Session {
    pub(crate) async fn dsh_record_context_checkpoint(
        &self,
        turn_context: &TurnContext,
    ) -> CodexResult<()> {
        let Some(binding) = turn_context.dsh_audit_binding() else {
            return Ok(());
        };
        let config = binding.context_config().cloned().ok_or_else(|| {
            CodexErr::Fatal("DSH checkpoint has no acknowledged turn configuration".to_string())
        })?;
        let source = self.dsh_context_source().await.map_err(|error| {
            CodexErr::Fatal(format!(
                "DSH checkpoint could not read durable context: {error}"
            ))
        })?;
        let snapshot = source
            .into_portable_snapshot(config)
            .map_err(|error| CodexErr::Fatal(format!("DSH checkpoint is not portable: {error}")))?;
        record_checkpoint_fact(binding.as_ref(), &turn_context.sub_id, snapshot).await
    }
}

async fn record_checkpoint_fact(
    binding: &DshAuditBinding,
    turn_id: &str,
    snapshot: DshContextSnapshotV2,
) -> CodexResult<()> {
    let effective_history_sha256 = snapshot.effective_history_sha256.clone();
    binding
        .append(json!({
            "eventId": format!("dsh-context-checkpoint-{turn_id}"),
            "turn": binding.turn(),
            "codexTurnId": turn_id,
            "kind": "checkpoint",
            "checkpointId": turn_id,
            "snapshot": { "kind": "inline", "value": snapshot },
            "effectiveHistorySha256": effective_history_sha256,
        }))
        .await
        .map_err(|error| CodexErr::Fatal(format!("DSH checkpoint audit failed: {error}")))
}

#[cfg(test)]
#[path = "dsh_context_checkpoint_tests.rs"]
mod tests;
