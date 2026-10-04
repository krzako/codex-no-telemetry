//! Required DSH facts for Codex-owned compaction, separate from optional rollout traces.

use std::sync::Arc;

use crate::Prompt;
use crate::dsh_audit::DshAuditBinding;
use crate::responses_metadata::CompactionTurnMetadata;
use codex_history::ResponseItemEnvelope;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ResponseItem;
use serde_json::Value;
use serde_json::json;
use uuid::Uuid;

pub(crate) async fn record_request(
    binding: Option<Arc<DshAuditBinding>>,
    compaction_id: &str,
    metadata: CompactionTurnMetadata,
    input_history: &[ResponseItem],
    prompt: &Prompt,
    model: &str,
) -> CodexResult<()> {
    let Some(binding) = binding else {
        return Ok(());
    };
    let reason = serde_json::to_value(metadata.reason()).map_err(|error| {
        CodexErr::Fatal(format!(
            "DSH compaction audit serialization failed: {error}"
        ))
    })?;
    let prompt = json!({
        "model": model,
        "baseInstructions": &prompt.base_instructions,
        "input": &prompt.input,
        "tools": &*prompt.tools,
        "parallelToolCalls": prompt.parallel_tool_calls,
        "outputSchema": &prompt.output_schema,
        "outputSchemaStrict": prompt.output_schema_strict,
        "cyberAccessProgram": &prompt.cyber_access_program,
        "compactionMetadata": metadata,
    });
    binding
        .append(json!({
            "eventId": format!("dsh-compaction-request-{}", Uuid::new_v4()),
            "turn": binding.turn(),
            "kind": "compaction/request",
            "compactionId": compaction_id,
            "reason": reason,
            "inputHistory": { "kind": "inline", "value": input_history },
            "prompt": { "kind": "inline", "value": prompt },
        }))
        .await
        .map_err(|error| CodexErr::Fatal(format!("DSH compaction audit failed: {error}")))
}

pub(crate) async fn record_result(
    binding: Option<Arc<DshAuditBinding>>,
    compaction_id: &str,
    result: Value,
) -> CodexResult<()> {
    let Some(binding) = binding else {
        return Ok(());
    };
    binding
        .append(json!({
            "eventId": format!("dsh-compaction-result-{}", Uuid::new_v4()),
            "turn": binding.turn(),
            "kind": "compaction/result",
            "compactionId": compaction_id,
            "result": { "kind": "inline", "value": result },
        }))
        .await
        .map_err(|error| CodexErr::Fatal(format!("DSH compaction audit failed: {error}")))
}

pub(crate) async fn record_failure(
    binding: Option<Arc<DshAuditBinding>>,
    compaction_id: &str,
    message: &str,
    details: Value,
) -> CodexResult<()> {
    let Some(binding) = binding else {
        return Ok(());
    };
    binding
        .append(json!({
            "eventId": format!("dsh-compaction-failure-{}", Uuid::new_v4()),
            "turn": binding.turn(),
            "kind": "compaction/failure",
            "compactionId": compaction_id,
            "message": message,
            "details": { "kind": "inline", "value": details },
        }))
        .await
        .map_err(|error| CodexErr::Fatal(format!("DSH compaction audit failed: {error}")))
}

pub(crate) async fn record_pending(
    binding: Arc<DshAuditBinding>,
    compaction_id: &str,
    effective_history: &[ResponseItemEnvelope],
) -> CodexResult<()> {
    binding
        .append(json!({
            "eventId": format!("dsh-compaction-pending-{compaction_id}"),
            "turn": binding.turn(),
            "kind": "compaction/pending",
            "compactionId": compaction_id,
            "effectiveHistory": { "kind": "inline", "value": effective_history },
        }))
        .await
        .map_err(|error| CodexErr::Fatal(format!("DSH compaction pending audit failed: {error}")))
}

pub(crate) async fn record_installed(
    binding: Arc<DshAuditBinding>,
    compaction_id: &str,
    effective_history: &[ResponseItemEnvelope],
) -> CodexResult<()> {
    binding
        .append(json!({
            "eventId": format!("dsh-compaction-installed-{compaction_id}"),
            "turn": binding.turn(),
            "kind": "compaction/installed",
            "compactionId": compaction_id,
            "effectiveHistory": { "kind": "inline", "value": effective_history },
        }))
        .await
        .map_err(|error| CodexErr::Fatal(format!("DSH compaction installed audit failed: {error}")))
}

#[cfg(test)]
#[path = "compaction_audit_tests.rs"]
mod tests;
