//! Read-only comparison of a durable rollout checkpoint with a pending DSH compaction fact.

use crate::CodexThread;
use crate::session::session::Session;
use codex_history::RolloutItem;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DshCompactionCheckpointStatus {
    /// The recorded replacement history matches and no later model-visible item exists.
    Installed,
    /// The rollout has no checkpoint with this DSH compaction identity.
    Absent,
    /// A checkpoint exists but its history or later model-visible state differs.
    Conflict,
}

impl CodexThread {
    /// Fingerprint the prior durable rollout before permitting an automatic pending abort.
    pub async fn dsh_prior_rollout_sha256(&self) -> ThreadStoreResult<String> {
        self.session.dsh_prior_rollout_sha256().await
    }

    /// Compare a pending audit fact with durable rollout data, never the mutable live history.
    pub async fn dsh_compaction_checkpoint_status(
        &self,
        compaction_id: &str,
        effective_history: &Value,
    ) -> ThreadStoreResult<DshCompactionCheckpointStatus> {
        self.session
            .dsh_compaction_checkpoint_status(compaction_id, effective_history)
            .await
    }
}

impl Session {
    pub(crate) async fn dsh_prior_rollout_sha256(&self) -> ThreadStoreResult<String> {
        let live_thread = self
            .live_thread_for_persistence("read prior DSH compaction rollout")
            .map_err(|error| ThreadStoreError::Internal {
                message: error.to_string(),
            })?;
        let stored = live_thread.load_history(/*include_archived*/ false).await?;
        let bytes =
            serde_json::to_vec(&stored.items).map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to serialize prior DSH compaction rollout: {error}"),
            })?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub(crate) async fn dsh_compaction_checkpoint_status(
        &self,
        compaction_id: &str,
        effective_history: &Value,
    ) -> ThreadStoreResult<DshCompactionCheckpointStatus> {
        let live_thread = self
            .live_thread_for_persistence("read DSH compaction checkpoint")
            .map_err(|error| ThreadStoreError::Internal {
                message: error.to_string(),
            })?;
        let stored = live_thread.load_history(/*include_archived*/ false).await?;
        checkpoint_status(&stored.items, compaction_id, effective_history)
    }
}

fn checkpoint_status(
    items: &[RolloutItem],
    compaction_id: &str,
    effective_history: &Value,
) -> ThreadStoreResult<DshCompactionCheckpointStatus> {
    let matching = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            RolloutItem::Compacted(checkpoint)
                if checkpoint
                    .replacement_history
                    .as_ref()
                    .and_then(|history| history.last())
                    .and_then(|item| item.metadata.as_ref())
                    .and_then(|metadata| metadata.dsh_compaction_id.as_deref())
                    == Some(compaction_id) =>
            {
                Some((index, checkpoint))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(index, checkpoint)] = matching.as_slice() else {
        return Ok(if matching.is_empty() {
            DshCompactionCheckpointStatus::Absent
        } else {
            DshCompactionCheckpointStatus::Conflict
        });
    };
    let Some(history) = checkpoint.replacement_history.as_ref() else {
        return Ok(DshCompactionCheckpointStatus::Conflict);
    };
    let recorded = serde_json::to_value(history).map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to serialize DSH compaction checkpoint: {error}"),
    })?;
    let mut trailing = &items[*index + 1..];
    if matches!(trailing.first(), Some(RolloutItem::WorldState(baseline)) if baseline.full) {
        trailing = &trailing[1..];
    }
    if matches!(trailing.first(), Some(RolloutItem::TurnContext(_))) {
        trailing = &trailing[1..];
    }
    if matches!(
        trailing.first(),
        Some(RolloutItem::EventMsg(
            codex_protocol::protocol::EventMsg::ThreadSettingsApplied(_)
        ))
    ) {
        trailing = &trailing[1..];
    }
    if recorded != *effective_history
        || trailing.iter().any(|item| {
            !matches!(
                item,
                RolloutItem::EventMsg(_)
                    | RolloutItem::RealtimeItem(_)
                    | RolloutItem::SecurityRiskScore(_)
            )
        })
    {
        return Ok(DshCompactionCheckpointStatus::Conflict);
    }
    Ok(DshCompactionCheckpointStatus::Installed)
}

#[cfg(test)]
#[path = "dsh_compaction_checkpoint_tests.rs"]
mod tests;
