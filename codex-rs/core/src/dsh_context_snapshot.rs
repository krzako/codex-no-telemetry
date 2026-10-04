//! Versioned private Codex context payload for the shared DSH session archive.

use crate::DshContextSource;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::ensure;
use codex_history::ResponseItemEnvelope;
use codex_history::RetainedContext;
use codex_history::RolloutItem;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashSet;

/// A compaction installed in the durable rollout, including its model-visible result.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DshCompactionStateV2 {
    pub id: String,
    pub summary: String,
    pub replacement_history: Vec<ResponseItemEnvelope>,
    pub response_id: Option<String>,
    pub window_id: Option<String>,
}

/// Exact Codex-owned state and its provenance; host paths in the rollout are explanatory only.
/// Import must restore `effective_history`, never replay `rollout` side effects.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DshContextSnapshotV2 {
    pub version: u8,
    pub hash_algorithm: String,
    pub runtime_revision: String,
    pub history_version: u64,
    pub effective_history: Vec<ResponseItemEnvelope>,
    pub effective_history_sha256: String,
    pub retained_context: RetainedContext,
    pub compactions: Vec<DshCompactionStateV2>,
    pub rollout: Vec<RolloutItem>,
    pub rollout_sha256: String,
    pub context_config: Value,
}

impl DshContextSource {
    /// Convert a stable live/durable capture and the ACKed turn configuration to archive data.
    pub fn into_portable_snapshot(self, context_config: Value) -> Result<DshContextSnapshotV2> {
        ensure!(
            context_config["schemaVersion"] == 1,
            "DSH context configuration is absent or has an unsupported version"
        );
        let runtime_revision = context_config
            .pointer("/fork/revision")
            .and_then(Value::as_str)
            .filter(|revision| !revision.is_empty())
            .or_else(|| {
                context_config
                    .pointer("/fork/packageVersion")
                    .and_then(Value::as_str)
                    .filter(|revision| !revision.is_empty())
            })
            .ok_or_else(|| anyhow!("DSH context configuration has no fork revision"))?
            .to_string();
        let rollout_hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&self.rollout)?));
        ensure!(
            self.rollout_sha256 == rollout_hash,
            "DSH context rollout changed after its durable capture"
        );
        let mut compactions = Vec::new();
        let mut ids = HashSet::new();
        for item in &self.rollout {
            let RolloutItem::Compacted(checkpoint) = item else {
                continue;
            };
            let replacement_history = checkpoint
                .replacement_history
                .as_ref()
                .ok_or_else(|| anyhow!("DSH compaction has no installed replacement history"))?;
            let id = replacement_history
                .last()
                .and_then(|item| item.metadata.as_ref())
                .and_then(|metadata| metadata.dsh_compaction_id.as_deref())
                .ok_or_else(|| anyhow!("DSH compaction has no durable identity"))?;
            ensure!(
                ids.insert(id.to_string()),
                "duplicate DSH compaction identity {id}"
            );
            compactions.push(DshCompactionStateV2 {
                id: id.to_string(),
                summary: checkpoint.message.clone(),
                replacement_history: replacement_history.clone(),
                response_id: checkpoint.compaction_response_id.clone(),
                window_id: checkpoint.window_id.clone(),
            });
        }
        // Hash the JSON value that the audit channel actually transports. Its
        // object keys are normalized by serde_json::Value, not the original
        // Rust struct field order. Numbers use IEEE-754 bits so JS and Rust
        // agree even when their JSON number printers differ.
        let history_hash = dsh_json_sha256(&serde_json::to_value(&self.effective_history)?)?;
        let portable_rollout_hash = dsh_json_sha256(&serde_json::to_value(&self.rollout)?)?;
        Ok(DshContextSnapshotV2 {
            version: 2,
            hash_algorithm: "dsh-json-v1".to_string(),
            runtime_revision,
            history_version: self.history_version,
            effective_history: self.effective_history,
            effective_history_sha256: history_hash,
            retained_context: self.retained_context,
            compactions,
            rollout: self.rollout,
            rollout_sha256: portable_rollout_hash,
            context_config,
        })
    }
}

/// SHA-256 of typed JSON values, independent of object field order or numeric print style.
fn dsh_json_sha256(value: &Value) -> Result<String> {
    fn write_len(hash: &mut Sha256, len: usize) -> Result<()> {
        hash.update(u64::try_from(len)?.to_be_bytes());
        Ok(())
    }

    fn write_value(hash: &mut Sha256, value: &Value) -> Result<()> {
        match value {
            Value::Null => hash.update(b"N"),
            Value::Bool(false) => hash.update(b"F"),
            Value::Bool(true) => hash.update(b"T"),
            Value::Number(number) => {
                hash.update(b"D");
                let value = number
                    .as_f64()
                    .ok_or_else(|| anyhow!("DSH JSON number cannot be represented as f64"))?;
                ensure!(value.is_finite(), "DSH JSON number is not finite");
                hash.update(value.to_bits().to_be_bytes());
            }
            Value::String(text) => {
                hash.update(b"S");
                write_len(hash, text.len())?;
                hash.update(text.as_bytes());
            }
            Value::Array(items) => {
                hash.update(b"A");
                write_len(hash, items.len())?;
                for item in items {
                    write_value(hash, item)?;
                }
            }
            Value::Object(properties) => {
                hash.update(b"O");
                write_len(hash, properties.len())?;
                let mut entries: Vec<_> = properties.iter().collect();
                entries.sort_unstable_by(|(left, _), (right, _)| {
                    left.as_bytes().cmp(right.as_bytes())
                });
                for (key, item) in entries {
                    write_len(hash, key.len())?;
                    hash.update(key.as_bytes());
                    write_value(hash, item)?;
                }
            }
        }
        Ok(())
    }

    let mut hash = Sha256::new();
    write_value(&mut hash, value)?;
    Ok(format!("{:x}", hash.finalize()))
}

#[cfg(test)]
#[path = "dsh_context_snapshot_tests.rs"]
mod tests;
