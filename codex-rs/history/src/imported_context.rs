//! Durable active context installed from the canonical DSH transfer checkpoint.

use crate::ResponseItemEnvelope;
use crate::RetainedContext;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Source provenance precedes this record in the rollout. Resume restores these exact
/// items and retained facts before applying later records, without replaying actions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ImportedContextV1 {
    pub effective_history: Vec<ResponseItemEnvelope>,
    pub retained_context: RetainedContext,
    pub history_version: u64,
}
