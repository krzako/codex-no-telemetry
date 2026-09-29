use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// A fork-owned fact offered to the DSH session audit. The client validates its v1 event shape.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct DshAuditAppendParams {
    pub thread_id: String,
    #[ts(type = "unknown")]
    pub event: Value,
}

/// The client returns this only after the event is durably committed.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct DshAuditAppendResponse {
    pub version: u32,
    pub sequence: u64,
    pub event_id: String,
}

/// Ask the client for the last durable sequence before replaying unacknowledged facts.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct DshAuditCursorParams {
    pub thread_id: String,
}

/// Sequence zero and a null event ID mean no fact has been committed for this thread.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct DshAuditCursorResponse {
    pub version: u32,
    pub sequence: u64,
    pub event_id: Option<String>,
}
