use super::*;
use codex_history::CodexHarnessMetadata;
use codex_history::CompactedItem;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;
use serde_json::json;

fn message() -> ResponseItemEnvelope {
    ResponseItemEnvelope::new(ResponseItem::Message {
        id: Some(ResponseItemId::from_server("response-1".to_string())),
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: "final answer".to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    })
}

fn compaction(id: Option<&str>) -> RolloutItem {
    RolloutItem::Compacted(CompactedItem {
        message: "summary".to_string(),
        replacement_history: Some(vec![ResponseItemEnvelope {
            item: ResponseItem::CompactionTrigger {},
            metadata: id.map(|id| CodexHarnessMetadata {
                dsh_compaction_id: Some(id.to_string()),
                ..Default::default()
            }),
        }]),
        guardian_history: None,
        retained_context: None,
        mcp_resource_origins: None,
        window_number: None,
        first_window_id: None,
        previous_window_id: None,
        window_id: Some("window-2".to_string()),
        compaction_response_id: Some("response-compact".to_string()),
        latest_token_usage_record: None,
    })
}

fn source(rollout: Vec<RolloutItem>) -> DshContextSource {
    let rollout_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&rollout).expect("rollout"))
    );
    DshContextSource {
        effective_history: vec![message()],
        retained_context: RetainedContext::default(),
        history_version: 4,
        rollout,
        rollout_sha256,
    }
}

fn config() -> Value {
    json!({
        "schemaVersion": 1,
        "fork": { "revision": "fork-abc", "packageVersion": "0.1.0" },
        "instructions": { "base": "base", "developer": "developer" },
        "dynamicTools": [{ "name": "tool" }],
    })
}

#[test]
fn preserves_effective_history_config_and_compaction_chain() {
    let snapshot = source(vec![compaction(Some("first")), compaction(Some("second"))])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    assert_eq!(snapshot.version, 2);
    assert_eq!(snapshot.runtime_revision, "fork-abc");
    assert_eq!(snapshot.compactions.len(), 2);
    assert_eq!(snapshot.compactions[0].id, "first");
    assert_eq!(snapshot.compactions[1].id, "second");
    assert_eq!(
        snapshot.compactions[1].response_id.as_deref(),
        Some("response-compact")
    );
    assert_eq!(snapshot.context_config, config());
    assert_eq!(snapshot.effective_history, vec![message()]);
    assert_eq!(
        snapshot.effective_history_sha256,
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&snapshot.effective_history).expect("history"))
        )
    );
    let serialized = serde_json::to_value(&snapshot).expect("serialize snapshot");
    // The DSH fixture `agent-codex/tests/fixtures/context-snapshot-v2-rust.json`
    // is generated from this snapshot; these anchors detect serialization drift.
    assert_eq!(
        snapshot.effective_history_sha256,
        "2e7a4c4074e9d49b76f303800f3e16a1fd69b29ad7b7925e10187708583413fa"
    );
    assert_eq!(
        snapshot.rollout_sha256,
        "519b57c72105b67760f31c06daa90aa65f2cd606c2507c9e979b4dbaf5ea94c5"
    );
    assert_eq!(serialized["rolloutSha256"], snapshot.rollout_sha256);
    assert!(serialized.get("rolloutPath").is_none());
}

#[test]
fn rejects_legacy_or_duplicate_compaction_identity() {
    assert!(
        source(vec![compaction(None)])
            .into_portable_snapshot(config())
            .is_err()
    );
    assert!(
        source(vec![compaction(Some("repeat")), compaction(Some("repeat"))])
            .into_portable_snapshot(config())
            .is_err()
    );
    let mut changed_rollout = source(vec![compaction(Some("valid"))]);
    changed_rollout.rollout_sha256 = "0".repeat(64);
    assert!(changed_rollout.into_portable_snapshot(config()).is_err());
}
