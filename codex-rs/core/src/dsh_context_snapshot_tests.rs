use super::*;
use crate::context_manager::ContextManager;
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
    assert_eq!(snapshot.hash_algorithm, "dsh-json-v1");
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
        dsh_json_sha256(&serde_json::to_value(&snapshot.effective_history).expect("history"))
            .expect("canonical history")
    );
    let serialized = serde_json::to_value(&snapshot).expect("serialize snapshot");
    // The DSH fixture `agent-codex/tests/fixtures/context-snapshot-v2-rust.json`
    // is generated from this snapshot; these anchors detect serialization drift.
    assert_eq!(
        snapshot.effective_history_sha256,
        "7c7e583c7f996b435ba1f715a6e57462537c7c85b510a3c0b7475f4f885429a0"
    );
    assert_eq!(
        snapshot.rollout_sha256,
        "5b9cdc10d41c6f08b87c348361738d2191a4c8e6bd2bdc093755a38b3bbb7632"
    );
    let audit_value = json!({ "snapshot": { "kind": "inline", "value": snapshot } });
    let transmitted = &audit_value["snapshot"]["value"];
    assert_eq!(
        transmitted["effectiveHistorySha256"],
        dsh_json_sha256(&transmitted["effectiveHistory"]).expect("transmitted history")
    );
    assert_eq!(
        transmitted["rolloutSha256"],
        dsh_json_sha256(&transmitted["rollout"]).expect("transmitted rollout")
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

#[test]
fn canonical_json_hash_covers_float_bits_and_utf8_key_order() {
    let value = json!({
        "10": 1e21,
        "2": -0.0,
        "nested": [1.0, 1e-7, 9_007_199_254_740_993_u64],
        "\u{e000}": "private-use",
        "😀": "supplementary",
    });
    assert_eq!(
        dsh_json_sha256(&value).expect("canonical JSON"),
        "b4b42ba5e7880e91469e8b01bdabf4a33bb05ed83b6583078e07af8e4cde2a9f"
    );
}

#[test]
fn imported_snapshot_round_trips_without_losing_fields() {
    let snapshot = source(vec![compaction(Some("first")), compaction(Some("second"))])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    let value = serde_json::to_value(&snapshot).expect("serialize snapshot");
    let imported = DshContextSnapshotV2::from_import_value(value.clone()).expect("valid import");
    assert_eq!(
        serde_json::to_value(imported).expect("serialize import"),
        value
    );
}

#[test]
fn imported_snapshot_rejects_corrupt_history_and_rollout_hashes() {
    let snapshot = source(vec![compaction(Some("first"))])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    let valid = serde_json::to_value(snapshot).expect("serialize snapshot");
    let mut history = valid.clone();
    history["effectiveHistorySha256"] = json!("0".repeat(64));
    assert!(DshContextSnapshotV2::from_import_value(history).is_err());

    let mut rollout = valid;
    rollout["rolloutSha256"] = json!("0".repeat(64));
    assert!(DshContextSnapshotV2::from_import_value(rollout).is_err());
}

#[test]
fn imported_snapshot_rejects_compaction_drift_even_with_valid_rollout_hash() {
    let snapshot = source(vec![compaction(Some("first")), compaction(Some("second"))])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    let valid = serde_json::to_value(snapshot).expect("serialize snapshot");

    let mut duplicate = valid.clone();
    duplicate["compactions"][1]["id"] = json!("first");
    assert!(DshContextSnapshotV2::from_import_value(duplicate).is_err());

    let mut changed_summary = valid.clone();
    changed_summary["compactions"][0]["summary"] = json!("different");
    assert!(DshContextSnapshotV2::from_import_value(changed_summary).is_err());

    let mut changed_rollout = valid;
    changed_rollout["rollout"][0]["payload"]["message"] = json!("different");
    changed_rollout["rolloutSha256"] =
        json!(dsh_json_sha256(&changed_rollout["rollout"]).expect("changed rollout hash"));
    assert!(DshContextSnapshotV2::from_import_value(changed_rollout).is_err());
}

#[test]
fn imported_snapshot_rejects_unsupported_or_unpreserved_data() {
    let snapshot = source(vec![compaction(Some("first"))])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    let valid = serde_json::to_value(snapshot).expect("serialize snapshot");

    let mut version = valid.clone();
    version["version"] = json!(3);
    assert!(DshContextSnapshotV2::from_import_value(version).is_err());

    let mut revision = valid.clone();
    revision["runtimeRevision"] = json!("another-fork");
    assert!(DshContextSnapshotV2::from_import_value(revision).is_err());

    let mut unknown = valid;
    unknown["retainedContext"]["futureFact"] = json!("must not be dropped");
    assert!(DshContextSnapshotV2::from_import_value(unknown).is_err());
}

#[test]
fn installs_only_verified_active_history_into_a_fresh_context() {
    let mut captured = source(vec![compaction(Some("first"))]);
    captured.retained_context = serde_json::from_value(json!({
        "verified_answers": [],
        "incomplete": false,
        "user_messages": [],
        "user_messages_incomplete": false,
        "next_order": 7,
    }))
    .expect("retained facts");
    let expected_history = captured.effective_history.clone();
    let expected_retained = captured.retained_context.clone();
    let expected_version = captured.history_version;
    let exported = captured
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    let imported = DshContextSnapshotV2::from_import_value(
        serde_json::to_value(exported).expect("serialize snapshot"),
    )
    .expect("verified import");

    let mut history = ContextManager::new();
    history
        .install_dsh_snapshot(imported)
        .expect("install checkpoint");
    assert_eq!(history.annotated_items(), expected_history);
    assert_eq!(history.retained_context(), &expected_retained);
    assert_eq!(history.history_version(), expected_version);
    assert_eq!(history.reset_version, expected_version);
    assert_eq!(history.reference_context_item(), None);
}

#[test]
fn refuses_invalid_or_nonfresh_install_without_changing_context() {
    let valid = source(vec![])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    let mut history = ContextManager::new();
    history.replace_annotated(vec![message()]);
    let before = history.annotated_items().to_vec();
    assert!(history.install_dsh_snapshot(valid).is_err());
    assert_eq!(history.annotated_items(), before);

    let mut corrupt = source(vec![])
        .into_portable_snapshot(config())
        .expect("portable snapshot");
    corrupt.effective_history_sha256 = "0".repeat(64);
    let mut fresh = ContextManager::new();
    assert!(fresh.install_dsh_snapshot(corrupt).is_err());
    assert!(fresh.annotated_items().is_empty());
    assert_eq!(fresh.history_version(), 0);
}
