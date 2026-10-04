use super::*;
use codex_history::CodexHarnessMetadata;
use codex_history::CompactedItem;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::WorldStateItem;
use pretty_assertions::assert_eq;

fn checkpoint(id: Option<&str>) -> RolloutItem {
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
        window_id: None,
        compaction_response_id: None,
        latest_token_usage_record: None,
    })
}

fn expected_history(checkpoint: &RolloutItem) -> Value {
    let RolloutItem::Compacted(checkpoint) = checkpoint else {
        panic!("expected checkpoint")
    };
    serde_json::to_value(checkpoint.replacement_history.as_ref().expect("history"))
        .expect("serialize history")
}

#[test]
fn matching_checkpoint_has_no_later_model_visible_change() {
    let installed = checkpoint(Some("compact-1"));
    let expected = expected_history(&installed);
    assert_eq!(
        checkpoint_status(&[installed], "compact-1", &expected).expect("read checkpoint"),
        DshCompactionCheckpointStatus::Installed
    );
}

#[test]
fn missing_checkpoint_including_older_rollout_is_absent() {
    let legacy = checkpoint(None);
    let expected = expected_history(&legacy);
    for items in [Vec::new(), vec![legacy], vec![checkpoint(Some("earlier"))]] {
        assert_eq!(
            checkpoint_status(&items, "compact-1", &expected).expect("read rollout"),
            DshCompactionCheckpointStatus::Absent
        );
    }
}

#[test]
fn changed_history_duplicate_identity_or_later_model_item_conflicts() {
    let installed = checkpoint(Some("compact-1"));
    let expected = expected_history(&installed);
    assert_eq!(
        checkpoint_status(&[installed.clone()], "compact-1", &serde_json::json!([]))
            .expect("read mismatched checkpoint"),
        DshCompactionCheckpointStatus::Conflict
    );
    assert_eq!(
        checkpoint_status(
            &[installed.clone(), installed.clone()],
            "compact-1",
            &expected
        )
        .expect("read duplicate checkpoint"),
        DshCompactionCheckpointStatus::Conflict
    );
    assert_eq!(
        checkpoint_status(
            &[
                installed,
                RolloutItem::ResponseItem(ResponseItemEnvelope::new(
                    ResponseItem::CompactionTrigger {},
                )),
            ],
            "compact-1",
            &expected,
        )
        .expect("read later model item"),
        DshCompactionCheckpointStatus::Conflict
    );
    assert_eq!(
        checkpoint_status(
            &[
                checkpoint(Some("compact-1")),
                RolloutItem::WorldState(WorldStateItem::full(serde_json::Map::new())),
                RolloutItem::WorldState(WorldStateItem::patch(serde_json::Map::new())),
            ],
            "compact-1",
            &expected,
        )
        .expect("read later world state"),
        DshCompactionCheckpointStatus::Conflict
    );
}
