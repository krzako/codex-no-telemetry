use super::RollbackPlanner;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;

#[test]
fn imported_context_followed_by_rollback_cannot_be_silently_migrated() {
    let mut planner = RollbackPlanner::new();
    let line = |item| RolloutLine {
        timestamp: "2026-10-13T00:00:00Z".to_string(),
        ordinal: None,
        item,
    };
    let seed = serde_json::from_value(serde_json::json!({
        "type": "dsh_imported_context_v1",
        "payload": { "effective_history": [], "history_version": 1,
            "retained_context": { "verified_answers": [], "incomplete": false,
                "user_messages": [], "user_messages_incomplete": false, "next_order": 0 } }
    }))
    .unwrap();
    planner.observe(&line(seed)).unwrap();
    let error = planner
        .observe(&line(RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
            ThreadRolledBackEvent { num_turns: 1 },
        ))))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot migrate rollback after DSH context import")
    );
}
