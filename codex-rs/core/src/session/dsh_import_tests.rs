use crate::DshContextSource;
use crate::context::ContextualUserFragment;
use crate::session::step_context::StepContext;
use crate::session::tests::build_world_state_from_turn_context;
use crate::session::tests::make_session_and_context;
use codex_history::ResponseItemEnvelope;
use codex_history::RetainedContext;
use codex_history::RolloutItem;
use codex_protocol::protocol::WorldStateItem;
use pretty_assertions::assert_eq;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use std::sync::Arc;

#[tokio::test]
async fn imported_context_does_not_reinject_unchanged_environment() {
    let (session, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let world = build_world_state_from_turn_context(&session, &turn).await;
    let history: Vec<ResponseItemEnvelope> = world
        .render_full()
        .into_iter()
        .map(ContextualUserFragment::into_boxed_response_item)
        .map(ResponseItemEnvelope::new)
        .collect();
    let mut rollout: Vec<_> = history
        .iter()
        .cloned()
        .map(RolloutItem::ResponseItem)
        .collect();
    rollout.push(RolloutItem::WorldState(WorldStateItem::full(
        world.snapshot().into_object(),
    )));
    rollout.push(RolloutItem::TurnContext(turn.to_turn_context_item()));
    let rollout_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&rollout).unwrap())
    );
    let snapshot = DshContextSource {
        effective_history: history.clone(),
        retained_context: RetainedContext::default(),
        history_version: 4,
        rollout,
        rollout_sha256,
    }
    .into_portable_snapshot(json!({"schemaVersion": 1, "fork": {"revision": "test"}}))
    .unwrap();
    session.install_dsh_context(snapshot).await.unwrap();
    session
        .record_context_updates_and_set_reference_context_item(&StepContext::for_test(turn))
        .await
        .unwrap();
    assert_eq!(session.clone_history().await.annotated_items(), history);
}
