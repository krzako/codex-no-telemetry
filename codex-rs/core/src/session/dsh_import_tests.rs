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
    let (mut session, turn) = make_session_and_context().await;
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
    let serialized = serde_json::to_value(&snapshot).unwrap();
    let duplicate = serde_json::from_value(serialized.clone()).unwrap();
    let missing_store = session.install_dsh_context(duplicate).await.unwrap_err();
    assert!(
        missing_store
            .to_string()
            .contains("Session persistence is disabled")
    );
    assert!(session.clone_history().await.annotated_items().is_empty());
    let path = crate::session::tests::open_thread_persistence(&mut session).await;
    session.install_dsh_context(snapshot).await.unwrap();
    let (stored, _, parse_errors) = codex_rollout::RolloutRecorder::load_rollout_items(&path)
        .await
        .unwrap();
    assert_eq!(parse_errors, 0);
    let seed = stored.iter().find_map(|item| match item {
        RolloutItem::DshImportedContextV1(seed) => Some(seed.as_ref()),
        _ => None,
    });
    assert_eq!(
        seed,
        Some(&codex_history::ImportedContextV1 {
            effective_history: history.clone(),
            retained_context: RetainedContext::default(),
            history_version: 4,
        })
    );
    session
        .record_context_updates_and_set_reference_context_item(&StepContext::for_test(turn))
        .await
        .unwrap();
    assert_eq!(session.clone_history().await.annotated_items(), history);
    let (mut failed_session, _) = make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut failed_session).await;
    failed_session
        .live_thread()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let error = failed_session
        .install_dsh_context(serde_json::from_value(serialized).unwrap())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to append DSH checkpoint")
    );
}

#[tokio::test]
async fn durable_import_seed_replaces_provenance_and_preserves_later_items() {
    use codex_history::CodexHarnessMetadata;
    use codex_history::ImportedContextV1;
    use codex_protocol::models::ContentItem;
    use codex_protocol::models::ResponseItem;

    let message = |text: &str| {
        ResponseItemEnvelope::new(ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        })
    };
    let (session, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let world = build_world_state_from_turn_context(&session, &turn).await;
    let mut imported = message("exact imported context");
    imported.metadata = Some(CodexHarnessMetadata {
        history_truncation_token_limit: Some(100),
        ..Default::default()
    });
    let mut imported_history: Vec<_> = world
        .render_full()
        .into_iter()
        .map(ContextualUserFragment::into_boxed_response_item)
        .map(ResponseItemEnvelope::new)
        .collect();
    imported_history.push(imported);
    let after = message("new turn after import");
    let mut retained_context = RetainedContext::default();
    retained_context.mark_user_messages_incomplete();
    let rollout = vec![
        RolloutItem::ResponseItem(message("provenance outside active context")),
        RolloutItem::WorldState(WorldStateItem::full(world.snapshot().into_object())),
        RolloutItem::TurnContext(turn.to_turn_context_item()),
        RolloutItem::DshImportedContextV1(Box::new(ImportedContextV1 {
            effective_history: imported_history.clone(),
            retained_context: retained_context.clone(),
            history_version: 52,
        })),
        RolloutItem::ResponseItem(after.clone()),
    ];
    let durable: Vec<RolloutItem> =
        serde_json::from_slice(&serde_json::to_vec(&rollout).unwrap()).unwrap();
    session.apply_rollout_reconstruction(&turn, &durable).await;
    let history = session.clone_history().await;
    imported_history.push(after);
    assert_eq!(history.annotated_items(), imported_history);
    assert_eq!(history.retained_context(), &retained_context);
    assert!(history.history_version() >= 52);
    assert_eq!(
        history.reference_context_item(),
        Some(turn.to_turn_context_item())
    );
    session
        .record_context_updates_and_set_reference_context_item(&StepContext::for_test(turn))
        .await
        .unwrap();
    assert_eq!(
        session.clone_history().await.annotated_items(),
        history.annotated_items()
    );
}
