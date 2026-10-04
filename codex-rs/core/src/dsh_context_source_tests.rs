use super::*;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_utils_output_truncation::TruncationPolicy;
use pretty_assertions::assert_eq;

fn assistant_message(text: &str) -> ResponseItemEnvelope {
    ResponseItemEnvelope::new(ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    })
}

#[test]
fn captures_effective_history_and_exact_durable_rollout() {
    let mut history = ContextManager::new();
    let item = assistant_message("checkpoint source");
    history.record_annotated_items(
        std::slice::from_ref(&item),
        TruncationPolicy::Tokens(10_000),
    );
    let rollout = vec![RolloutItem::ResponseItem(item.clone())];
    let source = capture_source(history.clone(), history, rollout.clone()).expect("capture");
    assert_eq!(source.effective_history, vec![item]);
    assert_eq!(
        serde_json::to_value(&source.rollout).expect("source rollout"),
        serde_json::to_value(&rollout).expect("expected rollout")
    );
    assert_eq!(
        source.rollout_sha256,
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&rollout).expect("serialize"))
        )
    );
}

#[test]
fn rejects_changed_live_history_during_flush_and_read() {
    let before = ContextManager::new();
    let mut after = before.clone();
    after.record_annotated_items(
        &[assistant_message("changed")],
        TruncationPolicy::Tokens(10_000),
    );
    assert!(capture_source(before, after, Vec::new()).is_err());
}

#[tokio::test]
async fn refuses_checkpoint_source_without_durable_thread_store() {
    let (session, _) = crate::session::tests::make_session_and_context().await;
    let error = session.dsh_context_source().await.expect_err("no store");
    assert!(error.to_string().contains("persistence is disabled"));
}
