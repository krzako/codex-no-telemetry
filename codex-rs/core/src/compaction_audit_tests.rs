use super::*;
use codex_analytics::CompactionImplementation;
use codex_analytics::CompactionPhase;
use codex_analytics::CompactionReason;
use codex_analytics::CompactionTrigger;
use pretty_assertions::assert_eq;
use std::sync::Mutex;

fn metadata() -> CompactionTurnMetadata {
    CompactionTurnMetadata::new(
        CompactionTrigger::Manual,
        CompactionReason::UserRequested,
        CompactionImplementation::Responses,
        CompactionPhase::StandaloneTurn,
    )
}

#[tokio::test]
async fn request_records_full_history_and_prompt_before_returning() -> anyhow::Result<()> {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let facts = Arc::clone(&recorded);
    let binding = Arc::new(DshAuditBinding::new("session-a".into(), 7, move |fact| {
        let facts = Arc::clone(&facts);
        Box::pin(async move {
            facts.lock().expect("audit facts lock").push(fact);
            Ok(())
        })
    }));
    let prompt = Prompt::default();
    let history = vec![ResponseItem::CompactionTrigger {}];

    record_request(
        Some(binding),
        "compaction-1",
        metadata(),
        &history,
        &prompt,
        "model-a",
    )
    .await?;

    let facts = recorded.lock().expect("audit facts lock");
    assert_eq!(facts.len(), 1);
    let event_id = facts[0]["eventId"].clone();
    assert_eq!(
        facts[0],
        json!({
            "eventId": event_id,
            "turn": 7,
            "kind": "compaction/request",
            "compactionId": "compaction-1",
            "reason": "user_requested",
            "inputHistory": { "kind": "inline", "value": history },
            "prompt": { "kind": "inline", "value": {
                "model": "model-a",
                "baseInstructions": prompt.base_instructions,
                "input": prompt.input,
                "tools": [],
                "parallelToolCalls": false,
                "outputSchema": null,
                "outputSchemaStrict": true,
                "cyberAccessProgram": null,
                "compactionMetadata": metadata(),
            } },
        })
    );
    Ok(())
}

#[tokio::test]
async fn request_fails_before_model_work_without_durable_ack() {
    let binding = Arc::new(DshAuditBinding::new("session-a".into(), 7, |_fact| {
        Box::pin(async { anyhow::bail!("storage unavailable") })
    }));
    let error = record_request(
        Some(binding),
        "compaction-1",
        metadata(),
        &[],
        &Prompt::default(),
        "model-a",
    )
    .await
    .expect_err("missing ACK must block compaction");
    assert!(error.to_string().contains("storage unavailable"));
}
