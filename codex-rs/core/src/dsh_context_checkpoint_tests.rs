use super::*;
use crate::DshContextSource;
use codex_history::RetainedContext;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::sync::Arc;
use std::sync::Mutex;

fn snapshot() -> DshContextSnapshotV2 {
    DshContextSource {
        effective_history: Vec::new(),
        retained_context: RetainedContext::default(),
        history_version: 0,
        rollout: Vec::new(),
        rollout_sha256: format!("{:x}", Sha256::digest(b"[]")),
    }
    .into_portable_snapshot(json!({
        "schemaVersion": 1,
        "fork": { "packageVersion": "test" },
    }))
    .expect("snapshot")
}

#[tokio::test]
async fn checkpoint_fact_preserves_snapshot_and_waits_for_ack() {
    let facts = Arc::new(Mutex::new(Vec::<Value>::new()));
    let recorded = Arc::clone(&facts);
    let binding = DshAuditBinding::new("session-a".to_string(), 4, move |fact| {
        let recorded = Arc::clone(&recorded);
        Box::pin(async move {
            recorded.lock().expect("facts lock").push(fact);
            Ok(())
        })
    });
    let turn_checkpoint = DshCheckpointTrigger::Turn.checkpoint_id("turn-4");
    let compaction_checkpoint =
        DshCheckpointTrigger::Compaction("compact-1").checkpoint_id("turn-4");
    record_checkpoint_fact(&binding, "turn-4", &turn_checkpoint, snapshot())
        .await
        .expect("durable ACK");
    record_checkpoint_fact(&binding, "turn-4", &compaction_checkpoint, snapshot())
        .await
        .expect("compaction ACK");
    let facts = facts.lock().expect("facts lock");
    assert_eq!(facts.len(), 2);
    assert_eq!(facts[0]["eventId"], "dsh-context-checkpoint-turn:turn-4");
    assert_eq!(facts[0]["checkpointId"], "turn:turn-4");
    assert_eq!(facts[0]["snapshot"]["kind"], "inline");
    assert_eq!(facts[0]["snapshot"]["value"]["version"], 2);
    assert_eq!(
        facts[0]["effectiveHistorySha256"],
        facts[0]["snapshot"]["value"]["effectiveHistorySha256"]
    );
    assert_eq!(facts[1]["checkpointId"], "compaction:compact-1");
    assert_eq!(facts[1]["codexTurnId"], "turn-4");
}

#[tokio::test]
async fn failed_ack_rejects_checkpoint_fact() {
    let binding = DshAuditBinding::new("session-a".to_string(), 4, |_| {
        Box::pin(async { anyhow::bail!("storage unavailable") })
    });
    let error = record_checkpoint_fact(&binding, "turn-4", "compaction:compact-1", snapshot())
        .await
        .expect_err("missing ACK");
    assert!(error.to_string().contains("checkpoint audit failed"));
}

#[tokio::test]
async fn missing_durable_source_fails_before_append() {
    let (session, turn_context) = crate::session::tests::make_session_and_context().await;
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = Arc::clone(&called);
    turn_context
        .extension_data
        .insert(Arc::new(DshAuditBinding::with_context_config(
            "session-a".to_string(),
            4,
            json!({ "schemaVersion": 1, "fork": { "packageVersion": "test" } }),
            move |_| {
                callback_called.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            },
        )));
    let error = session
        .dsh_record_context_checkpoint(&turn_context, DshCheckpointTrigger::Turn)
        .await
        .expect_err("no durable rollout");
    assert!(error.to_string().contains("durable context"));
    assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
}
