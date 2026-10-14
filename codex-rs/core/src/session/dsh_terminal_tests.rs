use crate::dsh_audit::DshAuditBinding;
use crate::dsh_audit::DshAuditBindingRegistry;
use crate::session::tests::make_session_and_context;
use crate::session::tests::open_thread_persistence;
use codex_history::RolloutItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;

fn completed(turn_id: String) -> EventMsg {
    EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id,
        last_agent_message: Some("saved answer".to_string()),
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    })
}

#[tokio::test]
async fn terminal_delivery_has_a_readable_durable_record() {
    for aborted in [false, true] {
        let (mut session, turn) = make_session_and_context().await;
        let (sender, receiver) = async_channel::unbounded();
        session.tx_event = sender;
        let path = open_thread_persistence(&mut session).await;
        session
            .live_thread()
            .unwrap()
            .persist(codex_thread_store::PersistContext::Standard)
            .await
            .unwrap();
        turn.extension_data.insert(Arc::new(DshAuditBinding::new(
            "session".to_string(),
            1,
            |_| Box::pin(async { Ok(()) }),
        )));
        let event = if aborted {
            EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn.sub_id.clone()),
                reason: TurnAbortReason::Interrupted,
                started_at: None,
                completed_at: None,
                duration_ms: None,
            })
        } else {
            completed(turn.sub_id.clone())
        };
        let expected = event.clone();
        let (_, ()) = tokio::join!(session.send_event(&turn, event), async {
            let received = receiver.recv().await.unwrap();
            assert_eq!(
                serde_json::to_value(received.msg).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            let (items, _, errors) = codex_rollout::RolloutRecorder::load_rollout_items(&path)
                .await
                .unwrap();
            assert_eq!(errors, 0);
            let expected = serde_json::to_value(expected).unwrap();
            assert!(
                items
                    .iter()
                    .any(|item| matches!(item, RolloutItem::EventMsg(record)
                if serde_json::to_value(record).unwrap() == expected))
            );
            // Closing the writer as soon as the client receives completion must be safe.
            session.live_thread().unwrap().shutdown().await.unwrap();
        });
    }
}

#[tokio::test]
async fn failed_terminal_write_reports_failure_and_blocks_later_audit_bindings() {
    let (mut session, turn) = make_session_and_context().await;
    let (sender, receiver) = async_channel::unbounded();
    session.tx_event = sender;
    open_thread_persistence(&mut session).await;
    session.live_thread().unwrap().shutdown().await.unwrap();
    let binding = Arc::new(DshAuditBinding::new("session".to_string(), 1, |_| {
        Box::pin(async { Ok(()) })
    }));
    turn.extension_data.insert(Arc::clone(&binding));
    let registry = session
        .services
        .thread_extension_data
        .get_or_init(DshAuditBindingRegistry::default);
    registry
        .insert(DshAuditBinding::new("session".to_string(), 2, |_| {
            Box::pin(async { Ok(()) })
        }))
        .unwrap();
    session
        .send_event(&turn, completed(turn.sub_id.clone()))
        .await;
    let EventMsg::Error(error) = receiver.recv().await.unwrap().msg else {
        panic!("persistence failure must precede terminal notification");
    };
    assert!(error.message.contains("DSH terminal persistence failed"));
    let EventMsg::TurnComplete(completed) = receiver.recv().await.unwrap().msg else {
        panic!("expected failed terminal notification");
    };
    assert_eq!(completed.error, Some(error));
    assert!(
        binding
            .append(json!({"kind": "model/request"}))
            .await
            .is_err()
    );
    assert!(registry.take(2).is_none());
    assert!(
        registry
            .insert(DshAuditBinding::new(
                "session".to_string(),
                3,
                |_| Box::pin(async { Ok(()) }),
            ))
            .is_err()
    );
}
