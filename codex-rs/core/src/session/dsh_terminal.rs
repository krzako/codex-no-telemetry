//! Persist terminal DSH events before the client can close the process or start another turn.

use super::session::Session;
use super::turn_context::TurnContext;
use crate::dsh_audit::DshAuditBindingRegistry;
use anyhow::Context;
use codex_history::RolloutItem;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;

impl Session {
    pub(super) async fn persist_dsh_terminal_event(
        &self,
        turn_context: &TurnContext,
        event: &mut EventMsg,
    ) {
        let result: anyhow::Result<()> = async {
            let writer = self.live_thread_for_persistence("complete a DSH turn")?;
            writer
                .append_items(&[RolloutItem::EventMsg(event.clone())])
                .await
                .context("append terminal event")?;
            writer.flush().await.context("flush terminal event")?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            if let Some(binding) = turn_context.dsh_audit_binding() {
                binding.fail();
            }
            self.services
                .thread_extension_data
                .get_or_init(DshAuditBindingRegistry::default)
                .fail();
            let error = ErrorEvent {
                misalignment: None,
                message: format!(
                    "DSH terminal persistence failed; further turns are blocked: {error:#}"
                ),
                codex_error_info: Some(CodexErrorInfo::Other),
            };
            turn_context
                .terminal_error
                .lock()
                .await
                .replace(error.clone());
            if let EventMsg::TurnComplete(completed) = event {
                completed.error = Some(error.clone());
            }
            // Storage failed: deliver the failure without recursively trying to persist it.
            self.deliver_event_raw(Event {
                id: turn_context.sub_id.clone(),
                msg: EventMsg::Error(error),
            })
            .await;
        }
    }
}

#[cfg(test)]
#[path = "dsh_terminal_tests.rs"]
mod tests;
