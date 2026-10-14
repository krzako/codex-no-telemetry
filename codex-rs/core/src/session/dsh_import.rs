//! Install and durably record an imported context without executing historical actions.

use super::session::Session;
use crate::DshContextSnapshotV2;
use crate::context_manager::is_user_turn_boundary;
use anyhow::Context;
use anyhow::Result;
use codex_history::ImportedContextV1;
use codex_history::RolloutItem;
use codex_thread_store::PersistContext;

impl Session {
    pub(super) async fn install_dsh_context(&self, snapshot: DshContextSnapshotV2) -> Result<()> {
        snapshot.validate_for_import()?;
        let live_thread = self.live_thread_for_persistence("import a DSH checkpoint")?;
        let _settings_guard = super::thread_settings::acquire_persistence_lock(self).await;
        let mut durable_items = snapshot.rollout.clone();
        durable_items.push(RolloutItem::DshImportedContextV1(Box::new(
            ImportedContextV1 {
                effective_history: snapshot.effective_history.clone(),
                retained_context: snapshot.retained_context.clone(),
                history_version: snapshot.history_version,
            },
        )));
        let turn_context = self.new_inject_items_context().await;
        let reconstructed = self
            .reconstruct_history_from_rollout(&turn_context, &snapshot.rollout)
            .await;
        let has_prior_user_turns = snapshot
            .effective_history
            .iter()
            .any(|item| is_user_turn_boundary(&item.item));
        let mut state = self.state.lock().await;
        state.history.install_dsh_snapshot(snapshot)?;
        state
            .history
            .set_reference_context_item(reconstructed.reference_context_item);
        if let Some(baseline) = reconstructed.world_state_baseline {
            state.history.set_world_state_baseline(baseline);
        }
        state.set_previous_turn_settings(reconstructed.previous_turn_settings);
        state.set_next_turn_is_first(!has_prior_user_turns);
        drop(state);
        // Historical settings remain provenance; the new thread keeps its resolved settings.
        durable_items.push(RolloutItem::EventMsg(
            super::thread_settings::applied_event(self).await,
        ));
        live_thread
            .append_items(&durable_items)
            .await
            .context("failed to append DSH checkpoint provenance and active context")?;
        live_thread
            .persist(PersistContext::Standard)
            .await
            .context("failed to materialize imported DSH checkpoint")?;
        live_thread
            .flush()
            .await
            .context("failed to flush imported DSH checkpoint")?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "dsh_import_tests.rs"]
mod tests;
