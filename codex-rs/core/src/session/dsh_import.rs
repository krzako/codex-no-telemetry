//! Restore prompt baselines from checkpoint provenance without executing historical actions.

use super::session::Session;
use crate::DshContextSnapshotV2;
use crate::context_manager::is_user_turn_boundary;
use anyhow::Result;

impl Session {
    pub(super) async fn install_dsh_context(&self, snapshot: DshContextSnapshotV2) -> Result<()> {
        snapshot.validate_for_import()?;
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
        Ok(())
    }
}

#[cfg(test)]
#[path = "dsh_import_tests.rs"]
mod tests;
