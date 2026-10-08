//! Installs an audited DSH checkpoint into a fresh model context without replaying its rollout.

use super::ContextManager;
use crate::DshContextSnapshotV2;
use anyhow::Result;
use anyhow::ensure;
use codex_history::RetainedContext;
use std::sync::Arc;

impl ContextManager {
    /// Seed only the active model window and retained facts; historical rollout is provenance.
    pub(crate) fn install_dsh_snapshot(&mut self, snapshot: DshContextSnapshotV2) -> Result<()> {
        ensure!(
            self.items.is_empty()
                && self.history_version == 0
                && self.reset_version == 0
                && self.user_message_revision == 0
                && self.retained_context.as_ref() == &RetainedContext::default()
                && self.review_history.is_none(),
            "DSH Codex checkpoint can only be installed into a fresh context"
        );
        snapshot.validate_for_import()?;

        self.items = Arc::new(snapshot.effective_history);
        self.retained_context = Arc::new(snapshot.retained_context);
        self.history_version = snapshot.history_version;
        self.reset_version = snapshot.history_version;
        // A new thread must not reuse any reviewer/cache generation from the source.
        self.user_message_revision = snapshot.history_version.saturating_add(1);
        self.reference_context_item = None;
        self.world_state_baseline = None;
        Ok(())
    }
}
