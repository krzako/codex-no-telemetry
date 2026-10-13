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

        self.restore_imported_context(codex_history::ImportedContextV1 {
            effective_history: snapshot.effective_history,
            retained_context: snapshot.retained_context,
            history_version: snapshot.history_version,
        });
        self.reference_context_item = None;
        self.world_state_baseline = None;
        Ok(())
    }

    /// Restore an explicit import boundary while replaying a durable rollout.
    pub(crate) fn restore_imported_context(&mut self, snapshot: codex_history::ImportedContextV1) {
        self.replace_annotated(snapshot.effective_history);
        self.retained_context = Arc::new(snapshot.retained_context);
        self.history_version = snapshot.history_version;
        self.reset_version = snapshot.history_version;
        self.user_message_revision = snapshot.history_version.saturating_add(1);
    }

    /// Keep the imported history generation as a floor across cold reconstruction.
    pub(crate) fn restore_imported_history_version(&mut self, version: u64) {
        self.history_version = self.history_version.max(version);
        self.reset_version = self.history_version;
        self.user_message_revision = self.user_message_revision.max(version.saturating_add(1));
    }
}
