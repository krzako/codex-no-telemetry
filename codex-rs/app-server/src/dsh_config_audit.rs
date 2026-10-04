//! Credential-free provenance for the DSH turn configuration fact.

use codex_config::CONFIG_TOML_FILE;
use codex_config::format_config_layer_source;
use codex_core::config::Config;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_utils_path_uri::PathUri;
use serde_json::Value;
use serde_json::json;

/// Records the thread baseline and requested overrides separately; the model request is
/// authoritative for the effective prompt, while DSH's gateway records the actual API route.
pub(crate) fn turn_context_config(
    session_id: &str,
    config: &Config,
    baseline: &ThreadSettingsSnapshot,
    requested: &ThreadSettingsOverrides,
    instruction_sources: &[PathUri],
    dynamic_tools: Option<&[DynamicToolSpec]>,
    fork_revision: Option<&str>,
) -> Value {
    let config_sources = config
        .config_layer_stack
        .layers_low_to_high()
        .map(|layer| format_config_layer_source(&layer.name, CONFIG_TOML_FILE))
        .collect::<Vec<_>>();
    let environments = requested.environments.as_ref().map(|selections| {
        json!({
            "legacyFallbackCwd": selections.legacy_fallback_cwd,
            "environments": selections.environments.iter().map(|environment| json!({
                "environmentId": environment.environment_id,
                "cwd": environment.cwd,
                "workspaceRoots": environment.workspace_roots,
            })).collect::<Vec<_>>(),
        })
    });
    json!({
        "schemaVersion": 1,
        "sessionId": session_id,
        "fork": {
            "revision": fork_revision,
            "packageVersion": env!("CARGO_PKG_VERSION"),
            "promptTemplateRevision": fork_revision,
        },
        "threadBaseline": baseline,
        "turnOverrides": {
            "model": requested.model,
            "reasoningEffort": requested.effort,
            "reasoningSummary": requested.summary,
            "environments": environments,
            "runtimeWorkspaceRoots": requested.runtime_workspace_roots,
        },
        "instructions": {
            "base": config.base_instructions,
            "baseProvenance": config.base_instructions_provenance,
            "developer": config.developer_instructions,
            "compactPrompt": config.compact_prompt,
            "sources": instruction_sources,
            "projectDocMaxBytes": config.project_doc_max_bytes,
            "projectDocFallbackFilenames": config.project_doc_fallback_filenames,
            "includePermissionsInstructions": config.include_permissions_instructions,
            "includeAppsInstructions": config.include_apps_instructions,
            "includeCollaborationModeInstructions": config.include_collaboration_mode_instructions,
            "includeSkillInstructions": config.include_skill_instructions,
            "includeEnvironmentContext": config.include_environment_context,
        },
        "configSources": config_sources,
        "dynamicTools": dynamic_tools,
    })
}

#[cfg(test)]
#[path = "dsh_config_audit_tests.rs"]
mod tests;
