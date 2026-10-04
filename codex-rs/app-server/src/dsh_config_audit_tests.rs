use super::turn_context_config;
use anyhow::Result;
use codex_core::config::ConfigBuilder;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_utils_path_uri::PathUri;
use pretty_assertions::assert_eq;
use serde_json::json;

#[tokio::test]
async fn records_prompt_provenance_without_provider_credentials() -> Result<()> {
    let home = tempfile::tempdir()?;
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .build()
        .await?;
    config.base_instructions = Some("base instruction".to_string());
    config.developer_instructions = Some("developer instruction".to_string());
    config.compact_prompt = Some("compact instruction".to_string());
    config.model_provider.experimental_bearer_token = Some("credential-must-not-leak".into());
    let baseline = ThreadSettingsSnapshot {
        model: "baseline-model".to_string(),
        model_provider_id: config.model_provider_id.clone(),
        service_tier: None,
        approval_policy: AskForApproval::OnRequest,
        approvals_reviewer: ApprovalsReviewer::User,
        permission_profile: PermissionProfile::read_only(),
        active_permission_profile: None,
        cwd: config.cwd.clone(),
        runtime_workspace_roots: Some(config.workspace_roots.clone()),
        reasoning_effort: config.model_reasoning_effort.clone(),
        reasoning_summary: None,
        personality: None,
        collaboration_mode: CollaborationMode {
            mode: ModeKind::Default,
            settings: Settings {
                model: "baseline-model".to_string(),
                reasoning_effort: None,
                developer_instructions: None,
            },
        },
        disabled_plugin_ids: Vec::new(),
    };
    let requested = ThreadSettingsOverrides {
        model: Some("selected-model".to_string()),
        ..Default::default()
    };
    let source = PathUri::parse("file:///workspace/AGENTS.md")?;
    let tools = [DynamicToolSpec::Function(DynamicToolFunctionSpec {
        name: "dsh_tool".to_string(),
        description: "Delegated tool".to_string(),
        input_schema: json!({ "type": "object" }),
        defer_loading: false,
    })];

    let recorded = turn_context_config(
        "session-1",
        &config,
        &baseline,
        &requested,
        &[source],
        Some(&tools),
        Some("fork-commit"),
    );
    assert_eq!(recorded["schemaVersion"], 1);
    assert_eq!(recorded["fork"]["revision"], "fork-commit");
    assert_eq!(recorded["fork"]["promptTemplateRevision"], "fork-commit");
    assert_eq!(recorded["threadBaseline"]["model"], "baseline-model");
    assert_eq!(recorded["turnOverrides"]["model"], "selected-model");
    assert_eq!(recorded["instructions"]["base"], "base instruction");
    assert_eq!(
        recorded["instructions"]["developer"],
        "developer instruction"
    );
    assert_eq!(
        recorded["instructions"]["compactPrompt"],
        "compact instruction"
    );
    assert_eq!(
        recorded["instructions"]["sources"],
        json!(["file:///workspace/AGENTS.md"])
    );
    assert_eq!(recorded["dynamicTools"][0]["name"], "dsh_tool");
    assert!(!recorded.to_string().contains("credential-must-not-leak"));
    Ok(())
}
