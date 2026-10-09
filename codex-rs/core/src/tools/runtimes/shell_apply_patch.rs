//! Internal shell command interception for apply_patch.
//!
//! This is not a model-facing tool; it preserves the shell runtime path that
//! recognizes the apply_patch command and applies it through DSH's runtime.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::apply_patch;
use crate::apply_patch::convert_apply_patch_to_protocol;
use crate::function_tool::FunctionCallError;
use crate::safety::PatchSandboxRoute;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use crate::session::turn_context::TurnContext;
use crate::session::turn_context::TurnEnvironment;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::SharedTurnDiffTracker;
use crate::tools::events::ToolEmitter;
use crate::tools::events::ToolEventCtx;
use crate::tools::handlers::apply_granted_turn_permissions;
use crate::tools::handlers::file_system_sandbox_policy_context_for_cwd;
use crate::tools::orchestrator::ToolOrchestrator;
use crate::tools::runtimes::apply_patch::ApplyPatchRequest;
use crate::tools::runtimes::apply_patch::ApplyPatchRuntime;
use crate::tools::sandboxing::ToolCtx;
use crate::windows_sandbox::windows_sandbox_level_for_legacy_checks;
use codex_apply_patch::ApplyPatchAction;
use codex_apply_patch::ApplyPatchFileChange;
use codex_apply_patch::ApplyPatchFileUpdateMode;
use codex_exec_server::ExecutorFileSystem;
use codex_features::Feature;
use codex_protocol::models::AdditionalPermissionProfile;
use codex_protocol::models::FileSystemPermissions;
use codex_protocol::permissions::FileSystemSandboxPolicyContext;
use codex_sandboxing::policy_transforms::effective_file_system_sandbox_policy;
use codex_sandboxing::policy_transforms::merge_permission_profiles;
use codex_sandboxing::policy_transforms::normalize_additional_permissions;
use codex_sandboxing::policy_transforms::normalize_additional_permissions_with_context;
use codex_tools::ToolName;
use codex_utils_path_uri::PathUri;

fn apply_patch_file_update_mode(turn: &TurnContext) -> ApplyPatchFileUpdateMode {
    if turn
        .config
        .features
        .enabled(Feature::ApplyPatchPreserveLineEndings)
    {
        ApplyPatchFileUpdateMode::PreserveLineEndings
    } else {
        ApplyPatchFileUpdateMode::NormalizeToLf
    }
}

fn file_paths_for_action(action: &ApplyPatchAction) -> Vec<PathUri> {
    let mut keys = Vec::new();
    for (path, change) in action.changes() {
        keys.push(path.clone());

        if let ApplyPatchFileChange::Update { move_path, .. } = change
            && let Some(dest) = move_path
        {
            keys.push(dest.clone());
        }
    }

    keys
}

fn write_permissions_for_paths(
    file_paths: &[PathUri],
    file_system_sandbox_policy: &codex_protocol::permissions::FileSystemSandboxPolicy,
    context: &FileSystemSandboxPolicyContext<'_>,
    sandbox_route: PatchSandboxRoute,
) -> Option<AdditionalPermissionProfile> {
    let mut write_paths = file_paths
        .iter()
        // Skip already-writable targets before deriving parent permissions.
        // Otherwise, a writable directory could grant access to its parent.
        .filter(|path| !file_system_sandbox_policy.can_write_path(path, context))
        .map(|path| {
            path.parent()
                .or_else(|| match sandbox_route {
                    PatchSandboxRoute::Platform(_) => {
                        // Host path rules can recover parents of opaque local paths.
                        // `to_abs_path` verifies that the target round-trips losslessly.
                        path.to_abs_path().ok()?.parent().map(PathUri::from)
                    }
                    PatchSandboxRoute::ExecutorManaged => None,
                })
                .unwrap_or_else(|| path.clone())
        })
        .filter(|path| !file_system_sandbox_policy.can_write_path(path, context))
        .collect::<Vec<_>>();
    write_paths.sort_by_key(PathUri::to_string);
    write_paths.dedup();

    let permissions = (!write_paths.is_empty()).then_some(AdditionalPermissionProfile {
        file_system: Some(FileSystemPermissions::from_read_write_path_uris(
            Some(vec![]),
            Some(write_paths),
        )),
        ..Default::default()
    })?;

    match sandbox_route {
        PatchSandboxRoute::Platform(_) => normalize_additional_permissions(permissions).ok(),
        PatchSandboxRoute::ExecutorManaged => {
            normalize_additional_permissions_with_context(permissions, context).ok()
        }
    }
}

async fn effective_patch_permissions(
    session: &Session,
    environment: &TurnEnvironment,
    action: &ApplyPatchAction,
    context: &FileSystemSandboxPolicyContext<'_>,
    sandbox_route: PatchSandboxRoute,
) -> (
    Vec<PathUri>,
    crate::tools::handlers::EffectiveAdditionalPermissions,
    codex_protocol::permissions::FileSystemSandboxPolicy,
) {
    let environment_id = environment.selection.environment_id.as_str();
    let file_paths = file_paths_for_action(action);
    let granted_permissions = merge_permission_profiles(
        session
            .granted_session_permissions(environment_id)
            .await
            .as_ref(),
        session
            .granted_turn_permissions(environment_id)
            .await
            .as_ref(),
    );
    let base_file_system_sandbox_policy = environment
        .permission_profile()
        .file_system_sandbox_policy();
    let file_system_sandbox_policy = effective_file_system_sandbox_policy(
        &base_file_system_sandbox_policy,
        granted_permissions.as_ref(),
    );
    let effective_additional_permissions = apply_granted_turn_permissions(
        session,
        environment,
        context.cwd,
        crate::sandboxing::SandboxPermissions::UseDefault,
        write_permissions_for_paths(
            &file_paths,
            &file_system_sandbox_policy,
            context,
            sandbox_route,
        ),
    )
    .await;

    (
        file_paths,
        effective_additional_permissions,
        file_system_sandbox_policy,
    )
}

pub(crate) async fn intercept_apply_patch(
    command: &[String],
    cwd: &PathUri,
    fs: &dyn ExecutorFileSystem,
    turn_environment: TurnEnvironment,
    session: Arc<Session>,
    step_context: Arc<StepContext>,
    cancellation_token: CancellationToken,
    tracker: Option<&SharedTurnDiffTracker>,
    call_id: &str,
    tool_name: &str,
) -> Result<Option<FunctionToolOutput>, FunctionCallError> {
    let turn = &step_context.turn;
    let sandbox = turn_environment.sandbox_context(/*additional_permissions*/ None);
    match codex_apply_patch::maybe_parse_apply_patch_verified_with_mode(
        command,
        cwd,
        apply_patch_file_update_mode(turn),
        fs,
        Some(&sandbox),
    )
    .await
    {
        codex_apply_patch::MaybeApplyPatchVerified::Body(changes) => {
            let tool_ctx = ToolCtx {
                session,
                step_context,
                cancellation_token,
                call_id: call_id.to_string(),
                tool_name: ToolName::plain(tool_name),
            };
            let content =
                execute_verified_patch(changes, turn_environment, tracker, tool_ctx).await?;
            Ok(Some(FunctionToolOutput::from_text(content, Some(true))))
        }
        codex_apply_patch::MaybeApplyPatchVerified::CorrectnessError(parse_error) => {
            Err(FunctionCallError::RespondToModel(format!(
                "apply_patch verification failed: {parse_error}"
            )))
        }
        codex_apply_patch::MaybeApplyPatchVerified::ShellParseError(error) => {
            tracing::trace!("Failed to parse apply_patch input, {error:?}");
            Ok(None)
        }
        codex_apply_patch::MaybeApplyPatchVerified::NotApplyPatch => Ok(None),
    }
}

async fn execute_verified_patch(
    action: ApplyPatchAction,
    turn_environment: TurnEnvironment,
    tracker: Option<&SharedTurnDiffTracker>,
    tool_ctx: ToolCtx,
) -> Result<String, FunctionCallError> {
    let cwd = action.cwd.clone();
    let sandbox_context = turn_environment.sandbox_context(/*additional_permissions*/ None);
    let policy_context = file_system_sandbox_policy_context_for_cwd(&sandbox_context, &cwd);
    let sandbox_route = if turn_environment.environment.is_remote() {
        PatchSandboxRoute::ExecutorManaged
    } else {
        PatchSandboxRoute::Platform(windows_sandbox_level_for_legacy_checks(
            turn_environment.config().windows_sandbox_type,
            turn_environment.config().windows_sandbox_level,
        ))
    };
    let (file_paths, effective_additional_permissions, file_system_sandbox_policy) =
        effective_patch_permissions(
            tool_ctx.session.as_ref(),
            &turn_environment,
            &action,
            &policy_context,
            sandbox_route,
        )
        .await;
    let apply = apply_patch::prepare_apply_patch(
        &tool_ctx.step_context,
        &turn_environment,
        &file_system_sandbox_policy,
        &policy_context,
        sandbox_route,
        action,
    )?;
    let changes = convert_apply_patch_to_protocol(&apply.action);
    let emitter = ToolEmitter::apply_patch_for_environment(
        changes.clone(),
        apply.auto_approved,
        turn_environment.selection.environment_id.clone(),
    );
    let event_ctx = ToolEventCtx::new(
        tool_ctx.session.as_ref(),
        tool_ctx.step_context.turn.as_ref(),
        &tool_ctx.step_context.settings.model_info,
        &tool_ctx.call_id,
        tracker,
    );
    emitter.begin(event_ctx).await;

    let request = ApplyPatchRequest {
        turn_environment,
        action: apply.action,
        file_paths,
        changes: Arc::new(changes),
        exec_approval_requirement: apply.exec_approval_requirement,
        additional_permissions: effective_additional_permissions.additional_permissions,
        permissions_preapproved: effective_additional_permissions.permissions_preapproved,
    };
    let mut orchestrator = ToolOrchestrator::new();
    let mut runtime = ApplyPatchRuntime::new();
    let result = orchestrator
        .run(&mut runtime, &request, &tool_ctx)
        .await
        .map(|result| result.output);
    let (result, delta) = match result {
        Ok(output) => (Ok(output.exec_output), Some(output.delta)),
        Err(error) => (Err(error), Some(runtime.committed_delta().clone())),
    };
    let event_ctx = ToolEventCtx::new(
        tool_ctx.session.as_ref(),
        tool_ctx.step_context.turn.as_ref(),
        &tool_ctx.step_context.settings.model_info,
        &tool_ctx.call_id,
        tracker,
    );
    emitter.finish(event_ctx, result, delta.as_ref()).await
}
