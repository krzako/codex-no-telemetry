use super::*;
use codex_extension_api::ExtensionData;
use codex_extension_api::TurnItemContributor;
use codex_protocol::ResponseItemId;
use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::items::AgentMessageContent;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

struct RewriteAgentMessageContributor;

impl TurnItemContributor for RewriteAgentMessageContributor {
    fn contribute<'a>(
        &'a self,
        _thread_store: &'a ExtensionData,
        _turn_store: &'a ExtensionData,
        item: &'a mut TurnItem,
    ) -> codex_extension_api::ExtensionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if let TurnItem::AgentMessage(agent_message) = item {
                agent_message.content = vec![AgentMessageContent::Text {
                    text: "plan contributed assistant text".to_string(),
                }];
            }
            Ok(())
        })
    }
}

fn assistant_output_text(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: Some(ResponseItemId::with_suffix("msg", "1")),
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

#[tokio::test]
async fn every_client_exposes_only_host_supplied_tools() {
    for delegated_tools_only in [false, true] {
        for has_host_tool in [false, true] {
            let (session, mut turn_context) =
                crate::session::tests::make_session_and_context().await;
            turn_context.delegated_tools_only = delegated_tools_only;
            let config = Arc::make_mut(&mut turn_context.config);
            config.update_plan_enabled = true;
            config
                .features
                .enable(Feature::ViewImage)
                .expect("view image feature should be enableable");
            turn_context.dynamic_tools = has_host_tool
                .then(|| {
                    DynamicToolSpec::Function(DynamicToolFunctionSpec {
                        name: "dsh_lookup".to_string(),
                        description: "Look up data through DSH".to_string(),
                        input_schema: serde_json::json!({"type": "object", "properties": {}}),
                        defer_loading: true,
                    })
                })
                .into_iter()
                .collect();
            let turn_context = Arc::new(turn_context);
            let step_context = StepContext::for_test(Arc::clone(&turn_context));
            let router = built_tools(
                &session,
                turn_context.as_ref(),
                step_context.settings.model_info.as_ref(),
            )
            .expect("host-only tool plan should build");
            let expected = if has_host_tool {
                vec!["dsh_lookup"]
            } else {
                Vec::new()
            };
            assert_eq!(
                router
                    .model_visible_specs()
                    .iter()
                    .map(codex_tools::ToolSpec::name)
                    .collect::<Vec<_>>(),
                expected,
                "delegated_tools_only={delegated_tools_only}, has_host_tool={has_host_tool}",
            );
            assert_eq!(
                router.registered_tool_names_for_test().len(),
                expected.len()
            );
            assert_eq!(
                router.tool_mode(),
                codex_protocol::openai_models::ToolMode::Direct
            );
        }
    }
}

#[test]
fn post_sampling_token_estimate_is_disabled_by_always_on_sinks() {
    let feedback = codex_feedback::CodexFeedback::new();
    let subscriber = tracing_subscriber::registry()
        .with(feedback.logger_layer())
        .with(tracing_subscriber::fmt::layer().with_filter(codex_state::log_db::default_filter()));

    tracing::subscriber::with_default(subscriber, || {
        tracing::callsite::rebuild_interest_cache();
        assert!(!tracing::event_enabled!(
            target: POST_SAMPLING_TOKEN_ESTIMATE_TARGET,
            tracing::Level::TRACE,
            turn_id,
            estimated_token_count,
            message
        ));
    });
}

#[tokio::test]
async fn plan_mode_uses_contributed_turn_item_for_last_agent_message() {
    let (mut session, turn_context) = crate::session::tests::make_session_and_context().await;
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_item_contributor(Arc::new(RewriteAgentMessageContributor));
    session.services.extensions = Arc::new(builder.build());
    let turn_store = ExtensionData::new(turn_context.sub_id.clone());
    let mut state = PlanModeStreamState::new(&turn_context.sub_id);
    let mut last_agent_message = None;
    let item = assistant_output_text("original assistant text");

    let step_context = StepContext::for_test(Arc::new(turn_context));
    let handled = handle_assistant_item_done_in_plan_mode(
        &session,
        &step_context,
        &turn_store,
        &item,
        &mut state,
        /*previously_active_item*/ None,
        &mut last_agent_message,
    )
    .await;

    assert!(handled);
    assert_eq!(
        last_agent_message.as_deref(),
        Some("plan contributed assistant text")
    );
}

#[test]
fn realtime_user_verification_notice_excludes_request_payload() {
    let event = EventMsg::ElicitationRequest(codex_protocol::approvals::ElicitationRequestEvent {
        turn_id: None,
        server_name: "private-server-name".to_string(),
        id: codex_protocol::mcp::RequestId::String("private-request-id".to_string()),
        request: codex_protocol::approvals::ElicitationRequest::UserVerification {
            title: "private-title".to_string(),
            description: "private-description".to_string(),
            challenge: "private-challenge".to_string(),
        },
    });
    assert_eq!(
        realtime_text_for_event(&event),
        Some((
            "<user_verification_notice>User verification is required. Please respond in the app.</user_verification_notice>".to_string(),
            None,
        )),
    );
}
