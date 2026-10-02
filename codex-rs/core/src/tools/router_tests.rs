use std::collections::BTreeMap;
use std::sync::Arc;

use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::tools::context::ToolPayload;
use crate::tools::handlers::McpHandler;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::RegisteredTool;
use crate::tools::registry::ToolExposure;
use crate::tools::registry::ToolRegistry;
use crate::tools::spec_plan::append_dynamic_tool_runtimes;
use codex_protocol::DEFAULT_FUNCTION_NAMESPACE;
use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
use codex_protocol::dynamic_tools::DynamicToolNamespaceSpec;
use codex_protocol::dynamic_tools::DynamicToolNamespaceTool;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::models::ResponseItem;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::ToolCall;
use super::ToolCallSource;
use super::ToolRouter;
use super::tool_log_payload;

#[test]
fn tool_log_payload_redacts_plaintext_multi_agent_messages() {
    let payload = ToolPayload::Function {
        arguments: json!({"target": "/root/worker", "message": "secret message"}).to_string(),
    };
    assert_eq!(
        tool_log_payload(&payload, &ToolCallSource::DirectPlaintextMessage),
        "[plaintext arguments]"
    );
    assert_eq!(
        tool_log_payload(&payload, &ToolCallSource::Direct),
        payload.log_payload()
    );
}

fn test_tool_router(
    step_context: &StepContext,
    mcp_tools: Vec<RegisteredTool>,
    dynamic_tools: &[DynamicToolSpec],
) -> ToolRouter {
    let mut registry = ToolRegistry::default();
    for tool in mcp_tools {
        registry.register_external_with_exposure(tool.runtime, tool.exposure);
    }
    append_dynamic_tool_runtimes(dynamic_tools, &mut registry);
    ToolRouter::from_registry(
        step_context.turn.as_ref(),
        step_context.turn.model_info(),
        registry,
        Vec::new(),
        &Default::default(),
    )
}

#[tokio::test]
async fn dynamic_host_tool_does_not_register_native_or_namespaced_shadow() -> anyhow::Result<()> {
    let (_, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let step_context = StepContext::for_test(Arc::clone(&turn));
    let host_tool = DynamicToolSpec::Function(DynamicToolFunctionSpec {
        name: "exec_command".to_string(),
        description: "Host-owned shell".to_string(),
        input_schema: json!({"type": "object", "properties": {}}),
        defer_loading: false,
    });
    let router = test_tool_router(step_context.as_ref(), Vec::new(), &[host_tool]);

    assert_eq!(
        router.registered_tool_names_for_test(),
        vec![ToolName::plain("exec_command").with_default_namespace()]
    );
    assert!(
        router
            .tool_runtime(&ToolName::plain("exec_command"))
            .is_some()
    );
    assert!(
        router
            .tool_runtime(&ToolName::namespaced("mcp__server__", "exec_command"))
            .is_none()
    );

    assert!(!router.tool_supports_parallel(&ToolCall {
        tool_name: ToolName::namespaced("mcp__server__", "exec_command"),
        call_id: "call-namespaced-tool".to_string(),
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
        encrypted_function_args: None,
    }));

    Ok(())
}

#[tokio::test]
async fn build_tool_call_uses_namespace_for_registry_name() -> anyhow::Result<()> {
    let tool_name = "create_event".to_string();

    let call = ToolRouter::build_tool_call(ResponseItem::FunctionCall {
        id: None,
        name: tool_name.clone(),
        namespace: Some("mcp__codex_apps__calendar".to_string()),
        arguments: "{}".to_string(),
        encrypted_function_args: Some(Vec::new()),
        call_id: "call-namespace".to_string(),
        internal_chat_message_metadata_passthrough: None,
    })?
    .expect("function_call should produce a tool call");

    assert_eq!(
        call.tool_name,
        ToolName::namespaced("mcp__codex_apps__calendar", tool_name)
    );
    assert_eq!(call.call_id, "call-namespace");
    assert_eq!(call.encrypted_function_args, Some(Vec::new()));
    assert_eq!(call.direct_source(), ToolCallSource::Direct);
    match call.payload {
        ToolPayload::Function { arguments } => {
            assert_eq!(arguments, "{}");
        }
        other => panic!("expected function payload, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn build_custom_tool_call_uses_namespace_for_registry_name() -> anyhow::Result<()> {
    let tool_name = "exec".to_string();

    let call = ToolRouter::build_tool_call(ResponseItem::CustomToolCall {
        id: None,
        status: None,
        call_id: "call-namespace".to_string(),
        name: tool_name.clone(),
        namespace: Some("mcp__python".to_string()),
        input: "print('hello')".to_string(),
        internal_chat_message_metadata_passthrough: None,
    })?
    .expect("custom_tool_call should produce a tool call");

    assert_eq!(
        call,
        ToolCall {
            tool_name: ToolName::namespaced("mcp__python", tool_name),
            call_id: "call-namespace".to_string(),
            payload: ToolPayload::Custom {
                input: "print('hello')".to_string(),
            },
            encrypted_function_args: None,
        }
    );

    Ok(())
}

#[test]
fn build_tool_call_normalizes_default_function_and_custom_namespaces() -> anyhow::Result<()> {
    for namespace in [None, Some(""), Some(DEFAULT_FUNCTION_NAMESPACE)] {
        let function_call = ToolRouter::build_tool_call(ResponseItem::FunctionCall {
            id: None,
            name: "lookup".to_string(),
            namespace: namespace.map(str::to_string),
            arguments: "{}".to_string(),
            encrypted_function_args: None,
            call_id: "call-function".to_string(),
            internal_chat_message_metadata_passthrough: None,
        })?
        .expect("function_call should produce a tool call");
        let custom_call = ToolRouter::build_tool_call(ResponseItem::CustomToolCall {
            id: None,
            status: None,
            call_id: "call-custom".to_string(),
            name: "apply_patch".to_string(),
            namespace: namespace.map(str::to_string),
            input: "patch".to_string(),
            internal_chat_message_metadata_passthrough: None,
        })?
        .expect("custom_tool_call should produce a tool call");

        assert_eq!(
            [function_call.tool_name, custom_call.tool_name],
            [
                ToolName::namespaced(DEFAULT_FUNCTION_NAMESPACE, "lookup"),
                ToolName::namespaced(DEFAULT_FUNCTION_NAMESPACE, "apply_patch"),
            ]
        );
    }

    Ok(())
}

#[tokio::test]
async fn mcp_parallel_support_uses_handler_data() -> anyhow::Result<()> {
    let (_, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let step_context = StepContext::for_test(Arc::clone(&turn));
    let router = test_tool_router(
        step_context.as_ref(),
        vec![
            mcp_runtime(mcp_tool_info(
                "echo",
                /*supports_parallel_tool_calls*/ true,
                "mcp__echo__",
                "query_with_delay",
            )),
            RegisteredTool {
                exposure: ToolExposure::DirectModelOnly,
                ..mcp_runtime(mcp_tool_info(
                    "hello_echo",
                    /*supports_parallel_tool_calls*/ false,
                    "mcp__hello_echo__",
                    "query_with_delay",
                ))
            },
            RegisteredTool {
                exposure: ToolExposure::Hidden,
                ..mcp_runtime(mcp_tool_info(
                    "hidden_echo",
                    /*supports_parallel_tool_calls*/ true,
                    "mcp__hidden_echo__",
                    "query_with_delay",
                ))
            },
            RegisteredTool {
                exposure: ToolExposure::CodeModeOnly,
                ..mcp_runtime(mcp_tool_info(
                    "nested_echo",
                    /*supports_parallel_tool_calls*/ true,
                    "mcp__nested_echo__",
                    "query_with_delay",
                ))
            },
        ],
        &turn.dynamic_tools,
    );

    let call = ToolCall {
        tool_name: ToolName::namespaced("mcp__echo__", "query_with_delay"),
        call_id: "call-handler".to_string(),
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
        encrypted_function_args: None,
    };
    assert!(router.tool_supports_parallel(&call));
    assert_eq!(
        router
            .tool_runtime(&call.tool_name)
            .map(|runtime| runtime.tool_name()),
        Some(call.tool_name.clone())
    );

    let different_server_call = ToolCall {
        tool_name: ToolName::namespaced("mcp__hello_echo__", "query_with_delay"),
        call_id: "call-other-server".to_string(),
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
        encrypted_function_args: None,
    };
    assert!(!router.tool_supports_parallel(&different_server_call));
    assert_eq!(
        router
            .tool_runtime(&different_server_call.tool_name)
            .map(|runtime| runtime.tool_name()),
        Some(different_server_call.tool_name.clone())
    );

    let hidden_call = ToolCall {
        tool_name: ToolName::namespaced("mcp__hidden_echo__", "query_with_delay"),
        call_id: "call-hidden".to_string(),
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
        encrypted_function_args: None,
    };
    assert!(!router.tool_supports_parallel(&hidden_call));
    assert!(router.tool_runtime(&hidden_call.tool_name).is_some());

    let nested_only_call = ToolCall {
        tool_name: ToolName::namespaced("mcp__nested_echo__", "query_with_delay"),
        call_id: "call-nested-only-server".to_string(),
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
        encrypted_function_args: None,
    };
    assert!(router.tool_supports_parallel(&nested_only_call));

    Ok(())
}

#[tokio::test]
async fn tools_without_handlers_do_not_support_parallel() -> anyhow::Result<()> {
    let (_, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let step_context = StepContext::for_test(Arc::clone(&turn));
    let router = test_tool_router(step_context.as_ref(), Vec::new(), &turn.dynamic_tools);

    assert!(!router.tool_supports_parallel(&ToolCall {
        tool_name: ToolName::plain("web_search"),
        call_id: "call-web-search".to_string(),
        payload: ToolPayload::Function {
            arguments: "{}".to_string(),
        },
        encrypted_function_args: None,
    }));

    Ok(())
}

#[tokio::test]
async fn specs_filter_deferred_dynamic_tools() -> anyhow::Result<()> {
    let (_, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let step_context = StepContext::for_test(Arc::clone(&turn));
    let hidden_tool = "hidden_dynamic_tool";
    let visible_tool = "visible_dynamic_tool";
    let dynamic_tools = vec![DynamicToolSpec::Namespace(DynamicToolNamespaceSpec {
        name: "codex_app".to_string(),
        description: "Codex app tools.".to_string(),
        tools: vec![
            DynamicToolNamespaceTool::Function(DynamicToolFunctionSpec {
                name: hidden_tool.to_string(),
                description: "Hidden until discovered.".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                }),
                defer_loading: true,
            }),
            DynamicToolNamespaceTool::Function(DynamicToolFunctionSpec {
                name: visible_tool.to_string(),
                description: "Visible immediately.".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                }),
                defer_loading: false,
            }),
        ],
    })];

    let router = test_tool_router(step_context.as_ref(), Vec::new(), &dynamic_tools);
    let visible_specs = router.model_visible_specs();

    assert!(Arc::ptr_eq(&visible_specs, &router.model_visible_specs()));
    assert_eq!(
        namespace_function_names(&visible_specs, "codex_app"),
        vec![visible_tool.to_string()]
    );
    assert_eq!(router.deferred_tool_namespaces(), BTreeMap::new());
    assert_eq!(
        router.registered_tool_names_for_test(),
        vec![ToolName::namespaced("codex_app", visible_tool)]
    );
    assert!(
        router
            .tool_runtime(&ToolName::namespaced("codex_app", hidden_tool))
            .is_none()
    );

    let mut direct_catalog = dynamic_tools.clone();
    let DynamicToolSpec::Namespace(namespace) = &mut direct_catalog[0] else {
        unreachable!("test catalog is namespaced")
    };
    let DynamicToolNamespaceTool::Function(hidden) = &mut namespace.tools[0];
    hidden.defer_loading = false;
    let direct_router = test_tool_router(step_context.as_ref(), Vec::new(), &direct_catalog);
    assert_eq!(
        namespace_function_names(&direct_router.model_visible_specs(), "codex_app"),
        vec![hidden_tool.to_string(), visible_tool.to_string()]
    );

    let updated_router = test_tool_router(step_context.as_ref(), Vec::new(), &[]);
    let updated_specs = updated_router.model_visible_specs();
    assert!(!Arc::ptr_eq(&visible_specs, &updated_specs));
    assert!(namespace_function_names(&updated_specs, "codex_app").is_empty());

    Ok(())
}

fn mcp_tool_info(
    server_name: &str,
    supports_parallel_tool_calls: bool,
    callable_namespace: &str,
    tool_name: &str,
) -> codex_mcp::ToolInfo {
    codex_mcp::ToolInfo {
        server_name: server_name.to_string(),
        supports_parallel_tool_calls,
        server_origin: None,
        callable_name: tool_name.to_string(),
        callable_namespace: callable_namespace.to_string(),
        namespace_description: None,
        tool: rmcp::model::Tool::new(
            tool_name.to_string(),
            "Test MCP tool",
            Arc::new(rmcp::model::object(json!({
                "type": "object",
            }))),
        ),
        openai_file_input_optional_fields: Default::default(),
        connector_id: None,
        connector_name: None,
        plugin_display_names: Vec::new(),
    }
}

fn mcp_runtime(tool_info: codex_mcp::ToolInfo) -> RegisteredTool {
    let runtime = Arc::new(McpHandler::new(tool_info).expect("MCP tool spec should build"))
        as Arc<dyn CoreToolRuntime>;
    RegisteredTool {
        exposure: runtime.exposure(),
        runtime,
    }
}

fn namespace_function_names(specs: &[ToolSpec], namespace_name: &str) -> Vec<String> {
    specs
        .iter()
        .find_map(|spec| match spec {
            ToolSpec::Namespace(namespace) if namespace.name == namespace_name => Some(
                namespace
                    .tools
                    .iter()
                    .map(|tool| match tool {
                        ResponsesApiNamespaceTool::Function(tool) => tool.name.clone(),
                        ResponsesApiNamespaceTool::Custom(tool) => tool.name.clone(),
                    })
                    .collect(),
            ),
            ToolSpec::Function(_)
            | ToolSpec::Freeform(_)
            | ToolSpec::ToolSearch { .. }
            | ToolSpec::WebSearch { .. }
            | ToolSpec::Namespace(_) => None,
        })
        .unwrap_or_default()
}
