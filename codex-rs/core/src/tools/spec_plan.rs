use crate::agent::exceeds_thread_spawn_depth_limit;
use crate::agent::next_thread_spawn_depth;
use crate::image_preparation::unified_image_budget_enabled;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::tools::code_mode::execute_spec::create_code_mode_tool;
use crate::tools::effective_tool_mode;
use crate::tools::handlers::CodeModeExecuteHandler;
use crate::tools::handlers::CodeModeWaitHandler;
use crate::tools::handlers::DynamicToolHandler;
use crate::tools::handlers::multi_agents_spec::MULTI_AGENT_V1_NAMESPACE;
use crate::tools::registry::ToolExposure;
use crate::tools::registry::ToolRegistry;
use crate::tools::router::ToolRouter;
use crate::tools::tool_namespaces_info::collect_tool_namespaces_info;
use codex_features::Feature;
use codex_protocol::DEFAULT_FUNCTION_NAMESPACE;
use codex_protocol::dynamic_tools::DynamicToolNamespaceTool;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ToolMode;
use codex_protocol::protocol::MultiAgentVersion;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::TOOL_SEARCH_TOOL_NAME;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_tools::collect_code_mode_exec_prompt_tool_definitions;
use codex_tools::default_namespace_description;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use tracing::instrument;

#[instrument(level = "trace", skip_all)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_tool_router(
    session: &Session,
    turn_context: &TurnContext,
    model_info: &ModelInfo,
) -> CodexResult<ToolRouter> {
    let mut registry = ToolRegistry::with_allowed_tools(session.allowed_tools.clone());
    append_dynamic_tool_runtimes(&turn_context.dynamic_tools, &mut registry);
    finalize_tool_router(turn_context, model_info, registry, Vec::new())
}

#[instrument(level = "trace", skip_all)]
pub(crate) fn finalize_tool_router(
    turn_context: &TurnContext,
    model_info: &ModelInfo,
    mut registry: ToolRegistry,
    mut hosted_specs: Vec<ToolSpec>,
) -> CodexResult<ToolRouter> {
    if let Some(allowed) = &registry.allowed_tools {
        hosted_specs.retain(|spec| allowed.contains(&ToolName::plain(spec.name())));
    }
    apply_direct_model_only_namespace_overrides(turn_context, &mut registry);
    let tool_mode = effective_tool_mode(turn_context, model_info);
    let code_mode_enabled = matches!(tool_mode, ToolMode::CodeMode | ToolMode::CodeModeOnly);
    if code_mode_enabled {
        for tool_name in [
            ToolName::plain(codex_code_mode::PUBLIC_TOOL_NAME),
            ToolName::plain(codex_code_mode::WAIT_TOOL_NAME),
        ] {
            if registry.remove(&tool_name).is_some() {
                registry.record_collision(tool_name);
            }
        }
    }
    let code_mode_tool_names =
        register_code_mode_executors(turn_context, model_info, &mut registry);
    let include_tool_namespaces_info = turn_context
        .config
        .tool_registry
        .turn_metadata_includes_tool_info
        && model_info.use_responses_lite;

    if turn_context.config.tool_registry.error_on_tool_collisions {
        if let Some(tool_name) = registry.first_collision() {
            let namespace = tool_name.namespace.as_deref().unwrap_or("functions");
            let name = format!("{namespace}.{}", tool_name.name);
            return Err(CodexErrorDetails::ToolCollision(name).into());
        }

        let mut namespace_descriptions = BTreeMap::new();
        let mut namespace_owners = BTreeMap::new();
        for tool in registry.entries() {
            let owned_spec;
            let spec = if let Some(spec) = tool.runtime.immutable_spec() {
                spec.as_ref()
            } else {
                owned_spec = tool.runtime.spec();
                &owned_spec
            };
            if include_tool_namespaces_info && tool.exposure != ToolExposure::Hidden {
                let namespace_name = match spec {
                    ToolSpec::Namespace(namespace) => namespace.name.as_str(),
                    ToolSpec::Function(_) | ToolSpec::Freeform(_) => DEFAULT_FUNCTION_NAMESPACE,
                    ToolSpec::ToolSearch { .. } => TOOL_SEARCH_TOOL_NAME,
                    ToolSpec::WebSearch { .. } => continue,
                };
                let owner = tool.runtime.mcp_server_name();
                match namespace_owners.get(namespace_name) {
                    Some(existing_owner) if existing_owner != &owner => {
                        return Err(
                            CodexErrorDetails::ToolCollision(namespace_name.to_string()).into()
                        );
                    }
                    Some(_) => {}
                    None => {
                        namespace_owners.insert(namespace_name.to_string(), owner);
                    }
                }
            }

            let ToolSpec::Namespace(namespace) = spec else {
                continue;
            };
            if namespace.description.trim().is_empty() {
                continue;
            }
            match namespace_descriptions.entry(namespace.name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(namespace.description.clone());
                }
                Entry::Occupied(entry) if entry.get() != &namespace.description => {
                    return Err(CodexErrorDetails::ToolCollision(entry.key().clone()).into());
                }
                Entry::Occupied(_) => {}
            }
        }
    }

    let model_visible_specs = build_model_visible_specs(
        turn_context,
        model_info,
        &registry,
        &code_mode_tool_names,
        hosted_specs,
    );
    let tool_namespaces_info = include_tool_namespaces_info
        .then(|| {
            collect_tool_namespaces_info(&registry, &code_mode_tool_names, &model_visible_specs)
        })
        .filter(|info| !info.is_empty());
    let child_management_tools = required_child_management_tool_names(turn_context, model_info);

    Ok(ToolRouter::from_parts(
        registry,
        model_visible_specs,
        tool_mode,
        code_mode_tool_names,
        tool_namespaces_info,
        &child_management_tools,
    ))
}

fn apply_direct_model_only_namespace_overrides(
    turn_context: &TurnContext,
    registry: &mut ToolRegistry,
) {
    if turn_context
        .config
        .code_mode
        .direct_only_tool_namespaces
        .is_empty()
    {
        return;
    }

    for tool in registry.entries_mut() {
        let configured = tool
            .runtime
            .tool_name()
            .with_default_namespace()
            .namespace
            .as_ref()
            .is_some_and(|namespace| {
                turn_context
                    .config
                    .code_mode
                    .direct_only_tool_namespaces
                    .contains(namespace)
            });
        if configured && tool.exposure.is_available_in_code_mode() {
            tool.exposure = ToolExposure::DirectModelOnly;
        }
    }
}

#[instrument(level = "trace", skip_all)]
fn build_model_visible_specs(
    turn_context: &TurnContext,
    model_info: &ModelInfo,
    registry: &ToolRegistry,
    code_mode_tool_names: &BTreeMap<String, ToolName>,
    hosted_specs: Vec<ToolSpec>,
) -> Vec<ToolSpec> {
    let mut specs = Vec::new();
    for tool in registry.entries() {
        let exposure = tool.exposure;
        if !exposure.is_direct() {
            continue;
        }

        let tool_name = tool.runtime.tool_name();
        if is_hidden_by_code_mode_only(turn_context, model_info, &tool_name, exposure) {
            continue;
        }

        let spec = tool.runtime.spec();
        specs.push(spec_for_model_request(
            turn_context,
            model_info,
            exposure,
            &tool_name,
            code_mode_tool_names,
            spec,
        ));
    }
    specs.extend(hosted_specs);

    merge_into_namespaces(specs)
        .into_iter()
        .filter(|spec| {
            namespace_tools_enabled(turn_context) || !matches!(spec, ToolSpec::Namespace(_))
        })
        .collect()
}

fn spec_for_model_request(
    turn_context: &TurnContext,
    model_info: &ModelInfo,
    exposure: ToolExposure,
    tool_name: &ToolName,
    code_mode_tool_names: &BTreeMap<String, ToolName>,
    spec: ToolSpec,
) -> ToolSpec {
    let tool_mode = effective_tool_mode(turn_context, model_info);
    if matches!(tool_mode, ToolMode::CodeMode | ToolMode::CodeModeOnly)
        && exposure.is_available_in_code_mode()
        && !is_excluded_from_code_mode(turn_context, tool_name)
        && codex_code_mode::is_code_mode_nested_tool(spec.name())
        && code_mode_tool_names
            .get(&codex_code_mode::normalize_code_mode_identifier(
                &codex_tools::code_mode_name_for_tool_name(tool_name),
            ))
            .is_some_and(|winner| winner == tool_name)
    {
        codex_tools::augment_tool_spec_for_code_mode(spec)
    } else {
        spec
    }
}

pub(crate) fn search_tool_enabled(turn_context: &TurnContext, model_info: &ModelInfo) -> bool {
    !turn_context.delegated_tools_only
        && model_info.supports_search_tool
        && namespace_tools_enabled(turn_context)
}

pub(crate) fn tool_suggest_enabled(turn_context: &TurnContext) -> bool {
    let features = turn_context.config.features.get();
    features.enabled(Feature::ToolSuggest)
        && features.enabled(Feature::Apps)
        && features.enabled(Feature::Plugins)
}

fn namespace_tools_enabled(turn_context: &TurnContext) -> bool {
    turn_context.provider.capabilities().namespace_tools
}

fn multi_agent_v2_enabled(turn_context: &TurnContext) -> bool {
    turn_context.multi_agent_version == MultiAgentVersion::V2
}

fn collab_tools_enabled(turn_context: &TurnContext, model_info: &ModelInfo) -> bool {
    match turn_context.multi_agent_version {
        MultiAgentVersion::Disabled => false,
        MultiAgentVersion::V1 => !exceeds_thread_spawn_depth_limit(
            next_thread_spawn_depth(&turn_context.session_source),
            turn_context.config.agent_max_depth,
        ),
        MultiAgentVersion::V2 => {
            turn_context.session_source.get_agent_path().is_none()
                || model_info.multi_agent_version == Some(MultiAgentVersion::V2)
        }
    }
}

fn required_child_management_tool_names(
    turn_context: &TurnContext,
    model_info: &ModelInfo,
) -> Vec<ToolName> {
    if !collab_tools_enabled(turn_context, model_info) {
        return Vec::new();
    }

    let (namespace, names): (_, &[&str]) = match turn_context.multi_agent_version {
        MultiAgentVersion::Disabled => return Vec::new(),
        MultiAgentVersion::V1 => (
            Some(MULTI_AGENT_V1_NAMESPACE),
            &["send_input", "wait_agent", "resume_agent", "close_agent"],
        ),
        MultiAgentVersion::V2 => (
            namespace_tools_enabled(turn_context)
                .then_some(turn_context.config.multi_agent_v2.tool_namespace.as_deref())
                .flatten(),
            &[
                "send_message",
                "followup_task",
                "interrupt_agent",
                "list_agents",
            ],
        ),
    };
    let mut tools = names
        .iter()
        .map(|name| ToolName::new(namespace.map(str::to_owned), (*name).to_owned()))
        .collect::<Vec<_>>();
    if multi_agent_v2_enabled(turn_context) && turn_context.config.multi_agent_v2.wait_agent_enabled
    {
        tools.push(ToolName::new(namespace.map(str::to_owned), "wait_agent"));
    }
    tools
}

fn is_hidden_by_code_mode_only(
    turn_context: &TurnContext,
    model_info: &ModelInfo,
    tool_name: &ToolName,
    exposure: ToolExposure,
) -> bool {
    let tool_mode = effective_tool_mode(turn_context, model_info);
    tool_mode == ToolMode::CodeModeOnly
        && exposure.is_available_in_code_mode()
        && codex_code_mode::is_code_mode_nested_tool(&codex_tools::code_mode_name_for_tool_name(
            tool_name,
        ))
}

fn is_excluded_from_code_mode(turn_context: &TurnContext, tool_name: &ToolName) -> bool {
    let tool_name = tool_name.clone().with_default_namespace();
    tool_name.namespace.as_ref().is_some_and(|namespace| {
        turn_context
            .config
            .code_mode
            .excluded_tool_namespaces
            .contains(namespace)
    })
}

fn register_code_mode_executors(
    turn_context: &TurnContext,
    model_info: &ModelInfo,
    registry: &mut ToolRegistry,
) -> BTreeMap<String, ToolName> {
    let tool_mode = effective_tool_mode(turn_context, model_info);
    if !matches!(tool_mode, ToolMode::CodeMode | ToolMode::CodeModeOnly) {
        return BTreeMap::new();
    }

    let mut code_mode_tool_names = BTreeMap::new();
    let mut code_mode_nested_tool_specs = Vec::new();
    let mut exec_prompt_tool_specs = Vec::new();
    let mut deferred_exec_prompt_tool_specs = Vec::new();
    let mut included_deferred_mcp_output_schema = false;
    let deferred_tools_guidance_enabled = search_tool_enabled(turn_context, model_info);
    for tool in registry.entries() {
        let exposure = tool.exposure;
        if !exposure.is_available_in_code_mode() {
            continue;
        }

        let tool_name = tool.runtime.tool_name();
        if is_excluded_from_code_mode(turn_context, &tool_name) {
            continue;
        }

        let immutable_spec = tool.runtime.immutable_spec();
        let cached_runtime = immutable_spec.map(|_| Arc::clone(&tool.runtime));
        let spec = immutable_spec
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::new(tool.runtime.spec()));
        // Derive the name without serializing and augmenting every tool schema.
        let code_mode_name = match spec.as_ref() {
            ToolSpec::Function(_) | ToolSpec::Freeform(_) => {
                codex_tools::code_mode_name_for_tool_name(&tool_name)
            }
            ToolSpec::Namespace(namespace) if !namespace.tools.is_empty() => {
                codex_tools::code_mode_name_for_tool_name(&tool_name)
            }
            ToolSpec::Namespace(_) | ToolSpec::ToolSearch { .. } | ToolSpec::WebSearch { .. } => {
                continue;
            }
        };
        if !codex_code_mode::is_code_mode_nested_tool(&code_mode_name) {
            continue;
        }
        match code_mode_tool_names.entry(codex_code_mode::normalize_code_mode_identifier(
            &code_mode_name,
        )) {
            Entry::Vacant(entry) => {
                entry.insert(tool_name);
            }
            Entry::Occupied(_) => {
                tracing::warn!(
                    tool_name = %tool_name,
                    code_mode_name = %code_mode_name,
                    "skipping tool with a duplicate normalized code-mode name"
                );
                continue;
            }
        }

        if exposure == ToolExposure::Deferred {
            if deferred_tools_guidance_enabled
                && (cached_runtime.is_none() || !included_deferred_mcp_output_schema)
            {
                if cached_runtime.is_some() {
                    included_deferred_mcp_output_schema = true;
                }
                deferred_exec_prompt_tool_specs.push(Arc::clone(&spec));
            }
        } else {
            exec_prompt_tool_specs.push(spec.as_ref().clone());
        }
        code_mode_nested_tool_specs.push((spec, cached_runtime));
    }

    let namespace_descriptions = code_mode_namespace_descriptions(&exec_prompt_tool_specs);
    let mut enabled_tools =
        collect_code_mode_exec_prompt_tool_definitions(exec_prompt_tool_specs.iter());
    let deferred_tools = collect_code_mode_exec_prompt_tool_definitions(
        deferred_exec_prompt_tool_specs.iter().map(Arc::as_ref),
    );
    enabled_tools
        .sort_by(|left, right| compare_code_mode_tools(left, right, &namespace_descriptions));
    let execute_handler = CodeModeExecuteHandler::new(
        create_code_mode_tool(
            &enabled_tools,
            &deferred_tools,
            &namespace_descriptions,
            turn_context.config.code_mode.default_exec_yield_time_ms,
            tool_mode == ToolMode::CodeModeOnly,
            if unified_image_budget_enabled(&turn_context.config.features, model_info) {
                codex_code_mode::ImageDetailVisibility::Hidden
            } else {
                codex_code_mode::ImageDetailVisibility::Visible
            },
        ),
        code_mode_nested_tool_specs,
    );

    registry.prepend_trusted(Arc::new(CodeModeWaitHandler));
    registry.prepend_trusted(Arc::new(execute_handler));

    code_mode_tool_names
}

#[instrument(level = "trace", skip_all, fields(tool_spec_count = specs.len()))]
fn merge_into_namespaces(specs: Vec<ToolSpec>) -> Vec<ToolSpec> {
    let mut merged_specs = Vec::with_capacity(specs.len());
    let mut namespace_indices = BTreeMap::<String, usize>::new();
    for spec in specs {
        match spec {
            ToolSpec::Namespace(mut namespace) => {
                if let Some(index) = namespace_indices.get(&namespace.name).copied() {
                    let ToolSpec::Namespace(existing_namespace) = &mut merged_specs[index] else {
                        unreachable!("namespace index must point to a namespace spec");
                    };
                    if existing_namespace.description.trim().is_empty()
                        && !namespace.description.trim().is_empty()
                    {
                        existing_namespace.description = namespace.description;
                    }
                    existing_namespace.tools.append(&mut namespace.tools);
                    continue;
                }

                namespace_indices.insert(namespace.name.clone(), merged_specs.len());
                merged_specs.push(ToolSpec::Namespace(namespace));
            }
            spec => merged_specs.push(spec),
        }
    }

    for spec in &mut merged_specs {
        let ToolSpec::Namespace(namespace) = spec else {
            continue;
        };

        namespace.tools.sort_by(|left, right| {
            let left_name = match left {
                ResponsesApiNamespaceTool::Function(tool) => &tool.name,
                ResponsesApiNamespaceTool::Custom(tool) => &tool.name,
            };
            let right_name = match right {
                ResponsesApiNamespaceTool::Function(tool) => &tool.name,
                ResponsesApiNamespaceTool::Custom(tool) => &tool.name,
            };
            left_name.cmp(right_name)
        });

        if namespace.description.trim().is_empty() {
            namespace.description = default_namespace_description(&namespace.name);
        }
    }

    merged_specs
}

fn code_mode_namespace_descriptions(
    specs: &[ToolSpec],
) -> BTreeMap<String, codex_code_mode::ToolNamespaceDescription> {
    let mut namespace_descriptions = BTreeMap::new();
    for spec in specs {
        let ToolSpec::Namespace(namespace) = spec else {
            continue;
        };

        let entry = namespace_descriptions
            .entry(namespace.name.clone())
            .or_insert_with(|| codex_code_mode::ToolNamespaceDescription {
                name: namespace.name.clone(),
                description: namespace.description.clone(),
            });
        if entry.description.trim().is_empty() && !namespace.description.trim().is_empty() {
            entry.description = namespace.description.clone();
        }
    }
    namespace_descriptions
}

#[instrument(level = "trace", skip_all, fields(dynamic_tool_count = dynamic_tools.len()))]
pub(crate) fn append_dynamic_tool_runtimes(
    dynamic_tools: &[DynamicToolSpec],
    registry: &mut ToolRegistry,
) {
    for spec in dynamic_tools {
        match spec {
            DynamicToolSpec::Function(tool) => {
                if tool.defer_loading {
                    continue;
                }
                let Some(handler) = DynamicToolHandler::new(tool) else {
                    tracing::error!(
                        "Failed to convert dynamic tool {:?} to OpenAI tool",
                        tool.name
                    );
                    continue;
                };
                registry.register_external(Arc::new(handler));
            }
            DynamicToolSpec::Namespace(namespace) => {
                for tool in &namespace.tools {
                    let DynamicToolNamespaceTool::Function(tool) = tool;
                    if tool.defer_loading {
                        continue;
                    }
                    let Some(handler) = DynamicToolHandler::new_in_namespace(namespace, tool)
                    else {
                        tracing::error!(
                            "Failed to convert dynamic tool {:?}.{:?} to OpenAI tool",
                            namespace.name,
                            tool.name
                        );
                        continue;
                    };
                    registry.register_external(Arc::new(handler));
                }
            }
        }
    }
}

fn compare_code_mode_tools(
    left: &codex_code_mode::ToolDefinition,
    right: &codex_code_mode::ToolDefinition,
    namespace_descriptions: &BTreeMap<String, codex_code_mode::ToolNamespaceDescription>,
) -> std::cmp::Ordering {
    let left_namespace = code_mode_namespace_name(left, namespace_descriptions);
    let right_namespace = code_mode_namespace_name(right, namespace_descriptions);

    left_namespace
        .cmp(&right_namespace)
        .then_with(|| left.tool_name.name.cmp(&right.tool_name.name))
        .then_with(|| left.name.cmp(&right.name))
}

fn code_mode_namespace_name<'a>(
    tool: &codex_code_mode::ToolDefinition,
    namespace_descriptions: &'a BTreeMap<String, codex_code_mode::ToolNamespaceDescription>,
) -> Option<&'a str> {
    tool.tool_name
        .namespace
        .as_ref()
        .and_then(|namespace| namespace_descriptions.get(namespace))
        .map(|namespace_description| namespace_description.name.as_str())
}
