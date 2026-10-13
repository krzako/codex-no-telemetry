use super::*;
use codex_skills_extension::NativeSkillsDisabled;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn disabled_thread_never_discovers_or_injects_skills_after_config_refresh() {
    let calls = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(StaticSkillProvider {
        catalog: SkillCatalog::default(),
        read_requests: Arc::clone(&reads),
        list_calls: Some(Arc::clone(&calls)),
        fail_first_list: false,
    });
    let mut builder = ExtensionRegistryBuilder::new();
    install_with_providers(
        &mut builder,
        SkillProviders::new()
            .with_executor_provider(provider.clone())
            .with_orchestrator_provider(provider.clone())
            .with_host_provider(provider),
        skills_extension_config,
    );
    let registry = builder.build();
    let config = default_config();
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    let turn = ExtensionData::new("turn");
    thread.insert(NativeSkillsDisabled);
    registry.thread_lifecycle_contributors()[0]
        .on_thread_start(ThreadStartInput {
            config: &config,
            session_source: &SessionSource::Cli,
            persistent_thread_state_available: true,
            environments: &[],
            mcp_resource_client: None,
            extension_metrics: None,
            session_store: &session,
            thread_store: &thread,
        })
        .await;
    for config_refresh in [false, true] {
        if config_refresh {
            registry.config_contributors()[0]
                .on_config_changed(&session, &thread, &config, &config);
        }
        assert!(
            registry.context_contributors()[0]
                .contribute_thread_context(&session, &thread)
                .await
                .is_empty()
        );
        assert!(
            registry.context_contributors()[0]
                .contribute_world_state(WorldStateContributionInput {
                    model_info: &catalog_model_info(),
                    thread_id: codex_protocol::ThreadId::new(),
                    turn_id: "turn",
                    environments: &[],
                    ready_selected_capability_roots: &[],
                    executor_capability_discovery: None,
                    extension_metrics: None,
                    session_store: &session,
                    thread_store: &thread,
                    turn_store: &turn,
                })
                .await
                .is_empty()
        );
        assert!(
            registry.tool_contributors()[0]
                .tools(&session, &thread)
                .is_empty()
        );
        assert!(
            registry.tool_contributors()[0]
                .tools_for_step(&session, &thread, &turn)
                .is_empty()
        );
        assert!(
            registry.turn_input_contributors()[0]
                .contribute(
                    TurnInputContext {
                        turn_id: "turn".to_string(),
                        user_input: vec![UserInput::Text {
                            text: "$demo".to_string(),
                            text_elements: Vec::new(),
                        }],
                        environments: Vec::new(),
                    },
                    /*extension_metrics*/ None,
                    &session,
                    &thread,
                    &turn,
                )
                .await
                .is_empty()
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(read_request_keys(&reads), Vec::new());
}
