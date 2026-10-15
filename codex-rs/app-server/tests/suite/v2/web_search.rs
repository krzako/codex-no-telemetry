use std::time::Duration;

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_app_server_protocol::WebSearchAction;
use codex_app_server_protocol::WebSearchItem;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

#[cfg(any(target_os = "macos", windows))]
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(not(any(target_os = "macos", windows)))]
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn web_search_response_items_preserve_notifications_and_history_after_restart() -> Result<()>
{
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_web_search_call_added_partial("search-1", "in_progress"),
            responses::ev_web_search_call_done("search-1", "completed", "saved search"),
            responses::ev_assistant_message("msg-1", "Done"),
            responses::ev_completed("resp-1"),
        ]),
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let mut client = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    let request = client
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, client.read_response(request)).await??;
    let request = client
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "Show the saved result".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, client.read_response(request)).await??;
    let started = timeout(
        DEFAULT_READ_TIMEOUT,
        wait_for_web_search_started(&mut client),
    )
    .await??;
    let completed = timeout(
        DEFAULT_READ_TIMEOUT,
        wait_for_web_search_completed(&mut client),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        client.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    assert_eq!(
        started.item,
        ThreadItem::WebSearch(WebSearchItem {
            id: "search-1".to_string(),
            query: String::new(),
            action: Some(WebSearchAction::Other),
            results: None,
        })
    );
    let expected = ThreadItem::WebSearch(WebSearchItem {
        id: "search-1".to_string(),
        query: "saved search".to_string(),
        action: Some(WebSearchAction::Search {
            query: Some("saved search".to_string()),
            queries: None,
        }),
        results: None,
    });
    assert_eq!(completed.item, expected);
    assert_eq!(response_mock.requests().len(), 1);
    drop(client);
    let mut client = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    let request = client
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread.id,
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread } =
        timeout(DEFAULT_READ_TIMEOUT, client.read_response(request)).await??;
    let searches: Vec<_> = thread
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter(|item| matches!(item, ThreadItem::WebSearch(_)))
        .collect();
    assert_eq!(searches, vec![&expected]);
    Ok(())
}

async fn wait_for_web_search_started(mcp: &mut TestAppServer) -> Result<ItemStartedNotification> {
    loop {
        let started: ItemStartedNotification = mcp.read_notification("item/started").await?;
        if matches!(&started.item, ThreadItem::WebSearch(_)) {
            return Ok(started);
        }
    }
}

async fn wait_for_web_search_completed(
    mcp: &mut TestAppServer,
) -> Result<ItemCompletedNotification> {
    loop {
        let completed: ItemCompletedNotification = mcp.read_notification("item/completed").await?;
        if matches!(&completed.item, ThreadItem::WebSearch(_)) {
            return Ok(completed);
        }
    }
}
