use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_state::SqliteConfig;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

const CHILD_MODE: &str = "DSH_TEST_SQLITE_FIXTURE_CHILD";

#[tokio::test]
async fn sqlite_fixture_isolates_host_and_honors_explicit_override() -> Result<()> {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        let codex_home = TempDir::new()?;
        let explicit_home = TempDir::new()?;
        MockResponsesConfig::new("http://127.0.0.1:1").write(codex_home.path())?;
        let mut builder = TestAppServer::builder().with_codex_home(codex_home.path());
        if mode == "override" {
            builder =
                builder.with_env_overrides(&[("CODEX_SQLITE_HOME", explicit_home.path().to_str())]);
        }
        let mut client = builder.build_initialized().await?;
        let request = client
            .send_thread_start_request_with_auto_env(ThreadStartParams::default())
            .await?;
        let _: ThreadStartResponse = client.read_response(request).await?;
        let default_db = SqliteConfig::from_sqlite_home(AbsolutePathBuf::try_from(
            codex_home.path().to_path_buf(),
        )?)
        .state_db_path();
        let explicit_db = SqliteConfig::from_sqlite_home(AbsolutePathBuf::try_from(
            explicit_home.path().to_path_buf(),
        )?)
        .state_db_path();
        assert_eq!(
            (default_db.is_file(), explicit_db.is_file()),
            (mode != "override", mode == "override")
        );
        return Ok(());
    }

    // Run the fixture in another test process to simulate an inherited host
    // setting without mutating this process's environment or touching host data.
    for mode in ["isolated", "override"] {
        let host_home = TempDir::new()?;
        let sentinel = host_home.path().join("sentinel");
        std::fs::write(&sentinel, b"preserved")?;
        let output = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "suite::v2::sqlite_fixture_isolation::sqlite_fixture_isolates_host_and_honors_explicit_override",
                "--nocapture",
            ])
            .env(CHILD_MODE, mode)
            .env("CODEX_SQLITE_HOME", host_home.path())
            .output()
            .await?;
        assert!(
            output.status.success(),
            "{mode}: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let entries = std::fs::read_dir(host_home.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(entries, vec![std::ffi::OsString::from("sentinel")]);
        assert_eq!(std::fs::read(sentinel)?, b"preserved");
    }
    Ok(())
}
