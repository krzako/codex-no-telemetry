//! Durable fork-side queue for facts that DSH must acknowledge before a turn proceeds.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::fs::create_dir_all;
use std::fs::read_dir;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use codex_app_server_protocol::DshAuditAppendParams;
use codex_app_server_protocol::DshAuditAppendResponse;
use codex_app_server_protocol::DshAuditCursorParams;
use codex_app_server_protocol::DshAuditCursorResponse;
use codex_app_server_protocol::DshAuditTurn;
use codex_app_server_protocol::ServerRequestPayload;
use codex_protocol::ThreadId;
use serde_json::Value;
use serde_json::json;
use tempfile::NamedTempFile;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use uuid::Uuid;

use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;

const DSH_AUDIT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

struct AuditSpool {
    directory: PathBuf,
    thread_id: String,
    events: Vec<Value>,
}

impl AuditSpool {
    fn open(root: &Path, thread_id: &str) -> Result<Self> {
        let thread_uuid = Uuid::parse_str(thread_id).context("invalid DSH audit thread ID")?;
        let directory = root.join(thread_uuid.to_string());
        create_dir_all(&directory).context("create DSH audit spool")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let mut paths = read_dir(&directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.retain(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        });
        paths.sort();
        let mut events = Vec::with_capacity(paths.len());
        let mut event_ids = HashSet::new();
        for path in paths {
            let sequence = u64::try_from(events.len() + 1)?;
            if path.file_name().and_then(|name| name.to_str())
                != Some(format!("{sequence:020}.json").as_str())
            {
                return Err(anyhow!("DSH audit spool has a sequence gap"));
            }
            let event: Value = serde_json::from_reader(File::open(&path)?)
                .context("invalid DSH audit spool event")?;
            if event["version"] != 1
                || event["sequence"] != sequence
                || event["threadId"] != thread_id
                || !event["eventId"]
                    .as_str()
                    .is_some_and(|id| event_ids.insert(id.to_string()))
            {
                return Err(anyhow!("DSH audit spool event identity is invalid"));
            }
            events.push(event);
        }
        Ok(Self {
            directory,
            thread_id: thread_id.to_string(),
            events,
        })
    }

    fn record(&mut self, mut event: Value) -> Result<()> {
        let object = event
            .as_object()
            .ok_or_else(|| anyhow!("DSH audit fact must be an object"))?;
        if ["version", "sequence", "time", "threadId"]
            .iter()
            .any(|field| object.contains_key(*field))
        {
            return Err(anyhow!("DSH audit fact supplied a spool-owned field"));
        }
        let event_id = object
            .get("eventId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("DSH audit fact requires an eventId"))?
            .to_string();
        if let Some(prior) = self
            .events
            .iter()
            .find(|prior| prior["eventId"] == event_id)
        {
            let mut comparable = prior.clone();
            let fields = comparable
                .as_object_mut()
                .ok_or_else(|| anyhow!("DSH audit spool event is invalid"))?;
            for field in ["version", "sequence", "time", "threadId"] {
                fields.remove(field);
            }
            if comparable == event {
                return Ok(());
            }
            return Err(anyhow!("DSH audit fact changed during retry"));
        }
        let sequence = u64::try_from(self.events.len() + 1)?;
        let time = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let object = event
            .as_object_mut()
            .ok_or_else(|| anyhow!("DSH audit fact must be an object"))?;
        object.insert("version".to_string(), json!(1));
        object.insert("sequence".to_string(), json!(sequence));
        object.insert("time".to_string(), json!(time));
        object.insert("threadId".to_string(), json!(self.thread_id));
        let path = self.directory.join(format!("{sequence:020}.json"));
        let mut temporary = NamedTempFile::new_in(&self.directory)?;
        serde_json::to_writer(&mut temporary, &event)?;
        temporary.flush()?;
        temporary.as_file().sync_all()?;
        temporary
            .persist_noclobber(&path)
            .context("publish DSH audit spool event")?;
        #[cfg(unix)]
        File::open(&self.directory)?.sync_all()?;
        self.events.push(event);
        Ok(())
    }

    fn record_context(&mut self, turn: &DshAuditTurn, config: Value) -> Result<()> {
        self.record(json!({
            "eventId": format!("dsh-turn-config-{}", turn.turn),
            "turn": turn.turn,
            "kind": "context/config",
            "config": { "kind": "inline", "value": config },
        }))
    }

    async fn reconcile<F, Fut>(&self, mut exchange: F) -> Result<()>
    where
        F: FnMut(ServerRequestPayload) -> Fut,
        Fut: Future<Output = Result<Value>>,
    {
        let cursor: DshAuditCursorResponse = serde_json::from_value(
            exchange(ServerRequestPayload::DshAuditCursor(DshAuditCursorParams {
                thread_id: self.thread_id.clone(),
            }))
            .await?,
        )?;
        let committed = usize::try_from(cursor.sequence)?;
        if cursor.version != 1
            || committed > self.events.len()
            || (committed == 0 && cursor.event_id.is_some())
            || (committed > 0
                && self.events[committed - 1]["eventId"].as_str() != cursor.event_id.as_deref())
        {
            return Err(anyhow!("DSH audit cursor conflicts with the fork spool"));
        }
        for event in &self.events[committed..] {
            let ack: DshAuditAppendResponse = serde_json::from_value(
                exchange(ServerRequestPayload::DshAuditAppend(DshAuditAppendParams {
                    thread_id: self.thread_id.clone(),
                    event: event.clone(),
                }))
                .await?,
            )?;
            if ack.version != 1
                || Some(ack.sequence) != event["sequence"].as_u64()
                || Some(ack.event_id.as_str()) != event["eventId"].as_str()
            {
                return Err(anyhow!(
                    "DSH audit acknowledgment conflicts with the fork spool"
                ));
            }
        }
        Ok(())
    }
}

pub(crate) struct DshAuditManager {
    root: PathBuf,
    outgoing: Arc<OutgoingMessageSender>,
    locks: Mutex<HashMap<ThreadId, Arc<Semaphore>>>,
}

impl DshAuditManager {
    pub(crate) fn new(root: PathBuf, outgoing: Arc<OutgoingMessageSender>) -> Self {
        Self {
            root,
            outgoing,
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Reconcile pending facts, persist this turn's configuration, and wait for its ACK.
    pub(crate) async fn before_turn(
        &self,
        request_id: &ConnectionRequestId,
        thread_id: ThreadId,
        turn: &DshAuditTurn,
        config: Value,
    ) -> Result<()> {
        let semaphore = {
            let mut locks = self.locks.lock().await;
            Arc::clone(
                locks
                    .entry(thread_id)
                    .or_insert_with(|| Arc::new(Semaphore::new(1))),
            )
        };
        let _permit = semaphore.acquire_owned().await?;
        let root = self.root.clone();
        let thread_id_text = thread_id.to_string();
        let mut spool =
            tokio::task::spawn_blocking(move || AuditSpool::open(&root, &thread_id_text)).await??;
        let connection = request_id.connection_id;
        let exchange = |payload| {
            let outgoing = Arc::clone(&self.outgoing);
            async move {
                let (id, response) = outgoing
                    .send_request_to_connections(Some(&[connection]), payload, Some(thread_id))
                    .await;
                match timeout(DSH_AUDIT_REQUEST_TIMEOUT, response).await {
                    Ok(Ok(Ok(value))) => Ok(value),
                    Ok(Ok(Err(error))) => {
                        Err(anyhow!("DSH audit request failed: {}", error.message))
                    }
                    Ok(Err(error)) => Err(anyhow!("DSH audit request lost its client: {error}")),
                    Err(_) => {
                        let _canceled = outgoing.cancel_request(&id).await;
                        Err(anyhow!(
                            "DSH audit request timed out after {}s",
                            DSH_AUDIT_REQUEST_TIMEOUT.as_secs()
                        ))
                    }
                }
            }
        };
        spool.reconcile(exchange).await?;
        let turn = turn.clone();
        tokio::task::spawn_blocking(move || {
            spool.record_context(&turn, config)?;
            Ok::<_, anyhow::Error>(spool)
        })
        .await??
        .reconcile(exchange)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct Remote {
        sequence: u64,
        event_id: Option<String>,
        appends: usize,
        lose_first_ack: bool,
    }

    fn exchange(
        remote: Arc<StdMutex<Remote>>,
    ) -> impl FnMut(ServerRequestPayload) -> std::future::Ready<Result<Value>> {
        move |request| {
            let mut remote = remote.lock().expect("remote lock");
            let response = match request {
                ServerRequestPayload::DshAuditCursor(_) => Ok(json!({
                    "version": 1, "sequence": remote.sequence, "eventId": remote.event_id,
                })),
                ServerRequestPayload::DshAuditAppend(params) => {
                    let event = params.event;
                    assert_eq!(event["sequence"], remote.sequence + 1);
                    remote.sequence += 1;
                    remote.event_id = event["eventId"].as_str().map(str::to_string);
                    remote.appends += 1;
                    if remote.lose_first_ack {
                        remote.lose_first_ack = false;
                        Err(anyhow!("connection lost after durable commit"))
                    } else {
                        Ok(json!({
                            "version": 1, "sequence": remote.sequence, "eventId": remote.event_id,
                        }))
                    }
                }
                _ => panic!("unexpected request"),
            };
            std::future::ready(response)
        }
    }

    #[tokio::test]
    async fn lost_ack_is_reconciled_without_duplicate_append_after_restart() -> Result<()> {
        let root = tempfile::tempdir()?;
        let thread_id = Uuid::now_v7().to_string();
        let turn = DshAuditTurn {
            session_id: "session-1".to_string(),
            turn: 1,
        };
        let config = json!({"sessionId":"session-1", "requestedModel":"model-a"});
        let mut spool = AuditSpool::open(root.path(), &thread_id)?;
        spool.record_context(&turn, config.clone())?;
        let remote = Arc::new(StdMutex::new(Remote {
            lose_first_ack: true,
            ..Default::default()
        }));
        assert!(
            spool
                .reconcile(exchange(Arc::clone(&remote)))
                .await
                .is_err()
        );
        drop(spool);

        let mut resumed = AuditSpool::open(root.path(), &thread_id)?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        resumed.record_context(&turn, config.clone())?;
        assert_eq!(resumed.events.len(), 1);
        assert!(
            resumed
                .record_context(&turn, json!({"changed": true}))
                .is_err()
        );
        resumed.record_context(&DshAuditTurn { turn: 2, ..turn }, config)?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        let remote = remote.lock().expect("remote lock");
        assert_eq!(remote.sequence, 2);
        assert_eq!(remote.appends, 2);
        Ok(())
    }

    #[tokio::test]
    async fn conflicting_cursor_and_ack_stop_replay() -> Result<()> {
        let root = tempfile::tempdir()?;
        let thread_id = Uuid::now_v7().to_string();
        let mut spool = AuditSpool::open(root.path(), &thread_id)?;
        spool.record_context(
            &DshAuditTurn {
                session_id: "session-1".into(),
                turn: 1,
            },
            json!({"model": "a"}),
        )?;
        let wrong_cursor = |_request| {
            std::future::ready(Ok(json!({
                "version": 1, "sequence": 1, "eventId": "different",
            })))
        };
        assert!(
            spool
                .reconcile(wrong_cursor)
                .await
                .unwrap_err()
                .to_string()
                .contains("cursor conflicts")
        );
        let wrong_ack = |request| {
            std::future::ready(match request {
                ServerRequestPayload::DshAuditCursor(_) => Ok(json!({
                    "version": 1, "sequence": 0, "eventId": null,
                })),
                ServerRequestPayload::DshAuditAppend(_) => Ok(json!({
                    "version": 1, "sequence": 1, "eventId": "different",
                })),
                _ => panic!("unexpected request"),
            })
        };
        assert!(
            spool
                .reconcile(wrong_ack)
                .await
                .unwrap_err()
                .to_string()
                .contains("acknowledgment conflicts")
        );
        Ok(())
    }
}
