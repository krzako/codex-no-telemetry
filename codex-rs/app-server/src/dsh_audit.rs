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
use codex_core::CodexThread;
use codex_core::DshCompactionCheckpointStatus;
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

fn resolved_compaction_fact(
    pending: &Value,
    status: DshCompactionCheckpointStatus,
    prior_rollout_sha256: Option<&str>,
    created_this_process: bool,
) -> Result<Value> {
    let compaction_id = pending["compactionId"]
        .as_str()
        .ok_or_else(|| anyhow!("pending DSH compaction has no compactionId"))?;
    let turn = pending["turn"]
        .as_u64()
        .ok_or_else(|| anyhow!("pending DSH compaction has no turn"))?;
    let effective_history = pending["effectiveHistory"]
        .as_object()
        .ok_or_else(|| anyhow!("pending DSH compaction has no effective history"))?;
    let history = Value::Object(effective_history.clone());
    match status {
        DshCompactionCheckpointStatus::Installed => Ok(json!({
            "eventId": format!("dsh-compaction-installed-{compaction_id}"),
            "turn": turn,
            "kind": "compaction/installed",
            "compactionId": compaction_id,
            "effectiveHistory": history,
        })),
        DshCompactionCheckpointStatus::Absent => {
            if created_this_process {
                return Err(anyhow!(
                    "DSH compaction {compaction_id} has no durable checkpoint; restart is required before deciding whether to abort"
                ));
            }
            let anchor = pending["priorRolloutSha256"].as_str().ok_or_else(|| {
                anyhow!("DSH compaction {compaction_id} has no prior rollout anchor")
            })?;
            if prior_rollout_sha256 != Some(anchor) {
                return Err(anyhow!(
                    "DSH compaction {compaction_id} rollout changed without a matching checkpoint"
                ));
            }
            Ok(json!({
                "eventId": format!("dsh-compaction-aborted-{compaction_id}"),
                "turn": turn,
                "kind": "compaction/aborted",
                "compactionId": compaction_id,
                "effectiveHistory": history,
                "reason": "durable rollout matches its pre-compaction anchor; checkpoint absent",
            }))
        }
        DshCompactionCheckpointStatus::Conflict => Err(anyhow!(
            "DSH compaction {compaction_id} checkpoint conflicts with pending audit"
        )),
    }
}

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

    #[cfg(test)]
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
    created_pending: Mutex<HashSet<(ThreadId, String)>>,
}

impl DshAuditManager {
    pub(crate) fn new(root: PathBuf, outgoing: Arc<OutgoingMessageSender>) -> Self {
        Self {
            root,
            outgoing,
            locks: Mutex::new(HashMap::new()),
            created_pending: Mutex::new(HashSet::new()),
        }
    }

    /// Reconcile pending facts, persist this turn's configuration, and wait for its ACK.
    pub(crate) async fn before_turn(
        &self,
        request_id: &ConnectionRequestId,
        thread_id: ThreadId,
        thread: &CodexThread,
        turn: &DshAuditTurn,
        config: Value,
    ) -> Result<()> {
        self.append_fact_impl(
            request_id,
            thread_id,
            Some(thread),
            json!({
                "eventId": format!("dsh-turn-config-{}", turn.turn),
                "turn": turn.turn,
                "kind": "context/config",
                "config": { "kind": "inline", "value": config },
            }),
        )
        .await
    }

    /// Persist one fork fact and wait for DSH to durably acknowledge it.
    /// A retry with the same event ID must have identical content.
    pub(crate) async fn append_fact(
        &self,
        request_id: &ConnectionRequestId,
        thread_id: ThreadId,
        fact: Value,
    ) -> Result<()> {
        self.append_fact_impl(request_id, thread_id, None, fact)
            .await
    }

    async fn append_fact_impl(
        &self,
        request_id: &ConnectionRequestId,
        thread_id: ThreadId,
        recovery_thread: Option<&CodexThread>,
        fact: Value,
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
        if fact["kind"] == "compaction/pending" {
            let compaction_id = fact["compactionId"]
                .as_str()
                .ok_or_else(|| anyhow!("DSH compaction pending fact has no compactionId"))?;
            self.created_pending
                .lock()
                .await
                .insert((thread_id, compaction_id.to_string()));
        }
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
        if let Some(thread) = recovery_thread
            && let Some(pending) = spool
                .events
                .last()
                .filter(|event| event["kind"] == "compaction/pending")
        {
            let compaction_id = pending["compactionId"]
                .as_str()
                .ok_or_else(|| anyhow!("pending DSH compaction has no compactionId"))?;
            let effective_history = pending["effectiveHistory"]["value"]
                .as_array()
                .ok_or_else(|| anyhow!("pending DSH compaction history is not inline JSON"))?;
            let status = thread
                .dsh_compaction_checkpoint_status(compaction_id, &json!(effective_history))
                .await
                .context("read durable DSH compaction checkpoint")?;
            let prior_rollout_sha256 = if status == DshCompactionCheckpointStatus::Absent {
                Some(
                    thread
                        .dsh_prior_rollout_sha256()
                        .await
                        .context("read prior DSH compaction rollout")?,
                )
            } else {
                None
            };
            let created_this_process = self
                .created_pending
                .lock()
                .await
                .contains(&(thread_id, compaction_id.to_string()));
            let resolution = resolved_compaction_fact(
                pending,
                status,
                prior_rollout_sha256.as_deref(),
                created_this_process,
            )?;
            spool = tokio::task::spawn_blocking(move || {
                spool.record(resolution)?;
                Ok::<_, anyhow::Error>(spool)
            })
            .await??;
            spool.reconcile(exchange).await?;
        }
        tokio::task::spawn_blocking(move || {
            spool.record(fact)?;
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

    fn pending_compaction() -> Value {
        json!({
            "eventId": "dsh-compaction-pending-compact-1",
            "turn": 1,
            "kind": "compaction/pending",
            "compactionId": "compact-1",
            "effectiveHistory": { "kind": "inline", "value": [{ "item": "summary" }] },
            "priorRolloutSha256": "a".repeat(64),
        })
    }

    #[test]
    fn pending_compaction_resolution_requires_durable_evidence() -> Result<()> {
        let pending = pending_compaction();
        let installed = resolved_compaction_fact(
            &pending,
            DshCompactionCheckpointStatus::Installed,
            None,
            true,
        )?;
        assert_eq!(installed["kind"], "compaction/installed");
        assert_eq!(installed["effectiveHistory"], pending["effectiveHistory"]);
        let aborted = resolved_compaction_fact(
            &pending,
            DshCompactionCheckpointStatus::Absent,
            Some(&"a".repeat(64)),
            false,
        )?;
        assert_eq!(aborted["kind"], "compaction/aborted");
        assert_eq!(aborted["effectiveHistory"], pending["effectiveHistory"]);
        for (candidate, status, anchor, same_process) in [
            (
                &pending,
                DshCompactionCheckpointStatus::Absent,
                Some("different"),
                false,
            ),
            (
                &pending,
                DshCompactionCheckpointStatus::Absent,
                Some("a"),
                true,
            ),
            (
                &pending,
                DshCompactionCheckpointStatus::Conflict,
                None,
                false,
            ),
        ] {
            assert!(resolved_compaction_fact(candidate, status, anchor, same_process).is_err());
        }
        let mut legacy = pending.clone();
        legacy
            .as_object_mut()
            .expect("pending object")
            .remove("priorRolloutSha256");
        assert!(
            resolved_compaction_fact(
                &legacy,
                DshCompactionCheckpointStatus::Absent,
                Some(&"a".repeat(64)),
                false,
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_recovery_replays_installation_before_next_turn_after_lost_acks() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let thread_id = Uuid::now_v7().to_string();
        let remote = Arc::new(StdMutex::new(Remote {
            lose_first_ack: true,
            ..Default::default()
        }));
        let pending = pending_compaction();
        let mut spool = AuditSpool::open(root.path(), &thread_id)?;
        spool.record(pending.clone())?;
        assert!(
            spool
                .reconcile(exchange(Arc::clone(&remote)))
                .await
                .is_err()
        );
        drop(spool);

        let mut resumed = AuditSpool::open(root.path(), &thread_id)?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        resumed.record(resolved_compaction_fact(
            &pending,
            DshCompactionCheckpointStatus::Installed,
            None,
            false,
        )?)?;
        remote.lock().expect("remote lock").lose_first_ack = true;
        assert!(
            resumed
                .reconcile(exchange(Arc::clone(&remote)))
                .await
                .is_err()
        );
        drop(resumed);

        let mut reconnected = AuditSpool::open(root.path(), &thread_id)?;
        reconnected.reconcile(exchange(Arc::clone(&remote))).await?;
        reconnected.record_context(
            &DshAuditTurn {
                session_id: "session-1".into(),
                turn: 2,
            },
            json!({"model": "next"}),
        )?;
        reconnected.reconcile(exchange(Arc::clone(&remote))).await?;
        assert_eq!(
            reconnected
                .events
                .iter()
                .map(|event| event["kind"].as_str())
                .collect::<Vec<_>>(),
            vec![
                Some("compaction/pending"),
                Some("compaction/installed"),
                Some("context/config")
            ]
        );
        let remote = remote.lock().expect("remote lock");
        assert_eq!(remote.sequence, 3);
        assert_eq!(remote.appends, 3);
        Ok(())
    }

    #[tokio::test]
    async fn pending_recovery_aborts_unchanged_rollout_before_next_turn() -> Result<()> {
        let root = tempfile::tempdir()?;
        let thread_id = Uuid::now_v7().to_string();
        let remote = Arc::new(StdMutex::new(Remote::default()));
        let pending = pending_compaction();
        let mut spool = AuditSpool::open(root.path(), &thread_id)?;
        spool.record(pending.clone())?;
        spool.reconcile(exchange(Arc::clone(&remote))).await?;
        drop(spool);

        let mut resumed = AuditSpool::open(root.path(), &thread_id)?;
        resumed.record(resolved_compaction_fact(
            &pending,
            DshCompactionCheckpointStatus::Absent,
            Some(&"a".repeat(64)),
            false,
        )?)?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        resumed.record_context(
            &DshAuditTurn {
                session_id: "session-1".into(),
                turn: 2,
            },
            json!({"model": "next"}),
        )?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        assert_eq!(resumed.events[1]["kind"], "compaction/aborted");
        assert_eq!(remote.lock().expect("remote lock").sequence, 3);
        Ok(())
    }

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

    #[tokio::test]
    async fn arbitrary_model_facts_replay_in_order_without_duplicate_execution() -> Result<()> {
        let root = tempfile::tempdir()?;
        let thread_id = Uuid::now_v7().to_string();
        let remote = Arc::new(StdMutex::new(Remote::default()));
        let request = json!({
            "eventId": "model-request-1",
            "turn": 1,
            "kind": "model/request",
            "attemptId": "attempt-1",
            "purpose": "inference",
            "stage": "model-visible",
            "apiFormat": "responses",
            "request": { "kind": "inline", "value": { "input": ["full prompt"] } },
        });
        let result = json!({
            "eventId": "model-result-1",
            "turn": 1,
            "kind": "model/result",
            "attemptId": "attempt-1",
            "purpose": "inference",
            "stage": "codex",
            "result": { "kind": "inline", "value": { "output": ["answer"] } },
        });
        let mut spool = AuditSpool::open(root.path(), &thread_id)?;
        spool.record(request.clone())?;
        remote.lock().expect("remote lock").lose_first_ack = true;
        assert!(
            spool
                .reconcile(exchange(Arc::clone(&remote)))
                .await
                .is_err()
        );
        drop(spool);

        let mut resumed = AuditSpool::open(root.path(), &thread_id)?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        resumed.record(request.clone())?;
        assert!(
            resumed
                .record(json!({ "eventId": "model-request-1", "changed": true }))
                .is_err()
        );
        resumed.record(result)?;
        resumed.reconcile(exchange(Arc::clone(&remote))).await?;
        assert_eq!(resumed.events.len(), 2);
        assert_eq!(resumed.events[0]["kind"], "model/request");
        assert_eq!(resumed.events[1]["kind"], "model/result");
        let remote = remote.lock().expect("remote lock");
        assert_eq!(remote.sequence, 2);
        assert_eq!(remote.appends, 2);
        Ok(())
    }
}
