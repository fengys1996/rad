use std::{
    path::PathBuf,
    process::Stdio,
    sync::atomic::Ordering,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU32},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use dashmap::DashMap;
use serde_json::Value;
use tokio::{
    io::AsyncWriteExt,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{
        Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    time::{self, Duration, MissedTickBehavior},
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::error::Result;
use crate::{
    mapper::ReqIdMapper,
    protocol::{LspFrame, LspFrameDecoder, LspFrameStream},
};

const INSTANCE_IDLE_TIMEOUT_SECS: i64 = 5 * 60;
const REAPER_INTERVAL_SECS: u64 = 30;

pub struct InstanceManager {
    instances: DashMap<InstanceKey, LspServerInstanceRef>,
}

pub type InstanceManagerRef = Arc<InstanceManager>;

impl Default for InstanceManager {
    fn default() -> Self {
        Self {
            instances: DashMap::new(),
        }
    }
}

impl InstanceManager {
    pub fn start_reaper(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(REAPER_INTERVAL_SECS));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;
                self.reap_idle_instances().await;
            }
        });
    }

    async fn reap_idle_instances(&self) {
        let now = now_ts();
        let mut to_remove = Vec::new();

        for entry in self.instances.iter() {
            let instance = entry.value();
            let inactive_secs = now - instance.last_used.load(Ordering::Relaxed);
            if instance.clients.is_empty() && inactive_secs >= INSTANCE_IDLE_TIMEOUT_SECS {
                to_remove.push((entry.key().clone(), instance.clone(), inactive_secs));
            }
        }

        for (key, instance, inactive_secs) in to_remove {
            if self.instances.remove(&key).is_some() {
                info!(
                    workspace = %key.workspace,
                    pid = instance.pid,
                    inactive_secs,
                    "reaping idle lsp instance"
                );
                instance.shutdown().await;
            }
        }
    }

    pub async fn spawn_instance(
        &self,
        client_id: u32,
        client_tx: Sender<Vec<u8>>,
        key: &InstanceKey,
    ) -> bool {
        let mut reused = false;
        let instance = if let Some(existing) = self.instances.get(key).map(|entry| entry.clone()) {
            if existing.is_healthy().await {
                debug!(
                    workspace = %key.workspace,
                    client_id,
                    "reusing existing lsp instance"
                );
                reused = true;
                existing
            } else {
                if let Some((_, stale)) = self.instances.remove(key) {
                    info!(
                        workspace = %key.workspace,
                        pid = stale.pid,
                        "dropping unhealthy lsp instance before re-spawn"
                    );
                    stale.shutdown().await;
                }
                let instance = Arc::new(LspServerInstance::new(key.workspace_dir()));
                info!(
                    workspace = %key.workspace,
                    pid = instance.pid,
                    "spawned new lsp instance"
                );
                self.instances.insert(key.clone(), instance.clone());
                instance
            }
        } else {
            let instance = Arc::new(LspServerInstance::new(key.workspace_dir()));
            info!(
                workspace = %key.workspace,
                pid = instance.pid,
                "spawned new lsp instance"
            );
            self.instances.insert(key.clone(), instance.clone());
            instance
        };

        instance.add_client(ClientHandle {
            id: client_id,
            tx: client_tx,
        });
        instance.set_active_client(client_id);
        info!(
            workspace = %key.workspace,
            client_id,
            pid = instance.pid,
            "attached client to lsp instance"
        );
        reused
    }

    pub fn send_to_instance(&self, key: &InstanceKey, client_id: u32, msg: Vec<u8>) {
        let Some(instance) = self.instances.get(key) else {
            error!(
                "failed to send message to instance for workspace {}: instance not found",
                key.workspace
            );
            return;
        };

        debug!(
            workspace = %key.workspace,
            client_id,
            pid = instance.pid,
            bytes = msg.len(),
            "sending client message to lsp instance"
        );

        if let Err(err) = instance.sender().try_send(ClientMessage {
            client_id,
            bytes: msg,
        }) {
            error!(
                "failed to send message to instance for workspace {}: {}",
                key.workspace, err
            );
        }
    }

    pub fn remove_client(&self, key: &InstanceKey, client_id: u32) {
        let Some(instance) = self.instances.get(key) else {
            return;
        };

        instance.remove_client(client_id);
        info!(
            workspace = %key.workspace,
            client_id,
            pid = instance.pid,
            "removed client from lsp instance"
        );
    }

    pub fn client_count(&self, key: &InstanceKey) -> usize {
        self.instances
            .get(key)
            .map(|instance| instance.clients.len())
            .unwrap_or(0)
    }

    pub fn build_initialize_response_from_cache(
        &self,
        key: &InstanceKey,
        request_id: Value,
    ) -> Option<Vec<u8>> {
        let instance = self.instances.get(key)?;
        instance.build_initialize_response_from_cache(request_id)
    }
}

pub struct LspServerInstance {
    pid: u32,
    process: Mutex<Child>,
    lsp_tx: Sender<ClientMessage>,
    cancel: CancellationToken,
    last_used: Arc<AtomicI64>,
    active_client_id: Arc<AtomicU32>,
    clients: Arc<DashMap<u32, ClientHandle>>,
    req_id_mapper: ReqIdMapper,
}

pub type LspServerInstanceRef = Arc<LspServerInstance>;

pub struct ClientMessage {
    pub client_id: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct ClientHandle {
    pub id: u32,
    pub tx: Sender<Vec<u8>>,
}

impl LspServerInstance {
    pub fn new(workspace_dir: Option<PathBuf>) -> LspServerInstance {
        let mut command = Command::new("rust-analyzer");
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some(dir) = workspace_dir {
            command.current_dir(dir);
        }
        let mut proc = command.spawn().unwrap();

        let pid = proc.id().unwrap_or_default();
        let stdin = proc.stdin.take().expect("rust-analyzer stdin is piped");
        let stdout = proc.stdout.take().expect("rust-analyzer stdout is piped");
        let (tx, rx) = channel(32);
        let cancel = CancellationToken::new();
        let last_used = Arc::new(AtomicI64::new(now_ts()));
        let active_client_id = Arc::new(AtomicU32::new(0));
        let clients = Arc::new(DashMap::new());
        let req_id_mapper = ReqIdMapper::new();

        tokio::spawn(forward_client_to_ra(
            rx,
            stdin,
            cancel.clone(),
            pid,
            last_used.clone(),
            active_client_id.clone(),
            req_id_mapper.clone(),
        ));
        tokio::spawn(forward_ra_to_active_client(
            stdout,
            cancel.clone(),
            pid,
            active_client_id.clone(),
            clients.clone(),
            req_id_mapper.clone(),
        ));

        LspServerInstance {
            pid,
            process: Mutex::new(proc),
            lsp_tx: tx,
            cancel,
            last_used,
            active_client_id,
            clients,
            req_id_mapper,
        }
    }

    pub fn add_client(&self, client: ClientHandle) {
        let client_id = client.id;
        self.clients.insert(client.id, client);
        info!(client_id, pid = self.pid, "registered client handle");
    }

    pub fn sender(&self) -> Sender<ClientMessage> {
        self.lsp_tx.clone()
    }

    pub fn remove_client(&self, client_id: u32) -> Option<(u32, ClientHandle)> {
        let removed = self.clients.remove(&client_id);

        if removed.is_none() {
            warn!(
                client_id,
                pid = self.pid,
                "client handle not found during removal"
            );
        }

        removed
    }

    pub fn set_active_client(&self, client_id: u32) {
        self.active_client_id.store(client_id, Ordering::Relaxed);
    }

    pub fn active_client(&self) -> Option<ClientHandle> {
        let client_id = self.active_client_id.load(Ordering::Relaxed);
        self.clients.get(&client_id).map(|client| client.clone())
    }

    pub async fn is_healthy(&self) -> bool {
        if self.lsp_tx.is_closed() {
            return false;
        }

        let mut process = self.process.lock().await;
        match process.try_wait() {
            Ok(None) => true,
            Ok(Some(status)) => {
                warn!(
                    pid = self.pid,
                    ?status,
                    "lsp instance process already exited"
                );
                false
            }
            Err(err) => {
                warn!(pid = self.pid, error = %err, "failed to check lsp instance process state");
                false
            }
        }
    }

    pub fn build_initialize_response_from_cache(&self, request_id: Value) -> Option<Vec<u8>> {
        self.req_id_mapper
            .initialize_response_from_cache(request_id)
    }

    pub async fn shutdown(&self) {
        self.cancel.cancel();
        info!(pid = self.pid, "shutting down lsp instance");

        let mut process = self.process.lock().await;

        if let Err(e) = process.kill().await {
            error!("failed to kill rust-analyzer, pid {}, error: {e}", self.pid);
        }

        if let Err(err) = process.wait().await {
            error!(
                "failed to reap rust-analyzer, pid {}, error: {err}",
                self.pid
            );
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct InstanceKey {
    workspace: String,
}

impl InstanceKey {
    pub fn new(workspace: impl Into<String>) -> Self {
        Self {
            workspace: workspace.into(),
        }
    }

    fn workspace_dir(&self) -> Option<PathBuf> {
        if let Some(path) = self.workspace.strip_prefix("file://") {
            if path.starts_with('/') {
                return Some(PathBuf::from(path));
            }
            if let Some(idx) = path.find('/') {
                return Some(PathBuf::from(&path[idx..]));
            }
            return None;
        }

        if self.workspace.starts_with('/') {
            return Some(PathBuf::from(&self.workspace));
        }

        None
    }
}

async fn forward_client_to_ra(
    mut rx: Receiver<ClientMessage>,
    mut ra_stdin: ChildStdin,
    cancel: CancellationToken,
    pid: u32,
    last_used: Arc<AtomicI64>,
    active_client_id: Arc<AtomicU32>,
    req_id_mapper: ReqIdMapper,
) {
    info!(pid, "started pump_client_to_ra task");

    loop {
        tokio::select! {
            msg = rx.recv() => {
                if handle_client_msg_to_ra(
                    msg,
                    &mut ra_stdin,
                    pid,
                    &last_used,
                    &active_client_id,
                    &req_id_mapper,
                ).await {
                    break;
                }
            }
            _ = cancel.cancelled() => {
                debug!(pid, "pump_client_to_ra cancelled");
                break;
            }
        }
    }

    if let Err(err) = ra_stdin.shutdown().await {
        error!("failed to shutdown rust-analyzer pid {} stdin: {err}", pid);
    }
}

async fn handle_client_msg_to_ra(
    msg: Option<ClientMessage>,
    ra_stdin: &mut ChildStdin,
    pid: u32,
    last_used: &AtomicI64,
    active_client_id: &AtomicU32,
    req_id_mapper: &ReqIdMapper,
) -> bool {
    let Some(msg) = msg else {
        debug!(pid, "client message channel closed");
        return true;
    };

    active_client_id.store(msg.client_id, Ordering::Relaxed);
    last_used.store(now_ts(), Ordering::Relaxed);
    // TODO: optimize
    let rewritten = match decode_single_packet(&msg.bytes) {
        Ok(Some(frame)) => match req_id_mapper
            .rewrite_client_packet(msg.client_id, frame, pid)
            .to_bytes()
        {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(pid, client_id = msg.client_id, error = %err, "failed to encode rewritten client frame, forwarding raw bytes");
                msg.bytes
            }
        },
        Ok(None) => msg.bytes,
        Err(err) => {
            warn!(pid, client_id = msg.client_id, error = %err, "invalid client frame, forwarding raw bytes");
            msg.bytes
        }
    };

    debug!(
        pid,
        client_id = msg.client_id,
        bytes = rewritten.len(),
        "forwarding client message to rust-analyzer"
    );

    if let Err(err) = ra_stdin.write_all(&rewritten).await {
        error!(
            "failed to forward message to rust-analyzer pid {} stdin: {err}",
            pid
        );
        return true;
    }

    false
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

async fn forward_ra_to_active_client(
    ra_stdout: ChildStdout,
    cancel: CancellationToken,
    pid: u32,
    active_client_id: Arc<AtomicU32>,
    clients: Arc<DashMap<u32, ClientHandle>>,
    req_id_mapper: ReqIdMapper,
) {
    let mut frame_stream = LspFrameStream::new(ra_stdout, LspFrameDecoder);
    info!(pid, "started pump_ra_to_active_client task");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!(pid, "pump_ra_to_active_client cancelled");
                break;
            }
            frame = frame_stream.next() => {
                match frame {
                    Some(Ok(frame)) => {
                        let active = active_client_id.load(Ordering::Relaxed);
                        let routed = match req_id_mapper.rewrite_ra_packet(frame, active, pid) {
                            Ok(routed) => routed,
                            Err(e) => {
                                warn!(pid, error = %e, "failed to rewrite rust-analyzer frame, forwarding raw bytes");
                                continue;
                            },
                        };

                        if routed.client_id == 0 {
                            warn!(pid, bytes = routed.bytes.len(), "dropping packet without active client");
                            continue;
                        }

                        let client_tx = clients.get(&routed.client_id).map(|client| client.tx.clone());
                        if let Some(tx) = client_tx {
                            if let Err(err) = tx.send(routed.bytes).await {
                                error!(
                                    "failed to forward rust-analyzer pid {} output to client {}: {}",
                                    pid,
                                    routed.client_id,
                                    err
                                );
                            }
                        } else {
                            warn!(
                                pid,
                                client_id = routed.client_id,
                                "dropping rust-analyzer packet because target client is not registered"
                            );
                        }
                    }
                    Some(Err(err)) => {
                        error!(pid, error = %err, "failed to parse rust-analyzer packet");
                        break;
                    }
                    None => {
                        debug!(pid, "rust-analyzer stdout closed");
                        break;
                    }
                }
            }
        }
    }
}

fn decode_single_packet(bytes: &[u8]) -> Result<Option<LspFrame>> {
    let mut decoder = LspFrameDecoder;
    let mut src = BytesMut::new();
    src.extend_from_slice(bytes);
    tokio_util::codec::Decoder::decode(&mut decoder, &mut src)
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[tokio::test]
    async fn shutdown_exits_rust_analyzer_process() {
        init_tracing();

        if !ra_available() {
            panic!(
                "skipping shutdown_exits_rust_analyzer_process: rust-analyzer not available in PATH"
            );
        }

        let instance = LspServerInstance::new(None);

        instance.shutdown().await;

        let _status = instance
            .process
            .lock()
            .await
            .try_wait()
            .expect("try_wait should succeed")
            .expect("rust-analyzer should be exited after shutdown");
    }

    #[test]
    fn rewrites_request_id_and_restores_response() {
        let req_id_mapper = ReqIdMapper::new();

        let req = LspFrame::new(
            serde_json::from_slice(br#"{"jsonrpc":"2.0","id":1,"method":"textDocument/hover"}"#)
                .expect("valid request json"),
        );
        let rewritten = req_id_mapper.rewrite_client_packet(3, req, 100);
        let req_json = rewritten.as_json();
        let mapped = req_json["id"].as_i64().expect("mapped id should be i64");

        let resp = LspFrame::new(
            serde_json::from_str(&format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":null}}",
                mapped
            ))
            .expect("valid response json"),
        );
        let routed = req_id_mapper.rewrite_ra_packet(resp, 9, 100).unwrap();
        let resp_packet = decode_single_packet(&routed.bytes)
            .unwrap()
            .expect("rewritten response packet should parse");
        let resp_json = resp_packet.as_json();

        assert_eq!(routed.client_id, 3);
        assert_eq!(resp_json["id"], Value::Number(1.into()));
    }

    fn init_tracing() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    }

    fn ra_available() -> bool {
        Command::new("rust-analyzer")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    }
}
