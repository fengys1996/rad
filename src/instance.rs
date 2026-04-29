use std::{
    path::PathBuf,
    process::Stdio,
    sync::atomic::Ordering,
    sync::{
        Arc, RwLock,
        atomic::{AtomicI64, AtomicU32, AtomicU64},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use serde_json::{Number, Value, map::Map};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{
        Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    time::{self, Duration, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::protocol::{LspPacket, LspPacketDecoder};

const INSTANCE_IDLE_TIMEOUT_SECS: i64 = 5 * 60;
const REAPER_INTERVAL_SECS: u64 = 30;

pub struct LspServerInstanceManager {
    instances: DashMap<InstanceKey, Arc<LspServerInstance>>,
}

impl Default for LspServerInstanceManager {
    fn default() -> Self {
        Self {
            instances: DashMap::new(),
        }
    }
}

impl LspServerInstanceManager {
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

    pub fn reply_initialize_from_cache(
        &self,
        key: &InstanceKey,
        client_id: u32,
        packet: &LspPacket,
    ) -> bool {
        let Some(instance) = self.instances.get(key) else {
            return false;
        };
        instance.reply_initialize_from_cache(client_id, packet)
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
    router: RequestRouter,
}

pub struct ClientMessage {
    pub client_id: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct ClientHandle {
    pub id: u32,
    pub tx: Sender<Vec<u8>>,
}

#[derive(Clone)]
struct PendingRequest {
    client_id: u32,
    client_local_id: JsonRpcId,
    method: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum JsonRpcId {
    Number(i64),
    String(String),
}

impl JsonRpcId {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Number(num) => num.as_i64().map(Self::Number),
            Value::String(s) => Some(Self::String(s.clone())),
            _ => None,
        }
    }

    fn to_value(&self) -> Value {
        match self {
            Self::Number(n) => Value::Number(Number::from(*n)),
            Self::String(s) => Value::String(s.clone()),
        }
    }
}

#[derive(Clone)]
struct RequestRouter {
    next_global_id: Arc<AtomicU64>,
    pending_by_global: Arc<DashMap<JsonRpcId, PendingRequest>>,
    pending_by_client_local: Arc<DashMap<(u32, JsonRpcId), JsonRpcId>>,
    initialize_result: Arc<RwLock<Option<Value>>>,
}

impl RequestRouter {
    fn new() -> Self {
        Self {
            next_global_id: Arc::new(AtomicU64::new(1)),
            pending_by_global: Arc::new(DashMap::new()),
            pending_by_client_local: Arc::new(DashMap::new()),
            initialize_result: Arc::new(RwLock::new(None)),
        }
    }

    fn rewrite_client_packet(
        &self,
        client_id: u32,
        packet: LspPacket,
        pid: u32,
    ) -> (Vec<u8>, usize) {
        let Some(mut json) = packet.parse_json() else {
            let bytes = packet.to_bytes();
            let len = bytes.len();
            return (bytes, len);
        };

        let Some(obj) = json.as_object_mut() else {
            let bytes = packet.to_bytes();
            let len = bytes.len();
            return (bytes, len);
        };

        self.remap_client_request_id(client_id, obj, pid);
        self.remap_cancel_request(client_id, obj, pid);

        match serde_json::to_vec(&json) {
            Ok(body) => {
                let bytes = LspPacket::from_body(body).to_bytes();
                let len = bytes.len();
                (bytes, len)
            }
            Err(err) => {
                warn!(pid, client_id, error = %err, "failed to re-serialize client packet");
                let bytes = packet.to_bytes();
                let len = bytes.len();
                (bytes, len)
            }
        }
    }

    fn remap_client_request_id(&self, client_id: u32, obj: &mut Map<String, Value>, pid: u32) {
        if !is_request(obj) {
            return;
        }

        let Some(local_id) = obj.get("id").and_then(JsonRpcId::from_value) else {
            return;
        };

        let global_raw = self.next_global_id.fetch_add(1, Ordering::Relaxed) as i64;
        let global_id = JsonRpcId::Number(global_raw);

        self.pending_by_global.insert(
            global_id.clone(),
            PendingRequest {
                client_id,
                client_local_id: local_id.clone(),
                method: obj
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
        );
        self.pending_by_client_local
            .insert((client_id, local_id.clone()), global_id.clone());

        obj.insert("id".to_string(), global_id.to_value());

        debug!(
            pid,
            client_id,
            local_id = ?local_id,
            global_id = global_raw,
            "remapped client request id"
        );
    }

    fn remap_cancel_request(&self, client_id: u32, obj: &mut Map<String, Value>, pid: u32) {
        let Some(Value::String(method)) = obj.get("method") else {
            return;
        };

        if method != "$/cancelRequest" {
            return;
        }

        let Some(cancel_id) = obj
            .get("params")
            .and_then(Value::as_object)
            .and_then(|params| params.get("id"))
            .and_then(JsonRpcId::from_value)
        else {
            return;
        };

        let Some(global_id) = self
            .pending_by_client_local
            .get(&(client_id, cancel_id.clone()))
            .map(|entry| entry.value().clone())
        else {
            debug!(
                pid,
                client_id,
                cancel_id = ?cancel_id,
                "cancel request id not found in mapping"
            );
            return;
        };

        if let Some(params) = obj.get_mut("params").and_then(Value::as_object_mut) {
            params.insert("id".to_string(), global_id.to_value());
        }

        debug!(
            pid,
            client_id,
            cancel_id = ?cancel_id,
            mapped_id = ?global_id,
            "rewrote cancel request id"
        );
    }

    fn rewrite_ra_packet(
        &self,
        packet: LspPacket,
        active_client_id: u32,
        pid: u32,
    ) -> RoutedPacket {
        let Some(mut json) = packet.parse_json() else {
            return RoutedPacket {
                client_id: active_client_id,
                bytes: packet.to_bytes(),
            };
        };

        let Some(obj) = json.as_object_mut() else {
            return RoutedPacket {
                client_id: active_client_id,
                bytes: packet.to_bytes(),
            };
        };

        if is_response(obj)
            && let Some(global_id) = obj.get("id").and_then(JsonRpcId::from_value)
            && let Some((_, pending)) = self.pending_by_global.remove(&global_id)
        {
            if pending.method == "initialize"
                && let Some(result) = obj.get("result").cloned()
                && let Ok(mut slot) = self.initialize_result.write()
            {
                *slot = Some(result);
            }

            self.pending_by_client_local
                .remove(&(pending.client_id, pending.client_local_id.clone()));
            obj.insert("id".to_string(), pending.client_local_id.to_value());

            let bytes = serde_json::to_vec(&json)
                .map(LspPacket::from_body)
                .map(|pkt| pkt.to_bytes())
                .unwrap_or_else(|_| packet.to_bytes());

            debug!(
                pid,
                client_id = pending.client_id,
                global_id = ?global_id,
                local_id = ?pending.client_local_id,
                "restored response id for client"
            );

            return RoutedPacket {
                client_id: pending.client_id,
                bytes,
            };
        }

        RoutedPacket {
            client_id: active_client_id,
            bytes: packet.to_bytes(),
        }
    }

    fn initialize_response_from_cache(&self, request_packet: &LspPacket) -> Option<Vec<u8>> {
        let request = request_packet.parse_json()?;
        let request_obj = request.as_object()?;
        if request_obj.get("method").and_then(Value::as_str)? != "initialize" {
            return None;
        }

        let request_id = request_obj.get("id")?.clone();
        let result = self.initialize_result.read().ok()?.clone()?;
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result,
        });
        let body = serde_json::to_vec(&response).ok()?;
        Some(LspPacket::from_body(body).to_bytes())
    }
}

struct RoutedPacket {
    client_id: u32,
    bytes: Vec<u8>,
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
        let router = RequestRouter::new();

        tokio::spawn(pump_client_to_ra(
            rx,
            stdin,
            cancel.clone(),
            pid,
            last_used.clone(),
            active_client_id.clone(),
            router.clone(),
        ));
        tokio::spawn(pump_ra_to_active_client(
            stdout,
            cancel.clone(),
            pid,
            active_client_id.clone(),
            clients.clone(),
            router.clone(),
        ));

        LspServerInstance {
            pid,
            process: Mutex::new(proc),
            lsp_tx: tx,
            cancel,
            last_used,
            active_client_id,
            clients,
            router,
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

    pub fn reply_initialize_from_cache(&self, client_id: u32, packet: &LspPacket) -> bool {
        let Some(bytes) = self.router.initialize_response_from_cache(packet) else {
            return false;
        };

        let Some(client_tx) = self.clients.get(&client_id).map(|handle| handle.tx.clone()) else {
            return false;
        };

        self.set_active_client(client_id);
        match client_tx.try_send(bytes) {
            Ok(()) => {
                info!(
                    client_id,
                    pid = self.pid,
                    "replied initialize from cached capability set"
                );
                true
            }
            Err(err) => {
                warn!(
                    client_id,
                    pid = self.pid,
                    error = %err,
                    "failed to send cached initialize response"
                );
                false
            }
        }
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

async fn pump_client_to_ra(
    mut rx: Receiver<ClientMessage>,
    mut ra_stdin: ChildStdin,
    cancel: CancellationToken,
    pid: u32,
    last_used: Arc<AtomicI64>,
    active_client_id: Arc<AtomicU32>,
    router: RequestRouter,
) {
    info!(pid, "started pump_client_to_ra task");

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    debug!(pid, "client message channel closed");
                    break;
                };

                active_client_id.store(msg.client_id, Ordering::Relaxed);
                last_used.store(now_ts(), Ordering::Relaxed);
                let original_len = msg.bytes.len();

                let (rewritten, size) = match decode_single_packet(&msg.bytes) {
                    Ok(Some(packet)) => router.rewrite_client_packet(msg.client_id, packet, pid),
                    Ok(None) => (msg.bytes, original_len),
                    Err(err) => {
                        warn!(pid, client_id = msg.client_id, error = %err, "invalid client packet, forwarding raw bytes");
                        (msg.bytes, original_len)
                    }
                };

                debug!(
                    pid,
                    client_id = msg.client_id,
                    bytes = size,
                    "forwarding client message to rust-analyzer"
                );

                if let Err(err) = ra_stdin.write_all(&rewritten).await {
                    error!("failed to forward message to rust-analyzer pid {} stdin: {err}", pid);
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

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

async fn pump_ra_to_active_client(
    mut ra_stdout: ChildStdout,
    cancel: CancellationToken,
    pid: u32,
    active_client_id: Arc<AtomicU32>,
    clients: Arc<DashMap<u32, ClientHandle>>,
    router: RequestRouter,
) {
    let mut read_buf = vec![0; 8192];
    let mut decoder = LspPacketDecoder::default();
    info!(pid, "started pump_ra_to_active_client task");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!(pid, "pump_ra_to_active_client cancelled");
                break;
            }
            read = ra_stdout.read(&mut read_buf) => {
                let n = match read {
                    Ok(0) => {
                        debug!(pid, "rust-analyzer stdout closed");
                        break;
                    }
                    Ok(n) => n,
                    Err(err) => {
                        error!("failed to read rust-analyzer pid {} stdout: {err}", pid);
                        break;
                    }
                };

                decoder.push(&read_buf[..n]);
                loop {
                    let packet = match decoder.next_packet() {
                        Ok(Some(packet)) => packet,
                        Ok(None) => break,
                        Err(err) => {
                            error!(pid, error = %err, "failed to parse rust-analyzer packet");
                            break;
                        }
                    };

                    let active = active_client_id.load(Ordering::Relaxed);
                    let routed = router.rewrite_ra_packet(packet, active, pid);

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
            }
        }
    }
}

fn decode_single_packet(bytes: &[u8]) -> std::io::Result<Option<LspPacket>> {
    let mut decoder = LspPacketDecoder::default();
    decoder.push(bytes);
    decoder.next_packet()
}

fn is_request(obj: &Map<String, Value>) -> bool {
    obj.contains_key("method") && obj.contains_key("id")
}

fn is_response(obj: &Map<String, Value>) -> bool {
    obj.contains_key("id") && (obj.contains_key("result") || obj.contains_key("error"))
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
        let router = RequestRouter::new();

        let req = LspPacket::from_body(
            br#"{"jsonrpc":"2.0","id":1,"method":"textDocument/hover"}"#.to_vec(),
        );
        let (rewritten, _) = router.rewrite_client_packet(3, req, 100);
        let req_packet = decode_single_packet(&rewritten)
            .unwrap()
            .expect("rewritten request packet should parse");
        let req_json = req_packet.parse_json().expect("request json");
        let mapped = req_json["id"].as_i64().expect("mapped id should be i64");

        let resp = LspPacket::from_body(
            format!("{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":null}}", mapped).into_bytes(),
        );
        let routed = router.rewrite_ra_packet(resp, 9, 100);
        let resp_packet = decode_single_packet(&routed.bytes)
            .unwrap()
            .expect("rewritten response packet should parse");
        let resp_json = resp_packet.parse_json().expect("response json");

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
