use std::{
    collections::HashMap,
    env::{join_paths, split_paths},
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use dashmap::DashMap;
#[cfg(test)]
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
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

use crate::error::{PlainTextSnafu, Result};
use crate::{config::ProjectConfig, error::IoSnafu};
use crate::{
    mapper::ReqIdMapper,
    protocol::{
        ClearedInstance, InstanceStatus, LspFrame, LspFrameDecoder, LspFrameStream, LspSender,
        lsp::JsonRpcId,
    },
};

const INSTANCE_SEND_TIMEOUT_MS: u64 = 100;

#[derive(Clone)]
pub struct InstanceManager {
    instances: Arc<DashMap<InstanceKey, InstanceRef>>,
    lsp_server_path: Arc<PathBuf>,
    path_prepend: Arc<Vec<PathBuf>>,
    project_overrides: Arc<HashMap<String, ProjectConfig>>,
}

impl InstanceManager {
    pub async fn new(
        instance_timeout: Duration,
        gc_interval: Duration,
        lsp_server_path: Option<PathBuf>,
        path_prepend: Vec<PathBuf>,
        project_overrides: HashMap<String, ProjectConfig>,
    ) -> Result<Self> {
        if let Some(path) = &lsp_server_path {
            ensure_absolute_path("lsp_server_path", path)?;
        }
        for path in &path_prepend {
            ensure_absolute_path("path_prepend", path)?;
        }
        for (project_path, cfg) in project_overrides.iter() {
            if let Some(path) = &cfg.lsp_server_path {
                ensure_absolute_path(&format!("projects.{project_path}.lsp_server_path"), path)?;
            }
        }

        let instances = Arc::new(DashMap::new());
        spawn_instance_reaper(instances.clone(), instance_timeout, gc_interval).await;
        let lsp_server_path = lsp_server_path.unwrap_or_else(|| PathBuf::from("rust-analyzer"));
        Ok(Self {
            instances,
            lsp_server_path: Arc::new(lsp_server_path),
            path_prepend: Arc::new(path_prepend),
            project_overrides: Arc::new(project_overrides),
        })
    }

    fn resolve_lsp_server_path(&self, workspace_dir: Option<&Path>) -> &Path {
        let dir = match workspace_dir {
            Some(d) => d,
            None => return &self.lsp_server_path,
        };
        for (project_path, cfg) in self.project_overrides.iter() {
            if let Some(ra_path) = &cfg.lsp_server_path {
                let project_path = Path::new(project_path);
                if dir == project_path
                    || dir
                        .canonicalize()
                        .is_ok_and(|cd| project_path.canonicalize().is_ok_and(|cp| cd == cp))
                {
                    return ra_path;
                }
            }
        }
        &self.lsp_server_path
    }

    /// Detaches a client from the specified LSP instance.
    ///
    /// This only removes the client attachment from the instance.
    /// Detaches a client from the specified LSP instance.
    ///
    /// This only removes the client attachment from the instance. It does not
    /// shutdown the instance, which may continue serving other clients or remain
    /// alive until the idle reaper removes it.
    pub fn detach_client(&self, key: &InstanceKey, client_id: u32) {
        let Some(instance) = self.instances.get(key) else {
            return;
        };

        instance.detach_client(client_id);

        info!(
            workspace = %key.workspace,
            client_id,
            pid = instance.pid,
            "detach client from lsp instance"
        );
    }
}

fn ensure_absolute_path(name: &str, path: &Path) -> Result<()> {
    if path.is_absolute() {
        return Ok(());
    }

    PlainTextSnafu {
        msg: format!("{name} must be an absolute path, got {}", path.display()),
    }
    .fail()
}

/// Spawns the background task that reaps idle LSP instances.
///
/// Checks all instances on the given interval. An instance is reaped when
/// it has no attached clients and has been idle for at least the configured
/// timeout.
async fn spawn_instance_reaper(
    instances: Arc<DashMap<InstanceKey, InstanceRef>>,
    instance_timeout: Duration,
    gc_interval: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = time::interval(gc_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let idle_timeout_secs = instance_timeout.as_secs() as i64;
        loop {
            ticker.tick().await;
            reap_idle_instances(&instances, idle_timeout_secs).await;
        }
    });
}

async fn reap_idle_instances(
    instances: &DashMap<InstanceKey, InstanceRef>,
    idle_timeout_secs: i64,
) {
    let now = now_ts();
    let mut to_remove = Vec::new();

    for entry in instances.iter() {
        let instance = entry.value();
        let inactive_secs = now - instance.last_used.load(Ordering::Relaxed);
        if !instance.pinned.load(Ordering::Relaxed)
            && instance.clients.is_empty()
            && inactive_secs >= idle_timeout_secs
        {
            to_remove.push((entry.key().clone(), instance.clone(), inactive_secs));
        }
    }

    for (key, instance, inactive_secs) in to_remove {
        if instances
            .remove_if(&key, |_, current| {
                !current.pinned.load(Ordering::Relaxed) && current.clients.is_empty()
            })
            .is_some()
        {
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

impl InstanceManager {
    pub async fn spawn_instance(
        &self,
        client_id: u32,
        to_client: LspSender,
        key: &InstanceKey,
    ) -> Result<(InstanceHandle, bool)> {
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
                let lsp_server_path = self.resolve_lsp_server_path(key.workspace_dir().as_deref());
                let instance = Arc::new(Instance::new(
                    lsp_server_path,
                    &self.path_prepend,
                    key.workspace_dir(),
                )?);
                info!(
                    workspace = %key.workspace,
                    pid = instance.pid,
                    lsp_server_path = %lsp_server_path.display(),
                    "spawned new lsp instance"
                );
                self.instances.insert(key.clone(), instance.clone());
                instance
            }
        } else {
            let lsp_server_path = self.resolve_lsp_server_path(key.workspace_dir().as_deref());
            let instance = Arc::new(Instance::new(
                lsp_server_path,
                &self.path_prepend,
                key.workspace_dir(),
            )?);
            info!(
                workspace = %key.workspace,
                pid = instance.pid,
                lsp_server_path = %lsp_server_path.display(),
                "spawned new lsp instance"
            );
            self.instances.insert(key.clone(), instance.clone());
            instance
        };

        instance.attach_client(ClientHandle {
            id: client_id,
            tx: to_client,
        });
        instance.set_active_client(client_id);
        info!(
            workspace = %key.workspace,
            client_id,
            pid = instance.pid,
            "attached client to lsp instance"
        );
        Ok((
            InstanceHandle {
                key: key.clone(),
                tx: instance.lsp_tx.clone(),
            },
            reused,
        ))
    }

    pub(crate) fn build_initialize_response_from_cache(
        &self,
        key: &InstanceKey,
        id: JsonRpcId,
    ) -> Option<LspFrame> {
        let instance = self.instances.get(key)?;
        instance.build_initialize_response_from_cache(id)
    }

    pub async fn status(&self) -> Vec<InstanceStatus> {
        let instances = self
            .instances
            .iter()
            .map(|entry| (entry.key().workspace().to_string(), entry.value().clone()))
            .collect::<Vec<_>>();
        let now = now_ts();
        let mut statuses = Vec::with_capacity(instances.len());

        for (workspace, instance) in instances {
            statuses.push(InstanceStatus {
                workspace,
                pid: instance.pid,
                client_count: instance.clients.len(),
                idle_secs: now - instance.last_used.load(Ordering::Relaxed),
                healthy: instance.is_healthy().await,
                pinned: instance.pinned.load(Ordering::Relaxed),
            });
        }

        statuses
    }

    pub async fn clear_idle(&self) -> Vec<ClearedInstance> {
        let to_clear: Vec<(InstanceKey, InstanceRef)> = self
            .instances
            .iter()
            .filter(|entry| {
                entry.value().clients.is_empty() && !entry.value().pinned.load(Ordering::Relaxed)
            })
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let mut cleared = Vec::with_capacity(to_clear.len());
        for (key, instance) in to_clear {
            if self
                .instances
                .remove_if(&key, |_, current| {
                    !current.pinned.load(Ordering::Relaxed) && current.clients.is_empty()
                })
                .is_some()
            {
                info!(
                    workspace = %key.workspace,
                    pid = instance.pid,
                    "clearing idle lsp instance"
                );
                cleared.push(ClearedInstance {
                    workspace: key.workspace.clone(),
                    pid: instance.pid,
                });
                instance.shutdown().await;
            }
        }
        cleared
    }

    pub fn set_pinned(&self, pid: u32, pinned: bool) -> bool {
        let Some(instance) = self.instances.iter().find(|entry| entry.value().pid == pid) else {
            return false;
        };

        instance.pinned.store(pinned, Ordering::Relaxed);
        true
    }

    pub async fn clear_all(&self) -> Vec<ClearedInstance> {
        let all: Vec<(InstanceKey, InstanceRef)> = self
            .instances
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let mut cleared = Vec::with_capacity(all.len());
        for (key, instance) in all {
            if self.instances.remove(&key).is_some() {
                info!(
                    workspace = %key.workspace,
                    pid = instance.pid,
                    "clearing lsp instance"
                );
                cleared.push(ClearedInstance {
                    workspace: key.workspace.clone(),
                    pid: instance.pid,
                });
                instance.shutdown().await;
            }
        }
        cleared
    }
}

/// A client-session scoped reference to an already-bound LSP instance.
#[derive(Clone)]
pub struct InstanceHandle {
    key: InstanceKey,
    tx: Sender<ClientMessage>,
}

impl InstanceHandle {
    /// Returns the key of the bound LSP instance.
    pub fn key(&self) -> &InstanceKey {
        &self.key
    }

    /// Enqueues a client packet for delivery to the instance's single stdin writer.
    ///
    /// This waits briefly for queue capacity instead of failing immediately when
    /// the instance input channel is temporarily full.
    pub async fn send_with_timeout(&self, client_id: u32, bytes: Vec<u8>) -> Result<()> {
        self.tx
            .send_timeout(
                ClientMessage { client_id, bytes },
                Duration::from_millis(INSTANCE_SEND_TIMEOUT_MS),
            )
            .await
            .map_err(|err| {
                PlainTextSnafu {
                    msg: format!("failed to enqueue client message for lsp instance: {err}"),
                }
                .build()
            })
    }
}

struct Instance {
    pid: u32,
    process: Mutex<Child>,
    lsp_tx: Sender<ClientMessage>,
    cancel: CancellationToken,
    last_used: Arc<AtomicI64>,
    active_client_id: Arc<AtomicU32>,
    clients: Arc<DashMap<u32, ClientHandle>>,
    pinned: AtomicBool,
    req_id_mapper: ReqIdMapper,
}

type InstanceRef = Arc<Instance>;

impl Instance {
    fn new(
        lsp_server_path: &Path,
        path_prepend: &[PathBuf],
        workspace_dir: Option<PathBuf>,
    ) -> Result<Instance> {
        let mut command = Command::new(lsp_server_path);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if !path_prepend.is_empty() {
            prepend_paths_to_path(&mut command, path_prepend)?;
        }
        if let Some(dir) = workspace_dir {
            command.current_dir(dir);
        }
        let mut process = command.spawn().with_context(|_| IoSnafu {
            detail: format!("lsp_server_path: {}", lsp_server_path.display()),
        })?;

        let pid = process.id().context(PlainTextSnafu {
            msg: "failed to read lsp instance id, since lsp instance may have already shut down",
        })?;

        let stdin = process.stdin.take().context(PlainTextSnafu {
            msg: "failed to get stdin of lsp instance",
        })?;
        let stdout = process.stdout.take().context(PlainTextSnafu {
            msg: "failed to get stdout of lsp instance",
        })?;
        let process = Mutex::new(process);

        let (lsp_tx, rx) = channel(32);
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

        Ok(Instance {
            pid,
            process,
            lsp_tx,
            cancel,
            last_used,
            active_client_id,
            clients,
            pinned: AtomicBool::new(false),
            req_id_mapper,
        })
    }

    fn attach_client(&self, client: ClientHandle) {
        let client_id = client.id;
        self.clients.insert(client.id, client);
        info!(client_id, pid = self.pid, "registered client handle");
    }

    fn detach_client(&self, client_id: u32) -> Option<(u32, ClientHandle)> {
        let removed = self.clients.remove(&client_id);
        if removed.is_none() {
            warn!(
                client_id,
                pid = self.pid,
                "client handle not found during detach"
            );
        }
        removed
    }

    fn set_active_client(&self, client_id: u32) {
        self.active_client_id.store(client_id, Ordering::Relaxed);
    }

    async fn is_healthy(&self) -> bool {
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
                warn!(pid = self.pid, error = ?err, "failed to check lsp instance process state");
                false
            }
        }
    }

    async fn shutdown(&self) {
        self.cancel.cancel();
        info!(pid = self.pid, "shutting down lsp instance");

        let mut process = self.process.lock().await;

        if let Err(e) = process.kill().await {
            error!(pid = self.pid, error = ?e, "failed to kill rust-analyzer");
        }

        if let Err(e) = process.wait().await {
            error!(pid = self.pid, error = ?e, "failed to reap rust-analyzer");
        }
    }

    fn build_initialize_response_from_cache(&self, id: JsonRpcId) -> Option<LspFrame> {
        self.req_id_mapper.initialize_response_from_cache(id)
    }
}

fn prepend_paths_to_path(command: &mut Command, path_prepend: &[PathBuf]) -> Result<()> {
    let curr_path = std::env::var_os("PATH").unwrap_or_default();
    let path = build_path_with_prepend(path_prepend, curr_path)?;

    command.env("PATH", path);
    Ok(())
}

fn build_path_with_prepend(path_prepend: &[PathBuf], curr_path: OsString) -> Result<OsString> {
    let paths = path_prepend.iter().cloned().chain(split_paths(&curr_path));
    let path = join_paths(paths).map_err(|err| {
        PlainTextSnafu {
            msg: format!("failed to build PATH with path_prepend: {err}"),
        }
        .build()
    })?;

    Ok(path)
}

struct ClientMessage {
    pub client_id: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
struct ClientHandle {
    pub id: u32,
    pub tx: LspSender,
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

    pub fn workspace(&self) -> &str {
        &self.workspace
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
        error!(pid, error = ?err, "failed to shutdown rust-analyzer stdin");
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
                warn!(pid, client_id = msg.client_id, error = ?err, "failed to encode rewritten client frame, forwarding raw bytes");
                msg.bytes
            }
        },
        Ok(None) => msg.bytes,
        Err(err) => {
            warn!(pid, client_id = msg.client_id, error = ?err, "invalid client frame, forwarding raw bytes");
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
            pid,
            error = ?err,
            "failed to forward message to rust-analyzer stdin"
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
                                warn!(pid, error = ?e, "failed to rewrite rust-analyzer frame, forwarding raw bytes");
                                continue;
                            },
                        };

                        if routed.client_id == 0 {
                            warn!(pid, "dropping packet without active client");
                            continue;
                        }

                        let client_tx = clients.get(&routed.client_id).map(|client| client.tx.clone());
                        if let Some(tx) = client_tx {
                            if let Err(err) = tx.send(routed.frame).await {
                                error!(
                                    pid,
                                    client_id = routed.client_id,
                                    error = ?err,
                                    "failed to forward rust-analyzer output to client"
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
                        error!(pid, error = ?err, "failed to parse rust-analyzer packet");
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
            eprintln!(
                "skipping shutdown_exits_rust_analyzer_process: rust-analyzer not available in PATH"
            );
            return;
        }

        let instance = Instance::new(Path::new("rust-analyzer"), &[], None).unwrap();

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
        let resp_json = routed.frame.as_json();

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

    #[test]
    fn builds_path_with_prepend_in_config_order() {
        let current = join_paths([PathBuf::from("/usr/bin"), PathBuf::from("/bin")]).unwrap();
        let path = build_path_with_prepend(
            &[PathBuf::from("/opt/a/bin"), PathBuf::from("/opt/b/bin")],
            current,
        )
        .unwrap();
        let paths = split_paths(&path).collect::<Vec<_>>();

        assert_eq!(
            vec![
                PathBuf::from("/opt/a/bin"),
                PathBuf::from("/opt/b/bin"),
                PathBuf::from("/usr/bin"),
                PathBuf::from("/bin"),
            ],
            paths
        );
    }
}
