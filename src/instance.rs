#![allow(dead_code)]
use std::{
    process::Stdio,
    sync::atomic::Ordering,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU32},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::mpsc::{Receiver, Sender, channel},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
    pub fn spawn_instance(
        &self,
        client_id: u32,
        client_tx: Sender<Vec<u8>>,
        key: &InstanceKey,
    ) {
        let instance = match self.instances.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                debug!(
                    workspace = %key.workspace,
                    client_id,
                    "reusing existing lsp instance"
                );
                entry.get().clone()
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let instance = Arc::new(LspServerInstance::new());
                info!(
                    workspace = %key.workspace,
                    pid = instance.pid,
                    "spawned new lsp instance"
                );
                entry.insert(instance.clone());
                instance
            }
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
                key.workspace,
                err
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
}

pub struct LspServerInstance {
    pid: u32,
    process: Child,
    lsp_tx: Sender<ClientMessage>,
    cancel: CancellationToken,
    last_used: Arc<AtomicI64>,
    active_client_id: Arc<AtomicU32>,
    clients: Arc<DashMap<u32, ClientHandle>>,
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

impl LspServerInstance {
    pub fn new() -> LspServerInstance {
        let mut proc = Command::new("rust-analyzer")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();

        let pid = proc.id().unwrap_or_default();
        let stdin = proc.stdin.take().expect("rust-analyzer stdin is piped");
        let stdout = proc.stdout.take().expect("rust-analyzer stdout is piped");
        let (tx, rx) = channel(10);
        let cancel = CancellationToken::new();
        let last_used = Arc::new(AtomicI64::new(now_ts()));
        let active_client_id = Arc::new(AtomicU32::new(0));
        let clients = Arc::new(DashMap::new());

        tokio::spawn(pump_client_to_ra(
            rx,
            stdin,
            cancel.clone(),
            pid,
            last_used.clone(),
            active_client_id.clone(),
        ));
        tokio::spawn(pump_ra_to_active_client(
            stdout,
            cancel.clone(),
            pid,
            active_client_id.clone(),
            clients.clone(),
        ));

        LspServerInstance {
            pid,
            process: proc,
            lsp_tx: tx,
            cancel,
            last_used,
            active_client_id,
            clients,
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
            warn!(client_id, pid = self.pid, "client handle not found during removal");
        }

        removed
    }

    pub fn set_active_client(&self, client_id: u32) {
        self.active_client_id
            .store(client_id, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn active_client(&self) -> Option<ClientHandle> {
        let client_id = self
            .active_client_id
            .load(std::sync::atomic::Ordering::Relaxed);
        self.clients.get(&client_id).map(|client| client.clone())
    }

    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        info!(pid = self.pid, "shutting down lsp instance");

        if let Err(e) = self.process.kill().await {
            error!("failed to kill rust-analyzer, pid {}, error: {e}", self.pid);
        }

        if let Err(err) = self.process.wait().await {
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
}

async fn pump_client_to_ra(
    mut rx: Receiver<ClientMessage>,
    mut ra_stdin: ChildStdin,
    cancel: CancellationToken,
    pid: u32,
    last_used: Arc<AtomicI64>,
    active_client_id: Arc<AtomicU32>,
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
                debug!(
                    pid,
                    client_id = msg.client_id,
                    bytes = msg.bytes.len(),
                    "forwarding client message to rust-analyzer"
                );

                if let Err(err) = ra_stdin.write_all(&msg.bytes).await {
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
) {
    let mut buf = vec![0; 8192];
    info!(pid, "started pump_ra_to_active_client task");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!(pid, "pump_ra_to_active_client cancelled");
                break;
            }
            read = ra_stdout.read(&mut buf) => {
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

                let client_id = active_client_id.load(Ordering::Relaxed);
                let client_tx = clients.get(&client_id).map(|client| client.tx.clone());

                if let Some(tx) = client_tx {
                    debug!(
                        pid,
                        client_id,
                        bytes = n,
                        "forwarding rust-analyzer stdout to active client"
                    );
                    if let Err(err) = tx.send(buf[..n].to_vec()).await {
                        error!(
                            "failed to forward rust-analyzer pid {} stdout to client {}: {}",
                            pid,
                            client_id,
                            err
                        );
                    }
                } else {
                    warn!(
                        pid,
                        client_id,
                        bytes = n,
                        "dropping rust-analyzer stdout because active client is not registered"
                    );
                }
            }
        }
    }
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

        let mut instance = LspServerInstance::new();

        instance.shutdown().await;

        let _status = instance
            .process
            .try_wait()
            .expect("try_wait should succeed")
            .expect("rust-analyzer should be exited after shutdown");
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
