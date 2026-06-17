use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use snafu::ResultExt;
use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc::{Receiver, Sender, channel},
};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::config::ProjectConfig;
use crate::error::{IoSnafu, Result};
use crate::{
    instance::{InstanceHandle, InstanceKey, InstanceManager},
    protocol::{LspFrame, LspFrameDecoder, LspFrameStream},
};

pub struct Options {
    pub server_addr: String,
    pub instance_timeout: std::time::Duration,
    pub gc_interval: std::time::Duration,
    pub default_lsp_server_path: String,
    pub project_overrides: HashMap<String, ProjectConfig>,
}

pub async fn run(opts: Options) -> Result<()> {
    let Options {
        server_addr,
        instance_timeout,
        gc_interval,
        default_lsp_server_path,
        project_overrides,
    } = opts;

    let listener = TcpListener::bind(&server_addr)
        .await
        .with_context(|_| IoSnafu {
            reason: format!("failed to bind, server addr: {}", server_addr),
        })?;

    info!(server_addr, "server listening");

    let manager = InstanceManager::new(
        instance_timeout,
        gc_interval,
        default_lsp_server_path,
        project_overrides,
    )
    .await;
    let next_client_id = Arc::new(AtomicU32::new(1));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let m = manager.clone();
                let cid = next_client_id.fetch_add(1, Ordering::Relaxed);
                info!(cid, "accepted client connection");
                tokio::spawn(process(m, cid, stream));
            }
            Err(e) => {
                warn!(error = ?e, "failed to accept client connection");
            }
        }
    }
}

async fn process(manager: InstanceManager, cid: u32, stream: TcpStream) {
    let (to_client, from_instance) = channel::<Vec<u8>>(4);
    let (r, w) = stream.into_split();

    let write_task = tokio::spawn(forward_instance_to_client(cid, w, from_instance));

    let m = manager.clone();
    let frame_stream = LspFrameStream::new(r, LspFrameDecoder);
    let read_task = tokio::spawn(forward_client_to_instance(m, cid, frame_stream, to_client));

    let may_reader_exit = match read_task.await {
        Ok(Ok(reader_exit)) => Some(reader_exit),
        Ok(Err(e)) => {
            warn!(cid, error = %e, "forward_client_to_instance task failed");
            None
        }
        Err(e) => {
            warn!(cid, error = %e, "forward_client_to_instance task panicked");
            None
        }
    };

    if let Some(reader_exit) = may_reader_exit
        && let Some(key) = reader_exit.instance_key
    {
        manager.detach_client(&key, cid);
        info!(
            cid,
            workspace = %reader_exit.workspace_label,
            "client detached from instance"
        );
    }

    if let Err(e) = write_task.await {
        warn!(cid, error = %e, "instance_to_client task failed");
    }
}

async fn forward_instance_to_client(
    client_id: u32,
    mut writer: OwnedWriteHalf,
    mut input_stream: Receiver<Vec<u8>>,
) {
    while let Some(msg) = input_stream.recv().await {
        debug!(
            client_id,
            bytes = msg.len(),
            "writing instance message to client socket"
        );

        if writer.write_all(&msg).await.is_err() {
            warn!(client_id, "failed writing message to client socket");
            break;
        }
    }

    if let Err(e) = writer.shutdown().await {
        warn!(err = %e, "failed to shutdown to_client channel");
    }
}

async fn forward_client_to_instance(
    manager: InstanceManager,
    cid: u32,
    mut input_stream: LspFrameStream<OwnedReadHalf>,
    to_client: Sender<Vec<u8>>,
) -> Result<ReaderExit> {
    let mut session = ClientSessionState::default();
    while let Some(frame) = input_stream.next().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(e) => {
                warn!(cid, error = %e, "failed to decode client frame");
                break;
            }
        };

        let action =
            make_client_packet_plan(&manager, cid, &to_client, &mut session, frame).await?;

        match action {
            ClientPacketAction::ForwardToInstance { handle, bytes } => {
                debug!(
                    cid,
                    workspace = handle.key().workspace(),
                    bytes = bytes.len(),
                    "sending client message to lsp instance"
                );

                if let Err(err) = handle.send_with_timeout(cid, bytes).await {
                    warn!(
                        cid,
                        workspace = handle.key().workspace(),
                        error = %err,
                        "failed to send message to lsp instance"
                    );
                }
            }
            ClientPacketAction::ReplyToClient(bytes) => {
                let _ = to_client.send(bytes).await;
            }
            ClientPacketAction::Ignore => {}
        }
    }

    info!(cid, "client socket closed");

    Ok(ReaderExit {
        instance_key: session.instance_key,
        workspace_label: session.workspace_label,
    })
}

async fn make_client_packet_plan(
    manager: &InstanceManager,
    cid: u32,
    to_client: &Sender<Vec<u8>>,
    session: &mut ClientSessionState,
    packet: LspFrame,
) -> Result<ClientPacketAction> {
    debug!(
        cid,
        bytes = packet.to_bytes().map(|b| b.len()).unwrap_or_default(),
        "read lsp packet from client socket"
    );

    if session.instance_key.is_none() {
        // Bind the client to a per-workspace instance on the first packet we can identify.
        session.workspace_label =
            extract_workspace_key(&packet.body).unwrap_or_else(|| "default-workspace".to_string());
        let key = InstanceKey::new(session.workspace_label.clone());
        let (handle, reused) = manager.spawn_instance(cid, to_client.clone(), &key).await?;
        session.instance_key = Some(key);
        session.instance_handle = Some(handle);
        session.reusing_existing_instance = reused;
        info!(
            cid,
            workspace = %session.workspace_label,
            "client attached to instance"
        );
    }

    let Some(handle) = session.instance_handle.clone() else {
        return Ok(ClientPacketAction::Ignore);
    };
    let key = handle.key().clone();

    // When attaching to an existing instance, satisfy initialize from cached capabilities
    // instead of replaying a second initialize into rust-analyzer.
    if session.reusing_existing_instance
        && packet.is_request_method("initialize")
        && let Some(request_id) = extract_request_id(&packet)
        && let Some(response) = manager.build_initialize_response_from_cache(&key, request_id)
    {
        session.initialize_replied_from_cache = true;
        debug!(cid, workspace = %session.workspace_label, "replying initialize from cached capabilities");
        return Ok(ClientPacketAction::ReplyToClient(response));
    }

    if session.initialize_replied_from_cache && packet.is_method("initialized") {
        debug!(cid, workspace = %session.workspace_label, "ignoring initialized after cached initialize");
        return Ok(ClientPacketAction::Ignore);
    }

    if packet.is_method("exit") {
        debug!(cid, workspace = %session.workspace_label, "ignoring client exit notification for shared instance");
        return Ok(ClientPacketAction::Ignore);
    }

    // Handle shutdown locally so we can let the shared backend instance keep running.
    if packet.is_request_method("shutdown")
        && let Some(response) = build_shutdown_response(&packet)
    {
        debug!(cid, workspace = %session.workspace_label, "replying shutdown locally for shared instance");
        return Ok(ClientPacketAction::ReplyToClient(response));
    }

    Ok(ClientPacketAction::ForwardToInstance {
        handle,
        bytes: packet.to_bytes()?,
    })
}

fn build_shutdown_response(packet: &LspFrame) -> Option<Vec<u8>> {
    let request = packet.body.clone();
    let request_obj = request.as_object()?;
    if request_obj.get("method")?.as_str()? != "shutdown" {
        return None;
    }

    let id = request_obj.get("id")?.clone();
    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": null,
    });
    Some(LspFrame::new(response).to_bytes().unwrap())
}

fn extract_workspace_key(json: &serde_json::Value) -> Option<String> {
    let method = json.get("method")?.as_str()?;

    if method != "initialize" {
        return None;
    }

    let params = json.get("params")?;

    if let Some(uri) = params
        .get("workspaceFolders")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("uri"))
        .and_then(serde_json::Value::as_str)
    {
        return Some(uri.to_string());
    }

    if let Some(uri) = params.get("rootUri").and_then(serde_json::Value::as_str)
        && !uri.is_empty()
    {
        return Some(uri.to_string());
    }

    if let Some(path) = params.get("rootPath").and_then(serde_json::Value::as_str)
        && !path.is_empty()
    {
        return Some(path.to_string());
    }

    None
}

#[derive(Default)]
struct ReaderExit {
    instance_key: Option<InstanceKey>,
    workspace_label: String,
}

struct ClientSessionState {
    instance_key: Option<InstanceKey>,
    instance_handle: Option<InstanceHandle>,
    workspace_label: String,
    reusing_existing_instance: bool,
    initialize_replied_from_cache: bool,
}

impl Default for ClientSessionState {
    fn default() -> Self {
        Self {
            instance_key: None,
            instance_handle: None,
            workspace_label: String::from("<unknown>"),
            reusing_existing_instance: false,
            initialize_replied_from_cache: false,
        }
    }
}

enum ClientPacketAction {
    // TODO: optimize make bytes to LspFrame
    ForwardToInstance {
        handle: InstanceHandle,
        bytes: Vec<u8>,
    },
    ReplyToClient(Vec<u8>),
    Ignore,
}

fn extract_request_id(packet: &LspFrame) -> Option<serde_json::Value> {
    packet.body.get("id").cloned()
}
