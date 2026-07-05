use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use futures_util::{Sink, SinkExt};
use snafu::ResultExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender, channel},
};
use tokio_stream::{Stream, StreamExt};
use tokio_util::codec::FramedWrite;
use tracing::{debug, error, info, warn};

use crate::error::{IoSnafu, Result};
use crate::{config::ProjectConfig, error::Error};
use crate::{
    instance::{InstanceHandle, InstanceKey, InstanceManager},
    protocol::{
        ControlMessage, LspFrame, LspSender, RadFrameCocdec, RadFrameStream, RadMessage,
        ServerStatus,
    },
};

pub struct Options {
    pub server_addr: String,
    pub instance_timeout: std::time::Duration,
    pub gc_interval: std::time::Duration,
    pub lsp_server_path: Option<PathBuf>,
    pub path_prepend: Vec<PathBuf>,
    pub project_overrides: HashMap<String, ProjectConfig>,
}

pub async fn run(opts: Options) -> Result<()> {
    let Options {
        server_addr,
        instance_timeout,
        gc_interval,
        lsp_server_path,
        path_prepend,
        project_overrides,
    } = opts;

    let listener = TcpListener::bind(&server_addr)
        .await
        .with_context(|_| IoSnafu {
            detail: format!("failed to bind, server addr: {}", server_addr),
        })?;

    info!(server_addr, "server listening");

    let manager = InstanceManager::new(
        instance_timeout,
        gc_interval,
        lsp_server_path,
        path_prepend,
        project_overrides,
    )
    .await?;
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

async fn process(instance_manager: InstanceManager, cid: u32, incoming: TcpStream) {
    let (to_client, instance_out) = channel::<RadMessage>(4);
    let (r, w) = incoming.into_split();

    let w = FramedWrite::new(w, RadFrameCocdec);
    let forward_bg_task = tokio::spawn(forward_instance_to_client(cid, w, instance_out));

    let ctx = Context {
        cid,
        instance_manager,
        to_client,
    };
    let incoming_msgs = RadFrameStream::new(r, RadFrameCocdec);
    let process_task =
        tokio::spawn(async move { process_incoming_msgs(incoming_msgs, &ctx).await });

    if let Err(e) = process_task.await {
        warn!(cid, error = ?e, "forward_client_to_instance task panicked");
    }

    if let Err(e) = forward_bg_task.await {
        warn!(cid, error = ?e, "forward instance to client task failed");
    }
}

#[derive(Clone)]
pub struct Context {
    cid: u32,
    instance_manager: InstanceManager,
    to_client: Sender<RadMessage>,
}

async fn process_incoming_msgs<S>(mut incoming: S, ctx: &Context)
where
    S: Stream<Item = Result<RadMessage>> + Unpin,
{
    let mut client_session = ClientSessionState::default();
    while let Some(msg) = incoming.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(e) => {
                warn!(cid = ctx.cid, error = ?e, "failed to decode rad message");
                break;
            }
        };

        match msg {
            RadMessage::Lsp(frame) => process_lsp_frame(frame, ctx, &mut client_session).await,
            RadMessage::Control(ControlMessage::StatusRequest) => process_show_status(ctx).await,
            RadMessage::Control(ControlMessage::ClearRequest { force }) => {
                process_clear(ctx, force).await
            }
            _ => process_unsupported(ctx).await,
        }
    }

    info!(cid = ctx.cid, "client socket closed");

    if let Some(key) = client_session.instance_key {
        let cid = ctx.cid;
        ctx.instance_manager.detach_client(&key, cid);
        info!(cid, "client detached from instance");
    }
}

async fn process_lsp_frame(frame: LspFrame, ctx: &Context, session: &mut ClientSessionState) {
    if let Err(e) = do_process_lsp_frame(frame, ctx, session).await {
        error!(error = ?e, "failed to handle lsp frame");
    }
}

async fn do_process_lsp_frame(
    frame: LspFrame,
    ctx: &Context,
    session: &mut ClientSessionState,
) -> Result<()> {
    let cid = ctx.cid;
    let manager = &ctx.instance_manager;
    let to_client = LspSender::new(ctx.to_client.clone());

    if session.instance_key.is_none() {
        // Bind the client to a per-workspace instance on the first packet we can identify.
        session.workspace_label =
            extract_workspace_key(&frame.body).unwrap_or_else(|| "default-workspace".to_string());
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
        return Ok(());
    };

    let key = handle.key().clone();
    // When attaching to an existing instance, satisfy initialize from cached capabilities
    // instead of replaying a second initialize into rust-analyzer.
    if session.reusing_existing_instance
        && frame.is_request_method("initialize")
        && let Some(request_id) = extract_request_id(&frame)
        && let Some(resp) = manager.build_initialize_response_from_cache(&key, request_id)
    {
        session.initialize_replied_from_cache = true;
        debug!(cid, workspace = %session.workspace_label, "replying initialize from cached capabilities");
        // TODO: use error handle, instead of ignore.
        let _ = to_client.send(resp).await;
        return Ok(());
    }

    if session.initialize_replied_from_cache && frame.is_method("initialized") {
        debug!(cid, workspace = %session.workspace_label, "ignoring initialized after cached initialize");
        return Ok(());
    }

    if frame.is_method("exit") {
        debug!(cid, workspace = %session.workspace_label, "ignoring client exit notification for shared instance");
        return Ok(());
    }

    // Handle shutdown locally so we can let the shared backend instance keep running.
    if frame.is_request_method("shutdown")
        && let Some(resp) = build_shutdown_response(&frame)
    {
        debug!(cid, workspace = %session.workspace_label, "replying shutdown locally for shared instance");
        // TODO: use error handle, instead of ignore.
        let _ = to_client.send(resp).await;
        return Ok(());
    }

    handle.send_with_timeout(cid, frame.to_bytes()?).await
}

async fn process_show_status(ctx: &Context) {
    let instances = ctx.instance_manager.status().await;
    let status = ServerStatus { instances };
    let status_resp = ControlMessage::StatusResponse { status };
    let _ = ctx.to_client.send(RadMessage::control(status_resp)).await;
}

async fn process_unsupported(ctx: &Context) {
    let cid = ctx.cid;
    warn!(cid, "ignoring unexpected control message from client");
}

async fn process_clear(ctx: &Context, force: bool) {
    let cleared = if force {
        ctx.instance_manager.clear_all().await
    } else {
        ctx.instance_manager.clear_idle().await
    };
    let _ = ctx
        .to_client
        .send(RadMessage::control(ControlMessage::ClearResponse {
            cleared,
        }))
        .await;
}

async fn forward_instance_to_client<W>(c_id: u32, mut w: W, mut instance_out: Receiver<RadMessage>)
where
    W: Sink<RadMessage, Error = Error> + Unpin,
{
    while let Some(msg) = instance_out.recv().await {
        debug!(c_id, "writing rad message to client socket");
        if let Err(e) = w.send(msg).await {
            warn!(c_id, error = ?e, "failed writing message to client socket");
            break;
        }
    }
}

fn build_shutdown_response(packet: &LspFrame) -> Option<LspFrame> {
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
    Some(LspFrame::new(response))
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

fn extract_request_id(packet: &LspFrame) -> Option<serde_json::Value> {
    packet.body.get("id").cloned()
}
