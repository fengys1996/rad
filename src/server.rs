use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use anyhow::{Result, bail};
use jiff::Timestamp;
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

use crate::{
    config::DEFAULT_ADDR,
    instance::{InstanceKey, InstanceManager, InstanceManagerRef},
    protocol::{
        LspPacket, RadMessage, RadMessageStream, TYPE_STATUS_REQ, encode_frame, encode_status_resp,
    },
};

pub struct Options {
    pub server_addr: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            server_addr: DEFAULT_ADDR.to_string(),
        }
    }
}

pub async fn run(opts: Options) -> Result<()> {
    let Options { server_addr } = opts;

    let listener = match TcpListener::bind(&server_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            bail!("failed to bind, err: {e:?}, server_addr: {server_addr}");
        }
    };

    info!(server_addr, "server listening");

    let manager = Arc::new(InstanceManager::default());
    manager.clone().start_reaper();
    let next_client_id = Arc::new(AtomicU32::new(1));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let m = manager.clone();
                let client_id = next_client_id.fetch_add(1, Ordering::Relaxed);
                info!(client_id, "accepted client connection");

                tokio::spawn(process(m, client_id, server_addr.clone(), stream));
            }
            Err(e) => {
                warn!(error = ?e, "failed to accept client connection");
            }
        }
    }
}

async fn process(
    manager: InstanceManagerRef,
    cid: u32,
    listen_addr: String,
    stream: TcpStream,
) {
    let (to_client, from_instance) = channel::<Vec<u8>>(4);
    let (r, mut w) = stream.into_split();
    let mut msg_stream = RadMessageStream::new(r);
    let first_msg = match msg_stream.next().await {
        Some(Ok(msg)) => msg,
        Some(Err(err)) => {
            warn!(cid, error = %err, "failed to decode first client message");
            return;
        }
        None => return,
    };

    if let RadMessage::Control(frame) = first_msg {
        if let Err(err) = handle_control_message(&manager, &mut w, &listen_addr, frame).await {
            warn!(cid, error = %err, "failed to handle control stream");
        }
        return;
    }

    let writer_task = tokio::spawn(forward_instance_to_client(cid, w, from_instance));

    let m = manager.clone();
    let first_packet = match first_msg {
        RadMessage::Lsp(packet) => packet,
        RadMessage::Control(_) => unreachable!(),
    };
    let read_task =
        tokio::spawn(forward_client_to_instance(m, cid, msg_stream, first_packet, to_client));

    let ReaderExit {
        instance_key,
        workspace_label,
    } = match read_task.await {
        Ok(exit) => exit,
        Err(e) => {
            warn!(cid, error = %e, "forward_client_to_instance task failed");
            ReaderExit::default()
        }
    };

    if let Some(key) = instance_key {
        manager.remove_client(&key, cid);
        info!(
            cid,
            workspace = %workspace_label,
            "client detached from instance"
        );
    }

    if let Err(e) = writer_task.await {
        warn!(cid, error = %e, "instance_to_client task failed");
    }
}

async fn handle_control_message(
    manager: &InstanceManager,
    writer: &mut OwnedWriteHalf,
    listen_addr: &str,
    frame: crate::protocol::ControlFrame,
) -> std::io::Result<()> {
    if frame.msg_type != TYPE_STATUS_REQ {
        let bytes = encode_frame(0x00FF, br#"{"ok":false,"error":"unsupported_control_msg"}"#);
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        return Ok(());
    }

    let instances = manager.status_instances().await;
    let payload = serde_json::json!({
        "ok": true,
        "listen_addr": listen_addr,
        "instances": instances.iter().map(|item| serde_json::json!({
            "workspace": item.workspace,
            "pid": item.ra_pid,
            "client_count": item.client_count,
            "last_used_at": format_local_time(item.last_used_ts),
            "healthy": item.healthy,
        })).collect::<Vec<_>>(),
    });
    let bytes = encode_status_resp(&payload)?;
    writer.write_all(&bytes).await?;
    writer.shutdown().await?;
    Ok(())
}

fn format_local_time(unix_ts_secs: i64) -> String {
    Timestamp::from_second(unix_ts_secs)
        .ok()
        .map(|ts| ts.to_zoned(jiff::tz::TimeZone::system()))
        .map(|zdt| zdt.to_string())
        .unwrap_or_else(|| unix_ts_secs.to_string())
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
    manager: InstanceManagerRef,
    cid: u32,
    mut input_stream: RadMessageStream<OwnedReadHalf>,
    first_packet: LspPacket,
    to_client: Sender<Vec<u8>>,
) -> ReaderExit {
    let mut session = ClientSessionState::default();
    let first_action =
        make_client_packet_plan(&manager, cid, &to_client, &mut session, first_packet).await;

    match first_action {
        ClientPacketAction::ForwardToInstance { key, bytes } => {
            manager.send_to_instance(&key, cid, bytes);
        }
        ClientPacketAction::ReplyToClient(bytes) => {
            let _ = to_client.send(bytes).await;
        }
        ClientPacketAction::Ignore => {}
    }

    loop {
        let msg = match input_stream.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                warn!(cid, error = %e, "failed to decode client packet");
                break;
            }
            None => break,
        };
        let packet = match msg {
            RadMessage::Lsp(packet) => packet,
            RadMessage::Control(_) => {
                warn!(cid, "control frame is not supported in lsp client stream");
                break;
            }
        };

        let action = make_client_packet_plan(&manager, cid, &to_client, &mut session, packet).await;

        match action {
            ClientPacketAction::ForwardToInstance { key, bytes } => {
                manager.send_to_instance(&key, cid, bytes);
            }
            ClientPacketAction::ReplyToClient(bytes) => {
                let _ = to_client.send(bytes).await;
            }
            ClientPacketAction::Ignore => {}
        }
    }

    info!(cid, "client socket closed");

    ReaderExit {
        instance_key: session.instance_key,
        workspace_label: session.workspace_label,
    }
}

async fn make_client_packet_plan(
    manager: &InstanceManager,
    cid: u32,
    to_client: &Sender<Vec<u8>>,
    session: &mut ClientSessionState,
    packet: LspPacket,
) -> ClientPacketAction {
    debug!(
        cid,
        bytes = packet.to_bytes().len(),
        "read lsp packet from client socket"
    );

    if session.instance_key.is_none() {
        // Bind the client to a per-workspace instance on the first packet we can identify.
        session.workspace_label =
            extract_workspace_key(&packet.body).unwrap_or_else(|| "default-workspace".to_string());
        let key = InstanceKey::new(session.workspace_label.clone());
        session.reusing_existing_instance =
            manager.spawn_instance(cid, to_client.clone(), &key).await;
        info!(
            cid,
            workspace = %session.workspace_label,
            "client attached to instance"
        );
        session.instance_key = Some(key);
    }

    let Some(key) = session.instance_key.clone() else {
        return ClientPacketAction::Ignore;
    };

    // When attaching to an existing instance, satisfy initialize from cached capabilities
    // instead of replaying a second initialize into rust-analyzer.
    if session.reusing_existing_instance
        && packet.is_request_method("initialize")
        && let Some(request_id) = extract_request_id(&packet)
        && let Some(response) = manager.build_initialize_response_from_cache(&key, request_id)
    {
        session.initialize_replied_from_cache = true;
        debug!(cid, workspace = %session.workspace_label, "replying initialize from cached capabilities");
        return ClientPacketAction::ReplyToClient(response);
    }

    if session.initialize_replied_from_cache && packet.is_method("initialized") {
        debug!(cid, workspace = %session.workspace_label, "ignoring initialized after cached initialize");
        return ClientPacketAction::Ignore;
    }

    if packet.is_method("exit") {
        debug!(cid, workspace = %session.workspace_label, "ignoring client exit notification for shared instance");
        return ClientPacketAction::Ignore;
    }

    // Handle shutdown locally so we can let the shared backend instance keep running.
    if packet.is_request_method("shutdown")
        && let Some(response) = build_shutdown_response(&packet)
    {
        debug!(cid, workspace = %session.workspace_label, "replying shutdown locally for shared instance");
        return ClientPacketAction::ReplyToClient(response);
    }

    ClientPacketAction::ForwardToInstance {
        key,
        bytes: packet.to_bytes(),
    }
}

fn build_shutdown_response(packet: &LspPacket) -> Option<Vec<u8>> {
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
    Some(LspPacket::from_body(response).to_bytes())
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
    workspace_label: String,
    reusing_existing_instance: bool,
    initialize_replied_from_cache: bool,
}

impl Default for ClientSessionState {
    fn default() -> Self {
        Self {
            instance_key: None,
            workspace_label: String::from("<unknown>"),
            reusing_existing_instance: false,
            initialize_replied_from_cache: false,
        }
    }
}

enum ClientPacketAction {
    ForwardToInstance { key: InstanceKey, bytes: Vec<u8> },
    ReplyToClient(Vec<u8>),
    Ignore,
}

fn extract_request_id(packet: &LspPacket) -> Option<serde_json::Value> {
    packet.body.get("id").cloned()
}
