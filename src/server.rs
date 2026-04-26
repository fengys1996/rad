use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::channel,
};
use tracing::{debug, info, warn};

use crate::{
    config::DEFAULT_ADDR,
    instance::{InstanceKey, LspServerInstanceManager},
    protocol::{LspPacket, LspPacketDecoder},
};

pub async fn run() {
    let addr = DEFAULT_ADDR;
    let listener = TcpListener::bind(addr).await.unwrap();
    info!(addr, "server listening");
    let manager = Arc::new(LspServerInstanceManager::default());
    manager.clone().start_reaper();
    let next_client_id = Arc::new(AtomicU32::new(1));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let manager = manager.clone();
                let next_client_id = next_client_id.clone();

                tokio::spawn(async move {
                    let client_id = next_client_id.fetch_add(1, Ordering::Relaxed);
                    info!(client_id, "accepted client connection");
                    process(manager, client_id, stream).await;
                });
            }
            Err(e) => {
                warn!(error = ?e, "failed to accept client connection");
            }
        }
    }
}

async fn process(manager: Arc<LspServerInstanceManager>, client_id: u32, stream: TcpStream) {
    let (client_tx, mut client_rx) = channel::<Vec<u8>>(32);
    let (mut read_stream, mut write_stream) = stream.into_split();

    let mut read_buf = vec![0; 8192];
    let mut decoder = LspPacketDecoder::default();
    let mut instance_key: Option<InstanceKey> = None;
    let mut workspace_label = String::from("<unknown>");
    let mut reusing_existing_instance = false;
    let mut short_circuited_initialize = false;

    loop {
        tokio::select! {
            msg = client_rx.recv() => {
                let Some(msg) = msg else {
                    break;
                };

                debug!(
                    client_id,
                    bytes = msg.len(),
                    "writing instance message to client socket"
                );
                if write_stream.write_all(&msg).await.is_err() {
                    warn!(client_id, "failed writing message to client socket");
                    break;
                }
            }
            read = read_stream.read(&mut read_buf) => {
                match read {
                    Ok(0) => {
                        info!(client_id, "client socket closed");
                        break;
                    }
                    Ok(n) => {
                        debug!(client_id, bytes = n, "read bytes from client socket");
                        decoder.push(&read_buf[..n]);

                        loop {
                            let packet = match decoder.next_packet() {
                                Ok(Some(packet)) => packet,
                                Ok(None) => break,
                                Err(err) => {
                                    warn!(client_id, error = %err, "failed to decode client packet");
                                    break;
                                }
                            };

                            if instance_key.is_none() {
                                workspace_label = extract_workspace_key(&packet.body)
                                    .unwrap_or_else(|| "default-workspace".to_string());
                                let key = InstanceKey::new(workspace_label.clone());
                                reusing_existing_instance =
                                    manager
                                        .spawn_instance(client_id, client_tx.clone(), &key)
                                        .await;
                                info!(
                                    client_id,
                                    workspace = %workspace_label,
                                    "client attached to instance"
                                );
                                instance_key = Some(key);
                            }

                            if let Some(key) = instance_key.as_ref() {
                                if reusing_existing_instance
                                    && is_initialize_request(&packet)
                                    && manager.reply_initialize_from_cache(key, client_id, &packet)
                                {
                                    short_circuited_initialize = true;
                                    continue;
                                }

                                if short_circuited_initialize && is_initialized_notification(&packet)
                                {
                                    continue;
                                }

                                if is_exit_notification(&packet) {
                                    continue;
                                }

                                if is_shutdown_request(&packet)
                                    && let Some(response) = build_shutdown_response(&packet)
                                {
                                    let _ = client_tx.send(response).await;
                                    continue;
                                }
                                manager.send_to_instance(key, client_id, packet.to_bytes());
                            }
                        }
                    }
                    Err(err) => {
                        warn!(client_id, "failed reading from client socket: {:?}", err);
                        break;
                    }
                }
            }
        }
    }

    let _ = write_stream.shutdown().await;

    if let Some(key) = instance_key {
        manager.remove_client(&key, client_id);
        info!(
            client_id,
            workspace = %workspace_label,
            "client detached from instance"
        );
    }
}

fn is_initialize_request(packet: &LspPacket) -> bool {
    packet
        .parse_json()
        .and_then(|json| {
            json.get("method")
                .and_then(serde_json::Value::as_str)
                .map(|method| method == "initialize")
        })
        .unwrap_or(false)
}

fn is_initialized_notification(packet: &LspPacket) -> bool {
    packet
        .parse_json()
        .and_then(|json| {
            json.get("method")
                .and_then(serde_json::Value::as_str)
                .map(|method| method == "initialized")
        })
        .unwrap_or(false)
}

fn is_shutdown_request(packet: &LspPacket) -> bool {
    packet
        .parse_json()
        .and_then(|json| {
            let method = json.get("method")?.as_str()?;
            if method == "shutdown" && json.get("id").is_some() {
                Some(true)
            } else {
                Some(false)
            }
        })
        .unwrap_or(false)
}

fn is_exit_notification(packet: &LspPacket) -> bool {
    packet
        .parse_json()
        .and_then(|json| {
            json.get("method")
                .and_then(serde_json::Value::as_str)
                .map(|method| method == "exit")
        })
        .unwrap_or(false)
}

fn build_shutdown_response(packet: &LspPacket) -> Option<Vec<u8>> {
    let request = packet.parse_json()?;
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
    let body = serde_json::to_vec(&response).ok()?;
    Some(LspPacket::from_body(body).to_bytes())
}

fn extract_workspace_key(body: &[u8]) -> Option<String> {
    let json: serde_json::Value = serde_json::from_slice(body).ok()?;
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
