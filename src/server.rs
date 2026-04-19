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

use crate::instance::{InstanceKey, LspServerInstanceManager};

pub async fn run() {
    let addr = "127.0.0.1:27631";
    let listener = TcpListener::bind(addr).await.unwrap();
    info!(addr, "server listening");
    let manager = Arc::new(LspServerInstanceManager::default());
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
    // TODO: Parse the LSP initialize request and derive a stable workspace key
    // from its workspace-specific fields instead of using a random UUID.
    let workspace = "/home/fys/projects/rad/";
    let key = InstanceKey::new(workspace);
    let (client_tx, mut client_rx) = channel(10);
    let (mut read_stream, mut write_stream) = stream.into_split();

    manager.spawn_instance(client_id, client_tx, &key);
    info!(
        client_id,
        workspace = %workspace,
        "client attached to instance"
    );

    let write_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            // TODO: remove it later.
            info!("recv msg from rust analyzer: {:?}", unsafe {
                String::from_utf8_unchecked(msg.clone())
            });
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

        let _ = write_stream.shutdown().await;
    });

    let manager_for_read = manager.clone();
    let key_for_read = key.clone();
    let read_task = tokio::spawn(async move {
        let mut buf = vec![0; 8192];

        loop {
            match read_stream.read(&mut buf).await {
                Ok(0) => {
                    info!(client_id, "client socket closed");
                    break;
                }
                Ok(n) => {
                    debug!(client_id, bytes = n, "read message from client socket");
                    // TODO: remove it later.
                    info!("recv msg from client: {:?}", unsafe {
                        String::from_utf8_unchecked(buf[..n].to_vec())
                    });
                    manager_for_read.send_to_instance(&key_for_read, client_id, buf[..n].to_vec());
                }
                Err(err) => {
                    warn!(client_id, "failed reading from client socket: {:?}", err);
                    break;
                }
            }
        }
    });

    tokio::select! {
        _ = read_task => {}
        _ = write_task => {}
    }

    manager.remove_client(&key, client_id);
    info!(
        client_id,
        workspace = %workspace,
        "client detached from instance"
    );
}
