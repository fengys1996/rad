use anyhow::{Result, bail};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tracing::{debug, info, warn};

use crate::{
    config::DEFAULT_ADDR,
    protocol::{TYPE_STATUS_RESP, decode_frame, encode_status_req},
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

    let stream = match TcpStream::connect(&server_addr).await {
        Ok(stream) => stream,
        Err(e) => {
            bail!("failed to connect to red server, err: {e:?}, server addr: {server_addr}");
        }
    };

    info!(server_addr, "client proxy connected to rad server");

    let (read, write) = stream.into_split();

    let upstream = tokio::spawn(stdin_to_server(write));
    let downstream = tokio::spawn(server_to_stdout(read));

    tokio::select! {
        _ = upstream => {}
        _ = downstream => {}
    }

    info!("rad client proxy stopped");

    Ok(())
}

pub async fn run_status(opts: Options) -> Result<()> {
    let Options { server_addr } = opts;
    let mut stream = TcpStream::connect(&server_addr).await?;
    stream.write_all(&encode_status_req()).await?;

    let mut header = [0u8; 13];
    stream.read_exact(&mut header).await?;
    let payload_len = u32::from_be_bytes([header[9], header[10], header[11], header[12]]) as usize;
    let mut frame_bytes = header.to_vec();
    if payload_len > 0 {
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).await?;
        frame_bytes.extend_from_slice(&payload);
    }

    let frame = decode_frame(&frame_bytes)?;
    if frame.msg_type != TYPE_STATUS_RESP {
        bail!("unexpected status response type: {}", frame.msg_type);
    }

    let json: serde_json::Value = serde_json::from_slice(&frame.payload)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json).unwrap_or_else(|_| "{}".to_string())
    );
    Ok(())
}

async fn stdin_to_server(mut write: OwnedWriteHalf) {
    let mut stdin = io::stdin();
    let mut buf = vec![0; 8192];

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) => {
                debug!("stdin reached eof");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "failed to read from stdin");
                break;
            }
        };

        if let Err(e) = write.write_all(&buf[..n]).await {
            warn!(error = %e, "failed to write to rad server");
            break;
        }
    }

    if let Err(e) = write.shutdown().await {
        warn!(error = %e, "failed to shutdown write");
    }
}

async fn server_to_stdout(mut read: OwnedReadHalf) {
    let mut stdout = io::stdout();
    let mut buf = vec![0; 8192];

    loop {
        let n = match read.read(&mut buf).await {
            Ok(0) => {
                debug!("rad server closed connection");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "failed to read from rad server");
                break;
            }
        };

        if let Err(e) = stdout.write_all(&buf[..n]).await {
            warn!(error = %e, "failed to write to stdout");
            break;
        }

        if let Err(e) = stdout.flush().await {
            warn!(error = %e, "failed to flush stdout");
            break;
        }
    }
}
