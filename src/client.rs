use futures_util::SinkExt;
use snafu::ResultExt;
use tokio::{
    io::{self, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::error::{PlainTextSnafu, Result};
use crate::protocol::{
    ControlMessage, LspFrameDecoder, LspFrameStream, RadFrameCocdec, RadFrameStream, RadMessage,
};
use crate::{config::DEFAULT_ADDR, error::IoSnafu};

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

    let stream = TcpStream::connect(&server_addr)
        .await
        .with_context(|_| IoSnafu {
            detail: format!("failed to connect to red server, server addr: {server_addr}"),
        })?;

    info!(server_addr, "client proxy connected to rad server");

    let (r, w) = stream.into_split();

    let upstream = tokio::spawn(stdin_to_server(w));
    let downstream = tokio::spawn(server_to_stdout(r));

    tokio::select! {
        _ = upstream => {}
        _ = downstream => {}
    }

    info!("rad client proxy stopped");

    Ok(())
}

pub async fn status(opts: Options) -> Result<()> {
    let Options { server_addr } = opts;

    let stream = TcpStream::connect(&server_addr)
        .await
        .with_context(|_| IoSnafu {
            detail: format!("failed to connect to red server, server addr: {server_addr}"),
        })?;

    let msg = RadMessage::control(ControlMessage::StatusRequest);
    let (r, w) = stream.into_split();

    let mut sink = FramedWrite::new(w, RadFrameCocdec);
    sink.send(msg).await?;

    let mut stream = FramedRead::new(r, RadFrameCocdec);
    let Some(msg) = stream.next().await else {
        return PlainTextSnafu {
            msg: "rad server closed connection before status response",
        }
        .fail();
    };

    match msg? {
        RadMessage::Control(ControlMessage::StatusResponse { status }) => {
            if status.instances.is_empty() {
                println!("no lsp instances");
                return Ok(());
            }

            println!("workspace\tpid\tclients\tidle_secs\thealthy");
            for instance in status.instances {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    instance.workspace,
                    instance.pid,
                    instance.client_count,
                    instance.idle_secs,
                    instance.healthy
                );
            }
            Ok(())
        }
        RadMessage::Control(ControlMessage::Error { message }) => {
            PlainTextSnafu { msg: message }.fail()
        }
        RadMessage::Control(ControlMessage::StatusRequest) => PlainTextSnafu {
            msg: "unexpected status request from rad server".to_string(),
        }
        .fail(),
        RadMessage::Lsp(_) => PlainTextSnafu {
            msg: "unexpected lsp message from rad server".to_string(),
        }
        .fail(),
    }
}

async fn stdin_to_server(mut write: OwnedWriteHalf) {
    let stdin = io::stdin();
    let mut stream = LspFrameStream::new(stdin, LspFrameDecoder);

    while let Some(frame) = stream.next().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(e) => {
                warn!(error = ?e, "failed to decode lsp frame from stdin");
                break;
            }
        };
        let bytes = match RadMessage::lsp(frame).to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(error = ?e, "failed to encode rad lsp message");
                break;
            }
        };

        if let Err(e) = write.write_all(&bytes).await {
            warn!(error = ?e, "failed to write to rad server");
            break;
        }
    }

    debug!("stdin reached eof");

    if let Err(e) = write.shutdown().await {
        warn!(error = ?e, "failed to shutdown write");
    }
}

async fn server_to_stdout(read: OwnedReadHalf) {
    let mut stdout = io::stdout();
    let mut stream = RadFrameStream::new(read, RadFrameCocdec);

    while let Some(message) = stream.next().await {
        let message = match message {
            Ok(message) => message,
            Err(e) => {
                warn!(error = ?e, "failed to read rad message from server");
                break;
            }
        };

        let RadMessage::Lsp(frame) = message else {
            warn!("ignoring unexpected control message from rad server");
            continue;
        };

        let bytes = match frame.to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(error = ?e, "failed to encode lsp frame for stdout");
                break;
            }
        };

        if let Err(e) = stdout.write_all(&bytes).await {
            warn!(error = ?e, "failed to write to stdout");
            break;
        }

        if let Err(e) = stdout.flush().await {
            warn!(error = ?e, "failed to flush stdout");
            break;
        }
    }

    debug!("rad server closed connection");
}
