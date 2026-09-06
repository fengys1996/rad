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
    ControlMessage, InstanceStatus, LspFrameDecoder, LspFrameStream, RadFrameCocdec,
    RadFrameStream, RadMessage, ServerStatus,
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
            detail: format!("failed to connect to rad server, server addr: {server_addr}"),
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
            detail: format!("failed to connect to rad server, server addr: {server_addr}"),
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
            print!("{}", format_status(status));
            Ok(())
        }
        RadMessage::Control(ControlMessage::Error { message }) => {
            PlainTextSnafu { msg: message }.fail()
        }
        RadMessage::Control(ControlMessage::StatusRequest) => PlainTextSnafu {
            msg: "unexpected status request from rad server".to_string(),
        }
        .fail(),
        RadMessage::Control(ControlMessage::ClearRequest { .. })
        | RadMessage::Control(ControlMessage::ClearResponse { .. })
        | RadMessage::Control(ControlMessage::PinRequest { .. })
        | RadMessage::Control(ControlMessage::PinResponse { .. }) => PlainTextSnafu {
            msg: "unexpected control message from rad server".to_string(),
        }
        .fail(),
        RadMessage::Lsp(_) => PlainTextSnafu {
            msg: "unexpected lsp message from rad server".to_string(),
        }
        .fail(),
    }
}

pub async fn clean(opts: Options, force: bool) -> Result<()> {
    let Options { server_addr } = opts;

    let stream = TcpStream::connect(&server_addr)
        .await
        .with_context(|_| IoSnafu {
            detail: format!("failed to connect to rad server, server addr: {server_addr}"),
        })?;

    let msg = RadMessage::control(ControlMessage::ClearRequest { force });
    let (r, w) = stream.into_split();

    let mut sink = FramedWrite::new(w, RadFrameCocdec);
    sink.send(msg).await?;

    let mut stream = FramedRead::new(r, RadFrameCocdec);
    let Some(msg) = stream.next().await else {
        return PlainTextSnafu {
            msg: "rad server closed connection before clean response",
        }
        .fail();
    };

    match msg? {
        RadMessage::Control(ControlMessage::ClearResponse { cleared }) => {
            if cleared.is_empty() {
                println!("no instances to clean");
            } else {
                for instance in &cleared {
                    println!("{} (pid: {})", instance.workspace, instance.pid);
                }
            }
            Ok(())
        }
        RadMessage::Control(ControlMessage::Error { message }) => {
            PlainTextSnafu { msg: message }.fail()
        }
        RadMessage::Control(ControlMessage::StatusResponse { .. }) => PlainTextSnafu {
            msg: "unexpected status response from rad server".to_string(),
        }
        .fail(),
        RadMessage::Control(ControlMessage::ClearRequest { .. })
        | RadMessage::Control(ControlMessage::StatusRequest)
        | RadMessage::Control(ControlMessage::PinRequest { .. })
        | RadMessage::Control(ControlMessage::PinResponse { .. }) => PlainTextSnafu {
            msg: "unexpected request from rad server".to_string(),
        }
        .fail(),
        RadMessage::Lsp(_) => PlainTextSnafu {
            msg: "unexpected lsp message from rad server".to_string(),
        }
        .fail(),
    }
}

pub async fn pin(opts: Options, pid: u32, pinned: bool) -> Result<()> {
    let server_addr = opts.server_addr;
    let stream = TcpStream::connect(&server_addr)
        .await
        .with_context(|_| IoSnafu {
            detail: format!("failed to connect to rad server, server addr: {server_addr}"),
        })?;

    let (r, w) = stream.into_split();
    let mut sink = FramedWrite::new(w, RadFrameCocdec);
    sink.send(RadMessage::control(ControlMessage::PinRequest {
        pid,
        pinned,
    }))
    .await?;

    let mut stream = FramedRead::new(r, RadFrameCocdec);
    let Some(msg) = stream.next().await else {
        return PlainTextSnafu {
            msg: "rad server closed connection before pin response",
        }
        .fail();
    };

    match msg? {
        RadMessage::Control(ControlMessage::PinResponse { pid, pinned }) => {
            println!(
                "{} instance {pid}",
                if pinned { "pinned" } else { "unpinned" }
            );
            Ok(())
        }
        RadMessage::Control(ControlMessage::Error { message }) => {
            PlainTextSnafu { msg: message }.fail()
        }
        RadMessage::Control(_) => PlainTextSnafu {
            msg: "unexpected control message from rad server".to_string(),
        }
        .fail(),
        RadMessage::Lsp(_) => PlainTextSnafu {
            msg: "unexpected lsp message from rad server".to_string(),
        }
        .fail(),
    }
}

fn format_status(mut status: ServerStatus) -> String {
    if status.instances.is_empty() {
        return "no lsp instances\n".to_string();
    }

    status.instances.sort_by(|a, b| {
        a.workspace
            .cmp(&b.workspace)
            .then_with(|| a.pid.cmp(&b.pid))
    });

    let mut out = String::new();
    for (idx, instance) in status.instances.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&format_instance_status(instance));
    }
    out
}

fn format_instance_status(instance: &InstanceStatus) -> String {
    format!(
        "workspace: {}\n  pid:      {}\n  clients:  {}\n  idle:     {}\n  pinned:   {}\n  healthy:  {}\n",
        instance.workspace,
        instance.pid,
        instance.client_count,
        format_duration_secs(instance.idle_secs),
        if instance.pinned { "yes" } else { "no" },
        if instance.healthy { "yes" } else { "no" },
    )
}

fn format_duration_secs(secs: i64) -> String {
    let secs = secs.max(0);
    if secs < 60 {
        return format!("{secs}s");
    }

    let minutes = secs / 60;
    let seconds = secs % 60;
    if minutes < 60 {
        return format!("{minutes}m {seconds}s");
    }

    let hours = minutes / 60;
    let minutes = minutes % 60;
    format!("{hours}h {minutes}m")
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

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(e) => {
                warn!(error = ?e, "failed to read rad message from server");
                break;
            }
        };

        let RadMessage::Lsp(frame) = msg else {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_empty_status() {
        let status = ServerStatus {
            instances: Vec::new(),
        };

        assert_eq!("no lsp instances\n", format_status(status));
    }

    #[test]
    fn formats_status_by_workspace() {
        let status = ServerStatus {
            instances: vec![
                InstanceStatus {
                    workspace: "file:///z".to_string(),
                    pid: 20,
                    client_count: 2,
                    idle_secs: 75,
                    healthy: false,
                    pinned: true,
                },
                InstanceStatus {
                    workspace: "file:///a".to_string(),
                    pid: 10,
                    client_count: 1,
                    idle_secs: 5,
                    healthy: true,
                    pinned: false,
                },
            ],
        };

        assert_eq!(
            concat!(
                "workspace: file:///a\n",
                "  pid:      10\n",
                "  clients:  1\n",
                "  idle:     5s\n",
                "  pinned:   no\n",
                "  healthy:  yes\n",
                "\n",
                "workspace: file:///z\n",
                "  pid:      20\n",
                "  clients:  2\n",
                "  idle:     1m 15s\n",
                "  pinned:   yes\n",
                "  healthy:  no\n",
            ),
            format_status(status)
        );
    }

    #[test]
    fn formats_duration_secs() {
        assert_eq!("0s", format_duration_secs(-1));
        assert_eq!("59s", format_duration_secs(59));
        assert_eq!("1m 0s", format_duration_secs(60));
        assert_eq!("1h 1m", format_duration_secs(3661));
    }
}
