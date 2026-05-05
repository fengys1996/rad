use anyhow::{Result, bail};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tracing::{debug, info, warn};

use crate::config::DEFAULT_ADDR;

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
