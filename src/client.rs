use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{debug, info, warn};

pub async fn run(addr: &str) {
    let stream = match TcpStream::connect(addr).await {
        Ok(stream) => stream,
        Err(err) => {
            warn!(addr, error = %err, "failed to connect to rad server");
            return;
        }
    };

    info!(addr, "client proxy connected to rad server");

    let (mut socket_read, mut socket_write) = stream.into_split();
    let mut stdin = io::stdin();
    let mut stdout = io::stdout();

    let upstream = tokio::spawn(async move {
        let mut buf = vec![0; 8192];
        loop {
            let n = match stdin.read(&mut buf).await {
                Ok(0) => {
                    debug!("stdin reached eof");
                    break;
                }
                Ok(n) => n,
                Err(err) => {
                    warn!(error = %err, "failed to read from stdin");
                    break;
                }
            };

            if let Err(err) = socket_write.write_all(&buf[..n]).await {
                warn!(error = %err, "failed to write to rad server");
                break;
            }
        }

        let _ = socket_write.shutdown().await;
    });

    let downstream = tokio::spawn(async move {
        let mut buf = vec![0; 8192];
        loop {
            let n = match socket_read.read(&mut buf).await {
                Ok(0) => {
                    debug!("rad server closed connection");
                    break;
                }
                Ok(n) => n,
                Err(err) => {
                    warn!(error = %err, "failed to read from rad server");
                    break;
                }
            };

            if let Err(err) = stdout.write_all(&buf[..n]).await {
                warn!(error = %err, "failed to write to stdout");
                break;
            }
            if let Err(err) = stdout.flush().await {
                warn!(error = %err, "failed to flush stdout");
                break;
            }
        }
    });

    tokio::select! {
        _ = upstream => {}
        _ = downstream => {}
    }

    info!("client proxy stopped");
}
