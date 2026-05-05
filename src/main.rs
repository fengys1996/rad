use crate::logging::{default_client_options, default_server_options, init_logging};
use anyhow::Result;
use config::{Mode, parse_mode};

pub mod client;
pub mod config;
pub mod instance;
pub mod logging;
pub mod mapper;
pub mod protocol;
pub mod server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _logging_guard = match parse_mode() {
        Mode::Server => {
            let guard = init_logging(default_server_options());
            server::run(server::Options::default()).await?;
            guard
        }
        Mode::Client => {
            let guard = init_logging(default_client_options());
            client::run(client::Options::default()).await?;
            guard
        }
    };
    Ok(())
}
