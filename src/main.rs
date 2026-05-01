use crate::logging::{default_client_options, default_server_options, init_logging};
use config::{Mode, parse_mode};

pub mod client;
pub mod config;
pub mod instance;
pub mod logging;
pub mod protocol;
pub mod server;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _logging_guard = match parse_mode() {
        Mode::Server => {
            let guard = init_logging(default_server_options());
            server::run().await;
            guard
        }
        Mode::Client { addr } => {
            let guard = init_logging(default_client_options());
            client::run(&addr).await;
            guard
        }
    };
}
