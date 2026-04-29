use crate::logging::init_tracing;
use config::{Mode, parse_mode};

pub mod client;
pub mod config;
pub mod instance;
pub mod logging;
pub mod protocol;
pub mod server;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match parse_mode() {
        Mode::Server => {
            init_tracing();
            server::run().await
        }
        Mode::Client { addr } => client::run(&addr).await,
    }
}
