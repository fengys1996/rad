use crate::logging::init_tracing;

pub mod config;
pub mod instance;
pub mod logging;
pub mod server;

#[tokio::main]
async fn main() {
    init_tracing();
    server::run().await;
}
