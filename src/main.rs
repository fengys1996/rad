use crate::logging::{default_client_options, default_server_options, init_logging};
use config::{
    Mode, load_config, parse_config_path, parse_mode, print_help_and_exit,
    print_help_and_exit_if_requested,
};
use std::time::Duration;

pub mod client;
pub mod config;
pub(crate) mod error;
pub mod instance;
pub mod logging;
pub mod mapper;
pub mod protocol;
pub mod server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_help_and_exit_if_requested();

    let Some(mode) = parse_mode() else {
        print_help_and_exit();
    };

    let config_path = parse_config_path();
    let config = load_config(&config_path);

    let _logging_guard = match mode {
        Mode::Server => {
            let guard = init_logging(default_server_options());
            server::run(server::Options {
                server_addr: config.listen_addr(),
                instance_timeout: Duration::from_secs(config.instance_timeout),
                gc_interval: Duration::from_secs(config.gc_interval),
                default_lsp_server_path: config.default_lsp_server_path,
                project_overrides: config.projects,
            })
            .await?;
            guard
        }
        Mode::Client => {
            let guard = init_logging(default_client_options());
            client::run(client::Options {
                server_addr: config.listen_addr(),
            })
            .await?;
            guard
        }
    };
    Ok(())
}
