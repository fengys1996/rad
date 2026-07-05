use std::collections::HashMap;
use std::path::PathBuf;

use clap::{CommandFactory, Parser, Subcommand};
use serde::Deserialize;

pub const DEFAULT_ADDR: &str = "127.0.0.1:27631";

#[derive(Clone, Deserialize)]
pub struct ProjectConfig {
    #[serde(default)]
    pub lsp_server_path: Option<PathBuf>,
}

#[derive(Deserialize)]
pub struct RadConfig {
    #[serde(default)]
    pub lsp_server_path: Option<PathBuf>,
    #[serde(default)]
    pub path_prepend: Vec<PathBuf>,
    #[serde(default)]
    pub projects: HashMap<String, ProjectConfig>,
    #[serde(default = "default_instance_timeout")]
    pub instance_timeout: u64,
    #[serde(default = "default_gc_interval")]
    pub gc_interval: u64,
    #[serde(default = "default_listen")]
    pub listen: (String, u16),
}

impl Default for RadConfig {
    fn default() -> Self {
        Self {
            lsp_server_path: None,
            path_prepend: Vec::new(),
            projects: HashMap::new(),
            instance_timeout: default_instance_timeout(),
            gc_interval: default_gc_interval(),
            listen: default_listen(),
        }
    }
}

impl RadConfig {
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen.0, self.listen.1)
    }
}

pub struct Args {
    pub mode: Option<Mode>,
    pub config_path: PathBuf,
}

#[derive(Parser)]
#[command(
    name = "rad",
    about = "rust-analyzer daemon",
    disable_help_subcommand = true,
    override_usage = "rad [server|client] [options]"
)]
struct Cli {
    #[command(subcommand)]
    mode: Option<Mode>,

    #[arg(
        short = 'c',
        long = "config-file",
        value_name = "path",
        help = config_file_help(),
        global = true,
    )]
    config_path: Option<PathBuf>,
}

pub fn parse_args() -> Args {
    parse_args_from(std::env::args())
}

pub fn print_help_and_exit() -> ! {
    let _ = Cli::command().print_help();
    println!();
    std::process::exit(0)
}

pub fn load_config(path: &PathBuf) -> RadConfig {
    match std::fs::read_to_string(path) {
        Ok(contents) => match toml::from_str(&contents) {
            Ok(config) => config,
            Err(e) => {
                eprintln!(
                    "failed to parse config file {}: {e}, using defaults",
                    path.display()
                );
                RadConfig::default()
            }
        },
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "failed to read config file {}: {e}, using defaults",
                    path.display()
                );
            }
            RadConfig::default()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Subcommand)]
pub enum Mode {
    Server,
    Client,
}

fn parse_args_from<I, T>(args: I) -> Args
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let cli = Cli::parse_from(args);
    Args {
        mode: cli.mode,
        config_path: cli.config_path.unwrap_or_else(default_config_path),
    }
}

fn config_file_help() -> String {
    format!(
        "Path to config file (default: {})",
        default_config_path().display()
    )
}

fn default_config_path() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME") {
        let mut path = PathBuf::from(dir);
        path.push("rad");
        path.push("rad.toml");
        return path;
    }
    if let Ok(home) = std::env::var("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".config");
        path.push("rad");
        path.push("rad.toml");
        return path;
    }
    PathBuf::from("rad.toml")
}

fn default_instance_timeout() -> u64 {
    300
}

fn default_gc_interval() -> u64 {
    30
}

fn default_listen() -> (String, u16) {
    ("127.0.0.1".to_string(), 27631)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_server_mode() {
        let args = parse_args_from(["rad", "server"]);

        assert_eq!(Some(Mode::Server), args.mode);
    }

    #[test]
    fn parses_client_mode() {
        let args = parse_args_from(["rad", "client"]);

        assert_eq!(Some(Mode::Client), args.mode);
    }

    #[test]
    fn parses_config_file_before_mode() {
        let args = parse_args_from(["rad", "--config-file", "/tmp/rad.toml", "server"]);

        assert_eq!(Some(Mode::Server), args.mode);
        assert_eq!(PathBuf::from("/tmp/rad.toml"), args.config_path);
    }

    #[test]
    fn parses_config_file_after_mode() {
        let args = parse_args_from(["rad", "server", "-c", "/tmp/rad.toml"]);

        assert_eq!(Some(Mode::Server), args.mode);
        assert_eq!(PathBuf::from("/tmp/rad.toml"), args.config_path);
    }
}
