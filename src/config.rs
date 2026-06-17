use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

pub const DEFAULT_ADDR: &str = "127.0.0.1:27631";

#[derive(Clone, Deserialize)]
pub struct ProjectConfig {
    #[serde(default)]
    pub lsp_server_path: Option<String>,
}

#[derive(Deserialize)]
pub struct RadConfig {
    #[serde(default = "default_lsp_server_path")]
    pub default_lsp_server_path: String,
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
            default_lsp_server_path: default_lsp_server_path(),
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

pub fn print_help_and_exit_if_requested() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help_and_exit();
    }
}

pub fn print_help_and_exit() -> ! {
    println!(
        "rad - rust-analyzer daemon

Usage:
  rad [server|client] [options]

Options:
  -h, --help                    Print this help message
  -c, --config-file <path>      Path to config file (default: {})",
        default_config_path().display()
    );
    println!(
        "
Config file format (TOML):
  default_lsp_server_path = \"{}\"  # default LSP server binary
  instance_timeout        = {}   # idle timeout in seconds before reaping
  gc_interval             = {}    # interval in seconds between reaper scans
  listen                  = [\"{}\", {}]  # daemon listen host and port

  [projects.\"/absolute/path\"]
  lsp_server_path = \"/custom/rust-analyzer\"  # per-project override",
        default_lsp_server_path(),
        default_instance_timeout(),
        default_gc_interval(),
        default_listen().0,
        default_listen().1,
    );
    std::process::exit(0);
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

pub enum Mode {
    Server,
    Client,
}

pub fn parse_mode() -> Option<Mode> {
    let mut args = std::env::args().skip(1);

    loop {
        match args.next().as_deref() {
            Some("-c") | Some("--config-file") => {
                args.next();
            }
            None => return None,
            Some("server") => return Some(Mode::Server),
            Some("client") => return Some(Mode::Client),
            Some(_) => return None,
        }
    }
}

pub fn parse_config_path() -> PathBuf {
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config-file" => {
                if let Some(path) = args.next() {
                    return PathBuf::from(path);
                }
                eprintln!("--config-file requires a path argument, using default config path");
                break;
            }
            "server" | "client" => {}
            _ => {}
        }
    }

    default_config_path()
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

fn default_lsp_server_path() -> String {
    "rust-analyzer".to_string()
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
