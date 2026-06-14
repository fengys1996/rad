use crate::error::{IoSnafu, Result, UnexpectedSnafu};
use snafu::ResultExt;
use std::{
    fs::{OpenOptions, create_dir_all},
    path::PathBuf,
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

pub fn init_logging(opts: LoggingOptions) -> Result<WorkerGuard> {
    let LoggingOptions { dir, level, kind } = opts;
    let file_path = match kind {
        LogKind::Server => dir.join("server.log"),
        LogKind::Client => {
            let pid = std::process::id();
            dir.join(format!("client-{pid}/rad.log"))
        }
    };

    if let Some(parent) = file_path.parent() {
        create_dir_all(parent).with_context(|_| IoSnafu {
            reason: format!("failed to create log directory {parent:?}"),
        })?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .with_context(|_| IoSnafu {
            reason: format!("failed to open log file {file_path:?}"),
        })?;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| level.into());
    let (file_appender, guard) = tracing_appender::non_blocking(file);

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(file_appender)
        .try_init()
        .map_err(|e| {
            UnexpectedSnafu {
                err_msg: format!("failed to init tracing subscriber: {:?}", e),
            }
            .build()
        })?;

    Ok(guard)
}

pub struct LoggingOptions {
    pub dir: PathBuf,
    pub level: String,
    pub kind: LogKind,
}

pub enum LogKind {
    Server,
    Client,
}

pub fn default_client_options() -> LoggingOptions {
    LoggingOptions {
        dir: PathBuf::from("/tmp/rad"),
        level: "info".to_string(),
        kind: LogKind::Client,
    }
}

pub fn default_server_options() -> LoggingOptions {
    LoggingOptions {
        dir: PathBuf::from("/tmp/rad"),
        level: "info".to_string(),
        kind: LogKind::Server,
    }
}
