use anyhow::{Context, Result, anyhow};
use std::{
    fs::{OpenOptions, create_dir_all},
    path::PathBuf,
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

pub fn init_logging(opts: LoggingOptions) -> Result<WorkerGuard> {
    let LoggingOptions { file_path, level } = opts;

    if let Some(parent) = file_path.parent() {
        create_dir_all(parent)
            .with_context(|| format!("failed to create log directory {parent:?}"))?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .with_context(|| format!("failed to open log file {file_path:?}"))?;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| level.into());
    let (file_appender, guard) = tracing_appender::non_blocking(file);

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(file_appender)
        .try_init()
        .map_err(|err| anyhow!("failed to init tracing subscriber: {err:?}"))?;

    Ok(guard)
}

pub struct LoggingOptions {
    pub file_path: PathBuf,
    pub level: String,
}

pub fn default_client_options() -> LoggingOptions {
    LoggingOptions {
        file_path: PathBuf::from("/tmp/rad/client.log"),
        level: "info".to_string(),
    }
}

pub fn default_server_options() -> LoggingOptions {
    LoggingOptions {
        file_path: PathBuf::from("/tmp/rad/server.log"),
        level: "info".to_string(),
    }
}
