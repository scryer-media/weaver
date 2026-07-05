mod args;
mod bootstrap;
mod commands;
mod http;
mod shutdown;
mod wiring;

use std::path::{Path, PathBuf};

use clap::Parser;
use tracing::error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::args::{Cli, Command};

const LOG_FILE_ENV: &str = "WEAVER_LOG_FILE";

// musl's bundled allocator serializes multi-threaded allocation heavily; the
// container (musl) builds swap in mimalloc. glibc/macOS builds keep the
// system allocator.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_stack_size(8 * 1024 * 1024); // 8 MB - pipeline futures are large
    weaver_server_core::runtime::affinity::install_tokio_worker_affinity(&mut builder);

    let runtime = builder.build().expect("failed to build tokio runtime");

    runtime.block_on(async_main());
}

async fn async_main() {
    let cli = Cli::parse();
    let config_path = cli.resolved_config_path();
    let Cli {
        log_file: log_file_override,
        command,
        ..
    } = cli;
    let command = command.unwrap_or_else(Command::default_serve);

    let log_ring_buffer =
        weaver_server_core::runtime::log_buffer::LogRingBuffer::with_default_capacity();
    let buffer_layer = tracing_subscriber::fmt::layer()
        .with_writer(LogBufferWriter(log_ring_buffer.clone()))
        .with_ansi(false);
    let stdout_layer = tracing_subscriber::fmt::layer();
    let env_log_file = std::env::var_os(LOG_FILE_ENV).map(PathBuf::from);
    let log_file_config = resolve_log_file_config(
        log_file_override.as_deref(),
        env_log_file.as_deref(),
        default_windows_log_file_path(),
    );
    let log_file_writer = match log_file_config.as_ref() {
        Some(config) => {
            match weaver_server_core::runtime::log_buffer::open_log_file(&config.path) {
                Ok(writer) => Some(writer),
                Err(error) if config.explicit => {
                    eprintln!(
                        "failed to open Weaver log file at {}: {error}",
                        config.path.display()
                    );
                    std::process::exit(1);
                }
                Err(error) => {
                    eprintln!(
                        "warning: failed to open default Weaver log file at {}: {error}; continuing with console and in-app logs",
                        config.path.display()
                    );
                    None
                }
            }
        }
        None => None,
    };
    let log_file_layer = log_file_writer.map(|writer| {
        tracing_subscriber::fmt::layer()
            .with_writer(LogFileMakeWriter(writer))
            .with_ansi(false)
    });
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(stdout_layer)
        .with(buffer_layer)
        .with(log_file_layer)
        .init();
    install_panic_hook();

    let command = match command {
        Command::Par2 { command } => match commands::par2::run(command) {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                error!("par2 command failed: {error}");
                std::process::exit(2);
            }
        },
        other => other,
    };

    let (mut db, mut config) = match bootstrap::open_db_and_config(&config_path) {
        Ok(result) => result,
        Err(error) => {
            error!("failed to load config: {error}");
            std::process::exit(1);
        }
    };

    bootstrap::reset_login_if_requested(&mut db);
    bootstrap::default_data_dir_from_config_path(&config_path, &mut config);

    if let Err(errors) = bootstrap::validate_config(&config) {
        for message in &errors {
            error!("config: {message}");
        }
        std::process::exit(1);
    }

    let data_dir = PathBuf::from(&config.data_dir);
    if let Err(error) = bootstrap::bootstrap_encryption(&data_dir, &mut db, &mut config) {
        error!("failed to bootstrap encryption key: {error}");
        std::process::exit(1);
    }

    let intermediate_dir = PathBuf::from(config.intermediate_dir());
    let complete_dir = PathBuf::from(config.complete_dir());
    if let Err(error) = bootstrap::ensure_runtime_directories(&[
        ("data_dir", &data_dir),
        ("intermediate_dir", &intermediate_dir),
        ("complete_dir", &complete_dir),
    ]) {
        error!("{error}");
        std::process::exit(1);
    }

    match command {
        Command::Download { nzb, output } => {
            if let Err(error) = commands::download::run(
                &mut config,
                &db,
                &nzb,
                output.as_deref(),
                &data_dir,
                &intermediate_dir,
                &complete_dir,
            )
            .await
            {
                error!("download failed: {error}");
                std::process::exit(1);
            }
        }
        Command::Serve { port, base_url } => {
            if let Err(error) =
                commands::serve::run(config, db, port, &base_url, log_ring_buffer.clone()).await
            {
                error!("server failed: {error}");
                std::process::exit(1);
            }
        }
        Command::Par2 { .. } => unreachable!("par2 command handled before config startup"),
    }
}

fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");

        if let Some(location) = info.location() {
            error!(
                panic.message = payload,
                panic.file = location.file(),
                panic.line = location.line(),
                panic.column = location.column(),
                "unexpected panic"
            );
        } else {
            error!(panic.message = payload, "unexpected panic");
        }

        default_hook(info);
    }));
}

struct ResolvedLogFileConfig {
    path: PathBuf,
    explicit: bool,
}

fn resolve_log_file_config(
    cli_override: Option<&Path>,
    env_override: Option<&Path>,
    default_path: Option<PathBuf>,
) -> Option<ResolvedLogFileConfig> {
    if let Some(path) = cli_override {
        return Some(ResolvedLogFileConfig {
            path: path.to_path_buf(),
            explicit: true,
        });
    }

    if let Some(path) = env_override {
        return Some(ResolvedLogFileConfig {
            path: path.to_path_buf(),
            explicit: true,
        });
    }

    default_path.map(|path| ResolvedLogFileConfig {
        path,
        explicit: false,
    })
}

fn default_windows_log_file_path() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        return std::env::var_os("LOCALAPPDATA").map(|base| {
            PathBuf::from(base)
                .join("weaver")
                .join("logs")
                .join("weaver.log")
        });
    }

    #[cfg(not(windows))]
    {
        None
    }
}

/// Adapter that lets `tracing_subscriber` write to a [`LogRingBuffer`].
#[derive(Clone)]
struct LogBufferWriter(weaver_server_core::runtime::log_buffer::LogRingBuffer);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBufferWriter {
    type Writer = weaver_server_core::runtime::log_buffer::LogRingBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}

#[derive(Clone)]
struct LogFileMakeWriter(weaver_server_core::runtime::log_buffer::LogFileWriter);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogFileMakeWriter {
    type Writer = weaver_server_core::runtime::log_buffer::LogFileWriteHandle;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.make_writer()
    }
}
