mod args;
mod bootstrap;
mod commands;
mod http;
mod shutdown;
mod wiring;

use std::path::PathBuf;

use clap::Parser;
use tracing::error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::args::{Cli, Command};

fn main() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_stack_size(8 * 1024 * 1024); // 8 MB - pipeline futures are large
    weaver_server_core::runtime::affinity::install_tokio_worker_affinity(&mut builder);

    let runtime = builder.build().expect("failed to build tokio runtime");

    runtime.block_on(async_main());
}

async fn async_main() {
    let log_ring_buffer =
        weaver_server_core::runtime::log_buffer::LogRingBuffer::with_default_capacity();
    let buffer_layer = tracing_subscriber::fmt::layer()
        .with_writer(LogBufferWriter(log_ring_buffer.clone()))
        .with_ansi(false);
    let stdout_layer = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(stdout_layer)
        .with(buffer_layer)
        .init();

    let Cli {
        config: config_path,
        command,
    } = Cli::parse();
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
    bootstrap::bootstrap_encryption(&data_dir, &mut db, &mut config);

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

/// Adapter that lets `tracing_subscriber` write to a [`LogRingBuffer`].
#[derive(Clone)]
struct LogBufferWriter(weaver_server_core::runtime::log_buffer::LogRingBuffer);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBufferWriter {
    type Writer = weaver_server_core::runtime::log_buffer::LogRingBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}
