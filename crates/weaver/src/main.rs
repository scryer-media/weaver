mod import;
mod pipeline;
mod server;
mod system;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use weaver_core::buffer::{BufferPool, BufferPoolConfig};
use weaver_core::config::{Config, SharedConfig};
use weaver_core::event::PipelineEvent;
use weaver_core::system::*;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_nntp::pool::ServerPoolConfig;
use weaver_scheduler::{SchedulerCommand, SchedulerHandle};

#[derive(Parser)]
#[command(name = "weaver", about = "Usenet binary downloader")]
struct Cli {
    /// Path to configuration file.
    #[arg(short, long, default_value = "weaver.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download an NZB file.
    Download {
        /// Path to the NZB file.
        nzb: PathBuf,

        /// Output directory (overrides config).
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Start the HTTP server with GraphQL API.
    Serve {
        /// Port to listen on (default: 9090).
        #[arg(short, long, default_value = "9090")]
        port: u16,
    },
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024) // 8 MB — pipeline futures are large
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async_main());
}

async fn async_main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .init();

    let cli = Cli::parse();

    // Load config.
    let mut config = match load_config(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config: {e}");
            std::process::exit(1);
        }
    };

    if let Err(errors) = config.validate() {
        for msg in &errors {
            error!("config: {msg}");
        }
        std::process::exit(1);
    }

    match cli.command {
        Command::Download { nzb, output } => {
            let output_dir = output.unwrap_or_else(|| PathBuf::from(&config.data_dir));
            if let Err(e) = run_download(&mut config, &nzb, &output_dir).await {
                error!("download failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Serve { port } => {
            if let Err(e) = run_server_command(config, port).await {
                error!("server failed: {e}");
                std::process::exit(1);
            }
        }
    }
}

/// Load and parse the TOML config file.
fn load_config(path: &PathBuf) -> Result<Config, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let mut config: Config = toml::from_str(&contents)?;
    config.config_path = Some(path.clone());
    config.assign_server_ids();
    Ok(config)
}

/// Build a system profile by probing the host machine.
fn detect_system(output_dir: &Path) -> SystemProfile {
    system::detect(output_dir)
}

/// Run a download job from an NZB file.
async fn run_download(
    config: &mut Config,
    nzb_path: &PathBuf,
    output_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read and parse NZB.
    let nzb_bytes = std::fs::read(nzb_path)?;
    let (job_id, job_spec) = import::import_nzb(&nzb_bytes, nzb_path)?;

    info!(
        job = %job_spec.name,
        files = job_spec.files.len(),
        bytes = job_spec.total_bytes,
        "starting download"
    );

    // Detect system capabilities.
    let profile = detect_system(output_dir);
    info!(
        cores = profile.cpu.physical_cores,
        storage = ?profile.disk.storage_class,
        "system profile"
    );

    // Initialize buffer pool scaled to available memory.
    let effective_memory = profile
        .memory
        .cgroup_limit
        .unwrap_or(profile.memory.available_bytes);
    let buf_config = BufferPoolConfig::for_available_memory(effective_memory);
    let write_buf_max = buf_config.write_buffer_max_pending();
    info!(
        available_mb = effective_memory / (1024 * 1024),
        total_mb = buf_config.total_bytes() / (1024 * 1024),
        small = buf_config.small_count,
        medium = buf_config.medium_count,
        large = buf_config.large_count,
        write_buf_max,
        "buffer pool initialized (memory-adaptive)"
    );
    let buffers = BufferPool::new(buf_config);

    // Detect server capabilities (pipelining, etc.) and build NNTP client.
    detect_server_capabilities(config).await;
    let nntp = build_nntp_client(config);

    // Set up scheduler channels.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone());

    // Subscribe to events for progress logging.
    let mut event_rx = event_tx.subscribe();
    let log_task = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            match &event {
                PipelineEvent::JobCreated {
                    name,
                    total_files,
                    total_bytes,
                    ..
                } => {
                    info!(
                        name,
                        files = total_files,
                        bytes = total_bytes,
                        "job created"
                    );
                }
                PipelineEvent::FileComplete {
                    filename,
                    total_bytes,
                    ..
                } => {
                    info!(filename, bytes = total_bytes, "file complete");
                }
                PipelineEvent::JobCompleted { job_id, .. } => {
                    info!(job_id = job_id.0, "job completed");
                }
                PipelineEvent::JobFailed { job_id, error, .. } => {
                    error!(job_id = job_id.0, error, "job failed");
                }
                _ => {}
            }
        }
    });

    // Create and start the pipeline.
    let total_connections: usize = config.servers.iter().map(|s| s.connections as usize).sum();
    let mut pipeline =
        pipeline::Pipeline::new(cmd_rx, event_tx, nntp, buffers, profile, output_dir.clone(), total_connections, write_buf_max, vec![])
            .await?;

    // Start the pipeline BEFORE submitting the job — add_job awaits a reply
    // from the pipeline loop, so the loop must be running first.
    let pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    // Submit the job via the handle.
    handle.add_job(job_id, job_spec, nzb_path.clone()).await?;

    // Wait for shutdown signal.
    wait_for_shutdown().await;
    info!("received shutdown signal, shutting down");
    handle.shutdown().await.ok();

    pipeline_task.await.ok();
    log_task.abort();

    Ok(())
}

/// Wait for either SIGTERM or ctrl-c.
async fn wait_for_shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}

/// Read all journal entries and compute the max job ID.
/// Returns (max_id, entries). Returns (0, vec![]) if the journal doesn't exist.
async fn read_journal(journal_path: &Path) -> (u64, Vec<weaver_state::JournalEntry>) {
    use weaver_state::{JournalEntry, JournalReader};

    if !journal_path.exists() {
        return (0, vec![]);
    }

    let mut reader = match JournalReader::open(journal_path).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "could not open journal");
            return (0, vec![]);
        }
    };

    let entries = match reader.read_all().await {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, "journal read error");
            return (0, vec![]);
        }
    };

    let mut max_id: u64 = 0;
    for entry in &entries {
        let id = match entry {
            JournalEntry::JobCreated { job_id, .. }
            | JournalEntry::JobStatusChanged { job_id, .. }
            | JournalEntry::Par2MetadataLoaded { job_id, .. }
            | JournalEntry::MemberExtracted { job_id, .. }
            | JournalEntry::ExtractionComplete { job_id, .. }
            | JournalEntry::Checkpoint { job_id, .. } => job_id.0,
            JournalEntry::SegmentCommitted { segment_id, .. } => segment_id.file_id.job_id.0,
            JournalEntry::FileComplete { file_id, .. }
            | JournalEntry::FileVerified { file_id, .. } => file_id.job_id.0,
        };
        if id > max_id {
            max_id = id;
        }
    }

    (max_id, entries)
}

/// Run the HTTP server with the GraphQL API.
async fn run_server_command(
    mut config: Config,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from(&config.data_dir);

    // Detect system capabilities.
    let profile = detect_system(&output_dir);
    info!(
        cores = profile.cpu.physical_cores,
        storage = ?profile.disk.storage_class,
        "system profile"
    );

    // Initialize buffer pool scaled to available memory.
    let effective_memory = profile
        .memory
        .cgroup_limit
        .unwrap_or(profile.memory.available_bytes);
    let buf_config = BufferPoolConfig::for_available_memory(effective_memory);
    let write_buf_max = buf_config.write_buffer_max_pending();
    info!(
        available_mb = effective_memory / (1024 * 1024),
        total_mb = buf_config.total_bytes() / (1024 * 1024),
        small = buf_config.small_count,
        medium = buf_config.medium_count,
        large = buf_config.large_count,
        write_buf_max,
        "buffer pool initialized (memory-adaptive)"
    );
    let buffers = BufferPool::new(buf_config);

    // Detect server capabilities (pipelining, etc.) and build NNTP client.
    detect_server_capabilities(&mut config).await;
    let nntp = build_nntp_client(&config);
    let total_connections: usize = config
        .servers
        .iter()
        .filter(|s| s.active)
        .map(|s| s.connections as usize)
        .sum();

    // Wrap config for shared runtime access.
    let shared_config: SharedConfig = Arc::new(RwLock::new(config));

    // Set up scheduler channels.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone());

    // Read journal and recover state.
    let journal_path = output_dir.join(".weaver-journal");
    let (max_id, journal_entries) = read_journal(&journal_path).await;
    if max_id > 0 {
        info!(max_id, "recovered max job ID from journal");
    }
    weaver_api::init_job_counter(max_id + 1);

    // Replay journal to recover job state.
    let recovery = weaver_state::recover(journal_entries);

    // Split recovered jobs into finished (history) vs in-progress (to restore).
    let mut initial_history = Vec::new();
    let mut to_restore = Vec::new();

    for (job_id, recovered) in recovery.jobs {
        let is_finished = matches!(
            recovered.status,
            weaver_state::PersistedJobStatus::Complete
                | weaver_state::PersistedJobStatus::Failed { .. }
        );

        if is_finished {
            // Build a minimal JobInfo for history display.
            let name = recovered
                .nzb_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("Unknown")
                .to_string();
            let status = persisted_to_job_status(&recovered.status);
            initial_history.push(weaver_scheduler::JobInfo {
                job_id,
                name,
                status,
                progress: 1.0,
                total_bytes: 0,
                downloaded_bytes: 0,
                failed_bytes: 0,
                health: 1000,
                password: None,
                category: recovered.category,
                metadata: recovered.metadata,
                output_dir: Some(recovered.output_dir.display().to_string()),
            });
        } else {
            // In-progress job — need to re-parse NZB and restore.
            if !recovered.nzb_path.exists() {
                tracing::warn!(
                    job_id = job_id.0,
                    nzb_path = %recovered.nzb_path.display(),
                    "NZB file missing for recovered job, marking as failed"
                );
                initial_history.push(weaver_scheduler::JobInfo {
                    job_id,
                    name: recovered
                        .nzb_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    status: weaver_scheduler::JobStatus::Failed {
                        error: "NZB file missing after restart".to_string(),
                    },
                    progress: 0.0,
                    total_bytes: 0,
                    downloaded_bytes: 0,
                    failed_bytes: 0,
                    health: 0,
                    password: None,
                    category: recovered.category,
                    metadata: recovered.metadata,
                    output_dir: Some(recovered.output_dir.display().to_string()),
                });
                continue;
            }

            match std::fs::read(&recovered.nzb_path) {
                Ok(nzb_bytes) => {
                    match weaver_nzb::parse_nzb(&nzb_bytes) {
                        Ok(nzb) => {
                            let spec = import::nzb_to_spec(
                                &nzb,
                                &recovered.nzb_path,
                                recovered.category,
                                recovered.metadata,
                            );
                            let status = persisted_to_job_status(&recovered.status);
                            to_restore.push((job_id, spec, recovered.committed_segments, status));
                        }
                        Err(e) => {
                            tracing::warn!(
                                job_id = job_id.0,
                                error = %e,
                                "failed to parse NZB for recovered job"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = job_id.0,
                        error = %e,
                        "failed to read NZB for recovered job"
                    );
                }
            }
        }
    }

    if !initial_history.is_empty() {
        info!(count = initial_history.len(), "recovered finished jobs for history");
    }
    if !to_restore.is_empty() {
        info!(count = to_restore.len(), "recovering in-progress jobs");
    }

    // Build GraphQL schema with shared config.
    let schema = weaver_api::build_schema(handle.clone(), shared_config);

    // Create and start the pipeline.
    let mut pipeline =
        pipeline::Pipeline::new(cmd_rx, event_tx, nntp, buffers, profile, output_dir, total_connections, write_buf_max, initial_history).await?;

    let pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    // Restore in-progress jobs from journal.
    for (job_id, spec, committed, status) in to_restore {
        let committed_count = committed.len();
        match handle.restore_job(job_id, spec, committed, status).await {
            Ok(()) => info!(job_id = job_id.0, committed_count, "job restored"),
            Err(e) => error!(job_id = job_id.0, error = %e, "failed to restore job"),
        }
    }

    // Run HTTP server.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let server_task = tokio::spawn(server::run_server(schema, addr));

    // Wait for shutdown signal.
    wait_for_shutdown().await;
    info!("received shutdown signal, shutting down");
    handle.shutdown().await.ok();

    pipeline_task.await.ok();
    server_task.abort();

    Ok(())
}

/// Probe each server for capability detection (pipelining, etc.) and update config.
///
/// Connects to each active server, checks CAPABILITIES, and saves the result.
/// Runs at startup so servers loaded from TOML get their capabilities detected.
async fn detect_server_capabilities(config: &mut Config) {
    for server in config.servers.iter_mut().filter(|s| s.active) {
        let nntp_config = weaver_nntp::ServerConfig {
            host: server.host.clone(),
            port: server.port,
            tls: server.tls,
            username: server.username.clone(),
            password: server.password.clone(),
            ..Default::default()
        };

        match weaver_nntp::NntpConnection::connect(&nntp_config).await {
            Ok(mut conn) => {
                let pipelining = conn.capabilities().supports_pipelining();
                server.supports_pipelining = pipelining;
                info!(
                    host = %server.host,
                    pipelining,
                    "detected server capabilities"
                );
                let _ = conn.quit().await;
            }
            Err(e) => {
                info!(
                    host = %server.host,
                    error = %e,
                    "capability detection failed, assuming no pipelining"
                );
                server.supports_pipelining = false;
            }
        }
    }

    if let Err(e) = config.save() {
        error!(error = %e, "failed to save config after capability detection");
    }
}

/// Convert a persisted journal status to a scheduler JobStatus.
fn persisted_to_job_status(status: &weaver_state::PersistedJobStatus) -> weaver_scheduler::JobStatus {
    match status {
        weaver_state::PersistedJobStatus::Downloading => weaver_scheduler::JobStatus::Downloading,
        weaver_state::PersistedJobStatus::Verifying => weaver_scheduler::JobStatus::Verifying,
        weaver_state::PersistedJobStatus::Repairing => weaver_scheduler::JobStatus::Repairing,
        weaver_state::PersistedJobStatus::Extracting => weaver_scheduler::JobStatus::Extracting,
        weaver_state::PersistedJobStatus::Complete => weaver_scheduler::JobStatus::Complete,
        weaver_state::PersistedJobStatus::Failed { error } => weaver_scheduler::JobStatus::Failed {
            error: error.clone(),
        },
        weaver_state::PersistedJobStatus::Paused => weaver_scheduler::JobStatus::Paused,
    }
}

/// Build an NntpClient from the config's active server list.
pub fn build_nntp_client(config: &Config) -> NntpClient {
    let servers: Vec<ServerPoolConfig> = config
        .servers
        .iter()
        .filter(|s| s.active)
        .map(|s| ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: s.host.clone(),
                port: s.port,
                tls: s.tls,
                username: s.username.clone(),
                password: s.password.clone(),
                ..Default::default()
            },
            max_connections: s.connections as usize,
        })
        .collect();

    let client_config = NntpClientConfig {
        servers,
        max_idle_age: std::time::Duration::from_secs(300),
        max_retries_per_server: 1,
    };

    NntpClient::new(client_config)
}
