mod import;
mod pipeline;
mod server;
mod system;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use weaver_core::buffer::{BufferPool, BufferPoolConfig};
use weaver_core::config::{Config, SharedConfig};
use weaver_core::event::PipelineEvent;
use weaver_core::system::*;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_nntp::pool::ServerPoolConfig;
use weaver_scheduler::{SchedulerCommand, SchedulerHandle};
use weaver_state::Database;

/// Decompress a stored NZB file. Handles both zstd-compressed and legacy
/// uncompressed files transparently (detects zstd magic bytes).
pub(crate) fn decompress_nzb(raw: &[u8]) -> Vec<u8> {
    // zstd magic: 0x28 0xB5 0x2F 0xFD
    if raw.len() >= 4 && raw[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
        zstd::bulk::decompress(raw, 64 * 1024 * 1024).unwrap_or_else(|_| raw.to_vec())
    } else {
        raw.to_vec()
    }
}

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

        /// Base URL path for reverse proxy hosting (e.g. "/weaver").
        #[arg(long, default_value = "/")]
        base_url: String,
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
    let log_ring_buffer = weaver_core::log_buffer::LogRingBuffer::with_default_capacity();
    let buffer_layer = tracing_subscriber::fmt::layer()
        .with_writer(LogBufferWriter(log_ring_buffer.clone()))
        .with_ansi(false);
    let stdout_layer = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(stdout_layer)
        .with(buffer_layer)
        .init();

    let cli = Cli::parse();

    // Open database and load config.
    let (mut db, mut config) = match open_db_and_config(&cli.config) {
        Ok(r) => r,
        Err(e) => {
            error!("failed to load config: {e}");
            std::process::exit(1);
        }
    };

    // Escape hatch: WEAVER_RESET_LOGIN=1 disables login protection on startup.
    if std::env::var("WEAVER_RESET_LOGIN").is_ok_and(|v| v == "1" || v == "true") {
        match db.clear_auth_credentials() {
            Ok(()) => warn!("WEAVER_RESET_LOGIN set — login protection has been disabled"),
            Err(e) => error!("failed to reset login credentials: {e}"),
        }
    }

    // When --config points to a directory and data_dir is unset (fresh DB),
    // default data_dir to that directory. This is the common Docker pattern:
    //   weaver --config /data serve
    if config.data_dir.is_empty() && cli.config.extension().is_none_or(|e| e != "toml") {
        let dir = cli.config.to_string_lossy().to_string();
        info!(data_dir = %dir, "defaulting data_dir to --config directory");
        config.data_dir = dir;
    }

    if let Err(errors) = config.validate() {
        for msg in &errors {
            error!("config: {msg}");
        }
        std::process::exit(1);
    }

    // Bootstrap encryption key for sensitive fields (NNTP/RSS passwords).
    // Must happen before any code reads passwords from the config — the
    // initial load_config above ran without a key, so encrypted passwords
    // were returned as ciphertext.
    let data_dir = PathBuf::from(&config.data_dir);
    match weaver_state::encryption::ensure_encryption_key(Some(data_dir.clone())) {
        Ok(key) => {
            db.set_encryption_key(key);
            if let Err(e) = db.migrate_plaintext_credentials() {
                error!("failed to encrypt existing passwords: {e}");
            }
            // Reload config now that the encryption key is set, so passwords
            // are properly decrypted. Preserve data_dir which was defaulted
            // from --config before the reload.
            let saved_data_dir = config.data_dir.clone();
            match db.load_config() {
                Ok(mut reloaded) => {
                    if reloaded.data_dir.is_empty() {
                        reloaded.data_dir = saved_data_dir;
                    }
                    config = reloaded;
                }
                Err(e) => error!("failed to reload config after setting encryption key: {e}"),
            }
        }
        Err(e) => error!("failed to bootstrap encryption key: {e}"),
    }

    // Preflight: ensure required directories exist and are writable.
    let intermediate_dir = PathBuf::from(config.intermediate_dir());
    let complete_dir = PathBuf::from(config.complete_dir());

    for (label, dir) in [
        ("data_dir", &data_dir),
        ("intermediate_dir", &intermediate_dir),
        ("complete_dir", &complete_dir),
    ] {
        if let Err(e) = std::fs::create_dir_all(dir) {
            error!(
                path = %dir.display(),
                error = %e,
                "cannot create {label} directory — check permissions and volume mounts",
            );
            std::process::exit(1);
        }
        // Verify we can actually write into the directory.
        let probe = dir.join(".weaver-write-probe");
        match std::fs::File::create(&probe) {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
            }
            Err(e) => {
                error!(
                    path = %dir.display(),
                    error = %e,
                    "{label} is not writable — check permissions and volume mounts",
                );
                std::process::exit(1);
            }
        }
    }

    match cli.command {
        Command::Download { nzb, output } => {
            let intermediate_dir = output.unwrap_or(intermediate_dir);
            if let Err(e) = run_download(
                &mut config,
                &db,
                &nzb,
                &data_dir,
                &intermediate_dir,
                &complete_dir,
            )
            .await
            {
                error!("download failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Serve { port, base_url } => {
            // Normalize: ensure leading slash, strip trailing slashes.
            let base_url = format!("/{}", base_url.trim_matches('/'));
            let base_url = if base_url == "/" {
                String::new()
            } else {
                base_url
            };
            if let Err(e) =
                run_server_command(config, db, port, &base_url, log_ring_buffer.clone()).await
            {
                error!("server failed: {e}");
                std::process::exit(1);
            }
        }
    }
}

/// Open the SQLite database and load config.
///
/// If `--config` points to a `.toml` file, the DB is created alongside it and
/// the TOML content is migrated. Otherwise, `--config` is treated as the data
/// directory containing `weaver.db`.
fn open_db_and_config(
    config_path: &Path,
) -> Result<(Database, Config), Box<dyn std::error::Error>> {
    let (db_path, toml_path) = if config_path.extension().is_some_and(|e| e == "toml") {
        // --config weaver.toml → DB lives next to the TOML file.
        let dir = config_path.parent().unwrap_or(Path::new("."));
        (dir.join("weaver.db"), Some(config_path.to_path_buf()))
    } else {
        // --config <dir> → directory containing weaver.db.
        let dir = config_path;
        let toml_candidate = dir.join("weaver.toml");
        let toml = if toml_candidate.exists() {
            Some(toml_candidate)
        } else {
            None
        };
        (dir.join("weaver.db"), toml)
    };

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = Database::open(&db_path)?;

    // Migrate from TOML if the DB is fresh.
    if let Some(ref toml) = toml_path {
        match db.migrate_from_toml(toml) {
            Ok(true) => info!(toml = %toml.display(), "migrated config from TOML to SQLite"),
            Ok(false) => {} // already migrated or TOML not found
            Err(e) => error!(error = %e, "TOML migration failed"),
        }
    }

    let config = db.load_config()?;
    Ok((db, config))
}

/// Build a system profile by probing the host machine.
fn detect_system(output_dir: &Path) -> SystemProfile {
    system::detect(output_dir)
}

/// Run a download job from an NZB file.
async fn run_download(
    config: &mut Config,
    db: &Database,
    nzb_path: &Path,
    data_dir: &Path,
    intermediate_dir: &Path,
    complete_dir: &Path,
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
    let profile = detect_system(data_dir);
    info!(
        cores = profile.cpu.physical_cores,
        storage = ?profile.disk.storage_class,
        iops = format!("{:.0}", profile.disk.random_read_iops),
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
    detect_server_capabilities(config, db).await;
    let nntp = build_nntp_client(config, &profile);
    let initial_global_paused = weaver_api::load_global_pause_from_db(db).await?;

    // Set up scheduler channels and shared control-plane state.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let metrics = weaver_scheduler::PipelineMetrics::new();
    let shared_state = weaver_scheduler::SharedPipelineState::new(metrics, vec![]);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

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
    let standalone_config: weaver_core::config::SharedConfig =
        std::sync::Arc::new(tokio::sync::RwLock::new(config.clone()));
    let mut pipeline = pipeline::Pipeline::new(
        cmd_rx,
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir.to_path_buf(),
        intermediate_dir.to_path_buf(),
        complete_dir.to_path_buf(),
        total_connections,
        write_buf_max,
        vec![],
        initial_global_paused,
        shared_state,
        db.clone(),
        standalone_config,
    )
    .await?;

    // Start the pipeline BEFORE submitting the job — add_job awaits a reply
    // from the pipeline loop, so the loop must be running first.
    let mut pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    // Submit the job via the handle.
    handle
        .add_job(job_id, job_spec, nzb_path.to_path_buf())
        .await?;

    tokio::select! {
        _ = wait_for_shutdown() => {
            info!("received shutdown signal, shutting down");
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during shutdown");
            }
            log_task.abort();
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = pipeline_exit_error(result);
            log_task.abort();
            Err(error.into())
        }
    }
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

fn pipeline_exit_error(result: Result<(), tokio::task::JoinError>) -> std::io::Error {
    match result {
        Ok(()) => {
            error!("pipeline task exited unexpectedly");
            std::io::Error::other("pipeline task exited unexpectedly")
        }
        Err(join_error) => {
            error!(error = %join_error, "pipeline task exited unexpectedly");
            std::io::Error::other(format!("pipeline task exited unexpectedly: {join_error}"))
        }
    }
}

/// Run the HTTP server with the GraphQL API.
async fn run_server_command(
    mut config: Config,
    db: Database,
    port: u16,
    base_url: &str,
    log_ring_buffer: weaver_core::log_buffer::LogRingBuffer,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from(&config.data_dir);
    let intermediate_dir = PathBuf::from(config.intermediate_dir());
    let complete_dir = PathBuf::from(config.complete_dir());

    // Detect system capabilities.
    let profile = detect_system(&data_dir);
    info!(
        cores = profile.cpu.physical_cores,
        storage = ?profile.disk.storage_class,
        iops = format!("{:.0}", profile.disk.random_read_iops),
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
    detect_server_capabilities(&mut config, &db).await;
    let nntp = build_nntp_client(&config, &profile);
    let total_connections: usize = config
        .servers
        .iter()
        .filter(|s| s.active)
        .map(|s| s.connections as usize)
        .sum();

    // Wrap config for shared runtime access.
    let shared_config: SharedConfig = Arc::new(RwLock::new(config));

    // Set up scheduler channels and shared control-plane state.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let metrics = weaver_scheduler::PipelineMetrics::new();
    // SharedPipelineState starts empty; initial_history is published below after recovery.
    let shared_state = weaver_scheduler::SharedPipelineState::new(metrics, vec![]);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

    // One-time migration: convert binary journal to SQLite if it exists.
    let journal_path = data_dir.join(".weaver-journal");
    match db.migrate_from_journal(&journal_path) {
        Ok(true) => info!("migrated journal to SQLite"),
        Ok(false) => {}
        Err(e) => error!(error = %e, "journal migration failed"),
    }

    // Recover active jobs from SQLite.
    let max_id = db.max_job_id_all().unwrap_or(0);
    if max_id > 0 {
        info!(max_id, "recovered max job ID");
    }
    weaver_api::init_job_counter(max_id + 1);

    let active_jobs = db.load_active_jobs().unwrap_or_default();

    // Split recovered jobs into finished (history) vs in-progress (to restore).
    let mut initial_history = Vec::new();
    let mut to_restore = Vec::new();

    for (job_id, recovered) in active_jobs {
        let is_finished = matches!(
            recovered.status.as_str(),
            "complete" | "failed" | "cancelled"
        );

        if is_finished {
            let name = recovered
                .nzb_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("Unknown")
                .to_string();
            let status = status_str_to_job_status(&recovered.status, recovered.error.as_deref());
            initial_history.push(weaver_scheduler::JobInfo {
                job_id,
                name,
                error: if let weaver_scheduler::JobStatus::Failed { error } = &status {
                    Some(error.clone())
                } else {
                    None
                },
                status,
                progress: 1.0,
                total_bytes: 0,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 1000,
                password: None,
                category: recovered.category,
                metadata: recovered.metadata,
                output_dir: Some(recovered.output_dir.display().to_string()),
                created_at_epoch_ms: recovered.created_at as f64 * 1000.0,
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
                    error: Some("NZB file missing after restart".to_string()),
                    status: weaver_scheduler::JobStatus::Failed {
                        error: "NZB file missing after restart".to_string(),
                    },
                    progress: 0.0,
                    total_bytes: 0,
                    downloaded_bytes: 0,
                    optional_recovery_bytes: 0,
                    optional_recovery_downloaded_bytes: 0,
                    failed_bytes: 0,
                    health: 0,
                    password: None,
                    category: recovered.category,
                    metadata: recovered.metadata,
                    output_dir: Some(recovered.output_dir.display().to_string()),
                    created_at_epoch_ms: recovered.created_at as f64 * 1000.0,
                });
                continue;
            }

            match std::fs::read(&recovered.nzb_path) {
                Ok(raw) => {
                    let nzb_bytes = decompress_nzb(&raw);
                    match weaver_nzb::parse_nzb(&nzb_bytes) {
                        Ok(nzb) => {
                            let spec = import::nzb_to_spec(
                                &nzb,
                                &recovered.nzb_path,
                                recovered.category,
                                recovered.metadata,
                            );
                            let status = status_str_to_job_status(
                                &recovered.status,
                                recovered.error.as_deref(),
                            );
                            to_restore.push((
                                job_id,
                                spec,
                                recovered.committed_segments,
                                recovered.extracted_members,
                                status,
                                recovered.output_dir,
                            ));
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

    // Also load archived job history from SQLite so the UI can show completed/failed jobs.
    match db.list_job_history(&weaver_state::HistoryFilter::default()) {
        Ok(history_rows) => {
            for row in history_rows {
                let job_id = weaver_core::id::JobId(row.job_id);
                // Skip if we already have this job from active_jobs recovery.
                if initial_history.iter().any(|j| j.job_id == job_id) {
                    continue;
                }
                let status = status_str_to_job_status(&row.status, row.error_message.as_deref());
                initial_history.push(weaver_scheduler::JobInfo {
                    job_id,
                    name: row.name,
                    error: if let weaver_scheduler::JobStatus::Failed { error } = &status {
                        Some(error.clone())
                    } else {
                        None
                    },
                    status,
                    progress: 1.0,
                    total_bytes: row.total_bytes,
                    downloaded_bytes: row.downloaded_bytes,
                    optional_recovery_bytes: row.optional_recovery_bytes,
                    optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
                    failed_bytes: row.failed_bytes,
                    health: row.health,
                    password: None,
                    category: row.category,
                    metadata: row
                        .metadata
                        .and_then(|m| serde_json::from_str(&m).ok())
                        .unwrap_or_default(),
                    output_dir: row.output_dir,
                    created_at_epoch_ms: row.created_at as f64 * 1000.0,
                });
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to load job history from database");
        }
    }

    if !initial_history.is_empty() {
        info!(
            count = initial_history.len(),
            "recovered finished jobs for history"
        );
    }
    if !to_restore.is_empty() {
        info!(count = to_restore.len(), "recovering in-progress jobs");
    }
    let initial_global_paused = weaver_api::load_global_pause_from_db(&db).await?;

    // Publish recovered history to shared state so the API has data immediately.
    shared_state.publish_jobs(initial_history.clone());

    let rss = weaver_api::RssService::new(handle.clone(), shared_config.clone(), db.clone());
    let backup = weaver_api::BackupService::new(
        handle.clone(),
        shared_config.clone(),
        db.clone(),
        rss.clone(),
    );

    // Load schedules from DB and spawn the schedule evaluator.
    let shared_schedules: weaver_scheduler::schedule::SharedSchedules = {
        let initial = db.list_schedules().unwrap_or_default();
        std::sync::Arc::new(tokio::sync::RwLock::new(initial))
    };
    weaver_scheduler::schedule::spawn_evaluator(handle.clone(), shared_schedules.clone());

    // Build GraphQL schema with shared config and database.
    let pipeline_config = shared_config.clone();
    let schema = weaver_api::build_schema(
        handle.clone(),
        shared_config,
        db.clone(),
        rss.clone(),
        shared_schedules,
        log_ring_buffer,
    );

    // Spawn event persistence subscriber (records meaningful events to SQLite).
    {
        let event_rx = event_tx.subscribe();
        let db_for_events = db.clone();
        tokio::spawn(async move {
            if let Err(panic) = tokio::spawn(persist_events(event_rx, db_for_events)).await {
                tracing::error!(
                    error = %panic,
                    "CRITICAL: event persistence task panicked — events will not be recorded"
                );
            }
        });
    }

    // Create and start the pipeline.
    let mut pipeline = pipeline::Pipeline::new(
        cmd_rx,
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir,
        intermediate_dir,
        complete_dir,
        total_connections,
        write_buf_max,
        initial_history,
        initial_global_paused,
        shared_state,
        db.clone(),
        pipeline_config,
    )
    .await?;

    let mut pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    // Restore in-progress jobs from SQLite.
    for (job_id, spec, committed, extracted_members, status, working_dir) in to_restore {
        let committed_count = committed.len();
        match handle
            .restore_job(
                job_id,
                spec,
                committed,
                extracted_members,
                status,
                working_dir,
            )
            .await
        {
            Ok(()) => info!(job_id = job_id.0, committed_count, "job restored"),
            Err(e) => error!(job_id = job_id.0, error = %e, "failed to restore job"),
        }
    }

    let rss_task = rss.start_background_loop();

    // Run HTTP server.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let server_task = tokio::spawn(server::run_server(
        schema,
        handle.clone(),
        db.clone(),
        backup,
        addr,
        base_url.to_owned(),
    ));

    tokio::select! {
        _ = wait_for_shutdown() => {
            info!("received shutdown signal, shutting down");
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during shutdown");
            }
            server_task.abort();
            rss_task.abort();
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = pipeline_exit_error(result);
            server_task.abort();
            rss_task.abort();
            Err(error.into())
        }
    }
}

/// Probe each server for capability detection (pipelining, etc.) and update config.
///
/// Connects to each active server, checks CAPABILITIES, and saves the result.
/// Runs at startup so servers loaded from the database get their capabilities detected.
async fn detect_server_capabilities(config: &mut Config, db: &Database) {
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

        // Persist capability detection to database.
        let s = server.clone();
        let db = db.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || db.update_server(&s)).await {
            error!(error = %e, "failed to persist server capabilities");
        }
    }
}

/// Convert a status string (from SQLite) to a scheduler JobStatus.
fn status_str_to_job_status(status: &str, error: Option<&str>) -> weaver_scheduler::JobStatus {
    match status {
        "downloading" => weaver_scheduler::JobStatus::Downloading,
        "verifying" => weaver_scheduler::JobStatus::Verifying,
        "repairing" => weaver_scheduler::JobStatus::Repairing,
        "extracting" => weaver_scheduler::JobStatus::Extracting,
        "complete" => weaver_scheduler::JobStatus::Complete,
        "failed" => weaver_scheduler::JobStatus::Failed {
            error: error.unwrap_or("unknown error").to_string(),
        },
        "paused" => weaver_scheduler::JobStatus::Paused,
        "cancelled" => weaver_scheduler::JobStatus::Failed {
            error: "cancelled".to_string(),
        },
        other => weaver_scheduler::JobStatus::Failed {
            error: format!("unknown status: {other}"),
        },
    }
}

/// Build an NntpClient from the config's active server list.
pub fn build_nntp_client(
    config: &Config,
    profile: &weaver_core::system::SystemProfile,
) -> NntpClient {
    let mut active: Vec<&weaver_core::config::ServerConfig> =
        config.servers.iter().filter(|s| s.active).collect();
    active.sort_by_key(|s| (s.priority, s.id));
    let total_connections: usize = active.iter().map(|s| s.connections as usize).sum();
    let effective_memory = profile
        .memory
        .cgroup_limit
        .unwrap_or(profile.memory.available_bytes);
    let buffer_profile =
        weaver_nntp::connection::NntpBufferProfile::adaptive(effective_memory, total_connections);
    let servers: Vec<ServerPoolConfig> = active
        .iter()
        .map(|s| ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: s.host.clone(),
                port: s.port,
                tls: s.tls,
                username: s.username.clone(),
                password: s.password.clone(),
                buffer_profile,
                ..Default::default()
            },
            max_connections: s.connections as usize,
            group: s.priority,
        })
        .collect();

    let client_config = NntpClientConfig {
        servers,
        max_idle_age: std::time::Duration::from_secs(300),
        max_retries_per_server: 1,
    };

    NntpClient::new(client_config)
}

/// Background task that subscribes to pipeline events and persists meaningful
/// ones to SQLite for the job event log.
async fn persist_events(mut rx: broadcast::Receiver<PipelineEvent>, db: Database) {
    use weaver_api::PipelineEventGql;

    // Only persist job-level milestones. Per-segment and per-file events go to
    // system logs (tracing) only — they're too numerous for SQLite on large NZBs.
    fn is_noisy(event: &PipelineEvent) -> bool {
        matches!(
            event,
            // Per-segment download/decode events
            PipelineEvent::ArticleDownloaded { .. }
                | PipelineEvent::ArticleNotFound { .. }
                | PipelineEvent::SegmentQueued { .. }
                | PipelineEvent::SegmentDecoded { .. }
                | PipelineEvent::SegmentDecodeFailed { .. }
                | PipelineEvent::SegmentCommitted { .. }
                | PipelineEvent::SegmentRetryScheduled { .. }
                | PipelineEvent::SegmentFailedPermanent { .. }
                // Per-file events
                | PipelineEvent::FileComplete { .. }
                | PipelineEvent::FileClassified { .. }
                | PipelineEvent::VerificationStarted { .. }
                | PipelineEvent::VerificationComplete { .. }
                | PipelineEvent::ExtractionProgress { .. }
                | PipelineEvent::RepairConfidenceUpdated { .. }
        )
    }

    let mut batch: Vec<weaver_state::JobEvent> = Vec::new();
    let flush_interval = tokio::time::Duration::from_secs(1);

    loop {
        let recv = if batch.is_empty() {
            // No pending events — wait indefinitely for the next one.
            tokio::select! {
                result = rx.recv() => result,
            }
        } else {
            // Pending events — flush after 1s if nothing new arrives.
            tokio::select! {
                result = rx.recv() => result,
                _ = tokio::time::sleep(flush_interval) => {
                    let events = std::mem::take(&mut batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
                        }
                    });
                    continue;
                }
            }
        };

        match recv {
            Ok(event) => {
                if !is_noisy(&event) {
                    let gql = PipelineEventGql::from(&event);
                    if let Some(job_id) = gql.job_id {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        batch.push(weaver_state::JobEvent {
                            job_id,
                            timestamp: now,
                            kind: format!("{:?}", gql.kind),
                            message: gql.message,
                            file_id: gql.file_id,
                        });
                    }
                }

                if batch.len() >= 50 {
                    let events = std::mem::take(&mut batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
                        }
                    });
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::debug!(skipped = n, "event persistence lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    // Flush remaining events on shutdown.
    if !batch.is_empty() {
        let _ = db.insert_job_events(&batch);
    }
}

/// Adapter that lets `tracing_subscriber` write to a [`LogRingBuffer`].
#[derive(Clone)]
struct LogBufferWriter(weaver_core::log_buffer::LogRingBuffer);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBufferWriter {
    type Writer = weaver_core::log_buffer::LogRingBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}
