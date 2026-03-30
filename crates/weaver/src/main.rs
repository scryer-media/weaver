mod e2e_failpoint;
mod import;
mod pipeline;
mod runtime_affinity;
mod server;
mod system;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
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

    /// Local PAR2 verification and repair.
    Par2 {
        #[command(subcommand)]
        command: Par2Command,
    },
}

#[derive(Args, Clone)]
struct Par2Args {
    /// PAR2 index/volume file, or a directory containing PAR2 files.
    #[arg(value_name = "PAR2")]
    input: PathBuf,

    /// Primary directory for reading and writing repaired files.
    #[arg(short = 'C', long, value_name = "DIR")]
    working_dir: Option<PathBuf>,

    /// Additional directories to search for data files.
    #[arg(value_name = "SEARCH_DIR")]
    search_dirs: Vec<PathBuf>,
}

#[derive(Subcommand, Clone)]
enum Par2Command {
    /// Verify files against a PAR2 set.
    #[command(alias = "v")]
    Verify(Par2Args),

    /// Repair files using a PAR2 set.
    #[command(alias = "r")]
    Repair {
        #[command(flatten)]
        args: Par2Args,
    },
}

struct ResolvedPar2Input {
    par2_paths: Vec<PathBuf>,
    primary_dir: PathBuf,
    search_dirs: Vec<PathBuf>,
}

fn main() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_stack_size(8 * 1024 * 1024); // 8 MB — pipeline futures are large
    runtime_affinity::install_tokio_worker_affinity(&mut builder);

    let runtime = builder.build().expect("failed to build tokio runtime");

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

    let Cli {
        config: config_path,
        command,
    } = Cli::parse();
    let command = match command {
        Command::Par2 { command } => match run_par2_command(command) {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                error!("par2 command failed: {error}");
                std::process::exit(2);
            }
        },
        other => other,
    };

    // Open database and load config.
    let (mut db, mut config) = match open_db_and_config(&config_path) {
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
    if config.data_dir.is_empty() && config_path.extension().is_none_or(|e| e != "toml") {
        let dir = config_path.to_string_lossy().to_string();
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

    match command {
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
        Command::Par2 { .. } => unreachable!("par2 command handled before config startup"),
    }
}

fn run_par2_command(command: Par2Command) -> Result<i32, Box<dyn std::error::Error>> {
    match command {
        Par2Command::Verify(args) => run_par2_verify(args),
        Par2Command::Repair { args } => run_par2_repair(args),
    }
}

fn run_par2_verify(args: Par2Args) -> Result<i32, Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let resolved = resolve_par2_input(&args)?;
    let par2_set = load_par2_set(&resolved.par2_paths)?;

    print_par2_context("verify", &resolved, &par2_set);

    let (verification, placement_plan) = verify_par2_set(&resolved, &par2_set)?;
    print_verification_report(&verification, placement_plan.as_ref(), &par2_set);
    println!("verify completed in {:.2?}", started.elapsed());

    Ok(if verification.total_missing_blocks == 0 {
        0
    } else {
        1
    })
}

fn run_par2_repair(args: Par2Args) -> Result<i32, Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let resolved = resolve_par2_input(&args)?;
    std::fs::create_dir_all(&resolved.primary_dir)?;
    let par2_set = load_par2_set(&resolved.par2_paths)?;

    print_par2_context("repair", &resolved, &par2_set);

    let (verification, placement_plan) = verify_par2_set(&resolved, &par2_set)?;
    println!("pre-repair verification:");
    print_verification_report(&verification, placement_plan.as_ref(), &par2_set);

    match verification.repairable {
        weaver_par2::Repairability::NotNeeded => {
            println!("no repair needed");
            println!("repair completed in {:.2?}", started.elapsed());
            return Ok(0);
        }
        weaver_par2::Repairability::Insufficient {
            blocks_needed,
            blocks_available,
            deficit,
        } => {
            println!(
                "repair not possible: need {blocks_needed} blocks, have {blocks_available} (deficit {deficit})"
            );
            println!("repair completed in {:.2?}", started.elapsed());
            return Ok(1);
        }
        weaver_par2::Repairability::Repairable { .. } => {}
    }

    if let Some(plan) = &placement_plan
        && (!plan.swaps.is_empty() || !plan.renames.is_empty())
    {
        let moved = weaver_par2::apply_placement_plan(&resolved.primary_dir, plan)?;
        println!("normalized file placement before repair: moved {moved} file(s)");
    }

    let repair_plan = weaver_par2::plan_repair(&par2_set, &verification)?;
    println!(
        "repairing {} slice(s) using {} recovery block(s)",
        repair_plan.missing_slices.len(),
        repair_plan.recovery_exponents.len()
    );

    let options = weaver_par2::RepairOptions::default();

    let repair_started = std::time::Instant::now();
    let mut repair_access: Box<dyn weaver_par2::FileAccess> =
        build_repair_access(&resolved, &par2_set, placement_plan.as_ref());
    weaver_par2::execute_repair_with_options(
        &repair_plan,
        &par2_set,
        &mut *repair_access,
        &options,
    )?;
    println!("repair pass completed in {:.2?}", repair_started.elapsed());

    let post_repair_started = std::time::Instant::now();
    let final_verification = verify_after_repair(&resolved, &par2_set)?;
    println!("post-repair verification:");
    print_verification_report(&final_verification, None, &par2_set);
    println!(
        "post-repair verify completed in {:.2?}",
        post_repair_started.elapsed()
    );
    println!("repair completed in {:.2?}", started.elapsed());

    Ok(if final_verification.total_missing_blocks == 0 {
        0
    } else {
        1
    })
}

fn resolve_par2_input(args: &Par2Args) -> Result<ResolvedPar2Input, Box<dyn std::error::Error>> {
    let input = if args.input.exists() {
        args.input.clone()
    } else {
        return Err(format!("input path does not exist: {}", args.input.display()).into());
    };

    let par2_paths = if input.is_dir() {
        collect_par2_paths_from_dir(&input)?
    } else {
        discover_matching_par2_paths(&input)?
    };

    let primary_dir = args.working_dir.clone().unwrap_or_else(|| {
        if input.is_dir() {
            input.clone()
        } else {
            input
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        }
    });
    validate_directory_path(&primary_dir, "working directory")?;
    for dir in &args.search_dirs {
        validate_directory_path(dir, "search directory")?;
    }

    Ok(ResolvedPar2Input {
        par2_paths,
        primary_dir,
        search_dirs: args.search_dirs.clone(),
    })
}

fn validate_directory_path(path: &Path, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() && !path.is_dir() {
        return Err(format!("{label} is not a directory: {}", path.display()).into());
    }
    Ok(())
}

fn collect_par2_paths_from_dir(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut par2_paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.to_ascii_lowercase().ends_with(".par2") {
            par2_paths.push(entry.path());
        }
    }
    par2_paths.sort();
    if par2_paths.is_empty() {
        return Err(format!("no .par2 files found in {}", dir.display()).into());
    }
    Ok(par2_paths)
}

fn discover_matching_par2_paths(input: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let seed_set = weaver_par2::Par2FileSet::from_paths(&[input])?;
    let parent = input.parent().unwrap_or_else(|| Path::new("."));
    let mut par2_paths = weaver_par2::identify_par2_files(parent, &seed_set.recovery_set_id)?;
    if par2_paths.is_empty() {
        par2_paths.push(input.to_path_buf());
    }
    par2_paths.sort();
    par2_paths.dedup();
    Ok(par2_paths)
}

fn cleanup_orphaned_persisted_nzbs(
    nzb_dir: &Path,
    referenced_paths: &HashSet<PathBuf>,
) -> Result<usize, std::io::Error> {
    if !nzb_dir.exists() {
        return Ok(0);
    }

    let mut removed = 0usize;
    for entry in std::fs::read_dir(nzb_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if referenced_paths.contains(&path) {
            continue;
        }
        std::fs::remove_file(&path)?;
        removed += 1;
    }

    Ok(removed)
}

fn load_par2_set(
    par2_paths: &[PathBuf],
) -> Result<weaver_par2::Par2FileSet, Box<dyn std::error::Error>> {
    Ok(weaver_par2::Par2FileSet::from_paths(par2_paths)?)
}

fn verify_par2_set(
    resolved: &ResolvedPar2Input,
    par2_set: &weaver_par2::Par2FileSet,
) -> Result<
    (
        weaver_par2::VerificationResult,
        Option<weaver_par2::PlacementPlan>,
    ),
    Box<dyn std::error::Error>,
> {
    if resolved.search_dirs.is_empty() {
        let placement_plan = weaver_par2::scan_placement(&resolved.primary_dir, par2_set)?;
        if !placement_plan.conflicts.is_empty() {
            let conflicts = format_conflict_filenames(&placement_plan, par2_set);
            return Err(format!("placement scan found ambiguous matches: {conflicts}").into());
        }
        let access = weaver_par2::PlacementFileAccess::from_plan(
            resolved.primary_dir.clone(),
            par2_set,
            &placement_plan,
        );
        let verification = weaver_par2::verify_all(par2_set, &access);
        Ok((verification, Some(placement_plan)))
    } else {
        let access = weaver_par2::MultiDirectoryFileAccess::new(
            resolved.primary_dir.clone(),
            resolved.search_dirs.clone(),
            par2_set,
        );
        let verification = weaver_par2::verify_all(par2_set, &access);
        Ok((verification, None))
    }
}

fn verify_after_repair(
    resolved: &ResolvedPar2Input,
    par2_set: &weaver_par2::Par2FileSet,
) -> Result<weaver_par2::VerificationResult, Box<dyn std::error::Error>> {
    if resolved.search_dirs.is_empty() {
        let access = weaver_par2::DiskFileAccess::new(resolved.primary_dir.clone(), par2_set);
        Ok(weaver_par2::verify_all(par2_set, &access))
    } else {
        let access = weaver_par2::MultiDirectoryFileAccess::new(
            resolved.primary_dir.clone(),
            resolved.search_dirs.clone(),
            par2_set,
        );
        Ok(weaver_par2::verify_all(par2_set, &access))
    }
}

fn build_repair_access(
    resolved: &ResolvedPar2Input,
    par2_set: &weaver_par2::Par2FileSet,
    placement_plan: Option<&weaver_par2::PlacementPlan>,
) -> Box<dyn weaver_par2::FileAccess> {
    if resolved.search_dirs.is_empty() {
        if let Some(plan) = placement_plan
            && plan.swaps.is_empty()
            && plan.renames.is_empty()
        {
            return Box::new(weaver_par2::PlacementFileAccess::from_plan(
                resolved.primary_dir.clone(),
                par2_set,
                plan,
            ));
        }
        Box::new(weaver_par2::DiskFileAccess::new(
            resolved.primary_dir.clone(),
            par2_set,
        ))
    } else {
        Box::new(weaver_par2::MultiDirectoryFileAccess::new(
            resolved.primary_dir.clone(),
            resolved.search_dirs.clone(),
            par2_set,
        ))
    }
}

fn print_par2_context(
    action: &str,
    resolved: &ResolvedPar2Input,
    par2_set: &weaver_par2::Par2FileSet,
) {
    println!("weaver par2 {action}");
    println!("par2 files: {}", resolved.par2_paths.len());
    for path in &resolved.par2_paths {
        println!("  {}", path.display());
    }
    println!("working dir: {}", resolved.primary_dir.display());
    if !resolved.search_dirs.is_empty() {
        println!("search dirs:");
        for dir in &resolved.search_dirs {
            println!("  {}", dir.display());
        }
    }
    println!(
        "par2 set: files={}, slice_size={}, recovery_blocks={}",
        par2_set.files.len(),
        par2_set.slice_size,
        par2_set.recovery_block_count()
    );
}

fn print_verification_report(
    verification: &weaver_par2::VerificationResult,
    placement_plan: Option<&weaver_par2::PlacementPlan>,
    par2_set: &weaver_par2::Par2FileSet,
) {
    if let Some(plan) = placement_plan {
        println!(
            "placement: exact={}, renames={}, swaps={}, unresolved={}, conflicts={}",
            plan.exact.len(),
            plan.renames.len(),
            plan.swaps.len(),
            plan.unresolved.len(),
            plan.conflicts.len()
        );
        for entry in &plan.renames {
            println!("  rename: {} -> {}", entry.current_name, entry.correct_name);
        }
        for (left, right) in &plan.swaps {
            println!("  swap: {} <-> {}", left.current_name, right.current_name);
        }
        if !plan.unresolved.is_empty() {
            let unresolved = plan
                .unresolved
                .iter()
                .filter_map(|file_id| par2_set.file_description(file_id))
                .map(|desc| desc.filename.clone())
                .collect::<Vec<_>>();
            if !unresolved.is_empty() {
                println!("  unresolved: {}", unresolved.join(", "));
            }
        }
    }

    let mut complete = 0usize;
    let mut damaged = 0usize;
    let mut missing = 0usize;
    for file in &verification.files {
        match &file.status {
            weaver_par2::FileStatus::Complete => complete += 1,
            weaver_par2::FileStatus::Damaged(bad_slices) => {
                damaged += 1;
                println!("  damaged: {} ({} bad slice(s))", file.filename, bad_slices);
            }
            weaver_par2::FileStatus::Missing => {
                missing += 1;
                println!(
                    "  missing: {} ({} slice(s))",
                    file.filename, file.missing_slice_count
                );
            }
            weaver_par2::FileStatus::Renamed(path) => {
                println!("  renamed: {} -> {}", file.filename, path.display());
            }
        }
    }

    println!(
        "summary: {} complete, {} damaged, {} missing",
        complete, damaged, missing
    );
    println!(
        "missing blocks: {}, recovery blocks available: {}",
        verification.total_missing_blocks, verification.recovery_blocks_available
    );
    match &verification.repairable {
        weaver_par2::Repairability::NotNeeded => println!("repairability: not needed"),
        weaver_par2::Repairability::Repairable {
            blocks_needed,
            blocks_available,
        } => println!(
            "repairability: repairable (need {}, have {})",
            blocks_needed, blocks_available
        ),
        weaver_par2::Repairability::Insufficient {
            blocks_needed,
            blocks_available,
            deficit,
        } => println!(
            "repairability: insufficient (need {}, have {}, deficit {})",
            blocks_needed, blocks_available, deficit
        ),
    }
}

fn format_conflict_filenames(
    placement_plan: &weaver_par2::PlacementPlan,
    par2_set: &weaver_par2::Par2FileSet,
) -> String {
    let names: Vec<String> = placement_plan
        .conflicts
        .iter()
        .filter_map(|file_id| par2_set.file_description(file_id))
        .map(|desc| desc.filename.clone())
        .collect();
    if names.is_empty() {
        format!("{} file id(s)", placement_plan.conflicts.len())
    } else {
        names.join(", ")
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

fn epoch_sec_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn spawn_metrics_history_task(
    exporter: server::PrometheusMetricsExporter,
    db: Database,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let rendered = exporter.render().await;
            let db = db.clone();
            let recorded_at_epoch_sec = epoch_sec_now();
            match tokio::task::spawn_blocking(move || {
                db.record_metrics_scrape(recorded_at_epoch_sec, &rendered)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "failed to persist metrics scrape");
                }
                Err(join_error) => {
                    tracing::warn!(error = %join_error, "metrics scrape persistence task failed");
                }
            }
        }
    })
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
    let mut referenced_nzb_paths: HashSet<PathBuf> = active_jobs
        .values()
        .map(|recovered| recovered.nzb_path.clone())
        .collect();

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
                            let paused_resume_status = recovered
                                .paused_resume_status
                                .as_deref()
                                .map(|status| status_str_to_job_status(status, None));
                            to_restore.push((
                                job_id,
                                spec,
                                recovered.committed_segments,
                                recovered.file_progress,
                                recovered.extracted_members,
                                status,
                                recovered.queued_repair_at_epoch_ms,
                                recovered.queued_extract_at_epoch_ms,
                                paused_resume_status,
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
                if let Some(nzb_path) = row.nzb_path.as_ref() {
                    referenced_nzb_paths.insert(PathBuf::from(nzb_path));
                }
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

    let nzb_dir = data_dir.join(".weaver-nzbs");
    match cleanup_orphaned_persisted_nzbs(&nzb_dir, &referenced_nzb_paths) {
        Ok(removed) if removed > 0 => {
            info!(removed, nzb_dir = %nzb_dir.display(), "removed orphaned persisted nzbs");
        }
        Ok(_) => {}
        Err(error) => {
            warn!(
                error = %error,
                nzb_dir = %nzb_dir.display(),
                "failed to cleanup orphaned persisted nzbs"
            );
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
        let handle_for_events = handle.clone();
        let config_for_events = pipeline_config.clone();
        tokio::spawn(async move {
            if let Err(panic) = tokio::spawn(persist_events(
                event_rx,
                db_for_events,
                handle_for_events,
                config_for_events,
            ))
            .await
            {
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

    let nntp_pool = pipeline.nntp.pool().clone();
    let metrics_exporter = server::PrometheusMetricsExporter::new(handle.clone(), nntp_pool);

    let mut pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    // Restore in-progress jobs from SQLite.
    for (
        job_id,
        spec,
        committed,
        file_progress,
        extracted_members,
        status,
        queued_repair_at_epoch_ms,
        queued_extract_at_epoch_ms,
        paused_resume_status,
        working_dir,
    ) in to_restore
    {
        let committed_count = committed.len();
        match handle
            .restore_job(
                job_id,
                spec,
                committed,
                file_progress,
                extracted_members,
                status,
                queued_repair_at_epoch_ms,
                queued_extract_at_epoch_ms,
                paused_resume_status,
                working_dir,
            )
            .await
        {
            Ok(()) => info!(job_id = job_id.0, committed_count, "job restored"),
            Err(e) => error!(job_id = job_id.0, error = %e, "failed to restore job"),
        }
    }

    let rss_task = rss.start_background_loop();
    let metrics_history_task = spawn_metrics_history_task(metrics_exporter.clone(), db.clone());

    // Run HTTP server.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let server_task = tokio::spawn(server::run_server(
        schema,
        handle.clone(),
        db.clone(),
        backup,
        metrics_exporter,
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
            metrics_history_task.abort();
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = pipeline_exit_error(result);
            server_task.abort();
            rss_task.abort();
            metrics_history_task.abort();
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
            tls_ca_cert: server.tls_ca_cert.clone(),
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
        "queued" => weaver_scheduler::JobStatus::Queued,
        "downloading" => weaver_scheduler::JobStatus::Downloading,
        "checking" => weaver_scheduler::JobStatus::Checking,
        "verifying" => weaver_scheduler::JobStatus::Verifying,
        "queued_repair" => weaver_scheduler::JobStatus::QueuedRepair,
        "repairing" => weaver_scheduler::JobStatus::Repairing,
        "queued_extract" => weaver_scheduler::JobStatus::QueuedExtract,
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
                tls_ca_cert: s.tls_ca_cert.clone(),
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
        soft_timeout: std::time::Duration::from_secs(15),
    };

    NntpClient::new(client_config)
}

fn pipeline_job_id(event: &PipelineEvent) -> Option<u64> {
    match event {
        PipelineEvent::JobCreated { job_id, .. }
        | PipelineEvent::JobPaused { job_id }
        | PipelineEvent::JobResumed { job_id }
        | PipelineEvent::JobCompleted { job_id }
        | PipelineEvent::JobFailed { job_id, .. }
        | PipelineEvent::DownloadStarted { job_id }
        | PipelineEvent::DownloadFinished { job_id }
        | PipelineEvent::Par2MetadataLoaded { job_id }
        | PipelineEvent::JobVerificationStarted { job_id }
        | PipelineEvent::JobVerificationComplete { job_id, .. }
        | PipelineEvent::RepairConfidenceUpdated { job_id, .. }
        | PipelineEvent::RepairStarted { job_id }
        | PipelineEvent::RepairComplete { job_id, .. }
        | PipelineEvent::RepairFailed { job_id, .. }
        | PipelineEvent::ExtractionReady { job_id }
        | PipelineEvent::ExtractionMemberStarted { job_id, .. }
        | PipelineEvent::ExtractionMemberWaitingStarted { job_id, .. }
        | PipelineEvent::ExtractionMemberWaitingFinished { job_id, .. }
        | PipelineEvent::ExtractionMemberAppendStarted { job_id, .. }
        | PipelineEvent::ExtractionMemberAppendFinished { job_id, .. }
        | PipelineEvent::ExtractionProgress { job_id, .. }
        | PipelineEvent::ExtractionMemberFinished { job_id, .. }
        | PipelineEvent::ExtractionMemberFailed { job_id, .. }
        | PipelineEvent::ExtractionComplete { job_id }
        | PipelineEvent::ExtractionFailed { job_id, .. }
        | PipelineEvent::MoveToCompleteStarted { job_id }
        | PipelineEvent::MoveToCompleteFinished { job_id } => Some(job_id.0),
        PipelineEvent::FileClassified { file_id, .. }
        | PipelineEvent::VerificationStarted { file_id }
        | PipelineEvent::VerificationComplete { file_id, .. }
        | PipelineEvent::FileComplete { file_id, .. }
        | PipelineEvent::FileMissing { file_id, .. } => Some(file_id.job_id.0),
        PipelineEvent::SegmentQueued { segment_id, .. }
        | PipelineEvent::ArticleDownloaded { segment_id, .. }
        | PipelineEvent::ArticleNotFound { segment_id }
        | PipelineEvent::SegmentRetryScheduled { segment_id, .. }
        | PipelineEvent::SegmentFailedPermanent { segment_id, .. }
        | PipelineEvent::SegmentDecoded { segment_id, .. }
        | PipelineEvent::SegmentDecodeFailed { segment_id, .. }
        | PipelineEvent::SegmentCommitted { segment_id } => Some(segment_id.file_id.job_id.0),
        PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed => None,
    }
}

/// Background task that subscribes to pipeline events and persists meaningful
/// ones to SQLite for both the legacy job-event log and the public integration
/// event stream.
async fn persist_events(
    mut rx: broadcast::Receiver<PipelineEvent>,
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
) {
    use weaver_api::{
        PersistedQueueEvent, PipelineEventGql, QueueEventKind, QueueItemState, global_queue_state,
        queue_item_from_job,
    };

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
    let mut integration_batch: Vec<weaver_state::IntegrationEventRow> = Vec::new();
    let mut last_states: HashMap<u64, QueueItemState> = HashMap::new();
    let mut last_progress_buckets: HashMap<u64, u8> = HashMap::new();
    let mut last_attention: HashMap<u64, Option<(String, String)>> = HashMap::new();
    let flush_interval = tokio::time::Duration::from_secs(1);

    loop {
        let recv = if batch.is_empty() && integration_batch.is_empty() {
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
                    let integration_events = std::mem::take(&mut integration_batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
                        }
                        if let Err(e) = db.insert_integration_events(&integration_events) {
                            tracing::warn!(error = %e, "failed to persist integration events");
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

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                if matches!(
                    event,
                    PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed
                ) {
                    let cfg = config.read().await;
                    let global_state = global_queue_state(
                        handle.is_globally_paused(),
                        &handle.get_download_block(),
                        cfg.max_download_speed.unwrap_or(0),
                    );
                    let record = PersistedQueueEvent {
                        occurred_at_ms: now,
                        kind: QueueEventKind::GlobalStateChanged,
                        item_id: None,
                        item: None,
                        state: None,
                        previous_state: None,
                        attention: None,
                        global_state: Some(global_state),
                    };
                    if let Ok(payload_json) = serde_json::to_string(&record) {
                        integration_batch.push(weaver_state::IntegrationEventRow {
                            id: 0,
                            timestamp: now,
                            kind: "GLOBAL_STATE_CHANGED".to_string(),
                            item_id: None,
                            payload_json,
                        });
                    }
                }

                if let Some(job_id) = pipeline_job_id(&event)
                    && let Ok(info) = handle.get_job(weaver_core::id::JobId(job_id))
                {
                    let item = queue_item_from_job(&info);
                    let should_evict = matches!(
                        item.state,
                        QueueItemState::Completed | QueueItemState::Failed
                    );
                    let previous_state = last_states.insert(job_id, item.state);
                    let progress_bucket = item.progress_percent.floor() as u8;
                    let previous_progress = last_progress_buckets.insert(job_id, progress_bucket);
                    let attention_signature = item
                        .attention
                        .as_ref()
                        .map(|value| (value.code.clone(), value.message.clone()));
                    let previous_attention =
                        last_attention.insert(job_id, attention_signature.clone());

                    let mut records: Vec<PersistedQueueEvent> = Vec::new();
                    if matches!(event, PipelineEvent::JobCreated { .. }) {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: QueueEventKind::ItemCreated,
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state,
                            attention: item.attention.clone(),
                            global_state: None,
                        });
                    }

                    if let Some(previous_state) = previous_state
                        && previous_state != item.state
                    {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: if item.state == QueueItemState::Completed {
                                QueueEventKind::ItemCompleted
                            } else {
                                QueueEventKind::ItemStateChanged
                            },
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state: Some(previous_state),
                            attention: item.attention.clone(),
                            global_state: None,
                        });
                    }

                    if item.state != QueueItemState::Completed
                        && item.state != QueueItemState::Failed
                        && progress_bucket > 0
                        && previous_progress.is_none_or(|value| progress_bucket > value)
                    {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: QueueEventKind::ItemProgress,
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state: None,
                            attention: None,
                            global_state: None,
                        });
                    }

                    if attention_signature.is_some()
                        && attention_signature != previous_attention.flatten()
                    {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: QueueEventKind::ItemAttention,
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state: None,
                            attention: item.attention.clone(),
                            global_state: None,
                        });
                    }

                    for record in records {
                        if let Ok(payload_json) = serde_json::to_string(&record) {
                            integration_batch.push(weaver_state::IntegrationEventRow {
                                id: 0,
                                timestamp: now,
                                kind: format!("{:?}", record.kind),
                                item_id: record.item_id,
                                payload_json,
                            });
                        }
                    }

                    if should_evict {
                        last_states.remove(&job_id);
                        last_progress_buckets.remove(&job_id);
                        last_attention.remove(&job_id);
                    }
                }

                if batch.len() >= 50 || integration_batch.len() >= 50 {
                    let events = std::mem::take(&mut batch);
                    let integration_events = std::mem::take(&mut integration_batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
                        }
                        if let Err(e) = db.insert_integration_events(&integration_events) {
                            tracing::warn!(error = %e, "failed to persist integration events");
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
    if !integration_batch.is_empty() {
        let _ = db.insert_integration_events(&integration_batch);
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

#[cfg(test)]
mod tests {
    use super::cleanup_orphaned_persisted_nzbs;
    use std::collections::HashSet;
    use std::path::PathBuf;

    #[test]
    fn cleanup_orphaned_persisted_nzbs_removes_only_unreferenced_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let nzb_dir = tempdir.path().join(".weaver-nzbs");
        std::fs::create_dir_all(&nzb_dir).unwrap();

        let kept = nzb_dir.join("kept.nzb");
        let removed = nzb_dir.join("removed.nzb");
        std::fs::write(&kept, b"kept").unwrap();
        std::fs::write(&removed, b"removed").unwrap();

        let referenced = HashSet::from([PathBuf::from(&kept)]);
        let removed_count = cleanup_orphaned_persisted_nzbs(&nzb_dir, &referenced).unwrap();

        assert_eq!(removed_count, 1);
        assert!(kept.exists());
        assert!(!removed.exists());
    }
}
