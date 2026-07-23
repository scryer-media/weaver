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
const DOTENV_FILE: &str = ".env";

// musl's bundled allocator serializes multi-threaded allocation heavily
// (measured −18% CPU on the container download benchmark when replaced), and
// the Windows system heap has the same reputation under threaded load, so
// both build targets swap in mimalloc. glibc/macOS builds keep the system
// allocator.
#[cfg(any(target_env = "musl", target_os = "windows", target_os = "macos"))]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    if let Some(code) =
        weaver_server_core::post_processing::runner::maybe_run_supervisor_from_process_args()
    {
        std::process::exit(code);
    }
    if let Err(error) = load_cwd_dotenv() {
        eprintln!("failed to load {DOTENV_FILE}: {error}");
        std::process::exit(1);
    }

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

    if let Err(error) = weaver_server_core::e2e_clock::validate() {
        error!("invalid Weaver e2e clock: {error}");
        std::process::exit(1);
    }

    let restore_locator_dir =
        weaver_server_core::persistence::setup::default_data_dir_for_config_path(&config_path);
    let db = match bootstrap::open_database(&config_path) {
        Ok(db) => db,
        Err(error) => {
            error!("failed to open database: {error}");
            std::process::exit(1);
        }
    };
    let (db, restore_outcome) =
        match weaver_server_core::operations::apply_pending_restore(db, &restore_locator_dir) {
            Ok(result) => result,
            Err(error) => {
                error!("failed to apply staged restore: {error}");
                std::process::exit(1);
            }
        };
    let (mut db, mut config) = if let Some(outcome) = restore_outcome {
        tracing::info!(
            restore_id = %outcome.restore_id,
            managed_packages = outcome.managed_packages_restored,
            "applied staged backup restore"
        );
        let config = match db.load_config() {
            Ok(config) => config,
            Err(error) => {
                error!("failed to load restored config: {error}");
                std::process::exit(1);
            }
        };
        (db, config)
    } else {
        match bootstrap::finish_open_db_and_config(&config_path, db) {
            Ok(result) => result,
            Err(error) => {
                error!("failed to load config: {error}");
                std::process::exit(1);
            }
        }
    };
    let env_seed = match bootstrap::parse_env_seed_from_process() {
        Ok(seed) => seed,
        Err(error) => {
            error!("invalid environment config: {error}");
            std::process::exit(1);
        }
    };

    bootstrap::default_data_dir_from_config_path(&config_path, &mut config);
    let data_dir = PathBuf::from(&config.data_dir);
    if let Err(error) = bootstrap::bootstrap_encryption(&data_dir, &mut db, &mut config) {
        error!("failed to bootstrap encryption key: {error}");
        std::process::exit(1);
    }
    bootstrap::reset_login_if_requested(&mut db);
    if let Err(error) = bootstrap::apply_core_env_seed(&db, &mut config, &env_seed) {
        error!("failed to seed config settings from environment: {error}");
        std::process::exit(1);
    }
    if let Err(error) = bootstrap::apply_server_env_seed(&db, &mut config, &env_seed) {
        error!("failed to seed servers from environment: {error}");
        std::process::exit(1);
    }

    if let Err(errors) = bootstrap::validate_config(&config) {
        for message in &errors {
            error!("config: {message}");
        }
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
        Command::Download { nzb, output, force } => {
            if let Err(error) = commands::download::run(
                &mut config,
                &db,
                &nzb,
                output.as_deref(),
                force,
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
            if let Err(error) = commands::serve::run(
                config,
                db,
                restore_locator_dir,
                port,
                &base_url,
                log_ring_buffer.clone(),
            )
            .await
            {
                error!("server failed: {error}");
                std::process::exit(1);
            }
        }
        Command::Par2 { .. } => unreachable!("par2 command handled before config startup"),
    }
}

fn load_cwd_dotenv() -> Result<bool, dotenvy::Error> {
    load_dotenv_path(Path::new(DOTENV_FILE))
}

fn load_dotenv_path(path: &Path) -> Result<bool, dotenvy::Error> {
    match dotenvy::from_path(path) {
        Ok(_) => Ok(true),
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
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

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex};

    use super::*;

    static DOTENV_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::remove_var(key) };
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    struct CurrentDirGuard {
        previous: PathBuf,
    }

    impl CurrentDirGuard {
        fn enter(path: &std::path::Path) -> Self {
            let previous = std::env::current_dir().unwrap();
            std::env::set_current_dir(path).unwrap();
            Self { previous }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.previous).unwrap();
        }
    }

    #[test]
    fn missing_dotenv_is_ignored() {
        let _lock = DOTENV_TEST_LOCK.lock().unwrap();
        let tempdir = tempfile::tempdir().unwrap();

        let loaded = load_dotenv_path(&tempdir.path().join(".env")).unwrap();

        assert!(!loaded);
    }

    #[test]
    fn cwd_dotenv_is_loaded() {
        let _lock = DOTENV_TEST_LOCK.lock().unwrap();
        let key = "WEAVER_DOTENV_TEST_CWD";
        let _env_guard = EnvVarGuard::remove(key);
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join(".env"), format!("{key}=loaded\n")).unwrap();
        let _cwd_guard = CurrentDirGuard::enter(tempdir.path());

        let loaded = load_cwd_dotenv().unwrap();

        assert!(loaded);
        assert_eq!(std::env::var(key).unwrap(), "loaded");
    }

    #[test]
    fn dotenv_parse_errors_are_fatal() {
        let _lock = DOTENV_TEST_LOCK.lock().unwrap();
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join(".env");
        std::fs::write(&path, "BROKEN=\"unterminated\n").unwrap();

        let error = load_dotenv_path(&path).unwrap_err();

        assert!(!error.to_string().is_empty());
    }

    #[test]
    fn process_env_wins_over_dotenv() {
        let _lock = DOTENV_TEST_LOCK.lock().unwrap();
        let key = "WEAVER_DOTENV_TEST_PRECEDENCE";
        let _env_guard = EnvVarGuard::set(key, "process");
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join(".env");
        std::fs::write(&path, format!("{key}=dotenv\n")).unwrap();

        let loaded = load_dotenv_path(&path).unwrap();

        assert!(loaded);
        assert_eq!(std::env::var(key).unwrap(), "process");
    }
}
