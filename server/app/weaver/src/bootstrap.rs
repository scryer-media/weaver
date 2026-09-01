use std::fmt;
use std::path::{Path, PathBuf};

use tracing::{error, info, warn};

use weaver_server_core::persistence::setup::default_data_dir_for_config_path;
use weaver_server_core::settings::Config;
use weaver_server_core::settings::env_seed::{EnvSeedConfig, EnvSeedError};
use weaver_server_core::{Database, StateError};

pub(crate) const ENV_BOOTSTRAP_LOGIN_USERNAME: &str = "WEAVER_BOOTSTRAP_LOGIN_USERNAME";
pub(crate) const ENV_BOOTSTRAP_LOGIN_PASSWORD: &str = "WEAVER_BOOTSTRAP_LOGIN_PASSWORD";
pub(crate) const ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE: &str = "WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE";

enum BootstrapPasswordSource {
    Environment(String),
    File(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BootstrapLoginOutcome {
    ExistingCredentialsRetained,
    CredentialsCreated,
    NoBootstrapRequested,
}

#[derive(Debug, Clone)]
pub(crate) struct BootstrapLoginError {
    message: String,
}

impl BootstrapLoginError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for BootstrapLoginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BootstrapLoginError {}

/// Create the initial login only when no credentials are already persisted.
/// Environment bootstrap input is deliberately ignored once login is configured.
pub(crate) async fn bootstrap_login_if_needed(
    db: &Database,
) -> Result<BootstrapLoginOutcome, BootstrapLoginError> {
    if db
        .get_auth_credentials()
        .map_err(|error| {
            BootstrapLoginError::new(format!("failed to inspect login credentials: {error}"))
        })?
        .is_some()
    {
        return Ok(BootstrapLoginOutcome::ExistingCredentialsRetained);
    }

    let username = optional_unicode_env(ENV_BOOTSTRAP_LOGIN_USERNAME)?;
    let password = optional_unicode_env(ENV_BOOTSTRAP_LOGIN_PASSWORD)?;
    let password_file = std::env::var_os(ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE).map(PathBuf::from);

    let Some(username) = username else {
        return match (password.is_some(), password_file.is_some()) {
            (false, false) => Ok(BootstrapLoginOutcome::NoBootstrapRequested),
            (true, true) => Err(BootstrapLoginError::new(format!(
                "{ENV_BOOTSTRAP_LOGIN_USERNAME} must be set; {ENV_BOOTSTRAP_LOGIN_PASSWORD} and {ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} are ambiguous"
            ))),
            _ => Err(BootstrapLoginError::new(format!(
                "{ENV_BOOTSTRAP_LOGIN_USERNAME} must be set when configuring a bootstrap login password"
            ))),
        };
    };
    let username = username.trim().to_string();
    if username.is_empty() {
        return Err(BootstrapLoginError::new(format!(
            "{ENV_BOOTSTRAP_LOGIN_USERNAME} must not be empty"
        )));
    }

    let source = match (password, password_file) {
        (Some(_), Some(_)) => {
            return Err(BootstrapLoginError::new(format!(
                "{ENV_BOOTSTRAP_LOGIN_PASSWORD} and {ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} cannot both be set"
            )));
        }
        (Some(password), None) if password.is_empty() => {
            return Err(BootstrapLoginError::new(format!(
                "{ENV_BOOTSTRAP_LOGIN_PASSWORD} must not be empty"
            )));
        }
        (Some(password), None) => BootstrapPasswordSource::Environment(password),
        (None, Some(path)) => BootstrapPasswordSource::File(path),
        (None, None) => {
            return Err(BootstrapLoginError::new(format!(
                "{ENV_BOOTSTRAP_LOGIN_USERNAME} requires exactly one of {ENV_BOOTSTRAP_LOGIN_PASSWORD} or {ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE}"
            )));
        }
    };

    let password = match source {
        BootstrapPasswordSource::Environment(password) => password,
        BootstrapPasswordSource::File(path) => read_bootstrap_password_file(&path)?,
    };
    let password_hash =
        tokio::task::spawn_blocking(move || weaver_server_core::auth::hash_password(&password))
            .await
            .map_err(|error| {
                BootstrapLoginError::new(format!("bootstrap password hashing task failed: {error}"))
            })?
            .map_err(|error| {
                BootstrapLoginError::new(format!(
                    "failed to hash bootstrap login password: {error}"
                ))
            })?;

    db.set_auth_credentials(&username, &password_hash)
        .map_err(|error| {
            BootstrapLoginError::new(format!(
                "failed to persist bootstrap login credentials: {error}"
            ))
        })?;
    info!(username = %username, "created bootstrap login credentials");
    Ok(BootstrapLoginOutcome::CredentialsCreated)
}

fn optional_unicode_env(name: &'static str) -> Result<Option<String>, BootstrapLoginError> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| BootstrapLoginError::new(format!("{name} must contain valid UTF-8")))
        })
        .transpose()
}

fn read_bootstrap_password_file(path: &Path) -> Result<String, BootstrapLoginError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        BootstrapLoginError::new(format!(
            "{ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} cannot read {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(BootstrapLoginError::new(format!(
            "{ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} must name a regular file ({})",
            path.display()
        )));
    }
    let bytes = std::fs::read(path).map_err(|error| {
        BootstrapLoginError::new(format!(
            "{ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} cannot read {}: {error}",
            path.display()
        ))
    })?;
    let password = String::from_utf8(bytes).map_err(|_| {
        BootstrapLoginError::new(format!(
            "{ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} must contain UTF-8 ({})",
            path.display()
        ))
    })?;
    let password = password.trim_end_matches(['\r', '\n']).to_string();
    if password.is_empty() {
        return Err(BootstrapLoginError::new(format!(
            "{ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE} must not contain an empty password ({})",
            path.display()
        )));
    }
    Ok(password)
}

pub(crate) fn open_database(config_path: &Path) -> Result<Database, Box<dyn std::error::Error>> {
    weaver_server_core::persistence::open_database(config_path)
}

pub(crate) fn finish_open_db_and_config(
    config_path: &Path,
    db: Database,
) -> Result<(Database, Config), Box<dyn std::error::Error>> {
    weaver_server_core::persistence::finish_open_db_and_config(config_path, db)
}

pub(crate) fn reset_login_if_requested(db: &mut Database) {
    // Clear the stored login before serve-time bootstrap decides whether to recreate it.
    if std::env::var("WEAVER_RESET_LOGIN").is_ok_and(|v| v == "1" || v == "true") {
        match db.clear_auth_credentials() {
            Ok(()) => warn!("WEAVER_RESET_LOGIN set - stored login credentials were cleared"),
            Err(e) => error!("failed to reset login credentials: {e}"),
        }
    }
}

pub(crate) fn parse_env_seed_from_process() -> Result<EnvSeedConfig, EnvSeedError> {
    weaver_server_core::settings::env_seed::parse_env_seed(std::env::vars())
}

pub(crate) fn apply_core_env_seed(
    db: &Database,
    config: &mut Config,
    seed: &EnvSeedConfig,
) -> Result<usize, StateError> {
    let seeded = weaver_server_core::settings::env_seed::apply_core_seed(db, config, seed)?;
    if seeded > 0 {
        info!(
            settings = seeded,
            "seeded missing config settings from environment"
        );
    }
    Ok(seeded)
}

pub(crate) fn apply_server_env_seed(
    db: &Database,
    config: &mut Config,
    seed: &EnvSeedConfig,
) -> Result<usize, StateError> {
    let seeded = weaver_server_core::settings::env_seed::apply_server_seed(db, config, seed)?;
    if seeded > 0 {
        info!(servers = seeded, "seeded NNTP servers from environment");
    }
    Ok(seeded)
}

pub(crate) fn default_data_dir_from_config_path(config_path: &Path, config: &mut Config) {
    if config.data_dir.is_empty() {
        let dir = default_data_dir_for_config_path(config_path);
        let dir = dir.to_string_lossy().to_string();
        info!(data_dir = %dir, "defaulting data_dir from config path");
        config.data_dir = dir;
    }
}

pub(crate) fn validate_config(config: &Config) -> Result<(), Vec<String>> {
    config.validate()
}

pub(crate) fn ensure_runtime_directories(
    directories: &[(&str, &Path)],
) -> Result<(), Box<dyn std::error::Error>> {
    for (label, dir) in directories {
        std::fs::create_dir_all(dir).map_err(|error| {
            std::io::Error::other(format!(
                "cannot create {label} directory ({}): {error}",
                dir.display()
            ))
        })?;

        // Verify we can actually write into the directory.
        let probe = dir.join(".weaver-write-probe");
        std::fs::File::create(&probe)
            .map(|_| {
                let _ = std::fs::remove_file(&probe);
            })
            .map_err(|error| {
                std::io::Error::other(format!(
                    "{label} is not writable ({}): {error}",
                    dir.display()
                ))
            })?;
    }

    Ok(())
}

pub(crate) fn bootstrap_encryption(
    data_dir: &Path,
    db: &mut Database,
    config: &mut Config,
) -> Result<(), String> {
    weaver_server_core::persistence::bootstrap_encryption(data_dir, db, config)
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // serializes process-environment mutation in these tests
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};

    use super::*;

    fn bootstrap_env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    struct BootstrapEnvGuard {
        previous: Vec<(&'static str, Option<OsString>)>,
    }

    impl BootstrapEnvGuard {
        fn clear() -> Self {
            let names = [
                ENV_BOOTSTRAP_LOGIN_USERNAME,
                ENV_BOOTSTRAP_LOGIN_PASSWORD,
                ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE,
            ];
            let previous = names
                .into_iter()
                .map(|name| (name, std::env::var_os(name)))
                .collect();
            for name in names {
                unsafe { std::env::remove_var(name) };
            }
            Self { previous }
        }
    }

    impl Drop for BootstrapEnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                if let Some(value) = value {
                    unsafe { std::env::set_var(name, value) };
                } else {
                    unsafe { std::env::remove_var(name) };
                }
            }
        }
    }

    fn empty_config() -> Config {
        Config {
            data_dir: String::new(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: Vec::new(),
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        }
    }

    #[test]
    fn default_toml_config_path_uses_parent_as_data_dir() {
        let mut config = empty_config();
        let config_path = PathBuf::from("weaver.toml");

        default_data_dir_from_config_path(&config_path, &mut config);

        assert_eq!(config.data_dir, ".");
    }

    #[test]
    fn directory_config_path_uses_directory_as_data_dir() {
        let mut config = empty_config();
        let config_path = PathBuf::from("/tmp/weaver-config");

        default_data_dir_from_config_path(&config_path, &mut config);

        assert_eq!(config.data_dir, "/tmp/weaver-config");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_login_creates_direct_password_without_trimming_it() {
        let _lock = bootstrap_env_lock();
        let _env = BootstrapEnvGuard::clear();
        unsafe {
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_USERNAME, "  admin  ");
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD, " password with spaces ");
        }
        let db = Database::open_in_memory().unwrap();

        assert_eq!(
            bootstrap_login_if_needed(&db).await.unwrap(),
            BootstrapLoginOutcome::CredentialsCreated
        );
        let credentials = db.get_auth_credentials().unwrap().unwrap();
        assert_eq!(credentials.username, "admin");
        assert!(weaver_server_core::auth::verify_password(
            " password with spaces ",
            &credentials.password_hash
        ));
        assert!(!credentials.password_hash.contains("password with spaces"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_login_reads_file_and_removes_only_final_line_endings() {
        let _lock = bootstrap_env_lock();
        let _env = BootstrapEnvGuard::clear();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("password");
        std::fs::write(&path, " leading password \r\n\n").unwrap();
        unsafe {
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_USERNAME, "admin");
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE, &path);
        }
        let db = Database::open_in_memory().unwrap();

        assert_eq!(
            bootstrap_login_if_needed(&db).await.unwrap(),
            BootstrapLoginOutcome::CredentialsCreated
        );
        let credentials = db.get_auth_credentials().unwrap().unwrap();
        assert!(weaver_server_core::auth::verify_password(
            " leading password ",
            &credentials.password_hash
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn existing_login_ignores_bootstrap_inputs() {
        let _lock = bootstrap_env_lock();
        let _env = BootstrapEnvGuard::clear();
        let db = Database::open_in_memory().unwrap();
        db.set_auth_credentials("existing", "existing-hash")
            .unwrap();
        unsafe {
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_USERNAME, "new");
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD, "password");
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE, "/does/not/exist");
        }

        assert_eq!(
            bootstrap_login_if_needed(&db).await.unwrap(),
            BootstrapLoginOutcome::ExistingCredentialsRetained
        );
        let credentials = db.get_auth_credentials().unwrap().unwrap();
        assert_eq!(credentials.username, "existing");
        assert_eq!(credentials.password_hash, "existing-hash");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_login_rejects_incomplete_or_ambiguous_input() {
        let _lock = bootstrap_env_lock();
        let _env = BootstrapEnvGuard::clear();
        let db = Database::open_in_memory().unwrap();
        assert_eq!(
            bootstrap_login_if_needed(&db).await.unwrap(),
            BootstrapLoginOutcome::NoBootstrapRequested
        );

        unsafe { std::env::set_var(ENV_BOOTSTRAP_LOGIN_USERNAME, "admin") };
        assert!(bootstrap_login_if_needed(&db).await.is_err());
        unsafe {
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD, "password");
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE, "/does/not/exist");
        }
        assert!(bootstrap_login_if_needed(&db).await.is_err());
    }
}
