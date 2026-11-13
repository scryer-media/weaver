// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

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

// Create the initial login only when no credentials are already persisted.
// Environment bootstrap input is deliberately ignored once login is configured.
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
    weaver_server_core::auth::check_password_length(&password)
        .map_err(|error| BootstrapLoginError::new(format!("bootstrap login {error}")))?;
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

pub(crate) const PENDING_DOWNLOAD_ROOT: &str = "bootstrap.pending_download_root";

pub(crate) fn open_database(
    config_path: &Path,
    implicit_download_root: Option<&Path>,
) -> Result<(Database, Option<PathBuf>), Box<dyn std::error::Error>> {
    let db = weaver_server_core::persistence::open_database(config_path)?;
    if db.pre_migration_schema_version().is_none() {
        // Keep initialization eligible after a failed first boot. In particular,
        // environment validation and encryption bootstrap happen after DB creation.
        if let Some(root) = implicit_download_root {
            db.set_setting(PENDING_DOWNLOAD_ROOT, &root.to_string_lossy())?;
        }
        // Record generation before deployment/bootstrap validation can fail.
        // A retry of a new install must not become a legacy no-code wizard.
        db.set_setting(
            weaver_server_core::security::SETTING_INSTALL_GENERATION,
            weaver_server_core::security::AUTHENTICATED_INSTALL_GENERATION,
        )?;
    }
    let pending_root = if implicit_download_root.is_some() {
        db.get_setting(PENDING_DOWNLOAD_ROOT)?.map(PathBuf::from)
    } else {
        None
    };
    Ok((db, pending_root))
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

// Persist platform defaults through the same missing-setting seed as explicit
// environment values. Existing profiles and explicit data roots keep their layout.
pub(crate) fn seed_implicit_download_defaults(
    seed: &mut EnvSeedConfig,
    config: &Config,
    new_profile: bool,
    download_root: Option<&Path>,
    profile_data_dir: &Path,
) {
    // The data root may already have been persisted before an interrupted seed.
    let custom_data_root =
        !config.data_dir.is_empty() && Path::new(&config.data_dir) != profile_data_dir;
    if !new_profile || custom_data_root || seed.core.data_dir.is_some() {
        return;
    }
    let Some(root) = download_root else {
        return;
    };
    seed.core
        .intermediate_dir
        .get_or_insert_with(|| root.join("incomplete").to_string_lossy().into_owned());
    seed.core
        .complete_dir
        .get_or_insert_with(|| root.join("complete").to_string_lossy().into_owned());
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

// Seed the servers from the environment, and return the distance each
// reachable seeded server answered from, which the download runtime wants
// before its lanes pick a first pipelining depth.
pub(crate) async fn apply_server_env_seed(
    db: &Database,
    config: &mut Config,
    seed: &mut EnvSeedConfig,
) -> Result<Vec<(u32, Duration)>, StateError> {
    let mut probe_latencies = Vec::new();
    // A server saved through the server form is asked for its capabilities
    // before it is stored. A seeded server is asked the same question here,
    // unless the environment already stated the answer.
    if weaver_server_core::settings::env_seed::server_seed_applies(config, seed) {
        // A seeded server is asked over its route, as every connection to it
        // is, so the network routes come up for the probe and go down after.
        let proxy_db = db.clone();
        let runtime_handle = tokio::runtime::Handle::current();
        let proxies = tokio::task::spawn_blocking(move || {
            weaver_server_core::proxies::ProxyRuntime::new(proxy_db, runtime_handle)
        })
        .await
        .map_err(|error| StateError::Database(error.to_string()))?
        .map_err(StateError::Database)?;
        let outcomes =
            weaver_server_core::settings::env_seed::probe_seeded_server_pipelining(
                seed,
                |server| {
                    let proxies = proxies.clone();
                    async move {
                        weaver_server_core::servers::probe_unsaved_server(&proxies, &server).await
                    }
                },
            )
            .await;
        proxies.stop_all().await;
        for outcome in outcomes {
            match outcome.failure {
                Some(reason) => warn!(
                    server = outcome.server_id,
                    %reason,
                    "could not ask a seeded server for its capabilities; it stays sequential until it is saved again"
                ),
                None => {
                    info!(
                        server = outcome.server_id,
                        pipelining = outcome.supports_pipelining,
                        "asked a seeded server for its capabilities"
                    );
                    if let Some(latency_ms) = outcome.first_byte_latency_ms {
                        probe_latencies
                            .push((outcome.server_id, Duration::from_millis(latency_ms)));
                    }
                }
            }
        }
    }

    let seeded = weaver_server_core::settings::env_seed::apply_server_seed(db, config, seed)?;
    if seeded > 0 {
        info!(servers = seeded, "seeded NNTP servers from environment");
    }
    Ok(probe_latencies)
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
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            servers: Vec::new(),
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
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
    fn fresh_platform_download_defaults_survive_config_reload() {
        let db = Database::open_in_memory().unwrap();
        let mut config = empty_config();
        let mut seed = EnvSeedConfig::default();
        let home = tempfile::tempdir().unwrap();
        let downloads = home.path().join("Downloads");
        seed_implicit_download_defaults(&mut seed, &config, true, Some(&downloads), home.path());
        apply_core_env_seed(&db, &mut config, &seed).unwrap();
        let reloaded = db.load_config().unwrap();
        assert_eq!(
            PathBuf::from(reloaded.intermediate_dir()),
            downloads.join("incomplete")
        );
        assert_eq!(
            PathBuf::from(reloaded.complete_dir()),
            downloads.join("complete")
        );
        assert!(!downloads.exists());
    }

    #[test]
    fn failed_first_boot_retries_platform_defaults_after_database_reopen() {
        let home = tempfile::tempdir().unwrap();
        let profile = home.path().join("profile");
        let downloads = home.path().join("Downloads");
        let (db, root) = open_database(&profile, Some(&downloads)).unwrap();
        assert_eq!(root.as_deref(), Some(downloads.as_path()));
        assert!(
            weaver_server_core::settings::env_seed::parse_env_seed([(
                "WEAVER_MAX_DOWNLOAD_SPEED".to_string(),
                "invalid".to_string()
            )])
            .is_err()
        );
        drop(db);

        let (db, root) = open_database(&profile, Some(&downloads)).unwrap();
        assert!(db.pre_migration_schema_version().is_some());
        let mut config = db.load_config().unwrap();
        let mut seed = EnvSeedConfig::default();
        seed_implicit_download_defaults(
            &mut seed,
            &config,
            root.is_some(),
            root.as_deref(),
            &profile,
        );
        apply_core_env_seed(&db, &mut config, &seed).unwrap();
        db.delete_setting(PENDING_DOWNLOAD_ROOT).unwrap();
        drop(db);

        let (db, root) = open_database(&profile, Some(&downloads)).unwrap();
        assert_eq!(root, None);
        let config = db.load_config().unwrap();
        assert_eq!(
            PathBuf::from(config.intermediate_dir()),
            downloads.join("incomplete")
        );
        assert_eq!(
            PathBuf::from(config.complete_dir()),
            downloads.join("complete")
        );
    }

    #[test]
    fn interrupted_core_seed_resumes_missing_paths_without_overwriting_saved_paths() {
        let home = tempfile::tempdir().unwrap();
        let profile = home.path().join("profile");
        let downloads = home.path().join("Downloads");
        let (db, _) = open_database(&profile, Some(&downloads)).unwrap();
        db.set_setting("data_dir", &profile.to_string_lossy())
            .unwrap();
        db.set_setting("intermediate_dir", "chosen-incomplete")
            .unwrap();
        drop(db);

        let (db, root) = open_database(&profile, Some(&downloads)).unwrap();
        let mut config = db.load_config().unwrap();
        let mut seed = EnvSeedConfig::default();
        seed_implicit_download_defaults(
            &mut seed,
            &config,
            root.is_some(),
            root.as_deref(),
            &profile,
        );
        apply_core_env_seed(&db, &mut config, &seed).unwrap();
        let reloaded = db.load_config().unwrap();
        assert_eq!(reloaded.intermediate_dir(), "chosen-incomplete");
        assert_eq!(
            PathBuf::from(reloaded.complete_dir()),
            downloads.join("complete")
        );
    }

    #[test]
    fn existing_database_without_pending_initialization_keeps_its_layout() {
        let profile = tempfile::tempdir().unwrap();
        let (db, root) = open_database(profile.path(), None).unwrap();
        assert_eq!(root, None);
        drop(db);
        let (db, root) = open_database(profile.path(), Some(Path::new("Downloads"))).unwrap();
        assert_eq!(root, None);
        assert_eq!(db.get_setting(PENDING_DOWNLOAD_ROOT).unwrap(), None);
    }

    #[test]
    fn platform_defaults_preserve_existing_profiles_and_explicit_data_roots() {
        for (new_profile, configured_data, env_data, root) in [
            (false, "", None, Some(Path::new("Downloads"))),
            (true, "existing-data", None, Some(Path::new("Downloads"))),
            (true, "", Some("custom-data"), Some(Path::new("Downloads"))),
            (true, "", None, None),
        ] {
            let mut config = empty_config();
            config.data_dir = configured_data.into();
            let mut seed = EnvSeedConfig::default();
            seed.core.data_dir = env_data.map(str::to_owned);
            seed_implicit_download_defaults(
                &mut seed,
                &config,
                new_profile,
                root,
                Path::new("profile"),
            );
            assert_eq!(seed.core.intermediate_dir, None);
            assert_eq!(seed.core.complete_dir, None);
        }
    }

    #[test]
    fn platform_defaults_preserve_environment_and_saved_download_paths() {
        let db = Database::open_in_memory().unwrap();
        let mut config = empty_config();
        config.complete_dir = Some("saved-complete".into());
        db.set_setting("complete_dir", "saved-complete").unwrap();
        let mut seed = EnvSeedConfig::default();
        seed.core.intermediate_dir = Some("env-incomplete".into());
        seed.core.complete_dir = Some("env-complete".into());
        seed_implicit_download_defaults(
            &mut seed,
            &config,
            true,
            Some(Path::new("Downloads")),
            Path::new("profile"),
        );
        assert_eq!(seed.core.complete_dir.as_deref(), Some("env-complete"));
        apply_core_env_seed(&db, &mut config, &seed).unwrap();
        let reloaded = db.load_config().unwrap();
        assert_eq!(reloaded.intermediate_dir(), "env-incomplete");
        assert_eq!(reloaded.complete_dir(), "saved-complete");
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

        unsafe {
            std::env::remove_var(ENV_BOOTSTRAP_LOGIN_PASSWORD_FILE);
            std::env::set_var(ENV_BOOTSTRAP_LOGIN_PASSWORD, "short");
        }
        let error = bootstrap_login_if_needed(&db).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "bootstrap login password must be at least 8 characters"
        );
        assert!(db.get_auth_credentials().unwrap().is_none());
    }
}
