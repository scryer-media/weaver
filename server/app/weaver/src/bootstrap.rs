use std::path::Path;

use tracing::{error, info, warn};

use weaver_server_core::Database;
use weaver_server_core::settings::Config;

pub(crate) fn open_db_and_config(
    config_path: &Path,
) -> Result<(Database, Config), Box<dyn std::error::Error>> {
    weaver_server_core::persistence::open_db_and_config(config_path)
}

pub(crate) fn reset_login_if_requested(db: &mut Database) {
    // Escape hatch: WEAVER_RESET_LOGIN=1 disables login protection on startup.
    if std::env::var("WEAVER_RESET_LOGIN").is_ok_and(|v| v == "1" || v == "true") {
        match db.clear_auth_credentials() {
            Ok(()) => warn!("WEAVER_RESET_LOGIN set - login protection has been disabled"),
            Err(e) => error!("failed to reset login credentials: {e}"),
        }
    }
}

pub(crate) fn default_data_dir_from_config_path(config_path: &Path, config: &mut Config) {
    // When --config points to a directory and data_dir is unset (fresh DB),
    // default data_dir to that directory. This is the common Docker pattern:
    //   weaver --config /config serve
    if config.data_dir.is_empty() && config_path.extension().is_none_or(|e| e != "toml") {
        let dir = config_path.to_string_lossy().to_string();
        info!(data_dir = %dir, "defaulting data_dir to --config directory");
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

pub(crate) fn bootstrap_encryption(data_dir: &Path, db: &mut Database, config: &mut Config) {
    weaver_server_core::persistence::bootstrap_encryption(data_dir, db, config);
}
