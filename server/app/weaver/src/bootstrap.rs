use std::path::Path;

use tracing::{error, info, warn};

use weaver_server_core::Database;
use weaver_server_core::persistence::setup::default_data_dir_for_config_path;
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
mod tests {
    use std::path::PathBuf;

    use super::*;

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
            diagnostic_upload_url: None,
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
}
