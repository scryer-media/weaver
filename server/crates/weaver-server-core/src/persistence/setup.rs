use std::path::{Path, PathBuf};

use tracing::{error, info};

use crate::Database;
use crate::settings::Config;

fn is_explicit_config_file(config_path: &Path) -> bool {
    config_path.extension().is_some_and(|extension| extension == "toml")
        || config_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".toml.migrated"))
        || config_path.is_file()
}

pub fn resolve_database_paths(config_path: &Path) -> (PathBuf, Option<PathBuf>) {
    if is_explicit_config_file(config_path) {
        let dir = config_path.parent().unwrap_or(Path::new("."));
        (dir.join("weaver.db"), Some(config_path.to_path_buf()))
    } else {
        let dir = config_path;
        let toml_candidate = dir.join("weaver.toml");
        let toml_path = toml_candidate.exists().then_some(toml_candidate);
        (dir.join("weaver.db"), toml_path)
    }
}

pub fn open_db_and_config(
    config_path: &Path,
) -> Result<(Database, Config), Box<dyn std::error::Error>> {
    let (db_path, toml_path) = resolve_database_paths(config_path);

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = Database::open(&db_path)?;

    if let Some(ref toml) = toml_path {
        match db.migrate_from_toml(toml) {
            Ok(true) => info!(toml = %toml.display(), "migrated config from TOML to SQLite"),
            Ok(false) => {}
            Err(e) => error!(error = %e, "TOML migration failed"),
        }
    }

    let config = db.load_config()?;
    Ok((db, config))
}

pub fn bootstrap_encryption(data_dir: &Path, db: &mut Database, config: &mut Config) {
    match crate::persistence::encryption::ensure_encryption_key(Some(data_dir.to_path_buf())) {
        Ok(key) => {
            db.set_encryption_key(key);
            if let Err(e) = db.migrate_plaintext_credentials() {
                error!("failed to encrypt existing passwords: {e}");
            }

            let saved_data_dir = config.data_dir.clone();
            match db.load_config() {
                Ok(mut reloaded) => {
                    if reloaded.data_dir.is_empty() {
                        reloaded.data_dir = saved_data_dir;
                    }
                    *config = reloaded;
                }
                Err(e) => error!("failed to reload config after setting encryption key: {e}"),
            }
        }
        Err(e) => error!("failed to bootstrap encryption key: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_test_config(path: &Path) {
        let mut file = std::fs::File::create(path).unwrap();
        write!(
            file,
            r#"
data_dir = "/tmp/weaver-data"

[[servers]]
id = 1
host = "news.example.com"
port = 443
tls = true
username = "user"
password = "pass"
connections = 10
active = true
"#
        )
        .unwrap();
    }

    #[test]
    fn resolve_database_paths_treats_migrated_toml_as_file() {
        let dir = tempfile::tempdir().unwrap();
        let migrated_path = dir.path().join("weaver.toml.migrated");
        write_test_config(&migrated_path);

        let (db_path, toml_path) = resolve_database_paths(&migrated_path);

        assert_eq!(db_path, dir.path().join("weaver.db"));
        assert_eq!(toml_path, Some(migrated_path));
    }

    #[test]
    fn open_db_and_config_imports_migrated_toml_without_renaming_again() {
        let dir = tempfile::tempdir().unwrap();
        let migrated_path = dir.path().join("weaver.toml.migrated");
        write_test_config(&migrated_path);

        let (_db, config) = open_db_and_config(&migrated_path).unwrap();

        assert_eq!(config.data_dir, "/tmp/weaver-data");
        assert!(migrated_path.exists());
        assert!(!dir.path().join("weaver.toml.toml.migrated").exists());
    }
}
