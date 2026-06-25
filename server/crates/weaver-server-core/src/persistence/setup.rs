use std::path::{Path, PathBuf};

use tracing::{error, info};

use crate::Database;
use crate::persistence::database_target::DatabaseTarget;
use crate::settings::Config;

fn is_explicit_config_file(config_path: &Path) -> bool {
    config_path
        .extension()
        .is_some_and(|extension| extension == "toml")
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
    let (_db_path, toml_path) = resolve_database_paths(config_path);
    let target = DatabaseTarget::resolve(config_path)?;

    if let DatabaseTarget::SqlitePath(path) = &target
        && let Some(parent) = path.parent()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut db = Database::open_target(target)?;

    if let Some(ref toml) = toml_path {
        bootstrap_encryption_for_toml_import(config_path, toml, &mut db)?;
        if db.migrate_from_toml(toml)? {
            info!(toml = %toml.display(), "migrated config from TOML to database");
        }
    }

    let config = db.load_config()?;
    Ok((db, config))
}

fn bootstrap_encryption_for_toml_import(
    config_path: &Path,
    toml_path: &Path,
    db: &mut Database,
) -> Result<(), Box<dyn std::error::Error>> {
    if !toml_path.exists() || !db.is_empty()? {
        return Ok(());
    }

    let contents = std::fs::read_to_string(toml_path)?;
    let config: Config = toml::from_str(&contents)
        .map_err(|e| format!("failed to parse TOML before encryption bootstrap: {e}"))?;
    let data_dir = if config.data_dir.is_empty() {
        default_data_dir_for_config_path(config_path)
    } else {
        PathBuf::from(config.data_dir)
    };

    let key = crate::persistence::encryption::ensure_encryption_key(Some(data_dir))?;
    db.set_encryption_key(key);
    Ok(())
}

pub fn default_data_dir_for_config_path(config_path: &Path) -> PathBuf {
    if is_explicit_config_file(config_path) {
        config_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    } else {
        config_path.to_path_buf()
    }
}

pub fn bootstrap_encryption(
    data_dir: &Path,
    db: &mut Database,
    config: &mut Config,
) -> Result<(), String> {
    let key = crate::persistence::encryption::ensure_encryption_key(Some(data_dir.to_path_buf()))?;
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn write_test_config(path: &Path, data_dir: &Path) {
        let mut file = std::fs::File::create(path).unwrap();
        write!(
            file,
            r#"
data_dir = "{}"

[[servers]]
id = 1
host = "news.example.com"
port = 443
tls = true
username = "user"
password = "pass"
connections = 10
active = true
"#,
            data_dir.display()
        )
        .unwrap();
    }

    #[test]
    fn resolve_database_paths_treats_migrated_toml_as_file() {
        let dir = tempfile::tempdir().unwrap();
        let migrated_path = dir.path().join("weaver.toml.migrated");
        write_test_config(&migrated_path, &dir.path().join("data"));

        let (db_path, toml_path) = resolve_database_paths(&migrated_path);

        assert_eq!(db_path, dir.path().join("weaver.db"));
        assert_eq!(toml_path, Some(migrated_path));
    }

    #[test]
    fn relative_toml_default_data_dir_uses_current_directory() {
        assert_eq!(
            default_data_dir_for_config_path(Path::new("weaver.toml")),
            PathBuf::from(".")
        );
    }

    #[test]
    fn migrated_toml_default_data_dir_uses_parent_directory() {
        assert_eq!(
            default_data_dir_for_config_path(Path::new("/tmp/weaver.toml.migrated")),
            PathBuf::from("/tmp")
        );
    }

    #[test]
    fn existing_extensionless_config_file_default_data_dir_uses_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("weaver-config");
        std::fs::write(&config_path, "").unwrap();

        assert_eq!(
            default_data_dir_for_config_path(&config_path),
            dir.path().to_path_buf()
        );
    }

    #[test]
    fn open_db_and_config_imports_migrated_toml_without_renaming_again() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let migrated_path = dir.path().join("weaver.toml.migrated");
        write_test_config(&migrated_path, &data_dir);

        let (_db, config) = open_db_and_config(&migrated_path).unwrap();

        assert_eq!(config.data_dir, data_dir.display().to_string());
        assert_eq!(config.servers.len(), 1);
        assert_eq!(config.servers[0].password, Some("pass".to_string()));
        assert!(!data_dir.join("encryption.key").exists());
        assert!(migrated_path.exists());
        assert!(!dir.path().join("weaver.toml.toml.migrated").exists());
    }

    #[test]
    fn open_db_and_config_fails_when_toml_import_fails() {
        let dir = tempfile::tempdir().unwrap();
        let toml_path = dir.path().join("weaver.toml");
        std::fs::write(&toml_path, "data_dir = [not valid toml]").unwrap();

        let error = match open_db_and_config(&toml_path) {
            Ok(_) => panic!("invalid TOML import unexpectedly succeeded"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("failed to parse TOML"));
        assert!(toml_path.exists());
    }
}
