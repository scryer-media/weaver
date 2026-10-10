use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sqlx::Connection;

use super::automatic::{AUTO_SETTINGS_KEY, decode_auto_settings};
use super::manifest::{BackupInstanceSecrets, BackupServiceError, build_bundle_manifest, io_err};
use super::stored::{
    BackupTrigger, STORAGE_KEY, complete_backup, new_backup_info, prune_retained_backups,
    validate_storage_dir, write_metadata,
};
use crate::persistence::database_target::DatabaseTarget;

// Settings key holding the version that last started against this database.
// It is written as soon as the database opens, before first-run configuration
// is imported, so it does not make the database hold settings of its own.
pub(crate) const LAST_VERSION: &str = "last_started_version";
const PENDING_VERSION: &str = "pending_auto_backup_version";

// Record the version after the database has opened and restore recovery has completed.
// A failed pre-upgrade backup remains pending, but a later retry must describe the
// migrated database rather than claiming to be the original rollback copy.
pub fn record_started_version(db: &crate::Database) -> Result<(), BackupServiceError> {
    record_started_version_for(db, env!("CARGO_PKG_VERSION"))
}

fn record_started_version_for(
    db: &crate::Database,
    version: &str,
) -> Result<(), BackupServiceError> {
    db.set_setting(LAST_VERSION, version).map_err(io_err)
}

// Explicit operator recovery from an unavailable pre-upgrade backup target.
// This deliberately forfeits rollback protection for this version transition.
pub async fn skip_upgrade_backup(config_path: &Path) -> Result<(), BackupServiceError> {
    let restore_locator = crate::persistence::setup::default_data_dir_for_config_path(config_path);
    if super::pending::pending_restore_status(&restore_locator).is_some() {
        return Ok(());
    }
    let target = DatabaseTarget::resolve(config_path).map_err(io_err)?;
    skip_upgrade_backup_for_target(&target, env!("CARGO_PKG_VERSION")).await
}

async fn skip_upgrade_backup_for_target(
    target: &DatabaseTarget,
    current_version: &str,
) -> Result<(), BackupServiceError> {
    if read_settings(target).await?.is_some() {
        write_marker(target, PENDING_VERSION, "").await?;
        write_marker(target, LAST_VERSION, current_version).await?;
    }
    tracing::error!(
        "pre-migration automatic backup explicitly skipped; rollback backup was not created; remove the skip option from persistent startup configuration after recovery"
    );
    Ok(())
}

// Explicitly discard only automatic-backup configuration for operator recovery.
pub async fn reset_automatic_backup_settings(config_path: &Path) -> Result<(), BackupServiceError> {
    let restore_locator = crate::persistence::setup::default_data_dir_for_config_path(config_path);
    if super::pending::pending_restore_status(&restore_locator).is_some() {
        return Err(BackupServiceError::Validation(
            "cannot reset automatic backup settings while restore recovery is pending".into(),
        ));
    }
    let target = DatabaseTarget::resolve(config_path).map_err(io_err)?;
    reset_automatic_backup_settings_for_target(&target).await
}

async fn reset_automatic_backup_settings_for_target(
    target: &DatabaseTarget,
) -> Result<(), BackupServiceError> {
    if read_settings(target).await?.is_some() {
        let defaults = serde_json::to_string(&super::automatic::StoredAutoSettings::default())
            .map_err(io_err)?;
        write_marker(target, AUTO_SETTINGS_KEY, &defaults).await?;
    }
    tracing::warn!(
        "automatic backup settings explicitly reset; the stored automatic backup password was discarded"
    );
    Ok(())
}

async fn read_settings(
    target: &DatabaseTarget,
) -> Result<Option<BTreeMap<String, String>>, BackupServiceError> {
    let rows: Vec<(String, String)> = match target {
        DatabaseTarget::PostgresUrl(url) => {
            let mut conn = sqlx::PgConnection::connect(url).await.map_err(io_err)?;
            let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'settings')").fetch_one(&mut conn).await.map_err(io_err)?;
            if !exists {
                return Ok(None);
            }
            sqlx::query_as("SELECT key, value FROM settings")
                .fetch_all(&mut conn)
                .await
                .map_err(io_err)?
        }
        target => {
            let path = target.sqlite_path().map_err(io_err)?.ok_or_else(|| {
                BackupServiceError::Validation("SQLite backup has no path".into())
            })?;
            if !path.exists() {
                return Ok(None);
            }
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .read_only(true)
                .create_if_missing(false);
            let mut conn = sqlx::SqliteConnection::connect_with(&options)
                .await
                .map_err(io_err)?;
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'settings'",
            )
            .fetch_one(&mut conn)
            .await
            .map_err(io_err)?;
            if exists == 0 {
                return Ok(None);
            }
            sqlx::query_as("SELECT key, value FROM settings")
                .fetch_all(&mut conn)
                .await
                .map_err(io_err)?
        }
    };
    Ok(Some(rows.into_iter().collect()))
}

async fn write_marker(
    target: &DatabaseTarget,
    key: &str,
    value: &str,
) -> Result<(), BackupServiceError> {
    match target {
        DatabaseTarget::PostgresUrl(url) => {
            let mut conn = sqlx::PgConnection::connect(url).await.map_err(io_err)?;
            sqlx::query("INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT(key) DO UPDATE SET value = excluded.value").bind(key).bind(value).execute(&mut conn).await.map_err(io_err)?;
            conn.close().await.map_err(io_err)?;
        }
        target => {
            let path = target.sqlite_path().map_err(io_err)?.ok_or_else(|| {
                BackupServiceError::Validation("SQLite backup has no path".into())
            })?;
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false);
            let mut conn = sqlx::SqliteConnection::connect_with(&options)
                .await
                .map_err(io_err)?;
            sqlx::query("INSERT INTO settings (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value").bind(key).bind(value).execute(&mut conn).await.map_err(io_err)?;
            conn.close().await.map_err(io_err)?;
        }
    }
    Ok(())
}

// Called only at server startup, before the normal database opener can migrate it.
// The caller chooses whether a failure blocks migration; failures retain the retry marker.
pub async fn prepare_upgrade_backup(config_path: &Path) -> Result<(), BackupServiceError> {
    let target = DatabaseTarget::resolve(config_path).map_err(io_err)?;
    prepare_upgrade_backup_for_target(
        &target,
        config_path,
        env!("CARGO_PKG_VERSION"),
        |data_dir| {
            crate::persistence::encryption::ensure_encryption_key_for_state(Some(data_dir), true)
        },
    )
    .await
}

pub(super) async fn prepare_upgrade_backup_for_target(
    target: &DatabaseTarget,
    config_path: &Path,
    current_version: &str,
    load_key: impl FnOnce(PathBuf) -> Result<crate::persistence::encryption::EncryptionKey, String>,
) -> Result<(), BackupServiceError> {
    let restore_locator = crate::persistence::setup::default_data_dir_for_config_path(config_path);
    if super::pending::pending_restore_status(&restore_locator).is_some() {
        // An interrupted promotion can have installed the source database before
        // its encryption key. Recovery must finish before reading those secrets
        // or changing the restored version markers.
        tracing::info!("deferring pre-migration automatic backup to pending restore recovery");
        return Ok(());
    }
    let Some(settings) = read_settings(target).await? else {
        return Ok(());
    };
    let previous = settings.get(LAST_VERSION).filter(|value| !value.is_empty());
    let pending = settings
        .get(PENDING_VERSION)
        .filter(|value| !value.is_empty());
    if previous
        .and_then(|version| semver::Version::parse(version).ok())
        .zip(semver::Version::parse(current_version).ok())
        .is_some_and(|(previous, current)| previous > current)
    {
        tracing::warn!("skipping pre-migration automatic backup on a version downgrade");
        write_marker(target, PENDING_VERSION, "").await?;
        write_marker(target, LAST_VERSION, current_version).await?;
        return Ok(());
    }
    if previous.is_some_and(|version| version == current_version) && pending.is_none() {
        write_marker(target, LAST_VERSION, current_version).await?;
        return Ok(());
    }
    let auto = settings
        .get(AUTO_SETTINGS_KEY)
        .map(|value| decode_auto_settings(value).map_err(BackupServiceError::Validation))
        .transpose()?
        .unwrap_or_default();
    if !auto.enabled || auto.encrypted_key.is_none() {
        tracing::info!(
            "skipping pre-migration automatic backup: automatic backups are disabled or the key is absent"
        );
        write_marker(target, LAST_VERSION, current_version).await?;
        write_marker(target, PENDING_VERSION, "").await?;
        return Ok(());
    }
    write_marker(target, PENDING_VERSION, current_version).await?;
    let data_dir = settings
        .get("data_dir")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            crate::persistence::setup::default_data_dir_for_config_path(config_path)
        });
    let data_dir = std::path::absolute(data_dir).map_err(io_err)?;
    let dir = settings
        .get(STORAGE_KEY)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("backups"));
    validate_storage_dir(&dir)?;
    if let Err(error) = super::stored::cleanup_stale_files(&dir, std::time::SystemTime::now()) {
        tracing::warn!(%error, "could not clean abandoned pre-migration backup files");
    }
    let key = load_key(data_dir.clone()).map_err(BackupServiceError::Validation)?;
    let password = crate::persistence::encryption::decrypt_value(
        &key,
        auto.encrypted_key
            .as_deref()
            .ok_or(BackupServiceError::PasswordRequired)?,
    )
    .map_err(BackupServiceError::Validation)?;
    let source_version = previous.map(String::as_str).unwrap_or("unknown");
    let engine = if matches!(target, DatabaseTarget::PostgresUrl(_)) {
        "postgres"
    } else {
        "sqlite"
    };
    let info = new_backup_info(BackupTrigger::Auto, engine, source_version)?;
    write_metadata(&dir, &info)?;
    let cancellation = super::archive::BackupCancellation::new();
    let run = async {
        let export =
            super::logical::export_before_migrations_cancellable(target, cancellation.clone())
                .await
                .map_err(io_err)?;
        let source_paths = super::service::source_paths_from_export(
            export.staging.path(),
            &data_dir.to_string_lossy(),
        )?;
        let secrets = BackupInstanceSecrets {
            encryption_master_key: key.to_base64(),
            key_source: crate::persistence::encryption::backup_key_source_name(
                Some(data_dir),
                &key,
            ),
        };
        let secrets_path = export.staging.path().join("instance-secrets.json");
        super::service::write_json(&secrets_path, &secrets)?;
        let mut manifest = build_bundle_manifest(
            source_paths,
            &export,
            super::service::checksum_hex(&secrets_path)?,
        );
        manifest.source_weaver_version = source_version.into();
        super::service::write_json(&export.staging.path().join("manifest.json"), &manifest)?;
        let output = dir.join(&info.filename);
        let staging = export.staging;
        let cancel = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            super::archive::write_bundle_archive_cancellable(
                &output,
                &password,
                staging.path(),
                cancel,
            )
        })
        .await
        .map_err(io_err)?
        .map_err(io_err)?;
        Ok::<_, BackupServiceError>(manifest)
    };
    // Startup waits for this export before migrations can touch the source.
    // Its duration scales with the database, so the interactive backup
    // deadline must not prevent a large installation from upgrading.
    let result = run.await;
    let info = complete_backup(&dir, info, result)?;
    // Clear pending first: a crash before advancing the last version safely retries.
    write_marker(target, PENDING_VERSION, "").await?;
    write_marker(target, LAST_VERSION, current_version).await?;
    prune_retained_backups(&dir, current_version);
    tracing::info!(filename = %info.filename, "created pre-migration automatic backup");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::automatic::StoredAutoSettings;
    use super::super::stored::{BackupArtifactStatus, list_backups};
    use super::*;
    use crate::persistence::encryption::{EncryptionKey, encrypt_value};

    #[tokio::test]
    async fn successful_open_keeps_retry_pending_and_labels_retry_with_running_version() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let db = crate::Database::open(&path).unwrap();
        let key = EncryptionKey::generate();
        db.set_setting("data_dir", &root.path().display().to_string())
            .unwrap();
        db.set_setting(LAST_VERSION, "0.14.1").unwrap();
        db.set_setting(PENDING_VERSION, "0.14.2").unwrap();
        db.set_setting(
            AUTO_SETTINGS_KEY,
            &serde_json::to_string(&StoredAutoSettings {
                enabled: true,
                daily_time_local: "03:00".into(),
                encrypted_key: Some(encrypt_value(&key, "upgrade password").unwrap()),
            })
            .unwrap(),
        )
        .unwrap();
        record_started_version_for(&db, "0.14.2").unwrap();
        assert_eq!(
            db.get_setting(PENDING_VERSION).unwrap().as_deref(),
            Some("0.14.2")
        );
        db.close().unwrap();
        let target = DatabaseTarget::SqlitePath(path);
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| Ok(key))
            .await
            .unwrap();
        let rows = list_backups(&root.path().join("backups"), crate::e2e_clock::utc_now()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_weaver_version, "0.14.2");
        assert_eq!(
            read_settings(&target).await.unwrap().unwrap()[PENDING_VERSION],
            ""
        );
    }

    #[tokio::test]
    async fn explicit_automatic_reset_preserves_other_credentials_and_version_markers() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let mut db = crate::Database::open(&path).unwrap();
        let key = EncryptionKey::generate();
        db.set_encryption_key(key.clone());
        let server = serde_json::from_value::<crate::servers::ServerConfig>(serde_json::json!({
            "id": 1, "host": "provider.invalid", "port": 563, "tls": true,
            "username": "fixture", "password": "provider password", "connections": 1
        }))
        .unwrap();
        db.insert_server(&server).unwrap();
        db.set_setting(LAST_VERSION, "0.13.0").unwrap();
        db.set_setting(PENDING_VERSION, "0.14.2").unwrap();
        db.set_setting("unrelated.setting", "retained").unwrap();
        db.set_setting(AUTO_SETTINGS_KEY, "{broken").unwrap();
        db.close().unwrap();
        let target = DatabaseTarget::SqlitePath(path.clone());
        let mut before = read_settings(&target).await.unwrap().unwrap();
        before.remove(AUTO_SETTINGS_KEY);
        reset_automatic_backup_settings_for_target(&target)
            .await
            .unwrap();
        let mut after = read_settings(&target).await.unwrap().unwrap();
        let reset = decode_auto_settings(&after.remove(AUTO_SETTINGS_KEY).unwrap()).unwrap();
        assert!(!reset.enabled);
        assert!(reset.encrypted_key.is_none());
        assert_eq!(after, before);
        let mut reopened = crate::Database::open(&path).unwrap();
        reopened.set_encryption_key(key.clone());
        reopened.validate_encrypted_credentials(&key).unwrap();
        assert_eq!(
            reopened.load_config().unwrap().servers[0]
                .password
                .as_deref(),
            Some("provider password")
        );
        assert!(
            reopened
                .validate_encrypted_credentials(&EncryptionKey::generate())
                .is_err()
        );
    }

    #[tokio::test]
    async fn explicit_upgrade_skip_clears_retry_without_reading_corrupt_settings() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let db = crate::Database::open(&path).unwrap();
        db.set_setting(LAST_VERSION, "0.13.0").unwrap();
        db.set_setting(PENDING_VERSION, "0.14.2").unwrap();
        db.set_setting(AUTO_SETTINGS_KEY, "{broken").unwrap();
        db.close().unwrap();
        let target = DatabaseTarget::SqlitePath(path);
        skip_upgrade_backup_for_target(&target, "0.14.2")
            .await
            .unwrap();
        let settings = read_settings(&target).await.unwrap().unwrap();
        assert_eq!(settings[LAST_VERSION], "0.14.2");
        assert_eq!(settings[PENDING_VERSION], "");
        assert_eq!(settings[AUTO_SETTINGS_KEY], "{broken");
        assert!(!root.path().join("backups").exists());
    }

    #[tokio::test]
    async fn downgrade_does_not_attempt_to_export_a_newer_catalog() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let db = crate::Database::open(&path).unwrap();
        db.set_setting(LAST_VERSION, "1.0.0").unwrap();
        db.set_setting(AUTO_SETTINGS_KEY, "{broken").unwrap();
        db.close().unwrap();
        let target = DatabaseTarget::SqlitePath(path);
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| {
            panic!("downgrade must not load the backup key")
        })
        .await
        .unwrap();
        assert_eq!(
            read_settings(&target).await.unwrap().unwrap()[LAST_VERSION],
            "0.14.2"
        );
        assert!(!root.path().join("backups").exists());
    }

    #[tokio::test]
    async fn upgrade_backup_preserves_old_schema_and_retries_before_migration() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let db = crate::Database::open(&path).unwrap();
        let key = EncryptionKey::generate();
        db.set_setting("data_dir", &root.path().display().to_string())
            .unwrap();
        db.set_setting(LAST_VERSION, "0.14.1").unwrap();
        db.set_setting(
            AUTO_SETTINGS_KEY,
            &serde_json::to_string(&StoredAutoSettings {
                enabled: true,
                daily_time_local: "03:00".into(),
                encrypted_key: Some(encrypt_value(&key, "upgrade password").unwrap()),
            })
            .unwrap(),
        )
        .unwrap();
        db.close().unwrap();
        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
        let mut conn = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        sqlx::raw_sql("DROP TABLE bandwidth_usage_minute_buckets; CREATE TABLE bandwidth_usage_minute_buckets (bucket_epoch_minute INTEGER PRIMARY KEY, payload_bytes INTEGER NOT NULL); INSERT INTO bandwidth_usage_minute_buckets VALUES (42, 123); UPDATE schema_version SET version = 50;").execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
        let target = DatabaseTarget::SqlitePath(path.clone());
        // A key read failure must preserve the source version and leave a retry marker.
        assert!(
            prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| Err(
                "key unavailable".into()
            ))
            .await
            .is_err()
        );
        let settings = read_settings(&target).await.unwrap().unwrap();
        assert_eq!(settings[LAST_VERSION], "0.14.1");
        assert_eq!(settings[PENDING_VERSION], "0.14.2");
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| Ok(key.clone()))
            .await
            .unwrap();
        let rows = list_backups(&root.path().join("backups"), crate::e2e_clock::utc_now()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, BackupArtifactStatus::Ready);
        assert_eq!(rows[0].source_weaver_version, "0.14.1");
        let unpacked = tempfile::tempdir().unwrap();
        let manifest = super::super::archive::unpack_bundle_archive(
            &root.path().join("backups").join(&rows[0].filename),
            unpacked.path(),
            Some("upgrade password".into()),
        )
        .unwrap();
        assert_eq!(manifest.weaver_schema_version, 50);
        assert_eq!(manifest.source_weaver_version, "0.14.1");
        assert_eq!(
            manifest.tables["bandwidth_usage_minute_buckets"].columns,
            ["bucket_epoch_minute", "payload_bytes"]
        );
        let old_rows = super::super::logical::read_table_objects(
            unpacked.path(),
            "bandwidth_usage_minute_buckets",
        )
        .unwrap();
        assert_eq!(old_rows[0]["payload_bytes"], 123);
        let exported = super::super::logical::export_before_migrations(&target)
            .await
            .unwrap();
        assert_eq!(exported.schema_version, 50);
        let settings = read_settings(&target).await.unwrap().unwrap();
        assert_eq!(settings[LAST_VERSION], "0.14.2");
        assert_eq!(settings[PENDING_VERSION], "");
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| {
            panic!("same-version start must not read key")
        })
        .await
        .unwrap();
        assert_eq!(
            list_backups(&root.path().join("backups"), crate::e2e_clock::utc_now())
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn enabled_unmarked_database_is_backed_up_before_establishing_version_baseline() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let db = crate::Database::open(&path).unwrap();
        let key = EncryptionKey::generate();
        db.set_setting("data_dir", &root.path().display().to_string())
            .unwrap();
        db.set_setting(
            AUTO_SETTINGS_KEY,
            &serde_json::to_string(&StoredAutoSettings {
                enabled: true,
                daily_time_local: "03:00".into(),
                encrypted_key: Some(encrypt_value(&key, "upgrade password").unwrap()),
            })
            .unwrap(),
        )
        .unwrap();
        db.close().unwrap();
        let target = DatabaseTarget::SqlitePath(path);
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| Ok(key))
            .await
            .unwrap();
        let rows = list_backups(&root.path().join("backups"), crate::e2e_clock::utc_now()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, BackupArtifactStatus::Ready);
        assert_eq!(rows[0].source_weaver_version, "unknown");
        assert_eq!(
            read_settings(&target).await.unwrap().unwrap()[LAST_VERSION],
            "0.14.2"
        );
    }

    #[tokio::test]
    async fn disabled_upgrade_backup_does_not_create_files_or_load_a_key() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("weaver.db");
        let target = DatabaseTarget::SqlitePath(path.clone());
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| {
            panic!("new installation")
        })
        .await
        .unwrap();
        assert!(!path.exists());
        let db = crate::Database::open(&path).unwrap();
        db.set_setting(LAST_VERSION, "0.14.1").unwrap();
        db.close().unwrap();
        prepare_upgrade_backup_for_target(&target, root.path(), "0.14.2", |_| panic!("disabled"))
            .await
            .unwrap();
        assert!(!root.path().join("backups").exists());
        assert_eq!(
            read_settings(&target).await.unwrap().unwrap()[LAST_VERSION],
            "0.14.2"
        );
    }
}
