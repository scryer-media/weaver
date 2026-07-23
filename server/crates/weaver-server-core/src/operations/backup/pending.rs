use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::logical::{read_table_objects, verify_table_parts};
use super::manifest::{
    BackupInstanceSecrets, BackupManifest, BackupServiceError, RestoreOptions, io_err,
    validate_manifest,
};
use super::permissions;
use crate::Database;
use crate::persistence::encryption::promote_restore_encryption_key;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::post_processing::discovery::{copy_package, hash_package, package_files};

pub(crate) const PENDING_RESTORE_DIR: &str = "restore-pending";
const READY_MARKER: &str = "restore-ready";
const LOCATION_POINTER: &str = "restore-pending-location";
const PENDING_METADATA: &str = "pending.json";
const PROMOTION_JOURNAL: &str = "promotion.json";
const FAILURE_REPORT: &str = "restore-error.json";
const PACKAGE_INSTALLS: &str = "package-installs.json";
const DATABASE_RESTORE_MARKER_KEY: &str = "backup_restore_generation";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingRestoreMetadata {
    pub restore_id: String,
    pub legacy: bool,
    pub current_data_dir: String,
    pub restore_locator_dir: String,
    #[serde(default)]
    pub legacy_key_fingerprint: Option<String>,
    #[serde(default)]
    pub legacy_backup_checksum: Option<String>,
    pub options: RestoreOptions,
}

#[derive(Clone, Debug)]
pub struct PendingRestoreOutcome {
    pub restore_id: String,
    pub managed_packages_restored: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRestoreStatus {
    pub restore_id: String,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PromotionPhase {
    Validated,
    DatabasePrepared,
    DatabaseInstalled,
    KeyPromoted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PromotionJournal {
    restore_id: String,
    phase: PromotionPhase,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RestoreFailure {
    restore_id: String,
    error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PackageInstallJournal {
    restore_id: String,
    digests: BTreeSet<String>,
}

#[cfg(test)]
thread_local! {
    static RESTORE_FAIL_AFTER_PHASE: std::cell::Cell<Option<PromotionPhase>> =
        const { std::cell::Cell::new(None) };
    static RESTORE_FAIL_DATABASE_IMPORT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn set_restore_test_fail_after(phase: Option<PromotionPhase>) {
    RESTORE_FAIL_AFTER_PHASE.set(phase);
}

#[cfg(test)]
pub(crate) fn set_restore_test_fail_database_import(fail: bool) {
    RESTORE_FAIL_DATABASE_IMPORT.set(fail);
}

#[cfg(test)]
fn fail_after_phase_if_requested(phase: PromotionPhase) -> Result<(), BackupServiceError> {
    if RESTORE_FAIL_AFTER_PHASE.get() == Some(phase) {
        RESTORE_FAIL_AFTER_PHASE.set(None);
        Err(BackupServiceError::Io(format!(
            "injected restore interruption after {phase:?}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(not(test))]
fn fail_after_phase_if_requested(_phase: PromotionPhase) -> Result<(), BackupServiceError> {
    Ok(())
}

#[cfg(test)]
fn fail_database_import_if_requested() -> Result<(), BackupServiceError> {
    if RESTORE_FAIL_DATABASE_IMPORT.replace(false) {
        Err(BackupServiceError::Io(
            "injected restore database import failure".into(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(not(test))]
fn fail_database_import_if_requested() -> Result<(), BackupServiceError> {
    Ok(())
}

pub(crate) fn pending_restore_path(data_dir: &Path) -> PathBuf {
    let direct = data_dir.join(PENDING_RESTORE_DIR);
    if direct.join(READY_MARKER).is_file() {
        return direct;
    }
    std::fs::read_to_string(data_dir.join(LOCATION_POINTER))
        .ok()
        .map(|path| PathBuf::from(path.trim()))
        .filter(|path| {
            path.is_absolute()
                && path
                    .file_name()
                    .is_some_and(|name| name == PENDING_RESTORE_DIR)
                && path.join(READY_MARKER).is_file()
        })
        .unwrap_or(direct)
}

pub(crate) fn pending_restore_status(data_dir: &Path) -> Option<PendingRestoreStatus> {
    let root = pending_restore_path(data_dir);
    if !root.join(READY_MARKER).is_file() {
        return None;
    }
    let restore_id = std::fs::read(root.join(PENDING_METADATA))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PendingRestoreMetadata>(&bytes).ok())
        .map(|metadata| metadata.restore_id)?;
    let error = std::fs::read(root.join(FAILURE_REPORT))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<RestoreFailure>(&bytes).ok())
        .map(|failure| failure.error);
    Some(PendingRestoreStatus { restore_id, error })
}

pub(crate) fn stage_pending_restore(
    extracted_root: &Path,
    current_data_dir: &Path,
    metadata: &PendingRestoreMetadata,
) -> Result<(), BackupServiceError> {
    let destination = current_data_dir.join(PENDING_RESTORE_DIR);
    if destination.exists() {
        return Err(BackupServiceError::Validation(
            "a restore is already pending; restart Weaver before staging another".into(),
        ));
    }
    std::fs::create_dir_all(current_data_dir).map_err(io_err)?;
    let pointer_destination = std::fs::canonicalize(current_data_dir)
        .map_err(io_err)?
        .join(PENDING_RESTORE_DIR);
    let mut pointer_dirs = Vec::new();
    for pointer_dir in [
        PathBuf::from(&metadata.restore_locator_dir),
        PathBuf::from(&metadata.options.data_dir),
    ] {
        if pointer_dir != current_data_dir && !pointer_dirs.contains(&pointer_dir) {
            pointer_dirs.push(pointer_dir);
        }
    }
    let mut written_pointers = Vec::new();
    for pointer_dir in pointer_dirs {
        if let Err(error) = write_location_pointer(&pointer_dir, &pointer_destination) {
            for written in written_pointers {
                let _ = std::fs::remove_file(written);
            }
            return Err(error);
        }
        written_pointers.push(pointer_dir.join(LOCATION_POINTER));
    }

    let staging = current_data_dir.join(format!(".restore-pending-{}", metadata.restore_id));
    let mut published = false;
    let result = (|| {
        if staging.exists() {
            std::fs::remove_dir_all(&staging).map_err(io_err)?;
        }
        std::fs::create_dir(&staging).map_err(io_err)?;
        set_directory_owner_only(&staging)?;
        copy_tree(extracted_root, &staging)?;
        write_owner_only_json(&staging.join(PENDING_METADATA), metadata)?;
        let ready = staging.join(READY_MARKER);
        write_owner_only(&ready, metadata.restore_id.as_bytes())?;
        sync_directory(&staging)?;
        std::fs::rename(&staging, &destination).map_err(io_err)?;
        published = true;
        sync_directory(current_data_dir)
    })();
    if let Err(error) = result {
        for written in written_pointers {
            let _ = std::fs::remove_file(written);
        }
        let _ = std::fs::remove_dir_all(&staging);
        if published {
            let _ = std::fs::remove_dir_all(&destination);
        }
        let _ = sync_directory(current_data_dir);
        return Err(error);
    }
    Ok(())
}

pub fn apply_pending_restore(
    db: Database,
    data_dir: &Path,
) -> Result<(Database, Option<PendingRestoreOutcome>), BackupServiceError> {
    cleanup_completed_restores(data_dir)?;
    let root = pending_restore_path(data_dir);
    let result = apply_pending_restore_inner(db, data_dir, &root);
    if let Err(error) = &result {
        if read_promotion_journal(&root)
            .ok()
            .flatten()
            .is_none_or(|journal| journal.phase <= PromotionPhase::Validated)
        {
            cleanup_pending_package_installs(&root);
        }
        if let Ok(metadata) = read_pending_metadata(&root) {
            let _ = write_atomic_owner_only_json(
                &root.join(FAILURE_REPORT),
                &RestoreFailure {
                    restore_id: metadata.restore_id,
                    error: error.to_string(),
                },
            );
        }
    }
    result
}

fn cleanup_completed_restores(data_dir: &Path) -> Result<(), BackupServiceError> {
    let mut parents = BTreeSet::from([data_dir.to_path_buf()]);
    let pointer = data_dir.join(LOCATION_POINTER);
    if let Ok(raw) = std::fs::read_to_string(&pointer) {
        let pending = PathBuf::from(raw.trim());
        if pending.is_absolute()
            && pending
                .file_name()
                .is_some_and(|name| name == PENDING_RESTORE_DIR)
            && let Some(parent) = pending.parent()
        {
            parents.insert(parent.to_path_buf());
            if !pending.exists() {
                remove_file_if_present(&pointer)?;
                sync_directory(data_dir)?;
            }
        }
    }
    for parent in parents {
        let entries = match std::fs::read_dir(&parent) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_err(error)),
        };
        for entry in entries {
            let entry = entry.map_err(io_err)?;
            if entry.file_name().to_str().is_some_and(|name| {
                name.starts_with(".restore-completed-") || name.starts_with(".restore-pending-")
            }) {
                std::fs::remove_dir_all(entry.path()).map_err(io_err)?;
            }
        }
        sync_directory(&parent)?;
    }
    Ok(())
}

fn apply_pending_restore_inner(
    db: Database,
    data_dir: &Path,
    root: &Path,
) -> Result<(Database, Option<PendingRestoreOutcome>), BackupServiceError> {
    if !root.exists() {
        return Ok((db, None));
    }
    if !root.join(READY_MARKER).is_file() {
        std::fs::remove_dir_all(root).map_err(io_err)?;
        return Ok((db, None));
    }
    let metadata = read_pending_metadata(root)?;
    let ready_restore_id = std::fs::read_to_string(root.join(READY_MARKER)).map_err(io_err)?;
    if ready_restore_id.trim() != metadata.restore_id {
        return Err(BackupServiceError::Validation(
            "staged restore ready marker does not match its metadata".into(),
        ));
    }
    let mut journal = read_promotion_journal(root)?;
    if journal
        .as_ref()
        .is_some_and(|journal| journal.restore_id != metadata.restore_id)
    {
        return Err(BackupServiceError::Validation(
            "staged restore promotion journal belongs to another restore".into(),
        ));
    }
    if journal.is_none() {
        if !db
            .restore_target_is_pristine()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?
        {
            return Err(BackupServiceError::NotPristine);
        }
        journal = Some(write_promotion_phase(
            root,
            &metadata.restore_id,
            PromotionPhase::Validated,
        )?);
        fail_after_phase_if_requested(PromotionPhase::Validated)?;
    }
    let _ = std::fs::remove_file(root.join(FAILURE_REPORT));

    let (manifest, restored_key, installed_packages) = if metadata.legacy {
        let expected_checksum = metadata.legacy_backup_checksum.as_deref().ok_or_else(|| {
            BackupServiceError::Validation("staged legacy restore has no database checksum".into())
        })?;
        if file_checksum(&root.join("backup.db"))? != expected_checksum {
            return Err(BackupServiceError::Validation(
                "staged legacy backup database failed checksum validation".into(),
            ));
        }
        let key = match db.encryption_key().cloned() {
            Some(key) => key,
            None => crate::persistence::encryption::ensure_encryption_key(Some(PathBuf::from(
                &metadata.current_data_dir,
            )))
            .map_err(BackupServiceError::KeyIncompatible)?,
        };
        if metadata
            .legacy_key_fingerprint
            .as_deref()
            .is_some_and(|expected| expected != encryption_key_fingerprint(&key))
        {
            return Err(BackupServiceError::KeyIncompatible(
                "the active target key changed after the legacy restore was staged".into(),
            ));
        }
        (None, key, Vec::new())
    } else {
        let manifest: BackupManifest =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json")).map_err(io_err)?)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let secrets: BackupInstanceSecrets = serde_json::from_slice(
            &std::fs::read(root.join("instance-secrets.json")).map_err(io_err)?,
        )
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        revalidate_logical_pending(&db, root, &manifest)?;
        let key = crate::persistence::encryption::EncryptionKey::from_base64(
            &secrets.encryption_master_key,
        )
        .map_err(BackupServiceError::Validation)?;
        let installed = install_managed_packages(
            root,
            &manifest,
            Path::new(&metadata.options.data_dir),
            &metadata.restore_id,
        )?;
        (Some((manifest, secrets)), key, installed)
    };

    let sqlite_path = db
        .database_target()
        .sqlite_path()
        .map_err(|error| BackupServiceError::Io(error.to_string()))?;
    let db = if let Some(live_path) = sqlite_path {
        apply_sqlite_restore(
            db,
            root,
            &metadata,
            manifest.as_ref().map(|(manifest, _)| manifest),
            &restored_key,
            &mut journal,
            &live_path,
            &installed_packages,
        )?
    } else {
        apply_postgres_restore(
            db,
            root,
            &metadata,
            manifest.as_ref().map(|(manifest, _)| manifest),
            &restored_key,
            &mut journal,
            &installed_packages,
        )?
    };

    let pending_parent = root.parent().unwrap_or(data_dir).to_path_buf();
    let completed = root.with_file_name(format!(".restore-completed-{}", metadata.restore_id));
    std::fs::rename(root, &completed).map_err(io_err)?;
    let _ = std::fs::remove_dir_all(&completed);
    let pointer_dirs = BTreeSet::from([
        PathBuf::from(&metadata.current_data_dir),
        PathBuf::from(&metadata.restore_locator_dir),
        PathBuf::from(&metadata.options.data_dir),
    ]);
    for pointer_dir in pointer_dirs {
        remove_file_if_present(&pointer_dir.join(LOCATION_POINTER))?;
        if pointer_dir.is_dir() {
            sync_directory(&pointer_dir)?;
        }
    }
    sync_directory(&pending_parent)?;
    Ok((
        db,
        Some(PendingRestoreOutcome {
            restore_id: metadata.restore_id,
            managed_packages_restored: installed_packages.len(),
        }),
    ))
}

#[allow(clippy::too_many_arguments)]
fn apply_sqlite_restore(
    mut db: Database,
    root: &Path,
    metadata: &PendingRestoreMetadata,
    manifest: Option<&BackupManifest>,
    restored_key: &crate::persistence::encryption::EncryptionKey,
    journal: &mut Option<PromotionJournal>,
    live_path: &Path,
    installed_packages: &[PathBuf],
) -> Result<Database, BackupServiceError> {
    let database_parent = live_path.parent().unwrap_or_else(|| Path::new("."));
    let preparing = database_parent.join(format!(
        ".weaver-restore-{}.preparing.db",
        metadata.restore_id
    ));
    let prepared = database_parent.join(format!(
        ".weaver-restore-{}.prepared.db",
        metadata.restore_id
    ));
    let prepare_now = journal_phase(journal) < PromotionPhase::DatabasePrepared;
    if prepare_now {
        remove_file_if_present(&preparing)?;
        remove_file_if_present(&prepared)?;
        let mut prepared_db = Database::open(&preparing)
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        set_file_owner_only(&preparing)?;
        prepared_db.set_encryption_key(restored_key.clone());
        if let Err(error) = import_restore_payload(&prepared_db, root, metadata, manifest) {
            let _ = prepared_db.close();
            let _ = std::fs::remove_file(&preparing);
            remove_installed_packages(installed_packages);
            return Err(error);
        }
        validate_restored_database(&prepared_db, manifest)?;
        set_database_restore_marker(&prepared_db, &metadata.restore_id)?;
        prepared_db
            .checkpoint_sqlite()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        prepared_db
            .close()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        remove_sqlite_sidecars(&preparing)?;
        sync_file(&preparing)?;
        std::fs::rename(&preparing, &prepared).map_err(io_err)?;
        sync_directory(database_parent)?;
        *journal = Some(write_promotion_phase(
            root,
            &metadata.restore_id,
            PromotionPhase::DatabasePrepared,
        )?);
        fail_after_phase_if_requested(PromotionPhase::DatabasePrepared)?;
    }

    if !prepare_now && prepared.exists() {
        let mut prepared_db = Database::open(&prepared)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        prepared_db.set_encryption_key(restored_key.clone());
        require_database_restore_marker(&prepared_db, &metadata.restore_id)?;
        validate_restored_database(&prepared_db, manifest)?;
        prepared_db
            .checkpoint_sqlite()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        prepared_db
            .close()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        remove_sqlite_sidecars(&prepared)?;
        sync_file(&prepared)?;
    }

    let target = db.database_target().clone();
    if journal_phase(journal) < PromotionPhase::DatabaseInstalled {
        if prepared.exists() {
            db.checkpoint_sqlite()
                .map_err(|error| BackupServiceError::Io(error.to_string()))?;
            db.close()
                .map_err(|error| BackupServiceError::Io(error.to_string()))?;
            remove_sqlite_sidecars(live_path)?;
            replace_sqlite_database(&prepared, live_path)?;
        } else {
            if !live_path.is_file() {
                return Err(BackupServiceError::Validation(
                    "prepared restore database disappeared before promotion".into(),
                ));
            }
            db.set_encryption_key(restored_key.clone());
            require_database_restore_marker(&db, &metadata.restore_id).map_err(|_| {
                BackupServiceError::Validation(
                    "prepared restore database disappeared before it was installed".into(),
                )
            })?;
            validate_restored_database(&db, manifest)?;
            db.checkpoint_sqlite()
                .map_err(|error| BackupServiceError::Io(error.to_string()))?;
            db.close()
                .map_err(|error| BackupServiceError::Io(error.to_string()))?;
            remove_sqlite_sidecars(live_path)?;
        }
        *journal = Some(write_promotion_phase(
            root,
            &metadata.restore_id,
            PromotionPhase::DatabaseInstalled,
        )?);
        fail_after_phase_if_requested(PromotionPhase::DatabaseInstalled)?;
    } else {
        db.close()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
    }

    activate_restore_key(metadata, restored_key)?;
    *journal = Some(write_promotion_phase(
        root,
        &metadata.restore_id,
        PromotionPhase::KeyPromoted,
    )?);
    fail_after_phase_if_requested(PromotionPhase::KeyPromoted)?;
    let mut restored =
        Database::open_target(target).map_err(|error| BackupServiceError::Io(error.to_string()))?;
    restored.set_encryption_key(restored_key.clone());
    validate_restored_database(&restored, manifest)?;
    clear_database_restore_marker(&restored, &metadata.restore_id)?;
    restored
        .checkpoint_sqlite()
        .map_err(|error| BackupServiceError::Io(error.to_string()))?;
    Ok(restored)
}

fn apply_postgres_restore(
    mut db: Database,
    root: &Path,
    metadata: &PendingRestoreMetadata,
    manifest: Option<&BackupManifest>,
    restored_key: &crate::persistence::encryption::EncryptionKey,
    journal: &mut Option<PromotionJournal>,
    installed_packages: &[PathBuf],
) -> Result<Database, BackupServiceError> {
    if journal_phase(journal) < PromotionPhase::DatabaseInstalled {
        if let Err(error) = import_restore_payload(&db, root, metadata, manifest) {
            remove_installed_packages(installed_packages);
            return Err(error);
        }
        *journal = Some(write_promotion_phase(
            root,
            &metadata.restore_id,
            PromotionPhase::DatabaseInstalled,
        )?);
        fail_after_phase_if_requested(PromotionPhase::DatabaseInstalled)?;
    }
    activate_restore_key(metadata, restored_key)?;
    db.set_encryption_key(restored_key.clone());
    *journal = Some(write_promotion_phase(
        root,
        &metadata.restore_id,
        PromotionPhase::KeyPromoted,
    )?);
    fail_after_phase_if_requested(PromotionPhase::KeyPromoted)?;
    validate_restored_database(&db, manifest)?;
    Ok(db)
}

fn import_restore_payload(
    db: &Database,
    root: &Path,
    metadata: &PendingRestoreMetadata,
    manifest: Option<&BackupManifest>,
) -> Result<(), BackupServiceError> {
    fail_database_import_if_requested()?;
    if metadata.legacy {
        db.import_stable_state_with_post_processing(&root.join("backup.db"), false)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))
    } else {
        let manifest = manifest.ok_or_else(|| {
            BackupServiceError::Validation("logical restore has no manifest".into())
        })?;
        db.import_logical_backup(
            &root.join("tables"),
            &manifest.tables,
            manifest.weaver_schema_version,
        )
        .map_err(|error| BackupServiceError::Validation(error.to_string()))
    }
}

fn activate_restore_key(
    metadata: &PendingRestoreMetadata,
    restored_key: &crate::persistence::encryption::EncryptionKey,
) -> Result<(), BackupServiceError> {
    let promoted = promote_restore_encryption_key(
        Some(PathBuf::from(&metadata.options.data_dir)),
        &restored_key.to_base64(),
    )
    .map_err(BackupServiceError::KeyIncompatible)?;
    if promoted.to_base64() != restored_key.to_base64() {
        return Err(BackupServiceError::KeyIncompatible(
            "promoted encryption key does not match the staged bundle".into(),
        ));
    }
    Ok(())
}

pub(crate) fn encryption_key_fingerprint(
    key: &crate::persistence::encryption::EncryptionKey,
) -> String {
    blake3::hash(key.to_base64().as_bytes())
        .to_hex()
        .to_string()
}

fn database_restore_marker(db: &Database) -> Result<Option<String>, BackupServiceError> {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "CREATE TABLE IF NOT EXISTS weaver_internal_metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )",
            &[],
        )
        .await?;
        SqlRuntime::fetch_optional(
            datastore.read_exec(),
            "SELECT value FROM weaver_internal_metadata WHERE key = {}",
            &[SqlArg::Text(DATABASE_RESTORE_MARKER_KEY.into())],
        )
        .await?
        .map(|row| row.text("value"))
        .transpose()
    })
    .map_err(|error| BackupServiceError::Io(error.to_string()))
}

fn set_database_restore_marker(db: &Database, restore_id: &str) -> Result<(), BackupServiceError> {
    let datastore = db.datastore();
    let restore_id = restore_id.to_string();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "CREATE TABLE IF NOT EXISTS weaver_internal_metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )",
            &[],
        )
        .await?;
        SqlRuntime::execute(
            datastore.read_exec(),
            "INSERT INTO weaver_internal_metadata (key, value)
             VALUES ({}, {})
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            &[
                SqlArg::Text(DATABASE_RESTORE_MARKER_KEY.into()),
                SqlArg::Text(restore_id),
            ],
        )
        .await?;
        Ok(())
    })
    .map_err(|error| BackupServiceError::Io(error.to_string()))
}

fn require_database_restore_marker(
    db: &Database,
    restore_id: &str,
) -> Result<(), BackupServiceError> {
    if database_restore_marker(db)?.as_deref() == Some(restore_id) {
        Ok(())
    } else {
        Err(BackupServiceError::Validation(
            "database restore marker does not match the pending restore".into(),
        ))
    }
}

fn clear_database_restore_marker(
    db: &Database,
    restore_id: &str,
) -> Result<(), BackupServiceError> {
    let Some(marker) = database_restore_marker(db)? else {
        return Ok(());
    };
    if marker != restore_id {
        return Err(BackupServiceError::Validation(
            "database restore marker belongs to another restore".into(),
        ));
    }
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "DELETE FROM weaver_internal_metadata WHERE key = {}",
            &[SqlArg::Text(DATABASE_RESTORE_MARKER_KEY.into())],
        )
        .await?;
        Ok(())
    })
    .map_err(|error| BackupServiceError::Io(error.to_string()))
}

fn validate_restored_database(
    db: &Database,
    manifest: Option<&BackupManifest>,
) -> Result<(), BackupServiceError> {
    let encryption_key = db.encryption_key().ok_or_else(|| {
        BackupServiceError::Validation(
            "restored database has no active encryption master key".into(),
        )
    })?;
    db.validate_encrypted_credentials(encryption_key)
        .map_err(|error| {
            BackupServiceError::Validation(format!(
                "restored database does not match its encryption master key: {error}"
            ))
        })?;
    let config = db
        .load_config()
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    config
        .validate()
        .map_err(|errors| BackupServiceError::Validation(errors.join("; ")))?;
    if let Some(manifest) = manifest {
        db.validate_logical_backup_import(&manifest.tables)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    }
    Ok(())
}

fn journal_phase(journal: &Option<PromotionJournal>) -> PromotionPhase {
    journal
        .as_ref()
        .map_or(PromotionPhase::Validated, |journal| journal.phase)
}

fn read_pending_metadata(root: &Path) -> Result<PendingRestoreMetadata, BackupServiceError> {
    serde_json::from_slice(&std::fs::read(root.join(PENDING_METADATA)).map_err(io_err)?)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))
}

fn read_promotion_journal(root: &Path) -> Result<Option<PromotionJournal>, BackupServiceError> {
    let path = root.join(PROMOTION_JOURNAL);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice(&std::fs::read(path).map_err(io_err)?)
        .map(Some)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))
}

fn write_promotion_phase(
    root: &Path,
    restore_id: &str,
    phase: PromotionPhase,
) -> Result<PromotionJournal, BackupServiceError> {
    let journal = PromotionJournal {
        restore_id: restore_id.to_string(),
        phase,
    };
    write_atomic_owner_only_json(&root.join(PROMOTION_JOURNAL), &journal)?;
    sync_directory(root)?;
    Ok(journal)
}

fn remove_installed_packages(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_dir_all(path);
    }
}

fn cleanup_pending_package_installs(root: &Path) {
    let Ok(metadata) = read_pending_metadata(root) else {
        return;
    };
    let Ok(bytes) = std::fs::read(root.join(PACKAGE_INSTALLS)) else {
        return;
    };
    let Ok(journal) = serde_json::from_slice::<PackageInstallJournal>(&bytes) else {
        return;
    };
    if journal.restore_id != metadata.restore_id {
        return;
    }
    let data_dir = Path::new(&metadata.options.data_dir);
    let paths = journal
        .digests
        .iter()
        .filter_map(|digest| digest.strip_prefix("blake3:"))
        .map(|digest| data_dir.join("managed-extensions/blake3").join(digest))
        .collect::<Vec<_>>();
    remove_installed_packages(&paths);
}

fn sync_file(path: &Path) -> Result<(), BackupServiceError> {
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_err)
}

fn remove_file_if_present(path: &Path) -> Result<(), BackupServiceError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_err(error)),
    }
}

fn remove_sqlite_sidecars(database: &Path) -> Result<(), BackupServiceError> {
    let raw = database.as_os_str().to_string_lossy();
    for suffix in ["-wal", "-shm"] {
        remove_file_if_present(Path::new(&format!("{raw}{suffix}")))?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_sqlite_database(prepared: &Path, live: &Path) -> Result<(), BackupServiceError> {
    std::fs::rename(prepared, live).map_err(io_err)?;
    sync_directory(live.parent().unwrap_or_else(|| Path::new(".")))
}

#[cfg(windows)]
fn replace_sqlite_database(prepared: &Path, live: &Path) -> Result<(), BackupServiceError> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::ptr;
    use windows_sys::Win32::Storage::FileSystem::{REPLACEFILE_WRITE_THROUGH, ReplaceFileW};

    if !live.exists() {
        std::fs::rename(prepared, live).map_err(io_err)?;
        return Ok(());
    }
    let backup_path = prepared.with_extension("original.db");
    remove_file_if_present(&backup_path)?;
    let wide = |path: &Path| {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>()
    };
    let live_wide = wide(live);
    let prepared_wide = wide(prepared);
    let backup_wide = wide(&backup_path);
    let result = unsafe {
        ReplaceFileW(
            live_wide.as_ptr(),
            prepared_wide.as_ptr(),
            backup_wide.as_ptr(),
            REPLACEFILE_WRITE_THROUGH,
            ptr::null(),
            ptr::null(),
        )
    };
    if result == 0 {
        return Err(io_err(std::io::Error::last_os_error()));
    }
    remove_file_if_present(&backup_path)?;
    sync_directory(live.parent().unwrap_or_else(|| Path::new(".")))
}

fn revalidate_logical_pending(
    db: &Database,
    root: &Path,
    manifest: &BackupManifest,
) -> Result<(), BackupServiceError> {
    validate_manifest(db, manifest)?;
    verify_table_parts(root, &manifest.tables)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let expected_secrets = manifest
        .part_checksums
        .get("instance-secrets.json")
        .ok_or_else(|| {
            BackupServiceError::Validation("logical bundle has no instance-secrets checksum".into())
        })?;
    if &file_checksum(&root.join("instance-secrets.json"))? != expected_secrets {
        return Err(BackupServiceError::Validation(
            "staged instance secrets failed checksum validation".into(),
        ));
    }

    for package in &manifest.managed_packages {
        let path = root.join(&package.archive_prefix);
        let files = package_files(&path)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let bytes = files.iter().try_fold(0_u64, |total, (_, file)| {
            total
                .checked_add(std::fs::metadata(file).map_err(io_err)?.len())
                .ok_or_else(|| BackupServiceError::Validation("package is too large".into()))
        })?;
        let digest = hash_package(&path)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        if files.len() != package.file_count
            || bytes != package.uncompressed_bytes
            || digest.as_str() != package.digest
        {
            return Err(BackupServiceError::Validation(format!(
                "staged managed package {} failed inventory validation",
                package.digest
            )));
        }
    }

    let mut referenced = BTreeSet::new();
    for row in read_table_objects(root, "post_processing_extension_revisions")
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?
    {
        if row
            .get("managed_path")
            .is_none_or(serde_json::Value::is_null)
        {
            continue;
        }
        let digest = row
            .get("digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                BackupServiceError::Validation("managed revision has no digest".into())
            })?;
        referenced.insert(digest.to_string());
    }
    let declared = manifest
        .managed_packages
        .iter()
        .map(|package| package.digest.clone())
        .collect::<BTreeSet<_>>();
    if referenced != declared {
        return Err(BackupServiceError::Validation(
            "managed package inventory no longer matches revision references".into(),
        ));
    }
    Ok(())
}

pub(crate) fn file_checksum(path: &Path) -> Result<String, BackupServiceError> {
    let mut file = std::fs::File::open(path).map_err(io_err)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut file, &mut hasher).map_err(io_err)?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn install_managed_packages(
    root: &Path,
    manifest: &BackupManifest,
    data_dir: &Path,
    restore_id: &str,
) -> Result<Vec<PathBuf>, BackupServiceError> {
    let path = root.join(PACKAGE_INSTALLS);
    let mut journal = if path.exists() {
        let journal: PackageInstallJournal =
            serde_json::from_slice(&std::fs::read(&path).map_err(io_err)?)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        if journal.restore_id != restore_id {
            return Err(BackupServiceError::Validation(
                "managed package install journal belongs to another restore".into(),
            ));
        }
        journal
    } else {
        PackageInstallJournal {
            restore_id: restore_id.to_string(),
            digests: BTreeSet::new(),
        }
    };
    let declared = manifest
        .managed_packages
        .iter()
        .map(|package| package.digest.clone())
        .collect::<BTreeSet<_>>();
    if !journal.digests.is_subset(&declared) {
        return Err(BackupServiceError::Validation(
            "managed package install journal contains an undeclared digest".into(),
        ));
    }

    let result = (|| {
        let mut installed = Vec::new();
        for package in &manifest.managed_packages {
            let source = root.join(&package.archive_prefix);
            let target = data_dir.join(&package.archive_prefix);
            if target.exists() {
                let actual = hash_package(&target)
                    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
                if actual.as_str() != package.digest {
                    return Err(BackupServiceError::Validation(format!(
                        "managed package {} already exists with different content",
                        package.digest
                    )));
                }
                if journal.digests.contains(&package.digest) {
                    installed.push(target);
                }
                continue;
            }

            if journal.digests.insert(package.digest.clone()) {
                write_atomic_owner_only_json(&path, &journal)?;
            }
            let parent = target.parent().ok_or_else(|| {
                BackupServiceError::Validation("managed package target has no parent".into())
            })?;
            std::fs::create_dir_all(parent).map_err(io_err)?;
            let temporary = parent.join(format!(
                ".restore-{}-{}",
                std::process::id(),
                package.digest.trim_start_matches("blake3:")
            ));
            if temporary.exists() {
                std::fs::remove_dir_all(&temporary).map_err(io_err)?;
            }
            std::fs::create_dir(&temporary).map_err(io_err)?;
            if let Err(error) = copy_package(&source, &temporary) {
                let _ = std::fs::remove_dir_all(&temporary);
                return Err(BackupServiceError::Validation(error.to_string()));
            }
            if let Err(error) = secure_package_tree(&temporary) {
                let _ = std::fs::remove_dir_all(&temporary);
                return Err(error);
            }
            let actual = match hash_package(&temporary) {
                Ok(actual) => actual,
                Err(error) => {
                    let _ = std::fs::remove_dir_all(&temporary);
                    return Err(BackupServiceError::Validation(error.to_string()));
                }
            };
            if actual.as_str() != package.digest {
                let _ = std::fs::remove_dir_all(&temporary);
                return Err(BackupServiceError::Validation(format!(
                    "managed package {} failed staged verification",
                    package.digest
                )));
            }
            match std::fs::rename(&temporary, &target) {
                Ok(()) => {
                    sync_directory(parent)?;
                    installed.push(target);
                }
                Err(error) if target.exists() => {
                    let _ = std::fs::remove_dir_all(&temporary);
                    let actual = hash_package(&target).map_err(|hash_error| {
                        BackupServiceError::Validation(hash_error.to_string())
                    })?;
                    if actual.as_str() != package.digest {
                        return Err(io_err(error));
                    }
                    installed.push(target);
                }
                Err(error) => {
                    let _ = std::fs::remove_dir_all(&temporary);
                    return Err(io_err(error));
                }
            }
        }
        Ok(installed)
    })();
    if result.is_err() {
        let paths = journal
            .digests
            .iter()
            .filter_map(|digest| digest.strip_prefix("blake3:"))
            .map(|digest| data_dir.join("managed-extensions/blake3").join(digest))
            .collect::<Vec<_>>();
        remove_installed_packages(&paths);
    }
    result
}

fn secure_package_tree(root: &Path) -> Result<(), BackupServiceError> {
    set_directory_owner_only(root)?;
    let files =
        package_files(root).map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let mut directories = BTreeSet::new();
    for (_, file) in files {
        set_file_owner_only(&file)?;
        sync_file(&file)?;
        let mut parent = file.parent();
        while let Some(directory) = parent {
            if directory == root {
                break;
            }
            directories.insert(directory.to_path_buf());
            parent = directory.parent();
        }
    }
    for directory in directories {
        set_directory_owner_only(&directory)?;
        sync_directory(&directory)?;
    }
    sync_directory(root)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), BackupServiceError> {
    for entry in std::fs::read_dir(source).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let file_type = entry.file_type().map_err(io_err)?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            std::fs::create_dir(&target).map_err(io_err)?;
            set_directory_owner_only(&target)?;
            copy_tree(&entry.path(), &target)?;
            sync_directory(&target)?;
        } else if file_type.is_file() {
            std::fs::copy(entry.path(), &target).map_err(io_err)?;
            set_file_owner_only(&target)?;
            sync_file(&target)?;
        } else {
            return Err(BackupServiceError::Validation(
                "prepared restore contains a link or unsupported entry".into(),
            ));
        }
    }
    Ok(())
}

fn write_owner_only_json(path: &Path, value: &impl Serialize) -> Result<(), BackupServiceError> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    write_owner_only(path, &bytes)
}

fn write_atomic_owner_only_json(
    path: &Path,
    value: &impl Serialize,
) -> Result<(), BackupServiceError> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    write_atomic_owner_only(path, &bytes)
}

fn write_location_pointer(
    target_data_dir: &Path,
    pending_root: &Path,
) -> Result<(), BackupServiceError> {
    std::fs::create_dir_all(target_data_dir).map_err(io_err)?;
    let mut bytes = pending_root.to_string_lossy().as_bytes().to_vec();
    bytes.push(b'\n');
    write_owner_only(&target_data_dir.join(LOCATION_POINTER), &bytes)?;
    sync_directory(target_data_dir)
}

fn write_atomic_owner_only(path: &Path, bytes: &[u8]) -> Result<(), BackupServiceError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("restore"),
        std::process::id()
    ));
    remove_file_if_present(&temporary)?;
    write_owner_only(&temporary, bytes)?;
    replace_atomic_file(&temporary, path)?;
    sync_directory(parent)
}

#[cfg(not(windows))]
fn replace_atomic_file(source: &Path, destination: &Path) -> Result<(), BackupServiceError> {
    std::fs::rename(source, destination).map_err(io_err)
}

#[cfg(windows)]
fn replace_atomic_file(source: &Path, destination: &Path) -> Result<(), BackupServiceError> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let wide = |path: &Path| {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>()
    };
    let source = wide(source);
    let destination = wide(destination);
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(io_err(std::io::Error::last_os_error()))
    } else {
        Ok(())
    }
}

fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<(), BackupServiceError> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io_err)?;
    let result = set_file_owner_only(path)
        .and_then(|()| file.write_all(bytes).map_err(io_err))
        .and_then(|()| file.sync_all().map_err(io_err));
    if result.is_err() {
        drop(file);
        let _ = std::fs::remove_file(path);
    }
    result
}

fn set_file_owner_only(path: &Path) -> Result<(), BackupServiceError> {
    permissions::set_file_owner_only(path).map_err(io_err)
}

fn set_directory_owner_only(path: &Path) -> Result<(), BackupServiceError> {
    permissions::set_directory_owner_only(path).map_err(io_err)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), BackupServiceError> {
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_err)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), BackupServiceError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_location_pointer_wins_over_an_incomplete_direct_staging_directory() {
        let launch = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap();
        std::fs::create_dir(launch.path().join(PENDING_RESTORE_DIR)).unwrap();
        let pending = staged.path().join(PENDING_RESTORE_DIR);
        std::fs::create_dir(&pending).unwrap();
        std::fs::write(pending.join(READY_MARKER), b"ready").unwrap();
        std::fs::write(
            launch.path().join(LOCATION_POINTER),
            format!("{}\n", pending.display()),
        )
        .unwrap();

        assert_eq!(pending_restore_path(launch.path()), pending);
    }

    #[test]
    fn location_pointer_never_overwrites_an_existing_restore_reservation() {
        let target = tempfile::tempdir().unwrap();
        let pointer = target.path().join(LOCATION_POINTER);
        std::fs::write(&pointer, b"/existing/restore-pending\n").unwrap();

        assert!(write_location_pointer(target.path(), Path::new("/new/restore-pending")).is_err());
        assert_eq!(
            std::fs::read_to_string(pointer).unwrap(),
            "/existing/restore-pending\n"
        );
    }
}
