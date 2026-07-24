use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::sync::Mutex;

use super::archive::{
    ManagedPackageSource, is_bundle_encrypted, maybe_decrypt_archive, unpack_bundle_archive,
    unpack_plain_archive, write_bundle_archive,
};
use super::logical::{
    read_table_objects, validate_legacy_encryption_key, verify_table_parts, visit_table_objects,
};
use super::manifest::{
    BackupArtifact, BackupInspectResult, BackupInstanceSecrets, BackupManifest, BackupServiceError,
    BackupSourcePaths, BackupStatus, CategoryRemapRequirement, ManagedPackageInventory,
    RestoreOptions, RestoreReport, build_bundle_manifest, io_err, required_category_remaps,
    validate_manifest,
};
use super::pending::{PendingRestoreMetadata, pending_restore_status, stage_pending_restore};
use super::restore::{
    normalize_restore_options, rewrite_backup_db_for_restore, rewrite_logical_bundle_for_restore,
};
use crate::post_processing::discovery::{copy_package, hash_package, package_files};
use crate::rss::RssService;
use crate::settings::{Config, SharedConfig};
use crate::{Database, SchedulerHandle};

#[derive(Clone)]
pub struct BackupService {
    inner: Arc<BackupServiceInner>,
}

struct BackupServiceInner {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    _rss: RssService,
    restore_locator_dir: PathBuf,
    op_lock: Mutex<()>,
}

enum LoadedBackupKind {
    Logical {
        secrets: BackupInstanceSecrets,
    },
    Legacy {
        backup_db_path: PathBuf,
        source_config: Box<Config>,
    },
}

struct LoadedBackup {
    _temp_dir: TempDir,
    root: PathBuf,
    manifest: BackupManifest,
    kind: LoadedBackupKind,
    required_category_remaps: Vec<CategoryRemapRequirement>,
    warnings: Vec<String>,
}

impl BackupService {
    pub fn new(
        handle: SchedulerHandle,
        config: SharedConfig,
        db: Database,
        rss: RssService,
        restore_locator_dir: PathBuf,
    ) -> Self {
        Self {
            inner: Arc::new(BackupServiceInner {
                db,
                handle,
                config,
                _rss: rss,
                restore_locator_dir,
                op_lock: Mutex::new(()),
            }),
        }
    }

    pub fn status(&self) -> Result<BackupStatus, BackupServiceError> {
        let busy = self.inner.op_lock.try_lock().is_err();
        let pristine = self
            .inner
            .db
            .restore_target_is_pristine()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        let current_data_dir = self
            .inner
            .config
            .try_read()
            .map(|config| PathBuf::from(&config.data_dir))
            .unwrap_or_default();
        let pending = pending_restore_status(&current_data_dir);
        let pending_restore = pending.as_ref().map(|pending| pending.restore_id.clone());
        let pending_restore_error = pending.and_then(|pending| pending.error);
        Ok(BackupStatus {
            can_restore: pristine && !busy && pending_restore.is_none(),
            busy,
            reason: if busy {
                Some("backup or restore already in progress".into())
            } else if pending_restore.is_some() {
                Some("a validated restore is staged; restart Weaver to apply it".into())
            } else if !pristine {
                Some("restore requires an instance with no active jobs or job history".into())
            } else {
                None
            },
            pending_restore,
            pending_restore_error,
        })
    }

    pub async fn create_backup(
        &self,
        password: Option<String>,
    ) -> Result<BackupArtifact, BackupServiceError> {
        let password = password
            .filter(|value| !value.trim().is_empty())
            .ok_or(BackupServiceError::PasswordRequired)?;
        let _guard = self.inner.op_lock.lock().await;

        self.inner
            .db
            .flush_write_queue()
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;

        if let Some(policy) = self.inner.handle.server_transfer_policy() {
            tokio::task::spawn_blocking(move || policy.flush_usage())
                .await
                .map_err(|error| BackupServiceError::Io(error.to_string()))?
                .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        }

        let db = self.inner.db.clone();
        let export = tokio::task::spawn_blocking(move || db.export_logical_backup())
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))?
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        let fallback_config = self.inner.config.read().await.clone();
        let export_root = export.staging.path().to_path_buf();
        let (source_paths, managed_packages) = tokio::task::spawn_blocking(move || {
            Ok::<_, BackupServiceError>((
                source_paths_from_export(&export_root, &fallback_config)?,
                managed_package_sources_from_export(&export_root)?,
            ))
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))??;

        let key = self
            .inner
            .db
            .encryption_key()
            .ok_or_else(|| {
                BackupServiceError::Validation(
                    "an encryption master key is required to create a backup".into(),
                )
            })?
            .clone();
        let secrets = BackupInstanceSecrets {
            encryption_master_key: key.to_base64(),
            key_source: crate::persistence::encryption::backup_key_source_name(
                Some(PathBuf::from(&source_paths.data_dir)),
                &key,
            ),
        };
        let secrets_path = export.staging.path().join("instance-secrets.json");
        write_json(&secrets_path, &secrets)?;
        let secrets_checksum = checksum_hex(&secrets_path)?;
        let manifest = build_bundle_manifest(
            source_paths,
            &export,
            managed_packages
                .iter()
                .map(|package| package.inventory.clone())
                .collect(),
            secrets_checksum,
        );
        write_json(&export.staging.path().join("manifest.json"), &manifest)?;

        let artifact_dir = super::create_backup_temp_dir().map_err(io_err)?;
        let timestamp = manifest.created_at_epoch_ms / 1000;
        let filename = format!("weaver_backup_{timestamp}.enc");
        let path = artifact_dir.path().join(&filename);
        let staging = export.staging.path().to_path_buf();
        let output = path.clone();
        tokio::task::spawn_blocking(move || {
            write_bundle_archive(&output, &password, &staging, &managed_packages)
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))?
        .map_err(io_err)?;

        Ok(BackupArtifact {
            filename,
            path,
            _temp_dir: artifact_dir,
        })
    }

    pub async fn inspect_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
    ) -> Result<BackupInspectResult, BackupServiceError> {
        let target_data_dir = PathBuf::from(&self.inner.config.read().await.data_dir);
        self.inspect_backup_for_target(archive_path, password, &target_data_dir)
            .await
    }

    pub async fn inspect_backup_for_target(
        &self,
        archive_path: &Path,
        password: Option<String>,
        target_data_dir: &Path,
    ) -> Result<BackupInspectResult, BackupServiceError> {
        let _guard = self.inner.op_lock.lock().await;
        let loaded = self.load_backup(archive_path, password).await?;
        let key_compatible = match &loaded.kind {
            LoadedBackupKind::Logical { secrets } => {
                crate::persistence::encryption::validate_restore_encryption_key(
                    Some(target_data_dir.to_path_buf()),
                    &secrets.encryption_master_key,
                )
                .is_ok()
            }
            LoadedBackupKind::Legacy { .. } => self.inner.db.encryption_key().is_some_and(|key| {
                crate::persistence::encryption::validate_restore_encryption_key(
                    Some(target_data_dir.to_path_buf()),
                    &key.to_base64(),
                )
                .is_ok()
            }),
        };
        Ok(BackupInspectResult {
            required_category_remaps: loaded.required_category_remaps,
            manifest: loaded.manifest,
            key_compatible,
            warnings: loaded.warnings,
        })
    }

    pub async fn restore_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
        options: RestoreOptions,
    ) -> Result<RestoreReport, BackupServiceError> {
        let _guard = self.inner.op_lock.lock().await;
        let options = normalize_restore_options(options)?;
        if !self
            .inner
            .db
            .restore_target_is_pristine()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?
        {
            return Err(BackupServiceError::NotPristine);
        }
        let mut loaded = self.load_backup(archive_path, password).await?;
        let current_data_dir = PathBuf::from(&self.inner.config.read().await.data_dir);
        let restore_id = format!("{}-{}", super::manifest::epoch_ms_now(), std::process::id());
        let legacy = matches!(loaded.kind, LoadedBackupKind::Legacy { .. });
        match &loaded.kind {
            LoadedBackupKind::Logical { secrets } => {
                crate::persistence::encryption::validate_restore_encryption_key(
                    Some(PathBuf::from(&options.data_dir)),
                    &secrets.encryption_master_key,
                )
                .map_err(BackupServiceError::KeyIncompatible)?;
                let root = loaded.root.clone();
                let mut manifest = loaded.manifest.clone();
                let rewrite_options = options.clone();
                loaded.manifest = tokio::task::spawn_blocking(move || {
                    rewrite_logical_bundle_for_restore(&root, &mut manifest, &rewrite_options)?;
                    Ok::<_, BackupServiceError>(manifest)
                })
                .await
                .map_err(|error| BackupServiceError::Io(error.to_string()))??;
                let restored_key = crate::persistence::encryption::EncryptionKey::from_base64(
                    &secrets.encryption_master_key,
                )
                .map_err(BackupServiceError::Validation)?;
                validate_logical_database(&loaded.root, &loaded.manifest, restored_key).await?;
            }
            LoadedBackupKind::Legacy {
                backup_db_path,
                source_config,
            } => {
                let active_key = self.inner.db.encryption_key().ok_or_else(|| {
                    BackupServiceError::KeyIncompatible(
                        "legacy restore requires the active instance encryption key".into(),
                    )
                })?;
                crate::persistence::encryption::validate_restore_encryption_key(
                    Some(PathBuf::from(&options.data_dir)),
                    &active_key.to_base64(),
                )
                .map_err(BackupServiceError::KeyIncompatible)?;
                let backup_db_path = backup_db_path.clone();
                let source_config = source_config.as_ref().clone();
                let rewrite_options = options.clone();
                tokio::task::spawn_blocking(move || {
                    rewrite_backup_db_for_restore(&backup_db_path, &source_config, &rewrite_options)
                })
                .await
                .map_err(|error| BackupServiceError::Io(error.to_string()))??;
            }
        }
        if !self
            .inner
            .db
            .restore_target_is_pristine()
            .map_err(|error| BackupServiceError::Io(error.to_string()))?
        {
            return Err(BackupServiceError::NotPristine);
        }
        let staged_root = loaded.root.clone();
        let mut pending = PendingRestoreMetadata {
            restore_id: restore_id.clone(),
            legacy,
            current_data_dir: current_data_dir.display().to_string(),
            restore_locator_dir: self.inner.restore_locator_dir.display().to_string(),
            legacy_key_fingerprint: if legacy {
                self.inner
                    .db
                    .encryption_key()
                    .map(super::pending::encryption_key_fingerprint)
            } else {
                None
            },
            legacy_backup_checksum: None,
            options: options.clone(),
        };
        tokio::task::spawn_blocking(move || {
            if legacy {
                pending.legacy_backup_checksum = Some(super::pending::file_checksum(
                    &staged_root.join("backup.db"),
                )?);
            }
            stage_pending_restore(&staged_root, &current_data_dir, &pending)
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))??;
        let history_jobs = loaded
            .manifest
            .tables
            .get("job_history")
            .map_or(0, |metadata| metadata.rows as usize);
        Ok(RestoreReport {
            restored: false,
            staged: true,
            restart_required: true,
            pending_restore_id: Some(restore_id),
            history_jobs,
            category_remaps_applied: options.category_remaps.len(),
            managed_packages_restored: loaded.manifest.managed_packages.len(),
            warnings: loaded.warnings,
        })
    }

    async fn load_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
    ) -> Result<LoadedBackup, BackupServiceError> {
        if is_bundle_encrypted(archive_path)? {
            return self.load_logical_backup(archive_path, password).await;
        }
        self.load_legacy_backup(archive_path, password).await
    }

    async fn load_logical_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
    ) -> Result<LoadedBackup, BackupServiceError> {
        let temp_dir = super::create_backup_temp_dir().map_err(io_err)?;
        let root = temp_dir.path().join("bundle");
        std::fs::create_dir(&root).map_err(io_err)?;
        super::permissions::set_directory_owner_only(&root).map_err(io_err)?;
        let archive = archive_path.to_path_buf();
        let extraction_root = root.clone();
        let manifest = tokio::task::spawn_blocking(move || {
            unpack_bundle_archive(&archive, &extraction_root, password)
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))??;
        if !manifest.format_version.is_bundle_v2() {
            return Err(BackupServiceError::UnsupportedFormat(
                manifest.format_version.to_string(),
            ));
        }
        validate_manifest(&self.inner.db, &manifest)?;
        self.inner
            .db
            .validate_backup_catalog()
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let validation_root = root.clone();
        let validation_manifest = manifest.clone();
        let (secrets, restored_key, required_category_remaps) =
            tokio::task::spawn_blocking(move || {
                verify_part_checksum(
                    &validation_root,
                    &validation_manifest,
                    "instance-secrets.json",
                )?;
                let secrets: BackupInstanceSecrets = serde_json::from_slice(
                    &std::fs::read(validation_root.join("instance-secrets.json"))
                        .map_err(io_err)?,
                )
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
                let restored_key = crate::persistence::encryption::EncryptionKey::from_base64(
                    &secrets.encryption_master_key,
                )
                .map_err(BackupServiceError::Validation)?;
                validate_extracted_packages(&validation_root, &validation_manifest)?;
                validate_managed_references(&validation_root, &validation_manifest)?;
                let remaps = category_remaps_from_logical(
                    &validation_root,
                    &validation_manifest.source_paths,
                )?;
                Ok::<_, BackupServiceError>((secrets, restored_key, remaps))
            })
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))??;
        validate_logical_database(&root, &manifest, restored_key).await?;
        Ok(LoadedBackup {
            _temp_dir: temp_dir,
            root,
            manifest,
            kind: LoadedBackupKind::Logical { secrets },
            required_category_remaps,
            warnings: Vec::new(),
        })
    }

    async fn load_legacy_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
    ) -> Result<LoadedBackup, BackupServiceError> {
        let temp_dir = super::create_backup_temp_dir().map_err(io_err)?;
        let root = temp_dir.path().join("legacy");
        let archive = archive_path.to_path_buf();
        let work_dir = temp_dir.path().to_path_buf();
        let extraction_root = root.clone();
        let manifest = tokio::task::spawn_blocking(move || {
            let extracted = maybe_decrypt_archive(&archive, password, &work_dir)?;
            std::fs::create_dir(&extraction_root).map_err(io_err)?;
            super::permissions::set_directory_owner_only(&extraction_root).map_err(io_err)?;
            unpack_plain_archive(&extracted, &extraction_root)
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))??;
        if !manifest.format_version.is_legacy() {
            return Err(BackupServiceError::UnsupportedFormat(
                manifest.format_version.to_string(),
            ));
        }
        validate_manifest(&self.inner.db, &manifest)?;
        let backup_db_path = root.join("backup.db");
        validate_legacy_encryption_key(&backup_db_path, self.inner.db.encryption_key())
            .await
            .map_err(|error| BackupServiceError::KeyIncompatible(error.to_string()))?;
        let source_config = {
            let path = backup_db_path.clone();
            tokio::task::spawn_blocking(move || {
                let db = Database::open(&path)
                    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
                db.load_config()
                    .map_err(|error| BackupServiceError::Validation(error.to_string()))
            })
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))??
        };
        let required_category_remaps = required_category_remaps(&source_config);
        Ok(LoadedBackup {
            _temp_dir: temp_dir,
            root,
            manifest,
            kind: LoadedBackupKind::Legacy {
                backup_db_path,
                source_config: Box::new(source_config),
            },
            required_category_remaps,
            warnings: vec![
                "legacy v1 backup contains no source master key or executable extension packages"
                    .into(),
            ],
        })
    }
}

async fn validate_logical_database(
    root: &Path,
    manifest: &BackupManifest,
    restored_key: crate::persistence::encryption::EncryptionKey,
) -> Result<(), BackupServiceError> {
    verify_table_parts(root, &manifest.tables)
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let validation_tables = root.join("tables");
    let validation_manifest = manifest.clone();
    tokio::task::spawn_blocking(move || {
        let validation_dir = super::create_backup_temp_dir().map_err(io_err)?;
        let validation_path = validation_dir.path().join("validation.db");
        let mut validation_db = Database::open(&validation_path)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        super::permissions::set_file_owner_only(&validation_path).map_err(io_err)?;
        validation_db.set_encryption_key(restored_key.clone());
        let validation_result = (|| {
            validation_db
                .import_logical_backup(
                    &validation_tables,
                    &validation_manifest.tables,
                    validation_manifest.weaver_schema_version,
                )
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
            validation_db
                .validate_encrypted_credentials(&restored_key)
                .map_err(|error| {
                    BackupServiceError::Validation(format!(
                        "backup data does not match its encryption master key: {error}"
                    ))
                })?;
            validation_db
                .load_config()
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?
                .validate()
                .map_err(|errors| BackupServiceError::Validation(errors.join("; ")))
        })();
        let close_result = validation_db
            .close()
            .map_err(|error| BackupServiceError::Io(error.to_string()));
        validation_result?;
        close_result
    })
    .await
    .map_err(|error| BackupServiceError::Io(error.to_string()))?
}

fn source_paths_from_export(
    root: &Path,
    fallback: &Config,
) -> Result<BackupSourcePaths, BackupServiceError> {
    let settings = read_table_objects(root, "settings")
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?
        .into_iter()
        .filter_map(|row| {
            Some((
                row.get("key")?.as_str()?.to_string(),
                row.get("value")?.as_str()?.to_string(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let data_dir = settings
        .get("data_dir")
        .cloned()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.data_dir.clone());
    let intermediate_dir = settings
        .get("intermediate_dir")
        .cloned()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            Path::new(&data_dir)
                .join("intermediate")
                .display()
                .to_string()
        });
    let complete_dir = settings
        .get("complete_dir")
        .cloned()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| Path::new(&data_dir).join("complete").display().to_string());
    Ok(BackupSourcePaths {
        data_dir,
        intermediate_dir,
        complete_dir,
    })
}

fn managed_package_sources_from_export(
    root: &Path,
) -> Result<Vec<ManagedPackageSource>, BackupServiceError> {
    let mut packages = BTreeMap::<String, PathBuf>::new();
    visit_table_objects(root, "post_processing_extension_revisions", |row| {
        let Some(path) = row.get("managed_path").and_then(JsonValue::as_str) else {
            return Ok(());
        };
        let digest = row
            .get("digest")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| crate::StateError::Database("managed revision has no digest".into()))?
            .to_string();
        if let Some(existing) = packages.insert(digest.clone(), PathBuf::from(path))
            && existing != Path::new(path)
        {
            return Err(crate::StateError::Database(format!(
                "managed package {digest} has conflicting paths"
            )));
        }
        Ok(())
    })
    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    packages
        .into_iter()
        .map(|(digest, path)| {
            let hex = digest
                .strip_prefix("blake3:")
                .ok_or_else(|| {
                    BackupServiceError::Validation("managed package digest must use blake3".into())
                })?
                .to_owned();
            let staged = root.join("managed-extensions/blake3").join(&hex);
            if let Some(parent) = staged.parent() {
                std::fs::create_dir_all(parent).map_err(io_err)?;
            }
            copy_package(&path, &staged)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
            let actual = hash_package(&staged)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
            if actual.as_str() != digest {
                let _ = std::fs::remove_dir_all(&staged);
                return Err(BackupServiceError::Validation(format!(
                    "managed package {digest} changed while the backup snapshot was prepared"
                )));
            }
            let files = package_files(&staged)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
            let uncompressed_bytes = files.iter().try_fold(0_u64, |total, (_, file)| {
                total
                    .checked_add(std::fs::metadata(file).map_err(io_err)?.len())
                    .ok_or_else(|| {
                        BackupServiceError::Validation("managed package is too large".into())
                    })
            })?;
            Ok(ManagedPackageSource {
                inventory: ManagedPackageInventory {
                    digest,
                    archive_prefix: format!("managed-extensions/blake3/{hex}"),
                    file_count: files.len(),
                    uncompressed_bytes,
                },
                path: staged,
            })
        })
        .collect()
}

fn validate_extracted_packages(
    root: &Path,
    manifest: &BackupManifest,
) -> Result<(), BackupServiceError> {
    for package in &manifest.managed_packages {
        let path = root.join(&package.archive_prefix);
        let files = package_files(&path)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        let bytes = files.iter().try_fold(0_u64, |total, (_, path)| {
            total
                .checked_add(std::fs::metadata(path).map_err(io_err)?.len())
                .ok_or_else(|| BackupServiceError::Validation("package is too large".into()))
        })?;
        if files.len() != package.file_count || bytes != package.uncompressed_bytes {
            return Err(BackupServiceError::Validation(format!(
                "managed package {} does not match its inventory",
                package.digest
            )));
        }
        let actual = hash_package(&path)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        if actual.as_str() != package.digest {
            return Err(BackupServiceError::Validation(format!(
                "managed package {} failed digest verification",
                package.digest
            )));
        }
    }
    Ok(())
}

fn validate_managed_references(
    root: &Path,
    manifest: &BackupManifest,
) -> Result<(), BackupServiceError> {
    let mut referenced = BTreeSet::new();
    visit_table_objects(root, "post_processing_extension_revisions", |row| {
        if row
            .get("managed_path")
            .is_none_or(serde_json::Value::is_null)
        {
            return Ok(());
        }
        let digest = row
            .get("digest")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| crate::StateError::Database("managed revision has no digest".into()))?;
        referenced.insert(digest.to_string());
        Ok(())
    })
    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
    let declared = manifest
        .managed_packages
        .iter()
        .map(|package| package.digest.clone())
        .collect::<BTreeSet<_>>();
    if referenced == declared {
        Ok(())
    } else {
        Err(BackupServiceError::Validation(
            "managed package inventory does not match revision references".into(),
        ))
    }
}

fn category_remaps_from_logical(
    root: &Path,
    source_paths: &BackupSourcePaths,
) -> Result<Vec<CategoryRemapRequirement>, BackupServiceError> {
    let complete = &source_paths.complete_dir;
    let mut remaps = read_table_objects(root, "categories")
        .map_err(|error| BackupServiceError::Validation(error.to_string()))?
        .into_iter()
        .filter_map(|row| {
            let name = row.get("name")?.as_str()?.to_string();
            let destination = row.get("dest_dir")?.as_str()?.to_string();
            (!super::restore::stored_path_has_prefix(&destination, complete)).then_some(
                CategoryRemapRequirement {
                    category_name: name,
                    current_dest_dir: destination,
                },
            )
        })
        .collect::<Vec<_>>();
    remaps.sort_by(|left, right| left.category_name.cmp(&right.category_name));
    Ok(remaps)
}

fn verify_part_checksum(
    root: &Path,
    manifest: &BackupManifest,
    relative: &str,
) -> Result<(), BackupServiceError> {
    let expected = manifest.part_checksums.get(relative).ok_or_else(|| {
        BackupServiceError::Validation(format!("manifest has no checksum for {relative}"))
    })?;
    let actual = checksum_hex(&root.join(relative))?;
    if &actual == expected {
        Ok(())
    } else {
        Err(BackupServiceError::Validation(format!(
            "{relative} failed checksum validation"
        )))
    }
}

fn checksum_hex(path: &Path) -> Result<String, BackupServiceError> {
    let mut file = std::fs::File::open(path).map_err(io_err)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut file, &mut hasher).map_err(io_err)?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<(), BackupServiceError> {
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?,
    )
    .map_err(io_err)
}
