use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::Mutex;

use super::archive::{
    is_bundle_encrypted, maybe_decrypt_archive, unpack_bundle_archive, unpack_plain_archive,
};
use super::logical::{read_table_objects, validate_legacy_encryption_key, verify_table_parts};
use super::manifest::{
    BackupArtifact, BackupInspectResult, BackupInstanceSecrets, BackupManifest, BackupServiceError,
    BackupSourcePaths, BackupStatus, CategoryRemapRequirement, RestoreOptions, RestoreReport,
    build_bundle_manifest, io_err, required_category_remaps, validate_manifest,
};
use super::pending::{PendingRestoreMetadata, pending_restore_status, stage_pending_restore};
use super::restore::{
    normalize_restore_options, rewrite_backup_db_for_restore, rewrite_logical_bundle_for_restore,
};
use crate::rss::RssService;
use crate::settings::{Config, SharedConfig};
use crate::{Database, SchedulerHandle};

#[derive(Clone)]
pub struct BackupService {
    pub(super) inner: Arc<BackupServiceInner>,
}

pub(super) struct BackupServiceInner {
    pub(super) db: Database,
    handle: SchedulerHandle,
    pub(super) config: SharedConfig,
    _rss: RssService,
    restore_locator_dir: PathBuf,
    pub(super) op_lock: Arc<Mutex<()>>,
    pub(super) manual_lock: Arc<Mutex<()>>,
    pub(super) auto_lock: Arc<Mutex<()>>,
    pub(super) shutting_down: std::sync::atomic::AtomicBool,
    pub(super) shutdown_signal: super::archive::BackupCancellation,
    pub(super) settings_lock: Mutex<()>,
    pub(super) settings_changed: tokio::sync::watch::Sender<u64>,
    pub(super) download_tokens: Mutex<BTreeMap<String, (String, tokio::time::Instant)>>,
    pub(super) next_run: std::sync::RwLock<Option<String>>,
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

/// Cancellation is cooperative: retain and drain the future so its blocking writers
/// have exited before reporting failure or releasing the execution lock.
pub(super) async fn run_backup_work<T>(
    work: impl std::future::Future<Output = Result<T, BackupServiceError>>,
    cancellation: super::archive::BackupCancellation,
    shutdown: super::archive::BackupCancellation,
) -> Result<T, BackupServiceError> {
    tokio::pin!(work);
    let error = tokio::select! {
        result = tokio::time::timeout(super::stored::BACKUP_EXECUTION_TIMEOUT, &mut work) => {
            match result {
                Ok(result) => return result,
                Err(_) => BackupServiceError::Io("backup timed out".into()),
            }
        },
        _ = shutdown.cancelled() => BackupServiceError::Io("backup cancelled during shutdown".into()),
    };
    cancellation.cancel();
    let _ = work.await;
    Err(error)
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
                op_lock: Arc::new(Mutex::new(())),
                manual_lock: Arc::new(Mutex::new(())),
                auto_lock: Arc::new(Mutex::new(())),
                shutting_down: std::sync::atomic::AtomicBool::new(false),
                shutdown_signal: super::archive::BackupCancellation::new(),
                settings_lock: Mutex::new(()),
                settings_changed: tokio::sync::watch::channel(0).0,
                download_tokens: Mutex::new(BTreeMap::new()),
                next_run: std::sync::RwLock::new(None),
            }),
        }
    }

    /// Stop accepting backups, cancel and drain accepted work, including archive tasks
    /// whose HTTP request or automatic scheduler has already gone away.
    pub async fn shutdown(&self) {
        self.inner
            .shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.inner.shutdown_signal.cancel();
        let _manual = self.inner.manual_lock.lock().await;
        let _auto = self.inner.auto_lock.lock().await;
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
        let execution = Arc::new(
            self.inner
                .manual_lock
                .clone()
                .try_lock_owned()
                .map_err(|_| BackupServiceError::Busy)?,
        );
        if self
            .inner
            .shutting_down
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(BackupServiceError::Validation(
                "backup service is shutting down".into(),
            ));
        }
        let temporary_directory = super::create_backup_temp_dir().map_err(io_err)?;
        let info = super::stored::new_backup_info(
            super::stored::BackupTrigger::Manual,
            self.inner.db.engine_name(),
            env!("CARGO_PKG_VERSION"),
        )?;
        let path = temporary_directory.path().join(&info.filename);
        let service = self.clone();
        // Keep the temporary directory and execution guard alive if the request disconnects.
        tokio::spawn(async move {
            let cancellation = super::archive::BackupCancellation::new();
            run_backup_work(
                service.write_backup(password, path.clone(), execution, cancellation.clone()),
                cancellation,
                service.inner.shutdown_signal.clone(),
            )
            .await?;
            Ok(BackupArtifact {
                filename: info.filename,
                path,
                temporary_directory: Some(temporary_directory),
            })
        })
        .await
        .map_err(io_err)?
    }

    pub(super) async fn write_backup(
        &self,
        password: String,
        path: PathBuf,
        execution: Arc<tokio::sync::OwnedMutexGuard<()>>,
        cancellation: super::archive::BackupCancellation,
    ) -> Result<BackupManifest, BackupServiceError> {
        self.inner
            .db
            .flush_write_queue()
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;

        if let Some(policy) = self.inner.handle.server_transfer_policy() {
            let flush_execution = execution.clone();
            tokio::task::spawn_blocking(move || {
                let _execution = flush_execution;
                policy.flush_usage()
            })
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))?
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        }

        let db = self.inner.db.clone();
        let export_execution = execution.clone();
        let export_cancellation = cancellation.clone();
        let export = tokio::task::spawn_blocking(move || {
            let _execution = export_execution;
            db.export_logical_backup_cancellable(export_cancellation)
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))?
        .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        let fallback_config = self.inner.config.read().await.clone();
        let export_root = export.staging.path().to_path_buf();
        let paths_execution = execution.clone();
        let source_paths = tokio::task::spawn_blocking(move || {
            let _execution = paths_execution;
            source_paths_from_export(&export_root, &fallback_config.data_dir)
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
        let manifest = build_bundle_manifest(source_paths, &export, secrets_checksum);
        write_json(&export.staging.path().join("manifest.json"), &manifest)?;

        let guard = self.inner.op_lock.clone().lock_owned().await;
        let staging = export.staging;
        let output = path.clone();
        tokio::task::spawn_blocking(move || {
            let (_execution, _guard) = (execution, guard);
            super::archive::write_bundle_archive_cancellable(
                &output,
                &password,
                staging.path(),
                cancellation,
            )
        })
        .await
        .map_err(|error| BackupServiceError::Io(error.to_string()))?
        .map_err(io_err)?;

        Ok(manifest)
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
                let remaps = category_remaps_from_logical(
                    &validation_root,
                    &validation_manifest.source_paths,
                )?;
                Ok::<_, BackupServiceError>((secrets, restored_key, remaps))
            })
            .await
            .map_err(|error| BackupServiceError::Io(error.to_string()))??;
        validate_logical_database(&root, &manifest, restored_key).await?;
        let mut warnings: Vec<String> = manifest.legacy_package_warning().into_iter().collect();
        for row in read_table_objects(&root, "settings").map_err(io_err)? {
            match row.get("key").and_then(serde_json::Value::as_str) {
                Some(super::automatic::AUTO_SETTINGS_KEY) => warnings.push("Restore carries the source automatic-backup schedule and encrypted automatic-backup key.".into()),
                Some(super::stored::STORAGE_KEY) if row.get("value").and_then(serde_json::Value::as_str).is_some_and(|value| !value.is_empty()) => warnings.push("Restore carries the source custom backup path; review it on this host before the next backup.".into()),
                _ => {}
            }
        }
        Ok(LoadedBackup {
            _temp_dir: temp_dir,
            root,
            manifest,
            kind: LoadedBackupKind::Logical { secrets },
            required_category_remaps,
            warnings,
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

pub(super) fn source_paths_from_export(
    root: &Path,
    fallback_data_dir: &str,
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
        .unwrap_or_else(|| fallback_data_dir.to_owned());
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

pub(super) fn checksum_hex(path: &Path) -> Result<String, BackupServiceError> {
    let mut file = std::fs::File::open(path).map_err(io_err)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut file, &mut hasher).map_err(io_err)?;
    Ok(hasher.finalize().to_hex().to_string())
}

pub(super) fn write_json(
    path: &Path,
    value: &impl serde::Serialize,
) -> Result<(), BackupServiceError> {
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?,
    )
    .map_err(io_err)
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_drains_writer_before_reporting_failure() {
        let cancellation = super::super::archive::BackupCancellation::new();
        let shutdown = super::super::archive::BackupCancellation::new();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let (done, mut done_rx) = tokio::sync::oneshot::channel();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("writer-finished");
        let writer_path = path.clone();
        let work = async move {
            tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                std::fs::write(writer_path, b"finished").unwrap();
            })
            .await
            .unwrap();
            Ok(())
        };
        let task_cancellation = cancellation.clone();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let result = run_backup_work(work, task_cancellation, task_shutdown).await;
            done.send(()).unwrap();
            result
        });
        started_rx.await.unwrap();
        shutdown.cancel();
        cancellation.cancelled().await;
        assert!(matches!(
            done_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("shutdown"));
        assert_eq!(std::fs::read(path).unwrap(), b"finished");
    }
}
