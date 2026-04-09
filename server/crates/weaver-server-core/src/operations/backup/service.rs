use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::Mutex;

use super::archive::{
    encrypt_archive, maybe_decrypt_archive, unpack_plain_archive, write_plain_archive,
};
use super::manifest::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupServiceError, BackupStatus,
    RestoreOptions, RestoreReport, build_manifest, io_err, required_category_remaps,
    validate_manifest,
};
use super::restore::{load_history_snapshot, rewrite_backup_db_for_restore};
use crate::rss::RssService;
use crate::runtime::reload_runtime_from_db;
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
    rss: RssService,
    op_lock: Mutex<()>,
}

struct LoadedBackup {
    _temp_dir: TempDir,
    manifest: BackupManifest,
    backup_db_path: PathBuf,
    source_config: Config,
}

impl BackupService {
    pub fn new(
        handle: SchedulerHandle,
        config: SharedConfig,
        db: Database,
        rss: RssService,
    ) -> Self {
        Self {
            inner: Arc::new(BackupServiceInner {
                db,
                handle,
                config,
                rss,
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
            .map_err(|e| BackupServiceError::Io(e.to_string()))?;
        Ok(BackupStatus {
            can_restore: pristine && !busy,
            busy,
            reason: if busy {
                Some("backup or restore already in progress".to_string())
            } else if !pristine {
                Some("restore requires an instance with no active jobs or job history".into())
            } else {
                None
            },
        })
    }

    pub async fn create_backup(
        &self,
        password: Option<String>,
    ) -> Result<BackupArtifact, BackupServiceError> {
        let _guard = self.inner.op_lock.lock().await;

        let temp_dir = tempfile::tempdir().map_err(io_err)?;
        let backup_db_path = temp_dir.path().join("backup.db");
        let export = {
            let db = self.inner.db.clone();
            let backup_db_path = backup_db_path.clone();
            tokio::task::spawn_blocking(move || db.export_stable_state(&backup_db_path))
                .await
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
        };

        let source_config = {
            let db = self.inner.db.clone();
            tokio::task::spawn_blocking(move || db.load_config())
                .await
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
        };

        let encrypted = password.as_ref().is_some_and(|p| !p.is_empty());
        let manifest = build_manifest(&source_config, &export, encrypted);
        let plain_archive_path = temp_dir.path().join("backup.tar.zst");
        let final_archive_path = if encrypted {
            temp_dir.path().join("backup.tar.zst.age")
        } else {
            plain_archive_path.clone()
        };

        let manifest_clone = manifest.clone();
        let backup_db_clone = backup_db_path.clone();
        let plain_archive_clone = plain_archive_path.clone();
        tokio::task::spawn_blocking(move || {
            write_plain_archive(&plain_archive_clone, &manifest_clone, &backup_db_clone)
        })
        .await
        .map_err(|e| BackupServiceError::Io(e.to_string()))?
        .map_err(io_err)?;

        if let Some(password) = password.filter(|value| !value.is_empty()) {
            let plain_archive_clone = plain_archive_path.clone();
            let final_archive_clone = final_archive_path.clone();
            tokio::task::spawn_blocking(move || {
                encrypt_archive(&plain_archive_clone, &final_archive_clone, &password)
            })
            .await
            .map_err(|e| BackupServiceError::Io(e.to_string()))?
            .map_err(io_err)?;
        }

        let bytes = tokio::fs::read(&final_archive_path).await.map_err(io_err)?;
        let timestamp = manifest.created_at_epoch_ms / 1000;
        let filename = if encrypted {
            format!("weaver_backup_{timestamp}.tar.zst.age")
        } else {
            format!("weaver_backup_{timestamp}.tar.zst")
        };

        Ok(BackupArtifact { filename, bytes })
    }

    pub async fn inspect_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
    ) -> Result<BackupInspectResult, BackupServiceError> {
        let _guard = self.inner.op_lock.lock().await;
        let loaded = self.load_backup(archive_path, password).await?;
        Ok(BackupInspectResult {
            required_category_remaps: required_category_remaps(&loaded.source_config),
            manifest: loaded.manifest,
        })
    }

    pub async fn restore_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
        options: RestoreOptions,
    ) -> Result<RestoreReport, BackupServiceError> {
        let _guard = self.inner.op_lock.lock().await;

        let pristine = {
            let db = self.inner.db.clone();
            tokio::task::spawn_blocking(move || db.restore_target_is_pristine())
                .await
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
        };
        if !pristine {
            return Err(BackupServiceError::NotPristine);
        }

        let loaded = self.load_backup(archive_path, password).await?;
        let remap_count = options.category_remaps.len();
        let backup_db_path = loaded.backup_db_path.clone();
        let source_config = loaded.source_config.clone();
        let options_clone = options.clone();
        tokio::task::spawn_blocking(move || {
            rewrite_backup_db_for_restore(&backup_db_path, &source_config, &options_clone)
        })
        .await
        .map_err(|e| BackupServiceError::Io(e.to_string()))??;

        {
            let db = self.inner.db.clone();
            let backup_db_path = loaded.backup_db_path.clone();
            tokio::task::spawn_blocking(move || db.import_stable_state(&backup_db_path))
                .await
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
                .map_err(|e| BackupServiceError::Io(e.to_string()))?;
        }

        let loaded_config =
            reload_runtime_from_db(&self.inner.config, &self.inner.handle, &self.inner.db)
                .await
                .map_err(BackupServiceError::Validation)?;

        self.inner
            .handle
            .update_runtime_paths(
                PathBuf::from(&loaded_config.data_dir),
                PathBuf::from(loaded_config.intermediate_dir()),
                PathBuf::from(loaded_config.complete_dir()),
            )
            .await
            .map_err(|e| BackupServiceError::Io(e.to_string()))?;

        self.inner.rss.reload_state().await;

        let history_jobs = load_history_snapshot(&self.inner.db).await?;
        let history_count = history_jobs.len();
        self.inner.handle.replace_jobs_snapshot(history_jobs);

        let max_job_id = {
            let db = self.inner.db.clone();
            tokio::task::spawn_blocking(move || db.max_job_id_all())
                .await
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
                .map_err(|e| BackupServiceError::Io(e.to_string()))?
        };
        crate::ingest::init_job_counter(max_job_id + 1);

        Ok(RestoreReport {
            restored: true,
            history_jobs: history_count,
            category_remaps_applied: remap_count,
        })
    }

    async fn load_backup(
        &self,
        archive_path: &Path,
        password: Option<String>,
    ) -> Result<LoadedBackup, BackupServiceError> {
        let temp_dir = tempfile::tempdir().map_err(io_err)?;
        let extracted_archive_path =
            maybe_decrypt_archive(archive_path, password, temp_dir.path())?;
        let unpack_dir = temp_dir.path().join("unpacked");
        std::fs::create_dir_all(&unpack_dir).map_err(io_err)?;
        unpack_plain_archive(&extracted_archive_path, &unpack_dir)?;

        let manifest_path = unpack_dir.join("manifest.json");
        let manifest_bytes = std::fs::read(&manifest_path).map_err(io_err)?;
        let manifest: BackupManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        validate_manifest(&self.inner.db, &manifest)?;

        let backup_db_path = unpack_dir.join("backup.db");
        if !backup_db_path.exists() {
            return Err(BackupServiceError::Validation(
                "backup archive is missing backup.db".into(),
            ));
        }

        let source_config = {
            let backup_db_path = backup_db_path.clone();
            tokio::task::spawn_blocking(move || {
                let db = Database::open(&backup_db_path)?;
                db.load_config()
            })
            .await
            .map_err(|e| BackupServiceError::Io(e.to_string()))?
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?
        };

        Ok(LoadedBackup {
            _temp_dir: temp_dir,
            manifest,
            backup_db_path,
            source_config,
        })
    }
}
