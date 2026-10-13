use super::manifest::{BACKUP_FORMAT_VERSION, io_err};
use super::{BackupArtifact, BackupService, BackupServiceError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const BACKUP_EXECUTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);
pub(super) const STORAGE_KEY: &str = "backup.custom_path";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupTrigger {
    Manual,
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupArtifactStatus {
    Creating,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    pub filename: String,
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub format_version: String,
    pub source_weaver_version: String,
    pub source_engine: String,
    pub encrypted: bool,
    pub row_counts: BTreeMap<String, u64>,
    pub trigger: BackupTrigger,
    pub status: BackupArtifactStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupSettings {
    pub custom_backup_path: Option<String>,
    pub backup_path: String,
}

pub(super) fn new_backup_info(
    trigger: BackupTrigger,
    source_engine: &str,
    version: &str,
) -> Result<BackupInfo, BackupServiceError> {
    let now = crate::e2e_clock::utc_now();
    let mut suffix = [0u8; 8];
    getrandom::fill(&mut suffix).map_err(io_err)?;
    let suffix = suffix
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(BackupInfo {
        filename: format!("weaver_backup_{}_{suffix}.enc", now.format("%Y%m%d_%H%M%S")),
        size_bytes: 0,
        created_at: now,
        format_version: BACKUP_FORMAT_VERSION.into(),
        source_weaver_version: version.into(),
        source_engine: source_engine.into(),
        encrypted: true,
        row_counts: BTreeMap::new(),
        trigger,
        status: BackupArtifactStatus::Creating,
        error: None,
    })
}

pub(super) fn valid_filename(filename: &str) -> bool {
    filename.starts_with("weaver_backup_")
        && filename.ends_with(".enc")
        && filename.len() <= 128
        && filename
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        && !filename.contains("..")
}

pub(super) fn write_metadata(dir: &Path, info: &BackupInfo) -> Result<(), BackupServiceError> {
    if !valid_filename(&info.filename) {
        return Err(BackupServiceError::Validation(
            "invalid backup filename".into(),
        ));
    }
    let mut temp = tempfile::Builder::new()
        .prefix(".backup-metadata-")
        .tempfile_in(dir)
        .map_err(io_err)?;
    super::permissions::set_file_owner_only(temp.path()).map_err(io_err)?;
    serde_json::to_writer_pretty(&mut temp, info).map_err(io_err)?;
    temp.flush().map_err(io_err)?;
    temp.as_file().sync_all().map_err(io_err)?;
    temp.persist(dir.join(format!("{}.metadata.json", info.filename)))
        .map_err(io_err)?;
    #[cfg(unix)]
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(io_err)?;
    Ok(())
}

pub(super) fn validate_storage_dir(path: &Path) -> Result<(), BackupServiceError> {
    if !path.is_absolute() {
        return Err(BackupServiceError::Validation(
            "backup path must be absolute".into(),
        ));
    }
    std::fs::create_dir_all(path).map_err(io_err)?;
    if !path.is_dir() {
        return Err(BackupServiceError::Validation(
            "backup path must be a directory".into(),
        ));
    }
    let mut probe = tempfile::Builder::new()
        .prefix(".backup-write-probe-")
        .tempfile_in(path)
        .map_err(io_err)?;
    probe
        .write_all(b"probe")
        .and_then(|()| probe.as_file().sync_all())
        .map_err(io_err)?;
    Ok(())
}

pub(super) fn cleanup_stale_files(
    dir: &Path,
    now: std::time::SystemTime,
) -> Result<(), BackupServiceError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(".weaver-backup-")
            || !entry.file_type().map_err(io_err)?.is_file()
        {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .map_err(io_err)?;
        if now
            .duration_since(modified)
            .is_ok_and(|age| age > BACKUP_EXECUTION_TIMEOUT)
            && let Err(error) = std::fs::remove_file(entry.path())
        {
            tracing::warn!(%error, "could not remove abandoned backup temporary file");
        }
    }
    Ok(())
}

pub(super) fn list_backups(
    dir: &Path,
    now: DateTime<Utc>,
) -> Result<Vec<BackupInfo>, BackupServiceError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut backups = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(io_err)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(%error, "could not read a backup directory entry");
                continue;
            }
        };
        let name = entry.file_name();
        let Some(filename) = name
            .to_str()
            .and_then(|name| name.strip_suffix(".metadata.json"))
        else {
            continue;
        };
        if !valid_filename(filename) || !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        let bytes = match std::fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(%error, %filename, "could not read backup metadata");
                continue;
            }
        };
        let Ok(mut info) = serde_json::from_slice::<BackupInfo>(&bytes) else {
            continue;
        };
        if info.filename != filename {
            continue;
        }
        let error = match info.status {
            BackupArtifactStatus::Ready if !dir.join(filename).is_file() => {
                Some("backup bundle is missing")
            }
            BackupArtifactStatus::Creating
                if now
                    .signed_duration_since(info.created_at)
                    .to_std()
                    .is_ok_and(|age| age > BACKUP_EXECUTION_TIMEOUT) =>
            {
                Some("backup timed out")
            }
            _ => None,
        };
        if let Some(error) = error {
            info.status = BackupArtifactStatus::Failed;
            info.error = Some(error.into());
        }
        backups.push(info);
    }
    backups.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| b.filename.cmp(&a.filename))
    });
    Ok(backups)
}

pub(super) fn delete_artifact(dir: &Path, info: &BackupInfo) -> Result<(), BackupServiceError> {
    if !valid_filename(&info.filename) {
        return Err(BackupServiceError::Validation(
            "invalid backup filename".into(),
        ));
    }
    let path = dir.join(&info.filename);
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_err(error)),
    }
    std::fs::remove_file(dir.join(format!("{}.metadata.json", info.filename))).map_err(io_err)
}

pub(super) fn complete_backup(
    dir: &Path,
    mut info: BackupInfo,
    result: Result<super::manifest::BackupManifest, BackupServiceError>,
) -> Result<BackupInfo, BackupServiceError> {
    let result = result.and_then(|manifest| {
        std::fs::metadata(dir.join(&info.filename))
            .map(|metadata| (manifest, metadata.len()))
            .map_err(io_err)
    });
    match &result {
        Ok((manifest, size)) => {
            info.status = BackupArtifactStatus::Ready;
            info.size_bytes = *size;
            info.row_counts = manifest
                .tables
                .iter()
                .map(|(name, part)| (name.clone(), part.rows))
                .collect();
        }
        Err(error) => {
            info.status = BackupArtifactStatus::Failed;
            info.error = Some(error.to_string());
        }
    }
    write_metadata(dir, &info)?;
    result?;
    Ok(info)
}

pub(super) fn prune_retained_backups(dir: &Path, current_version: &str) {
    match list_backups(dir, crate::e2e_clock::utc_now()) {
        Ok(backups) => {
            for info in retention_victims(&backups, current_version) {
                if let Err(error) = delete_artifact(dir, info) {
                    tracing::warn!(%error, filename = %info.filename, "could not prune retained backup");
                }
            }
        }
        Err(error) => tracing::warn!(%error, "could not list backups for retention"),
    }
}

pub(super) fn retention_victims<'a>(
    backups: &'a [BackupInfo],
    current_version: &str,
) -> Vec<&'a BackupInfo> {
    let Ok(current) = semver::Version::parse(current_version) else {
        tracing::warn!(%current_version, "skipping retention for an unrecognized backup version");
        return Vec::new();
    };
    let mut ready: Vec<_> = backups
        .iter()
        .filter(|info| {
            info.trigger == BackupTrigger::Auto
                && matches!(
                    info.status,
                    BackupArtifactStatus::Ready | BackupArtifactStatus::Failed
                )
        })
        .collect();
    ready.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| b.filename.cmp(&a.filename))
    });
    let previous = ready
        .iter()
        .filter(|info| info.status == BackupArtifactStatus::Ready)
        .filter_map(|info| semver::Version::parse(&info.source_weaver_version).ok())
        .filter(|version| version < &current)
        .max();
    let (mut current_kept, mut previous_kept, mut failures_kept) = (0, 0, 0);
    ready
        .into_iter()
        .filter(|info| {
            let version = semver::Version::parse(&info.source_weaver_version).ok();
            if version.as_ref().is_some_and(|version| version > &current) {
                return false;
            }
            if info.status == BackupArtifactStatus::Failed {
                failures_kept += 1;
                return failures_kept > 1;
            }
            if version.as_ref() == Some(&current) {
                current_kept += 1;
                current_kept > 3
            } else if (previous.is_some()
                && semver::Version::parse(&info.source_weaver_version).ok() == previous)
                || (previous.is_none() && info.source_weaver_version == "unknown")
            {
                previous_kept += 1;
                previous_kept > 1
            } else {
                true
            }
        })
        .collect()
}

impl BackupService {
    pub(super) async fn cleanup_stale_backup_files(&self) -> Result<(), BackupServiceError> {
        let dir = PathBuf::from(self.backup_settings().await?.backup_path);
        tokio::task::spawn_blocking(move || cleanup_stale_files(&dir, std::time::SystemTime::now()))
            .await
            .map_err(io_err)?
    }

    pub async fn backup_settings(&self) -> Result<BackupSettings, BackupServiceError> {
        let db = self.inner.db.clone();
        let custom = tokio::task::spawn_blocking(move || db.get_setting(STORAGE_KEY))
            .await
            .map_err(io_err)?
            .map_err(io_err)?;
        let custom = custom.filter(|value| !value.trim().is_empty());
        let default_dir = std::path::absolute(&self.inner.config.read().await.data_dir)
            .map_err(io_err)?
            .join("backups");
        let path = custom.as_ref().map(PathBuf::from).unwrap_or(default_dir);
        Ok(BackupSettings {
            custom_backup_path: custom,
            backup_path: path.to_string_lossy().into_owned(),
        })
    }

    pub async fn update_backup_settings(
        &self,
        custom_backup_path: Option<String>,
    ) -> Result<BackupSettings, BackupServiceError> {
        let _guard = self.inner.settings_lock.lock().await;
        let _manual = self
            .inner
            .manual_lock
            .try_lock()
            .map_err(|_| BackupServiceError::Busy)?;
        let _auto = self
            .inner
            .auto_lock
            .try_lock()
            .map_err(|_| BackupServiceError::Busy)?;
        let custom = custom_backup_path.filter(|value| !value.trim().is_empty());
        let default_dir = std::path::absolute(&self.inner.config.read().await.data_dir)
            .map_err(io_err)?
            .join("backups");
        let path = custom.as_ref().map(PathBuf::from).unwrap_or(default_dir);
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || {
            validate_storage_dir(&path)?;
            db.set_setting(STORAGE_KEY, custom.as_deref().unwrap_or(""))
                .map_err(io_err)
        })
        .await
        .map_err(io_err)??;
        self.backup_settings().await
    }

    pub async fn backups(&self) -> Result<Vec<BackupInfo>, BackupServiceError> {
        let dir = PathBuf::from(self.backup_settings().await?.backup_path);
        tokio::task::spawn_blocking(move || list_backups(&dir, crate::e2e_clock::utc_now()))
            .await
            .map_err(io_err)?
    }

    pub async fn backup_artifact(
        &self,
        filename: &str,
    ) -> Result<BackupArtifact, BackupServiceError> {
        if !valid_filename(filename) {
            return Err(BackupServiceError::Validation(
                "invalid backup filename".into(),
            ));
        }
        let dir = PathBuf::from(self.backup_settings().await?.backup_path);
        let list_dir = dir.clone();
        let info = tokio::task::spawn_blocking(move || {
            list_backups(&list_dir, crate::e2e_clock::utc_now())
        })
        .await
        .map_err(io_err)??
        .into_iter()
        .find(|info| info.filename == filename && info.status == BackupArtifactStatus::Ready)
        .ok_or_else(|| BackupServiceError::Validation("ready backup not found".into()))?;
        let path = dir.join(&info.filename);
        if std::fs::symlink_metadata(&path)
            .map_err(io_err)?
            .file_type()
            .is_symlink()
        {
            return Err(BackupServiceError::Validation(
                "backup bundle must be a regular file".into(),
            ));
        }
        Ok(BackupArtifact {
            filename: info.filename,
            path,
            temporary_directory: None,
        })
    }

    pub async fn delete_backup(&self, filename: &str) -> Result<bool, BackupServiceError> {
        let _manual = self
            .inner
            .manual_lock
            .try_lock()
            .map_err(|_| BackupServiceError::Busy)?;
        let _auto = self
            .inner
            .auto_lock
            .try_lock()
            .map_err(|_| BackupServiceError::Busy)?;
        let Some(info) = self
            .backups()
            .await?
            .into_iter()
            .find(|info| info.filename == filename)
        else {
            return Ok(false);
        };
        if info.status == BackupArtifactStatus::Creating {
            return Err(BackupServiceError::Busy);
        }
        let dir = PathBuf::from(self.backup_settings().await?.backup_path);
        tokio::task::spawn_blocking(move || delete_artifact(&dir, &info))
            .await
            .map_err(io_err)??;
        Ok(true)
    }

    pub async fn create_stored_backup(
        &self,
        password: Option<String>,
    ) -> Result<BackupInfo, BackupServiceError> {
        let (info, _finished) = self.begin_backup(password, BackupTrigger::Manual).await?;
        Ok(info)
    }

    pub(super) async fn begin_backup(
        &self,
        password: Option<String>,
        trigger: BackupTrigger,
    ) -> Result<
        (
            BackupInfo,
            tokio::task::JoinHandle<Result<BackupInfo, BackupServiceError>>,
        ),
        BackupServiceError,
    > {
        let (info, _, finished) = self.begin_backup_with_artifact(password, trigger).await?;
        Ok((info, finished))
    }

    pub(super) async fn begin_backup_with_artifact(
        &self,
        password: Option<String>,
        trigger: BackupTrigger,
    ) -> Result<
        (
            BackupInfo,
            BackupArtifact,
            tokio::task::JoinHandle<Result<BackupInfo, BackupServiceError>>,
        ),
        BackupServiceError,
    > {
        let password = password
            .filter(|value| !value.trim().is_empty())
            .ok_or(BackupServiceError::PasswordRequired)?;
        let lock = match trigger {
            BackupTrigger::Manual => &self.inner.manual_lock,
            BackupTrigger::Auto => &self.inner.auto_lock,
        };
        let execution = Arc::new(lock.clone().try_lock_owned().map_err(|_| {
            BackupServiceError::Validation(
                match trigger {
                    BackupTrigger::Manual => "a manual backup is already running",
                    BackupTrigger::Auto => "an automatic backup is already running",
                }
                .into(),
            )
        })?);
        if self
            .inner
            .shutting_down
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(BackupServiceError::Validation(
                "backup service is shutting down".into(),
            ));
        }
        let info = new_backup_info(
            trigger,
            self.inner.db.engine_name(),
            env!("CARGO_PKG_VERSION"),
        )?;
        let dir = PathBuf::from(self.backup_settings().await?.backup_path);
        let write_dir = dir.clone();
        let creating = info.clone();
        let metadata_execution = execution.clone();
        tokio::task::spawn_blocking(move || {
            let _execution = metadata_execution;
            validate_storage_dir(&write_dir)?;
            write_metadata(&write_dir, &creating)
        })
        .await
        .map_err(io_err)??;
        let service = self.clone();
        let completed = info.clone();
        let artifact = BackupArtifact {
            filename: info.filename.clone(),
            path: dir.join(&info.filename),
            temporary_directory: None,
        };
        let finished = tokio::spawn(async move {
            let cancellation = super::archive::BackupCancellation::new();
            let result = super::service::run_backup_work(
                service.write_backup(
                    password,
                    dir.join(&completed.filename),
                    execution.clone(),
                    cancellation.clone(),
                ),
                cancellation,
                service.inner.shutdown_signal.clone(),
            )
            .await;
            let completed = complete_backup(&dir, completed, result)?;
            if trigger == BackupTrigger::Auto {
                prune_retained_backups(&dir, env!("CARGO_PKG_VERSION"));
            }
            Ok(completed)
        });
        Ok((info, artifact, finished))
    }
}

impl BackupService {
    pub async fn create_download_token(
        &self,
        filename: &str,
    ) -> Result<String, BackupServiceError> {
        self.backup_artifact(filename).await?;
        let mut tokens = self.inner.download_tokens.lock().await;
        let now = tokio::time::Instant::now();
        tokens.retain(|_, (_, expiry)| *expiry > now);
        if tokens.len() >= 1024 {
            return Err(BackupServiceError::Busy);
        }
        let token = crate::auth::generate_api_key();
        tokens.insert(
            token.clone(),
            (filename.into(), now + std::time::Duration::from_secs(60)),
        );
        Ok(token)
    }

    pub async fn consume_download_token(&self, filename: &str, token: &str) -> bool {
        self.inner
            .download_tokens
            .lock()
            .await
            .remove(token)
            .is_some_and(|(expected, expiry)| {
                expected == filename && expiry > tokio::time::Instant::now()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_recovers_missing_and_stale_artifacts_without_invalidating_old_versions() {
        let dir = tempfile::tempdir().unwrap();
        let now = crate::e2e_clock::utc_now();
        let mut missing = new_backup_info(BackupTrigger::Manual, "sqlite", "0.1.0").unwrap();
        missing.status = BackupArtifactStatus::Ready;
        write_metadata(dir.path(), &missing).unwrap();
        let mut stale = new_backup_info(BackupTrigger::Auto, "sqlite", "0.1.0").unwrap();
        stale.created_at = now
            - chrono::Duration::from_std(BACKUP_EXECUTION_TIMEOUT).unwrap()
            - chrono::Duration::seconds(1);
        write_metadata(dir.path(), &stale).unwrap();
        let mut old = new_backup_info(BackupTrigger::Auto, "sqlite", "0.1.0").unwrap();
        old.status = BackupArtifactStatus::Ready;
        std::fs::write(dir.path().join(&old.filename), b"retained").unwrap();
        write_metadata(dir.path(), &old).unwrap();
        let current = new_backup_info(BackupTrigger::Manual, "sqlite", "0.2.0").unwrap();
        write_metadata(dir.path(), &current).unwrap();
        let rows = list_backups(dir.path(), now).unwrap();
        let row = |filename: &str| rows.iter().find(|row| row.filename == filename).unwrap();
        assert_eq!(row(&missing.filename).status, BackupArtifactStatus::Failed);
        assert_eq!(
            row(&stale.filename).error.as_deref(),
            Some("backup timed out")
        );
        assert_eq!(row(&old.filename).status, BackupArtifactStatus::Ready);
        assert_eq!(
            row(&current.filename).status,
            BackupArtifactStatus::Creating
        );
        let saved: BackupInfo = serde_json::from_slice(
            &std::fs::read(dir.path().join(format!("{}.metadata.json", stale.filename))).unwrap(),
        )
        .unwrap();
        assert_eq!(saved.status, BackupArtifactStatus::Creating);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path().join(format!("{}.metadata.json", old.filename)))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn retention_keeps_three_current_one_previous_and_all_non_ready_or_manual() {
        let now = crate::e2e_clock::utc_now();
        let mut rows = Vec::new();
        for (index, version) in [
            "1.0.0", "1.0.0", "1.0.0", "1.0.0", "0.9.0", "0.9.0", "0.8.0",
        ]
        .into_iter()
        .enumerate()
        {
            let mut info = new_backup_info(BackupTrigger::Auto, "sqlite", version).unwrap();
            info.status = BackupArtifactStatus::Ready;
            info.created_at = now - chrono::Duration::minutes(index as i64);
            rows.push(info);
        }
        for (trigger, status) in [
            (BackupTrigger::Manual, BackupArtifactStatus::Ready),
            (BackupTrigger::Auto, BackupArtifactStatus::Failed),
            (BackupTrigger::Auto, BackupArtifactStatus::Creating),
        ] {
            let mut info = new_backup_info(trigger, "sqlite", "0.0.1").unwrap();
            info.status = status;
            rows.push(info);
        }
        let victims = retention_victims(&rows, "1.0.0");
        assert_eq!(
            victims.iter().map(|row| &row.filename).collect::<Vec<_>>(),
            [&rows[3].filename, &rows[5].filename, &rows[6].filename]
        );
    }

    #[test]
    fn retention_selects_the_highest_older_version_after_a_downgrade() {
        let now = crate::e2e_clock::utc_now();
        let rows: Vec<_> = ["2.0.0", "0.8.0", "0.9.0", "0.9.0", "1.0.0"]
            .into_iter()
            .enumerate()
            .map(|(index, version)| {
                let mut info = new_backup_info(BackupTrigger::Auto, "sqlite", version).unwrap();
                info.status = BackupArtifactStatus::Ready;
                info.created_at = now - chrono::Duration::minutes(index as i64);
                info
            })
            .collect();
        let victims = retention_victims(&rows, "1.0.0");
        assert_eq!(
            victims.iter().map(|row| &row.filename).collect::<Vec<_>>(),
            [&rows[1].filename, &rows[3].filename]
        );
    }

    #[test]
    fn retention_keeps_the_newest_failure_and_all_newer_version_backups() {
        let now = crate::e2e_clock::utc_now();
        let rows: Vec<_> = ["1.0.0", "1.0.0", "2.0.0", "2.0.0"]
            .into_iter()
            .enumerate()
            .map(|(index, version)| {
                let mut info = new_backup_info(BackupTrigger::Auto, "sqlite", version).unwrap();
                info.status = BackupArtifactStatus::Failed;
                info.created_at = now - chrono::Duration::minutes(index as i64);
                info
            })
            .collect();
        let victims = retention_victims(&rows, "1.0.0");
        assert_eq!(
            victims.iter().map(|row| &row.filename).collect::<Vec<_>>(),
            [&rows[1].filename]
        );
    }

    #[test]
    fn stale_cleanup_removes_only_abandoned_regular_archive_temporary_files() {
        let dir = tempfile::tempdir().unwrap();
        let now = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10_000);
        for (name, age) in [
            (
                ".weaver-backup-abandoned",
                BACKUP_EXECUTION_TIMEOUT.as_secs() + 1,
            ),
            (".weaver-backup-active", 0),
            ("unrelated", BACKUP_EXECUTION_TIMEOUT.as_secs() + 1),
        ] {
            let file = std::fs::File::create(dir.path().join(name)).unwrap();
            file.set_times(
                std::fs::FileTimes::new().set_modified(now - std::time::Duration::from_secs(age)),
            )
            .unwrap();
        }
        std::fs::create_dir(dir.path().join(".weaver-backup-directory")).unwrap();
        cleanup_stale_files(dir.path(), now).unwrap();
        assert!(!dir.path().join(".weaver-backup-abandoned").exists());
        assert!(dir.path().join(".weaver-backup-active").exists());
        assert!(dir.path().join("unrelated").exists());
        assert!(dir.path().join(".weaver-backup-directory").is_dir());
    }

    #[test]
    fn completed_archive_stat_failure_publishes_failed_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let info = new_backup_info(BackupTrigger::Manual, "sqlite", "1.0.0").unwrap();
        write_metadata(dir.path(), &info).unwrap();
        let manifest = super::super::manifest::BackupManifest {
            format_version: super::super::manifest::BackupFormatVersion::Named(
                BACKUP_FORMAT_VERSION.into(),
            ),
            scope: String::new(),
            created_at_epoch_ms: 0,
            weaver_schema_version: 0,
            source_weaver_version: "1.0.0".into(),
            source_engine: "sqlite".into(),
            included_tables: Vec::new(),
            tables: BTreeMap::new(),
            part_checksums: BTreeMap::new(),
            source_paths: Default::default(),
            encrypted: true,
            legacy_managed_packages: Vec::new(),
            notes: Vec::new(),
        };
        assert!(complete_backup(dir.path(), info.clone(), Ok(manifest)).is_err());
        let saved: BackupInfo = serde_json::from_slice(
            &std::fs::read(dir.path().join(format!("{}.metadata.json", info.filename))).unwrap(),
        )
        .unwrap();
        assert_eq!(saved.status, BackupArtifactStatus::Failed);
        assert!(saved.error.is_some());
    }

    #[test]
    fn custom_storage_requires_an_absolute_writable_directory() {
        assert!(validate_storage_dir(Path::new("relative/backups")).is_err());
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        std::fs::write(&file, b"file").unwrap();
        assert!(validate_storage_dir(&file).is_err());
        let nested = dir.path().join("new/backups");
        validate_storage_dir(&nested).unwrap();
        assert!(nested.is_dir());
        assert_eq!(std::fs::read_dir(nested).unwrap().count(), 0);
    }
}
