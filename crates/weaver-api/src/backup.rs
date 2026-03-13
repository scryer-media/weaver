use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::iter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use age::secrecy::SecretString;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;

use weaver_core::config::{Config, SharedConfig};
use weaver_scheduler::{JobInfo, JobStatus, SchedulerHandle};
use weaver_state::{Database, HistoryFilter, StableStateExport};

use crate::rss::RssService;
use crate::runtime::reload_runtime_from_db;
use crate::submit::init_job_counter;

const BACKUP_FORMAT_VERSION: u32 = 1;
const BACKUP_SCOPE: &str = "stable_state";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSourcePaths {
    pub data_dir: String,
    pub intermediate_dir: String,
    pub complete_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format_version: u32,
    pub scope: String,
    pub created_at_epoch_ms: u64,
    pub weaver_schema_version: i64,
    pub included_tables: Vec<String>,
    pub source_paths: BackupSourcePaths,
    pub encrypted: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupStatus {
    pub can_restore: bool,
    pub busy: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CategoryRemapRequirement {
    pub category_name: String,
    pub current_dest_dir: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupInspectResult {
    pub manifest: BackupManifest,
    pub required_category_remaps: Vec<CategoryRemapRequirement>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RestoreReport {
    pub restored: bool,
    pub history_jobs: usize,
    pub category_remaps_applied: usize,
}

#[derive(Debug, Clone)]
pub struct BackupArtifact {
    pub filename: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryRemapInput {
    pub category_name: String,
    pub new_dest_dir: String,
}

#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub data_dir: String,
    pub intermediate_dir: Option<String>,
    pub complete_dir: Option<String>,
    pub category_remaps: Vec<CategoryRemapInput>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackupServiceError {
    #[error("backup or restore already in progress")]
    Busy,
    #[error("restore target is not pristine")]
    NotPristine,
    #[error("backup password is required for this archive")]
    PasswordRequired,
    #[error("backup password is invalid")]
    InvalidPassword,
    #[error("unsupported backup format version {0}")]
    UnsupportedFormat(u32),
    #[error("backup scope '{0}' is not supported")]
    UnsupportedScope(String),
    #[error(
        "backup schema version {backup} is not compatible with runtime schema version {runtime}"
    )]
    SchemaMismatch { backup: i64, runtime: i64 },
    #[error("restore requires category remaps for: {0}")]
    MissingCategoryRemaps(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("I/O error: {0}")]
    Io(String),
}

impl BackupServiceError {
    pub fn status_code(&self) -> axum::http::StatusCode {
        match self {
            Self::Busy => axum::http::StatusCode::CONFLICT,
            Self::NotPristine => axum::http::StatusCode::CONFLICT,
            Self::PasswordRequired | Self::InvalidPassword => axum::http::StatusCode::BAD_REQUEST,
            Self::UnsupportedFormat(_)
            | Self::UnsupportedScope(_)
            | Self::SchemaMismatch { .. }
            | Self::MissingCategoryRemaps(_)
            | Self::Validation(_) => axum::http::StatusCode::BAD_REQUEST,
            Self::Io(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

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
        init_job_counter(max_job_id + 1);

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

fn build_manifest(config: &Config, export: &StableStateExport, encrypted: bool) -> BackupManifest {
    BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        scope: BACKUP_SCOPE.to_string(),
        created_at_epoch_ms: epoch_ms_now(),
        weaver_schema_version: export.schema_version,
        included_tables: export.included_tables.clone(),
        source_paths: BackupSourcePaths {
            data_dir: config.data_dir.clone(),
            intermediate_dir: config.intermediate_dir(),
            complete_dir: config.complete_dir(),
        },
        encrypted,
        notes: vec![
            "Stable-state backup only: active jobs, saved NZBs, and media payloads are excluded."
                .into(),
            "API keys are preserved as stored hashes; restored keys remain valid but raw key values cannot be shown again."
                .into(),
        ],
    }
}

fn write_plain_archive(
    dest: &Path,
    manifest: &BackupManifest,
    backup_db_path: &Path,
) -> Result<(), std::io::Error> {
    let file = File::create(dest)?;
    let encoder = zstd::stream::write::Encoder::new(file, 19)?;
    let mut tar = tar::Builder::new(encoder);

    let manifest_bytes = serde_json::to_vec_pretty(manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_bytes.as_slice())?;
    tar.append_path_with_name(backup_db_path, "backup.db")?;

    let encoder = tar.into_inner()?;
    encoder.finish()?;
    Ok(())
}

fn encrypt_archive(input: &Path, output: &Path, password: &str) -> Result<(), std::io::Error> {
    let encryptor = age::Encryptor::with_user_passphrase(SecretString::from(password.to_owned()));
    let mut input_file = File::open(input)?;
    let output_file = File::create(output)?;
    let mut writer = encryptor
        .wrap_output(output_file)
        .map_err(std::io::Error::other)?;
    std::io::copy(&mut input_file, &mut writer)?;
    writer.finish().map_err(std::io::Error::other)?;
    Ok(())
}

fn maybe_decrypt_archive(
    input: &Path,
    password: Option<String>,
    work_dir: &Path,
) -> Result<PathBuf, BackupServiceError> {
    if !is_age_encrypted(input)? {
        return Ok(input.to_path_buf());
    }

    let password = password
        .filter(|value| !value.is_empty())
        .ok_or(BackupServiceError::PasswordRequired)?;
    let output = work_dir.join("backup.tar.zst");
    decrypt_archive(input, &output, &password)?;
    Ok(output)
}

fn decrypt_archive(input: &Path, output: &Path, password: &str) -> Result<(), BackupServiceError> {
    let input_file = File::open(input).map_err(io_err)?;
    let decryptor = age::Decryptor::new(BufReader::new(input_file))
        .map_err(|_| BackupServiceError::InvalidPassword)?;
    let identity = age::scrypt::Identity::new(SecretString::from(password.to_owned()));
    let mut reader = decryptor
        .decrypt(iter::once(&identity as &dyn age::Identity))
        .map_err(|_| BackupServiceError::InvalidPassword)?;
    let mut output_file = File::create(output).map_err(io_err)?;
    std::io::copy(&mut reader, &mut output_file).map_err(io_err)?;
    Ok(())
}

fn unpack_plain_archive(input: &Path, output_dir: &Path) -> Result<(), BackupServiceError> {
    let file = File::open(input).map_err(io_err)?;
    let decoder = zstd::stream::read::Decoder::new(file).map_err(io_err)?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(output_dir).map_err(io_err)?;
    Ok(())
}

fn is_age_encrypted(path: &Path) -> Result<bool, BackupServiceError> {
    let mut header = [0u8; 32];
    let mut file = File::open(path).map_err(io_err)?;
    let read = file.read(&mut header).map_err(io_err)?;
    Ok(header[..read].starts_with(b"age-encryption.org/"))
}

fn validate_manifest(db: &Database, manifest: &BackupManifest) -> Result<(), BackupServiceError> {
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(BackupServiceError::UnsupportedFormat(
            manifest.format_version,
        ));
    }
    if manifest.scope != BACKUP_SCOPE {
        return Err(BackupServiceError::UnsupportedScope(manifest.scope.clone()));
    }
    let runtime_schema = db
        .schema_version()
        .map_err(|e| BackupServiceError::Io(e.to_string()))?;
    if manifest.weaver_schema_version != runtime_schema {
        return Err(BackupServiceError::SchemaMismatch {
            backup: manifest.weaver_schema_version,
            runtime: runtime_schema,
        });
    }
    Ok(())
}

fn required_category_remaps(config: &Config) -> Vec<CategoryRemapRequirement> {
    let old_complete_dir = PathBuf::from(config.complete_dir());
    let mut out = Vec::new();
    for category in &config.categories {
        let Some(dest_dir) = &category.dest_dir else {
            continue;
        };
        if Path::new(dest_dir).strip_prefix(&old_complete_dir).is_err() {
            out.push(CategoryRemapRequirement {
                category_name: category.name.clone(),
                current_dest_dir: dest_dir.clone(),
            });
        }
    }
    out.sort_by(|a, b| a.category_name.cmp(&b.category_name));
    out
}

fn rewrite_backup_db_for_restore(
    backup_db_path: &Path,
    source_config: &Config,
    options: &RestoreOptions,
) -> Result<(), BackupServiceError> {
    let remap_map: BTreeMap<&str, &str> = options
        .category_remaps
        .iter()
        .map(|entry| (entry.category_name.trim(), entry.new_dest_dir.trim()))
        .collect();
    let old_complete = PathBuf::from(source_config.complete_dir());
    let new_complete = PathBuf::from(
        options
            .complete_dir
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("{}/complete", options.data_dir)),
    );

    let mut missing = Vec::new();
    let mut category_updates = Vec::new();
    for category in &source_config.categories {
        let Some(dest_dir) = &category.dest_dir else {
            continue;
        };
        let new_dest = if let Ok(relative) = Path::new(dest_dir).strip_prefix(&old_complete) {
            new_complete.join(relative).to_string_lossy().to_string()
        } else if let Some(mapped) = remap_map.get(category.name.as_str()) {
            (*mapped).to_string()
        } else {
            missing.push(category.name.clone());
            continue;
        };

        category_updates.push((category.id, new_dest));
    }

    if !missing.is_empty() {
        missing.sort();
        return Err(BackupServiceError::MissingCategoryRemaps(
            missing.join(", "),
        ));
    }

    let conn = rusqlite::Connection::open(backup_db_path)
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;

    set_or_insert_setting(&tx, "data_dir", &options.data_dir)?;

    match &options.intermediate_dir {
        Some(value) if !value.trim().is_empty() => {
            set_or_insert_setting(&tx, "intermediate_dir", value.trim())?
        }
        _ => {
            tx.execute("DELETE FROM settings WHERE key = ?1", ["intermediate_dir"])
                .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        }
    }

    match &options.complete_dir {
        Some(value) if !value.trim().is_empty() => {
            set_or_insert_setting(&tx, "complete_dir", value.trim())?
        }
        _ => {
            tx.execute("DELETE FROM settings WHERE key = ?1", ["complete_dir"])
                .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        }
    }

    for (category_id, new_dest) in category_updates {
        let updated = tx
            .execute(
                "UPDATE categories SET dest_dir = ?1 WHERE id = ?2",
                rusqlite::params![new_dest, category_id],
            )
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        if updated == 0 {
            return Err(BackupServiceError::Validation(format!(
                "category id {category_id} not found in backup"
            )));
        }
    }

    tx.commit()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;

    let backup_db = Database::open(backup_db_path)
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    let rewritten = backup_db
        .load_config()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    rewritten
        .validate()
        .map_err(|errors| BackupServiceError::Validation(errors.join("; ")))?;

    Ok(())
}

fn set_or_insert_setting(
    conn: &rusqlite::Transaction<'_>,
    key: &str,
    value: &str,
) -> Result<(), BackupServiceError> {
    let updated = conn
        .execute(
            "UPDATE settings SET value = ?1 WHERE key = ?2",
            rusqlite::params![value, key],
        )
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)",
            rusqlite::params![key, value],
        )
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    }
    Ok(())
}

async fn load_history_snapshot(db: &Database) -> Result<Vec<JobInfo>, BackupServiceError> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        let rows = db.list_job_history(&HistoryFilter::default())?;
        Ok::<_, weaver_state::StateError>(
            rows.into_iter()
                .map(job_info_from_history)
                .collect::<Vec<_>>(),
        )
    })
    .await
    .map_err(|e| BackupServiceError::Io(e.to_string()))?
    .map_err(|e| BackupServiceError::Io(e.to_string()))
}

fn job_info_from_history(row: weaver_state::JobHistoryRow) -> JobInfo {
    let status = match row.status.as_str() {
        "downloading" => JobStatus::Downloading,
        "verifying" => JobStatus::Verifying,
        "repairing" => JobStatus::Repairing,
        "extracting" => JobStatus::Extracting,
        "complete" => JobStatus::Complete,
        "failed" => JobStatus::Failed {
            error: row
                .error_message
                .clone()
                .unwrap_or_else(|| "unknown error".into()),
        },
        "paused" => JobStatus::Paused,
        "cancelled" => JobStatus::Failed {
            error: "cancelled".into(),
        },
        other => JobStatus::Failed {
            error: format!("unknown status: {other}"),
        },
    };

    JobInfo {
        job_id: weaver_core::id::JobId(row.job_id),
        name: row.name,
        status: status.clone(),
        progress: 1.0,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        failed_bytes: row.failed_bytes,
        health: row.health,
        password: None,
        category: row.category,
        metadata: row
            .metadata
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default(),
        output_dir: row.output_dir,
        error: if let JobStatus::Failed { error } = &status {
            Some(error.clone())
        } else {
            None
        },
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
    }
}

fn epoch_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn io_err(e: impl std::fmt::Display) -> BackupServiceError {
    BackupServiceError::Io(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use tokio::sync::{RwLock, broadcast, mpsc};
    use weaver_core::config::{CategoryConfig, Config};
    use weaver_scheduler::{PipelineMetrics, SchedulerCommand, SharedPipelineState};
    use weaver_state::JobHistoryRow;

    #[derive(Default, Clone)]
    struct RuntimeCapture {
        updated_paths: StdArc<StdMutex<Option<(PathBuf, PathBuf, PathBuf)>>>,
        speed_limits: StdArc<StdMutex<Vec<u64>>>,
    }

    fn open_temp_db() -> Database {
        let dir = tempfile::tempdir().unwrap();
        Database::open(&dir.path().join("weaver.db")).unwrap()
    }

    fn build_service(db: Database, config: Config) -> (BackupService, RuntimeCapture) {
        let capture = RuntimeCapture::default();
        let shared_config = StdArc::new(RwLock::new(config));
        let handle = test_scheduler_handle(capture.clone());
        let rss = RssService::new(handle.clone(), shared_config.clone(), db.clone());
        (BackupService::new(handle, shared_config, db, rss), capture)
    }

    fn test_scheduler_handle(capture: RuntimeCapture) -> SchedulerHandle {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
        let (event_tx, _) = broadcast::channel(16);
        let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        let task_state = state.clone();
        tokio::spawn(async move {
            while let Some(command) = cmd_rx.recv().await {
                match command {
                    SchedulerCommand::SetSpeedLimit {
                        bytes_per_sec,
                        reply,
                    } => {
                        capture.speed_limits.lock().unwrap().push(bytes_per_sec);
                        let _ = reply.send(());
                    }
                    SchedulerCommand::SetBandwidthCapPolicy { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::RebuildNntp { reply, .. } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::UpdateRuntimePaths {
                        data_dir,
                        intermediate_dir,
                        complete_dir,
                        reply,
                    } => {
                        *capture.updated_paths.lock().unwrap() =
                            Some((data_dir, intermediate_dir, complete_dir));
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::PauseAll { reply } => {
                        task_state.set_paused(true);
                        let _ = reply.send(());
                    }
                    SchedulerCommand::ResumeAll { reply } => {
                        task_state.set_paused(false);
                        let _ = reply.send(());
                    }
                    _ => {}
                }
            }
        });
        SchedulerHandle::new(cmd_tx, event_tx, state)
    }

    fn sample_config() -> Config {
        Config {
            data_dir: "/old/data".into(),
            intermediate_dir: Some("/old/data/intermediate".into()),
            complete_dir: Some("/old/data/complete".into()),
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![
                CategoryConfig {
                    id: 1,
                    name: "tv".into(),
                    dest_dir: Some("/old/data/complete/tv".into()),
                    aliases: String::new(),
                },
                CategoryConfig {
                    id: 2,
                    name: "movies".into(),
                    dest_dir: Some("/mnt/media/movies".into()),
                    aliases: String::new(),
                },
            ],
            retry: None,
            max_download_speed: Some(1234),
            isp_bandwidth_cap: None,
            cleanup_after_extract: Some(true),
            config_path: None,
        }
    }

    fn populate_source_db(db: &Database) {
        db.save_config(&sample_config()).unwrap();
        db.insert_job_history(&JobHistoryRow {
            job_id: 41,
            name: "Backup Fixture".into(),
            status: "complete".into(),
            error_message: None,
            total_bytes: 10,
            downloaded_bytes: 10,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: Some("tv".into()),
            output_dir: Some("/old/data/complete/tv/fixture".into()),
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
        })
        .unwrap();
    }

    #[test]
    fn external_category_paths_require_manual_mapping() {
        let config = sample_config();
        let remaps = required_category_remaps(&config);
        assert_eq!(remaps.len(), 1);
        assert_eq!(remaps[0].category_name, "movies");
    }

    #[tokio::test]
    async fn password_protected_backup_roundtrip_inspects() {
        let source_db = open_temp_db();
        populate_source_db(&source_db);
        let (service, _) = build_service(source_db, sample_config());

        let artifact = service
            .create_backup(Some("secret-pass".into()))
            .await
            .unwrap();
        assert!(artifact.filename.ends_with(".age"));

        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), artifact.bytes).unwrap();

        let inspect = service
            .inspect_backup(temp.path(), Some("secret-pass".into()))
            .await
            .unwrap();
        assert_eq!(inspect.manifest.scope, BACKUP_SCOPE);
        assert_eq!(inspect.required_category_remaps.len(), 1);
        assert_eq!(inspect.required_category_remaps[0].category_name, "movies");
    }

    #[tokio::test]
    async fn plain_backup_contains_manifest_and_database() {
        let source_db = open_temp_db();
        populate_source_db(&source_db);
        let (service, _) = build_service(source_db, sample_config());

        let artifact = service.create_backup(None).await.unwrap();
        assert!(artifact.filename.ends_with(".tar.zst"));

        let archive = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(archive.path(), artifact.bytes).unwrap();

        let mut entries = Vec::new();
        let file = File::open(archive.path()).unwrap();
        let decoder = zstd::stream::read::Decoder::new(file).unwrap();
        let mut tar = tar::Archive::new(decoder);
        for entry in tar.entries().unwrap() {
            let entry = entry.unwrap();
            entries.push(entry.path().unwrap().to_string_lossy().to_string());
        }
        entries.sort();

        assert_eq!(
            entries,
            vec!["backup.db".to_string(), "manifest.json".to_string()]
        );
    }

    #[tokio::test]
    async fn restore_requires_category_remap_for_external_paths() {
        let source_db = open_temp_db();
        populate_source_db(&source_db);
        let (source_service, _) = build_service(source_db, sample_config());

        let artifact = source_service.create_backup(None).await.unwrap();
        let archive = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(archive.path(), artifact.bytes).unwrap();

        let target_db = open_temp_db();
        target_db
            .save_config(&Config {
                data_dir: "/bootstrap".into(),
                intermediate_dir: None,
                complete_dir: None,
                buffer_pool: None,
                tuner: None,
                servers: vec![],
                categories: vec![],
                retry: None,
                max_download_speed: None,
                isp_bandwidth_cap: None,
                cleanup_after_extract: Some(true),
                config_path: None,
            })
            .unwrap();

        let (target_service, _) = build_service(
            target_db.clone(),
            Config {
                data_dir: "/bootstrap".into(),
                intermediate_dir: None,
                complete_dir: None,
                buffer_pool: None,
                tuner: None,
                servers: vec![],
                categories: vec![],
                retry: None,
                max_download_speed: None,
                isp_bandwidth_cap: None,
                cleanup_after_extract: Some(true),
                config_path: None,
            },
        );

        let err = target_service
            .restore_backup(
                archive.path(),
                None,
                RestoreOptions {
                    data_dir: "/new/data".into(),
                    intermediate_dir: None,
                    complete_dir: None,
                    category_remaps: vec![],
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            BackupServiceError::MissingCategoryRemaps(ref names) if names == "movies"
        ));
        assert_eq!(target_db.load_config().unwrap().data_dir, "/bootstrap");
        assert!(
            target_db
                .list_job_history(&HistoryFilter::default())
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn restore_rewrites_paths_and_refreshes_runtime() {
        let source_db = open_temp_db();
        populate_source_db(&source_db);
        let (source_service, _) = build_service(source_db, sample_config());

        let artifact = source_service.create_backup(None).await.unwrap();
        let archive = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(archive.path(), artifact.bytes).unwrap();

        let target_db = open_temp_db();
        target_db
            .save_config(&Config {
                data_dir: "/bootstrap".into(),
                intermediate_dir: None,
                complete_dir: None,
                buffer_pool: None,
                tuner: None,
                servers: vec![],
                categories: vec![],
                retry: None,
                max_download_speed: None,
                isp_bandwidth_cap: None,
                cleanup_after_extract: Some(true),
                config_path: None,
            })
            .unwrap();

        let (target_service, capture) = build_service(
            target_db.clone(),
            Config {
                data_dir: "/bootstrap".into(),
                intermediate_dir: None,
                complete_dir: None,
                buffer_pool: None,
                tuner: None,
                servers: vec![],
                categories: vec![],
                retry: None,
                max_download_speed: None,
                isp_bandwidth_cap: None,
                cleanup_after_extract: Some(true),
                config_path: None,
            },
        );

        let report = target_service
            .restore_backup(
                archive.path(),
                None,
                RestoreOptions {
                    data_dir: "/new/data".into(),
                    intermediate_dir: None,
                    complete_dir: None,
                    category_remaps: vec![CategoryRemapInput {
                        category_name: "movies".into(),
                        new_dest_dir: "/srv/media/movies".into(),
                    }],
                },
            )
            .await
            .unwrap();

        assert!(report.restored);
        let restored = target_db.load_config().unwrap();
        assert_eq!(restored.data_dir, "/new/data");
        assert_eq!(restored.complete_dir(), "/new/data/complete");
        let tv = restored
            .categories
            .iter()
            .find(|category| category.name == "tv")
            .unwrap();
        assert_eq!(tv.dest_dir.as_deref(), Some("/new/data/complete/tv"));
        let movies = restored
            .categories
            .iter()
            .find(|category| category.name == "movies")
            .unwrap();
        assert_eq!(movies.dest_dir.as_deref(), Some("/srv/media/movies"));
        assert_eq!(
            target_db
                .list_job_history(&HistoryFilter::default())
                .unwrap()
                .len(),
            1
        );

        let updated = capture.updated_paths.lock().unwrap().clone().unwrap();
        assert_eq!(updated.0, Path::new("/new/data"));
        assert_eq!(updated.1, Path::new("/new/data/intermediate"));
        assert_eq!(updated.2, Path::new("/new/data/complete"));
        assert_eq!(
            capture.speed_limits.lock().unwrap().last().copied(),
            Some(1234)
        );
    }
}
