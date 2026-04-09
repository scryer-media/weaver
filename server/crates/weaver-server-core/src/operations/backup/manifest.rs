use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::StableStateExport;
use crate::Database;
use crate::settings::Config;

pub(crate) const BACKUP_FORMAT_VERSION: u32 = 1;
pub(crate) const BACKUP_SCOPE: &str = "stable_state";

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

pub(crate) fn build_manifest(
    config: &Config,
    export: &StableStateExport,
    encrypted: bool,
) -> BackupManifest {
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

pub(crate) fn validate_manifest(
    db: &Database,
    manifest: &BackupManifest,
) -> Result<(), BackupServiceError> {
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

pub(crate) fn required_category_remaps(config: &Config) -> Vec<CategoryRemapRequirement> {
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

fn epoch_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn io_err(e: impl std::fmt::Display) -> BackupServiceError {
    BackupServiceError::Io(e.to_string())
}
