use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::catalog::{BackupTableClassification, catalog_tables};
use super::logical::{LogicalBackupExport, TablePartMetadata};
use crate::Database;
use crate::settings::Config;

pub(crate) const LEGACY_BACKUP_FORMAT_VERSION: u32 = 1;
pub(crate) const BACKUP_FORMAT_VERSION: &str = "weaver-backup-bundle-v2";
pub(crate) const BACKUP_SCOPE: &str = "stable_state";

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackupFormatVersion {
    Legacy(u32),
    Named(String),
}

impl BackupFormatVersion {
    pub fn is_legacy(&self) -> bool {
        matches!(self, Self::Legacy(LEGACY_BACKUP_FORMAT_VERSION))
    }

    pub fn is_bundle_v2(&self) -> bool {
        matches!(self, Self::Named(value) if value == BACKUP_FORMAT_VERSION)
    }
}

impl std::fmt::Display for BackupFormatVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(version) => version.fmt(formatter),
            Self::Named(version) => version.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackupSourcePaths {
    pub data_dir: String,
    pub intermediate_dir: String,
    pub complete_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format_version: BackupFormatVersion,
    pub scope: String,
    pub created_at_epoch_ms: u64,
    pub weaver_schema_version: i64,
    #[serde(default)]
    pub source_weaver_version: String,
    #[serde(default)]
    pub source_engine: String,
    #[serde(default)]
    pub included_tables: Vec<String>,
    #[serde(default)]
    pub tables: BTreeMap<String, TablePartMetadata>,
    #[serde(default)]
    pub part_checksums: BTreeMap<String, String>,
    pub source_paths: BackupSourcePaths,
    pub encrypted: bool,
    #[serde(default)]
    pub managed_packages: Vec<ManagedPackageInventory>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManagedPackageInventory {
    pub digest: String,
    pub archive_prefix: String,
    pub file_count: usize,
    pub uncompressed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BackupInstanceSecrets {
    pub encryption_master_key: String,
    pub key_source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupStatus {
    pub can_restore: bool,
    pub busy: bool,
    pub reason: Option<String>,
    pub pending_restore: Option<String>,
    pub pending_restore_error: Option<String>,
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
    pub key_compatible: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RestoreReport {
    pub restored: bool,
    pub staged: bool,
    pub restart_required: bool,
    pub pending_restore_id: Option<String>,
    pub history_jobs: usize,
    pub category_remaps_applied: usize,
    pub managed_packages_restored: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct BackupArtifact {
    pub filename: String,
    pub path: PathBuf,
    pub(crate) _temp_dir: tempfile::TempDir,
}

impl BackupArtifact {
    pub fn into_parts(self) -> (String, PathBuf, tempfile::TempDir) {
        (self.filename, self.path, self._temp_dir)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryRemapInput {
    pub category_name: String,
    pub new_dest_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreOptions {
    pub data_dir: String,
    pub intermediate_dir: Option<String>,
    pub complete_dir: Option<String>,
    pub category_remaps: Vec<CategoryRemapInput>,
}

#[derive(Debug, Clone, thiserror::Error)]
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
    UnsupportedFormat(String),
    #[error("backup scope '{0}' is not supported")]
    UnsupportedScope(String),
    #[error(
        "backup schema version {backup} is not compatible with runtime schema version {runtime}"
    )]
    SchemaMismatch { backup: i64, runtime: i64 },
    #[error("restore requires category remaps for: {0}")]
    MissingCategoryRemaps(String),
    #[error("restore encryption key is incompatible: {0}")]
    KeyIncompatible(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("I/O error: {0}")]
    Io(String),
}

pub(crate) fn build_bundle_manifest(
    source_paths: BackupSourcePaths,
    export: &LogicalBackupExport,
    managed_packages: Vec<ManagedPackageInventory>,
    instance_secrets_checksum: String,
) -> BackupManifest {
    let mut part_checksums = export
        .tables
        .iter()
        .map(|(table, metadata)| (format!("tables/{table}.ndjson"), metadata.checksum.clone()))
        .collect::<BTreeMap<_, _>>();
    part_checksums.insert("instance-secrets.json".into(), instance_secrets_checksum);
    BackupManifest {
        format_version: BackupFormatVersion::Named(BACKUP_FORMAT_VERSION.into()),
        scope: BACKUP_SCOPE.to_string(),
        created_at_epoch_ms: epoch_ms_now(),
        weaver_schema_version: export.schema_version,
        source_weaver_version: env!("CARGO_PKG_VERSION").to_string(),
        source_engine: export.source_engine.clone(),
        included_tables: export.tables.keys().cloned().collect(),
        tables: export.tables.clone(),
        part_checksums,
        source_paths,
        encrypted: true,
        managed_packages,
        notes: vec![
            "Logical stable-state bundle; active work and media payloads are excluded.".into(),
            "The encrypted bundle contains the source instance encryption master key.".into(),
        ],
    }
}

pub(crate) fn validate_manifest(
    db: &Database,
    manifest: &BackupManifest,
) -> Result<(), BackupServiceError> {
    validate_manifest_structure(manifest)?;
    let runtime_schema = db
        .schema_version()
        .map_err(|error| BackupServiceError::Io(error.to_string()))?;
    if manifest.weaver_schema_version > runtime_schema {
        return Err(BackupServiceError::SchemaMismatch {
            backup: manifest.weaver_schema_version,
            runtime: runtime_schema,
        });
    }
    Ok(())
}

pub(crate) fn validate_manifest_structure(
    manifest: &BackupManifest,
) -> Result<(), BackupServiceError> {
    if !manifest.format_version.is_legacy() && !manifest.format_version.is_bundle_v2() {
        return Err(BackupServiceError::UnsupportedFormat(
            manifest.format_version.to_string(),
        ));
    }
    if manifest.scope != BACKUP_SCOPE {
        return Err(BackupServiceError::UnsupportedScope(manifest.scope.clone()));
    }
    if manifest.format_version.is_legacy() && !manifest.managed_packages.is_empty() {
        return Err(BackupServiceError::Validation(
            "legacy backup unexpectedly declares managed packages".into(),
        ));
    }
    if manifest.format_version.is_bundle_v2() {
        let export_tables = catalog_tables(&[BackupTableClassification::Export]);
        if manifest.tables.keys().any(|table| {
            table.is_empty()
                || !table
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                || !export_tables.contains(table)
        }) {
            return Err(BackupServiceError::Validation(
                "logical bundle declares an unsupported table".into(),
            ));
        }
        let expected_checksums = manifest
            .tables
            .iter()
            .map(|(table, metadata)| (format!("tables/{table}.ndjson"), metadata.checksum.clone()))
            .chain(std::iter::once((
                "instance-secrets.json".to_string(),
                manifest
                    .part_checksums
                    .get("instance-secrets.json")
                    .cloned()
                    .unwrap_or_default(),
            )))
            .collect::<BTreeMap<_, _>>();
        if !manifest.encrypted
            || manifest.tables.is_empty()
            || !matches!(manifest.source_engine.as_str(), "sqlite" | "postgres")
            || manifest.source_paths.data_dir.trim().is_empty()
            || manifest.source_paths.intermediate_dir.trim().is_empty()
            || manifest.source_paths.complete_dir.trim().is_empty()
            || manifest.included_tables != manifest.tables.keys().cloned().collect::<Vec<_>>()
            || manifest.part_checksums != expected_checksums
        {
            return Err(BackupServiceError::Validation(
                "logical bundle manifest is incomplete".into(),
            ));
        }
        validate_package_inventory(&manifest.managed_packages)?;
    }
    Ok(())
}

fn validate_package_inventory(
    packages: &[ManagedPackageInventory],
) -> Result<(), BackupServiceError> {
    let mut digests = std::collections::HashSet::new();
    let mut prefixes = std::collections::HashSet::new();
    for package in packages {
        let Some(hex) = package.digest.strip_prefix("blake3:") else {
            return Err(BackupServiceError::Validation(
                "managed package digest must use blake3".into(),
            ));
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(BackupServiceError::Validation(
                "managed package digest is invalid".into(),
            ));
        }
        let expected_prefix = format!("managed-extensions/blake3/{hex}");
        if package.archive_prefix != expected_prefix
            || package.file_count == 0
            || !digests.insert(package.digest.as_str())
            || !prefixes.insert(package.archive_prefix.as_str())
        {
            return Err(BackupServiceError::Validation(
                "managed package inventory is invalid".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn required_category_remaps(config: &Config) -> Vec<CategoryRemapRequirement> {
    let old_complete_dir = config.complete_dir();
    let mut out = Vec::new();
    for category in &config.categories {
        let Some(dest_dir) = &category.dest_dir else {
            continue;
        };
        if !super::restore::stored_path_has_prefix(dest_dir, &old_complete_dir) {
            out.push(CategoryRemapRequirement {
                category_name: category.name.clone(),
                current_dest_dir: dest_dir.clone(),
            });
        }
    }
    out.sort_by(|a, b| a.category_name.cmp(&b.category_name));
    out
}

pub(crate) fn epoch_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn io_err(error: impl std::fmt::Display) -> BackupServiceError {
    BackupServiceError::Io(error.to_string())
}
