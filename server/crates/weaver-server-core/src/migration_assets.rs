#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use crate::migration_hook_ids;
use serde::{Deserialize, Serialize};

pub const DEFAULT_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    Blake3,
}

impl ChecksumAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blake3 => "blake3",
        }
    }

    pub fn digest(self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Self::Blake3 => blake3::hash(bytes).as_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationInstallKind {
    FreshInstall,
    Upgrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StepScope {
    #[default]
    All,
    UpgradeOnly,
    NewInstallOnly,
}

impl StepScope {
    pub fn applies_to(self, install_kind: MigrationInstallKind) -> bool {
        matches!(
            (self, install_kind),
            (Self::All, _)
                | (Self::UpgradeOnly, MigrationInstallKind::Upgrade)
                | (Self::NewInstallOnly, MigrationInstallKind::FreshInstall)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMigrationManifest {
    #[serde(default = "default_manifest_version")]
    pub format_version: u32,
    pub starting_version: i64,
    #[serde(default, rename = "migration")]
    pub migrations: Vec<SourceMigration>,
    #[serde(default, rename = "baseline")]
    pub baselines: Vec<SourceBaseline>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMigration {
    pub version: i64,
    pub description: String,
    #[serde(default = "default_checksum_algorithm")]
    pub checksum_algo: ChecksumAlgorithm,
    #[serde(default)]
    pub steps: Vec<SourceMigrationStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceMigrationStep {
    Sql {
        file: String,
        #[serde(default)]
        scope: StepScope,
    },
    Rust {
        hook_id: String,
        #[serde(default)]
        scope: StepScope,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBaseline {
    pub through_version: i64,
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledMigrationBundle {
    pub catalog: CompiledMigrationCatalog,
    pub payload_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledMigrationCatalog {
    pub format_version: u32,
    pub starting_version: i64,
    pub payload_checksum_algo: ChecksumAlgorithm,
    pub payload_checksum: Vec<u8>,
    pub migrations: Vec<CompiledMigration>,
    pub baselines: Vec<CompiledBaseline>,
}

impl CompiledMigrationCatalog {
    pub fn max_version(&self) -> i64 {
        self.migrations
            .last()
            .map(|migration| migration.version)
            .unwrap_or(self.starting_version - 1)
    }

    pub fn find_migration(&self, version: i64) -> Option<&CompiledMigration> {
        self.migrations
            .iter()
            .find(|migration| migration.version == version)
    }

    pub fn latest_baseline_at_or_below(&self, version: i64) -> Option<&CompiledBaseline> {
        self.baselines
            .iter()
            .filter(|baseline| baseline.through_version <= version)
            .max_by_key(|baseline| baseline.through_version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledMigration {
    pub version: i64,
    pub description: String,
    pub key: String,
    pub filename: String,
    pub checksum_algo: ChecksumAlgorithm,
    pub checksum: Vec<u8>,
    pub steps: Vec<CompiledMigrationStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompiledMigrationStep {
    Sql {
        file: String,
        scope: StepScope,
        payload: PayloadSlice,
    },
    Rust {
        hook_id: String,
        scope: StepScope,
    },
}

impl CompiledMigrationStep {
    pub fn scope(&self) -> StepScope {
        match self {
            Self::Sql { scope, .. } | Self::Rust { scope, .. } => *scope,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledBaseline {
    pub through_version: i64,
    pub file: String,
    pub payload: PayloadSlice,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PayloadSlice {
    pub start: u64,
    pub len: u64,
}

impl PayloadSlice {
    pub fn bytes<'a>(&self, payload_bytes: &'a [u8]) -> Result<&'a [u8], String> {
        let start = usize::try_from(self.start).map_err(|_| "payload start out of range")?;
        let len = usize::try_from(self.len).map_err(|_| "payload length out of range")?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| "payload slice overflow".to_string())?;
        payload_bytes
            .get(start..end)
            .ok_or_else(|| "payload slice outside bundle".to_string())
    }

    pub fn text<'a>(&self, payload_bytes: &'a [u8]) -> Result<&'a str, String> {
        std::str::from_utf8(self.bytes(payload_bytes)?)
            .map_err(|error| format!("payload is not valid UTF-8: {error}"))
    }
}

#[derive(Debug, Serialize)]
struct CanonicalMigration {
    version: i64,
    description: String,
    steps: Vec<CanonicalStep>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalStep {
    Sql {
        scope: StepScope,
        sql_blake3: String,
    },
    Rust {
        scope: StepScope,
        hook_id: String,
    },
}

fn default_manifest_version() -> u32 {
    DEFAULT_MANIFEST_VERSION
}

fn default_checksum_algorithm() -> ChecksumAlgorithm {
    ChecksumAlgorithm::Blake3
}

pub fn source_manifest_path(db_root: &Path) -> PathBuf {
    db_root.join("migrations/manifest.toml")
}

pub fn load_source_manifest(db_root: &Path) -> Result<SourceMigrationManifest, String> {
    let path = source_manifest_path(db_root);
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    toml::from_str(&source).map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

pub fn compile_source_bundle(db_root: &Path) -> Result<CompiledMigrationBundle, String> {
    let manifest = load_source_manifest(db_root)?;
    if manifest.format_version != DEFAULT_MANIFEST_VERSION {
        return Err(format!(
            "unsupported migration manifest version {}",
            manifest.format_version
        ));
    }
    if manifest.starting_version <= 0 {
        return Err("starting_version must be positive".to_string());
    }

    let mut payload_bytes = Vec::new();
    let mut migrations = manifest.migrations.clone();
    migrations.sort_by_key(|migration| migration.version);

    let mut compiled_migrations = Vec::with_capacity(migrations.len());
    for (expected_version, migration) in (manifest.starting_version..).zip(migrations) {
        if migration.version != expected_version {
            return Err(format!(
                "migration versions must be contiguous starting at {:04}; expected {:04}, found {:04}",
                manifest.starting_version, expected_version, migration.version
            ));
        }
        compiled_migrations.push(compile_migration(db_root, &migration, &mut payload_bytes)?);
    }

    let mut baselines = Vec::new();
    let mut baseline_versions = std::collections::HashSet::new();
    for baseline in manifest.baselines {
        if !baseline_versions.insert(baseline.through_version) {
            return Err(format!(
                "duplicate baseline entry for version {:04}",
                baseline.through_version
            ));
        }
        if compiled_migrations
            .iter()
            .all(|migration| migration.version != baseline.through_version)
        {
            return Err(format!(
                "baseline {:04} does not match any known migration version",
                baseline.through_version
            ));
        }
        let path = db_root.join(&baseline.file);
        let sql = fs::read(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let payload = push_payload(&sql, &mut payload_bytes);
        baselines.push(CompiledBaseline {
            through_version: baseline.through_version,
            file: baseline.file,
            payload,
        });
    }
    baselines.sort_by_key(|baseline| baseline.through_version);

    let payload_checksum_algo = ChecksumAlgorithm::Blake3;
    let payload_checksum = payload_checksum_algo.digest(&payload_bytes);
    Ok(CompiledMigrationBundle {
        catalog: CompiledMigrationCatalog {
            format_version: manifest.format_version,
            starting_version: manifest.starting_version,
            payload_checksum_algo,
            payload_checksum,
            migrations: compiled_migrations,
            baselines,
        },
        payload_bytes,
    })
}

fn compile_migration(
    db_root: &Path,
    migration: &SourceMigration,
    payload_bytes: &mut Vec<u8>,
) -> Result<CompiledMigration, String> {
    if migration.steps.is_empty() {
        return Err(format!("migration {:04} has no steps", migration.version));
    }

    let mut compiled_steps = Vec::with_capacity(migration.steps.len());
    let mut canonical_steps = Vec::with_capacity(migration.steps.len());
    for step in &migration.steps {
        match step {
            SourceMigrationStep::Sql { file, scope } => {
                let path = db_root.join(file);
                let sql = fs::read(&path)
                    .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
                let payload = push_payload(&sql, payload_bytes);
                compiled_steps.push(CompiledMigrationStep::Sql {
                    file: file.clone(),
                    scope: *scope,
                    payload,
                });
                canonical_steps.push(CanonicalStep::Sql {
                    scope: *scope,
                    sql_blake3: checksum_hex(&ChecksumAlgorithm::Blake3.digest(&sql)),
                });
            }
            SourceMigrationStep::Rust { hook_id, scope } => {
                migration_hook_ids::validate_migration_hook_id(hook_id)?;
                compiled_steps.push(CompiledMigrationStep::Rust {
                    hook_id: hook_id.clone(),
                    scope: *scope,
                });
                canonical_steps.push(CanonicalStep::Rust {
                    scope: *scope,
                    hook_id: hook_id.clone(),
                });
            }
        }
    }

    let canonical = CanonicalMigration {
        version: migration.version,
        description: migration.description.clone(),
        steps: canonical_steps,
    };
    let canonical_bytes = serde_json::to_vec(&canonical).map_err(|error| {
        format!(
            "failed to serialize canonical migration {:04}: {error}",
            migration.version
        )
    })?;
    let key = migration_key_from_version_and_desc(migration.version, &migration.description);
    let filename = infer_filename(migration, &key);
    Ok(CompiledMigration {
        version: migration.version,
        description: migration.description.clone(),
        key,
        filename,
        checksum_algo: migration.checksum_algo,
        checksum: migration.checksum_algo.digest(&canonical_bytes),
        steps: compiled_steps,
    })
}

fn infer_filename(migration: &SourceMigration, key: &str) -> String {
    if migration.steps.len() == 1
        && let SourceMigrationStep::Sql { file, .. } = &migration.steps[0]
        && let Some(name) = Path::new(file).file_name().and_then(|value| value.to_str())
    {
        return name.to_string();
    }
    format!("{key}.migration")
}

fn push_payload(bytes: &[u8], payload_bytes: &mut Vec<u8>) -> PayloadSlice {
    let start = payload_bytes.len() as u64;
    payload_bytes.extend_from_slice(bytes);
    PayloadSlice {
        start,
        len: bytes.len() as u64,
    }
}

pub fn encode_catalog(catalog: &CompiledMigrationCatalog) -> Result<Vec<u8>, String> {
    serde_json::to_vec(catalog)
        .map_err(|error| format!("failed to serialize migration catalog: {error}"))
}

pub fn decode_catalog(bytes: &[u8]) -> Result<CompiledMigrationCatalog, String> {
    serde_json::from_slice(bytes)
        .map_err(|error| format!("failed to decode migration catalog: {error}"))
}

pub fn checksum_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|value| format!("{value:02x}")).collect()
}

pub fn migration_key_from_version_and_desc(version: i64, description: &str) -> String {
    format!("{version:04}_{}", description.replace(' ', "_"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_scope_matches_install_kind() {
        assert!(StepScope::All.applies_to(MigrationInstallKind::FreshInstall));
        assert!(StepScope::All.applies_to(MigrationInstallKind::Upgrade));
        assert!(StepScope::NewInstallOnly.applies_to(MigrationInstallKind::FreshInstall));
        assert!(!StepScope::NewInstallOnly.applies_to(MigrationInstallKind::Upgrade));
        assert!(StepScope::UpgradeOnly.applies_to(MigrationInstallKind::Upgrade));
        assert!(!StepScope::UpgradeOnly.applies_to(MigrationInstallKind::FreshInstall));
    }
}
