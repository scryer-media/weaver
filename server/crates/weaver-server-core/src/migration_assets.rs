#![allow(dead_code)]

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::range::Range;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EngineScope {
    #[default]
    All,
    Sqlite,
    Postgres,
}

impl EngineScope {
    pub fn applies_to(self, engine: EngineScope) -> bool {
        matches!(self, Self::All) || self == engine
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
        engine: Option<EngineScope>,
        #[serde(default)]
        scope: StepScope,
    },
    Rust {
        hook_id: String,
        #[serde(default)]
        engine: Option<EngineScope>,
        #[serde(default)]
        scope: StepScope,
    },
}

impl SourceMigrationStep {
    fn resolved_engine(&self) -> EngineScope {
        match self {
            Self::Sql { engine, .. } | Self::Rust { engine, .. } => {
                engine.unwrap_or(EngineScope::All)
            }
        }
    }

    fn explicit_engine(&self) -> Option<EngineScope> {
        match self {
            Self::Sql { engine, .. } | Self::Rust { engine, .. } => *engine,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBaseline {
    pub through_version: i64,
    pub file: String,
    #[serde(default)]
    pub engine: EngineScope,
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

    pub(crate) fn pending_keys(
        &self,
        applied_versions: impl IntoIterator<Item = i64>,
    ) -> Vec<String> {
        let applied_versions: HashSet<_> = applied_versions.into_iter().collect();
        self.migrations
            .iter()
            .filter(|migration| !applied_versions.contains(&migration.version))
            .map(|migration| migration.key.clone())
            .collect()
    }

    pub fn find_migration(&self, version: i64) -> Option<&CompiledMigration> {
        self.migrations
            .iter()
            .find(|migration| migration.version == version)
    }

    pub fn latest_baseline_at_or_below(&self, version: i64) -> Option<&CompiledBaseline> {
        self.latest_baseline_at_or_below_for_engine(version, EngineScope::All)
    }

    pub fn latest_baseline_at_or_below_for_engine(
        &self,
        version: i64,
        engine: EngineScope,
    ) -> Option<&CompiledBaseline> {
        self.baselines
            .iter()
            .filter(|baseline| baseline.through_version <= version)
            .filter(|baseline| baseline.engine.applies_to(engine))
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
    /// Checksums this migration would have had under the pre-canonicalization
    /// rule, where the SQL was hashed exactly as it sat on disk.
    ///
    /// `checksum` is now computed over an LF-canonical body, so a build from an
    /// LF checkout and a build from a CRLF checkout agree. Databases written
    /// before that change recorded whichever form their build happened to
    /// embed, so both variants stay acceptable at startup and are healed to the
    /// canonical value in place. Empty when the body has no line breaks at all
    /// (every variant collapses onto `checksum`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legacy_line_ending_checksums: Vec<Vec<u8>>,
    pub steps: Vec<CompiledMigrationStep>,
}

impl CompiledMigration {
    /// True when `checksum` (under algorithm `checksum_algo`) is a ledger value
    /// this migration used to produce before the checksum was made independent
    /// of line endings. Callers that accept one must rewrite the ledger to
    /// `self.checksum`.
    pub(crate) fn is_legacy_line_ending_checksum(
        &self,
        checksum_algo: &str,
        checksum: &[u8],
    ) -> bool {
        checksum_algo == self.checksum_algo.as_str()
            && self
                .legacy_line_ending_checksums
                .iter()
                .any(|legacy| legacy.as_slice() == checksum)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompiledMigrationStep {
    Sql {
        file: String,
        engine: EngineScope,
        scope: StepScope,
        payload: PayloadSlice,
    },
    Rust {
        hook_id: String,
        engine: EngineScope,
        scope: StepScope,
    },
}

impl CompiledMigrationStep {
    pub fn scope(&self) -> StepScope {
        match self {
            Self::Sql { scope, .. } | Self::Rust { scope, .. } => *scope,
        }
    }

    pub fn engine(&self) -> EngineScope {
        match self {
            Self::Sql { engine, .. } | Self::Rust { engine, .. } => *engine,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledBaseline {
    pub through_version: i64,
    pub file: String,
    pub engine: EngineScope,
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
        let range = Range {
            start,
            end: start
                .checked_add(len)
                .ok_or_else(|| "payload slice overflow".to_string())?,
        };
        payload_bytes
            .get(range)
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
        #[serde(skip_serializing_if = "Option::is_none")]
        engine: Option<EngineScope>,
        scope: StepScope,
        sql_blake3: String,
    },
    Rust {
        #[serde(skip_serializing_if = "Option::is_none")]
        engine: Option<EngineScope>,
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
        if !baseline_versions.insert((baseline.through_version, baseline.engine)) {
            return Err(format!(
                "duplicate baseline entry for version {:04} and engine {:?}",
                baseline.through_version, baseline.engine
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
            engine: baseline.engine,
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
    let mut step_sources = Vec::with_capacity(migration.steps.len());
    for step in &migration.steps {
        match step {
            SourceMigrationStep::Sql { file, scope, .. } => {
                let engine = step.resolved_engine();
                let path = db_root.join(file);
                let sql = fs::read(&path)
                    .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
                let payload = push_payload(&sql, payload_bytes);
                compiled_steps.push(CompiledMigrationStep::Sql {
                    file: file.clone(),
                    engine,
                    scope: *scope,
                    payload,
                });
                step_sources.push(CanonicalStepSource::Sql {
                    engine: step.explicit_engine(),
                    scope: *scope,
                    sql,
                });
            }
            SourceMigrationStep::Rust { hook_id, scope, .. } => {
                let engine = step.resolved_engine();
                migration_hook_ids::validate_migration_hook_id(hook_id)?;
                compiled_steps.push(CompiledMigrationStep::Rust {
                    hook_id: hook_id.clone(),
                    engine,
                    scope: *scope,
                });
                step_sources.push(CanonicalStepSource::Rust {
                    engine: step.explicit_engine(),
                    scope: *scope,
                    hook_id: hook_id.clone(),
                });
            }
        }
    }

    // The checksum is taken over the LF-canonical body so that a checkout with
    // `core.autocrlf=true` (GitHub's Windows runner) and an LF checkout of the
    // same commit agree. The CRLF and raw-LF forms are recorded alongside it
    // because databases written before this change recorded one of them.
    let checksum = migration_checksum(migration, &step_sources, SqlLineEndings::Canonical)?;
    let mut legacy_line_ending_checksums = Vec::new();
    for form in [SqlLineEndings::Lf, SqlLineEndings::Crlf] {
        let legacy = migration_checksum(migration, &step_sources, form)?;
        if legacy != checksum && !legacy_line_ending_checksums.contains(&legacy) {
            legacy_line_ending_checksums.push(legacy);
        }
    }

    let key = migration_key_from_version_and_desc(migration.version, &migration.description);
    let filename = infer_filename(migration, &key);
    Ok(CompiledMigration {
        version: migration.version,
        description: migration.description.clone(),
        key,
        filename,
        checksum_algo: migration.checksum_algo,
        checksum,
        legacy_line_ending_checksums,
        steps: compiled_steps,
    })
}

/// Which line-ending form a SQL body is hashed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlLineEndings {
    /// LF-canonical: every CRLF collapsed to LF. What `checksum` uses.
    Canonical,
    /// The body exactly as an LF checkout stores it. Identical to `Canonical`
    /// in practice, and computed separately so the legacy set stays explicit
    /// rather than implied.
    Lf,
    /// The body as a `core.autocrlf=true` checkout stores it: every LF written
    /// as CRLF.
    Crlf,
}

impl SqlLineEndings {
    fn apply(self, sql: &[u8]) -> Vec<u8> {
        let canonical = to_lf(sql);
        match self {
            Self::Canonical | Self::Lf => canonical,
            Self::Crlf => to_crlf(&canonical),
        }
    }
}

/// Drops the CR of every CRLF pair, leaving a lone CR (which `core.autocrlf`
/// never introduces) untouched.
fn to_lf(sql: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(sql.len());
    let mut index = 0;
    while index < sql.len() {
        if sql[index] == b'\r' && sql.get(index + 1) == Some(&b'\n') {
            index += 1;
            continue;
        }
        out.push(sql[index]);
        index += 1;
    }
    out
}

/// Expands every LF of an already-LF-canonical body back to CRLF.
fn to_crlf(canonical: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(canonical.len());
    for byte in canonical {
        if *byte == b'\n' {
            out.push(b'\r');
        }
        out.push(*byte);
    }
    out
}

enum CanonicalStepSource {
    Sql {
        engine: Option<EngineScope>,
        scope: StepScope,
        sql: Vec<u8>,
    },
    Rust {
        engine: Option<EngineScope>,
        scope: StepScope,
        hook_id: String,
    },
}

fn migration_checksum(
    migration: &SourceMigration,
    step_sources: &[CanonicalStepSource],
    line_endings: SqlLineEndings,
) -> Result<Vec<u8>, String> {
    let steps = step_sources
        .iter()
        .map(|source| match source {
            CanonicalStepSource::Sql { engine, scope, sql } => CanonicalStep::Sql {
                engine: *engine,
                scope: *scope,
                sql_blake3: checksum_hex(
                    &ChecksumAlgorithm::Blake3.digest(&line_endings.apply(sql)),
                ),
            },
            CanonicalStepSource::Rust {
                engine,
                scope,
                hook_id,
            } => CanonicalStep::Rust {
                engine: *engine,
                scope: *scope,
                hook_id: hook_id.clone(),
            },
        })
        .collect();

    let canonical = CanonicalMigration {
        version: migration.version,
        description: migration.description.clone(),
        steps,
    };
    let canonical_bytes = serde_json::to_vec(&canonical).map_err(|error| {
        format!(
            "failed to serialize canonical migration {:04}: {error}",
            migration.version
        )
    })?;
    Ok(migration.checksum_algo.digest(&canonical_bytes))
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

/// Historical per-migration ledger checksums that must remain acceptable after a
/// migration's already-shipped payload is deliberately superseded in place.
///
/// The per-migration checksum is derived from the migration's SQL (see
/// `compile_migration`) and is persisted in every install's `_sqlx_migrations`
/// ledger when the migration is applied. On every subsequent startup the runners
/// (`schema_migrations::validate_known_migrations` and
/// `postgres_migrations::validate_known_migrations`) compare the ledger value
/// against the freshly recomputed embedded value. Editing a released migration's
/// payload therefore changes its checksum and would otherwise brick every install
/// that already applied it with a "checksum mismatch" hard failure.
///
/// Each entry here whitelists exactly one prior `(version, algo, checksum)` for a
/// migration whose payload we intentionally rewrote. This only ever *additionally
/// accepts* a specific known-old value for a specific version; it never weakens
/// detection of any other unexpected/corrupt ledger checksum.
///
/// Entry rationale:
/// - v27 "history poll indexes": shipped in weaver-v0.6.9. Its Postgres backfill
///   used `h.metadata::jsonb` / `jsonb_array_elements(...)`, which raise on
///   corrupt (unparseable or non-array) `job_history.metadata` rows and abort the
///   upgrade. The payload was rewritten to skip such rows (matching the SQLite
///   payload). Because the per-migration checksum spans both the SQLite and
///   Postgres steps, editing the Postgres step shifts the checksum for BOTH
///   engines' installs, so this one entry protects v27 SQLite and Postgres alike.
const SUPERSEDED_MIGRATION_LEDGER_CHECKSUMS: &[(i64, ChecksumAlgorithm, &str)] = &[(
    27,
    ChecksumAlgorithm::Blake3,
    "e05da91d94e32687581efb95f0cbeb0562d3aa5617d74b2d3254395f3ea1d286",
)];

/// Returns true if `checksum` (with algorithm `checksum_algo`) is an explicitly
/// whitelisted historical ledger checksum for migration `version` whose payload
/// was superseded in place. Used by the runners to avoid bricking installs that
/// already applied the pre-edit payload.
pub(crate) fn is_superseded_migration_checksum(
    version: i64,
    checksum_algo: &str,
    checksum: &[u8],
) -> bool {
    let actual_hex = checksum_hex(checksum);
    SUPERSEDED_MIGRATION_LEDGER_CHECKSUMS
        .iter()
        .any(|(entry_version, entry_algo, entry_hex)| {
            *entry_version == version
                && entry_algo.as_str() == checksum_algo
                && entry_hex.eq_ignore_ascii_case(&actual_hex)
        })
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

    #[test]
    fn engine_scope_matches_requested_engine() {
        assert!(EngineScope::All.applies_to(EngineScope::Sqlite));
        assert!(EngineScope::All.applies_to(EngineScope::Postgres));
        assert!(EngineScope::Sqlite.applies_to(EngineScope::Sqlite));
        assert!(!EngineScope::Sqlite.applies_to(EngineScope::Postgres));
        assert!(EngineScope::Postgres.applies_to(EngineScope::Postgres));
        assert!(!EngineScope::Postgres.applies_to(EngineScope::Sqlite));
    }

    #[test]
    fn source_step_defaults_missing_engine_to_all() {
        let manifest = r#"
format_version = 1
starting_version = 21

[[migration]]
version = 21
description = "missing engine"

[[migration.steps]]
kind = "sql"
file = "0021.sql"
"#;
        let parsed = toml::from_str::<SourceMigrationManifest>(manifest).unwrap();
        match &parsed.migrations[0].steps[0] {
            SourceMigrationStep::Sql { engine, scope, .. } => {
                assert_eq!(*engine, None);
                assert_eq!(*scope, StepScope::All);
                assert_eq!(
                    parsed.migrations[0].steps[0].resolved_engine(),
                    EngineScope::All
                );
            }
            other => panic!("expected sql step, got {other:?}"),
        }
    }

    #[test]
    fn catalog_baseline_lookup_filters_by_engine() {
        let sqlite_payload = PayloadSlice { start: 0, len: 1 };
        let postgres_payload = PayloadSlice { start: 1, len: 1 };
        let catalog = CompiledMigrationCatalog {
            format_version: DEFAULT_MANIFEST_VERSION,
            starting_version: 21,
            payload_checksum_algo: ChecksumAlgorithm::Blake3,
            payload_checksum: Vec::new(),
            migrations: Vec::new(),
            baselines: vec![
                CompiledBaseline {
                    through_version: 25,
                    file: "sqlite.sql".into(),
                    engine: EngineScope::Sqlite,
                    payload: sqlite_payload,
                },
                CompiledBaseline {
                    through_version: 25,
                    file: "postgres.sql".into(),
                    engine: EngineScope::Postgres,
                    payload: postgres_payload,
                },
            ],
        };

        assert_eq!(
            catalog
                .latest_baseline_at_or_below_for_engine(25, EngineScope::Sqlite)
                .unwrap()
                .file,
            "sqlite.sql"
        );
        assert_eq!(
            catalog
                .latest_baseline_at_or_below_for_engine(25, EngineScope::Postgres)
                .unwrap()
                .file,
            "postgres.sql"
        );
    }

    #[test]
    fn source_bundle_registers_postgres_current_baseline() {
        let db_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/db");
        let bundle = compile_source_bundle(&db_root).unwrap();
        let baseline = bundle
            .catalog
            .latest_baseline_at_or_below_for_engine(25, EngineScope::Postgres)
            .unwrap();

        assert_eq!(baseline.through_version, 25);
        assert_eq!(baseline.file, "postgres/baselines/0025_baseline.sql");
    }

    const LINE_ENDING_MANIFEST: &str = r#"
format_version = 1
starting_version = 21

[[migration]]
version = 21
description = "line ending probe"

[[migration.steps]]
kind = "sql"
file = "0021.sql"
"#;

    /// Writes a one-migration source tree whose SQL body carries `line_ending`,
    /// standing in for the two ways a checkout can land the same commit.
    fn line_ending_source_tree(body: &str, line_ending: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("migrations")).unwrap();
        fs::write(
            source_manifest_path(dir.path()),
            LINE_ENDING_MANIFEST.replace('\n', line_ending),
        )
        .unwrap();
        fs::write(
            dir.path().join("0021.sql"),
            body.replace('\n', line_ending).into_bytes(),
        )
        .unwrap();
        dir
    }

    fn compiled_probe(body: &str, line_ending: &str) -> CompiledMigration {
        let dir = line_ending_source_tree(body, line_ending);
        let mut bundle = compile_source_bundle(dir.path()).unwrap();
        bundle.catalog.migrations.remove(0)
    }

    #[test]
    fn crlf_and_lf_migration_bodies_share_one_checksum() {
        let body = "CREATE TABLE probe (id INTEGER PRIMARY KEY);\nDROP TABLE probe;\n";
        let lf = compiled_probe(body, "\n");
        let crlf = compiled_probe(body, "\r\n");

        // The whole point: a Windows runner checkout (core.autocrlf=true) and an
        // LF checkout of the same commit must record the same ledger value.
        assert_eq!(
            checksum_hex(&lf.checksum),
            checksum_hex(&crlf.checksum),
            "CRLF and LF bodies must hash identically"
        );
        // Both builds must also offer the same amnesty set, so either one can
        // open a database the other wrote before the canonicalization.
        assert_eq!(
            lf.legacy_line_ending_checksums,
            crlf.legacy_line_ending_checksums
        );
        assert_eq!(
            lf.legacy_line_ending_checksums.len(),
            1,
            "a multi-line body has exactly one non-canonical legacy form"
        );
        assert!(
            !lf.legacy_line_ending_checksums.contains(&lf.checksum),
            "the canonical value is never listed as legacy"
        );
    }

    #[test]
    fn legacy_amnesty_covers_the_crlf_ledger_value_only() {
        let body = "CREATE TABLE probe (id INTEGER PRIMARY KEY);\nDROP TABLE probe;\n";
        let migration = compiled_probe(body, "\n");
        let legacy = migration.legacy_line_ending_checksums[0].clone();

        assert!(migration.is_legacy_line_ending_checksum("blake3", &legacy));
        // A different algorithm name, or any other value, is not amnestied: this
        // must not become a general escape hatch from checksum validation.
        assert!(!migration.is_legacy_line_ending_checksum("sha256", &legacy));
        assert!(!migration.is_legacy_line_ending_checksum("blake3", &[0u8; 32]));

        // A genuinely different body shares neither the canonical value nor the
        // amnesty.
        let edited = compiled_probe(
            "CREATE TABLE probe (id INTEGER PRIMARY KEY, extra TEXT);\nDROP TABLE probe;\n",
            "\n",
        );
        assert_ne!(migration.checksum, edited.checksum);
        assert!(!edited.is_legacy_line_ending_checksum("blake3", &legacy));
        assert!(!migration.is_legacy_line_ending_checksum("blake3", &edited.checksum));
    }

    #[test]
    fn canonicalization_only_touches_crlf_pairs() {
        assert_eq!(to_lf(b"a\r\nb\r\n"), b"a\nb\n");
        assert_eq!(to_lf(b"a\nb\n"), b"a\nb\n");
        // A lone CR is data, not a line ending core.autocrlf would have written.
        assert_eq!(to_lf(b"a\rb"), b"a\rb");
        assert_eq!(to_crlf(b"a\nb\n"), b"a\r\nb\r\n");
    }
}
