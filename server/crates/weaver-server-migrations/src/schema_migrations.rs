// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{AssertSqlSafe, Row, SqlitePool};

use crate::migration_assets::{
    CompiledBaseline, CompiledMigration, CompiledMigrationCatalog, CompiledMigrationStep,
    EngineScope, MigrationInstallKind,
};
use crate::{EMBEDDED_MIGRATION_CATALOG, EMBEDDED_MIGRATION_PAYLOAD};
use weaver_server_db::persistence::StateError;

pub trait SqliteMigrationHooks {
    fn run_committed_prework(
        pool: &SqlitePool,
        migration: &CompiledMigration,
    ) -> impl std::future::Future<Output = Result<(), StateError>>;
    fn run_rust_hook(
        hook_id: &str,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        version: i64,
        install_kind: MigrationInstallKind,
    ) -> impl std::future::Future<Output = Result<(), StateError>>;
}

const WEAVER_SCHEMA_OBJECTS_SQL: &str = r#"
SELECT COUNT(*)
  FROM sqlite_master
 WHERE name IN (
    'schema_version',
    'settings',
    'servers',
    'job_history',
    'active_jobs',
    'job_events',
    'integration_events',
    'rss_feeds'
 )
"#;

#[derive(Debug, Clone)]
struct MigrationLedgerRow {
    version: i64,
    description: String,
    success: bool,
    checksum: Vec<u8>,
    checksum_algo: String,
}

pub fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

pub fn sqlite_connect_options(path: &Path) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5_000))
        .pragma("auto_vacuum", "INCREMENTAL")
        .pragma("journal_mode", "WAL")
        .pragma("synchronous", "NORMAL")
        .pragma("cache_size", "-16000")
        .pragma("mmap_size", "16777216")
        .pragma("temp_store", "MEMORY")
}

pub async fn run_embedded_migrations<H: SqliteMigrationHooks>(
    pool: &SqlitePool,
    validate_only: bool,
) -> Result<(), StateError> {
    let catalog = embedded_catalog()?;
    if !validate_only {
        ensure_migration_ledger_shape(pool).await?;
    }

    let applied = load_applied_migrations(pool).await?;
    let stale_line_endings = validate_known_migrations(&applied, &catalog)?;
    if !validate_only {
        heal_line_ending_checksums(pool, &catalog, &stale_line_endings).await?;
    }
    let pending = list_pending_migrations_from_applied(&applied, &catalog);
    if pending.is_empty() {
        return Ok(());
    }
    if validate_only {
        return Err(StateError::Database(format!(
            "database migration check failed; pending migrations: {}",
            pending.join(", ")
        )));
    }

    let payload_bytes = embedded_payload_bytes()?;
    validate_payload_checksum(&catalog, &payload_bytes)?;
    let install_kind = detect_install_kind(pool, &applied).await?;
    match install_kind {
        MigrationInstallKind::FreshInstall => {
            replay_catalog_into_fresh_db::<H>(pool, &catalog, &payload_bytes, None, true).await?;
        }
        MigrationInstallKind::Upgrade => {
            apply_version_range::<H>(
                pool,
                &catalog,
                &payload_bytes,
                MigrationInstallKind::Upgrade,
                catalog.starting_version,
                catalog.max_version(),
            )
            .await?;
        }
    }

    Ok(())
}

pub async fn replay_catalog_into_fresh_db<H: SqliteMigrationHooks>(
    pool: &SqlitePool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    through_version: Option<i64>,
    enable_baselines: bool,
) -> Result<(), StateError> {
    ensure_migration_ledger_shape(pool).await?;
    let applied = load_applied_migrations(pool).await?;
    if !applied.is_empty() || has_weaver_schema_objects(pool).await? {
        return Err(StateError::Database(
            "replay_catalog_into_fresh_db requires an empty database".to_string(),
        ));
    }

    let target_version = through_version.unwrap_or_else(|| catalog.max_version());
    if target_version < catalog.starting_version {
        return Ok(());
    }

    let mut start_version = catalog.starting_version;
    // Scope the baseline lookup to SQLite, mirroring the Postgres runner
    // (postgres_migrations.rs passes EngineScope::Postgres). The engine-agnostic
    // `latest_baseline_at_or_below` requests `EngineScope::All`, which matches no
    // engine-tagged baseline, so it silently returned None and replayed every
    // migration individually.
    if enable_baselines
        && let Some(baseline) =
            catalog.latest_baseline_at_or_below_for_engine(target_version, EngineScope::Sqlite)
    {
        apply_baseline(pool, catalog, payload_bytes, baseline).await?;
        start_version = baseline.through_version + 1;
    }

    apply_version_range::<H>(
        pool,
        catalog,
        payload_bytes,
        MigrationInstallKind::FreshInstall,
        start_version,
        target_version,
    )
    .await
}

pub fn embedded_catalog() -> Result<CompiledMigrationCatalog, StateError> {
    let bytes = zstd::stream::decode_all(EMBEDDED_MIGRATION_CATALOG).map_err(|error| {
        StateError::Database(format!("failed to decompress migration catalog: {error}"))
    })?;
    crate::migration_assets::decode_catalog(&bytes).map_err(StateError::Database)
}

pub fn embedded_payload_bytes() -> Result<Vec<u8>, StateError> {
    zstd::stream::decode_all(EMBEDDED_MIGRATION_PAYLOAD).map_err(|error| {
        StateError::Database(format!("failed to decompress migration payload: {error}"))
    })
}

pub fn validate_payload_checksum(
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
) -> Result<(), StateError> {
    let actual = catalog.payload_checksum_algo.digest(payload_bytes);
    if actual == catalog.payload_checksum {
        Ok(())
    } else {
        Err(StateError::Database(
            "embedded migration payload checksum mismatch".to_string(),
        ))
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
pub async fn execute_sql(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sql: &str,
) -> Result<(), StateError> {
    if sql.trim().is_empty() {
        return Ok(());
    }
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    Ok(())
}

pub async fn query_schema_version(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<Option<i64>, StateError> {
    sqlx::query_scalar("SELECT version FROM schema_version LIMIT 1")
        .fetch_optional(&mut **tx)
        .await
        .map_err(db_err)
}

pub async fn ensure_column(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), StateError> {
    let exists_sql = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let exists: i64 = sqlx::query_scalar(AssertSqlSafe(exists_sql.as_str()))
        .bind(column)
        .fetch_one(&mut **tx)
        .await
        .map_err(db_err)?;
    if exists == 0 {
        execute_sql(
            tx,
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition};"),
        )
        .await?;
    }
    Ok(())
}

async fn has_weaver_schema_objects(pool: &SqlitePool) -> Result<bool, StateError> {
    let count: i64 = sqlx::query_scalar(WEAVER_SCHEMA_OBJECTS_SQL)
        .fetch_one(pool)
        .await
        .map_err(db_err)?;
    Ok(count > 0)
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
pub async fn ensure_migration_ledger_shape(pool: &SqlitePool) -> Result<(), StateError> {
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL,
    checksum_algo TEXT NOT NULL
)
        "#,
    )
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn load_applied_migrations(pool: &SqlitePool) -> Result<Vec<MigrationLedgerRow>, StateError> {
    let rows = sqlx::query(
        "SELECT version, description, success, checksum, checksum_algo
           FROM _sqlx_migrations
          ORDER BY version ASC",
    )
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    rows.into_iter()
        .map(|row| {
            Ok(MigrationLedgerRow {
                version: row.try_get("version").map_err(db_err)?,
                description: row.try_get("description").map_err(db_err)?,
                success: row.try_get("success").map_err(db_err)?,
                checksum: row.try_get("checksum").map_err(db_err)?,
                checksum_algo: row.try_get("checksum_algo").map_err(db_err)?,
            })
        })
        .collect()
}

// Highest migration version recorded in the ledger, or `None` when no ledger
// exists yet.
//
// Read *before* a migration run, this answers "which release last wrote to
// this database": no ledger means nothing has ever migrated it (a database
// this process is about to create), while a recorded maximum names the newest
// migration the previous binary shipped. That is the only reliable way to tell
// a fresh install from an upgrade of a specific older line, because the data a
// database holds says nothing about which version wrote it.
//
// Deliberately not filtered by `success`: a recorded-but-failed row still
// proves the binary that wrote it reached that version, and counting it can
// only make a database look newer than it is, never older.
pub async fn max_recorded_migration_version(pool: &SqlitePool) -> Result<Option<i64>, StateError> {
    if !migration_ledger_exists(pool).await? {
        return Ok(None);
    }
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(pool)
        .await
        .map_err(db_err)
}

async fn migration_ledger_exists(pool: &SqlitePool) -> Result<bool, StateError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await
    .map_err(db_err)?;
    Ok(count > 0)
}

fn list_pending_migrations_from_applied(
    applied: &[MigrationLedgerRow],
    catalog: &CompiledMigrationCatalog,
) -> Vec<String> {
    catalog.pending_keys(
        applied
            .iter()
            .filter(|row| row.success)
            .map(|row| row.version),
    )
}

// Checks the ledger against the embedded catalog and returns the versions
// whose recorded checksum is a legacy line-ending variant of the embedded one.
// Those rows are valid but stale: callers that may write should heal them with
// [`heal_line_ending_checksums`] so the next startup takes the plain path.
fn validate_known_migrations(
    applied: &[MigrationLedgerRow],
    catalog: &CompiledMigrationCatalog,
) -> Result<Vec<i64>, StateError> {
    let mut stale_line_endings = Vec::new();
    for row in applied {
        if !row.success {
            return Err(StateError::Database(format!(
                "database has failed migration version {} recorded in _sqlx_migrations; manual recovery required before startup can continue",
                row.version
            )));
        }
        let Some(migration) = catalog.find_migration(row.version) else {
            return Err(StateError::Database(format!(
                "database has unknown migration version {}",
                row.version
            )));
        };
        if row.description != migration.description {
            return Err(StateError::Database(format!(
                "database migration {} description mismatch: ledger `{}`, embedded `{}`",
                row.version, row.description, migration.description
            )));
        }
        if row.checksum_algo != migration.checksum_algo.as_str() {
            return Err(StateError::Database(format!(
                "database migration {} checksum algorithm mismatch: ledger `{}`, embedded `{}`",
                row.version,
                row.checksum_algo,
                migration.checksum_algo.as_str()
            )));
        }
        if row.success && row.checksum != migration.checksum {
            if migration.is_legacy_line_ending_checksum(&row.checksum_algo, &row.checksum) {
                stale_line_endings.push(row.version);
            } else if !crate::migration_assets::is_superseded_migration_checksum(
                row.version,
                &row.checksum_algo,
                &row.checksum,
            ) {
                return Err(StateError::Database(format!(
                    "database migration {} checksum mismatch",
                    row.version
                )));
            }
        }
    }
    Ok(stale_line_endings)
}

// Rewrites the ledger checksum of `versions` to the embedded canonical value.
//
// These rows were written by a build whose checkout carried the other line
// ending (GitHub's Windows runner checks out with `core.autocrlf=true`), so
// they hash the same SQL in a different form. Healing them keeps a database
// from bouncing between the released binary and a from-source build.
#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
async fn heal_line_ending_checksums(
    pool: &SqlitePool,
    catalog: &CompiledMigrationCatalog,
    versions: &[i64],
) -> Result<(), StateError> {
    for version in versions {
        let Some(migration) = catalog.find_migration(*version) else {
            continue;
        };
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ?1 WHERE version = ?2")
            .bind(&migration.checksum)
            .bind(version)
            .execute(pool)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

async fn detect_install_kind(
    pool: &SqlitePool,
    applied: &[MigrationLedgerRow],
) -> Result<MigrationInstallKind, StateError> {
    if !applied.is_empty() {
        return Ok(MigrationInstallKind::Upgrade);
    }
    if !has_weaver_schema_objects(pool).await? {
        Ok(MigrationInstallKind::FreshInstall)
    } else {
        Ok(MigrationInstallKind::Upgrade)
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
async fn apply_baseline(
    pool: &SqlitePool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    baseline: &CompiledBaseline,
) -> Result<(), StateError> {
    let sql = baseline
        .payload
        .text(payload_bytes)
        .map_err(StateError::Database)?
        .to_owned();
    let mut tx = pool.begin().await.map_err(db_err)?;
    if !sql.trim().is_empty() {
        sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }
    for migration in catalog
        .migrations
        .iter()
        .filter(|migration| migration.version <= baseline.through_version)
    {
        insert_applied_migration(&mut tx, migration, 0).await?;
    }
    tx.commit().await.map_err(db_err)?;
    Ok(())
}

pub async fn apply_version_range<H: SqliteMigrationHooks>(
    pool: &SqlitePool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
    start_version: i64,
    target_version: i64,
) -> Result<(), StateError> {
    if target_version < start_version {
        return Ok(());
    }

    let applied_versions: HashSet<i64> = load_applied_migrations(pool)
        .await?
        .into_iter()
        .filter(|row| row.success)
        .map(|row| row.version)
        .collect();

    let unapplied: Vec<&CompiledMigration> = catalog
        .migrations
        .iter()
        .filter(|migration| {
            migration.version >= start_version
                && migration.version <= target_version
                && !applied_versions.contains(&migration.version)
        })
        .collect();
    // Only an upgrade is worth showing: someone is waiting on an install that
    // was already running.
    let mut progress = (matches!(install_kind, MigrationInstallKind::Upgrade)
        && !unapplied.is_empty())
    .then(|| crate::schema_upgrade::UpgradeProgress::start(unapplied.len()));
    for migration in unapplied {
        apply_single_migration::<H>(pool, migration, payload_bytes, install_kind).await?;
        if let Some(progress) = progress.as_mut() {
            progress.advance();
        }
    }
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
pub async fn apply_single_migration<H: SqliteMigrationHooks>(
    pool: &SqlitePool,
    migration: &CompiledMigration,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    let start = Instant::now();
    H::run_committed_prework(pool, migration).await?;
    let mut tx = pool.begin().await.map_err(db_err)?;
    // The pool may hand this migration a connection whose cached schema
    // predates the migrations that ran on another. A statement that reads
    // a table checks the schema cookie and reloads the cache; `ALTER TABLE
    // DROP COLUMN` looks its column up in the cache first and fails on a
    // stale one without that check.
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sqlite_master")
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
    for step in &migration.steps {
        if !step.engine().applies_to(EngineScope::Sqlite) {
            continue;
        }
        if !step.scope().applies_to(install_kind) {
            continue;
        }
        match step {
            CompiledMigrationStep::Sql { payload, .. } => {
                let sql = payload
                    .text(payload_bytes)
                    .map_err(StateError::Database)?
                    .to_owned();
                if !sql.trim().is_empty() {
                    sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
                        .execute(&mut *tx)
                        .await
                        .map_err(db_err)?;
                }
            }
            CompiledMigrationStep::Rust { hook_id, .. } => {
                H::run_rust_hook(hook_id, &mut tx, migration.version, install_kind).await?;
            }
        }
    }
    let elapsed_ns = start.elapsed().as_nanos().min(i64::MAX as u128) as i64;
    insert_applied_migration(&mut tx, migration, elapsed_ns).await?;
    tx.commit().await.map_err(db_err)?;
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
async fn insert_applied_migration(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    migration: &CompiledMigration,
    execution_time: i64,
) -> Result<(), StateError> {
    sqlx::query(
        "INSERT INTO _sqlx_migrations
            (version, description, success, checksum, execution_time, checksum_algo)
         VALUES (?1, ?2, 1, ?3, ?4, ?5)",
    )
    .bind(migration.version)
    .bind(&migration.description)
    .bind(&migration.checksum)
    .bind(execution_time)
    .bind(migration.checksum_algo.as_str())
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;

    mirror_schema_version(tx, migration.version).await
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the migration run, alone at startup before the gate exists"
)]
pub async fn mirror_schema_version(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    version: i64,
) -> Result<(), StateError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    sqlx::query("DELETE FROM schema_version")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    sqlx::query("INSERT INTO schema_version (version) VALUES (?1)")
        .bind(version)
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    Ok(())
}
