// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;
use std::time::Instant;

use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::migration_assets::{
    self, CompiledBaseline, CompiledMigration, CompiledMigrationCatalog, CompiledMigrationStep,
    EngineScope, MigrationInstallKind,
};
use weaver_server_db::persistence::StateError;
pub trait PostgresMigrationHooks {
    fn run_rust_hook(
        hook_id: &str,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        version: i64,
        install_kind: MigrationInstallKind,
    ) -> impl std::future::Future<Output = Result<(), StateError>>;
}

#[derive(Clone, Debug)]
pub struct MigrationStatus {
    pub migration_key: String,
    pub migration_checksum_algo: String,
    pub migration_checksum: String,
    pub applied_at: String,
    pub success: bool,
    pub error_message: Option<String>,
    pub runtime_version: String,
}

#[derive(Clone, Debug)]
struct MigrationLedgerRow {
    version: i64,
    description: String,
    installed_on: String,
    success: bool,
    checksum_algo: String,
    checksum: Vec<u8>,
    runtime_version: String,
    error_message: Option<String>,
}

fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

// Session-level advisory lock key serializing concurrent Postgres migrators
// (ASCII "weaver"). A session lock is connection-bound, so it is held on a
// dedicated pinned connection for the whole migration sequence.
const WEAVER_MIGRATION_ADVISORY_LOCK_KEY: i64 = 0x7765_6176_6572;

pub async fn replay_catalog_into_fresh_db<H: PostgresMigrationHooks>(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    through_version: Option<i64>,
) -> Result<(), StateError> {
    ensure_migration_ledger_shape(pool).await?;

    let applied = load_applied_migrations(pool).await?;
    if !applied.is_empty() || app_object_count(pool).await? > 0 {
        return Err(StateError::Database(
            "replay_catalog_into_fresh_db requires an empty PostgreSQL database".to_string(),
        ));
    }

    let target_version = through_version.unwrap_or_else(|| catalog.max_version());
    if target_version <= 0 {
        return Ok(());
    }

    let baseline = catalog
        .latest_baseline_at_or_below_for_engine(target_version, EngineScope::Postgres)
        .ok_or_else(|| {
            StateError::Database(format!(
                "missing PostgreSQL baseline at or below {target_version:04}"
            ))
        })?;

    apply_postgres_baseline(pool, catalog, payload_bytes, baseline).await?;
    apply_version_range::<H>(
        pool,
        catalog,
        payload_bytes,
        MigrationInstallKind::FreshInstall,
        baseline.through_version + 1,
        target_version,
    )
    .await
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
pub async fn run_migrations<H: PostgresMigrationHooks>(
    pool: &PgPool,
    validate_only: bool,
) -> Result<(), StateError> {
    let catalog = crate::schema_migrations::embedded_catalog()?;

    // Validation is read-only: no ledger DDL, no advisory lock, no mutation.
    if validate_only {
        let applied = load_applied_migrations(pool).await?;
        // Read-only: legacy line-ending checksums are accepted here but healed
        // only by a run that may write.
        validate_known_migrations(&applied, &catalog)?;
        let pending = list_pending_migrations_from_applied(&applied, &catalog);
        if pending.is_empty() {
            return Ok(());
        }
        return Err(StateError::Database(format!(
            "database migration check failed; pending PostgreSQL migrations: {}",
            pending.join(", ")
        )));
    }

    ensure_migration_ledger_shape(pool).await?;

    // Serialize concurrent migrators. A session-level advisory lock is bound to
    // its connection, so pin one connection and hold the lock across the whole
    // sequence (individual steps still `pool.begin()` on other connections; the
    // lock only blocks a second migrator). Release on every exit path.
    let mut lock_conn = pool.acquire().await.map_err(db_err)?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(WEAVER_MIGRATION_ADVISORY_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await
        .map_err(db_err)?;

    let result = run_migrations_locked::<H>(pool, &catalog).await;

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(WEAVER_MIGRATION_ADVISORY_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await;
    drop(lock_conn);

    result
}

// The mutating migration sequence, run while holding the advisory lock. The
// ledger is re-read here (after the lock is acquired) so a migrator that waited
// on the lock observes the winner's completed work and no-ops.
async fn run_migrations_locked<H: PostgresMigrationHooks>(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
) -> Result<(), StateError> {
    let applied = load_applied_migrations(pool).await?;
    let stale_line_endings = validate_known_migrations(&applied, catalog)?;
    heal_line_ending_checksums(pool, catalog, &stale_line_endings).await?;
    let pending = list_pending_migrations_from_applied(&applied, catalog);
    if pending.is_empty() {
        mirror_schema_version_to_latest_successful_migration(pool).await?;
        return Ok(());
    }

    let install_kind = detect_install_kind(pool, &applied).await?;
    let payload_bytes = crate::schema_migrations::embedded_payload_bytes()?;
    // Parity with the SQLite runner: reject a corrupted/mismatched embedded
    // payload before applying any step that slices into it.
    crate::schema_migrations::validate_payload_checksum(catalog, &payload_bytes)?;
    match install_kind {
        MigrationInstallKind::FreshInstall => {
            let target_version = catalog.max_version();
            let baseline = catalog
                .latest_baseline_at_or_below_for_engine(target_version, EngineScope::Postgres)
                .ok_or_else(|| {
                    StateError::Database(format!(
                        "missing PostgreSQL baseline through {target_version:04}"
                    ))
                })?;
            apply_postgres_baseline(pool, catalog, &payload_bytes, baseline).await?;
            apply_version_range::<H>(
                pool,
                catalog,
                &payload_bytes,
                MigrationInstallKind::FreshInstall,
                baseline.through_version + 1,
                target_version,
            )
            .await?;
        }
        MigrationInstallKind::Upgrade => {
            apply_version_range::<H>(
                pool,
                catalog,
                &payload_bytes,
                MigrationInstallKind::Upgrade,
                1,
                catalog.max_version(),
            )
            .await?;
        }
    }

    mirror_schema_version_to_latest_successful_migration(pool).await?;
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
async fn ensure_migration_ledger_shape(pool: &PgPool) -> Result<(), StateError> {
    sqlx::raw_sql(
        r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL,
    checksum_algo TEXT NOT NULL DEFAULT 'blake3',
    runtime_version TEXT NOT NULL DEFAULT '',
    error_message TEXT
)
        "#,
    )
    .execute(pool)
    .await
    .map_err(db_err)?;

    for sql in [
        "ALTER TABLE _sqlx_migrations
             ADD COLUMN IF NOT EXISTS checksum_algo TEXT NOT NULL DEFAULT 'blake3'",
        "ALTER TABLE _sqlx_migrations
             ADD COLUMN IF NOT EXISTS runtime_version TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE _sqlx_migrations
             ADD COLUMN IF NOT EXISTS error_message TEXT",
    ] {
        sqlx::raw_sql(sql).execute(pool).await.map_err(db_err)?;
    }

    Ok(())
}

async fn migration_table_exists(pool: &PgPool) -> Result<bool, StateError> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
              FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name = '_sqlx_migrations'
        )",
    )
    .fetch_one(pool)
    .await
    .map_err(db_err)
}

// PostgreSQL twin of [`crate::schema_migrations::max_recorded_migration_version`],
// with the same contract: `None` when no ledger exists yet, otherwise the
// highest version any row records regardless of success.
pub async fn max_recorded_migration_version(pool: &PgPool) -> Result<Option<i64>, StateError> {
    if !migration_table_exists(pool).await? {
        return Ok(None);
    }
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(pool)
        .await
        .map_err(db_err)
}

async fn load_applied_migrations(pool: &PgPool) -> Result<Vec<MigrationLedgerRow>, StateError> {
    if !migration_table_exists(pool).await? {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        "SELECT
             version,
             description,
	             installed_on::TEXT AS installed_on,
	             success,
	             checksum,
	             COALESCE(NULLIF(BTRIM(checksum_algo), ''), 'blake3') AS checksum_algo,
	             COALESCE(runtime_version, '') AS runtime_version,
	             error_message
	         FROM _sqlx_migrations
	         ORDER BY version",
    )
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    rows.into_iter()
        .map(|row| {
            Ok(MigrationLedgerRow {
                version: row.try_get("version").map_err(db_err)?,
                description: row.try_get("description").map_err(db_err)?,
                installed_on: row.try_get("installed_on").map_err(db_err)?,
                success: row.try_get("success").map_err(db_err)?,
                checksum: row.try_get("checksum").map_err(db_err)?,
                checksum_algo: row.try_get("checksum_algo").map_err(db_err)?,
                runtime_version: row.try_get("runtime_version").map_err(db_err)?,
                error_message: row.try_get("error_message").map_err(db_err)?,
            })
        })
        .collect()
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
async fn mirror_schema_version_to_latest_successful_migration(
    pool: &PgPool,
) -> Result<(), StateError> {
    let Some(version) = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE",
    )
    .fetch_one(pool)
    .await
    .map_err(db_err)?
    else {
        return Ok(());
    };

    let mut tx = pool.begin().await.map_err(db_err)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version BIGINT NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    sqlx::query("DELETE FROM schema_version")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    sqlx::query("INSERT INTO schema_version (version) VALUES ($1)")
        .bind(version)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(())
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
// [`heal_line_ending_checksums`].
fn validate_known_migrations(
    applied: &[MigrationLedgerRow],
    catalog: &CompiledMigrationCatalog,
) -> Result<Vec<i64>, StateError> {
    let max_supported_version = catalog.max_version();
    let mut unknown = Vec::new();
    let mut invalid_checksum = Vec::new();
    let mut stale_line_endings = Vec::new();

    for row in applied {
        if !row.success {
            return Err(StateError::Database(format!(
                "PostgreSQL migration {} was not applied successfully",
                migration_assets::migration_key_from_version_and_desc(
                    row.version,
                    &row.description
                )
            )));
        }

        let key =
            migration_assets::migration_key_from_version_and_desc(row.version, &row.description);
        let Some(expected) = catalog.find_migration(row.version) else {
            if row.version > max_supported_version {
                unknown.push(key);
            }
            continue;
        };

        let checksum_matches = row.checksum_algo == expected.checksum_algo.as_str()
            && row.checksum == expected.checksum;
        if !checksum_matches {
            if expected.is_legacy_line_ending_checksum(&row.checksum_algo, &row.checksum) {
                stale_line_endings.push(row.version);
            } else if !migration_assets::is_superseded_migration_checksum(
                row.version,
                &row.checksum_algo,
                &row.checksum,
            ) {
                invalid_checksum.push(key);
            }
        }
    }

    if !invalid_checksum.is_empty() {
        return Err(StateError::Database(format!(
            "checksum mismatch for PostgreSQL migrations: {}",
            invalid_checksum.join(", ")
        )));
    }

    if !unknown.is_empty() {
        return Err(StateError::Database(format!(
            "PostgreSQL migrations newer than supported ({max_supported_version}): {}. Please update Weaver.",
            unknown.join(", ")
        )));
    }

    Ok(stale_line_endings)
}

// Rewrites the ledger checksum of `versions` to the embedded canonical value.
//
// These rows were written by a build whose checkout carried the other line
// ending (GitHub's Windows runner checks out with `core.autocrlf=true`), so
// they hash the same SQL in a different form.
#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
async fn heal_line_ending_checksums(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
    versions: &[i64],
) -> Result<(), StateError> {
    for version in versions {
        let Some(migration) = catalog.find_migration(*version) else {
            continue;
        };
        sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = $2")
            .bind(&migration.checksum)
            .bind(version)
            .execute(pool)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

async fn detect_install_kind(
    pool: &PgPool,
    applied: &[MigrationLedgerRow],
) -> Result<MigrationInstallKind, StateError> {
    if !applied.is_empty() {
        return Ok(MigrationInstallKind::Upgrade);
    }

    let app_objects = app_object_count(pool).await?;
    if app_objects == 0 {
        Ok(MigrationInstallKind::FreshInstall)
    } else {
        Err(StateError::Database(
            "PostgreSQL database contains application schema or data but has no applied migration ledger".to_string(),
        ))
    }
}

async fn app_object_count(pool: &PgPool) -> Result<i64, StateError> {
    sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM information_schema.tables
          WHERE table_schema = current_schema()
            AND table_name NOT LIKE '_sqlx_%'",
    )
    .fetch_one(pool)
    .await
    .map_err(db_err)
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
async fn apply_postgres_baseline(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    baseline: &CompiledBaseline,
) -> Result<(), StateError> {
    let mut tx = pool.begin().await.map_err(db_err)?;
    let baseline_sql = baseline
        .payload
        .text(payload_bytes)
        .map_err(StateError::Database)?;

    sqlx::raw_sql(AssertSqlSafe(baseline_sql))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

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

async fn apply_version_range<H: PostgresMigrationHooks>(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
    start_version: i64,
    target_version: i64,
) -> Result<(), StateError> {
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
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
async fn apply_single_migration<H: PostgresMigrationHooks>(
    pool: &PgPool,
    migration: &CompiledMigration,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    let start = Instant::now();
    let mut tx = pool.begin().await.map_err(db_err)?;

    for step in &migration.steps {
        if !step.engine().applies_to(EngineScope::Postgres)
            || !step.scope().applies_to(install_kind)
        {
            continue;
        }

        match step {
            CompiledMigrationStep::Sql { payload, .. } => {
                let sql = payload
                    .text(payload_bytes)
                    .map_err(StateError::Database)?
                    .to_owned();
                if sql.trim().is_empty() {
                    continue;
                }
                sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        StateError::Database(format!(
                            "failed to apply PostgreSQL migration {:04}: {error}",
                            migration.version
                        ))
                    })?;
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
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
async fn insert_applied_migration(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    migration: &CompiledMigration,
    execution_time: i64,
) -> Result<(), StateError> {
    sqlx::query(
        "INSERT INTO _sqlx_migrations
            (version, description, success, checksum, execution_time, checksum_algo, runtime_version)
         VALUES ($1, $2, TRUE, $3, $4, $5, $6)
         ON CONFLICT (version) DO NOTHING",
    )
    .bind(migration.version)
    .bind(&migration.description)
    .bind(&migration.checksum)
    .bind(execution_time)
    .bind(migration.checksum_algo.as_str())
    .bind(env!("CARGO_PKG_VERSION"))
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

pub async fn list_applied_migrations(pool: &PgPool) -> Result<Vec<MigrationStatus>, StateError> {
    let rows = load_applied_migrations(pool).await?;
    let mut out = Vec::with_capacity(rows.len());

    for row in rows {
        out.push(MigrationStatus {
            migration_key: migration_assets::migration_key_from_version_and_desc(
                row.version,
                &row.description,
            ),
            migration_checksum_algo: row.checksum_algo,
            migration_checksum: migration_assets::checksum_hex(&row.checksum),
            applied_at: row.installed_on,
            success: row.success,
            error_message: row.error_message,
            runtime_version: row.runtime_version,
        });
    }

    Ok(out)
}
