#![allow(dead_code)]

use std::collections::HashSet;
use std::time::Instant;

use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::StateError;
use crate::migration_assets::{
    self, CompiledBaseline, CompiledMigration, CompiledMigrationCatalog, CompiledMigrationStep,
    EngineScope, MigrationInstallKind,
};
use crate::schema_migrations::MigrationMode;

#[derive(Clone, Debug)]
pub(crate) struct MigrationStatus {
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

/// Session-level advisory lock key serializing concurrent Postgres migrators
/// (ASCII "weaver"). A session lock is connection-bound, so it is held on a
/// dedicated pinned connection for the whole migration sequence.
const WEAVER_MIGRATION_ADVISORY_LOCK_KEY: i64 = 0x7765_6176_6572;

pub(crate) async fn replay_catalog_into_fresh_db(
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
    apply_version_range(
        pool,
        catalog,
        payload_bytes,
        MigrationInstallKind::FreshInstall,
        baseline.through_version + 1,
        target_version,
    )
    .await
}

pub(crate) async fn run_migrations(pool: &PgPool, mode: MigrationMode) -> Result<(), StateError> {
    let catalog = crate::schema_migrations::embedded_catalog()?;

    // Validation is read-only: no ledger DDL, no advisory lock, no mutation.
    if matches!(mode, MigrationMode::ValidateOnly) {
        let applied = load_applied_migrations(pool).await?;
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

    let result = run_migrations_locked(pool, &catalog).await;

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(WEAVER_MIGRATION_ADVISORY_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await;
    drop(lock_conn);

    result
}

/// The mutating migration sequence, run while holding the advisory lock. The
/// ledger is re-read here (after the lock is acquired) so a migrator that waited
/// on the lock observes the winner's completed work and no-ops.
async fn run_migrations_locked(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
) -> Result<(), StateError> {
    let applied = load_applied_migrations(pool).await?;
    validate_known_migrations(&applied, catalog)?;
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
            apply_version_range(
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
            apply_version_range(
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
    let applied_versions: HashSet<i64> = applied
        .iter()
        .filter(|row| row.success)
        .map(|row| row.version)
        .collect();

    catalog
        .migrations
        .iter()
        .filter(|migration| !applied_versions.contains(&migration.version))
        .map(|migration| migration.key.clone())
        .collect()
}

fn validate_known_migrations(
    applied: &[MigrationLedgerRow],
    catalog: &CompiledMigrationCatalog,
) -> Result<(), StateError> {
    let max_supported_version = catalog.max_version();
    let mut unknown = Vec::new();
    let mut invalid_checksum = Vec::new();

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
        if !checksum_matches
            && !migration_assets::is_superseded_migration_checksum(
                row.version,
                &row.checksum_algo,
                &row.checksum,
            )
        {
            invalid_checksum.push(key);
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

async fn apply_version_range(
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

    for migration in catalog.migrations.iter().filter(|migration| {
        migration.version >= start_version && migration.version <= target_version
    }) {
        if applied_versions.contains(&migration.version) {
            continue;
        }
        apply_single_migration(pool, migration, payload_bytes, install_kind).await?;
    }

    Ok(())
}

async fn apply_single_migration(
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
                run_postgres_rust_hook(hook_id, &mut tx, migration.version, install_kind).await?;
            }
        }
    }

    let elapsed_ns = start.elapsed().as_nanos().min(i64::MAX as u128) as i64;
    insert_applied_migration(&mut tx, migration, elapsed_ns).await?;
    tx.commit().await.map_err(db_err)?;
    Ok(())
}

async fn run_postgres_rust_hook(
    hook_id: &str,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    _version: i64,
    _install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    crate::migration_hook_ids::validate_migration_hook_id(hook_id).map_err(StateError::Database)?;
    match hook_id {
        "restart_active_jobs_drop_active_segments_v28" => {
            restart_active_jobs_drop_active_segments_v28(tx).await
        }
        other => Err(StateError::Database(format!(
            "PostgreSQL migration hook '{other}' is not implemented"
        ))),
    }
}

fn implemented_postgres_rust_hooks() -> &'static [&'static str] {
    &["restart_active_jobs_drop_active_segments_v28"]
}

async fn restart_active_jobs_drop_active_segments_v28(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), StateError> {
    for table in [
        "active_file_progress",
        "active_files",
        "active_file_identities",
        "active_par2",
        "active_par2_files",
        "active_extracted",
        "active_failed_extractions",
        "active_extraction_chunks",
        "active_archive_headers",
        "active_rar_volume_facts",
        "active_detected_archives",
        "active_volume_status",
        "active_rar_verified_suspect",
    ] {
        let sql = format!("DELETE FROM {table}");
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(&mut **tx)
            .await
            .map_err(db_err)?;
    }

    sqlx::query(
        "UPDATE active_jobs
         SET download_state = 'queued',
             post_state = 'idle',
             error = NULL,
             queued_repair_at_epoch_ms = NULL,
             queued_extract_at_epoch_ms = NULL,
             paused_resume_status = CASE
                 WHEN status = 'paused' OR run_state = 'paused' THEN 'queued'
                 ELSE NULL
             END,
             paused_resume_download_state = CASE
                 WHEN status = 'paused' OR run_state = 'paused' THEN 'queued'
                 ELSE NULL
             END,
             paused_resume_post_state = CASE
                 WHEN status = 'paused' OR run_state = 'paused' THEN 'idle'
                 ELSE NULL
             END,
             status = CASE
                 WHEN status = 'paused' OR run_state = 'paused' THEN 'paused'
                 ELSE 'queued'
             END,
             run_state = CASE
                 WHEN status = 'paused' OR run_state = 'paused' THEN 'paused'
                 ELSE 'active'
             END",
    )
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;

    sqlx::query("DROP TABLE IF EXISTS active_segments")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;

    Ok(())
}

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

pub(crate) async fn list_applied_migrations(
    pool: &PgPool,
) -> Result<Vec<MigrationStatus>, StateError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_postgres_rust_migrations_require_implemented_hooks() {
        let catalog = crate::schema_migrations::embedded_catalog().unwrap();
        let baseline = catalog
            .latest_baseline_at_or_below_for_engine(catalog.max_version(), EngineScope::Postgres)
            .unwrap();
        let implemented_hooks = implemented_postgres_rust_hooks();
        let mut offenders = Vec::new();

        for migration in catalog
            .migrations
            .iter()
            .filter(|migration| migration.version > baseline.through_version)
        {
            for step in &migration.steps {
                let CompiledMigrationStep::Rust {
                    hook_id, engine, ..
                } = step
                else {
                    continue;
                };
                if engine.applies_to(EngineScope::Postgres)
                    && !implemented_hooks.contains(&hook_id.as_str())
                {
                    offenders.push(format!(
                        "{} uses unimplemented PostgreSQL hook {hook_id}",
                        migration.key
                    ));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "PostgreSQL-applicable Rust migration hooks need explicit implementations: {}",
            offenders.join(", ")
        );
    }

    /// Provisions an isolated schema on the server named by WEAVER_TEST_POSTGRES_URL
    /// and returns an admin pool, the schema name, and a pool whose search_path is
    /// pinned to that schema. Returns None (test skips) when the env var is unset or
    /// empty, mirroring the pattern in persistence/connection/tests.rs.
    async fn create_scoped_pool() -> Option<(PgPool, String, PgPool)> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
            eprintln!("skipping postgres migration test; WEAVER_TEST_POSTGRES_URL is not set");
            return None;
        };
        if base_url.trim().is_empty() {
            eprintln!("skipping postgres migration test; WEAVER_TEST_POSTGRES_URL is empty");
            return None;
        }

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let schema = format!("weaver_mig_test_{}_{}", std::process::id(), suffix);
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin_pool)
            .await
            .unwrap();

        let separator = if base_url.contains('?') { '&' } else { '?' };
        let target_url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
        let scoped_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&target_url)
            .await
            .unwrap();

        Some((admin_pool, schema, scoped_pool))
    }

    async fn insert_v26_history_row(pool: &PgPool, job_id: i64, metadata: Option<&str>) {
        sqlx::query(
            "INSERT INTO job_history
                (job_id, name, status, total_bytes, downloaded_bytes,
                 optional_recovery_bytes, optional_recovery_downloaded_bytes,
                 failed_bytes, health, created_at, completed_at, metadata)
             VALUES ($1, $2, 'complete', 0, 0, 0, 0, 0, 1000, 10, 20, $3)",
        )
        .bind(job_id)
        .bind(format!("job-{job_id}"))
        .bind(metadata)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A Postgres install upgrading from schema <= 26 that carries a corrupt
    /// job_history.metadata row (unparseable text or valid-but-non-array JSON)
    /// must not hard-fail on the 0027/0030 backfills. Corrupt rows contribute no
    /// attributes and are left untouched; a well-formed array row backfills and
    /// has its diagnostic-only keys stripped by 0030.
    #[tokio::test]
    async fn postgres_upgrade_from_v26_tolerates_corrupt_metadata_when_configured() {
        let Some((admin_pool, schema, pool)) = create_scoped_pool().await else {
            return;
        };

        let catalog = crate::schema_migrations::embedded_catalog().unwrap();
        let payload = crate::schema_migrations::embedded_payload_bytes().unwrap();

        // Build a schema stamped exactly through migration 26 (baseline 25 + 26),
        // so run_migrations() below drives the real <=26 -> current upgrade path
        // over pre-existing data, exactly reproducing the reported landmine.
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(26))
            .await
            .unwrap();
        let stamped_max: i64 =
            sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stamped_max, 26, "test fixture must start at schema 26");

        // Row 1: well-formed metadata array with a public attribute plus the two
        // diagnostic-only keys that 0027 excludes and 0030 strips.
        let good_metadata = serde_json::to_string(&vec![
            ("*scryer_title_id".to_string(), "title-a".to_string()),
            (
                "__weaver_diagnostic_source_job_id".to_string(),
                "42".to_string(),
            ),
            (
                "__weaver_diagnostic_include_server_hostnames".to_string(),
                "false".to_string(),
            ),
        ])
        .unwrap();
        insert_v26_history_row(&pool, 1, Some(&good_metadata)).await;
        // Row 2: unparseable text - `::jsonb` would raise here pre-fix.
        insert_v26_history_row(&pool, 2, Some("this is not json{")).await;
        // Row 3: valid JSON but an object, not an array - jsonb_array_elements and
        // jsonb_typeof(...::jsonb) would each mishandle/raise pre-fix.
        insert_v26_history_row(&pool, 3, Some(r#"{"unexpected":"object"}"#)).await;
        // Row 4: NULL metadata (the common case) must be untouched.
        insert_v26_history_row(&pool, 4, None).await;

        // The whole <=26 -> current upgrade must succeed despite the corrupt rows.
        run_migrations(&pool, MigrationMode::Apply)
            .await
            .expect("upgrade across corrupt metadata rows must not fail");

        let schema_version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(schema_version, catalog.max_version());

        // Only the good array row contributed an attribute, and the diagnostic
        // keys were excluded (0027) / stripped (0030).
        let attrs: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT job_id, key, value FROM job_history_attributes ORDER BY job_id, key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            attrs,
            vec![(1_i64, "*scryer_title_id".to_string(), "title-a".to_string())],
            "corrupt rows must contribute no attributes; good row keeps only its public attr"
        );

        // 0030 rewrote the good row's metadata to drop the diagnostic keys, while
        // leaving the corrupt/non-array/NULL rows byte-for-byte untouched.
        let good_after: String =
            sqlx::query_scalar("SELECT metadata FROM job_history WHERE job_id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let good_pairs: Vec<(String, String)> = serde_json::from_str(&good_after).unwrap();
        assert_eq!(
            good_pairs,
            vec![("*scryer_title_id".to_string(), "title-a".to_string())]
        );

        let bad_text: String =
            sqlx::query_scalar("SELECT metadata FROM job_history WHERE job_id = 2")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(bad_text, "this is not json{");

        let object_text: String =
            sqlx::query_scalar("SELECT metadata FROM job_history WHERE job_id = 3")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(object_text, r#"{"unexpected":"object"}"#);

        let null_meta: Option<String> =
            sqlx::query_scalar("SELECT metadata FROM job_history WHERE job_id = 4")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(null_meta, None);

        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin_pool)
            .await
            .unwrap();
        admin_pool.close().await;
    }

    /// A weaver-v0.6.9 Postgres install already applied migration 27 and stored
    /// its pre-edit checksum. After 0027's payload is edited in place the embedded
    /// checksum differs, so without the supersede amnesty startup validation would
    /// hard-fail with "checksum mismatch". This proves the amnesty lets such an
    /// install continue and no-op cleanly.
    #[tokio::test]
    async fn postgres_supersede_amnesty_accepts_pre_edit_v27_ledger_when_configured() {
        let Some((admin_pool, schema, pool)) = create_scoped_pool().await else {
            return;
        };

        let catalog = crate::schema_migrations::embedded_catalog().unwrap();
        let payload = crate::schema_migrations::embedded_payload_bytes().unwrap();

        // Fully migrate to current, then rewrite the migration-27 ledger checksum
        // to the pre-edit weaver-v0.6.9 value to emulate an install that applied
        // the old payload before the fix shipped.
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, None)
            .await
            .unwrap();

        const V27_PRE_EDIT_CHECKSUM_HEX: &str =
            "e05da91d94e32687581efb95f0cbeb0562d3aa5617d74b2d3254395f3ea1d286";
        let old_checksum = (0..V27_PRE_EDIT_CHECKSUM_HEX.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&V27_PRE_EDIT_CHECKSUM_HEX[i..i + 2], 16).unwrap())
            .collect::<Vec<u8>>();
        // Sanity: the current embedded checksum must actually differ, otherwise
        // this test proves nothing.
        assert_ne!(
            catalog.find_migration(27).unwrap().checksum,
            old_checksum,
            "embedded v27 checksum unexpectedly equals the pre-edit value"
        );
        sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 27")
            .bind(&old_checksum)
            .execute(&pool)
            .await
            .unwrap();

        // Read-only validation must accept the superseded checksum...
        run_migrations(&pool, MigrationMode::ValidateOnly)
            .await
            .expect("validate-only must accept the superseded v27 ledger checksum");
        // ...and a normal startup run must no-op without a checksum-mismatch error.
        run_migrations(&pool, MigrationMode::Apply)
            .await
            .expect("apply must accept the superseded v27 ledger checksum");

        // A genuinely wrong checksum for a different migration must still be
        // rejected, confirming the amnesty is narrow.
        sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 26")
            .bind(vec![0xAB_u8; 32])
            .execute(&pool)
            .await
            .unwrap();
        let err = run_migrations(&pool, MigrationMode::ValidateOnly)
            .await
            .expect_err("a corrupted v26 checksum must still be rejected");
        assert!(
            err.to_string().contains("checksum mismatch"),
            "unexpected error: {err}"
        );

        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin_pool)
            .await
            .unwrap();
        admin_pool.close().await;
    }
}
