use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::Instant;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, Row, SqlitePool};

use crate::StateError;
use crate::migration_assets::{
    CompiledBaseline, CompiledMigration, CompiledMigrationCatalog, CompiledMigrationStep,
    EngineScope, MigrationInstallKind,
};
use crate::migration_hook_ids;

const EMBEDDED_MIGRATION_CATALOG: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/migration_catalog.json.zst"));
const EMBEDDED_MIGRATION_PAYLOAD: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/migration_payload.bin.zst"));
const MIGRATION_21_BASE_SCHEMA_SQL: &str =
    include_str!("db/migrations/0021_legacy_schema_parity/fresh.sql");
const MIGRATION_22_SCHEMA_SQL: &str =
    include_str!("db/migrations/0022_diagnostic_and_async_state/schema.sql");
const LEGACY_SCHEMA_VERSION: i64 = 20;
const CURRENT_SCHEMA_VERSION: i64 = 29;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    Apply,
    ValidateOnly,
}

#[derive(Debug, Clone)]
struct MigrationLedgerRow {
    version: i64,
    description: String,
    success: bool,
    checksum: Vec<u8>,
    checksum_algo: String,
}

fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

pub fn run_embedded_migrations_on_path_blocking(path: &Path) -> Result<(), StateError> {
    let path = path.to_path_buf();
    let handle = std::thread::spawn(move || run_embedded_migrations_on_path_thread(path));
    handle
        .join()
        .map_err(|_| StateError::Database("schema migration worker thread panicked".to_string()))?
}

fn run_embedded_migrations_on_path_thread(path: PathBuf) -> Result<(), StateError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(db_err)?;
    runtime.block_on(async move {
        let options = sqlite_connect_options(&path);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(db_err)?;
        let result = run_embedded_migrations(&pool, MigrationMode::Apply).await;
        pool.close().await;
        result
    })
}

fn sqlite_connect_options(path: &Path) -> SqliteConnectOptions {
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

pub async fn run_embedded_migrations(
    pool: &SqlitePool,
    mode: MigrationMode,
) -> Result<(), StateError> {
    let catalog = embedded_catalog()?;
    if !matches!(mode, MigrationMode::ValidateOnly) {
        ensure_migration_ledger_shape(pool).await?;
    }

    let applied = load_applied_migrations(pool).await?;
    validate_known_migrations(&applied, &catalog)?;
    let pending = list_pending_migrations_from_applied(&applied, &catalog);
    if pending.is_empty() {
        return Ok(());
    }
    if matches!(mode, MigrationMode::ValidateOnly) {
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
            replay_catalog_into_fresh_db(pool, &catalog, &payload_bytes, None, true).await?;
        }
        MigrationInstallKind::Upgrade => {
            apply_version_range(
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

pub async fn replay_catalog_into_fresh_db(
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
    if enable_baselines && let Some(baseline) = catalog.latest_baseline_at_or_below(target_version)
    {
        apply_baseline(pool, catalog, payload_bytes, baseline).await?;
        start_version = baseline.through_version + 1;
    }

    apply_version_range(
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

pub(crate) fn embedded_payload_bytes() -> Result<Vec<u8>, StateError> {
    zstd::stream::decode_all(EMBEDDED_MIGRATION_PAYLOAD).map_err(|error| {
        StateError::Database(format!("failed to decompress migration payload: {error}"))
    })
}

fn validate_payload_checksum(
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

async fn execute_sql(
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

async fn query_schema_version(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<Option<i64>, StateError> {
    sqlx::query_scalar("SELECT version FROM schema_version LIMIT 1")
        .fetch_optional(&mut **tx)
        .await
        .map_err(db_err)
}

async fn ensure_column(
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

async fn ensure_migration_ledger_shape(pool: &SqlitePool) -> Result<(), StateError> {
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
            return Err(StateError::Database(format!(
                "database migration {} checksum mismatch",
                row.version
            )));
        }
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

async fn apply_version_range(
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
    pool: &SqlitePool,
    migration: &CompiledMigration,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    let start = Instant::now();
    let mut tx = pool.begin().await.map_err(db_err)?;
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
                run_rust_hook(hook_id, &mut tx, migration.version, install_kind).await?;
            }
        }
    }
    let elapsed_ns = start.elapsed().as_nanos().min(i64::MAX as u128) as i64;
    insert_applied_migration(&mut tx, migration, elapsed_ns).await?;
    tx.commit().await.map_err(db_err)?;
    Ok(())
}

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

async fn mirror_schema_version(
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

async fn cleanup_legacy_queue_event_storage(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StateError> {
    sqlx::query("DELETE FROM integration_events")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    sqlx::query("DELETE FROM sqlite_sequence WHERE name = 'integration_events'")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    sqlx::query("DROP TABLE IF EXISTS metrics_scrapes")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn backfill_persisted_nzb_blobs(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
) -> Result<(), StateError> {
    let select_sql = format!(
        "SELECT job_id, nzb_path
           FROM {table}
          WHERE nzb_zstd IS NULL
            AND nzb_path IS NOT NULL
            AND nzb_path != ''"
    );
    let rows = sqlx::query(AssertSqlSafe(select_sql.as_str()))
        .fetch_all(&mut **tx)
        .await
        .map_err(db_err)?;

    let mut updates = Vec::new();
    for row in rows {
        let job_id: i64 = row.try_get("job_id").map_err(db_err)?;
        let path: String = row.try_get("nzb_path").map_err(db_err)?;
        match crate::ingest::load_persisted_nzb_storage_bytes(Path::new(&path)) {
            Ok(bytes) => updates.push((job_id, bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    table,
                    job_id,
                    nzb_path = %path,
                    error = %error,
                    "failed to backfill persisted nzb blob from filesystem"
                );
            }
        }
    }

    let update_sql = format!(
        "UPDATE {table}
            SET nzb_zstd = ?1
          WHERE job_id = ?2"
    );
    for (job_id, bytes) in updates {
        sqlx::query(AssertSqlSafe(update_sql.as_str()))
            .bind(bytes)
            .bind(job_id)
            .execute(&mut **tx)
            .await
            .map_err(db_err)?;
    }

    Ok(())
}

async fn refresh_active_job_hashes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StateError> {
    let rows = sqlx::query(
        "SELECT job_id, nzb_zstd, nzb_path
           FROM active_jobs",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(db_err)?;

    for row in rows {
        let job_id: i64 = row.try_get("job_id").map_err(db_err)?;
        let nzb_zstd: Option<Vec<u8>> = row.try_get("nzb_zstd").map_err(db_err)?;
        let nzb_path: String = row.try_get("nzb_path").map_err(db_err)?;
        let hash = match nzb_zstd {
            Some(bytes) => crate::ingest::hash_persisted_nzb_bytes(&bytes).to_vec(),
            None => match crate::ingest::load_persisted_nzb_storage_bytes(Path::new(&nzb_path)) {
                Ok(bytes) => crate::ingest::hash_persisted_nzb_bytes(&bytes).to_vec(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    tracing::warn!(
                        job_id,
                        nzb_path = %nzb_path,
                        error = %error,
                        "failed to refresh active job hash from filesystem"
                    );
                    continue;
                }
            },
        };
        sqlx::query(
            "UPDATE active_jobs
                SET nzb_hash = ?1
              WHERE job_id = ?2",
        )
        .bind(hash)
        .bind(job_id)
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    }

    Ok(())
}

async fn backfill_job_history_hashes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StateError> {
    let rows = sqlx::query(
        "SELECT job_id, nzb_zstd, nzb_path
           FROM job_history
          WHERE job_hash IS NULL
            AND ((nzb_zstd IS NOT NULL) OR (nzb_path IS NOT NULL AND nzb_path != ''))",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(db_err)?;

    for row in rows {
        let job_id: i64 = row.try_get("job_id").map_err(db_err)?;
        let nzb_zstd: Option<Vec<u8>> = row.try_get("nzb_zstd").map_err(db_err)?;
        let nzb_path: Option<String> = row.try_get("nzb_path").map_err(db_err)?;
        let hash = match nzb_zstd {
            Some(bytes) => crate::ingest::hash_persisted_nzb_bytes(&bytes).to_vec(),
            None => {
                let Some(path) = nzb_path else {
                    continue;
                };
                match crate::ingest::load_persisted_nzb_storage_bytes(Path::new(&path)) {
                    Ok(bytes) => crate::ingest::hash_persisted_nzb_bytes(&bytes).to_vec(),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        tracing::warn!(
                            job_id,
                            nzb_path = %path,
                            error = %error,
                            "failed to backfill history job hash from filesystem"
                        );
                        continue;
                    }
                }
            }
        };
        sqlx::query(
            "UPDATE job_history
                SET job_hash = ?1
              WHERE job_id = ?2",
        )
        .bind(hash)
        .bind(job_id)
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    }

    Ok(())
}

async fn run_rust_hook(
    hook_id: &str,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    version: i64,
    install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    migration_hook_ids::validate_migration_hook_id(hook_id).map_err(StateError::Database)?;
    match hook_id {
        "adopt_legacy_schema_to_21" => adopt_legacy_schema_to_21(tx, version, install_kind).await,
        "upgrade_to_schema_22" => upgrade_to_schema_22(tx).await,
        "upgrade_to_schema_23" => upgrade_to_schema_23(tx).await,
        "upgrade_to_schema_25" => upgrade_to_schema_25(tx).await,
        "restart_active_jobs_drop_active_segments_v28" => {
            restart_active_jobs_drop_active_segments_v28(tx).await
        }
        other => Err(StateError::Database(format!(
            "unknown migration hook id '{other}'"
        ))),
    }
}

async fn adopt_legacy_schema_to_21(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    _version: i64,
    install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    if install_kind != MigrationInstallKind::Upgrade {
        return Err(StateError::Database(
            "adopt_legacy_schema_to_21 can only run during upgrade".to_string(),
        ));
    }
    execute_sql(tx, MIGRATION_21_BASE_SCHEMA_SQL).await?;

    let legacy_version = query_schema_version(tx).await?;
    let Some(legacy_version) = legacy_version else {
        return Err(StateError::Database(format!(
            "legacy schema adoption requires an existing schema_version row, found {legacy_version:?}"
        )));
    };

    match legacy_version {
        1 => {
            // The legacy synchronous SQLite path used VACUUM here to retroactively apply
            // incremental auto_vacuum. Transactional sqlx adoption cannot issue
            // VACUUM, so we preserve the schema/data transition and let runtime
            // maintenance handle compaction separately.
            mirror_schema_version(tx, LEGACY_SCHEMA_VERSION).await?;
        }
        2..=4 => {
            ensure_column(
                tx,
                "active_extraction_chunks",
                "appended",
                "INTEGER NOT NULL DEFAULT 0",
            )
            .await?;
            ensure_column(tx, "servers", "priority", "INTEGER NOT NULL DEFAULT 0").await?;
            mirror_schema_version(tx, LEGACY_SCHEMA_VERSION).await?;
        }
        5 => {
            ensure_column(tx, "servers", "priority", "INTEGER NOT NULL DEFAULT 0").await?;
            mirror_schema_version(tx, LEGACY_SCHEMA_VERSION).await?;
        }
        6..=18 => {
            mirror_schema_version(tx, LEGACY_SCHEMA_VERSION).await?;
        }
        19..=CURRENT_SCHEMA_VERSION => {}
        other => {
            return Err(StateError::Database(format!(
                "unsupported legacy schema version {other} (expected <= {CURRENT_SCHEMA_VERSION})"
            )));
        }
    }

    ensure_column(
        tx,
        "job_history",
        "optional_recovery_bytes",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        tx,
        "job_history",
        "optional_recovery_downloaded_bytes",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        tx,
        "active_extraction_chunks",
        "start_offset",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        tx,
        "active_extraction_chunks",
        "end_offset",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        tx,
        "active_jobs",
        "normalization_retried",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(tx, "active_jobs", "queued_repair_at_epoch_ms", "REAL").await?;
    ensure_column(tx, "active_jobs", "queued_extract_at_epoch_ms", "REAL").await?;
    ensure_column(tx, "active_jobs", "paused_resume_status", "TEXT").await?;
    ensure_column(tx, "active_jobs", "download_state", "TEXT").await?;
    ensure_column(tx, "active_jobs", "post_state", "TEXT").await?;
    ensure_column(tx, "active_jobs", "run_state", "TEXT").await?;
    ensure_column(tx, "active_jobs", "paused_resume_download_state", "TEXT").await?;
    ensure_column(tx, "active_jobs", "paused_resume_post_state", "TEXT").await?;
    ensure_column(tx, "servers", "tls_ca_cert", "TEXT").await?;

    if legacy_version <= LEGACY_SCHEMA_VERSION {
        cleanup_legacy_queue_event_storage(tx).await?;
    }

    Ok(())
}

async fn upgrade_to_schema_22(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StateError> {
    execute_sql(tx, MIGRATION_22_SCHEMA_SQL).await?;
    ensure_column(tx, "job_history", "last_diagnostic_id", "TEXT").await?;
    ensure_column(
        tx,
        "job_history",
        "last_diagnostic_uploaded_at_epoch_ms",
        "INTEGER",
    )
    .await?;
    Ok(())
}

async fn upgrade_to_schema_23(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StateError> {
    ensure_column(tx, "active_jobs", "nzb_zstd", "BLOB").await?;
    ensure_column(tx, "job_history", "nzb_zstd", "BLOB").await?;
    backfill_persisted_nzb_blobs(tx, "active_jobs").await?;
    backfill_persisted_nzb_blobs(tx, "job_history").await?;
    Ok(())
}

async fn upgrade_to_schema_25(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StateError> {
    ensure_column(tx, "job_history", "job_hash", "BLOB").await?;
    ensure_column(tx, "active_jobs", "nzb_zstd", "BLOB").await?;
    ensure_column(tx, "job_history", "nzb_zstd", "BLOB").await?;
    backfill_persisted_nzb_blobs(tx, "active_jobs").await?;
    backfill_persisted_nzb_blobs(tx, "job_history").await?;
    refresh_active_job_hashes(tx).await?;
    backfill_job_history_hashes(tx).await?;
    Ok(())
}

async fn restart_active_jobs_drop_active_segments_v28(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration_assets::{
        ChecksumAlgorithm, CompiledMigration, CompiledMigrationStep, EngineScope, PayloadSlice,
        StepScope,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use std::time::{SystemTime, UNIX_EPOCH};

    type PausedActiveJobRow = (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<f64>,
        Option<f64>,
        Option<String>,
        Option<String>,
        Option<String>,
    );

    async fn open_test_pool(path: &Path) -> sqlx::SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_connect_options(path))
            .await
            .unwrap()
    }

    async fn execute_batch(pool: &sqlx::SqlitePool, sql: &str) {
        let mut tx = pool.begin().await.unwrap();
        execute_sql(&mut tx, sql).await.unwrap();
        tx.commit().await.unwrap();
    }

    async fn sqlite_table_exists(pool: &sqlx::SqlitePool, table: &str) -> bool {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap();
        count > 0
    }

    #[tokio::test]
    async fn fresh_install_uses_baseline_and_stamps_version() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();

        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(21), true)
            .await
            .unwrap();

        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 21);
        let stamped: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stamped, 1);
    }

    #[tokio::test]
    async fn fresh_install_runs_remaining_migrations_to_current_schema() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let expected_migrations = embedded_catalog().unwrap().migrations.len() as i64;

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();

        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);

        let stamped: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stamped, expected_migrations);

        let tls_ca_cert_cols: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('servers') WHERE name = 'tls_ca_cert'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(tls_ca_cert_cols, 1);

        let diagnostic_cols: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('job_history')
             WHERE name IN ('last_diagnostic_id', 'last_diagnostic_uploaded_at_epoch_ms')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(diagnostic_cols, 2);

        let nzb_cols: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('active_jobs') WHERE name = 'nzb_zstd'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(nzb_cols, 1);
        assert!(!sqlite_table_exists(&pool, "active_segments").await);
    }

    #[tokio::test]
    async fn sqlite_v28_restarts_active_jobs_and_drops_active_segments() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(27), true)
            .await
            .unwrap();
        assert!(sqlite_table_exists(&pool, "active_segments").await);

        execute_batch(
            &pool,
            "INSERT INTO active_jobs (
                 job_id, nzb_hash, nzb_path, output_dir, status, download_state, post_state,
                 run_state, error, created_at, queued_repair_at_epoch_ms,
                 queued_extract_at_epoch_ms, paused_resume_status,
                 paused_resume_download_state, paused_resume_post_state, nzb_zstd
             ) VALUES
                 (1, X'010203', '/tmp/a.nzb', '/tmp/a', 'downloading', 'fetching',
                  'repairing', 'active', 'boom', 100, 11.0, 12.0, NULL, NULL, NULL, X'01'),
                 (2, X'040506', '/tmp/b.nzb', '/tmp/b', 'paused', 'fetching',
                  'extracting', 'paused', 'wait', 200, 21.0, 22.0, 'checking',
                  'checking', 'extracting', X'02');
             INSERT INTO active_segments VALUES (1, 0, 0, 0, 10, 123);
             INSERT INTO active_file_progress VALUES (1, 0, 10);
             INSERT INTO active_files VALUES (1, 0, 'payload.bin', X'01010101010101010101010101010101');
             INSERT INTO active_par2 VALUES (1, 8, 1);
             INSERT INTO active_par2_files VALUES (1, 0, 'repair.par2', 1, 1);
             INSERT INTO active_extracted (job_id, member_name, output_path, output_size)
                 VALUES (1, 'payload.bin', '/tmp/a/payload.bin', 10);
             INSERT INTO active_failed_extractions VALUES (1, 'bad.bin');
             INSERT INTO active_extraction_chunks (
                 job_id, set_name, member_name, volume_index, bytes_written,
                 temp_path, start_offset, end_offset, verified, appended
             ) VALUES (1, 'set', 'payload.bin', 0, 10, '/tmp/chunk', 0, 10, 1, 1);
             INSERT INTO active_archive_headers VALUES (1, 'set', X'AA');
             INSERT INTO active_rar_volume_facts VALUES (1, 'set', 0, X'BB');
             INSERT INTO active_detected_archives VALUES (1, 0, 'rar', 'set', 0);
             INSERT INTO active_volume_status VALUES (1, 'set', 0, 1, 1, 0);
             INSERT INTO active_rar_verified_suspect VALUES (1, 'set', 0);
             INSERT INTO active_file_identities (
                 job_id, file_index, source_filename, current_filename,
                 classification_source
             ) VALUES (1, 0, 'payload.bin', 'payload.bin', 'declared');",
        )
        .await;

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();

        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);
        assert!(!sqlite_table_exists(&pool, "active_segments").await);

        for table in [
            "active_file_progress",
            "active_files",
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
            "active_file_identities",
        ] {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = sqlx::query_scalar(AssertSqlSafe(sql.as_str()))
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "{table} should be cleared");
        }

        let active: (
            String,
            String,
            String,
            String,
            Option<String>,
            Option<f64>,
            Option<f64>,
        ) = sqlx::query_as(
            "SELECT status, download_state, post_state, run_state, error,
                        queued_repair_at_epoch_ms, queued_extract_at_epoch_ms
                 FROM active_jobs WHERE job_id = 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            active,
            (
                "queued".to_string(),
                "queued".to_string(),
                "idle".to_string(),
                "active".to_string(),
                None,
                None,
                None,
            )
        );

        let paused: PausedActiveJobRow = sqlx::query_as(
            "SELECT status, download_state, post_state, run_state, error,
                    queued_repair_at_epoch_ms, queued_extract_at_epoch_ms,
                    paused_resume_status, paused_resume_download_state,
                    paused_resume_post_state
             FROM active_jobs WHERE job_id = 2",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            paused,
            (
                "paused".to_string(),
                "queued".to_string(),
                "idle".to_string(),
                "paused".to_string(),
                None,
                None,
                None,
                Some("queued".to_string()),
                Some("queued".to_string()),
                Some("idle".to_string()),
            )
        );

        let mut tx = pool.begin().await.unwrap();
        restart_active_jobs_drop_active_segments_v28(&mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_history_attribute_migration_backfills_public_metadata() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(26), true)
            .await
            .unwrap();

        let metadata = serde_json::to_string(&vec![
            ("*scryer_title_id".to_string(), "title-a".to_string()),
            (
                crate::history::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                "req-1".to_string(),
            ),
            (
                crate::history::DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY.to_string(),
                "42".to_string(),
            ),
        ])
        .unwrap();
        sqlx::query(
            "INSERT INTO job_history
                (job_id, name, status, total_bytes, downloaded_bytes,
                 optional_recovery_bytes, optional_recovery_downloaded_bytes,
                 failed_bytes, health, created_at, completed_at, metadata)
             VALUES (1, 'history', 'complete', 1, 1, 0, 0, 0, 1000, 10, 20, ?)",
        )
        .bind(metadata)
        .execute(&pool)
        .await
        .unwrap();

        apply_version_range(
            &pool,
            &catalog,
            &payload,
            MigrationInstallKind::Upgrade,
            27,
            27,
        )
        .await
        .unwrap();

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key, value FROM job_history_attributes ORDER BY key")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![("*scryer_title_id".to_string(), "title-a".to_string())]
        );
    }

    #[tokio::test]
    async fn direct_upgrade_from_v3_adds_legacy_columns() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("legacy-v3.db");
        let pool = open_test_pool(&db_path).await;
        execute_batch(
            &pool,
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (3);
             CREATE TABLE servers (
                 id                  INTEGER PRIMARY KEY NOT NULL,
                 host                TEXT NOT NULL,
                 port                INTEGER NOT NULL,
                 tls                 INTEGER NOT NULL DEFAULT 1,
                 username            TEXT,
                 password            TEXT,
                 connections         INTEGER NOT NULL DEFAULT 10,
                 active              INTEGER NOT NULL DEFAULT 1,
                 supports_pipelining INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE active_extraction_chunks (
                 job_id        INTEGER NOT NULL,
                 set_name      TEXT NOT NULL,
                 member_name   TEXT NOT NULL,
                 volume_index  INTEGER NOT NULL,
                 bytes_written INTEGER NOT NULL,
                 temp_path     TEXT NOT NULL,
                 verified      INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (job_id, set_name, member_name, volume_index)
              ) WITHOUT ROWID;",
        )
        .await;
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);

        assert_eq!(
            pragma_column_count(&pool, "servers", &["priority", "tls_ca_cert"]).await,
            2
        );
        assert_eq!(
            pragma_column_count(
                &pool,
                "active_extraction_chunks",
                &["appended", "start_offset", "end_offset"]
            )
            .await,
            3
        );
    }

    #[tokio::test]
    async fn direct_upgrade_from_v20_runs_cleanup_and_backfill() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("legacy-v20.db");
        let nzb_path = temp.path().join("fixture.nzb");
        std::fs::write(&nzb_path, b"<nzb>fixture</nzb>").unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let metrics_cutoff_epoch_sec =
            now - crate::operations::metrics_store::RAW_METRICS_RETENTION_SECS;

        let pool = open_test_pool(&db_path).await;
        execute_batch(
            &pool,
            "PRAGMA journal_mode=WAL;
             CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (20);
             CREATE TABLE servers (
                 id                  INTEGER PRIMARY KEY NOT NULL,
                 host                TEXT NOT NULL,
                 port                INTEGER NOT NULL,
                 tls                 INTEGER NOT NULL DEFAULT 1,
                 username            TEXT,
                 password            TEXT,
                 connections         INTEGER NOT NULL DEFAULT 10,
                 active              INTEGER NOT NULL DEFAULT 1,
                 supports_pipelining INTEGER NOT NULL DEFAULT 0,
                 priority            INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE job_history (
                 job_id           INTEGER PRIMARY KEY NOT NULL,
                 name             TEXT NOT NULL,
                 status           TEXT NOT NULL,
                 error_message    TEXT,
                 total_bytes      INTEGER NOT NULL DEFAULT 0,
                 downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                 failed_bytes     INTEGER NOT NULL DEFAULT 0,
                 health           INTEGER NOT NULL DEFAULT 1000,
                 category         TEXT,
                 output_dir       TEXT,
                 nzb_path         TEXT,
                 created_at       INTEGER NOT NULL,
                 completed_at     INTEGER NOT NULL,
                 metadata         TEXT
             );
             CREATE TABLE active_jobs (
                 job_id       INTEGER PRIMARY KEY NOT NULL,
                 nzb_hash     BLOB NOT NULL,
                 nzb_path     TEXT NOT NULL,
                 output_dir   TEXT NOT NULL,
                 status       TEXT NOT NULL DEFAULT 'downloading',
                 error        TEXT,
                 created_at   INTEGER NOT NULL,
                 category     TEXT,
                 metadata     TEXT
             );
             CREATE TABLE integration_events (
                 id           INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp    INTEGER NOT NULL,
                 kind         TEXT NOT NULL,
                 item_id      INTEGER,
                 payload_json TEXT NOT NULL
             );
              CREATE TABLE metrics_scrapes (
                  scraped_at_epoch_sec INTEGER PRIMARY KEY NOT NULL,
                  body_zstd            BLOB NOT NULL
              ) WITHOUT ROWID;",
        )
        .await;
        sqlx::query(
            "INSERT INTO servers (
                 id, host, port, tls, username, password, connections, active, supports_pipelining, priority
             ) VALUES (?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)",
        )
        .bind(1_i64)
        .bind("news.example.com")
        .bind(563_i64)
        .bind(1_i64)
        .bind(20_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_history (
                 job_id, name, status, error_message, total_bytes, downloaded_bytes,
                 failed_bytes, health, category, output_dir, nzb_path, created_at,
                 completed_at, metadata
             ) VALUES (
                 ?, ?, ?, NULL, 0, 0, 0, 1000, NULL, NULL, ?, ?, ?, NULL
             )",
        )
        .bind(41_i64)
        .bind("history")
        .bind("complete")
        .bind(nzb_path.to_string_lossy().to_string())
        .bind(now - 120)
        .bind(now - 60)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO active_jobs (
                 job_id, nzb_hash, nzb_path, output_dir, status, error, created_at, category, metadata
             ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL)",
        )
        .bind(42_i64)
        .bind(vec![1_u8, 2_u8, 3_u8])
        .bind(nzb_path.to_string_lossy().to_string())
        .bind(temp.path().join("out").to_string_lossy().to_string())
        .bind("downloading")
        .bind(now - 30)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO integration_events (timestamp, kind, item_id, payload_json)
             VALUES (?, ?, ?, ?)",
        )
        .bind(now * 1000)
        .bind("ITEM_CREATED")
        .bind(7_i64)
        .bind("{\"kind\":\"ITEM_CREATED\"}")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO metrics_scrapes (scraped_at_epoch_sec, body_zstd) VALUES (?, ?)")
            .bind(metrics_cutoff_epoch_sec - 60)
            .bind(vec![1_u8])
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO metrics_scrapes (scraped_at_epoch_sec, body_zstd) VALUES (?, ?)")
            .bind(metrics_cutoff_epoch_sec + 60)
            .bind(vec![2_u8])
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let expected_nzb_bytes =
            crate::ingest::load_persisted_nzb_storage_bytes(&nzb_path).unwrap();
        let pool = open_test_pool(&db_path).await;

        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);

        let stamped: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stamped, embedded_catalog().unwrap().migrations.len() as i64);

        let integration_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM integration_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(integration_events, 0);

        let legacy_metrics_tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'metrics_scrapes'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(legacy_metrics_tables, 0);

        let retained_metrics_history_chunks: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM metrics_history_chunks")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retained_metrics_history_chunks, 0);

        assert_eq!(
            pragma_column_count(
                &pool,
                "job_history",
                &[
                    "optional_recovery_bytes",
                    "optional_recovery_downloaded_bytes",
                    "last_diagnostic_id",
                    "last_diagnostic_uploaded_at_epoch_ms",
                    "nzb_zstd"
                ]
            )
            .await,
            5
        );
        assert_eq!(
            pragma_column_count(
                &pool,
                "active_jobs",
                &[
                    "normalization_retried",
                    "queued_repair_at_epoch_ms",
                    "queued_extract_at_epoch_ms",
                    "paused_resume_status",
                    "download_state",
                    "post_state",
                    "run_state",
                    "paused_resume_download_state",
                    "paused_resume_post_state",
                    "nzb_zstd"
                ]
            )
            .await,
            10
        );

        let job_history_nzb: Vec<u8> =
            sqlx::query_scalar("SELECT nzb_zstd FROM job_history WHERE job_id = 41")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(job_history_nzb, expected_nzb_bytes);

        let active_job_nzb: Vec<u8> =
            sqlx::query_scalar("SELECT nzb_zstd FROM active_jobs WHERE job_id = 42")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(active_job_nzb, expected_nzb_bytes);
    }

    #[test]
    fn embedded_payload_checksum_validates() {
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        validate_payload_checksum(&catalog, &payload).unwrap();
    }

    #[tokio::test]
    async fn sqlite_migration_replay_skips_postgres_only_steps() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        ensure_migration_ledger_shape(&pool).await.unwrap();

        let payload = b"CREATE TABLE postgres_only_marker (id INTEGER PRIMARY KEY);";
        let migration = CompiledMigration {
            version: 9_999,
            description: "postgres only synthetic step".to_string(),
            key: "9999_postgres_only_synthetic_step".to_string(),
            filename: "synthetic.sql".to_string(),
            checksum_algo: ChecksumAlgorithm::Blake3,
            checksum: ChecksumAlgorithm::Blake3.digest(payload),
            steps: vec![CompiledMigrationStep::Sql {
                file: "synthetic.sql".to_string(),
                engine: EngineScope::Postgres,
                scope: StepScope::All,
                payload: PayloadSlice {
                    start: 0,
                    len: payload.len() as u64,
                },
            }],
        };

        apply_single_migration(
            &pool,
            &migration,
            payload,
            MigrationInstallKind::FreshInstall,
        )
        .await
        .unwrap();

        let marker_tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
               FROM sqlite_master
              WHERE type = 'table'
                AND name = 'postgres_only_marker'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(marker_tables, 0);

        let stamped: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 9999")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stamped, 1);
    }

    #[tokio::test]
    async fn apply_rejects_failed_migration_rows_before_replay() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        ensure_migration_ledger_shape(&pool).await.unwrap();
        let migration = catalog.find_migration(21).unwrap();

        sqlx::query(
            "INSERT INTO _sqlx_migrations
                (version, description, success, checksum, execution_time, checksum_algo)
             VALUES (?1, ?2, 0, ?3, 0, ?4)",
        )
        .bind(migration.version)
        .bind(&migration.description)
        .bind(&migration.checksum)
        .bind(migration.checksum_algo.as_str())
        .execute(&pool)
        .await
        .unwrap();

        let error = run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("failed migration version 21"));
        assert!(message.contains("_sqlx_migrations"));
    }

    #[test]
    fn checksum_algorithm_name_is_explicit() {
        assert_eq!(ChecksumAlgorithm::Blake3.as_str(), "blake3");
    }

    async fn pragma_column_count(pool: &sqlx::SqlitePool, table: &str, columns: &[&str]) -> i64 {
        let joined = columns
            .iter()
            .map(|column| format!("'{column}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql =
            format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name IN ({joined})");
        sqlx::query_scalar(AssertSqlSafe(sql))
            .fetch_one(pool)
            .await
            .unwrap()
    }
}
