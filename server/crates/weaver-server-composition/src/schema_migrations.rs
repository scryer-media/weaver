// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::migration_assets::{
    CompiledMigration, CompiledMigrationCatalog, CompiledMigrationStep, MigrationInstallKind,
};
use crate::migration_hook_ids;
use crate::persistence::sql_runtime::SqlConn;
#[cfg(test)]
use runner::ensure_migration_ledger_shape;
#[cfg(test)]
use runner::execute_sql;
use runner::{db_err, sqlite_connect_options};
pub use runner::{embedded_catalog, max_recorded_migration_version};
#[cfg(any(test, feature = "test-support"))]
pub use runner::{embedded_payload_bytes, validate_payload_checksum};
#[cfg(test)]
use sqlx::AssertSqlSafe;
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use std::path::{Path, PathBuf};
use weaver_server_migrations::schema_migrations as runner;

pub(crate) mod egress_quotas_v53;
pub(crate) mod history_release_name_v87;
pub(crate) mod history_retention_v89;
pub(crate) mod history_search_key_v88;
mod history_swept_v91;
pub(crate) mod networking_bandwidth_v94;
pub(crate) mod nzb_tables_v85;
pub(crate) mod proxy_dns_v125;
pub(crate) mod proxy_resolver_v64;
pub(crate) mod script_concurrency_v58;
pub(crate) mod script_instances_v55;
pub(crate) mod server_limit_v92;
pub mod torrent_private_v126;
pub(crate) use weaver_server_migrations::schema_migrations::tracker_hosts_v105;
pub(crate) mod unwanted_extensions_v56;

#[cfg(test)]
use runner::CURRENT_SCHEMA_VERSION;
use runner::{
    adopt_legacy_schema_to_21, restart_active_jobs_drop_active_segments_v28, upgrade_to_schema_22,
    upgrade_to_schema_23, upgrade_to_schema_25,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    Apply,
    ValidateOnly,
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

struct DomainHooks;

impl runner::SqliteMigrationHooks for DomainHooks {
    async fn run_committed_prework(
        pool: &SqlitePool,
        migration: &CompiledMigration,
    ) -> Result<(), StateError> {
        run_committed_prework(pool, migration).await
    }
    async fn run_rust_hook(
        hook_id: &str,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        version: i64,
        install_kind: MigrationInstallKind,
    ) -> Result<(), StateError> {
        run_rust_hook(hook_id, tx, version, install_kind).await
    }
}

pub async fn run_embedded_migrations(
    pool: &SqlitePool,
    mode: MigrationMode,
) -> Result<(), StateError> {
    runner::run_embedded_migrations::<DomainHooks>(
        pool,
        matches!(mode, MigrationMode::ValidateOnly),
    )
    .await
}

pub async fn replay_catalog_into_fresh_db(
    pool: &SqlitePool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    through_version: Option<i64>,
    enable_baselines: bool,
) -> Result<(), StateError> {
    runner::replay_catalog_into_fresh_db::<DomainHooks>(
        pool,
        catalog,
        payload_bytes,
        through_version,
        enable_baselines,
    )
    .await
}

#[cfg(test)]
async fn apply_version_range(
    pool: &SqlitePool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
    start_version: i64,
    target_version: i64,
) -> Result<(), StateError> {
    runner::apply_version_range::<DomainHooks>(
        pool,
        catalog,
        payload_bytes,
        install_kind,
        start_version,
        target_version,
    )
    .await
}

#[cfg(test)]
async fn apply_single_migration(
    pool: &SqlitePool,
    migration: &CompiledMigration,
    payload_bytes: &[u8],
    install_kind: MigrationInstallKind,
) -> Result<(), StateError> {
    runner::apply_single_migration::<DomainHooks>(pool, migration, payload_bytes, install_kind)
        .await
}

// The part of a migration too large for one transaction: it runs first and
// commits as it goes, and must carry on where it stopped if it is run again.
async fn run_committed_prework(
    pool: &SqlitePool,
    migration: &CompiledMigration,
) -> Result<(), StateError> {
    for step in &migration.steps {
        if let CompiledMigrationStep::Rust { hook_id, .. } = step
            && hook_id.as_str() == nzb_tables_v85::HOOK_ID
        {
            nzb_tables_v85::move_nzbs_in_committed_batches(pool).await?;
        }
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
        egress_quotas_v53::HOOK_ID => {
            egress_quotas_v53::move_isp_cap_to_system_egress(&mut SqlConn::Sqlite(tx)).await
        }
        script_instances_v55::HOOK_ID => {
            script_instances_v55::move_script_wiring_to_instances(&mut SqlConn::Sqlite(tx)).await
        }
        unwanted_extensions_v56::HOOK_ID => {
            unwanted_extensions_v56::fill_default_unwanted_extensions(&mut SqlConn::Sqlite(tx))
                .await
        }
        script_concurrency_v58::HOOK_ID => {
            script_concurrency_v58::raise_script_concurrency(&mut SqlConn::Sqlite(tx)).await
        }
        proxy_resolver_v64::HOOK_ID => {
            proxy_resolver_v64::name_proxy_resolvers(&mut SqlConn::Sqlite(tx)).await
        }
        nzb_tables_v85::HOOK_ID => {
            nzb_tables_v85::move_nzbs_and_outlines(&mut SqlConn::Sqlite(tx)).await
        }
        history_release_name_v87::HOOK_ID => {
            history_release_name_v87::fill_history_release_names(&mut SqlConn::Sqlite(tx)).await
        }
        history_search_key_v88::HOOK_ID => {
            history_search_key_v88::fill_history_search_keys_and_fuzzy(&mut SqlConn::Sqlite(tx))
                .await
        }
        history_retention_v89::HOOK_ID => {
            history_retention_v89::seed_history_retention(&mut SqlConn::Sqlite(tx)).await
        }
        server_limit_v92::HOOK_ID => {
            server_limit_v92::trim_servers_to_limit(&mut SqlConn::Sqlite(tx)).await
        }
        networking_bandwidth_v94::HOOK_ID => {
            networking_bandwidth_v94::move_bandwidth_to_networking(&mut SqlConn::Sqlite(tx)).await
        }
        tracker_hosts_v105::HOOK_ID => {
            tracker_hosts_v105::fill_tracker_hosts(&mut SqlConn::Sqlite(tx)).await
        }
        proxy_dns_v125::HOOK_ID => {
            proxy_dns_v125::dns_profiles_from_proxies(&mut SqlConn::Sqlite(tx)).await
        }
        torrent_private_v126::HOOK_ID => {
            torrent_private_v126::fill_torrent_private(&mut SqlConn::Sqlite(tx)).await
        }
        other => Err(StateError::Database(format!(
            "unknown migration hook id '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration_assets::{
        ChecksumAlgorithm, CompiledMigration, CompiledMigrationStep, EngineScope, PayloadSlice,
        StepScope,
    };
    use crate::operations::backup::OperationsBackupLogicalInternalQueries;
    use crate::proxies::ProxiesNetworkPersistenceQueries;
    use crate::servers::queries::ServersQueries;
    use crate::torrents::persistence::TorrentsPersistenceJobHistoryFilesInternalQueries;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn current_schema_version_matches_embedded_catalog_max() {
        // Guards against bumping the migration manifest (adding migration N)
        // without bumping CURRENT_SCHEMA_VERSION, which silently shifts the
        // legacy-adoption acceptance range.
        let catalog = embedded_catalog().unwrap();
        assert_eq!(CURRENT_SCHEMA_VERSION, catalog.max_version());
    }

    #[tokio::test]
    async fn migration_135_deletes_the_stored_signing_secret() {
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        // The secret was written plain by old builds and sealed by later ones.
        let sealed = crate::persistence::encryption::encrypt_value(
            &crate::persistence::encryption::EncryptionKey::generate(),
            &"ab".repeat(32),
        )
        .unwrap();
        for stored in ["ab".repeat(32), sealed] {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(134), true)
                .await
                .unwrap();
            for (key, value) in [
                ("auth.jwt_signing_secret", stored.as_str()),
                ("auth.username", "admin"),
            ] {
                sqlx::query("INSERT INTO settings (key, value) VALUES (?, ?)")
                    .bind(key)
                    .bind(value)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            sqlx::query(
                "INSERT INTO browser_sessions
                    (token_hash, csrf_verifier, origin, remembered, created_at, expires_at)
                 VALUES ('session', 'verifier', 'http://localhost', 1, 1, 4102444800)",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO browser_session_verifications (token_hash, verified_at)
                 VALUES ('session', 2)",
            )
            .execute(&pool)
            .await
            .unwrap();

            apply_version_range(
                &pool,
                &catalog,
                &payload,
                MigrationInstallKind::Upgrade,
                135,
                135,
            )
            .await
            .unwrap();

            let keys: Vec<String> =
                sqlx::query_scalar("SELECT key FROM settings WHERE key LIKE 'auth.%' ORDER BY key")
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert_eq!(keys, vec!["auth.username".to_string()]);
            // Every browser session ends, with its verification.
            for query in [
                "SELECT COUNT(*) FROM browser_sessions",
                "SELECT COUNT(*) FROM browser_session_verifications",
            ] {
                let rows: i64 = sqlx::query_scalar(query).fetch_one(&pool).await.unwrap();
                assert_eq!(rows, 0, "{query}");
            }
        }
    }

    #[tokio::test]
    async fn migration_39_forgets_only_orphaned_duplicate_identities() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(38), true)
            .await
            .unwrap();

        for job_id in [100_i64, 101, 102] {
            sqlx::query(
                "INSERT INTO duplicate_job_snapshots
                    (job_id, lifecycle, normalized_name, admission_action, origin, created_at, updated_at)
                 VALUES (?, 'succeeded', ?, 'accept', 'api', 1, 1)",
            )
            .bind(job_id)
            .bind(format!("release-{job_id}"))
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO active_jobs
                (job_id, nzb_hash, nzb_path, output_dir, status, error, created_at, category, metadata)
             VALUES (101, ?, 'active.nzb', 'active-output', 'queued', NULL, 1, NULL, NULL)",
        )
        .bind(vec![1_u8; 32])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_history
                (job_id, name, status, total_bytes, downloaded_bytes,
                 optional_recovery_bytes, optional_recovery_downloaded_bytes,
                 failed_bytes, health, created_at, completed_at)
             VALUES (102, 'history', 'complete', 0, 0, 0, 0, 0, 1000, 1, 2)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_fingerprints
                (job_id, fingerprint_kind, fingerprint_version, fingerprint_digest, created_at)
             VALUES (100, 'raw_nzb', 1, ?, 1)",
        )
        .bind(vec![1_u8; 32])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO duplicate_admission_claims
                (fingerprint_kind, fingerprint_version, fingerprint_digest, job_id, claimed_at)
             VALUES ('raw_nzb', 1, ?, 100, 1)",
        )
        .bind(vec![1_u8; 32])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO submission_idempotency
                (caller_scope, idempotency_key, request_hash, job_id, created_at)
             VALUES ('test', 'orphan', ?, 100, 1)",
        )
        .bind(vec![2_u8; 32])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO semantic_duplicate_groups (group_id, normalized_key, created_at, updated_at)
             VALUES (1, 'orphan', 1, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO semantic_duplicate_candidates
                (job_id, group_id, score, candidate_state, created_at, updated_at)
             VALUES (100, 1, 100, 'active', 1, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        apply_version_range(
            &pool,
            &catalog,
            &payload,
            MigrationInstallKind::Upgrade,
            39,
            39,
        )
        .await
        .unwrap();

        let retained: Vec<i64> =
            sqlx::query_scalar("SELECT job_id FROM duplicate_job_snapshots ORDER BY job_id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(retained, vec![101, 102]);
        let forgotten: Vec<i64> =
            sqlx::query_scalar("SELECT job_id FROM forgotten_duplicate_identities ORDER BY job_id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(forgotten, vec![100]);
        for table in [
            "job_fingerprints",
            "duplicate_admission_claims",
            "submission_idempotency",
            "semantic_duplicate_candidates",
            "semantic_duplicate_groups",
        ] {
            let count: i64 = match table {
                "job_fingerprints" => sqlx::query_scalar("SELECT COUNT(*) FROM job_fingerprints"),
                "duplicate_admission_claims" => {
                    sqlx::query_scalar("SELECT COUNT(*) FROM duplicate_admission_claims")
                }
                "submission_idempotency" => {
                    sqlx::query_scalar("SELECT COUNT(*) FROM submission_idempotency")
                }
                "semantic_duplicate_candidates" => {
                    sqlx::query_scalar("SELECT COUNT(*) FROM semantic_duplicate_candidates")
                }
                "semantic_duplicate_groups" => {
                    sqlx::query_scalar("SELECT COUNT(*) FROM semantic_duplicate_groups")
                }
                _ => unreachable!("test table allowlist is exhaustive"),
            }
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, 0, "{table} should not retain orphaned data");
        }
    }

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
    async fn max_recorded_migration_version_reports_the_pre_run_ledger() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        // Nothing has ever migrated this database, so there is no ledger to read.
        assert_eq!(max_recorded_migration_version(&pool).await.unwrap(), None);

        // A ledger with no rows still records nothing.
        ensure_migration_ledger_shape(&pool).await.unwrap();
        assert_eq!(max_recorded_migration_version(&pool).await.unwrap(), None);

        // A directory an older release line left behind reports that line's
        // last migration, which is what identifies it as an upgrade.
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(39), true)
            .await
            .unwrap();
        assert_eq!(
            max_recorded_migration_version(&pool).await.unwrap(),
            Some(39)
        );

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();
        assert_eq!(
            max_recorded_migration_version(&pool).await.unwrap(),
            Some(CURRENT_SCHEMA_VERSION)
        );
    }

    #[tokio::test]
    async fn previous_release_schema_upgrades_to_current_and_opens() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("previous-release.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(50), true)
            .await
            .unwrap();
        assert_eq!(
            max_recorded_migration_version(&pool).await.unwrap(),
            Some(50)
        );
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);
        pool.close().await;
        let db = crate::Database::open(&db_path).unwrap();
        db.validate_backup_catalog().unwrap();
        assert_eq!(db.list_egress_interfaces().unwrap().len(), 1);
    }

    // A database at 0.16.16's last schema, 92, upgrades to the current one
    // and keeps what 0.16.16 stored: a finished job with the release name,
    // search key, archive and sweep 0.16.16's migrations added, its archived
    // files, a waiting job with its stored NZB, and a server. The finished
    // job reads as an NZB job.
    #[tokio::test]
    async fn the_0_16_16_schema_upgrades_to_current_and_keeps_its_data() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("v0.16.16.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(92), true)
            .await
            .unwrap();
        assert_eq!(
            max_recorded_migration_version(&pool).await.unwrap(),
            Some(92)
        );
        sqlx::query(
            "INSERT INTO active_jobs
                (job_id, nzb_hash, nzb_path, output_dir, status, error, created_at, category,
                 metadata, staging_dir, nzb_title)
             VALUES (101, ?, 'active.nzb', 'active-output', 'queued', NULL, 1, NULL, NULL,
                     'staging', 'Waiting.Release')",
        )
        .bind(vec![1_u8; 32])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO job_nzbs (job_id, nzb_zstd) VALUES (101, ?)")
            .bind(vec![7_u8; 16])
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO job_history
                (job_id, name, status, total_bytes, downloaded_bytes,
                 optional_recovery_bytes, optional_recovery_downloaded_bytes,
                 failed_bytes, health, created_at, completed_at,
                 release_name, search_key, archived_at, archived_file_count,
                 archived_file_bytes, files_swept_at)
             VALUES (102, 'history', 'complete', 10, 10, 0, 0, 0, 1000, 1, 2,
                     'Finished.Release.2026', 'finished release 2026', 3, 1, 10, 4)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_history_archived_files (job_id, ordinal, rel_path, size_bytes)
             VALUES (102, 0, 'Finished.Release.2026.mkv', 10)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO servers (id, host, port, tls, connections, active, priority, backfill)
             VALUES (1, 'news.example.test', 563, 1, 8, 1, 0, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        assert_eq!(CURRENT_SCHEMA_VERSION, 136);
        assert_eq!(
            max_recorded_migration_version(&pool).await.unwrap(),
            Some(CURRENT_SCHEMA_VERSION)
        );
        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);
        let finished: (
            String,
            String,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
        ) = sqlx::query_as(
            "SELECT release_name, search_key, archived_at, archived_file_count,
                        files_swept_at, protocol
                   FROM job_history WHERE job_id = 102",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            finished,
            (
                "Finished.Release.2026".to_string(),
                "finished release 2026".to_string(),
                Some(3),
                Some(1),
                Some(4),
                "nzb".to_string()
            )
        );
        let archived: (String, i64) = sqlx::query_as(
            "SELECT rel_path, size_bytes FROM job_history_archived_files WHERE job_id = 102",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(archived, ("Finished.Release.2026.mkv".to_string(), 10));
        let waiting: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT staging_dir, nzb_title FROM active_jobs WHERE job_id = 101")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            waiting,
            (
                Some("staging".to_string()),
                Some("Waiting.Release".to_string())
            )
        );
        let nzb: Vec<u8> = sqlx::query_scalar("SELECT nzb_zstd FROM job_nzbs WHERE job_id = 101")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(nzb, vec![7_u8; 16]);
        let host: String = sqlx::query_scalar("SELECT host FROM servers WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(host, "news.example.test");
        pool.close().await;
        let db = crate::Database::open(&db_path).unwrap();
        db.validate_backup_catalog().unwrap();
        assert_eq!(db.list_servers().unwrap().len(), 1);
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
        assert_eq!(diagnostic_cols, 0);
        assert!(!sqlite_table_exists(&pool, "diagnostic_runs").await);

        // The stored NZB has its own table since schema 84.
        let nzb_cols: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('active_jobs') WHERE name = 'nzb_zstd'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(nzb_cols, 0);
        assert!(sqlite_table_exists(&pool, "job_nzbs").await);
        assert!(!sqlite_table_exists(&pool, "active_segments").await);

        let server_limit_cols: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('servers')
             WHERE name IN (
                'max_download_speed',
                'download_quota_enabled',
                'download_quota_limit_bytes',
                'download_quota_period',
                'download_quota_reset_time_minutes_local',
                'download_quota_weekly_reset_weekday',
                'download_quota_monthly_reset_day'
             )",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(server_limit_cols, 7);
        assert!(sqlite_table_exists(&pool, "server_download_usage").await);
        let usage_cascade: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_foreign_key_list('server_download_usage')
             WHERE \"table\" = 'servers' AND \"from\" = 'server_id' AND on_delete = 'CASCADE'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(usage_cascade, 1);
    }

    // Status exists from the first script-output schema and never needs to
    // parse a result blob during upgrade.
    #[tokio::test]
    async fn sqlite_script_output_status_is_present_from_creation() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(52), true)
            .await
            .unwrap();

        for (seq, status) in [(1_i64, "succeeded"), (2, "timed_out")] {
            sqlx::query(
                "INSERT INTO script_outputs
                    (id, job_id, event, script, seq, raw_bytes, truncated, output, stored_bytes,
                     result_json, created_at, status)
                 VALUES (?, NULL, 'scan', 'scan.sh', ?, 0, 0, x'', 0, 'invalid JSON', 1, ?)",
            )
            .bind(format!("run-{seq}"))
            .bind(seq)
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        }

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();

        let statuses: Vec<(String, String)> =
            sqlx::query_as("SELECT id, status FROM script_outputs ORDER BY seq")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            statuses,
            [
                ("run-1".to_string(), "succeeded".to_string()),
                ("run-2".to_string(), "timed_out".to_string()),
            ]
        );
    }

    // The proven BODY pipelining depth is optional: a server that has never
    // been measured must read back as NULL, not as a depth nobody proved.
    #[tokio::test]
    async fn sqlite_v45_upgrade_adds_a_nullable_server_pipelining_depth() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();

        let (count, notnull): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(MAX(\"notnull\"), 0) FROM pragma_table_info('servers')
             WHERE name = 'pipelining_depth'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(notnull, 0);

        sqlx::query(
            "INSERT INTO servers (id, host, port, tls, username, password, connections, active,
                                  supports_pipelining, priority, backfill, retention_days)
             VALUES (1, 'news.example.com', 563, 1, NULL, NULL, 8, 1, 1, 0, 0, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let depth: Option<i64> =
            sqlx::query_scalar("SELECT pipelining_depth FROM servers WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(depth, None);

        sqlx::query("UPDATE servers SET pipelining_depth = 8 WHERE id = 1")
            .execute(&pool)
            .await
            .unwrap();
        let depth: Option<i64> =
            sqlx::query_scalar("SELECT pipelining_depth FROM servers WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(depth, Some(8));
    }

    // A job that was active or finished before migration 74 keeps its row
    // when the engine column goes, active or finished.
    #[tokio::test]
    async fn sqlite_migration_74_drops_the_engine_column_and_keeps_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-graph-engine.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(73), true)
            .await
            .unwrap();
        assert_eq!(
            pragma_column_count(&pool, "active_jobs", &["engine"]).await,
            1
        );
        sqlx::query(
            "INSERT INTO active_jobs
                (job_id, nzb_hash, nzb_path, output_dir, status, error, created_at, category, metadata, engine)
             VALUES (101, ?, 'active.nzb', 'active-output', 'queued', NULL, 1, NULL, NULL, 'classic')",
        )
        .bind(vec![1_u8; 32])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_history
                (job_id, name, status, total_bytes, downloaded_bytes,
                 optional_recovery_bytes, optional_recovery_downloaded_bytes,
                 failed_bytes, health, created_at, completed_at, engine)
             VALUES (102, 'history', 'complete', 0, 0, 0, 0, 0, 1000, 1, 2, 'classic')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        assert_eq!(
            pragma_column_count(&pool, "active_jobs", &["engine"]).await,
            0
        );
        assert_eq!(
            pragma_column_count(&pool, "job_history", &["engine"]).await,
            0
        );
        let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM active_jobs WHERE job_id = 101")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(active, 1);
        let finished: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM job_history WHERE job_id = 102")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(finished, 1);
        pool.close().await;
    }

    // Migration 104 gives each torrent its saved peers and bans: an existing
    // torrent starts with none. They sit on its ledger row once the lean
    // resume migration moves them there.
    #[tokio::test]
    async fn sqlite_migration_85_gives_torrents_saved_peers_and_bans() {
        let columns = ["peers", "banned_peers"];
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        let temp = tempfile::tempdir().unwrap();

        let fresh = open_test_pool(&temp.path().join("fresh.db")).await;
        replay_catalog_into_fresh_db(&fresh, &catalog, &payload, None, true)
            .await
            .unwrap();
        assert_eq!(pragma_column_count(&fresh, "torrents", &columns).await, 0);
        assert_eq!(
            pragma_column_count(&fresh, "torrent_counters", &columns).await,
            2
        );
        fresh.close().await;

        let db_path = temp.path().join("pre-torrent-peers.db");
        let pool = open_test_pool(&db_path).await;
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(101), true)
            .await
            .unwrap();
        assert_eq!(pragma_column_count(&pool, "torrents", &columns).await, 0);
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
             VALUES (7, ?, 'saved', 'magnet', 1, 'queued')",
        )
        .bind(vec![7_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let row: (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT torrents.name, torrent_counters.peers, torrent_counters.banned_peers
               FROM torrents JOIN torrent_counters USING (job_id) WHERE job_id = 7",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, ("saved".to_string(), None, None));
        pool.close().await;
    }

    // The lean resume migration keeps only what a torrent's next start
    // needs: the peers and bans move from the torrent's row to its ledger
    // row, a torrent with no ledger row gets one, and the columns nothing
    // reads back go.
    #[tokio::test]
    async fn sqlite_lean_resume_migration_moves_peers_to_the_ledger_row() {
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let gone_torrents = ["peers", "banned_peers", "private"];
        let gone_counters = [
            "checked_bytes",
            "last_announce_uploaded_bytes",
            "updated_at_ms",
        ];

        let fresh = open_test_pool(&temp.path().join("fresh.db")).await;
        replay_catalog_into_fresh_db(&fresh, &catalog, &payload, None, true)
            .await
            .unwrap();
        // Of the three, only the privacy is back, from 126, as a column the
        // add writes.
        assert_eq!(
            pragma_column_count(&fresh, "torrents", &gone_torrents).await,
            1
        );
        assert_eq!(
            pragma_column_count(&fresh, "torrent_counters", &gone_counters).await,
            0
        );
        assert_eq!(
            pragma_column_count(
                &fresh,
                "torrent_counters",
                &["failed_bytes", "redundant_bytes"]
            )
            .await,
            2
        );
        fresh.close().await;

        let db_path = temp.path().join("pre-lean-resume.db");
        let pool = open_test_pool(&db_path).await;
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(107), true)
            .await
            .unwrap();
        assert_eq!(
            pragma_column_count(&pool, "torrents", &gone_torrents).await,
            3
        );
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state,
                                   peers, banned_peers, private)
             VALUES (7, ?, 'ledger', 'magnet', 1, 'queued', '10.0.0.1:6881', '10.0.0.9', 1),
                    (8, ?, 'bare', 'magnet', 2, 'queued', NULL, '10.0.0.8', 0)",
        )
        .bind(vec![7_u8; 20])
        .bind(vec![8_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO torrent_counters (job_id, uploaded_bytes, checked_bytes, updated_at_ms)
             VALUES (7, 42, 40, 5)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        assert_eq!(
            pragma_column_count(&pool, "torrents", &gone_torrents).await,
            1
        );
        assert_eq!(
            pragma_column_count(&pool, "torrent_counters", &gone_counters).await,
            0
        );
        let rows: Vec<(i64, i64, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT job_id, uploaded_bytes, peers, banned_peers
               FROM torrent_counters ORDER BY job_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            [
                (
                    7,
                    42,
                    Some("10.0.0.1:6881".to_string()),
                    Some("10.0.0.9".to_string())
                ),
                (8, 0, None, Some("10.0.0.8".to_string())),
            ]
        );
        // The ledger keeps the wasted bytes, from none for every torrent.
        let wasted: Vec<(i64, i64, i64)> = sqlx::query_as(
            "SELECT job_id, failed_bytes, redundant_bytes
               FROM torrent_counters ORDER BY job_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(wasted, [(7, 0, 0), (8, 0, 0)]);
        pool.close().await;
    }

    // Migration 105 keeps the hosts the torrents already announce to, each once
    // and as a rule matches it.
    #[tokio::test]
    async fn sqlite_migration_86_fills_tracker_hosts_from_the_torrents() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-tracker-hosts.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(104), true)
            .await
            .unwrap();
        for (job_id, hash) in [(7_i64, 7_u8), (8, 8)] {
            sqlx::query(
                "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
                 VALUES (?, ?, 'hosts', 'magnet', 1, 'queued')",
            )
            .bind(job_id)
            .bind(vec![hash; 20])
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO torrent_trackers (job_id, tier, url)
             VALUES (7, 0, 'https://Tracker.Invalid./k1/announce'),
                    (7, 1, 'udp://tracker.invalid:6969'),
                    (8, 0, 'udp://Bücher.invalid:80'),
                    (8, 1, 'not a url')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let hosts: Vec<String> = sqlx::query_scalar("SELECT host FROM tracker_hosts ORDER BY host")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(hosts, ["tracker.invalid", "xn--bcher-kva.invalid"]);
        pool.close().await;
    }

    // Migration 120 lets a torrent list read its page in the order the
    // torrents were added: the existing torrents stay, the two ordered
    // indexes exist, the category-only index they cover is gone, and a page
    // of every torrent or of one category walks an index instead of sorting.
    #[tokio::test]
    async fn sqlite_migration_120_orders_the_torrent_list_by_an_index() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-list-order.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(119), true)
            .await
            .unwrap();
        for (job_id, category) in [(7_i64, Some(3_i64)), (8, None), (9, Some(3)), (10, Some(4))] {
            sqlx::query(
                "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state, category_id)
                 VALUES (?, ?, 'order', 'magnet', ?, 'queued', ?)",
            )
            .bind(job_id)
            .bind(vec![job_id as u8; 20])
            .bind(100 - job_id)
            .bind(category)
            .execute(&pool)
            .await
            .unwrap();
        }
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master
             WHERE type = 'index' AND tbl_name = 'torrents' AND name LIKE 'idx_torrents_%'
             ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            indexes,
            [
                "idx_torrents_added",
                "idx_torrents_category_added",
                "idx_torrents_category_private_added",
                "idx_torrents_private_added",
                "idx_torrents_retiring"
            ]
        );
        let page: Vec<i64> = sqlx::query_scalar(
            "SELECT job_id FROM torrents ORDER BY added_at_ms, job_id LIMIT 10 OFFSET 0",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(page, [10, 9, 8, 7]);
        for (query, index) in [
            (
                "SELECT job_id FROM torrents ORDER BY added_at_ms, job_id LIMIT 10 OFFSET 0",
                "idx_torrents_added",
            ),
            (
                "SELECT job_id FROM torrents WHERE category_id IN (3)
                 ORDER BY added_at_ms, job_id LIMIT 10 OFFSET 0",
                "idx_torrents_category_added",
            ),
        ] {
            let plan = explain(&pool, query).await;
            assert!(
                plan.iter().any(|step| step.contains(index)),
                "{query}: {plan:?}"
            );
            assert!(
                !plan.iter().any(|step| step.contains("TEMP B-TREE")),
                "{query}: {plan:?}"
            );
        }

        // The list's own statements for two categories and more, with and
        // without the torrents in none: each category walks its index in
        // the order of adding, and the compound merges them unsorted, to the
        // rows `IN` keeps, in the same order.
        use crate::persistence::sql_runtime::{SqlArg, SqlEngine};
        use crate::torrents::queries::{CategoryIds, TorrentListFilter, list_statements};
        for (ids, none, kept) in [
            (vec![3, 4], false, vec![10, 9, 7]),
            (vec![3, 4, 5], false, vec![10, 9, 7]),
            (vec![4, 3], true, vec![10, 9, 8, 7]),
            (vec![3], true, vec![9, 8, 7]),
        ] {
            let filter = TorrentListFilter {
                categories: Some(CategoryIds {
                    ids: ids.clone(),
                    none,
                    known: vec![3, 4, 5],
                }),
                hashes: None,
                private: None,
            };
            let statements = list_statements(SqlEngine::Sqlite, &filter, 0, Some(10));
            assert_eq!(statements.len(), 1);
            let (sql, args) = &statements[0];
            let sql = sql.replace("{}", "?");
            let bind = |query: &str| {
                let mut statement = sqlx::query(sqlx::AssertSqlSafe(query.to_owned()));
                for arg in args {
                    let SqlArg::I64(value) = arg else {
                        panic!("a category list binds integers")
                    };
                    statement = statement.bind(*value);
                }
                statement
            };
            let plan: Vec<String> = {
                use sqlx::Row;
                bind(&format!("EXPLAIN QUERY PLAN {sql}"))
                    .fetch_all(&pool)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.get::<String, _>("detail"))
                    .collect()
            };
            assert!(
                plan.iter()
                    .filter(|step| step.contains("idx_torrents_category_added"))
                    .count()
                    >= ids.len(),
                "{sql}: {plan:?}"
            );
            assert!(
                !plan.iter().any(|step| step.contains("TEMP B-TREE")),
                "{sql}: {plan:?}"
            );
            let rows: Vec<i64> = {
                use sqlx::Row;
                bind(&sql)
                    .fetch_all(&pool)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.get::<i64, _>("job_id"))
                    .collect()
            };
            assert_eq!(rows, kept, "{ids:?}, none {none}");
        }
        pool.close().await;
    }

    // Migration 126 keeps a torrent's privacy in the table: each torrent
    // kept before it takes the privacy its metainfo reads as, a page of rows
    // at a time across more than one page, a magnet with none and a
    // metainfo that does not read stay public, and the list's own
    // statements for a privacy, alone or in a category, walk the privacy's
    // indexes in the order of adding, unsorted.
    #[tokio::test]
    async fn sqlite_migration_126_fills_each_torrents_privacy_from_its_metainfo() {
        use crate::persistence::sql_runtime::{SqlArg, SqlEngine};
        use crate::schema_migrations::torrent_private_v126::tiny_metainfo;
        use crate::torrents::queries::{
            CategoryIds, TorrentListFilter, count_statements, list_statements,
        };
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-list-private.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(125), true)
            .await
            .unwrap();
        // 200 torrents, more than three pages of the step's 64: by job id,
        // private, public, a magnet with no metainfo, and one whose
        // metainfo does not read.
        let kind = |job_id: i64| job_id % 4;
        for job_id in 1..=200_i64 {
            let metainfo = match kind(job_id) {
                0 => Some(tiny_metainfo(&format!("t{job_id}"), true)),
                1 => Some(tiny_metainfo(&format!("t{job_id}"), false)),
                2 => None,
                _ => Some(b"not bencode".to_vec()),
            };
            sqlx::query(
                "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state,
                                       category_id, metainfo)
                 VALUES (?, ?, 'privacy', 'magnet', ?, 'queued', ?, ?)",
            )
            .bind(job_id)
            .bind(job_id.to_be_bytes().repeat(3)[..20].to_vec())
            .bind(1000 - job_id / 2)
            .bind((job_id % 3 == 0).then_some(3_i64))
            .bind(metainfo)
            .execute(&pool)
            .await
            .unwrap();
        }
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let private: Vec<i64> =
            sqlx::query_scalar("SELECT job_id FROM torrents WHERE private = 1 ORDER BY job_id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            private,
            (1..=200)
                .filter(|job_id| kind(*job_id) == 0)
                .collect::<Vec<_>>()
        );
        let public: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM torrents WHERE private = 0")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(public, 150);

        let bind_all = |query: String, args: &[SqlArg]| {
            let mut statement = sqlx::query(sqlx::AssertSqlSafe(query));
            for arg in args {
                statement = match arg {
                    SqlArg::I64(value) => statement.bind(*value),
                    SqlArg::Bool(value) => statement.bind(*value),
                    other => panic!("a privacy list binds no {other:?}"),
                };
            }
            statement
        };
        for (categories, index) in [
            (None, "idx_torrents_private_added"),
            (
                Some(CategoryIds {
                    ids: vec![3],
                    none: false,
                    known: vec![3, 4],
                }),
                "idx_torrents_category_private_added",
            ),
            (
                Some(CategoryIds {
                    ids: vec![3],
                    none: true,
                    known: vec![3, 4],
                }),
                "idx_torrents_category_private_added",
            ),
        ] {
            for private in [true, false] {
                let filter = TorrentListFilter {
                    categories: categories.clone(),
                    hashes: None,
                    private: Some(private),
                };
                let statements = list_statements(SqlEngine::Sqlite, &filter, 5, Some(20));
                assert_eq!(statements.len(), 1);
                let (sql, args) = &statements[0];
                let sql = sql.replace("{}", "?");
                let plan: Vec<String> = {
                    use sqlx::Row;
                    bind_all(format!("EXPLAIN QUERY PLAN {sql}"), args)
                        .fetch_all(&pool)
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.get::<String, _>("detail"))
                        .collect()
                };
                assert!(
                    plan.iter().any(|step| step.contains(index)),
                    "{sql}: {plan:?}"
                );
                assert!(
                    !plan.iter().any(|step| step.contains("TEMP B-TREE")),
                    "{sql}: {plan:?}"
                );
                assert!(
                    !plan
                        .iter()
                        .any(|step| step.starts_with("SCAN torrents") && !step.contains("INDEX")),
                    "{sql}: {plan:?}"
                );
                // The page is the one `ORDER BY` over the privacy gives.
                let rows: Vec<i64> = {
                    use sqlx::Row;
                    bind_all(sql.clone(), args)
                        .fetch_all(&pool)
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.get::<i64, _>("job_id"))
                        .collect()
                };
                let in_category = |category: Option<i64>| match &categories {
                    None => true,
                    Some(categories) => {
                        category.is_some_and(|id| categories.ids.contains(&(id as u32)))
                            || (categories.none
                                && category
                                    .is_none_or(|id| !categories.known.contains(&(id as u32))))
                    }
                };
                let every: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(
                    "SELECT added_at_ms, job_id, category_id FROM torrents
                      WHERE private = ? ORDER BY added_at_ms, job_id",
                )
                .bind(private)
                .fetch_all(&pool)
                .await
                .unwrap();
                let kept: Vec<i64> = every
                    .iter()
                    .filter(|(_, _, category)| in_category(*category))
                    .map(|(_, job_id, _)| *job_id)
                    .collect();
                assert_eq!(
                    rows,
                    kept.iter().skip(5).take(20).copied().collect::<Vec<_>>(),
                    "{categories:?}, private {private}"
                );
                for (sql, args) in count_statements(SqlEngine::Sqlite, &filter) {
                    let sql = sql.replace("{}", "?");
                    let plan: Vec<String> = {
                        use sqlx::Row;
                        bind_all(format!("EXPLAIN QUERY PLAN {sql}"), &args)
                            .fetch_all(&pool)
                            .await
                            .unwrap()
                            .iter()
                            .map(|row| row.get::<String, _>("detail"))
                            .collect()
                    };
                    assert!(
                        !plan.iter().any(
                            |step| step.starts_with("SCAN torrents") && !step.contains("INDEX")
                        ),
                        "{sql}: {plan:?}"
                    );
                }
            }
        }
        pool.close().await;
    }

    async fn explain(pool: &sqlx::SqlitePool, query: &str) -> Vec<String> {
        use sqlx::Row;
        sqlx::query(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {query}")))
            .fetch_all(pool)
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect()
    }

    // Migration 106 gives each torrent its own upload and download limits: a
    // fresh database has them, and an existing torrent is unlimited both
    // ways.
    #[tokio::test]
    async fn sqlite_migration_87_gives_torrents_their_own_rate_limits() {
        let columns = ["upload_limit", "download_limit"];
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        let temp = tempfile::tempdir().unwrap();

        let fresh = open_test_pool(&temp.path().join("fresh.db")).await;
        replay_catalog_into_fresh_db(&fresh, &catalog, &payload, None, true)
            .await
            .unwrap();
        assert_eq!(pragma_column_count(&fresh, "torrents", &columns).await, 2);
        fresh.close().await;

        let db_path = temp.path().join("pre-rate-limits.db");
        let pool = open_test_pool(&db_path).await;
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(105), true)
            .await
            .unwrap();
        assert_eq!(pragma_column_count(&pool, "torrents", &columns).await, 0);
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
             VALUES (7, ?, 'limits', 'magnet', 1, 'queued')",
        )
        .bind(vec![7_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        assert_eq!(pragma_column_count(&pool, "torrents", &columns).await, 2);
        let row: (String, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT name, upload_limit, download_limit FROM torrents WHERE job_id = 7",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, ("limits".to_string(), None, None));
        pool.close().await;
    }

    // The file-priority migration gives each torrent one
    // value for its file levels, none set, and its sequential and
    // first-and-last toggles, both off, and drops the file rows: their path
    // and length are the metainfo's, and `selected` held only whether the
    // file was padding.
    #[tokio::test]
    async fn sqlite_file_priority_migration_keeps_levels_on_the_torrent() {
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let columns = ["file_priorities", "sequential", "first_last_piece_first"];
        async fn has_file_rows(pool: &sqlx::SqlitePool) -> bool {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'torrent_files'",
            )
            .fetch_one(pool)
            .await
            .unwrap()
                > 0
        }

        let fresh = open_test_pool(&temp.path().join("fresh.db")).await;
        replay_catalog_into_fresh_db(&fresh, &catalog, &payload, None, true)
            .await
            .unwrap();
        assert_eq!(pragma_column_count(&fresh, "torrents", &columns).await, 3);
        assert!(!has_file_rows(&fresh).await);
        fresh.close().await;

        let db_path = temp.path().join("pre-file-priority.db");
        let pool = open_test_pool(&db_path).await;
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(106), true)
            .await
            .unwrap();
        assert_eq!(pragma_column_count(&pool, "torrents", &columns).await, 0);
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
             VALUES (7, ?, 'levels', 'metainfo', 1, 'queued')",
        )
        .bind(vec![7_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO torrent_files (job_id, file_index, path, length, selected)
             VALUES (7, 0, 'a.bin', 10, 1), (7, 1, '.pad/1', 6, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        assert!(!has_file_rows(&pool).await);
        let row: (Option<String>, bool, bool) = sqlx::query_as(
            "SELECT file_priorities, sequential, first_last_piece_first
               FROM torrents WHERE job_id = 7",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (None, false, false));
        pool.close().await;
    }

    // Migration 97 keeps each tracker's place in the announce list and drops
    // the announce columns, which the session answers instead.
    #[tokio::test]
    async fn sqlite_migration_78_keeps_only_the_tracker_listing() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-tracker-listing.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(96), true)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
             VALUES (7, ?, 'listing', 'magnet', 1, 'queued')",
        )
        .bind(vec![7_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO torrent_trackers (job_id, tier, url, status, seeders)
             VALUES (7, 0, 'http://a/announce', 'working', 3),
                    (7, 1, 'udp://b:80', 'not_contacted', NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let dropped = [
            "status",
            "last_announce_ms",
            "next_announce_ms",
            "message",
            "seeders",
            "leechers",
        ];
        assert_eq!(
            pragma_column_count(&pool, "torrent_trackers", &dropped).await,
            0
        );
        let rows: Vec<(i64, i64, String)> =
            sqlx::query_as("SELECT job_id, tier, url FROM torrent_trackers ORDER BY tier")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![
                (7, 0, "http://a/announce".to_string()),
                (7, 1, "udp://b:80".to_string()),
            ]
        );
        let sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'torrent_trackers'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(sql.contains("WITHOUT ROWID"), "{sql}");
        sqlx::query("DELETE FROM torrents WHERE job_id = 7")
            .execute(&pool)
            .await
            .unwrap();
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM torrent_trackers")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(left, 0);
        pool.close().await;
    }

    // Migration 111 records who named each tracker; every row before it is
    // the torrent's, as nothing recorded which were added by hand.
    #[tokio::test]
    async fn sqlite_migration_92_marks_every_existing_tracker_the_torrents() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-tracker-named-by.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(110), true)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
             VALUES (7, ?, 'named', 'magnet', 1, 'queued')",
        )
        .bind(vec![7_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO torrent_trackers (job_id, tier, url)
             VALUES (7, 0, 'http://a/announce'), (7, 1, 'udp://b:80')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let rows: Vec<(i64, String, String)> =
            sqlx::query_as("SELECT tier, url, named_by FROM torrent_trackers ORDER BY tier")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![
                (0, "http://a/announce".to_string(), "torrent".to_string()),
                (1, "udp://b:80".to_string(), "torrent".to_string()),
            ]
        );
        let other = sqlx::query(
            "INSERT INTO torrent_trackers (job_id, tier, url, named_by)
             VALUES (7, 2, 'http://c/announce', 'peer')",
        )
        .execute(&pool)
        .await;
        assert!(other.is_err(), "{other:?}");
        pool.close().await;
    }

    // Migration 112 forgets the mapped ports a test kept for routes; the
    // settings beside them stay.
    #[tokio::test]
    async fn sqlite_migration_93_forgets_saved_mapped_ports() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("pre-forget-mapped-ports.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(111), true)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO settings (key, value)
             VALUES ('torrent.mapped_ports', '{\"r\":40001}'), ('torrent.session_slots', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let keys: Vec<String> =
            sqlx::query_scalar("SELECT key FROM settings WHERE key LIKE 'torrent.%' ORDER BY key")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(keys, vec!["torrent.session_slots".to_string()]);
        pool.close().await;
    }

    // A database at 120 upgrades to the current schema: each torrent it
    // holds stays live, with no history row kept on it, and the start finds
    // the torrents a removal left retiring by their index.
    #[tokio::test]
    async fn sqlite_migration_120_torrents_upgrade_with_no_kept_history_row() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("v120.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(120), true)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO torrents (job_id, infohash_v1, name, source, added_at_ms, state)
             VALUES (7, ?, 'kept', 'magnet', 1, 'queued')",
        )
        .bind(vec![7_u8; 20])
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let rows: Vec<(i64, Option<String>)> =
            sqlx::query_as("SELECT job_id, retiring FROM torrents")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, vec![(7, None)]);
        let detail: Vec<(i64, i64, i64, String)> = sqlx::query_as(
            "EXPLAIN QUERY PLAN SELECT job_id, retiring FROM torrents WHERE retiring IS NOT NULL",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            detail
                .iter()
                .any(|(_, _, _, step)| step.contains("idx_torrents_retiring")),
            "{detail:?}"
        );
        pool.close().await;
        let db = crate::Database::open(&db_path).unwrap();
        db.validate_backup_catalog().unwrap();
    }

    // A database at 122 upgrades to the current schema with the RSS items
    // held while torrents are off found by their index, never by a scan of
    // every seen item.
    #[tokio::test]
    async fn sqlite_migration_123_finds_rss_items_held_by_their_index() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("v122.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(122), true)
            .await
            .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        for query in [
            "EXPLAIN QUERY PLAN SELECT 1 FROM rss_seen_items WHERE decision = 'waiting' LIMIT 1",
            "EXPLAIN QUERY PLAN SELECT feed_id FROM rss_seen_items WHERE decision = 'waiting'",
        ] {
            let detail: Vec<(i64, i64, i64, String)> =
                sqlx::query_as(query).fetch_all(&pool).await.unwrap();
            assert!(
                detail
                    .iter()
                    .any(|(_, _, _, step)| step.contains("idx_rss_seen_waiting")),
                "{query}: {detail:?}"
            );
        }
        pool.close().await;
    }

    // A database at 130 upgrades to the current schema with its data used rows
    // kept, and the expiry of a removed server's kept rows, and the read of
    // when the next is due, served by an index, never by a scan.
    #[tokio::test]
    async fn sqlite_migration_131_serves_usage_expiry_by_index() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("v130.db");
        let pool = open_test_pool(&db_path).await;
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(130), true)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO usage_daily (day, consumer_kind, consumer_id, bytes_downloaded)
             VALUES ('2026-11-02', 'server', 'removed-3-1', 10),
                    ('2026-11-02', 'server', '4', 20)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO usage_removed_servers (consumer_id, server_id, host, remove_at)
             VALUES ('removed-3-1', 3, 'news.example', 1000)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        run_embedded_migrations_on_path_blocking(&db_path).unwrap();

        let pool = open_test_pool(&db_path).await;
        let kept: Vec<(String, i64)> = sqlx::query_as(
            "SELECT consumer_id, bytes_downloaded FROM usage_daily ORDER BY consumer_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            kept,
            vec![("4".to_string(), 20), ("removed-3-1".to_string(), 10)]
        );
        for (query, index) in [
            (
                "DELETE FROM usage_daily WHERE consumer_kind = 'server' AND consumer_id IN
                    (SELECT consumer_id FROM usage_removed_servers WHERE remove_at <= 1000)",
                "idx_usage_daily_consumer",
            ),
            (
                "DELETE FROM usage_daily WHERE consumer_kind = 'server' AND consumer_id IN
                    (SELECT consumer_id FROM usage_removed_servers WHERE remove_at <= 1000)",
                "idx_usage_removed_servers_remove_at",
            ),
            (
                "DELETE FROM usage_removed_servers WHERE remove_at <= 1000",
                "idx_usage_removed_servers_remove_at",
            ),
            (
                "SELECT MIN(remove_at) FROM usage_removed_servers",
                "idx_usage_removed_servers_remove_at",
            ),
        ] {
            let plan = explain(&pool, query).await;
            assert!(
                plan.iter().any(|step| step.contains(index)),
                "{query}: {plan:?}"
            );
        }
        pool.close().await;
    }

    // A database at 0.16.16's schema (92) or at 120 upgrades to the current
    // one with the history files list there and empty: a history row it
    // held lists no files, its archive goes by its marked folders as before,
    // and a read of one job's list is served by the list's key.
    #[tokio::test]
    async fn sqlite_upgrades_from_0_16_16_and_120_add_an_empty_history_files_list() {
        for from in [92, 120] {
            let temp = tempfile::tempdir().unwrap();
            let db_path = temp.path().join(format!("v{from}.db"));
            let pool = open_test_pool(&db_path).await;
            let catalog = embedded_catalog().unwrap();
            let payload = embedded_payload_bytes().unwrap();
            replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(from), true)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO job_history
                    (job_id, name, status, total_bytes, downloaded_bytes,
                     optional_recovery_bytes, optional_recovery_downloaded_bytes,
                     failed_bytes, health, created_at, completed_at)
                 VALUES (102, 'history', 'complete', 10, 10, 0, 0, 0, 1000, 1, 2)",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;

            run_embedded_migrations_on_path_blocking(&db_path).unwrap();

            let pool = open_test_pool(&db_path).await;
            let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(version, CURRENT_SCHEMA_VERSION, "from {from}");
            let listed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_history_files")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(listed, 0, "from {from}");
            let detail: Vec<(i64, i64, i64, String)> = sqlx::query_as(
                "EXPLAIN QUERY PLAN SELECT job_id, path, whole FROM job_history_files WHERE job_id IN (102, 103)",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert!(
                detail
                    .iter()
                    .any(|(_, _, _, step)| step.contains("sqlite_autoindex_job_history_files")),
                "from {from}: {detail:?}"
            );
            // The paths live torrents hold under a folder history removes
            // from are found by the path index, never a scan.
            let detail: Vec<(i64, i64, i64, String)> = sqlx::query_as(
                "EXPLAIN QUERY PLAN SELECT DISTINCT h.path FROM torrent_held_paths h \
                 JOIN torrents t ON t.job_id = h.job_id WHERE t.retiring IS NULL AND \
                 (h.path = '/d' OR (h.path >= '/d/' AND h.path < '/d0'))",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert!(
                detail
                    .iter()
                    .any(|(_, _, _, step)| step.contains("idx_torrent_held_paths_path")),
                "from {from}: {detail:?}"
            );
            let detail_above: Vec<(i64, i64, i64, String)> = sqlx::query_as(
                "EXPLAIN QUERY PLAN SELECT DISTINCT h.path FROM torrent_held_paths h \
                 JOIN torrents t ON t.job_id = h.job_id WHERE t.retiring IS NULL AND \
                 h.path IN ('/', '/e')",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert!(
                detail_above
                    .iter()
                    .any(|(_, _, _, step)| step.contains("idx_torrent_held_paths_path")),
                "from {from}: {detail_above:?}"
            );
            for step in detail.iter().chain(&detail_above) {
                assert!(
                    !step.3.starts_with("SCAN h") && !step.3.starts_with("SCAN torrent_held_paths"),
                    "from {from}: {step:?}"
                );
            }
            pool.close().await;
            let db = crate::Database::open(&db_path).unwrap();
            db.validate_backup_catalog().unwrap();
            assert!(
                db.job_history_files(&[crate::JobId(102)])
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn sqlite_v31_upgrade_adds_server_download_limit_defaults() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(31), true)
            .await
            .unwrap();
        execute_batch(
            &pool,
            "INSERT INTO servers
                (id, host, port, tls, connections, active, supports_pipelining,
                 priority, backfill, retention_days)
             VALUES (1, 'news.example.com', 563, 1, 4, 1, 0, 0, 0, 0)",
        )
        .await;

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();

        let defaults: (i64, i64, i64, String, i64, String, i64) = sqlx::query_as(
            "SELECT max_download_speed, download_quota_enabled,
                    download_quota_limit_bytes, download_quota_period,
                    download_quota_reset_time_minutes_local,
                    download_quota_weekly_reset_weekday,
                    download_quota_monthly_reset_day
               FROM servers WHERE id = 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            defaults,
            (0, 0, 0, "one_time".to_string(), 0, "mon".to_string(), 1)
        );
        assert!(sqlite_table_exists(&pool, "server_download_usage").await);
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
            "active_extracted",
            "active_detected_archives",
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
                "__weaver_diagnostic_source_job_id".to_string(),
                "42".to_string(),
            ),
            (
                "__weaver_diagnostic_include_server_hostnames".to_string(),
                "false".to_string(),
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

        apply_version_range(
            &pool,
            &catalog,
            &payload,
            MigrationInstallKind::Upgrade,
            28,
            30,
        )
        .await
        .unwrap();

        let stored_metadata: String =
            sqlx::query_scalar("SELECT metadata FROM job_history WHERE job_id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let metadata_pairs: Vec<(String, String)> = serde_json::from_str(&stored_metadata).unwrap();
        assert_eq!(
            metadata_pairs,
            vec![
                ("*scryer_title_id".to_string(), "title-a".to_string()),
                (
                    crate::history::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                    "req-1".to_string()
                )
            ]
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
        // A later version drops the table these columns were added to.
        assert!(!sqlite_table_exists(&pool, "active_extraction_chunks").await);
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
                    "nzb_zstd"
                ]
            )
            .await,
            2
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
                ]
            )
            .await,
            9
        );

        let job_history_nzb: Vec<u8> =
            sqlx::query_scalar("SELECT nzb_zstd FROM job_nzbs WHERE job_id = 41")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(job_history_nzb, expected_nzb_bytes);

        let active_job_nzb: Vec<u8> =
            sqlx::query_scalar("SELECT nzb_zstd FROM job_nzbs WHERE job_id = 42")
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
            legacy_line_ending_checksums: Vec::new(),
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

    // A connection caches the schema it last read; a migration that drops a
    // column added through another connection fails on that cache.
    #[tokio::test]
    async fn a_migration_drops_a_column_another_connection_added() {
        use sqlx::Connection as _;
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("stale.db").display()
        );
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        ensure_migration_ledger_shape(&pool).await.unwrap();
        sqlx::raw_sql("CREATE TABLE stale (id INTEGER PRIMARY KEY);")
            .execute(&pool)
            .await
            .unwrap();
        let mut other = sqlx::SqliteConnection::connect(&url).await.unwrap();
        sqlx::raw_sql("ALTER TABLE stale ADD COLUMN gone INTEGER;")
            .execute(&mut other)
            .await
            .unwrap();
        other.close().await.unwrap();

        let payload = b"ALTER TABLE stale DROP COLUMN gone;";
        let migration = CompiledMigration {
            version: 9_998,
            description: "drops a column another connection added".to_string(),
            key: "9998_drop_gone".to_string(),
            filename: "synthetic.sql".to_string(),
            checksum_algo: ChecksumAlgorithm::Blake3,
            checksum: ChecksumAlgorithm::Blake3.digest(payload),
            legacy_line_ending_checksums: Vec::new(),
            steps: vec![CompiledMigrationStep::Sql {
                file: "synthetic.sql".to_string(),
                engine: EngineScope::Sqlite,
                scope: StepScope::All,
                payload: PayloadSlice {
                    start: 0,
                    len: payload.len() as u64,
                },
            }],
        };
        apply_single_migration(&pool, &migration, payload, MigrationInstallKind::Upgrade)
            .await
            .unwrap();

        let columns: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('stale') WHERE name = 'gone'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(columns, 0);
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

    #[test]
    fn superseded_v27_ledger_checksum_is_accepted_but_current_uses_normal_path() {
        // A pre-edit weaver-v0.6.9 install stores this blake3 checksum for
        // migration 27 in its ledger. Editing 0027's Postgres payload changes the
        // embedded checksum, so this historical value must remain acceptable via
        // the amnesty or every already-upgraded install would fail startup.
        const V27_PRE_EDIT_CHECKSUM_HEX: &str =
            "e05da91d94e32687581efb95f0cbeb0562d3aa5617d74b2d3254395f3ea1d286";
        let old_checksum = (0..V27_PRE_EDIT_CHECKSUM_HEX.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&V27_PRE_EDIT_CHECKSUM_HEX[i..i + 2], 16).unwrap())
            .collect::<Vec<u8>>();

        assert!(crate::migration_assets::is_superseded_migration_checksum(
            27,
            "blake3",
            &old_checksum
        ));

        // The current embedded checksum is different and is NOT itself in the
        // amnesty list (it validates through the normal equality path).
        let catalog = embedded_catalog().unwrap();
        let current = catalog.find_migration(27).unwrap();
        assert_ne!(
            crate::migration_assets::checksum_hex(&current.checksum),
            V27_PRE_EDIT_CHECKSUM_HEX
        );
        assert!(!crate::migration_assets::is_superseded_migration_checksum(
            27,
            current.checksum_algo.as_str(),
            &current.checksum
        ));

        // The amnesty is exact: a wrong version or a random checksum is rejected.
        assert!(!crate::migration_assets::is_superseded_migration_checksum(
            26,
            "blake3",
            &old_checksum
        ));
        assert!(!crate::migration_assets::is_superseded_migration_checksum(
            27,
            "blake3",
            &[0xAB; 32]
        ));
    }

    // A database written by a build whose checkout carried CRLF SQL (the
    // released Windows binary) must open under a build from an LF checkout,
    // and must come out of that startup holding the canonical checksum so the
    // next open takes the plain equality path.
    #[tokio::test]
    async fn legacy_line_ending_ledger_checksum_opens_and_is_healed() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .unwrap();

        let catalog = embedded_catalog().unwrap();
        // Pick a migration whose body actually has line breaks, so a CRLF form
        // exists at all.
        let migration = catalog
            .migrations
            .iter()
            .find(|migration| !migration.legacy_line_ending_checksums.is_empty())
            .expect("at least one embedded migration must have a CRLF variant");
        let legacy = migration.legacy_line_ending_checksums[0].clone();
        assert_ne!(legacy, migration.checksum);

        sqlx::query("UPDATE _sqlx_migrations SET checksum = ?1 WHERE version = ?2")
            .bind(&legacy)
            .bind(migration.version)
            .execute(&pool)
            .await
            .unwrap();

        run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .expect("a CRLF-era ledger checksum must be accepted");

        let healed: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = ?1")
                .bind(migration.version)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            healed, migration.checksum,
            "the ledger must be rewritten to the canonical checksum"
        );

        // And the amnesty stays narrow: a body that is genuinely different
        // still fails.
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ?1 WHERE version = ?2")
            .bind(vec![0xABu8; 32])
            .bind(migration.version)
            .execute(&pool)
            .await
            .unwrap();
        let err = run_embedded_migrations(&pool, MigrationMode::Apply)
            .await
            .expect_err("an unrelated checksum must still be rejected");
        assert!(
            err.to_string().contains("checksum mismatch"),
            "unexpected error: {err}"
        );
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

    // The batched held-path writes and the tag reads that replaced the
    // per-torrent statements and the whole-table read each go through a
    // key or index of the current schema, with no table walked and no sort.
    #[tokio::test]
    async fn held_path_batches_and_tag_reads_are_served_by_keys() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("current.db");
        run_embedded_migrations_on_path_blocking(&db_path).unwrap();
        let pool = open_test_pool(&db_path).await;
        for (query, step) in [
            (
                "DELETE FROM torrent_held_paths WHERE job_id IN (1, 2, 3)",
                "SEARCH torrent_held_paths USING INDEX sqlite_autoindex_torrent_held_paths_1 (job_id=?)",
            ),
            (
                "UPDATE torrents SET held_set = 1 WHERE job_id IN (1, 2, 3)",
                "USING INTEGER PRIMARY KEY (rowid=?)",
            ),
            (
                "SELECT tag FROM job_tags WHERE job_id = 7",
                "USING COVERING INDEX sqlite_autoindex_job_tags_1 (job_id=?)",
            ),
            (
                "SELECT tag, COUNT(*) AS jobs FROM job_tags GROUP BY tag",
                "USING COVERING INDEX idx_job_tags_tag",
            ),
        ] {
            let plan = explain(&pool, query).await;
            assert!(
                plan.iter().any(|detail| detail.contains(step)),
                "{query}: {plan:?}"
            );
            assert!(
                !plan.iter().any(|detail| detail.contains("TEMP B-TREE")),
                "{query}: {plan:?}"
            );
        }
        pool.close().await;
    }
}
