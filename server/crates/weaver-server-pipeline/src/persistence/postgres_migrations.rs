// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
#[cfg(test)]
use crate::migration_assets::CompiledMigrationCatalog;
use crate::migration_assets::MigrationInstallKind;
#[cfg(test)]
use crate::migration_assets::{CompiledMigrationStep, EngineScope};
use crate::persistence::sql_runtime::SqlConn;
use crate::schema_migrations::{
    MigrationMode, egress_quotas_v53, history_release_name_v87, history_retention_v89,
    history_search_key_v88, networking_bandwidth_v94, nzb_tables_v85, proxy_dns_v125,
    proxy_resolver_v64, script_concurrency_v58, script_instances_v55, server_limit_v92,
    torrent_private_v126, tracker_hosts_v105, unwanted_extensions_v56,
};
pub(crate) use runner::max_recorded_migration_version;
use sqlx::{AssertSqlSafe, PgPool};
use weaver_server_migrations::postgres_migrations as runner;
use weaver_server_migrations::schema_migrations::db_err;

struct DomainHooks;

impl runner::PostgresMigrationHooks for DomainHooks {
    async fn run_rust_hook(
        hook_id: &str,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        version: i64,
        install_kind: MigrationInstallKind,
    ) -> Result<(), StateError> {
        run_postgres_rust_hook(hook_id, tx, version, install_kind).await
    }
}

pub(crate) async fn run_migrations(pool: &PgPool, mode: MigrationMode) -> Result<(), StateError> {
    runner::run_migrations::<DomainHooks>(pool, matches!(mode, MigrationMode::ValidateOnly)).await
}

#[cfg(test)]
pub(crate) async fn replay_catalog_into_fresh_db(
    pool: &PgPool,
    catalog: &CompiledMigrationCatalog,
    payload_bytes: &[u8],
    through_version: Option<i64>,
) -> Result<(), StateError> {
    runner::replay_catalog_into_fresh_db::<DomainHooks>(
        pool,
        catalog,
        payload_bytes,
        through_version,
    )
    .await
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
        egress_quotas_v53::HOOK_ID => {
            egress_quotas_v53::move_isp_cap_to_system_egress(&mut SqlConn::Postgres(tx)).await
        }
        script_instances_v55::HOOK_ID => {
            script_instances_v55::move_script_wiring_to_instances(&mut SqlConn::Postgres(tx)).await
        }
        unwanted_extensions_v56::HOOK_ID => {
            unwanted_extensions_v56::fill_default_unwanted_extensions(&mut SqlConn::Postgres(tx))
                .await
        }
        script_concurrency_v58::HOOK_ID => {
            script_concurrency_v58::raise_script_concurrency(&mut SqlConn::Postgres(tx)).await
        }
        proxy_resolver_v64::HOOK_ID => {
            proxy_resolver_v64::name_proxy_resolvers(&mut SqlConn::Postgres(tx)).await
        }
        nzb_tables_v85::HOOK_ID => {
            nzb_tables_v85::move_nzbs_and_outlines(&mut SqlConn::Postgres(tx)).await
        }
        history_release_name_v87::HOOK_ID => {
            history_release_name_v87::fill_history_release_names(&mut SqlConn::Postgres(tx)).await
        }
        history_search_key_v88::HOOK_ID => {
            history_search_key_v88::fill_history_search_keys_and_fuzzy(&mut SqlConn::Postgres(tx))
                .await
        }
        history_retention_v89::HOOK_ID => {
            history_retention_v89::seed_history_retention(&mut SqlConn::Postgres(tx)).await
        }
        server_limit_v92::HOOK_ID => {
            server_limit_v92::trim_servers_to_limit(&mut SqlConn::Postgres(tx)).await
        }
        networking_bandwidth_v94::HOOK_ID => {
            networking_bandwidth_v94::move_bandwidth_to_networking(&mut SqlConn::Postgres(tx)).await
        }
        tracker_hosts_v105::HOOK_ID => {
            tracker_hosts_v105::fill_tracker_hosts(&mut SqlConn::Postgres(tx)).await
        }
        proxy_dns_v125::HOOK_ID => {
            proxy_dns_v125::dns_profiles_from_proxies(&mut SqlConn::Postgres(tx)).await
        }
        torrent_private_v126::HOOK_ID => {
            torrent_private_v126::fill_torrent_private(&mut SqlConn::Postgres(tx)).await
        }
        other => Err(StateError::Database(format!(
            "PostgreSQL migration hook '{other}' is not implemented"
        ))),
    }
}

#[cfg(test)]
fn implemented_postgres_rust_hooks() -> &'static [&'static str] {
    &[
        "restart_active_jobs_drop_active_segments_v28",
        egress_quotas_v53::HOOK_ID,
        script_instances_v55::HOOK_ID,
        unwanted_extensions_v56::HOOK_ID,
        script_concurrency_v58::HOOK_ID,
        proxy_resolver_v64::HOOK_ID,
        nzb_tables_v85::HOOK_ID,
        history_release_name_v87::HOOK_ID,
        history_search_key_v88::HOOK_ID,
        history_retention_v89::HOOK_ID,
        server_limit_v92::HOOK_ID,
        networking_bandwidth_v94::HOOK_ID,
        tracker_hosts_v105::HOOK_ID,
        proxy_dns_v125::HOOK_ID,
        torrent_private_v126::HOOK_ID,
    ]
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: Postgres, no writer gate; the migration run is alone at startup"
)]
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

    // Provisions an isolated schema on the server named by WEAVER_TEST_POSTGRES_URL
    // and returns an admin pool, the schema name, and a pool whose search_path is
    // pinned to that schema. Returns None (test skips) when the env var is unset or
    // empty, mirroring the pattern in persistence/connection/tests.rs.
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
        let scoped_pool = match sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&target_url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => {
                drop_schema(&admin_pool, &schema).await;
                panic!("could not connect to the test schema: {error}");
            }
        };

        Some((admin_pool, schema, scoped_pool))
    }

    async fn drop_schema(admin_pool: &PgPool, schema: &str) {
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(admin_pool)
            .await
            .unwrap();
        admin_pool.close().await;
    }

    // Runs `body` over a schema of its own and drops the schema after it,
    // whether the body passed or panicked; a panic then goes on. Skips, as
    // `create_scoped_pool` does, when no server is named.
    async fn in_scoped_schema<F, Fut>(body: F)
    where
        F: FnOnce(PgPool) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let Some((admin_pool, schema, pool)) = create_scoped_pool().await else {
            return;
        };
        let ran = tokio::spawn(body(pool.clone())).await;
        pool.close().await;
        // The drop runs on its own task too, so a panic in it cannot hide
        // the body's: the body's panic goes on first.
        let dropped = tokio::spawn(async move { drop_schema(&admin_pool, &schema).await }).await;
        if let Err(error) = ran {
            match error.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(error) => panic!("the test body did not finish: {error}"),
            }
        }
        // The body passed: a drop that failed fails the test.
        if let Err(error) = dropped {
            match error.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(error) => panic!("the schema drop did not finish: {error}"),
            }
        }
    }

    // A schema stamped through `version`, upgraded from there to the
    // current one after `seed` writes its rows at that version.
    async fn upgrade_from<F, Fut>(pool: &PgPool, version: i64, seed: F)
    where
        F: FnOnce(PgPool) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let catalog = crate::schema_migrations::embedded_catalog().unwrap();
        let payload = crate::schema_migrations::embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(pool, &catalog, &payload, Some(version))
            .await
            .unwrap();
        let stamped: i64 =
            sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(stamped, version, "the fixture starts at {version}");
        seed(pool.clone()).await;
        run_migrations(pool, MigrationMode::Apply)
            .await
            .expect("the upgrade applies");
        let current: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(current, catalog.max_version());
    }

    // Each index of `table` in the test schema, by name, with its definition.
    async fn indexes_of(pool: &PgPool, table: &str) -> std::collections::BTreeMap<String, String> {
        sqlx::query_as::<_, (String, String)>(
            "SELECT indexname, indexdef FROM pg_indexes
              WHERE schemaname = current_schema() AND tablename = $1",
        )
        .bind(table)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .collect()
    }

    async fn insert_kept_torrent(
        pool: &PgPool,
        job_id: i64,
        files_dir: Option<&str>,
        storage_dir: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO torrents
                (job_id, infohash_v1, name, source, added_at_ms, state, files_dir, storage_dir)
             VALUES ($1, $2, $3, 'magnet', $1, 'downloading', $4, $5)",
        )
        .bind(job_id)
        .bind(vec![u8::try_from(job_id).unwrap(); 20])
        .bind(format!("torrent-{job_id}"))
        .bind(files_dir)
        .bind(storage_dir)
        .execute(pool)
        .await
        .unwrap();
    }

    // 0120: a torrent list's page is served in added order, the whole list's
    // and a category's, and the category index it supersedes is gone.
    #[tokio::test]
    async fn postgres_0120_orders_torrents_by_added_when_configured() {
        in_scoped_schema(|pool| async move {
            upgrade_from(&pool, 119, |_| async {}).await;
            let indexes = indexes_of(&pool, "torrents").await;
            assert!(
                indexes["idx_torrents_added"].ends_with("(added_at_ms, job_id)"),
                "{indexes:?}"
            );
            assert!(
                indexes["idx_torrents_category_added"]
                    .ends_with("(category_id, added_at_ms, job_id)"),
                "{indexes:?}"
            );
            assert!(
                !indexes.contains_key("idx_torrents_category"),
                "{indexes:?}"
            );
        })
        .await;
    }

    // 0126: a torrent row keeps its privacy, filled for each torrent kept
    // before it from its metainfo, a page at a time across more than one
    // page; a magnet with none and a metainfo that does not read stay
    // public. Two indexes serve a privacy's list in the order of adding.
    #[tokio::test]
    async fn postgres_0126_fills_each_torrents_privacy_when_configured() {
        use crate::schema_migrations::torrent_private_v126::tiny_metainfo;
        in_scoped_schema(|pool| async move {
            upgrade_from(&pool, 125, |pool| async move {
                for job_id in 1..=200_i64 {
                    let metainfo = match job_id % 4 {
                        0 => Some(tiny_metainfo(&format!("t{job_id}"), true)),
                        1 => Some(tiny_metainfo(&format!("t{job_id}"), false)),
                        2 => None,
                        _ => Some(b"not bencode".to_vec()),
                    };
                    sqlx::query(
                        "INSERT INTO torrents
                            (job_id, infohash_v1, name, source, added_at_ms, state, metainfo)
                         VALUES ($1, $2, 'privacy', 'magnet', $1, 'queued', $3)",
                    )
                    .bind(job_id)
                    .bind(job_id.to_be_bytes().repeat(3)[..20].to_vec())
                    .bind(metainfo)
                    .execute(&pool)
                    .await
                    .unwrap();
                }
            })
            .await;
            let private: Vec<i64> =
                sqlx::query_scalar("SELECT job_id FROM torrents WHERE private ORDER BY job_id")
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                private,
                (1..=200)
                    .filter(|job_id| job_id % 4 == 0)
                    .collect::<Vec<_>>()
            );
            let indexes = indexes_of(&pool, "torrents").await;
            assert!(
                indexes["idx_torrents_private_added"]
                    .ends_with("(private, added_at_ms, job_id, category_id)"),
                "{indexes:?}"
            );
            assert!(
                indexes["idx_torrents_category_private_added"]
                    .ends_with("(category_id, private, added_at_ms, job_id)"),
                "{indexes:?}"
            );
        })
        .await;
    }

    // 0121: a torrent row carries its removal, NULL unless one is under
    // way, and the torrents being removed are found by a partial index.
    #[tokio::test]
    async fn postgres_0121_adds_torrent_retiring_when_configured() {
        in_scoped_schema(|pool| async move {
            upgrade_from(&pool, 120, |pool| async move {
                insert_kept_torrent(&pool, 1, None, Some("/save/a")).await;
            })
            .await;
            let (kind, nullable): (String, String) = sqlx::query_as(
                "SELECT data_type, is_nullable FROM information_schema.columns
                  WHERE table_schema = current_schema()
                    AND table_name = 'torrents' AND column_name = 'retiring'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!((kind.as_str(), nullable.as_str()), ("text", "YES"));
            let retiring: Option<String> =
                sqlx::query_scalar("SELECT retiring FROM torrents WHERE job_id = 1")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                retiring, None,
                "a torrent kept before it is not being removed"
            );
            let indexes = indexes_of(&pool, "torrents").await;
            let partial = &indexes["idx_torrents_retiring"];
            assert!(partial.contains("(job_id)"), "{partial}");
            assert!(
                partial.contains("WHERE (retiring IS NOT NULL)"),
                "{partial}"
            );
        })
        .await;
    }

    // 0122: a history row's files have a table of their own, and each
    // torrent kept before it holds its whole folder, as spelled, its files'
    // folder before its storage folder, until its next start writes its own.
    #[tokio::test]
    async fn postgres_0122_holds_each_kept_torrents_folder_when_configured() {
        in_scoped_schema(|pool| async move {
            upgrade_from(&pool, 121, |pool| async move {
                insert_kept_torrent(&pool, 1, Some("/files/a"), Some("/save/a")).await;
                insert_kept_torrent(&pool, 2, None, Some("/save/b")).await;
                insert_kept_torrent(&pool, 3, None, None).await;
            })
            .await;
            let held: Vec<(i64, String)> =
                sqlx::query_as("SELECT job_id, path FROM torrent_held_paths ORDER BY job_id, path")
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                held,
                vec![(1, "/files/a".to_string()), (2, "/save/b".to_string())]
            );
            let set: Vec<bool> =
                sqlx::query_scalar("SELECT held_set FROM torrents ORDER BY job_id")
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                set,
                vec![false, false, false],
                "no torrent's rows are its own yet"
            );
            let by_path =
                &indexes_of(&pool, "torrent_held_paths").await["idx_torrent_held_paths_path"];
            assert!(by_path.contains(r#"(path COLLATE "C")"#), "{by_path}");

            let files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_history_files")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(files, 0, "no history row lists files before it");
            sqlx::query("INSERT INTO job_history_files (job_id, path) VALUES (1, '/files/a/x')")
                .execute(&pool)
                .await
                .unwrap();
            let whole: bool =
                sqlx::query_scalar("SELECT whole FROM job_history_files WHERE job_id = 1")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert!(!whole, "a listed file is a file unless it is said whole");
        })
        .await;
    }

    // 0123: the RSS items held while torrents are off are found by a
    // partial index of their own.
    #[tokio::test]
    async fn postgres_0123_indexes_rss_items_held_when_configured() {
        in_scoped_schema(|pool| async move {
            upgrade_from(&pool, 122, |_| async move {}).await;
            let indexes = indexes_of(&pool, "rss_seen_items").await;
            let partial = &indexes["idx_rss_seen_waiting"];
            assert!(partial.contains("(feed_id)"), "{partial}");
            assert!(
                partial.contains("WHERE (decision = 'waiting'::text)"),
                "{partial}"
            );
        })
        .await;
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

    // A Postgres install upgrading from schema <= 26 that carries a corrupt
    // job_history.metadata row (unparseable text or valid-but-non-array JSON)
    // must not hard-fail on the 0027/0030 backfills. Corrupt rows contribute no
    // attributes and are left untouched; a well-formed array row backfills and
    // has its diagnostic-only keys stripped by 0030.
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

    // A weaver-v0.6.9 Postgres install already applied migration 27 and stored
    // its pre-edit checksum. After 0027's payload is edited in place the embedded
    // checksum differs, so without the supersede amnesty startup validation would
    // hard-fail with "checksum mismatch". This proves the amnesty lets such an
    // install continue and no-op cleanly.
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
