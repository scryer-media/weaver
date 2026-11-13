// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 89, upgrade step: an install that already has history keeps all
// of it.
//
// History is pruned by age from 89 on, after 90 days unless the operator
// chooses otherwise. An install upgrading with history in it would lose
// every entry older than that on its first maintenance pass, so it upgrades
// with retention off: the setting is written as 0. An install with no
// history writes nothing and reads the default. A setting already saved is
// kept. A restore of an older bundle runs the same step.
//
// The settings key and the value are spelled out here as they stood at
// schema 89, so later changes to the settings type do not change what this
// step does.

use crate::StateError;
use crate::persistence::sql_runtime::SqlConn;

pub(crate) const HOOK_ID: &str = "seed_history_retention_v89";
// The schema that first prunes history by age. A backup taken below it
// restores history that was never pruned.
pub(crate) const SCHEMA_VERSION: i64 = 89;

const SEED: &str = "INSERT INTO settings (key, value)
    SELECT 'history_retention_days', '0'
     WHERE EXISTS (SELECT 1 FROM job_history)
    ON CONFLICT(key) DO NOTHING";

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn seed_history_retention(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    conn.execute(SEED, &[]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use super::*;

    async fn pool_with(history_rows: usize, saved: Option<&str>) -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE settings (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
             CREATE TABLE job_history (job_id INTEGER PRIMARY KEY)",
        )
        .execute(&pool)
        .await
        .unwrap();
        for job_id in 0..history_rows {
            sqlx::query("INSERT INTO job_history (job_id) VALUES (?)")
                .bind(job_id as i64)
                .execute(&pool)
                .await
                .unwrap();
        }
        if let Some(saved) = saved {
            sqlx::query("INSERT INTO settings (key, value) VALUES ('history_retention_days', ?)")
                .bind(saved)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool
    }

    async fn seeded(pool: &SqlitePool) -> Option<String> {
        let mut conn = pool.acquire().await.unwrap();
        // Twice: the step is idempotent.
        for _ in 0..2 {
            seed_history_retention(&mut SqlConn::Sqlite(&mut conn))
                .await
                .unwrap();
        }
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'history_retention_days'")
            .fetch_optional(&mut *conn)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_install_with_history_upgrades_with_retention_off() {
        let pool = pool_with(2, None).await;
        assert_eq!(seeded(&pool).await.as_deref(), Some("0"));
    }

    #[tokio::test]
    async fn an_install_without_history_reads_the_default() {
        let pool = pool_with(0, None).await;
        assert_eq!(seeded(&pool).await, None);
    }

    #[tokio::test]
    async fn a_saved_retention_is_kept() {
        let pool = pool_with(1, Some("30")).await;
        assert_eq!(seeded(&pool).await.as_deref(), Some("30"));
    }

    // The real catalog, replayed to 88 and upgraded to 89: the column is
    // added and the step runs as part of it.
    async fn upgraded_from_88(history: bool) -> SqlitePool {
        use super::super::{
            apply_version_range, embedded_catalog, embedded_payload_bytes,
            replay_catalog_into_fresh_db,
        };
        use crate::migration_assets::MigrationInstallKind;

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(SCHEMA_VERSION - 1), true)
            .await
            .unwrap();
        if history {
            sqlx::query(
                "INSERT INTO job_history (
                     job_id, name, status, error_message, total_bytes, downloaded_bytes,
                     failed_bytes, health, category, output_dir, nzb_path, created_at,
                     completed_at, metadata
                 ) VALUES (7, 'kept', 'complete', NULL, 0, 0, 0, 1000, NULL, NULL, NULL, 1, 2, NULL)",
            )
            .execute(&pool)
            .await
            .unwrap();
        }
        apply_version_range(
            &pool,
            &catalog,
            &payload,
            MigrationInstallKind::Upgrade,
            SCHEMA_VERSION,
            SCHEMA_VERSION,
        )
        .await
        .unwrap();
        pool
    }

    async fn saved(pool: &SqlitePool) -> Option<String> {
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'history_retention_days'")
            .fetch_optional(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_upgrade_adds_an_empty_stamp_and_keeps_existing_history() {
        let pool = upgraded_from_88(true).await;
        let stamp: Option<i64> =
            sqlx::query_scalar("SELECT files_removed_at FROM job_history WHERE job_id = 7")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stamp, None);
        assert_eq!(saved(&pool).await.as_deref(), Some("0"));
    }

    #[tokio::test]
    async fn the_upgrade_without_history_leaves_the_default() {
        let pool = upgraded_from_88(false).await;
        assert_eq!(saved(&pool).await, None);
    }
}
