// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 90: a history entry keeps when housekeeping swept a failed job's
// files and left nothing of it, with a partial index for the sweep's page.
// The step is SQL only; these tests replay the real catalog to 89 and
// upgrade it.

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    const SCHEMA_VERSION: i64 = 90;

    async fn upgraded_from_89() -> SqlitePool {
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
        sqlx::query(
            "INSERT INTO job_history (
                 job_id, name, status, error_message, total_bytes, downloaded_bytes,
                 failed_bytes, health, category, output_dir, nzb_path, created_at,
                 completed_at, metadata, files_removed_at
             ) VALUES (7, 'kept', 'failed', NULL, 0, 0, 0, 1000, NULL, NULL, NULL, 1, 2, NULL, 5)",
        )
        .execute(&pool)
        .await
        .unwrap();
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

    // Existing history upgrades unswept, so the first sweep after it looks
    // at every failed job once; nothing else on the row changes.
    #[tokio::test]
    async fn the_upgrade_adds_an_empty_swept_stamp_and_keeps_existing_history() {
        let pool = upgraded_from_89().await;
        let (swept, removed): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT files_swept_at, files_removed_at FROM job_history WHERE job_id = 7",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(swept, None);
        assert_eq!(removed, Some(5));
    }

    #[tokio::test]
    async fn the_upgrade_adds_the_partial_index_for_the_sweep() {
        let pool = upgraded_from_89().await;
        let sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master
              WHERE type = 'index' AND name = 'idx_job_history_failed_unswept'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(sql.contains("WHERE status = 'failed' AND files_swept_at IS NULL"));
    }
}
