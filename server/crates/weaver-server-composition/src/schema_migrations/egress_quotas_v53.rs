// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::egress_quotas_v53::*;

#[cfg(test)]
mod tests {
    use crate::bandwidth::QuotaWeekday;
    use crate::persistence::sql_runtime::SqlConn;
    use crate::servers::{ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};
    use chrono::Local;
    use chrono::TimeZone;
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;

    // A database as the build before egress quotas left it.
    async fn at_schema_52() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        super::super::replay_catalog_into_fresh_db(
            &pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            Some(SCHEMA_VERSION - 1),
            true,
        )
        .await
        .unwrap();
        pool
    }

    async fn upgrade_to_53(pool: &SqlitePool) {
        apply_version_range(
            pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            MigrationInstallKind::Upgrade,
            SCHEMA_VERSION,
            SCHEMA_VERSION,
        )
        .await
        .unwrap();
    }

    async fn setting(pool: &SqlitePool, key: &str, value: &str) {
        sqlx::query("INSERT INTO settings (key, value) VALUES (?1, ?2)")
            .bind(key)
            .bind(value)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn bucket(pool: &SqlitePool, minute: i64, metered: bool, bytes: i64) {
        sqlx::query(
            "INSERT INTO bandwidth_usage_minute_buckets (bucket_epoch_minute, metered, payload_bytes)
             VALUES (?1, ?2, ?3)",
        )
        .bind(minute)
        .bind(i64::from(metered))
        .bind(bytes)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn remaining_cap_keys(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key LIKE 'bandwidth_cap.%'")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_enabled_cap_becomes_the_system_egress_quota_with_its_window_usage() {
        let pool = at_schema_52().await;
        for (key, value) in [
            ("bandwidth_cap.enabled", "true"),
            ("bandwidth_cap.period", "monthly"),
            ("bandwidth_cap.limit_bytes", "5000"),
            ("bandwidth_cap.reset_time_minutes_local", "120"),
            ("bandwidth_cap.weekly_reset_weekday", "wed"),
            ("bandwidth_cap.monthly_reset_day", "3"),
        ] {
            setting(&pool, key, value).await;
        }
        let now = Local.with_ymd_and_hms(2026, 7, 9, 12, 0, 0).unwrap();
        let quota = ServerDownloadQuotaConfig {
            enabled: true,
            limit_bytes: 5_000,
            period: ServerDownloadQuotaPeriod::Monthly,
            reset_time_minutes_local: 120,
            weekly_reset_weekday: QuotaWeekday::Wed,
            monthly_reset_day: 3,
        };
        let window = crate::servers::transfer_policy::quota_window(now, (&quota).into()).unwrap();
        let first = window.start.timestamp().div_euclid(60);
        let last = window.end.timestamp().div_euclid(60) - 1;
        // Counted: metered bytes inside the window.
        bucket(&pool, first, true, 300).await;
        bucket(&pool, last, true, 400).await;
        // Not counted: unmetered bytes, and bytes outside the window.
        bucket(&pool, first, false, 9_000).await;
        bucket(&pool, first - 1, true, 9_000).await;
        bucket(&pool, last + 1, true, 9_000).await;

        // The schema 53 tables, then the step, with a fixed clock.
        let mut tx = pool.begin().await.unwrap();
        for statement in include_str!(
            "../../../weaver-server-migrations/src/db/migrations/0053_egress_interfaces/schema.sql"
        )
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
        {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        move_at(&mut SqlConn::Sqlite(&mut tx), now).await.unwrap();
        tx.commit().await.unwrap();

        let row: (i64, i64, String, i64, String, i64) = sqlx::query_as(
            "SELECT download_quota_enabled, download_quota_limit_bytes, download_quota_period,
                    download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday,
                    download_quota_monthly_reset_day
               FROM egress_interfaces WHERE id = 0",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (1, 5_000, "monthly".into(), 120, "wed".into(), 3));
        let usage: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT lifetime_bytes, quota_baseline_bytes, window_start_epoch_seconds,
                    window_end_epoch_seconds
               FROM egress_download_usage WHERE egress_id = 0",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        // The quota picks the window up as its own, so the bytes count as used.
        assert_eq!(
            usage,
            (700, 0, window.start.timestamp(), window.end.timestamp())
        );
        assert_eq!(remaining_cap_keys(&pool).await, 0);
    }

    #[tokio::test]
    async fn an_upgrade_runs_the_step_and_leaves_no_cap_behind() {
        let pool = at_schema_52().await;
        for (key, value) in [
            ("bandwidth_cap.enabled", "true"),
            ("bandwidth_cap.period", "daily"),
            ("bandwidth_cap.limit_bytes", "1000"),
            ("bandwidth_cap.reset_time_minutes_local", "0"),
            ("bandwidth_cap.weekly_reset_weekday", "mon"),
            ("bandwidth_cap.monthly_reset_day", "1"),
        ] {
            setting(&pool, key, value).await;
        }

        upgrade_to_53(&pool).await;

        let enabled: (i64, i64) = sqlx::query_as(
            "SELECT download_quota_enabled, download_quota_limit_bytes
               FROM egress_interfaces WHERE id = 0",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(enabled, (1, 1_000));
        let usage: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM egress_download_usage")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(usage, 1);
        assert_eq!(remaining_cap_keys(&pool).await, 0);
    }

    #[tokio::test]
    async fn no_cap_leaves_the_system_egress_without_a_quota() {
        let pool = at_schema_52().await;
        // A cap that was saved but turned off is removed without a quota.
        for (key, value) in [
            ("bandwidth_cap.enabled", "false"),
            ("bandwidth_cap.period", "monthly"),
            ("bandwidth_cap.limit_bytes", "1000"),
            ("bandwidth_cap.reset_time_minutes_local", "0"),
            ("bandwidth_cap.weekly_reset_weekday", "mon"),
            ("bandwidth_cap.monthly_reset_day", "1"),
        ] {
            setting(&pool, key, value).await;
        }
        bucket(&pool, 1, true, 500).await;

        upgrade_to_53(&pool).await;

        let enabled: (i64, i64) = sqlx::query_as(
            "SELECT download_quota_enabled, download_quota_limit_bytes
               FROM egress_interfaces WHERE id = 0",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(enabled, (0, 0));
        let usage: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM egress_download_usage")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(usage, 0);
        assert_eq!(remaining_cap_keys(&pool).await, 0);

        // And a database that never saved a cap upgrades the same way.
        let pool = at_schema_52().await;
        upgrade_to_53(&pool).await;
        let enabled: i64 =
            sqlx::query_scalar("SELECT download_quota_enabled FROM egress_interfaces WHERE id = 0")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(enabled, 0);
    }
}
