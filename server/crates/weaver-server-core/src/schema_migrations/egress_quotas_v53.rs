//! Migration 53, upgrade step: the bandwidth cap earlier builds kept for the
//! whole instance becomes the download quota of the System egress.
//!
//! An enabled cap is copied onto egress 0 as its download quota, and the
//! bytes the cap had already counted in its current window are carried into
//! that egress's usage, so the quota resumes where the cap left off. The
//! settings that held the cap are removed either way.
//!
//! The settings keys and the cap's stored forms are spelled out here as they
//! stood at schema 53. The one thing borrowed from the running build is the
//! calendar arithmetic that places a window, which the quota shares.

use chrono::{DateTime, Local};

use crate::StateError;
use crate::bandwidth::QuotaWeekday;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};
use crate::servers::{ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};

pub(crate) const HOOK_ID: &str = "move_isp_cap_to_system_egress_v53";
/// The schema that first keeps quotas on egresses. A backup taken below it
/// can carry the cap instead.
pub(crate) const SCHEMA_VERSION: i64 = 53;

const ENABLED_KEY: &str = "bandwidth_cap.enabled";
const PERIOD_KEY: &str = "bandwidth_cap.period";
const LIMIT_KEY: &str = "bandwidth_cap.limit_bytes";
const RESET_TIME_KEY: &str = "bandwidth_cap.reset_time_minutes_local";
const WEEKDAY_KEY: &str = "bandwidth_cap.weekly_reset_weekday";
const MONTHLY_DAY_KEY: &str = "bandwidth_cap.monthly_reset_day";
const KEYS: [&str; 6] = [
    ENABLED_KEY,
    PERIOD_KEY,
    LIMIT_KEY,
    RESET_TIME_KEY,
    WEEKDAY_KEY,
    MONTHLY_DAY_KEY,
];

const SYSTEM_EGRESS_ID: i64 = 0;

/// Run the step on a connection that is already inside the transaction it
/// belongs to.
pub(crate) async fn move_isp_cap_to_system_egress(
    conn: &mut SqlConn<'_>,
) -> Result<(), StateError> {
    move_at(conn, crate::e2e_clock::local_now()).await
}

async fn move_at(conn: &mut SqlConn<'_>, now: DateTime<Local>) -> Result<(), StateError> {
    let placeholders = vec!["{}"; KEYS.len()].join(", ");
    let rows = conn
        .fetch_all(
            &format!("SELECT key, value FROM settings WHERE key IN ({placeholders})"),
            &KEYS
                .iter()
                .map(|key| SqlArg::Text((*key).into()))
                .collect::<Vec<_>>(),
        )
        .await?;
    let mut saved = std::collections::BTreeMap::new();
    for row in rows {
        saved.insert(row.text("key")?, row.text("value")?);
    }

    match saved_cap(&saved) {
        Some(cap) if cap.enabled => copy_cap(conn, &cap, now).await?,
        Some(_) => {}
        None if saved.is_empty() => {}
        None => tracing::warn!(
            "the saved bandwidth cap could not be read and was not carried onto the System egress"
        ),
    }

    conn.execute(
        &format!("DELETE FROM settings WHERE key IN ({placeholders})"),
        &KEYS
            .iter()
            .map(|key| SqlArg::Text((*key).into()))
            .collect::<Vec<_>>(),
    )
    .await?;
    Ok(())
}

/// The saved cap, read as the download quota it becomes.
fn saved_cap(
    saved: &std::collections::BTreeMap<String, String>,
) -> Option<ServerDownloadQuotaConfig> {
    let period = match saved.get(PERIOD_KEY)?.as_str() {
        "daily" => ServerDownloadQuotaPeriod::Daily,
        "weekly" => ServerDownloadQuotaPeriod::Weekly,
        "monthly" => ServerDownloadQuotaPeriod::Monthly,
        _ => return None,
    };
    let weekly_reset_weekday = match saved.get(WEEKDAY_KEY)?.as_str() {
        "mon" => QuotaWeekday::Mon,
        "tue" => QuotaWeekday::Tue,
        "wed" => QuotaWeekday::Wed,
        "thu" => QuotaWeekday::Thu,
        "fri" => QuotaWeekday::Fri,
        "sat" => QuotaWeekday::Sat,
        "sun" => QuotaWeekday::Sun,
        _ => return None,
    };
    let cap = ServerDownloadQuotaConfig {
        enabled: saved
            .get(ENABLED_KEY)
            .and_then(|value| value.parse().ok())
            .unwrap_or(false),
        period,
        limit_bytes: saved.get(LIMIT_KEY)?.parse().ok()?,
        reset_time_minutes_local: saved.get(RESET_TIME_KEY)?.parse().ok()?,
        weekly_reset_weekday,
        monthly_reset_day: saved.get(MONTHLY_DAY_KEY)?.parse().ok()?,
    };
    // The bounds a download quota is held to.
    let valid = cap.limit_bytes > 0
        && cap.limit_bytes <= i64::MAX as u64
        && cap.reset_time_minutes_local < 24 * 60
        && (1..=31).contains(&cap.monthly_reset_day);
    valid.then_some(cap)
}

async fn copy_cap(
    conn: &mut SqlConn<'_>,
    cap: &ServerDownloadQuotaConfig,
    now: DateTime<Local>,
) -> Result<(), StateError> {
    // `saved_cap` only reads the three windowed periods.
    let Some(window) = crate::bandwidth::service::compute_window(now, cap) else {
        return Ok(());
    };
    let period = cap.period.as_str();
    conn.execute(
        "UPDATE egress_interfaces
            SET download_quota_enabled = 1,
                download_quota_limit_bytes = {},
                download_quota_period = {},
                download_quota_reset_time_minutes_local = {},
                download_quota_weekly_reset_weekday = {},
                download_quota_monthly_reset_day = {}
          WHERE id = {}",
        &[
            SqlArg::I64(cap.limit_bytes as i64),
            SqlArg::Text(period.into()),
            SqlArg::I64(i64::from(cap.reset_time_minutes_local)),
            SqlArg::Text(
                crate::servers::record::quota_weekday_str(cap.weekly_reset_weekday).into(),
            ),
            SqlArg::I64(i64::from(cap.monthly_reset_day)),
            SqlArg::I64(SYSTEM_EGRESS_ID),
        ],
    )
    .await?;

    // What the cap had counted in the window it was in: every metered byte
    // in the per-minute ledger between the window's edges.
    let start = window.starts_at().timestamp();
    let end = window.ends_at().timestamp();
    let used = conn
        .fetch_all(
            "SELECT CAST(COALESCE(SUM(payload_bytes), 0) AS BIGINT) AS total
               FROM bandwidth_usage_minute_buckets
              WHERE bucket_epoch_minute >= {} AND bucket_epoch_minute < {} AND metered = 1",
            &[
                SqlArg::I64(start.div_euclid(60)),
                SqlArg::I64(end.div_euclid(60)),
            ],
        )
        .await?
        .first()
        .map(|row| row.i64("total"))
        .transpose()?
        .unwrap_or(0)
        .max(0);
    conn.execute(
        "INSERT INTO egress_download_usage
            (egress_id, lifetime_bytes, quota_baseline_bytes,
             window_start_epoch_seconds, window_end_epoch_seconds, updated_at_epoch_seconds)
         VALUES ({}, {}, 0, {}, {}, {})",
        &[
            SqlArg::I64(SYSTEM_EGRESS_ID),
            SqlArg::I64(used),
            SqlArg::I64(start),
            SqlArg::I64(end),
            SqlArg::I64(now.timestamp()),
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;

    /// A database as the build before egress quotas left it.
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
            (ENABLED_KEY, "true"),
            (PERIOD_KEY, "monthly"),
            (LIMIT_KEY, "5000"),
            (RESET_TIME_KEY, "120"),
            (WEEKDAY_KEY, "wed"),
            (MONTHLY_DAY_KEY, "3"),
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
        let window = crate::servers::transfer_policy::server_quota_window(now, &quota).unwrap();
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
        for statement in include_str!("../db/migrations/0053_egress_interfaces/schema.sql")
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
            (ENABLED_KEY, "true"),
            (PERIOD_KEY, "daily"),
            (LIMIT_KEY, "1000"),
            (RESET_TIME_KEY, "0"),
            (WEEKDAY_KEY, "mon"),
            (MONTHLY_DAY_KEY, "1"),
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
            (ENABLED_KEY, "false"),
            (PERIOD_KEY, "monthly"),
            (LIMIT_KEY, "1000"),
            (RESET_TIME_KEY, "0"),
            (WEEKDAY_KEY, "mon"),
            (MONTHLY_DAY_KEY, "1"),
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
