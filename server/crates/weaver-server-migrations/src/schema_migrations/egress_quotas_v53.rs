// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 53, upgrade step: the bandwidth cap earlier builds kept for the
// whole instance becomes the download quota of the System egress.
//
// An enabled cap is copied onto egress 0 as its download quota, and the
// bytes the cap had already counted in its current window are carried into
// that egress's usage, so the quota resumes where the cap left off. The
// settings that held the cap are removed either way.
//
// The settings keys and the cap's stored forms are spelled out here as they
// stood at schema 53. The one thing borrowed from the running build is the
// calendar arithmetic that places a window, which the quota shares.

use chrono::{DateTime, Local};

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};
use weaver_server_net::bandwidth::QuotaWeekday;
use weaver_server_net::servers::{ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};

pub const HOOK_ID: &str = "move_isp_cap_to_system_egress_v53";
// The schema that first keeps quotas on egresses. A backup taken below it
// can carry the cap instead.
pub const SCHEMA_VERSION: i64 = 53;

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

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn move_isp_cap_to_system_egress(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    move_at(conn, weaver_server_base::e2e_clock::local_now()).await
}

pub async fn move_at(conn: &mut SqlConn<'_>, now: DateTime<Local>) -> Result<(), StateError> {
    if let Some(carried) = take_cap(conn, now).await? {
        copy_cap(conn, &carried).await?;
        carry_usage(conn, &carried, now).await?;
    }
    Ok(())
}

// An enabled cap as the quota it becomes, with the window it was in and the
// bytes it had counted there.
pub(crate) struct CarriedCap {
    pub(crate) cap: ServerDownloadQuotaConfig,
    pub(crate) window_start: i64,
    pub(crate) window_end: i64,
    pub(crate) used: i64,
}

// Reads the saved cap and removes the settings that held it. `None` when no
// enabled cap was saved.
pub(crate) async fn take_cap(
    conn: &mut SqlConn<'_>,
    now: DateTime<Local>,
) -> Result<Option<CarriedCap>, StateError> {
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

    let carried = match saved_cap(&saved) {
        Some(cap) if cap.enabled => carried_cap(conn, cap, now).await?,
        Some(_) => None,
        None if saved.is_empty() => None,
        None => {
            tracing::warn!(
                "the saved bandwidth cap could not be read and was not carried onto the System egress"
            );
            None
        }
    };

    conn.execute(
        &format!("DELETE FROM settings WHERE key IN ({placeholders})"),
        &KEYS
            .iter()
            .map(|key| SqlArg::Text((*key).into()))
            .collect::<Vec<_>>(),
    )
    .await?;
    Ok(carried)
}

// The saved cap, read as the download quota it becomes.
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

async fn carried_cap(
    conn: &mut SqlConn<'_>,
    cap: ServerDownloadQuotaConfig,
    now: DateTime<Local>,
) -> Result<Option<CarriedCap>, StateError> {
    // `saved_cap` only reads the three windowed periods.
    let Some(window) = weaver_server_net::bandwidth::service::compute_window(now, (&cap).into())
    else {
        return Ok(None);
    };
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
    Ok(Some(CarriedCap {
        cap,
        window_start: start,
        window_end: end,
        used,
    }))
}

// The cap as the System egress's download quota, in the columns schema 53
// keeps it in.
async fn copy_cap(conn: &mut SqlConn<'_>, carried: &CarriedCap) -> Result<(), StateError> {
    let cap = &carried.cap;
    conn.execute(
        "UPDATE egress_interfaces
            SET download_quota_enabled = TRUE,
                download_quota_limit_bytes = {},
                download_quota_period = {},
                download_quota_reset_time_minutes_local = {},
                download_quota_weekly_reset_weekday = {},
                download_quota_monthly_reset_day = {}
          WHERE id = {}",
        &[
            SqlArg::I64(cap.limit_bytes as i64),
            SqlArg::Text(cap.period.as_str().into()),
            SqlArg::I64(i64::from(cap.reset_time_minutes_local)),
            SqlArg::Text(
                weaver_server_net::servers::record::quota_weekday_str(cap.weekly_reset_weekday)
                    .into(),
            ),
            SqlArg::I64(i64::from(cap.monthly_reset_day)),
            SqlArg::I64(SYSTEM_EGRESS_ID),
        ],
    )
    .await?;
    Ok(())
}

// The bytes the cap had counted, as the System egress's usage in its window.
pub(crate) async fn carry_usage(
    conn: &mut SqlConn<'_>,
    carried: &CarriedCap,
    now: DateTime<Local>,
) -> Result<(), StateError> {
    conn.execute(
        "INSERT INTO egress_download_usage
            (egress_id, lifetime_bytes, quota_baseline_bytes,
             window_start_epoch_seconds, window_end_epoch_seconds, updated_at_epoch_seconds)
         VALUES ({}, {}, 0, {}, {}, {})",
        &[
            SqlArg::I64(SYSTEM_EGRESS_ID),
            SqlArg::I64(carried.used),
            SqlArg::I64(carried.window_start),
            SqlArg::I64(carried.window_end),
            SqlArg::I64(now.timestamp()),
        ],
    )
    .await?;
    Ok(())
}
