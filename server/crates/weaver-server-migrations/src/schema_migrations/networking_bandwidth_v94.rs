// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 69, upgrade step: the global download limit moves from the
// settings into the networking bandwidth row, and saved schedules stop
// spelling "no limit" as zero.
//
// Before 69 a limit of 0 meant no limit. From 69 every limit is optional:
// no value is no limit, and 0 lets nothing through. So a saved global limit
// of 0 becomes no limit, and a schedule rule that set a limit of 0 becomes a
// rule that lifts it.
//
// The settings keys, the schedule JSON and the egress columns are spelled
// out here as they stood before 69, so later changes to the running types
// do not change what this step does.

use serde_json::{Map, Value};

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};

pub const HOOK_ID: &str = "move_bandwidth_to_networking_v94";
// The schema that first keeps bandwidth limits in networking. A backup taken
// below it carries the global limit as a setting and egress rows in the
// older columns.
pub const SCHEMA_VERSION: i64 = 94;

const SPEED_KEY: &str = "max_download_speed";
const SCHEDULES_KEY: &str = "schedules";
const SYSTEM_EGRESS_ID: i64 = 0;

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn move_bandwidth_to_networking(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let download = setting(conn, SPEED_KEY)
        .await?
        .and_then(|value| legacy_limit(&value));
    conn.execute(
        "INSERT INTO network_bandwidth (id, download_bps, upload_bps) VALUES (1, {}, NULL)
         ON CONFLICT(id) DO UPDATE SET download_bps = excluded.download_bps",
        &[SqlArg::OptI64(download)],
    )
    .await?;
    conn.execute(
        "DELETE FROM settings WHERE key = {}",
        &[SqlArg::Text(SPEED_KEY.into())],
    )
    .await?;

    if let Some(raw) = setting(conn, SCHEDULES_KEY).await?
        && let Some(schedules) = schedules_without_zero_limits(&raw)
    {
        conn.execute(
            "UPDATE settings SET value = {} WHERE key = {}",
            &[SqlArg::Text(schedules), SqlArg::Text(SCHEDULES_KEY.into())],
        )
        .await?;
    }
    Ok(())
}

async fn setting(conn: &mut SqlConn<'_>, key: &str) -> Result<Option<String>, StateError> {
    conn.fetch_all(
        "SELECT value FROM settings WHERE key = {}",
        &[SqlArg::Text(key.into())],
    )
    .await?
    .into_iter()
    .next()
    .map(|row| row.text("value"))
    .transpose()
}

// A limit saved before 69, where 0 meant no limit.
fn legacy_limit(value: &str) -> Option<i64> {
    match value.trim().parse::<i64>() {
        Ok(limit) if limit > 0 => Some(limit),
        Ok(_) => None,
        Err(_) => {
            tracing::warn!("the saved global speed limit could not be read and was lifted");
            None
        }
    }
}

// The saved schedules with each speed rule's limit of 0 written as no
// limit, or `None` when nothing changes or they cannot be read.
fn schedules_without_zero_limits(raw: &str) -> Option<String> {
    let mut rows: Vec<Value> = match serde_json::from_str(raw) {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, "saved schedules could not be migrated");
            return None;
        }
    };
    let mut changed = false;
    for row in &mut rows {
        if row.pointer("/action/type").and_then(Value::as_str) != Some("speed_limit") {
            continue;
        }
        let Some(limits) = row
            .pointer_mut("/action/limits")
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        for limit in limits {
            if limit.get("bytes_per_sec").and_then(Value::as_u64) == Some(0) {
                limit["bytes_per_sec"] = Value::Null;
                changed = true;
            }
        }
    }
    changed.then(|| Value::Array(rows).to_string())
}

// An egress row from a backup taken below 69, in the columns 69 keeps it in.
// Applied to a row's column names too, as an object of nulls, so the set of
// columns a bundle carries is mapped by the same rule as its rows.
pub fn upgrade_egress_row(row: &mut Map<String, Value>) {
    if let Some(speed) = row.remove("max_download_speed") {
        let limit = speed.as_u64().filter(|&speed| speed > 0);
        row.insert(
            "download_bps".into(),
            limit.map_or(Value::Null, Value::from),
        );
    }
    let enabled = row.remove("download_quota_enabled");
    let limit = row.remove("download_quota_limit_bytes");
    if enabled.is_some() || limit.is_some() {
        let enabled = enabled.is_some_and(|enabled| {
            enabled
                .as_bool()
                .unwrap_or_else(|| enabled.as_i64().is_some_and(|value| value != 0))
        });
        row.insert("quota_download_bytes".into(), limit.unwrap_or(Value::Null));
        row.insert("quota_download_enabled".into(), Value::Bool(enabled));
    }
    for (older, current) in [
        ("download_quota_period", "quota_period"),
        (
            "download_quota_reset_time_minutes_local",
            "quota_reset_time_minutes_local",
        ),
        (
            "download_quota_weekly_reset_weekday",
            "quota_weekly_reset_weekday",
        ),
        (
            "download_quota_monthly_reset_day",
            "quota_monthly_reset_day",
        ),
    ] {
        if let Some(value) = row.remove(older) {
            row.insert(current.into(), value);
        }
    }
}

// The bandwidth cap of a backup taken below 53, moved onto the System egress
// in the columns 69 keeps its quota in.
pub async fn move_older_cap(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let now = weaver_server_base::e2e_clock::local_now();
    let Some(carried) = super::egress_quotas_v53::take_cap(conn, now).await? else {
        return Ok(());
    };
    let cap = &carried.cap;
    conn.execute(
        "UPDATE egress_interfaces
            SET quota_download_bytes = {},
                quota_download_enabled = TRUE,
                quota_period = {},
                quota_reset_time_minutes_local = {},
                quota_weekly_reset_weekday = {},
                quota_monthly_reset_day = {}
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
    super::egress_quotas_v53::carry_usage(conn, &carried, now).await
}
