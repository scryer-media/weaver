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

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "move_bandwidth_to_networking_v69";
// The schema that first keeps bandwidth limits in networking. A backup taken
// below it carries the global limit as a setting and egress rows in the
// older columns.
pub(crate) const SCHEMA_VERSION: i64 = 69;

const SPEED_KEY: &str = "max_download_speed";
const SCHEDULES_KEY: &str = "schedules";
const SYSTEM_EGRESS_ID: i64 = 0;

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn move_bandwidth_to_networking(
    conn: &mut SqlConn<'_>,
) -> Result<(), StateError> {
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
            &[
                SqlArg::Text(schedules),
                SqlArg::Text(SCHEDULES_KEY.into()),
            ],
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
    .map_err(Into::into)
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
pub(crate) fn upgrade_egress_row(row: &mut Map<String, Value>) {
    if let Some(speed) = row.remove("max_download_speed") {
        let limit = speed.as_u64().filter(|&speed| speed > 0);
        row.insert("download_bps".into(), limit.map_or(Value::Null, Value::from));
    }
    let enabled = row.remove("download_quota_enabled");
    let limit = row.remove("download_quota_limit_bytes");
    if enabled.is_some() || limit.is_some() {
        let enabled = enabled.is_some_and(|enabled| {
            enabled.as_bool().unwrap_or_else(|| enabled.as_i64().is_some_and(|value| value != 0))
        });
        let bytes = limit.filter(|_| enabled).filter(|limit| !limit.is_null());
        row.insert("quota_download_bytes".into(), bytes.unwrap_or(Value::Null));
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
        ("download_quota_monthly_reset_day", "quota_monthly_reset_day"),
    ] {
        if let Some(value) = row.remove(older) {
            row.insert(current.into(), value);
        }
    }
}

// The bandwidth cap of a backup taken below 53, moved onto the System egress
// in the columns 69 keeps its quota in.
pub(crate) async fn move_older_cap(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let now = crate::e2e_clock::local_now();
    let Some(carried) = super::egress_quotas_v53::take_cap(conn, now).await? else {
        return Ok(());
    };
    let cap = &carried.cap;
    conn.execute(
        "UPDATE egress_interfaces
            SET quota_download_bytes = {},
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
                crate::servers::record::quota_weekday_str(cap.weekly_reset_weekday).into(),
            ),
            SqlArg::I64(i64::from(cap.monthly_reset_day)),
            SqlArg::I64(SYSTEM_EGRESS_ID),
        ],
    )
    .await?;
    super::egress_quotas_v53::carry_usage(conn, &carried, now).await
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;

    async fn at_schema_68() -> SqlitePool {
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

    async fn upgrade_to_69(pool: &SqlitePool) {
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

    async fn bandwidth(pool: &SqlitePool) -> (Option<i64>, Option<i64>) {
        sqlx::query_as("SELECT download_bps, upload_bps FROM network_bandwidth WHERE id = 1")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn speed_keys(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key = 'max_download_speed'")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_saved_global_limit_becomes_the_bandwidth_row() {
        for (saved, expected) in [
            (Some("0"), None),
            (Some("1048576"), Some(1_048_576)),
            (Some("garbage"), None),
            (None, None),
        ] {
            let pool = at_schema_68().await;
            if let Some(saved) = saved {
                setting(&pool, "max_download_speed", saved).await;
            }
            upgrade_to_69(&pool).await;
            assert_eq!(bandwidth(&pool).await, (expected, None), "saved {saved:?}");
            assert_eq!(speed_keys(&pool).await, 0);
        }
    }

    #[tokio::test]
    async fn egress_rates_and_quotas_move_to_their_new_columns() {
        let pool = at_schema_68().await;
        sqlx::query(
            "UPDATE egress_interfaces
                SET max_download_speed = 0, download_quota_enabled = TRUE,
                    download_quota_limit_bytes = 5000, download_quota_period = 'monthly',
                    download_quota_reset_time_minutes_local = 120,
                    download_quota_weekly_reset_weekday = 'wed',
                    download_quota_monthly_reset_day = 3
              WHERE id = 0",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO egress_interfaces
                (id, name, binding_kind, binding_value, enabled, max_download_speed,
                 download_quota_enabled, download_quota_limit_bytes, created_at, updated_at)
             VALUES (1, 'Second', 'interface', 'en1', TRUE, 4096, FALSE, 9000, 0, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        upgrade_to_69(&pool).await;

        let rows: Vec<(i64, Option<i64>, Option<i64>, Option<i64>, i64, String, i64, String, i64)> =
            sqlx::query_as(
                "SELECT id, download_bps, upload_bps, quota_download_bytes,
                        quota_upload_trickle_bps, quota_period, quota_reset_time_minutes_local,
                        quota_weekly_reset_weekday, quota_monthly_reset_day
                   FROM egress_interfaces ORDER BY id",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (0, None, None, Some(5_000), 2_048, "monthly".into(), 120, "wed".into(), 3),
                (1, Some(4_096), None, None, 2_048, "one_time".into(), 0, "mon".into(), 1),
            ]
        );
        let uncapped: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM egress_interfaces
              WHERE quota_upload_soft_bytes IS NULL AND quota_upload_hard_bytes IS NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(uncapped, 2);
    }

    #[tokio::test]
    async fn schedule_rules_lift_a_limit_they_saved_as_zero() {
        let pool = at_schema_68().await;
        setting(
            &pool,
            "schedules",
            r#"[{"id":"a","time":"01:00","action":{"type":"speed_limit","limits":[
                {"target":{"kind":"global"},"bytes_per_sec":0},
                {"target":{"kind":"egress","id":1},"bytes_per_sec":500}]}},
               {"id":"b","time":"02:00","action":{"type":"pause"}}]"#,
        )
        .await;

        upgrade_to_69(&pool).await;

        let saved: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE key = 'schedules'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let saved: Value = serde_json::from_str(&saved).unwrap();
        assert_eq!(
            saved.pointer("/0/action/limits/0/bytes_per_sec"),
            Some(&Value::Null)
        );
        assert_eq!(
            saved.pointer("/0/action/limits/1/bytes_per_sec"),
            Some(&Value::from(500))
        );
        assert_eq!(saved.pointer("/1/action/type"), Some(&Value::from("pause")));
    }

    #[test]
    fn an_older_egress_row_reads_in_the_current_columns() {
        let mut row: Map<String, Value> = serde_json::from_value(serde_json::json!({
            "id": 1, "max_download_speed": 0, "download_quota_enabled": 1,
            "download_quota_limit_bytes": 700, "download_quota_period": "daily",
            "download_quota_reset_time_minutes_local": 5,
            "download_quota_weekly_reset_weekday": "fri",
            "download_quota_monthly_reset_day": 2,
        }))
        .unwrap();
        upgrade_egress_row(&mut row);
        assert_eq!(
            Value::Object(row),
            serde_json::json!({
                "id": 1, "download_bps": null, "quota_download_bytes": 700,
                "quota_period": "daily", "quota_reset_time_minutes_local": 5,
                "quota_weekly_reset_weekday": "fri", "quota_monthly_reset_day": 2,
            })
        );
        let mut off: Map<String, Value> = serde_json::from_value(serde_json::json!({
            "max_download_speed": 2048, "download_quota_enabled": false,
            "download_quota_limit_bytes": 700,
        }))
        .unwrap();
        upgrade_egress_row(&mut off);
        assert_eq!(off.get("download_bps"), Some(&Value::from(2048)));
        assert_eq!(off.get("quota_download_bytes"), Some(&Value::Null));
    }
}
