// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::networking_bandwidth_v94::*;

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};
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

        type Row = (
            i64,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            bool,
            i64,
            String,
            i64,
            String,
            i64,
        );
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, download_bps, upload_bps, quota_download_bytes, quota_download_enabled,
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
                (
                    0,
                    None,
                    None,
                    Some(5_000),
                    true,
                    2_048,
                    "monthly".into(),
                    120,
                    "wed".into(),
                    3
                ),
                (
                    1,
                    Some(4_096),
                    None,
                    // A limit turned off is kept, and stays off.
                    Some(9_000),
                    false,
                    2_048,
                    "one_time".into(),
                    0,
                    "mon".into(),
                    1
                ),
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
                "quota_download_enabled": true,
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
        assert_eq!(off.get("quota_download_bytes"), Some(&Value::from(700)));
        assert_eq!(off.get("quota_download_enabled"), Some(&Value::Bool(false)));
    }
}
