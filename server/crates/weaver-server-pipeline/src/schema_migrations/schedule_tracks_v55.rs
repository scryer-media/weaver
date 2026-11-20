// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(test)]
mod tests {
    use crate::bandwidth::{ScheduleAction, SpeedLimitChange, SpeedTarget};
    use serde_json::{Value, json};
    use weaver_server_migrations::schema_migrations::schedule_tracks_v55::migrate_saved_rows as migrate;
    use weaver_server_net::bandwidth::Weekday;
    #[test]
    fn a_pause_after_a_speed_rule_keeps_putting_the_configured_limit_back() {
        let rows = migrate(
            vec![
                json!({"id":"slow", "time":"08:00", "action":{"type":"speed_limit", "bytes_per_sec":1000}}),
                json!({"id":"stop", "days":["mon","tue"], "time":"12:00", "action":{"type":"pause"}}),
                json!({"id":"go", "time":"13:00", "action":{"type":"resume"}}),
            ],
            9000,
        );
        let rows: Vec<crate::bandwidth::ScheduleEntry> =
            serde_json::from_value(Value::Array(rows)).unwrap();
        let configured = ScheduleAction::SpeedLimit {
            limits: vec![SpeedLimitChange {
                target: SpeedTarget::Global,
                bytes_per_sec: Some(9000),
                upload_bytes_per_sec: None,
            }],
        };
        assert_eq!(
            rows.iter()
                .map(|row| (row.id.as_str(), row.days.clone(), row.time.as_str()))
                .collect::<Vec<_>>(),
            [
                ("slow", vec![], "08:00"),
                ("stop", vec![Weekday::Mon, Weekday::Tue], "12:00"),
                (
                    "stop-speed-reset",
                    vec![Weekday::Mon, Weekday::Tue],
                    "12:00"
                ),
                ("go", vec![], "13:00"),
                // Only on the days no pause came between.
                (
                    "go-speed-reset",
                    vec![
                        Weekday::Wed,
                        Weekday::Thu,
                        Weekday::Fri,
                        Weekday::Sat,
                        Weekday::Sun
                    ],
                    "13:00"
                ),
            ]
        );
        assert_eq!(rows[2].action, configured);
        assert_eq!(rows[4].action, configured);
        assert_eq!(rows[2].label, "Preserve legacy speed reset");
    }
}
