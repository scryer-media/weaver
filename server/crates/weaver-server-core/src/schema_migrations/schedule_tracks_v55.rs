// Move the 0.14.7 shared pause/resume/speed track to independent tracks.
// Called only by the v55 upgrade hook and older logical-backup imports.
use crate::persistence::sql_runtime::{SqlArg, SqlConn};
use crate::{StateError, bandwidth::Weekday};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn move_schedule_tracks(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let Some(row) = conn
        .fetch_all(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text("schedules".into())],
        )
        .await?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
    let rows: Vec<Value> = match serde_json::from_str(&row.text("value")?) {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, "saved schedules could not be migrated");
            return Ok(());
        }
    };
    let speed = conn
        .fetch_all(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text("max_download_speed".into())],
        )
        .await?
        .into_iter()
        .next()
        .and_then(|row| row.text("value").ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let rows = migrate(rows, speed);
    conn.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[
            SqlArg::Text(Value::Array(rows).to_string()),
            SqlArg::Text("schedules".into()),
        ],
    )
    .await?;
    Ok(())
}

fn migrate(rows: Vec<Value>, speed: u64) -> Vec<Value> {
    let resets = legacy_speed_resets(&rows);
    let mut ids = rows
        .iter()
        .filter_map(|row| row.get("id").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut kept = Vec::new();
    for (position, mut row) in rows.into_iter().enumerate() {
        if rule_kind(&row) == "speed_limit"
            && let Some(bytes) = row.pointer("/action/bytes_per_sec").and_then(Value::as_u64)
        {
            row["action"] = global_speed(bytes);
        }
        let reset = resets.get(&position).map(|days| {
            let mut id = format!("{}-speed-reset", row.get("id").and_then(Value::as_str).unwrap_or("rule"));
            while !ids.insert(id.clone()) { id.push_str("-reset"); }
            json!({
                "id": id, "enabled": rule_enabled(&row), "label": "Preserve legacy speed reset",
                "days": if days.len() == 7 { Vec::new() } else { days.iter().map(|day| day.as_str()).collect::<Vec<_>>() },
                "time": row.get("time").cloned().unwrap_or(Value::Null),
                "action": global_speed(speed),
            })
        });
        kept.push(row);
        if let Some(reset) = reset {
            kept.push(reset);
        }
    }
    kept
}

fn rule_kind(row: &Value) -> &str {
    row.pointer("/action/type")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn rule_enabled(row: &Value) -> bool {
    row.get("enabled").and_then(Value::as_bool).unwrap_or(true)
}

// A rule's days. Empty is every day.
fn rule_days(row: &Value) -> Vec<Weekday> {
    row.get("days")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(Weekday::parse)
        .collect()
}

// `HH:MM` as minutes past midnight.
fn minute_of_day(time: &str) -> Option<u32> {
    let (hour, minute) = time.trim().split_once(':')?;
    let (hour, minute) = (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?);
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
}

// Earlier builds held pause, resume and speed rules on one track, so a pause
// or resume ended a scheduled speed limit and put the configured one back.
// Each now holds its own track, so for every day a pause or resume followed
// a speed rule, a speed rule setting the configured limit is added beside
// it. Returns those days by the rule's position.
fn legacy_speed_resets(rows: &[Value]) -> BTreeMap<usize, Vec<Weekday>> {
    const SHARED: [&str; 3] = ["pause", "resume", "speed_limit"];
    let on = |row: &Value, day: Weekday| {
        let days = rule_days(row);
        days.is_empty() || days.contains(&day)
    };
    let time = |row: &Value| {
        row.get("time")
            .and_then(Value::as_str)
            .and_then(minute_of_day)
    };
    let mut week = Vec::new();
    for (day_index, day) in Weekday::ALL.into_iter().enumerate() {
        for (index, row) in rows.iter().enumerate() {
            // A rule turned off can be turned on later, so it gets its
            // companion too, turned off the same way.
            if !on(row, day) || !SHARED.contains(&rule_kind(row)) {
                continue;
            }
            if let Some(time) = time(row) {
                week.push((day_index, time, index));
            }
        }
    }
    // A later watch-folder rule in the same minute took the one track.
    week.retain(|&(day, minute, index)| {
        !rows.iter().skip(index + 1).any(|row| {
            rule_enabled(row)
                && on(row, Weekday::ALL[day])
                && time(row) == Some(minute)
                && matches!(
                    rule_kind(row),
                    "pause_watch_folder_scanning" | "resume_watch_folder_scanning"
                )
        })
    });
    week.sort_unstable();
    let mut resets = BTreeMap::<usize, Vec<Weekday>>::new();
    for (position, &(day, _minute, index)) in week.iter().enumerate() {
        let previous = (1..=week.len())
            .map(|offset| week[(position + week.len() - offset) % week.len()].2)
            .find(|&candidate| {
                rule_enabled(&rows[candidate]) || rule_kind(&rows[candidate]) == "speed_limit"
            })
            .unwrap_or(index);
        if matches!(rule_kind(&rows[index]), "pause" | "resume")
            && rule_kind(&rows[previous]) == "speed_limit"
        {
            resets.entry(index).or_default().push(Weekday::ALL[day]);
        }
    }
    resets
}

// A speed rule for the global limit alone.
fn global_speed(bytes_per_sec: u64) -> Value {
    json!({
        "type": "speed_limit",
        "limits": [{"target": {"kind": "global"}, "bytes_per_sec": bytes_per_sec}],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bandwidth::{ScheduleAction, SpeedLimitChange, SpeedTarget};
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
                bytes_per_sec: 9000,
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
