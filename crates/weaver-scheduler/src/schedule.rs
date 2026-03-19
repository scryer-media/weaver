//! Schedule evaluator — background task that applies time-based download rules.
//!
//! Every 60 seconds, evaluates all enabled schedule entries against the current
//! local time and day-of-week. When the most recent applicable entry changes,
//! sends the appropriate command to the scheduler.

use std::sync::Arc;

use chrono::{Datelike, Local, NaiveTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use weaver_core::config::{ScheduleAction, ScheduleEntry, Weekday};

use crate::SchedulerHandle;

/// State shared between the evaluator and the API layer for reloading schedules.
pub type SharedSchedules = Arc<RwLock<Vec<ScheduleEntry>>>;

/// Spawn the schedule evaluator background task.
///
/// The evaluator loads schedules from the shared state (populated by the API on
/// startup and on config changes), evaluates them against the current time every
/// 60 seconds, and sends commands to the scheduler when the active action changes.
pub fn spawn_evaluator(handle: SchedulerHandle, schedules: SharedSchedules) {
    tokio::spawn(async move {
        let mut last_action: Option<ScheduleAction> = None;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            interval.tick().await;

            let entries = schedules.read().await;
            let now = Local::now();
            let current_day = Weekday::from_chrono(now.weekday());
            let current_time = now.time();

            let active = find_active_entry(&entries, current_day, current_time);

            let desired_action = active.map(|e| e.action.clone());

            if desired_action == last_action {
                continue; // no transition
            }

            match &desired_action {
                Some(action) => {
                    info!(
                        action = ?action,
                        "schedule transition: applying new action"
                    );
                    if let Err(e) = handle.apply_schedule_action(action.clone()).await {
                        warn!(error = %e, "failed to apply schedule action");
                    }
                }
                None => {
                    if last_action.is_some() {
                        info!("schedule transition: clearing scheduled action");
                        if let Err(e) = handle.clear_schedule_action().await {
                            warn!(error = %e, "failed to clear schedule action");
                        }
                    }
                }
            }

            last_action = desired_action;
        }
    });
}

/// Find the most recently applicable schedule entry for the given time.
///
/// Returns the entry whose `time` is closest to (but not after) `current_time`
/// on the current day. If multiple entries have the same time, the last one wins.
fn find_active_entry<'a>(
    entries: &'a [ScheduleEntry],
    current_day: Weekday,
    current_time: NaiveTime,
) -> Option<&'a ScheduleEntry> {
    let mut best: Option<&ScheduleEntry> = None;
    let mut best_time: Option<NaiveTime> = None;

    for entry in entries {
        if !entry.enabled {
            continue;
        }

        // Day filter: empty means every day
        if !entry.days.is_empty() && !entry.days.contains(&current_day) {
            continue;
        }

        let entry_time = match parse_time(&entry.time) {
            Some(t) => t,
            None => {
                debug!(time = %entry.time, id = %entry.id, "invalid schedule time, skipping");
                continue;
            }
        };

        // Only consider entries at or before the current time
        if entry_time > current_time {
            continue;
        }

        // Pick the most recent (latest time) entry
        if best_time.is_none() || entry_time >= best_time.unwrap() {
            best = Some(entry);
            best_time = Some(entry_time);
        }
    }

    best
}

fn parse_time(s: &str) -> Option<NaiveTime> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let hour: u32 = parts[0].parse().ok()?;
    let minute: u32 = parts[1].parse().ok()?;
    NaiveTime::from_hms_opt(hour, minute, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, time: &str, days: Vec<Weekday>, action: ScheduleAction) -> ScheduleEntry {
        ScheduleEntry {
            id: id.into(),
            enabled: true,
            label: String::new(),
            days,
            time: time.into(),
            action,
        }
    }

    #[test]
    fn picks_most_recent_entry() {
        let entries = vec![
            entry("1", "08:00", vec![], ScheduleAction::Pause),
            entry("2", "18:00", vec![], ScheduleAction::Resume),
        ];
        let now = NaiveTime::from_hms_opt(20, 0, 0).unwrap();
        let active = find_active_entry(&entries, Weekday::Mon, now).unwrap();
        assert_eq!(active.id, "2");
    }

    #[test]
    fn picks_pause_before_resume_fires() {
        let entries = vec![
            entry("1", "08:00", vec![], ScheduleAction::Pause),
            entry("2", "18:00", vec![], ScheduleAction::Resume),
        ];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let active = find_active_entry(&entries, Weekday::Mon, now).unwrap();
        assert_eq!(active.id, "1");
    }

    #[test]
    fn respects_day_filter() {
        let entries = vec![
            entry("1", "08:00", vec![Weekday::Mon, Weekday::Fri], ScheduleAction::Pause),
        ];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert!(find_active_entry(&entries, Weekday::Mon, now).is_some());
        assert!(find_active_entry(&entries, Weekday::Wed, now).is_none());
    }

    #[test]
    fn empty_days_means_every_day() {
        let entries = vec![
            entry("1", "08:00", vec![], ScheduleAction::Pause),
        ];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert!(find_active_entry(&entries, Weekday::Sat, now).is_some());
    }

    #[test]
    fn disabled_entry_skipped() {
        let mut e = entry("1", "08:00", vec![], ScheduleAction::Pause);
        e.enabled = false;
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert!(find_active_entry(&[e], Weekday::Mon, now).is_none());
    }

    #[test]
    fn no_entries_before_current_time() {
        let entries = vec![
            entry("1", "18:00", vec![], ScheduleAction::Pause),
        ];
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        assert!(find_active_entry(&entries, Weekday::Mon, now).is_none());
    }

    #[test]
    fn speed_limit_entry() {
        let entries = vec![
            entry("1", "09:00", vec![], ScheduleAction::SpeedLimit { bytes_per_sec: 1_000_000 }),
            entry("2", "17:00", vec![], ScheduleAction::Resume),
        ];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let active = find_active_entry(&entries, Weekday::Mon, now).unwrap();
        assert_eq!(active.action, ScheduleAction::SpeedLimit { bytes_per_sec: 1_000_000 });
    }
}
