//! Schedule evaluator — background task that applies time-based download rules.
//!
//! Every 60 seconds, evaluates all enabled schedule entries against the current
//! local time and day-of-week. When the most recent applicable entry changes,
//! sends the appropriate command to the scheduler.
//!
//! A rule stays in force until the next one fires, across midnight and across
//! days the rules skip, so "pause at 23:00, resume at 06:00" pauses all night.
//!
//! Hardware-profile entries are a second, independent track: a profile rule
//! never ends a pause or speed limit, and neither of those ends it.

use std::sync::Arc;

use chrono::{Datelike, NaiveTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::bandwidth::{ScheduleAction, ScheduleEntry, Weekday};
use crate::runtime::HardwareProfile;

use crate::jobs::handle::SchedulerHandle;
use crate::watch_folder::WatchFolderService;

/// State shared between the evaluator and the API layer for reloading schedules.
pub type SharedSchedules = Arc<RwLock<Vec<ScheduleEntry>>>;

/// Spawn the schedule evaluator background task.
///
/// The evaluator loads schedules from the shared state (populated by the API on
/// startup and on config changes), evaluates them against the current time every
/// 60 seconds, and sends commands to the scheduler when the active action changes.
pub fn spawn_evaluator(handle: SchedulerHandle, schedules: SharedSchedules) {
    spawn_evaluator_with_watch_folder(handle, schedules, None);
}

pub fn spawn_evaluator_with_watch_folder(
    handle: SchedulerHandle,
    schedules: SharedSchedules,
    watch_folder: Option<WatchFolderService>,
) {
    tokio::spawn(async move {
        let mut last_action: Option<ScheduleAction> = None;
        let mut last_profile: Option<HardwareProfile> = None;
        let mut interval = tokio::time::interval(crate::e2e_clock::schedule_poll_interval());

        loop {
            interval.tick().await;

            let schedules = schedules.clone();
            let handle = handle.clone();
            let watch_folder = watch_folder.clone();
            let prev_action = last_action.clone();
            let prev_profile = last_profile;

            let result = tokio::spawn(async move {
                let entries = schedules.read().await;
                let now = crate::e2e_clock::local_now();
                let current_day = Weekday::from_chrono(now.weekday());
                let current_time = now.time();

                let desired_profile = find_active_profile(&entries, current_day, current_time);
                if desired_profile != prev_profile {
                    info!(
                        profile = desired_profile.map(HardwareProfile::as_str),
                        "schedule transition: hardware profile"
                    );
                    if let Err(e) = handle.set_scheduled_hardware_profile(desired_profile).await {
                        warn!(error = %e, "failed to apply the scheduled hardware profile");
                    }
                }

                let active = find_active_entry(&entries, current_day, current_time);
                let desired_action = active.map(|e| e.action.clone());

                if desired_action == prev_action {
                    return (desired_action, desired_profile); // no transition
                }

                match &desired_action {
                    Some(action) => {
                        info!(
                            action = ?action,
                            "schedule transition: applying new action"
                        );
                        if let Err(e) = apply_schedule_action(
                            handle.clone(),
                            watch_folder.clone(),
                            action.clone(),
                        )
                        .await
                        {
                            warn!(error = %e, "failed to apply schedule action");
                        }
                    }
                    None => {
                        if prev_action.is_some() {
                            info!("schedule transition: clearing scheduled action");
                            if let Err(e) = handle.clear_schedule_action().await {
                                warn!(error = %e, "failed to clear schedule action");
                            }
                        }
                    }
                }

                (desired_action, desired_profile)
            })
            .await;

            match result {
                Ok((action, profile)) => {
                    last_action = action;
                    last_profile = profile;
                }
                Err(panic) => {
                    tracing::error!(error = %panic, "CRITICAL: schedule evaluator tick panicked — loop continues");
                }
            }
        }
    });
}

async fn apply_schedule_action(
    handle: SchedulerHandle,
    watch_folder: Option<WatchFolderService>,
    action: ScheduleAction,
) -> Result<(), String> {
    match action {
        ScheduleAction::PauseWatchFolderScanning => {
            let Some(watch_folder) = watch_folder else {
                return Err("watch folder service is not available".to_string());
            };
            watch_folder
                .set_scanning_paused(true)
                .await
                .map_err(|error| error.to_string())
        }
        ScheduleAction::ResumeWatchFolderScanning => {
            let Some(watch_folder) = watch_folder else {
                return Err("watch folder service is not available".to_string());
            };
            watch_folder
                .set_scanning_paused(false)
                .await
                .map_err(|error| error.to_string())
        }
        ScheduleAction::HardwareProfile { profile } => handle
            .set_scheduled_hardware_profile(Some(profile))
            .await
            .map_err(|error| error.to_string()),
        other => handle
            .apply_schedule_action(other)
            .await
            .map_err(|error| error.to_string()),
    }
}

/// The pause, resume, speed-limit or watch-folder rule in force, or `None`
/// when no such rule is enabled. Hardware-profile rules are not candidates;
/// see [`find_active_profile`].
fn find_active_entry(
    entries: &[ScheduleEntry],
    current_day: Weekday,
    current_time: NaiveTime,
) -> Option<&ScheduleEntry> {
    most_recently_fired(entries, current_day, current_time, |entry| {
        !entry.action.is_hardware_profile()
    })
}

/// The hardware profile the schedule has in force, or `None` when no enabled
/// profile rule exists.
fn find_active_profile(
    entries: &[ScheduleEntry],
    current_day: Weekday,
    current_time: NaiveTime,
) -> Option<HardwareProfile> {
    most_recently_fired(entries, current_day, current_time, |entry| {
        entry.action.is_hardware_profile()
    })
    .and_then(|entry| match entry.action {
        ScheduleAction::HardwareProfile { profile } => Some(profile),
        _ => None,
    })
}

/// The enabled rule among `candidate`s that fired most recently.
///
/// A rule stays in force until the next one fires, however long that takes:
/// today's rules up to now are considered first, then each earlier day's in
/// turn, back to the rest of this weekday a week ago. A pause set at 23:00 is
/// therefore still in force at 00:05, and a rule that runs on Fridays only is
/// still in force on Sunday. Among rules at the same minute the last one
/// wins.
fn most_recently_fired(
    entries: &[ScheduleEntry],
    current_day: Weekday,
    current_time: NaiveTime,
    candidate: impl Fn(&ScheduleEntry) -> bool,
) -> Option<&ScheduleEntry> {
    let rules: Vec<(&ScheduleEntry, NaiveTime)> = entries
        .iter()
        .filter(|entry| entry.enabled && candidate(entry))
        .filter_map(|entry| match parse_time(&entry.time) {
            Some(time) => Some((entry, time)),
            None => {
                debug!(time = %entry.time, id = %entry.id, "invalid schedule time, skipping");
                None
            }
        })
        .collect();

    let mut day = current_day;
    for days_back in 0..=7 {
        let fired = rules
            .iter()
            .filter(|(entry, _)| entry.days.is_empty() || entry.days.contains(&day))
            .filter(|(_, time)| match days_back {
                0 => *time <= current_time,
                7 => *time > current_time,
                _ => true,
            })
            .fold(
                None,
                |best: Option<(&ScheduleEntry, NaiveTime)>, (entry, time)| match best {
                    Some((_, best_time)) if best_time > *time => best,
                    _ => Some((*entry, *time)),
                },
            );
        if let Some((entry, _)) = fired {
            return Some(entry);
        }
        day = day.previous();
    }
    None
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
mod tests;
