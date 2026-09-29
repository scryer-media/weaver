//! Schedule evaluator — background task that applies time-based download rules.
//!
//! Every 60 seconds, evaluates all enabled schedule entries against the current
//! local time and day-of-week. When the most recent applicable entry changes,
//! sends the appropriate command to the scheduler.
//!
//! A rule stays in force until the next rule on its track fires, across midnight and across
//! days the rules skip, so "pause at 23:00, resume at 06:00" pauses all night.
//!
//! Downloads, watch-folder scanning, speed and hardware profile hold independently.
//! Deleting the last rule on a track does not undo its last applied state.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDateTime, NaiveTime};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::bandwidth::{ScheduleAction, ScheduleEntry, ScheduleTrack, Weekday};

use crate::jobs::handle::SchedulerHandle;
use crate::watch_folder::WatchFolderService;

/// State shared between the evaluator and the API layer for reloading schedules.
pub type SharedSchedules = Arc<RwLock<Vec<ScheduleEntry>>>;

/// Cooperative cancellation: owned database and intake transactions drain;
/// interruptible network reads and admission waits observe this signal.
#[derive(Clone)]
pub(crate) struct ScheduleCancellation(tokio::sync::watch::Sender<bool>);
impl ScheduleCancellation {
    pub(crate) fn new() -> Self {
        Self(tokio::sync::watch::channel(false).0)
    }
    pub(crate) fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub(crate) async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

#[derive(Clone, Default)]
pub struct ScheduleServices {
    pub watch_folder: Option<WatchFolderService>,
    pub rss: Option<crate::rss::RssService>,
    pub servers: Option<crate::servers::service::ServersService>,
    pub db: Option<crate::Database>,
}

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
    let _initial_replay = spawn_evaluator_with_services(
        handle,
        schedules,
        ScheduleServices {
            watch_folder,
            ..Default::default()
        },
    );
}

/// Owns orderly shutdown of schedule work. Dropping it detaches the evaluator,
/// preserving the behavior of the convenience spawn functions.
pub struct ScheduleEvaluatorTask {
    stop: tokio::sync::oneshot::Sender<()>,
    cancellation: crate::bandwidth::schedule::ScheduleCancellation,
    task: tokio::task::JoinHandle<()>,
}

impl ScheduleEvaluatorTask {
    /// Stop admitting actions and finish running work before its services stop.
    pub async fn shutdown(self) {
        self.cancellation.cancel();
        let _ = self.stop.send(());
        if let Err(error) = self.task.await {
            warn!(%error, "schedule evaluator failed during shutdown");
        }
    }
}

pub fn spawn_evaluator_with_services(
    handle: SchedulerHandle,
    schedules: SharedSchedules,
    services: ScheduleServices,
) -> (
    ScheduleEvaluatorTask,
    tokio::sync::oneshot::Receiver<Result<(), String>>,
) {
    let (ready, replayed) = tokio::sync::oneshot::channel();
    handle.set_schedule_admission_hold(Some("waiting for the first schedule replay".into()));
    let (stop, mut stopping) = tokio::sync::oneshot::channel();
    let cancellation = crate::bandwidth::schedule::ScheduleCancellation::new();
    let stopping_actions = cancellation.clone();
    let task = tokio::spawn(async move {
        let mut stop_open = true;
        let mut ready = Some(ready);
        let mut evaluator = HoldEvaluator::default();
        let mut intake_state_loaded = services.db.is_none();
        let mut intake_before_failure = None;
        let mut one_shots = OneShotEvaluator::default();
        let mut dispatcher = OneShotDispatcher::default();
        let mut interval = tokio::time::interval(crate::e2e_clock::schedule_poll_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                result = &mut stopping, if stop_open => {
                    if result.is_ok() { break; }
                    stop_open = false;
                    continue;
                }
                _ = interval.tick() => {},
                result = dispatcher.running.join_next(), if !dispatcher.running.is_empty() => {
                    if let Some(Err(error)) = result { warn!(%error, "scheduled one-shot task failed"); }
                    continue;
                }
                _ = std::future::ready(()), if !dispatcher.pending.is_empty()
                    && dispatcher.running.len() < MAX_RUNNING_ONE_SHOTS => {
                    dispatcher.dispatch(|entry| {
                        let handle = handle.clone();
                        let services = services.clone();
                        let cancellation = stopping_actions.clone();
                        async move {
                            if let Err(error) = apply_one_shot(handle, services, entry.action, cancellation).await {
                                warn!(%error, id = %entry.id, "scheduled one-shot failed");
                            }
                        }
                    });
                    continue;
                }
            }

            let entries = schedules.read().await.clone();
            if !intake_state_loaded {
                let db = services.db.clone().expect("database checked above");
                let pause_all_configured = entries
                    .iter()
                    .any(|entry| entry.enabled && matches!(entry.action, ScheduleAction::PauseAll));
                match tokio::task::spawn_blocking(move || {
                    if pause_all_configured {
                        db.set_setting("schedule_pause_all_used", "true")?;
                    }
                    db.get_setting("schedule_pause_all_used")
                })
                .await
                {
                    Ok(Ok(value)) => {
                        evaluator.pause_all_seen |= value.as_deref() == Some("true");
                        intake_state_loaded = true;
                    }
                    result => {
                        if intake_before_failure.is_none() {
                            let watch_paused = match &services.watch_folder {
                                Some(watch) => watch.scanning_paused().await,
                                None => false,
                            };
                            let rss_paused = services
                                .rss
                                .as_ref()
                                .is_some_and(|rss| rss.is_scheduled_paused());
                            intake_before_failure = Some((watch_paused, rss_paused));
                        }
                        if let Some(watch) = &services.watch_folder {
                            watch.pause_scanning_runtime().await;
                        }
                        if let Some(rss) = &services.rss {
                            rss.set_scheduled_paused(true);
                        }
                        warn!(
                            ?result,
                            "cannot load schedule intake state; retrying next tick"
                        );
                        handle.set_schedule_admission_hold(
                            holds_admission(&entries)
                                .then(|| format!("cannot load schedule state: {result:?}")),
                        );
                        if let Some(ready) = ready.take() {
                            let _ = ready.send(Err(format!(
                                "cannot load schedule intake state: {result:?}"
                            )));
                        }
                        continue;
                    }
                }
            }
            let clock = crate::e2e_clock::local_now();
            let now = clock.naive_local();
            let utc = clock.naive_utc();
            let due = one_shots.due_at(&entries, now, utc);

            let handle = handle.clone();
            let services = services.clone();
            let mut tick = evaluator.clone();
            let tick_handle = handle.clone();
            let tick_services = services.clone();
            let result = tokio::spawn(async move {
                // Do not hold the settings lock while applying runtime commands.
                let failures = tick
                    .apply_effects_at(&entries, now, utc, |action, track| {
                        apply_schedule_action(
                            tick_handle.clone(),
                            tick_services.clone(),
                            action,
                            Some(track),
                        )
                    })
                    .await;
                (tick, failures)
            })
            .await;

            match result {
                Ok((tick, failures)) => {
                    evaluator = tick;
                    handle.set_schedule_admission_hold(evaluator.admission_hold.clone());
                    if failures.is_empty()
                        && let Some((watch_paused, rss_paused)) = intake_before_failure.take()
                    {
                        if !evaluator.applied.contains_key(&ScheduleTrack::WatchFolder)
                            && let Some(watch) = &services.watch_folder
                            && let Err(error) = watch.restore_scanning_runtime(watch_paused).await
                        {
                            watch.pause_scanning_runtime().await;
                            intake_before_failure = Some((watch_paused, rss_paused));
                            warn!(%error, "cannot restore watch intake after schedule recovery; retrying next tick");
                        }
                        if !evaluator.applied.contains_key(&ScheduleTrack::Rss)
                            && let Some(rss) = &services.rss
                        {
                            rss.set_scheduled_paused(rss_paused);
                        }
                    }
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(if failures.is_empty() {
                            Ok(())
                        } else {
                            Err(failures.join("; "))
                        });
                    }
                }
                Err(panic) => {
                    handle.set_schedule_admission_hold(
                        holds_admission(&schedules.read().await)
                            .then(|| format!("schedule evaluation failed: {panic}")),
                    );
                    evaluator.applied.remove(&ScheduleTrack::WatchFolder);
                    evaluator.applied.remove(&ScheduleTrack::Rss);
                    if intake_before_failure.is_none() {
                        let watch_paused = match &services.watch_folder {
                            Some(watch) => watch.scanning_paused().await,
                            None => false,
                        };
                        let rss_paused = services
                            .rss
                            .as_ref()
                            .is_some_and(|rss| rss.is_scheduled_paused());
                        intake_before_failure = Some((watch_paused, rss_paused));
                    }
                    if let Some(watch_folder) = &services.watch_folder {
                        watch_folder.pause_scanning_runtime().await;
                    }
                    if let Some(rss) = &services.rss {
                        rss.set_scheduled_paused(true);
                    }
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Err(format!("initial schedule replay failed: {panic}")));
                    }
                    tracing::error!(error = %panic, "CRITICAL: schedule evaluator tick panicked — loop continues");
                }
            }
            dispatcher.pending.extend(due);
        }
        while let Some(result) = dispatcher.running.join_next().await {
            if let Err(error) = result {
                warn!(%error, "scheduled one-shot task failed during shutdown");
            }
        }
    });
    (
        ScheduleEvaluatorTask {
            stop,
            task,
            cancellation,
        },
        replayed,
    )
}

/// Whether any enabled rule, left unapplied, would have to hold admission.
fn holds_admission(entries: &[ScheduleEntry]) -> bool {
    entries
        .iter()
        .any(|entry| entry.enabled && entry.action.holds_admission())
}

async fn apply_one_shot(
    handle: SchedulerHandle,
    services: ScheduleServices,
    action: ScheduleAction,
    cancellation: crate::bandwidth::schedule::ScheduleCancellation,
) -> Result<(), ScheduleApplyError> {
    if cancellation.is_cancelled() {
        return Ok(());
    }
    match action {
        ScheduleAction::FetchRss { feed_id } => services
            .rss
            .ok_or("RSS service is not available")?
            .run_scheduled_sync_cancellable(feed_id, cancellation)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string().into()),
        ScheduleAction::ScanWatchFolder => services
            .watch_folder
            .ok_or("watch folder service is not available")?
            .scan_scheduled_cancellable(cancellation)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string().into()),
        // History acceptance is a short durable transaction. Drain its owned
        // blocking task instead of abandoning a writer during service teardown.
        other => apply_schedule_action(handle, services, other, None).await,
    }
}

const MAX_RUNNING_ONE_SHOTS: usize = 32;

/// Bound concurrent work without losing occurrences already accepted by the
/// evaluator. Pending actions do not block hold transitions or the next tick.
#[derive(Default)]
struct OneShotDispatcher {
    pending: VecDeque<ScheduleEntry>,
    running: tokio::task::JoinSet<()>,
}

impl OneShotDispatcher {
    fn dispatch<F, Fut>(&mut self, mut apply: F)
    where
        F: FnMut(ScheduleEntry) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        while self.running.len() < MAX_RUNNING_ONE_SHOTS {
            let Some(entry) = self.pending.pop_front() else {
                break;
            };
            self.running.spawn(apply(entry));
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AppliedRule {
    id: String,
    occurrence: NaiveDateTime,
    action: ScheduleAction,
}

/// At most one successful occurrence per track. Failures remain pending, and
/// editing an action on the current occurrence reapplies that action.
#[derive(Clone, Default)]
struct HoldEvaluator {
    applied: BTreeMap<ScheduleTrack, AppliedRule>,
    last_tick: Option<NaiveDateTime>,
    last_utc: Option<NaiveDateTime>,
    repeated_until: Option<NaiveDateTime>,
    pause_all_seen: bool,
    /// Set when a pause or server disable failed this tick. New downloads
    /// stay held until it applies, so the rule's intent is not overrun.
    admission_hold: Option<String>,
}

impl HoldEvaluator {
    #[cfg(test)]
    async fn apply<F, Fut>(&mut self, entries: &[ScheduleEntry], now: NaiveDateTime, mut apply: F)
    where
        F: FnMut(ScheduleAction) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        self.apply_effects(entries, now, |action, _track| {
            let pending = apply(action);
            async move { pending.await.map_err(ScheduleApplyError::Failed) }
        })
        .await;
    }

    #[cfg(test)]
    async fn apply_effects<F, Fut>(
        &mut self,
        entries: &[ScheduleEntry],
        now: NaiveDateTime,
        apply: F,
    ) where
        F: FnMut(ScheduleAction, ScheduleTrack) -> Fut,
        Fut: std::future::Future<Output = Result<(), ScheduleApplyError>>,
    {
        self.apply_effects_at(entries, now, now, apply).await;
    }

    async fn apply_effects_at<F, Fut>(
        &mut self,
        entries: &[ScheduleEntry],
        now: NaiveDateTime,
        utc: NaiveDateTime,
        mut apply: F,
    ) -> Vec<String>
    where
        F: FnMut(ScheduleAction, ScheduleTrack) -> Fut,
        Fut: std::future::Future<Output = Result<(), ScheduleApplyError>>,
    {
        let jumped = self.last_utc.is_some_and(|last| {
            last - utc > Duration::minutes(5) || utc - last > Duration::minutes(90)
        });
        if jumped {
            self.applied.clear();
            self.repeated_until = None;
        } else if self.last_tick.is_some_and(|last| now < last) {
            self.repeated_until = self.last_tick;
        }
        if self.repeated_until.is_some_and(|until| now > until) {
            self.repeated_until = None;
        }
        self.last_tick = Some(now);
        self.last_utc = Some(utc);
        let mut failures = Vec::new();
        self.admission_hold = None;
        // A legacy Resume only affects downloads. Once PauseAll has introduced
        // intake holds, Resume must clear them even if that rule is later deleted.
        self.pause_all_seen |= entries
            .iter()
            .any(|entry| entry.enabled && matches!(entry.action, ScheduleAction::PauseAll));
        let tracks: BTreeSet<_> = entries
            .iter()
            .filter(|entry| entry.enabled)
            .flat_map(|entry| entry.action.tracks())
            .collect();
        self.applied.retain(|track, _| tracks.contains(track));
        for track in tracks {
            let Some((entry, days_back, time)) = most_recently_fired(
                entries,
                Weekday::from_chrono(now.weekday()),
                now.time(),
                |entry| {
                    entry.action.tracks().contains(&track)
                        && (self.pause_all_seen
                            || !matches!(entry.action, ScheduleAction::Resume)
                            || track == ScheduleTrack::Downloads)
                },
            ) else {
                // Forget the occurrence, but preserve the runtime state. Re-enabling
                // a rule later must apply it again even within the same occurrence.
                self.applied.remove(&track);
                continue;
            };
            let desired = AppliedRule {
                id: entry.id.clone(),
                occurrence: (now.date() - Duration::days(days_back)).and_time(time),
                action: entry.action.clone(),
            };
            if self.applied.get(&track) == Some(&desired) {
                continue;
            }
            if self.repeated_until.is_some()
                && self.applied.get(&track).is_some_and(|applied| {
                    desired.occurrence < applied.occurrence
                        && entries.iter().any(|entry| {
                            entry.enabled
                                && entry.id == applied.id
                                && entry.action == applied.action
                        })
                })
            {
                continue;
            }
            info!(id = %entry.id, ?track, action = ?entry.action, "schedule transition");
            match apply(entry.action.clone(), track).await {
                Ok(()) => {
                    self.applied.insert(track, desired);
                }
                Err(ScheduleApplyError::Pending) => {}
                Err(ScheduleApplyError::Failed(error)) => {
                    if matches!(track, ScheduleTrack::Downloads | ScheduleTrack::Server(_))
                        && entry.action.holds_admission()
                        && self.admission_hold.is_none()
                    {
                        let rule = if entry.label.is_empty() {
                            &entry.id
                        } else {
                            &entry.label
                        };
                        self.admission_hold = Some(format!(
                            "schedule rule \"{rule}\" could not be applied: {error}"
                        ));
                    }
                    warn!(%error, id = %entry.id, "failed to apply schedule action; retrying next tick");
                    failures.push(error);
                }
            }
        }
        failures
    }
}

#[derive(Debug)]
enum ScheduleApplyError {
    Pending,
    Failed(String),
}
impl From<String> for ScheduleApplyError {
    fn from(value: String) -> Self {
        Self::Failed(value)
    }
}
impl From<&str> for ScheduleApplyError {
    fn from(value: &str) -> Self {
        Self::Failed(value.into())
    }
}
impl std::fmt::Display for ScheduleApplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => formatter.write_str("schedule action pending"),
            Self::Failed(error) => formatter.write_str(error),
        }
    }
}

async fn apply_schedule_action(
    handle: SchedulerHandle,
    services: ScheduleServices,
    action: ScheduleAction,
    track: Option<ScheduleTrack>,
) -> Result<(), ScheduleApplyError> {
    if track == Some(ScheduleTrack::Downloads)
        && matches!(action, ScheduleAction::Resume)
        && let Some(db) = services.db.clone()
        && tokio::task::spawn_blocking(move || db.get_setting("nzbget.scheduled_resume_at"))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|at| at > 0)
    {
        return Err(ScheduleApplyError::Pending);
    }
    if track == Some(ScheduleTrack::Rss) {
        services
            .rss
            .ok_or("RSS service is not available")?
            .set_scheduled_paused(matches!(action, ScheduleAction::PauseAll));
        return Ok(());
    }
    if track == Some(ScheduleTrack::WatchFolder) {
        let watch_folder = services
            .watch_folder
            .ok_or("watch folder service is not available")?;
        let pausing = matches!(
            action,
            ScheduleAction::PauseAll | ScheduleAction::PauseWatchFolderScanning
        );
        let result = watch_folder.set_scanning_paused(pausing).await;
        // A failed pause holds scanning stopped until the retry lands. A failed
        // resume leaves scanning as it was, so removing the rule cannot strand
        // an in-memory pause the stored setting does not record.
        if result.is_err() && pausing {
            watch_folder.pause_scanning_runtime().await;
        }
        return result.map_err(|error| ScheduleApplyError::Failed(error.to_string()));
    }
    let result = match action {
        ScheduleAction::SetServerActive { server_id, active } => {
            services
                .servers
                .ok_or("servers service is not available")?
                .set_active(server_id, active)
                .await
        }
        ScheduleAction::ScanWatchFolder => services
            .watch_folder
            .ok_or("watch folder service is not available")?
            .scan_scheduled()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        ScheduleAction::FetchRss { feed_id } => services
            .rss
            .ok_or("RSS service is not available")?
            .run_scheduled_sync(feed_id)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        ScheduleAction::PruneHistory {
            failed,
            completed,
            cancelled,
        } => {
            services
                .db
                .ok_or("database is not available")?
                .prune_history(failed, completed, cancelled)
                .await
        }
        ScheduleAction::HardwareProfile { profile } => handle
            .set_scheduled_hardware_profile(Some(profile))
            .await
            .map_err(|error| error.to_string()),
        other => handle
            .apply_schedule_action(other)
            .await
            .map_err(|error| error.to_string()),
    };
    result.map_err(ScheduleApplyError::Failed)
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
) -> Option<(&ScheduleEntry, i64, NaiveTime)> {
    let rules: Vec<(&ScheduleEntry, NaiveTime)> = entries
        .iter()
        .filter(|entry| entry.enabled && candidate(entry))
        .flat_map(|entry| {
            entry_times(entry)
                .into_iter()
                .map(move |time| (entry, time))
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
        if let Some((entry, time)) = fired {
            return Some((entry, days_back, time));
        }
        day = day.previous();
    }
    None
}

fn entry_times(entry: &ScheduleEntry) -> Vec<NaiveTime> {
    if let Some(minute) = entry.every_hour_at_minute {
        return (0..24)
            .filter_map(|hour| NaiveTime::from_hms_opt(hour, u32::from(minute), 0))
            .collect();
    }
    let mut times: Vec<_> = if entry.times.is_empty() {
        parse_time(&entry.time).into_iter().collect()
    } else {
        entry
            .times
            .iter()
            .filter_map(|time| parse_time(time))
            .collect()
    };
    times.sort_unstable();
    times.dedup();
    times
}

#[derive(Default)]
struct OneShotEvaluator {
    last_tick: Option<NaiveDateTime>,
    last_utc: Option<NaiveDateTime>,
    fired: BTreeSet<(String, NaiveDateTime)>,
}

impl OneShotEvaluator {
    #[cfg(test)]
    fn due(&mut self, entries: &[ScheduleEntry], now: NaiveDateTime) -> Vec<ScheduleEntry> {
        self.due_at(entries, now, now)
    }

    fn due_at(
        &mut self,
        entries: &[ScheduleEntry],
        now: NaiveDateTime,
        utc: NaiveDateTime,
    ) -> Vec<ScheduleEntry> {
        // Retain both sides of a repeated local hour, but bound memory by the
        // current rules and two calendar days instead of process lifetime.
        self.fired.retain(|(id, at)| {
            *at >= now - Duration::days(1)
                && *at <= now + Duration::days(1)
                && entries.iter().any(|entry| entry.id == *id)
        });
        let last_utc = self.last_utc.replace(utc);
        let Some(last) = self.last_tick.replace(now) else {
            return Vec::new();
        };
        if now < last
            || last_utc.is_some_and(|last| utc < last || utc - last > Duration::minutes(90))
        {
            return Vec::new();
        }
        let mut due = Vec::new();
        let mut date = last.date();
        while date <= now.date() {
            let day = Weekday::from_chrono(date.weekday());
            for entry in entries.iter().filter(|entry| {
                entry.enabled
                    && entry.action.track().is_none()
                    && !entry.action.is_script()
                    && (entry.days.is_empty() || entry.days.contains(&day))
            }) {
                for time in entry_times(entry) {
                    let at = date.and_time(time);
                    if last < at && at <= now && self.fired.insert((entry.id.clone(), at)) {
                        due.push(entry.clone());
                    }
                }
            }
            let Some(next) = date.succ_opt() else {
                break;
            };
            date = next;
        }
        due
    }
}

pub fn parse_time(s: &str) -> Option<NaiveTime> {
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
