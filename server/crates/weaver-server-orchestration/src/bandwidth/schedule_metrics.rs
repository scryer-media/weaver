// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Process-local counters for the schedule evaluator.
//
// Action kinds, tracks, outcomes and hold reasons are closed sets held in
// fixed arrays. The per-rule figures sit in a registry keyed by rule id: a
// rule's first transition adds its slots under a write lock, later ones find
// them under a read lock. Only rules still configured are rendered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

use chrono::NaiveDateTime;

use super::model::{ScheduleAction, ScheduleTrack};
use super::schedule::SharedSchedules;
use crate::jobs::{ByProtocol, HeldClass, Protocol};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActionKind {
    Pause,
    Resume,
    PauseAll,
    PauseClass,
    ResumeClass,
    PausePostProcessing,
    ResumePostProcessing,
    PauseWatchFolderScanning,
    ResumeWatchFolderScanning,
    PauseRss,
    ResumeRss,
    SpeedLimit,
    BandwidthSplit,
    HardwareProfile,
    SetServerActive,
    SetQuotaMetering,
    PruneHistory,
}

impl ActionKind {
    pub const ALL: [Self; 17] = [
        Self::Pause,
        Self::Resume,
        Self::PauseAll,
        Self::PauseClass,
        Self::ResumeClass,
        Self::PausePostProcessing,
        Self::ResumePostProcessing,
        Self::PauseWatchFolderScanning,
        Self::ResumeWatchFolderScanning,
        Self::PauseRss,
        Self::ResumeRss,
        Self::SpeedLimit,
        Self::BandwidthSplit,
        Self::HardwareProfile,
        Self::SetServerActive,
        Self::SetQuotaMetering,
        Self::PruneHistory,
    ];

    pub fn of(action: &ScheduleAction) -> Self {
        match action {
            ScheduleAction::Pause => Self::Pause,
            ScheduleAction::Resume => Self::Resume,
            ScheduleAction::PauseAll => Self::PauseAll,
            ScheduleAction::PauseClass { .. } => Self::PauseClass,
            ScheduleAction::ResumeClass { .. } => Self::ResumeClass,
            ScheduleAction::PausePostProcessing => Self::PausePostProcessing,
            ScheduleAction::ResumePostProcessing => Self::ResumePostProcessing,
            ScheduleAction::PauseWatchFolderScanning => Self::PauseWatchFolderScanning,
            ScheduleAction::ResumeWatchFolderScanning => Self::ResumeWatchFolderScanning,
            ScheduleAction::PauseRss => Self::PauseRss,
            ScheduleAction::ResumeRss => Self::ResumeRss,
            ScheduleAction::SpeedLimit { .. } => Self::SpeedLimit,
            ScheduleAction::BandwidthSplit { .. } => Self::BandwidthSplit,
            ScheduleAction::HardwareProfile { .. } => Self::HardwareProfile,
            ScheduleAction::SetServerActive { .. } => Self::SetServerActive,
            ScheduleAction::SetQuotaMetering { .. } => Self::SetQuotaMetering,
            ScheduleAction::PruneHistory { .. } => Self::PruneHistory,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::PauseAll => "pause_all",
            Self::PauseClass => "pause_class",
            Self::ResumeClass => "resume_class",
            Self::PausePostProcessing => "pause_post_processing",
            Self::ResumePostProcessing => "resume_post_processing",
            Self::PauseWatchFolderScanning => "pause_watch_folder_scanning",
            Self::ResumeWatchFolderScanning => "resume_watch_folder_scanning",
            Self::PauseRss => "pause_rss",
            Self::ResumeRss => "resume_rss",
            Self::SpeedLimit => "speed_limit",
            Self::BandwidthSplit => "bandwidth_split",
            Self::HardwareProfile => "hardware_profile",
            Self::SetServerActive => "set_server_active",
            Self::SetQuotaMetering => "set_quota_metering",
            Self::PruneHistory => "prune_history",
        }
    }

    pub fn is_one_shot(self) -> bool {
        matches!(self, Self::PruneHistory)
    }

    /// Whether an action of this kind is over one class of job, counted
    /// for each class apart.
    pub const fn over_class(self) -> bool {
        self.class_cell().is_some()
    }

    // Where a class action's counts sit among each class's cells; `None`
    // for an action over no one class.
    const fn class_cell(self) -> Option<usize> {
        match self {
            Self::PauseClass => Some(0),
            Self::ResumeClass => Some(1),
            Self::Pause
            | Self::Resume
            | Self::PauseAll
            | Self::PausePostProcessing
            | Self::ResumePostProcessing
            | Self::PauseWatchFolderScanning
            | Self::ResumeWatchFolderScanning
            | Self::PauseRss
            | Self::ResumeRss
            | Self::SpeedLimit
            | Self::BandwidthSplit
            | Self::HardwareProfile
            | Self::SetServerActive
            | Self::SetQuotaMetering
            | Self::PruneHistory => None,
        }
    }

    // The tracks an action of this kind can be applied on.
    pub fn tracks(self) -> &'static [TrackKind] {
        match self {
            Self::Pause => &[TrackKind::Downloads],
            Self::PauseClass | Self::ResumeClass => &[TrackKind::Class],
            Self::Resume | Self::PauseAll => {
                &[TrackKind::Downloads, TrackKind::WatchFolder, TrackKind::Rss]
            }
            Self::PausePostProcessing | Self::ResumePostProcessing => &[TrackKind::PostProcessing],
            Self::PauseWatchFolderScanning | Self::ResumeWatchFolderScanning => {
                &[TrackKind::WatchFolder]
            }
            Self::PauseRss | Self::ResumeRss => &[TrackKind::Rss],
            Self::SpeedLimit => &[TrackKind::Speed],
            Self::BandwidthSplit => &[TrackKind::Split],
            Self::HardwareProfile => &[TrackKind::Profile],
            Self::SetServerActive => &[TrackKind::Server],
            Self::SetQuotaMetering => &[TrackKind::Quota],
            Self::PruneHistory => &[TrackKind::OneShot],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TrackKind {
    Downloads,
    Class,
    PostProcessing,
    WatchFolder,
    Rss,
    Speed,
    Split,
    Profile,
    Quota,
    Server,
    OneShot,
}

impl TrackKind {
    pub const ALL: [Self; 11] = [
        Self::Downloads,
        Self::Class,
        Self::PostProcessing,
        Self::WatchFolder,
        Self::Rss,
        Self::Speed,
        Self::Split,
        Self::Profile,
        Self::Quota,
        Self::Server,
        Self::OneShot,
    ];

    pub fn of(track: ScheduleTrack) -> Self {
        match track {
            ScheduleTrack::Downloads => Self::Downloads,
            ScheduleTrack::Class(_) => Self::Class,
            ScheduleTrack::PostProcessing => Self::PostProcessing,
            ScheduleTrack::WatchFolder => Self::WatchFolder,
            ScheduleTrack::Rss => Self::Rss,
            ScheduleTrack::Speed(_) => Self::Speed,
            ScheduleTrack::Split => Self::Split,
            ScheduleTrack::Profile => Self::Profile,
            ScheduleTrack::Quota(_) => Self::Quota,
            ScheduleTrack::Server(_) => Self::Server,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Downloads => "downloads",
            Self::Class => "class",
            Self::PostProcessing => "post_processing",
            Self::WatchFolder => "watch_folder",
            Self::Rss => "rss",
            Self::Speed => "speed",
            Self::Split => "split",
            Self::Profile => "profile",
            Self::Quota => "quota",
            Self::Server => "server",
            Self::OneShot => "one_shot",
        }
    }
}

// What became of one action. `Skipped` is an action still pending, left for
// a later tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActionOutcome {
    Applied,
    Failed,
    Skipped,
}

impl ActionOutcome {
    pub const ALL: [Self; 3] = [Self::Applied, Self::Failed, Self::Skipped];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

// Why the evaluator holds new downloads back, or `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldReason {
    None,
    InitialReplay,
    StateUnavailable,
    ActionFailed,
    EvaluationFailed,
}

impl HoldReason {
    pub const ALL: [Self; 5] = [
        Self::None,
        Self::InitialReplay,
        Self::StateUnavailable,
        Self::ActionFailed,
        Self::EvaluationFailed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InitialReplay => "initial_replay",
            Self::StateUnavailable => "state_unavailable",
            Self::ActionFailed => "action_failed",
            Self::EvaluationFailed => "evaluation_failed",
        }
    }
}

// Why an evaluator applied rules again from scratch: its first tick after
// startup, a clock jump, or a one-shot it caught up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayReason {
    Startup,
    ClockJump,
    CatchUp,
}

impl ReplayReason {
    pub const ALL: [Self; 3] = [Self::Startup, Self::ClockJump, Self::CatchUp];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::ClockJump => "clock_jump",
            Self::CatchUp => "catch_up",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evaluator {
    Hold,
    OneShot,
}

impl Evaluator {
    pub const ALL: [Self; 2] = [Self::Hold, Self::OneShot];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hold => "hold",
            Self::OneShot => "one_shot",
        }
    }
}

const ACTIONS: usize = ActionKind::ALL.len();
const TRACKS: usize = TrackKind::ALL.len();
const OUTCOMES: usize = ActionOutcome::ALL.len();

const CLASS_ACTIONS: usize = 2;

static EVALUATIONS: AtomicU64 = AtomicU64::new(0);
// The actions over no one class, by kind, track and outcome.
static ACTIONS_APPLIED: [[[AtomicU64; OUTCOMES]; TRACKS]; ACTIONS] =
    [const { [const { [const { AtomicU64::new(0) }; OUTCOMES] }; TRACKS] }; ACTIONS];
// A class's pauses and resumes, each class's on its own track, by outcome.
static CLASS_ACTIONS_APPLIED: ByProtocol<[[AtomicU64; OUTCOMES]; CLASS_ACTIONS]> = ByProtocol::new(
    [const { [const { [const { AtomicU64::new(0) }; OUTCOMES] }; CLASS_ACTIONS] }; Protocol::COUNT],
);
static ONE_SHOT_FIRES: [AtomicU64; ACTIONS] = [const { AtomicU64::new(0) }; ACTIONS];
static REPLAYS: [AtomicU64; ReplayReason::ALL.len()] =
    [const { AtomicU64::new(0) }; ReplayReason::ALL.len()];
static CLOCK_JUMPS: [AtomicU64; Evaluator::ALL.len()] =
    [const { AtomicU64::new(0) }; Evaluator::ALL.len()];
static HOLD: AtomicU8 = AtomicU8::new(HoldReason::None as u8);

const NEVER: i64 = i64::MIN;

struct RuleSlots {
    last_fire_ms: AtomicI64,
    last_outcome: AtomicU8,
    fires: [AtomicU64; OUTCOMES],
}

static RULES: LazyLock<RwLock<HashMap<Arc<str>, Arc<RuleSlots>>>> = LazyLock::new(Default::default);

fn rule(id: &str) -> Arc<RuleSlots> {
    if let Some(slots) = RULES
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .get(id)
    {
        return slots.clone();
    }
    RULES
        .write()
        .unwrap_or_else(|error| error.into_inner())
        .entry(Arc::from(id))
        .or_insert_with(|| {
            Arc::new(RuleSlots {
                last_fire_ms: AtomicI64::new(NEVER),
                last_outcome: AtomicU8::new(0),
                fires: [const { AtomicU64::new(0) }; OUTCOMES],
            })
        })
        .clone()
}

pub(crate) fn record_evaluation() {
    EVALUATIONS.fetch_add(1, Ordering::Relaxed);
}

// One action of rule `id` on `track`, at `utc`. `replay` says why it was
// applied again, when it was.
pub(crate) fn record_action(
    id: &str,
    action: &ScheduleAction,
    track: Option<ScheduleTrack>,
    outcome: ActionOutcome,
    utc: NaiveDateTime,
    replay: Option<ReplayReason>,
) {
    let kind = ActionKind::of(action);
    let class = match action {
        ScheduleAction::PauseClass { class } | ScheduleAction::ResumeClass { class } => {
            Some(*class)
        }
        _ => None,
    };
    match (class, kind.class_cell()) {
        (Some(class), Some(cell)) => CLASS_ACTIONS_APPLIED[class.protocol()][cell]
            [outcome as usize]
            .fetch_add(1, Ordering::Relaxed),
        _ => {
            let track = track.map_or(TrackKind::OneShot, TrackKind::of);
            ACTIONS_APPLIED[kind as usize][track as usize][outcome as usize]
                .fetch_add(1, Ordering::Relaxed)
        }
    };
    if let (ActionOutcome::Applied, Some(reason)) = (outcome, replay) {
        REPLAYS[reason as usize].fetch_add(1, Ordering::Relaxed);
    }
    let slots = rule(id);
    slots.fires[outcome as usize].fetch_add(1, Ordering::Relaxed);
    slots
        .last_fire_ms
        .store(utc.and_utc().timestamp_millis(), Ordering::Relaxed);
    slots.last_outcome.store(outcome as u8, Ordering::Relaxed);
}

pub(crate) fn record_one_shot_fire(action: &ScheduleAction) {
    ONE_SHOT_FIRES[ActionKind::of(action) as usize].fetch_add(1, Ordering::Relaxed);
}

// A one-shot occurrence missed while weaver was down and fired on startup.
pub fn record_catch_up_fire() {
    REPLAYS[ReplayReason::CatchUp as usize].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_clock_jump(evaluator: Evaluator) {
    CLOCK_JUMPS[evaluator as usize].fetch_add(1, Ordering::Relaxed);
}

// Hold or release admission, and note why.
pub(crate) fn set_admission_hold(
    handle: &crate::jobs::handle::SchedulerHandle,
    reason: HoldReason,
    hold: Option<String>,
) {
    let reason = if hold.is_some() {
        reason
    } else {
        HoldReason::None
    };
    HOLD.store(reason as u8, Ordering::Relaxed);
    handle.set_schedule_admission_hold(hold);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleMetrics {
    pub id: String,
    pub action: ActionKind,
    pub enabled: bool,
    pub fires: Vec<(ActionOutcome, u64)>,
    pub last_fire_epoch_ms: Option<i64>,
    pub last_outcome: Option<ActionOutcome>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduleMetricsSnapshot {
    pub evaluations: u64,
    // Every outcome of every track each action kind can be applied on, and
    // for a class's action, of each class an operator can hold apart.
    pub actions: Vec<(ActionKind, Option<HeldClass>, TrackKind, ActionOutcome, u64)>,
    pub one_shot_fires: Vec<(ActionKind, u64)>,
    pub replays: Vec<(ReplayReason, u64)>,
    pub clock_jumps: Vec<(Evaluator, u64)>,
    pub hold: Option<HoldReason>,
    // Configured rules by action kind: (kind, total, enabled).
    pub rules_by_action: Vec<(ActionKind, u64, u64)>,
    // One per configured rule, in configured order.
    pub rules: Vec<RuleMetrics>,
}

// The counters, without the configured rules.
pub fn counters_snapshot() -> ScheduleMetricsSnapshot {
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let hold = HOLD.load(Ordering::Relaxed);
    ScheduleMetricsSnapshot {
        evaluations: load(&EVALUATIONS),
        actions: ActionKind::ALL
            .into_iter()
            .flat_map(|action| -> Vec<_> {
                match action.class_cell() {
                    Some(cell) => HeldClass::all()
                        .flat_map(|class| {
                            ActionOutcome::ALL.into_iter().map(move |outcome| {
                                (
                                    action,
                                    Some(class),
                                    TrackKind::Class,
                                    outcome,
                                    load(
                                        &CLASS_ACTIONS_APPLIED[class.protocol()][cell]
                                            [outcome as usize],
                                    ),
                                )
                            })
                        })
                        .collect(),
                    None => action
                        .tracks()
                        .iter()
                        .flat_map(|track| {
                            ActionOutcome::ALL.into_iter().map(move |outcome| {
                                (
                                    action,
                                    None,
                                    *track,
                                    outcome,
                                    load(
                                        &ACTIONS_APPLIED[action as usize][*track as usize]
                                            [outcome as usize],
                                    ),
                                )
                            })
                        })
                        .collect(),
                }
            })
            .collect(),
        one_shot_fires: ActionKind::ALL
            .into_iter()
            .filter(|action| action.is_one_shot())
            .map(|action| (action, load(&ONE_SHOT_FIRES[action as usize])))
            .collect(),
        replays: ReplayReason::ALL
            .into_iter()
            .map(|reason| (reason, load(&REPLAYS[reason as usize])))
            .collect(),
        clock_jumps: Evaluator::ALL
            .into_iter()
            .map(|evaluator| (evaluator, load(&CLOCK_JUMPS[evaluator as usize])))
            .collect(),
        hold: HoldReason::ALL
            .into_iter()
            .find(|reason| *reason as u8 == hold),
        rules_by_action: Vec::new(),
        rules: Vec::new(),
    }
}

// The counters plus the rules configured now.
pub async fn snapshot(schedules: &SharedSchedules) -> ScheduleMetricsSnapshot {
    let mut snapshot = counters_snapshot();
    let entries = schedules.read().await;
    snapshot.rules_by_action = ActionKind::ALL
        .into_iter()
        .map(|kind| {
            let of_kind = entries
                .iter()
                .filter(|entry| ActionKind::of(&entry.action) == kind);
            let total = of_kind.clone().count() as u64;
            let enabled = of_kind.filter(|entry| entry.enabled).count() as u64;
            (kind, total, enabled)
        })
        .collect();
    let registry = RULES.read().unwrap_or_else(|error| error.into_inner());
    snapshot.rules = entries
        .iter()
        .map(|entry| {
            let slots = registry.get(entry.id.as_str());
            let fire = slots
                .map(|slots| slots.last_fire_ms.load(Ordering::Relaxed))
                .filter(|at| *at != NEVER);
            RuleMetrics {
                id: entry.id.clone(),
                action: ActionKind::of(&entry.action),
                enabled: entry.enabled,
                fires: ActionOutcome::ALL
                    .into_iter()
                    .map(|outcome| {
                        (
                            outcome,
                            slots.map_or(0, |slots| {
                                slots.fires[outcome as usize].load(Ordering::Relaxed)
                            }),
                        )
                    })
                    .collect(),
                last_fire_epoch_ms: fire,
                last_outcome: fire.and_then(|_| {
                    let outcome = slots?.last_outcome.load(Ordering::Relaxed);
                    ActionOutcome::ALL
                        .into_iter()
                        .find(|candidate| *candidate as u8 == outcome)
                }),
            }
        })
        .collect();
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bandwidth::ScheduleEntry;

    fn entry(id: &str, enabled: bool) -> ScheduleEntry {
        ScheduleEntry {
            id: id.into(),
            enabled,
            label: String::new(),
            days: Vec::new(),
            time: "02:00".into(),
            times: Vec::new(),
            every_hour_at_minute: None,
            action: ScheduleAction::Pause,
        }
    }

    #[tokio::test]
    async fn a_rule_reports_its_last_fire_and_outcome_and_only_configured_rules_show() {
        // Rule ids no other test records under keep the shared registry's
        // counts this test's own.
        let fired = "schedule-metrics-fired";
        let at = chrono::DateTime::from_timestamp(1_700_000_060, 0)
            .unwrap()
            .naive_utc();
        record_action(
            fired,
            &ScheduleAction::Pause,
            None,
            ActionOutcome::Failed,
            at,
            None,
        );
        record_action(
            fired,
            &ScheduleAction::Pause,
            None,
            ActionOutcome::Applied,
            at,
            Some(ReplayReason::Startup),
        );
        record_action(
            "schedule-metrics-removed",
            &ScheduleAction::Pause,
            None,
            ActionOutcome::Applied,
            at,
            None,
        );

        let schedules: SharedSchedules = Arc::new(tokio::sync::RwLock::new(vec![
            entry(fired, true),
            entry("schedule-metrics-never", false),
        ]));
        let snapshot = snapshot(&schedules).await;

        assert_eq!(snapshot.rules.len(), 2);
        let rule = &snapshot.rules[0];
        assert_eq!(rule.id, fired);
        assert_eq!(rule.last_outcome, Some(ActionOutcome::Applied));
        assert_eq!(rule.last_fire_epoch_ms, Some(1_700_000_060_000));
        assert_eq!(
            rule.fires,
            vec![
                (ActionOutcome::Applied, 1),
                (ActionOutcome::Failed, 1),
                (ActionOutcome::Skipped, 0),
            ]
        );
        let never = &snapshot.rules[1];
        assert_eq!(never.last_outcome, None);
        assert_eq!(never.last_fire_epoch_ms, None);
        assert!(!never.enabled);
        let pause = snapshot
            .rules_by_action
            .iter()
            .find(|(kind, _, _)| *kind == ActionKind::Pause)
            .unwrap();
        assert_eq!((pause.1, pause.2), (2, 1));
        assert!(
            snapshot
                .replays
                .iter()
                .any(|(r, n)| *r == ReplayReason::Startup && *n >= 1)
        );
    }

    // A class's pause is counted in that class's own cells, under its key,
    // on the class track; the cells of actions over no one class are not
    // touched by it.
    #[test]
    fn a_class_action_is_counted_per_class() {
        let class = crate::torrents::CLASS;
        let at = chrono::DateTime::from_timestamp(1_700_000_060, 0)
            .unwrap()
            .naive_utc();
        let count = |snapshot: &ScheduleMetricsSnapshot, kind: ActionKind| {
            snapshot
                .actions
                .iter()
                .filter(|(action, of, track, outcome, _)| {
                    *action == kind
                        && *of == Some(class)
                        && *track == TrackKind::Class
                        && *outcome == ActionOutcome::Applied
                })
                .map(|(.., count)| *count)
                .collect::<Vec<_>>()
        };
        let before = counters_snapshot();
        record_action(
            "schedule-metrics-class",
            &ScheduleAction::PauseClass { class },
            Some(ScheduleTrack::Class(class)),
            ActionOutcome::Applied,
            at,
            None,
        );
        let after = counters_snapshot();
        let (was, now) = (
            count(&before, ActionKind::PauseClass),
            count(&after, ActionKind::PauseClass),
        );
        assert_eq!(was.len(), 1, "one cell per held class");
        assert!(now[0] > was[0]);
        assert!(
            after
                .actions
                .iter()
                .filter(|(action, ..)| matches!(
                    action,
                    ActionKind::PauseClass | ActionKind::ResumeClass
                ))
                .all(|(_, of, track, ..)| of.is_some() && *track == TrackKind::Class)
        );
        assert!(
            after
                .actions
                .iter()
                .filter(|(action, ..)| !matches!(
                    action,
                    ActionKind::PauseClass | ActionKind::ResumeClass
                ))
                .all(|(_, of, ..)| of.is_none())
        );
    }
}
