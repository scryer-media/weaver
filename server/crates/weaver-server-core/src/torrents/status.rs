// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent's state as consumers see it, read off what the session reports
//! and what holds it back, and its seeding goal.

use std::collections::BTreeSet;

use librqbit::{TorrentStats, TorrentStatsState};

use crate::jobs::JobId;
use crate::jobs::QueueHold;
use crate::settings::{SeedingGoalMode, SeedingPolicy};
use crate::torrents::{TorrentCounters, TorrentIntent, TorrentState};

pub const ROUTE_DOWN_SEEDING: &str = "Not seeding: route down";
pub const ROUTE_DOWN_FETCHING: &str = "Not downloading: route down";
pub const SESSION_LIMIT_SEEDING: &str = "Not seeding: session limit reached";
pub const SESSION_LIMIT_FETCHING: &str = "Not downloading: session limit reached";
pub const DISK_FULL: &str = "Waiting: disk full";
pub const GOAL_MET: &str = "Seeding goal met";
pub const OFF: &str = "torrents are off";

/// What the session says of a torrent.
#[derive(Debug, Clone, PartialEq)]
pub enum Engine {
    /// Not in a session.
    Absent,
    Checking,
    Live {
        finished: bool,
        peers: u32,
    },
    Paused {
        finished: bool,
    },
    Error(String),
}

impl Engine {
    pub fn of(stats: &TorrentStats) -> Self {
        match &stats.state {
            TorrentStatsState::Initializing { .. } => Self::Checking,
            TorrentStatsState::Live => Self::Live {
                finished: stats.finished,
                peers: stats
                    .live
                    .as_ref()
                    .map_or(0, |live| live.snapshot.peer_stats.live),
            },
            TorrentStatsState::Paused => Self::Paused {
                finished: stats.finished,
            },
            TorrentStatsState::Error => Self::Error(
                stats
                    .error
                    .clone()
                    .unwrap_or_else(|| "the session stopped the torrent".to_owned()),
            ),
        }
    }
}

/// Why a torrent has no session to run in.
#[derive(Debug, Clone, PartialEq)]
pub enum Held {
    /// Its route is down, for the reason given.
    RouteDown(String),
    /// Every session there can be is in use by other routes.
    SessionLimit,
    /// Its route's session could not be built.
    Session(String),
    /// Torrents are off: no session runs.
    Off,
}

impl Held {
    fn state(&self) -> TorrentState {
        match self {
            Self::Off => TorrentState::Stopped,
            _ => TorrentState::Stalled,
        }
    }

    fn detail(&self, complete: bool) -> String {
        let lead = if complete {
            "Not seeding"
        } else {
            "Not downloading"
        };
        match self {
            Self::RouteDown(reason) if reason.is_empty() => format!("{lead}: route down"),
            Self::RouteDown(reason) => format!("{lead}: route down ({reason})"),
            Self::SessionLimit => format!("{lead}: session limit reached"),
            Self::Session(error) => format!("{lead}: {error}"),
            Self::Off => OFF.to_owned(),
        }
    }
}

/// Everything a torrent's state is read from.
#[derive(Debug, Clone, PartialEq)]
pub struct Observed {
    pub engine: Engine,
    pub has_metadata: bool,
    pub complete: bool,
    /// What the operator asked.
    pub intent: TorrentIntent,
    /// Held back by the download queue.
    pub queued: bool,
    /// Stopped by the global pause.
    pub paused_globally: bool,
    /// Why unpacking refused the torrent's output, while that stands.
    pub failed: Option<String>,
    pub held: Option<Held>,
    pub disk_full: bool,
}

/// The state and the reason behind it, when there is one to give.
pub fn project(seen: &Observed) -> (TorrentState, Option<String>) {
    let detail = |text: &str| Some(text.to_owned());
    match seen.intent {
        TorrentIntent::Paused => return (TorrentState::Paused, None),
        TorrentIntent::Stopped => {
            let why = seen
                .failed
                .clone()
                .or_else(|| seen.complete.then(|| GOAL_MET.to_owned()));
            return (TorrentState::Stopped, why);
        }
        TorrentIntent::Resumed => {}
    }
    if seen.paused_globally {
        return (TorrentState::Paused, None);
    }
    if let Some(held) = &seen.held {
        return (held.state(), Some(held.detail(seen.complete)));
    }
    if seen.disk_full {
        return (TorrentState::Stalled, detail(DISK_FULL));
    }
    if !seen.has_metadata {
        return (TorrentState::FetchingMetadata, None);
    }
    if seen.queued {
        return (TorrentState::Queued, None);
    }
    match &seen.engine {
        Engine::Absent => (TorrentState::Queued, None),
        Engine::Checking => (TorrentState::CheckingFiles, None),
        Engine::Live { .. } | Engine::Paused { .. } if seen.complete => {
            (TorrentState::Seeding, None)
        }
        Engine::Live { peers: 0, .. } => (TorrentState::Stalled, None),
        Engine::Live { .. } => (TorrentState::Downloading, None),
        Engine::Paused { .. } => (TorrentState::Paused, None),
        Engine::Error(error) => (TorrentState::Stopped, Some(error.clone())),
    }
}

/// Uploaded over downloaded; over the torrent's size while less than 1% of
/// it was downloaded here, as for a torrent added complete.
pub fn ratio(counters: &TorrentCounters, size: u64) -> f64 {
    let downloaded = counters.downloaded_bytes;
    let over = if downloaded.saturating_mul(100) < size {
        size
    } else {
        downloaded
    };
    if over == 0 {
        0.0
    } else {
        counters.uploaded_bytes as f64 / over as f64
    }
}

/// The goal still to meet, named, or `None` once it is met. No policy, or
/// one with no goal set, is never met.
pub fn goal_left(
    policy: Option<&SeedingPolicy>,
    counters: &TorrentCounters,
    total_bytes: u64,
    now_ms: i64,
) -> Option<String> {
    let Some(policy) = policy else {
        return Some("no seeding goal".to_owned());
    };
    let mut goals: Vec<(bool, String)> = Vec::new();
    if let Some(goal) = policy.ratio_goal {
        let ratio = ratio(counters, total_bytes);
        goals.push((
            ratio >= f64::from(goal),
            format!("ratio {ratio:.2} of {goal:.2}"),
        ));
    }
    if let Some(minutes) = policy.seed_time_goal_minutes {
        let seeded = counters.finished_duration_s / 60;
        goals.push((
            seeded >= u64::from(minutes),
            format!("seeded {seeded} of {minutes} minutes"),
        ));
    }
    if let Some(minutes) = policy.inactive_goal_minutes {
        let idle = counters
            .last_activity_ms
            .map(|at| (now_ms - at).max(0) as u64 / 60_000);
        goals.push((
            idle.is_some_and(|idle| idle >= u64::from(minutes)),
            format!("inactive {} of {minutes} minutes", idle.unwrap_or(0)),
        ));
    }
    if goals.is_empty() {
        return Some("no seeding goal".to_owned());
    }
    let met = match policy.mode {
        SeedingGoalMode::Any => goals.iter().any(|(met, _)| *met),
        SeedingGoalMode::All => goals.iter().all(|(met, _)| *met),
    };
    (!met).then(|| {
        goals
            .into_iter()
            .filter(|(met, _)| !met)
            .map(|(_, goal)| goal)
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// Seconds left at the download speed, while there is a speed.
pub fn eta_seconds(left_bytes: u64, bytes_per_second: u64) -> Option<u64> {
    (bytes_per_second > 0 && left_bytes > 0).then(|| left_bytes.div_ceil(bytes_per_second))
}

/// A torrent as the download queue sees it, in the order it was added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Queued {
    pub job_id: JobId,
    pub complete: bool,
    pub forced: bool,
    pub paused: bool,
}

/// The torrents the queue lets run: of those the global pause does not
/// stop, every forced or complete one, and the first `max_active` of the
/// rest that are not paused.
pub fn active(torrents: &[Queued], max_active: u32, hold: QueueHold) -> BTreeSet<JobId> {
    let mut slots = max_active as usize;
    let mut active = BTreeSet::new();
    for torrent in torrents {
        if hold.stops(torrent.complete) {
            continue;
        }
        if torrent.forced || torrent.complete {
            active.insert(torrent.job_id);
        } else if !torrent.paused && slots > 0 {
            slots -= 1;
            active.insert(torrent.job_id);
        }
    }
    active
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::SeedingAction;

    fn seen(engine: Engine) -> Observed {
        Observed {
            engine,
            has_metadata: true,
            complete: false,
            intent: TorrentIntent::Resumed,
            queued: false,
            paused_globally: false,
            failed: None,
            held: None,
            disk_full: false,
        }
    }

    #[test]
    fn the_state_follows_the_session_and_what_holds_it() {
        let live = Engine::Live {
            finished: false,
            peers: 3,
        };
        assert_eq!(
            project(&seen(live.clone())),
            (TorrentState::Downloading, None)
        );
        assert_eq!(
            project(&seen(Engine::Live {
                finished: false,
                peers: 0
            })),
            (TorrentState::Stalled, None)
        );
        assert_eq!(
            project(&seen(Engine::Checking)),
            (TorrentState::CheckingFiles, None)
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                ..seen(live.clone())
            }),
            (TorrentState::Seeding, None)
        );
        assert_eq!(
            project(&Observed {
                has_metadata: false,
                ..seen(Engine::Absent)
            }),
            (TorrentState::FetchingMetadata, None)
        );
        assert_eq!(
            project(&Observed {
                queued: true,
                ..seen(Engine::Paused { finished: false })
            }),
            (TorrentState::Queued, None)
        );
        // The global pause outranks what else holds a torrent, and a complete
        // torrent it stops is paused, not seeding.
        assert_eq!(
            project(&Observed {
                paused_globally: true,
                complete: true,
                held: Some(Held::RouteDown(String::new())),
                ..seen(Engine::Paused { finished: true })
            }),
            (TorrentState::Paused, None)
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                held: Some(Held::RouteDown(String::new())),
                ..seen(live.clone())
            }),
            (TorrentState::Paused, None)
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                held: Some(Held::RouteDown(String::new())),
                ..seen(Engine::Absent)
            }),
            (TorrentState::Stalled, Some(ROUTE_DOWN_SEEDING.into()))
        );
        assert_eq!(
            project(&Observed {
                held: Some(Held::RouteDown("tunnel gone".into())),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some(format!("{ROUTE_DOWN_FETCHING} (tunnel gone)"))
            )
        );
        assert_eq!(
            project(&Observed {
                disk_full: true,
                ..seen(live.clone())
            }),
            (TorrentState::Stalled, Some(DISK_FULL.into()))
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Stopped,
                complete: true,
                failed: Some("x.exe has an unwanted extension".into()),
                ..seen(live.clone())
            }),
            (
                TorrentState::Stopped,
                Some("x.exe has an unwanted extension".into())
            )
        );
        // A refusal the operator resumed past seeds again.
        assert_eq!(
            project(&Observed {
                complete: true,
                failed: Some("x.exe has an unwanted extension".into()),
                ..seen(live.clone())
            }),
            (TorrentState::Seeding, None)
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                intent: TorrentIntent::Stopped,
                ..seen(live.clone())
            }),
            (TorrentState::Stopped, Some(GOAL_MET.into()))
        );
        // An engine error on a complete torrent is the error, never the goal.
        assert_eq!(
            project(&Observed {
                complete: true,
                ..seen(Engine::Error("disk gone".into()))
            }),
            (TorrentState::Stopped, Some("disk gone".into()))
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Stopped,
                ..seen(live)
            }),
            (TorrentState::Stopped, None)
        );
    }

    #[test]
    fn a_torrent_with_no_session_says_why() {
        assert_eq!(
            project(&Observed {
                held: Some(Held::SessionLimit),
                ..seen(Engine::Absent)
            }),
            (TorrentState::Stalled, Some(SESSION_LIMIT_FETCHING.into()))
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                held: Some(Held::SessionLimit),
                ..seen(Engine::Absent)
            }),
            (TorrentState::Stalled, Some(SESSION_LIMIT_SEEDING.into()))
        );
        assert_eq!(
            project(&Observed {
                held: Some(Held::Session("no listening port".into())),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some("Not downloading: no listening port".into())
            )
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                held: Some(Held::Off),
                ..seen(Engine::Absent)
            }),
            (TorrentState::Stopped, Some(OFF.into()))
        );
    }

    fn policy(ratio: Option<f32>, seed: Option<u32>, mode: SeedingGoalMode) -> SeedingPolicy {
        SeedingPolicy {
            ratio_goal: ratio,
            seed_time_goal_minutes: seed,
            inactive_goal_minutes: None,
            mode,
            action: SeedingAction::Stop,
            max_upload_bps: None,
        }
    }

    #[test]
    fn no_goal_is_never_met_and_a_set_goal_is_met_by_its_mode() {
        let counters = TorrentCounters {
            uploaded_bytes: 150,
            finished_duration_s: 30 * 60,
            ..TorrentCounters::default()
        };
        assert!(goal_left(None, &counters, 100, 0).is_some());
        let none = policy(None, None, SeedingGoalMode::Any);
        assert!(goal_left(Some(&none), &counters, 100, 0).is_some());
        let any = policy(Some(1.0), Some(60), SeedingGoalMode::Any);
        assert_eq!(goal_left(Some(&any), &counters, 100, 0), None);
        let all = policy(Some(1.0), Some(60), SeedingGoalMode::All);
        assert_eq!(
            goal_left(Some(&all), &counters, 100, 0).as_deref(),
            Some("seeded 30 of 60 minutes")
        );
        let inactive = SeedingPolicy {
            inactive_goal_minutes: Some(10),
            ..none
        };
        let idle = TorrentCounters {
            last_activity_ms: Some(0),
            ..counters
        };
        assert!(goal_left(Some(&inactive), &idle, 100, 9 * 60_000).is_some());
        assert_eq!(goal_left(Some(&inactive), &idle, 100, 10 * 60_000), None);
    }

    #[test]
    fn the_queue_runs_forced_and_complete_torrents_beside_its_limit() {
        let torrent = |id, complete, forced, paused| Queued {
            job_id: JobId(id),
            complete,
            forced,
            paused,
        };
        let torrents = [
            torrent(1, true, false, false),
            torrent(2, false, false, true),
            torrent(3, false, false, false),
            torrent(4, false, true, false),
            torrent(5, false, false, false),
            torrent(6, false, false, false),
        ];
        let ids = |hold| {
            active(&torrents, 2, hold)
                .into_iter()
                .map(|id| id.0)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(QueueHold::None), [1, 3, 4, 5]);
        // A pause stops every download, forced ones too, and leaves the
        // complete ones seeding; pausing everything stops those too.
        assert_eq!(ids(QueueHold::Downloads), [1]);
        assert!(ids(QueueHold::Everything).is_empty());
        assert_eq!(eta_seconds(1000, 300), Some(4));
        assert_eq!(eta_seconds(1000, 0), None);
    }

    #[test]
    fn the_ratio_is_over_what_was_downloaded_or_over_the_size_until_one_percent() {
        let counters = |uploaded_bytes, downloaded_bytes| TorrentCounters {
            uploaded_bytes,
            downloaded_bytes,
            ..TorrentCounters::default()
        };
        assert_eq!(ratio(&counters(500, 250), 1000), 2.0);
        // Added complete: nothing downloaded, so the size stands in.
        assert_eq!(ratio(&counters(500, 0), 1000), 0.5);
        assert_eq!(ratio(&counters(500, 9), 1000), 0.5);
        assert_eq!(ratio(&counters(500, 10), 1000), 50.0);
        assert_eq!(ratio(&counters(0, 0), 0), 0.0);
        let policy = policy(Some(2.0), None, SeedingGoalMode::Any);
        assert_eq!(goal_left(Some(&policy), &counters(500, 250), 1000, 0), None);
    }
}
