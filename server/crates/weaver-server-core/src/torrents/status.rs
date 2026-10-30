// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent's state as consumers see it, read off what the session reports
//! and what holds it back, and its seeding goal.

use std::collections::BTreeSet;

use weaver_torrent::{
    AnnounceErrorKind, FetchPhase, TorrentStats, TorrentStatsState, TrackerAnnounce,
};

use crate::jobs::JobId;
use crate::jobs::handle::HoldTarget;
use crate::jobs::{HoldOrigin, HoldOwner, HoldSet};
use crate::proxies::ResolvedBy;
use crate::settings::{Seeding, SeedingGoalMode};
use crate::torrents::sessions::{RouteFailure, TorrentRouteReason};
use crate::torrents::{
    NamedBy, TorrentCounters, TorrentIntent, TorrentState, TorrentTracker, TrackerEntry,
    TrackerStatus,
};

pub const DISK_FULL: &str = "Waiting: disk full";
pub const GOAL_MET: &str = "Seeding goal met";
pub const OFF: &str = "torrents are off";

/// What the session says of a torrent.
#[derive(Debug, Clone, PartialEq)]
pub enum Engine {
    /// Not in a session.
    Absent,
    /// In a session, starting from what was saved; no check reads pieces.
    Starting,
    /// Reading every piece back.
    Checking,
    Live {
        finished: bool,
        peers: u32,
        /// A write found no room: it fetches nothing and keeps seeding.
        upload_mode: bool,
    },
    Paused {
        finished: bool,
    },
    /// An SSL torrent with no identity to present: it runs nowhere.
    NeedsCertificate,
    Error(String),
}

impl Engine {
    /// Every piece it wants is had (libtorrent `torrent::is_finished`).
    pub fn finished(&self) -> bool {
        matches!(
            self,
            Self::Live { finished: true, .. } | Self::Paused { finished: true }
        )
    }

    pub fn of(stats: &TorrentStats) -> Self {
        match &stats.state {
            TorrentStatsState::Initializing { checking: true, .. } => Self::Checking,
            TorrentStatsState::Initializing {
                checking: false, ..
            } => Self::Starting,
            TorrentStatsState::Live => Self::Live {
                finished: stats.finished,
                peers: stats
                    .live
                    .as_ref()
                    .map_or(0, |live| live.snapshot.peer_stats.live),
                upload_mode: stats.live.as_ref().is_some_and(|live| live.upload_mode),
            },
            TorrentStatsState::Paused => Self::Paused {
                finished: stats.finished,
            },
            TorrentStatsState::NeedsCertificate => Self::NeedsCertificate,
            TorrentStatsState::Error => Self::Error(
                stats
                    .error
                    .clone()
                    .unwrap_or_else(|| "the session stopped the torrent".to_owned()),
            ),
        }
    }
}

/// A torrent seeds once it is complete or once it has every piece it wants,
/// whichever files it leaves out (qBittorrent `TorrentImpl::isFinished`).
pub fn seeding(complete: bool, engine: &Engine) -> bool {
    complete || engine.finished()
}

/// Why a torrent has no session to run in.
#[derive(Debug, Clone, PartialEq)]
pub enum Held {
    /// Its route is down, for the reason given.
    RouteDown(RouteFailure),
    /// Every session there can be is in use by other routes.
    SessionLimit,
    /// Its route's session could not be built.
    Session(RouteFailure),
}

impl Held {
    // Torrents being off stops every torrent; any other hold stalls it.
    fn off(&self) -> bool {
        matches!(self, Self::RouteDown(failure) if failure.reason == TorrentRouteReason::TorrentsDisabled)
    }

    fn state(&self) -> TorrentState {
        if self.off() {
            TorrentState::Stopped
        } else {
            TorrentState::Stalled
        }
    }

    fn reason(&self) -> TorrentRouteReason {
        match self {
            Self::RouteDown(failure) | Self::Session(failure) => failure.reason,
            Self::SessionLimit => TorrentRouteReason::SessionLimit,
        }
    }
}

/// Why a torrent is in its state: in words, or, while its route holds it,
/// by the route's reason.
#[derive(Debug, Clone, PartialEq)]
pub enum Why {
    Said(String),
    Route(TorrentRouteReason),
}

impl Why {
    /// The words, and the route's reason, whichever this is.
    pub fn split(why: Option<Self>) -> (Option<String>, Option<TorrentRouteReason>) {
        match why {
            Some(Self::Said(text)) => (Some(text), None),
            Some(Self::Route(reason)) => (None, Some(reason)),
            None => (None, None),
        }
    }
}

impl From<&str> for Why {
    fn from(text: &str) -> Self {
        Self::Said(text.to_owned())
    }
}

impl From<String> for Why {
    fn from(text: String) -> Self {
        Self::Said(text)
    }
}

/// Everything a torrent's state is read from.
#[derive(Debug, Clone, PartialEq)]
pub struct Observed {
    pub engine: Engine,
    pub has_metadata: bool,
    /// Where its metadata fetch asks for peers, while one runs.
    pub fetch: Option<FetchPhase>,
    pub complete: bool,
    /// What the operator asked.
    pub intent: TorrentIntent,
    /// Held back by the download queue.
    pub queued: bool,
    /// Who holds it stopped, while a hold does.
    pub paused_by: Option<HoldOrigin>,
    /// Why unpacking refused the torrent's output, while that stands.
    pub failed: Option<String>,
    pub held: Option<Held>,
    /// Its directory is being moved; it goes on from the new one.
    pub moving: bool,
}

impl Observed {
    fn seeding(&self) -> bool {
        seeding(self.complete, &self.engine)
    }
}

/// The state and the reason behind it, when there is one to give.
pub fn project(seen: &Observed) -> (TorrentState, Option<Why>) {
    let detail = |text: &str| Some(text.into());
    // A move shows first: the torrent's files are on their way, whatever
    // else the engine is doing with them (qBittorrent
    // `TorrentImpl::updateState` decides Moving before Checking).
    if seen.moving {
        return (TorrentState::Moving, None);
    }
    match seen.intent {
        TorrentIntent::Paused => return (TorrentState::Paused, None),
        TorrentIntent::Stopped => {
            let why = seen
                .failed
                .clone()
                .or_else(|| seen.seeding().then(|| GOAL_MET.to_owned()));
            return (TorrentState::Stopped, why.map(Why::Said));
        }
        TorrentIntent::Resumed => {}
    }
    // A check shows while it runs, past whatever holds the torrent in the
    // queue (qBittorrent `TorrentImpl::updateState` decides Checking before
    // Queued, and only for a torrent that is not stopped).
    if matches!(seen.engine, Engine::Checking) {
        return (TorrentState::CheckingFiles, None);
    }
    // A hold on the queue leaves the torrent as it was asked to be, and
    // holds it in the queue, as it does any other job; the hold says whose.
    if let Some(origin) = seen.paused_by {
        return (TorrentState::Queued, detail(origin.reason()));
    }
    if let Some(held) = &seen.held {
        return (held.state(), Some(Why::Route(held.reason())));
    }
    if !seen.has_metadata {
        return (
            TorrentState::FetchingMetadata,
            seen.fetch.map(|phase| phase.to_string().into()),
        );
    }
    if seen.queued {
        return (TorrentState::Queued, None);
    }
    match &seen.engine {
        Engine::Absent | Engine::Starting => (TorrentState::Queued, None),
        Engine::Checking => (TorrentState::CheckingFiles, None),
        Engine::Live { .. } | Engine::Paused { .. } if seen.seeding() => {
            (TorrentState::Seeding, None)
        }
        Engine::Live {
            upload_mode: true, ..
        } => (TorrentState::Stalled, detail(DISK_FULL)),
        Engine::Live { peers: 0, .. } => (TorrentState::Stalled, None),
        Engine::Live { .. } => (TorrentState::Downloading, None),
        Engine::Paused { .. } => (TorrentState::Paused, None),
        Engine::NeedsCertificate => (TorrentState::NeedsCertificate, None),
        Engine::Error(error) => (TorrentState::Stopped, Some(error.as_str().into())),
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

/// The goal still to meet, named, or `None` once it is met. Terms with no
/// goal are never met.
pub fn goal_left(
    policy: &Seeding,
    counters: &TorrentCounters,
    total_bytes: u64,
    now_ms: i64,
) -> Option<String> {
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

/// The seconds a torrent has been active: the whole seconds its counter
/// holds and the part of a second run past them, a second begun counting
/// whole. Its averages run over these same seconds, so its first second
/// reads every byte moved in it as its rate.
pub fn active_seconds(whole_seconds: u64, part_ms: u64) -> u64 {
    whole_seconds.saturating_add(part_ms.div_ceil(1000))
}

/// Bytes over the seconds active; 0 before any time is active.
pub fn average_bps(bytes: u64, active_seconds: u64) -> u64 {
    bytes.checked_div(active_seconds).unwrap_or(0)
}

/// A torrent as the download queue sees it, in the order it was added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Queued {
    pub job_id: JobId,
    /// Complete or finished: it seeds, and fetches nothing.
    pub seeding: bool,
    pub forced: bool,
    pub paused: bool,
    /// Under a tracker rule that never pauses, still downloading or with
    /// its seeding goal to meet.
    pub runs_through_schedule: bool,
}

impl Queued {
    /// The torrent as the holds are asked about it.
    pub fn target(&self) -> HoldTarget {
        HoldTarget {
            owner: HoldOwner::Torrents,
            complete: self.seeding,
            runs_through_schedule: self.runs_through_schedule,
        }
    }
}

/// The torrents the queue lets run: of those no hold stops, every forced
/// or seeding one, and the first `max_active` of the rest that are not
/// paused. Only a torrent still downloading takes one of the `max_active`
/// slots.
pub fn active(torrents: &[Queued], max_active: u32, holds: &HoldSet) -> BTreeSet<JobId> {
    let mut slots = max_active as usize;
    let mut active = BTreeSet::new();
    for torrent in torrents {
        if holds.reason_for(torrent.target()).is_some() {
            continue;
        }
        if torrent.forced || torrent.seeding {
            active.insert(torrent.job_id);
        } else if !torrent.paused && slots > 0 {
            slots -= 1;
            active.insert(torrent.job_id);
        }
    }
    active
}

/// A tracker as the views show it, from how announcing to it last went.
///
/// The announce loop answers every accepted announce by announcing again
/// once the interval has passed, and retries a failed one without end, so
/// while the run announces to the tracker the latest outcome stands until
/// the next one replaces it: an accepted announce reads as working however
/// long ago it was. A tracker the run no longer announces to, nothing due
/// and nothing in flight, keeps its last outcome, with no floor for a next
/// announce that is not coming. It reads as the torrent's own until the
/// torrent's list says who named it.
pub fn tracker_view(tier: u32, announce: &TrackerAnnounce) -> TorrentTracker {
    let ms = |at: std::time::SystemTime| {
        at.duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|since| i64::try_from(since.as_millis()).ok())
    };
    let last = announce.last.as_ref();
    let accepted = last.and_then(|last| last.result.as_ref().ok());
    let announcing = announce.in_flight || announce.next_announce.is_some();
    TorrentTracker {
        tier,
        url: announce.url.clone(),
        named_by: NamedBy::Torrent,
        // The state is read off the last outcome; an announce in flight is
        // told apart by `updating`.
        status: match last.map(|last| last.result.as_ref()) {
            None => TrackerStatus::NotContacted,
            Some(Ok(_)) => TrackerStatus::Working,
            Some(Err(error)) => match error.kind {
                AnnounceErrorKind::Refused => TrackerStatus::TrackerError,
                AnnounceErrorKind::Skipped => TrackerStatus::Unreachable,
                AnnounceErrorKind::Failed => TrackerStatus::NotWorking,
            },
        },
        updating: announce.in_flight,
        last_announce_ms: last.and_then(|last| ms(last.at)),
        next_announce_ms: announce.next_announce.and_then(ms),
        min_announce_ms: last
            .zip(accepted)
            .filter(|_| announcing)
            .and_then(|(last, accepted)| ms(last.at + accepted.min_interval)),
        // libtorrent `announce_entry::message`, the tracker's warning, else
        // the error (qBittorrent shows one, then the other).
        message: last.and_then(|last| match &last.result {
            Ok(accepted) => accepted.warning.clone(),
            Err(error) => Some(error.message.clone()),
        }),
        seeders: accepted.and_then(|accepted| accepted.seeders),
        leechers: accepted.and_then(|accepted| accepted.leechers),
        downloaded: accepted.and_then(|accepted| accepted.downloaded),
        peers: accepted.map(|accepted| u32::try_from(accepted.peers.len()).unwrap_or(u32::MAX)),
        fails: u32::from(announce.fails),
        resolved_by: None,
    }
}

/// The swarm as the trackers last counted it, and when the next announce is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swarm {
    /// The most seeds any tracker counts: trackers count the same swarm, so
    /// their counts overlap rather than add up.
    pub seeds: Option<u32>,
    pub leechers: Option<u32>,
    /// Seconds until the soonest tracker announce.
    pub reannounce_seconds: Option<u64>,
}

/// The swarm read off how announcing to each tracker went, at `now_ms`.
pub fn swarm(announces: &[TrackerAnnounce], now_ms: i64) -> Swarm {
    let trackers: Vec<TorrentTracker> = announces
        .iter()
        .map(|announce| tracker_view(0, announce))
        .collect();
    Swarm {
        seeds: trackers.iter().filter_map(|tracker| tracker.seeders).max(),
        leechers: trackers.iter().filter_map(|tracker| tracker.leechers).max(),
        reannounce_seconds: trackers
            .iter()
            .filter_map(|tracker| tracker.next_announce_ms)
            .min()
            .map(|next| ((next - now_ms).max(0) as u64).div_ceil(1000)),
    }
}

/// A torrent's trackers: the ones its run announces to, each in the tier
/// the torrent lists it in and as whoever named it there, or, before it has
/// run, the ones it lists. A tracker the torrent does not list came with its
/// magnet, whose trackers each take a tier of their own in the link's order.
/// Each says who resolves its host on a route whose names `names` resolves.
pub fn trackers(
    listed: Vec<TrackerEntry>,
    announces: &[TrackerAnnounce],
    names: Option<ResolvedBy>,
) -> Vec<TorrentTracker> {
    let mut trackers: Vec<TorrentTracker> = if announces.is_empty() {
        listed
            .into_iter()
            .map(|entry| TorrentTracker {
                tier: entry.tier,
                url: entry.url,
                named_by: entry.named_by,
                status: TrackerStatus::NotContacted,
                updating: false,
                last_announce_ms: None,
                next_announce_ms: None,
                min_announce_ms: None,
                message: None,
                seeders: None,
                leechers: None,
                downloaded: None,
                peers: None,
                fails: 0,
                resolved_by: None,
            })
            .collect()
    } else {
        // The run names a tracker by its parsed URL.
        let tiers: Vec<(String, u32, NamedBy)> = listed
            .into_iter()
            .filter_map(|entry| {
                let url = reqwest::Url::parse(&entry.url).ok()?.into();
                Some((url, entry.tier, entry.named_by))
            })
            .collect();
        announces
            .iter()
            .map(|announce| {
                let (tier, named_by) = tiers
                    .iter()
                    .find(|(url, ..)| *url == announce.url)
                    .map_or((0, NamedBy::Torrent), |(_, tier, named_by)| {
                        (*tier, *named_by)
                    });
                TorrentTracker {
                    named_by,
                    ..tracker_view(tier, announce)
                }
            })
            .collect()
    };
    trackers.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
    for tracker in &mut trackers {
        tracker.resolved_by = resolved_by(&tracker.url, names);
    }
    trackers
}

/// Who resolves the host `url` names, on a route whose names `names`
/// resolves: no one for an address, which is dialled as it stands.
pub fn resolved_by(url: &str, names: Option<ResolvedBy>) -> Option<ResolvedBy> {
    let host = reqwest::Url::parse(url).ok()?.host_str()?.to_owned();
    let address = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>();
    match address {
        Ok(_) => Some(ResolvedBy::Literal),
        Err(_) => names,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use weaver_torrent::{AnnounceError, AnnounceOk, LastAnnounce};

    use super::*;
    use crate::settings::SeedingAction;

    fn seen(engine: Engine) -> Observed {
        Observed {
            engine,
            has_metadata: true,
            fetch: None,
            complete: false,
            intent: TorrentIntent::Resumed,
            queued: false,
            paused_by: None,
            failed: None,
            held: None,
            moving: false,
        }
    }

    // A magnet fetching its metadata says where it asks for peers.
    #[test]
    fn a_fetch_names_where_it_asks_for_peers() {
        for (phase, detail) in [
            (FetchPhase::Trackers, "trackers"),
            (FetchPhase::Discovery, "trackers, DHT, LSD"),
            (FetchPhase::Waiting, "no peers from trackers"),
        ] {
            assert_eq!(
                project(&Observed {
                    has_metadata: false,
                    fetch: Some(phase),
                    ..seen(Engine::Absent)
                }),
                (TorrentState::FetchingMetadata, Some(detail.into()))
            );
        }
    }

    // An SSL torrent with no certificate to present says so, complete or
    // not, until its rule gives one; asked to stop, it stops.
    #[test]
    fn an_ssl_torrent_without_a_certificate_needs_one() {
        assert_eq!(
            project(&seen(Engine::NeedsCertificate)),
            (TorrentState::NeedsCertificate, None)
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                ..seen(Engine::NeedsCertificate)
            }),
            (TorrentState::NeedsCertificate, None)
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                ..seen(Engine::NeedsCertificate)
            }),
            (TorrentState::Paused, None)
        );
    }

    // A torrent whose directory is on the move says so, whatever its
    // session is doing.
    #[test]
    fn a_moving_torrent_shows_as_moving() {
        let moving = |engine| Observed {
            moving: true,
            ..seen(engine)
        };
        assert_eq!(
            project(&moving(Engine::Live {
                finished: false,
                peers: 3,
                upload_mode: false
            })),
            (TorrentState::Moving, None)
        );
        assert_eq!(
            project(&moving(Engine::Checking)),
            (TorrentState::Moving, None)
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                ..moving(Engine::Paused { finished: false })
            }),
            (TorrentState::Moving, None)
        );
    }

    #[test]
    fn the_state_follows_the_session_and_what_holds_it() {
        let live = Engine::Live {
            finished: false,
            peers: 3,
            upload_mode: false,
        };
        assert_eq!(
            project(&seen(live.clone())),
            (TorrentState::Downloading, None)
        );
        assert_eq!(
            project(&seen(Engine::Live {
                finished: false,
                peers: 0,
                upload_mode: false,
            })),
            (TorrentState::Stalled, None)
        );
        assert_eq!(
            project(&seen(Engine::Starting)),
            (TorrentState::Queued, None)
        );
        assert_eq!(
            project(&seen(Engine::Checking)),
            (TorrentState::CheckingFiles, None)
        );
        // A recheck shows while it runs, past what holds the torrent in the
        // queue; a paused or stopped torrent being checked is still paused
        // or stopped.
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                ..seen(Engine::Checking)
            }),
            (TorrentState::Paused, None)
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Stopped,
                complete: true,
                ..seen(Engine::Checking)
            }),
            (TorrentState::Stopped, Some(GOAL_MET.into()))
        );
        for held in [
            Observed {
                queued: true,
                ..seen(Engine::Checking)
            },
            Observed {
                paused_by: Some(HoldOrigin::Manual),
                ..seen(Engine::Checking)
            },
        ] {
            assert_eq!(project(&held), (TorrentState::CheckingFiles, None));
        }
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
        // A hold outranks what else holds a torrent, a complete torrent it
        // stops waits in the queue, not seeding, and the hold says whose it
        // is.
        assert_eq!(
            project(&Observed {
                paused_by: Some(HoldOrigin::SpeedTest),
                complete: true,
                held: Some(Held::RouteDown(RouteFailure::leg_down(String::new()))),
                ..seen(Engine::Paused { finished: true })
            }),
            (TorrentState::Queued, Some("speed test".into()))
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                held: Some(Held::RouteDown(RouteFailure::leg_down(String::new()))),
                ..seen(live.clone())
            }),
            (TorrentState::Paused, None)
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                held: Some(Held::RouteDown(RouteFailure::leg_down(String::new()))),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some(Why::Route(TorrentRouteReason::LegDown))
            )
        );
        assert_eq!(
            project(&Observed {
                held: Some(Held::RouteDown(RouteFailure::leg_down(
                    "tunnel gone".into()
                ))),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some(Why::Route(TorrentRouteReason::LegDown))
            )
        );
        // A full disk: the session fetches nothing and keeps seeding.
        assert_eq!(
            project(&seen(Engine::Live {
                finished: false,
                peers: 3,
                upload_mode: true,
            })),
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

    // A torrent with every piece it wants seeds, whichever files it leaves
    // out and whether or not its job is complete; wanting a file it lacks
    // takes it back to downloading.
    #[test]
    fn a_finished_torrent_seeds_until_it_wants_more() {
        let live = |finished, peers| Engine::Live {
            finished,
            peers,
            upload_mode: false,
        };
        assert_eq!(project(&seen(live(true, 0))), (TorrentState::Seeding, None));
        assert_eq!(project(&seen(live(true, 3))), (TorrentState::Seeding, None));
        assert_eq!(
            project(&seen(Engine::Paused { finished: true })),
            (TorrentState::Seeding, None)
        );
        assert_eq!(
            project(&seen(live(false, 0))),
            (TorrentState::Stalled, None)
        );
        assert_eq!(
            project(&seen(live(false, 3))),
            (TorrentState::Downloading, None)
        );
        // Stopped once seeding, it met its goal; stopped short of that, it
        // did not.
        let stopped = |engine| Observed {
            intent: TorrentIntent::Stopped,
            ..seen(engine)
        };
        assert_eq!(
            project(&stopped(Engine::Paused { finished: true })),
            (TorrentState::Stopped, Some(GOAL_MET.into()))
        );
        assert_eq!(
            project(&stopped(Engine::Paused { finished: false })),
            (TorrentState::Stopped, None)
        );
        // Asked to pause, it is paused; an engine error is the error.
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                ..seen(live(true, 3))
            }),
            (TorrentState::Paused, None)
        );
        assert_eq!(
            project(&seen(Engine::Error("disk gone".into()))),
            (TorrentState::Stopped, Some("disk gone".into()))
        );
    }

    #[test]
    fn a_torrent_with_no_session_says_why() {
        assert_eq!(
            project(&Observed {
                held: Some(Held::SessionLimit),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some(Why::Route(TorrentRouteReason::SessionLimit))
            )
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                held: Some(Held::SessionLimit),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some(Why::Route(TorrentRouteReason::SessionLimit))
            )
        );
        assert_eq!(
            project(&Observed {
                held: Some(Held::Session(RouteFailure::with_detail(
                    crate::torrents::sessions::TorrentRouteReason::ListenPortUnavailable,
                    "no listening port"
                ))),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stalled,
                Some(Why::Route(TorrentRouteReason::ListenPortUnavailable))
            )
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                held: Some(Held::RouteDown(TorrentRouteReason::TorrentsDisabled.into())),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Stopped,
                Some(Why::Route(TorrentRouteReason::TorrentsDisabled))
            )
        );
    }

    // The seconds active and the average over them come from one
    // quantity: the bytes over the seconds is the average, a second begun
    // counts whole, and the first second's bytes are its rate.
    #[test]
    fn the_average_is_the_bytes_over_the_seconds_active() {
        for (whole, part_ms, bytes, seconds, average) in [
            (0, 0, 0, 0, 0),
            (0, 1, 16_384, 1, 16_384),
            (0, 500, 16_384, 1, 16_384),
            (1, 0, 16_384, 1, 16_384),
            (4, 0, 262_144, 4, 65_536),
            (4, 943, 262_144, 5, 52_428),
            (10, 999, 1_000_000, 11, 90_909),
        ] {
            let active = active_seconds(whole, part_ms);
            let bps = average_bps(bytes, active);
            assert_eq!((active, bps), (seconds, average), "{whole} s {part_ms} ms");
            if active > 0 {
                assert_eq!(bytes / active, bps);
            }
        }
    }

    fn policy(ratio: Option<f32>, seed: Option<u32>, mode: SeedingGoalMode) -> Seeding {
        Seeding {
            ratio_goal: ratio,
            seed_time_goal_minutes: seed,
            inactive_goal_minutes: None,
            mode,
            action: SeedingAction::Stop,
        }
    }

    #[test]
    fn no_goal_is_never_met_and_a_set_goal_is_met_by_its_mode() {
        let counters = TorrentCounters {
            uploaded_bytes: 150,
            finished_duration_s: 30 * 60,
            ..TorrentCounters::default()
        };
        let none = policy(None, None, SeedingGoalMode::Any);
        assert!(goal_left(&none, &counters, 100, 0).is_some());
        let any = policy(Some(1.0), Some(60), SeedingGoalMode::Any);
        assert_eq!(goal_left(&any, &counters, 100, 0), None);
        let all = policy(Some(1.0), Some(60), SeedingGoalMode::All);
        assert_eq!(
            goal_left(&all, &counters, 100, 0).as_deref(),
            Some("seeded 30 of 60 minutes")
        );
        let inactive = Seeding {
            inactive_goal_minutes: Some(10),
            ..none
        };
        let idle = TorrentCounters {
            last_activity_ms: Some(0),
            ..counters
        };
        assert!(goal_left(&inactive, &idle, 100, 9 * 60_000).is_some());
        assert_eq!(goal_left(&inactive, &idle, 100, 10 * 60_000), None);
    }

    #[test]
    fn the_queue_runs_forced_and_complete_torrents_beside_its_limit() {
        let torrent = |id, seeding, forced, paused| Queued {
            job_id: JobId(id),
            seeding,
            forced,
            paused,
            runs_through_schedule: false,
        };
        let torrents = [
            torrent(1, true, false, false),
            torrent(2, false, false, true),
            torrent(3, false, false, false),
            torrent(4, false, true, false),
            torrent(5, false, false, false),
            torrent(6, false, false, false),
        ];
        let ids = |hold: Option<crate::jobs::Hold>| {
            let mut holds = HoldSet::default();
            if let Some(hold) = hold {
                holds.hold(hold);
            }
            active(&torrents, 2, &holds)
                .into_iter()
                .map(|id| id.0)
                .collect::<Vec<_>>()
        };
        let hold = |scope, owner| {
            Some(crate::jobs::Hold {
                origin: HoldOrigin::Manual,
                scope,
                owner,
            })
        };
        use crate::jobs::HoldScope;
        assert_eq!(ids(None), [1, 3, 4, 5]);
        // A pause stops every download, forced ones too, and leaves the
        // complete ones seeding; pausing everything stops those too, and a
        // hold over the torrents alone stops them as one over the queue does.
        assert_eq!(ids(hold(HoldScope::Downloads, HoldOwner::All)), [1]);
        assert!(ids(hold(HoldScope::Everything, HoldOwner::All)).is_empty());
        assert!(ids(hold(HoldScope::Everything, HoldOwner::Torrents)).is_empty());
        assert_eq!(eta_seconds(1000, 300), Some(4));
        assert_eq!(eta_seconds(1000, 0), None);
    }

    // The limit counts the torrents still downloading: however many are
    // complete, forced or paused, the first `max_active` of the rest run,
    // and a limit of zero runs none of them.
    #[test]
    fn only_downloading_torrents_take_a_slot() {
        let torrent = |id, seeding| Queued {
            job_id: JobId(id),
            seeding,
            forced: false,
            paused: false,
            runs_through_schedule: false,
        };
        let torrents = [
            torrent(1, true),
            torrent(2, true),
            torrent(3, false),
            torrent(4, true),
            torrent(5, false),
            torrent(6, false),
        ];
        let ids = |max_active| {
            active(&torrents, max_active, &HoldSet::default())
                .into_iter()
                .map(|id| id.0)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(0), [1, 2, 4]);
        assert_eq!(ids(1), [1, 2, 3, 4]);
        assert_eq!(ids(2), [1, 2, 3, 4, 5]);
        assert_eq!(ids(9), [1, 2, 3, 4, 5, 6]);
    }

    // The schedule's torrent pause leaves a torrent that seeds through it;
    // the operator's pause, a speed test's and a pause of the downloads
    // reach it as they reach any other.
    #[test]
    fn a_torrent_that_seeds_through_the_schedule_runs_through_its_holds_alone() {
        use crate::jobs::{Hold, HoldScope};

        let held = |origin, scope, owner| {
            let mut holds = HoldSet::default();
            holds.hold(Hold {
                origin,
                scope,
                owner,
            });
            holds
        };
        let through = Queued {
            job_id: JobId(1),
            seeding: true,
            forced: false,
            paused: false,
            runs_through_schedule: true,
        };
        let plain = Queued {
            job_id: JobId(2),
            runs_through_schedule: false,
            ..through
        };
        let window = held(
            HoldOrigin::Scheduled,
            HoldScope::Everything,
            HoldOwner::Torrents,
        );
        assert_eq!(
            active(&[through, plain], 3, &window),
            BTreeSet::from([JobId(1)])
        );
        assert!(window.spares(through.target()));
        assert!(!window.spares(plain.target()));
        assert_eq!(
            window.reason_for(plain.target()),
            Some(HoldOrigin::Scheduled)
        );
        let everything = held(HoldOrigin::Scheduled, HoldScope::Everything, HoldOwner::All);
        assert_eq!(everything.reason_for(through.target()), None);

        for holds in [
            held(
                HoldOrigin::Manual,
                HoldScope::Everything,
                HoldOwner::Torrents,
            ),
            held(HoldOrigin::SpeedTest, HoldScope::Everything, HoldOwner::All),
        ] {
            assert!(active(&[through], 3, &holds).is_empty());
            assert!(!holds.spares(through.target()));
        }
        // Nothing to spare when no hold reaches it.
        let downloads = held(HoldOrigin::Scheduled, HoldScope::Downloads, HoldOwner::All);
        assert!(!downloads.spares(through.target()));
        assert!(!HoldSet::default().spares(through.target()));
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
        assert_eq!(goal_left(&policy, &counters(500, 250), 1000, 0), None);
    }

    // An announce that ended at 1,000 s, and the next as the engine sets it:
    // after the interval, or after the first retry's wait.
    fn announce(
        in_flight: bool,
        last: Option<Result<AnnounceOk, AnnounceError>>,
    ) -> TrackerAnnounce {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        let (fails, next) = match &last {
            None => (0, None),
            Some(Ok(ok)) => (0, Some(at + ok.interval.max(ok.min_interval))),
            Some(Err(_)) => (1, Some(at + Duration::from_secs(17))),
        };
        TrackerAnnounce {
            url: "http://tracker.invalid/announce".into(),
            in_flight,
            last: last.map(|result| LastAnnounce { at, result }),
            fails,
            next_announce: next,
        }
    }

    fn accepted(interval: u64, min_interval: u64) -> AnnounceOk {
        AnnounceOk {
            interval: Duration::from_secs(interval),
            min_interval: Duration::from_secs(min_interval),
            seeders: Some(3),
            leechers: Some(5),
            downloaded: Some(8),
            peers: vec![
                std::net::SocketAddr::from(([10, 0, 0, 1], 6881)),
                std::net::SocketAddr::from(([10, 0, 0, 2], 6881)),
            ],
            warning: None,
        }
    }

    fn refused(kind: AnnounceErrorKind) -> AnnounceError {
        AnnounceError {
            kind,
            message: "refused".into(),
        }
    }

    #[test]
    fn a_trackers_status_is_its_latest_announce() {
        let never = tracker_view(2, &announce(false, None));
        assert_eq!(
            (never.tier, never.status, never.last_announce_ms),
            (2, TrackerStatus::NotContacted, None)
        );
        // In flight, a tracker keeps the state of its last outcome.
        let updating = tracker_view(0, &announce(true, None));
        assert_eq!(
            (updating.status, updating.updating),
            (TrackerStatus::NotContacted, true)
        );
        let updating = tracker_view(0, &announce(true, Some(Ok(accepted(1800, 30)))));
        assert_eq!(
            (updating.status, updating.updating),
            (TrackerStatus::Working, true)
        );

        let working = tracker_view(0, &announce(false, Some(Ok(accepted(1800, 30)))));
        assert_eq!(working.status, TrackerStatus::Working);
        assert!(!working.updating);
        assert_eq!(working.last_announce_ms, Some(1_000_000));
        assert_eq!(working.next_announce_ms, Some(2_800_000));
        assert_eq!(working.min_announce_ms, Some(1_030_000));
        assert_eq!(
            (
                working.seeders,
                working.leechers,
                working.downloaded,
                working.peers
            ),
            (Some(3), Some(5), Some(8), Some(2))
        );
        assert_eq!((working.message, working.fails), (None, 0));
        // The tracker's warning is its message.
        let warned = AnnounceOk {
            warning: Some("slow".into()),
            ..accepted(1800, 30)
        };
        assert_eq!(
            tracker_view(0, &announce(false, Some(Ok(warned))))
                .message
                .as_deref(),
            Some("slow")
        );

        // qBittorrent TorrentImpl: tracker_failure is a tracker error,
        // announce_skipped unreachable, anything else not working.
        for (kind, status) in [
            (AnnounceErrorKind::Refused, TrackerStatus::TrackerError),
            (AnnounceErrorKind::Skipped, TrackerStatus::Unreachable),
            (AnnounceErrorKind::Failed, TrackerStatus::NotWorking),
        ] {
            let failed = tracker_view(0, &announce(false, Some(Err(refused(kind)))));
            assert_eq!(failed.status, status);
            assert_eq!(failed.message.as_deref(), Some("refused"));
            assert_eq!(failed.last_announce_ms, Some(1_000_000));
            assert_eq!(
                (
                    failed.seeders,
                    failed.leechers,
                    failed.next_announce_ms,
                    failed.fails
                ),
                (None, None, Some(1_017_000), 1)
            );
        }
    }

    #[test]
    fn the_swarm_is_the_largest_tracker_count_and_the_soonest_announce() {
        let counted = |seeders, leechers, interval| {
            announce(
                false,
                Some(Ok(AnnounceOk {
                    seeders,
                    leechers,
                    ..accepted(interval, 30)
                })),
            )
        };
        let announces = [
            counted(Some(3), Some(9), 1800),
            counted(Some(7), None, 60),
            announce(false, Some(Err(refused(AnnounceErrorKind::Failed)))),
        ];
        // The failed announce at 1,000 s is retried at 1,017 s.
        assert_eq!(
            swarm(&announces, 1_010_500),
            Swarm {
                seeds: Some(7),
                leechers: Some(9),
                reannounce_seconds: Some(7),
            }
        );
        // An announce already due is due now.
        assert_eq!(swarm(&announces, 1_100_000).reannounce_seconds, Some(0));
        assert_eq!(swarm(&[], 0), Swarm::default());
    }

    // An answer whose interval has long passed with no announce in flight
    // stands while the run still announces to the tracker.
    #[test]
    fn an_answer_stands_past_its_interval() {
        let stale = announce(false, Some(Ok(accepted(1, 30))));
        assert_eq!(tracker_view(0, &stale).status, TrackerStatus::Working);
    }

    // The run has stopped announcing to the tracker: its `stopped` answer,
    // or the failure of it, is the last outcome the row reads, with no
    // floor for an announce that is not coming.
    #[test]
    fn a_tracker_the_run_stopped_announcing_to_keeps_its_last_outcome() {
        let stopped = TrackerAnnounce {
            next_announce: None,
            ..announce(false, Some(Ok(accepted(1800, 30))))
        };
        let view = tracker_view(0, &stopped);
        assert_eq!(
            (
                view.status,
                view.updating,
                view.next_announce_ms,
                view.min_announce_ms
            ),
            (TrackerStatus::Working, false, None, None)
        );
        assert_eq!(
            (view.last_announce_ms, view.peers, view.seeders),
            (Some(1_000_000), Some(2), Some(3))
        );
        let failed = TrackerAnnounce {
            next_announce: None,
            ..announce(false, Some(Err(refused(AnnounceErrorKind::Failed))))
        };
        let view = tracker_view(0, &failed);
        assert_eq!(
            (view.status, view.message.as_deref(), view.min_announce_ms),
            (TrackerStatus::NotWorking, Some("refused"), None)
        );
    }

    // A tracker named by its address is resolved by no one; one named by
    // its host, by whoever resolves the route's names.
    #[test]
    fn a_trackers_host_is_resolved_by_the_routes_naming_unless_it_is_an_address() {
        for (url, by) in [
            ("http://tracker.invalid/announce", Some(ResolvedBy::Proxy)),
            ("udp://tracker.invalid:80", Some(ResolvedBy::Proxy)),
            ("http://192.0.2.7:6969/announce", Some(ResolvedBy::Literal)),
            ("udp://192.0.2.7:6969", Some(ResolvedBy::Literal)),
            ("udp://[2001:db8::7]:6969", Some(ResolvedBy::Literal)),
            ("not a url", None),
        ] {
            assert_eq!(resolved_by(url, Some(ResolvedBy::Proxy)), by, "{url}");
        }
        assert_eq!(resolved_by("http://tracker.invalid/announce", None), None);

        let listed = vec![TrackerEntry {
            tier: 0,
            url: "http://tracker.invalid/announce".into(),
            named_by: NamedBy::Torrent,
        }];
        let rows = trackers(listed, &[], Some(ResolvedBy::Host));
        assert_eq!(rows[0].resolved_by, Some(ResolvedBy::Host));
    }

    #[test]
    fn a_torrent_lists_its_trackers_until_it_runs_then_the_runs() {
        let listed = vec![
            TrackerEntry {
                tier: 1,
                url: "udp://backup.invalid:80".into(),
                named_by: NamedBy::Operator,
            },
            TrackerEntry {
                tier: 0,
                url: "http://tracker.invalid".into(),
                named_by: NamedBy::Torrent,
            },
        ];
        let never: Vec<_> = trackers(listed.clone(), &[], None)
            .into_iter()
            .map(|tracker| (tracker.tier, tracker.url, tracker.status, tracker.named_by))
            .collect();
        assert_eq!(
            never,
            [
                (
                    0,
                    "http://tracker.invalid".into(),
                    TrackerStatus::NotContacted,
                    NamedBy::Torrent
                ),
                (
                    1,
                    "udp://backup.invalid:80".into(),
                    TrackerStatus::NotContacted,
                    NamedBy::Operator
                ),
            ]
        );

        // The run names each tracker by its parsed URL, and one the torrent
        // does not list came with its magnet.
        let run = [
            TrackerAnnounce {
                url: "udp://backup.invalid:80".into(),
                ..Default::default()
            },
            TrackerAnnounce {
                url: "http://tracker.invalid/".into(),
                in_flight: true,
                ..Default::default()
            },
            TrackerAnnounce {
                url: "http://magnet.invalid/announce".into(),
                ..Default::default()
            },
        ];
        let ran: Vec<_> = trackers(listed, &run, None)
            .into_iter()
            .map(|tracker| {
                (
                    tracker.tier,
                    tracker.url,
                    tracker.updating,
                    tracker.named_by,
                )
            })
            .collect();
        assert_eq!(
            ran,
            [
                (
                    0,
                    "http://magnet.invalid/announce".into(),
                    false,
                    NamedBy::Torrent
                ),
                (0, "http://tracker.invalid/".into(), true, NamedBy::Torrent),
                (
                    1,
                    "udp://backup.invalid:80".into(),
                    false,
                    NamedBy::Operator
                ),
            ]
        );
    }
}
