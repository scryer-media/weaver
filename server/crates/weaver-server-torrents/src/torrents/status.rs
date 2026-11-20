// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent's state as consumers see it, read off what the session reports
//! and what holds it back, and its seeding goal.

pub use weaver_server_torrent_store::torrents::status::*;
use weaver_torrent::{FetchPhase, TorrentStats, TorrentStatsState};

use crate::jobs::JobId;
use crate::jobs::handle::HoldTarget;
use crate::jobs::{HoldOrigin, HoldOwner, HoldSet};
use crate::torrents::sessions::{RouteFailure, TorrentRouteReason};
use crate::torrents::{TorrentIntent, TorrentState};

/// A reason the service gives a torrent's state itself, carried typed, never
/// as words: the API sends it as an enum for the web to put in words, the
/// support bundle and the torrent's job row by its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stated {
    /// Kept from starting for room on its disk, as a usenet job waits.
    DiskSpace,
    /// A write found no room: it fetches nothing and keeps seeding.
    DiskFull,
    /// Stopped once its seeding goal was met.
    GoalMet,
    /// Held in the queue by a hold from this origin.
    Held(HoldOrigin),
    /// Fetching its metadata, asking for peers where this phase asks.
    Fetching(FetchPhase),
}

impl Stated {
    pub const ALL: [Self; 9] = [
        Self::DiskSpace,
        Self::DiskFull,
        Self::GoalMet,
        Self::Held(HoldOrigin::Manual),
        Self::Held(HoldOrigin::Scheduled),
        Self::Held(HoldOrigin::SpeedTest),
        Self::Fetching(FetchPhase::Trackers),
        Self::Fetching(FetchPhase::Discovery),
        Self::Fetching(FetchPhase::Waiting),
    ];

    /// Its key in the support bundle, and the wait reason its torrent's job
    /// gives: a wait for room and a hold give what a usenet job waiting for
    /// the same gives.
    pub const fn key(self) -> &'static str {
        match self {
            Self::DiskSpace => crate::jobs::handle::DISK_SPACE_WAIT_REASON,
            Self::DiskFull => "disk_full",
            Self::GoalMet => "goal_met",
            Self::Held(origin) => origin.reason(),
            Self::Fetching(FetchPhase::Trackers) => "fetch_trackers",
            Self::Fetching(FetchPhase::Discovery) => "fetch_discovery",
            Self::Fetching(FetchPhase::Waiting) => "fetch_waiting",
        }
    }
}

// Its key names it whole: two reasons with one key are one reason.
impl std::hash::Hash for Stated {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

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
    // Torrents being off stops every torrent; a route opening queues it;
    // any other hold stalls it.
    fn off(&self) -> bool {
        matches!(self, Self::RouteDown(failure) if failure.reason == TorrentRouteReason::TorrentsDisabled)
    }

    fn state(&self) -> TorrentState {
        if self.off() {
            TorrentState::Stopped
        } else if self.reason() == TorrentRouteReason::Opening {
            // In no session yet, as one with no session waits for one.
            TorrentState::Queued
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

    // The diagnostic behind the hold's reason, when it has one: why the
    // session could not be built, or why the leg is down. Torrents being
    // off has none: a stop with words is a failure.
    fn diagnostic(&self) -> Option<String> {
        if self.off() {
            return None;
        }
        match self {
            Self::RouteDown(failure) | Self::Session(failure) => {
                failure.detail.clone().filter(|detail| !detail.is_empty())
            }
            Self::SessionLimit => None,
        }
    }
}

/// Why a torrent is in its state: in words, or, while its route holds it,
/// by the route's reason and the diagnostic behind it.
#[derive(Debug, Clone, PartialEq)]
pub enum Why {
    Said(String),
    /// A reason the service states itself.
    Stated(Stated),
    Route(TorrentRouteReason, Option<String>),
}

/// A torrent's reason taken apart for its view.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WhyParts {
    /// Free words (an error, an unpacking refusal), or the route's
    /// diagnostic; never a stated reason's.
    pub detail: Option<String>,
    pub stated: Option<Stated>,
    pub route_reason: Option<TorrentRouteReason>,
}

impl Why {
    pub fn split(why: Option<Self>) -> WhyParts {
        match why {
            Some(Self::Said(text)) => WhyParts {
                detail: Some(text),
                ..WhyParts::default()
            },
            Some(Self::Stated(stated)) => WhyParts {
                stated: Some(stated),
                ..WhyParts::default()
            },
            Some(Self::Route(reason, diagnostic)) => WhyParts {
                detail: diagnostic,
                stated: None,
                route_reason: Some(reason),
            },
            None => WhyParts::default(),
        }
    }
}

impl From<Stated> for Why {
    fn from(stated: Stated) -> Self {
        Self::Stated(stated)
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
    pub held: Option<Held>,
    /// Its directory is being moved; it goes on from the new one.
    pub moving: bool,
    /// Why its restore is held, while it is: one a crash holds keeps the
    /// torrent paused, or stopped, until a resume.
    pub restore_held: Option<String>,
    /// Not one of the files its pieces are in was there when it was loaded:
    /// it is held stopped until a start, a recheck or a move looks again.
    pub files_missing: bool,
    /// It waits for room on its disk, as a usenet job held for disk space
    /// does: in upload mode, seeding, until room is made.
    pub disk_space: bool,
}

impl Observed {
    fn seeding(&self) -> bool {
        seeding(self.complete, &self.engine)
    }
}

/// The state and the reason behind it, when there is one to give.
pub fn project(seen: &Observed) -> (TorrentState, Option<Why>) {
    // A torrent starting from what was saved shows that before anything
    // else, a move or a stop included (qBittorrent
    // `TorrentImpl::updateState`, torrentimpl.cpp:1291-1293, decides
    // CheckingResumeData first).
    if matches!(seen.engine, Engine::Starting) {
        return (TorrentState::CheckingResumeData, None);
    }
    // A move shows next: the torrent's files are on their way, whatever
    // else the engine is doing with them (qBittorrent
    // `TorrentImpl::updateState` decides Moving before Checking).
    if seen.moving {
        return (TorrentState::Moving, None);
    }
    // Files not there show next, whatever the torrent was asked to be
    // (qBittorrent `TorrentImpl::updateState` decides MissingFiles after
    // Moving and before anything else, torrentimpl.cpp:1299-1301).
    if seen.files_missing {
        return (TorrentState::FilesMissing, None);
    }
    match seen.intent {
        TorrentIntent::Paused => {
            return (
                TorrentState::Paused,
                seen.restore_held.clone().map(Why::Said),
            );
        }
        TorrentIntent::Stopped => {
            let why = seen
                .restore_held
                .clone()
                .map(Why::Said)
                .or_else(|| seen.seeding().then_some(Why::Stated(Stated::GoalMet)));
            return (TorrentState::Stopped, why);
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
        return (TorrentState::Queued, Some(Stated::Held(origin).into()));
    }
    if let Some(held) = &seen.held {
        return (
            held.state(),
            Some(Why::Route(held.reason(), held.diagnostic())),
        );
    }
    if !seen.has_metadata {
        return (
            TorrentState::FetchingMetadata,
            seen.fetch.map(|phase| Stated::Fetching(phase).into()),
        );
    }
    if seen.queued {
        return (TorrentState::Queued, None);
    }
    // Kept from starting for room on its disk, it waits in the queue and
    // says so as a usenet job does. Waiting in upload mode is told below.
    if seen.disk_space && !matches!(seen.engine, Engine::Live { .. }) {
        return (TorrentState::Queued, Some(Stated::DiskSpace.into()));
    }
    match &seen.engine {
        // In no session yet: it waits for one to take it.
        Engine::Absent => (TorrentState::Queued, None),
        Engine::Starting => (TorrentState::CheckingResumeData, None),
        Engine::Checking => (TorrentState::CheckingFiles, None),
        // A torrent seeds whatever its unpacking came to.
        Engine::Live { .. } | Engine::Paused { .. } if seen.seeding() => {
            (TorrentState::Seeding, None)
        }
        // Waiting for room, it says so as a usenet job does.
        Engine::Live {
            upload_mode: true, ..
        } if seen.disk_space => (TorrentState::Stalled, Some(Stated::DiskSpace.into())),
        Engine::Live {
            upload_mode: true, ..
        } => (TorrentState::Stalled, Some(Stated::DiskFull.into())),
        Engine::Live { peers: 0, .. } => (TorrentState::Stalled, None),
        Engine::Live { .. } => (TorrentState::Downloading, None),
        Engine::Paused { .. } => (TorrentState::Paused, None),
        Engine::NeedsCertificate => (TorrentState::NeedsCertificate, None),
        Engine::Error(error) => (TorrentState::Stopped, Some(error.as_str().into())),
    }
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
            owner: HoldOwner::Class(crate::torrents::CLASS),
            complete: self.seeding,
            runs_through_schedule: self.runs_through_schedule,
        }
    }
}

/// Each torrent's turn, in queue order: of those no hold stops, every
/// forced or seeding one runs, and the first `max_active` of the rest that
/// are not paused. Only a torrent still downloading takes one of the
/// `max_active` slots, or a rank. With them, the queue's end as the walk
/// left it.
pub fn turns(torrents: &[Queued], max_active: u32, holds: &HoldSet) -> (Vec<Turn>, Slots) {
    let mut slots = Slots::new(max_active);
    let turns = torrents
        .iter()
        .map(|torrent| slots.take(torrent, holds))
        .collect();
    (turns, slots)
}

/// A torrent's turn in the queue: whether it runs, and its place among
/// the torrents still downloading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Turn {
    pub active: bool,
    pub rank: Option<u32>,
}

/// A walk down the queue: the download slots still free and the ranks
/// given. A torrent's turn depends only on those before it, so the walk's
/// end is all a torrent added after them needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slots {
    left: u32,
    ranked: u32,
}

impl Slots {
    pub fn new(max_active: u32) -> Self {
        Self {
            left: max_active,
            ranked: 0,
        }
    }

    /// The turn of `torrent`, next in the queue.
    pub fn take(&mut self, torrent: &Queued, holds: &HoldSet) -> Turn {
        let rank = (!torrent.seeding).then(|| {
            self.ranked += 1;
            self.ranked
        });
        Turn {
            active: self.admit(torrent, holds),
            rank,
        }
    }

    // Whether `torrent` runs: taking a slot if it is still downloading and
    // not forced.
    fn admit(&mut self, torrent: &Queued, holds: &HoldSet) -> bool {
        if holds.reason_for(torrent.target()).is_some() {
            return false;
        }
        if torrent.forced || torrent.seeding {
            return true;
        }
        if torrent.paused || self.left == 0 {
            return false;
        }
        self.left -= 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    // The torrents the queue lets run.
    fn active(torrents: &[Queued], max_active: u32, holds: &HoldSet) -> BTreeSet<JobId> {
        let (turns, _) = turns(torrents, max_active, holds);
        torrents
            .iter()
            .zip(turns)
            .filter(|(_, turn)| turn.active)
            .map(|(torrent, _)| torrent.job_id)
            .collect()
    }

    fn seen(engine: Engine) -> Observed {
        Observed {
            engine,
            has_metadata: true,
            fetch: None,
            complete: false,
            intent: TorrentIntent::Resumed,
            queued: false,
            paused_by: None,
            held: None,
            moving: false,
            restore_held: None,
            files_missing: false,
            disk_space: false,
        }
    }

    // Each reason the service states has a key of its own, one a usenet
    // job waiting for the same gives, and reaches the view typed, with no
    // words beside it.
    #[test]
    fn a_stated_reason_keeps_its_key_and_reaches_the_view_without_words() {
        assert_eq!(
            Stated::ALL.map(Stated::key),
            [
                "disk_space",
                "disk_full",
                "goal_met",
                "paused",
                "scheduled",
                "speed test",
                "fetch_trackers",
                "fetch_discovery",
                "fetch_waiting",
            ]
        );
        assert_eq!(
            Stated::DiskSpace.key(),
            crate::jobs::handle::DISK_SPACE_WAIT_REASON
        );
        for origin in [
            HoldOrigin::Manual,
            HoldOrigin::Scheduled,
            HoldOrigin::SpeedTest,
        ] {
            assert_eq!(Stated::Held(origin).key(), origin.reason());
        }
        let keys: std::collections::HashSet<_> = Stated::ALL.map(Stated::key).into();
        assert_eq!(keys.len(), Stated::ALL.len());
        assert_eq!(TORRENTS_OFF, "torrents are off");
        for stated in Stated::ALL {
            assert_eq!(
                Why::split(Some(stated.into())),
                WhyParts {
                    detail: None,
                    stated: Some(stated),
                    route_reason: None,
                }
            );
        }
        // Words that read like a stated reason's key are free words.
        assert_eq!(
            Why::split(Some(Stated::GoalMet.key().into())),
            WhyParts {
                detail: Some("goal_met".to_owned()),
                ..WhyParts::default()
            }
        );
    }

    // A torrent whose files were not there when it was loaded says so,
    // paused, stopped, complete or held; a move or a start from what was
    // saved shows first.
    #[test]
    fn a_torrent_whose_files_are_missing_says_so() {
        let missing = |engine| Observed {
            files_missing: true,
            ..seen(engine)
        };
        for observed in [
            missing(Engine::Absent),
            Observed {
                complete: true,
                ..missing(Engine::Absent)
            },
            Observed {
                intent: TorrentIntent::Paused,
                ..missing(Engine::Absent)
            },
            Observed {
                intent: TorrentIntent::Stopped,
                complete: true,
                ..missing(Engine::Absent)
            },
            Observed {
                queued: true,
                paused_by: Some(HoldOrigin::Manual),
                ..missing(Engine::Absent)
            },
        ] {
            assert_eq!(
                project(&observed),
                (TorrentState::FilesMissing, None),
                "{observed:?}"
            );
        }
        assert_eq!(
            project(&Observed {
                moving: true,
                ..missing(Engine::Absent)
            }),
            (TorrentState::Moving, None)
        );
        assert_eq!(
            project(&missing(Engine::Starting)),
            (TorrentState::CheckingResumeData, None)
        );
    }

    // A magnet fetching its metadata says where it asks for peers.
    #[test]
    fn a_fetch_names_where_it_asks_for_peers() {
        for phase in [
            FetchPhase::Trackers,
            FetchPhase::Discovery,
            FetchPhase::Waiting,
        ] {
            assert_eq!(
                project(&Observed {
                    has_metadata: false,
                    fetch: Some(phase),
                    ..seen(Engine::Absent)
                }),
                (
                    TorrentState::FetchingMetadata,
                    Some(Stated::Fetching(phase).into())
                )
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

    // A torrent starting from what was saved says so before anything else,
    // as qBittorrent decides CheckingResumeData first
    // (torrentimpl.cpp:1291-1293). One in no session yet is queued.
    #[test]
    fn a_starting_torrent_checks_its_resume_data_before_anything_else() {
        let starting = seen(Engine::Starting);
        for observed in [
            starting.clone(),
            Observed {
                moving: true,
                ..starting.clone()
            },
            Observed {
                intent: TorrentIntent::Stopped,
                complete: true,
                ..starting.clone()
            },
            Observed {
                intent: TorrentIntent::Paused,
                ..starting.clone()
            },
            Observed {
                paused_by: Some(HoldOrigin::Scheduled),
                ..starting.clone()
            },
            Observed {
                queued: true,
                has_metadata: false,
                ..starting.clone()
            },
        ] {
            assert_eq!(
                project(&observed),
                (TorrentState::CheckingResumeData, None),
                "{observed:?}"
            );
        }
        assert_eq!(project(&seen(Engine::Absent)), (TorrentState::Queued, None));
        assert_eq!(
            project(&Observed {
                queued: true,
                ..seen(Engine::Absent)
            }),
            (TorrentState::Queued, None)
        );
    }

    // A torrent past its saved state and waiting its turn to hash reads as
    // checking its files, with nothing hashed yet, and follows the rules of
    // a check; only one still loading what was saved checks its resume data.
    #[test]
    fn a_torrent_waiting_its_turn_to_hash_checks_its_files() {
        let initializing = |checking| TorrentStats {
            state: TorrentStatsState::Initializing {
                paused: false,
                checking,
            },
            file_progress: vec![0],
            error: None,
            progress_bytes: 0,
            uploaded_bytes: 0,
            total_bytes: 8192,
            finished: false,
            live: None,
        };
        let waiting = seen(Engine::of(&initializing(true)));
        assert_eq!(project(&waiting), (TorrentState::CheckingFiles, None));
        assert_eq!(
            project(&Observed {
                queued: true,
                ..waiting.clone()
            }),
            (TorrentState::CheckingFiles, None)
        );
        assert_eq!(
            project(&Observed {
                intent: TorrentIntent::Paused,
                ..waiting
            }),
            (TorrentState::Paused, None)
        );
        assert_eq!(
            project(&seen(Engine::of(&initializing(false)))),
            (TorrentState::CheckingResumeData, None)
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
            (TorrentState::CheckingResumeData, None)
        );
        assert_eq!(project(&seen(Engine::Absent)), (TorrentState::Queued, None));
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
            (TorrentState::Stopped, Some(Stated::GoalMet.into()))
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
            (
                TorrentState::Queued,
                Some(Stated::Held(HoldOrigin::SpeedTest).into())
            )
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
                Some(Why::Route(TorrentRouteReason::LegDown, None))
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
                Some(Why::Route(
                    TorrentRouteReason::LegDown,
                    Some("tunnel gone".into())
                ))
            )
        );
        // A full disk: the session fetches nothing and keeps seeding.
        assert_eq!(
            project(&seen(Engine::Live {
                finished: false,
                peers: 3,
                upload_mode: true,
            })),
            (TorrentState::Stalled, Some(Stated::DiskFull.into()))
        );
        // Waiting for room on its disk, it says so as a usenet job held for
        // disk space does.
        assert_eq!(
            project(&Observed {
                disk_space: true,
                ..seen(Engine::Live {
                    finished: false,
                    peers: 3,
                    upload_mode: true,
                })
            }),
            (TorrentState::Stalled, Some(Stated::DiskSpace.into()))
        );
        // Kept from starting for room on its disk, it waits in the queue.
        assert_eq!(
            project(&Observed {
                disk_space: true,
                ..seen(Engine::Paused { finished: false })
            }),
            (TorrentState::Queued, Some(Stated::DiskSpace.into()))
        );
        assert_eq!(
            project(&Observed {
                complete: true,
                intent: TorrentIntent::Stopped,
                ..seen(live.clone())
            }),
            (TorrentState::Stopped, Some(Stated::GoalMet.into()))
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
            (TorrentState::Stopped, Some(Stated::GoalMet.into()))
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
                Some(Why::Route(TorrentRouteReason::SessionLimit, None))
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
                Some(Why::Route(TorrentRouteReason::SessionLimit, None))
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
                Some(Why::Route(
                    TorrentRouteReason::ListenPortUnavailable,
                    Some("no listening port".into())
                ))
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
                Some(Why::Route(TorrentRouteReason::TorrentsDisabled, None))
            )
        );
        // Waiting on its route's session to open, it is queued for it.
        assert_eq!(
            project(&Observed {
                held: Some(Held::Session(crate::torrents::sessions::OPENING)),
                ..seen(Engine::Absent)
            }),
            (
                TorrentState::Queued,
                Some(Why::Route(TorrentRouteReason::Opening, None))
            )
        );
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
        assert!(
            ids(hold(
                HoldScope::Everything,
                HoldOwner::Class(crate::torrents::CLASS)
            ))
            .is_empty()
        );
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
            HoldOwner::Class(crate::torrents::CLASS),
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
                HoldOwner::Class(crate::torrents::CLASS),
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
}
