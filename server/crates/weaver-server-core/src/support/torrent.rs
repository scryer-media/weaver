// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Support reports for torrents: the torrent's analysis, from its metainfo or
// from the link it was added by, plus what Weaver did with the job.
//
// The same rule holds as for NZB jobs: nothing here names the torrent, a
// file, a path, a tracker's passkey or a peer. Trackers appear by scheme and
// host, as in the analysis; words the session or a tracker said are reduced
// to a constant.

use std::fmt::Write as _;

use serde::Serialize;
use weaver_torrent::analysis::{Endpoint, TorrentReport, analyze_magnet, analyze_metainfo};
use weaver_torrent::core::Id20;
use weaver_torrent::{FetchPhase, Magnet};

use super::{
    FailureVerdict, JobSource, OutputShape, ReportEnvironment, SupportReport, SupportReportError,
    clip, human_age, human_bytes, known_status, write_environment, yes_no,
};
use crate::history::JobEvent;
use crate::jobs::JobId;
use crate::jobs::handle::HoldOrigin;
use crate::persistence::Database;
use crate::proxies::{TorrentRouteFault, TorrentScope};
use crate::settings::Level;
use crate::settings::SeedingGoalMode;
use crate::torrents::metrics::{
    JobSeries, SessionSeries, TorrentMetrics, TorrentSeries, TrackerSeries, fetch_phase_str,
};
use crate::torrents::service::{PeerKinds, TorrentLeg, TorrentSupport};
use crate::torrents::sessions::TorrentRouteReason;
use crate::torrents::status;
use crate::torrents::{NamedBy, TorrentState, TorrentTracker, TrackerStatus};
use crate::{JobHistoryRow, JobInfo};

// Trackers listed in the text before the rest are left to the JSON.
const MAX_TRACKERS: usize = 6;
// Event kinds listed in the text before the rest are left to the JSON.
const MAX_EVENT_KINDS: usize = 6;

#[derive(Serialize)]
struct CombinedTorrentReport<'a> {
    torrent: &'a TorrentReport,
    job: Option<&'a TorrentJobSection>,
    environment: &'a ReportEnvironment,
}

pub(super) fn render(
    torrent: &TorrentReport,
    job: Option<&TorrentJobSection>,
    environment: &ReportEnvironment,
) -> SupportReport {
    let combined = CombinedTorrentReport {
        torrent,
        job,
        environment,
    };
    let json = serde_json::to_string_pretty(&combined).expect("report types always serialize");
    let mut text = torrent.to_text();
    if let Some(job) = job {
        job.render(&mut text);
    }
    write_environment(&mut text, environment);
    SupportReport { text, json }
}

pub(super) fn analysis_error(error: impl std::fmt::Display) -> SupportReportError {
    SupportReportError::TorrentParse(error.to_string())
}

// The report of a torrent job: from what the service holds of it while it
// lives, else from its history row.
pub(super) fn build_job_report(
    db: &Database,
    live: Option<JobInfo>,
    held: Option<TorrentSupport>,
    job_id: u64,
    environment: &ReportEnvironment,
    now_epoch_secs: u64,
) -> Result<SupportReport, SupportReportError> {
    let now_ms = i64::try_from(now_epoch_secs.saturating_mul(1000)).unwrap_or(i64::MAX);
    let history = db.get_job_history(job_id)?;
    let (mut job, analysis) = match (&held, &history) {
        (Some(held), _) => {
            let mut job = TorrentJobSection::from_held(held, now_ms);
            if let Some(info) = &live {
                job.apply_live(info);
            }
            let analysis = match &held.metainfo {
                Some(bytes) => analyze_metainfo(bytes, now_epoch_secs).map_err(analysis_error)?,
                None => analyze_magnet(&held_magnet(held)?, now_epoch_secs),
            };
            (job, analysis)
        }
        (None, Some(row)) => {
            // A retired torrent keeps no metainfo: the hash it went by is
            // what is left of it.
            let magnet =
                history_magnet(row).ok_or(SupportReportError::TorrentUnavailable(job_id))?;
            (
                TorrentJobSection::from_history(row),
                analyze_magnet(&magnet, now_epoch_secs),
            )
        }
        (None, None) => {
            return Err(match live {
                Some(_) => SupportReportError::TorrentUnavailable(job_id),
                None => SupportReportError::NotFound(job_id),
            });
        }
    };
    job.apply_events(&db.get_job_events(job_id)?);
    job.checkpoint_in_hand = db.load_graph_root(JobId(job_id))?.is_some();
    let analysis = analysis.with_weaver_version(environment.weaver_version);
    Ok(render(&analysis, Some(&job), environment))
}

// The link a held torrent goes by: its hashes from its view, with the
// trackers it announces to.
fn held_magnet(held: &TorrentSupport) -> Result<Magnet, SupportReportError> {
    let mut magnet = Magnet::parse(&held.view.magnet).map_err(analysis_error)?;
    magnet.trackers = held
        .view
        .trackers
        .iter()
        .map(|tracker| tracker.url.clone())
        .collect();
    Ok(magnet)
}

fn history_magnet(row: &JobHistoryRow) -> Option<Magnet> {
    let hash: [u8; 20] = row.job_hash.as_deref()?.try_into().ok()?;
    Some(Magnet::from_id20(Id20::new(hash), Vec::new(), None))
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct LegOutcome {
    pub position: u32,
    // `e3` is configured egress 3.
    pub egress: EgressToken,
    pub own_sockets: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressToken(u32);

impl std::fmt::Display for EgressToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.0)
    }
}

impl Serialize for EgressToken {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl From<TorrentLeg> for LegOutcome {
    fn from(leg: TorrentLeg) -> Self {
        Self {
            position: u32::try_from(leg.position).unwrap_or(u32::MAX),
            egress: EgressToken(leg.egress_id),
            own_sockets: leg.own_sockets,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct TorrentCounts {
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub wasted_bytes: u64,
    pub hash_failed_bytes: u64,
    pub redundant_bytes: u64,
    pub active_secs: u64,
    pub seeding_secs: u64,
    pub consecutive_seeding_secs: u64,
    // Thousandths.
    pub ratio_milli: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct PieceCounts {
    pub total: u32,
    pub have: u32,
    pub piece_bytes: u64,
    pub wanted_bytes: u64,
    pub wanted_have_bytes: u64,
    // The pieces that failed their check this run, as the bytes thrown away
    // over the piece length: the session counts bytes, not pieces.
    pub hash_failures: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct PeerCounts {
    pub established: u32,
    pub seeders: u32,
    pub connecting: u32,
    pub tcp: u32,
    pub utp: u32,
    pub outgoing: u32,
    pub incoming: u32,
    pub encrypted: u32,
}

impl PeerCounts {
    fn of((established, seeders, connecting): (u32, u32, u32), kinds: PeerKinds) -> Self {
        Self {
            established,
            seeders,
            connecting,
            tcp: kinds.tcp,
            utp: kinds.utp,
            outgoing: kinds.outgoing,
            incoming: kinds.incoming,
            encrypted: kinds.encrypted,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TrackerOutcome {
    pub endpoint: Endpoint,
    pub named_by: &'static str,
    pub status: &'static str,
    pub updating: bool,
    pub last_answer_age_secs: Option<u64>,
    pub next_announce_in_secs: Option<u64>,
    pub min_announce_in_secs: Option<u64>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub completed: Option<u32>,
    pub peers: Option<u32>,
    pub fails: u32,
    // What the tracker's message says, as one of a fixed set.
    pub message: Option<&'static str>,
}

impl TrackerOutcome {
    fn of(tracker: &TorrentTracker, now_ms: i64) -> Self {
        let age = |at: Option<i64>| at.map(|at| u64::try_from((now_ms - at) / 1000).unwrap_or(0));
        let until = |at: Option<i64>| at.map(|at| u64::try_from((at - now_ms) / 1000).unwrap_or(0));
        Self {
            endpoint: Endpoint::of(tracker.tier, tracker.url.as_bytes()),
            named_by: match tracker.named_by {
                NamedBy::Torrent => "torrent",
                NamedBy::Operator => "operator",
            },
            status: tracker_status(tracker.status),
            updating: tracker.updating,
            last_answer_age_secs: age(tracker.last_announce_ms),
            next_announce_in_secs: until(tracker.next_announce_ms),
            min_announce_in_secs: until(tracker.min_announce_ms),
            seeders: tracker.seeders,
            leechers: tracker.leechers,
            completed: tracker.downloaded,
            peers: tracker.peers,
            fails: tracker.fails,
            message: tracker.message.as_deref().map(tracker_message),
        }
    }
}

fn tracker_status(status: TrackerStatus) -> &'static str {
    match status {
        TrackerStatus::NotContacted => "not_contacted",
        TrackerStatus::Working => "working",
        TrackerStatus::NotWorking => "not_working",
        TrackerStatus::TrackerError => "tracker_error",
        TrackerStatus::Unreachable => "unreachable",
        TrackerStatus::Disabled => "disabled",
    }
}

// A tracker's own words reduced to their meaning: they can quote the
// operator's account.
fn tracker_message(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    let any = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));
    if any(&[
        "unregistered",
        "not registered",
        "torrent not found",
        "unknown torrent",
    ]) {
        "unregistered"
    } else if any(&[
        "passkey",
        "auth",
        "login",
        "credential",
        "not authorized",
        "unauthorized",
    ]) {
        "authorization"
    } else if any(&["client", "whitelist", "banned client", "not allowed"]) {
        "client_refused"
    } else if any(&["ratio", "hit and run", "download privileges"]) {
        "account_standing"
    } else if any(&["too many", "rate", "slow down", "interval"]) {
        "rate_limited"
    } else if any(&[
        "timed out",
        "timeout",
        "connection",
        "refused",
        "resolve",
        "dns",
    ]) {
        "unreachable"
    } else if any(&["maintenance", "offline", "down"]) {
        "maintenance"
    } else {
        "other"
    }
}

// The level each seeding term comes from.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SeedingLevelNames {
    pub ratio_goal: &'static str,
    pub seed_time_goal: &'static str,
    pub inactive_goal: &'static str,
    pub mode: &'static str,
    pub action: &'static str,
    pub seed_time_counting: &'static str,
    pub upload_cap: &'static str,
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Torrent => "torrent",
        Level::TrackerRule => "tracker_rule",
        Level::Category => "category",
        Level::Global => "global",
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SeedingOutcome {
    pub levels: SeedingLevelNames,
    pub mode: &'static str,
    pub action: &'static str,
    pub seed_time_counting: &'static str,
    pub ratio_goal_milli: Option<u64>,
    pub ratio_left_milli: Option<u64>,
    pub seed_minutes_goal: Option<u32>,
    pub seed_minutes_left: Option<u64>,
    pub inactive_minutes_goal: Option<u32>,
    pub inactive_minutes_left: Option<u64>,
    pub upload_cap_bps: Option<u64>,
}

impl SeedingOutcome {
    fn of(held: &TorrentSupport, now_ms: i64) -> Self {
        let policy = &held.view.seeding_policy;
        let counters = &held.counters;
        let ratio_milli = milli(held.view.ratio);
        let seeded = policy.seeded_seconds(counters) / 60;
        let idle = counters
            .last_activity_ms
            .map(|at| u64::try_from((now_ms - at).max(0)).unwrap_or(0) / 60_000);
        Self {
            levels: {
                let levels = &held.view.seeding_levels;
                SeedingLevelNames {
                    ratio_goal: level_name(levels.ratio_goal),
                    seed_time_goal: level_name(levels.seed_time_goal_minutes),
                    inactive_goal: level_name(levels.inactive_goal_minutes),
                    mode: level_name(levels.mode),
                    action: level_name(levels.action),
                    seed_time_counting: level_name(levels.seed_time_counting),
                    upload_cap: level_name(levels.upload_cap),
                }
            },
            mode: match policy.mode {
                SeedingGoalMode::Any => "any",
                SeedingGoalMode::All => "all",
            },
            action: policy.action.as_str(),
            seed_time_counting: policy.seed_time_counting.as_str(),
            ratio_goal_milli: policy.ratio_goal.map(|goal| milli(f64::from(goal))),
            ratio_left_milli: policy
                .ratio_goal
                .map(|goal| milli(f64::from(goal)).saturating_sub(ratio_milli)),
            seed_minutes_goal: policy.seed_time_goal_minutes,
            seed_minutes_left: policy
                .seed_time_goal_minutes
                .map(|goal| u64::from(goal).saturating_sub(seeded)),
            inactive_minutes_goal: policy.inactive_goal_minutes,
            inactive_minutes_left: policy
                .inactive_goal_minutes
                .map(|goal| u64::from(goal).saturating_sub(idle.unwrap_or(0))),
            upload_cap_bps: held.view.upload_cap,
        }
    }
}

fn milli(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        (value * 1000.0).round() as u64
    } else {
        0
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EventCount {
    pub kind: String,
    pub count: u32,
}

// What a torrent job did, from what the service holds and rows already
// stored.
#[derive(Debug, Clone, Serialize)]
pub struct TorrentJobSection {
    pub source: JobSource,
    pub status: &'static str,
    pub state: Option<&'static str>,
    // Why it is in its state: a constant the state is known by, or the
    // cause the words name.
    pub why: Option<&'static str>,
    pub route_reason: Option<&'static str>,
    pub paused_by: Option<&'static str>,
    pub failure: Option<FailureVerdict>,
    pub fetch_phase: Option<&'static str>,
    pub scope: Option<String>,
    // The route's key: a digest of its legs, never their addresses.
    pub route: Option<String>,
    pub leg: Option<LegOutcome>,
    pub in_session: bool,
    pub private: Option<bool>,
    pub forced: bool,
    pub total_bytes: u64,
    pub counts: Option<TorrentCounts>,
    pub pieces: Option<PieceCounts>,
    pub peers: Option<PeerCounts>,
    pub trackers: Vec<TrackerOutcome>,
    pub seeding: Option<SeedingOutcome>,
    pub checkpoint_in_hand: bool,
    // While its job is not built yet: the pieces its checkpoint gives back.
    pub restore_pieces: Option<u32>,
    pub events: Vec<EventCount>,
    pub extraction_member_failures: u32,
    pub output: OutputShape,
    pub category_set: bool,
    pub run_secs: Option<u64>,
    // The exporter's figures for the job, from the snapshot it renders.
    pub metrics: Option<MetricsSection>,
}

impl TorrentJobSection {
    fn empty(source: JobSource, status: &'static str) -> Self {
        Self {
            source,
            status,
            state: None,
            why: None,
            route_reason: None,
            paused_by: None,
            failure: None,
            fetch_phase: None,
            scope: None,
            route: None,
            leg: None,
            in_session: false,
            private: None,
            forced: false,
            total_bytes: 0,
            counts: None,
            pieces: None,
            peers: None,
            trackers: Vec::new(),
            seeding: None,
            checkpoint_in_hand: false,
            restore_pieces: None,
            events: Vec::new(),
            extraction_member_failures: 0,
            output: OutputShape::of(None),
            category_set: false,
            run_secs: None,
            metrics: None,
        }
    }

    pub fn from_held(held: &TorrentSupport, now_ms: i64) -> Self {
        let view = &held.view;
        let mut section = Self::empty(JobSource::Queue, known_status(view.state.as_str()));
        section.state = Some(view.state.as_str());
        section.why = view.detail.as_deref().map(why);
        section.route_reason = view.route_reason.map(route_reason);
        section.paused_by = view.paused_by.map(HoldOrigin::reason);
        // A stop for any reason but its met goal is the torrent's failure.
        section.failure = match (view.state, view.detail.as_deref()) {
            (TorrentState::Stopped, Some(status::GOAL_MET)) => None,
            (TorrentState::Stopped, Some(detail)) => Some(FailureVerdict::classify(detail)),
            _ => None,
        };
        section.fetch_phase = held.fetch.map(fetch_phase);
        section.scope = Some(scope(view.scope));
        section.route = view.route.clone();
        section.leg = held.leg.map(LegOutcome::from);
        section.in_session = held.in_session;
        section.private = view.version.map(|_| view.private);
        section.forced = view.forced;
        section.total_bytes = view.total_bytes;
        section.counts = Some(TorrentCounts {
            downloaded_bytes: held.counters.downloaded_bytes,
            uploaded_bytes: held.counters.uploaded_bytes,
            wasted_bytes: view.wasted_bytes,
            hash_failed_bytes: held.failed_bytes,
            redundant_bytes: held.redundant_bytes,
            active_secs: view.active_seconds,
            seeding_secs: held.counters.finished_duration_s,
            consecutive_seeding_secs: held.counters.consecutive_seeding_s,
            ratio_milli: milli(view.ratio),
        });
        section.pieces = view.pieces.map(|pieces| PieceCounts {
            total: pieces.count,
            have: pieces.have,
            piece_bytes: pieces.size,
            wanted_bytes: held.wanted_bytes,
            wanted_have_bytes: held.wanted_have_bytes,
            hash_failures: held.failed_bytes.checked_div(pieces.size).unwrap_or(0),
        });
        section.peers = Some(PeerCounts::of(held.peers, held.peer_kinds));
        section.trackers = view
            .trackers
            .iter()
            .map(|tracker| TrackerOutcome::of(tracker, now_ms))
            .collect();
        section.seeding = Some(SeedingOutcome::of(held, now_ms));
        section.restore_pieces =
            (!held.has_job && view.pieces.is_some()).then_some(held.kept_pieces);
        section.output = OutputShape::of(view.storage_dir.as_deref());
        section.category_set = view.category.as_deref().is_some_and(|c| !c.is_empty());
        section.metrics = MetricsSection::of(&held.metrics, view.job_id, now_ms);
        section.run_secs = view
            .completed_at_ms
            .filter(|done| *done >= view.added_at_ms)
            .map(|done| u64::try_from((done - view.added_at_ms) / 1000).unwrap_or(0));
        section
    }

    // The queue's word on the job, where it has one.
    fn apply_live(&mut self, info: &JobInfo) {
        self.status = known_status(info.status.persisted_status());
        if let Some(error) = info.error.as_deref() {
            self.failure = Some(FailureVerdict::classify(error));
        }
    }

    pub fn from_history(row: &JobHistoryRow) -> Self {
        let mut section = Self::empty(JobSource::History, known_status(&row.status));
        section.failure = match (row.status.as_str(), row.error_message.as_deref()) {
            ("cancelled", _) => Some(FailureVerdict::Cancelled),
            ("failed", message) => Some(FailureVerdict::classify(message.unwrap_or(""))),
            (_, Some(message)) if !message.trim().is_empty() => {
                Some(FailureVerdict::classify(message))
            }
            _ => None,
        };
        section.total_bytes = row.total_bytes;
        section.counts = Some(TorrentCounts {
            downloaded_bytes: row.downloaded_bytes,
            uploaded_bytes: 0,
            wasted_bytes: row.failed_bytes,
            hash_failed_bytes: row.failed_bytes,
            redundant_bytes: 0,
            active_secs: 0,
            seeding_secs: 0,
            consecutive_seeding_secs: 0,
            ratio_milli: 0,
        });
        section.output = OutputShape::of(row.output_dir.as_deref());
        section.category_set = row.category.as_deref().is_some_and(|c| !c.is_empty());
        section.run_secs = (row.completed_at >= row.created_at && row.created_at > 0)
            .then(|| u64::try_from(row.completed_at - row.created_at).unwrap_or(0));
        section
    }

    // Count the job's recorded events by kind. Only kinds are read; their
    // messages can name files.
    pub fn apply_events(&mut self, events: &[JobEvent]) {
        for event in events {
            let kind = event.kind.as_str();
            if kind == "ExtractionMemberFailed" {
                self.extraction_member_failures += 1;
            }
            // Kinds are the code's own names; anything else is not one.
            if kind.is_empty()
                || kind.len() > 48
                || !kind.bytes().all(|byte| byte.is_ascii_alphanumeric())
            {
                continue;
            }
            match self.events.iter_mut().find(|count| count.kind == kind) {
                Some(count) => count.count += 1,
                None => self.events.push(EventCount {
                    kind: kind.to_owned(),
                    count: 1,
                }),
            }
        }
    }

    fn render(&self, text: &mut String) {
        let source = match self.source {
            JobSource::Queue => "queue",
            JobSource::History => "history",
        };
        let _ = writeln!(text, "job ({source}, torrent)");
        let mut status = format!("  status {}", self.status);
        if let Some(state) = self.state {
            let _ = write!(status, "  state {state}");
        }
        if let Some(failure) = self.failure {
            let _ = write!(status, "  verdict {}", failure.as_str());
        }
        if let Some(secs) = self.run_secs {
            let _ = write!(status, "  ran {secs}s");
        }
        let _ = writeln!(text, "{}", clip(&status));
        let mut why_line = String::from("  why");
        let mut any = false;
        for (label, value) in [
            ("", self.why),
            ("route", self.route_reason),
            ("held by", self.paused_by),
            ("fetch", self.fetch_phase),
        ] {
            if let Some(value) = value {
                any = true;
                if label.is_empty() {
                    let _ = write!(why_line, " {value}");
                } else {
                    let _ = write!(why_line, "  {label} {value}");
                }
            }
        }
        if any {
            let _ = writeln!(text, "{}", clip(&why_line));
        }
        if let Some(scope) = &self.scope {
            let mut route = format!(
                "  scope {scope}  route {}",
                self.route.as_deref().unwrap_or("-")
            );
            match &self.leg {
                Some(leg) => {
                    let _ = write!(
                        route,
                        "  leg {} {}{}",
                        leg.position,
                        leg.egress,
                        if leg.own_sockets { " own sockets" } else { "" }
                    );
                }
                None => route.push_str("  leg -"),
            }
            let _ = write!(route, "  session {}", yes_no(self.in_session));
            let _ = writeln!(text, "{}", clip(&route));
        }
        if let Some(counts) = &self.counts {
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "  down {}  up {}  ratio {}.{:03}  of {}",
                    human_bytes(counts.downloaded_bytes),
                    human_bytes(counts.uploaded_bytes),
                    counts.ratio_milli / 1000,
                    counts.ratio_milli % 1000,
                    human_bytes(self.total_bytes),
                ))
            );
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "  wasted {} (hash fail {}, redundant {})  active {}  seeding {} (unbroken {})",
                    human_bytes(counts.wasted_bytes),
                    human_bytes(counts.hash_failed_bytes),
                    human_bytes(counts.redundant_bytes),
                    human_age(counts.active_secs),
                    human_age(counts.seeding_secs),
                    human_age(counts.consecutive_seeding_secs),
                ))
            );
        }
        if let Some(pieces) = &self.pieces {
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "  pieces {}/{} x {}  wanted {} of {}  hash fails ~{}",
                    pieces.have,
                    pieces.total,
                    human_bytes(pieces.piece_bytes),
                    human_bytes(pieces.wanted_have_bytes),
                    human_bytes(pieces.wanted_bytes),
                    pieces.hash_failures,
                ))
            );
        }
        if let Some(peers) = &self.peers {
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "  peers {} (seeds {}) connecting {}  tcp {} utp {}  out {} in {}  enc {}",
                    peers.established,
                    peers.seeders,
                    peers.connecting,
                    peers.tcp,
                    peers.utp,
                    peers.outgoing,
                    peers.incoming,
                    peers.encrypted,
                ))
            );
        }
        for tracker in self.trackers.iter().take(MAX_TRACKERS) {
            let endpoint = &tracker.endpoint;
            let mut line = format!(
                "  tracker {} {} {}",
                endpoint.tier,
                endpoint.scheme.as_str(),
                endpoint.host.as_deref().unwrap_or("-"),
            );
            let _ = write!(line, "  {}", tracker.status);
            if tracker.fails > 0 {
                let _ = write!(line, " x{}", tracker.fails);
            }
            if let Some(message) = tracker.message {
                let _ = write!(line, " ({message})");
            }
            let _ = writeln!(text, "{}", clip(&line));
            let count = |value: Option<u32>| value.map_or("-".to_owned(), |v| v.to_string());
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "    seeds {} leech {} done {} peers {}  last {}  next {}",
                    count(tracker.seeders),
                    count(tracker.leechers),
                    count(tracker.completed),
                    count(tracker.peers),
                    tracker
                        .last_answer_age_secs
                        .map_or("-".to_owned(), |secs| format!("{} ago", human_age(secs))),
                    tracker
                        .next_announce_in_secs
                        .map_or("-".to_owned(), |secs| format!("in {}", human_age(secs))),
                ))
            );
        }
        if self.trackers.len() > MAX_TRACKERS {
            let _ = writeln!(
                text,
                "  ... {} more trackers in the json",
                self.trackers.len() - MAX_TRACKERS
            );
        }
        if let Some(seeding) = &self.seeding {
            let levels = &seeding.levels;
            let mut line = format!(
                "  seeding {} ({}) then {} ({})",
                seeding.mode, levels.mode, seeding.action, levels.action
            );
            if let (Some(goal), Some(left)) = (seeding.ratio_goal_milli, seeding.ratio_left_milli) {
                let _ = write!(
                    line,
                    "  ratio {}.{:03} ({}) left {}.{:03}",
                    goal / 1000,
                    goal % 1000,
                    levels.ratio_goal,
                    left / 1000,
                    left % 1000
                );
            }
            if let (Some(goal), Some(left)) = (seeding.seed_minutes_goal, seeding.seed_minutes_left)
            {
                let _ = write!(
                    line,
                    "  time {goal}m ({}) left {left}m",
                    levels.seed_time_goal
                );
            }
            if let (Some(goal), Some(left)) =
                (seeding.inactive_minutes_goal, seeding.inactive_minutes_left)
            {
                let _ = write!(
                    line,
                    "  idle {goal}m ({}) left {left}m",
                    levels.inactive_goal
                );
            }
            let _ = writeln!(text, "{}", clip(&line));
        }
        let mut checkpoint = format!(
            "  checkpoint {}",
            if self.checkpoint_in_hand {
                "in hand"
            } else {
                "missing"
            }
        );
        if let Some(pieces) = self.restore_pieces {
            let _ = write!(checkpoint, "  restoring {pieces} pieces");
        }
        let _ = writeln!(text, "{}", clip(&checkpoint));
        if !self.events.is_empty() {
            let mut line = String::from("  events");
            for event in self.events.iter().take(MAX_EVENT_KINDS) {
                let _ = write!(line, " {} x{}", event.kind, event.count);
            }
            if self.events.len() > MAX_EVENT_KINDS {
                let _ = write!(line, " +{}", self.events.len() - MAX_EVENT_KINDS);
            }
            let _ = writeln!(text, "{}", clip(&line));
        }
        let _ = writeln!(
            text,
            "  output {}  category {}  member fails {}",
            self.output.as_text(),
            yes_no(self.category_set),
            self.extraction_member_failures,
        );
        if let Some(metrics) = &self.metrics {
            metrics.render(text);
        }
    }
}

// The state's reason, as a constant: one the service states itself, else
// the cause its words name.
fn why(detail: &str) -> &'static str {
    match detail {
        status::DISK_FULL => "disk_full",
        status::GOAL_MET => "goal_met",
        status::OFF => "torrents_off",
        _ => {
            for origin in [
                HoldOrigin::Manual,
                HoldOrigin::Scheduled,
                HoldOrigin::SpeedTest,
            ] {
                if detail == origin.reason() {
                    return origin.reason();
                }
            }
            for phase in [
                FetchPhase::Trackers,
                FetchPhase::Discovery,
                FetchPhase::Waiting,
            ] {
                if detail == phase.to_string() {
                    return fetch_phase(phase);
                }
            }
            FailureVerdict::classify(detail).as_str()
        }
    }
}

fn fetch_phase(phase: FetchPhase) -> &'static str {
    match phase {
        FetchPhase::Trackers => "fetch_trackers",
        FetchPhase::Discovery => "fetch_discovery",
        FetchPhase::Waiting => "fetch_waiting",
    }
}

fn route_reason(reason: TorrentRouteReason) -> &'static str {
    match reason {
        TorrentRouteReason::NotAskedToOpen => "not_asked_to_open",
        TorrentRouteReason::LegDown => "leg_down",
        TorrentRouteReason::LegMissing => "leg_missing",
        TorrentRouteReason::LegRevoked => "leg_revoked",
        TorrentRouteReason::QuotaSilenced => "quota_silenced",
        TorrentRouteReason::TorrentsDisabled => "torrents_disabled",
        TorrentRouteReason::RouteBroken(fault) => match fault {
            TorrentRouteFault::Legs => "route_broken_legs",
            TorrentRouteFault::DirectFallback => "route_broken_direct_fallback",
            TorrentRouteFault::Rungs => "route_broken_rungs",
            TorrentRouteFault::Pool => "route_broken_pool",
        },
        TorrentRouteReason::RoutePlanFailed => "route_plan_failed",
        TorrentRouteReason::LegOpenFailed => "leg_open_failed",
        TorrentRouteReason::StateDirectoryFailed => "state_directory_failed",
        TorrentRouteReason::ListenPortUnavailable => "listen_port_unavailable",
        TorrentRouteReason::ListenPortBindFailed => "listen_port_bind_failed",
        TorrentRouteReason::SessionStateUnreadable => "session_state_unreadable",
        TorrentRouteReason::SessionBuildFailed => "session_build_failed",
        TorrentRouteReason::BlocklistFailed => "blocklist_failed",
        TorrentRouteReason::SessionLimit => "session_limit",
    }
}

// The scope by its kind and configured id, never its name.
fn scope(scope: TorrentScope) -> String {
    match scope {
        TorrentScope::Default => "default".to_owned(),
        TorrentScope::Category(id) => format!("category c{id}"),
        TorrentScope::TrackerRule(id) => format!("rule r{id}"),
    }
}

// The exporter's figures for one torrent, its route's session and the
// service's event counters, read from the snapshot the exporter renders and
// nothing else. A category appears as whether one is set, never its name.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSection {
    pub torrent: Option<TorrentFigures>,
    pub session: Option<SessionFigures>,
    pub events: Vec<(&'static str, u64)>,
    pub fetches: Vec<(&'static str, u64)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentFigures {
    pub state: &'static str,
    pub category_set: bool,
    pub route: Option<String>,
    pub have_bytes: u64,
    pub total_bytes: u64,
    pub wanted_bytes: u64,
    pub pieces_have: Option<u32>,
    pub pieces_total: Option<u32>,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub hash_failed_bytes: u64,
    pub redundant_bytes: u64,
    pub active_secs: u64,
    pub seeding_secs: u64,
    pub consecutive_seeding_secs: u64,
    pub download_bps: u64,
    pub upload_bps: u64,
    pub ratio_milli: u64,
    pub peers_established: u32,
    pub peers_connecting: u32,
    pub peer_seeders: u32,
    // The leg's consumer key and position: `torrent:category:4` names a
    // configured id, never a name.
    pub leg: Option<(String, usize)>,
    pub fetch_phase: Option<&'static str>,
    // Each goal of the policy in force: its target and what is left.
    pub seeding_goals: Vec<(&'static str, f64, f64)>,
    pub seeding_met: bool,
    pub trackers: Vec<TrackerFigures>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TrackerFigures {
    pub host: String,
    pub status: &'static str,
    pub last_announce_age_secs: Option<u64>,
    pub next_announce_in_secs: Option<u64>,
    pub interval_secs: Option<u64>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub completed: Option<u32>,
    pub fails: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionFigures {
    pub open: bool,
    pub listening: bool,
    pub external_addresses: usize,
    pub uptime_secs: u64,
    pub received_payload_bytes: u64,
    pub received_protocol_bytes: u64,
    pub sent_payload_bytes: u64,
    pub sent_protocol_bytes: u64,
    pub blocked_incoming: u64,
    pub blocked_outgoing: u64,
    pub peers: Vec<(&'static str, u32)>,
    // Transport, address family, then attempted, succeeded, failed.
    pub connects: Vec<(&'static str, &'static str, [u64; 3])>,
    // Nodes in the v4 and v6 tables, and requests awaiting an answer.
    pub dht: Option<(usize, usize, usize)>,
}

impl MetricsSection {
    fn of(metrics: &TorrentMetrics, job_id: JobId, now_ms: i64) -> Option<Self> {
        let job = metrics.published.job(job_id);
        let events: Vec<(&'static str, u64)> = metrics
            .events
            .iter()
            .map(|(event, count)| (event.as_str(), *count))
            .collect();
        let fetches: Vec<(&'static str, u64)> = metrics
            .fetches
            .iter()
            .map(|(outcome, count)| (outcome.as_str(), *count))
            .collect();
        if job.is_none() && events.iter().chain(&fetches).all(|(_, count)| *count == 0) {
            return None;
        }
        Some(Self {
            torrent: job.map(|JobSeries { torrent, .. }| TorrentFigures::of(torrent, now_ms)),
            session: job.and_then(|job| job.session).map(SessionFigures::of),
            events,
            fetches,
        })
    }

    fn render(&self, text: &mut String) {
        let _ = writeln!(text, "metrics (as exported)");
        if let Some(torrent) = &self.torrent {
            torrent.render(text);
        }
        if let Some(session) = &self.session {
            session.render(text);
        }
        let mut line = String::from("  events");
        for (kind, count) in &self.events {
            let _ = write!(line, " {kind} {count}");
        }
        let _ = writeln!(text, "{}", clip(&line));
        let mut line = String::from("  fetches");
        for (outcome, count) in &self.fetches {
            let _ = write!(line, " {outcome} {count}");
        }
        let _ = writeln!(text, "{}", clip(&line));
    }
}

impl TorrentFigures {
    fn of(torrent: &TorrentSeries, now_ms: i64) -> Self {
        Self {
            state: torrent.state.as_str(),
            category_set: !torrent.category.is_empty(),
            route: (!torrent.route.is_empty()).then(|| torrent.route.clone()),
            have_bytes: torrent.have_bytes,
            total_bytes: torrent.total_bytes,
            wanted_bytes: torrent.wanted_bytes,
            pieces_have: torrent.pieces.map(|pieces| pieces.have),
            pieces_total: torrent.pieces.map(|pieces| pieces.count),
            downloaded_bytes: torrent.downloaded_bytes,
            uploaded_bytes: torrent.uploaded_bytes,
            hash_failed_bytes: torrent.hash_failed_bytes,
            redundant_bytes: torrent.redundant_bytes,
            active_secs: torrent.active_seconds,
            seeding_secs: torrent.seeding_seconds,
            consecutive_seeding_secs: torrent.consecutive_seeding_seconds,
            download_bps: torrent.download_bps,
            upload_bps: torrent.upload_bps,
            ratio_milli: milli(torrent.ratio),
            peers_established: torrent.peers_established,
            peers_connecting: torrent.peers_connecting,
            peer_seeders: torrent.peer_seeders,
            leg: torrent.leg.clone(),
            fetch_phase: torrent.fetch.map(fetch_phase_str),
            seeding_goals: torrent
                .seeding
                .goals
                .iter()
                .map(|(goal, target, left)| (goal.as_str(), *target, *left))
                .collect(),
            seeding_met: torrent.seeding.met,
            trackers: torrent
                .trackers
                .iter()
                .map(|tracker| TrackerFigures::of(tracker, now_ms))
                .collect(),
        }
    }

    fn render(&self, text: &mut String) {
        let mut line = format!(
            "  {}  have {} of {} (wanted {})",
            self.state,
            human_bytes(self.have_bytes),
            human_bytes(self.total_bytes),
            human_bytes(self.wanted_bytes),
        );
        if let (Some(have), Some(total)) = (self.pieces_have, self.pieces_total) {
            let _ = write!(line, "  pieces {have}/{total}");
        }
        let _ = writeln!(text, "{}", clip(&line));
        let _ = writeln!(
            text,
            "{}",
            clip(&format!(
                "  rate down {}/s up {}/s  ratio {}.{:03}  peers {} (seeds {}) +{}",
                human_bytes(self.download_bps),
                human_bytes(self.upload_bps),
                self.ratio_milli / 1000,
                self.ratio_milli % 1000,
                self.peers_established,
                self.peer_seeders,
                self.peers_connecting,
            ))
        );
        let mut line = format!(
            "  route {}  leg {}",
            self.route.as_deref().unwrap_or("-"),
            self.leg
                .as_ref()
                .map_or("-".to_owned(), |(consumer, position)| format!(
                    "{consumer}#{position}"
                )),
        );
        if let Some(phase) = self.fetch_phase {
            let _ = write!(line, "  fetch {phase}");
        }
        let _ = writeln!(text, "{}", clip(&line));
        let mut line = format!("  seeding met {}", yes_no(self.seeding_met));
        for (goal, target, left) in &self.seeding_goals {
            let _ = write!(line, "  {goal} {target:.2} left {left:.2}");
        }
        let _ = writeln!(text, "{}", clip(&line));
        for tracker in self.trackers.iter().take(MAX_TRACKERS) {
            let count = |value: Option<u32>| value.map_or("-".to_owned(), |v| v.to_string());
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "  tracker {} {} x{}  every {}  seeds {} leech {} done {}",
                    tracker.host,
                    tracker.status,
                    tracker.fails,
                    tracker.interval_secs.map_or("-".to_owned(), human_age),
                    count(tracker.seeders),
                    count(tracker.leechers),
                    count(tracker.completed),
                ))
            );
        }
    }
}

impl TrackerFigures {
    fn of(tracker: &TrackerSeries, now_ms: i64) -> Self {
        let age = |at: Option<i64>| at.map(|at| u64::try_from((now_ms - at) / 1000).unwrap_or(0));
        let until = |at: Option<i64>| at.map(|at| u64::try_from((at - now_ms) / 1000).unwrap_or(0));
        Self {
            host: tracker.host.clone(),
            status: tracker_status(tracker.status),
            last_announce_age_secs: age(tracker.last_announce_ms),
            next_announce_in_secs: until(tracker.next_announce_ms),
            interval_secs: tracker.interval_seconds,
            seeders: tracker.seeders,
            leechers: tracker.leechers,
            completed: tracker.completed,
            fails: tracker.fails,
        }
    }
}

impl SessionFigures {
    fn of(session: &SessionSeries) -> Self {
        Self {
            open: session.open,
            listening: session.listen_port.is_some(),
            external_addresses: session.external_addresses,
            uptime_secs: session.uptime_seconds,
            received_payload_bytes: session.received_payload_bytes,
            received_protocol_bytes: session.received_protocol_bytes,
            sent_payload_bytes: session.sent_payload_bytes,
            sent_protocol_bytes: session.sent_protocol_bytes,
            blocked_incoming: session.blocked_incoming,
            blocked_outgoing: session.blocked_outgoing,
            peers: session.peers.clone(),
            connects: session.connects.clone(),
            dht: session.dht,
        }
    }

    fn render(&self, text: &mut String) {
        let _ = writeln!(
            text,
            "{}",
            clip(&format!(
                "  session open {}  listening {}  addresses {}  up {}",
                yes_no(self.open),
                yes_no(self.listening),
                self.external_addresses,
                human_age(self.uptime_secs),
            ))
        );
        let _ = writeln!(
            text,
            "{}",
            clip(&format!(
                "    in {} (+{} protocol)  out {} (+{} protocol)  blocked {}/{}",
                human_bytes(self.received_payload_bytes),
                human_bytes(self.received_protocol_bytes),
                human_bytes(self.sent_payload_bytes),
                human_bytes(self.sent_protocol_bytes),
                self.blocked_incoming,
                self.blocked_outgoing,
            ))
        );
        let mut line = String::from("    peers");
        for (state, count) in &self.peers {
            let _ = write!(line, " {state} {count}");
        }
        let _ = writeln!(text, "{}", clip(&line));
        let mut line = String::from("    connects");
        for (transport, family, [tried, ok, failed]) in &self.connects {
            let _ = write!(line, " {transport}/{family} {ok}/{tried} x{failed}");
        }
        let _ = writeln!(text, "{}", clip(&line));
        if let Some((v4, v6, waiting)) = self.dht {
            let _ = writeln!(text, "    dht nodes v4 {v4} v6 {v6}  waiting {waiting}");
        }
    }
}
