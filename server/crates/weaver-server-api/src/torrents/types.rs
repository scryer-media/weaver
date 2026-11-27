// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{
    ComplexObject, Context, Enum, InputObject, OneofObject, Result, SimpleObject, Upload,
};
use chrono::{DateTime, Utc};
use weaver_server_core::auth::CallerScope;
use weaver_server_core::jobs::handle::HoldOrigin;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::proxies::{TorrentRouteFault, TorrentScope};
use weaver_server_core::settings::Seeding;
use weaver_server_core::torrents::net::Exposure;
use weaver_server_core::torrents::service::{
    FilesPage, ScheduleSeeding, SeedingLevels, TorrentRoute, TorrentView, updates::TorrentChanges,
};
use weaver_server_core::torrents::sessions::TorrentRouteReason;
use weaver_server_core::torrents::sessions::{PortMapped, PortmapProtocol, PortmapTransport};
use weaver_server_core::torrents::status::Stated;
use weaver_server_core::torrents::{
    ConnectionKind, Encryption, FetchPhase, FilePriority, NamedBy, PeerInfo, PeerSourceFlag,
    TorrentState, TorrentTracker, TorrentVersion, TrackerStatus, UnpackEnd, UnpackResult,
    UnpackedFile, redacted_tracker_url,
};

use crate::auth::graphql_error;
use crate::jobs::types::{decode_offset_cursor, encode_offset_cursor, ms_to_datetime};
use crate::networking::ResolvedBy;
use crate::settings::types::{
    SeedTimeCountingGql, SeedingActionGql, SeedingGoalModeGql, SeedingPolicyInput,
    SeedingPolicySettings, SettingLevelGql,
};

/// What a torrent is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentState")]
pub enum TorrentStateGql {
    /// Waiting in the queue.
    Queued,
    /// Fetching its metadata.
    FetchingMetadata,
    /// Checking the pieces on disk.
    CheckingFiles,
    /// Downloading.
    Downloading,
    /// Downloading, with nothing arriving.
    Stalled,
    /// Complete and seeding.
    Seeding,
    /// Paused.
    Paused,
    /// Stopped, its seeding over.
    Stopped,
    /// Moving its files.
    Moving,
    /// Waiting for the certificate its SSL torrent needs.
    NeedsCertificate,
    /// Checking its saved resume data.
    CheckingResumeData,
    /// Held: its files are missing from disk.
    FilesMissing,
}

impl From<TorrentState> for TorrentStateGql {
    fn from(state: TorrentState) -> Self {
        match state {
            TorrentState::Queued => Self::Queued,
            TorrentState::FetchingMetadata => Self::FetchingMetadata,
            TorrentState::CheckingFiles => Self::CheckingFiles,
            TorrentState::Downloading => Self::Downloading,
            TorrentState::Stalled => Self::Stalled,
            TorrentState::Seeding => Self::Seeding,
            TorrentState::Paused => Self::Paused,
            TorrentState::Stopped => Self::Stopped,
            TorrentState::Moving => Self::Moving,
            TorrentState::NeedsCertificate => Self::NeedsCertificate,
            TorrentState::CheckingResumeData => Self::CheckingResumeData,
            TorrentState::FilesMissing => Self::FilesMissing,
        }
    }
}

/// Which hashes verify a torrent's pieces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentVersion")]
pub enum TorrentVersionGql {
    /// SHA-1 piece hashes.
    V1,
    /// SHA-256 merkle trees.
    V2,
    /// Both.
    Hybrid,
}

impl From<TorrentVersion> for TorrentVersionGql {
    fn from(version: TorrentVersion) -> Self {
        match version {
            TorrentVersion::V1 => Self::V1,
            TorrentVersion::V2 => Self::V2,
            TorrentVersion::Hybrid => Self::Hybrid,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
/// How announcing to a tracker went.
#[graphql(name = "TorrentTrackerStatus")]
pub enum TrackerStatusGql {
    /// Not announced to yet.
    NotContacted,
    /// Its last announce was answered.
    Working,
    /// Its last announce failed.
    NotWorking,
    /// It answered its last announce with an error.
    TrackerError,
    /// It could not be reached.
    Unreachable,
    /// It is not announced to.
    Disabled,
}

impl From<TrackerStatus> for TrackerStatusGql {
    fn from(status: TrackerStatus) -> Self {
        match status {
            TrackerStatus::NotContacted => Self::NotContacted,
            TrackerStatus::Working => Self::Working,
            TrackerStatus::NotWorking => Self::NotWorking,
            TrackerStatus::TrackerError => Self::TrackerError,
            TrackerStatus::Unreachable => Self::Unreachable,
            TrackerStatus::Disabled => Self::Disabled,
        }
    }
}

/// The level each of a torrent's seeding terms comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, SimpleObject)]
#[graphql(name = "TorrentSeedingLevels")]
pub struct SeedingLevelsGql {
    /// The level the ratio goal comes from.
    pub ratio_goal: SettingLevelGql,
    /// The level the seeding time goal comes from.
    pub seed_time_goal_minutes: SettingLevelGql,
    /// The level the inactive time goal comes from.
    pub inactive_goal_minutes: SettingLevelGql,
    /// The level the goal mode comes from.
    pub mode: SettingLevelGql,
    /// The level the action at the goal comes from.
    pub action: SettingLevelGql,
    /// The level the way seeding time is counted comes from.
    pub seed_time_counting: SettingLevelGql,
    /// The level the upload cap comes from.
    pub upload_cap: SettingLevelGql,
}

impl From<SeedingLevels> for SeedingLevelsGql {
    fn from(levels: SeedingLevels) -> Self {
        Self {
            ratio_goal: levels.ratio_goal.into(),
            seed_time_goal_minutes: levels.seed_time_goal_minutes.into(),
            inactive_goal_minutes: levels.inactive_goal_minutes.into(),
            mode: levels.mode.into(),
            action: levels.action.into(),
            seed_time_counting: levels.seed_time_counting.into(),
            upload_cap: levels.upload_cap.into(),
        }
    }
}

/// The seeding terms a torrent is held to, each resolved over its levels.
#[derive(Clone, Copy, Debug, PartialEq, SimpleObject)]
#[graphql(name = "TorrentSeedingTerms")]
pub struct SeedingTermsGql {
    /// The share ratio to reach; absent is none.
    pub ratio_goal: Option<f64>,
    /// The minutes to seed; absent is none.
    pub seed_time_goal_minutes: Option<u32>,
    /// The minutes without upload after which the goal is met; absent is
    /// none.
    pub inactive_goal_minutes: Option<u32>,
    /// Whether any goal or every goal must be met.
    pub mode: SeedingGoalModeGql,
    /// What is done once the goal is met.
    pub action: SeedingActionGql,
    /// How seeding time is counted.
    pub seed_time_counting: SeedTimeCountingGql,
    /// The upload cap in bytes per second; absent is none.
    pub upload_cap: Option<u64>,
    /// The level each term comes from.
    pub levels: SeedingLevelsGql,
}

impl SeedingTermsGql {
    fn of(seeding: Seeding, upload_cap: Option<u64>, levels: SeedingLevels) -> Self {
        Self {
            ratio_goal: seeding.ratio_goal,
            seed_time_goal_minutes: seeding.seed_time_goal_minutes,
            inactive_goal_minutes: seeding.inactive_goal_minutes,
            mode: seeding.mode.into(),
            action: seeding.action.into(),
            seed_time_counting: seeding.seed_time_counting.into(),
            upload_cap,
            levels: levels.into(),
        }
    }
}

/// The torrents the schedule's pause leaves seeding under one tracker rule.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentScheduleSeeding")]
pub struct ScheduleSeedingGql {
    /// The tracker rule's id.
    pub rule_id: u32,
    /// The rule's name.
    pub name: String,
    /// How many torrents the rule keeps seeding.
    pub torrents: u32,
}

impl From<ScheduleSeeding> for ScheduleSeedingGql {
    fn from(seeding: ScheduleSeeding) -> Self {
        Self {
            rule_id: seeding.rule_id,
            name: seeding.name,
            torrents: seeding.torrents,
        }
    }
}

/// What runs on a torrent route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentRouteScope")]
pub enum TorrentRouteScopeGql {
    /// The route torrents take when nothing else names one.
    Default,
    /// A category's route.
    Category,
    /// A tracker rule's route.
    TrackerRule,
}

/// A scope that runs on a torrent route: the default, or a category or
/// tracker rule by its id and name.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentRouteUse")]
pub struct TorrentRouteUseGql {
    /// What kind of scope it is.
    pub scope: TorrentRouteScopeGql,
    /// The category's or tracker rule's id; absent for the default.
    pub id: Option<u32>,
    /// The category's or tracker rule's name; absent for the default.
    pub name: Option<String>,
}

impl From<(TorrentScope, Option<String>)> for TorrentRouteUseGql {
    fn from((scope, name): (TorrentScope, Option<String>)) -> Self {
        let (scope, id) = match scope {
            TorrentScope::Default => (TorrentRouteScopeGql::Default, None),
            TorrentScope::Category(id) => (TorrentRouteScopeGql::Category, Some(id)),
            TorrentScope::TrackerRule(id) => (TorrentRouteScopeGql::TrackerRule, Some(id)),
        };
        Self { scope, id, name }
    }
}

/// What asked a gateway for a port mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentPortMapper")]
pub enum PortMapperGql {
    /// UPnP.
    Upnp,
    /// NAT-PMP.
    NatPmp,
    /// PCP from a global IPv6 address.
    PcpIpv6,
}

/// The protocol a port mapping forwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentPortMapProtocol")]
pub enum PortMapProtocolGql {
    /// TCP.
    Tcp,
    /// UDP.
    Udp,
}

/// A gateway's last word on one port mapping.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentPortMapping")]
pub struct TorrentPortMappingGql {
    /// What asked for the mapping.
    pub mapper: PortMapperGql,
    /// The protocol it forwards.
    pub protocol: PortMapProtocolGql,
    /// The port it forwards to.
    pub local_port: u16,
    /// The port the gateway forwards; none once the mapping failed or was
    /// removed.
    pub external_port: Option<u16>,
    /// The gateway's external address, when it told a public one. A gateway
    /// inside a tunnel may tell the tunnel's own private address: not shown.
    pub gateway_address: Option<String>,
    /// The gateway's error, when the mapping failed.
    pub error: Option<String>,
}

impl From<PortMapped> for TorrentPortMappingGql {
    fn from(mapped: PortMapped) -> Self {
        Self {
            mapper: match mapped.transport {
                PortmapTransport::Upnp => PortMapperGql::Upnp,
                PortmapTransport::NatPmp => PortMapperGql::NatPmp,
                PortmapTransport::PcpIpv6 => PortMapperGql::PcpIpv6,
            },
            protocol: match mapped.protocol {
                PortmapProtocol::Tcp => PortMapProtocolGql::Tcp,
                PortmapProtocol::Udp => PortMapProtocolGql::Udp,
            },
            local_port: mapped.local_port,
            external_port: mapped.external_port,
            gateway_address: mapped.public_ip().map(|ip| ip.to_string()),
            error: mapped.error,
        }
    }
}

/// What a torrent session shows the trackers and peers it reaches.
#[derive(Clone, Debug, PartialEq, Eq, SimpleObject)]
#[graphql(name = "TorrentExposure")]
pub struct TorrentExposureGql {
    /// Who resolves the names the session looks up.
    pub resolved_by: Option<ResolvedBy>,
    /// The address a tracker sees; none until one is known.
    pub address: Option<String>,
    /// The port announced; null when none is.
    pub announced_port: Option<u16>,
    /// Whether peers can connect in.
    pub inbound: bool,
    /// Whether UDP trackers are announced to.
    pub udp_trackers: bool,
    /// Whether the DHT runs.
    pub dht: bool,
    /// Whether uTP runs.
    pub utp: bool,
}

impl From<Exposure> for TorrentExposureGql {
    fn from(exposure: Exposure) -> Self {
        Self {
            resolved_by: exposure.resolved_by.map(Into::into),
            address: exposure.address.map(|ip| ip.to_string()),
            announced_port: (exposure.announced_port != 0).then_some(exposure.announced_port),
            inbound: exposure.inbound,
            udp_trackers: exposure.udp_trackers,
            dht: exposure.dht,
            utp: exposure.utp,
        }
    }
}

/// A torrent route and whether peers can reach its session.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentRoute")]
pub struct TorrentRouteGql {
    /// The route's key.
    pub key: String,
    /// The scopes that run on it.
    pub scopes: Vec<TorrentRouteUseGql>,
    /// Whether its session is open.
    pub open: bool,
    /// The address its session listens on, TCP's, and uTP's when `utp`.
    pub listen_address: Option<String>,
    /// Whether its session runs uTP.
    pub utp: bool,
    /// Its session's port mappings.
    pub mappings: Vec<TorrentPortMappingGql>,
    /// Its address as the internet sees it.
    pub external_addresses: Vec<String>,
    /// Why its session is closed: held closed, its leg down, or not opened
    /// as nothing has asked it to open. None while open.
    pub reason: Option<TorrentRouteReasonGql>,
    /// The cause behind `reason`, as the server logged it, when there is one.
    pub detail: Option<String>,
    /// Who resolves the names its sessions look up.
    pub resolved_by: Option<ResolvedBy>,
    /// What its session shows trackers and peers, while open.
    pub exposure: Option<TorrentExposureGql>,
}

/// Why a torrent is in its state, as the server states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentStateReason")]
pub enum TorrentStateReasonGql {
    /// Kept from starting for room on its disk, as a usenet job waits.
    DiskSpace,
    /// Kept from starting by a disk that never holds what it has left to
    /// fetch, as a usenet job is.
    DiskTooSmall,
    /// A write found no room: it fetches nothing and keeps seeding.
    DiskFull,
    /// Stopped once its seeding goal was met.
    GoalMet,
    /// Held in the queue by the operator's pause.
    HeldManual,
    /// Held in the queue by the schedule.
    HeldScheduled,
    /// Held in the queue while a speed test runs.
    HeldSpeedTest,
    /// Fetching its metadata from its trackers.
    FetchTrackers,
    /// Fetching its metadata from its trackers, the DHT and local discovery.
    FetchDiscovery,
    /// Fetching its metadata, with no peers named that may serve it.
    FetchWaiting,
}

impl From<Stated> for TorrentStateReasonGql {
    fn from(stated: Stated) -> Self {
        match stated {
            Stated::DiskSpace => Self::DiskSpace,
            Stated::DiskTooSmall => Self::DiskTooSmall,
            Stated::DiskFull => Self::DiskFull,
            Stated::GoalMet => Self::GoalMet,
            Stated::Held(HoldOrigin::Manual) => Self::HeldManual,
            Stated::Held(HoldOrigin::Scheduled) => Self::HeldScheduled,
            Stated::Held(HoldOrigin::SpeedTest) => Self::HeldSpeedTest,
            Stated::Fetching(FetchPhase::Trackers) => Self::FetchTrackers,
            Stated::Fetching(FetchPhase::Discovery) => Self::FetchDiscovery,
            Stated::Fetching(FetchPhase::Waiting) => Self::FetchWaiting,
        }
    }
}

/// Why a torrent route's session is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentRouteReason")]
pub enum TorrentRouteReasonGql {
    /// Nothing has asked its session to open.
    NotAskedToOpen,
    /// Its leg is down.
    LegDown,
    /// Its leg is not configured.
    LegMissing,
    /// Its leg was taken away.
    LegRevoked,
    /// Its egress quota is spent.
    QuotaSilenced,
    /// Torrents are turned off.
    TorrentsDisabled,
    /// Its route's legs cannot carry it.
    TorrentRouteLegs,
    /// Its route falls back to the host address, which it may not.
    TorrentRouteDirectFallback,
    /// Its route's rungs cannot carry it.
    TorrentRouteRungs,
    /// Its leg failed to open.
    LegOpenFailed,
    /// Its session's state directory could not be made.
    StateDirectoryFailed,
    /// Its listen port is not free.
    ListenPortUnavailable,
    /// Its listen port could not be bound.
    ListenPortBindFailed,
    /// Its session's saved state could not be read.
    SessionStateUnreadable,
    /// Its session could not be built.
    SessionBuildFailed,
    /// uTP could not start on it.
    UtpUnavailable,
    /// Its blocklist could not be loaded.
    BlocklistFailed,
    /// The most sessions are open.
    SessionLimit,
    /// Its session is opening.
    Opening,
}

impl From<TorrentRouteReason> for TorrentRouteReasonGql {
    fn from(reason: TorrentRouteReason) -> Self {
        match reason {
            TorrentRouteReason::NotAskedToOpen => Self::NotAskedToOpen,
            TorrentRouteReason::LegDown => Self::LegDown,
            TorrentRouteReason::LegMissing => Self::LegMissing,
            TorrentRouteReason::LegRevoked => Self::LegRevoked,
            TorrentRouteReason::QuotaSilenced => Self::QuotaSilenced,
            TorrentRouteReason::TorrentsDisabled => Self::TorrentsDisabled,
            TorrentRouteReason::RouteBroken(fault) => match fault {
                TorrentRouteFault::Legs => Self::TorrentRouteLegs,
                TorrentRouteFault::DirectFallback => Self::TorrentRouteDirectFallback,
                TorrentRouteFault::Rungs => Self::TorrentRouteRungs,
            },
            TorrentRouteReason::LegOpenFailed => Self::LegOpenFailed,
            TorrentRouteReason::StateDirectoryFailed => Self::StateDirectoryFailed,
            TorrentRouteReason::ListenPortUnavailable => Self::ListenPortUnavailable,
            TorrentRouteReason::ListenPortBindFailed => Self::ListenPortBindFailed,
            TorrentRouteReason::SessionStateUnreadable => Self::SessionStateUnreadable,
            TorrentRouteReason::SessionBuildFailed => Self::SessionBuildFailed,
            TorrentRouteReason::UtpUnavailable => Self::UtpUnavailable,
            TorrentRouteReason::BlocklistFailed => Self::BlocklistFailed,
            TorrentRouteReason::SessionLimit => Self::SessionLimit,
            TorrentRouteReason::Opening => Self::Opening,
        }
    }
}

impl From<TorrentRoute> for TorrentRouteGql {
    fn from(route: TorrentRoute) -> Self {
        let reason = route.down.or_else(|| match &route.session {
            Some(session) => session.held.clone(),
            None => Some(TorrentRouteReason::NotAskedToOpen.into()),
        });
        // A previous session can outlive the route becoming unavailable.
        let session = route.session.filter(|_| reason.is_none());
        let detail = reason
            .as_ref()
            .and_then(|failure| failure.detail.clone())
            .filter(|detail| !detail.is_empty());
        let reason = reason.map(|failure| failure.reason.into());
        Self {
            key: route.key.as_str().to_owned(),
            scopes: route.scopes.into_iter().map(Into::into).collect(),
            open: session.is_some(),
            listen_address: session
                .as_ref()
                .and_then(|session| session.listen)
                .map(|addr| addr.to_string()),
            utp: session.as_ref().is_some_and(|session| session.utp),
            mappings: session
                .as_ref()
                .map(|session| session.mappings.iter().cloned().map(Into::into).collect())
                .unwrap_or_default(),
            exposure: session
                .as_ref()
                .and_then(|session| session.exposure)
                .map(Into::into),
            external_addresses: session
                .map(|session| session.external.iter().map(ToString::to_string).collect())
                .unwrap_or_default(),
            reason,
            detail,
            resolved_by: route.names.map(Into::into),
        }
    }
}

/// How eagerly a torrent file is fetched: qBittorrent's four levels. An
/// ignored file is not fetched; padding is always ignored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentFilePriority")]
pub enum TorrentFilePriorityGql {
    /// Not fetched.
    Ignored,
    /// Fetched at the normal level.
    Normal,
    /// Fetched before normal files.
    High,
    /// Fetched before every other file.
    Maximum,
}

impl From<FilePriority> for TorrentFilePriorityGql {
    fn from(priority: FilePriority) -> Self {
        match priority {
            FilePriority::Ignored => Self::Ignored,
            FilePriority::Normal => Self::Normal,
            FilePriority::High => Self::High,
            FilePriority::Maximum => Self::Maximum,
        }
    }
}

impl From<TorrentFilePriorityGql> for FilePriority {
    fn from(priority: TorrentFilePriorityGql) -> Self {
        match priority {
            TorrentFilePriorityGql::Ignored => Self::Ignored,
            TorrentFilePriorityGql::Normal => Self::Normal,
            TorrentFilePriorityGql::High => Self::High,
            TorrentFilePriorityGql::Maximum => Self::Maximum,
        }
    }
}

/// A file's level, by its index in the torrent.
#[derive(Debug, InputObject)]
#[graphql(name = "TorrentFilePriorityInput")]
pub struct TorrentFilePriorityInputGql {
    /// The file's index in the torrent.
    pub index: u32,
    /// The level to give it.
    pub priority: TorrentFilePriorityGql,
}

/// One file of a torrent.
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "TorrentFile")]
pub struct TorrentFileGql {
    /// Its index in the torrent.
    pub index: u32,
    /// Its path inside the torrent.
    pub path: String,
    /// Its length in bytes.
    pub length: u64,
    /// Whether the file is fetched at all: its level is above ignored.
    pub selected: bool,
    /// Its level.
    pub priority: TorrentFilePriorityGql,
    /// The share of the file in hand, from 0 to 1.
    pub progress: f64,
}

/// One tracker of a torrent, with how announcing to it went.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentTracker", complex)]
pub struct TorrentTrackerGql {
    /// Its tier: trackers of a lower tier are tried first.
    pub tier: u32,
    #[graphql(skip)]
    pub url: String,
    /// Who named it.
    pub source: TrackerSourceGql,
    /// How announcing to it went.
    pub status: TrackerStatusGql,
    /// An announce to it is in flight.
    pub updating: bool,
    /// What it said in its last answer, when it said anything.
    pub message: Option<String>,
    /// The seeds it last counted.
    pub seeders: Option<u32>,
    /// The leechers it last counted.
    pub leechers: Option<u32>,
    /// The completed downloads it last counted.
    pub completed_downloads: Option<u32>,
    /// The peers its last answer named.
    pub announced_peers: Option<u32>,
    /// Announces to it that failed in a row.
    pub fails: u32,
    /// When it was last announced to.
    pub last_announce_at: Option<DateTime<Utc>>,
    /// When it is next announced to.
    pub next_announce_at: Option<DateTime<Utc>>,
    /// The earliest the tracker takes the next announce.
    pub min_announce_at: Option<DateTime<Utc>>,
    /// Who resolves its host; none for an address.
    pub resolved_by: Option<ResolvedBy>,
}

impl From<TorrentTracker> for TorrentTrackerGql {
    fn from(tracker: TorrentTracker) -> Self {
        Self {
            tier: tracker.tier,
            url: tracker.url,
            source: tracker.named_by.into(),
            status: tracker.status.into(),
            updating: tracker.updating,
            message: tracker.message,
            seeders: tracker.seeders,
            leechers: tracker.leechers,
            completed_downloads: tracker.downloaded,
            announced_peers: tracker.peers,
            fails: tracker.fails,
            last_announce_at: tracker.last_announce_ms.map(ms_to_datetime),
            next_announce_at: tracker.next_announce_ms.map(ms_to_datetime),
            min_announce_at: tracker.min_announce_ms.map(ms_to_datetime),
            resolved_by: tracker.resolved_by.map(Into::into),
        }
    }
}

#[ComplexObject]
impl TorrentTrackerGql {
    /// The tracker's whole URL to a caller that may change trackers;
    /// its scheme, host and port to one that only reads.
    async fn url(&self, ctx: &Context<'_>) -> Result<String> {
        Ok(if ctx.data::<CallerScope>()?.can_control() {
            self.url.clone()
        } else {
            redacted_tracker_url(&self.url)
        })
    }
}

/// Who named a tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentTrackerSource")]
pub enum TrackerSourceGql {
    /// The torrent's metainfo or magnet link.
    Torrent,
    /// The operator.
    Operator,
}

impl From<NamedBy> for TrackerSourceGql {
    fn from(named_by: NamedBy) -> Self {
        match named_by {
            NamedBy::Torrent => Self::Torrent,
            NamedBy::Operator => Self::Operator,
        }
    }
}

/// A tracker in a torrent's list, by tier.
#[derive(Clone, Debug, InputObject)]
#[graphql(name = "TorrentTrackerInput")]
pub struct TorrentTrackerInput {
    /// Its tier: trackers of a lower tier are tried first.
    pub tier: u32,
    /// Its announce URL.
    pub url: String,
}

/// How a torrent's unpacking came out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentUnpackResult")]
pub enum TorrentUnpackResultGql {
    /// Everything it would deliver is beside the torrent's files.
    Whole,
    /// Some of it is; the reason says what is short.
    Part,
    /// Nothing usable was delivered.
    Failed,
    /// The unpacking refused the torrent's files.
    Refused,
}

impl From<UnpackResult> for TorrentUnpackResultGql {
    fn from(result: UnpackResult) -> Self {
        match result {
            UnpackResult::Whole => Self::Whole,
            UnpackResult::Part => Self::Part,
            UnpackResult::Failed => Self::Failed,
            UnpackResult::Refused => Self::Refused,
        }
    }
}

/// The unpacked files a page holds when `first` is not given.
pub const DEFAULT_UNPACKED_FILE_PAGE: usize = 100;
/// The most unpacked files a page holds.
pub const MAX_UNPACKED_FILE_PAGE: usize = 1000;
/// The files a torrent's file page holds when `first` is not given.
pub const DEFAULT_FILE_PAGE: usize = 200;
/// The most files a torrent's file page holds.
pub const MAX_FILE_PAGE: usize = 1000;

/// A torrent's unpacking, once over. It is the unpacking's own: the
/// torrent's state, history and counters never read it.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentUnpacking", complex)]
pub struct TorrentUnpackingGql {
    #[graphql(skip)]
    pub id: u64,
    /// How it came out.
    pub result: TorrentUnpackResultGql,
    /// Why it is short of whole.
    pub reason: Option<String>,
}

#[ComplexObject]
impl TorrentUnpackingGql {
    /// One page of the files it placed beside the torrent's own, in the
    /// order it placed them: the `first` after the offset cursor `after`,
    /// 100 when not given and at most 1000. Read on `torrent(id)` only; a
    /// file no longer on disk is left out of its page.
    async fn files(
        &self,
        ctx: &Context<'_>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<TorrentUnpackedFilePageGql> {
        let (offset, limit) = page_args(
            first,
            after.as_deref(),
            DEFAULT_UNPACKED_FILE_PAGE,
            MAX_UNPACKED_FILE_PAGE,
        )?;
        let (files, more) = super::service(ctx)?
            .unpacked_files_page(JobId(self.id), offset, limit)
            .await
            .map_err(super::mutation::action_error)?;
        Ok(TorrentUnpackedFilePageGql {
            items: files.into_iter().map(Into::into).collect(),
            next_cursor: (limit > 0 && more).then(|| encode_offset_cursor(offset + limit)),
        })
    }
}

/// One page of the files a torrent's unpacking placed.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentUnpackedFilePage")]
pub struct TorrentUnpackedFilePageGql {
    /// The page's files.
    pub items: Vec<TorrentUnpackedFileGql>,
    /// The `after` that asks for the next page; null on the last page.
    pub next_cursor: Option<String>,
}

/// A file a torrent's unpacking placed beside the torrent's own: no piece
/// of the torrent, so it has no index, level or progress of its own.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentUnpackedFile")]
pub struct TorrentUnpackedFileGql {
    /// Its path beside the torrent's files.
    pub path: String,
    /// Its length in bytes.
    pub length: u64,
}

impl From<UnpackedFile> for TorrentUnpackedFileGql {
    fn from(file: UnpackedFile) -> Self {
        Self {
            path: file.name,
            length: file.length,
        }
    }
}

/// One page of a torrent's files, in file-index order.
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "TorrentFilePage")]
pub struct TorrentFilePageGql {
    /// The page's files.
    pub items: Vec<TorrentFileGql>,
    /// How many files the torrent has.
    pub total_count: u64,
    /// The `after` that asks for the next page; null on the last page.
    pub next_cursor: Option<String>,
}

impl TorrentFilePageGql {
    /// The page of `page`'s files after `offset`, at most `limit` of them.
    pub fn of(page: FilesPage, offset: usize, limit: usize) -> Self {
        let items = page
            .files
            .into_iter()
            .map(|(file, progress)| TorrentFileGql {
                index: file.index,
                selected: file.selected(),
                path: file.path,
                length: file.length,
                priority: file.priority.into(),
                progress,
            })
            .collect();
        Self {
            items,
            total_count: page.total as u64,
            next_cursor: (limit > 0 && offset.saturating_add(limit) < page.total)
                .then(|| encode_offset_cursor(offset + limit)),
        }
    }
}

// A page's offset and size from its arguments: `first` defaults to
// `default` and is at most `max`.
pub(crate) fn page_args(
    first: Option<u32>,
    after: Option<&str>,
    default: usize,
    max: usize,
) -> Result<(usize, usize)> {
    let offset =
        decode_offset_cursor(after).map_err(|message| graphql_error("CURSOR_INVALID", message))?;
    Ok((offset, first.map_or(default, |first| first as usize).min(max)))
}

/// One page of `torrents`.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentPage")]
pub struct TorrentPageGql {
    /// The page's torrents.
    pub items: Vec<TorrentGql>,
    /// The `after` that asks for the next page; null on the last page.
    pub next_cursor: Option<String>,
}

/// A torrent.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "Torrent", complex)]
pub struct TorrentGql {
    /// Its job's id.
    pub id: u64,
    /// Its name.
    pub name: String,
    /// Its v1 hash, hex; null when it has none.
    pub infohash_v1: Option<String>,
    /// Its v2 hash, hex; null when it has none.
    pub infohash_v2: Option<String>,
    /// Which hashes verify its pieces, once its metainfo is in hand.
    pub version: Option<TorrentVersionGql>,
    /// What it is doing.
    pub state: TorrentStateGql,
    /// Why the torrent is in its state, when the server states the reason
    /// itself.
    pub state_reason: Option<TorrentStateReasonGql>,
    /// Why the torrent is in its state, in free words (an error, an
    /// unpacking refusal), when that is the reason. While its route holds
    /// it, the cause behind `routeReason`, when one is known.
    pub state_detail: Option<String>,
    /// Why its route holds it, while its state comes from that hold.
    pub route_reason: Option<TorrentRouteReasonGql>,
    /// Its category's name, when it has one.
    pub category: Option<String>,
    /// Its job's tags, in tag order.
    pub tags: Vec<String>,
    /// Whether it is private: it uses its trackers only.
    pub private: bool,
    /// Whether it runs outside the queue's limits.
    pub forced: bool,
    /// The share of its wanted bytes in hand, from 0 to 1.
    pub progress: f64,
    /// The bytes it wants.
    pub total_bytes: u64,
    /// The wanted bytes in hand, verified.
    pub have_bytes: u64,
    /// Bytes uploaded since it was added.
    pub uploaded_bytes: u64,
    /// Bytes downloaded since it was added.
    pub downloaded_bytes: u64,
    /// Its share ratio: uploaded over downloaded.
    pub ratio: f64,
    /// Its download rate in bytes per second.
    pub download_bps: u64,
    /// Its upload rate in bytes per second.
    pub upload_bps: u64,
    /// The torrent's own upload limit in bytes per second; null is
    /// unlimited and 0 closed.
    pub upload_limit_bps: Option<u64>,
    /// The torrent's own download limit in bytes per second; null is
    /// unlimited and 0 closed.
    pub download_limit_bps: Option<u64>,
    /// Seconds until it is complete, when that can be told.
    pub eta_seconds: Option<u64>,
    /// The peers it is connected to.
    pub connected_peers: u32,
    /// The seeds among its connected peers.
    pub connected_seeds: u32,
    /// The leechers among its connected peers.
    pub connected_leechers: u32,
    /// The most peers a torrent connects to.
    pub max_peers: u32,
    /// The swarm's seeds as its trackers last counted them.
    pub swarm_seeds: Option<u32>,
    /// The swarm's leechers as its trackers last counted them.
    pub swarm_leechers: Option<u32>,
    /// Seconds until the next tracker announce.
    pub reannounce_seconds: Option<u64>,
    /// Seconds spent running in a session.
    pub active_seconds: u64,
    /// Seconds spent complete and seeding, in all.
    pub seeding_seconds: u64,
    /// Seconds of the current unbroken seeding run.
    pub consecutive_seeding_seconds: u64,
    /// Its average download rate in bytes per second while running.
    pub download_avg_bps: u64,
    /// Its average upload rate in bytes per second while running.
    pub upload_avg_bps: u64,
    /// Bytes received and thrown away this run: pieces that failed their
    /// check, and blocks that arrived when already in hand. Counts from zero
    /// each time the torrent starts.
    pub wasted_bytes: u64,
    /// The last time a complete copy was in reach.
    pub last_seen_complete_at: Option<DateTime<Utc>>,
    /// How many pieces it has, once its metainfo is in hand.
    pub piece_count: Option<u32>,
    /// Its piece size in bytes, once its metainfo is in hand.
    pub piece_size: Option<u64>,
    /// The pieces in hand, verified.
    pub pieces_have: Option<u32>,
    /// The program its metainfo says made it.
    pub created_by: Option<String>,
    /// When its metainfo says it was made.
    pub metainfo_created_at: Option<DateTime<Utc>>,
    /// Its metainfo's comment.
    pub comment: Option<String>,
    /// When it was added.
    pub added_at: DateTime<Utc>,
    /// When it completed.
    pub completed_at: Option<DateTime<Utc>>,
    /// The folder its files are written in.
    pub storage_dir: Option<String>,
    /// The folder that holds the torrent's output.
    pub save_path: Option<String>,
    #[graphql(skip)]
    pub seeding_left: Option<String>,
    /// The torrent's own seeding terms; absent when it inherits every one.
    pub seeding_override: Option<SeedingPolicySettings>,
    /// The seeding terms in effect.
    pub seeding_policy: SeedingTermsGql,
    /// The host pattern of the tracker rule the torrent is under, when it is.
    pub tracker_rule: Option<String>,
    /// Whether its archives are extracted.
    pub unpack: bool,
    /// The torrent's own unpack; absent inherits.
    pub unpack_override: Option<bool>,
    /// The networking route it takes, by its key, when one is resolved.
    pub route: Option<String>,
    /// The networking route the torrent takes, by its consumer key:
    /// `torrent:default`, `torrent:category:<id>` or `torrent:tracker:<id>`.
    pub route_consumer: String,
    /// Whether its pieces are fetched in order.
    pub sequential: bool,
    /// Whether each file's first and last pieces are fetched first.
    pub first_last_piece_first: bool,
    /// Its trackers, each with how announcing to it went: read only when
    /// selected.
    pub trackers: Vec<TorrentTrackerGql>,
    #[graphql(skip)]
    pub unpack_end: Option<UnpackEnd>,
}

#[ComplexObject]
impl TorrentGql {
    /// The seeding goal still to meet; absent once it is met and for a
    /// torrent with no goal.
    async fn seeding_goal_left(&self) -> Option<&str> {
        let terms = &self.seeding_policy;
        let has_goal = terms.ratio_goal.is_some()
            || terms.seed_time_goal_minutes.is_some()
            || terms.inactive_goal_minutes.is_some();
        self.seeding_left.as_deref().filter(|_| has_goal)
    }

    /// How its unpacking came out, once over; absent while it runs, and for
    /// a torrent with nothing to unpack.
    async fn unpacking(&self) -> Option<TorrentUnpackingGql> {
        self.unpack_end.as_ref().map(|end| TorrentUnpackingGql {
            id: self.id,
            result: end.result.into(),
            reason: end.why.clone(),
        })
    }

    /// One page of its files in file-index order: the `first` after the
    /// offset cursor `after`, 200 when not given and at most 1000. Read on
    /// `torrent(id)` and `torrentUpdates(id)` only, each time from the
    /// torrent as it is then. A push of `torrentUpdates` comes when the
    /// torrent moves, not when its files alone do: `torrentFileUpdates`
    /// follows a page of the files.
    async fn files(
        &self,
        ctx: &Context<'_>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<TorrentFilePageGql> {
        let (offset, limit) = page_args(first, after.as_deref(), DEFAULT_FILE_PAGE, MAX_FILE_PAGE)?;
        let page = super::service(ctx)?
            .torrent_files(JobId(self.id), offset, limit)
            .await
            .unwrap_or(FilesPage {
                files: Vec::new(),
                total: 0,
            });
        Ok(TorrentFilePageGql::of(page, offset, limit))
    }

    /// Its live peer connections, by address; empty while it is in no
    /// session. Read on `torrent(id)` only.
    async fn peer_connections(&self, ctx: &Context<'_>) -> Result<Vec<TorrentPeerGql>> {
        Ok(super::service(ctx)?
            .peers(JobId(self.id))
            .await
            .map_err(super::mutation::action_error)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

/// What a `torrentRowUpdates` push carries.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentChanges")]
pub struct TorrentChangesGql {
    /// The first push: `torrents` is every followed torrent.
    pub full_update: bool,
    /// The torrents that changed or came.
    pub torrents: Vec<TorrentGql>,
    /// The ids of the torrents gone since the last push.
    pub removed_ids: Vec<u64>,
}

impl From<TorrentChanges> for TorrentChangesGql {
    fn from(changes: TorrentChanges) -> Self {
        Self {
            full_update: changes.full_update,
            torrents: changes
                .torrents
                .into_iter()
                .zip(changes.one_file)
                .map(|(view, one_file)| TorrentGql::of(view, one_file))
                .collect(),
            removed_ids: changes.removed.into_iter().map(|job_id| job_id.0).collect(),
        }
    }
}

impl From<TorrentView> for TorrentGql {
    fn from(view: TorrentView) -> Self {
        let one_file = view.is_one_file();
        Self::of(view, one_file)
    }
}

impl TorrentGql {
    /// `view`, its save path read as one file named as the torrent is when
    /// `one_file`: for a view read without its files. Its files are not
    /// held: `files` reads its page.
    pub(crate) fn of(view: TorrentView, one_file: bool) -> Self {
        let (save_path, _) = view.paths_as(one_file);
        Self {
            id: view.job_id.0,
            name: view.name,
            infohash_v1: view.infohash_v1.clone().filter(|hash| !hash.is_empty()),
            infohash_v2: view.infohash_v2.clone(),
            version: view.version.map(Into::into),
            state: view.state.into(),
            state_reason: view.stated.map(Into::into),
            state_detail: view.detail,
            route_reason: view.route_reason.map(Into::into),
            category: view.category,
            tags: view.tags,
            private: view.private,
            forced: view.forced,
            progress: view.progress,
            total_bytes: view.total_bytes,
            have_bytes: view.have_bytes,
            uploaded_bytes: view.uploaded_bytes,
            downloaded_bytes: view.downloaded_bytes,
            ratio: view.ratio,
            download_bps: view.download_bps,
            upload_bps: view.upload_bps,
            upload_limit_bps: view.upload_limit,
            download_limit_bps: view.download_limit,
            eta_seconds: view.eta_seconds,
            connected_peers: view.peers,
            connected_seeds: view.seeds,
            connected_leechers: view.leechers,
            max_peers: view.max_peers,
            swarm_seeds: view.swarm_seeds,
            swarm_leechers: view.swarm_leechers,
            reannounce_seconds: view.reannounce_seconds,
            active_seconds: view.active_seconds,
            seeding_seconds: view.seeding_seconds,
            consecutive_seeding_seconds: view.consecutive_seeding_seconds,
            download_avg_bps: view.download_avg_bps,
            upload_avg_bps: view.upload_avg_bps,
            wasted_bytes: view.wasted_bytes,
            last_seen_complete_at: view.last_seen_complete_ms.map(ms_to_datetime),
            piece_count: view.pieces.map(|pieces| pieces.count),
            piece_size: view.pieces.map(|pieces| pieces.size),
            pieces_have: view.pieces.map(|pieces| pieces.have),
            created_by: view.created_by,
            metainfo_created_at: view.created_at_ms.map(ms_to_datetime),
            comment: view.comment,
            added_at: ms_to_datetime(view.added_at_ms),
            completed_at: view.completed_at_ms.map(ms_to_datetime),
            storage_dir: view.storage_dir,
            save_path: (!save_path.is_empty()).then_some(save_path),
            seeding_left: view.seeding_left,
            seeding_override: view.seeding_override.map(Into::into),
            seeding_policy: SeedingTermsGql::of(
                view.seeding_policy,
                view.upload_cap,
                view.seeding_levels,
            ),
            tracker_rule: view.tracker_rule,
            unpack: view.unpack,
            unpack_override: view.unpack_override,
            route: view.route,
            route_consumer: weaver_server_core::proxies::Consumer::Torrent(view.scope).key(),
            sequential: view.sequential,
            first_last_piece_first: view.first_last_piece_first,
            trackers: view.trackers.into_iter().map(Into::into).collect(),
            unpack_end: view.unpack_end,
        }
    }
}

/// The transport a peer connection runs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentPeerConnection")]
pub enum PeerConnectionGql {
    /// TCP.
    Tcp,
    /// uTP.
    Utp,
}

impl From<ConnectionKind> for PeerConnectionGql {
    fn from(kind: ConnectionKind) -> Self {
        match kind {
            ConnectionKind::Tcp => Self::Tcp,
            ConnectionKind::Utp => Self::Utp,
        }
    }
}

/// What of a peer connection is encrypted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentPeerEncryption")]
pub enum PeerEncryptionGql {
    /// Nothing.
    None,
    /// The handshake only.
    HeaderOnly,
    /// All of it.
    #[graphql(name = "RC4")]
    Rc4,
}

/// A source that named a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentPeerSource")]
pub enum PeerSourceGql {
    /// A tracker.
    Tracker,
    /// The DHT.
    Dht,
    /// Peer exchange.
    Pex,
    /// Local discovery.
    Lsd,
    /// The torrent's own list.
    ResumeData,
    /// The peer connected to us.
    Incoming,
}

impl From<PeerSourceFlag> for PeerSourceGql {
    fn from(source: PeerSourceFlag) -> Self {
        match source {
            PeerSourceFlag::Tracker => Self::Tracker,
            PeerSourceFlag::Dht => Self::Dht,
            PeerSourceFlag::Pex => Self::Pex,
            PeerSourceFlag::Lsd => Self::Lsd,
            PeerSourceFlag::ResumeData => Self::ResumeData,
            PeerSourceFlag::Incoming => Self::Incoming,
        }
    }
}

/// What a connection shows of a peer and of us.
#[derive(Clone, Copy, Debug, SimpleObject)]
#[graphql(name = "TorrentPeerFlags")]
pub struct TorrentPeerFlagsGql {
    /// We want pieces the peer has.
    pub interesting: bool,
    /// We choke the peer.
    pub choked: bool,
    /// The peer wants pieces we have.
    pub remote_interested: bool,
    /// The peer chokes us.
    pub remote_choked: bool,
    /// The peer has sent nothing asked of it for a while.
    pub snubbed: bool,
    /// The peer is unchoked by the optimistic turn.
    pub optimistic_unchoke: bool,
    /// The peer has every piece.
    pub seed: bool,
    /// The peer only uploads.
    pub upload_only: bool,
    /// The peer sent a piece that failed its check and is watched.
    pub on_parole: bool,
    /// The connection was made through a hole punch.
    pub holepunched: bool,
}

/// One connection to a peer of a torrent.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentPeer")]
pub struct TorrentPeerGql {
    /// The peer's address and port.
    pub address: String,
    /// The client the peer says it is.
    pub client: String,
    /// The peer id, hex.
    pub peer_id: String,
    /// The transport it runs over.
    pub connection: PeerConnectionGql,
    /// We made the connection.
    pub outgoing: bool,
    /// What of it is encrypted.
    pub encryption: PeerEncryptionGql,
    /// What it shows of the peer and of us.
    pub flags: TorrentPeerFlagsGql,
    /// Every source that named the peer.
    pub sources: Vec<PeerSourceGql>,
    /// How many of the torrent's pieces the peer has.
    pub pieces: u32,
    /// The share of the torrent the peer has, from 0 to 1.
    pub progress: f64,
    /// The rate it sends to us in bytes per second.
    pub download_bps: u64,
    /// The rate we send to it in bytes per second.
    pub upload_bps: u64,
    /// Bytes received over this connection.
    pub downloaded_bytes: u64,
    /// Bytes sent over this connection.
    pub uploaded_bytes: u64,
}

impl From<PeerInfo> for TorrentPeerGql {
    fn from(peer: PeerInfo) -> Self {
        let flags = peer.flags;
        Self {
            address: peer.addr.to_string(),
            client: peer.client,
            peer_id: peer.peer_id.as_string(),
            connection: peer.connection.into(),
            outgoing: peer.outgoing,
            encryption: match peer.encryption {
                Encryption::None => PeerEncryptionGql::None,
                Encryption::HeaderOnly => PeerEncryptionGql::HeaderOnly,
                Encryption::Rc4 => PeerEncryptionGql::Rc4,
            },
            flags: TorrentPeerFlagsGql {
                interesting: flags.interesting,
                choked: flags.choked,
                remote_interested: flags.remote_interested,
                remote_choked: flags.remote_choked,
                snubbed: flags.snubbed,
                optimistic_unchoke: flags.optimistic_unchoke,
                seed: flags.seed,
                upload_only: flags.upload_only,
                on_parole: flags.on_parole,
                holepunched: flags.holepunched,
            },
            sources: peer.sources.iter().map(PeerSourceGql::from).collect(),
            pieces: peer.pieces,
            progress: peer.progress,
            download_bps: peer.download_rate,
            upload_bps: peer.upload_rate,
            downloaded_bytes: peer.downloaded,
            uploaded_bytes: peer.uploaded,
        }
    }
}

/// Where a torrent comes from: exactly one of its metainfo, base64 or as
/// an upload, or a magnet link.
#[derive(Debug, OneofObject)]
#[graphql(name = "TorrentSourceInput")]
pub enum TorrentSourceInput {
    /// Its metainfo file, base64.
    MetainfoBase64(String),
    /// Its metainfo file, uploaded.
    MetainfoUpload(Upload),
    /// A magnet link.
    Magnet(String),
}

/// A torrent to add.
#[derive(InputObject)]
#[graphql(name = "SubmitTorrentInput")]
pub struct SubmitTorrentInputGql {
    /// Where it comes from.
    pub source: TorrentSourceInput,
    /// Its category's name; absent is none.
    pub category: Option<String>,
    /// Whether it is added paused.
    #[graphql(default)]
    pub paused: bool,
    /// Whether it runs outside the queue's limits.
    #[graphql(default)]
    pub forced: bool,
    /// Its own seeding terms; absent inherits.
    pub seeding: Option<SeedingPolicyInput>,
    /// Whether its archives are extracted; absent inherits.
    pub unpack: Option<bool>,
    /// The tags its job is added with.
    #[graphql(default)]
    pub tags: Vec<String>,
    /// Its own upload limit in bytes per second; absent is no limit and 0
    /// lets nothing through.
    pub upload_limit_bps: Option<u64>,
    /// Its own download limit in bytes per second; absent is no limit and 0
    /// lets nothing through.
    pub download_limit_bps: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // A torrent held for its files missing reads as FILES_MISSING.
    #[test]
    fn a_torrent_missing_its_files_reads_as_files_missing() {
        let state = TorrentStateGql::from(TorrentState::FilesMissing);
        assert_eq!(state, TorrentStateGql::FilesMissing);
        assert_eq!(
            async_graphql::InputType::to_value(&state),
            async_graphql::Value::Enum(async_graphql::Name::new("FILES_MISSING"))
        );
    }

    // Each connection kind reads in GraphQL as its key in the kinds' name
    // table, upper case: TCP, UTP.
    #[test]
    fn a_connection_kind_reads_as_its_key_upper_case() {
        for &kind in ConnectionKind::ALL {
            assert_eq!(
                async_graphql::InputType::to_value(&PeerConnectionGql::from(kind)),
                async_graphql::Value::Enum(async_graphql::Name::new(
                    kind.names().key.to_ascii_uppercase()
                )),
                "{kind:?}"
            );
        }
    }

    // Each reason the server states reads in GraphQL as its own name, one
    // per reason.
    #[test]
    fn a_stated_reason_reads_as_its_name() {
        let names = Stated::ALL.map(|stated| {
            match async_graphql::InputType::to_value(&TorrentStateReasonGql::from(stated)) {
                async_graphql::Value::Enum(name) => name.to_string(),
                other => panic!("{other:?}"),
            }
        });
        assert_eq!(
            names,
            [
                "DISK_SPACE",
                "DISK_TOO_SMALL",
                "DISK_FULL",
                "GOAL_MET",
                "HELD_MANUAL",
                "HELD_SCHEDULED",
                "HELD_SPEED_TEST",
                "FETCH_TRACKERS",
                "FETCH_DISCOVERY",
                "FETCH_WAITING",
            ]
        );
    }
}
