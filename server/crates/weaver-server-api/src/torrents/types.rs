// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Enum, InputObject, SimpleObject};
use weaver_server_core::torrents::service::{ScheduleSeeding, SeedingSource, TorrentView};
use weaver_server_core::torrents::{
    ConnectionKind, Encryption, FilePriority, PeerInfo, PeerSourceFlag, TorrentState,
    TorrentTracker, TorrentVersion, TrackerStatus,
};

use crate::settings::types::{SeedingPolicyInput, SeedingPolicySettings};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentState")]
pub enum TorrentStateGql {
    Queued,
    FetchingMetadata,
    CheckingFiles,
    Downloading,
    Stalled,
    Seeding,
    Paused,
    Stopped,
    Moving,
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
        }
    }
}

/// Which hashes verify a torrent's pieces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentVersion")]
pub enum TorrentVersionGql {
    V1,
    V2,
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
#[graphql(name = "TrackerStatus")]
pub enum TrackerStatusGql {
    NotContacted,
    Updating,
    Working,
    NotWorking,
    Disabled,
}

impl From<TrackerStatus> for TrackerStatusGql {
    fn from(status: TrackerStatus) -> Self {
        match status {
            TrackerStatus::NotContacted => Self::NotContacted,
            TrackerStatus::Updating => Self::Updating,
            TrackerStatus::Working => Self::Working,
            TrackerStatus::NotWorking => Self::NotWorking,
            TrackerStatus::Disabled => Self::Disabled,
        }
    }
}

/// Where a torrent's seeding policy comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "SeedingSource")]
pub enum SeedingSourceGql {
    Category,
    Global,
    Torrent,
    TrackerRule,
}

impl From<&SeedingSource> for SeedingSourceGql {
    fn from(source: &SeedingSource) -> Self {
        match source {
            SeedingSource::Category => Self::Category,
            SeedingSource::Global => Self::Global,
            SeedingSource::Torrent => Self::Torrent,
            SeedingSource::TrackerRule(_) => Self::TrackerRule,
        }
    }
}

/// The torrents the schedule's pause leaves seeding under one tracker rule.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "ScheduleSeeding")]
pub struct ScheduleSeedingGql {
    pub host_pattern: String,
    pub torrents: u32,
}

impl From<ScheduleSeeding> for ScheduleSeedingGql {
    fn from(seeding: ScheduleSeeding) -> Self {
        Self {
            host_pattern: seeding.host_pattern,
            torrents: seeding.torrents,
        }
    }
}

/// How eagerly a torrent file is fetched: qBittorrent's four levels. An
/// ignored file is not fetched; padding is always ignored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "TorrentFilePriority")]
pub enum TorrentFilePriorityGql {
    Ignored,
    Normal,
    High,
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
pub struct TorrentFilePriorityInput {
    pub index: u32,
    pub priority: TorrentFilePriorityGql,
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentFile")]
pub struct TorrentFileGql {
    pub index: u32,
    pub path: String,
    pub length: u64,
    /// Whether the file is fetched at all: its level is above ignored.
    pub selected: bool,
    pub priority: TorrentFilePriorityGql,
    /// The share of the file in hand, from 0 to 1.
    pub progress: f64,
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentTracker")]
pub struct TorrentTrackerGql {
    pub tier: u32,
    pub url: String,
    pub status: TrackerStatusGql,
    pub message: Option<String>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub last_announce_ms: Option<i64>,
    pub next_announce_ms: Option<i64>,
}

impl From<TorrentTracker> for TorrentTrackerGql {
    fn from(tracker: TorrentTracker) -> Self {
        Self {
            tier: tracker.tier,
            url: tracker.url,
            status: tracker.status.into(),
            message: tracker.message,
            seeders: tracker.seeders,
            leechers: tracker.leechers,
            last_announce_ms: tracker.last_announce_ms,
            next_announce_ms: tracker.next_announce_ms,
        }
    }
}

#[derive(Clone, Debug, SimpleObject)]
pub struct Torrent {
    pub id: u64,
    pub name: String,
    /// Its v1 hash; empty when it has none.
    pub infohash: String,
    pub infohash_v2: Option<String>,
    /// Which hashes verify its pieces, once its metainfo is in hand.
    pub version: Option<TorrentVersionGql>,
    pub state: TorrentStateGql,
    /// Why the torrent is in its state, when there is a reason to give.
    pub detail: Option<String>,
    pub category: Option<String>,
    pub private: bool,
    pub forced: bool,
    pub progress: f64,
    pub total_bytes: u64,
    pub have_bytes: u64,
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub ratio: f64,
    pub download_bps: u64,
    pub upload_bps: u64,
    /// The torrent's own upload limit in bytes per second; null is
    /// unlimited and 0 closed.
    pub upload_limit: Option<u64>,
    /// The torrent's own download limit in bytes per second; null is
    /// unlimited and 0 closed.
    pub download_limit: Option<u64>,
    pub eta_seconds: Option<u64>,
    pub peers: u32,
    pub seeds: u32,
    pub leechers: u32,
    /// The most peers a torrent connects to.
    pub max_peers: u32,
    /// The swarm's seeds and leechers as its trackers last counted them.
    pub swarm_seeds: Option<u32>,
    pub swarm_leechers: Option<u32>,
    /// Seconds until the next tracker announce.
    pub reannounce_seconds: Option<u64>,
    /// Seconds spent running in a session.
    pub active_seconds: u64,
    pub download_avg_bps: u64,
    pub upload_avg_bps: u64,
    /// Fetched bytes whose pieces did not verify.
    pub wasted_bytes: u64,
    /// The last time a complete copy was in reach.
    pub last_seen_complete_ms: Option<i64>,
    pub piece_count: Option<u32>,
    pub piece_size: Option<u64>,
    /// The pieces in hand, verified.
    pub pieces_have: Option<u32>,
    pub created_by: Option<String>,
    pub created_at_ms: Option<i64>,
    pub comment: Option<String>,
    pub added_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
    /// The folder that holds the torrent's output.
    pub save_path: Option<String>,
    /// The seeding goal still to meet; absent once it is met.
    pub seeding_left: Option<String>,
    /// The torrent's own seeding policy; absent when it inherits one.
    pub seeding_override: Option<SeedingPolicySettings>,
    /// The seeding policy in effect.
    pub seeding_policy: SeedingPolicySettings,
    pub seeding_source: SeedingSourceGql,
    /// The host pattern of the tracker rule the policy comes from, when it
    /// does.
    pub seeding_rule: Option<String>,
    /// Whether its archives are extracted.
    pub unpack: bool,
    pub route: Option<String>,
    pub files: Vec<TorrentFileGql>,
    /// Whether its pieces are fetched in order.
    pub sequential: bool,
    /// Whether each file's first and last pieces are fetched first.
    pub first_last_piece_first: bool,
    pub trackers: Vec<TorrentTrackerGql>,
}

impl From<TorrentView> for Torrent {
    fn from(view: TorrentView) -> Self {
        let files = view
            .files
            .iter()
            .map(|file| TorrentFileGql {
                index: file.index,
                path: file.path.clone(),
                length: file.length,
                selected: file.selected(),
                priority: file.priority.into(),
                progress: view.file_progress(file),
            })
            .collect();
        let (save_path, _) = view.paths();
        Self {
            id: view.job_id.0,
            name: view.name,
            infohash: view.infohash_v1.clone().unwrap_or_default(),
            infohash_v2: view.infohash_v2.clone(),
            version: view.version.map(Into::into),
            state: view.state.into(),
            detail: view.detail,
            category: view.category,
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
            upload_limit: view.upload_limit,
            download_limit: view.download_limit,
            eta_seconds: view.eta_seconds,
            peers: view.peers,
            seeds: view.seeds,
            leechers: view.leechers,
            max_peers: view.max_peers,
            swarm_seeds: view.swarm_seeds,
            swarm_leechers: view.swarm_leechers,
            reannounce_seconds: view.reannounce_seconds,
            active_seconds: view.active_seconds,
            download_avg_bps: view.download_avg_bps,
            upload_avg_bps: view.upload_avg_bps,
            wasted_bytes: view.wasted_bytes,
            last_seen_complete_ms: view.last_seen_complete_ms,
            piece_count: view.pieces.map(|pieces| pieces.count),
            piece_size: view.pieces.map(|pieces| pieces.size),
            pieces_have: view.pieces.map(|pieces| pieces.have),
            created_by: view.created_by,
            created_at_ms: view.created_at_ms,
            comment: view.comment,
            added_at_ms: view.added_at_ms,
            completed_at_ms: view.completed_at_ms,
            storage_dir: view.storage_dir,
            save_path: (!save_path.is_empty()).then_some(save_path),
            seeding_left: view.seeding_left,
            seeding_override: view.seeding_override.map(Into::into),
            seeding_policy: view.seeding_policy.into(),
            seeding_source: (&view.seeding_source).into(),
            seeding_rule: match view.seeding_source {
                SeedingSource::TrackerRule(pattern) => Some(pattern),
                _ => None,
            },
            unpack: view.unpack,
            route: view.route,
            files,
            sequential: view.sequential,
            first_last_piece_first: view.first_last_piece_first,
            trackers: view.trackers.into_iter().map(Into::into).collect(),
        }
    }
}

/// The transport a peer connection runs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "PeerConnection")]
pub enum PeerConnectionGql {
    Tcp,
    Utp,
}

/// What of a peer connection is encrypted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "PeerEncryption")]
pub enum PeerEncryptionGql {
    None,
    /// The handshake only.
    HeaderOnly,
    /// All of it.
    #[graphql(name = "RC4")]
    Rc4,
}

/// A source that named a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "PeerSource")]
pub enum PeerSourceGql {
    Tracker,
    Dht,
    Pex,
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
    pub snubbed: bool,
    pub optimistic_unchoke: bool,
    pub seed: bool,
    pub upload_only: bool,
    pub on_parole: bool,
    pub holepunched: bool,
}

/// One connection to a peer of a torrent.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentPeer")]
pub struct TorrentPeerGql {
    /// The peer's address and port.
    pub address: String,
    pub client: String,
    /// The peer id, hex.
    pub peer_id: String,
    pub connection: PeerConnectionGql,
    /// We made the connection.
    pub outgoing: bool,
    pub encryption: PeerEncryptionGql,
    pub flags: TorrentPeerFlagsGql,
    /// Every source that named the peer.
    pub sources: Vec<PeerSourceGql>,
    /// How many of the torrent's pieces the peer has.
    pub pieces: u32,
    pub progress: f64,
    pub download_bps: u64,
    pub upload_bps: u64,
    /// Bytes over this connection.
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
}

impl From<PeerInfo> for TorrentPeerGql {
    fn from(peer: PeerInfo) -> Self {
        let flags = peer.flags;
        Self {
            address: peer.addr.to_string(),
            client: peer.client,
            peer_id: peer.peer_id.as_string(),
            connection: match peer.connection {
                ConnectionKind::Tcp => PeerConnectionGql::Tcp,
                ConnectionKind::Utp => PeerConnectionGql::Utp,
            },
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

/// A torrent to add: its metainfo file, base64, or a magnet link.
#[derive(Debug, InputObject)]
pub struct SubmitTorrentInput {
    pub metainfo_base64: Option<String>,
    pub magnet: Option<String>,
    pub category: Option<String>,
    #[graphql(default)]
    pub paused: bool,
    #[graphql(default)]
    pub forced: bool,
    pub seeding: Option<SeedingPolicyInput>,
    /// Whether its archives are extracted; absent inherits.
    pub unpack: Option<bool>,
}
