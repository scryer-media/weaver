// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Enum, InputObject, SimpleObject};
use weaver_server_core::torrents::service::{ScheduleSeeding, SeedingSource, TorrentView};
use weaver_server_core::torrents::{TorrentState, TorrentTracker, TrackerStatus};

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

#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "TorrentFile")]
pub struct TorrentFileGql {
    pub index: u32,
    pub path: String,
    pub length: u64,
    pub selected: bool,
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
    pub infohash: String,
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
    pub eta_seconds: Option<u64>,
    pub peers: u32,
    pub seeds: u32,
    pub leechers: u32,
    pub added_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
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
    pub route: Option<String>,
    pub files: Vec<TorrentFileGql>,
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
                selected: file.selected,
                progress: view.file_progress(file),
            })
            .collect();
        Self {
            id: view.job_id.0,
            name: view.name,
            infohash: view.infohash,
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
            eta_seconds: view.eta_seconds,
            peers: view.peers,
            seeds: view.seeds,
            leechers: view.leechers,
            added_at_ms: view.added_at_ms,
            completed_at_ms: view.completed_at_ms,
            storage_dir: view.storage_dir,
            seeding_left: view.seeding_left,
            seeding_override: view.seeding_override.map(Into::into),
            seeding_policy: view.seeding_policy.into(),
            seeding_source: (&view.seeding_source).into(),
            seeding_rule: match view.seeding_source {
                SeedingSource::TrackerRule(pattern) => Some(pattern),
                _ => None,
            },
            route: view.route,
            files,
            trackers: view.trackers.into_iter().map(Into::into).collect(),
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
}
