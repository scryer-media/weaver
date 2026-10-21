// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};

use crate::jobs::JobId;
use crate::settings::SeedingPolicy;
pub use weaver_torrent::FilePriority;

// A hash a torrent is known by. A hybrid torrent has both, and a torrent
// sharing either is a duplicate of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Infohash {
    V1([u8; 20]),
    V2([u8; 32]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentSource {
    Magnet,
    Metainfo,
}

impl TorrentSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Magnet => "magnet",
            Self::Metainfo => "metainfo",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "magnet" => Some(Self::Magnet),
            "metainfo" => Some(Self::Metainfo),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentState {
    Queued,
    FetchingMetadata,
    CheckingFiles,
    Downloading,
    Stalled,
    Seeding,
    Paused,
    // Finished seeding, or stopped by its seeding policy.
    Stopped,
    // Its directory is on the way to another, where it goes on.
    Moving,
}

impl TorrentState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::FetchingMetadata => "fetching_metadata",
            Self::CheckingFiles => "checking_files",
            Self::Downloading => "downloading",
            Self::Stalled => "stalled",
            Self::Seeding => "seeding",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Moving => "moving",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "fetching_metadata" => Some(Self::FetchingMetadata),
            "checking_files" => Some(Self::CheckingFiles),
            "downloading" => Some(Self::Downloading),
            "stalled" => Some(Self::Stalled),
            "seeding" => Some(Self::Seeding),
            "paused" => Some(Self::Paused),
            "stopped" => Some(Self::Stopped),
            "moving" => Some(Self::Moving),
            _ => None,
        }
    }
}

// What the operator asked of a torrent, which is all of its state that is
// kept: the state consumers read is projected from it, the session and what
// holds the torrent back, each time it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentIntent {
    Resumed,
    Paused,
    // Stopped by its seeding goal, or by a refusal found while unpacking,
    // until the operator resumes it.
    Stopped,
}

impl TorrentIntent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resumed => "resumed",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "resumed" => Some(Self::Resumed),
            "paused" => Some(Self::Paused),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TorrentRecord {
    pub job_id: JobId,
    pub infohash_v1: Option<[u8; 20]>,
    pub infohash_v2: Option<[u8; 32]>,
    pub name: String,
    pub source: TorrentSource,
    /// The one copy of the metainfo: the job's tree and the session's
    /// torrent are made from views of it.
    pub metainfo: Option<bytes::Bytes>,
    pub private: bool,
    pub category_id: Option<u32>,
    pub added_at_ms: i64,
    pub intent: TorrentIntent,
    pub forced: bool,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
    pub policy_override: Option<SeedingPolicy>,
    // The tracker rule its trackers matched, if any.
    pub tracker_rule_id: Option<u32>,
    // Whether its archives are extracted: the torrent's own value until its
    // job is built, then the value in force.
    pub unpack: Option<bool>,
    // The attributes its job was added with.
    pub metadata: Vec<(String, String)>,
    // Its own upload and download limits in bytes per second: None is
    // unlimited and zero holds that direction closed.
    pub upload_limit: Option<u64>,
    pub download_limit: Option<u64>,
    // Whether its pieces are fetched in order.
    pub sequential: bool,
    // Whether each file's first and last pieces are fetched first.
    pub first_last_piece_first: bool,
}

impl TorrentRecord {
    // The hashes it goes by: a torrent sharing either is a duplicate of it.
    pub fn infohashes(&self) -> Vec<Infohash> {
        self.infohash_v1
            .map(Infohash::V1)
            .into_iter()
            .chain(self.infohash_v2.map(Infohash::V2))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentFile {
    pub index: u32,
    pub path: String,
    pub length: u64,
    // The operator's level for the file; padding is always Ignored.
    pub priority: FilePriority,
}

impl TorrentFile {
    /// Whether the file is fetched at all: above Ignored.
    pub fn selected(&self) -> bool {
        self.priority.wanted()
    }
}

/// A file level as qBittorrent numbers it, in its API and in the resume
/// data it hands libtorrent (qBittorrent `BitTorrent::DownloadPriority`).
pub const fn file_priority_number(priority: FilePriority) -> u8 {
    match priority {
        FilePriority::Ignored => 0,
        FilePriority::Normal => 1,
        FilePriority::High => 6,
        FilePriority::Maximum => 7,
    }
}

/// The level a number names: the four qBittorrent defines and nothing else
/// (qBittorrent `BitTorrent::isValidDownloadPriority`).
pub const fn file_priority_of(number: u8) -> Option<FilePriority> {
    Some(match number {
        0 => FilePriority::Ignored,
        1 => FilePriority::Normal,
        6 => FilePriority::High,
        7 => FilePriority::Maximum,
        _ => return None,
    })
}

/// A torrent's file levels as stored: one number per file in the
/// metainfo's order, the normal files at the end left off, and nothing when
/// every file is normal. A file past the list is normal, as one is past
/// libtorrent resume data's `file_priority`.
pub fn file_priorities_str(levels: &[FilePriority]) -> Option<String> {
    let last = levels
        .iter()
        .rposition(|level| *level != FilePriority::Normal)?;
    Some(
        levels[..=last]
            .iter()
            .map(|level| char::from(b'0' + file_priority_number(*level)))
            .collect(),
    )
}

pub fn parse_file_priorities(value: &str) -> Option<Vec<FilePriority>> {
    value
        .chars()
        .map(|digit| file_priority_of(u8::try_from(digit.to_digit(10)?).ok()?))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackerStatus {
    NotContacted,
    Updating,
    Working,
    NotWorking,
    Disabled,
}

/// A tracker the torrent names, and its place in the announce list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerEntry {
    pub tier: u32,
    pub url: String,
}

/// A tracker and how announcing to it is going.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentTracker {
    pub tier: u32,
    pub url: String,
    pub status: TrackerStatus,
    pub last_announce_ms: Option<i64>,
    pub next_announce_ms: Option<i64>,
    pub message: Option<String>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
}

// The seeding ledger.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TorrentCounters {
    pub uploaded_bytes: u64,
    // Every byte fetched from peers, whether it verified or not.
    pub downloaded_bytes: u64,
    // The fetched bytes whose pieces verified.
    pub checked_bytes: u64,
    // Time spent running in a session, in seconds.
    pub active_duration_s: u64,
    // The last time a complete copy was in reach: the torrent's own while it
    // ran complete, or a connected seed's.
    pub last_seen_complete_ms: Option<i64>,
    // Time spent complete and seeding, in seconds.
    pub finished_duration_s: u64,
    pub last_activity_ms: Option<i64>,
    // The upload total reported in the last announce.
    pub last_announce_uploaded_bytes: u64,
    pub updated_at_ms: i64,
}

impl TorrentCounters {
    /// The fetched bytes that did not verify.
    pub fn wasted_bytes(&self) -> u64 {
        self.downloaded_bytes.saturating_sub(self.checked_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentInsert {
    Inserted,
    // A torrent with the same infohash is already this job.
    Duplicate(JobId),
}
