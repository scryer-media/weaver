// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};

use crate::jobs::JobId;
use crate::settings::SeedingPolicy;

// The hash a torrent is known by. A hybrid torrent has both; a duplicate is
// found by its v1 hash, or by its full v2 hash when it has no v1 hash.
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
    pub metainfo: Option<Vec<u8>>,
    pub private: bool,
    pub category_id: Option<u32>,
    pub added_at_ms: i64,
    pub intent: TorrentIntent,
    pub forced: bool,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
    pub policy_override: Option<SeedingPolicy>,
}

impl TorrentRecord {
    // The hash a duplicate of this torrent shares; `None` when it has neither.
    pub fn infohash(&self) -> Option<Infohash> {
        self.infohash_v1
            .map(Infohash::V1)
            .or(self.infohash_v2.map(Infohash::V2))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentFile {
    pub index: u32,
    pub path: String,
    pub length: u64,
    pub selected: bool,
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
    pub downloaded_bytes: u64,
    // Time spent complete and seeding, in seconds.
    pub finished_duration_s: u64,
    pub last_activity_ms: Option<i64>,
    // The upload total reported in the last announce.
    pub last_announce_uploaded_bytes: u64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentInsert {
    Inserted,
    // A torrent with the same infohash is already this job.
    Duplicate(JobId),
}
