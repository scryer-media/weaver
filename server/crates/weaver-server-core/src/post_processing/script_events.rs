// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptKind {
    PostProcessing,
    Queue,
    Scan,
    Scheduler,
    Feed,
}

impl ScriptKind {
    pub const ALL: [Self; 5] = [
        Self::PostProcessing,
        Self::Queue,
        Self::Scan,
        Self::Scheduler,
        Self::Feed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PostProcessing => "POST-PROCESSING",
            Self::Queue => "QUEUE",
            Self::Scan => "SCAN",
            Self::Scheduler => "SCHEDULER",
            Self::Feed => "FEED",
        }
    }
}

// Declaration order is NZBGet's event priority, from lowest to highest.
// Weaver's own events follow NZBGet's, so each keeps the priority a queued
// event was stored with.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueueEvent {
    FileDownloaded,
    UrlCompleted,
    NzbMarked,
    NzbAdded,
    NzbNamed,
    NzbDownloaded,
    NzbDeleted,
    // A torrent met its seeding goals.
    SeedingFinished,
    // A torrent stopped seeding before its goals were met.
    SeedingStopped,
    // An egress's quota, in one direction, came to a state: back to normal,
    // trickling past an upload soft cap, or stopped.
    QuotaNormal,
    QuotaTrickle,
    QuotaStopped,
    // A configured route went from up to down.
    RouteDown,
    // A torrent's job at the points an NZB job raises NZB_MARKED, NZB_ADDED,
    // NZB_DOWNLOADED and NZB_DELETED.
    TorrentMarked,
    TorrentAdded,
    TorrentDownloaded,
    TorrentDeleted,
    // Raised beside the job's own ADDED, DOWNLOADED or DELETED event, for
    // every kind of job.
    JobAdded,
    JobDownloaded,
    JobDeleted,
    // A route told down is available again.
    RouteUp,
}

impl QueueEvent {
    // NZB_NAMED is retained for truthful declarations, but Weaver does not raise it.
    pub const ALL: [Self; 21] = [
        Self::FileDownloaded,
        Self::UrlCompleted,
        Self::NzbMarked,
        Self::NzbAdded,
        Self::NzbNamed,
        Self::NzbDownloaded,
        Self::NzbDeleted,
        Self::SeedingFinished,
        Self::SeedingStopped,
        Self::QuotaNormal,
        Self::QuotaTrickle,
        Self::QuotaStopped,
        Self::RouteDown,
        Self::TorrentMarked,
        Self::TorrentAdded,
        Self::TorrentDownloaded,
        Self::TorrentDeleted,
        Self::JobAdded,
        Self::JobDownloaded,
        Self::JobDeleted,
        Self::RouteUp,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::FileDownloaded => "FILE_DOWNLOADED",
            Self::UrlCompleted => "URL_COMPLETED",
            Self::NzbMarked => "NZB_MARKED",
            Self::NzbAdded => "NZB_ADDED",
            Self::NzbNamed => "NZB_NAMED",
            Self::NzbDownloaded => "NZB_DOWNLOADED",
            Self::NzbDeleted => "NZB_DELETED",
            Self::SeedingFinished => "SEEDING_FINISHED",
            Self::SeedingStopped => "SEEDING_STOPPED",
            Self::QuotaNormal => "QUOTA_NORMAL",
            Self::QuotaTrickle => "QUOTA_TRICKLE",
            Self::QuotaStopped => "QUOTA_STOPPED",
            Self::RouteDown => "ROUTE_DOWN",
            Self::TorrentMarked => "TORRENT_MARKED",
            Self::TorrentAdded => "TORRENT_ADDED",
            Self::TorrentDownloaded => "TORRENT_DOWNLOADED",
            Self::TorrentDeleted => "TORRENT_DELETED",
            Self::JobAdded => "JOB_ADDED",
            Self::JobDownloaded => "JOB_DOWNLOADED",
            Self::JobDeleted => "JOB_DELETED",
            Self::RouteUp => "ROUTE_UP",
        }
    }

    // The event every kind of job raises beside this one, if any.
    pub fn generic(self) -> Option<Self> {
        match self {
            Self::NzbAdded | Self::TorrentAdded => Some(Self::JobAdded),
            Self::NzbDownloaded | Self::TorrentDownloaded => Some(Self::JobDownloaded),
            Self::NzbDeleted | Self::TorrentDeleted => Some(Self::JobDeleted),
            _ => None,
        }
    }

    // A job's download is complete: NZB_DOWNLOADED, TORRENT_DOWNLOADED or
    // JOB_DOWNLOADED.
    pub fn downloaded(self) -> bool {
        matches!(
            self,
            Self::NzbDownloaded | Self::TorrentDownloaded | Self::JobDownloaded
        )
    }

    // A job was marked: NZB_MARKED or TORRENT_MARKED.
    pub fn marked(self) -> bool {
        matches!(self, Self::NzbMarked | Self::TorrentMarked)
    }

    // The event for an egress's quota coming to `state`.
    pub fn quota(state: crate::bandwidth::QuotaState) -> Self {
        match state {
            crate::bandwidth::QuotaState::Normal => Self::QuotaNormal,
            crate::bandwidth::QuotaState::Trickle => Self::QuotaTrickle,
            crate::bandwidth::QuotaState::Stopped => Self::QuotaStopped,
        }
    }
}

// The variables Weaver's own events give a script, beside NZBGet's.
pub mod vars {
    // `torrent` for a torrent's job.
    pub const PROTOCOL: &str = "WEAVER_PROTOCOL";
    // A torrent's hashes in hex, empty when it has none of that version.
    pub const INFOHASH_V1: &str = "WEAVER_INFOHASH_V1";
    pub const INFOHASH_V2: &str = "WEAVER_INFOHASH_V2";
    // The tracker a torrent last announced to, else its first.
    pub const TRACKER: &str = "WEAVER_TRACKER";
    // SEEDING_FINISHED: what the goal does now, `stop`, `remove` or
    // `remove_with_data`.
    pub const SEEDING_ACTION: &str = "WEAVER_SEEDING_ACTION";
    // SEEDING_STOPPED: why, `paused`, `removed` or `failed`.
    pub const SEEDING_STOPPED_REASON: &str = "WEAVER_SEEDING_STOPPED_REASON";
    // QUOTA_*, ROUTE_DOWN and ROUTE_UP: the egress. QUOTA_*: the direction (`download` or `upload`) and the
    // state it came to (`normal`, `trickle` or `stopped`).
    pub const EGRESS_ID: &str = "WEAVER_EGRESS_ID";
    pub const QUOTA_DIRECTION: &str = "WEAVER_QUOTA_DIRECTION";
    pub const QUOTA_STATE: &str = "WEAVER_QUOTA_STATE";
    // ROUTE_DOWN and ROUTE_UP: the route's kind (`interface`,
    // `source_address`, `http_connect`, `http3_connect`, `socks5`, `ssh`,
    // `wireguard` or `pool`) and its name. ROUTE_DOWN alone: why it is down.
    pub const ROUTE_KIND: &str = "WEAVER_ROUTE_KIND";
    pub const ROUTE_NAME: &str = "WEAVER_ROUTE_NAME";
    pub const ROUTE_REASON: &str = "WEAVER_ROUTE_REASON";
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub enum ScriptTaskTime {
    Startup,
    Hourly { minute: u8 },
    Daily { hour: u8, minute: u8 },
}

impl std::str::FromStr for ScriptTaskTime {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = "invalid script task time";
        if value == "*" {
            return Ok(Self::Startup);
        }
        let (hour, minute) = value.split_once(':').ok_or(invalid)?;
        let number = |value: &str, max: u8| {
            if value.is_empty() || value.len() > 2 || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid);
            }
            value
                .parse::<u8>()
                .ok()
                .filter(|value| *value <= max)
                .ok_or(invalid)
        };
        let minute = number(minute, 59)?;
        if hour == "*" {
            Ok(Self::Hourly { minute })
        } else {
            Ok(Self::Daily {
                hour: number(hour, 23)?,
                minute,
            })
        }
    }
}

impl fmt::Display for ScriptTaskTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup => f.write_str("*"),
            Self::Hourly { minute } => write!(f, "*:{minute:02}"),
            Self::Daily { hour, minute } => write!(f, "{hour:02}:{minute:02}"),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum ScriptEventLabel {
    #[default]
    PostProcessing,
    Queue(QueueEvent),
    Scan,
    Scheduler(u64),
    Feed(u64),
}

impl ScriptEventLabel {
    pub fn kind(&self) -> ScriptKind {
        match self {
            Self::PostProcessing => ScriptKind::PostProcessing,
            Self::Queue(_) => ScriptKind::Queue,
            Self::Scan => ScriptKind::Scan,
            Self::Scheduler(_) => ScriptKind::Scheduler,
            Self::Feed(_) => ScriptKind::Feed,
        }
    }
}

impl fmt::Display for ScriptEventLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PostProcessing => f.write_str("post_processing"),
            Self::Queue(event) => write!(f, "queue:{}", event.as_str()),
            Self::Scan => f.write_str("scan"),
            Self::Scheduler(id) => write!(f, "scheduler:{id}"),
            Self::Feed(id) => write!(f, "feed:{id}"),
        }
    }
}

impl Serialize for ScriptEventLabel {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ScriptEventLabel {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let event = match value.as_str() {
            "post_processing" => Some(Self::PostProcessing),
            "scan" => Some(Self::Scan),
            _ => value.split_once(':').and_then(|(kind, value)| match kind {
                "queue" => QueueEvent::ALL
                    .into_iter()
                    .find(|event| event.as_str() == value)
                    .map(Self::Queue),
                "scheduler" => value.parse().ok().map(Self::Scheduler),
                "feed" => value.parse().ok().map(Self::Feed),
                _ => None,
            }),
        };
        event.ok_or_else(|| serde::de::Error::custom("invalid script event"))
    }
}
