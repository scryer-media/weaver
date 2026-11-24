// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/// The start of the server consumer id a removed server's kept rows move
/// to. A live server's id is a number, so no live server ever has one.
pub const REMOVED_SERVER_ID_PREFIX: &str = "removed-";

/// Who used the data a row counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UsageConsumerKind {
    /// An NNTP server, by its id.
    Server,
    /// A category, by its name; the empty name is no category.
    Category,
    /// NZB jobs: `finished` counts each one that ended complete, partial or
    /// failed, `repaired` each of those that had a repair applied.
    Job,
    /// A job stage, by its name: the runs timed and their time.
    Stage,
    /// Every torrent together, under the empty id.
    Torrent,
    /// The torrents that list a tracker, by the tracker's host; the empty
    /// host is the torrents that list none. A torrent counts under each host
    /// it lists, so trackers are never added together.
    Tracker,
}

impl UsageConsumerKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Category => "category",
            Self::Job => "job",
            Self::Stage => "stage",
            Self::Torrent => "torrent",
            Self::Tracker => "tracker",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "server" => Some(Self::Server),
            "category" => Some(Self::Category),
            "job" => Some(Self::Job),
            "stage" => Some(Self::Stage),
            "torrent" => Some(Self::Torrent),
            "tracker" => Some(Self::Tracker),
            _ => None,
        }
    }
}
