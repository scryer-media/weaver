// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod admission;
pub(crate) mod files;
pub mod identity;
pub(crate) mod intake;
pub(crate) mod layout;
pub mod metrics;
pub mod model;
pub mod net;
pub mod persistence;
pub mod queries;
pub mod rate;
pub(crate) mod relocate;
pub mod service;
pub mod sessions;
pub mod status;
pub(crate) mod storage;
pub(crate) mod store;
pub(crate) mod tee;

pub use model::{
    ContentLayout, FilePriority, Infohash, TorrentCounters, TorrentFile, TorrentInsert,
    TorrentIntent, TorrentRecord, TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
    TrackerStatus,
};
pub use weaver_torrent::analysis;
pub use weaver_torrent::tracker::redacted_tracker_url;
pub use weaver_torrent::{
    ConnectionKind, Encryption, FetchPhase, NamedBy, PeerFlags, PeerInfo, PeerSourceFlag,
    PeerSources, TorrentVersion,
};

#[cfg(test)]
mod tests;
