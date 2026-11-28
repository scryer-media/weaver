// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_net::torrents::net;
pub use weaver_server_settings::torrents::identity;
pub use weaver_server_torrent_store::torrents::admission;
pub use weaver_server_torrent_store::torrents::availability;
pub use weaver_server_torrent_store::torrents::metrics;
pub use weaver_server_torrent_store::torrents::model;
pub mod persistence;
pub mod queries;
pub use weaver_server_torrent_store::torrents::rate;
pub mod sealed;
pub use weaver_server_net_runtime::torrents::sessions;
pub use weaver_server_torrent_store::torrents::service;
pub use weaver_server_torrent_store::torrents::status;

pub use availability::{
    SeatUnavailableTorrents, TORRENT_ADD_UNAVAILABLE, TORRENTS_ENV, TORRENTS_UNAVAILABLE,
    TorrentAvailability,
};
pub use model::{
    ContentLayout, FilePriority, Infohash, KeptIntent, TorrentCounters, TorrentFile, TorrentInsert,
    TorrentIntent, TorrentRecord, TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
    TrackerStatus, UnpackCause, UnpackEnd, UnpackResult, UnpackedFile,
};
pub use weaver_torrent::analysis;
pub use weaver_torrent::tracker::redacted_tracker_url;
pub use weaver_torrent::{
    ConnectionKind, Encryption, FetchPhase, NamedBy, PeerFlags, PeerInfo, PeerSourceFlag,
    PeerSources, TorrentVersion,
};

/// The torrents as a class an operator holds apart: their own pause.
pub const CLASS: crate::jobs::HeldClass =
    crate::jobs::HeldClass::must(crate::jobs::Protocol::Torrent);

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub mod test_fixture;
#[cfg(test)]
pub(crate) mod tests;
