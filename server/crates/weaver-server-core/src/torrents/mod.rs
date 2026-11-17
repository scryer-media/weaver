// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod admission;
pub mod availability;
pub(crate) mod files;
pub mod identity;
pub(crate) mod intake;
pub(crate) mod layout;
pub mod metrics;
pub mod model;
pub mod naming;
pub mod net;
pub mod persistence;
pub mod queries;
pub mod rate;
pub(crate) mod relocate;
pub mod service;
pub mod sessions;
pub(crate) mod side;
pub mod status;
pub(crate) mod storage;
pub(crate) mod store;
pub(crate) mod tee;
pub(crate) mod writes;

pub use availability::{
    TORRENT_ADD_UNAVAILABLE, TORRENTS_ENV, TORRENTS_UNAVAILABLE, TorrentAvailability,
};
pub use model::{
    ContentLayout, FilePriority, Infohash, KeptIntent, TorrentCounters, TorrentFile, TorrentInsert,
    TorrentIntent, TorrentRecord, TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
    TrackerStatus,
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

// The torrents the database still keeps, among `job_ids`: theirs whether or
// not the client runs, so a history row never takes their folders.
pub(crate) fn live_among(
    db: &crate::Database,
    job_ids: &[crate::jobs::ids::JobId],
) -> Result<std::collections::BTreeSet<crate::jobs::ids::JobId>, crate::StateError> {
    db.torrent_job_ids_among(job_ids)
}

#[cfg(test)]
pub(crate) mod tests;
