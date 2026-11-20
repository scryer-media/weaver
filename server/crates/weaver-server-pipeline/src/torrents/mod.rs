// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::torrents::queries::TorrentsInternalQueries;
pub use weaver_server_torrents::torrents::admission;
pub mod availability;
pub use weaver_server_settings::torrents::identity;
pub(crate) use weaver_server_torrents::torrents::files;
pub(crate) use weaver_server_torrents::torrents::intake;
pub(crate) use weaver_server_torrents::torrents::layout;
pub use weaver_server_torrents::torrents::metrics;
pub use weaver_server_torrents::torrents::model;
pub mod naming;
pub mod net;
pub mod persistence;
pub mod queries;
pub use weaver_server_torrents::torrents::rate;
pub(crate) use weaver_server_torrents::torrents::relocate;
pub(crate) mod sealed;
pub mod service;
pub mod sessions;
pub(crate) use weaver_server_torrents::torrents::side;
pub mod status;
pub(crate) mod storage;
pub(crate) mod store;
pub(crate) mod tee;
pub(crate) use weaver_server_torrents::torrents::writes;

pub use availability::{
    TORRENT_ADD_UNAVAILABLE, TORRENTS_ENV, TORRENTS_UNAVAILABLE, TorrentAvailability,
};
pub use model::{
    ContentLayout, FilePriority, Infohash, KeptIntent, TorrentCounters, TorrentFile, TorrentInsert,
    TorrentIntent, TorrentRecord, TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
    TrackerStatus, UnpackEnd, UnpackResult, UnpackedFile,
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
    job_ids: &[weaver_server_base::jobs::ids::JobId],
) -> Result<std::collections::BTreeSet<weaver_server_base::jobs::ids::JobId>, crate::StateError> {
    db.torrent_job_ids_among(job_ids)
}

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(crate) mod test_fixture;
#[cfg(test)]
pub(crate) mod tests;
