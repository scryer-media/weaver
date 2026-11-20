// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use super::queries::{TorrentDatabase, TorrentsInternalQueries};
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_jobs::jobs::model::Protocol;

/// Which of some job ids a class still keeps live, read from the database
/// whether or not the class runs. Blocking: called off the async runtime.
pub type LiveAmong<D> = fn(&D, &[JobId]) -> Result<BTreeSet<JobId>, StateError>;

/// Which jobs a class keeps live past their history row, whose folders are
/// the class's; `None` when a history row is the whole job. Kept here, out
/// of the class's names, because the torrents' lookup is the pipeline's.
pub const fn live_of<D: TorrentDatabase>(protocol: Protocol) -> Option<LiveAmong<D>> {
    match protocol {
        Protocol::Nzb => None,
        Protocol::Torrent => Some(live_among::<D>),
    }
}

/// Which jobs the classes keep live, for `Protocol`, whose own impls are in
/// weaver-server-jobs.
pub trait ProtocolLiveAmong {
    fn live_among<D: TorrentDatabase>(
        db: &D,
        job_ids: &[JobId],
    ) -> Result<BTreeSet<JobId>, StateError>;
}

impl ProtocolLiveAmong for Protocol {
    /// Which of `job_ids` any class keeps live, as one batched lookup per
    /// class that keeps them. Blocking: called off the async runtime.
    fn live_among<D: TorrentDatabase>(
        db: &D,
        job_ids: &[JobId],
    ) -> Result<BTreeSet<JobId>, StateError> {
        let mut live = BTreeSet::new();
        for protocol in Self::ALL {
            if let Some(among) = live_of::<D>(*protocol) {
                live.extend(among(db, job_ids)?);
            }
        }
        Ok(live)
    }
}

// The torrents the database still keeps, among `job_ids`: theirs whether or
// not the client runs, so a history row never takes their folders.
pub fn live_among<D: TorrentDatabase>(
    db: &D,
    job_ids: &[JobId],
) -> Result<BTreeSet<JobId>, StateError> {
    db.torrent_job_ids_among(job_ids)
}
