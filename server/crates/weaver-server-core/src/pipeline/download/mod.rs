// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub(super) mod owned_lane;
pub mod queue;
pub(crate) mod repeated;
mod retention;
pub(in crate::pipeline) mod scheduler;
pub(crate) mod transport;
mod worker;

#[cfg(test)]
pub(in crate::pipeline) use worker::lane_acquire_failure_for_work;

pub(super) use worker::HeldDownloadRefill;
#[cfg(test)]
pub(in crate::pipeline) use worker::{
    DOWNLOAD_DISPATCH_STALL_KICK_AFTER, DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
};
pub(crate) use worker::{
    DOWNLOAD_STALL_RECOVERY_ENV, DispatchStallWatch, JobLogThrottle,
    download_stall_recovery_from_env_value,
};

pub use queue::{DownloadQueue, DownloadWork};
pub(super) use transport::{DownloadLaneMode, DownloadLaneRuntimeState, LaneParkReason};

// The NNTP wire arguments for a batch of leased work, in lease order.
//
// The lane issues each BODY from `MessageId::wire_form`; this is the batch
// spelling of the same thing, kept so the invariant can be stated and tested
// over a whole lease. `DownloadWork::message_id` stores the *bare* id (the
// NZB parser strips the angle brackets), and an unbracketed BODY argument is
// a legal article-*number* reference, so a server with a group selected
// answers 430 for every article — a silent, total download failure that only
// shows up against real providers. Borrowing the stored `Arc<str>` to save an
// allocation is exactly how that regression happened.
#[cfg(test)]
pub(super) fn lease_message_id_wire_forms(works: &[DownloadWork]) -> Vec<String> {
    works
        .iter()
        .map(|work| work.message_id.wire_form())
        .collect()
}
