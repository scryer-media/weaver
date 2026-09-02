use super::*;

pub(super) mod owned_lane;
pub mod queue;
mod rar_unlock;
mod retention;
pub(super) mod transport;
mod worker;

#[cfg(test)]
pub(in crate::pipeline) use worker::{
    is_ip_replacement_policy_stop, lane_acquire_failure_for_work,
    should_neutrally_park_ip_replacement,
};

pub use queue::{DownloadQueue, DownloadWork};
pub(super) use transport::{
    DownloadLaneMode, DownloadLaneRuntimeState, JobTransportProfile, LaneParkReason,
};

/// The NNTP wire arguments for a batch of leased work, in lease order.
///
/// Both download lanes go through this. `DownloadWork::message_id` stores the
/// *bare* id (the NZB parser strips the angle brackets), and an unbracketed
/// BODY argument is a legal article-*number* reference, so a server with a
/// group selected answers 430 for every article — a silent, total download
/// failure that only shows up against real providers. Borrowing the stored
/// `Arc<str>` to save an allocation is exactly how that regression happened.
pub(super) fn lease_message_id_wire_forms(works: &[DownloadWork]) -> Vec<String> {
    works
        .iter()
        .map(|work| work.message_id.wire_form())
        .collect()
}
