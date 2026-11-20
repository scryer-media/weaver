// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(test)]
use weaver_model::files::FileRole;
use weaver_server_base::jobs::ids::JobId;

pub use weaver_server_jobs::jobs::model::*;

/// Which of some job ids a class still keeps live, read from the database
/// whether or not the class runs. Blocking: called off the async runtime.
pub type LiveAmong =
    fn(&crate::Database, &[JobId]) -> Result<std::collections::BTreeSet<JobId>, crate::StateError>;

/// Which jobs a class keeps live past their history row, whose folders are
/// the class's; `None` when a history row is the whole job. Kept here, out
/// of the class's names, because the torrents' lookup is the pipeline's.
pub const fn live_of(protocol: Protocol) -> Option<LiveAmong> {
    match protocol {
        Protocol::Nzb => None,
        Protocol::Torrent => Some(crate::torrents::live_among),
    }
}

/// Which jobs the classes keep live, for `Protocol`, whose own impls are in
/// weaver-server-jobs.
pub trait ProtocolLiveAmong {
    fn live_among(
        db: &crate::Database,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeSet<JobId>, crate::StateError>;
}

impl ProtocolLiveAmong for Protocol {
    /// Which of `job_ids` any class keeps live, as one batched lookup per
    /// class that keeps them. Blocking: called off the async runtime.
    fn live_among(
        db: &crate::Database,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeSet<JobId>, crate::StateError> {
        let mut live = std::collections::BTreeSet::new();
        for protocol in Self::ALL {
            if let Some(among) = live_of(*protocol) {
                live.extend(among(db, job_ids)?);
            }
        }
        Ok(live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_with_ordinals(ordinals: &[u32]) -> FileSpec {
        FileSpec {
            filename: "file.bin".to_string(),
            role: FileRole::from_filename("file.bin"),
            groups: Vec::new(),
            posted_at_epoch: None,
            segments: ordinals
                .iter()
                .map(|&ordinal| SegmentSpec {
                    ordinal,
                    article_number: ordinal + 1,
                    bytes: ordinal * 10,
                    message_ids: MessageIds::new(vec![format!("id-{ordinal}")]),
                })
                .collect(),
        }
    }

    #[test]
    fn segment_lookup_matches_a_scan_by_ordinal() {
        let layouts: [&[u32]; 6] = [
            &[0, 1, 2, 3, 4, 5],
            &[0, 2, 3, 7, 9],
            &[1, 2, 3],
            &[5, 3, 0, 4, 1],
            &[],
            &[4],
        ];
        for ordinals in layouts {
            let mut file = file_with_ordinals(ordinals);
            for ordinal in 0..12 {
                let scanned = file
                    .segments
                    .iter()
                    .find(|segment| segment.ordinal == ordinal)
                    .map(|segment| segment.bytes);
                assert_eq!(
                    file.segment(ordinal).map(|segment| segment.bytes),
                    scanned,
                    "ordinals {ordinals:?}, lookup {ordinal}"
                );
                assert_eq!(
                    file.segment_mut(ordinal).map(|segment| segment.bytes),
                    scanned,
                    "ordinals {ordinals:?}, mutable lookup {ordinal}"
                );
            }
        }
    }

    #[test]
    fn persisted_status_parser_supports_moving() {
        assert_eq!(
            job_status_from_persisted_str("moving", None),
            JobStatus::Moving
        );
    }

    #[test]
    fn partial_round_trips_through_its_persisted_string_and_ends_like_complete() {
        assert_eq!(JobStatus::Partial.persisted_status(), "partial");
        assert_eq!(
            job_status_from_persisted_str("partial", None),
            JobStatus::Partial
        );
        assert_eq!(
            runtime_lanes_from_status_snapshot(&JobStatus::Partial),
            runtime_lanes_from_status_snapshot(&JobStatus::Complete)
        );
        assert_eq!(
            JobStatus::Partial.shortfall_stage(),
            Some(crate::post_processing::model::PipelineFailureStage::Extract)
        );
        assert_eq!(JobStatus::Complete.shortfall_stage(), None);
    }

    #[test]
    fn legacy_status_derivation_keeps_waiting_jobs_as_downloading() {
        assert_eq!(
            derive_legacy_job_status(
                DownloadState::Downloading,
                PostState::WaitingForVolumes,
                RunState::Active,
                None,
            ),
            JobStatus::Downloading
        );
    }

    #[test]
    fn runtime_lane_snapshot_maps_repairing_and_moving() {
        assert_eq!(
            runtime_lanes_from_status_snapshot(&JobStatus::Repairing),
            (
                DownloadState::Complete,
                PostState::Repairing,
                RunState::Active,
            )
        );
        assert_eq!(
            runtime_lanes_from_status_snapshot(&JobStatus::Moving),
            (
                DownloadState::Complete,
                PostState::Finalizing,
                RunState::Active,
            )
        );
    }
}
