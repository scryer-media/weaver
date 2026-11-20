// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The history timeline as the pipeline sees it, with the one event-window test
// that replays a log through the pipeline's own finalization reading.
pub use weaver_server_composition::history::timeline::*;

#[cfg(test)]
mod event_window_tests {
    use super::*;
    use crate::Database;

    // Reading only the newest finalization event decides a restore exactly
    // as replaying the whole log does.
    #[test]
    fn the_newest_finalization_event_decides_like_the_whole_log() {
        let marker = Some(JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER);
        let sequences: [&[(&str, Option<&str>)]; 7] = [
            &[],
            &[("JobCreated", None), ("DownloadFinished", marker)],
            &[
                ("DownloadFinished", marker),
                ("DownloadFinished", Some("file-1")),
                ("DownloadFinished", None),
                ("FileComplete", None),
            ],
            &[
                ("DownloadFinished", marker),
                ("DownloadPipelineDrained", None),
            ],
            &[
                ("DownloadFinished", marker),
                ("JobFailed", None),
                ("ExtractionComplete", None),
            ],
            &[
                ("JobCompleted", None),
                ("JobCreated", None),
                ("DownloadFinished", marker),
                ("RepairStarted", None),
            ],
            &[("DownloadFinished", Some("file-2")), ("JobCancelled", None)],
        ];
        let db = Database::open_in_memory().unwrap();
        for (index, sequence) in sequences.iter().enumerate() {
            let job_id = 40 + index as u64;
            for (at, (kind, file_id)) in sequence.iter().enumerate() {
                db.insert_job_event(job_id, at as i64, kind, "", *file_id)
                    .unwrap();
            }
            let whole = crate::Pipeline::download_finalization_open(
                crate::JobId(job_id),
                db.get_job_events(job_id),
            );
            let newest = crate::Pipeline::download_finalization_open(
                crate::JobId(job_id),
                db.get_last_job_event_of_kinds(
                    job_id,
                    crate::Pipeline::DOWNLOAD_FINALIZATION_KINDS,
                    JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER,
                )
                .map(|event| event.into_iter().collect()),
            );
            assert_eq!(newest, whole, "{sequence:?}");
        }
    }
}
