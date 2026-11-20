// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(test)]
use crate::persistence::{
    Database,
    sql_runtime::{SqlRuntime, max_rows_for_engine},
};
use crate::{StateError, persistence::DatabaseWriterExecutor};
pub(crate) use weaver_server_history::history::timeline::HistoryTimelineInternalQueries;
use weaver_server_history::history::timeline::insert_job_events_sql;
#[cfg(test)]
use weaver_server_history::history::timeline::job_events_window_sql;
pub use weaver_server_history::history::timeline::{
    HistoryTimelineQueries, JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER, JobEvent, JobEventRecord,
    JobEventStageBounds,
};

impl DatabaseWriterExecutor {
    pub(crate) fn insert_job_events(&self, events: &[JobEvent]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(insert_job_events_sql(datastore, events))
    }
}

#[cfg(test)]
mod tests;

// Inline (rather than added to `tests.rs`, which is owned by another change in
// flight against this crate): covers the new batched multi-job event read.
#[cfg(test)]
mod event_window_tests {
    use super::*;

    fn kinds(events: &[JobEvent]) -> Vec<String> {
        events.iter().map(|event| event.kind.clone()).collect()
    }

    #[test]
    fn a_window_counts_back_from_the_newest_event() {
        let db = Database::open_in_memory().unwrap();
        for at in 0..10 {
            db.insert_job_event(1, at, &format!("K{at}"), "", None)
                .unwrap();
            db.insert_job_event(2, at, "OTHER", "", None).unwrap();
        }
        let window = db.get_job_events_window(1, 3, 0).unwrap();
        assert_eq!(kinds(&window), ["K7", "K8", "K9"]);
        assert_eq!(
            kinds(&window),
            kinds(&db.get_job_events_latest(1, 3).unwrap())
        );
        let window = db.get_job_events_window(1, 3, 2).unwrap();
        assert_eq!(kinds(&window), ["K5", "K6", "K7"]);
        let window = db.get_job_events_window(1, 5, 8).unwrap();
        assert_eq!(kinds(&window), ["K0", "K1"]);
        assert!(db.get_job_events_window(1, 5, 10).unwrap().is_empty());
    }

    // Paging back by row id returns every event once, in order, even with
    // events added between the pages; the count is all of them.
    #[test]
    fn paging_before_a_row_id_holds_while_events_are_added() {
        let db = Database::open_in_memory().unwrap();
        for at in 0..10 {
            db.insert_job_event(1, at, &format!("K{at}"), "", None)
                .unwrap();
            db.insert_job_event(2, at, "OTHER", "", None).unwrap();
        }
        let newest = db.get_job_event_records_window(1, 4, 0, None).unwrap();
        assert_eq!(
            kinds(&newest.iter().map(|r| r.event.clone()).collect::<Vec<_>>()),
            ["K6", "K7", "K8", "K9"]
        );
        let mut seen = newest.clone();
        let mut before = newest.first().map(|record| record.id);
        let mut added = 10;
        while let Some(oldest) = before {
            // A live job keeps writing between the requests.
            db.insert_job_event(1, added, &format!("K{added}"), "", None)
                .unwrap();
            added += 1;
            let page = db
                .get_job_event_records_window(1, 4, 0, Some(oldest))
                .unwrap();
            before = page.first().map(|record| record.id);
            let mut older = page;
            older.extend(seen);
            seen = older;
        }
        assert_eq!(
            kinds(&seen.iter().map(|r| r.event.clone()).collect::<Vec<_>>()),
            (0..10).map(|at| format!("K{at}")).collect::<Vec<_>>()
        );
        assert!(seen.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert_eq!(db.count_job_events(1).unwrap(), added as u64);
        assert_eq!(db.count_job_events(2).unwrap(), 10);
        assert_eq!(db.count_job_events(3).unwrap(), 0);
    }

    // The events page is a range of the job's index read in row id order,
    // with and without `before`, and sorts nothing.
    #[test]
    fn the_events_page_is_served_by_the_job_index() {
        let db = Database::open_in_memory().unwrap();
        for before in [None, Some(5_000)] {
            let (sql, args) = job_events_window_sql(7, 100, 0, before);
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            let datastore = db.datastore();
            let detail = db
                .run_sql_blocking_read(async move {
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                    rows.iter()
                        .map(|row| row.text("detail"))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .join("; ");
            println!("events page plan, before {before:?}: {detail}");
            assert!(
                detail.contains("SEARCH job_events USING INDEX idx_job_events_job_id (job_id=?"),
                "{detail}"
            );
            if before.is_some() {
                assert!(detail.contains("rowid<?"), "{detail}");
            }
            assert!(!detail.contains("TEMP B-TREE"), "{detail}");
        }
    }

    // An id list past one statement's bind budget reads the same rows as a
    // list that fits.
    #[test]
    fn a_long_job_list_reads_what_a_short_one_reads() {
        let db = Database::open_in_memory().unwrap();
        let far = 39_999u64;
        for at in 0..6 {
            db.insert_job_event(3, 100 + at, "JobCreated", &format!("near {at}"), None)
                .unwrap();
            db.insert_job_event(far, 200 + at, "JobCompleted", &format!("far {at}"), None)
                .unwrap();
        }
        let short = [3, far];
        let long: Vec<u64> = (1..=40_000).collect();
        assert!(long.len() > max_rows_for_engine(db.datastore().engine(), 1));

        let ids =
            |events: Vec<JobEventRecord>| events.iter().map(|event| event.id).collect::<Vec<_>>();
        for (min_id, limit) in [(0, 4), (0, 100), (5, 3)] {
            assert_eq!(
                ids(db
                    .get_recent_job_events_multi(&long, min_id, limit)
                    .unwrap()),
                ids(db
                    .get_recent_job_events_multi(&short, min_id, limit)
                    .unwrap()),
                "min_id {min_id}, limit {limit}"
            );
        }

        let kinds = ["JobCreated", "JobCompleted"];
        let sorted = |mut bounds: JobEventStageBounds| {
            bounds.values_mut().for_each(|entries| entries.sort());
            let mut bounds: Vec<_> = bounds.into_iter().collect();
            bounds.sort();
            bounds
        };
        let long_bounds = sorted(db.get_job_event_stage_bounds(&long, &kinds).unwrap());
        assert_eq!(
            long_bounds,
            sorted(db.get_job_event_stage_bounds(&short, &kinds).unwrap())
        );
        assert_eq!(
            long_bounds,
            vec![
                (3, vec![("JobCreated".to_string(), 100, 105)]),
                (far, vec![("JobCompleted".to_string(), 200, 205)]),
            ]
        );
    }

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

#[cfg(test)]
mod get_recent_job_events_multi_tests {
    use super::*;

    #[test]
    fn returns_newest_first_across_jobs_respecting_min_id_and_limit() {
        let db = Database::open_in_memory().unwrap();
        // Row id 1 is below the requested IDFrom floor.
        db.insert_job_event(1, 500, "OLD", "too old", None).unwrap();
        db.insert_job_event(1, 1000, "A", "job1-a", None).unwrap();
        db.insert_job_event(2, 1001, "B", "job2-a", None).unwrap();
        db.insert_job_event(1, 1002, "C", "job1-b", None).unwrap();
        db.insert_job_event(2, 1003, "D", "job2-b", None).unwrap();
        // Unrelated job must not leak into the batch.
        db.insert_job_event(3, 2000, "OTHER", "other job", None)
            .unwrap();

        let events = db.get_recent_job_events_multi(&[1, 2], 2, 10).unwrap();
        // Newest first (ORDER BY id DESC): D, C, B, A.
        let messages: Vec<&str> = events.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, vec!["job2-b", "job1-b", "job2-a", "job1-a"]);
        assert_eq!(
            events.iter().map(|event| event.id).collect::<Vec<_>>(),
            vec![5, 4, 3, 2]
        );

        // `limit` caps the total row count across jobs while staying
        // newest-first.
        let limited = db.get_recent_job_events_multi(&[1, 2], 2, 2).unwrap();
        let limited_messages: Vec<&str> = limited.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(limited_messages, vec!["job2-b", "job1-b"]);

        let latest = db.get_job_event_records_latest(1, 2).unwrap();
        assert_eq!(
            latest.iter().map(|event| event.id).collect::<Vec<_>>(),
            vec![2, 4]
        );
    }

    #[test]
    fn empty_job_ids_returns_empty_without_querying() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_event(1, 1000, "A", "job1-a", None).unwrap();
        assert!(
            db.get_recent_job_events_multi(&[], 0, 10)
                .unwrap()
                .is_empty()
        );
    }
}
