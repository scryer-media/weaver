// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_history::history::retention::HistoryInternalQueries;
use weaver_server_pipeline::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::history::{
    nzbget_state::HistoryNzbgetStateQueries, timeline::HistoryTimelineQueries,
};
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::jobs::repository::JobsRepositoryQueries;
use weaver_server_pipeline::pipeline::Pipeline;
use weaver_server_pipeline::pipeline::tests::history_fixture::marked_sweep_working_dir as marked_working_dir;
use weaver_server_pipeline::pipeline::tests::{activate_job, new_direct_pipeline_at_roots};
use weaver_server_pipeline::pipeline::tests::{
    build_multifile_multivolume_rar_set, cancel_one_job, insert_delivered_job,
    queue_fixture::{finish_retiring_graph, installed_spec, metrics},
    rar_job_spec,
};
use weaver_server_pipeline::pipeline::tests::{
    drain_job_events, finished_job_info, pump_pipeline_runtime_queues, queue_fixture::fail_job,
    settle_failed_releases,
};
use weaver_server_pipeline::pipeline::tests::{
    finish_cancel_jobs, nzb_zstd_for,
    queue_fixture::{apply_intents, handle, working_dir},
    segmented_job_spec,
};
use weaver_server_pipeline::pipeline::tests::{
    history_fixture::*, history_row_with_output_dir, new_direct_pipeline,
};
use weaver_server_pipeline::pipeline::tests::{insert_active_job, queue_fixture::installed_status};
use weaver_server_pipeline::pipeline::tests::{
    queue_fixture::report_all, retire_pipeline_database,
};
use weaver_server_pipeline::pipeline::tests::{
    sample_nzb_bytes, sample_nzb_zstd, standalone_job_spec,
};
use weaver_server_pipeline::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use weaver_server_pipeline::{JobSpec, RestoreJobRequest};
use weaver_server_pipeline::{JobStatus, SchedulerError};

#[tokio::test]
async fn delete_history_removes_intermediate_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let output_dir = intermediate_dir.join("history-cleanup-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    // Historical versions wrote an empty marker; deletion upgrades it from
    // the durable job record without changing the existing history action.
    tokio::fs::write(
        weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&output_dir),
        [],
    )
    .await
    .unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();

    database(&pipeline)
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "History Cleanup",
            "failed",
            output_dir.clone(),
        ))
        .unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;
    assert!(outcome.refused.is_empty());

    assert!(!output_dir.exists());
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_history_with_a_stale_marker_removes_the_row_and_keeps_the_dir() {
    // A marker that names this job but whose hash the directory no longer
    // reproduces used to abort the whole delete, leaving the record behind
    // for good. The record goes; the directory stays and is reported.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let output_dir = intermediate_dir.join("history-stale-marker-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(
        weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&output_dir),
        format!("weaver-job-v1:{}:{}\n", job_id.0, "0".repeat(64)),
    )
    .await
    .unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();
    database(&pipeline)
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "History Stale Marker",
            "failed",
            output_dir.clone(),
        ))
        .unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;

    assert_eq!(
        outcome.deleted[&job_id].as_ref().unwrap().left_in_place,
        vec![output_dir.clone()]
    );
    assert!(output_dir.join("leftover.bin").exists());
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_history_removes_db_only_history_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let output_dir = intermediate_dir.join("history-db-only-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(
        weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&output_dir),
        [],
    )
    .await
    .unwrap();

    let row = history_row_with_output_dir(job_id, "History DB NZB", "failed", output_dir);
    database(&pipeline).insert_job_history(&row).unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;
    assert!(outcome.refused.is_empty());

    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_history_batch_keeps_complete_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let failed_job_id = JobId(30021);
    let complete_job_id = JobId(30022);
    let failed_output_dir = intermediate_dir.join("failed-history-job");
    let complete_output_dir = complete_dir.join("complete-history-job");

    tokio::fs::create_dir_all(&failed_output_dir).await.unwrap();
    tokio::fs::write(
        weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&failed_output_dir),
        [],
    )
    .await
    .unwrap();
    tokio::fs::create_dir_all(&complete_output_dir)
        .await
        .unwrap();
    tokio::fs::write(failed_output_dir.join("partial.mkv"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(complete_output_dir.join("episode.mkv"), b"complete")
        .await
        .unwrap();

    database(&pipeline)
        .insert_job_history(&history_row_with_output_dir(
            failed_job_id,
            "Failed History Cleanup",
            "failed",
            failed_output_dir.clone(),
        ))
        .unwrap();
    database(&pipeline)
        .insert_job_history(&history_row_with_output_dir(
            complete_job_id,
            "Complete History Cleanup",
            "complete",
            complete_output_dir.clone(),
        ))
        .unwrap();

    let outcome =
        delete_history_batch(&mut pipeline, &[failed_job_id, complete_job_id], false).await;
    assert!(outcome.refused.is_empty());

    assert!(!failed_output_dir.exists());
    assert!(complete_output_dir.exists());
    assert!(
        database(&pipeline)
            .list_job_history(&weaver_server_pipeline::HistoryFilter::default())
            .unwrap()
            .is_empty()
    );
}

// A failed job's record names its working directory: deleting the history
// with its files takes all of it, what it delivered and the volumes and
// recovery files beside it, and what its staging directory kept.
#[tokio::test]
async fn delete_history_with_files_removes_a_failed_job_s_whole_working_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30027);
    let staging = staging_dir_for(&complete_dir, job_id);
    make_staging_dir(&staging, job_id).unwrap();
    tokio::fs::write(staging.join("lantern.mkv"), b"staged")
        .await
        .unwrap();
    let output_dir = intermediate_dir.join("history-failed-volumes-job");
    tokio::fs::create_dir_all(output_dir.join("Sample"))
        .await
        .unwrap();
    weaver_server_pipeline::jobs::working_dir::mark_weaver_owned_working_dir(
        &intermediate_dir,
        &output_dir,
        job_id,
    )
    .unwrap();
    for name in [
        "lantern.part01.rar",
        "lantern.part02.rar",
        "lantern.vol00+01.par2",
        "lantern.par2",
        "Sample/lantern-sample.mkv",
    ] {
        tokio::fs::write(output_dir.join(name), b"kept")
            .await
            .unwrap();
    }
    database(&pipeline)
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "History Failed Volumes",
            "failed",
            output_dir.clone(),
        ))
        .unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], true).await;

    assert!(outcome.refused.is_empty());
    assert!(
        outcome.deleted[&job_id]
            .as_ref()
            .unwrap()
            .left_in_place
            .is_empty()
    );
    assert!(!output_dir.exists());
    assert!(!staging.exists());
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_none()
    );
}

// With the setting on, a failed job that finished before the cutoff gives up
// its working directory and its staging directory, and keeps its record.
#[tokio::test]
async fn housekeeping_removes_an_old_failed_job_s_files_and_keeps_its_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30041);
    let output_dir = marked_working_dir(&intermediate_dir, "housekeeping-failed", job_id).await;
    let staging = staging_dir_for(&complete_dir, job_id);
    make_staging_dir(&staging, job_id).unwrap();
    tokio::fs::write(staging.join("lantern.mkv"), b"staged")
        .await
        .unwrap();
    database(&pipeline)
        .insert_job_history(&history_row_finished_at(
            job_id,
            "failed",
            output_dir.clone(),
            100,
        ))
        .unwrap();
    report_all(&pipeline);

    sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH).await;

    assert!(!output_dir.exists());
    assert!(!staging.exists());
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_some()
    );
    retire_pipeline_database(pipeline).await;
}

// A failed job's recorded working directory with no marker is not
// housekeeping's to take, whoever made it: it stays, with what it holds. The
// job is stamped all the same, so no later sweep lists it again; removing
// the entry's files is still the user's to ask for.
#[tokio::test]
async fn housekeeping_leaves_an_unmarked_recorded_folder_and_lists_its_job_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30050);
    let working_dir = intermediate_dir.join("job-30050");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::write(working_dir.join("lantern.part01.rar"), b"partial")
        .await
        .unwrap();
    database(&pipeline)
        .insert_job_history(&history_row_finished_at(
            job_id,
            "failed",
            working_dir.clone(),
            100,
        ))
        .unwrap();
    report_all(&pipeline);
    let listed = |pipeline: &Pipeline| {
        database(pipeline)
            .failed_job_history_page(100, None, 10)
            .unwrap()
    };
    assert_eq!(listed(&pipeline).len(), 1);

    for _ in 0..2 {
        sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH).await;
        assert!(working_dir.join("lantern.part01.rar").exists());
        assert!(
            !weaver_server_pipeline::jobs::working_dir::is_weaver_owned_working_dir(&working_dir)
        );
        assert!(swept_at(&pipeline, job_id).await.is_some());
        assert!(listed(&pipeline).is_empty());
    }
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_some()
    );
    retire_pipeline_database(pipeline).await;
}

// With the intermediate root away, as on a drive that is offline, the sweep
// judges no job: none is stamped, whatever its record names. With the root
// back, the job's marked folder goes and the job is stamped.
#[tokio::test]
async fn housekeeping_stamps_nothing_while_the_intermediate_root_is_away() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let (delivered, unrecorded) = (JobId(30090), JobId(30091));
    let working_dir = marked_working_dir(&intermediate_dir, "Harbor", delivered).await;
    let delivery = complete_dir.join("Harbor");
    tokio::fs::create_dir_all(&delivery).await.unwrap();
    database(&pipeline)
        .insert_job_history(&history_row_finished_at(delivered, "failed", delivery, 2))
        .unwrap();
    let mut bare = history_row_finished_at(unrecorded, "failed", PathBuf::new(), 2);
    bare.output_dir = None;
    database(&pipeline).insert_job_history(&bare).unwrap();
    report_all(&pipeline);
    let away = temp_dir.path().join("intermediate-away");
    std::fs::rename(&intermediate_dir, &away).unwrap();

    sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH).await;
    assert_eq!(swept_at(&pipeline, delivered).await, None);
    assert_eq!(swept_at(&pipeline, unrecorded).await, None);
    assert!(!intermediate_dir.exists());

    std::fs::rename(&away, &intermediate_dir).unwrap();
    sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH).await;
    assert!(!working_dir.exists());
    assert!(swept_at(&pipeline, delivered).await.is_some());
    assert!(swept_at(&pipeline, unrecorded).await.is_some());
    retire_pipeline_database(pipeline).await;
}

// The sweep reads the failed jobs a page at a time and each page's records
// again in one statement, and removes the same folders as the one unpaged
// listing it replaced: every failed job that finished by the cutoff, finish
// times tied across page edges included, and no other.
#[tokio::test]
async fn housekeeping_pages_remove_what_the_unpaged_listing_named() {
    use weaver_server_pipeline::persistence::sql_runtime::SqlRuntime;
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let count = FAILED_SWEEP_PAGE as u64 * 3 + 37;
    let mut dirs = BTreeMap::new();
    for n in 0..count {
        let job_id = JobId(40_000 + n);
        let dir = marked_working_dir(&intermediate_dir, &format!("paged-{n}"), job_id).await;
        // Few distinct finish times, so ties cross every page edge; every
        // seventh job finished after the cutoff, every eleventh is partial.
        let completed_at = if n % 7 == 0 { 500 } else { (n % 3) as i64 };
        let status = if n % 11 == 0 { "partial" } else { "failed" };
        database(&pipeline)
            .insert_job_history(&history_row_finished_at(
                job_id,
                status,
                dir.clone(),
                completed_at,
            ))
            .unwrap();
        dirs.insert(job_id, dir);
    }
    report_all(&pipeline);
    // The listing the pages replaced, as one statement.
    let unpaged: BTreeSet<JobId> = SqlRuntime::fetch_all(
        database(&pipeline).datastore().read_exec(),
        "SELECT job_id, output_dir FROM job_history WHERE status = 'failed' AND completed_at <= 100",
        &[],
    )
    .await
    .unwrap()
    .into_iter()
    .map(|row| JobId(row.i64("job_id").unwrap() as u64))
    .collect();
    assert!(unpaged.len() > FAILED_SWEEP_PAGE * 2);

    sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH).await;

    let removed: BTreeSet<JobId> = dirs
        .iter()
        .filter(|(_, dir)| !dir.exists())
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(removed, unpaged);

    // A second sweep with nothing new lists no job: no hold, no folder scan,
    // no removal and no write. A swept job's folder made again stays.
    assert!(
        database(&pipeline)
            .failed_job_history_page(100, None, FAILED_SWEEP_PAGE)
            .unwrap()
            .is_empty()
    );
    for id in &removed {
        marked_working_dir(&intermediate_dir, &format!("paged-{}", id.0 - 40_000), *id).await;
    }
    sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH).await;
    assert!(removed.iter().all(|id| dirs[id].exists()));
    retire_pipeline_database(pipeline).await;
}

// The sweep stamps, internally, every failed job it has no more to do for:
// one it left nothing of, and one whose delivery folder is not its to take.
// Neither is listed again. Nothing any view shows of either entry changes,
// "remove files" included. A job stamped and then finished again as failed
// is swept again.
#[tokio::test]
async fn housekeeping_stamps_a_failed_job_once_it_has_no_more_to_do_for_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let delivered = JobId(30060);
    let delivery = complete_dir.join("Lantern.Harbor.2024");
    tokio::fs::create_dir_all(&delivery).await.unwrap();
    tokio::fs::write(delivery.join("lantern.mkv"), b"partial")
        .await
        .unwrap();
    let swept = JobId(30061);
    let working = marked_working_dir(&intermediate_dir, "swept-whole", swept).await;
    for (id, dir) in [(delivered, delivery.clone()), (swept, working.clone())] {
        database(&pipeline)
            .insert_job_history(&history_row_finished_at(id, "failed", dir, 2))
            .unwrap();
    }
    report_all(&pipeline);
    let before = views_of(&pipeline, &[delivered, swept]);

    let sweep = || sweep_job_files(&pipeline, Some(100), std::time::SystemTime::UNIX_EPOCH);
    sweep().await;

    assert!(delivery.join("lantern.mkv").exists());
    assert!(!working.exists());
    assert!(swept_at(&pipeline, delivered).await.is_some());
    assert!(swept_at(&pipeline, swept).await.is_some());
    assert_eq!(views_of(&pipeline, &[delivered, swept]), before);
    let listed = |pipeline: &Pipeline| -> Vec<JobId> {
        database(pipeline)
            .failed_job_history_page(100, None, 10)
            .unwrap()
            .into_iter()
            .map(|(_, id, _)| id)
            .collect()
    };
    assert_eq!(listed(&pipeline), Vec::<JobId>::new());

    // Restarted and failed again: the archive writes the entry afresh, the
    // stamp goes, and the next sweep takes the new working folder.
    let again = marked_working_dir(&intermediate_dir, "swept-again", swept).await;
    database(&pipeline)
        .archive_job(
            swept,
            &history_row_finished_at(swept, "failed", again.clone(), 3),
        )
        .unwrap();
    assert_eq!(swept_at(&pipeline, swept).await, None);
    assert_eq!(listed(&pipeline), vec![swept]);
    sweep().await;
    assert!(!again.exists());
    assert!(swept_at(&pipeline, swept).await.is_some());
    retire_pipeline_database(pipeline).await;
}

// The sweep's page is served by the unswept-failed index, filter and sort,
// so a sweep with nothing to do reads an empty index range.
#[tokio::test]
async fn the_failed_sweep_page_reads_the_unswept_index() {
    use weaver_server_pipeline::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use weaver_server_history::history::retention::{
        FAILED_SWEEP_FIRST_PAGE_SQL, FAILED_SWEEP_NEXT_PAGE_SQL,
    };
    // The texts production runs, each under EXPLAIN.
    for (sql, args) in [
        (
            format!("EXPLAIN QUERY PLAN {FAILED_SWEEP_FIRST_PAGE_SQL}"),
            vec![SqlArg::I64(100), SqlArg::I64(50)],
        ),
        (
            format!("EXPLAIN QUERY PLAN {FAILED_SWEEP_NEXT_PAGE_SQL}"),
            vec![
                SqlArg::I64(100),
                SqlArg::I64(5),
                SqlArg::I64(5),
                SqlArg::I64(9),
                SqlArg::I64(50),
            ],
        ),
    ] {
        let plan: Vec<String> =
            SqlRuntime::fetch_all(database(&pipeline).datastore().read_exec(), &sql, &args)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.text("detail").unwrap())
                .collect();
        assert!(
            plan.iter()
                .any(|detail| detail.contains("idx_job_history_failed_unswept")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("TEMP B-TREE")),
            "{plan:?}"
        );
    }
    retire_pipeline_database(pipeline).await;
}

// With the setting off, a failed job's files stay however old it is.
#[tokio::test]
async fn housekeeping_with_the_setting_off_leaves_a_failed_job_s_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30042);
    let output_dir = marked_working_dir(&intermediate_dir, "housekeeping-off", job_id).await;
    let staging = staging_dir_for(&complete_dir, job_id);
    tokio::fs::create_dir_all(&staging).await.unwrap();
    database(&pipeline)
        .insert_job_history(&history_row_finished_at(
            job_id,
            "failed",
            output_dir.clone(),
            2,
        ))
        .unwrap();
    report_all(&pipeline);

    sweep_job_files(&pipeline, None, after_every_marker()).await;

    assert!(output_dir.exists());
    assert!(staging.exists());
    retire_pipeline_database(pipeline).await;
}

// A rowless marked directory written since the cutoff may be a job still
// being admitted, and stays.
#[tokio::test]
async fn housekeeping_leaves_a_rowless_marker_younger_than_the_cutoff() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let rowless_dir = marked_working_dir(
        &intermediate_dir,
        "housekeeping-young-rowless",
        JobId(30050),
    )
    .await;
    report_all(&pipeline);

    sweep_job_files(&pipeline, None, std::time::SystemTime::UNIX_EPOCH).await;

    assert!(rowless_dir.exists());
    retire_pipeline_database(pipeline).await;
}

// Everything the history page, the job page and the API read of an entry,
// rendered: its row, its files state, its removed-file record, and the
// listing.
fn views_of(pipeline: &Pipeline, ids: &[JobId]) -> String {
    let db = database(pipeline);
    let mut out = format!(
        "{:?}",
        db.list_job_history(&weaver_server_pipeline::HistoryFilter::default())
            .unwrap()
    );
    out.push_str(&format!(
        "{:?}",
        db.list_job_history_file_states(ids).unwrap()
    ));
    for id in ids {
        out.push_str(&format!("{:?}", db.get_job_history(id.0).unwrap()));
        out.push_str(&format!(
            "{:?}",
            db.list_archived_history_files(id.0, 0, 100).unwrap()
        ));
    }
    out
}

// With no reading of the root a page step stops the sweep at once: nothing
// is read or counted for its jobs, and none is listed or held.
#[tokio::test]
async fn a_page_step_with_no_reading_of_the_root_stops_the_sweep() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let id = JobId(30092);
    let mut row = history_row_finished_at(id, "failed", PathBuf::new(), 2);
    row.output_dir = None;
    database(&pipeline).insert_job_history(&row).unwrap();
    report_all(&pipeline);
    let mut places = SweepPage::unread();

    assert!(!places.sweep(&pipeline, &[], 100, vec![(id, None)]).await);
    assert!(places.waiting().is_empty());
    assert!(places.parents().is_empty());
    assert!(sweep_list_and_holds_empty(&pipeline));
    assert_eq!(swept_at(&pipeline, id).await, None);
    retire_pipeline_database(pipeline).await;
}

// Three hundred failed jobs whose records stand in one folder that is not
// there: the folder is judged once for them all, every one is left waiting
// on it, and none is stamped. A job recorded under a folder that is there,
// its own folder gone, is stamped in the same pass.
#[tokio::test]
async fn jobs_under_one_missing_folder_cost_one_check_and_are_not_stamped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let gone = temp_dir.path().join("retired-drive");
    let there = temp_dir.path().join("old-intermediate");
    tokio::fs::create_dir_all(&there).await.unwrap();
    let ids: Vec<JobId> = (0..300).map(|n| JobId(31000 + n)).collect();
    let settled = JobId(31999);
    let mut page = Vec::new();
    for id in &ids {
        let dir = gone.join(format!("job-{}", id.0));
        database(&pipeline)
            .insert_job_history(&history_row_finished_at(*id, "failed", dir.clone(), 2))
            .unwrap();
        page.push((*id, Some(dir)));
    }
    let settled_dir = there.join("job-settled");
    database(&pipeline)
        .insert_job_history(&history_row_finished_at(
            settled,
            "failed",
            settled_dir.clone(),
            2,
        ))
        .unwrap();
    page.push((settled, Some(settled_dir)));
    report_all(&pipeline);
    assert_eq!(sweep_root(&pipeline), intermediate_dir);
    let mut places = SweepPage::read(Vec::new(), 0);

    assert!(places.sweep(&pipeline, &[], 100, page).await);

    // One answer per folder, however many jobs name it.
    assert_eq!(
        *places.parents(),
        std::collections::HashMap::from([(gone.clone(), false), (there, true)])
    );
    assert_eq!(*places.waiting(), BTreeMap::from([(gone, 300)]));
    let stamped = database(&pipeline)
        .failed_job_history_page(100, None, 500)
        .unwrap();
    assert_eq!(stamped.len(), 300);
    assert!(swept_at(&pipeline, settled).await.is_some());
    assert_eq!(swept_at(&pipeline, ids[0]).await, None);
    assert_eq!(swept_at(&pipeline, ids[299]).await, None);
    retire_pipeline_database(pipeline).await;
}

// A failed job whose record stands in a folder that is gone still gives up
// the marked folder the reading of the root found of it, and is not
// stamped. With nothing more found of it, the next page step passes it: no
// hold, and nothing of it is looked for, so a staging directory made since
// stays.
#[tokio::test]
async fn a_job_under_a_missing_folder_gives_up_what_the_reading_found_and_is_then_passed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let id = JobId(30093);
    let working_dir = marked_working_dir(&intermediate_dir, "Harbor", id).await;
    let gone = temp_dir.path().join("tv");
    let recorded = gone.join("Harbor");
    database(&pipeline)
        .insert_job_history(&history_row_finished_at(id, "failed", recorded.clone(), 2))
        .unwrap();
    report_all(&pipeline);
    let mut places = SweepPage::read(vec![(id, working_dir.clone())], 0);
    let staging_roots = [complete_dir.clone()];

    assert!(
        places
            .sweep(
                &pipeline,
                &staging_roots,
                100,
                vec![(id, Some(recorded.clone()))],
            )
            .await
    );
    assert!(!working_dir.exists());
    assert_eq!(swept_at(&pipeline, id).await, None);
    assert_eq!(*places.waiting(), BTreeMap::from([(gone.clone(), 1)]));
    assert!(history_holds(&pipeline).is_empty());

    // Nothing of the job is in the reading now. A staging directory made
    // since would go if the job were swept again.
    let staging = staging_dir_for(&complete_dir, id);
    make_staging_dir(&staging, id).unwrap();
    assert!(
        places
            .sweep(&pipeline, &staging_roots, 100, vec![(id, Some(recorded))],)
            .await
    );
    assert!(staging.exists());
    assert_eq!(swept_at(&pipeline, id).await, None);
    assert_eq!(*places.waiting(), BTreeMap::from([(gone, 2)]));
    retire_pipeline_database(pipeline).await;
}

// A script a later job of the page gains while an earlier one is swept, as
// a mark queues one, keeps that job's folder: it is left unswept and
// unstamped, as a job with a script at the page's read is.
#[tokio::test]
async fn a_script_a_later_job_gains_mid_page_keeps_its_folder() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let (first, marked) = (JobId(30097), JobId(30098));
    let first_dir = marked_working_dir(&intermediate_dir, "first", first).await;
    let marked_dir = marked_working_dir(&intermediate_dir, "marked", marked).await;
    for id in [first, marked] {
        let mut row = history_row_finished_at(id, "failed", PathBuf::new(), 2);
        row.output_dir = None;
        database(&pipeline).insert_job_history(&row).unwrap();
    }
    report_all(&pipeline);
    let mut places = SweepPage::read(
        vec![(first, first_dir.clone()), (marked, marked_dir.clone())],
        0,
    );
    // On the first job's turn, after the page's read, the second gains a
    // script.
    let held: std::rc::Rc<std::cell::RefCell<Vec<Box<dyn std::any::Any>>>> = Default::default();
    {
        let (db, held) = (database(&pipeline).clone(), held.clone());
        between_sweeps(move |id| {
            if id == first {
                held.borrow_mut().push(Box::new(
                    weaver_server_pipeline::post_processing::events::hold_test_event_run(
                        &db, marked.0, "marked",
                    ),
                ));
            }
        });
    }

    assert!(
        places
            .sweep(&pipeline, &[], 100, vec![(first, None), (marked, None)],)
            .await
    );
    assert!(!first_dir.exists());
    assert!(swept_at(&pipeline, first).await.is_some());
    assert!(marked_dir.exists());
    assert_eq!(swept_at(&pipeline, marked).await, None);
    held.borrow_mut().clear();
    retire_pipeline_database(pipeline).await;
}

// A reading of the root that could not read one of its entries stamps no
// job: the entry it missed may be any job's. What it did find still goes.
#[tokio::test]
async fn a_reading_that_missed_an_entry_sweeps_what_it_found_and_stamps_no_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let (found, missed) = (JobId(30095), JobId(30096));
    let found_dir = marked_working_dir(&intermediate_dir, "found", found).await;
    for id in [found, missed] {
        let mut row = history_row_finished_at(id, "failed", PathBuf::new(), 2);
        row.output_dir = None;
        database(&pipeline).insert_job_history(&row).unwrap();
    }
    report_all(&pipeline);
    let mut places = SweepPage::read(vec![(found, found_dir.clone())], 1);

    assert!(
        places
            .sweep(&pipeline, &[], 100, vec![(found, None), (missed, None)],)
            .await
    );

    assert!(!found_dir.exists());
    assert_eq!(swept_at(&pipeline, found).await, None);
    assert_eq!(swept_at(&pipeline, missed).await, None);
    retire_pipeline_database(pipeline).await;
}

// One page of the sweep works from the reading of the intermediate root it
// is given, and reads no root of its own: a job the reading names gives up
// its folder and is stamped, and a job whose marked folder the reading does
// not name keeps it and is listed again.
#[tokio::test]
async fn a_sweep_page_works_from_the_one_reading_of_the_root_it_is_given() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let (read, unread) = (JobId(30070), JobId(30071));
    let read_dir = marked_working_dir(&intermediate_dir, "sweep-read", read).await;
    let unread_dir = marked_working_dir(&intermediate_dir, "sweep-unread", unread).await;
    for (id, dir) in [(read, read_dir.clone()), (unread, unread_dir.clone())] {
        database(&pipeline)
            .insert_job_history(&history_row_finished_at(id, "failed", dir, 2))
            .unwrap();
    }
    report_all(&pipeline);
    let mut places = SweepPage::read(vec![(read, read_dir.clone())], 0);

    assert!(
        places
            .sweep(
                &pipeline,
                &[],
                100,
                vec![
                    (read, Some(read_dir.clone())),
                    (unread, Some(unread_dir.clone())),
                ],
            )
            .await
    );

    assert!(!read_dir.exists());
    assert!(unread_dir.exists());
    assert!(swept_at(&pipeline, read).await.is_some());
    assert_eq!(swept_at(&pipeline, unread).await, None);
    // Each job's hold went as its own sweep ended.
    assert!(history_holds(&pipeline).is_empty());
    retire_pipeline_database(pipeline).await;
}

// The sweep's step for one job holds that job alone and lets it go as it
// returns: with one job of a page swept, a delete of another job of the
// page, not yet swept, is accepted. The page's stamp then takes only what
// is still the failure listed: not a job gone since, not one that failed
// again after the sweep's bound, not one that is no longer failed.
#[tokio::test]
async fn a_swept_job_holds_no_other_job_of_its_page_and_the_stamp_takes_only_listed_failures() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let (first, second, again, done) = (JobId(30080), JobId(30081), JobId(30082), JobId(30083));
    let first_dir = marked_working_dir(&intermediate_dir, "step-first", first).await;
    let second_dir = marked_working_dir(&intermediate_dir, "step-second", second).await;
    for (id, status, dir, finished) in [
        (first, "failed", first_dir.clone(), 2),
        (second, "failed", second_dir.clone(), 2),
        (again, "failed", intermediate_dir.join("step-again"), 500),
        (done, "complete", intermediate_dir.join("step-done"), 2),
    ] {
        database(&pipeline)
            .insert_job_history(&history_row_finished_at(id, status, dir, finished))
            .unwrap();
    }
    report_all(&pipeline);
    let job = SweepJob::observe(&pipeline);
    // The page lists both as due, as its reading of their rows does.
    job.list([first, second]);

    let swept = job
        .sweep(&[], first, Some(first_dir.clone()), vec![first_dir.clone()])
        .await;

    assert_eq!(swept, Some(first));
    assert!(!first_dir.exists());
    assert!(history_holds(&pipeline).is_empty());
    // The second job, listed and not yet swept, is free to be deleted.
    let outcome = delete_history_batch(&mut pipeline, &[second], false).await;
    assert!(outcome.refused.is_empty());
    assert!(
        database(&pipeline)
            .get_job_history(second.0)
            .unwrap()
            .is_none()
    );

    stamp_history_swept(&pipeline, &[first, second, again, done], 900, 100).unwrap();
    assert_eq!(swept_at(&pipeline, first).await, Some(900));
    assert_eq!(swept_at(&pipeline, again).await, None);
    assert_eq!(swept_at(&pipeline, done).await, None);

    // A job admitted after the page read it is no longer listed, and its
    // hold is refused: nothing of it is touched.
    let late = JobId(30084);
    let late_dir = marked_working_dir(&intermediate_dir, "step-late", late).await;
    let swept = job
        .sweep(&[], late, Some(late_dir.clone()), vec![late_dir.clone()])
        .await;
    assert_eq!(swept, None);
    assert!(late_dir.exists());
    retire_pipeline_database(pipeline).await;
}

// A job of a listed page that is sent for again, by a retry from history or
// by a new add under its id, is no longer the failure the page read. When
// it has left the queue again before the sweep reaches it, its hold is
// refused: the folder its new run left stays, and it is not stamped.
#[tokio::test]
async fn a_job_sent_for_again_while_its_page_is_swept_keeps_its_new_folder() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let handle = handle(&pipeline);
    let (retried, added) = (JobId(30097), JobId(30098));
    let spec = |id: JobId| segmented_job_spec(&format!("Sweep {}", id.0), "payload.bin", &[100]);
    let mut events = handle.subscribe_events();
    for id in [retried, added] {
        let spec = spec(id);
        let nzb = nzb_zstd_for(&spec);
        handle
            .add_job(id, spec, PathBuf::from("sweep.nzb"), nzb)
            .await
            .unwrap();
        apply_intents(&mut pipeline).await;
        handle.cancel_job(id).await.unwrap();
        apply_intents(&mut pipeline).await;
        finish_cancel_jobs(&mut pipeline).await;
        while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
        {
        }
    }
    let job = SweepJob::observe(&pipeline);
    // The page lists both as due, as its reading of their rows does.
    job.list([retried, added]);

    // Each is sent for again by its real command, runs, and leaves the
    // queue with its files kept.
    handle.redownload_job(retried).await.unwrap();
    let again = spec(added);
    let nzb = nzb_zstd_for(&again);
    handle
        .add_job(added, again, PathBuf::from("sweep.nzb"), nzb)
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    let mut left = Vec::new();
    for id in [retried, added] {
        let dir = working_dir(&pipeline, id).clone();
        handle.cancel_job_keeping_files(id).await.unwrap();
        apply_intents(&mut pipeline).await;
        finish_cancel_jobs(&mut pipeline).await;
        while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
        {
        }
        assert!(dir.exists());
        left.push((id, dir));
    }

    for (id, dir) in left {
        let swept = job
            .sweep(&[], id, Some(dir.clone()), vec![dir.clone()])
            .await;
        assert_eq!(swept, None);
        assert!(dir.exists(), "the folder of the job's new run was swept");
    }
    retire_pipeline_database(pipeline).await;
}

// A failed job whose files will not go is tried on every sweep and stays
// unstamped, but is warned about once in a run. The sweep that removes them
// hands the job back to be stamped and forgets the warning, so a failure
// after that warns again.
#[tokio::test]
async fn a_failed_job_whose_files_will_not_go_is_warned_about_once_and_tried_every_sweep() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job = SweepJob::observe(&pipeline);
    let id = JobId(30099);
    // A regular file where the staging folder belongs: every removal of the
    // job's staging fails.
    let staging_root = temp_dir.path().join("stuck-staging-root");
    std::fs::create_dir_all(&staging_root).unwrap();
    let blocker = staging_root.join(STAGING_DIR_NAME);
    std::fs::write(&blocker, b"not a folder").unwrap();
    let roots = [staging_root.clone()];
    let (job_ref, roots_ref) = (&job, &roots);
    let sweep = || async move {
        job_ref.list([id]);
        job_ref.sweep(roots_ref, id, None, Vec::new()).await
    };
    let warned = || job.warned(id);

    assert_eq!(sweep().await, None, "the files are still there");
    assert!(warned());
    assert_eq!(sweep().await, None, "every sweep tries again");
    assert!(warned());
    assert_eq!(job.warning_count(), 1);

    std::fs::remove_file(&blocker).unwrap();
    assert_eq!(
        sweep().await,
        Some(id),
        "the sweep that removes them stamps it"
    );
    assert!(!warned());
    retire_pipeline_database(pipeline).await;
}

// Admission holds nothing back for memory: a job is added past a ledger
// with no room, and waits its turn charged nothing.
#[tokio::test]
async fn admission_reserves_nothing_per_article() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let spec = standalone_job_spec("Memory admission", &[("small.bin".to_string(), 16)]);
    set_empty_memory_budget(&mut pipeline);
    admit_job(
        &mut pipeline,
        JobId(31001),
        spec,
        PathBuf::from("31001.nzb"),
        sample_nzb_zstd(),
    )
    .await;
    assert_eq!(memory_reserved(&pipeline), 0);
    retire_pipeline_database(pipeline).await;
}

// What housekeeping never takes: a failure younger than the cutoff, a
// Partial or Complete job, a live job, a directory whose marker names
// another job or stopped matching it, and a directory with neither a marker
// nor a row. A marked directory whose job has no row goes.
#[tokio::test]
async fn housekeeping_takes_only_what_an_old_failure_or_a_rowless_marker_accounts_for() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;

    let fresh = JobId(30043);
    let fresh_dir = marked_working_dir(&intermediate_dir, "housekeeping-fresh", fresh).await;
    let partial = JobId(30044);
    let partial_dir = marked_working_dir(&intermediate_dir, "housekeeping-partial", partial).await;
    let complete = JobId(30045);
    let complete_dir =
        marked_working_dir(&intermediate_dir, "housekeeping-complete", complete).await;
    // A failed job's record names a directory another recorded job's marker owns.
    let foreign = JobId(30046);
    let foreign_dir = complete_dir.clone();
    // A failed job's marker stopped matching once its directory was renamed.
    let moved = JobId(30047);
    let before_move =
        marked_working_dir(&intermediate_dir, "housekeeping-before-move", moved).await;
    let moved_dir = intermediate_dir.join("housekeeping-moved");
    tokio::fs::rename(&before_move, &moved_dir).await.unwrap();
    // No row and no marker.
    let unmarked_dir = intermediate_dir.join("housekeeping-unmarked");
    tokio::fs::create_dir_all(&unmarked_dir).await.unwrap();
    tokio::fs::write(unmarked_dir.join("user.mkv"), b"theirs")
        .await
        .unwrap();
    // A marker whose job has no row.
    let rowless = JobId(30048);
    let rowless_dir = marked_working_dir(&intermediate_dir, "housekeeping-rowless", rowless).await;
    // A live job, whose record says it failed before it was restarted.
    let live = JobId(30049);
    let live_dir = admit_job(
        &mut pipeline,
        live,
        standalone_job_spec("Housekeeping live", &[("small.bin".to_string(), 16)]),
        PathBuf::from("30049.nzb"),
        sample_nzb_zstd(),
    )
    .await;
    tokio::fs::write(live_dir.join("lantern.part01.rar"), b"live")
        .await
        .unwrap();

    for row in [
        history_row_finished_at(fresh, "failed", fresh_dir.clone(), 1_000),
        history_row_finished_at(partial, "partial", partial_dir.clone(), 2),
        history_row_finished_at(complete, "complete", complete_dir.clone(), 2),
        history_row_finished_at(foreign, "failed", foreign_dir.clone(), 2),
        history_row_finished_at(moved, "failed", moved_dir.clone(), 2),
        history_row_finished_at(live, "failed", live_dir.clone(), 2),
    ] {
        database(&pipeline).insert_job_history(&row).unwrap();
    }
    report_all(&pipeline);

    sweep_job_files(&pipeline, Some(100), after_every_marker()).await;

    for kept in [
        &fresh_dir,
        &partial_dir,
        &complete_dir,
        &moved_dir,
        &unmarked_dir,
        &live_dir,
    ] {
        assert!(
            kept.join(if *kept == unmarked_dir {
                "user.mkv"
            } else {
                "lantern.part01.rar"
            })
            .exists(),
            "{} stayed",
            kept.display()
        );
    }
    assert!(!rowless_dir.exists());
    retire_pipeline_database(pipeline).await;
}

// A restart from history takes every directory still carrying the job's
// marker, beside the one its record names and its staging directory; a
// reprocess keeps the directory it works in and takes the others.
#[tokio::test]
async fn a_restart_from_history_takes_the_job_s_left_working_directories() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30028);
    let mark = |dir: &std::path::Path| {
        std::fs::create_dir_all(dir.join("lantern.vol00+01.par2")).unwrap();
        std::fs::write(dir.join("lantern.vol00+01.par2/held"), b"held").unwrap();
        weaver_server_pipeline::jobs::working_dir::mark_weaver_owned_working_dir(
            &intermediate_dir,
            dir,
            job_id,
        )
        .unwrap();
    };
    let delivered = complete_dir.join("Lantern");
    std::fs::create_dir_all(&delivered).unwrap();
    weaver_server_pipeline::jobs::working_dir::mark_weaver_owned_output_dir(&delivered, job_id)
        .unwrap();
    let left = intermediate_dir.join("Lantern");
    mark(&left);
    let staging = staging_dir_for(&complete_dir, job_id);
    std::fs::create_dir_all(&staging).unwrap();

    clear_restart_artifacts(&pipeline, job_id, &delivered)
        .await
        .unwrap();

    assert!(!delivered.exists());
    assert!(!left.exists());
    assert!(!staging.exists());

    let kept = intermediate_dir.join("Lantern reprocess");
    let other = intermediate_dir.join("Lantern");
    mark(&kept);
    mark(&other);
    clear_left_working_dirs(&intermediate_dir, job_id, &kept)
        .await
        .unwrap();
    assert!(kept.join("lantern.vol00+01.par2/held").exists());
    assert!(!other.exists());
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn delete_history_reports_a_failed_cleanup_as_deleted_with_its_error() {
    // The record goes in the delete transaction; the directory only after the
    // job's scripts have stopped. A cleanup that fails in between must not
    // pass for a record that survived: the id is deleted, and its error rides
    // with it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let failing = JobId(30026);
    let clean = JobId(30027);
    let mut dirs = BTreeMap::new();
    for (job_id, name) in [
        (failing, "history-cleanup-fails"),
        (clean, "history-cleanup-ok"),
    ] {
        let output_dir = intermediate_dir.join(name);
        tokio::fs::create_dir_all(&output_dir).await.unwrap();
        tokio::fs::write(
            weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&output_dir),
            [],
        )
        .await
        .unwrap();
        database(&pipeline)
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                name,
                "failed",
                output_dir.clone(),
            ))
            .unwrap();
        dirs.insert(job_id, output_dir);
    }
    // A script still running for the failing job holds its cleanup at the
    // script-stop barrier, after its record is gone and before its directory
    // goes.
    let running = weaver_server_pipeline::post_processing::events::hold_test_event_run(
        database(&pipeline),
        failing.0,
        "history-cleanup-barrier",
    );

    report_all(&pipeline);
    let delete = start_history_delete(&pipeline, [failing, clean].into_iter().collect(), false);
    let db = database(&pipeline).clone();
    weaver_server_pipeline::post_processing::events::wait_for_test_script_change(&db, || {
        db.get_job_history(failing.0).unwrap().is_none()
    })
    .await;
    // The directory now names another job, so its removal is refused.
    tokio::fs::write(
        weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&dirs[&failing]),
        format!("weaver-job-v1:{}:{}\n", 99_999, "0".repeat(64)),
    )
    .await
    .unwrap();
    drop(running);
    let outcome = delete.await.unwrap().unwrap();

    assert!(outcome.refused.is_empty(), "{:?}", outcome.refused);
    let error = outcome.deleted[&failing]
        .as_ref()
        .expect_err("the failing id's cleanup error is reported");
    assert!(
        error.to_string().contains("no longer belongs"),
        "unexpected cleanup error: {error}"
    );
    assert!(outcome.deleted[&clean].is_ok());
    for job_id in [failing, clean] {
        assert!(
            database(&pipeline)
                .get_job_history(job_id.0)
                .unwrap()
                .is_none()
        );
    }
    assert!(dirs[&failing].exists());
    assert!(!dirs[&clean].exists());
}

#[tokio::test]
async fn a_rename_survives_restore_through_the_metadata() {
    let mut metadata = vec![("drone".to_string(), "abc".to_string())];
    weaver_server_pipeline::ingest::set_renamed_title(&mut metadata, "Renamed Release");
    let nzb = weaver_nzb::parse_nzb(&sample_nzb_bytes()).unwrap();
    let spec = weaver_server_pipeline::ingest::nzb_to_spec(
        &nzb,
        std::path::Path::new("Original.nzb"),
        None,
        metadata,
    );
    assert_eq!(spec.name, "Renamed Release");
    assert_eq!(
        weaver_server_pipeline::ingest::original_release_title(&spec.name, &spec.metadata),
        "Renamed Release"
    );
}

#[tokio::test]
async fn history_deletion_waits_for_scripts_without_blocking_commands() {
    for delete_all in [false, true] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30025);
        let output_dir = intermediate_dir.join("script-delete-job");
        tokio::fs::create_dir_all(&output_dir).await.unwrap();
        tokio::fs::write(
            weaver_server_pipeline::jobs::working_dir::working_dir_marker_path(&output_dir),
            [],
        )
        .await
        .unwrap();
        database(&pipeline)
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "Script Delete",
                "failed",
                output_dir.clone(),
            ))
            .unwrap();
        let running = weaver_server_pipeline::post_processing::events::hold_test_event_run(
            database(&pipeline),
            job_id.0,
            "deletion-barrier",
        );
        // The batch form also carries a second, script-free history row.
        let other_job = JobId(30027);
        let mut job_ids = BTreeSet::from([job_id]);
        if delete_all {
            database(&pipeline)
                .insert_job_history(&history_row_with_output_dir(
                    other_job,
                    "Script-free Delete",
                    "failed",
                    intermediate_dir.join("script-free-delete-job"),
                ))
                .unwrap();
            job_ids.insert(other_job);
        }
        report_all(&pipeline);
        let handle = handle(&pipeline);
        let mut deletion = Box::pin(handle.delete_history_batch(job_ids, false));
        tokio::select! {
            biased;
            result = &mut deletion => panic!("delete completed before its script barrier: {result:?}"),
            () = std::future::ready(()) => {},
        }
        assert!(output_dir.exists());
        let (diagnostics, ()) = tokio::join!(handle.pipeline_diagnostics(), async {
            answer_diagnostics_request(&pipeline);
        });
        diagnostics.unwrap();
        for reprocess in [false, true] {
            let result = if reprocess {
                handle.reprocess_job(job_id).await
            } else {
                handle.redownload_job(job_id).await
            };
            assert!(matches!(result, Err(SchedulerError::Conflict(_))));
        }

        // A job that completes while deletion drains must survive delete-all's completion.
        let newer_job = JobId(30026);
        insert_active_job(
            &mut pipeline,
            newer_job,
            standalone_job_spec("Newer completion", &[("new.bin".into(), 1)]),
        )
        .await;
        set_installed_status(&mut pipeline, newer_job, JobStatus::Complete);
        drop(running);
        let outcome = deletion.await.unwrap();
        assert!(outcome.refused.is_empty());
        assert!(outcome.deleted.contains_key(&job_id));
        assert_eq!(outcome.deleted.contains_key(&other_job), delete_all);
        assert!(!output_dir.exists());
        assert!(installed_status(&pipeline, newer_job).is_some());
        assert!(history_holds(&pipeline).is_empty());
    }
}

#[tokio::test]
async fn record_job_history_caps_finished_job_runtime_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let base_job_id = 900_000;
    fill_finished_history(&pipeline, base_job_id);
    let evicted_job_id = JobId(base_job_id + FINISHED_JOBS_RUNTIME_CAP as u64 - 1);

    let job_id = JobId(base_job_id + FINISHED_JOBS_RUNTIME_CAP as u64 + 1);
    let spec = standalone_job_spec(
        "Finished Runtime Cap",
        &[("finished-runtime-cap.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    set_installed_status(&mut pipeline, job_id, JobStatus::Complete);

    record_history(&mut pipeline, job_id);
    database(&pipeline).flush_write_queue().await.unwrap();

    assert_eq!(
        finished_jobs(&mut pipeline).await.len(),
        FINISHED_JOBS_RUNTIME_CAP
    );
    assert_eq!(
        finished_jobs(&mut pipeline).await.first().unwrap().job_id,
        job_id
    );
    assert!(
        finished_jobs(&mut pipeline)
            .await
            .into_iter()
            .all(|job| job.job_id != evicted_job_id)
    );
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn record_job_history_retains_failed_job_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30034);
    let spec = standalone_job_spec(
        "Failed History Retention",
        &[("episode.mkv".to_string(), 123)],
    );
    let stored_nzb = nzb_zstd_for(&spec);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let expected_nzb_path = database(&pipeline)
        .load_active_job_persisted_nzb(job_id)
        .unwrap()
        .unwrap()
        .0;

    set_installed_status(
        &mut pipeline,
        job_id,
        JobStatus::Failed {
            error: "boom".to_string(),
        },
    );
    record_history(&mut pipeline, job_id);
    database(&pipeline).flush_write_queue().await.unwrap();

    let history = database(&pipeline)
        .get_job_history(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(history.status, "failed");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(expected_nzb_path.to_str().unwrap())
    );
    let (_, nzb_zstd) = database(&pipeline)
        .load_history_job_persisted_nzb(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(nzb_zstd.unwrap(), stored_nzb);
}

#[tokio::test]
async fn record_job_history_retains_complete_job_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30036);
    let spec = standalone_job_spec(
        "Complete History Retention",
        &[("episode.mkv".to_string(), 123)],
    );
    let stored_nzb = nzb_zstd_for(&spec);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let expected_nzb_path = database(&pipeline)
        .load_active_job_persisted_nzb(job_id)
        .unwrap()
        .unwrap()
        .0;

    set_installed_status(&mut pipeline, job_id, JobStatus::Complete);
    record_history(&mut pipeline, job_id);
    database(&pipeline).flush_write_queue().await.unwrap();

    let history = database(&pipeline)
        .get_job_history(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(history.status, "complete");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(expected_nzb_path.to_str().unwrap())
    );
    let (_, nzb_zstd) = database(&pipeline)
        .load_history_job_persisted_nzb(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(nzb_zstd.unwrap(), stored_nzb);
}

#[tokio::test]
async fn job_failed_event_publishes_after_history_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = handle(&pipeline).subscribe_events();
    let job_id = JobId(30041);
    let spec = standalone_job_spec(
        "Durable Failure Event",
        &[("durable-failure.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let release = hold_database_writer(&pipeline);
    fail_job(&mut pipeline, job_id, "durable failure".to_string());
    settle_failed_releases(&mut pipeline).await;

    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "failure must not be announced while the history archive is still queued"
    );
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_none()
    );

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobFailed { job_id: id, .. } if *id == job_id),
    )
    .await;

    let archived = database(&pipeline).get_job_history(job_id.0).unwrap();
    assert_eq!(
        archived.map(|row| row.status),
        Some("failed".to_string()),
        "history row must be readable as soon as the failure event is observed"
    );
    let listed = database(&pipeline)
        .list_job_history(&weaver_server_pipeline::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == job_id.0));
}

// A failed job's working directory is what the history lists as its
// output: the graph's internals never stay in it, and one that delivered
// nothing goes whole.
#[tokio::test]
async fn a_failed_job_leaves_only_what_it_delivered_in_its_working_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let mut dirs = Vec::new();
    for (job_id, name, delivered) in [
        (JobId(30061), "Refused Nothing Delivered", None),
        (JobId(30062), "Refused One Delivered", Some("kept.bin")),
    ] {
        let spec = standalone_job_spec(name, &[("posted.zip".to_string(), 123)]);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        weaver_server_pipeline::jobs::working_dir::mark_weaver_owned_working_dir(
            &intermediate_dir,
            &working_dir,
            job_id,
        )
        .unwrap();
        let internal = working_dir.join(weaver_graph::names::INTERNAL_DIR);
        std::fs::create_dir_all(&internal).unwrap();
        std::fs::write(internal.join("graph-envelope-1-0"), b"posted bytes").unwrap();
        if let Some(delivered) = delivered {
            std::fs::write(working_dir.join(delivered), b"delivered").unwrap();
        }
        fail_job(&mut pipeline, job_id, "refused".to_string());
        // The history row waits on the release, which comes back as an
        // event the actor hears.
        settle_failed_releases(&mut pipeline).await;
        database(&pipeline).flush_write_queue().await.unwrap();
        assert!(
            database(&pipeline)
                .get_job_history(job_id.0)
                .unwrap()
                .is_some()
        );
        dirs.push((working_dir, delivered));
    }
    let (nothing, _) = &dirs[0];
    assert!(!nothing.exists(), "{} is still there", nothing.display());
    let (one, delivered) = &dirs[1];
    let mut left: Vec<String> = std::fs::read_dir(one)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(
        left,
        vec![
            weaver_server_pipeline::jobs::working_dir::WORKING_DIR_MARKER.to_string(),
            delivered.unwrap().to_string(),
        ]
    );
}

#[tokio::test]
async fn failed_history_row_keeps_why_the_job_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = handle(&pipeline).subscribe_events();
    let cases = [
        (
            30051,
            JobStatus::Repairing,
            "not enough recovery blocks",
            weaver_server_pipeline::history::HistoryFailureKind::Repair,
        ),
        (
            30052,
            JobStatus::Extracting,
            "invalid password for encrypted archive",
            weaver_server_pipeline::history::HistoryFailureKind::Password,
        ),
        (
            30053,
            JobStatus::Extracting,
            "write failed: No space left on device (os error 28)",
            weaver_server_pipeline::history::HistoryFailureKind::DiskFull,
        ),
        (
            30054,
            JobStatus::Extracting,
            "unexpected end of archive",
            weaver_server_pipeline::history::HistoryFailureKind::Unpack,
        ),
        (
            30055,
            JobStatus::Moving,
            "could not move the output",
            weaver_server_pipeline::history::HistoryFailureKind::Move,
        ),
        (
            30056,
            JobStatus::Downloading,
            "too many articles are missing",
            weaver_server_pipeline::history::HistoryFailureKind::Health,
        ),
    ];
    for (id, status, error, expected) in cases {
        let job_id = JobId(id);
        let spec = standalone_job_spec(
            &format!("Failure Kind {id}"),
            &[(format!("failure-kind-{id}.mkv"), 123)],
        );
        insert_active_job(&mut pipeline, job_id, spec).await;
        set_installed_status(&mut pipeline, job_id, status);
        fail_job(&mut pipeline, job_id, error.to_string());
        settle_failed_releases(&mut pipeline).await;
        wait_for_job_event(
            &mut events,
            |event| matches!(event, PipelineEvent::JobFailed { job_id: failed, .. } if *failed == job_id),
        )
        .await;
        let states = database(&pipeline).nzbget_history_states(&[id]).unwrap();
        assert_eq!(states[&id].failure_kind, Some(expected), "{error}");
    }
}

// A job restored in post-processing has its post-processing rows read with
// the rest of its install, off the pipeline thread; any other job reads none.
#[tokio::test]
async fn an_install_read_carries_the_post_processing_rows_of_a_job_in_post_processing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    for (job_id, status, carries) in [
        (JobId(31210), JobStatus::PostProcessing, true),
        (JobId(31211), JobStatus::QueuedPostProcessing, true),
        (JobId(31212), JobStatus::Downloading, false),
    ] {
        let working_dir = temp_dir.path().join(format!("restore-read-{}", job_id.0));
        tokio::fs::create_dir_all(&working_dir).await.unwrap();
        let request = RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: standalone_job_spec("Restore read", &[("sample.bin".to_string(), 100)]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        };
        let carries_post_processing =
            install_carries_post_processing(&pipeline, job_id, request).await;
        assert_eq!(carries_post_processing, carries, "{job_id:?}");
    }
}

#[tokio::test]
async fn restore_job_rehydrates_existing_deterministic_staging_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31207);
    let spec = standalone_job_spec(
        "Restore staged extraction",
        &[("sample.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-staged-extraction");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    let staging_dir = staging_dir_for(&complete_dir, job_id);
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    assert_eq!(installed_staging_dir(&pipeline, job_id), Some(staging_dir));
}

#[tokio::test]
async fn restore_job_normalizes_persisted_checking_to_queued_when_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    let spec = standalone_job_spec(
        "Restore Checking Pending",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    let working_dir = temp_dir.path().join("restore-checking-pending");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::Checking,
            download_state: Some(weaver_server_pipeline::jobs::model::DownloadState::Checking),
            post_state: Some(weaver_server_pipeline::jobs::model::PostState::Idle),
            run_state: Some(weaver_server_pipeline::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    let download_state = installed_download_state(&pipeline, job_id);
    assert!(matches!(
        download_state,
        weaver_server_pipeline::jobs::model::DownloadState::Queued
    ));
}

#[tokio::test]
async fn restore_job_normalizes_persisted_checking_to_complete_when_no_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30093);
    let spec = standalone_job_spec(
        "Restore Checking Complete",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    // A job with no article left to fetch.
    let spec = JobSpec {
        files: Vec::new(),
        ..spec
    };
    let working_dir = temp_dir.path().join("restore-checking-complete");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::Checking,
            download_state: Some(weaver_server_pipeline::jobs::model::DownloadState::Checking),
            post_state: Some(weaver_server_pipeline::jobs::model::PostState::Idle),
            run_state: Some(weaver_server_pipeline::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    let download_state = installed_download_state(&pipeline, job_id);
    assert!(matches!(
        download_state,
        weaver_server_pipeline::jobs::model::DownloadState::Complete
    ));
}

#[tokio::test]
async fn restore_queued_postprocessing_schedules_completion_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31201);
    let spec = standalone_job_spec("Restore queued repair", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-queued-repair");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::QueuedRepair,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    assert_eq!(pending_completion_checks(&pipeline), vec![job_id]);
    assert_eq!(
        installed_status(&pipeline, job_id).cloned(),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn restore_job_reemits_open_download_finalization_and_drains_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = handle(&pipeline).subscribe_events();
    let job_id = JobId(30094);
    let spec = standalone_job_spec(
        "Restore Finalizing Download",
        &[("payload.bin".to_string(), 100)],
    );
    // A job with no article left to fetch.
    let spec = JobSpec {
        files: Vec::new(),
        ..spec
    };
    let working_dir = temp_dir.path().join("restore-finalizing-download");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    database(&pipeline)
        .insert_job_events(&[
            weaver_server_pipeline::JobEvent {
                job_id: job_id.0,
                timestamp: 1,
                kind: "JobCreated".to_string(),
                message: "created".to_string(),
                file_id: None,
            },
            weaver_server_pipeline::JobEvent {
                job_id: job_id.0,
                timestamp: 2,
                kind: "DownloadFinished".to_string(),
                message: "download finished".to_string(),
                file_id: Some(
                    weaver_server_history::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER.to_string(),
                ),
            },
        ])
        .unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::Downloading,
            download_state: Some(weaver_server_pipeline::jobs::model::DownloadState::Complete),
            post_state: Some(weaver_server_pipeline::jobs::model::PostState::Idle),
            run_state: Some(weaver_server_pipeline::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    assert!(is_finalizing_download(&pipeline, job_id));

    pump_pipeline_runtime_queues(&mut pipeline).await;

    let drained_events = drain_job_events(&mut events, job_id);
    assert!(
        drained_events.iter().any(|event| {
            matches!(
                event,
                PipelineEvent::DownloadFinished {
                    finalization_pending: true,
                    ..
                }
            )
        }),
        "events: {drained_events:?}"
    );
    let finish_idx = drained_events
        .iter()
        .position(|event| {
            matches!(
                event,
                PipelineEvent::DownloadFinished {
                    finalization_pending: true,
                    ..
                }
            )
        })
        .expect("restored download finished event");
    let drained_idx = drained_events
        .iter()
        .position(|event| matches!(event, PipelineEvent::DownloadPipelineDrained { .. }))
        .expect("download pipeline drained event");
    assert!(finish_idx < drained_idx, "events: {drained_events:?}");
    assert!(!is_finalizing_download(&pipeline, job_id));
}

// A job another owner retired joins the finished jobs once, newest first,
// and the published queue lists it.
#[tokio::test]
async fn a_retired_job_joins_the_finished_jobs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30031);
    let mut info = finished_job_info(job_id);
    info.protocol = weaver_server_pipeline::jobs::model::Protocol::Torrent;
    let handle = handle(&pipeline);
    handle.retire_job(info.clone()).await.unwrap();
    handle.retire_job(info).await.unwrap();
    let finished_jobs = finished_jobs(&mut pipeline).await;
    let finished: Vec<JobId> = finished_jobs
        .iter()
        .filter(|job| job.job_id == job_id)
        .map(|job| job.job_id)
        .collect();
    assert_eq!(finished, [job_id]);
    assert_eq!(finished_jobs[0].job_id, job_id);
    let listed = published_job(&pipeline, job_id).unwrap();
    assert_eq!(
        listed.protocol,
        weaver_server_pipeline::jobs::model::Protocol::Torrent
    );
}

fn rename(name: &str) -> weaver_server_pipeline::jobs::JobUpdate {
    weaver_server_pipeline::jobs::JobUpdate {
        name: Some(name.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn record_job_history_purges_terminal_job_runtime_and_queue_metrics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Terminal Runtime Cleanup", &files);
    let job_id = JobId(30033);
    insert_active_job(&mut pipeline, job_id, spec).await;

    update_queue_metrics(&pipeline);
    assert!(
        metrics(&pipeline)
            .download_queue_depth
            .load(Ordering::Relaxed)
            > 0
    );

    set_installed_status(&mut pipeline, job_id, JobStatus::Complete);
    record_history(&mut pipeline, job_id);
    database(&pipeline).flush_write_queue().await.unwrap();

    assert!(database(&pipeline).load_active_jobs().unwrap().is_empty());
    let history = database(&pipeline).get_job_history(job_id.0).unwrap();
    assert!(history.is_some());
    assert_eq!(history.unwrap().status, "complete");
    assert!(installed_status(&pipeline, job_id).is_none());
    assert_eq!(
        metrics(&pipeline)
            .download_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline)
            .recovery_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        finished_jobs(&mut pipeline)
            .await
            .into_iter()
            .any(|job| job.job_id == job_id)
    );
}

#[tokio::test]
async fn renaming_a_quiet_job_preserves_existing_payload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    for (id, written) in [(30061, false), (30062, true)] {
        let job_id = JobId(id);
        let spec = standalone_job_spec(
            &format!("Before Rename {id}"),
            &[(format!("rename-{id}.mkv"), 123)],
        );
        let old_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        weaver_server_pipeline::jobs::working_dir::mark_weaver_owned_working_dir(
            &intermediate_dir,
            &old_dir,
            job_id,
        )
        .unwrap();
        // A quiet job holds no articles and so no graph: a graph keeps the
        // folder it was built in.
        release_job_articles(&mut pipeline, job_id);
        finish_retiring_graph(&mut pipeline, job_id).await;
        set_queued_status(&mut pipeline, job_id);
        if written {
            std::fs::write(old_dir.join("rename.part"), b"written").unwrap();
        }

        let new_name = format!("After Rename {id}");
        update_job(&mut pipeline, job_id, rename(&new_name)).await;

        let spec = installed_spec(&pipeline, job_id).unwrap();
        let dir = working_dir(&pipeline, job_id);
        assert_eq!(spec.name, new_name);
        assert_eq!(
            weaver_server_pipeline::ingest::completed_folder_name(&spec.name, &spec.metadata),
            new_name
        );
        assert_eq!(*dir, intermediate_dir.join(&new_name));
        assert!(!old_dir.exists());
        assert!(weaver_server_pipeline::jobs::working_dir::is_weaver_owned_working_dir(dir));
        if written {
            assert_eq!(std::fs::read(dir.join("rename.part")).unwrap(), b"written");
        }
    }
}

#[tokio::test]
async fn parking_a_job_keeps_its_working_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30063);
    let spec = standalone_job_spec("Parked Job", &[("parked.mkv".to_string(), 123)]);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    std::fs::write(working_dir.join("parked.part"), b"kept").unwrap();
    let done = cancel_one_job(
        &mut pipeline,
        job_id,
        weaver_server_pipeline::jobs::handle::CancellationOrigin::UserKeepingFiles,
    )
    .await;
    done.unwrap();
    assert!(installed_status(&pipeline, job_id).is_none());
    assert_eq!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    // Nothing was scheduled to remove the directory.
    assert!(working_dir.join("parked.part").exists());
}

#[tokio::test]
async fn updating_history_edits_the_row_and_the_finished_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = handle(&pipeline).subscribe_events();
    let job_id = JobId(30064);
    let spec = standalone_job_spec("Finished Job", &[("finished.mkv".to_string(), 123)]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    fail_job(
        &mut pipeline,
        job_id,
        "too many articles are missing".to_string(),
    );
    settle_failed_releases(&mut pipeline).await;
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobFailed { job_id: failed, .. } if *failed == job_id),
    )
    .await;

    let update = weaver_server_pipeline::jobs::JobUpdate {
        name: Some("Renamed Finished".to_string()),
        category: weaver_server_pipeline::jobs::FieldUpdate::Set("tv".to_string()),
        ..Default::default()
    };
    update_history(&pipeline, job_id, update).await.unwrap();

    let row = database(&pipeline)
        .get_job_history(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(row.name, "Renamed Finished");
    assert_eq!(row.category.as_deref(), Some("tv"));
    let metadata = weaver_server_pipeline::history::parse_history_metadata(row.metadata.as_deref());
    assert_eq!(
        weaver_server_pipeline::ingest::renamed_title(&metadata),
        Some("Renamed Finished")
    );
    let finished = published_job(&pipeline, job_id).unwrap();
    assert_eq!(finished.name, "Renamed Finished");
    assert_eq!(finished.category.as_deref(), Some("tv"));

    // A job still in the queue is not a history row; an unknown id is not
    // found.
    let queued = JobId(30065);
    let spec = standalone_job_spec("Still Queued", &[("queued.mkv".to_string(), 123)]);
    insert_active_job(&mut pipeline, queued, spec).await;
    for (id, conflict) in [(queued, true), (JobId(30066), false)] {
        report_all(&pipeline);
        let error = update_history(&pipeline, id, rename("Nope"))
            .await
            .unwrap_err();
        if conflict {
            assert!(
                matches!(error, weaver_server_pipeline::SchedulerError::Conflict(_)),
                "{error}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    weaver_server_pipeline::SchedulerError::JobNotFound(_)
                ),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn pause_clears_stale_completion_rechecks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31231);

    install_minimal_job(
        &mut pipeline,
        job_id,
        "paused-job",
        temp_dir.path().join("paused-job"),
    );
    schedule_completion_check(&mut pipeline, job_id);
    assert_eq!(pending_completion_checks(&pipeline), vec![job_id]);

    pause_runtime(&mut pipeline, job_id).unwrap();
    assert!(pending_completion_checks(&pipeline).is_empty());

    check_completion(&mut pipeline, job_id).await;
    assert_eq!(
        installed_status(&pipeline, job_id).cloned(),
        Some(JobStatus::Paused)
    );
}

#[tokio::test]
async fn job_completed_event_publishes_after_history_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = handle(&pipeline).subscribe_events();
    let job_id = JobId(30040);
    // A job with no article left to account for: its one file was delivered.
    insert_delivered_job(
        &mut pipeline,
        job_id,
        "Durable Completion Event",
        "durable-completion.mkv",
        b"durable completion payload",
    )
    .await;

    let release = hold_database_writer(&pipeline);
    complete_after_post_processing(&mut pipeline, job_id);

    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobCompleted { .. })),
        "completion must not be announced while the history archive is still queued"
    );
    assert!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .is_none()
    );

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id: id } if *id == job_id),
    )
    .await;

    // The facade refuses history lookups for jobs the scheduler still lists as
    // live, so the finished snapshot has to be published by the time the event
    // is observable.
    assert!(
        handle(&pipeline)
            .list_jobs()
            .iter()
            .all(|info| info.job_id != job_id || terminal_status(&info.status)),
        "completed job must be published as finished before its terminal event"
    );

    // Deliberately no flush_write_queue: a subscriber that reacts to the
    // terminal event must find the row through the same reads the history
    // facade uses.
    let archived = database(&pipeline).get_job_history(job_id.0).unwrap();
    assert_eq!(
        archived.map(|row| row.status),
        Some("complete".to_string()),
        "history row must be readable as soon as the completion event is observed"
    );
    let listed = database(&pipeline)
        .list_job_history(&weaver_server_pipeline::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == job_id.0));
}

// A torrent's unpacking folder carries its job's marker, and its job is a
// torrent row, in neither the queue's rows nor history. Housekeeping leaves
// it however old the marker is, live or left retiring, and whether or not
// the server runs torrents: the sweep goes by the rows alone. A marked
// folder no row accounts for still goes.
#[tokio::test]
async fn housekeeping_leaves_a_torrent_s_marked_folder_however_old() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let (live, retiring, rowless) = (JobId(30201), JobId(30202), JobId(30203));
    let live_dir = marked_working_dir(&intermediate_dir, "torrent-30201", live).await;
    let retiring_dir = marked_working_dir(&intermediate_dir, "torrent-30202", retiring).await;
    let rowless_dir = marked_working_dir(&intermediate_dir, "torrent-30203", rowless).await;
    for (job_id, byte) in [(live, 1_u8), (retiring, 2)] {
        database(&pipeline)
            .insert_torrent(&torrent(job_id.0, Some([byte; 20]), None), &[])
            .unwrap();
    }
    mark_torrents_retiring(&pipeline, &[(retiring, false)]).unwrap();
    report_all(&pipeline);

    sweep_job_files(&pipeline, None, after_every_marker()).await;

    assert!(live_dir.join("lantern.part01.rar").exists());
    assert!(retiring_dir.join("lantern.part01.rar").exists());
    assert!(!rowless_dir.exists());
    retire_pipeline_database(pipeline).await;
}

// A job that began staging in its category's folder finds that staging
// directory again after a restart, even with the category since deleted.
#[tokio::test]
async fn a_restart_finds_the_staging_directory_the_job_chose() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = || {
        (
            temp_dir.path().join("data"),
            temp_dir.path().join("intermediate"),
            temp_dir.path().join("complete"),
            temp_dir.path().join("weaver.db"),
        )
    };
    let backlog_base_bytes = || 16 * 1024 * 1024;
    let movies = temp_dir.path().join("movies");
    let (data, intermediate, complete, db) = roots();
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
    let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
        data,
        intermediate,
        complete,
        db,
        backlog_base_bytes(),
        0,
        None,
    )
    .await;
    set_categories(
        &pipeline,
        vec![weaver_server_pipeline::categories::CategoryConfig {
            id: 0,
            name: "movies".to_string(),
            dest_dir: Some(movies.display().to_string()),
            aliases: String::new(),
            upload_cap: Default::default(),
            seeding: None,
            torrent_route: false,
            unpack: None,
        }],
    )
    .await;
    let job_id = JobId(31208);
    let mut spec = segmented_job_spec("Restart staged", "payload.bin", &[100]);
    spec.category = Some("movies".to_string());
    let nzb_zstd = nzb_zstd_for(&spec);
    add_job_for_restore(
        &mut pipeline,
        job_id,
        spec.clone(),
        PathBuf::from("restart-staged.nzb"),
        nzb_zstd,
        weaver_server_pipeline::jobs::AddJobOptions::default(),
    )
    .await
    .unwrap();
    activate_job(&mut pipeline, job_id).await;
    let staging_dir = staging_dir_for(&movies, job_id);
    assert_eq!(
        installed_staging_dir(&pipeline, job_id),
        Some(staging_dir.clone())
    );
    let working_dir = working_dir(&pipeline, job_id).clone();
    retire_pipeline_database(pipeline).await;

    let (data, intermediate, complete, db) = roots();
    let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
        data,
        intermediate,
        complete,
        db,
        backlog_base_bytes(),
        0,
        None,
    )
    .await;
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    assert_eq!(installed_staging_dir(&pipeline, job_id), Some(staging_dir));
}

// Shutdown waits for a failed job's folder release however long it takes,
// and the job's history row is written when it reports. Left behind at the
// bound, the job would stay an active row with its folder perhaps already
// gone, and the next start would download it again. The clock is paused:
// the release reports an hour of virtual time after shutdown began.
#[tokio::test(start_paused = true)]
async fn a_failed_job_whose_folder_release_outlasts_the_shutdown_bound_is_still_recorded() {
    use weaver_server_base::runtime::shutdown_steps::SHUTDOWN_STEP_BOUND;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30042);
    let spec = standalone_job_spec(
        "Slow Release Failure",
        &[("slow-release.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    fail_job(
        &mut pipeline,
        job_id,
        "too many articles are missing".to_string(),
    );
    // The release ran; its report is handed back to the actor only long
    // after the bound on a step that writes nothing.
    let (released, send_release) = take_failed_release(&mut pipeline).await;
    assert_eq!(released, job_id);
    assert!(releasing_failed_job(&pipeline, job_id));
    tokio::spawn(async move {
        tokio::time::sleep(SHUTDOWN_STEP_BOUND * 360).await;
        send_release.await;
    });
    let started = tokio::time::Instant::now();

    drain_pipeline(&mut pipeline).await;

    assert!(started.elapsed() >= SHUTDOWN_STEP_BOUND * 360);
    assert!(!releasing_failed_job(&pipeline, job_id));
    assert_eq!(
        database(&pipeline)
            .get_job_history(job_id.0)
            .unwrap()
            .map(|row| row.status),
        Some("failed".to_string()),
        "the failure is recorded before the pipeline is down"
    );
}
