// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// History deletion finishes without a pipeline turn.
async fn delete_history_batch(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    delete_files: bool,
) -> crate::HistoryDeleteBatchOutcome {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .delete_history(job_ids.iter().copied().collect(), delete_files)
        .await
        .unwrap()
}

// Admission holds nothing back for memory: a job is added past a ledger
// with no room, and waits its turn charged nothing.
#[tokio::test]
async fn admission_reserves_nothing_per_article() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let spec = standalone_job_spec("Memory admission", &[("small.bin".to_string(), 16)]);
    pipeline.process_memory_budget = Arc::new(ProcessMemoryBudget::new(0));
    let resources = pipeline.admission_resources();
    let prepared = Pipeline::prepare_add(
        &resources,
        JobId(31001),
        spec,
        PathBuf::from("31001.nzb"),
        sample_nzb_zstd(),
        crate::jobs::AddJobOptions::default(),
    )
    .await
    .unwrap();
    pipeline.install_prepared_add(JobId(31001), prepared);
    assert_eq!(pipeline.process_memory_budget.reserved_bytes(), 0);
    retire_pipeline_database(pipeline).await;
}

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
        crate::jobs::working_dir::working_dir_marker_path(&output_dir),
        [],
    )
    .await
    .unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();

    pipeline
        .db
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
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
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
        crate::jobs::working_dir::working_dir_marker_path(&output_dir),
        format!("weaver-job-v1:{}:{}\n", job_id.0, "0".repeat(64)),
    )
    .await
    .unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();
    pipeline
        .db
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
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
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
            crate::jobs::working_dir::working_dir_marker_path(&output_dir),
            [],
        )
        .await
        .unwrap();
        pipeline
            .db
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
    let running = crate::post_processing::events::hold_test_event_run(
        &pipeline.db,
        failing.0,
        "history-cleanup-barrier",
    );

    pipeline.report_all_for_test();
    let delete = tokio::spawn(
        pipeline
            .shared_state
            .queue
            .clone()
            .delete_history([failing, clean].into_iter().collect(), false),
    );
    let db = pipeline.db.clone();
    crate::post_processing::events::wait_for_test_script_change(&db, || {
        db.get_job_history(failing.0).unwrap().is_none()
    })
    .await;
    // The directory now names another job, so its removal is refused.
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&dirs[&failing]),
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
        assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
    }
    assert!(dirs[&failing].exists());
    assert!(!dirs[&clean].exists());
}

#[tokio::test]
async fn delete_history_removes_db_only_history_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let output_dir = intermediate_dir.join("history-db-only-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&output_dir),
        [],
    )
    .await
    .unwrap();

    let row = history_row_with_output_dir(job_id, "History DB NZB", "failed", output_dir);
    pipeline.db.insert_job_history(&row).unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;
    assert!(outcome.refused.is_empty());

    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
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
            crate::jobs::working_dir::working_dir_marker_path(&output_dir),
            [],
        )
        .await
        .unwrap();
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "Script Delete",
                "failed",
                output_dir.clone(),
            ))
            .unwrap();
        let running = crate::post_processing::events::hold_test_event_run(
            &pipeline.db,
            job_id.0,
            "deletion-barrier",
        );
        // The batch form also carries a second, script-free history row.
        let other_job = JobId(30027);
        let mut job_ids = BTreeSet::from([job_id]);
        if delete_all {
            pipeline
                .db
                .insert_job_history(&history_row_with_output_dir(
                    other_job,
                    "Script-free Delete",
                    "failed",
                    intermediate_dir.join("script-free-delete-job"),
                ))
                .unwrap();
            job_ids.insert(other_job);
        }
        pipeline.report_all_for_test();
        let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
        let mut deletion = Box::pin(handle.delete_history_batch(job_ids, false));
        tokio::select! {
            biased;
            result = &mut deletion => panic!("delete completed before its script barrier: {result:?}"),
            () = std::future::ready(()) => {},
        }
        assert!(output_dir.exists());
        let (diagnostics, ()) = tokio::join!(handle.pipeline_diagnostics(), async {
            pipeline.answer_diagnostics_request();
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
        pipeline.jobs.get_mut(&newer_job).unwrap().status = JobStatus::Complete;
        drop(running);
        let outcome = deletion.await.unwrap();
        assert!(outcome.refused.is_empty());
        assert!(outcome.deleted.contains_key(&job_id));
        assert_eq!(outcome.deleted.contains_key(&other_job), delete_all);
        assert!(!output_dir.exists());
        assert!(pipeline.jobs.contains_key(&newer_job));
        assert!(
            pipeline
                .shared_state
                .queue
                .read(|inner| inner.history_operations.is_empty())
        );
    }
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
        crate::jobs::working_dir::working_dir_marker_path(&failed_output_dir),
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

    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            failed_job_id,
            "Failed History Cleanup",
            "failed",
            failed_output_dir.clone(),
        ))
        .unwrap();
    pipeline
        .db
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
        pipeline
            .db
            .list_job_history(&crate::HistoryFilter::default())
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn record_job_history_purges_terminal_job_runtime_and_queue_metrics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Terminal Runtime Cleanup", &files);
    let job_id = JobId(30033);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.update_queue_metrics();
    assert!(
        pipeline
            .metrics
            .download_queue_depth
            .load(Ordering::Relaxed)
            > 0
    );

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;
    pipeline.record_job_history(job_id, None);
    pipeline.db.flush_write_queue().await.unwrap();

    assert!(pipeline.db.load_active_jobs().unwrap().is_empty());
    let history = pipeline.db.get_job_history(job_id.0).unwrap();
    assert!(history.is_some());
    assert_eq!(history.unwrap().status, "complete");
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .download_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .recovery_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .any(|job| job.job_id == job_id)
    );
}

#[tokio::test]
async fn record_job_history_caps_finished_job_runtime_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let base_job_id = 900_000;
    pipeline.shared_state.queue.write(|inner| {
        let history = (0..crate::jobs::FINISHED_JOBS_RUNTIME_CAP)
            .map(|offset| finished_job_info(JobId(base_job_id + offset as u64)))
            .collect::<Vec<_>>()
            .into();
        pipeline.shared_state.queue.replace_history(inner, history);
        crate::pipeline::queue::Change::changed(())
    });
    let evicted_job_id = JobId(base_job_id + crate::jobs::FINISHED_JOBS_RUNTIME_CAP as u64 - 1);

    let job_id = JobId(base_job_id + crate::jobs::FINISHED_JOBS_RUNTIME_CAP as u64 + 1);
    let spec = standalone_job_spec(
        "Finished Runtime Cap",
        &[("finished-runtime-cap.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;

    pipeline.record_job_history(job_id, None);
    pipeline.db.flush_write_queue().await.unwrap();

    assert_eq!(
        pipeline.finished_jobs().await.len(),
        crate::jobs::FINISHED_JOBS_RUNTIME_CAP
    );
    assert_eq!(
        pipeline.finished_jobs().await.first().unwrap().job_id,
        job_id
    );
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .all(|job| job.job_id != evicted_job_id)
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_some());
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
    let expected_nzb_path = pipeline
        .db
        .load_active_job_persisted_nzb(job_id)
        .unwrap()
        .unwrap()
        .0;

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Failed {
        error: "boom".to_string(),
    };
    pipeline.record_job_history(job_id, None);
    pipeline.db.flush_write_queue().await.unwrap();

    let history = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(expected_nzb_path.to_str().unwrap())
    );
    let (_, nzb_zstd) = pipeline
        .db
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
    let expected_nzb_path = pipeline
        .db
        .load_active_job_persisted_nzb(job_id)
        .unwrap()
        .unwrap()
        .0;

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;
    pipeline.record_job_history(job_id, None);
    pipeline.db.flush_write_queue().await.unwrap();

    let history = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "complete");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(expected_nzb_path.to_str().unwrap())
    );
    let (_, nzb_zstd) = pipeline
        .db
        .load_history_job_persisted_nzb(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(nzb_zstd.unwrap(), stored_nzb);
}

// Park the serialized database writer until the returned sender is dropped, so
// a queued archive cannot commit while the test inspects the event stream.
pub(in crate::pipeline::tests) fn hold_database_writer(
    pipeline: &Pipeline,
) -> std::sync::mpsc::Sender<()> {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    pipeline
        .db
        .try_queue_write("test_hold_database_writer", move |_db| {
            let _ = release_rx.recv();
            Ok(())
        })
        .unwrap();
    release_tx
}

pub(in crate::pipeline::tests) async fn wait_for_job_event(
    events: &mut broadcast::Receiver<PipelineEvent>,
    is_expected: impl Fn(&PipelineEvent) -> bool,
) {
    async {
        loop {
            let event = events.recv().await.expect("pipeline event channel closed");
            if is_expected(&event) {
                return;
            }
        }
    }
    .await;
}

#[tokio::test]
async fn job_failed_event_publishes_after_history_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30041);
    let spec = standalone_job_spec(
        "Durable Failure Event",
        &[("durable-failure.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let release = hold_database_writer(&pipeline);
    pipeline.fail_job(job_id, "durable failure".to_string());
    settle_failed_releases(&mut pipeline).await;

    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "failure must not be announced while the history archive is still queued"
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobFailed { job_id: id, .. } if *id == job_id),
    )
    .await;

    let archived = pipeline.db.get_job_history(job_id.0).unwrap();
    assert_eq!(
        archived.map(|row| row.status),
        Some("failed".to_string()),
        "history row must be readable as soon as the failure event is observed"
    );
    let listed = pipeline
        .db
        .list_job_history(&crate::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == job_id.0));
}

// A failed job's working directory is what the history lists as its
// output: the graph's internals never stay in it, and one that delivered
// nothing goes whole.
#[tokio::test]
async fn a_failed_job_leaves_only_what_it_delivered_in_its_working_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut dirs = Vec::new();
    for (job_id, name, delivered) in [
        (JobId(30061), "Refused Nothing Delivered", None),
        (JobId(30062), "Refused One Delivered", Some("kept.bin")),
    ] {
        let spec = standalone_job_spec(name, &[("posted.zip".to_string(), 123)]);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        crate::jobs::working_dir::mark_weaver_owned_working_dir(
            &pipeline.intermediate_dir,
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
        pipeline.fail_job(job_id, "refused".to_string());
        // The history row waits on the release, which comes back as an
        // event the actor hears.
        settle_failed_releases(&mut pipeline).await;
        pipeline.db.flush_write_queue().await.unwrap();
        assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_some());
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
            crate::jobs::working_dir::WORKING_DIR_MARKER.to_string(),
            delivered.unwrap().to_string(),
        ]
    );
}

#[tokio::test]
async fn failed_history_row_keeps_why_the_job_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let cases = [
        (
            30051,
            JobStatus::Repairing,
            "not enough recovery blocks",
            crate::history::HistoryFailureKind::Repair,
        ),
        (
            30052,
            JobStatus::Extracting,
            "invalid password for encrypted archive",
            crate::history::HistoryFailureKind::Password,
        ),
        (
            30053,
            JobStatus::Extracting,
            "write failed: No space left on device (os error 28)",
            crate::history::HistoryFailureKind::DiskFull,
        ),
        (
            30054,
            JobStatus::Extracting,
            "unexpected end of archive",
            crate::history::HistoryFailureKind::Unpack,
        ),
        (
            30055,
            JobStatus::Moving,
            "could not move the output",
            crate::history::HistoryFailureKind::Move,
        ),
        (
            30056,
            JobStatus::Downloading,
            "too many articles are missing",
            crate::history::HistoryFailureKind::Health,
        ),
    ];
    for (id, status, error, expected) in cases {
        let job_id = JobId(id);
        let spec = standalone_job_spec(
            &format!("Failure Kind {id}"),
            &[(format!("failure-kind-{id}.mkv"), 123)],
        );
        insert_active_job(&mut pipeline, job_id, spec).await;
        pipeline.jobs.get_mut(&job_id).unwrap().status = status;
        pipeline.fail_job(job_id, error.to_string());
        settle_failed_releases(&mut pipeline).await;
        wait_for_job_event(
            &mut events,
            |event| matches!(event, PipelineEvent::JobFailed { job_id: failed, .. } if *failed == job_id),
        )
        .await;
        let states = pipeline.db.nzbget_history_states(&[id]).unwrap();
        assert_eq!(states[&id].failure_kind, Some(expected), "{error}");
    }
}

async fn update_job(pipeline: &mut Pipeline, job_id: JobId, update: crate::jobs::JobUpdate) {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .update_job(job_id, update)
        .unwrap();
    pipeline.apply_queue_intents().await;
}

fn rename(name: &str) -> crate::jobs::JobUpdate {
    crate::jobs::JobUpdate {
        name: Some(name.to_string()),
        ..Default::default()
    }
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
        crate::jobs::working_dir::mark_weaver_owned_working_dir(
            &intermediate_dir,
            &old_dir,
            job_id,
        )
        .unwrap();
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Queued;
        state.download_state = crate::jobs::model::DownloadState::Queued;
        if written {
            std::fs::write(old_dir.join("rename.part"), b"written").unwrap();
        }

        let new_name = format!("After Rename {id}");
        update_job(&mut pipeline, job_id, rename(&new_name)).await;

        let state = &pipeline.jobs[&job_id];
        assert_eq!(state.spec.name, new_name);
        assert_eq!(
            crate::ingest::completed_folder_name(&state.spec.name, &state.spec.metadata),
            new_name
        );
        assert_eq!(state.working_dir, intermediate_dir.join(&new_name));
        assert!(!old_dir.exists());
        assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(
            &state.working_dir
        ));
        if written {
            assert_eq!(
                std::fs::read(state.working_dir.join("rename.part")).unwrap(),
                b"written"
            );
        }
    }
}

#[tokio::test]
async fn a_rename_survives_restore_through_the_metadata() {
    let mut metadata = vec![("drone".to_string(), "abc".to_string())];
    crate::ingest::set_renamed_title(&mut metadata, "Renamed Release");
    let nzb = weaver_nzb::parse_nzb(&sample_nzb_bytes()).unwrap();
    let spec =
        crate::ingest::nzb_to_spec(&nzb, std::path::Path::new("Original.nzb"), None, metadata);
    assert_eq!(spec.name, "Renamed Release");
    assert_eq!(
        crate::ingest::original_release_title(&spec.name, &spec.metadata),
        "Renamed Release"
    );
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
        crate::jobs::handle::CancellationOrigin::UserKeepingFiles,
    )
    .await;
    done.unwrap();
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(
        pipeline
            .db
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
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30064);
    let spec = standalone_job_spec("Finished Job", &[("finished.mkv".to_string(), 123)]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.fail_job(job_id, "too many articles are missing".to_string());
    settle_failed_releases(&mut pipeline).await;
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobFailed { job_id: failed, .. } if *failed == job_id),
    )
    .await;

    let update = crate::jobs::JobUpdate {
        name: Some("Renamed Finished".to_string()),
        category: crate::jobs::FieldUpdate::Set("tv".to_string()),
        ..Default::default()
    };
    pipeline
        .shared_state
        .queue
        .clone()
        .update_history(job_id, update)
        .await
        .unwrap();

    let row = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(row.name, "Renamed Finished");
    assert_eq!(row.category.as_deref(), Some("tv"));
    let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
    assert_eq!(
        crate::ingest::renamed_title(&metadata),
        Some("Renamed Finished")
    );
    let finished = pipeline.shared_state.get_job(job_id).unwrap();
    assert_eq!(finished.name, "Renamed Finished");
    assert_eq!(finished.category.as_deref(), Some("tv"));

    // A job still in the queue is not a history row; an unknown id is not
    // found.
    let queued = JobId(30065);
    let spec = standalone_job_spec("Still Queued", &[("queued.mkv".to_string(), 123)]);
    insert_active_job(&mut pipeline, queued, spec).await;
    for (id, conflict) in [(queued, true), (JobId(30066), false)] {
        pipeline.report_all_for_test();
        let error = pipeline
            .shared_state
            .queue
            .clone()
            .update_history(id, rename("Nope"))
            .await
            .unwrap_err();
        if conflict {
            assert!(
                matches!(error, crate::SchedulerError::Conflict(_)),
                "{error}"
            );
        } else {
            assert!(
                matches!(error, crate::SchedulerError::JobNotFound(_)),
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

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "paused-job", temp_dir.path().join("paused-job")),
    );
    pipeline.schedule_job_completion_check(job_id);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );

    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(pipeline.pending_completion_checks.is_empty());

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
}

#[tokio::test]
async fn job_completed_event_publishes_after_history_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
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
    pipeline.complete_job_after_terminal_post_processing(job_id);

    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobCompleted { .. })),
        "completion must not be announced while the history archive is still queued"
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());

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
        pipeline
            .shared_state
            .list_jobs()
            .iter()
            .all(|info| info.job_id != job_id || is_terminal_status(&info.status)),
        "completed job must be published as finished before its terminal event"
    );

    // Deliberately no flush_write_queue: a subscriber that reacts to the
    // terminal event must find the row through the same reads the history
    // facade uses.
    let archived = pipeline.db.get_job_history(job_id.0).unwrap();
    assert_eq!(
        archived.map(|row| row.status),
        Some("complete".to_string()),
        "history row must be readable as soon as the completion event is observed"
    );
    let listed = pipeline
        .db
        .list_job_history(&crate::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == job_id.0));
}

#[tokio::test]
async fn restore_job_rehydrates_existing_deterministic_staging_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31207);
    let spec = standalone_job_spec(
        "Restore staged extraction",
        &[("sample.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-staged-extraction");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    let staging_dir = pipeline.deterministic_extraction_staging_dir(job_id);
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
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.staging_dir.clone()),
        Some(staging_dir)
    );
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
            download_state: Some(crate::jobs::model::DownloadState::Checking),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Queued
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
            download_state: Some(crate::jobs::model::DownloadState::Checking),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Complete
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
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn restore_job_reemits_open_download_finalization_and_drains_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
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

    pipeline
        .db
        .insert_job_events(&[
            crate::JobEvent {
                job_id: job_id.0,
                timestamp: 1,
                kind: "JobCreated".to_string(),
                message: "created".to_string(),
                file_id: None,
            },
            crate::JobEvent {
                job_id: job_id.0,
                timestamp: 2,
                kind: "DownloadFinished".to_string(),
                message: "download finished".to_string(),
                file_id: Some(
                    crate::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER.to_string(),
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
            download_state: Some(crate::jobs::model::DownloadState::Complete),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert!(pipeline.jobs_finalizing_download.contains(&job_id));

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
    assert!(!pipeline.jobs_finalizing_download.contains(&job_id));
}
