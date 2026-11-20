// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue many times longer than its slots. Each test pins what a pass
// visits or writes, never how long it takes.

use super::*;
use crate::history::queries::HistoryQueries;
use crate::jobs::handle::QueueMoveTarget;
use crate::jobs::outline_persistence::JobsOutlinePersistenceInternalQueries;
use crate::jobs::persistence::JobsPersistenceQueries;
use crate::jobs::queries::JobsInternalQueries;
use crate::jobs::queries::JobsQueries;
use crate::pipeline::queue_window::QUEUE_WINDOW;

use super::queue_scale_fixture::{
    QUEUED, enqueue, hold_writer, long_queue, row_walks, sql, turn_completes,
};
use super::window_fixture::settle as settle_window;

fn nzb_parses() -> u64 {
    crate::ingest::NZB_PARSES.load(Ordering::Relaxed)
}

// A long queue restarted, its restores queued and none installed. One of
// the two jobs holding a slot was moved to the bottom first, so it sits
// last. Returns the restart's order and each job's summaries before it.
struct Restarted {
    pipeline: Pipeline,
    handle: SchedulerHandle,
    order: Vec<JobId>,
    summaries: HashMap<JobId, Vec<crate::jobs::model::FileSummary>>,
    recovery_parses: u64,
}

const LATE_HOLDER: JobId = JobId(1);

async fn restarted_long_queue(temp: &tempfile::TempDir) -> Restarted {
    restarted_long_queue_with(temp, false).await
}

// As `restarted_long_queue`; with `unoutlined`, every job is stored as one
// admitted before outlines were, with no outline.
async fn restarted_long_queue_with(temp: &tempfile::TempDir, unoutlined: bool) -> Restarted {
    let (mut pipeline, handle) = long_queue(temp, 2).await;
    assert!(pipeline.nzb_admitted.contains(&LATE_HOLDER));
    handle
        .reorder_jobs(vec![(LATE_HOLDER, QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    let summaries = pipeline
        .jobs
        .iter()
        .map(|(id, state)| {
            let summaries = match &state.articles {
                crate::jobs::model::JobArticles::Queued(summaries) => summaries.0.clone(),
                _ => state
                    .spec
                    .files
                    .iter()
                    .map(crate::jobs::model::FileSummary::of)
                    .collect(),
            };
            (*id, summaries)
        })
        .collect();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(temp).await;
    // No list is read ahead, so every parse counted is the restart's.
    pipeline.next_up.depth = 0;
    if unoutlined {
        sql(&pipeline.db, "DELETE FROM active_job_outlines").await;
        sql(
            &pipeline.db,
            "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
        )
        .await;
    }
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let parses = nzb_parses();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    let recovery_parses = nzb_parses() - parses;
    assert_eq!(recovered.to_restore.len(), QUEUED as usize);
    // Recovery holds each job's files, never its article lists.
    for candidate in &recovered.to_restore {
        assert!(
            candidate
                .request
                .spec
                .files
                .iter()
                .all(|file| file.segments.is_empty()),
            "job {}",
            candidate.job_id.0
        );
    }
    let order: Vec<JobId> = recovered.to_restore.iter().map(|c| c.job_id).collect();
    assert_eq!(order.last(), Some(&LATE_HOLDER));
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    Restarted {
        pipeline,
        handle,
        order,
        summaries,
        recovery_parses,
    }
}

#[tokio::test]
async fn a_restart_restores_a_long_queue_a_batch_a_turn_holding_no_article_lists() {
    use crate::pipeline::completion::finalize::RUNTIME_WRITES;
    use crate::pipeline::orchestrator::RESTORE_BATCH;
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle: _handle,
        order,
        summaries: expected,
        recovery_parses,
    } = restarted_long_queue(&temp).await;
    // Recovery reads each job's stored outline and parses no stored NZB.
    assert_eq!(recovery_parses, 0);

    // Each turn installs one batch: the jobs holding a slot first, wherever
    // they stand, then the rest in queue order. It says more wait.
    let writes = RUNTIME_WRITES.load(Ordering::Relaxed);
    let parses = nzb_parses();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs.len(), RESTORE_BATCH);
    assert!(pipeline.restores_waiting);
    let mut first: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
    first.sort_unstable();
    first.extend(
        order
            .iter()
            .filter(|id| !pipeline.nzb_admitted.contains(id))
            .take(RESTORE_BATCH - first.len()),
    );
    assert!(first.contains(&LATE_HOLDER));
    assert!(first.iter().all(|id| pipeline.jobs.contains_key(id)));
    let mut turns = 1;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
        turns += 1;
    }
    // Only the window is installed: the first rows in queue order, and the
    // holder moved to the bottom. Every row stays.
    let window = QUEUE_WINDOW + 1;
    assert_eq!(pipeline.jobs.len(), window);
    assert!(pipeline.jobs.contains_key(&LATE_HOLDER));
    assert!(
        order[..QUEUE_WINDOW]
            .iter()
            .all(|id| pipeline.jobs.contains_key(id))
    );
    assert_eq!(
        pipeline.shared_state.queue.read(|inner| inner.jobs.len()),
        QUEUED as usize
    );
    assert_eq!(turns, window.div_ceil(RESTORE_BATCH));
    // A restore that changes nothing in its row writes nothing back.
    assert_eq!(RUNTIME_WRITES.load(Ordering::Relaxed), writes);
    // No job here ever started, so each waits its turn holding the
    // summaries its lists gave, and the install parses no NZB: a job's
    // lists are read when it is handed work.
    assert_eq!(nzb_parses(), parses);
    for (id, state) in &pipeline.jobs {
        match &state.articles {
            crate::jobs::model::JobArticles::Queued(summaries) => {
                assert_eq!(summaries.0, expected[id], "job {}", id.0)
            }
            _ => assert!(pipeline.nzb_admitted.contains(id), "job {}", id.0),
        }
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 2);
}

// A restart reads each job's file rows as it installs the job, not before
// the server answers: a row written after recovery is the one installed,
// for a job that was downloading and for one waiting its turn alike. The
// pass ends once, when the last restore is in.
#[tokio::test]
async fn a_restart_reads_each_jobs_file_rows_when_it_installs_it() {
    const DOWNLOADING: JobId = JobId(1);
    const WAITING: JobId = JobId(QUEUE_WINDOW as u64);
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    pipeline
        .db
        .set_active_job_runtime(
            DOWNLOADING,
            "downloading",
            Some("downloading"),
            Some("idle"),
            Some("active"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    assert_eq!(recovered.to_restore.len(), QUEUED as usize);
    assert!(
        recovered
            .to_restore
            .iter()
            .all(|candidate| candidate.request.file_identities.is_empty())
    );
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    let identity = |job_id: JobId| crate::jobs::ActiveFileIdentity {
        file_index: 0,
        source_filename: "payload.bin".to_string(),
        current_filename: "payload.bin".to_string(),
        canonical_filename: Some(format!("written-after-recovery-{}.bin", job_id.0)),
        classification: None,
        classification_source: crate::jobs::FileIdentitySource::Declared,
    };
    for job_id in [DOWNLOADING, WAITING] {
        pipeline
            .db
            .save_file_identities(job_id, &[identity(job_id)])
            .unwrap();
    }

    pipeline.apply_queue_intents().await;
    assert!(pipeline.restores_waiting);
    assert!(pipeline.restore_pass.is_some());
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    assert!(pipeline.restore_pass.is_none());
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW);
    assert_eq!(pipeline.jobs[&DOWNLOADING].status, JobStatus::Downloading);
    for job_id in [DOWNLOADING, WAITING] {
        assert_eq!(
            pipeline.jobs[&job_id].file_identities.get(&0),
            Some(&identity(job_id)),
            "job {}",
            job_id.0
        );
    }
    // Later turns start no second pass.
    pipeline.apply_queue_intents().await;
    assert!(pipeline.restore_pass.is_none());
}

// A file-row read that fails at install holds only that job, paused and
// blocked; resuming it reads again and installs it with its rows.
#[tokio::test]
async fn a_failed_file_row_read_blocks_the_job_and_resume_reads_again() {
    const DOWNLOADING: JobId = JobId(1);
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    pipeline
        .db
        .set_active_job_runtime(
            DOWNLOADING,
            "downloading",
            Some("downloading"),
            Some("idle"),
            Some("active"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    let identity = crate::jobs::ActiveFileIdentity {
        file_index: 0,
        source_filename: "payload.bin".to_string(),
        current_filename: "payload.bin".to_string(),
        canonical_filename: Some("kept-across-the-block.bin".to_string()),
        classification: None,
        classification_source: crate::jobs::FileIdentitySource::Declared,
    };
    pipeline
        .db
        .save_file_identities(DOWNLOADING, std::slice::from_ref(&identity))
        .unwrap();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    let rename = |db: &Database, sql: &'static str| {
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.write_exec(), sql, &[]).await?;
            Ok(())
        })
        .unwrap();
    };
    rename(
        &pipeline.db,
        "ALTER TABLE active_detected_archives RENAME TO hidden_detected_archives",
    );
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    assert!(pipeline.blocked_restores.contains_key(&DOWNLOADING));
    assert_eq!(pipeline.jobs[&DOWNLOADING].status, JobStatus::Paused);
    let stored = pipeline.db.load_active_job_rows().unwrap();
    assert_eq!(stored[&DOWNLOADING].status, "paused");

    rename(
        &pipeline.db,
        "ALTER TABLE hidden_detected_archives RENAME TO active_detected_archives",
    );
    pipeline.resume_restored_job(DOWNLOADING).await.unwrap();
    assert!(!pipeline.blocked_restores.contains_key(&DOWNLOADING));
    assert_eq!(
        pipeline.jobs[&DOWNLOADING].file_identities.get(&0),
        Some(&identity)
    );
}

// Between a restart's batches the actor still hears what is ready: a decode
// that completes while restores wait is heard before they are done.
#[tokio::test]
async fn a_decode_done_during_a_restart_is_heard_between_batches() {
    use crate::pipeline::orchestrator::DECODES_HEARD_RESTORING;
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle,
        ..
    } = restarted_long_queue(&temp).await;
    pipeline
        .decode_done_tx
        .send(DecodeDone::Failed {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(QUEUED),
                    file_index: 0,
                },
                segment_number: 0,
            },
            raw_size: 0,
            error: "synthetic decode failure".to_string(),
            servers: PoolServers::stamped(
                pipeline.pool_generation,
                None,
                weaver_nntp::ServerSet::EMPTY,
            ),
        })
        .await
        .unwrap();
    let queue = pipeline.shared_state.queue.clone();
    let mut revisions = queue.subscribe();
    let heard = DECODES_HEARD_RESTORING.load(Ordering::Relaxed);
    let runner = tokio::spawn(async move { pipeline.run().await });
    revisions
        .wait_for(|_| {
            queue.read(|inner| {
                inner
                    .jobs
                    .iter()
                    .all(|row| row.intents.restore.is_none() && !row.report.restoring)
            })
        })
        .await
        .unwrap();
    assert_eq!(DECODES_HEARD_RESTORING.load(Ordering::Relaxed), heard + 1);
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

#[tokio::test]
async fn a_restart_installs_its_restores_while_the_database_is_busy() {
    use crate::pipeline::orchestrator::RESTORE_BATCH;
    let temp = tempfile::tempdir().unwrap();
    let Restarted { mut pipeline, .. } = restarted_long_queue(&temp).await;
    let release = hold_writer(&pipeline.db).await;
    assert!(turn_completes(&mut pipeline));
    assert_eq!(pipeline.installing.len(), RESTORE_BATCH);
    assert!(pipeline.jobs.is_empty());
    release.send(()).unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs.len(), RESTORE_BATCH);
}

// On a long queue, the turn that hears a download result and the turn that
// refills a lane visit no queue row and ask the database nothing: neither
// grows with the queue.
#[tokio::test]
async fn a_result_turn_and_a_refill_turn_walk_no_row_and_ask_no_database_on_a_long_queue() {
    use crate::persistence::connection::actor_thread;
    use crate::pipeline::orchestrator::actor_db;
    const LONG: u64 = 2_500;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(14).unwrap())
        .await
        .unwrap();
    for id in 1..=LONG {
        enqueue(&mut pipeline, id).await;
    }
    settle_window(&mut pipeline).await;
    assert_eq!(pipeline.nzb_admitted.len(), 14);
    let job_id = *pipeline.nzb_admitted.iter().min().unwrap();
    activate_job(&mut pipeline, job_id).await;
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work = pipeline.dispatched(work);
    let segment_id = work.segment_id;
    pipeline.hold_test_lane_works(vec![work], false);

    let walks = row_walks();
    let awaited = actor_db::total();
    let called = actor_thread::total();
    let (response_tx, _response_rx) = tokio::sync::oneshot::channel();
    actor_thread::OnActor(Box::pin(async {
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::Fetch(
                    DownloadFailure::from_lane_acquire_failure(Some(
                        &weaver_nntp::NntpError::ConnectionClosed,
                    )),
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
        pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
            lane_id: 0,
            runtime_generation: 0,
            server_idx: 0,
            supports_pipelining: false,
            current_mode: DownloadLaneMode::Sequential,
            response_tx,
        });
    }))
    .await;
    assert_eq!(row_walks(), walks);
    assert_eq!(actor_db::total(), awaited);
    assert_eq!(actor_thread::total(), called);
}

// Hear the database's answers until no list read ahead is out.
async fn settle_next_up(pipeline: &mut Pipeline) {
    while pipeline.next_up.reading() {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

// The jobs that take the next slots, past the ones holding them, have their
// lists read ahead, and a job that takes a slot keeps its lists until it
// activates. A job that drops out of them, as one paused does, drops its
// lists, and the next job in order has its own read. A job that takes a slot
// activates on the lists read ahead, the same lists a read at activation
// gives, with no parse of its own.
#[tokio::test]
async fn the_jobs_next_up_hold_their_lists_read_ahead() {
    use crate::pipeline::next_up::NEXT_UP_DEPTH;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    settle_next_up(&mut pipeline).await;
    // Jobs 1 and 2 hold the slots and have not activated.
    let next: Vec<JobId> = (1..3 + NEXT_UP_DEPTH as u64).map(JobId).collect();
    assert_eq!(pipeline.next_up.held(), next);

    let paused = JobId(5);
    handle.pause_job(paused).await.unwrap();
    pipeline.apply_queue_intents().await;
    settle_next_up(&mut pipeline).await;
    let next: Vec<JobId> = (1..4 + NEXT_UP_DEPTH as u64)
        .map(JobId)
        .filter(|id| *id != paused)
        .collect();
    assert_eq!(pipeline.next_up.held(), next);

    // A job holding a slot activates on the lists read ahead.
    let job_id = JobId(1);
    let (category, metadata) = {
        let spec = &pipeline.jobs[&job_id].spec;
        (spec.category.clone(), spec.metadata.clone())
    };
    let fresh =
        crate::pipeline::activation::read_stored_spec(&pipeline.db, job_id, category, metadata)
            .unwrap();
    let parses = nzb_parses();
    pipeline.request_job_activation(job_id);
    let done = pipeline.job_activated_rx.recv().await.unwrap();
    pipeline.handle_job_activated(done);
    assert_eq!(nzb_parses(), parses);
    let state = &pipeline.jobs[&job_id];
    assert!(matches!(
        state.articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    let lists = |spec: &JobSpec| -> Vec<Vec<(u32, u32, u32, crate::jobs::MessageIds)>> {
        spec.files
            .iter()
            .map(|file| {
                file.segments
                    .iter()
                    .map(|segment| {
                        (
                            segment.ordinal,
                            segment.article_number,
                            segment.bytes,
                            segment.message_ids.clone(),
                        )
                    })
                    .collect()
            })
            .collect()
    };
    assert_eq!(lists(&state.spec), lists(&fresh));
    assert!(!pipeline.next_up.held().contains(&job_id));
}

// A job stored before outlines were restores with no NZB read before the
// server answers. Each such job reads its NZB once, wherever it stands, and
// stores the outline it reads: a job in the window installs, and one out of
// it reads its outline alone and waits as its row, which states its files.
// Each job then holds the summaries it held before the restart, and the next
// restart reads no NZB.
#[tokio::test]
async fn a_restart_fills_the_outlines_of_jobs_stored_without_one() {
    use crate::jobs::service::OUTLINE_ONLY_READS;
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle,
        order,
        summaries: expected,
        recovery_parses,
    } = restarted_long_queue_with(&temp, true).await;
    assert_eq!(recovery_parses, 0);
    let parses = nzb_parses();
    let outline_only = OUTLINE_ONLY_READS.load(Ordering::Relaxed);
    settle_window(&mut pipeline).await;
    assert_eq!(nzb_parses() - parses, QUEUED);
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW + 1);
    // Only the window and the late slot holder were installed.
    assert_eq!(
        OUTLINE_ONLY_READS.load(Ordering::Relaxed) - outline_only,
        QUEUED - (QUEUE_WINDOW as u64 + 1)
    );
    let parked = order[QUEUE_WINDOW];
    assert!(pipeline.window_parked(parked));
    assert!(
        order[..QUEUE_WINDOW]
            .iter()
            .all(|id| pipeline.jobs.contains_key(id))
    );
    for (id, state) in &pipeline.jobs {
        match &state.articles {
            crate::jobs::model::JobArticles::Queued(summaries) => {
                assert_eq!(summaries.0, expected[id], "job {}", id.0)
            }
            _ => assert!(pipeline.nzb_admitted.contains(id), "job {}", id.0),
        }
    }
    pipeline.db.flush_write_queue().await.unwrap();
    let outlines = pipeline.db.load_job_outlines(None).unwrap();
    assert_eq!(outlines.len(), QUEUED as usize);
    // A job that waits as its row states the files its outline read.
    let row = handle.get_job(parked).unwrap();
    assert_eq!(row.total_files as usize, outlines[&parked].files.len());
    assert_eq!(
        row.total_bytes,
        outlines[&parked]
            .files
            .iter()
            .map(|file| file.bytes)
            .sum::<u64>()
    );
    assert!(row.total_bytes > 0);
    // Its wanted and remaining bytes are what the job states once it
    // installs.
    let parked_figures = (row.wanted_bytes, row.remaining_bytes);
    assert!(parked_figures.0 > 0);
    handle
        .reorder_jobs(vec![(parked, QueueMoveTarget::Top)])
        .await
        .unwrap();
    settle_window(&mut pipeline).await;
    assert!(pipeline.jobs.contains_key(&parked));
    let installed = handle.get_job(parked).unwrap();
    assert_eq!(
        (installed.wanted_bytes, installed.remaining_bytes),
        parked_figures
    );
    // Every job installed before the restart holds the same files.
    for (id, summaries) in &expected {
        let files: Vec<(u32, u64)> = outlines[id]
            .files
            .iter()
            .map(|file| (file.articles, file.bytes))
            .collect();
        let summaries: Vec<(u32, u64)> = summaries
            .iter()
            .map(|file| (file.articles, file.bytes))
            .collect();
        assert_eq!(files, summaries, "job {}", id.0);
    }
}

// A job stored before outlines were whose stored NZB is gone, or does not
// parse, restores with no NZB read before the server answers, and fails as
// it installs: it goes to history with the reason, and its active rows and
// its slot go. A readable job stored the same way installs beside it.
#[tokio::test]
async fn a_restart_fails_the_jobs_stored_without_an_outline_whose_nzb_cannot_be_read() {
    let temp = tempfile::tempdir().unwrap();
    let slots = std::num::NonZeroU32::new(3).unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.set_max_concurrent_nzbs(slots).await.unwrap();
    for id in 1..=3 {
        enqueue(&mut pipeline, id).await;
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 3);
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    pipeline.next_up.depth = 0;
    sql(&pipeline.db, "DELETE FROM active_job_outlines").await;
    sql(
        &pipeline.db,
        "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
    )
    .await;
    sql(
        &pipeline.db,
        "UPDATE job_nzbs SET nzb_zstd = x'' WHERE job_id = 1",
    )
    .await;
    sql(
        &pipeline.db,
        "UPDATE job_nzbs SET nzb_zstd = x'01020304' WHERE job_id = 2",
    )
    .await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.set_max_concurrent_nzbs(slots).await.unwrap();
    let parses = nzb_parses();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    assert_eq!(nzb_parses(), parses);
    assert_eq!(recovered.to_restore.len(), 3);
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    settle_window(&mut pipeline).await;
    settle_failed_releases(&mut pipeline).await;
    // Each failed job's archive lands with the job's queued writes, and its
    // row leaves the queue then; the turn after lets its slot go, and hears
    // the slot's write.
    let failed = [JobId(1), JobId(2)];
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline.shared_state.queue.read(|inner| {
        for id in failed {
            assert!(!inner.contains_job(id), "job {}", id.0);
        }
    });
    pipeline.apply_queue_intents().await;
    pipeline.db.flush_write_queue().await.unwrap();

    let active = pipeline.db.load_active_jobs().unwrap();
    let admitted = pipeline.db.load_nzb_admissions().unwrap();
    for id in failed {
        let history = pipeline
            .db
            .get_job_history(id.0)
            .unwrap()
            .expect("history row");
        assert_eq!(history.status, "failed", "job {}", id.0);
        assert!(
            history
                .error_message
                .as_deref()
                .is_some_and(|error| error.starts_with("Cannot recover persisted NZB: ")),
            "job {}: {:?}",
            id.0,
            history.error_message
        );
        assert!(!active.contains_key(&id), "job {}", id.0);
        assert!(!admitted.contains(&id), "job {}", id.0);
        assert!(!pipeline.nzb_admitted.contains(&id), "job {}", id.0);
        assert!(!pipeline.jobs.contains_key(&id), "job {}", id.0);
    }
    let readable = JobId(3);
    assert!(active.contains_key(&readable));
    assert!(pipeline.db.get_job_history(readable.0).unwrap().is_none());
    let state = &pipeline.jobs[&readable];
    assert!(!state.spec.files.is_empty());
    assert!(state.failure_error.is_none());
    assert!(!pipeline.blocked_restores.contains_key(&readable));
}

// A long queue on a working disk whose free space the test sets, held to a
// reserve. What each pass does is counted, never timed, so the counts hold
// at any length: the same test at ten thousand jobs passes too.
mod disk_room {
    use super::*;
    use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
    use std::sync::atomic::AtomicU64;

    const LONG: u64 = 2_500;
    const SLOTS: u32 = 4;
    const RESERVE: u64 = 1 << 30;
    const NEVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);

    // Counted on every thread but a sampler's.
    fn disk_looks() -> u64 {
        weaver_server_disk::operations::disk::DISK_LOOKS.load(Ordering::Relaxed)
    }

    fn refresh_asks() -> u64 {
        weaver_server_disk::operations::disk::REFRESH_ASKS.load(Ordering::Relaxed)
    }

    fn visits() -> u64 {
        crate::pipeline::nzb_admission::ADMISSION_VISITS.with(std::cell::Cell::get)
    }

    // The disk under every root, read by its sampler alone: `room` over the
    // reserve, and the probes made on the thread the pipeline runs on.
    struct Disk {
        room: Arc<AtomicU64>,
        on_actor: Arc<AtomicU64>,
    }

    impl Disk {
        fn under(pipeline: &mut Pipeline) -> Self {
            let room = Arc::new(AtomicU64::new(RESERVE));
            let on_actor = Arc::new(AtomicU64::new(0));
            let actor = std::thread::current().id();
            let probe = {
                let (room, on_actor) = (Arc::clone(&room), Arc::clone(&on_actor));
                Arc::new(move |_: &std::path::Path| {
                    if std::thread::current().id() == actor {
                        on_actor.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(DiskSpace {
                        total_bytes: RESERVE * 64,
                        available_bytes: RESERVE + room.load(Ordering::Acquire),
                    })
                })
            };
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
            let mut limits = (*pipeline.extraction_limits).clone();
            limits.min_free_bytes = RESERVE;
            pipeline.extraction_limits = Arc::new(limits);
            Self { room, on_actor }
        }

        // The working disk reads `room` over the reserve: the test waits
        // for the reading that says so, whatever other probes ran first.
        // `room` differs from the room the disk read last.
        fn reads(&self, pipeline: &Pipeline, room: u64) {
            self.room.store(room, Ordering::Release);
            let capacity = &pipeline.storage_capacity;
            capacity.request_refresh(StorageRoot::Intermediate);
            capacity.wait_for_reading(StorageRoot::Intermediate, |reading| {
                reading.available_bytes == RESERVE + room
            });
        }
    }

    // Hears each admitted job's article read come back, so what the read
    // does is done before the test counts.
    async fn activations_heard(pipeline: &mut Pipeline) {
        while pipeline.jobs.values().any(|state| {
            matches!(
                state.articles,
                crate::jobs::model::JobArticles::Activating(..)
            )
        }) {
            let done = pipeline.job_activated_rx.recv().await.unwrap();
            pipeline.handle_job_activated(done);
        }
    }

    fn reasons(pipeline: &Pipeline) -> Vec<Option<String>> {
        pipeline.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .map(|row| row.report.job.download_wait_reason.clone())
                .collect()
        })
    }

    fn restated(before: &[Option<String>], after: &[Option<String>]) -> usize {
        before.iter().zip(after).filter(|(a, b)| a != b).count()
    }

    fn waiting_for_disk(pipeline: &Pipeline) -> usize {
        reasons(pipeline)
            .iter()
            .filter(|reason| reason.as_deref() == Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON))
            .count()
    }

    // A burst of adds at the end of the queue and moves among the jobs out
    // of the window, as a client sends them.
    async fn burst(pipeline: &mut Pipeline, handle: &SchedulerHandle, first: u64) {
        for id in first..first + 500 {
            enqueue(pipeline, id).await;
        }
        let moves: Vec<_> = (0..100)
            .map(|step| (JobId(LONG - step), QueueMoveTarget::Offset(-3)))
            .collect();
        handle.reorder_jobs(moves).await.unwrap();
        pipeline.apply_queue_intents().await;
    }

    // With the disk not short and then short, adds and moves of a long
    // queue ask the disk nothing on the pipeline's thread and
    // ring its sampler at most once, the walk looks at no more candidates
    // than the slots call for, and a hold that begins or ends restates no
    // more rows than there are slots.
    #[tokio::test]
    async fn a_long_queue_reads_the_disk_from_the_cache_short_or_not() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let disk = Disk::under(&mut pipeline);
        let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
        handle
            .set_max_concurrent_nzbs(std::num::NonZeroU32::new(SLOTS).unwrap())
            .await
            .unwrap();
        for id in 1..=LONG {
            enqueue(&mut pipeline, id).await;
        }
        settle_window(&mut pipeline).await;
        activations_heard(&mut pipeline).await;
        assert_eq!(pipeline.nzb_admitted.len(), SLOTS as usize);

        // Not short.
        let (looks, asks, walks, visited) = (disk_looks(), refresh_asks(), row_walks(), visits());
        burst(&mut pipeline, &handle, LONG + 1).await;
        assert_eq!(disk_looks(), looks, "nothing asked of the disk itself");
        assert_eq!(refresh_asks(), asks, "nor of its sampler");
        let walked = row_walks() - walks;
        assert!(walked > 0);
        assert!(
            visits() - visited <= walked,
            "each walk looked at one candidate, the slots being full"
        );
        assert!(!pipeline.admission_disk_held);
        assert_eq!(waiting_for_disk(&pipeline), 0);

        // The disk goes short, and a slot comes free: the walk judges the
        // disk once, shows the next job waiting for it, and starts none.
        disk.reads(&pipeline, 0);
        let before = reasons(&pipeline);
        let (looks, asks, walks, visited) = (disk_looks(), refresh_asks(), row_walks(), visits());
        handle
            .set_max_concurrent_nzbs(std::num::NonZeroU32::new(SLOTS + 1).unwrap())
            .await
            .unwrap();
        pipeline.apply_queue_intents().await;
        assert!(pipeline.admission_disk_held, "the hold began");
        assert_eq!(
            pipeline.nzb_admitted.len(),
            SLOTS as usize,
            "no job started"
        );
        assert_eq!(
            pipeline.disk_unadmitted.len(),
            1,
            "one free slot, one shown"
        );
        assert_eq!(waiting_for_disk(&pipeline), 1);
        assert!(restated(&before, &reasons(&pipeline)) <= 1);
        assert_eq!(disk_looks(), looks);
        assert_eq!(refresh_asks() - asks, 1, "the hold asked one early reading");
        let walked = row_walks() - walks;
        assert!(visits() - visited <= walked * u64::from(SLOTS + 1));

        // Short: a burst asks the disk nothing, rings no sampler again, and
        // each walk looks at no more candidates than there are slots.
        let (looks, asks, walks, visited) = (disk_looks(), refresh_asks(), row_walks(), visits());
        burst(&mut pipeline, &handle, LONG + 501).await;
        assert_eq!(disk_looks(), looks);
        assert_eq!(refresh_asks(), asks, "the hold is rung once, not per job");
        let walked = row_walks() - walks;
        assert!(walked > 0);
        assert!(visits() - visited <= walked * u64::from(SLOTS + 1));
        assert!(pipeline.disk_unadmitted.len() <= SLOTS as usize + 1);
        assert_eq!(pipeline.nzb_admitted.len(), SLOTS as usize);

        // A reading with room ends the hold: the job shown waiting starts,
        // and no more rows are restated than there are slots.
        disk.reads(&pipeline, 64 * RESERVE);
        let before = reasons(&pipeline);
        let looks = disk_looks();
        pipeline.disk_reading();
        assert_eq!(disk_looks(), looks, "the hold ends on the cached reading");
        pipeline.apply_queue_intents().await;
        assert!(!pipeline.admission_disk_held);
        assert_eq!(waiting_for_disk(&pipeline), 0);
        assert!(restated(&before, &reasons(&pipeline)) <= SLOTS as usize + 1);
        // The job that starts reads its articles off the pipeline's
        // thread: that asks the disk nothing either, its staging device
        // and the kind of storage it logs included, each read from the
        // disk owner's cache.
        assert_eq!(pipeline.nzb_admitted.len(), SLOTS as usize + 1);
        activations_heard(&mut pipeline).await;
        assert_eq!(disk_looks(), looks);
        assert_eq!(
            disk.on_actor.load(Ordering::Relaxed),
            0,
            "only the sampler's own thread probed"
        );
    }
}
