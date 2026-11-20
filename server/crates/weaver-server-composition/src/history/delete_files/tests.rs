// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use tokio::sync::oneshot;

// A recorded folder is the job's working directory only as a plain child
// of the intermediate root: never a destination root that sits there,
// nor a folder delivered into one.
#[test]
fn a_recorded_destination_root_is_never_a_working_dir() {
    let intermediate = std::path::Path::new("/data/intermediate");
    let complete = intermediate.join("complete");
    let destinations = [complete.clone()];
    assert_eq!(
        working_dir_on_record(intermediate, &complete, &destinations),
        None
    );
    assert_eq!(
        working_dir_on_record(intermediate, &complete.join("show"), &destinations),
        None
    );
    let working = intermediate.join("show.42");
    assert_eq!(
        working_dir_on_record(intermediate, &working, &destinations),
        Some(working)
    );
}

// A job no script held when the batch was read, then given one before
// its turn, waits at its barrier: the jobs still to go are read again.
#[tokio::test]
async fn a_script_a_later_job_gains_after_the_batch_read_holds_its_barrier() {
    let db = crate::Database::open_in_memory().unwrap();
    let order = [61, 62, 63];
    let mut scripted = LiveScripts::read(&db, order.to_vec()).await;
    assert!(!scripted.waits(&db, &order[0..]).await);
    // Job 62's script starts while job 61's files go.
    let held = crate::post_processing::events::hold_test_event_run(&db, 62, "marked");
    assert!(scripted.waits(&db, &order[1..]).await);
    assert!(!scripted.waits(&db, &order[2..]).await);
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn history_cleanup_surfaces_persistent_failure_after_bounded_backoff() {
    let mut attempts = Vec::new();
    let error = retry_script_stop_wait(JobId(42), || {
        attempts.push(tokio::time::Instant::now());
        std::future::ready(Err(crate::StateError::Database(
            "fixture unavailable".into(),
        )))
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("after seven attempts"));
    assert_eq!(attempts.len(), 7);
    let gaps: Vec<_> = attempts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert_eq!(gaps, [1, 2, 4, 8, 16, 32].map(Duration::from_secs));
}

#[tokio::test(start_paused = true)]
async fn history_cleanup_retries_uncertain_wait_and_still_waits_for_active_script() {
    let db = crate::Database::open_in_memory().unwrap();
    let job_id = JobId(41);
    let active =
        crate::post_processing::events::hold_test_event_run(&db, job_id.0, "cleanup-retry");
    let (failed, failure_seen) = oneshot::channel();
    let (retried, retry_seen) = oneshot::channel();
    let cleanup = tokio::spawn(async move {
        let mut failed = Some(failed);
        let mut retried = Some(retried);
        retry_script_stop_wait(job_id, || {
            let failed = failed.take();
            let retried = if failed.is_none() {
                retried.take()
            } else {
                None
            };
            let db = db.clone();
            async move {
                if let Some(failed) = failed {
                    let _ = failed.send(());
                    return Err(crate::StateError::Database(
                        "injected transient read failure".into(),
                    ));
                }
                let _ = retried.unwrap().send(());
                crate::post_processing::events::wait_for_job_events_stopped(&db, job_id.0).await
            }
        })
        .await
        .unwrap();
    });
    failure_seen.await.unwrap();
    assert!(!cleanup.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    retry_seen.await.unwrap();
    assert!(!cleanup.is_finished());
    drop(active);
    cleanup.await.unwrap();
}
