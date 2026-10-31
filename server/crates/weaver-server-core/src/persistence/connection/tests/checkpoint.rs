// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::feed_verdict::FeedVerdict;
use crate::jobs::graph_checkpoint::{Delta, NodeRow};
use crate::jobs::ids::JobId;
use crate::persistence::checkpoint_write::{CheckpointControl, CheckpointWrite};

fn checkpoint() -> Delta {
    let mut verdict = FeedVerdict::new("post cannot complete".into());
    verdict.abandoned_files.insert(2);
    Delta {
        feed_verdict: Some(verdict),
        whole: true,
        nodes: BTreeMap::from([(
            0,
            Some(NodeRow {
                node: 0,
                parent: None,
                parent_member: None,
                volumes: None,
                window: None,
                kind: "root".into(),
                state: "open".into(),
                reason: None,
                facts_version: 1,
                facts: vec![0],
                winning_password: None,
            }),
        )]),
        ..Delta::default()
    }
}

fn reject_verdict(db: &Database) {
    execute(
        db,
        "CREATE TRIGGER reject_verdict BEFORE UPDATE OF feed_verdict ON active_jobs BEGIN SELECT RAISE(FAIL, 'injected checkpoint failure'); END",
        vec![],
    );
}

#[tokio::test]
async fn checkpoint_cancelled_while_waiting_for_execution_does_not_write() {
    let db = Database::open_in_memory().unwrap();
    let job = JobId(4270);
    db.create_active_job(&postgres_sample_job(job)).unwrap();
    let permits = Arc::new(Semaphore::new(1));
    let held = permits.clone().acquire_owned().await.unwrap();
    let control = CheckpointControl::default();
    let write = CheckpointWrite {
        delta: Some(Arc::new(checkpoint())),
        control: control.clone(),
        blocked: Box::new(|_, _| panic!("cancelled checkpoint must not execute")),
        finished: Box::new(|result| assert!(result.is_err())),
    };
    let (_shutdown, stopped) = watch::channel(false);
    let run = write.run(job, &db, stopped, &permits);
    tokio::pin!(run);
    assert!(
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(run.as_mut(), cx))
        })
        .await
        .is_pending()
    );
    control.cancel();
    drop(held);
    assert!(!run.await.unwrap());
    assert!(db.load_graph_checkpoint(job).unwrap().is_none());
}

#[tokio::test]
async fn sqlite_paused_checkpoint_releases_execution_and_abandoned_flush() {
    paused_checkpoints_release_execution(false).await;
}

#[tokio::test]
async fn paused_checkpoints_do_not_exhaust_lane_execution_permits() {
    paused_checkpoints_release_execution(true).await;
}

async fn paused_checkpoints_release_execution(direct_lanes: bool) {
    let db = Database::open_in_memory().unwrap();
    let lanes = direct_lanes.then(|| spawn_lane_writer(&db, 2));
    let enqueue = |command| {
        if let Some((tx, _)) = &lanes {
            tx.try_send(QueuedWrite {
                queued_at: Instant::now(),
                command,
            })
            .unwrap();
        } else {
            db.try_send_with_retry(command, "checkpoint_probe").unwrap();
        }
    };
    reject_verdict(&db);
    let mut paused = Vec::new();
    // Fill every execution slot with a checkpoint awaiting explicit resume.
    for ordinal in 0..if direct_lanes { 2 } else { 1 } {
        let job = JobId(4260 + ordinal);
        db.create_active_job(&postgres_sample_job(job)).unwrap();
        let control = CheckpointControl::default();
        let retry = control.clone();
        let (blocked, mut failures) = mpsc::unbounded_channel();
        let (finished, completion) = oneshot::channel();
        enqueue(DbWriteCommand::GraphCheckpoint {
            job_id: job,
            prepare: Box::new(move || CheckpointWrite {
                delta: Some(Arc::new(checkpoint())),
                control: retry,
                blocked: Box::new(move |_, _| {
                    blocked.send(()).unwrap();
                }),
                finished: Box::new(move |result| {
                    finished.send(result).unwrap();
                }),
            }),
        });
        failures.recv().await.unwrap();
        paused.push((control, completion));
    }
    let (following, mut followed) = oneshot::channel();
    enqueue(DbWriteCommand::Write {
        label: "following_checkpoint",
        lane: Some(WriteLane::Job(4260)),
        op: Box::new(move |db| {
            assert!(db.load_graph_checkpoint(JobId(4260))?.is_some());
            following.send(()).unwrap();
            Ok(())
        }),
    });
    // A flush that reports a paused checkpoint drops its receiver. Such an
    // abandoned barrier must not hold unrelated jobs behind the paused lane.
    let (reply, abandoned) = oneshot::channel();
    enqueue(DbWriteCommand::Flush { reply });
    drop(abandoned);
    let other = JobId(4269);
    db.create_active_job(&postgres_sample_job(other)).unwrap();
    let (committed, archived) = oneshot::channel();
    enqueue(DbWriteCommand::ArchiveJob {
        job_id: other,
        history: Box::new(postgres_sample_history(other)),
        typed_terminal_cause: None,
        failure_kind: None,
        committed: Some(committed),
    });
    archived.await.unwrap();
    assert!(db.get_job_history(other.0).unwrap().is_some());
    assert!(matches!(
        followed.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(db.load_graph_checkpoint(JobId(4260)).unwrap().is_none());
    execute(&db, "DROP TRIGGER reject_verdict", vec![]);
    for (control, completion) in paused {
        control.resume();
        completion.await.unwrap().unwrap();
    }
    followed.await.unwrap();
    if let Some((tx, worker)) = lanes {
        flush_lane_writer(&tx).await;
        drop(tx);
        worker.await.unwrap();
    } else {
        db.flush_write_queue().await.unwrap();
    }
}

#[tokio::test]
async fn permanent_checkpoint_failure_reports_flush_preserves_order_and_replays_atomically() {
    let db = Database::open_in_memory().unwrap();
    let job = JobId(4243);
    db.create_active_job(&postgres_sample_job(job)).unwrap();
    reject_verdict(&db);
    let control = CheckpointControl::default();
    let retry = control.clone();
    let delta = Arc::new(checkpoint());
    let expected = delta.feed_verdict.clone();
    let (completed, completion) = oneshot::channel();
    db.try_queue_graph_checkpoint(job, move || CheckpointWrite {
        delta: Some(delta),
        control: retry,
        blocked: Box::new(|_, _| {}),
        finished: Box::new(move |result| {
            completed.send(result).unwrap();
        }),
    })
    .unwrap();
    let (later, mut observed) = oneshot::channel();
    db.try_queue_write("after_checkpoint", move |db| {
        let rows = db
            .load_graph_checkpoint(job)?
            .expect("checkpoint precedes later writes");
        later.send(rows.feed_verdict).unwrap();
        Ok(())
    })
    .unwrap();

    // This flush begins before the writer necessarily reaches the trigger.
    // It must observe the failure even while queued behind the retry slot.
    let error = db.flush_write_queue().await.unwrap_err();
    assert!(error.to_string().contains("injected checkpoint failure"));
    assert!(matches!(
        observed.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        db.load_graph_checkpoint(job).unwrap().is_none(),
        "the failed verdict rolls back the graph rows too"
    );
    assert_eq!(
        fetch_i64(
            &db,
            "SELECT COUNT(*) FROM active_jobs WHERE feed_verdict IS NOT NULL",
            vec![]
        ),
        0
    );

    execute(&db, "DROP TRIGGER reject_verdict", vec![]);
    control.resume();
    completion.await.unwrap().unwrap();
    assert_eq!(observed.await.unwrap(), expected);
    db.flush_write_queue().await.unwrap();
}

#[tokio::test]
async fn shutdown_reports_unwritten_checkpoint_and_does_not_acknowledge_it() {
    let db = Database::open_in_memory().unwrap();
    let job = JobId(4244);
    db.create_active_job(&postgres_sample_job(job)).unwrap();
    reject_verdict(&db);
    let (completed, completion) = oneshot::channel();
    db.try_queue_graph_checkpoint(job, move || CheckpointWrite {
        delta: Some(Arc::new(checkpoint())),
        control: CheckpointControl::default(),
        blocked: Box::new(|_, _| {}),
        finished: Box::new(move |result| {
            completed.send(result).unwrap();
        }),
    })
    .unwrap();
    assert!(db.flush_write_queue().await.is_err());
    db.stop_checkpoint_retries();
    let error = completion.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("injected checkpoint failure"));
    assert!(db.flush_write_queue().await.is_err());
    assert!(db.load_graph_checkpoint(job).unwrap().is_none());
}

#[tokio::test]
async fn successful_archives_clear_only_their_cancelled_checkpoint_failures() {
    let db = Database::open_in_memory().unwrap();
    reject_verdict(&db);
    let jobs = [JobId(4250), JobId(4251), JobId(4252)];
    for job in jobs {
        db.create_active_job(&postgres_sample_job(job)).unwrap();
        let control = CheckpointControl::default();
        let retry = control.clone();
        let (completed, completion) = oneshot::channel();
        let (blocked, mut failures) = tokio::sync::mpsc::unbounded_channel();
        db.try_queue_graph_checkpoint(job, move || CheckpointWrite {
            delta: Some(Arc::new(checkpoint())),
            control: retry,
            blocked: Box::new(move |_, _| {
                blocked.send(()).unwrap();
            }),
            finished: Box::new(move |result| {
                completed.send(result).unwrap();
            }),
        })
        .unwrap();
        // Observe this job's own failure; an earlier global flush error is
        // not evidence that this attempt reached the writer.
        failures.recv().await.unwrap();
        assert!(db.flush_write_queue().await.is_err());
        control.cancel();
        assert!(completion.await.unwrap().is_err());
    }
    db.try_queue_archive_job_with_terminal_cause(
        jobs[0],
        postgres_sample_history(jobs[0]),
        None,
        None,
    )
    .unwrap()
    .await
    .unwrap();
    assert!(db.flush_write_queue().await.is_err());
    assert!(!db.checkpoint_failures.borrow().contains_key(&jobs[0]));
    execute(
        &db,
        "CREATE TRIGGER reject_history BEFORE INSERT ON job_history WHEN NEW.job_id = 4252 BEGIN SELECT RAISE(ABORT, 'injected archive failure'); END",
        vec![],
    );
    let failed = db
        .try_queue_archive_jobs(
            jobs[1..]
                .iter()
                .map(|job| (*job, postgres_sample_history(*job)))
                .collect(),
            None,
        )
        .unwrap()
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert!(failed.contains_key(&jobs[2]));
    assert!(db.flush_write_queue().await.is_err());
    let remaining: Vec<_> = db.checkpoint_failures.borrow().keys().copied().collect();
    assert_eq!(remaining, vec![jobs[2]]);
    execute(&db, "DROP TRIGGER reject_history", vec![]);
    let failed = db
        .try_queue_archive_jobs(vec![(jobs[2], postgres_sample_history(jobs[2]))], None)
        .unwrap()
        .await
        .unwrap();
    assert!(failed.is_empty());
    db.flush_write_queue().await.unwrap();
    for job in jobs {
        assert!(db.load_graph_checkpoint(job).unwrap().is_none());
        assert!(db.get_job_history(job.0).unwrap().is_some());
    }
}
