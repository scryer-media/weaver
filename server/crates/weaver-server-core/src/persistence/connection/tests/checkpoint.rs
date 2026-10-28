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
