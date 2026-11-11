// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// When the pipeline gives memory back: once it settles with nothing it can
// do, whether no job waits or jobs hold slots no server can serve, and after
// a turn that did something while it stays settled; never on an idle tick.

use super::*;

const QUEUED: u64 = 40;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Settled NZB {id}"), "payload.bin", &[512; 4]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("settled-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// A queue with two jobs holding slots, its ring's reads not yet heard.
async fn queue_with_reads_out(temp: &tempfile::TempDir) -> (Pipeline, SchedulerHandle) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    (pipeline, handle)
}

async fn settle_next_up(pipeline: &mut Pipeline) {
    while pipeline.next_up.reading() {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

// Jobs holding slots with no server set up: the ticks still run, as before,
// and the pipeline counts as settled once the ring's reads are in.
#[tokio::test]
async fn jobs_holding_slots_with_no_server_settle_once_the_ring_is_read() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = queue_with_reads_out(&temp).await;
    assert_eq!(pipeline.nntp.pool().server_count(), 0);
    assert_eq!(pipeline.nzb_admitted.len(), 2);
    assert!(pipeline.has_work_to_drive());
    // A read still out keeps it unsettled.
    assert_eq!(pipeline.memory_settled(true), !pipeline.next_up.reading());
    settle_next_up(&mut pipeline).await;
    assert!(pipeline.has_work_to_drive());
    assert!(pipeline.memory_settled(true));
}

// The same queue with a server set up has work it can do: it is not settled.
#[tokio::test]
async fn jobs_holding_slots_with_a_server_are_not_settled() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = queue_with_reads_out(&temp).await;
    settle_next_up(&mut pipeline).await;
    pipeline.nntp = Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            max_connections: 4,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    }));
    assert!(pipeline.has_work_to_drive());
    assert!(!pipeline.memory_settled(true));
    assert!(pipeline.memory_settled(false));
}

// Under a pause over the whole queue nothing drives, and the pipeline is
// settled once the ring's reads are in.
#[tokio::test]
async fn a_paused_queue_settles_once_the_ring_is_read() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = queue_with_reads_out(&temp).await;
    handle.pause_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    settle_next_up(&mut pipeline).await;
    assert!(!pipeline.has_work_to_drive());
    assert!(pipeline.memory_settled(false));
}

// The release follows a turn's answers: once on settling, once per turn that
// did something while settled, never on an idle tick or while unsettled.
#[test]
fn memory_goes_back_once_on_settling_and_after_each_active_turn() {
    // (settled, turn active, released)
    let turns = [
        (false, true, false),  // start-up, reads out
        (false, true, false),  // a read heard, more out
        (true, true, true),    // the last read heard: settled
        (true, false, false),  // an idle tick
        (true, false, false),  // another
        (true, true, true),    // an intent while settled
        (false, true, false),  // work starts
        (false, false, false), // a tick while it moves
        (true, false, true),   // settled on a tick: the work ran out
        (true, false, false),  // an idle tick
    ];
    let mut was = false;
    for (index, (settled, active, released)) in turns.into_iter().enumerate() {
        assert_eq!(
            Pipeline::memory_release_due(was, settled, active),
            released,
            "turn {index}"
        );
        was = settled;
    }
}
