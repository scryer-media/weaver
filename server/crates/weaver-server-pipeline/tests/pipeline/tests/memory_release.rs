// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// When the pipeline gives memory back: once it settles with nothing it can
// do, whether no job waits or jobs hold slots no server can serve, and after
// a turn that did something while it stays settled; never on an idle tick.

use std::time::Duration;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_server_pipeline::pipeline::tests::{
    memory_fixture::*, queue_fixture::apply_intents, queue_scale_fixture::has_work_to_drive,
};

// Jobs holding slots with no server set up: the ticks still run, as before,
// and the pipeline counts as settled once the ring's reads are in.
#[tokio::test]
async fn jobs_holding_slots_with_no_server_settle_once_the_ring_is_read() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = queue_with_reads_out(&temp).await;
    assert_eq!(server_count(&pipeline), 0);
    assert_eq!(admitted_count(&pipeline), 2);
    assert!(has_work_to_drive(&pipeline));
    // A read still out keeps it unsettled.
    assert_eq!(settled(&pipeline, true), !reading(&pipeline));
    settle_next_up(&mut pipeline).await;
    assert!(has_work_to_drive(&pipeline));
    assert!(settled(&pipeline, true));
}

// The same queue with a server set up has work it can do: it is not settled.
#[tokio::test]
async fn jobs_holding_slots_with_a_server_are_not_settled() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = queue_with_reads_out(&temp).await;
    settle_next_up(&mut pipeline).await;
    set_client(
        &mut pipeline,
        NntpClient::new(NntpClientConfig {
            servers: vec![weaver_nntp::pool::ServerPoolConfig {
                max_connections: 4,
                ..Default::default()
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }),
    );
    assert!(has_work_to_drive(&pipeline));
    assert!(!settled(&pipeline, true));
    assert!(settled(&pipeline, false));
}

// Under a pause over the whole queue nothing drives, and the pipeline is
// settled once the ring's reads are in.
#[tokio::test]
async fn a_paused_queue_settles_once_the_ring_is_read() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = queue_with_reads_out(&temp).await;
    handle.pause_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    assert!(!has_work_to_drive(&pipeline));
    assert!(settled(&pipeline, false));
}

// The release happens once per settling: never while unsettled, and not again
// on any turn while the pipeline stays settled.
#[test]
fn memory_goes_back_once_each_time_the_pipeline_settles() {
    // (settled, released)
    let turns = [
        (false, false), // start-up, reads out
        (false, false), // a read heard, more out
        (true, true),   // the last read heard: settled
        (true, false),  // an idle tick
        (true, false),  // a wake that changes nothing
        (false, false), // work starts
        (false, false), // a tick while it moves
        (true, true),   // the work ran out: settled again
        (true, false),  // an idle tick
    ];
    let mut was = false;
    for (index, (settled, released)) in turns.into_iter().enumerate() {
        assert_eq!(release_due(was, settled), released, "turn {index}");
        was = settled;
    }
}
