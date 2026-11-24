// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The queue's one dispatch order against what each consumer computed for
// itself before it took its order from it, over seeded queues of mixed
// priorities with paused, admitted, cancelled, retiring and edited rows.

use weaver_server_composition::{JobId, QueueMoveTarget};
use weaver_server_pipeline::pipeline::tests::{
    dispatch_order_fixture::*, queue_scale_fixture::admitted_ids, window_fixture::settle,
};

fn sorted(mut ids: Vec<JobId>) -> Vec<JobId> {
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn every_consumer_ranks_as_it_did_before_the_shared_order() {
    let (_temp, pipeline, _handle) = seeded().await;
    assert_eq!(admitted_ids(&pipeline).len(), SLOTS as usize);

    let window = current_window(&pipeline);
    assert_eq!(window, former_window(&pipeline));
    assert!(window.contains(&JobId(121)), "a pending raise is in force");

    assert_eq!(
        canonical_admission_order(&pipeline),
        former_admission_order(&pipeline)
    );

    let eligible = eligible(&pipeline);
    assert!(!eligible.is_empty());
    assert_eq!(eligible, former_eligible(&pipeline));

    // The ring now reads a pending edit in force, as the window and the
    // admission pass do. With the edits applied the two agree.
    let ring = sorted(ring(&pipeline));
    let former = sorted(former_next_up(&pipeline));
    assert_ne!(ring, former, "the pending edits move the ring");
}

#[tokio::test]
async fn the_ring_ranks_as_before_once_every_edit_is_applied() {
    let (_temp, mut pipeline, _handle) = seeded().await;
    settle(&mut pipeline).await;
    let pending = pending_updates(&pipeline);
    assert!(!pending);
    assert_eq!(sorted(ring(&pipeline)), sorted(former_next_up(&pipeline)));
    let window = current_window(&pipeline);
    assert_eq!(window, former_window(&pipeline));
    assert_eq!(
        canonical_admission_order(&pipeline),
        former_admission_order(&pipeline)
    );
    assert_eq!(eligible(&pipeline), former_eligible(&pipeline));
}

// Moves, priority changes and pauses reach every consumer through the one
// order: after each, the window, the admission order and the ring are each
// the shared order filtered by what that consumer considers.
#[tokio::test]
async fn operator_moves_reach_every_consumer_the_same_way() {
    let (_temp, mut pipeline, _handle) = seeded().await;
    settle(&mut pipeline).await;
    let steps: [fn(&OrderEdits); 5] = [
        |queue| queue.reorder(JobId(140), QueueMoveTarget::Top),
        |queue| queue.reorder(JobId(30), QueueMoveTarget::Bottom),
        |queue| queue.reorder(JobId(33), QueueMoveTarget::Offset(-20)),
        |queue| queue.priority(JobId(149), "high"),
        |queue| queue.pause(JobId(42)),
    ];
    let queue = OrderEdits::new(&pipeline);
    for step in steps {
        step(&queue);
        settle(&mut pipeline).await;
        let order = canonical_admission_order(&pipeline);
        assert_eq!(order, former_admission_order(&pipeline));
        let window = current_window(&pipeline);
        assert_eq!(window, former_window(&pipeline));
        // The ring is the first jobs of the shared order that wait their
        // turn installed.
        let waiting = waiting_in_dispatch_order(&pipeline, &order);
        assert_eq!(sorted(ring(&pipeline)), sorted(waiting));
    }
}
