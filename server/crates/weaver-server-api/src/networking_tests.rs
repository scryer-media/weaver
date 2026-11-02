// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::pool_flow;
use std::time::{Duration, Instant};
use weaver_nntp::candidate_plan::CandidatePlan;
use weaver_server_core::proxies::PoolMemberStatus;

fn status(id: u32) -> PoolMemberStatus {
    PoolMemberStatus {
        id,
        open: 0,
        opening: 0,
        session_handshake: None,
        blocked: None,
        warmed: true,
        bytes_per_second: 0,
    }
}

#[test]
fn pool_member_rank_is_the_plan_dial_order() {
    let now = Instant::now();
    let mut plan = CandidatePlan::<u32>::default();
    plan.set_candidates(vec![1, 2, 3], now);
    // The pin is the slowest to connect: rank follows the plan, not the
    // connect times.
    plan.connected(3, Duration::from_millis(400), now);
    plan.connected(1, Duration::from_millis(200), now);
    plan.connected(2, Duration::from_millis(50), now);
    plan.lease_succeeded(3, now);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(3));
    assert_eq!(snapshot.dial_order, vec![3, 2, 1]);

    let flow = pool_flow(
        7,
        1,
        snapshot,
        vec![status(1), status(2), status(3), status(4)],
    );
    let ranks: Vec<_> = flow.members.iter().map(|m| (m.id, m.rank)).collect();
    assert_eq!(
        ranks,
        vec![(1, Some(2)), (2, Some(1)), (3, Some(0)), (4, None)]
    );
}
