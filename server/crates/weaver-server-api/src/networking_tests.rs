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

// A leg with nothing open is idle while a rung of its path may carry, and
// down for its first failing hop once every rung fails at one.
#[test]
fn an_unused_leg_whose_every_rung_fails_at_a_hop_is_down_not_idle() {
    use super::{NetworkFailingHop, NetworkLegFlow, RouteLegGql, settle_unused};
    use weaver_server_core::proxies::{LegPath, RouteLeg, Rung};
    let hop = |rung, proxy_id| NetworkFailingHop {
        rung,
        proxy_id,
        reason: format!("no handshake from proxy {proxy_id}"),
        code: "WIREGUARD_NO_HANDSHAKE".into(),
        params: async_graphql::Json(serde_json::Map::new()),
    };
    let leg = |failing_hops| NetworkLegFlow {
        consumer: "torrent:default".into(),
        position: 0,
        egress_id: 0,
        weight: 100,
        target: 0,
        open: 0,
        opening: 0,
        state: "UP".into(),
        reason: None,
        path: RouteLegGql::from(RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![Rung::Proxy { id: 3 }, Rung::Proxy { id: 4 }],
                direct_fallback: false,
            },
        })
        .path,
        pinned_address: None,
        source_address: None,
        bytes_per_second: 0,
        selected_rung: None,
        selected_proxy_id: None,
        rung_states: vec!["ACTIVE".into(), "STANDBY".into()],
        failing_hops,
    };

    let mut idle = leg(vec![hop(0, 3)]);
    settle_unused(&mut idle, 0);
    assert_eq!((idle.state.as_str(), idle.reason), ("IDLE", None));

    let mut down = leg(vec![hop(0, 3), hop(1, 4)]);
    settle_unused(&mut down, 0);
    assert_eq!(down.state, "DOWN");
    assert_eq!(
        down.reason.as_deref(),
        Some("proxy 3: no handshake from proxy 3")
    );

    // A leg carrying something is as its health says.
    let mut open = leg(vec![hop(0, 3), hop(1, 4)]);
    open.open = 1;
    settle_unused(&mut open, 0);
    assert_eq!((open.state.as_str(), open.reason), ("UP", None));

    // A leg a session is being built on is opening while a rung of its path
    // may carry, and down for its first failing hop once none may.
    let mut opening = leg(vec![hop(0, 3)]);
    settle_unused(&mut opening, 1);
    assert_eq!((opening.state.as_str(), opening.reason), ("OPENING", None));
    let mut failed = leg(vec![hop(0, 3), hop(1, 4)]);
    settle_unused(&mut failed, 1);
    assert_eq!(
        (failed.state.as_str(), failed.reason.as_deref()),
        ("DOWN", Some("proxy 3: no handshake from proxy 3"))
    );
}
