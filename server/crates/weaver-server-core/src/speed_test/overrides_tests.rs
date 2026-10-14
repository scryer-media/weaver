use std::collections::HashMap;

use super::*;
use crate::proxies::{Failover, ProxyKind};

fn leg(egress_id: u32, weight: u8, path: LegPath) -> RouteLeg {
    RouteLeg {
        egress_id,
        weight,
        path,
    }
}

fn ladder(rungs: Vec<Rung>, direct_fallback: bool) -> LegPath {
    LegPath::Ladder {
        rungs,
        direct_fallback,
    }
}

fn route(legs: Vec<RouteLeg>, failover: Failover) -> Route {
    Route { legs, failover }
}

fn pools() -> HashMap<u32, ProxyPool> {
    HashMap::from([(
        7,
        ProxyPool {
            id: 7,
            name: "Pool".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![11, 12],
            enabled: true,
        },
    )])
}

fn down(overrides: PathOverride, route: &Route) -> TestedRoute {
    overrides.apply(route, &pools()).unwrap()
}

#[test]
fn nothing_down_keeps_every_leg_as_it_is() {
    let route = route(
        vec![
            leg(0, 60, LegPath::Direct),
            leg(
                1,
                40,
                ladder(vec![Rung::Proxy { id: 3 }, Rung::Pool { id: 7 }], true),
            ),
        ],
        Failover::Redistribute,
    );
    let tested = down(PathOverride::default(), &route);
    assert_eq!(
        tested.legs,
        vec![
            TestedLeg::Up {
                leg: route.legs[0].clone(),
                rung_map: vec![],
            },
            TestedLeg::Up {
                leg: route.legs[1].clone(),
                rung_map: vec![0, 1, 2],
            },
        ]
    );
    assert_eq!(tested.drafted(), route);
    assert_eq!(tested.healthy(), vec![true, true]);
}

#[test]
fn a_downed_leg_is_down_and_drafts_as_a_direct_leg_with_its_weight() {
    let route = route(
        vec![
            leg(0, 50, LegPath::Direct),
            leg(2, 50, ladder(vec![Rung::Proxy { id: 3 }], false)),
        ],
        Failover::Hold,
    );
    let tested = down(
        PathOverride {
            down_legs: vec![1],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        tested.legs[1],
        TestedLeg::Down {
            reason: "leg taken down".into()
        }
    );
    assert_eq!(
        tested.drafted().legs[1],
        leg(2, 50, LegPath::Direct),
        "weights are not renormalised"
    );
    assert_eq!(tested.drafted().failover, Failover::Hold);
}

#[test]
fn a_downed_egress_takes_down_every_leg_on_it() {
    let route = route(
        vec![
            leg(0, 40, LegPath::Direct),
            leg(4, 30, LegPath::Direct),
            leg(4, 30, ladder(vec![Rung::Proxy { id: 3 }], true)),
        ],
        Failover::Redistribute,
    );
    let tested = down(
        PathOverride {
            down_egresses: vec![4],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(tested.healthy(), vec![true, false, false]);
    assert_eq!(
        tested.legs[2],
        TestedLeg::Down {
            reason: "egress 4 taken down".into()
        }
    );
}

#[test]
fn a_downed_rung_is_removed_and_the_rest_keep_their_original_positions() {
    let route = route(
        vec![leg(
            0,
            100,
            ladder(
                vec![
                    Rung::Proxy { id: 3 },
                    Rung::Proxy { id: 4 },
                    Rung::Proxy { id: 5 },
                ],
                false,
            ),
        )],
        Failover::Redistribute,
    );
    let tested = down(
        PathOverride {
            down_rungs: vec![(0, 1)],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        tested.legs[0],
        TestedLeg::Up {
            leg: leg(
                0,
                100,
                ladder(vec![Rung::Proxy { id: 3 }, Rung::Proxy { id: 5 }], false)
            ),
            rung_map: vec![0, 2],
        }
    );
}

#[test]
fn a_downed_proxy_removes_its_rung_and_every_chain_it_is_a_hop_of() {
    let route = route(
        vec![
            leg(
                0,
                50,
                ladder(
                    vec![
                        Rung::Chain { ids: vec![3, 4] },
                        Rung::Proxy { id: 4 },
                        Rung::Proxy { id: 5 },
                    ],
                    false,
                ),
            ),
            leg(
                0,
                50,
                ladder(vec![Rung::Chain { ids: vec![5, 6, 4] }], true),
            ),
        ],
        Failover::Redistribute,
    );
    let tested = down(
        PathOverride {
            down_proxies: vec![4],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        tested.legs[0],
        TestedLeg::Up {
            leg: leg(0, 50, ladder(vec![Rung::Proxy { id: 5 }], false)),
            rung_map: vec![2],
        }
    );
    assert_eq!(
        tested.legs[1],
        TestedLeg::Up {
            leg: leg(0, 50, LegPath::Direct),
            rung_map: vec![],
        },
        "a ladder that loses every rung falls back to direct when it allows it"
    );
}

#[test]
fn a_pool_keeps_its_members_that_are_up_and_goes_with_its_last_one() {
    let route = route(
        vec![leg(
            0,
            100,
            ladder(vec![Rung::Pool { id: 7 }, Rung::Proxy { id: 3 }], false),
        )],
        Failover::Redistribute,
    );
    let one = down(
        PathOverride {
            down_proxies: vec![11],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        one.legs[0],
        TestedLeg::Up {
            leg: route.legs[0].clone(),
            rung_map: vec![0, 1],
        },
        "the draft drops the member; the rung stays"
    );
    let both = down(
        PathOverride {
            down_proxies: vec![11, 12],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        both.legs[0],
        TestedLeg::Up {
            leg: leg(0, 100, ladder(vec![Rung::Proxy { id: 3 }], false)),
            rung_map: vec![1],
        }
    );
}

#[test]
fn a_ladder_without_a_fallback_that_loses_every_rung_is_down() {
    let route = route(
        vec![
            leg(0, 50, ladder(vec![Rung::Proxy { id: 3 }], false)),
            leg(0, 50, LegPath::Direct),
        ],
        Failover::Redistribute,
    );
    let tested = down(
        PathOverride {
            down_rungs: vec![(0, 0)],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        tested.legs[0],
        TestedLeg::Down {
            reason: "every rung is down".into()
        }
    );
    assert_eq!(tested.drafted().legs[0], leg(0, 50, LegPath::Direct));
}

#[test]
fn the_direct_fallback_keeps_its_original_position() {
    let route = route(
        vec![leg(
            0,
            100,
            ladder(vec![Rung::Proxy { id: 3 }, Rung::Proxy { id: 4 }], true),
        )],
        Failover::Redistribute,
    );
    let tested = down(
        PathOverride {
            down_rungs: vec![(0, 0)],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(
        tested.legs[0],
        TestedLeg::Up {
            leg: leg(0, 100, ladder(vec![Rung::Proxy { id: 4 }], true)),
            rung_map: vec![1, 2],
        }
    );
}

#[test]
fn positions_that_do_not_exist_are_refused_by_name() {
    let route = route(
        vec![
            leg(0, 50, LegPath::Direct),
            leg(0, 50, ladder(vec![Rung::Proxy { id: 3 }], false)),
        ],
        Failover::Redistribute,
    );
    let refused = |overrides: PathOverride| overrides.apply(&route, &pools()).unwrap_err();
    assert_eq!(
        refused(PathOverride {
            down_legs: vec![2],
            ..Default::default()
        }),
        "leg 2 does not exist; the route has 2 legs"
    );
    assert_eq!(
        refused(PathOverride {
            down_rungs: vec![(0, 0)],
            ..Default::default()
        }),
        "leg 0 is direct and has no rung 0"
    );
    assert_eq!(
        refused(PathOverride {
            down_rungs: vec![(1, 1)],
            ..Default::default()
        }),
        "leg 1 has no rung 1; it has 1 rungs"
    );
    assert_eq!(
        refused(PathOverride {
            down_rungs: vec![(5, 0)],
            ..Default::default()
        }),
        "leg 5 does not exist; the route has 2 legs"
    );
}

#[test]
fn every_leg_down_leaves_nothing_up() {
    let route = route(
        vec![leg(0, 50, LegPath::Direct), leg(1, 50, LegPath::Direct)],
        Failover::Redistribute,
    );
    let tested = down(
        PathOverride {
            down_legs: vec![0],
            down_egresses: vec![1],
            ..Default::default()
        },
        &route,
    );
    assert!(!tested.any_up());
}

fn three_legs(failover: Failover) -> Route {
    route(
        vec![
            leg(0, 50, LegPath::Direct),
            leg(1, 30, LegPath::Direct),
            leg(2, 20, LegPath::Direct),
        ],
        failover,
    )
}

#[test]
fn redistribute_gives_a_downed_legs_connections_to_the_rest() {
    let route = three_legs(Failover::Redistribute);
    let all = down(PathOverride::default(), &route);
    assert_eq!(all.connection_targets(10).unwrap(), vec![5, 3, 2]);
    let middle = down(
        PathOverride {
            down_legs: vec![1],
            ..Default::default()
        },
        &route,
    );
    // 50:20 of 10 by largest remainder.
    assert_eq!(middle.connection_targets(10).unwrap(), vec![7, 0, 3]);
}

#[test]
fn hold_parks_a_downed_legs_connections() {
    let route = three_legs(Failover::Hold);
    let middle = down(
        PathOverride {
            down_legs: vec![1],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(middle.connection_targets(10).unwrap(), vec![5, 0, 2]);
    let first = down(
        PathOverride {
            down_legs: vec![0],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(first.connection_targets(7).unwrap(), vec![0, 2, 1]);
}

fn shares(tested: &TestedRoute, count: u32) -> Vec<u16> {
    weight_shares(&tested.weights(), &tested.healthy(), count)
}

#[test]
fn shares_follow_weight_with_at_least_one_per_leg_up() {
    let route = route(
        vec![leg(0, 60, LegPath::Direct), leg(1, 40, LegPath::Direct)],
        Failover::Redistribute,
    );
    let all = down(PathOverride::default(), &route);
    assert_eq!(
        shares(&all, 4),
        vec![2, 2],
        "2.4 and 1.6: the larger remainder wins"
    );
    assert_eq!(shares(&all, 5), vec![3, 2]);
    assert_eq!(shares(&all, 10), vec![6, 4]);
    assert_eq!(shares(&all, 1), vec![1, 1], "a leg never gets zero");
    let second = down(
        PathOverride {
            down_legs: vec![0],
            ..Default::default()
        },
        &route,
    );
    assert_eq!(shares(&second, 4), vec![0, 4]);
    let three = down(PathOverride::default(), &three_legs(Failover::Hold));
    assert_eq!(shares(&three, 5), vec![3, 1, 1]);
    let lopsided = Route {
        legs: vec![leg(0, 90, LegPath::Direct), leg(1, 10, LegPath::Direct)],
        failover: Failover::Redistribute,
    };
    let lopsided = down(PathOverride::default(), &lopsided);
    assert_eq!(
        shares(&lopsided, 4),
        vec![3, 1],
        "the light leg's one comes from the heavy leg"
    );
}

#[test]
fn the_next_connection_goes_to_the_leg_furthest_below_its_share() {
    let weights = [60, 40];
    let both = [true, true];
    let mut opened = vec![1u16, 1];
    let mut placed = Vec::new();
    for _ in 0..8 {
        let leg = next_leg(&weights, &both, &opened).unwrap();
        opened[leg] += 1;
        placed.push(leg);
    }
    assert_eq!(placed, vec![0, 1, 0, 0, 1, 0, 1, 0]);
    assert_eq!(opened, vec![6, 4], "ten connections split as the shares do");
    assert_eq!(
        next_leg(&[50, 50], &both, &[1, 1]),
        Some(0),
        "an even tie goes to the earlier leg"
    );
    assert_eq!(
        next_leg(&weights, &[false, true], &[3, 1]),
        Some(1),
        "a leg that is not usable is never picked"
    );
    assert_eq!(next_leg(&weights, &[false, false], &[1, 1]), None);
}
