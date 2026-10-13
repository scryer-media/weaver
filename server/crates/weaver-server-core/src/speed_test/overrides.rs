use std::collections::HashMap;

use super::model::PathOverride;
use crate::proxies::{LegPath, ProxyPool, Route, RouteLeg, Rung};

// One leg of the route as a run tests it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestedLeg {
    Up {
        // The leg with its downed rungs removed.
        leg: RouteLeg,
        // For each rung the trimmed ladder dials, its position in the leg as
        // the route has it; the direct fallback, when kept, is last.
        rung_map: Vec<usize>,
    },
    Down {
        reason: String,
    },
}

impl TestedLeg {
    pub fn is_up(&self) -> bool {
        matches!(self, Self::Up { .. })
    }
}

// The route a run tests: every leg of the original, each either up with its
// trimmed path or down with its reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestedRoute {
    pub original: Route,
    pub legs: Vec<TestedLeg>,
}

impl PathOverride {
    // Derive the tested route. Positions that do not exist in the route are
    // refused with the leg and rung named; nothing is normalised silently.
    pub fn apply(
        &self,
        route: &Route,
        pools: &HashMap<u32, ProxyPool>,
    ) -> Result<TestedRoute, String> {
        for &position in &self.down_legs {
            if position >= route.legs.len() {
                return Err(format!(
                    "leg {position} does not exist; the route has {} legs",
                    route.legs.len()
                ));
            }
        }
        for &(position, rung) in &self.down_rungs {
            let leg = route.legs.get(position).ok_or_else(|| {
                format!(
                    "leg {position} does not exist; the route has {} legs",
                    route.legs.len()
                )
            })?;
            match &leg.path {
                LegPath::Direct => {
                    return Err(format!("leg {position} is direct and has no rung {rung}"));
                }
                LegPath::Ladder { rungs, .. } if rung >= rungs.len() => {
                    return Err(format!(
                        "leg {position} has no rung {rung}; it has {} rungs",
                        rungs.len()
                    ));
                }
                LegPath::Ladder { .. } => {}
            }
        }
        let legs = route
            .legs
            .iter()
            .enumerate()
            .map(|(position, leg)| self.tested_leg(position, leg, pools))
            .collect();
        Ok(TestedRoute {
            original: route.clone(),
            legs,
        })
    }

    fn tested_leg(
        &self,
        position: usize,
        leg: &RouteLeg,
        pools: &HashMap<u32, ProxyPool>,
    ) -> TestedLeg {
        if self.down_legs.contains(&position) {
            return TestedLeg::Down {
                reason: "leg taken down".into(),
            };
        }
        if self.down_egresses.contains(&leg.egress_id) {
            return TestedLeg::Down {
                reason: format!("egress {} taken down", leg.egress_id),
            };
        }
        let (rungs, direct_fallback) = match &leg.path {
            LegPath::Direct => {
                return TestedLeg::Up {
                    leg: leg.clone(),
                    rung_map: Vec::new(),
                };
            }
            LegPath::Ladder {
                rungs,
                direct_fallback,
            } => (rungs, *direct_fallback),
        };
        let mut kept = Vec::new();
        let mut rung_map = Vec::new();
        for (index, rung) in rungs.iter().enumerate() {
            if self.rung_is_down(position, index, rung, pools) {
                continue;
            }
            kept.push(rung.clone());
            rung_map.push(index);
        }
        if kept.is_empty() {
            if !direct_fallback {
                return TestedLeg::Down {
                    reason: "every rung is down".into(),
                };
            }
            return TestedLeg::Up {
                leg: RouteLeg {
                    path: LegPath::Direct,
                    ..leg.clone()
                },
                rung_map: Vec::new(),
            };
        }
        if direct_fallback {
            rung_map.push(rungs.len());
        }
        TestedLeg::Up {
            leg: RouteLeg {
                path: LegPath::Ladder {
                    rungs: kept,
                    direct_fallback,
                },
                ..leg.clone()
            },
            rung_map,
        }
    }

    fn rung_is_down(
        &self,
        position: usize,
        index: usize,
        rung: &Rung,
        pools: &HashMap<u32, ProxyPool>,
    ) -> bool {
        if self.down_rungs.contains(&(position, index)) {
            return true;
        }
        match rung {
            Rung::Proxy { id } => self.down_proxies.contains(id),
            Rung::Chain { ids } => ids.iter().any(|id| self.down_proxies.contains(id)),
            // A pool keeps the members still up; with none left it is down.
            Rung::Pool { id } => pools.get(id).is_some_and(|pool| {
                !pool.member_ids.is_empty()
                    && pool
                        .member_ids
                        .iter()
                        .all(|member| self.down_proxies.contains(member))
            }),
        }
    }
}

impl TestedRoute {
    pub fn healthy(&self) -> Vec<bool> {
        self.legs.iter().map(TestedLeg::is_up).collect()
    }

    pub fn any_up(&self) -> bool {
        self.legs.iter().any(TestedLeg::is_up)
    }

    // The route the draft is built from. Weights are kept as they are, so
    // the healthy mask and `Route::targets` give exactly the split the live
    // runtime would under the route's failover. A down leg is never dialed;
    // it drafts as a direct leg so nothing it referenced has to be built.
    pub fn drafted(&self) -> Route {
        Route {
            legs: self
                .original
                .legs
                .iter()
                .zip(&self.legs)
                .map(|(original, tested)| match tested {
                    TestedLeg::Up { leg, .. } => leg.clone(),
                    TestedLeg::Down { .. } => RouteLeg {
                        path: LegPath::Direct,
                        ..original.clone()
                    },
                })
                .collect(),
            failover: self.original.failover,
        }
    }

    // Connections per leg, as the live runtime would split `connections`
    // with these legs down.
    pub fn connection_targets(&self, connections: u16) -> Result<Vec<u16>, String> {
        self.original.targets(connections, &self.healthy())
    }

    // Streams per leg: largest-remainder shares of the weight across the
    // legs that are up, then at least one per leg that is up, taken from the
    // leg holding the most. With fewer streams than legs up, each still gets
    // one.
    pub fn stream_targets(&self, streams: u8) -> Vec<u16> {
        let streams = u32::from(streams);
        let healthy = self.healthy();
        let up: Vec<usize> = (0..self.legs.len()).filter(|i| healthy[*i]).collect();
        let mut targets = vec![0u16; self.legs.len()];
        if up.is_empty() {
            return targets;
        }
        let weight = |i: usize| u32::from(self.original.legs[i].weight);
        let total_weight: u32 = up.iter().map(|&i| weight(i)).sum();
        if total_weight == 0 {
            // Every leg that is up has weight zero; share them out evenly.
            for k in 0..(streams as usize).max(up.len()) {
                targets[up[k % up.len()]] += 1;
            }
            return targets;
        }
        let mut assigned = 0u32;
        for &i in &up {
            let share = streams * weight(i) / total_weight;
            targets[i] = share as u16;
            assigned += share;
        }
        // The streams left go to the largest remainders; a tie goes to the
        // heavier leg, then the earlier one.
        let mut order = up.clone();
        order.sort_by_key(|&i| {
            (
                std::cmp::Reverse(streams * weight(i) % total_weight),
                std::cmp::Reverse(weight(i)),
                i,
            )
        });
        for &i in order.iter().take((streams - assigned) as usize) {
            targets[i] += 1;
        }
        for &i in &up {
            if targets[i] > 0 {
                continue;
            }
            let donor = up
                .iter()
                .copied()
                .filter(|&d| targets[d] > 1)
                .max_by_key(|&d| {
                    (
                        targets[d],
                        std::cmp::Reverse(weight(d)),
                        std::cmp::Reverse(d),
                    )
                });
            if let Some(donor) = donor {
                targets[donor] -= 1;
            }
            targets[i] = 1;
        }
        targets
    }
}

#[cfg(test)]
#[path = "overrides_tests.rs"]
mod tests;
