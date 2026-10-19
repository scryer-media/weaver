// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How fast one direction of one egress may move.
//
// Every limit here is an `Option`: `None` is no limit, and `Some(n)` is a
// limit of `n` bytes per second, so `Some(0)` lets nothing through.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Download,
    Upload,
}

// Where an egress stands against its quota in one direction.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum QuotaState {
    #[default]
    Normal,
    // Past the soft cap: the direction moves at its trickle rate.
    Trickle,
    // At or past the hard cap: the direction moves nothing.
    Stopped,
}

impl QuotaState {
    // The state `used` bytes put a direction in. A cap of `Some(0)` is
    // reached before the first byte.
    pub fn of(used: u64, soft: Option<u64>, hard: Option<u64>) -> Self {
        if hard.is_some_and(|hard| used >= hard) {
            Self::Stopped
        } else if soft.is_some_and(|soft| used >= soft) {
            Self::Trickle
        } else {
            Self::Normal
        }
    }

    // The rate this state allows, given the direction's trickle rate.
    pub const fn rate(self, trickle_bps: u64) -> Option<u64> {
        match self {
            Self::Normal => None,
            Self::Trickle => Some(trickle_bps),
            Self::Stopped => Some(0),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Trickle => "trickle",
            Self::Stopped => "stopped",
        }
    }
}

// The limits that bear on one direction of one egress.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimits {
    // The operator's global limit.
    pub global: Option<u64>,
    // The global limit a schedule rule has in force.
    pub schedule: Option<u64>,
    // The egress's own cap.
    pub egress: Option<u64>,
    // The egress's quota state, and the trickle rate it falls to.
    pub quota: QuotaState,
    pub trickle_bps: u64,
}

// The tighter of two limits.
pub const fn tighter(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

// The rate one direction of one egress may move at: the tightest of every
// limit on it. The quota state is a floor of its own, so a schedule can
// lower the rate under it but never lift it.
pub const fn effective_rate(limits: RateLimits) -> Option<u64> {
    tighter(
        tighter(limits.global, limits.schedule),
        tighter(limits.egress, limits.quota.rate(limits.trickle_bps)),
    )
}

// How the global download limit is shared between usenet and torrents:
// usenet's percentage of it, torrents the rest. With no split set the two
// share it equally and borrow from each other freely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BandwidthSplit {
    pub usenet_pct: u8,
}

impl Default for BandwidthSplit {
    fn default() -> Self {
        Self { usenet_pct: 50 }
    }
}

impl BandwidthSplit {
    pub const MIN_PCT: u8 = 1;
    pub const MAX_PCT: u8 = 99;

    pub fn new(usenet_pct: u8) -> Result<Self, String> {
        let split = Self { usenet_pct };
        split.validate().map(|()| split)
    }

    pub fn validate(&self) -> Result<(), String> {
        if (Self::MIN_PCT..=Self::MAX_PCT).contains(&self.usenet_pct) {
            Ok(())
        } else {
            Err(format!(
                "a bandwidth split gives usenet {}% to {}%",
                Self::MIN_PCT,
                Self::MAX_PCT
            ))
        }
    }

    // Usenet's and torrents' shares of `cap`, adding up to it exactly.
    pub const fn shares(&self, cap: u64) -> [u64; 2] {
        let usenet = (cap as u128 * self.usenet_pct as u128 / 100) as u64;
        [usenet, cap - usenet]
    }
}

// What usenet and torrents each downloaded at over the last tick, in bytes
// per second.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitUse {
    pub usenet: u64,
    pub torrent: u64,
}

// The download rate each protocol may move at. `None` is no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitRates {
    pub usenet: Option<u64>,
    pub torrent: Option<u64>,
}

// The rates torrents move at, one per direction: their side of the download
// split, and the whole upload limit, since usenet uploads nothing. `None` is
// no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TorrentRates {
    pub download: Option<u64>,
    pub upload: Option<u64>,
}

// The split as it stands, as the API shows it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitState {
    // The operator's split.
    pub stored: Option<BandwidthSplit>,
    // The split in force, a scheduled one over the operator's.
    pub in_force: Option<BandwidthSplit>,
    // The global download limit in force. Without one the split does
    // nothing.
    pub cap: Option<u64>,
    pub rates: SplitRates,
}

// A side using at least this percentage of its share wants all of it.
const DEMAND_PCT: u128 = 95;
// What a side leaves unused is lent in whole steps of this fraction of the
// cap, so the rates hold still while throughput wobbles inside a step.
const LEND_STEPS: u64 = 20;

// Each side's rate out of `cap`, work-conserving. `shares` add up to `cap`
// and `used` is what each side moved at over the last tick.
//
// A side using at least 95% of its share wants it all and gets it. A side
// using less lends what it leaves, in whole twentieths of the cap, to the
// sides that want theirs, split between them by their shares. A lender keeps
// its own share as its limit, so the tick after it starts moving again shows
// it at its share and every share is back: one tick. While every side wants
// its share the rates add up to `cap` exactly; while one lends, the rates add
// up to `cap` and what was lent, and the lender moves at what it uses.
//
// This is the one predicate behind the usenet/torrent split, and behind the
// torrent rate's sharing between torrent sessions.
pub fn share_out(cap: u64, shares: &[u64], used: &[u64]) -> Vec<u64> {
    debug_assert_eq!(shares.len(), used.len());
    debug_assert_eq!(shares.iter().sum::<u64>(), cap);
    let step = (cap / LEND_STEPS).max(1);
    let wants = |side: usize| used[side] as u128 * 100 >= shares[side] as u128 * DEMAND_PCT;
    let lent: u64 = (0..shares.len())
        .filter(|&side| !wants(side))
        .map(|side| shares[side].saturating_sub(used[side]) / step * step)
        .sum();
    let wanting: u64 = (0..shares.len())
        .filter(|&side| wants(side))
        .map(|side| shares[side])
        .sum();
    let mut rates = shares.to_vec();
    if lent == 0 || wanting == 0 {
        return rates;
    }
    // The last side that wants more takes what rounding leaves, so the
    // lent bandwidth is handed out whole.
    let mut handed = 0;
    let last = (0..shares.len()).rev().find(|&side| wants(side));
    for side in (0..shares.len()).filter(|&side| wants(side)) {
        let part = if Some(side) == last {
            lent - handed
        } else {
            (lent as u128 * shares[side] as u128 / wanting as u128) as u64
        };
        handed += part;
        rates[side] += part;
    }
    rates
}

// The download rate usenet and torrents each move at under the global limit
// `cap` in force. Without a limit there is nothing to split; without a split
// the two share the limit equally, so together they never pass it.
pub fn split_rates(cap: Option<u64>, split: Option<BandwidthSplit>, used: SplitUse) -> SplitRates {
    let Some(cap) = cap else {
        return SplitRates::default();
    };
    let split = split.unwrap_or_default();
    let [usenet, torrent] = share_out(cap, &split.shares(cap), &[used.usenet, used.torrent])[..]
    else {
        unreachable!("two sides in, two rates out")
    };
    SplitRates {
        usenet: Some(usenet),
        torrent: Some(torrent),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RateLimits {
        RateLimits {
            trickle_bps: 2_048,
            ..RateLimits::default()
        }
    }

    #[test]
    fn no_limit_anywhere_is_unlimited() {
        assert_eq!(effective_rate(limits()), None);
    }

    #[test]
    fn the_tightest_limit_wins() {
        let rate = |global, schedule, egress| {
            effective_rate(RateLimits {
                global,
                schedule,
                egress,
                ..limits()
            })
        };
        assert_eq!(rate(Some(500), None, None), Some(500));
        assert_eq!(rate(None, Some(400), None), Some(400));
        assert_eq!(rate(None, None, Some(300)), Some(300));
        assert_eq!(rate(Some(500), Some(400), Some(300)), Some(300));
        assert_eq!(rate(Some(100), Some(400), Some(300)), Some(100));
        // A schedule tighter than the global limit lowers it; a looser one
        // does not lift it.
        assert_eq!(rate(Some(500), Some(900), None), Some(500));
    }

    #[test]
    fn a_zero_limit_is_zero_at_every_level() {
        for limits in [
            RateLimits {
                global: Some(0),
                ..limits()
            },
            RateLimits {
                schedule: Some(0),
                egress: Some(1_000),
                ..limits()
            },
            RateLimits {
                egress: Some(0),
                global: Some(1_000),
                ..limits()
            },
        ] {
            assert_eq!(effective_rate(limits), Some(0));
        }
    }

    #[test]
    fn a_quota_state_is_a_floor_a_schedule_cannot_lift() {
        let trickle = RateLimits {
            quota: QuotaState::Trickle,
            schedule: Some(1_000_000),
            ..limits()
        };
        assert_eq!(effective_rate(trickle), Some(2_048));
        let lower = RateLimits {
            schedule: Some(1_000),
            ..trickle
        };
        assert_eq!(effective_rate(lower), Some(1_000));
        let stopped = RateLimits {
            quota: QuotaState::Stopped,
            global: Some(1_000_000),
            ..limits()
        };
        assert_eq!(effective_rate(stopped), Some(0));
    }

    const MB: u64 = 1_000_000;

    fn split(pct: u8) -> Option<BandwidthSplit> {
        Some(BandwidthSplit::new(pct).unwrap())
    }

    // Each side downloads what it wants, up to the rate it was given at the
    // tick before; the split is re-evaluated each tick from those samples.
    // The clock is the tick count.
    struct Sim {
        cap: u64,
        split: Option<BandwidthSplit>,
        rates: SplitRates,
    }

    impl Sim {
        fn new(cap: u64, split: Option<BandwidthSplit>) -> Self {
            Self {
                cap,
                split,
                rates: split_rates(Some(cap), split, SplitUse::default()),
            }
        }

        fn tick(&mut self, want: SplitUse) -> (SplitUse, SplitRates) {
            let moved = SplitUse {
                usenet: want.usenet.min(self.rates.usenet.unwrap_or(u64::MAX)),
                torrent: want.torrent.min(self.rates.torrent.unwrap_or(u64::MAX)),
            };
            self.rates = split_rates(Some(self.cap), self.split, moved);
            (moved, self.rates)
        }
    }

    fn both(usenet: u64, torrent: u64) -> SplitUse {
        SplitUse { usenet, torrent }
    }

    #[test]
    fn a_split_out_of_range_is_refused() {
        assert!(BandwidthSplit::new(0).is_err());
        assert!(BandwidthSplit::new(100).is_err());
        assert!(BandwidthSplit::new(1).is_ok());
        assert!(BandwidthSplit::new(99).is_ok());
    }

    #[test]
    fn both_busy_get_their_shares_exactly() {
        for (pct, usenet, torrent) in [(50, 50 * MB, 50 * MB), (80, 80 * MB, 20 * MB)] {
            let mut sim = Sim::new(100 * MB, split(pct));
            for _ in 0..5 {
                let (_, rates) = sim.tick(both(u64::MAX, u64::MAX));
                assert_eq!(
                    rates,
                    SplitRates {
                        usenet: Some(usenet),
                        torrent: Some(torrent),
                    }
                );
            }
        }
    }

    #[test]
    fn an_idle_side_lends_the_whole_cap() {
        let mut sim = Sim::new(100 * MB, split(50));
        let (_, rates) = sim.tick(both(u64::MAX, 0));
        assert_eq!(rates.usenet, Some(100 * MB));
        let (moved, rates) = sim.tick(both(u64::MAX, 0));
        assert_eq!(moved.usenet, 100 * MB);
        assert_eq!(rates.usenet, Some(100 * MB));
        // The idle side keeps its share, so it can show it wants it back.
        assert_eq!(rates.torrent, Some(50 * MB));

        let mut sim = Sim::new(100 * MB, split(80));
        sim.tick(both(0, u64::MAX));
        let (moved, rates) = sim.tick(both(0, u64::MAX));
        assert_eq!(moved.torrent, 100 * MB);
        assert_eq!(rates.torrent, Some(100 * MB));
    }

    #[test]
    fn a_side_using_part_of_its_share_lends_the_rest_in_steps() {
        // Torrents use 23 of their 50: 25 is lent (five steps of 5), never
        // the 2 that would leave them short of what they use.
        let rates = split_rates(Some(100 * MB), split(50), both(60 * MB, 23 * MB));
        assert_eq!(rates.usenet, Some(75 * MB));
        assert_eq!(rates.torrent, Some(50 * MB));
        // A wobble inside the same step changes nothing.
        let wobble = split_rates(Some(100 * MB), split(50), both(60 * MB, 24 * MB));
        assert_eq!(wobble, rates);
    }

    #[test]
    fn a_waking_side_gets_its_share_back_on_the_next_tick() {
        let mut sim = Sim::new(100 * MB, split(80));
        for _ in 0..3 {
            sim.tick(both(u64::MAX, 0));
        }
        assert_eq!(sim.rates.usenet, Some(100 * MB));
        // Torrents wake. The first tick to sample them moving restores both
        // shares.
        let (moved, rates) = sim.tick(both(u64::MAX, u64::MAX));
        assert_eq!(moved.torrent, 20 * MB);
        assert_eq!(
            rates,
            SplitRates {
                usenet: Some(80 * MB),
                torrent: Some(20 * MB),
            }
        );
        let (moved, _) = sim.tick(both(u64::MAX, u64::MAX));
        assert_eq!(moved.usenet + moved.torrent, 100 * MB);
    }

    #[test]
    fn a_borrower_that_goes_idle_hands_the_lent_share_back_on_the_next_tick() {
        for pct in [20, 50, 80] {
            let mut sim = Sim::new(100 * MB, split(pct));
            for _ in 0..3 {
                sim.tick(both(0, u64::MAX));
            }
            assert_eq!(sim.rates.torrent, Some(100 * MB), "{pct}%");
            // Holding the whole limit while moving nothing is no demand.
            let (_, rates) = sim.tick(both(0, 0));
            let [usenet, torrent] = BandwidthSplit::new(pct).unwrap().shares(100 * MB);
            let shares = SplitRates {
                usenet: Some(usenet),
                torrent: Some(torrent),
            };
            assert_eq!(rates, shares, "{pct}%");
            let (_, rates) = sim.tick(both(0, 0));
            assert_eq!(rates, shares, "{pct}%");
        }
        // The same for the sessions sharing the torrents' rate.
        assert_eq!(share_out(90, &[30, 30, 30], &[0, 0, 0]), vec![30, 30, 30]);
    }

    #[test]
    fn while_both_want_more_the_rates_add_up_to_the_cap() {
        for cap in [0, 1, 7, 999, 100 * MB, 123_456_789] {
            for pct in [1, 33, 50, 67, 99] {
                for (usenet, torrent) in [(u64::MAX, u64::MAX), (cap, cap)] {
                    let rates = split_rates(Some(cap), split(pct), both(usenet, torrent));
                    assert_eq!(
                        rates.usenet.unwrap() + rates.torrent.unwrap(),
                        cap,
                        "cap {cap}, {pct}%"
                    );
                }
            }
        }
    }

    #[test]
    fn without_a_limit_nothing_is_split_and_without_a_split_the_limit_is_shared_equally() {
        assert_eq!(
            split_rates(None, split(50), both(1, 1)),
            SplitRates::default()
        );
        // Both busy: half each. One idle: the other borrows, never past the limit.
        assert_eq!(
            split_rates(Some(100), None, both(50, 50)),
            SplitRates {
                usenet: Some(50),
                torrent: Some(50),
            }
        );
        assert_eq!(
            split_rates(Some(100), None, both(50, 0)),
            SplitRates {
                usenet: Some(100),
                torrent: Some(50),
            }
        );
    }

    #[test]
    fn sharing_out_between_many_sides_hands_out_what_is_lent_whole() {
        // 28 of the idle side's 30 is lent: whole steps of 90 / 20 = 4.
        let rates = share_out(90, &[30, 30, 30], &[30, 0, 30]);
        assert_eq!(rates, vec![44, 30, 44]);
        let rates = share_out(100, &[34, 33, 33], &[34, 0, 33]);
        assert_eq!(rates.iter().sum::<u64>(), 100 + 30);
        assert_eq!(rates[1], 33);
    }

    #[test]
    fn quota_state_follows_usage_through_both_caps() {
        let soft = Some(1_000);
        let hard = Some(2_000);
        assert_eq!(QuotaState::of(0, soft, hard), QuotaState::Normal);
        assert_eq!(QuotaState::of(999, soft, hard), QuotaState::Normal);
        assert_eq!(QuotaState::of(1_000, soft, hard), QuotaState::Trickle);
        assert_eq!(QuotaState::of(1_999, soft, hard), QuotaState::Trickle);
        assert_eq!(QuotaState::of(2_000, soft, hard), QuotaState::Stopped);
        assert_eq!(QuotaState::of(u64::MAX, None, None), QuotaState::Normal);
        // A zero cap is reached at once; no cap is never reached.
        assert_eq!(QuotaState::of(0, None, Some(0)), QuotaState::Stopped);
        assert_eq!(QuotaState::of(0, Some(0), None), QuotaState::Trickle);
    }
}
