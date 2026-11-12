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
// per second: the bytes each counted since the tick before, over the time
// between them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitUse {
    pub usenet: u64,
    pub torrent: u64,
}

// What the split measured over its last tick: the rates usenet and torrents
// were each held to over it, and what each moved at.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitTick {
    pub given: SplitRates,
    pub used: SplitUse,
}

// The download rate each protocol may move at. `None` is no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitRates {
    pub usenet: Option<u64>,
    pub torrent: Option<u64>,
}

impl SplitRates {
    // Each side's lower of the two rates, no limit above any.
    pub fn lower(self, other: Self) -> Self {
        Self {
            usenet: lower(self.usenet, other.usenet),
            torrent: lower(self.torrent, other.torrent),
        }
    }
}

// The lower of two rates, `None` being no limit.
pub fn lower(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) | (None, a) => a,
    }
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

// How long the split measures each side's bytes before it shares the limit
// out again. It spans two of a torrent session's quota rounds and four times
// what a usenet bucket holds, so a side moving at its rate shows it.
pub const SPLIT_TICK: std::time::Duration = std::time::Duration::from_secs(1);

// `bytes` moved over `elapsed`, in bytes per second.
pub fn per_second(bytes: u64, elapsed: std::time::Duration) -> u64 {
    let nanos = elapsed.as_nanos();
    if nanos == 0 {
        return 0;
    }
    u64::try_from(u128::from(bytes) * 1_000_000_000 / nanos).unwrap_or(u64::MAX)
}

// A side moving at 95% or more of the rate it was given presses it: it may
// want more than it was given.
const DEMAND_PCT: u128 = 95;
// A side that does not press its rate keeps what it moved at and a fifth
// more, so the next tick, moving as it did, it does not press.
const HEADROOM_PCT: u128 = 120;
// A side that moves nothing keeps a hundredth of the limit, at most its
// share: a side that starts moving presses it at once.
const PROBE_PCT: u128 = 1;

// Each side's rate out of `cap`, work-conserving, from the rate each was
// `given` at the last tick and what each `used` over it. `shares` add up to
// `cap`, and so do the rates, every tick: together the sides never pass the
// limit. A side given no rate, or one past `cap` (from before a change of
// the limit), counts as given its share.
//
// A side that presses the rate it was given is owed its share. A side that
// does not keeps what it measurably uses (with headroom, and a probe's
// hundredth of the limit when idle); what it keeps past its share it keeps
// only while the limit has room. What is left goes to the sides that press,
// by their shares; with none pressing, back to the sides that keep less than
// their share, up to it. So a side going idle hands the limit back the next
// tick, a lone active side holds all of it but the idle side's probe, and a
// side waking presses its probe and has its share the tick after.
//
// This is the one predicate behind the usenet/torrent split, and behind the
// torrent rate's sharing between torrent sessions.
pub fn share_out(cap: u64, shares: &[u64], given: &[Option<u64>], used: &[u64]) -> Vec<u64> {
    let sides = shares.len();
    debug_assert_eq!(given.len(), sides);
    debug_assert_eq!(used.len(), sides);
    debug_assert_eq!(shares.iter().sum::<u64>(), cap);
    let given: Vec<u64> = given
        .iter()
        .zip(shares)
        .map(|(given, &share)| given.filter(|&given| given <= cap).unwrap_or(share))
        .collect();
    let presses = |side: usize| used[side] as u128 * 100 >= given[side] as u128 * DEMAND_PCT;
    let probe = (cap as u128 * PROBE_PCT / 100).max(1);
    // What a side that does not press keeps, past its share or not.
    let keeps = |side: usize| {
        (used[side] as u128 * HEADROOM_PCT / 100)
            .max(probe)
            .min(cap as u128)
    };
    let mut rates: Vec<u128> = (0..sides)
        .map(|side| {
            if presses(side) {
                shares[side] as u128
            } else {
                keeps(side).min(shares[side] as u128)
            }
        })
        .collect();
    let mut left = cap as u128 - rates.iter().sum::<u128>();
    // What the sides that do not press keep past their shares, while there is
    // room for it.
    let over: Vec<u128> = (0..sides)
        .map(|side| match presses(side) {
            true => 0,
            false => keeps(side).saturating_sub(shares[side] as u128),
        })
        .collect();
    let wanted: u128 = over.iter().sum();
    if wanted > 0 {
        let handed = hand_out(left.min(wanted), &over);
        for (rate, part) in rates.iter_mut().zip(handed) {
            *rate += part;
        }
        left -= left.min(wanted);
    }
    // The rest: to the sides that press, by their shares; else back toward
    // the shares of the sides keeping less than theirs.
    let weights: Vec<u128> = if (0..sides).any(presses) {
        (0..sides)
            .map(|side| match presses(side) {
                true => shares[side] as u128,
                false => 0,
            })
            .collect()
    } else {
        (0..sides)
            .map(|side| (shares[side] as u128).saturating_sub(rates[side]))
            .collect()
    };
    let weights = if weights.iter().sum::<u128>() == 0 {
        shares.iter().map(|&share| share as u128).collect()
    } else {
        weights
    };
    for (rate, part) in rates.iter_mut().zip(hand_out(left, &weights)) {
        *rate += part;
    }
    rates.into_iter().map(|rate| rate as u64).collect()
}

// `amount` handed out by `weights`, whole: the last side with a weight takes
// what rounding leaves. Nothing goes out when no side has a weight.
fn hand_out(amount: u128, weights: &[u128]) -> Vec<u128> {
    let total: u128 = weights.iter().sum();
    let mut parts = vec![0; weights.len()];
    let Some(last) = weights.iter().rposition(|&weight| weight > 0) else {
        return parts;
    };
    let mut handed = 0;
    for (side, &weight) in weights.iter().enumerate().filter(|(_, w)| **w > 0) {
        let part = if side == last {
            amount - handed
        } else {
            amount * weight / total
        };
        handed += part;
        parts[side] = part;
    }
    parts
}

// The download rate usenet and torrents each move at under the global limit
// `cap` in force, from the rates they were `given` at the last tick and what
// they `used` over it. Without a limit there is nothing to split; without a
// split the two share the limit equally. Together they never pass it.
pub fn split_rates(
    cap: Option<u64>,
    split: Option<BandwidthSplit>,
    given: SplitRates,
    used: SplitUse,
) -> SplitRates {
    let Some(cap) = cap else {
        return SplitRates::default();
    };
    let split = split.unwrap_or_default();
    let shares = split.shares(cap);
    let given = [given.usenet, given.torrent];
    let [usenet, torrent] = share_out(cap, &shares, &given, &[used.usenet, used.torrent])[..]
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
    const MIB: u64 = 1_048_576;
    // A side that wants all it is given.
    const MAX: u64 = u64::MAX;

    fn split(pct: u8) -> Option<BandwidthSplit> {
        Some(BandwidthSplit::new(pct).unwrap())
    }

    fn both(usenet: u64, torrent: u64) -> SplitUse {
        SplitUse { usenet, torrent }
    }

    fn rates(usenet: u64, torrent: u64) -> SplitRates {
        SplitRates {
            usenet: Some(usenet),
            torrent: Some(torrent),
        }
    }

    // Usenet held to its rate by a bucket of a quarter second's capacity, as
    // the usenet `TokenBucket` is, and torrents by a session's quota as
    // `weaver_torrent::limits` keeps it. Each half-second round in which
    // torrents want bytes adds half a second of their rate, rounded to the
    // byte, to what they hold, and what they hold past three seconds' worth
    // goes back to three seconds' worth (`Budget::round`). The real channel
    // fills only in a round with a request waiting; here every round in
    // which torrents want anything fills, the most they can keep. A change
    // of the torrents' rate by the split holds what they keep to one round
    // of the new rate (`Limit::set`, called only when the rate changes);
    // an unchanged rate keeps it. Each side moves what it wants up to what
    // it holds. The clock is mocked: a split tick is ten steps of a tenth of
    // a second, and at each tick the split shares the cap out again from the
    // bytes each side moved over it, and usenet's bucket is clamped to a
    // quarter second of its new rate.
    struct Sim {
        cap: u64,
        split: Option<BandwidthSplit>,
        rates: SplitRates,
        tokens: [u64; 2],
    }

    impl Sim {
        fn new(cap: u64, split: Option<BandwidthSplit>) -> Self {
            Self {
                cap,
                split,
                rates: split_rates(Some(cap), split, SplitRates::default(), SplitUse::default()),
                tokens: [0, 0],
            }
        }

        // One torrent round's fill at `rate`: half a second of it, rounded
        // to the byte (`Budget::round_quota`).
        fn round_quota(rate: u64) -> u64 {
            (rate * 500 + 500) / 1000
        }

        // One split tick in which each side wants `want` bytes a second:
        // what each moved over it, and the rates the split puts in force
        // for the next.
        fn tick(&mut self, want: SplitUse) -> (SplitUse, SplitRates) {
            const STEPS: u64 = 10;
            let given = self.rates;
            let limits = [given.usenet.unwrap(), given.torrent.unwrap()];
            let wants = [want.usenet, want.torrent];
            let mut moved = [0u64; 2];
            for step in 0..STEPS {
                let usenet = &mut self.tokens[0];
                *usenet = (*usenet + limits[0] / STEPS).min(limits[0] / 4);
                if step % 5 == 0 && wants[1] > 0 {
                    let kept = self.tokens[1] + Self::round_quota(limits[1]);
                    self.tokens[1] = if kept / 3 > limits[1] {
                        3 * limits[1]
                    } else {
                        kept
                    };
                }
                for side in 0..2 {
                    let took = self.tokens[side].min(wants[side] / STEPS);
                    self.tokens[side] -= took;
                    moved[side] += took;
                }
            }
            let used = both(moved[0], moved[1]);
            self.rates = split_rates(Some(self.cap), self.split, given, used);
            let [usenet, torrent] = [self.rates.usenet.unwrap(), self.rates.torrent.unwrap()];
            self.tokens[0] = self.tokens[0].min(usenet / 4);
            if torrent != limits[1] {
                self.tokens[1] = self.tokens[1].min(Self::round_quota(torrent));
            }
            (used, self.rates)
        }
    }

    // The most both sides can move over the five ticks after rates
    // `before` were put in force. The rates the split gives add up to the
    // cap every tick, so the two sides fill with five seconds of the cap
    // over the five ticks, give or take a byte a torrent round of rounding.
    // On top of that each side spends what it held when the window opened:
    // usenet a quarter second of its rate at most, which is within 0.05 of
    // five seconds of the cap; torrents three seconds of the rate `before`
    // gave them at most, and two bytes more, since a round keeps up to three
    // seconds' worth and what a third rounds away. So the bound is 1.05 of
    // five seconds of the cap, three seconds of the torrents' rate when the
    // window opened, and seven bytes.
    fn window_bound(cap: u64, before: SplitRates) -> u64 {
        5 * cap * 105 / 100 + 3 * before.torrent.unwrap() + 7
    }

    // What both sides moved over every five seconds of `ticks`, run from
    // rates `start`, stays within `window_bound`.
    fn assert_windows_hold(
        cap: u64,
        start: SplitRates,
        ticks: &[(SplitUse, SplitRates)],
        what: &str,
    ) {
        for (tick, window) in ticks.windows(5).enumerate() {
            let moved: u64 = window
                .iter()
                .map(|(moved, _)| moved.usenet + moved.torrent)
                .sum();
            let before = match tick {
                0 => start,
                _ => ticks[tick - 1].1,
            };
            let bound = window_bound(cap, before);
            assert!(
                moved <= bound,
                "{what}: window from {tick}: {moved} past {bound}"
            );
        }
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
                let (_, given) = sim.tick(both(MAX, MAX));
                assert_eq!(given, rates(usenet, torrent));
            }
        }
    }

    #[test]
    fn nothing_measured_holds_each_side_to_its_share() {
        let sim = Sim::new(100 * MB, split(80));
        assert_eq!(sim.rates, rates(80 * MB, 20 * MB));
        let sim = Sim::new(100 * MB, None);
        assert_eq!(sim.rates, rates(50 * MB, 50 * MB));
    }

    #[test]
    fn an_idle_side_lends_the_whole_cap_but_its_probe_at_once() {
        // Usenet idle, torrents busy: the first tick that measures them
        // hands torrents all but a hundredth of the cap, the idle side's
        // probe to show it wants its share back.
        for pct in [None, split(30), split(70)] {
            let mut sim = Sim::new(100 * MB, pct);
            let (_, given) = sim.tick(both(0, MAX));
            assert_eq!(given, rates(MB, 99 * MB), "{pct:?}");
            let (moved, given) = sim.tick(both(0, MAX));
            assert_eq!(moved.torrent, 99 * MB, "{pct:?}");
            assert_eq!(given, rates(MB, 99 * MB), "{pct:?}");
        }
    }

    #[test]
    fn a_side_using_part_of_its_share_keeps_what_it_uses_with_headroom() {
        // Torrents move 23 of their 50: they keep 23 and a fifth, and usenet,
        // pressing, has the rest.
        let given = rates(50 * MB, 50 * MB);
        let next = split_rates(Some(100 * MB), split(50), given, both(50 * MB, 23 * MB));
        assert_eq!(next, rates(100 * MB - 27_600_000, 27_600_000));
        // Moving the same against what they keep, they do not press it, and
        // keep the same.
        let again = split_rates(
            Some(100 * MB),
            split(50),
            next,
            both(next.usenet.unwrap(), 23 * MB),
        );
        assert_eq!(again, next);
    }

    #[test]
    fn a_waking_side_gets_its_share_back_on_the_next_tick() {
        let mut sim = Sim::new(100 * MB, split(80));
        for _ in 0..3 {
            sim.tick(both(MAX, 0));
        }
        assert_eq!(sim.rates, rates(99 * MB, MB));
        // Torrents wake and press their probe, a round of it at a time: the
        // tick that measures it restores both shares.
        let (moved, given) = sim.tick(both(MAX, MAX));
        assert_eq!(moved.torrent, MB);
        assert_eq!(given, rates(80 * MB, 20 * MB));
        let (moved, _) = sim.tick(both(MAX, MAX));
        assert_eq!(moved.usenet + moved.torrent, 100 * MB);
    }

    #[test]
    fn a_borrower_that_goes_idle_hands_the_lent_share_back_on_the_next_tick() {
        for pct in [20, 50, 80] {
            let mut sim = Sim::new(100 * MB, split(pct));
            for _ in 0..3 {
                sim.tick(both(0, MAX));
            }
            assert_eq!(sim.rates.torrent, Some(99 * MB), "{pct}%");
            // Holding the limit while moving nothing is no demand.
            let (_, given) = sim.tick(both(0, 0));
            let [usenet, torrent] = BandwidthSplit::new(pct).unwrap().shares(100 * MB);
            assert_eq!(given, rates(usenet, torrent), "{pct}%");
            let (_, given) = sim.tick(both(0, 0));
            assert_eq!(given, rates(usenet, torrent), "{pct}%");
        }
        // The same for the sessions sharing the torrents' rate.
        assert_eq!(
            share_out(90, &[30, 30, 30], &[Some(30); 3], &[0, 0, 0]),
            vec![30, 30, 30]
        );
    }

    #[test]
    fn the_rates_add_up_to_the_cap_every_tick() {
        for cap in [0, 1, 7, 999, 100 * MB, 123_456_789] {
            for pct in [1, 33, 50, 67, 99] {
                let [usenet_share, torrent_share] = BandwidthSplit::new(pct).unwrap().shares(cap);
                for given in [
                    SplitRates::default(),
                    rates(usenet_share, torrent_share),
                    rates(cap, 0),
                    rates(0, cap),
                ] {
                    for used in [
                        both(MAX, MAX),
                        both(cap, cap),
                        both(0, 0),
                        both(0, cap),
                        both(cap / 3, 0),
                    ] {
                        let next = split_rates(Some(cap), split(pct), given, used);
                        assert_eq!(
                            next.usenet.unwrap() + next.torrent.unwrap(),
                            cap,
                            "cap {cap}, {pct}%, {given:?}, {used:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn without_a_limit_nothing_is_split_and_without_a_split_the_limit_is_shared_equally() {
        assert_eq!(
            split_rates(None, split(50), SplitRates::default(), both(1, 1)),
            SplitRates::default()
        );
        let shares = rates(50, 50);
        assert_eq!(split_rates(Some(100), None, shares, both(50, 50)), shares);
        assert_eq!(
            split_rates(Some(100), None, shares, both(50, 0)),
            rates(99, 1)
        );
    }

    #[test]
    fn sharing_out_between_many_sides_hands_out_the_cap_whole() {
        // Two press, one idles: the idle one keeps its probe, the two
        // pressing share the rest by their shares.
        let rates = share_out(90, &[30, 30, 30], &[Some(30); 3], &[30, 0, 30]);
        assert_eq!(rates, vec![44, 1, 45]);
        let rates = share_out(
            100,
            &[34, 33, 33],
            &[Some(34), Some(33), Some(33)],
            &[34, 0, 33],
        );
        assert_eq!(rates.iter().sum::<u64>(), 100);
        assert_eq!(rates[1], 1);
    }

    // The acceptance for the split, over the contention run's steps: a cap
    // of 8 MiB/s, torrents downloading alone, usenet starting beside them,
    // then usenet paused and resumed three times, at each split.
    #[test]
    fn the_split_holds_the_cap_reclaims_an_idle_share_and_lends_to_a_lone_side() {
        let cap = 8 * MIB;
        let mut script = vec![both(0, MAX); 10];
        script.extend(vec![both(MAX, MAX); 10]);
        for _ in 0..3 {
            script.extend(vec![both(0, MAX); 10]);
            script.extend(vec![both(MAX, MAX); 10]);
        }
        for pct in [None, split(30), split(50), split(70)] {
            let mut sim = Sim::new(cap, pct);
            let start = sim.rates;
            let ticks: Vec<_> = script.iter().map(|&want| sim.tick(want)).collect();

            // The limits never add up past the cap, and what both moved
            // over every five seconds stays within the window bound.
            for (tick, (_, given)) in ticks.iter().enumerate() {
                let sum = given.usenet.unwrap() + given.torrent.unwrap();
                assert!(sum <= cap, "{pct:?} tick {tick}: {sum}");
            }
            assert_windows_hold(cap, start, &ticks, &format!("{pct:?}"));

            // A lone active side has 0.98 of the cap or more from the first
            // tick that measures it.
            assert!(ticks[0].1.torrent.unwrap() * 100 >= cap * 98, "{pct:?}");

            // Each time usenet goes idle, the torrents' limit reaches the
            // cap, all but the idle side's probe, within two ticks.
            for start in script
                .windows(2)
                .enumerate()
                .filter(|(_, pair)| pair[0].usenet == MAX && pair[1].usenet == 0)
                .map(|(tick, _)| tick + 1)
            {
                let within = ticks[start + 1].1.torrent.unwrap();
                assert!(
                    within * 100 >= cap * 99,
                    "{pct:?} idle at {start}: {within}"
                );
            }
        }
    }

    // A side moving part of its share, then pressing it, holds every five
    // seconds within the window bound: torrents, or usenet, moving a
    // fraction of the cap beside the other side pressing it, then pressing
    // it themselves.
    #[test]
    fn a_side_moving_part_of_its_share_then_pressing_holds_the_cap() {
        let cap = 8 * MIB;
        for pct in [split(30), split(50), split(70)] {
            for part in [5, 20, 30, 40, 45, 60, 80, 95] {
                let part = cap * part / 100;
                for (what, before) in [("torrents", both(MAX, part)), ("usenet", both(part, MAX))] {
                    let mut script = vec![before; 10];
                    script.extend(vec![both(MAX, MAX); 10]);
                    let mut sim = Sim::new(cap, pct);
                    let start = sim.rates;
                    let ticks: Vec<_> = script.iter().map(|&want| sim.tick(want)).collect();
                    assert_windows_hold(cap, start, &ticks, &format!("{pct:?} {what} at {part}"));
                }
            }
        }
    }

    // What torrents kept while moving part of their rate is spent once:
    // a 30% usenet split, torrents moving 45% of the cap beside usenet
    // pressing, then both pressing. The window that spends what torrents
    // kept passes 1.05 of five seconds of the cap; every window that opens
    // once torrents hold no more than a round of their rate is back within
    // 1.05 of it.
    #[test]
    fn what_torrents_kept_is_spent_once_and_later_windows_hold_the_cap() {
        let cap = 8 * MIB;
        let mut script = vec![both(MAX, cap * 45 / 100); 10];
        script.extend(vec![both(MAX, MAX); 20]);
        let mut sim = Sim::new(cap, split(30));
        let mut ticks = Vec::new();
        let mut spent = None;
        for (tick, &want) in script.iter().enumerate() {
            ticks.push(sim.tick(want));
            let round = Sim::round_quota(sim.rates.torrent.unwrap());
            if spent.is_none() && want.torrent == MAX && sim.tokens[1] <= round {
                spent = Some(tick + 1);
            }
        }
        let spent = spent.expect("torrents spend what they kept");
        let moved = |window: &[(SplitUse, SplitRates)]| -> u64 {
            window.iter().map(|(m, _)| m.usenet + m.torrent).sum()
        };
        let past: Vec<_> = ticks.windows(5).map(moved).collect();
        assert!(
            past[..spent].iter().any(|&m| m * 100 > 5 * cap * 105),
            "no window spent a kept quota: {past:?}"
        );
        assert!(spent + 5 <= ticks.len(), "spent at {spent}");
        for (tick, &m) in past.iter().enumerate().skip(spent) {
            assert!(m * 100 <= 5 * cap * 105, "window from {tick}: {m}");
        }
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
