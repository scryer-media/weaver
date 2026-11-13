// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The policy for the per-volume write buffer: how many decoded bytes may wait
// for one volume's writer, how that room is shared once more volumes are
// written to than the pool holds full shares for, and how fast articles may
// be sent towards a volume given how full its share is.
//
// Every function here is pure: numbers in, numbers out, no clock and no
// pipeline state. Each limit is a cap on what may wait, not memory taken up
// front.

// Nothing calls this module yet; the wiring into dispatch follows.
#![allow(dead_code)]

use std::num::NonZeroU64;

use super::DOWNLOAD_PRESSURE_SOFT_PERCENT;
use crate::runtime::HardwareProfile;

const MIB: u64 = 1024 * 1024;

// The Performance target: what the reference load needs (70 connections of
// 0.8 MB articles is about 55 MB in flight), with room to put it in order.
const VOLUME_BUFFER_PERFORMANCE_BYTES: u64 = 256 * MIB;

// The Balanced target: half of Performance, as the 4-8 GiB memory tier's base
// size (144 MiB) is half of the 16-32 GiB tier's (288 MiB), the tiers the two
// profiles start at.
const VOLUME_BUFFER_BALANCED_BYTES: u64 = 128 * MIB;

// The Efficient target: half of Balanced again. It sits between the base
// sizes of the 1-2 GiB and 2-4 GiB memory tiers (48 and 96 MiB) and equals
// the least the write backlog limit was allowed to be before this buffer.
const VOLUME_BUFFER_EFFICIENT_BYTES: u64 = 64 * MIB;

// The floor, in multiples of what is in flight (connections x article size):
// a buffer smaller than what is in flight cannot put anything in order.
const VOLUME_BUFFER_IN_FLIGHT_MULTIPLE: u64 = 4;

// The ceiling, as a divisor of the memory ceiling: an eighth, the share the
// write backlog limit was held to before this buffer.
const VOLUME_BUFFER_MEMORY_DIVISOR: u64 = 8;

// The pool every volume's buffer is drawn from, in full figures. Up to this
// many volumes each get the full figure; more share the pool. Ruled by the
// operator: "full figure up to 2x then shared".
const VOLUME_BUFFER_POOL_FIGURES: u64 = 2;

// The most the pool may be, as a divisor of the memory ceiling: a quarter,
// which is what two figures of an eighth each come to.
const VOLUME_BUFFER_POOL_MEMORY_DIVISOR: u64 = 4;

// The figure's own ceiling keeps the pool within its share of memory.
const _: () = assert!(
    VOLUME_BUFFER_MEMORY_DIVISOR >= VOLUME_BUFFER_POOL_FIGURES * VOLUME_BUFFER_POOL_MEMORY_DIVISOR
);

// Past the soft limit, dispatch into a volume starts at this share of its
// measured write rate, so the buffer drains instead of holding level.
const PACE_MEASURED_RATE_PERCENT: u64 = 90;

// With no measured write rate, the room left below the hard limit may be
// filled no faster than over this many seconds.
const PACE_UNMEASURED_ROOM_FILL_SECS: u64 = 1;

// The figure: the decoded bytes that may wait for one volume's writer when
// the volume has a full share (see `volume_limits`).
//
// The profile gives the target, the load gives a floor and memory gives a
// ceiling. `memory_ceiling_bytes` is the memory the backlogs are carved
// from, already capped by a container's limit. The ceiling wins over the
// floor, so a small container is respected whatever the load. Never zero.
pub(in crate::pipeline) fn total_buffer_budget_bytes(
    profile: HardwareProfile,
    memory_ceiling_bytes: u64,
    connections: usize,
    typical_article_bytes: u64,
) -> u64 {
    let target = match profile {
        HardwareProfile::Efficient => VOLUME_BUFFER_EFFICIENT_BYTES,
        HardwareProfile::Balanced => VOLUME_BUFFER_BALANCED_BYTES,
        HardwareProfile::Performance => VOLUME_BUFFER_PERFORMANCE_BYTES,
    };
    let floor = in_flight_bytes(connections, typical_article_bytes)
        .saturating_mul(VOLUME_BUFFER_IN_FLIGHT_MULTIPLE);
    let ceiling = memory_ceiling_bytes / VOLUME_BUFFER_MEMORY_DIVISOR;
    target.max(floor).min(ceiling).max(1)
}

// What every connection holds when each carries one typical article.
pub(in crate::pipeline) fn in_flight_bytes(connections: usize, typical_article_bytes: u64) -> u64 {
    (connections as u64).saturating_mul(typical_article_bytes)
}

// One volume's share of the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct VolumeLimits {
    // From here dispatch into the volume is paced.
    pub(in crate::pipeline) soft_bytes: u64,
    // Here dispatch into the volume stops.
    pub(in crate::pipeline) hard_bytes: u64,
}

impl VolumeLimits {
    // The limits of a volume whose buffer may hold `hard_bytes`. The soft
    // limit sits at the same share of the hard limit as the pipeline's other
    // pressure limits. Neither limit is ever zero.
    fn with_hard(hard_bytes: u64) -> Self {
        let hard_bytes = hard_bytes.max(1);
        let soft = u128::from(hard_bytes) * u128::from(DOWNLOAD_PRESSURE_SOFT_PERCENT) / 100;
        let soft_bytes = u64::try_from(soft)
            .unwrap_or(u64::MAX)
            .max(1)
            .min(hard_bytes);
        Self {
            soft_bytes,
            hard_bytes,
        }
    }
}

// The limits of each volume that has jobs writing to it, in the order of
// `measured_write_bytes_per_sec`, which holds one measured write rate per
// such volume (zero = not measured).
//
// `figure_bytes` is what `total_buffer_budget_bytes` gives. One or two
// volumes each get the full figure. Three or more share a pool of two
// figures by slowness: a volume's share is in proportion to one over its
// write rate, so the slowest volume gets the most room and the fastest the
// least. No volume gets more than the figure; what a slow volume cannot
// take goes to the others in the same proportions. No volume gets less than
// `in_flight_bytes` (connections x typical article size), so a fast volume
// is not stopped by a limit smaller than a burst. When that minimum cannot
// be given to every volume the pool is split evenly instead.
//
// An unmeasured volume counts as one of mean slowness among the measured
// ones; with none measured the shares are equal.
//
// The hard limits never add up to more than the pool, except that no limit
// is ever zero: a pool of fewer bytes than there are volumes still gives
// each volume one byte.
//
// This runs when pressure is refreshed, not once per article. It allocates
// the result and nothing else.
pub(in crate::pipeline) fn volume_limits(
    figure_bytes: u64,
    in_flight_bytes: u64,
    measured_write_bytes_per_sec: &[u64],
) -> Vec<VolumeLimits> {
    let rates = measured_write_bytes_per_sec;
    let figure = figure_bytes.max(1);
    if rates.len() as u64 <= VOLUME_BUFFER_POOL_FIGURES {
        return rates
            .iter()
            .map(|_| VolumeLimits::with_hard(figure))
            .collect();
    }

    let volumes = rates.len() as u128;
    let pool = u128::from(figure) * u128::from(VOLUME_BUFFER_POOL_FIGURES);
    let least = in_flight_bytes.max(1);
    if u128::from(least) * volumes > pool {
        let even = u64::try_from(pool / volumes).unwrap_or(u64::MAX);
        return rates.iter().map(|_| VolumeLimits::with_hard(even)).collect();
    }

    // Here three or more volumes share the pool and `least` fits for each,
    // which also puts `least` below the figure.
    let stand_in = unmeasured_stand_in_rate(rates);
    let rate_of = |rate: u64| if rate == 0 { stand_in } else { rate };
    // A volume's share at a given level is the level over its rate, held
    // between the minimum and the figure. Every share rises with the level,
    // so the level that uses up the pool is found by halving.
    let share = |level: u128, rate: u64| {
        u64::try_from(level / u128::from(rate_of(rate)))
            .unwrap_or(u64::MAX)
            .clamp(least, figure)
    };
    let shares_fit = |level: u128| {
        rates
            .iter()
            .map(|&rate| u128::from(share(level, rate)))
            .sum::<u128>()
            <= pool
    };

    // At level zero every volume holds the minimum, which fits. At the top
    // level every volume holds the figure, which is more than the pool.
    let fastest = rates.iter().map(|&rate| rate_of(rate)).max().unwrap_or(1);
    let mut fits: u128 = 0;
    let mut too_much = u128::from(figure) * u128::from(fastest);
    while too_much - fits > 1 {
        let level = fits + (too_much - fits) / 2;
        if shares_fit(level) {
            fits = level;
        } else {
            too_much = level;
        }
    }

    rates
        .iter()
        .map(|&rate| VolumeLimits::with_hard(share(fits, rate)))
        .collect()
}

// The write rate an unmeasured volume is counted at: the rate whose
// slowness (one over the rate) is the mean slowness of the measured
// volumes. With none measured every volume counts the same.
fn unmeasured_stand_in_rate(rates: &[u64]) -> u64 {
    // One over a rate, as a whole number.
    const ONE: u128 = 1 << 62;
    let mut measured: u128 = 0;
    let mut slowness: u128 = 0;
    for &rate in rates {
        if rate != 0 {
            measured += 1;
            slowness += ONE / u128::from(rate);
        }
    }
    if measured == 0 {
        return 1;
    }
    u64::try_from(measured * ONE / slowness.max(1))
        .unwrap_or(u64::MAX)
        .max(1)
}

// How fast articles may be sent towards one volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum DispatchAllowance {
    // The volume does not hold dispatch back.
    Unlimited,
    // At most this many bytes per second.
    BytesPerSecond(NonZeroU64),
    // Nothing is sent towards the volume until its buffer drains.
    Stopped,
}

impl DispatchAllowance {
    // The allowance as a speed limiter rate, where zero means unlimited.
    // `None` when dispatch is stopped: no rate says that, so the caller
    // holds the volume's work back itself.
    pub(in crate::pipeline) fn limiter_rate(self) -> Option<u64> {
        match self {
            Self::Unlimited => Some(0),
            Self::BytesPerSecond(rate) => Some(rate.get()),
            Self::Stopped => None,
        }
    }
}

// The dispatch allowance into a volume holding `buffered_bytes`.
//
// Below the soft limit nothing is held back. At the hard limit dispatch
// stops. Between them the allowance falls in a straight line with the room
// left: from 90% of the measured write rate at the soft limit to nothing at
// the hard limit, so the buffer always drains towards the soft limit. A
// `measured_write_bytes_per_sec` of zero means the volume has not been
// measured; the allowance is then the room left per second, which falls
// with fill alone and cannot carry the buffer to the hard limit.
//
// The allowance never rises as the buffer fills, and past the soft limit it
// is never above 90% of a measured rate. A rate that rounds down to nothing
// reads as stopped.
pub(in crate::pipeline) fn dispatch_allowance(
    buffered_bytes: u64,
    limits: VolumeLimits,
    measured_write_bytes_per_sec: u64,
) -> DispatchAllowance {
    let hard = limits.hard_bytes;
    let soft = limits.soft_bytes.min(hard);
    if buffered_bytes >= hard {
        return DispatchAllowance::Stopped;
    }
    if buffered_bytes < soft {
        return DispatchAllowance::Unlimited;
    }

    // Here soft <= buffered < hard, so both spans are above zero.
    let room = hard - buffered_bytes;
    let rate = if measured_write_bytes_per_sec == 0 {
        room / PACE_UNMEASURED_ROOM_FILL_SECS
    } else {
        // The rate at the soft limit first, then its share for the room
        // left: each product fits in 128 bits and each result in 64.
        let at_soft = u128::from(measured_write_bytes_per_sec)
            * u128::from(PACE_MEASURED_RATE_PERCENT)
            / 100;
        let scaled = at_soft * u128::from(room) / u128::from(hard - soft);
        u64::try_from(scaled).unwrap_or(u64::MAX)
    };
    match NonZeroU64::new(rate) {
        Some(rate) => DispatchAllowance::BytesPerSecond(rate),
        None => DispatchAllowance::Stopped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;
    // The reference load: 70 connections of 0.8 MB articles.
    const REFERENCE_CONNECTIONS: usize = 70;
    const REFERENCE_ARTICLE_BYTES: u64 = 800_000;

    // The Performance figure, and the write rates of a network share, a
    // USB disk and an SSD.
    const FIGURE: u64 = 256 * MIB;
    const MB: u64 = 1_000_000;
    const NAS: u64 = 60 * MB;
    const USB: u64 = 100 * MB;
    const SSD: u64 = 400 * MB;

    fn hard_bytes(volumes: &[VolumeLimits]) -> Vec<u64> {
        volumes.iter().map(|volume| volume.hard_bytes).collect()
    }

    fn hard_sum(volumes: &[VolumeLimits]) -> u64 {
        volumes.iter().map(|volume| volume.hard_bytes).sum()
    }

    fn limits(soft_bytes: u64, hard_bytes: u64) -> VolumeLimits {
        VolumeLimits {
            soft_bytes,
            hard_bytes,
        }
    }

    // Orders allowances from stopped to unlimited, to compare them.
    fn rank(allowance: DispatchAllowance) -> u128 {
        match allowance {
            DispatchAllowance::Stopped => 0,
            DispatchAllowance::BytesPerSecond(rate) => u128::from(rate.get()),
            DispatchAllowance::Unlimited => u128::from(u64::MAX) + 1,
        }
    }

    fn rate_of(allowance: DispatchAllowance) -> u64 {
        match allowance {
            DispatchAllowance::BytesPerSecond(rate) => rate.get(),
            other => panic!("expected a rate, got {other:?}"),
        }
    }

    #[test]
    fn volume_pace_reference_machine_gets_the_performance_figure() {
        // A 16 GB machine: no load floor or memory ceiling is in the way.
        let budget = total_buffer_budget_bytes(
            HardwareProfile::Performance,
            16 * GIB,
            REFERENCE_CONNECTIONS,
            REFERENCE_ARTICLE_BYTES,
        );
        assert_eq!(budget, 256 * MIB);
        // And the figure holds four times what is in flight.
        assert!(budget >= 4 * 70 * 800_000);
    }

    #[test]
    fn volume_pace_profiles_step_down_from_performance() {
        let budget = |profile| total_buffer_budget_bytes(profile, 16 * GIB, 10, 800_000);
        assert_eq!(budget(HardwareProfile::Performance), 256 * MIB);
        assert_eq!(budget(HardwareProfile::Balanced), 128 * MIB);
        assert_eq!(budget(HardwareProfile::Efficient), 64 * MIB);
    }

    #[test]
    fn volume_pace_floor_binds_when_the_load_outgrows_the_profile() {
        // Balanced at the reference load: 4 x 70 x 0.8 MB is above 128 MiB.
        let budget = total_buffer_budget_bytes(
            HardwareProfile::Balanced,
            16 * GIB,
            REFERENCE_CONNECTIONS,
            REFERENCE_ARTICLE_BYTES,
        );
        assert_eq!(budget, 224_000_000);
        assert!(budget > 128 * MIB);

        // A lighter load leaves the profile's own figure in place.
        let light = total_buffer_budget_bytes(HardwareProfile::Balanced, 16 * GIB, 20, 800_000);
        assert_eq!(light, 128 * MIB);
    }

    #[test]
    fn volume_pace_ceiling_binds_over_the_profile_and_the_floor() {
        // An eighth of 1 GiB is below the Performance figure and the floor.
        let budget = total_buffer_budget_bytes(
            HardwareProfile::Performance,
            GIB,
            REFERENCE_CONNECTIONS,
            REFERENCE_ARTICLE_BYTES,
        );
        assert_eq!(budget, 128 * MIB);

        // Just enough memory for the figure: the ceiling no longer binds.
        let enough = total_buffer_budget_bytes(
            HardwareProfile::Performance,
            2 * GIB,
            REFERENCE_CONNECTIONS,
            REFERENCE_ARTICLE_BYTES,
        );
        assert_eq!(enough, 256 * MIB);
    }

    #[test]
    fn volume_pace_tiny_container_is_respected() {
        // 256 MiB is the least the memory ceiling is ever given as.
        for profile in HardwareProfile::ALL {
            let budget = total_buffer_budget_bytes(
                profile,
                256 * MIB,
                REFERENCE_CONNECTIONS,
                REFERENCE_ARTICLE_BYTES,
            );
            assert_eq!(budget, 32 * MIB, "{profile:?}");
        }
    }

    #[test]
    fn volume_pace_budget_is_never_zero_and_never_overflows() {
        assert_eq!(
            total_buffer_budget_bytes(HardwareProfile::Efficient, 0, 0, 0),
            1
        );
        assert_eq!(
            total_buffer_budget_bytes(HardwareProfile::Performance, u64::MAX, usize::MAX, u64::MAX),
            u64::MAX / 8
        );
    }

    #[test]
    fn volume_pace_one_or_two_volumes_each_get_the_full_figure() {
        let full = limits(FIGURE * 70 / 100, FIGURE);
        // Whatever the rates and whatever is in flight.
        assert_eq!(volume_limits(FIGURE, MIB, &[NAS]), vec![full]);
        assert_eq!(volume_limits(FIGURE, MIB, &[0]), vec![full]);
        assert_eq!(volume_limits(FIGURE, MIB, &[NAS, SSD]), vec![full, full]);
        assert_eq!(volume_limits(FIGURE, FIGURE, &[SSD, 0]), vec![full, full]);
        assert!(volume_limits(FIGURE, MIB, &[]).is_empty());
    }

    #[test]
    fn volume_pace_three_volumes_share_two_figures_by_slowness() {
        // A network share at 60 MB/s, a USB disk at 100 MB/s, an SSD at
        // 400 MB/s. By slowness alone the network share would take about
        // 293 MiB; it is held to the figure, and the other 256 MiB goes
        // four parts to the USB disk and one to the SSD.
        let shared = volume_limits(FIGURE, MIB, &[NAS, USB, SSD]);
        assert_eq!(shared[0].hard_bytes, FIGURE);
        assert!(shared[1].hard_bytes.abs_diff(FIGURE * 4 / 5) <= 3);
        assert!(shared[2].hard_bytes.abs_diff(FIGURE / 5) <= 3);
        for volume in &shared {
            assert_eq!(volume.soft_bytes, volume.hard_bytes * 70 / 100);
        }
        // The pool is used up to within a byte per volume.
        assert!(2 * FIGURE - hard_sum(&shared) < 3);

        // The order of the answer is the order of the rates.
        let reversed = volume_limits(FIGURE, MIB, &[SSD, USB, NAS]);
        assert_eq!(reversed, vec![shared[2], shared[1], shared[0]]);
    }

    #[test]
    fn volume_pace_uncapped_shares_follow_one_over_the_rate() {
        // Rates of 100, 200 and 200 weigh 2 : 1 : 1; no share reaches the
        // figure, so the pool of two figures splits a half and two quarters.
        let shared = volume_limits(FIGURE, MIB, &[100, 200, 200]);
        assert!(shared[0].hard_bytes.abs_diff(FIGURE) <= 3);
        assert!(shared[1].hard_bytes.abs_diff(FIGURE / 2) <= 3);
        assert_eq!(shared[1], shared[2]);
    }

    #[test]
    fn volume_pace_five_volumes_share_two_figures_by_slowness() {
        // Two very slow volumes would take the whole pool between them; the
        // three fast ones keep the minimum, which comes out of the slow
        // volumes' shares evenly.
        let slow = volume_limits(FIGURE, MIB, &[1, 1, SSD, SSD, SSD]);
        let most = FIGURE - 3 * MIB / 2;
        assert_eq!(hard_bytes(&slow), [most, most, MIB, MIB, MIB]);

        // Five alike: a fifth of the pool each.
        let alike = volume_limits(FIGURE, MIB, &[USB; 5]);
        assert_eq!(hard_bytes(&alike), [2 * FIGURE / 5; 5]);

        // The reference load in flight still fits five times in the pool.
        let in_flight = in_flight_bytes(REFERENCE_CONNECTIONS, REFERENCE_ARTICLE_BYTES);
        let loaded = volume_limits(FIGURE, in_flight, &[NAS, USB, USB, SSD, SSD]);
        assert!(hard_sum(&loaded) <= 2 * FIGURE);
        assert!(loaded.iter().all(|volume| volume.hard_bytes >= in_flight));
        assert_eq!(loaded[3].hard_bytes, in_flight);
        assert!(loaded[0].hard_bytes > loaded[1].hard_bytes);
    }

    #[test]
    fn volume_pace_excess_over_the_figure_goes_to_the_other_volumes() {
        // The first volume is so slow that its share alone would be nearly
        // the whole pool. It keeps one figure; the other figure is split
        // between the rest in proportion to their own slowness, 3 : 1.
        let shared = volume_limits(FIGURE, 1, &[1, 100 * MB, 300 * MB]);
        assert_eq!(shared[0].hard_bytes, FIGURE);
        assert!(shared[1].hard_bytes.abs_diff(FIGURE * 3 / 4) <= 3);
        assert!(shared[2].hard_bytes.abs_diff(FIGURE / 4) <= 3);
    }

    #[test]
    fn volume_pace_unmeasured_volumes_share_evenly() {
        for volumes in [3_usize, 4, 5, 8] {
            let shared = volume_limits(FIGURE, MIB, &vec![0; volumes]);
            let each = 2 * FIGURE / volumes as u64;
            assert!(
                shared.iter().all(|volume| volume.hard_bytes == each),
                "{volumes} volumes: {shared:?}"
            );
        }
    }

    #[test]
    fn volume_pace_unmeasured_volume_counts_as_mean_slowness() {
        // The mean of one over 60 and one over 400 is one over about 104.3.
        let shared = volume_limits(FIGURE, MIB, &[NAS, 0, SSD]);
        let stated = volume_limits(FIGURE, MIB, &[NAS, 104_347_826, SSD]);
        for (got, want) in shared.iter().zip(&stated) {
            assert!(got.hard_bytes.abs_diff(want.hard_bytes) <= 16);
        }
        assert!(shared[0].hard_bytes > shared[1].hard_bytes);
        assert!(shared[1].hard_bytes > shared[2].hard_bytes);

        // Beside volumes that are all alike it is one more of the same.
        let alike = volume_limits(FIGURE, MIB, &[USB, 0, USB, USB]);
        assert_eq!(hard_bytes(&alike), [FIGURE / 2; 4]);
    }

    #[test]
    fn volume_pace_minimum_holds_a_fast_volume_above_what_is_in_flight() {
        // By slowness the SSD would get about 51 MiB; 60 MiB is in flight.
        let in_flight = 60 * MIB;
        let shared = volume_limits(FIGURE, in_flight, &[NAS, USB, SSD]);
        assert_eq!(shared[0].hard_bytes, FIGURE);
        assert_eq!(shared[2].hard_bytes, in_flight);
        // The USB disk gives up what the SSD gained.
        assert!(shared[1].hard_bytes.abs_diff(2 * FIGURE - FIGURE - in_flight) <= 3);
        assert!(hard_sum(&shared) <= 2 * FIGURE);
    }

    #[test]
    fn volume_pace_minimums_that_do_not_fit_give_an_even_split() {
        // Three times 200 MiB is more than the pool of 512 MiB.
        let shared = volume_limits(FIGURE, 200 * MIB, &[NAS, USB, SSD]);
        assert_eq!(hard_bytes(&shared), [2 * FIGURE / 3; 3]);
        assert!(hard_sum(&shared) <= 2 * FIGURE);

        // The minimum fitting exactly is still shared by slowness: every
        // volume gets exactly the minimum.
        let exact = volume_limits(FIGURE, 2 * FIGURE / 4, &[NAS, USB, SSD, SSD]);
        assert_eq!(hard_bytes(&exact), [FIGURE / 2; 4]);
    }

    #[test]
    fn volume_pace_limits_are_never_zero() {
        for rates in [&[0_u64][..], &[0, 0], &[0, 0, 0], &[1, u64::MAX, 7, 0, 3]] {
            for figure in [0, 1, 2, 3] {
                for in_flight in [0, 1, u64::MAX] {
                    for volume in volume_limits(figure, in_flight, rates) {
                        assert!(volume.hard_bytes >= 1);
                        assert!(volume.soft_bytes >= 1);
                        assert!(volume.soft_bytes <= volume.hard_bytes);
                    }
                }
            }
        }
    }

    #[test]
    fn volume_pace_pool_stays_within_a_quarter_of_the_memory_ceiling() {
        let rates = [NAS, USB, SSD, 0, SSD, USB];
        for profile in HardwareProfile::ALL {
            for ceiling in [256 * MIB, GIB, 3 * GIB, 16 * GIB, 64 * GIB] {
                for connections in [1_usize, 10, 70, 400] {
                    let figure = total_buffer_budget_bytes(profile, ceiling, connections, 800_000);
                    let in_flight = in_flight_bytes(connections, 800_000);
                    for count in 1..=rates.len() {
                        let shared = volume_limits(figure, in_flight, &rates[..count]);
                        assert!(
                            hard_sum(&shared) <= ceiling / 4,
                            "{profile:?}, ceiling {ceiling}, {connections} connections, {count} volumes"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn volume_pace_shares_hold_their_rules_across_rates_and_counts() {
        // A fixed sequence of rates: unmeasured, crawling, ordinary, absurd.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next_rate = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let pick = state >> 33;
            match pick % 8 {
                0 => 0,
                1 => 1 + pick % 1_000,
                2 => u64::MAX - pick % 1_000,
                3 => 100 * MB,
                _ => MB + pick % (2_000 * MB),
            }
        };
        let figures = [64, 1_000, 64 * MIB, FIGURE, 4 * GIB, u64::MAX];
        let in_flights = [0, 1, 20, 8 * MIB, 56_000_000, 200 * MIB, u64::MAX];

        for count in 1..=12_usize {
            for _ in 0..40 {
                let rates: Vec<u64> = (0..count).map(|_| next_rate()).collect();
                for figure in figures {
                    for in_flight in in_flights {
                        let shared = volume_limits(figure, in_flight, &rates);
                        let context = format!(
                            "figure {figure}, in flight {in_flight}, rates {rates:?}: {shared:?}"
                        );
                        assert_eq!(shared.len(), count, "{context}");

                        let pool = 2 * u128::from(figure);
                        let sum: u128 = shared.iter().map(|v| u128::from(v.hard_bytes)).sum();
                        assert!(sum <= pool, "{context}");

                        for volume in &shared {
                            assert!(volume.hard_bytes >= 1, "{context}");
                            assert!(volume.hard_bytes <= figure, "{context}");
                            assert_eq!(
                                u128::from(volume.soft_bytes),
                                (u128::from(volume.hard_bytes) * 70 / 100).max(1),
                                "{context}"
                            );
                        }

                        // A slower volume never gets less than a faster one.
                        for (i, slow) in rates.iter().enumerate() {
                            for (j, fast) in rates.iter().enumerate() {
                                if *slow != 0 && *fast != 0 && slow <= fast {
                                    assert!(
                                        shared[i].hard_bytes >= shared[j].hard_bytes,
                                        "{context}"
                                    );
                                }
                            }
                        }

                        if count <= 2 {
                            assert!(shared.iter().all(|v| v.hard_bytes == figure), "{context}");
                        } else if u128::from(in_flight.max(1)) * count as u128 <= pool {
                            // Shared by slowness: nobody is below the
                            // minimum and the pool is used up to within a
                            // byte per volume.
                            assert!(
                                shared.iter().all(|v| v.hard_bytes >= in_flight),
                                "{context}"
                            );
                            assert!(pool - sum < count as u128, "{context}");
                        } else {
                            let even = u64::try_from(pool / count as u128).unwrap();
                            assert!(shared.iter().all(|v| v.hard_bytes == even), "{context}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn volume_pace_unlimited_below_the_soft_limit() {
        let limits = limits(700, 1000);
        for measured in [0, 5_000] {
            assert_eq!(
                dispatch_allowance(0, limits, measured),
                DispatchAllowance::Unlimited
            );
            assert_eq!(
                dispatch_allowance(699, limits, measured),
                DispatchAllowance::Unlimited
            );
        }
    }

    #[test]
    fn volume_pace_soft_limit_holds_to_ninety_percent_of_the_measured_rate() {
        let allowance = dispatch_allowance(700, limits(700, 1000), 10_000);
        assert_eq!(rate_of(allowance), 9_000);
    }

    #[test]
    fn volume_pace_falls_between_the_soft_and_hard_limits() {
        let limits = limits(700, 1000);
        // Halfway through the span: half of 90%.
        assert_eq!(rate_of(dispatch_allowance(850, limits, 10_000)), 4_500);
        // One byte short of the hard limit: 90% of a three-hundredth.
        assert_eq!(rate_of(dispatch_allowance(999, limits, 10_000)), 30);
    }

    #[test]
    fn volume_pace_stopped_at_and_past_the_hard_limit() {
        let limits = limits(700, 1000);
        for measured in [0, 10_000, u64::MAX] {
            for buffered in [1000, 1001, u64::MAX] {
                assert_eq!(
                    dispatch_allowance(buffered, limits, measured),
                    DispatchAllowance::Stopped
                );
            }
        }
    }

    #[test]
    fn volume_pace_unmeasured_rate_falls_with_fill_alone() {
        let limits = limits(700, 1000);
        assert_eq!(rate_of(dispatch_allowance(700, limits, 0)), 300);
        assert_eq!(rate_of(dispatch_allowance(850, limits, 0)), 150);
        assert_eq!(rate_of(dispatch_allowance(999, limits, 0)), 1);
        assert_eq!(
            dispatch_allowance(1000, limits, 0),
            DispatchAllowance::Stopped
        );
    }

    #[test]
    fn volume_pace_a_rate_that_rounds_to_nothing_is_stopped() {
        // 90% of one byte per second is less than a byte.
        assert_eq!(
            dispatch_allowance(700, limits(700, 1000), 1),
            DispatchAllowance::Stopped
        );
    }

    #[test]
    fn volume_pace_soft_limit_at_or_past_the_hard_limit_never_paces() {
        for soft in [1000, 2000] {
            let limits = limits(soft, 1000);
            assert_eq!(
                dispatch_allowance(999, limits, 10_000),
                DispatchAllowance::Unlimited
            );
            assert_eq!(
                dispatch_allowance(1000, limits, 10_000),
                DispatchAllowance::Stopped
            );
        }
    }

    #[test]
    fn volume_pace_allowance_never_rises_with_fill() {
        let shapes = [
            VolumeLimits::with_hard(256 * MIB),
            VolumeLimits::with_hard(2 * 256 * MIB / 5),
            limits(700, 1000),
            limits(1, 1),
            limits(1, 2),
            limits(u64::MAX / 2, u64::MAX),
        ];
        let rates = [0, 1, 7, 10_000, 150 * MIB, u64::MAX];
        for shape in shapes {
            for measured in rates {
                let cap = u128::from(measured) * 90 / 100;
                let mut last = rank(DispatchAllowance::Unlimited);
                // 2,000 even steps from empty to past the hard limit, then
                // each boundary and its neighbours.
                let step = (shape.hard_bytes / 2_000).max(1);
                let sweep = (0..=2_100_u64).map(|i| i.saturating_mul(step));
                let mut edges = [
                    shape.soft_bytes.saturating_sub(1),
                    shape.soft_bytes,
                    shape.soft_bytes.saturating_add(1),
                    shape.hard_bytes.saturating_sub(1),
                    shape.hard_bytes,
                    shape.hard_bytes.saturating_add(1),
                ];
                edges.sort_unstable();
                let mut fills: Vec<u64> = sweep.chain(edges).collect();
                fills.sort_unstable();
                for buffered in fills {
                    let allowance = dispatch_allowance(buffered, shape, measured);
                    let now = rank(allowance);
                    assert!(
                        now <= last,
                        "allowance rose at {buffered} of {shape:?} with rate {measured}"
                    );
                    last = now;

                    if buffered < shape.soft_bytes {
                        assert_eq!(allowance, DispatchAllowance::Unlimited);
                    } else if buffered >= shape.hard_bytes {
                        assert_eq!(allowance, DispatchAllowance::Stopped);
                    } else {
                        assert_ne!(allowance, DispatchAllowance::Unlimited);
                        if measured != 0 {
                            assert!(
                                now <= cap,
                                "above 90% of {measured} at {buffered} of {shape:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn volume_pace_limiter_rate_uses_zero_for_unlimited() {
        assert_eq!(DispatchAllowance::Unlimited.limiter_rate(), Some(0));
        assert_eq!(
            dispatch_allowance(700, limits(700, 1000), 10_000).limiter_rate(),
            Some(9_000)
        );
        assert_eq!(DispatchAllowance::Stopped.limiter_rate(), None);
    }
}
