// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The policy for the per-volume write buffer: how many decoded bytes may wait
// for the volume writers in total, how that room is shared between volumes,
// and how fast articles may be sent towards a volume given how full its
// share is.
//
// Every function here is pure: numbers in, numbers out, no clock and no
// pipeline state. The total is a cap on what may wait, not memory taken up
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

// Past the soft limit, dispatch into a volume starts at this share of its
// measured write rate, so the buffer drains instead of holding level.
const PACE_MEASURED_RATE_PERCENT: u64 = 90;

// With no measured write rate, the room left below the hard limit may be
// filled no faster than over this many seconds.
const PACE_UNMEASURED_ROOM_FILL_SECS: u64 = 1;

// The decoded bytes that may wait for the volume writers, all volumes
// together.
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
fn in_flight_bytes(connections: usize, typical_article_bytes: u64) -> u64 {
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

// The limits of each volume that has jobs writing to it.
//
// The total is split evenly, so a slow volume that fills its share leaves
// every other volume's share untouched. No volumes reads as one. The soft
// limit sits at the same share of the hard limit as the pipeline's other
// pressure limits. Neither limit is ever zero.
pub(in crate::pipeline) fn volume_limits(
    total_budget_bytes: u64,
    writing_volumes: usize,
) -> VolumeLimits {
    let hard_bytes = (total_budget_bytes / (writing_volumes.max(1) as u64)).max(1);
    let soft_bytes = (hard_bytes.saturating_mul(DOWNLOAD_PRESSURE_SOFT_PERCENT) / 100)
        .max(1)
        .min(hard_bytes);
    VolumeLimits {
        soft_bytes,
        hard_bytes,
    }
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
    fn volume_pace_one_volume_takes_the_whole_budget() {
        let total = 256 * MIB;
        let one = volume_limits(total, 1);
        assert_eq!(one.hard_bytes, total);
        assert_eq!(one.soft_bytes, total * 70 / 100);
        // No volume yet reads as one.
        assert_eq!(volume_limits(total, 0), one);
    }

    #[test]
    fn volume_pace_volumes_split_the_budget_evenly() {
        let total = 256 * MIB;
        for volumes in [2_usize, 5] {
            let each = volume_limits(total, volumes);
            assert_eq!(each.hard_bytes, total / volumes as u64, "{volumes} volumes");
            assert_eq!(
                each.soft_bytes,
                each.hard_bytes * 70 / 100,
                "{volumes} volumes"
            );
            // Every volume full at once stays within the total, so one full
            // volume takes nothing from another.
            assert!(each.hard_bytes * volumes as u64 <= total);
        }
    }

    #[test]
    fn volume_pace_limits_are_never_zero() {
        let each = volume_limits(3, 5);
        assert_eq!(each, limits(1, 1));
        let none = volume_limits(0, 1);
        assert_eq!(none, limits(1, 1));
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
            volume_limits(256 * MIB, 1),
            volume_limits(256 * MIB, 5),
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
