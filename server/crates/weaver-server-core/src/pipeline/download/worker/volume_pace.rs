// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The policy for the per-volume write buffer. Each volume has one writer and
// one buffer limit, shared by every stream writing to that volume. This
// module says how large that limit is, how the room is shared between
// volumes, and what a volume asks of its lanes given how full it is.
//
// What a limit counts (`Volume::unwritten_bytes`): the articles leased to
// lanes and not arrived, at their size in the post, what arrived and is
// being decoded, and what waits for the writer. Nothing is counted past the
// hard limit, but the one article a volume that counts nothing grants.
//
// The sizing functions here are pure: numbers in, numbers out, no clock and
// no pipeline state. `VolumeBuffers` keeps what they are given and puts
// their answers on the volumes. Each limit is a cap on what may wait, not
// memory taken up front.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use crate::operations::disk::volume::Volume;
use crate::runtime::HardwareProfile;

const MIB: u64 = 1024 * 1024;

// The Performance figure. The reference load, 70 connections of 800,000 B
// articles, has 56,000,000 B in flight; this holds that with room to put it
// in order.
const VOLUME_BUFFER_PERFORMANCE_BYTES: u64 = 256 * MIB;

// The Balanced figure: half of Performance.
const VOLUME_BUFFER_BALANCED_BYTES: u64 = 128 * MIB;

// The Efficient figure: half of Balanced.
const VOLUME_BUFFER_EFFICIENT_BYTES: u64 = 64 * MIB;

// The figure's floor, in multiples of what is in flight (connections x
// article size): a buffer smaller than what is in flight cannot put
// anything in order.
const VOLUME_BUFFER_IN_FLIGHT_MULTIPLE: u64 = 4;

// The memory ceiling, as a divisor of memory: an eighth. It caps the figure,
// and it caps what all volumes' limits add up to.
const VOLUME_BUFFER_MEMORY_DIVISOR: u64 = 8;

// The pool every volume's buffer is drawn from, in full figures. Up to this
// many volumes each get the full figure; more share the pool. Ruled by the
// operator: "full figure up to 2x then shared, weighted by drive speed".
const VOLUME_BUFFER_POOL_FIGURES: u64 = 2;

// The least a volume sharing the pool is given, however fast it is beside
// the others.
const VOLUME_BUFFER_MINIMUM_BYTES: u64 = 16 * MIB;

// A volume's soft limit, as a share of its hard limit: a reported figure.
// Leases do not look at it.
const VOLUME_BUFFER_SOFT_PERCENT: u64 = 70;

// The size of a typical article, for the figure's floor: that of the
// reference load.
pub(in crate::pipeline) const TYPICAL_ARTICLE_BYTES: u64 = 800_000;

// A volume's measured write rate has changed materially when it moved by
// this share of the rate its limit was sized on, or more.
const VOLUME_RATE_CHANGE_PERCENT: u64 = 25;

// The figure: the bytes that may wait for one volume's writer when the
// volume has a full share (see `volume_limits`).
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
    // From here the volume's lanes ask for one article at a time.
    pub(in crate::pipeline) soft_bytes: u64,
    // Here the volume's lanes stop sending.
    pub(in crate::pipeline) hard_bytes: u64,
}

impl VolumeLimits {
    // The limits of a volume whose buffer may hold `hard_bytes`. Neither
    // limit is ever zero.
    fn with_hard(hard_bytes: u64) -> Self {
        let hard_bytes = hard_bytes.max(1);
        let soft = u128::from(hard_bytes) * u128::from(VOLUME_BUFFER_SOFT_PERCENT) / 100;
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
// `figure_bytes` is what `total_buffer_budget_bytes` gives, and
// `memory_ceiling_bytes` is the memory it was given.
//
// One or two volumes each get the full figure. Three or more share a pool of
// two figures, weighted by the inverse of each volume's write rate: the
// slower the drive, the larger its buffer, and the fastest gets the
// smallest. Example with the 256 MiB figure and drives writing 2000, 200 and
// 50 MB/s: the weights are 1 : 10 : 40, the slowest is held to 256 MiB, and
// the other 256 MiB goes 1 : 10, about 23 MiB and 233 MiB.
//
// The rules that bound a share:
// - no volume gets more than the figure; what a slow volume cannot take goes
//   to the others by the same weights;
// - a volume sharing the pool gets at least 16 MiB, taken from the others;
// - a volume with no measured rate counts at the average weight of the
//   measured ones, and with none measured the split is even;
// - the hard limits never add up to more than an eighth of memory. Where
//   that is less than the volumes' full figures, it is the pool they share,
//   even for two volumes;
// - when the pool cannot give every volume the minimum, it is split evenly.
//
// No limit is ever zero: a pool of fewer bytes than there are volumes still
// gives each volume one byte.
//
// This runs when pressure is refreshed, not once per article. It allocates
// the result and nothing else.
pub(in crate::pipeline) fn volume_limits(
    figure_bytes: u64,
    memory_ceiling_bytes: u64,
    measured_write_bytes_per_sec: &[u64],
) -> Vec<VolumeLimits> {
    let rates = measured_write_bytes_per_sec;
    let volumes = rates.len() as u128;
    let figure = figure_bytes.max(1);
    let all = |hard: u64| {
        rates
            .iter()
            .map(|_| VolumeLimits::with_hard(hard))
            .collect()
    };

    let pool = (u128::from(figure) * u128::from(VOLUME_BUFFER_POOL_FIGURES)).min(u128::from(
        memory_ceiling_bytes / VOLUME_BUFFER_MEMORY_DIVISOR,
    ));
    if u128::from(figure) * volumes <= pool {
        return all(figure);
    }

    // Here the volumes share the pool. No share is above the figure or the
    // pool itself.
    let most = u64::try_from(pool).unwrap_or(u64::MAX).min(figure);
    let least = VOLUME_BUFFER_MINIMUM_BYTES.min(most);
    if u128::from(least) * volumes > pool {
        return all(u64::try_from(pool / volumes).unwrap_or(u64::MAX));
    }

    // A volume's weight is one over its write rate.
    let (weight_sum, measured) = rates
        .iter()
        .filter(|&&rate| rate != 0)
        .fold((0.0_f64, 0_u32), |(sum, count), &rate| {
            (sum + 1.0 / rate as f64, count + 1)
        });
    let unmeasured_weight = if measured == 0 {
        1.0
    } else {
        weight_sum / f64::from(measured)
    };
    let weight_of = |rate: u64| {
        if rate == 0 {
            unmeasured_weight
        } else {
            1.0 / rate as f64
        }
    };

    // A volume's share at a given level is the level times its weight, held
    // between the minimum and the most. Every share rises with the level, so
    // the level that uses up the pool is found by halving.
    let share = |level: f64, rate: u64| ((level * weight_of(rate)) as u64).clamp(least, most);
    let shares_fit = |level: f64| {
        rates
            .iter()
            .map(|&rate| u128::from(share(level, rate)))
            .sum::<u128>()
            <= pool
    };

    // At level zero every volume holds the minimum, which fits. At the top
    // level every volume holds the most, which is more than the pool. The
    // halving ends when no number lies between the two levels.
    let smallest_weight = rates
        .iter()
        .map(|&rate| weight_of(rate))
        .fold(f64::INFINITY, f64::min);
    let mut fits = 0.0_f64;
    let mut too_much = 2.0 * most as f64 / smallest_weight;
    loop {
        let level = fits + (too_much - fits) / 2.0;
        if level <= fits || level >= too_much {
            break;
        }
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

// What a volume asks of the lanes sending towards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum VolumeIntake {
    // The volume does not hold its lanes back.
    Open,
    // Its lanes stop sending.
    Stopped,
}

// The state of a volume counting `counted_bytes` against a hard
// limit of `limits.hard_bytes`. Below it the volume is open, from it
// stopped. How much a lane is handed below it is the room the volume has
// (`volume_share_of_lane`); the soft limit is a reported figure, not a line
// for leases.
pub(in crate::pipeline) fn volume_intake(counted_bytes: u64, limits: VolumeLimits) -> VolumeIntake {
    if counted_bytes >= limits.hard_bytes {
        VolumeIntake::Stopped
    } else {
        VolumeIntake::Open
    }
}

// The state of a volume as it stands: what it counts, against the limit
// leases stop at (`Volume::lease_limit`). A volume that counts nothing is
// open whatever its limit, for the one article it grants then.
pub(in crate::pipeline) fn intake_of(volume: &Volume) -> VolumeIntake {
    let (soft_bytes, _) = volume.buffer_limits();
    let counted = volume.unwritten_bytes();
    if counted == 0 {
        return VolumeIntake::Open;
    }
    volume_intake(
        counted,
        VolumeLimits {
            soft_bytes,
            hard_bytes: volume.lease_limit(),
        },
    )
}

// The keeper of every volume's limits: the one place they are sized and
// put on the volumes. It knows the volumes that have a job writing to them
// now, with how many jobs each has and the write rate it was last sized on.
// A volume no job writes to any more is forgotten: it shares nothing with
// the others, whatever it is, the stand-in for an unread device included.
//
// Sizing is asked for when a volume gains its first job or loses its last,
// when the figure changes (the profile in force, the connection count) and
// when a volume's measured rate has moved. Nothing here runs per article.
pub(crate) struct VolumeBuffers {
    sizing: Mutex<Sizing>,
}

struct Sizing {
    figure_bytes: u64,
    memory_ceiling_bytes: u64,
    // Each volume a job writes to now.
    volumes: Vec<Written>,
    // A limit changed as a volume joined or left, and nobody was told yet:
    // the pipeline's next tick hears it from `follow_rates`.
    untold: bool,
}

struct Written {
    volume: Arc<Volume>,
    // The write rate the volume's limit was sized on.
    sized_on: u64,
    // The jobs writing to the volume now: never zero.
    jobs: usize,
}

impl VolumeBuffers {
    pub(in crate::pipeline) fn new(figure_bytes: u64, memory_ceiling_bytes: u64) -> Self {
        Self {
            sizing: Mutex::new(Sizing {
                figure_bytes,
                memory_ceiling_bytes,
                volumes: Vec::new(),
                untold: false,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Sizing> {
        self.sizing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // A job was given `volume`'s writer. When it is the volume's first,
    // the volume joins those written to and every limit is sized again.
    // Otherwise one count is raised. Each call is answered by one `left`.
    pub(crate) fn joined(&self, volume: &Arc<Volume>) {
        let mut sizing = self.lock();
        if let Some(written) = sizing.find(volume) {
            written.jobs += 1;
            return;
        }
        sizing.volumes.push(Written {
            volume: Arc::clone(volume),
            sized_on: 0,
            jobs: 1,
        });
        if sizing.size() {
            sizing.untold = true;
        }
    }

    // A job that was given `volume`'s writer let it go. When it was the
    // volume's last, the volume shares nothing with the others any more and
    // their limits are sized again. Otherwise one count is lowered.
    pub(crate) fn left(&self, volume: &Arc<Volume>) {
        let mut sizing = self.lock();
        let Some(at) = sizing
            .volumes
            .iter()
            .position(|written| Arc::ptr_eq(&written.volume, volume))
        else {
            return;
        };
        sizing.volumes[at].jobs -= 1;
        if sizing.volumes[at].jobs > 0 {
            return;
        }
        sizing.volumes.swap_remove(at);
        if sizing.size() {
            sizing.untold = true;
        }
    }

    // Size every limit on another figure. Whether a limit changed.
    pub(in crate::pipeline) fn set_figure(
        &self,
        figure_bytes: u64,
        memory_ceiling_bytes: u64,
    ) -> bool {
        let mut sizing = self.lock();
        if (sizing.figure_bytes, sizing.memory_ceiling_bytes)
            == (figure_bytes, memory_ceiling_bytes)
        {
            return false;
        }
        sizing.figure_bytes = figure_bytes;
        sizing.memory_ceiling_bytes = memory_ceiling_bytes;
        sizing.size()
    }

    // Size every limit again when a volume's measured rate has moved
    // materially from the one its limit was sized on. Whether a limit
    // changed, here or as a volume joined or left since the last call.
    // With no rate moved and no such change this reads each volume's rate
    // and writes nothing.
    pub(in crate::pipeline) fn follow_rates(&self) -> bool {
        let mut sizing = self.lock();
        let moved = sizing.volumes.iter().any(|written| {
            rate_moved(
                written.sized_on,
                written.volume.write_rate.load(Ordering::Relaxed),
            )
        });
        let sized = moved && sizing.size();
        let untold = sizing.untold;
        if untold {
            sizing.untold = false;
        }
        sized || untold
    }
}

impl Sizing {
    fn find(&mut self, volume: &Arc<Volume>) -> Option<&mut Written> {
        self.volumes
            .iter_mut()
            .find(|written| Arc::ptr_eq(&written.volume, volume))
    }

    // Put each written volume's limits on it. Whether any changed.
    fn size(&mut self) -> bool {
        for written in &mut self.volumes {
            written.sized_on = written.volume.write_rate.load(Ordering::Relaxed);
        }
        let rates: Vec<u64> = self
            .volumes
            .iter()
            .map(|written| written.sized_on)
            .collect();
        let limits = volume_limits(self.figure_bytes, self.memory_ceiling_bytes, &rates);
        let mut changed = false;
        for (Written { volume, .. }, limits) in self.volumes.iter().zip(limits) {
            let now = (limits.soft_bytes, limits.hard_bytes);
            if volume.buffer_limits() != now {
                volume.set_buffer_limits(now.0, now.1);
                changed = true;
            }
        }
        changed
    }
}

// Whether `now` differs materially from `sized_on`. A rate measured for the
// first time, or no longer measured, always does.
fn rate_moved(sized_on: u64, now: u64) -> bool {
    if sized_on == 0 || now == 0 {
        return sized_on != now;
    }
    u128::from(sized_on.abs_diff(now)) * 100
        >= u128::from(sized_on) * u128::from(VOLUME_RATE_CHANGE_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;
    // The reference load: 70 connections of 800,000 B articles.
    const REFERENCE_CONNECTIONS: usize = 70;
    const REFERENCE_ARTICLE_BYTES: u64 = 800_000;

    // The Performance figure, memory that does not bind, and the write
    // rates of a hard disk, a SATA SSD and an NVMe SSD.
    const FIGURE: u64 = 256 * MIB;
    const ROOMY: u64 = 64 * GIB;
    const MB: u64 = 1_000_000;
    const HDD: u64 = 50 * MB;
    const SATA: u64 = 200 * MB;
    const NVME: u64 = 2_000 * MB;

    // Shares are rounded down, and the weights are floating point.
    const ROUNDING: u64 = 3;

    fn hard_bytes(volumes: &[VolumeLimits]) -> Vec<u64> {
        volumes.iter().map(|volume| volume.hard_bytes).collect()
    }

    fn hard_sum(volumes: &[VolumeLimits]) -> u128 {
        volumes.iter().map(|v| u128::from(v.hard_bytes)).sum()
    }

    fn assert_near(volumes: &[VolumeLimits], expected: &[u64]) {
        assert_eq!(volumes.len(), expected.len(), "{volumes:?}");
        for (volume, &want) in volumes.iter().zip(expected) {
            assert!(
                volume.hard_bytes.abs_diff(want) <= ROUNDING,
                "{volumes:?} against {expected:?}"
            );
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
        // A container's limit is the memory ceiling however small it is: an
        // eighth of 256 MiB, and of 128 MiB.
        for profile in HardwareProfile::ALL {
            for (ceiling, figure) in [(256 * MIB, 32 * MIB), (128 * MIB, 16 * MIB)] {
                let budget = total_buffer_budget_bytes(
                    profile,
                    ceiling,
                    REFERENCE_CONNECTIONS,
                    REFERENCE_ARTICLE_BYTES,
                );
                assert_eq!(budget, figure, "{profile:?}");
            }
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
        let full = VolumeLimits {
            soft_bytes: FIGURE * 70 / 100,
            hard_bytes: FIGURE,
        };
        // Whatever the rates.
        assert_eq!(volume_limits(FIGURE, ROOMY, &[HDD]), vec![full]);
        assert_eq!(volume_limits(FIGURE, ROOMY, &[0]), vec![full]);
        assert_eq!(volume_limits(FIGURE, ROOMY, &[HDD, NVME]), vec![full; 2]);
        assert_eq!(volume_limits(FIGURE, ROOMY, &[NVME, 0]), vec![full; 2]);
        assert!(volume_limits(FIGURE, ROOMY, &[]).is_empty());
    }

    #[test]
    fn volume_pace_three_volumes_share_two_figures_by_inverse_speed() {
        // 2000, 200 and 50 MB/s weigh 1 : 10 : 40. By weight alone the hard
        // disk would take about 401 MiB of the 512; it is held to the
        // figure, and the other 256 MiB goes one part to the NVMe SSD and
        // ten to the SATA SSD.
        let shared = volume_limits(FIGURE, ROOMY, &[NVME, SATA, HDD]);
        assert_near(&shared, &[FIGURE / 11, FIGURE * 10 / 11, FIGURE]);
        assert_eq!(shared[2].hard_bytes, FIGURE);
        assert_near(&shared[..2], &[24_403_223, 244_032_232]);
        for volume in &shared {
            assert_eq!(volume.soft_bytes, volume.hard_bytes * 70 / 100);
        }
        // The pool is used up, and not exceeded.
        assert!(hard_sum(&shared) <= 2 * u128::from(FIGURE));
        assert!(2 * u128::from(FIGURE) - hard_sum(&shared) <= u128::from(ROUNDING));

        // The order of the answer is the order of the rates.
        let reversed = volume_limits(FIGURE, ROOMY, &[HDD, SATA, NVME]);
        assert_eq!(reversed, vec![shared[2], shared[1], shared[0]]);
    }

    #[test]
    fn volume_pace_uncapped_shares_follow_one_over_the_rate() {
        // Rates of 100, 200 and 200 weigh 2 : 1 : 1 and no share passes the
        // figure: half of the pool, and a quarter twice.
        let shared = volume_limits(FIGURE, ROOMY, &[100 * MB, 200 * MB, 200 * MB]);
        assert_near(&shared, &[FIGURE, FIGURE / 2, FIGURE / 2]);
        assert_eq!(shared[1], shared[2]);

        // Five alike: a fifth of the pool each.
        let alike = volume_limits(FIGURE, ROOMY, &[SATA; 5]);
        assert_near(&alike, &[2 * FIGURE / 5; 5]);
    }

    #[test]
    fn volume_pace_no_volume_gets_more_than_the_figure() {
        // The first volume is so slow that its weight alone would take
        // nearly the whole pool. It keeps one figure; the other figure is
        // split between the rest by their own weights, 3 : 1.
        let shared = volume_limits(FIGURE, ROOMY, &[1, 100 * MB, 300 * MB]);
        assert_eq!(shared[0].hard_bytes, FIGURE);
        assert_near(&shared[1..], &[FIGURE * 3 / 4, FIGURE / 4]);

        // Two crawling volumes take a figure each; the fast ones keep the
        // minimum, which the pool cannot give on top, so the slow volumes
        // give it up evenly.
        let slow = volume_limits(FIGURE, ROOMY, &[1, 1, NVME, NVME, NVME]);
        let most = FIGURE - 24 * MIB;
        assert_near(&slow, &[most, most, 16 * MIB, 16 * MIB, 16 * MIB]);
    }

    #[test]
    fn volume_pace_unmeasured_volume_counts_at_the_average_measured_weight() {
        // The weights of 100 and 300 MB/s average to that of 150 MB/s.
        let shared = volume_limits(FIGURE, ROOMY, &[100 * MB, 0, 300 * MB]);
        let stated = volume_limits(FIGURE, ROOMY, &[100 * MB, 150 * MB, 300 * MB]);
        assert_near(&shared, &hard_bytes(&stated));
        // 3 : 2 : 1 of two figures.
        assert_near(&shared, &[FIGURE, 2 * FIGURE / 3, FIGURE / 3]);

        // Beside volumes that are all alike it is one more of the same.
        let alike = volume_limits(FIGURE, ROOMY, &[SATA, 0, SATA, SATA]);
        assert_near(&alike, &[FIGURE / 2; 4]);
    }

    #[test]
    fn volume_pace_unmeasured_volumes_split_evenly() {
        for volumes in [3_usize, 4, 5, 8] {
            let shared = volume_limits(FIGURE, ROOMY, &vec![0; volumes]);
            assert_eq!(
                hard_bytes(&shared),
                vec![2 * FIGURE / volumes as u64; volumes]
            );
        }
    }

    #[test]
    fn volume_pace_fast_volume_keeps_the_minimum() {
        // 20,000, 200 and 50 MB/s. The hard disk takes the figure. By
        // weight the fastest would get a hundred-and-first of the rest,
        // about 2.5 MiB; it gets 16 MiB and the SATA SSD gives that up.
        let shared = volume_limits(FIGURE, ROOMY, &[20_000 * MB, SATA, HDD]);
        assert_eq!(shared[0].hard_bytes, 16 * MIB);
        assert_near(&shared, &[16 * MIB, FIGURE - 16 * MIB, FIGURE]);
        assert!(hard_sum(&shared) <= 2 * u128::from(FIGURE));
    }

    #[test]
    fn volume_pace_minimums_that_do_not_fit_give_an_even_split() {
        // 40 volumes at 16 MiB each is more than the pool of 512 MiB.
        let rates: Vec<u64> = (1..=40).map(|i| i * MB).collect();
        let shared = volume_limits(FIGURE, ROOMY, &rates);
        assert_eq!(hard_bytes(&shared), vec![2 * FIGURE / 40; 40]);

        // 32 volumes fit exactly: every volume gets the minimum.
        let exact = volume_limits(FIGURE, ROOMY, &rates[..32]);
        assert_eq!(hard_bytes(&exact), vec![16 * MIB; 32]);
    }

    #[test]
    fn volume_pace_memory_ceiling_caps_the_sum() {
        // An eighth of 3 GiB is 384 MiB: one volume has the figure, two
        // cannot both have it and share the 384 MiB by weight.
        let memory = 3 * GIB;
        assert_eq!(hard_bytes(&volume_limits(FIGURE, memory, &[HDD])), [FIGURE]);
        let two = volume_limits(FIGURE, memory, &[SATA, SATA]);
        assert_eq!(hard_bytes(&two), [192 * MIB; 2]);
        let two = volume_limits(FIGURE, memory, &[HDD, NVME]);
        assert_near(&two, &[FIGURE, 128 * MIB]);
        let three = volume_limits(FIGURE, memory, &[NVME, SATA, HDD]);
        assert_near(&three, &[16 * MIB, 112 * MIB, FIGURE]);

        // A 1 GiB container: the figure is the whole eighth, and two
        // volumes halve it.
        let figure = total_buffer_budget_bytes(HardwareProfile::Performance, GIB, 70, 800_000);
        assert_eq!(figure, 128 * MIB);
        let two = volume_limits(figure, GIB, &[SATA, SATA]);
        assert_eq!(hard_bytes(&two), [64 * MIB; 2]);

        let rates = [HDD, SATA, NVME, 0, NVME, SATA];
        for profile in HardwareProfile::ALL {
            for ceiling in [128 * MIB, 256 * MIB, GIB, 3 * GIB, 16 * GIB, 64 * GIB] {
                for connections in [1_usize, 10, 70, 400] {
                    let figure = total_buffer_budget_bytes(profile, ceiling, connections, 800_000);
                    for count in 1..=rates.len() {
                        let shared = volume_limits(figure, ceiling, &rates[..count]);
                        assert!(
                            hard_sum(&shared) <= u128::from(ceiling / 8),
                            "{profile:?}, ceiling {ceiling}, {connections} connections, {count} volumes"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn volume_pace_limits_are_never_zero() {
        for rates in [&[0_u64][..], &[0, 0], &[0, 0, 0], &[1, u64::MAX, 7, 0, 3]] {
            for figure in [0, 1, 2, 3, u64::MAX] {
                for memory in [0, 1, 8, 64, u64::MAX] {
                    for volume in volume_limits(figure, memory, rates) {
                        assert!(volume.hard_bytes >= 1);
                        assert!(volume.soft_bytes >= 1);
                        assert!(volume.soft_bytes <= volume.hard_bytes);
                    }
                }
            }
        }
    }

    // A fixed sequence of rates: unmeasured, crawling, absurd, ordinary.
    fn rate_sequence() -> impl FnMut() -> u64 {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        move || {
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
        }
    }

    #[test]
    fn volume_pace_shares_hold_their_rules_across_rates_and_counts() {
        let mut next_rate = rate_sequence();
        for count in 1..=12_usize {
            for _ in 0..40 {
                let rates: Vec<u64> = (0..count).map(|_| next_rate()).collect();
                for figure in [64, 1_000, 64 * MIB, FIGURE, 4 * GIB, u64::MAX] {
                    for memory in [GIB, 3 * GIB, ROOMY, u64::MAX] {
                        let shared = volume_limits(figure, memory, &rates);
                        let context = format!(
                            "figure {figure}, memory {memory}, rates {rates:?}: {shared:?}"
                        );
                        assert_eq!(shared.len(), count, "{context}");

                        let pool = (2 * u128::from(figure)).min(u128::from(memory / 8));
                        assert!(hard_sum(&shared) <= pool, "{context}");

                        for volume in &shared {
                            assert!(volume.hard_bytes >= 1, "{context}");
                            assert!(volume.hard_bytes <= figure, "{context}");
                            assert_eq!(
                                u128::from(volume.soft_bytes),
                                (u128::from(volume.hard_bytes) * 70 / 100).max(1),
                                "{context}"
                            );
                        }

                        // Of two measured volumes the slower never gets
                        // less than the faster.
                        for (i, &slow) in rates.iter().enumerate() {
                            for (j, &fast) in rates.iter().enumerate() {
                                if slow != 0 && slow <= fast {
                                    assert!(
                                        shared[i].hard_bytes >= shared[j].hard_bytes,
                                        "{context}"
                                    );
                                }
                            }
                        }

                        if u128::from(figure) * count as u128 <= pool {
                            assert!(shared.iter().all(|v| v.hard_bytes == figure), "{context}");
                        } else {
                            let least = (16 * MIB).min(figure);
                            if u128::from(least) * count as u128 <= pool {
                                assert!(shared.iter().all(|v| v.hard_bytes >= least), "{context}");
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn volume_pace_intake_is_open_below_the_full_line_and_stopped_at_it() {
        let limits = VolumeLimits::with_hard(1000);
        assert_eq!(limits.soft_bytes, 700);
        assert_eq!(volume_intake(0, limits), VolumeIntake::Open);
        assert_eq!(volume_intake(700, limits), VolumeIntake::Open);
        assert_eq!(volume_intake(999, limits), VolumeIntake::Open);
        assert_eq!(volume_intake(1000, limits), VolumeIntake::Stopped);
        assert_eq!(volume_intake(u64::MAX, limits), VolumeIntake::Stopped);

        // The full Performance figure: 179.2 MiB and 256 MiB.
        let limits = VolumeLimits::with_hard(FIGURE);
        let soft = FIGURE * 70 / 100;
        assert_eq!(volume_intake(soft, limits), VolumeIntake::Open);
        assert_eq!(volume_intake(FIGURE - 1, limits), VolumeIntake::Open);
        assert_eq!(volume_intake(FIGURE, limits), VolumeIntake::Stopped);

        // A one-byte limit is open when empty and stopped otherwise.
        let limits = VolumeLimits::with_hard(1);
        assert_eq!(volume_intake(0, limits), VolumeIntake::Open);
        assert_eq!(volume_intake(1, limits), VolumeIntake::Stopped);
    }

    fn volume(rate: u64) -> Arc<Volume> {
        let volume = Arc::new(Volume::new(None, "test".to_owned()));
        volume.write_rate.store(rate, Ordering::Relaxed);
        volume
    }

    fn hard(volume: &Volume) -> u64 {
        volume.buffer_limits().1
    }

    // A volume has no limit until a job writes to it, and the full figure
    // from then on while there are no more than two. Another job on it
    // changes nothing.
    #[test]
    fn volume_buffers_size_a_volume_when_it_is_first_seen() {
        let buffers = VolumeBuffers::new(FIGURE, ROOMY);
        let (first, second) = (volume(HDD), volume(0));
        assert_eq!(first.buffer_limits(), (u64::MAX, u64::MAX));
        assert_eq!(intake_of(&first), VolumeIntake::Open);

        buffers.joined(&first);
        assert_eq!(first.buffer_limits(), (FIGURE * 70 / 100, FIGURE));
        buffers.joined(&second);
        buffers.joined(&first);
        assert_eq!(hard(&first), FIGURE);
        assert_eq!(hard(&second), FIGURE);
        assert_eq!(buffers.lock().volumes.len(), 2);
    }

    // The pool is shared only among volumes a job writes to now. A fast
    // drive, a hard disk and the stand-in for an unread device share two
    // figures while each has a job, which holds the fast drive to a small
    // share. When the stand-in's last job is gone it shares nothing any
    // more: the two drives have the full figure each again, and the
    // pipeline is told once that a limit changed. A volume that keeps a
    // job keeps its place.
    #[test]
    fn volume_buffers_share_only_among_volumes_a_job_writes_to_now() {
        let buffers = VolumeBuffers::new(FIGURE, ROOMY);
        let (nvme, hdd, unread) = (volume(NVME), volume(HDD), volume(0));
        buffers.joined(&nvme);
        buffers.joined(&hdd);
        assert_eq!((hard(&nvme), hard(&hdd)), (FIGURE, FIGURE));
        assert!(buffers.follow_rates(), "the first limits are told");
        assert!(!buffers.follow_rates());

        // Two jobs write through the stand-in.
        buffers.joined(&unread);
        buffers.joined(&unread);
        assert!(hard(&nvme) < FIGURE / 4, "{}", hard(&nvme));
        assert!(buffers.follow_rates());

        // One of them is gone: the stand-in is still written to.
        buffers.left(&unread);
        assert!(hard(&nvme) < FIGURE / 4);
        assert!(
            !buffers.follow_rates(),
            "nothing was sized, nothing is told"
        );

        // The last is gone.
        buffers.left(&unread);
        assert_eq!((hard(&nvme), hard(&hdd)), (FIGURE, FIGURE));
        assert_eq!(buffers.lock().volumes.len(), 2);
        assert!(buffers.follow_rates(), "a limit rose");
        assert!(!buffers.follow_rates(), "told once");

        // Written to again, it shares again.
        buffers.joined(&unread);
        assert!(hard(&nvme) < FIGURE / 4);
        // A volume nobody joined is left by nobody.
        buffers.left(&volume(SATA));
        assert_eq!(buffers.lock().volumes.len(), 3);
    }

    // Three volumes share two figures by the inverse of their speeds: the
    // hard disk is held to the figure, and the rest goes 1 : 10 to the NVMe
    // and the SATA drive.
    #[test]
    fn volume_buffers_give_three_volumes_inverse_speed_shares() {
        let buffers = VolumeBuffers::new(FIGURE, ROOMY);
        let (nvme, sata, hdd) = (volume(NVME), volume(SATA), volume(HDD));
        for seen in [&nvme, &sata, &hdd] {
            buffers.joined(seen);
        }
        assert_eq!(hard(&hdd), FIGURE);
        assert!(
            hard(&nvme).abs_diff(FIGURE / 11) <= ROUNDING,
            "{}",
            hard(&nvme)
        );
        assert!(
            hard(&sata).abs_diff(FIGURE * 10 / 11) <= ROUNDING,
            "{}",
            hard(&sata)
        );
        assert!(hard(&nvme) < hard(&sata) && hard(&sata) < hard(&hdd));
        for sized in [&nvme, &sata, &hdd] {
            let (soft, limit) = sized.buffer_limits();
            assert_eq!(soft, limit * 70 / 100);
        }
    }

    // A rate that moved by less than a quarter leaves every limit alone. One
    // that moved by a quarter or more, or was measured for the first time,
    // sizes them all again.
    #[test]
    fn volume_buffers_follow_a_rate_that_moved_materially() {
        let buffers = VolumeBuffers::new(FIGURE, ROOMY);
        let (nvme, sata, unmeasured) = (volume(NVME), volume(SATA), volume(0));
        for seen in [&nvme, &sata, &unmeasured] {
            buffers.joined(seen);
        }
        let sized = |volumes: [&Arc<Volume>; 3]| volumes.map(|volume| hard(volume));
        let before = sized([&nvme, &sata, &unmeasured]);
        assert!(
            buffers.follow_rates(),
            "the limits the volumes joined on are told"
        );
        assert!(!buffers.follow_rates(), "nothing moved");

        sata.write_rate.store(SATA * 110 / 100, Ordering::Relaxed);
        assert!(!buffers.follow_rates(), "a tenth is not material");
        assert_eq!(sized([&nvme, &sata, &unmeasured]), before);

        sata.write_rate.store(SATA * 3 / 4, Ordering::Relaxed);
        assert!(buffers.follow_rates());
        let slower = sized([&nvme, &sata, &unmeasured]);
        assert_ne!(slower, before, "the shares follow the new rate");
        assert!(!buffers.follow_rates(), "sized on the rate it has now");

        // The third volume's probe returns: it is the slowest of the three.
        unmeasured.write_rate.store(HDD, Ordering::Relaxed);
        assert!(buffers.follow_rates());
        assert_eq!(hard(&unmeasured), FIGURE);
        assert!(hard(&sata) < slower[1]);
    }

    #[test]
    fn volume_buffers_tell_a_material_change_of_rate() {
        assert!(!rate_moved(0, 0));
        assert!(rate_moved(0, 1));
        assert!(rate_moved(1, 0));
        assert!(!rate_moved(1000, 1000));
        assert!(!rate_moved(1000, 1249));
        assert!(!rate_moved(1000, 751));
        assert!(rate_moved(1000, 1250));
        assert!(rate_moved(1000, 750));
        assert!(rate_moved(u64::MAX, u64::MAX / 2));
    }

    // Another figure sizes every limit again; the same one changes nothing.
    // A limit lowered below what a volume holds stops it until it drains.
    #[test]
    fn volume_buffers_size_every_volume_on_another_figure() {
        let buffers = VolumeBuffers::new(FIGURE, ROOMY);
        let held = volume(SATA);
        buffers.joined(&held);
        held.count_for_test(100 * MIB);
        assert_eq!(intake_of(&held), VolumeIntake::Open);

        assert!(!buffers.set_figure(FIGURE, ROOMY));
        assert!(buffers.set_figure(64 * MIB, ROOMY));
        assert_eq!(held.buffer_limits(), (64 * MIB * 70 / 100, 64 * MIB));
        assert_eq!(intake_of(&held), VolumeIntake::Stopped);
        held.count_for_test(50 * MIB);
        assert_eq!(intake_of(&held), VolumeIntake::Open);
        held.count_for_test(0);
        assert_eq!(intake_of(&held), VolumeIntake::Open);

        // A volume seen later is sized on the figure in force.
        let later = volume(0);
        buffers.joined(&later);
        assert_eq!(hard(&later), 64 * MIB);
    }
}
