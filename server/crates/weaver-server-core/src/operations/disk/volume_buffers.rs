// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The per-volume write buffers: how the room every volume's buffer is
// drawn from is shared between the volumes written to now, and the one
// keeper that puts those limits on the volumes. Each volume has one writer
// and one buffer limit, shared by every caller writing to that volume.
//
// The figure a full share holds is set from outside (`set_figure`); this
// says how the volumes share it. The sizing functions are pure: numbers
// in, numbers out, no clock. Each limit is a cap on what may wait, not
// memory taken up front.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::volume::Volume;
use super::volume_rate::WINDOW;

const MIB: u64 = 1024 * 1024;

// The memory ceiling, as a divisor of memory: an eighth. It caps the figure,
// and it caps what all volumes' limits add up to.
pub(crate) const VOLUME_BUFFER_MEMORY_DIVISOR: u64 = 8;

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

// The split takes a volume's live rate in place of the rate it reads only
// once the live rate has been this many times above it, or below it, at
// every recompute for a whole window (see `SplitRate`).
const VOLUME_RATE_SETTLE_RATIO: u64 = 2;

// How long the live rate must stay that far off before the split takes it:
// the live rate's own window.
const VOLUME_RATE_SETTLE_WINDOW: Duration = WINDOW;

// One volume's share of the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VolumeLimits {
    // From here the volume's lanes ask for one article at a time.
    pub(crate) soft_bytes: u64,
    // Here the volume's lanes stop sending.
    pub(crate) hard_bytes: u64,
}

impl VolumeLimits {
    // The limits of a volume whose buffer may hold `hard_bytes`. Neither
    // limit is ever zero.
    pub(crate) fn with_hard(hard_bytes: u64) -> Self {
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
pub(crate) fn volume_limits(
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

    let pool = pool_bytes(figure, memory_ceiling_bytes);
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

// What every volume's hard limit may add up to: two figures, or an eighth of
// memory where that is less.
fn pool_bytes(figure_bytes: u64, memory_ceiling_bytes: u64) -> u128 {
    (u128::from(figure_bytes.max(1)) * u128::from(VOLUME_BUFFER_POOL_FIGURES)).min(u128::from(
        memory_ceiling_bytes / VOLUME_BUFFER_MEMORY_DIVISOR,
    ))
}

// `volume_limits`, where each volume already holds `held_bytes` (in the same
// order) counted against its limit. A limit only shrinks into room that is
// free: a volume whose share is below what it holds keeps what it holds (up
// to the figure), and the others give that up from their room above what
// they hold, so the limits still add up to no more than the pool. When they
// cannot (the figure or memory fell under what is held), the shares stand
// and a volume over its share stops until it drains.
//
// Answers the limits and whether any volume was held above its share: then
// the split is asked again as volumes drain, until none is.
pub(crate) fn volume_limits_holding(
    figure_bytes: u64,
    memory_ceiling_bytes: u64,
    rates: &[u64],
    held_bytes: &[u64],
) -> (Vec<VolumeLimits>, bool) {
    let shares = volume_limits(figure_bytes, memory_ceiling_bytes, rates);
    let pool = pool_bytes(figure_bytes, memory_ceiling_bytes);
    let most = u64::try_from(pool)
        .unwrap_or(u64::MAX)
        .min(figure_bytes.max(1));
    let floor = |at: usize| held_bytes.get(at).copied().unwrap_or(0).min(most);

    let raised: u128 = shares
        .iter()
        .enumerate()
        .map(|(at, share)| u128::from(floor(at).saturating_sub(share.hard_bytes)))
        .sum();
    if raised == 0 {
        return (shares, false);
    }

    // The room above what each volume holds, never down to zero.
    let spare = |at: usize, share: &VolumeLimits| {
        u128::from(share.hard_bytes.saturating_sub(floor(at).max(1)))
    };
    let total: u128 = shares
        .iter()
        .map(|share| u128::from(share.hard_bytes))
        .sum();
    let need = (total + raised).saturating_sub(pool);
    let spare_sum: u128 = shares
        .iter()
        .enumerate()
        .map(|(at, share)| spare(at, share))
        .sum();
    if need > spare_sum {
        return (shares, false);
    }

    // The room needed is taken from each volume in proportion to its room,
    // rounded up, so at least `need` is taken and no volume gives more than
    // it has.
    let limits = shares
        .iter()
        .enumerate()
        .map(|(at, share)| {
            let held = floor(at);
            if held > share.hard_bytes {
                return VolumeLimits::with_hard(held);
            }
            let room = spare(at, share);
            let cut = if spare_sum == 0 {
                0
            } else {
                (need * room).div_ceil(spare_sum).min(room)
            };
            VolumeLimits::with_hard(share.hard_bytes - u64::try_from(cut).unwrap_or(0))
        })
        .collect();
    (limits, true)
}

// The rate the split reads for one volume. The rule: the split keeps the
// first rate measured for the volume (normally the start-up probe's), and
// takes the live rate in its place only once the live rate has been at least
// twice it, or at most half of it, on the same side at every recompute for a
// whole window. A live gauge that swings with the OS cache never moves it; a
// drive that really changed speed moves it within one window of the live
// rate crossing the line, plus one recompute.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SplitRate {
    // The rate the split reads; zero until the volume is measured.
    rate: u64,
    // Since when, and on which side (`true` above), the live rate has been
    // off by the ratio at every recompute.
    off_since: Option<(Instant, bool)>,
}

impl SplitRate {
    fn new(live: u64) -> Self {
        Self {
            rate: live,
            off_since: None,
        }
    }

    // Take the live rate seen at `now`. Whether the rate the split reads
    // changed. A live rate of zero is no figure and changes nothing.
    fn follow(&mut self, live: u64, now: Instant) -> bool {
        if live == 0 {
            return false;
        }
        if self.rate == 0 {
            *self = Self::new(live);
            return true;
        }
        let above = live >= self.rate.saturating_mul(VOLUME_RATE_SETTLE_RATIO);
        let below = live.saturating_mul(VOLUME_RATE_SETTLE_RATIO) <= self.rate;
        if !above && !below {
            self.off_since = None;
            return false;
        }
        match self.off_since {
            Some((since, side)) if side == above => {
                if now.saturating_duration_since(since) < VOLUME_RATE_SETTLE_WINDOW {
                    return false;
                }
                *self = Self::new(live);
                true
            }
            _ => {
                self.off_since = Some((now, above));
                false
            }
        }
    }
}

// The keeper of every volume's limits: the one place they are sized and
// put on the volumes. It knows the volumes that have a job writing to them
// now, with how many jobs each has and the write rate it was last sized on.
// A volume no job writes to any more is forgotten: it shares nothing with
// the others, whatever it is, the stand-in for an unread device included.
//
// Sizing is asked for when a volume gains its first job or loses its last,
// when the figure changes (the profile in force, the connection count),
// when the rate the split reads for a volume changed (`SplitRate`) and while
// a volume is held above its share. Nothing here runs per article.
pub struct VolumeBuffers {
    sizing: Mutex<Sizing>,
    // Told, outside the lock, whenever a limit changed: a writer that waits
    // on a volume's room without the pipeline's tick (the torrents) looks
    // again.
    resized: Mutex<Option<Resized>>,
}

type Resized = Arc<dyn Fn() + Send + Sync>;

struct Sizing {
    // The figure and the memory it was given; none until the first is set,
    // and no volume is sized until then.
    figure: Option<(u64, u64)>,
    // Each volume a job writes to now.
    volumes: Vec<Written>,
    // A limit changed as a volume joined or left, and nobody was told yet:
    // the pipeline's next tick hears it from `follow_rates`.
    untold: bool,
    // A volume was held above its share as the limits were last sized: they
    // are sized again at every tick until none is.
    holding: bool,
}

struct Written {
    volume: Arc<Volume>,
    // The write rate the split reads for the volume.
    rate: SplitRate,
    // The jobs writing to the volume now: never zero.
    jobs: usize,
}

impl VolumeBuffers {
    // No figure yet: no volume is sized until `set_figure` gives one.
    #[expect(
        clippy::new_without_default,
        reason = "one keeper per process, built where the writers are"
    )]
    pub fn new() -> Self {
        Self {
            sizing: Mutex::new(Sizing {
                figure: None,
                volumes: Vec::new(),
                untold: false,
                holding: false,
            }),
            resized: Mutex::new(None),
        }
    }

    // Have `resized` told whenever a limit changes.
    pub(crate) fn on_resize(&self, resized: Resized) {
        *self.resized.lock().unwrap_or_else(PoisonError::into_inner) = Some(resized);
    }

    fn told(&self, changed: bool) -> bool {
        if changed {
            let resized = self
                .resized
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            if let Some(resized) = resized {
                resized();
            }
        }
        changed
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
            rate: SplitRate::new(volume.write_rate.load(Ordering::Relaxed)),
            jobs: 1,
        });
        let changed = sizing.size();
        if changed {
            sizing.untold = true;
        }
        drop(sizing);
        self.told(changed);
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
        let changed = sizing.size();
        if changed {
            sizing.untold = true;
        }
        drop(sizing);
        self.told(changed);
    }

    // Size every limit on another figure. Whether a limit changed.
    pub(crate) fn set_figure(&self, figure_bytes: u64, memory_ceiling_bytes: u64) -> bool {
        let mut sizing = self.lock();
        let figure = Some((figure_bytes, memory_ceiling_bytes));
        if sizing.figure == figure {
            return false;
        }
        sizing.figure = figure;
        let changed = sizing.size();
        drop(sizing);
        self.told(changed)
    }

    // Size every limit again when the rate the split reads for a volume
    // changed (`SplitRate`), or while a volume is held above its share.
    // Whether a limit changed, here or as a volume joined or left since the
    // last call. With neither, this reads each volume's rate and writes
    // nothing.
    pub(crate) fn follow_rates(&self) -> bool {
        self.follow_rates_at(Instant::now())
    }

    fn follow_rates_at(&self, now: Instant) -> bool {
        let mut sizing = self.lock();
        let mut moved = false;
        for written in &mut sizing.volumes {
            let live = written.volume.write_rate.load(Ordering::Relaxed);
            moved |= written.rate.follow(live, now);
        }
        let sized = (moved || sizing.holding) && sizing.size();
        let untold = sizing.untold;
        if untold {
            sizing.untold = false;
        }
        drop(sizing);
        // A change heard as a volume joined or left was told then.
        self.told(sized);
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
    // A volume still unmeasured takes its first measured rate here; every
    // other keeps the rate the split reads for it.
    fn size(&mut self) -> bool {
        let Some((figure_bytes, memory_ceiling_bytes)) = self.figure else {
            return false;
        };
        for written in &mut self.volumes {
            if written.rate.rate == 0 {
                written.rate = SplitRate::new(written.volume.write_rate.load(Ordering::Relaxed));
            }
        }
        let rates: Vec<u64> = self
            .volumes
            .iter()
            .map(|written| written.rate.rate)
            .collect();
        let held: Vec<u64> = self
            .volumes
            .iter()
            .map(|written| written.volume.unwritten_bytes())
            .collect();
        let (limits, holding) =
            volume_limits_holding(figure_bytes, memory_ceiling_bytes, &rates, &held);
        self.holding = holding;
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

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;
    // A full figure, memory that does not bind, and the write rates of a
    // hard disk, a SATA SSD and an NVMe SSD.
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

    // A keeper sized on `figure_bytes` of `memory_ceiling_bytes`.
    fn sized(figure_bytes: u64, memory_ceiling_bytes: u64) -> VolumeBuffers {
        let buffers = VolumeBuffers::new();
        buffers.set_figure(figure_bytes, memory_ceiling_bytes);
        buffers
    }

    // Whether writes to `volume` stop as it stands: what it counts has
    // reached its hard limit less the share its writer keeps. One that
    // counts nothing never stops.
    fn stopped(volume: &Volume) -> bool {
        let counted = volume.unwritten_bytes();
        let (_, hard) = volume.buffer_limits();
        counted != 0 && hard != u64::MAX && counted >= hard.saturating_sub(volume.write_piece())
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
        let buffers = sized(FIGURE, ROOMY);
        let (first, second) = (volume(HDD), volume(0));
        assert_eq!(first.buffer_limits(), (u64::MAX, u64::MAX));
        assert!(!stopped(&first));

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
        let buffers = sized(FIGURE, ROOMY);
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
        let buffers = sized(FIGURE, ROOMY);
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

    // The pipeline's tune tick, and the clock the tests drive it on.
    const TICK: Duration = Duration::from_secs(5);

    fn hards(volumes: &[&Arc<Volume>]) -> Vec<u64> {
        volumes.iter().map(|volume| hard(volume)).collect()
    }

    fn sum_of(volumes: &[&Arc<Volume>]) -> u128 {
        volumes.iter().map(|volume| u128::from(hard(volume))).sum()
    }

    // Three volumes joined on their probes, and the limits told once.
    fn three_joined() -> (VolumeBuffers, [Arc<Volume>; 3], Instant) {
        let buffers = sized(FIGURE, ROOMY);
        let volumes = [volume(NVME), volume(SATA), volume(HDD)];
        for seen in &volumes {
            buffers.joined(seen);
        }
        let start = Instant::now();
        assert!(buffers.follow_rates_at(start), "the first limits are told");
        (buffers, volumes, start)
    }

    #[test]
    fn volume_pace_split_rate_takes_a_lasting_change_only() {
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);

        // Unmeasured: the first figure is taken at once; zero is no figure.
        let mut rate = SplitRate::new(0);
        assert!(!rate.follow(0, at(0)));
        assert!(rate.follow(SATA, at(0)));
        assert_eq!(rate.rate, SATA);
        assert!(!rate.follow(0, at(100)));
        assert_eq!(rate.rate, SATA);

        // Within twice and half it is never off, however long.
        for tick in 0..100 {
            let live = if tick % 2 == 0 {
                SATA * 2 - 1
            } else {
                SATA / 2 + 1
            };
            assert!(!rate.follow(live, at(tick * 5)));
        }
        assert_eq!(rate, SplitRate::new(SATA));

        // Off on one side for less than a window: kept, and a return
        // inside the line starts the count over.
        assert!(!rate.follow(SATA * 2, at(1000)));
        assert!(!rate.follow(SATA * 3, at(1025)));
        assert!(!rate.follow(SATA, at(1029)));
        assert!(!rate.follow(SATA * 2, at(1030)));
        assert!(!rate.follow(SATA * 2, at(1059)));
        // A turn to the other side starts it over too.
        assert!(!rate.follow(SATA / 2, at(1060)));
        assert!(!rate.follow(SATA / 2, at(1089)));
        assert_eq!(rate.rate, SATA);
        // A whole window below: taken, as it stands then.
        assert!(rate.follow(SATA / 3, at(1090)));
        assert_eq!(rate, SplitRate::new(SATA / 3));

        // No overflow at the ends.
        let mut rate = SplitRate::new(u64::MAX);
        assert!(!rate.follow(u64::MAX, at(0)));
        assert!(!rate.follow(1, at(0)));
        assert!(rate.follow(1, at(30)));
        assert_eq!(rate.rate, 1);
    }

    // (1) Steady rates, with the gauge's ordinary wobble, never change a
    // limit and never ask for a sizing.
    #[test]
    fn volume_buffers_steady_rates_never_change_a_limit() {
        let (buffers, volumes, start) = three_joined();
        let [nvme, sata, hdd] = &volumes;
        let before = hards(&[nvme, sata, hdd]);
        for tick in 1..=240_u32 {
            // Up to a fifth either way.
            let wobble = |rate: u64| rate / 100 * u64::from(80 + (tick * 7) % 41);
            nvme.write_rate.store(wobble(NVME), Ordering::Relaxed);
            sata.write_rate.store(wobble(SATA), Ordering::Relaxed);
            hdd.write_rate.store(wobble(HDD), Ordering::Relaxed);
            assert!(!buffers.follow_rates_at(start + TICK * tick));
            assert_eq!(hards(&[nvme, sata, hdd]), before);
        }
    }

    // (2) A gauge swinging between x and 4x moves no limit at all: not
    // every tick, not two ticks each way, not between half and twice the
    // rate the split reads. The stated fraction is zero.
    #[test]
    fn volume_buffers_a_swinging_rate_moves_no_limit() {
        let patterns: [fn(u32) -> u64; 3] = [
            |tick| if tick % 2 == 0 { SATA } else { 4 * SATA },
            |tick| if tick / 2 % 2 == 0 { SATA } else { 4 * SATA },
            |tick| if tick % 2 == 0 { SATA / 2 } else { 2 * SATA },
        ];
        for pattern in patterns {
            let (buffers, volumes, start) = three_joined();
            let [nvme, sata, hdd] = &volumes;
            let before = hards(&[nvme, sata, hdd]);
            for tick in 1..=240 {
                sata.write_rate.store(pattern(tick), Ordering::Relaxed);
                // The fastest drive swings too, a step behind.
                nvme.write_rate
                    .store(NVME / SATA * pattern(tick + 1), Ordering::Relaxed);
                assert!(!buffers.follow_rates_at(start + TICK * tick));
                assert_eq!(hards(&[nvme, sata, hdd]), before);
                assert!(sum_of(&[nvme, sata, hdd]) <= 2 * u128::from(FIGURE));
            }
        }
    }

    // (3) A drive that really became four times slower moves the limits,
    // one window after the gauge first showed it, and not before.
    #[test]
    fn volume_buffers_a_lasting_change_moves_the_limits_within_a_window() {
        let (buffers, volumes, start) = three_joined();
        let [nvme, sata, hdd] = &volumes;
        let before = hards(&[nvme, sata, hdd]);
        sata.write_rate.store(SATA / 4, Ordering::Relaxed);
        let ticks_in_window = (WINDOW.as_secs() / TICK.as_secs()) as u32;
        for tick in 1..=ticks_in_window {
            assert!(!buffers.follow_rates_at(start + TICK * tick));
            assert_eq!(hards(&[nvme, sata, hdd]), before);
        }
        assert!(buffers.follow_rates_at(start + TICK * (ticks_in_window + 1)));
        let after = hards(&[nvme, sata, hdd]);
        // The SATA drive now writes as slowly as the hard disk: it takes
        // more, and the NVMe drive falls to the minimum.
        assert!(after[1] > before[1], "{before:?} -> {after:?}");
        assert!(after[0] < before[0], "{before:?} -> {after:?}");
        assert!(sum_of(&[nvme, sata, hdd]) <= 2 * u128::from(FIGURE));
        // And it holds there.
        assert!(!buffers.follow_rates_at(start + TICK * (ticks_in_window + 2)));
    }

    // (4) One or two volumes have the full figure each, whatever the
    // gauges do.
    #[test]
    fn volume_buffers_one_or_two_volumes_keep_the_full_figure() {
        let buffers = sized(FIGURE, ROOMY);
        let (first, second) = (volume(SATA), volume(HDD));
        buffers.joined(&first);
        let start = Instant::now();
        for tick in 0..120_u32 {
            if tick == 10 {
                buffers.joined(&second);
            }
            let live = if tick % 2 == 0 { SATA } else { 4 * SATA };
            first.write_rate.store(live, Ordering::Relaxed);
            // A lasting change from tick 40 on.
            second
                .write_rate
                .store(if tick < 40 { HDD } else { HDD / 10 }, Ordering::Relaxed);
            let _ = buffers.follow_rates_at(start + TICK * tick);
            assert_eq!(hard(&first), FIGURE);
            if tick >= 10 {
                assert_eq!(hard(&second), FIGURE);
            }
        }
    }

    // A volume whose share falls under what it holds keeps what it holds;
    // the others give it up from their free room, and as it drains its
    // limit comes down to its share. The pool is never passed.
    #[test]
    fn volume_buffers_a_limit_only_shrinks_into_free_room() {
        let (buffers, volumes, start) = three_joined();
        let [nvme, sata, hdd] = &volumes;
        // The SATA drive holds nearly all of its share.
        let share = hard(sata);
        sata.count_for_test(share - MIB);
        // The NVMe drive becomes a crawl for good: it takes the large share
        // and the SATA drive's falls far below what it holds.
        nvme.write_rate.store(HDD / 10, Ordering::Relaxed);
        let mut tick = 1;
        while !buffers.follow_rates_at(start + TICK * tick) {
            tick += 1;
        }
        assert_eq!(hard(sata), share - MIB, "held, not cut under its bytes");
        assert!(stopped(sata));
        assert!(sum_of(&[nvme, sata, hdd]) <= 2 * u128::from(FIGURE));
        assert!(buffers.lock().holding);

        // It drains in steps; at each tick its limit follows it down.
        let target = volume_limits(FIGURE, ROOMY, &[HDD / 10, SATA, HDD]);
        for held in [share / 2, share / 4, 0] {
            sata.count_for_test(held);
            tick += 1;
            let _ = buffers.follow_rates_at(start + TICK * tick);
            assert_eq!(hard(sata), held.max(target[1].hard_bytes));
            assert!(sum_of(&[nvme, sata, hdd]) <= 2 * u128::from(FIGURE));
        }
        assert_eq!(hards(&[nvme, sata, hdd]), hard_bytes(&target));
        assert!(!buffers.lock().holding);
        tick += 1;
        assert!(!buffers.follow_rates_at(start + TICK * tick));
    }

    // Across a long sequence of swinging and lasting rates, joins, leaves
    // and held bytes, the limits never add up to more than the pool, and a
    // volume is never cut under what it holds while the pool has room.
    #[test]
    fn volume_buffers_never_pass_the_pool_while_rates_swing() {
        for memory in [ROOMY, 3 * GIB] {
            let pool = (2 * u128::from(FIGURE)).min(u128::from(memory / 8));
            let buffers = sized(FIGURE, memory);
            let volumes: Vec<Arc<Volume>> =
                [NVME, SATA, HDD, SATA, 0].into_iter().map(volume).collect();
            let mut next = rate_sequence();
            let start = Instant::now();
            let mut joined = [false; 5];
            for tick in 0..2_000_u32 {
                let pick = next();
                let at = (pick % 5) as usize;
                match pick % 7 {
                    0 if !joined[at] => {
                        buffers.joined(&volumes[at]);
                        joined[at] = true;
                    }
                    1 if joined[at] => {
                        buffers.left(&volumes[at]);
                        joined[at] = false;
                    }
                    _ => {}
                }
                for (i, written) in volumes.iter().enumerate() {
                    // Each gauge swings x to 4x, and the base moves for good
                    // every 300 ticks.
                    let base = [NVME, SATA, HDD, SATA, HDD][i] >> ((tick / 300 + i as u32) % 3);
                    let live = if (tick + i as u32).is_multiple_of(2) {
                        base
                    } else {
                        4 * base
                    };
                    written.write_rate.store(live, Ordering::Relaxed);
                    let room = hard(written).min(FIGURE);
                    written.count_for_test(next() % room.max(1));
                }
                let _ = buffers.follow_rates_at(start + TICK * tick);
                let sum: u128 = volumes
                    .iter()
                    .zip(joined)
                    .filter(|(_, joined)| *joined)
                    .map(|(volume, _)| u128::from(hard(volume)))
                    .sum();
                assert!(sum <= pool, "tick {tick}: {sum} over {pool}");
            }
        }
    }

    #[test]
    fn volume_pace_holding_split_keeps_held_bytes_inside_the_pool() {
        let mut next = rate_sequence();
        for count in 1..=8_usize {
            for _ in 0..200 {
                let rates: Vec<u64> = (0..count).map(|_| next()).collect();
                let held: Vec<u64> = (0..count).map(|_| next() % (FIGURE + MIB)).collect();
                for memory in [GIB, 3 * GIB, ROOMY] {
                    let pool = (2 * u128::from(FIGURE)).min(u128::from(memory / 8));
                    let most = FIGURE.min(memory / 8);
                    let (limits, holding) = volume_limits_holding(FIGURE, memory, &rates, &held);
                    let plain = volume_limits(FIGURE, memory, &rates);
                    let context = format!("rates {rates:?}, held {held:?}: {limits:?}");
                    assert!(hard_sum(&limits) <= pool, "{context}");
                    let floors: u128 = held.iter().map(|&h| u128::from(h.min(most))).sum();
                    if floors <= pool / 2 {
                        // Room enough: nobody is cut under what it holds.
                        for (limit, &h) in limits.iter().zip(&held) {
                            assert!(limit.hard_bytes >= h.min(most), "{context}");
                        }
                    }
                    if !holding {
                        assert_eq!(limits, plain, "{context}");
                    }
                    for limit in &limits {
                        assert!(
                            limit.hard_bytes >= 1 && limit.hard_bytes <= most,
                            "{context}"
                        );
                    }
                }
            }
        }
    }

    // Another figure sizes every limit again; the same one changes nothing.
    // A limit lowered below what a volume holds stops it until it drains.
    #[test]
    fn volume_buffers_size_every_volume_on_another_figure() {
        let buffers = sized(FIGURE, ROOMY);
        let held = volume(SATA);
        buffers.joined(&held);
        held.count_for_test(100 * MIB);
        assert!(!stopped(&held));

        assert!(!buffers.set_figure(FIGURE, ROOMY));
        assert!(buffers.set_figure(64 * MIB, ROOMY));
        assert_eq!(held.buffer_limits(), (64 * MIB * 70 / 100, 64 * MIB));
        assert!(stopped(&held));
        held.count_for_test(50 * MIB);
        assert!(!stopped(&held));
        held.count_for_test(0);
        assert!(!stopped(&held));

        // A volume seen later is sized on the figure in force.
        let later = volume(0);
        buffers.joined(&later);
        assert_eq!(hard(&later), 64 * MIB);
    }
}
