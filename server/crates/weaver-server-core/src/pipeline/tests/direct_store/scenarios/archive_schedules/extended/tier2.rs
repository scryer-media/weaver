//! Tier two of the archive matrix: articles that arrive wrong, recovery sets
//! as posters author them, uuencode, and the wider shapes of real posts.
//!
//! Every family crosses its axes into cells, runs each cell under every
//! extraction profile, and samples the matrix's schedules for each by a fixed
//! stride. [`sizing`] holds each family's exact count. Every run must end the
//! same way: the expected files published byte for byte, or a named failure
//! that publishes none of them. A cell the product does not yet hold to that
//! is held open by name in its family's `open_defect`.
//!
//! The campaigns are opt-in and run only from the manually dispatched
//! extended workflow; each family's smokes run in the default suite.
use super::*;
use crate::pipeline::direct_unpack::wiring::{AbortLatch, DemotionReason as ChaseDemotion};
use std::ops::Range;

mod breadth;
mod damage;
mod fixtures;
mod par2_realism;
mod par3_breadth;
mod post;
mod recovery;
mod run;
mod uu;

use post::Post;
use recovery::Geometry;
use run::Verdict;

pub(super) const PROFILES: [ExtractionProfile; 3] = [
    ExtractionProfile::DirectStore,
    ExtractionProfile::Chase,
    ExtractionProfile::Conventional,
];

/// The password every encrypted tier-two fixture is written under.
pub(super) const PASSWORD: &str = "lantern-quay";

/// A cell the product does not yet hold to its ruling.
#[derive(Clone, Copy, Debug)]
pub(super) enum Defect {
    /// The cases run, and at least one must still miss the ruling: once none
    /// does, the entry is stale and the campaign says so.
    Diverges(&'static str),
}

/// Which pool of schedules a family samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pool {
    /// The matrix's smoke schedules: every case family once.
    Smoke,
    /// The archive matrix's combined cases.
    Combined,
}

impl Pool {
    /// The pool's cases the profile includes, built once per process.
    fn all(self, profile: ExtractionProfile) -> &'static [(usize, Schedule)] {
        static POOLS: [[std::sync::OnceLock<Vec<(usize, Schedule)>>; 3]; 2] =
            [const { [const { std::sync::OnceLock::new() }; 3] }; 2];
        let at = PROFILES.iter().position(|&p| p == profile).unwrap();
        POOLS[self as usize][at].get_or_init(|| {
            let cases: Vec<(usize, Schedule)> = match self {
                Self::Smoke => schedules().into_iter().enumerate().collect(),
                Self::Combined => combined_schedule_cases(),
            };
            cases
                .into_iter()
                .filter(|(_, (_, interruption))| profile.includes(*interruption))
                .collect()
        })
    }

    /// `per` of the pool's cases the profile includes, by a fixed stride
    /// from an offset that turns with `rotation`, so neighbouring cells see
    /// different schedules and every schedule is reached across the cells.
    pub(super) fn sampled(self, profile: ExtractionProfile, per: usize, rotation: usize) -> Vec<(usize, Schedule)> {
        let all = self.all(profile);
        let n = all.len();
        if n <= per {
            return all.to_vec();
        }
        let spacing = n / per;
        (0..per)
            .map(|k| all[(k * n / per + rotation % spacing) % n].clone())
            .collect()
    }

    /// How many cases [`Self::sampled`] yields for the profile.
    pub(super) fn count(self, profile: ExtractionProfile, per: usize) -> usize {
        self.all(profile).len().min(per)
    }
}

/// One cell of a tier-two family.
pub(super) trait Cell: Copy + std::fmt::Debug {
    /// The post for a schedule's loss mask and whether it is starved.
    fn post(self, interruption: Interruption) -> Built;
    /// The defect the cell is held open for under `profile`, if any.
    fn defect(self, profile: ExtractionProfile) -> Option<Defect>;
    /// Whether the cell's recovery set is PAR2: a defect there blocks a
    /// release.
    fn par2(self) -> bool;

}

/// A post and the recovery geometry its oracle counts in.
pub(super) struct Built {
    pub post: Post,
    pub geometry: Option<Geometry>,
    /// A ruling that overrides the oracle, for a cell whose outcome follows
    /// from what it is rather than from what it loses.
    pub ruling: Option<Verdict>,
}

/// Runs one cell under one profile over `cases`, holding each run to its
/// verdict, or to its defect where the cell is held open.
pub(super) async fn run_cell<C: Cell>(cell: C, profile: ExtractionProfile, cases: Vec<(usize, Schedule)>) {
    let defect = cell.defect(profile);
    let mut built: BTreeMap<(u8, bool), Built> = BTreeMap::new();
    let mut diverged = 0;
    for (case, (order, interruption)) in cases {
        if !profile.includes(interruption) {
            continue;
        }
        let key = (
            interruption.loss().map_or(0, |(mask, _)| mask),
            interruption.fails(),
        );
        let fixture = built.entry(key).or_insert_with(|| cell.post(interruption));
        let lost = run::lost_articles(&fixture.post, interruption);
        let verdict = fixture.ruling.unwrap_or_else(|| {
            recovery::verdict(&fixture.post, fixture.geometry, &lost, interruption.fails())
        });
        let context = format!(
            "{cell:?} profile={profile:?} case={case} order={order:?} interruption={interruption:?} verdict={verdict:?} par2={}",
            cell.par2()
        );
        eprintln!("{context}");
        let ran = run::run(&fixture.post, profile, &order, interruption).await;
        let check = || ran.assert(&fixture.post, profile, verdict, &context);
        match defect {
            Some(Defect::Diverges(why)) => {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)).is_err() {
                    eprintln!("{context}: held open: {why}");
                    diverged += 1;
                }
            }
            _ => check(),
        }
    }
    if let Some(Defect::Diverges(why)) = defect
        && diverged == 0
    {
        // A hold covers the cases that trigger it; a run that samples none of
        // them is not proof the hold is stale, so it is reported, not failed.
        eprintln!("{cell:?} profile={profile:?} met its ruling on every case run while held open: {why}");
    }
}

/// A cell's smoke: every data group in order and then reversed, under every
/// profile, with nothing lost.
pub(super) async fn smoke<C: Cell>(cell: C) {
    let forward = slot_arrivals(4);
    let mut backward = forward.clone();
    backward.reverse();
    for profile in PROFILES {
        run_cell(
            cell,
            profile,
            vec![
                (0, (forward.clone(), Interruption::None)),
                (1, (backward.clone(), Interruption::None)),
            ],
        )
        .await;
    }
}

/// A cell's smoke under a loss: the first and last groups each lose their
/// middle article, in order and reversed, under every profile.
pub(super) async fn loss_smoke<C: Cell>(cell: C) {
    let forward = slot_arrivals(4);
    let mut backward = forward.clone();
    backward.reverse();
    for profile in PROFILES {
        run_cell(
            cell,
            profile,
            vec![
                (0, (forward.clone(), Interruption::Loss { mask: 0b1001, index_first: true })),
                (1, (backward.clone(), Interruption::Loss { mask: 0b1001, index_first: false })),
            ],
        )
        .await;
    }
}

/// One family's work: its (cell, profile) units with the cases each runs,
/// laid end to end and cut into contiguous shards so a shard rebuilds as few
/// fixtures as it can.
pub(super) struct Family<C> {
    pub units: Vec<(C, ExtractionProfile, Pool, usize, usize)>,
}

impl<C: Cell> Family<C> {
    pub(super) fn total(&self) -> usize {
        self.units
            .iter()
            .map(|&(_, profile, pool, per, _)| pool.count(profile, per))
            .sum()
    }

    /// Runs shard `shard` of `shards`.
    ///
    /// `WEAVER_TIER2_CASE=<n>` (or `<start>..<end>`) runs those of the
    /// family's case indices that fall in the shard.
    pub(super) async fn shard(&self, shard: usize, shards: usize) {
        assert!(shard < shards);
        let total = self.total();
        let range = shard * total / shards..(shard + 1) * total / shards;
        let replay = std::env::var("WEAVER_TIER2_CASE").ok().map(|selection| {
            match selection.split_once("..") {
                Some((start, end)) => start.parse::<usize>().unwrap()..end.parse::<usize>().unwrap(),
                None => {
                    let case = selection.parse::<usize>().unwrap();
                    case..case + 1
                }
            }
        });
        let mut at = 0;
        for &(cell, profile, pool, per, rotation) in &self.units {
            let count = pool.count(profile, per);
            let unit = at..at + count;
            at += count;
            if unit.end <= range.start || unit.start >= range.end {
                continue;
            }
            let cases: Vec<_> = pool
                .sampled(profile, per, rotation)
                .into_iter()
                .enumerate()
                .filter_map(|(k, (_, schedule))| {
                    // The run names cases by family index so the printed
                    // number is the one the replay switch takes.
                    let index = unit.start + k;
                    (range.contains(&index) && replay.as_ref().is_none_or(|replay| replay.contains(&index)))
                        .then_some((index, schedule))
                })
                .collect();
            if !cases.is_empty() {
                run_cell(cell, profile, cases).await;
            }
        }
    }
}

/// Emits `shards` ignored campaign tests for a family, as nested modules of
/// hundreds, tens and units, so shard `h d s` runs shard `100h + 10d + s`.
macro_rules! tier2_shards {
    ($family:path, $shards:expr; $($h:ident $hv:literal)+) => {
        const _: () = assert!(100 * [$($hv),+].len() == $shards);
        $(mod $h {
            use super::*;
            tier2_shards!(@tens $family, $shards, $hv);
        })+
    };
    (@tens $family:path, $shards:expr, $hv:literal) => {
        tier2_shards!(@ten $family, $shards, $hv; d0 0 d1 1 d2 2 d3 3 d4 4 d5 5 d6 6 d7 7 d8 8 d9 9);
    };
    (@ten $family:path, $shards:expr, $hv:literal; $($d:ident $dv:literal)+) => {
        $(mod $d {
            use super::*;
            tier2_shards!(@units $family, $shards, $hv, $dv; s0 0 s1 1 s2 2 s3 3 s4 4 s5 5 s6 6 s7 7 s8 8 s9 9);
        })+
    };
    (@units $family:path, $shards:expr, $hv:literal, $dv:literal; $($s:ident $sv:literal)+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $s() {
                $family().shard($hv * 100 + $dv * 10 + $sv, $shards).await;
            }
        )+
    };
}
pub(super) use tier2_shards;

/// Every family's scenario count, as its enumerator yields it.
#[test]
fn tier2_sizing() {
    let damage = damage::family().total();
    let par2 = par2_realism::family().total();
    let par3 = par3_breadth::family().total();
    let uu = uu::family().total();
    let breadth = breadth::totals();
    assert_eq!(damage, damage::TOTAL, "damage");
    assert_eq!(par2, par2_realism::TOTAL, "PAR2 realism");
    assert_eq!(par3, par3_breadth::TOTAL, "PAR3 breadth");
    assert_eq!(uu, uu::TOTAL, "uuencode");
    let total = damage + par2 + par3 + uu + breadth.iter().map(|(_, count)| count).sum::<usize>();
    eprintln!("damage={damage} par2={par2} par3={par3} uu={uu} {breadth:?} total={total}");
    assert_eq!(total, TOTAL);
}

/// Tier two's scenarios across every family.
const TOTAL: usize = damage::TOTAL
    + par2_realism::TOTAL
    + par3_breadth::TOTAL
    + uu::TOTAL
    + breadth::TOTAL;
