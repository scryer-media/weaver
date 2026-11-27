// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, Durable, Facts, Give, Kind, MemberId, Segment, SliceVerdict, Take};

use super::*;
use crate::testing::{payload, segments};

const MEMBER: MemberId = MemberId(1);

fn padded_crc(bytes: &[u8], block: usize) -> u32 {
    let mut padded = bytes.to_vec();
    padded.resize(block, 0);
    crc32(&padded)
}

#[test]
fn combine_and_zeros_agree_with_a_direct_crc() {
    let bytes = payload(1, 3000);
    let (left, right) = bytes.split_at(1234);
    assert_eq!(
        crc32_combine(crc32(left), crc32(right), right.len() as u64),
        crc32(&bytes)
    );
    assert_eq!(crc32_of_zeros(777), crc32(&[0; 777]));
    assert_eq!(crc32_of_zeros(0), crc32(&[]));
}

#[test]
fn pieces_that_tile_a_block_fold_to_its_crc() {
    let bytes = payload(2, 1024);
    let mut grid = Grid::default();
    for segment in segments(0, &bytes, 300, 1024, true) {
        grid.offer(MEMBER, 1024, &segment);
    }
    let folded = grid.fold(MEMBER, 1024, 0, ByteRange::new(0, 1024));
    assert_eq!(
        verdict(folded, 1024, 1024, crc32(&bytes)),
        SliceVerdict::Intact
    );
}

#[test]
fn a_short_last_block_is_padded_with_zeros() {
    let bytes = payload(3, 1500);
    let mut grid = Grid::default();
    for segment in segments(0, &bytes, 1500, 1024, true) {
        grid.offer(MEMBER, 1024, &segment);
    }
    let folded = grid.fold(MEMBER, 1024, 1, ByteRange::new(1024, 1500));
    assert_eq!(
        verdict(folded, 476, 1024, padded_crc(&bytes[1024..], 1024)),
        SliceVerdict::Intact
    );
}

#[test]
fn a_gap_withholds_and_a_mismatch_is_damage() {
    let bytes = payload(4, 1024);
    let mut grid = Grid::default();
    let pieces = segments(0, &bytes, 256, 1024, true);
    for segment in &pieces[1..] {
        grid.offer(MEMBER, 1024, segment);
    }
    let block = ByteRange::new(0, 1024);
    assert_eq!(
        verdict(grid.fold(MEMBER, 1024, 0, block), 1024, 1024, crc32(&bytes)),
        SliceVerdict::Unknown
    );
    grid.offer(MEMBER, 1024, &pieces[0]);
    assert_eq!(
        verdict(
            grid.fold(MEMBER, 1024, 0, block),
            1024,
            1024,
            !crc32(&bytes)
        ),
        SliceVerdict::Damaged
    );
}

#[test]
fn an_unverified_piece_withholds_proof_and_a_repair_restores_it() {
    let bytes = payload(5, 1024);
    let mut grid = Grid::default();
    let mut pieces = segments(0, &bytes, 256, 1024, true);
    pieces[2].verified = false;
    for segment in &pieces {
        grid.offer(MEMBER, 1024, segment);
    }
    let block = ByteRange::new(0, 1024);
    assert_eq!(
        verdict(grid.fold(MEMBER, 1024, 0, block), 1024, 1024, crc32(&bytes)),
        SliceVerdict::Unknown
    );
    // The repaired range arrives again and supersedes what it overlaps.
    pieces[2].verified = true;
    grid.offer(MEMBER, 1024, &pieces[2]);
    assert_eq!(
        verdict(grid.fold(MEMBER, 1024, 0, block), 1024, 1024, crc32(&bytes)),
        SliceVerdict::Intact
    );
}

#[test]
fn a_segment_crossing_blocks_is_no_evidence_for_either() {
    let bytes = payload(6, 2048);
    let mut grid = Grid::default();
    grid.offer(
        MEMBER,
        1024,
        &Segment {
            range: ByteRange::new(0, 2048),
            crc32: crc32(&bytes),
            verified: true,
        },
    );
    assert!(
        grid.fold(MEMBER, 1024, 0, ByteRange::new(0, 1024))
            .is_none()
    );
    assert!(
        grid.fold(MEMBER, 1024, 1, ByteRange::new(1024, 2048))
            .is_none()
    );
}

#[test]
fn the_whole_member_grid_folds_every_piece() {
    let bytes = payload(7, 5000);
    let mut grid = Grid::default();
    for segment in segments(0, &bytes, 700, 512, true) {
        grid.offer(MEMBER, WHOLE, &segment);
    }
    let folded = grid.fold(MEMBER, WHOLE, 0, ByteRange::new(0, 5000));
    assert_eq!(
        verdict(folded, 5000, 0, crc32(&bytes)),
        SliceVerdict::Intact
    );
}

// A piece inside a folded whole comes again with other bytes, as an article
// sent back can: what the whole folded to before stands for bytes no longer
// there, so the whole is not proved on it.
#[test]
fn a_piece_said_again_with_other_bytes_leaves_the_whole_unproved() {
    let bytes = payload(8, 5000);
    let mut grid = Grid::default();
    let pieces = segments(0, &bytes, 700, 512, true);
    for segment in &pieces {
        grid.offer(MEMBER, WHOLE, segment);
    }
    let whole = ByteRange::new(0, 5000);
    let proved = |grid: &Grid| verdict(grid.fold(MEMBER, WHOLE, 0, whole), 5000, 0, crc32(&bytes));
    assert_eq!(proved(&grid), SliceVerdict::Intact);
    let mut other = pieces[3];
    other.crc32 = !other.crc32;
    grid.offer(MEMBER, WHOLE, &other);
    assert_ne!(proved(&grid), SliceVerdict::Intact);
}

#[test]
fn a_file_over_its_run_budget_forgets_the_block() {
    // Unmergeable runs: every other byte of one huge block.
    let mut grid = Grid::default();
    let block = 1 << 20;
    for at in 0..=MAX_PENDING_RUNS_PER_FILE as u64 {
        grid.offer(
            MEMBER,
            block,
            &Segment {
                range: ByteRange::at(at * 2, 1),
                crc32: 0,
                verified: true,
            },
        );
    }
    assert!(
        grid.files
            .get(&(MEMBER, block))
            .is_none_or(FileGrid::is_empty)
    );
}

// The supersede the range probe replaced: every run of every block the
// segment spans is tested against it.
fn supersede_by_scan(grid: &mut Grid, member: MemberId, block_size: u64, segment: &Segment) {
    if segment.range.is_empty() {
        return;
    }
    let first = segment.range.start / block_size;
    let last = (segment.range.end - 1) / block_size;
    let file = grid.files.entry((member, block_size)).or_default();
    for index in first..=last {
        let overlaps = file.block(index).is_some_and(|block| {
            block
                .values()
                .any(|run| run.range.intersect(segment.range).is_some())
        });
        if overlaps {
            let (before, after) = file.change(index, |block| {
                let before = Load::of(block, index, block_size);
                block.retain(|_, run| run.range.intersect(segment.range).is_none());
                (before, Load::of(block, index, block_size))
            });
            file.load.sub(before);
            file.load.add(after);
            grid.load.sub(before);
            grid.load.add(after);
        }
    }
}

fn offer_by_scan(grid: &mut Grid, member: MemberId, block_size: u64, segments: &[Segment]) {
    for segment in segments {
        supersede_by_scan(grid, member, block_size, segment);
    }
    for segment in segments {
        grid.record(member, block_size, segment);
    }
}

// Every block of the member's grid of `block_size`, and the loads.
fn state(grid: &Grid, block_size: u64, length: u64) -> (Vec<Option<Block>>, Load, Load) {
    let file = grid.files.get(&(MEMBER, block_size));
    let blocks = (0..=length / block_size.min(length))
        .map(|index| file.and_then(|file| file.block(index)).cloned())
        .collect();
    (blocks, file.map(|file| file.load).unwrap_or_default(), grid.load)
}

// Out of order, overlapping and said again, with verified and unverified
// pieces, on the whole member's grid and on a grid of small blocks: the
// probe leaves the runs and loads the scan left, after every delivery.
#[test]
fn the_range_probe_supersedes_what_the_scan_did() {
    const LENGTH: u64 = 4096;
    let deliveries = fuzzed(LENGTH, 2000);
    for block_size in [WHOLE, 64, 100] {
        let mut probed = Grid::default();
        let mut scanned = Grid::default();
        for (at, delivery) in deliveries.iter().enumerate() {
            probed.offer_all(MEMBER, block_size, delivery);
            offer_by_scan(&mut scanned, MEMBER, block_size, delivery);
            assert_eq!(
                state(&probed, block_size, LENGTH),
                state(&scanned, block_size, LENGTH),
                "blocks of {block_size}, after delivery {at}"
            );
        }
    }
}

// Two pieces of one delivery that overlap disagree, or may, on the bytes
// they share: neither is recorded, so no run covers the stretch and a fold
// that needs it is unproven, on a grid of blocks and on the whole member's.
#[test]
fn overlapping_pieces_of_one_delivery_leave_their_stretch_unproven() {
    let bytes = payload(5, 1024);
    let piece = |start: usize, end: usize| Segment {
        range: ByteRange::new(start as u64, end as u64),
        crc32: crc32(&bytes[start..end]),
        verified: true,
    };
    for block_size in [1024, WHOLE] {
        let mut grid = Grid::default();
        grid.offer_all(MEMBER, block_size, &[piece(0, 600), piece(400, 1024)]);
        let file = &grid.files[&(MEMBER, block_size)];
        assert!(
            file.block(0)
                .is_none_or(|block| overlapping(block, ByteRange::new(400, 600))
                    .next()
                    .is_none())
        );
        assert_eq!(grid.fold(MEMBER, block_size, 0, ByteRange::new(0, 1024)), None);
        assert_eq!(grid.fold(MEMBER, block_size, 0, ByteRange::new(0, 600)), None);
        assert_eq!(file.load, Load::default());
        assert_eq!(grid.load, Load::default());
        // The same bytes delivered again, apart, are proven.
        grid.offer_all(MEMBER, block_size, &[piece(0, 400), piece(400, 1024)]);
        assert_eq!(
            grid.fold(MEMBER, block_size, 0, ByteRange::new(0, 1024)),
            Some(Folded {
                crc32: crc32(&bytes),
                verified: true,
            })
        );
    }
}

// The deliveries a fuzzed run makes over a member of `length`: out of
// order, overlapping and said again, verified and not.
fn fuzzed(length: u64, count: usize) -> Vec<Vec<Segment>> {
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move |bound: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % bound
    };
    let mut deliveries = Vec::new();
    for _ in 0..count {
        let pieces = 1 + next(3);
        let mut at = next(length);
        let mut delivery = Vec::new();
        for _ in 0..pieces {
            let len = 1 + next(300);
            let end = (at + len).min(length);
            if at < end {
                delivery.push(Segment {
                    range: ByteRange::new(at, end),
                    crc32: next(u64::from(u32::MAX)) as u32,
                    verified: next(4) != 0,
                });
            }
            at = end;
        }
        deliveries.push(delivery);
    }
    deliveries
}

// The accounting before a whole member's grid read in order counted for
// nothing: a block closed by one run over its full extent is derived,
// anything else is pending runs.
fn load_before(block: &Block, bounds: ByteRange) -> Load {
    match block.values().next() {
        Some(run) if block.len() == 1 && run.range == bounds => Load {
            pending: 0,
            derived: 1,
        },
        _ => Load {
            pending: block.len(),
            derived: 0,
        },
    }
}

// Every block of a grid, counted as `count` counts it.
fn counted(grid: &Grid, count: impl Fn(&Block, u64, u64) -> Load) -> Load {
    let mut load = Load::default();
    for (&(_, block_size), file) in &grid.files {
        for (_, blocks) in file.blocks.chunks() {
            for (&index, block) in blocks {
                load.add(count(block, index, block_size));
            }
        }
    }
    load
}

// A grid kept in facts and read back, as its owner keeps it.
#[derive(Serialize, Deserialize)]
struct Kept(Grid);

impl Durable for Kept {
    const VERSION: u16 = 1;

    fn parts(&self, take: &mut Take) -> Result<(), String> {
        self.0.parts(take)
    }

    fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        self.0.restore_parts(give)
    }
}

fn round_trip(grid: &Grid) -> Grid {
    Facts::of(Kind::Sfv, &Kept(grid.clone()))
        .read::<Kept>(Kind::Sfv)
        .expect("the grid restores")
        .0
}

// More members than the job's run budget, each read in order on its whole
// grid: none is forgotten, none holds a load, and every one folds.
#[test]
fn whole_grids_read_in_order_hold_no_load() {
    const MEMBERS: u32 = 20_000;
    let bytes = payload(9, 1000);
    let mut grid = Grid::default();
    for member in 0..MEMBERS {
        for segment in segments(0, &bytes, 300, 1000, true) {
            grid.offer(MemberId(member), WHOLE, &segment);
        }
    }
    assert_eq!(grid.load, Load::default());
    for member in 0..MEMBERS {
        let folded = grid.fold(MemberId(member), WHOLE, 0, ByteRange::new(0, 1000));
        assert_eq!(
            verdict(folded, 1000, 0, crc32(&bytes)),
            SliceVerdict::Intact,
            "member {member}"
        );
    }
}

// A whole member's grid with runs apart, or one not from its start, still
// counts each run as pending.
#[test]
fn a_whole_grid_with_runs_apart_counts_them_pending() {
    let piece = |start: u64, end: u64| Segment {
        range: ByteRange::new(start, end),
        crc32: 0,
        verified: true,
    };
    let mut grid = Grid::default();
    grid.offer_all(MEMBER, WHOLE, &[piece(0, 100), piece(200, 300)]);
    let pending = |pending| Load {
        pending,
        derived: 0,
    };
    assert_eq!(grid.load, pending(2));
    assert_eq!(grid.files[&(MEMBER, WHOLE)].load, pending(2));
    let mut late = Grid::default();
    late.offer(MEMBER, WHOLE, &piece(100, 200));
    assert_eq!(late.load, pending(1));
}

// On grids of small blocks the loads are what the accounting before
// counted, after every delivery.
#[test]
fn small_block_grids_count_as_before() {
    const LENGTH: u64 = 4096;
    let deliveries = fuzzed(LENGTH, 2000);
    for block_size in [64, 100, 1000] {
        let mut grid = Grid::default();
        for (at, delivery) in deliveries.iter().enumerate() {
            grid.offer_all(MEMBER, block_size, delivery);
            let before = counted(&grid, |block, index, block_size| {
                load_before(block, block_bounds(index, block_size))
            });
            assert_eq!(grid.load, before, "blocks of {block_size}, delivery {at}");
            assert_eq!(
                grid.files[&(MEMBER, block_size)].load,
                before,
                "blocks of {block_size}, delivery {at}"
            );
        }
    }
}

// Facts written while a whole grid read in order still counted a pending
// run come back with the loads counted again from their blocks.
#[test]
fn a_restored_grid_counts_its_loads_again() {
    let bytes = payload(10, 1000);
    let mut grid = Grid::default();
    for member in 0..3 {
        for segment in segments(0, &bytes, 300, 1000, true) {
            grid.offer(MemberId(member), WHOLE, &segment);
        }
    }
    for delivery in fuzzed(4096, 500) {
        grid.offer_all(MEMBER, 64, &delivery);
    }
    let small = grid.files[&(MEMBER, 64)].load;
    assert_ne!(small, Load::default());
    // As the accounting before kept them: one pending run per whole grid.
    let old = |pending| Load {
        pending,
        derived: 0,
    };
    for member in 0..3 {
        grid.files
            .get_mut(&(MemberId(member), WHOLE))
            .expect("a grid")
            .load = old(1);
    }
    grid.load = old(3);
    grid.load.add(small);
    let restored = round_trip(&grid);
    for member in 0..3 {
        assert_eq!(
            restored.files[&(MemberId(member), WHOLE)].load,
            Load::default()
        );
    }
    assert_eq!(restored.files[&(MEMBER, 64)].load, small);
    assert_eq!(restored.load, small);
    assert_eq!(restored.load, counted(&restored, Load::of));
}
