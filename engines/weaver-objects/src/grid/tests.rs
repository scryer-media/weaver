// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{ByteRange, MemberId, Segment, SliceVerdict};

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
        let bounds = block_bounds(index, block_size);
        let overlaps = file.block(index).is_some_and(|block| {
            block
                .values()
                .any(|run| run.range.intersect(segment.range).is_some())
        });
        if overlaps {
            let (before, after) = file.change(index, |block| {
                let before = Load::of(block, bounds);
                block.retain(|_, run| run.range.intersect(segment.range).is_none());
                (before, Load::of(block, bounds))
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
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move |bound: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % bound
    };
    let mut deliveries = Vec::new();
    for _ in 0..2000 {
        let pieces = 1 + next(3);
        let mut at = next(LENGTH);
        let mut delivery = Vec::new();
        for _ in 0..pieces {
            let len = 1 + next(300);
            let end = (at + len).min(LENGTH);
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
