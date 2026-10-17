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
            .is_none_or(|file| file.blocks.is_empty())
    );
}
