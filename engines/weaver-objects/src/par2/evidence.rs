// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the evidence in hand says of one described file, before anything is
// read back. Two arms come from the grid: each slice's CRC32 against the
// set's, and the whole member's CRC32 against the one the slices fold to.
// The third arm, the MD5s, needs the bytes, so it is the readback's. One
// damaged verdict from any arm vetoes the others.

use par2_rs::{FileId, Par2FileSet};
use weaver_graph::{ByteRange, MemberId, SliceVerdict};

use crate::grid::{self, Grid, WHOLE};

// How many times a member's slice verdicts were read through, for the tests
// that count it.
#[cfg(test)]
thread_local! {
    pub(super) static VETOED_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
pub(super) struct Evidence {
    // Written only by `refold`, which counts the damaged ones as it goes.
    slices: Vec<SliceVerdict>,
    damaged: u64,
    pub(super) whole: SliceVerdict,
    // The member's length is exactly the described one.
    pub(super) exact: bool,
}

impl Evidence {
    pub(super) fn of(
        grid: &Grid,
        index: &Par2FileSet,
        expected_whole: Option<u32>,
        file: FileId,
        member: MemberId,
        length: Option<u64>,
    ) -> Self {
        let Some(description) = index.file_description(&file) else {
            return Self {
                slices: Vec::new(),
                damaged: 0,
                whole: SliceVerdict::Unknown,
                exact: false,
            };
        };
        let described = description.length;
        let count = index.slice_count_for_file(described) as usize;
        let mut evidence = Self {
            slices: vec![SliceVerdict::Unknown; count],
            damaged: 0,
            whole: SliceVerdict::Unknown,
            exact: length == Some(described),
        };
        evidence.refold(
            grid,
            index,
            expected_whole,
            file,
            member,
            ByteRange::new(0, described),
        );
        evidence
    }

    // Fold again what the grid holds over `range` of the member: the slices
    // it touches and the whole. The rest of the slices stand as they were,
    // since nothing they fold from has changed.
    pub(super) fn refold(
        &mut self,
        grid: &Grid,
        index: &Par2FileSet,
        expected_whole: Option<u32>,
        file: FileId,
        member: MemberId,
        range: ByteRange,
    ) {
        let Some(description) = index.file_description(&file) else {
            return;
        };
        let described = description.length;
        let block = index.slice_size;
        let checksums = index.file_checksums(&file).unwrap_or(&[]);
        let exact = self.exact;
        if !range.is_empty() && block > 0 {
            let first = (range.start / block) as usize;
            let last = ((range.end - 1) / block) as usize;
            for slice in first..(last + 1).min(self.slices.len()) {
                let start = slice as u64 * block;
                let end = (start + block).min(described);
                let verdict = match checksums.get(slice) {
                    None => SliceVerdict::Unknown,
                    // A short final slice is compared on its zero padding,
                    // which says something only when the member ends where
                    // described.
                    Some(_) if end - start < block && !exact => SliceVerdict::Unknown,
                    Some(expected) => grid::verdict(
                        grid.fold(member, block, slice as u64, ByteRange::new(start, end)),
                        end - start,
                        block,
                        expected.crc32,
                    ),
                };
                let was = std::mem::replace(&mut self.slices[slice], verdict);
                self.damaged += u64::from(verdict == SliceVerdict::Damaged);
                self.damaged -= u64::from(was == SliceVerdict::Damaged);
            }
        }
        self.whole = match expected_whole {
            Some(_) if described == 0 && exact => SliceVerdict::Intact,
            Some(expected) if exact => grid::verdict(
                grid.fold(member, WHOLE, 0, ByteRange::new(0, described)),
                described,
                0,
                expected,
            ),
            _ => SliceVerdict::Unknown,
        };
    }

    pub(super) fn is_damaged(&self) -> bool {
        self.whole == SliceVerdict::Damaged || self.slices.contains(&SliceVerdict::Damaged)
    }

    // How many slices the slice checksums say are damaged. The whole
    // member's veto takes no slice to damaged, so this is the vetoed count
    // too.
    pub(super) fn damaged(&self) -> u64 {
        self.damaged
    }

    // Proven intact without a read: every slice intact with independent
    // coverage, at exactly the described length, and no arm says damaged.
    pub(super) fn is_proven(&self) -> bool {
        !self.is_damaged()
            && self.exact
            && self
                .slices
                .iter()
                .all(|slice| *slice == SliceVerdict::Intact)
    }

    // Per-slice verdicts with the veto applied: a whole-member mismatch
    // leaves no slice proven.
    pub(super) fn vetoed(&self) -> impl Iterator<Item = SliceVerdict> + '_ {
        #[cfg(test)]
        VETOED_READS.with(|reads| reads.set(reads.get() + 1));
        let veto = self.whole == SliceVerdict::Damaged;
        self.slices.iter().map(move |slice| match slice {
            SliceVerdict::Intact if veto => SliceVerdict::Unknown,
            other => *other,
        })
    }
}
