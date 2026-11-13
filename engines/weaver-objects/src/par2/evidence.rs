// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the evidence in hand says of one described file, before anything is
// read back. Two arms come from the grid: each slice's CRC32 against the
// set's, and the whole member's CRC32 against the one the slices fold to.
// The third arm, the MD5s, needs the bytes, so it is the readback's. A
// whole member that folds wrong over bytes every article vouched for vetoes
// the slices: the witness whose account it contradicts is the one that
// proved them. Damage an article already owned to vouches for nothing else,
// so it vetoes nothing, and only the slices it touches are read back.

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
    // Every article the whole folds from vouched for its bytes.
    vouched: bool,
    // The member's length is exactly the described one.
    pub(super) exact: bool,
    // Per slice, empty until the first refusal: `UNMARKED`, or refused and
    // counted, with whether the grid holds all of its bytes (`HELD`) or not
    // (`ABSENT`). A refused slice an alike slice held when it was refused is
    // left unmarked: the repair copies it and spends no block on it.
    marks: Vec<u8>,
    // The marked slices still absent: each will never come and needs a
    // block, counted as the refusals and the folds change.
    lost: u64,
}

const UNMARKED: u8 = 0;
const ABSENT: u8 = 1;
const HELD: u8 = 2;

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
                vouched: false,
                exact: false,
                marks: Vec::new(),
                lost: 0,
            };
        };
        let described = description.length;
        let count = index.slice_count_for_file(described) as usize;
        let mut evidence = Self {
            slices: vec![SliceVerdict::Unknown; count],
            damaged: 0,
            whole: SliceVerdict::Unknown,
            vouched: false,
            exact: length == Some(described),
            marks: Vec::new(),
            lost: 0,
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
                // A short final slice is compared on its zero padding, which
                // says something only when the member ends where described.
                let expected = checksums
                    .get(slice)
                    .filter(|_| end - start == block || exact);
                let marked = self.marks.get(slice).is_some_and(|mark| *mark != UNMARKED);
                let folded = (expected.is_some() || marked)
                    .then(|| grid.fold(member, block, slice as u64, ByteRange::new(start, end)))
                    .flatten();
                if marked {
                    self.mark(slice, folded.is_some());
                }
                let verdict = match expected {
                    None => SliceVerdict::Unknown,
                    Some(expected) => grid::verdict(folded, end - start, block, expected.crc32),
                };
                let was = std::mem::replace(&mut self.slices[slice], verdict);
                self.damaged += u64::from(verdict == SliceVerdict::Damaged);
                self.damaged -= u64::from(was == SliceVerdict::Damaged);
            }
        }
        let folded = grid.fold(member, WHOLE, 0, ByteRange::new(0, described));
        self.vouched = folded.is_some_and(|folded| folded.verified);
        self.whole = match expected_whole {
            Some(_) if described == 0 && exact => SliceVerdict::Intact,
            Some(expected) if exact => grid::verdict(folded, described, 0, expected),
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

    // How many refused slices are still absent. None of them is damaged:
    // a damaged slice was folded, so its bytes are held.
    pub(super) fn lost(&self) -> u64 {
        self.lost
    }

    // Whether the slice checksums say the slice is damaged.
    pub(super) fn damaged_at(&self, slice: u64) -> bool {
        usize::try_from(slice)
            .ok()
            .and_then(|at| self.slices.get(at))
            .is_some_and(|verdict| *verdict == SliceVerdict::Damaged)
    }

    // Whether a refusal already marked the slice.
    pub(super) fn marked(&self, slice: usize) -> bool {
        self.marks.get(slice).is_some_and(|mark| *mark != UNMARKED)
    }

    // A refused slice counts toward what the set needs while its bytes are
    // not all held; `held` says whether the grid holds them now.
    pub(super) fn refuse(&mut self, slice: usize, held: bool) {
        if slice >= self.slices.len() {
            return;
        }
        if self.marks.is_empty() {
            self.marks = vec![UNMARKED; self.slices.len()];
        }
        self.mark(slice, held);
    }

    fn mark(&mut self, slice: usize, held: bool) {
        let was = std::mem::replace(&mut self.marks[slice], if held { HELD } else { ABSENT });
        self.lost += u64::from(!held);
        self.lost -= u64::from(was == ABSENT);
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

    // Per-slice verdicts with the veto applied: a whole-member mismatch over
    // vouched bytes leaves no slice proven.
    pub(super) fn vetoed(&self) -> impl Iterator<Item = SliceVerdict> + '_ {
        #[cfg(test)]
        VETOED_READS.with(|reads| reads.set(reads.get() + 1));
        let veto = self.whole == SliceVerdict::Damaged && self.vouched;
        self.slices.iter().map(move |slice| match slice {
            SliceVerdict::Intact if veto => SliceVerdict::Unknown,
            other => *other,
        })
    }
}
