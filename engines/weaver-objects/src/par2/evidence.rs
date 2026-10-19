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

#[derive(Debug)]
pub(super) struct Evidence {
    pub(super) slices: Vec<SliceVerdict>,
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
                whole: SliceVerdict::Unknown,
                exact: false,
            };
        };
        let described = description.length;
        let exact = length == Some(described);
        let block = index.slice_size;
        let checksums = index.file_checksums(&file).unwrap_or(&[]);
        let count = index.slice_count_for_file(described) as usize;
        let slices = (0..count)
            .map(|slice| {
                let start = slice as u64 * block;
                let end = (start + block).min(described);
                let Some(expected) = checksums.get(slice) else {
                    return SliceVerdict::Unknown;
                };
                // A short final slice is compared on its zero padding, which
                // says something only when the member ends where described.
                if end - start < block && !exact {
                    return SliceVerdict::Unknown;
                }
                let folded = grid.fold(member, block, slice as u64, ByteRange::new(start, end));
                grid::verdict(folded, end - start, block, expected.crc32)
            })
            .collect();
        let whole = match expected_whole {
            Some(_) if described == 0 && exact => SliceVerdict::Intact,
            Some(expected) if exact => grid::verdict(
                grid.fold(member, WHOLE, 0, ByteRange::new(0, described)),
                described,
                0,
                expected,
            ),
            _ => SliceVerdict::Unknown,
        };
        Self {
            slices,
            whole,
            exact,
        }
    }

    pub(super) fn is_damaged(&self) -> bool {
        self.whole == SliceVerdict::Damaged || self.slices.contains(&SliceVerdict::Damaged)
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
        let veto = self.whole == SliceVerdict::Damaged;
        self.slices.iter().map(move |slice| match slice {
            SliceVerdict::Intact if veto => SliceVerdict::Unknown,
            other => *other,
        })
    }
}
