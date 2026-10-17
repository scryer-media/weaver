// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verifying and rebuilding one PAR3 set through the namespace. par3-rs
// repairs files on disk; here its layout and its Cauchy decoder are driven
// directly, so every byte read and written goes through the namespace.

use std::collections::{BTreeMap, BTreeSet};

use par3_rs::gf::{AnyField, for_set};
use par3_rs::layout::{BlockLayout, ExtentKind, FileLayout};
use par3_rs::runtime::ExecutionOptions;
use par3_rs::{Decoder, FileVerdict, Geometry, PacketBody, Par3Set, RecoveredBlock, verify_file};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{ByteRange, MemberId};

pub(super) struct SetRepair {
    pub(super) outcome: RepairOutcome,
    // Lost input blocks found, for the recovery asked for while waiting.
    pub(super) lost: u64,
}

// Recovery blocks the set can spend: computed with its one Cauchy matrix,
// carried with it, and of the set's block size. Ascending.
pub(super) fn usable_recovery(set: &Par3Set) -> Vec<u64> {
    let matrix = match set.matrix_packets() {
        [only] if matches!(only.body(), PacketBody::CauchyMatrix(_)) => only.hash(),
        _ => return Vec::new(),
    };
    let mut rows: Vec<u64> = set
        .recovery_blocks()
        .iter()
        .filter(|block| {
            block.matrix_hash() == matrix
                && block.matrix_present()
                && block.data_len() as u64 == set.block_size()
        })
        .map(|block| block.index())
        .collect();
    rows.sort_unstable();
    rows.dedup();
    rows
}

pub(super) fn repair_set(
    set: &Par3Set,
    bound: &BTreeMap<String, MemberId>,
    coming: bool,
    access: &mut dyn MemberAccess,
) -> SetRepair {
    let failed = |lost: u64, reachable: u64| SetRepair {
        outcome: RepairOutcome::Impossible {
            damaged: lost,
            reachable,
        },
        lost,
    };
    let Ok(layout) = BlockLayout::new(set, &ExecutionOptions::default()) else {
        return failed(0, 0);
    };
    let mut lost = BTreeSet::new();
    let mut rewrite = BTreeSet::new();
    for (index, file) in set.files().iter().enumerate() {
        let Some(shape) = layout.files().get(index) else {
            return failed(0, 0);
        };
        let member = bound.get(file.path()).copied();
        let verdict = match member {
            Some(member) => read_member(member, access)
                .map_or(FileVerdict::Missing, |bytes| verify_file(set, file, &bytes)),
            None => FileVerdict::Missing,
        };
        match verdict {
            FileVerdict::Complete => continue,
            FileVerdict::Missing => lost.extend(blocks_of(shape, |_| true)),
            FileVerdict::Damaged {
                actual_size,
                damaged_blocks,
                unchecked_blocks,
                damaged_tail_blocks,
                ..
            } => {
                lost.extend(damaged_blocks);
                lost.extend(unchecked_blocks);
                lost.extend(damaged_tail_blocks);
                // A truncation takes the blocks past the end with it.
                lost.extend(blocks_of(shape, |range| range.end > actual_size));
            }
            _ => return failed(0, 0),
        }
        rewrite.insert(index);
    }
    let count = lost.len() as u64;
    if rewrite.is_empty() {
        return SetRepair {
            outcome: RepairOutcome::Clean,
            lost: 0,
        };
    }
    let available = usable_recovery(set);
    if (available.len() as u64) < count {
        if coming {
            return SetRepair {
                outcome: RepairOutcome::Waiting,
                lost: count,
            };
        }
        return failed(count, available.len() as u64);
    }
    let lost: Vec<u64> = lost.into_iter().collect();
    let rows = &available[..lost.len()];
    let Some(recovered) = solve(set, &layout, bound, &lost, rows, access) else {
        return failed(count, available.len() as u64);
    };
    write_back(&layout, set, bound, &rewrite, &recovered, access);
    SetRepair {
        outcome: RepairOutcome::Repaired { slices: count },
        lost: count,
    }
}

fn read_member(member: MemberId, access: &mut dyn MemberAccess) -> Option<Vec<u8>> {
    let length = access.length(member)?;
    let mut bytes = Vec::new();
    access
        .read(member, ByteRange::new(0, length), &mut bytes)
        .then_some(bytes)
}

// The input blocks of a file's extents whose range passes `keep`.
fn blocks_of(shape: &FileLayout, keep: impl Fn(&std::ops::Range<u64>) -> bool) -> Vec<u64> {
    (0..shape.extents.len())
        .filter_map(|at| shape.extents.get(at))
        .filter(|extent| keep(&extent.range))
        .filter_map(|extent| match extent.kind {
            ExtentKind::Block { index, .. } => Some(index),
            _ => None,
        })
        .collect()
}

// A surviving input block, assembled from every extent that names it.
fn assemble(
    layout: &BlockLayout,
    set: &Par3Set,
    bound: &BTreeMap<String, MemberId>,
    block: u64,
    access: &mut dyn MemberAccess,
) -> Option<Vec<u8>> {
    let mut bytes = vec![0u8; usize::try_from(set.block_size()).ok()?];
    let Some(locations) = layout.locations(block) else {
        return Some(bytes);
    };
    let mut piece = Vec::new();
    for location in locations.iter() {
        let shape = layout.files().get(location.file)?;
        let extent = shape.extents.get(location.extent)?;
        let ExtentKind::Block { offset, .. } = extent.kind else {
            continue;
        };
        let member = *bound.get(set.files().get(location.file)?.path())?;
        piece.clear();
        let range = ByteRange::new(extent.range.start, extent.range.end);
        if !access.read(member, range, &mut piece) || piece.len() as u64 != range.len() {
            return None;
        }
        let at = usize::try_from(offset).ok()?;
        bytes.get_mut(at..at + piece.len())?.copy_from_slice(&piece);
    }
    Some(bytes)
}

fn solve(
    set: &Par3Set,
    layout: &BlockLayout,
    bound: &BTreeMap<String, MemberId>,
    lost: &[u64],
    rows: &[u64],
    access: &mut dyn MemberAccess,
) -> Option<Vec<RecoveredBlock>> {
    let geometry = Geometry {
        block_size: set.block_size(),
        input_blocks: set.block_count(),
        recovery_blocks: rows.last()?.checked_add(1)?,
        first_recovery: 0,
    };
    let mut decoder = AnyDecoder::new(for_set(&set.galois_field()).ok()?, geometry, lost, rows)?;
    for block in 0..set.block_count() {
        if lost.binary_search(&block).is_ok() {
            continue;
        }
        let bytes = assemble(layout, set, bound, block, access)?;
        decoder.add_input(block, &bytes)?;
    }
    // A set may carry two copies of one block; each row is added once.
    for &row in rows {
        let recovery = set
            .recovery_blocks()
            .iter()
            .find(|block| block.index() == row && block.data_len() as u64 == set.block_size())?;
        decoder.add_recovery(row, recovery.data())?;
    }
    decoder.solve()
}

// Write each rebuilt block into every damaged file that names it, and the
// inline tails those files keep in their metadata.
fn write_back(
    layout: &BlockLayout,
    set: &Par3Set,
    bound: &BTreeMap<String, MemberId>,
    rewrite: &BTreeSet<usize>,
    recovered: &[RecoveredBlock],
    access: &mut dyn MemberAccess,
) {
    let member_of = |file: usize| {
        let path = set.files().get(file)?.path();
        bound.get(path).copied()
    };
    for block in recovered {
        let Some(locations) = layout.locations(block.index()) else {
            continue;
        };
        for location in locations.iter() {
            let (true, Some(member)) = (rewrite.contains(&location.file), member_of(location.file))
            else {
                continue;
            };
            let Some(extent) = layout
                .files()
                .get(location.file)
                .and_then(|shape| shape.extents.get(location.extent))
            else {
                continue;
            };
            let ExtentKind::Block { offset, .. } = extent.kind else {
                continue;
            };
            let start = offset as usize;
            let end = start + (extent.range.end - extent.range.start) as usize;
            if let Some(bytes) = block.data().get(start..end) {
                access.write_repaired(member, extent.range.start, bytes);
            }
        }
    }
    for &file in rewrite {
        let (Some(shape), Some(member)) = (layout.files().get(file), member_of(file)) else {
            continue;
        };
        for at in 0..shape.extents.len() {
            if let Some(extent) = shape.extents.get(at)
                && let ExtentKind::Inline(bytes) = &extent.kind
            {
                access.write_repaired(member, extent.range.start, bytes);
            }
        }
    }
}

// The decoder for whichever field the set declared.
enum AnyDecoder {
    Gf8(Box<Decoder<par3_rs::Gf8>>),
    Gf16(Box<Decoder<par3_rs::Gf16>>),
}

impl AnyDecoder {
    fn new(field: AnyField, geometry: Geometry, lost: &[u64], rows: &[u64]) -> Option<Self> {
        Some(match field {
            AnyField::Gf8(field) => {
                Self::Gf8(Box::new(Decoder::new(field, geometry, lost, rows).ok()?))
            }
            AnyField::Gf16(field) => {
                Self::Gf16(Box::new(Decoder::new(field, geometry, lost, rows).ok()?))
            }
        })
    }

    fn add_input(&mut self, index: u64, data: &[u8]) -> Option<()> {
        match self {
            Self::Gf8(decoder) => decoder.add_input_block(index, data).ok(),
            Self::Gf16(decoder) => decoder.add_input_block(index, data).ok(),
        }
    }

    fn add_recovery(&mut self, index: u64, data: &[u8]) -> Option<()> {
        match self {
            Self::Gf8(decoder) => decoder.add_recovery_block(index, data).ok(),
            Self::Gf16(decoder) => decoder.add_recovery_block(index, data).ok(),
        }
    }

    fn solve(self) -> Option<Vec<RecoveredBlock>> {
        match self {
            Self::Gf8(decoder) => decoder.solve().ok(),
            Self::Gf16(decoder) => decoder.solve().ok(),
        }
    }
}
