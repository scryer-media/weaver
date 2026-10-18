// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verifying and rebuilding one PAR3 set through the namespace. par3-rs
// repairs files on disk; here its layout, its streaming verifier and its
// Cauchy decoder are driven directly, so every byte read and written goes
// through the namespace and no member is ever held whole.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use par3_rs::evidence::{ExtentVerdict, FileEvidence, StreamingVerifier};
use par3_rs::gf::{AnyField, for_set};
use par3_rs::layout::{BlockLayout, ExtentKind, FileExtent, FileLayout};
use par3_rs::runtime::ExecutionOptions;
use par3_rs::source::{SourceId, SourceSnapshot};
use par3_rs::{CodecLimits, Decoder, Geometry, PacketBody, Par3Set, RecoveredBlock};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{ByteRange, MemberId};

// How much of a member one verification read takes.
const READ: u64 = 1 << 20;

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
    memory_limit: u64,
    access: &mut dyn MemberAccess,
) -> SetRepair {
    let failed = |lost: u64, reachable: u64| SetRepair {
        outcome: RepairOutcome::Impossible {
            damaged: lost,
            reachable,
        },
        lost,
    };
    let options = ExecutionOptions::default();
    let Ok(layout) = BlockLayout::new(set, &options).map(Arc::new) else {
        return failed(0, 0);
    };
    let mut lost = BTreeSet::new();
    let mut rewrite = BTreeSet::new();
    for (index, file) in set.files().iter().enumerate() {
        let Some(shape) = layout.files().get(index) else {
            return failed(0, 0);
        };
        let evidence = bound
            .get(file.path())
            .and_then(|&member| evidence(&layout, index, member, &options, access));
        match evidence {
            Some(evidence) if evidence.protected_complete() => continue,
            Some(evidence) => lost.extend(unresolved(shape, &evidence)),
            None => lost.extend(shape.extents.iter().filter_map(|extent| block(&extent))),
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
    let solve = Solve {
        set,
        layout: &layout,
        bound,
        rewrite: &rewrite,
        lost: &lost,
        rows,
        limits: CodecLimits::new(memory_limit),
    };
    if solve.run(access).is_none() {
        return failed(count, available.len() as u64);
    }
    write_tails(&layout, set, bound, &rewrite, access);
    SetRepair {
        outcome: RepairOutcome::Repaired { slices: count },
        lost: count,
    }
}

// What the member's bytes prove against its description, read a piece at a
// time. A set's blocks are judged over the described range alone, so bytes
// a member carries past it are not read; a piece that does not read back
// leaves the extents it holds unknown, and the reading carries on past it.
fn evidence(
    layout: &Arc<BlockLayout>,
    file: usize,
    member: MemberId,
    options: &ExecutionOptions,
    access: &mut dyn MemberAccess,
) -> Option<FileEvidence> {
    let described = layout.files().get(file)?.len;
    let len = access.length(member)?.min(described);
    let snapshot = SourceSnapshot { len, generation: 0 };
    let source = SourceId(u64::from(member.0));
    let mut verifier =
        StreamingVerifier::new(Arc::clone(layout), file, source, snapshot, options.clone()).ok()?;
    let mut bytes = Vec::new();
    let mut offset = 0;
    while offset < len {
        let end = len.min(offset.saturating_add(READ));
        bytes.clear();
        if access.read(member, ByteRange::new(offset, end), &mut bytes)
            && bytes.len() as u64 == end - offset
        {
            verifier.feed(offset, &bytes).ok()?;
        }
        offset = end;
    }
    Some(verifier.finish())
}

// The input blocks of the file the evidence does not prove intact.
fn unresolved<'a>(
    shape: &'a FileLayout,
    evidence: &'a FileEvidence,
) -> impl Iterator<Item = u64> + 'a {
    shape
        .extents
        .iter()
        .zip(evidence.verdicts().iter())
        .filter(|(_, verdict)| matches!(verdict, ExtentVerdict::Unknown | ExtentVerdict::Damaged))
        .filter_map(|(extent, _)| block(&extent))
}

fn block(extent: &FileExtent) -> Option<u64> {
    match extent.kind {
        ExtentKind::Block { index, .. } => Some(index),
        _ => None,
    }
}

// Where a block extent and a stripe of the block meet: the member offset of
// the shared bytes and their place in the stripe.
fn overlap(extent: &FileExtent, stripe: &Range<u64>) -> Option<(u64, Range<usize>)> {
    let ExtentKind::Block { offset, .. } = extent.kind else {
        return None;
    };
    let start = offset.max(stripe.start);
    let end = (offset + (extent.range.end - extent.range.start)).min(stripe.end);
    (start < end).then(|| {
        let at = extent.range.start + (start - offset);
        let into = (start - stripe.start) as usize..(end - stripe.start) as usize;
        (at, into)
    })
}

// One rebuild: the lost blocks, the recovery rows spent on them, and the
// members the rebuilt bytes go back into.
struct Solve<'a> {
    set: &'a Par3Set,
    layout: &'a BlockLayout,
    bound: &'a BTreeMap<String, MemberId>,
    rewrite: &'a BTreeSet<usize>,
    lost: &'a [u64],
    rows: &'a [u64],
    limits: CodecLimits,
}

impl Solve<'_> {
    // The decoder holds two buffers per lost block and the matrix over them,
    // so the blocks are rebuilt a stripe at a time, each stripe as wide as
    // the memory limit leaves room for. The code is linear symbol by symbol,
    // so a stripe solves on its own.
    fn run(&self, access: &mut dyn MemberAccess) -> Option<()> {
        let field = for_set(&self.set.galois_field()).ok()?;
        let symbol = field.symbol_bytes() as u64;
        let count = self.lost.len() as u64;
        let matrix = count
            .checked_mul(count)?
            .checked_mul(2)?
            .checked_mul(symbol)?;
        let room = self.limits.max_buffer_bytes.checked_sub(matrix)? / (2 * count + 1);
        let width = room.min(self.set.block_size()) / symbol * symbol;
        if width == 0 {
            return None;
        }
        let mut start = 0;
        while start < self.set.block_size() {
            let stripe = start..self.set.block_size().min(start + width);
            let recovered = self.stripe(field.clone(), &stripe, access)?;
            self.write_back(&stripe, &recovered, access);
            start = stripe.end;
        }
        Some(())
    }

    fn stripe(
        &self,
        field: AnyField,
        stripe: &Range<u64>,
        access: &mut dyn MemberAccess,
    ) -> Option<Vec<RecoveredBlock>> {
        let geometry = Geometry {
            block_size: stripe.end - stripe.start,
            input_blocks: self.set.block_count(),
            recovery_blocks: self.rows.last()?.checked_add(1)?,
            first_recovery: 0,
        };
        let mut decoder = AnyDecoder::new(field, geometry, self.lost, self.rows, &self.limits)?;
        let mut bytes = Vec::new();
        for block in 0..self.set.block_count() {
            if self.lost.binary_search(&block).is_ok() {
                continue;
            }
            self.assemble(block, stripe, &mut bytes, access)?;
            decoder.add_input(block, &bytes)?;
        }
        let range = stripe.start as usize..stripe.end as usize;
        // A set may carry two copies of one block; each row is added once.
        for &row in self.rows {
            let recovery = self.set.recovery_blocks().iter().find(|block| {
                block.index() == row && block.data_len() as u64 == self.set.block_size()
            })?;
            decoder.add_recovery(row, recovery.data().get(range.clone())?)?;
        }
        decoder.solve()
    }

    // A stripe of a surviving input block, from every extent that names it.
    fn assemble(
        &self,
        block: u64,
        stripe: &Range<u64>,
        bytes: &mut Vec<u8>,
        access: &mut dyn MemberAccess,
    ) -> Option<()> {
        bytes.clear();
        bytes.resize((stripe.end - stripe.start) as usize, 0);
        let Some(locations) = self.layout.locations(block) else {
            return Some(());
        };
        let mut piece = Vec::new();
        for location in locations.iter() {
            let extent = self
                .layout
                .files()
                .get(location.file)?
                .extents
                .get(location.extent)?;
            let Some((at, into)) = overlap(&extent, stripe) else {
                continue;
            };
            let member = *self
                .bound
                .get(self.set.files().get(location.file)?.path())?;
            piece.clear();
            let range = ByteRange::at(at, into.len() as u64);
            if !access.read(member, range, &mut piece) || piece.len() != into.len() {
                return None;
            }
            bytes.get_mut(into)?.copy_from_slice(&piece);
        }
        Some(())
    }

    // Write each rebuilt stripe into every damaged file that names its block.
    fn write_back(
        &self,
        stripe: &Range<u64>,
        recovered: &[RecoveredBlock],
        access: &mut dyn MemberAccess,
    ) {
        for block in recovered {
            let Some(locations) = self.layout.locations(block.index()) else {
                continue;
            };
            for location in locations.iter() {
                let (true, Some(member)) = (
                    self.rewrite.contains(&location.file),
                    member_of(self.set, self.bound, location.file),
                ) else {
                    continue;
                };
                let Some(extent) = self
                    .layout
                    .files()
                    .get(location.file)
                    .and_then(|shape| shape.extents.get(location.extent))
                else {
                    continue;
                };
                if let Some((at, from)) = overlap(&extent, stripe)
                    && let Some(bytes) = block.data().get(from)
                {
                    access.write_repaired(member, at, bytes);
                }
            }
        }
    }
}

fn member_of(set: &Par3Set, bound: &BTreeMap<String, MemberId>, file: usize) -> Option<MemberId> {
    bound.get(set.files().get(file)?.path()).copied()
}

// The inline tails the rewritten files keep in their metadata.
fn write_tails(
    layout: &BlockLayout,
    set: &Par3Set,
    bound: &BTreeMap<String, MemberId>,
    rewrite: &BTreeSet<usize>,
    access: &mut dyn MemberAccess,
) {
    for &file in rewrite {
        let (Some(shape), Some(member)) = (layout.files().get(file), member_of(set, bound, file))
        else {
            continue;
        };
        for extent in shape.extents.iter() {
            if let ExtentKind::Inline(bytes) = &extent.kind {
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
    fn new(
        field: AnyField,
        geometry: Geometry,
        lost: &[u64],
        rows: &[u64],
        limits: &CodecLimits,
    ) -> Option<Self> {
        Some(match field {
            AnyField::Gf8(field) => Self::Gf8(Box::new(
                Decoder::with_limits(field, geometry, lost, rows, limits).ok()?,
            )),
            AnyField::Gf16(field) => Self::Gf16(Box::new(
                Decoder::with_limits(field, geometry, lost, rows, limits).ok()?,
            )),
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
