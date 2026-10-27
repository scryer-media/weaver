// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verifying and rebuilding one PAR3 set through the namespace. par3-rs
// repairs files on disk; here its layout, its streaming verifier and its
// Cauchy and FFT decoders are driven directly, so every byte read and
// written goes through the namespace and no member is ever held whole.
//
// A lost input block the set carries as a Data packet, whose bytes the
// set's own fingerprints vouch for, is input the set holds: it is written
// back as it is and costs no recovery block.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use par3_rs::evidence::{ExtentVerdict, FileEvidence, StreamingVerifier};
use par3_rs::fft::{FftCodec, FftGeometry, FftInput};
use par3_rs::gf::{AnyField, for_set};
use par3_rs::layout::{BlockLayout, ExtentKind, FileExtent, FileLayout};
use par3_rs::runtime::{EngineError, ExecutionOptions};
use par3_rs::source::{SourceId, SourceSnapshot};
use par3_rs::{
    CodecLimits, Decoder, FingerprintHasher, Geometry, PacketBody, Par3Set, RecoveredBlock,
    RecoveryBlock,
};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{Budget, ByteRange, MemberId};

// How much of a member one verification read takes.
const READ: u64 = 1 << 20;

pub(super) struct SetRepair {
    pub(super) outcome: RepairOutcome,
    // Lost input blocks found, for the recovery asked for while waiting.
    pub(super) lost: u64,
}

// The one matrix a set's recovery blocks were computed with.
enum Matrix {
    // One code over every input block.
    Cauchy,
    // The blocks of `coverage` dealt round `cohorts` by index, each cohort
    // its own code of at most `1 << capacity_log2` recovery blocks; a
    // recovery block's cohort is its index round the cohorts too.
    Fft {
        coverage: Range<u64>,
        cohorts: u64,
        capacity_log2: i8,
    },
}

impl Matrix {
    fn of(set: &Par3Set) -> Option<(par3_rs::Fingerprint, Self)> {
        let [only] = set.matrix_packets() else {
            return None;
        };
        let matrix = match only.body() {
            PacketBody::CauchyMatrix(_) => Self::Cauchy,
            PacketBody::FftMatrix(fft) => {
                let count = set.block_count();
                let range = fft.range;
                let coverage = match range.covers_all() {
                    true => 0..count,
                    false if range.first < range.end && range.end <= count => {
                        range.first..range.end
                    }
                    false => return None,
                };
                Self::Fft {
                    coverage,
                    cohorts: fft.interleave.checked_add(1)?,
                    capacity_log2: fft.max_recovery_blocks_log2,
                }
            }
            _ => return None,
        };
        Some((only.hash(), matrix))
    }

    // Whether the recovery rows rebuild the lost blocks: as many rows as
    // blocks, and under FFT that many in every cohort, over blocks the
    // matrix covers.
    fn rebuilds(&self, lost: &[u64], rows: &[u64]) -> bool {
        match self {
            Self::Cauchy => rows.len() >= lost.len(),
            Self::Fft {
                coverage, cohorts, ..
            } => {
                let have = by_cohort(rows, *cohorts);
                lost.iter().all(|block| coverage.contains(block))
                    && by_cohort(lost, *cohorts)
                        .iter()
                        .all(|(cohort, lost)| have.get(cohort).map_or(0, Vec::len) >= lost.len())
            }
        }
    }
}

// Indices grouped by their cohort, ascending in each.
fn by_cohort(indices: &[u64], cohorts: u64) -> BTreeMap<u64, Vec<u64>> {
    let mut grouped: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for &index in indices {
        grouped.entry(index % cohorts).or_default().push(index);
    }
    grouped
}

// Recovery blocks the set can spend: computed with its one matrix, carried
// with it, and of the set's block size. Ascending.
pub(super) fn usable_recovery(set: &Par3Set) -> Vec<u64> {
    let Some((matrix, _)) = Matrix::of(set) else {
        return Vec::new();
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

// The input blocks the set carries as Data packets whose bytes every
// extent naming the block vouches for by its fingerprint. A block some
// extent of has no fingerprint is not vouched for, and is not carried.
fn carried<'s>(set: &'s Par3Set, layout: &BlockLayout) -> BTreeMap<u64, &'s [u8]> {
    let mut carried = BTreeMap::new();
    for packet in set.data_packets() {
        let data = packet.data.as_slice();
        if data.len() as u64 > set.block_size() || carried.contains_key(&packet.block_index) {
            continue;
        }
        if vouched(layout, packet.block_index, data) {
            carried.insert(packet.block_index, data);
        }
    }
    carried
}

fn vouched(layout: &BlockLayout, block: u64, data: &[u8]) -> bool {
    let Some(locations) = layout.locations(block) else {
        return false;
    };
    let mut any = false;
    for location in locations.iter() {
        let Some(extent) = layout
            .files()
            .get(location.file)
            .and_then(|shape| shape.extents.get(location.extent))
        else {
            return false;
        };
        let ExtentKind::Block {
            offset,
            fingerprint: Some(expected),
            ..
        } = extent.kind
        else {
            return false;
        };
        let length = extent.range.end - extent.range.start;
        let mut hash = FingerprintHasher::new();
        let start = (offset as usize).min(data.len());
        let end = offset.saturating_add(length).min(data.len() as u64) as usize;
        hash.update(&data[start..end.max(start)]);
        hash.update_zeros(length - (end.max(start) - start) as u64);
        if hash.finalize() != expected {
            return false;
        }
        any = true;
    }
    any
}

pub(super) fn repair_set(
    set: &Par3Set,
    bound: &BTreeMap<String, MemberId>,
    coming: bool,
    budget: Budget,
    access: &mut dyn MemberAccess,
) -> SetRepair {
    // The ranges the bytes proved bad, which a repair that cannot rebuild
    // them reports to the namespace, so the members' coverage says what the
    // set found: damage only the set's fingerprints can see arrived marked
    // clean.
    let mut proven: Vec<(MemberId, ByteRange)> = Vec::new();
    let failed = |lost: u64,
                  reachable: u64,
                  proven: &[(MemberId, ByteRange)],
                  access: &mut dyn MemberAccess| {
        for &(member, range) in proven {
            access.found_damaged(member, range);
        }
        SetRepair {
            outcome: RepairOutcome::Impossible {
                damaged: lost,
                reachable,
            },
            lost,
        }
    };
    let mut options = ExecutionOptions::default();
    options.workers = budget.threads();
    let Ok(layout) = BlockLayout::new(set, &options).map(Arc::new) else {
        return failed(0, 0, &proven, access);
    };
    let mut lost = BTreeSet::new();
    let mut rewrite = BTreeSet::new();
    for (index, file) in set.files().iter().enumerate() {
        let Some(shape) = layout.files().get(index) else {
            return failed(0, 0, &proven, access);
        };
        let member = bound.get(file.path()).copied();
        let evidence = member.and_then(|member| evidence(&layout, index, member, &options, access));
        match (member, evidence) {
            (_, Some(evidence)) if evidence.protected_complete() => continue,
            (Some(member), Some(evidence)) => {
                proven.extend(damaged(shape, &evidence).map(|range| (member, range)));
                lost.extend(unresolved(shape, &evidence));
            }
            _ => lost.extend(shape.extents.iter().filter_map(|extent| block(&extent))),
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
    let carried = carried(set, &layout);
    let lost: Vec<u64> = lost
        .into_iter()
        .filter(|block| !carried.contains_key(block))
        .collect();
    let available = usable_recovery(set);
    let matrix = Matrix::of(set);
    let rebuilds = lost.is_empty()
        || matrix
            .as_ref()
            .is_some_and(|(_, matrix)| matrix.rebuilds(&lost, &available));
    if !rebuilds {
        if coming {
            return SetRepair {
                outcome: RepairOutcome::Waiting,
                lost: count,
            };
        }
        return failed(count, available.len() as u64, &proven, access);
    }
    let rows = match &matrix {
        Some((_, Matrix::Cauchy)) => &available[..lost.len()],
        _ => &available[..],
    };
    let solve = Solve {
        set,
        layout: &layout,
        bound,
        rewrite: &rewrite,
        carried: &carried,
        lost: &lost,
        rows,
        limits: CodecLimits::new(budget.memory_or(super::REPAIR_MEMORY_LIMIT)),
    };
    solve.write_carried(access);
    let solved = match &matrix {
        _ if lost.is_empty() => Some(()),
        Some((_, Matrix::Cauchy)) => solve.run(access),
        Some((
            _,
            Matrix::Fft {
                coverage,
                cohorts,
                capacity_log2,
            },
        )) => solve.fft(coverage, *cohorts, *capacity_log2, access),
        None => None,
    };
    if solved.is_none() {
        return failed(count, available.len() as u64, &proven, access);
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

// The member ranges of the file whose bytes disagreed with the set.
fn damaged<'a>(
    shape: &'a FileLayout,
    evidence: &'a FileEvidence,
) -> impl Iterator<Item = ByteRange> + 'a {
    shape
        .extents
        .iter()
        .zip(evidence.verdicts().iter())
        .filter(|(_, verdict)| matches!(verdict, ExtentVerdict::Damaged))
        .map(|(extent, _)| ByteRange::new(extent.range.start, extent.range.end))
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

// One rebuild: the lost blocks, the recovery rows spent on them, the
// blocks the set carries, and the members the rebuilt bytes go back into.
struct Solve<'a> {
    set: &'a Par3Set,
    layout: &'a BlockLayout,
    bound: &'a BTreeMap<String, MemberId>,
    rewrite: &'a BTreeSet<usize>,
    carried: &'a BTreeMap<u64, &'a [u8]>,
    lost: &'a [u64],
    rows: &'a [u64],
    limits: CodecLimits,
}

impl Solve<'_> {
    // Each carried block, into every damaged file that names it.
    fn write_carried(&self, access: &mut dyn MemberAccess) {
        let whole = 0..self.set.block_size();
        let mut block = Vec::new();
        for (&index, data) in self.carried {
            block.clear();
            block.extend_from_slice(data);
            block.resize(whole.end as usize, 0);
            self.put(index, &whole, &block, access);
        }
    }

    // Each cohort with a lost block decoded on its own, a stripe at a time
    // as the codec reads them; a cohort's last positions past the matrix's
    // range are zero.
    fn fft(
        &self,
        coverage: &Range<u64>,
        cohorts: u64,
        capacity_log2: i8,
        access: &mut dyn MemberAccess,
    ) -> Option<()> {
        let span = coverage.end - coverage.start;
        let geometry = FftGeometry::new(span.div_ceil(cohorts), capacity_log2).ok()?;
        let codec = FftCodec::new(geometry, ExecutionOptions::default()).ok()?;
        let access = RefCell::new(access);
        let rows = by_cohort(self.rows, cohorts);
        for (cohort, lost) in by_cohort(self.lost, cohorts) {
            let first = coverage.start + (cohort + cohorts - coverage.start % cohorts) % cohorts;
            let block = |local: usize| first + local as u64 * cohorts;
            let lost: Vec<usize> = lost
                .iter()
                .map(|index| ((index - first) / cohorts) as usize)
                .collect();
            let recovery: BTreeMap<usize, &RecoveryBlock> = rows
                .get(&cohort)?
                .iter()
                .map(|&row| Some(((row / cohorts) as usize, self.recovery(row)?)))
                .collect::<Option<_>>()?;
            let indices: Vec<usize> = recovery.keys().copied().collect();
            let mut bytes = Vec::new();
            let refused = || EngineError::InvalidState("a PAR3 input did not read back");
            codec
                .decode(
                    self.set.block_size(),
                    &lost,
                    &indices,
                    |row, offset, out| {
                        let start = offset as usize;
                        match row {
                            FftInput::Original(local) if block(local) >= coverage.end => {
                                out.fill(0);
                            }
                            FftInput::Original(local) => {
                                let stripe = offset..offset + out.len() as u64;
                                let mut access = access.borrow_mut();
                                self.assemble(block(local), &stripe, &mut bytes, *access)
                                    .ok_or_else(refused)?;
                                out.copy_from_slice(&bytes);
                            }
                            FftInput::Recovery(local) => {
                                let data = recovery
                                    .get(&local)
                                    .and_then(|row| row.data().get(start..start + out.len()))
                                    .ok_or_else(refused)?;
                                out.copy_from_slice(data);
                            }
                        }
                        Ok(())
                    },
                    |local, offset, rebuilt| {
                        let stripe = offset..offset + rebuilt.len() as u64;
                        self.put(block(local), &stripe, rebuilt, *access.borrow_mut());
                        Ok(())
                    },
                )
                .ok()?;
        }
        Some(())
    }

    // The recovery block a row names, of the set's block size: a set may
    // carry two copies of one block.
    fn recovery(&self, row: u64) -> Option<&RecoveryBlock> {
        self.set
            .recovery_blocks()
            .iter()
            .find(|block| block.index() == row && block.data_len() as u64 == self.set.block_size())
    }

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
        for &row in self.rows {
            let recovery = self.recovery(row)?;
            decoder.add_recovery(row, recovery.data().get(range.clone())?)?;
        }
        decoder.solve()
    }

    // A stripe of a surviving input block: from the set when it carries
    // the block, and otherwise from every extent that names it.
    fn assemble(
        &self,
        block: u64,
        stripe: &Range<u64>,
        bytes: &mut Vec<u8>,
        access: &mut dyn MemberAccess,
    ) -> Option<()> {
        bytes.clear();
        bytes.resize((stripe.end - stripe.start) as usize, 0);
        if let Some(data) = self.carried.get(&block) {
            let start = (stripe.start as usize).min(data.len());
            let end = (stripe.end as usize).min(data.len());
            bytes[..end - start].copy_from_slice(&data[start..end]);
            return Some(());
        }
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
            self.put(block.index(), stripe, block.data(), access);
        }
    }

    // One stripe of a rebuilt block into every damaged file that names it.
    fn put(&self, block: u64, stripe: &Range<u64>, data: &[u8], access: &mut dyn MemberAccess) {
        let Some(locations) = self.layout.locations(block) else {
            return;
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
                && let Some(bytes) = data.get(from)
            {
                access.write_repaired(member, at, bytes);
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
