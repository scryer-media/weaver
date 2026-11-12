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
//
// The set's data and recovery blocks stay in its volumes: each is read back
// a stripe at a time as the decoder takes it, never held whole.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use par3_rs::evidence::{ExtentVerdict, FileEvidence, StreamingVerifier};
use par3_rs::fft::{FftCodec, FftGeometry, FftInput};
use par3_rs::gf::{AnyField, for_set};
use par3_rs::ingest::PayloadRef;
use par3_rs::layout::{BlockLayout, ExtentKind, FileExtent, FileLayout};
use par3_rs::runtime::{EngineError, ExecutionOptions};
use par3_rs::source::{SourceId, SourceSnapshot};
use par3_rs::{
    CodecLimits, Decoder, FingerprintHasher, Geometry, PacketBody, Par3Set, RecoveredBlock,
};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{Budget, ByteRange, MemberId};

use super::volumes::{Payloads, Row, Volumes};

// How much of a member one verification read takes.
pub(super) const READ: u64 = 1 << 20;

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
pub(super) fn usable_recovery(set: &Par3Set, payloads: &Payloads) -> Vec<u64> {
    let Some((matrix, _)) = Matrix::of(set) else {
        return Vec::new();
    };
    let mut rows: Vec<u64> = payloads
        .recovery
        .iter()
        .filter(|row| row.matrix == matrix && row.present && row.payload.len() == set.block_size())
        .map(|row| row.index)
        .collect();
    rows.sort_unstable();
    rows.dedup();
    rows
}

// The input blocks the set carries as Data packets whose bytes every
// extent naming the block vouches for by its fingerprint. A block some
// extent of has no fingerprint is not vouched for, and is not carried, nor
// is one whose bytes do not read back.
fn carried<'p>(
    set: &Par3Set,
    layout: &BlockLayout,
    (payloads, volumes): (&'p Payloads, &Volumes),
    access: &mut dyn MemberAccess,
) -> BTreeMap<u64, &'p PayloadRef> {
    let mut carried = BTreeMap::new();
    let mut piece = Vec::new();
    for (index, data) in &payloads.data {
        if data.len() > set.block_size() || carried.contains_key(index) {
            continue;
        }
        if vouched(layout, *index, (data, volumes, &mut piece), access) {
            carried.insert(*index, data);
        }
    }
    carried
}

fn vouched(
    layout: &BlockLayout,
    block: u64,
    (data, volumes, piece): (&PayloadRef, &Volumes, &mut Vec<u8>),
    access: &mut dyn MemberAccess,
) -> bool {
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
        let start = offset.min(data.len());
        let end = offset.saturating_add(length).min(data.len()).max(start);
        let mut at = start;
        while at < end {
            let take = (end - at).min(READ);
            piece.resize(take as usize, 0);
            if !volumes.read(data, at, piece, access) {
                return false;
            }
            hash.update(piece);
            at += take;
        }
        hash.update_zeros(length - (end - start));
        if hash.finalize() != expected {
            return false;
        }
        any = true;
    }
    any
}

pub(super) fn repair_set(
    (set, payloads, volumes): (&Par3Set, &Payloads, &Volumes),
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
    // The extents the members proved, by fingerprint and length, and those
    // they did not, with the member each would go back into.
    let mut held = BTreeMap::new();
    let mut unproven = Vec::new();
    let mut rewrite = BTreeSet::new();
    for (index, file) in set.files().iter().enumerate() {
        let Some(shape) = layout.files().get(index) else {
            return failed(0, 0, &proven, access);
        };
        let member = bound.get(file.path()).copied();
        let evidence = member.and_then(|member| evidence(&layout, index, member, &options, access));
        if let (Some(member), Some(evidence)) = (member, &evidence) {
            for extent in verdicted(shape, evidence, ExtentVerdict::Intact) {
                if let Some(key) = sum(&extent) {
                    held.entry(key).or_insert((member, extent.range));
                }
            }
        }
        match (member, evidence) {
            (_, Some(evidence)) if evidence.protected_complete() => continue,
            (Some(member), Some(evidence)) => {
                proven.extend(damaged(shape, &evidence).map(|range| (member, range)));
                unproven.extend(unresolved(shape, &evidence).map(|extent| (Some(member), extent)));
            }
            (member, _) => unproven.extend(shape.extents.iter().map(|extent| (member, extent))),
        }
        rewrite.insert(index);
    }
    let memory = budget.memory_or(super::REPAIR_MEMORY_LIMIT);
    let copied = copy_held(&held, &unproven, memory.max(READ), access);
    proven.retain(|(member, range)| !copied.contains(&(*member, range.start, range.end)));
    let lost: BTreeSet<u64> = unproven
        .iter()
        .filter(|(member, extent)| {
            member.is_none_or(|member| {
                !copied.contains(&(member, extent.range.start, extent.range.end))
            })
        })
        .filter_map(|(_, extent)| block(extent))
        .collect();
    let count = lost.len() as u64;
    if rewrite.is_empty() {
        return SetRepair {
            outcome: RepairOutcome::Clean,
            lost: 0,
        };
    }
    let rebuilt = count + copied.len() as u64;
    let carried = carried(set, &layout, (payloads, volumes), access);
    let lost: Vec<u64> = lost
        .into_iter()
        .filter(|block| !carried.contains_key(block))
        .collect();
    let available = usable_recovery(set, payloads);
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
        payloads,
        volumes,
        layout: &layout,
        bound,
        rewrite: &rewrite,
        carried: &carried,
        lost: &lost,
        rows,
        limits: CodecLimits::new(memory),
    };
    if solve.write_carried(access).is_none() {
        return failed(count, available.len() as u64, &proven, access);
    }
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
        outcome: RepairOutcome::Repaired { slices: rebuilt },
        lost: count,
    }
}

// An extent's fingerprint and length, which name its bytes.
fn sum(extent: &FileExtent) -> Option<(par3_rs::Fingerprint, u64)> {
    match extent.kind {
        ExtentKind::Block {
            fingerprint: Some(fingerprint),
            ..
        } => Some((fingerprint, extent.range.end - extent.range.start)),
        _ => None,
    }
}

// An extent is its bytes: one a member did not prove whose fingerprint an
// extent another member proved carries is those bytes, copied from it, and
// costs no recovery block. Each copied, as its member and range. It is
// copied a piece of at most `piece` bytes at a time, so an extent no longer
// than a piece is one read and one write; one whose piece does not read is
// not copied, and the pieces before it are written by then.
fn copy_held(
    held: &BTreeMap<(par3_rs::Fingerprint, u64), (MemberId, Range<u64>)>,
    unproven: &[(Option<MemberId>, FileExtent)],
    piece: u64,
    access: &mut dyn MemberAccess,
) -> BTreeSet<(MemberId, u64, u64)> {
    let piece = piece.max(1);
    let mut copied = BTreeSet::new();
    let mut bytes = Vec::new();
    for (member, extent) in unproven {
        let (Some(into), Some((holder, from))) =
            (member, sum(extent).and_then(|key| held.get(&key)))
        else {
            continue;
        };
        let mut at = from.start;
        let whole = loop {
            let end = from.end.min(at.saturating_add(piece));
            let range = ByteRange::new(at, end);
            bytes.clear();
            if !access.read(*holder, range, &mut bytes) || bytes.len() as u64 != range.len() {
                break false;
            }
            access.write_repaired(*into, extent.range.start + (at - from.start), &bytes);
            at = end;
            if at >= from.end {
                break true;
            }
        };
        if whole {
            copied.insert((*into, extent.range.start, extent.range.end));
        }
    }
    copied
}

// What the member's bytes prove against its description, read a piece at a
// time, each piece by the runs of it that arrived. A set's blocks are judged
// over the described range alone, so bytes a member carries past it are not
// read; a hole leaves unknown only the extents it touches, and a run that
// does not read back leaves those it holds unknown, and the reading carries
// on past it.
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
    let mut runs = Vec::new();
    let mut offset = 0;
    while offset < len {
        let end = len.min(offset.saturating_add(READ));
        runs.clear();
        access.arrived(member, ByteRange::new(offset, end), &mut runs);
        for run in &runs {
            bytes.clear();
            if access.read(member, *run, &mut bytes) && bytes.len() as u64 == run.len() {
                verifier.feed(run.start, &bytes).ok()?;
            }
        }
        offset = end;
    }
    Some(verifier.finish())
}

// The extents of the file the evidence does not prove intact.
fn unresolved<'a>(
    shape: &'a FileLayout,
    evidence: &'a FileEvidence,
) -> impl Iterator<Item = FileExtent> + 'a {
    verdicted(shape, evidence, ExtentVerdict::Unknown).chain(verdicted(
        shape,
        evidence,
        ExtentVerdict::Damaged,
    ))
}

// The extents of the file the evidence gives `verdict`.
fn verdicted<'a>(
    shape: &'a FileLayout,
    evidence: &'a FileEvidence,
    verdict: ExtentVerdict,
) -> impl Iterator<Item = FileExtent> + 'a {
    shape
        .extents
        .iter()
        .zip(evidence.verdicts().iter())
        .filter(move |(_, given)| *given == verdict)
        .map(|(extent, _)| extent)
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
    payloads: &'a Payloads,
    volumes: &'a Volumes,
    layout: &'a BlockLayout,
    bound: &'a BTreeMap<String, MemberId>,
    rewrite: &'a BTreeSet<usize>,
    carried: &'a BTreeMap<u64, &'a PayloadRef>,
    lost: &'a [u64],
    rows: &'a [u64],
    limits: CodecLimits,
}

impl Solve<'_> {
    // Each carried block, into every damaged file that names it. `None`
    // once one does not read back.
    fn write_carried(&self, access: &mut dyn MemberAccess) -> Option<()> {
        let whole = 0..self.set.block_size();
        let mut block = Vec::new();
        for (&index, data) in self.carried {
            block.clear();
            block.resize(whole.end as usize, 0);
            let len = usize::try_from(data.len()).ok()?;
            if !self.volumes.read(data, 0, block.get_mut(..len)?, access) {
                return None;
            }
            self.put(index, &whole, &block, access);
        }
        Some(())
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
            let recovery: BTreeMap<usize, &Row> = rows
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
                                let row = recovery.get(&local).ok_or_else(refused)?;
                                let mut access = access.borrow_mut();
                                if !self.volumes.read(&row.payload, offset, out, *access) {
                                    return Err(refused());
                                }
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
    fn recovery(&self, row: u64) -> Option<&Row> {
        self.payloads
            .recovery
            .iter()
            .find(|block| block.index == row && block.payload.len() == self.set.block_size())
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
        bytes.resize((stripe.end - stripe.start) as usize, 0);
        for &row in self.rows {
            let recovery = self.recovery(row)?;
            if !self
                .volumes
                .read(&recovery.payload, stripe.start, &mut bytes, access)
            {
                return None;
            }
            decoder.add_recovery(row, &bytes)?;
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
            let start = stripe.start.min(data.len());
            let end = stripe.end.min(data.len());
            let into = bytes.get_mut(..(end - start) as usize)?;
            return self.volumes.read(data, start, into, access).then_some(());
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

#[cfg(test)]
mod tests {
    use super::*;

    // An extent copied a piece at a time writes the bytes a copy taken whole
    // writes, at the same offsets, reading every byte of the holder's once;
    // no read or write is longer than a piece, and an extent no longer than
    // one is one read and one write. A piece that does not read leaves the
    // extent not copied.
    #[test]
    fn a_held_extent_is_copied_a_piece_at_a_time() {
        struct Pieces {
            bytes: Vec<u8>,
            fail_from: u64,
            reads: Vec<ByteRange>,
            writes: Vec<(u64, Vec<u8>)>,
        }
        impl MemberAccess for Pieces {
            fn read(&mut self, _: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
                self.reads.push(range);
                if range.end > self.fail_from {
                    return false;
                }
                into.extend_from_slice(&self.bytes[range.start as usize..range.end as usize]);
                true
            }
            fn write_repaired(&mut self, _: MemberId, offset: u64, bytes: &[u8]) {
                self.writes.push((offset, bytes.to_vec()));
            }
            fn length(&self, _: MemberId) -> Option<u64> {
                Some(self.bytes.len() as u64)
            }
        }
        let bytes: Vec<u8> = (0..64u8).map(|byte| byte.wrapping_mul(37) ^ 0x5A).collect();
        let (holder, into) = (MemberId(1), MemberId(2));
        let fingerprint = par3_rs::Fingerprint::default();
        let extent = FileExtent {
            range: 100..123,
            kind: ExtentKind::Block {
                index: 0,
                offset: 0,
                fingerprint: Some(fingerprint),
                rolling_hash: None,
            },
        };
        let mut held = BTreeMap::new();
        held.insert((fingerprint, 23), (holder, 10..33));
        let unproven = [(Some(into), extent)];
        for piece in [1, 4, 7, u64::MAX] {
            let mut access = Pieces {
                bytes: bytes.clone(),
                fail_from: u64::MAX,
                reads: Vec::new(),
                writes: Vec::new(),
            };
            let copied = copy_held(&held, &unproven, piece, &mut access);
            assert_eq!(copied, BTreeSet::from([(into, 100, 123)]));
            let mut written = Vec::new();
            for (offset, piece_bytes) in &access.writes {
                assert_eq!(*offset, 100 + written.len() as u64);
                assert!(piece_bytes.len() as u64 <= piece);
                written.extend_from_slice(piece_bytes);
            }
            assert_eq!(written, bytes[10..33]);
            let mut next = 10;
            for read in &access.reads {
                assert_eq!(read.start, next);
                assert!(read.len() <= piece);
                next = read.end;
            }
            assert_eq!(next, 33);
            if piece == u64::MAX {
                assert_eq!((access.reads.len(), access.writes.len()), (1, 1));
            }
        }
        let mut access = Pieces {
            bytes,
            fail_from: 18,
            reads: Vec::new(),
            writes: Vec::new(),
        };
        assert!(copy_held(&held, &unproven, 4, &mut access).is_empty());
        assert_eq!(access.writes.len(), 2);
    }
}
