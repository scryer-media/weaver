// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Repairing one set through the namespace. Slices the grid proved are taken
// as they are; slices it could not prove are read back one at a time and
// checked against their CRC32 and MD5, and those that pass are kept, within
// the repair memory limit, as the repair's input; recovery packets are read
// from the volumes the namespace kept, their hashes checked again, and spent
// through par2-rs under the repair memory limit. Nothing here touches a
// file: every byte goes through the namespace's access.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::io;

use par2_rs::verify::FileAccess;
use par2_rs::{
    FileHashState, FileId, FileStatus, FileVerification, Packet, Par2Error, Par2FileSet,
    RecoverySlice, RepairOptions, Repairability, SliceChecksumState, VerificationResult,
    execute_repair_with_options, parse_packet, plan_repair_with_memory_limit,
};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{ByteRange, MemberId, SliceVerdict};

use super::evidence::Evidence;
use super::sets::{Length, SetState};
use crate::grid::Grid;

// What one set's repair came to.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct SetRepair {
    pub(super) outcome: RepairOutcome,
    // Damaged slices it found, for the recovery it asks for while waiting.
    pub(super) damaged: u64,
    // It read member bytes back to rule: what the grid proved was not
    // enough. A clean ruling without a read is the grid's alone.
    #[serde(default)]
    pub(super) read: bool,
    // The recovery blocks it must hold before it is tried again: as many as
    // the slices it found damaged, and more than it held.
    pub(super) needed: u64,
}

impl SetRepair {
    // Waiting on a description, not on blocks.
    pub(super) fn waiting() -> Self {
        Self {
            outcome: RepairOutcome::Waiting,
            damaged: 0,
            read: false,
            needed: 0,
        }
    }

    pub(super) fn unread() -> Self {
        Self {
            outcome: RepairOutcome::Unread,
            damaged: 0,
            read: false,
            needed: 0,
        }
    }

    fn clean(read: bool) -> Self {
        Self {
            outcome: RepairOutcome::Clean,
            damaged: 0,
            read,
            needed: 0,
        }
    }
}

// A described file no member is bound to may be an unnamed member its
// front could not name: the first whose slice reads back as the file's is
// it, and the repair mends it in place, front and all, so the bind after
// names it. A described file no post carries is announced by the namespace
// before the repair, so it is a member here with every slice missing, and
// the rebuild writes it whole.
pub(super) fn repair_set(
    state: &mut SetState,
    grid: &Grid,
    coming: u64,
    memory_limit: usize,
    access: &mut dyn MemberAccess,
) -> SetRepair {
    let mut read = false;
    for (file, member) in known_by_slices(state, access, &mut read) {
        state.know(file, member);
    }
    let Some(index) = state.usable() else {
        return SetRepair::clean(read);
    };
    let slice_size = index.slice_size;
    // The slices the bytes proved bad, which a repair that cannot rebuild
    // them reports to the namespace, so the members' coverage says what the
    // set found: damage only the set's checksums can see arrived marked
    // clean.
    let mut proven = Vec::new();
    let mut kept = Kept::new(memory_limit);
    let mut validity: Vec<(FileId, Vec<bool>)> = index
        .recovery_file_ids
        .iter()
        .map(|file| {
            let mut reading = Reading {
                access: &mut *access,
                read: &mut read,
                proven: &mut proven,
                kept: &mut kept,
            };
            (
                *file,
                slice_validity(index, state, grid, *file, &mut reading),
            )
        })
        .collect();
    let mut donors = BTreeMap::new();
    let copied = copy_held(state, index, &mut validity, access, &mut donors);
    proven.retain(|(member, range)| {
        !copied.iter().any(|(into, slice)| {
            into == member && slice.start <= range.start && range.end <= slice.end
        })
    });
    let invalid = |valid: &Vec<bool>| valid.iter().filter(|valid| !**valid).count() as u64;
    // Only damage in a bound member starts a repair; a described file no
    // member is bound to yet is missing in the arithmetic of one that does.
    let own: u64 = validity
        .iter()
        .filter(|(file, _)| state.members().contains_key(file))
        .map(|(_, valid)| invalid(valid))
        .sum();
    if own == 0 {
        return match copied.len() as u64 {
            0 => SetRepair::clean(read),
            slices => SetRepair {
                outcome: RepairOutcome::Repaired { slices },
                damaged: 0,
                read: true,
                needed: 0,
            },
        };
    }
    let damaged: u64 = validity.iter().map(|(_, valid)| invalid(valid)).sum();
    // Short with every block it holds read, or with enough that would not
    // rebuild: another has to come before it is worth trying again.
    let needed = damaged.max(state.recovery.len() as u64 + 1);
    let short = |reachable: u64, access: &mut dyn MemberAccess| {
        if coming > 0 {
            return SetRepair {
                outcome: RepairOutcome::Waiting,
                damaged,
                read,
                needed,
            };
        }
        for &(member, range) in &proven {
            access.found_damaged(member, range);
        }
        SetRepair {
            outcome: RepairOutcome::Impossible { damaged, reachable },
            damaged,
            read,
            needed,
        }
    };
    let Some(index) = state.usable() else {
        return SetRepair::clean(read);
    };
    let mut set = Par2FileSet {
        recovery_slices: BTreeMap::new(),
        ..index.clone()
    };
    // As many blocks as slices are damaged, and more while those read do
    // not solve for them: not every choice of blocks does.
    let mut wanted = damaged;
    let plan = loop {
        spend(state, slice_size, wanted, &mut set.recovery_slices, access);
        let available = set.recovery_slices.len() as u64;
        if available < wanted {
            return short(available, access);
        }
        let verification = verification(&set, &validity, damaged);
        match plan_repair_with_memory_limit(&set, &verification, Some(memory_limit)) {
            Ok(plan) => break plan,
            Err(Par2Error::InsufficientRecoveryData { deficit, .. }) => {
                wanted = available + u64::from(deficit);
            }
            Err(error) => {
                access.repair_error(
                    "plan repair",
                    &format!("{:02x?}", set.recovery_set_id),
                    &error,
                );
                return short(available, access);
            }
        }
    };
    let available = set.recovery_slices.len() as u64;
    let members: BTreeMap<FileId, MemberId> = state.members().clone();
    let mut files = Files {
        access: RefCell::new(access),
        members: &members,
        set: &set,
        donors: &donors,
        kept: &kept,
    };
    let options = RepairOptions {
        memory_limit: Some(memory_limit),
        ..Default::default()
    };
    if let Err(error) = execute_repair_with_options(&plan, &set, &mut files, &options) {
        files.access.get_mut().repair_error(
            "execute repair",
            &format!("{:02x?}", set.recovery_set_id),
            &error,
        );
        return short(available, files.access.into_inner());
    }
    SetRepair {
        outcome: RepairOutcome::Repaired {
            slices: damaged + copied.len() as u64,
        },
        damaged,
        read,
        needed,
    }
}

// A slice is its bytes: an invalid slice of a bound member whose checksums
// an intact slice of the set shares is those bytes, copied from it, and
// needs no recovery block. Each is marked valid, and its member range
// returned. Equal checksums are equal bytes once padded to the slice size,
// so a copy longer than the slice it fills ends in that padding.
fn copy_held(
    state: &SetState,
    index: &Par2FileSet,
    validity: &mut [(FileId, Vec<bool>)],
    access: &mut dyn MemberAccess,
    donors: &mut BTreeMap<(FileId, u64), (MemberId, ByteRange)>,
) -> Vec<(MemberId, ByteRange)> {
    let intact = |validity: &[(FileId, Vec<bool>)], file: FileId, slice: u64| {
        validity
            .iter()
            .find(|(described, _)| *described == file)
            .and_then(|(_, valid)| valid.get(slice as usize).copied())
            .unwrap_or(false)
    };
    let held: Vec<(usize, u64, FileId, u64)> = validity
        .iter()
        .enumerate()
        .filter(|(_, (file, _))| state.members().contains_key(file))
        .flat_map(|(at, (file, valid))| {
            valid
                .iter()
                .enumerate()
                .filter(|(_, valid)| !**valid)
                .map(move |(slice, _)| (at, *file, slice as u64))
        })
        .filter_map(|(at, file, slice)| {
            let &(from, from_slice) = state
                .alike(file, slice)
                .iter()
                .find(|&&(other, other_slice)| intact(validity, other, other_slice))?;
            Some((at, slice, from, from_slice))
        })
        .collect();
    let mut copied = Vec::new();
    let mut buffer = Vec::new();
    for (at, slice, from, from_slice) in held {
        let file = validity[at].0;
        let (Some(&into), Some(&holder), Some(target), Some(source)) = (
            state.members().get(&file),
            state.members().get(&from),
            described_slice(index, file, slice),
            described_slice(index, from, from_slice),
        ) else {
            continue;
        };
        buffer.clear();
        if !access.read(holder, source, &mut buffer) || buffer.len() as u64 != source.len() {
            continue;
        }
        buffer.resize(target.len() as usize, 0);
        access.write_repaired(into, target.start, &buffer);
        validity[at].1[slice as usize] = true;
        // The destination remains staged until acknowledgment. Keep the
        // original verified source for any later Reed-Solomon reads.
        donors.insert((file, slice), (holder, source));
        copied.push((into, target));
    }
    copied
}

// One slice's share of the described file.
fn described_slice(index: &Par2FileSet, file: FileId, slice: u64) -> Option<ByteRange> {
    let described = index.file_description(&file)?.length;
    let start = slice * index.slice_size;
    let end = (start + index.slice_size).min(described);
    (start < end).then(|| ByteRange::new(start, end))
}

// Which slices of a described file are good, reading back only the ones the
// evidence did not settle; `read` says one was. The member range of each
// slice its bytes proved bad, by the evidence or the read, goes in `proven`;
// one that could not be read is not proven anything. A file the set carries
// no slice checksums for is checked whole, against the hash its description
// carries: every slice is good when it matches, and none is known good
// otherwise, so the set rebuilds them all. What the reads prove good is
// kept for the repair to take as its input.
fn slice_validity(
    index: &Par2FileSet,
    state: &SetState,
    grid: &Grid,
    file: FileId,
    reading: &mut Reading<'_>,
) -> Vec<bool> {
    let Reading {
        access,
        read,
        proven,
        kept,
    } = reading;
    let count = index.file_description(&file).map_or(0, |description| {
        index.slice_count_for_file(description.length)
    }) as usize;
    let Some(member) = state.members().get(&file).copied() else {
        return vec![false; count];
    };
    if index.file_checksums(&file).is_none() {
        **read = true;
        let whole = whole_file_matches(index, file, member, *access, kept);
        if whole == Some(false)
            && let Some(described) = index.file_description(&file).map(|d| d.length)
            && described > 0
        {
            proven.push((member, ByteRange::new(0, described)));
        }
        return vec![whole == Some(true); count];
    }
    let evidence = Evidence::of(
        grid,
        index,
        state.whole(file),
        file,
        member,
        access.length(member),
    );
    if evidence.is_proven() {
        return vec![true; count];
    }
    let verdicts: Vec<SliceVerdict> = evidence.vetoed().collect();
    let mut buffer = Vec::new();
    (0..count)
        .map(|slice| {
            let good = match verdicts.get(slice) {
                Some(SliceVerdict::Intact) => Some(true),
                Some(SliceVerdict::Damaged) => Some(false),
                _ => {
                    **read = true;
                    let good = read_slice(index, file, member, slice, *access, &mut buffer);
                    if good == Some(true) {
                        kept.keep(file, slice as u64, std::mem::take(&mut buffer));
                    }
                    good
                }
            };
            if good == Some(false)
                && let Some(range) = slice_range(index, file, slice, access.length(member))
            {
                proven.push((member, range));
            }
            good == Some(true)
        })
        .collect()
}

// Each described file no member is bound to that an unnamed member holds a
// slice of, to the first that does: the repair knows it by its content. What
// no slice named has no intact byte to say what it is, so it is what is
// left once nothing still to come can be it: longest file first, each takes
// the unnamed member it fits most tightly, and the repair makes every byte
// of it over.
fn known_by_slices(
    state: &SetState,
    access: &mut dyn MemberAccess,
    read: &mut bool,
) -> Vec<(FileId, MemberId)> {
    let Some(index) = state.usable() else {
        return Vec::new();
    };
    let mut unnamed = state.unnamed().to_vec();
    let mut known = Vec::new();
    for file in &index.recovery_file_ids {
        if !state.members().contains_key(file)
            && let Some(at) = unnamed
                .iter()
                .position(|&candidate| holds_slice_of(index, *file, candidate, access, read))
        {
            known.push((*file, unnamed.swap_remove(at).0));
        }
    }
    let mut free: Vec<(FileId, u64)> = index
        .recovery_file_ids
        .iter()
        .filter(|file| {
            !state.members().contains_key(file) && !known.iter().any(|(k, _)| k == *file)
        })
        .filter_map(|file| Some((*file, index.file_description(file)?.length)))
        .collect();
    free.sort_by_key(|(_, long)| Reverse(*long));
    // A file is a member's by elimination only when it is what is left: the
    // free files the members that fit it could be are no more than they
    // are. A file over is one a member still to come may be. Each file taken
    // leaves less for the rest, so the files are tried again until none is.
    while let Some((at, file, member)) = free.iter().enumerate().find_map(|(at, (file, long))| {
        let fitting: Vec<usize> = (0..unnamed.len())
            .filter(|candidate| unnamed[*candidate].1.fits(*long))
            .collect();
        let could_be = free
            .iter()
            .filter(|(_, other)| {
                fitting
                    .iter()
                    .any(|candidate| unnamed[*candidate].1.fits(*other))
            })
            .count();
        let tightest = fitting
            .iter()
            .copied()
            .min_by_key(|candidate| unnamed[*candidate].1.bound())
            .filter(|_| could_be <= fitting.len())?;
        Some((at, *file, tightest))
    }) {
        free.remove(at);
        known.push((file, unnamed.swap_remove(member).0));
    }
    known
}

// Whether the member, as long as the described file, holds one of its
// slices: read back in order until one checks, so `read` says one was.
fn holds_slice_of(
    index: &Par2FileSet,
    file: FileId,
    (member, length): (MemberId, Length),
    access: &mut dyn MemberAccess,
    read: &mut bool,
) -> bool {
    let mut buffer = Vec::new();
    index
        .file_description(&file)
        .filter(|description| length == Length::Exact(description.length))
        .is_some_and(|description| {
            (0..index.slice_count_for_file(description.length) as usize).any(|slice| {
                *read = true;
                read_slice(index, file, member, slice, access, &mut buffer) == Some(true)
            })
        })
}

// The member range of one slice: its share of the described file, as far
// as the member reaches.
fn slice_range(
    index: &Par2FileSet,
    file: FileId,
    slice: usize,
    length: Option<u64>,
) -> Option<ByteRange> {
    let described = index.file_description(&file)?.length;
    let start = slice as u64 * index.slice_size;
    let end = (start + index.slice_size).min(described).min(length?);
    (start < end).then(|| ByteRange::new(start, end))
}

// Read the member back whole and check it against the described file's
// hash: `None` when it is not the described length or could not be read,
// so nothing is proven of it. The sweep is the repair's input too: its
// slices are kept as it reads them, and let go again unless it matches.
fn whole_file_matches(
    index: &Par2FileSet,
    file: FileId,
    member: MemberId,
    access: &mut dyn MemberAccess,
    kept: &mut Kept,
) -> Option<bool> {
    const CHUNK: u64 = 1 << 20;
    let description = index.file_description(&file)?;
    if access.length(member) != Some(description.length) {
        return None;
    }
    let slice = index.slice_size.max(1);
    let chunk = (CHUNK / slice).max(1) * slice;
    let mut hasher = FileHashState::new();
    let mut buffer = Vec::new();
    let mut at = 0;
    while at < description.length {
        let end = (at + chunk).min(description.length);
        buffer.clear();
        if !access.read(member, ByteRange::new(at, end), &mut buffer)
            || buffer.len() as u64 != end - at
        {
            kept.forget(file);
            return None;
        }
        hasher.update(&buffer);
        for (step, piece) in buffer.chunks(slice as usize).enumerate() {
            kept.keep(file, at / slice + step as u64, piece.to_vec());
        }
        at = end;
    }
    let matches = hasher.finalize() == description.hash_full;
    if !matches {
        kept.forget(file);
    }
    Some(matches)
}

// Where a set's verification reads, and what it leaves for the repair.
struct Reading<'a> {
    access: &'a mut dyn MemberAccess,
    read: &'a mut bool,
    proven: &'a mut Vec<(MemberId, ByteRange)>,
    kept: &'a mut Kept,
}

// The slices the verification read back and proved good, by file and
// index, for the repair to take as its input instead of reading them again:
// on an encrypted store every read enciphers its bytes again. At most
// `limit` bytes, the repair's own memory limit: past it the earliest kept go
// first, and the repair reads those again. A slice proved bad is never
// kept, and nothing is kept past one set's repair.
struct Kept {
    slices: BTreeMap<(FileId, u64), Vec<u8>>,
    order: VecDeque<(FileId, u64)>,
    bytes: usize,
    limit: usize,
}

impl Kept {
    fn new(limit: usize) -> Self {
        Self {
            slices: BTreeMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            limit,
        }
    }

    fn keep(&mut self, file: FileId, slice: u64, bytes: Vec<u8>) {
        if bytes.len() > self.limit {
            return;
        }
        while self.bytes + bytes.len() > self.limit
            && let Some(earliest) = self.order.pop_front()
        {
            self.bytes -= self.slices.remove(&earliest).map_or(0, |gone| gone.len());
        }
        self.bytes += bytes.len();
        if let Some(was) = self.slices.insert((file, slice), bytes) {
            self.bytes -= was.len();
        } else {
            self.order.push_back((file, slice));
        }
    }

    // Every slice of `file` let go: it was not proved after all.
    fn forget(&mut self, file: FileId) {
        let gone: Vec<(FileId, u64)> = self
            .slices
            .range((file, 0)..=(file, u64::MAX))
            .map(|(key, _)| *key)
            .collect();
        for key in gone {
            self.bytes -= self.slices.remove(&key).map_or(0, |bytes| bytes.len());
        }
        self.order.retain(|(of, _)| *of != file);
    }

    fn get(&self, file: FileId, slice: u64) -> Option<&[u8]> {
        self.slices.get(&(file, slice)).map(Vec::as_slice)
    }
}

// Read one slice back and check it against both of its checksums: `None`
// when it could not be read, so nothing is proven of it.
fn read_slice(
    index: &Par2FileSet,
    file: FileId,
    member: MemberId,
    slice: usize,
    access: &mut dyn MemberAccess,
    buffer: &mut Vec<u8>,
) -> Option<bool> {
    let description = index.file_description(&file)?;
    let expected = index
        .file_checksums(&file)
        .and_then(|checksums| checksums.get(slice))?;
    let block = index.slice_size;
    let start = slice as u64 * block;
    let end = (start + block).min(description.length);
    if access.length(member).is_none_or(|length| length < end) {
        return None;
    }
    buffer.clear();
    if !access.read(member, ByteRange::new(start, end), buffer)
        || buffer.len() as u64 != end - start
    {
        return None;
    }
    let mut state = SliceChecksumState::new();
    state.update(buffer);
    let (crc32, md5) = state.finalize(Some(block));
    Some(crc32 == expected.crc32 && md5 == expected.md5)
}

// Read recovery packets back from the volumes, in exponent order, until
// `wanted` pass their hash, on from the last already read. The scan checked it;
// the volume is read again here, so it is checked again. One that fails is
// forgotten.
fn spend(
    state: &mut SetState,
    slice_size: u64,
    wanted: u64,
    slices: &mut BTreeMap<u32, RecoverySlice>,
    access: &mut dyn MemberAccess,
) {
    let mut bad = Vec::new();
    let mut buffer = Vec::new();
    let from = slices
        .last_key_value()
        .map_or(0, |(&exponent, _)| exponent + 1);
    for (&exponent, location) in state.recovery.range(from..) {
        if slices.len() as u64 >= wanted {
            break;
        }
        buffer.clear();
        let read = access.read_volume(location.volume, location.range, &mut buffer);
        match read.then(|| recovery_slice(&buffer, slice_size)).flatten() {
            Some(data) => {
                slices.insert(exponent, RecoverySlice { exponent, data });
            }
            None => bad.push(exponent),
        }
    }
    for exponent in bad {
        state.recovery.remove(&exponent);
    }
}

fn recovery_slice(packet: &[u8], slice_size: u64) -> Option<par2_rs::RecoverySliceData> {
    match parse_packet(packet, 0).ok()?.0 {
        Packet::RecoverySlice(parsed) if parsed.data.len() as u64 == slice_size => {
            Some(parsed.data)
        }
        _ => None,
    }
}

fn verification(
    set: &Par2FileSet,
    validity: &[(FileId, Vec<bool>)],
    damaged: u64,
) -> VerificationResult {
    let available = set.recovery_slices.len() as u32;
    let files = validity
        .iter()
        .map(|(file, valid)| {
            let missing = valid.iter().filter(|valid| !**valid).count() as u32;
            FileVerification {
                file_id: *file,
                filename: set
                    .file_description(file)
                    .map(|description| description.filename.clone())
                    .unwrap_or_default(),
                status: if missing == 0 {
                    FileStatus::Complete
                } else {
                    FileStatus::Damaged(missing)
                },
                valid_slices: valid.clone(),
                missing_slice_count: missing,
            }
        })
        .collect();
    VerificationResult {
        files,
        recovery_blocks_available: available,
        total_missing_blocks: damaged as u32,
        repairable: Repairability::Repairable {
            blocks_needed: damaged as u32,
            blocks_available: available,
        },
    }
}

// par2-rs's view of the members: files by id, read and written through the
// namespace. A file no member is bound to was not announced, as when the
// namespace refused to, so what is rebuilt for it is dropped.
struct Files<'a> {
    access: RefCell<&'a mut dyn MemberAccess>,
    members: &'a BTreeMap<FileId, MemberId>,
    set: &'a Par2FileSet,
    donors: &'a BTreeMap<(FileId, u64), (MemberId, ByteRange)>,
    kept: &'a Kept,
}

impl Files<'_> {
    fn member(&self, file: &FileId) -> io::Result<MemberId> {
        self.members
            .get(file)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no member is this file"))
    }

    fn length(&self, file: &FileId) -> Option<u64> {
        self.member(file).ok()?;
        self.set
            .file_description(file)
            .map(|description| description.length)
    }
}

impl FileAccess for Files<'_> {
    fn read_file_range(&self, file: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let member = self.member(file)?;
        let end = offset
            .saturating_add(len)
            .min(self.length(file).unwrap_or(0));
        let mut bytes = Vec::new();
        if offset >= end {
            return Ok(bytes);
        }
        let mut access = self.access.borrow_mut();
        let mut append = |bytes: &mut Vec<u8>, source, range: ByteRange, length| {
            let before = bytes.len();
            if !range.is_empty()
                && (!access.read(source, range, bytes)
                    || bytes.len() - before != range.len() as usize)
            {
                return Err(io::Error::other("the namespace could not read the member"));
            }
            bytes.resize(before + length as usize, 0);
            Ok(())
        };
        // A copied slice reads from its donor, a kept one from what the
        // verification kept; the member is read in one span between them.
        let size = self.set.slice_size;
        let (mut at, mut from) = (offset, offset);
        while at < end {
            let slice = at / size;
            let base = slice * size;
            let until = end.min(base.saturating_add(size));
            let donor = self.donors.get(&(*file, slice));
            let kept = self.kept.get(*file, slice);
            if donor.is_some() || kept.is_some() {
                append(&mut bytes, member, ByteRange::new(from, at), at - from)?;
                from = until;
            }
            if let Some(&(source, range)) = donor {
                let start = range.start + (at - base).min(range.len());
                let stop = range.start + (until - base).min(range.len());
                append(&mut bytes, source, ByteRange::new(start, stop), until - at)?;
            } else if let Some(kept) = kept {
                bytes.extend_from_slice(&kept[(at - base) as usize..(until - base) as usize]);
            }
            at = until;
        }
        append(&mut bytes, member, ByteRange::new(from, end), end - from)?;
        Ok(bytes)
    }

    fn file_exists(&self, file: &FileId) -> bool {
        self.members.contains_key(file)
    }

    fn file_length(&self, file: &FileId) -> Option<u64> {
        self.length(file)
    }

    // A repair reads slices, never a whole member.
    fn read_file(&self, _file: &FileId) -> io::Result<Vec<u8>> {
        Err(io::Error::other(
            "a repair reads members one slice at a time",
        ))
    }

    fn write_file_range(&mut self, file: &FileId, offset: u64, data: &[u8]) -> io::Result<()> {
        let Ok(member) = self.member(file) else {
            return Ok(());
        };
        let described = self
            .set
            .file_description(file)
            .map_or(0, |description| description.length);
        let end = offset.saturating_add(data.len() as u64).min(described);
        if offset < end {
            let take = (end - offset) as usize;
            self.access
                .get_mut()
                .write_repaired(member, offset, &data[..take]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::par2::Par2;
    use crate::testing::{Job, Store, par2_set, payload};
    use weaver_graph::{Budget, ObjectId, Recovery, VolumeIndex};

    const BLOCK: u64 = 512;

    struct Staged {
        original: Store,
        writes: Vec<(MemberId, u64, Vec<u8>)>,
    }

    impl Staged {
        fn publish(&mut self) {
            for (member, offset, bytes) in std::mem::take(&mut self.writes) {
                self.original.write_repaired(member, offset, &bytes);
            }
        }
    }

    impl MemberAccess for Staged {
        fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
            self.original.read(member, range, into)
        }
        fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
            self.writes.push((member, offset, bytes.to_vec()));
        }
        fn length(&self, member: MemberId) -> Option<u64> {
            self.original.length(member)
        }
        fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
            self.original.arrived(member, range, out);
        }
        fn read_volume(
            &mut self,
            volume: VolumeIndex,
            range: ByteRange,
            into: &mut Vec<u8>,
        ) -> bool {
            self.original.read_volume(volume, range, into)
        }
    }

    fn fixture(files: &[(String, Vec<u8>)], wrong: &[u8]) -> (Job<Par2>, MemberId) {
        let id = ObjectId(7);
        let mut job = Job::new(Par2::new(id), id);
        for (name, bytes) in files {
            job.volume(name, bytes, 700, false);
        }
        let member = job.member("payload.bin", wrong);
        job.object.bind(&mut job.namespace, &mut Vec::new());
        // The copied twin is physically readable, with old wrong bytes.
        // Only the distinct block is unavailable to source reads.
        job.store
            .holes
            .insert(member, vec![ByteRange::new(2 * BLOCK, 3 * BLOCK)]);
        (job, member)
    }

    #[test]
    fn source_reads_split_only_at_donors_in_the_requested_range() {
        let data = payload(98, 32 * BLOCK as usize);
        let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 1, 1);
        for donor_slice in [None, Some(24), Some(8)] {
            let (mut job, member) = fixture(&files, &data);
            job.store.holes.clear();
            job.store.reads = 0;
            let state = job.object.sets.values().next().unwrap();
            let set = state.usable().unwrap();
            let file = set.recovery_file_ids[0];
            let members = state.members();
            let mut donors = BTreeMap::new();
            if let Some(slice) = donor_slice {
                donors.insert((file, slice), (member, ByteRange::new(0, BLOCK)));
            }
            let offset = 97;
            let end = 16 * BLOCK + 19;
            let actual = Files {
                access: RefCell::new(&mut job.store),
                members,
                set,
                donors: &donors,
                kept: &Kept::new(0),
            }
            .read_file_range(&file, offset, end - offset)
            .unwrap();
            let mut expected = data[offset as usize..end as usize].to_vec();
            if donor_slice == Some(8) {
                let into = (8 * BLOCK - offset) as usize;
                expected[into..into + BLOCK as usize].copy_from_slice(&data[..BLOCK as usize]);
            }
            assert_eq!(actual, expected);
            assert_eq!(job.store.reads, if donor_slice == Some(8) { 3 } else { 1 });
        }
    }

    #[test]
    fn readable_old_copy_target_never_becomes_an_rs_source() {
        let twin = payload(96, BLOCK as usize);
        let distinct = payload(97, BLOCK as usize);
        let data = [twin.as_slice(), twin.as_slice(), distinct.as_slice()].concat();
        let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 1, 1);
        let mut wrong = data.clone();
        wrong[BLOCK as usize..].fill(0);

        // Keep the repair plan and staged writes identical. Clearing only
        // the original donor map recreates the old readable source view.
        for use_donors in [false, true] {
            let (mut job, member) = fixture(&files, &wrong);
            let state = job.object.sets.values_mut().next().unwrap();
            let index = state.usable().unwrap().clone();
            let file = index.recovery_file_ids[0];
            let mut access = Staged {
                original: job.store,
                writes: Vec::new(),
            };
            let mut read = false;
            let mut proven = Vec::new();
            let mut kept = Kept::new(usize::MAX);
            let mut reading = Reading {
                access: &mut access,
                read: &mut read,
                proven: &mut proven,
                kept: &mut kept,
            };
            let valid = slice_validity(&index, state, &job.object.grid, file, &mut reading);
            assert_eq!(valid, [true, false, false]);
            let mut validity = vec![(file, valid)];
            let mut donors = BTreeMap::new();
            let copied = copy_held(state, &index, &mut validity, &mut access, &mut donors);
            assert_eq!(copied, [(member, ByteRange::new(BLOCK, 2 * BLOCK))]);
            assert_eq!(validity[0].1, [true, true, false]);
            let mut old_target = Vec::new();
            assert!(access.read(member, copied[0].1, &mut old_target));
            assert_ne!(
                old_target, twin,
                "the readable destination still contains old bytes"
            );
            let mut set = index;
            set.recovery_slices.clear();
            spend(state, BLOCK, 1, &mut set.recovery_slices, &mut access);
            assert_eq!(set.recovery_slices.len(), 1);
            let verified = verification(&set, &validity, 1);
            let plan = plan_repair_with_memory_limit(&set, &verified, None).unwrap();
            let members = state.members().clone();
            if !use_donors {
                donors.clear();
            }
            let executed = {
                let mut view = Files {
                    access: RefCell::new(&mut access),
                    members: &members,
                    set: &set,
                    donors: &donors,
                    kept: &Kept::new(0),
                };
                execute_repair_with_options(&plan, &set, &mut view, &RepairOptions::default())
            };
            access.publish();
            if use_donors {
                executed.unwrap();
                assert_eq!(access.original.members[&member], data);
            } else {
                executed
                    .expect("old readable donor arithmetic must complete for this counterfactual");
                assert_ne!(
                    access.original.members[&member], data,
                    "successful old arithmetic must expose wrong reconstructed bytes"
                );
            }
        }

        // Exercise the production entry with the same readable wrong twin.
        let (mut job, member) = fixture(&files, &wrong);
        let mut access = Staged {
            original: job.store,
            writes: Vec::new(),
        };
        assert_eq!(
            job.object.repair(&mut access, Budget::default()),
            RepairOutcome::Repaired { slices: 2 }
        );
        assert_eq!(
            access.original.members[&member], wrong,
            "publication remains actor-owned"
        );
        access.publish();
        assert_eq!(access.original.members[&member], data);
    }

    // An encrypted RAR set as a namespace keeps one: each volume's header
    // and the plaintext that came out of its cipher, so what PAR2 reads of a
    // volume is sealed again from that plaintext, as the store does. The
    // scale of a real post, shrunk: slices of 1 MiB over articles of 750 KiB
    // become 16 KiB over 12,000 bytes, and five volumes of 32 slices.
    mod encrypted {
        use super::*;
        use crate::crypt::{Cbc, Frontier, Padding, Plain, block_ceil, sealed};
        use crate::testing::segments;
        use unrar_rs::MemberCipherKey;
        use weaver_graph::{Coverage, Provenance};

        const KEY: [u8; 32] = [7; 32];
        const IV: [u8; 16] = [9; 16];
        pub(super) const SLICE: u64 = 16 * 1024;
        pub(super) const ARTICLE: u64 = 12_000;
        const HEADER: usize = 77;
        pub(super) const VOLUMES: usize = 5;
        pub(super) const SLICES: u64 = 32;

        // One volume as the store holds it.
        struct Volume {
            header: Vec<u8>,
            cbc: Cbc,
            frontier: Frontier,
            plain: Vec<u8>,
            coverage: Coverage,
            padding: Padding,
        }

        // The namespace's face over the volumes: reads of a volume seal its
        // plaintext again; repaired bytes are staged, as the owner stages
        // them until it accepts the repair.
        pub(super) struct Sealing {
            volumes: BTreeMap<MemberId, Volume>,
            pub(super) store: Store,
            pub(super) writes: Vec<(MemberId, u64, Vec<u8>)>,
            pub(super) reads: Vec<(MemberId, ByteRange)>,
        }

        impl MemberAccess for Sealing {
            fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
                self.reads.push((member, range));
                let Some(volume) = self.volumes.get(&member) else {
                    return self.store.read(member, range, into);
                };
                let start = into.len();
                let header = volume.header.len() as u64;
                if range.start < header {
                    into.extend_from_slice(
                        &volume.header[range.start as usize..range.end.min(header) as usize],
                    );
                }
                if range.end > header {
                    let stream =
                        ByteRange::new(range.start.max(header) - header, range.end - header);
                    let plain = &volume.plain;
                    let mut read = |wanted: ByteRange, buffer: &mut Vec<u8>| {
                        buffer
                            .extend_from_slice(&plain[wanted.start as usize..wanted.end as usize]);
                        true
                    };
                    if !volume.cbc.seal_on(
                        &volume.frontier,
                        &volume.coverage,
                        &volume.padding,
                        stream,
                        &mut read,
                        into,
                    ) {
                        into.truncate(start);
                        return false;
                    }
                }
                true
            }
            fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
                self.writes.push((member, offset, bytes.to_vec()));
            }
            fn length(&self, member: MemberId) -> Option<u64> {
                self.store.length(member)
            }
            fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
                self.store.arrived(member, range, out);
            }
            fn read_volume(
                &mut self,
                volume: VolumeIndex,
                range: ByteRange,
                into: &mut Vec<u8>,
            ) -> bool {
                self.store.read_volume(volume, range, into)
            }
        }

        // What befalls a post: a byte flipped before it was posted, so its
        // article carries the flipped byte's checksum, or an article that
        // never came.
        #[derive(Clone, Copy)]
        pub(super) enum Fault {
            Flipped { volume: usize, at: usize },
            Lost { volume: usize, article: u64 },
            // Arrived with a byte wrong, its CRC failed by the decoder.
            Garbled { volume: usize, article: u64 },
        }

        // The volumes as written, each its header and its cipher stream,
        // and their plaintext.
        fn written(seed: u64) -> Vec<(String, Vec<u8>, Vec<u8>)> {
            (0..VOLUMES)
                .map(|index| {
                    let length = (SLICES * SLICE) as usize - 1000 * index - 3;
                    let header = payload(seed + 100 + index as u64, HEADER);
                    let plain_len = length - HEADER - 5;
                    let plain = payload(seed + index as u64, plain_len);
                    let mut cipher = plain.clone();
                    cipher.resize(block_ceil(plain_len as u64) as usize, 0xA5);
                    MemberCipherKey::Aes256(KEY)
                        .encrypt_range(&IV, &mut cipher)
                        .unwrap();
                    let posted = [header, cipher].concat();
                    (format!("set.part{}.rar", index + 1), plain, posted)
                })
                .collect()
        }

        // The PAR2 files without their slice checksums.
        fn without_slice_checksums(files: &mut [(String, Vec<u8>)]) {
            for (_, bytes) in files {
                let mut kept = Vec::new();
                let mut at = 0;
                while at < bytes.len() {
                    let len =
                        u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
                    let packet = &bytes[at..at + len];
                    if !packet[48..64].starts_with(b"PAR 2.0\0IFSC") {
                        kept.extend_from_slice(packet);
                    }
                    at += len;
                }
                *bytes = kept;
            }
        }

        pub(super) struct Post {
            pub(super) job: Job<Par2>,
            pub(super) access: Sealing,
            pub(super) members: Vec<MemberId>,
            // The volumes as they were written.
            pub(super) written: Vec<Vec<u8>>,
        }

        impl Post {
            pub(super) fn new(faults: &[Fault], recovery: u32, slice_checksums: bool) -> Self {
                let written = written(41);
                let sources: Vec<(&str, &[u8])> = written
                    .iter()
                    .map(|(name, _, posted)| (name.as_str(), posted.as_slice()))
                    .collect();
                let mut files = par2_set("set", &sources, SLICE, recovery, 1);
                if !slice_checksums {
                    without_slice_checksums(&mut files);
                }
                let id = ObjectId(7);
                let mut job = Job::new(Par2::new(id), id);
                for (name, bytes) in &files {
                    job.volume(name, bytes, 700, false);
                }
                let mut volumes = BTreeMap::new();
                let mut members = Vec::new();
                let mut delivered = Vec::new();
                for (index, (name, plain, posted)) in written.iter().enumerate() {
                    let mut posted = posted.clone();
                    for fault in faults {
                        if let Fault::Flipped { volume, at } = *fault
                            && volume == index
                        {
                            posted[at] ^= 0x5A;
                        }
                    }
                    let length = posted.len() as u64;
                    let member = job.namespace.announce(name, Some(length));
                    let cipher_len = length - HEADER as u64;
                    let mut volume = Volume {
                        header: posted[..HEADER].to_vec(),
                        cbc: Cbc::new(&KEY, IV, cipher_len, plain.len() as u64).unwrap(),
                        frontier: Frontier::default(),
                        plain: vec![0; plain.len()],
                        coverage: Coverage::new(),
                        padding: Padding::default(),
                    };
                    let mut holes = Vec::new();
                    let mut articles = Vec::new();
                    for article in 0..length.div_ceil(ARTICLE) {
                        let range = ByteRange::new(
                            article * ARTICLE,
                            ((article + 1) * ARTICLE).min(length),
                        );
                        let lost = faults.iter().any(|fault| {
                            matches!(*fault, Fault::Lost { volume, article: lost }
                                if volume == index && lost == article)
                        });
                        if lost {
                            holes.push(range);
                            continue;
                        }
                        let garbled = faults.iter().any(|fault| {
                            matches!(*fault, Fault::Garbled { volume, article: garbled }
                                if volume == index && garbled == article)
                        });
                        let provenance = if garbled {
                            posted[(range.start + range.len() / 2) as usize] ^= 0x5A;
                            Provenance::Damaged
                        } else {
                            Provenance::Fresh
                        };
                        let bytes = &posted[range.start as usize..range.end as usize];
                        job.arrive(member, range.start, bytes, provenance);
                        articles.push((range, !garbled));
                        let header = HEADER as u64;
                        if range.end > header {
                            let stream = ByteRange::new(range.start.max(header), range.end);
                            let cipher = &posted[stream.start as usize..stream.end as usize];
                            let mut plains: Vec<Plain> = Vec::new();
                            let placed = volume.plain.clone();
                            volume.cbc.deliver(
                                &volume.coverage,
                                &mut volume.padding,
                                (stream.start - header, cipher, provenance),
                                &mut |wanted, buffer| {
                                    buffer.extend_from_slice(
                                        &placed[wanted.start as usize..wanted.end as usize],
                                    );
                                    true
                                },
                                &mut plains,
                            );
                            for plain in plains {
                                let at = plain.offset as usize;
                                volume.plain[at..at + plain.bytes.len()]
                                    .copy_from_slice(&plain.bytes);
                                volume.coverage.write(
                                    ByteRange::at(plain.offset, plain.bytes.len() as u64),
                                    plain.provenance,
                                );
                            }
                        }
                    }
                    job.store.members.insert(member, posted.clone());
                    job.store.holes.insert(member, holes);
                    volumes.insert(member, volume);
                    members.push(member);
                    delivered.push((member, posted, articles));
                }
                job.object.bind(&mut job.namespace, &mut Vec::new());
                for (member, posted, articles) in &delivered {
                    for &(range, verified) in articles {
                        let bytes = &posted[range.start as usize..range.end as usize];
                        let pieces = segments(range.start, bytes, ARTICLE, SLICE, verified);
                        job.object.feed(*member, &pieces);
                    }
                }
                let store = std::mem::take(&mut job.store);
                Self {
                    job,
                    access: Sealing {
                        volumes,
                        store,
                        writes: Vec::new(),
                        reads: Vec::new(),
                    },
                    members,
                    written: written.into_iter().map(|(_, _, posted)| posted).collect(),
                }
            }

            // Repairs the set under `memory`, and gives the outcome and the
            // bytes sealed again doing it.
            pub(super) fn repair(&mut self, memory: Option<u64>) -> (RepairOutcome, u64) {
                sealed();
                let budget = Budget {
                    memory_bytes: memory,
                    ..Budget::default()
                };
                let outcome = self.job.object.repair(&mut self.access, budget);
                (outcome, sealed())
            }

            // Each volume as the staged repair leaves it.
            pub(super) fn repaired(&self) -> Vec<Vec<u8>> {
                self.members
                    .iter()
                    .map(|member| {
                        let mut bytes = self.access.store.members[member].clone();
                        for (into, offset, written) in &self.access.writes {
                            if into == member {
                                let at = *offset as usize;
                                bytes[at..at + written.len()].copy_from_slice(written);
                            }
                        }
                        bytes
                    })
                    .collect()
            }

            pub(super) fn total(&self) -> u64 {
                self.written.iter().map(|bytes| bytes.len() as u64).sum()
            }
        }
    }

    // What the verification of `post` reads back and keeps, file by file,
    // as `repair_set` runs it, under a bound of `limit` bytes.
    fn verified(post: &mut encrypted::Post, limit: usize) -> (Vec<(MemberId, Vec<bool>)>, Kept) {
        let state = post.job.object.sets.values_mut().next().unwrap();
        let index = state.usable().unwrap().clone();
        let (mut read, mut proven, mut kept) = (false, Vec::new(), Kept::new(limit));
        let validity = index
            .recovery_file_ids
            .iter()
            .map(|file| {
                let mut reading = Reading {
                    access: &mut post.access,
                    read: &mut read,
                    proven: &mut proven,
                    kept: &mut kept,
                };
                let valid =
                    slice_validity(&index, state, &post.job.object.grid, *file, &mut reading);
                (state.members()[file], valid)
            })
            .collect();
        (validity, kept)
    }

    // The slices of a volume a set of reads touches.
    fn touched(reads: &[(MemberId, ByteRange)], member: MemberId) -> Vec<u64> {
        let size = encrypted::SLICE;
        let mut slices: Vec<u64> = reads
            .iter()
            .filter(|(of, _)| *of == member)
            .flat_map(|(_, range)| range.start / size..range.end.div_ceil(size))
            .collect();
        slices.sort_unstable();
        slices.dedup();
        slices
    }

    // An ordinary post with one article lost: the verification reads back
    // nothing the decoder evidence settled, and the repair reads each good
    // slice of every volume once, so every posted byte is sealed again once,
    // with at most a block of lead-in per read.
    #[test]
    fn one_lost_article_reseals_each_good_slice_once() {
        use encrypted::{ARTICLE, Fault, Post};
        let lost = Fault::Lost {
            volume: 2,
            article: 20,
        };
        let mut post = Post::new(&[lost], 4, true);
        let (_, kept) = verified(&mut post, usize::MAX);
        let lost = ByteRange::at(20 * ARTICLE, ARTICLE);
        let near = touched(&[(post.members[2], lost)], post.members[2]);
        for member in post.members.clone() {
            let read = touched(&post.access.reads, member);
            assert!(
                read.iter()
                    .all(|slice| near.contains(slice) && member == post.members[2]),
                "{member:?} read back {read:?}"
            );
        }
        assert!(kept.slices.is_empty());

        let mut post = Post::new(
            &[Fault::Lost {
                volume: 2,
                article: 20,
            }],
            4,
            true,
        );
        let (outcome, sealed) = post.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(post.repaired(), post.written);
        let reads = post.access.reads.len() as u64;
        assert!(
            sealed <= post.total() + reads * crate::crypt::BLOCK,
            "sealed {sealed} of {} posted bytes over {reads} reads",
            post.total()
        );
    }

    // An ordinary post, one article damaged on the way: the decoder failed
    // its CRC, so the damage vouches for nothing else, and the verification
    // reads back only the slices that article touches.
    #[test]
    fn a_garbled_article_reads_back_only_the_slices_it_touches() {
        use encrypted::{ARTICLE, Fault, Post};
        let mut post = Post::new(
            &[Fault::Garbled {
                volume: 2,
                article: 20,
            }],
            4,
            true,
        );
        let (validity, _) = verified(&mut post, usize::MAX);
        let near = touched(
            &[(post.members[2], ByteRange::at(20 * ARTICLE, ARTICLE))],
            post.members[2],
        );
        for member in post.members.clone() {
            let read = touched(&post.access.reads, member);
            assert!(
                read.iter()
                    .all(|slice| near.contains(slice) && member == post.members[2]),
                "{member:?} read back {read:?}"
            );
        }
        let (_, valid) = validity
            .iter()
            .find(|(member, _)| *member == post.members[2])
            .unwrap();
        let bad: Vec<u64> = (0..valid.len() as u64)
            .filter(|slice| !valid[*slice as usize])
            .collect();
        assert!(
            !bad.is_empty() && bad.iter().all(|slice| near.contains(slice)),
            "{bad:?}"
        );

        let mut post = Post::new(
            &[Fault::Garbled {
                volume: 2,
                article: 20,
            }],
            4,
            true,
        );
        let (outcome, _) = post.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(post.repaired(), post.written);
    }

    // A seal that chains on from a garbled article chains on from the cipher
    // that arrived, which is what the plaintext after it came out of: the
    // slices either side read back as they arrived, through one frontier in
    // order as through a fresh one each, and the good slice after the
    // garbled one stays good and is never rewritten. A set without slice
    // checksums sweeps the same volume whole and rebuilds it.
    #[test]
    fn a_good_slice_after_a_garbled_one_reads_back_as_it_arrived() {
        use encrypted::{ARTICLE, Fault, Post, SLICE};
        // Article 20 spans 240,000..252,000, the end of slice 14 and the
        // start of slice 15; its garbled byte sits at 246,000, in slice 15.
        let garbled = Fault::Garbled {
            volume: 2,
            article: 20,
        };
        let mut post = Post::new(&[garbled], 4, true);
        let member = post.members[2];
        let arrived = post.access.store.members[&member].clone();
        for slice in 13..17u64 {
            let range = ByteRange::at(slice * SLICE, SLICE);
            let mut chained = Vec::new();
            assert!(post.access.read(member, range, &mut chained));
            assert_eq!(
                chained,
                arrived[range.start as usize..range.end as usize],
                "{slice}"
            );
        }
        let (validity, _) = verified(&mut post, usize::MAX);
        let (_, valid) = validity.iter().find(|(of, _)| *of == member).unwrap();
        let garbled_slice = (20 * ARTICLE + ARTICLE / 2) / SLICE;
        for slice in 0..valid.len() as u64 {
            assert_eq!(valid[slice as usize], slice != garbled_slice, "{slice}");
        }
        let (outcome, _) = post.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        for (into, offset, bytes) in &post.access.writes {
            if *into == member {
                let written = offset / SLICE..(offset + bytes.len() as u64).div_ceil(SLICE);
                assert!(
                    written.clone().all(|slice| slice == garbled_slice),
                    "{written:?}"
                );
            }
        }
        assert_eq!(post.repaired(), post.written);

        let mut whole = Post::new(&[garbled], 40, false);
        let (outcome, _) = whole.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(whole.repaired(), whole.written);
    }

    // One flipped byte vetoes the volume's slices, so the verification
    // reads it back whole: what it proves good is the repair's input, and
    // nothing of the volume is sealed twice.
    #[test]
    fn slices_read_back_good_are_the_repairs_input() {
        use encrypted::{Fault, Post};
        let mut post = Post::new(
            &[Fault::Flipped {
                volume: 2,
                at: 300_000,
            }],
            16,
            true,
        );
        let (outcome, sealed) = post.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(post.repaired(), post.written);
        let reads = post.access.reads.len() as u64;
        assert!(
            sealed <= post.total() + reads * crate::crypt::BLOCK,
            "sealed {sealed} of {} posted bytes over {reads} reads",
            post.total()
        );
    }

    // A slice the verification proves bad is never kept: the repair writes
    // it from the recovery, and the bytes it writes are the posted ones.
    #[test]
    fn a_slice_proved_bad_is_never_kept() {
        use encrypted::{Fault, Post, SLICE};
        let at = 300_000;
        let mut post = Post::new(&[Fault::Flipped { volume: 2, at }], 16, true);
        let (validity, kept) = verified(&mut post, usize::MAX);
        let bad = at as u64 / SLICE;
        let (_, valid) = validity
            .iter()
            .find(|(member, _)| *member == post.members[2])
            .unwrap();
        assert!(!valid[bad as usize]);
        let state = post.job.object.sets.values().next().unwrap();
        let file = *state
            .members()
            .iter()
            .find(|(_, member)| **member == post.members[2])
            .unwrap()
            .0;
        assert!(kept.get(file, bad).is_none());
        for (slice, good) in valid.iter().enumerate() {
            if *good {
                let start = slice * SLICE as usize;
                let end = (start + SLICE as usize).min(post.written[2].len());
                assert_eq!(
                    kept.get(file, slice as u64),
                    Some(&post.written[2][start..end])
                );
            }
        }

        let (outcome, _) = post.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        let rewritten: Vec<u64> = post
            .access
            .writes
            .iter()
            .filter(|(member, _, _)| *member == post.members[2])
            .flat_map(|(_, offset, bytes)| {
                offset / SLICE..(offset + bytes.len() as u64).div_ceil(SLICE)
            })
            .collect();
        assert!(rewritten.contains(&bad), "{rewritten:?}");
        assert_eq!(post.repaired(), post.written);
    }

    // Past the bound the earliest kept slices go and are read again: the
    // repair comes to the same bytes, sealing more than it does unbounded.
    #[test]
    fn slices_past_the_bound_are_read_again() {
        use encrypted::{Fault, Post, SLICE, VOLUMES};
        let faults: Vec<Fault> = (0..VOLUMES)
            .flat_map(|volume| [40_000, 200_000, 420_000].map(|at| Fault::Flipped { volume, at }))
            .collect();
        let mut unbounded = Post::new(&faults, 24, true);
        let (outcome, all) = unbounded.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(unbounded.repaired(), unbounded.written);

        let mut bounded = Post::new(&faults, 24, true);
        let (outcome, some) = bounded.repair(Some(64 * SLICE));
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(bounded.repaired(), bounded.written);
        assert!(some > all, "bounded sealed {some}, unbounded {all}");
    }

    // A set without slice checksums proves a volume by hashing it whole:
    // that sweep is the repair's input too, so each volume is read once.
    #[test]
    fn a_volume_without_slice_checksums_is_read_once() {
        use encrypted::{Fault, Post};
        let mut post = Post::new(
            &[Fault::Lost {
                volume: 2,
                article: 20,
            }],
            40,
            false,
        );
        let (outcome, _) = post.repair(None);
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "{outcome:?}"
        );
        assert_eq!(post.repaired(), post.written);
        for (index, member) in post.members.iter().enumerate() {
            if index == 2 {
                continue;
            }
            let reads: Vec<ByteRange> = post
                .access
                .reads
                .iter()
                .filter(|(of, _)| of == member)
                .map(|(_, range)| *range)
                .collect();
            let read: u64 = reads.iter().map(ByteRange::len).sum();
            assert_eq!(
                read,
                post.written[index].len() as u64,
                "{member:?} read {reads:?}"
            );
        }
    }
}
