// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Repairing one set through the namespace. Slices the grid proved are taken
// as they are; slices it could not prove are read back one at a time and
// checked against their CRC32 and MD5; recovery packets are read from the
// volumes the namespace kept, their hashes checked again, and spent through
// par2-rs under the repair memory limit. Nothing here touches a file: every
// byte goes through the namespace's access.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;

use par2_rs::verify::FileAccess;
use par2_rs::{
    FileHashState, FileId, FileStatus, FileVerification, Packet, Par2FileSet, RecoverySlice,
    RepairOptions, Repairability, SliceChecksumState, VerificationResult,
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
    let mut validity: Vec<(FileId, Vec<bool>)> = index
        .recovery_file_ids
        .iter()
        .map(|file| {
            let valid = slice_validity(index, state, grid, *file, access, &mut read, &mut proven);
            (*file, valid)
        })
        .collect();
    let copied = copy_held(state, index, &mut validity, access);
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
    let slices = spend(state, slice_size, damaged as usize, access);
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
    if (slices.len() as u64) < damaged {
        return short(slices.len() as u64, access);
    }
    let Some(index) = state.usable() else {
        return SetRepair::clean(read);
    };
    let mut set = index.clone();
    set.recovery_slices = slices;
    let verification = verification(&set, &validity, damaged);
    let available = set.recovery_slices.len() as u64;
    let Ok(plan) = plan_repair_with_memory_limit(&set, &verification, Some(memory_limit)) else {
        return short(available, access);
    };
    let members: BTreeMap<FileId, MemberId> = state.members().clone();
    let mut files = Files {
        access: RefCell::new(access),
        members: &members,
        set: &set,
    };
    let options = RepairOptions {
        memory_limit: Some(memory_limit),
        ..Default::default()
    };
    if execute_repair_with_options(&plan, &set, &mut files, &options).is_err() {
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
// otherwise, so the set rebuilds them all.
fn slice_validity(
    index: &Par2FileSet,
    state: &SetState,
    grid: &Grid,
    file: FileId,
    access: &mut dyn MemberAccess,
    read: &mut bool,
    proven: &mut Vec<(MemberId, ByteRange)>,
) -> Vec<bool> {
    let count = index.file_description(&file).map_or(0, |description| {
        index.slice_count_for_file(description.length)
    }) as usize;
    let Some(member) = state.members().get(&file).copied() else {
        return vec![false; count];
    };
    if index.file_checksums(&file).is_none() {
        *read = true;
        let whole = whole_file_matches(index, file, member, access);
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
                    *read = true;
                    read_slice(index, file, member, slice, access, &mut buffer)
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
// so nothing is proven of it.
fn whole_file_matches(
    index: &Par2FileSet,
    file: FileId,
    member: MemberId,
    access: &mut dyn MemberAccess,
) -> Option<bool> {
    const CHUNK: u64 = 1 << 20;
    let description = index.file_description(&file)?;
    if access.length(member) != Some(description.length) {
        return None;
    }
    let mut hasher = FileHashState::new();
    let mut buffer = Vec::new();
    let mut at = 0;
    while at < description.length {
        let end = (at + CHUNK).min(description.length);
        buffer.clear();
        if !access.read(member, ByteRange::new(at, end), &mut buffer)
            || buffer.len() as u64 != end - at
        {
            return None;
        }
        hasher.update(&buffer);
        at = end;
    }
    Some(hasher.finalize() == description.hash_full)
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
// `wanted` pass their hash. The scan checked it; the volume is read again
// here, so it is checked again. One that fails is forgotten.
fn spend(
    state: &mut SetState,
    slice_size: u64,
    wanted: usize,
    access: &mut dyn MemberAccess,
) -> BTreeMap<u32, RecoverySlice> {
    let mut slices = BTreeMap::new();
    let mut bad = Vec::new();
    let mut buffer = Vec::new();
    for (&exponent, location) in &state.recovery {
        if slices.len() >= wanted {
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
    slices
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
}

impl Files<'_> {
    fn member(&self, file: &FileId) -> io::Result<MemberId> {
        self.members
            .get(file)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no member is this file"))
    }

    fn length(&self, file: &FileId) -> Option<u64> {
        let member = self.member(file).ok()?;
        self.access.borrow().length(member)
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
        if self
            .access
            .borrow_mut()
            .read(member, ByteRange::new(offset, end), &mut bytes)
        {
            Ok(bytes)
        } else {
            Err(io::Error::other("the namespace could not read the member"))
        }
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
