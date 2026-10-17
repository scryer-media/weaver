// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Repairing one set through the namespace. Slices the grid proved are taken
// as they are; slices it could not prove are read back one at a time and
// checked against their CRC32 and MD5; recovery packets are read from the
// volumes the namespace kept, their hashes checked again, and spent through
// par2-rs under the repair memory limit. Nothing here touches a file: every
// byte goes through the namespace's access.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;

use par2_rs::verify::FileAccess;
use par2_rs::{
    FileId, FileStatus, FileVerification, Packet, Par2FileSet, RecoverySlice, RepairOptions,
    Repairability, SliceChecksumState, VerificationResult, execute_repair_with_options,
    parse_packet, plan_repair_with_memory_limit,
};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{ByteRange, MemberId, SliceVerdict};

use super::evidence::Evidence;
use super::sets::SetState;
use crate::grid::Grid;

// What one set's repair came to.
#[derive(Clone, Copy, Debug)]
pub(super) struct SetRepair {
    pub(super) outcome: RepairOutcome,
    // Damaged slices it found, for the recovery it asks for while waiting.
    pub(super) damaged: u64,
}

impl SetRepair {
    fn clean() -> Self {
        Self {
            outcome: RepairOutcome::Clean,
            damaged: 0,
        }
    }
}

// A described file no post carries is announced by the namespace before the
// repair, so it is a member here with every slice missing, and the rebuild
// writes it whole.
pub(super) fn repair_set(
    state: &mut SetState,
    grid: &Grid,
    coming: u64,
    memory_limit: usize,
    access: &mut dyn MemberAccess,
) -> SetRepair {
    let Some(index) = state.usable() else {
        return SetRepair::clean();
    };
    let slice_size = index.slice_size;
    let validity: Vec<(FileId, Vec<bool>)> = index
        .recovery_file_ids
        .iter()
        .map(|file| (*file, slice_validity(index, state, grid, *file, access)))
        .collect();
    let invalid = |valid: &Vec<bool>| valid.iter().filter(|valid| !**valid).count() as u64;
    // Only damage in a bound member starts a repair; a described file no
    // member is bound to yet is missing in the arithmetic of one that does.
    let own: u64 = validity
        .iter()
        .filter(|(file, _)| state.members().contains_key(file))
        .map(|(_, valid)| invalid(valid))
        .sum();
    if own == 0 {
        return SetRepair::clean();
    }
    let damaged = validity.iter().map(|(_, valid)| invalid(valid)).sum();
    let short = |reachable: u64| SetRepair {
        outcome: if coming > 0 {
            RepairOutcome::Waiting
        } else {
            RepairOutcome::Impossible { damaged, reachable }
        },
        damaged,
    };
    let slices = spend(state, slice_size, damaged as usize, access);
    if (slices.len() as u64) < damaged {
        return short(slices.len() as u64);
    }
    let Some(index) = state.usable() else {
        return SetRepair::clean();
    };
    let mut set = index.clone();
    set.recovery_slices = slices;
    let verification = verification(&set, &validity, damaged);
    let available = set.recovery_slices.len() as u64;
    let Ok(plan) = plan_repair_with_memory_limit(&set, &verification, Some(memory_limit)) else {
        return short(available);
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
        return short(available);
    }
    SetRepair {
        outcome: RepairOutcome::Repaired { slices: damaged },
        damaged,
    }
}

// Which slices of a described file are good, reading back only the ones the
// evidence did not settle.
fn slice_validity(
    index: &Par2FileSet,
    state: &SetState,
    grid: &Grid,
    file: FileId,
    access: &mut dyn MemberAccess,
) -> Vec<bool> {
    let count = index.file_description(&file).map_or(0, |description| {
        index.slice_count_for_file(description.length)
    }) as usize;
    let Some(member) = state.members().get(&file).copied() else {
        return vec![false; count];
    };
    let evidence = Evidence::of(grid, index, file, member, access.length(member));
    if evidence.is_proven() {
        return vec![true; count];
    }
    let verdicts: Vec<SliceVerdict> = evidence.vetoed().collect();
    let mut buffer = Vec::new();
    (0..count)
        .map(|slice| match verdicts.get(slice) {
            Some(SliceVerdict::Intact) => true,
            Some(SliceVerdict::Damaged) => false,
            _ => read_slice(index, file, member, slice, access, &mut buffer),
        })
        .collect()
}

// Read one slice back and check it against both of its checksums.
fn read_slice(
    index: &Par2FileSet,
    file: FileId,
    member: MemberId,
    slice: usize,
    access: &mut dyn MemberAccess,
    buffer: &mut Vec<u8>,
) -> bool {
    let (Some(description), Some(expected)) = (
        index.file_description(&file),
        index
            .file_checksums(&file)
            .and_then(|checksums| checksums.get(slice)),
    ) else {
        return false;
    };
    let block = index.slice_size;
    let start = slice as u64 * block;
    let end = (start + block).min(description.length);
    if access.length(member).is_none_or(|length| length < end) {
        return false;
    }
    buffer.clear();
    if !access.read(member, ByteRange::new(start, end), buffer)
        || buffer.len() as u64 != end - start
    {
        return false;
    }
    let mut state = SliceChecksumState::new();
    state.update(buffer);
    let (crc32, md5) = state.finalize(Some(block));
    crc32 == expected.crc32 && md5 == expected.md5
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
