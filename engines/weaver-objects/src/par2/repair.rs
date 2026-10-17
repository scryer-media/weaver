// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Repairing one set through the namespace. Slices the grid proved are taken
// as they are; slices it could not prove are read back and checked against
// their CRC32 and MD5; recovery packets are read from the volumes the
// namespace kept, their hashes checked, and spent through par2-rs. Nothing
// here touches a file: every byte goes through the namespace's access.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

use par2_rs::packet::{HEADER_SIZE, PacketHeader};
use par2_rs::verify::FileAccess;
use par2_rs::{
    FileId, FileStatus, FileVerification, Par2FileSet, RecoverySlice, RecoverySlicePacket,
    RepairOptions, Repairability, SliceChecksumState, VerificationResult,
    execute_repair_with_options, plan_repair,
};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{ByteRange, MemberId, SliceVerdict};

use super::evidence::Evidence;
use super::sets::{SetState, Volume};
use crate::grid::Grid;

// What one set's repair came to.
pub(super) struct SetRepair {
    pub(super) outcome: RepairOutcome,
    // Members whose slices it rewrote.
    pub(super) repaired: Vec<MemberId>,
    // Damaged slices it found, for the recovery it asks for while waiting.
    pub(super) damaged: u64,
}

impl SetRepair {
    fn clean() -> Self {
        Self {
            outcome: RepairOutcome::Clean,
            repaired: Vec::new(),
            damaged: 0,
        }
    }
}

pub(super) fn repair_set(
    state: &mut SetState,
    grid: &Grid,
    volumes: &[Volume],
    coming: u64,
    settled: &BTreeSet<MemberId>,
    access: &mut dyn MemberAccess,
) -> SetRepair {
    let Some(index) = state.usable() else {
        return SetRepair::clean();
    };
    let slice_size = index.slice_size;
    let validity: Vec<(FileId, Vec<bool>)> = index
        .recovery_file_ids
        .iter()
        .map(|file| {
            (
                *file,
                slice_validity(index, state, grid, *file, settled, access),
            )
        })
        .collect();
    let damaged = validity
        .iter()
        .flat_map(|(_, valid)| valid.iter())
        .filter(|valid| !**valid)
        .count() as u64;
    if damaged == 0 {
        return SetRepair::clean();
    }
    let short = |reachable: u64| SetRepair {
        outcome: if coming > 0 {
            RepairOutcome::Waiting
        } else {
            RepairOutcome::Impossible { damaged, reachable }
        },
        repaired: Vec::new(),
        damaged,
    };
    let slices = spend(state, slice_size, damaged as usize, volumes, access);
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
    let Ok(plan) = plan_repair(&set, &verification) else {
        return short(available);
    };
    let members: BTreeMap<FileId, MemberId> = state.members.clone();
    let mut files = Files {
        access: RefCell::new(access),
        members: &members,
        set: &set,
    };
    if execute_repair_with_options(&plan, &set, &mut files, &RepairOptions::default()).is_err() {
        return short(available);
    }
    let repaired = validity
        .iter()
        .filter(|(_, valid)| valid.contains(&false))
        .filter_map(|(file, _)| members.get(file).copied())
        .collect();
    SetRepair {
        outcome: RepairOutcome::Repaired { slices: damaged },
        repaired,
        damaged,
    }
}

// Which slices of a described file are good, reading back only the ones the
// evidence did not settle. A member an earlier set repaired is settled.
fn slice_validity(
    index: &Par2FileSet,
    state: &SetState,
    grid: &Grid,
    file: FileId,
    settled: &BTreeSet<MemberId>,
    access: &mut dyn MemberAccess,
) -> Vec<bool> {
    let count = index.file_description(&file).map_or(0, |description| {
        index.slice_count_for_file(description.length)
    }) as usize;
    let Some(member) = state.members.get(&file).copied() else {
        return vec![false; count];
    };
    if settled.contains(&member) {
        return vec![true; count];
    }
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
// `wanted` pass their hash. One that fails is forgotten.
fn spend(
    state: &mut SetState,
    slice_size: u64,
    wanted: usize,
    volumes: &[Volume],
    access: &mut dyn MemberAccess,
) -> BTreeMap<u32, RecoverySlice> {
    let mut slices = BTreeMap::new();
    let mut bad = Vec::new();
    let mut buffer = Vec::new();
    for (&exponent, location) in &state.recovery {
        if slices.len() >= wanted {
            break;
        }
        let spoiled = volumes
            .get(location.volume.0 as usize)
            .is_none_or(|volume| volume.is_spoiled(location.range));
        if spoiled {
            continue;
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
    let header = PacketHeader::parse(packet, 0).ok()?;
    header.validate_hash(packet, 0).ok()?;
    let body = packet.get(HEADER_SIZE..usize::try_from(header.length).ok()?)?;
    let parsed = RecoverySlicePacket::parse(body).ok()?;
    (parsed.data.len() as u64 == slice_size).then_some(parsed.data)
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
// namespace. A described file no member is bound to has nowhere to go, so
// what is rebuilt for it is dropped.
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

    fn read_file(&self, file: &FileId) -> io::Result<Vec<u8>> {
        self.read_file_range(file, 0, self.length(file).unwrap_or(0))
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
