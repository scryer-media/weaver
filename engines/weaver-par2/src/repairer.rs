//! High-level PAR2 verifier/repairer.
//!
//! This module mirrors the repairer shape used by traditional PAR2 tools:
//! load packets, build source blocks, scan job-local files for usable blocks,
//! stage/copy known-good blocks, run RS reconstruction for the missing blocks,
//! and verify repaired output before installing it.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::checksum;
use crate::disk::DiskFileAccess;
use crate::error::{Par2Error, Result};
use crate::packet::{Packet, scan_packets_from_path_with_set_ids};
use crate::par2_set::Par2FileSet;
use crate::path::is_generated_par2_artifact_name;
use crate::repair::{
    DEFAULT_REPAIR_MEMORY_LIMIT, RepairOptions, execute_repair_with_options, plan_repair,
};
use crate::types::{
    CancellationToken, FileId, MAX_SLICES_PER_FILE, ProgressCallback, SliceChecksum,
};
use crate::verify::{FileStatus, FileVerification, Repairability, VerificationResult, verify_all};
use md5::{Digest, Md5};
use memmap2::MmapOptions;

const ZERO_PAD_CHUNK: [u8; 8192] = [0u8; 8192];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Par2RepairStatus {
    Verified,
    RepairPossible,
    Repaired,
    Insufficient,
}

#[derive(Debug, Clone, Default)]
pub struct PacketDiagnostics {
    pub packets_loaded: u32,
    pub corrupt_packets: u32,
    pub duplicate_packets: u32,
    pub discarded_recovery_blocks: u32,
    pub inconsistent_packets: u32,
    pub conflicting_packets: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ScanDiagnostics {
    pub files_scanned: u32,
    pub bytes_scanned: u64,
    pub blocks_found: u32,
    pub duplicate_blocks: u32,
    pub files_skipped: u32,
}

#[derive(Debug, Clone)]
pub struct Par2RepairOutcome {
    pub status: Par2RepairStatus,
    pub files_complete: u32,
    pub files_renamed: u32,
    pub files_damaged: u32,
    pub files_missing: u32,
    pub available_blocks: u32,
    pub missing_blocks: u32,
    pub recovery_blocks_available: u32,
    pub recovery_blocks_used: u32,
    pub bytes_copied: u64,
    pub bytes_reconstructed: u64,
    pub packets: PacketDiagnostics,
    pub scan: ScanDiagnostics,
    pub verification: VerificationResult,
}

#[derive(Clone)]
pub struct Par2RepairerOptions {
    pub base_dir: PathBuf,
    pub file_set: Option<Par2FileSet>,
    pub par2_paths: Vec<PathBuf>,
    pub recovery_paths: Vec<PathBuf>,
    pub extra_paths: Vec<PathBuf>,
    pub repair: bool,
    pub memory_limit: Option<usize>,
    pub cancel: Option<CancellationToken>,
    pub progress: Option<ProgressCallback>,
}

impl Par2RepairerOptions {
    pub fn new(base_dir: PathBuf, par2_paths: Vec<PathBuf>) -> Self {
        Self {
            base_dir,
            file_set: None,
            par2_paths,
            recovery_paths: Vec::new(),
            extra_paths: Vec::new(),
            repair: true,
            memory_limit: Some(DEFAULT_REPAIR_MEMORY_LIMIT),
            cancel: None,
            progress: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockLocationKind {
    Canonical,
    Renamed,
    Extra,
}

#[derive(Debug, Clone)]
pub struct BlockLocation {
    pub path: PathBuf,
    pub offset: u64,
    pub len: u64,
    pub kind: BlockLocationKind,
}

#[derive(Debug, Clone)]
pub struct SourceBlock {
    pub global_index: usize,
    pub file_id: FileId,
    pub local_index: u32,
    pub expected_len: u64,
    pub checksum: SliceChecksum,
    pub location: Option<BlockLocation>,
}

#[derive(Debug, Clone)]
pub struct SourceFileEntry {
    pub file_id: FileId,
    pub par2_name: String,
    pub safe_path: PathBuf,
    pub safe_name: String,
    pub length: u64,
    pub hash_full: [u8; 16],
    pub hash_16k: [u8; 16],
    pub recoverable: bool,
    pub first_block: usize,
    pub expected_block_count: usize,
    pub block_count: usize,
    pub target_exists: bool,
    pub complete_location: Option<BlockLocation>,
}

#[derive(Debug, Clone)]
pub struct PacketInventory {
    pub set: Par2FileSet,
    pub diagnostics: PacketDiagnostics,
}

pub struct Par2Repairer {
    options: Par2RepairerOptions,
}

impl Par2Repairer {
    pub fn new(options: Par2RepairerOptions) -> Self {
        Self { options }
    }

    pub fn verify_or_repair(&self) -> Result<Par2RepairOutcome> {
        let inventory = self.load_inventory()?;
        let mut state = RepairState::from_set(&self.options.base_dir, inventory.set)?;
        let mut packet_diagnostics = inventory.diagnostics;

        packet_diagnostics.discarded_recovery_blocks = state.discarded_recovery_blocks;
        packet_diagnostics.inconsistent_packets = state.inconsistent_packets;

        let scan = state.scan(&self.options)?;
        let verification = state.verification_result();

        if verification.total_missing_blocks == 0 && state.files_are_canonical_complete() {
            return Ok(state.outcome(
                Par2RepairStatus::Verified,
                0,
                0,
                packet_diagnostics,
                scan,
                verification,
            ));
        }

        if !self.options.repair {
            let status = if verification.total_missing_blocks <= state.set.recovery_block_count() {
                Par2RepairStatus::RepairPossible
            } else {
                Par2RepairStatus::Insufficient
            };
            return Ok(state.outcome(status, 0, 0, packet_diagnostics, scan, verification));
        }

        if verification.total_missing_blocks > state.set.recovery_block_count() {
            return Ok(state.outcome(
                Par2RepairStatus::Insufficient,
                0,
                0,
                packet_diagnostics,
                scan,
                verification,
            ));
        }

        let repair = state.repair(&self.options, &verification)?;
        let repaired_access = DiskFileAccess::new(repair.install_dir.clone(), &state.set);
        let post = verify_all(&state.set, &repaired_access);
        if post.total_missing_blocks > 0
            || !post
                .files
                .iter()
                .all(|file| matches!(file.status, FileStatus::Complete))
        {
            let _ = fs::remove_dir_all(&repair.install_dir);
            return Err(Par2Error::ReedSolomonError {
                reason: format!(
                    "post-repair verification failed: {} blocks remain damaged",
                    post.total_missing_blocks
                ),
            });
        }

        state.install_repaired_files(&repair.install_dir)?;
        let _ = fs::remove_dir_all(&repair.install_dir);

        Ok(state.outcome(
            Par2RepairStatus::Repaired,
            repair.bytes_copied,
            repair.bytes_reconstructed,
            packet_diagnostics,
            scan,
            post,
        ))
    }

    fn load_inventory(&self) -> Result<PacketInventory> {
        if let Some(set) = self.options.file_set.clone() {
            return Ok(PacketInventory {
                set,
                diagnostics: PacketDiagnostics::default(),
            });
        }

        let mut paths = Vec::<PathBuf>::new();
        let mut seen = HashSet::<PathBuf>::new();
        for path in self
            .options
            .par2_paths
            .iter()
            .chain(self.options.recovery_paths.iter())
        {
            if seen.insert(path.clone()) {
                paths.push(path.clone());
            }
        }

        for adjacent in discover_adjacent_par2_files(&self.options.base_dir)? {
            if seen.insert(adjacent.clone()) {
                paths.push(adjacent);
            }
        }

        let mut diagnostics = PacketDiagnostics::default();
        let mut recovery_set_id = None;
        let mut scanned_files = Vec::new();

        for path in paths {
            let packet_list = scan_packets_from_path_with_set_ids(&path)?;
            if packet_list.is_empty() {
                diagnostics.corrupt_packets += 1;
                continue;
            }

            if let Some(file_set_id) = packet_list.iter().find_map(|packet| match &packet.packet {
                Packet::Main(main) => Some(main.recovery_set_id),
                _ => None,
            }) {
                if let Some(existing) = recovery_set_id {
                    if existing != file_set_id {
                        diagnostics.conflicting_packets += packet_list.len() as u32;
                        continue;
                    }
                } else {
                    recovery_set_id = Some(file_set_id);
                }
            }

            scanned_files.push(packet_list);
        }

        let mut packets = Vec::new();
        for packet_list in scanned_files {
            for scanned in packet_list {
                if let Some(active_set_id) = recovery_set_id
                    && scanned.recovery_set_id != active_set_id
                {
                    diagnostics.conflicting_packets += 1;
                    continue;
                }
                diagnostics.packets_loaded += 1;
                packets.push(scanned.packet);
            }
        }

        let set = Par2FileSet::from_packets(packets)?;
        Ok(PacketInventory { set, diagnostics })
    }
}

struct RepairState {
    set: Par2FileSet,
    files: Vec<SourceFileEntry>,
    blocks: Vec<SourceBlock>,
    file_index_by_id: HashMap<FileId, usize>,
    block_index_by_file_slice: HashMap<(FileId, u32), usize>,
    hash_table: VerificationHashTable,
    discarded_recovery_blocks: u32,
    inconsistent_packets: u32,
}

struct RepairInstall {
    install_dir: PathBuf,
    bytes_copied: u64,
    bytes_reconstructed: u64,
}

impl RepairState {
    fn from_set(base_dir: &Path, mut set: Par2FileSet) -> Result<Self> {
        let mut discarded_recovery_blocks = 0;
        let slice_size = set.slice_size;
        set.recovery_slices.retain(|_, recovery| {
            let keep = recovery.data.len() as u64 == slice_size;
            if !keep {
                discarded_recovery_blocks += 1;
            }
            keep
        });

        let mut inconsistent_packets = 0;
        let mut files = Vec::new();
        let mut blocks = Vec::new();
        let mut file_index_by_id = HashMap::new();
        let mut block_index_by_file_slice = HashMap::new();

        for file_id in set
            .recovery_file_ids
            .iter()
            .chain(set.non_recovery_file_ids.iter())
        {
            let recoverable = set.recovery_file_ids.contains(file_id);
            let Some(desc) = set.files.get(file_id) else {
                if recoverable {
                    return Err(Par2Error::InsufficientCriticalData {
                        reason: format!("recoverable file {file_id} has no file description"),
                    });
                }
                inconsistent_packets += 1;
                continue;
            };
            let safe_path = base_dir.join(&desc.filename);
            let first_block = blocks.len();
            let checksums = set.slice_checksums.get(file_id);
            let expected_blocks =
                usize::try_from(set.slice_count_for_file(desc.length)).map_err(|_| {
                    Par2Error::ResourceLimitExceeded {
                        reason: format!(
                            "file {} has more than {MAX_SLICES_PER_FILE} addressable PAR2 slices",
                            desc.filename
                        ),
                    }
                })?;
            if expected_blocks > MAX_SLICES_PER_FILE {
                return Err(Par2Error::ResourceLimitExceeded {
                    reason: format!(
                        "file {} has {expected_blocks} PAR2 slices; max is {MAX_SLICES_PER_FILE}",
                        desc.filename
                    ),
                });
            }
            let mut block_count = 0usize;

            if recoverable {
                if expected_blocks == 0 {
                    // Zero-length files have no IFSC entries but still need a
                    // source entry so repair can create/verify the target.
                } else if let Some(checksums) = checksums {
                    if checksums.len() != expected_blocks {
                        return Err(Par2Error::InsufficientCriticalData {
                            reason: format!(
                                "recoverable file {} has {} IFSC entries, expected {expected_blocks}",
                                desc.filename,
                                checksums.len()
                            ),
                        });
                    }
                    block_count = checksums.len();
                    for (local_index, checksum) in checksums.iter().enumerate() {
                        let offset = local_index as u64 * slice_size;
                        let expected_len = desc.length.saturating_sub(offset).min(slice_size);
                        let global_index = blocks.len();
                        block_index_by_file_slice
                            .insert((*file_id, local_index as u32), global_index);
                        blocks.push(SourceBlock {
                            global_index,
                            file_id: *file_id,
                            local_index: local_index as u32,
                            expected_len,
                            checksum: *checksum,
                            location: None,
                        });
                    }
                } else {
                    // Match par2cmdline-turbo: a missing IFSC packet removes
                    // block-scanner evidence for this file, but FileDesc still
                    // permits exact full-hash verification and RS output.
                    inconsistent_packets += 1;
                }
            }

            let entry = SourceFileEntry {
                file_id: *file_id,
                par2_name: desc.par2_name.clone(),
                safe_path,
                safe_name: desc.filename.clone(),
                length: desc.length,
                hash_full: desc.hash_full,
                hash_16k: desc.hash_16k,
                recoverable,
                first_block,
                expected_block_count: if recoverable { expected_blocks } else { 0 },
                block_count: if recoverable { block_count } else { 0 },
                target_exists: false,
                complete_location: None,
            };
            file_index_by_id.insert(*file_id, files.len());
            files.push(entry);
        }

        let hash_table = VerificationHashTable::new(&blocks, slice_size);

        Ok(Self {
            set,
            files,
            blocks,
            file_index_by_id,
            block_index_by_file_slice,
            hash_table,
            discarded_recovery_blocks,
            inconsistent_packets,
        })
    }

    fn scan(&mut self, options: &Par2RepairerOptions) -> Result<ScanDiagnostics> {
        let mut diagnostics = ScanDiagnostics::default();
        let mut candidates = Vec::new();

        for file in &self.files {
            candidates.push((file.safe_path.clone(), BlockLocationKind::Canonical));
        }

        for path in discover_candidate_files(&options.base_dir)? {
            candidates.push((path, BlockLocationKind::Extra));
        }
        for path in &options.extra_paths {
            candidates.push((path.clone(), BlockLocationKind::Extra));
        }

        candidates.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        candidates.dedup_by(|left, right| left.0 == right.0);

        for (path, mut kind) in candidates {
            check_cancel(options)?;
            if should_skip_candidate(&path) {
                diagnostics.files_skipped += 1;
                continue;
            }
            if !path.is_file() {
                continue;
            }
            if kind == BlockLocationKind::Extra
                && self.files.iter().any(|file| file.safe_path == path)
            {
                kind = BlockLocationKind::Canonical;
            }
            let metadata = fs::metadata(&path)?;
            diagnostics.files_scanned += 1;
            diagnostics.bytes_scanned += metadata.len();

            self.scan_complete_file(&path, kind)?;
            let found_before = self
                .blocks
                .iter()
                .filter(|block| block.location.is_some())
                .count();
            RollingBlockScanner::new(&self.hash_table, self.set.slice_size).scan_file(
                &path,
                kind,
                &self.files,
                &self.file_index_by_id,
                &mut self.blocks,
            )?;
            let found_after = self
                .blocks
                .iter()
                .filter(|block| block.location.is_some())
                .count();
            diagnostics.blocks_found += found_after.saturating_sub(found_before) as u32;
        }

        self.refresh_file_states();
        Ok(diagnostics)
    }

    fn scan_complete_file(&mut self, path: &Path, kind: BlockLocationKind) -> Result<()> {
        let len = fs::metadata(path)?.len();
        let first = read_first_16k(path)?;
        let hash_16k = checksum::md5(&first);

        let candidates: Vec<usize> = self
            .files
            .iter()
            .enumerate()
            .filter_map(|(idx, file)| {
                (file.length == len && file.hash_16k == hash_16k).then_some(idx)
            })
            .collect();

        if candidates.is_empty() {
            return Ok(());
        }

        let full = hash_file(path)?;
        for idx in candidates {
            if self.files[idx].hash_full != full {
                continue;
            }

            let file_id = self.files[idx].file_id;
            let complete_kind = if self.files[idx].safe_path == path {
                BlockLocationKind::Canonical
            } else {
                kind
            };
            self.files[idx].complete_location = Some(BlockLocation {
                path: path.to_path_buf(),
                offset: 0,
                len,
                kind: complete_kind,
            });

            for local_index in 0..self.files[idx].block_count {
                let Some(block_index) = self
                    .block_index_by_file_slice
                    .get(&(file_id, local_index as u32))
                    .copied()
                else {
                    continue;
                };
                let offset = local_index as u64 * self.set.slice_size;
                let expected_len = self.blocks[block_index].expected_len;
                self.record_block_location(
                    block_index,
                    BlockLocation {
                        path: path.to_path_buf(),
                        offset,
                        len: expected_len,
                        kind: complete_kind,
                    },
                );
            }
        }

        Ok(())
    }

    fn record_block_location(&mut self, block_index: usize, location: BlockLocation) {
        let replace = self.blocks[block_index]
            .location
            .as_ref()
            .is_none_or(|existing| {
                location.kind < existing.kind
                    || (location.kind == existing.kind && location.path < existing.path)
            });
        if replace {
            self.blocks[block_index].location = Some(location);
        }
    }

    fn refresh_file_states(&mut self) {
        for file in &mut self.files {
            file.target_exists = file.safe_path.exists();
            if !file.recoverable || file.complete_location.is_some() {
                continue;
            }
            let all_blocks = (0..file.block_count).all(|local| {
                let idx = file.first_block + local;
                self.blocks
                    .get(idx)
                    .and_then(|block| block.location.as_ref())
                    .is_some()
            });
            if all_blocks && file.target_exists {
                let access = DiskFileAccess::new(
                    file.safe_path
                        .parent()
                        .unwrap_or_else(|| Path::new(""))
                        .to_path_buf(),
                    &self.set,
                );
                let _ = access;
            }
        }
    }

    fn verification_result(&self) -> VerificationResult {
        let mut files = Vec::new();
        let mut total_missing_blocks = 0u32;

        for file in self.files.iter().filter(|file| file.recoverable) {
            let mut valid_slices = vec![false; file.expected_block_count];
            for (local, valid) in valid_slices.iter_mut().enumerate().take(file.block_count) {
                let block = &self.blocks[file.first_block + local];
                *valid = block.location.is_some();
            }
            if file.complete_location.is_some() {
                valid_slices.fill(true);
            }
            let missing = if file.complete_location.is_some() {
                0
            } else {
                valid_slices.iter().filter(|valid| !**valid).count() as u32
            };
            total_missing_blocks = total_missing_blocks.saturating_add(missing);

            let status = if self.is_canonical_complete(file) {
                FileStatus::Complete
            } else if let Some(location) = file.complete_location.as_ref() {
                FileStatus::Renamed(location.path.clone())
            } else if !file.target_exists && file.complete_location.is_none() && missing > 0 {
                FileStatus::Missing
            } else {
                FileStatus::Damaged(missing)
            };

            files.push(FileVerification {
                file_id: file.file_id,
                filename: file.safe_name.clone(),
                status,
                valid_slices,
                missing_slice_count: missing,
            });
        }

        let recovery_blocks_available = self.set.recovery_block_count();
        let repairable = if total_missing_blocks == 0 && self.files_are_canonical_complete() {
            Repairability::NotNeeded
        } else if total_missing_blocks <= recovery_blocks_available {
            Repairability::Repairable {
                blocks_needed: total_missing_blocks,
                blocks_available: recovery_blocks_available,
            }
        } else {
            Repairability::Insufficient {
                blocks_needed: total_missing_blocks,
                blocks_available: recovery_blocks_available,
                deficit: total_missing_blocks - recovery_blocks_available,
            }
        };

        VerificationResult {
            files,
            recovery_blocks_available,
            total_missing_blocks,
            repairable,
        }
    }

    fn files_are_canonical_complete(&self) -> bool {
        self.files
            .iter()
            .filter(|file| file.recoverable)
            .all(|file| self.is_canonical_complete(file))
    }

    fn is_canonical_complete(&self, file: &SourceFileEntry) -> bool {
        file.complete_location.as_ref().is_some_and(|location| {
            location.kind == BlockLocationKind::Canonical && location.path == file.safe_path
        })
    }

    fn repair(
        &self,
        options: &Par2RepairerOptions,
        verification: &VerificationResult,
    ) -> Result<RepairInstall> {
        let install_dir = unique_repair_dir(&options.base_dir);
        fs::create_dir_all(&install_dir)?;
        let mut bytes_copied = 0u64;

        for file in self.files.iter().filter(|file| file.recoverable) {
            let target = install_dir.join(&file.safe_name);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            let out = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&target)?;
            out.set_len(file.length)?;
        }

        for file in self
            .files
            .iter()
            .filter(|file| file.recoverable && file.block_count == 0)
        {
            let Some(location) = file.complete_location.as_ref() else {
                continue;
            };
            let target = install_dir.join(&file.safe_name);
            copy_range(&location.path, 0, &target, 0, file.length)?;
            bytes_copied += file.length;
        }

        for block in &self.blocks {
            check_cancel(options)?;
            let Some(location) = block.location.as_ref() else {
                continue;
            };
            let Some(file_idx) = self.file_index_by_id.get(&block.file_id).copied() else {
                continue;
            };
            let target = install_dir.join(&self.files[file_idx].safe_name);
            copy_range(
                &location.path,
                location.offset,
                &target,
                block.local_index as u64 * self.set.slice_size,
                block.expected_len,
            )?;
            bytes_copied += block.expected_len;
        }

        let mut bytes_reconstructed = 0u64;
        if verification.total_missing_blocks > 0 {
            let mut access = DiskFileAccess::new(install_dir.clone(), &self.set);
            let plan = plan_repair(&self.set, verification)?;
            bytes_reconstructed = plan
                .missing_slices
                .iter()
                .filter_map(|(file_id, local)| {
                    if let Some(idx) = self.block_index_by_file_slice.get(&(*file_id, *local)) {
                        return Some(self.blocks[*idx].expected_len);
                    }
                    self.set.file_description(file_id).map(|desc| {
                        let offset = *local as u64 * self.set.slice_size;
                        desc.length.saturating_sub(offset).min(self.set.slice_size)
                    })
                })
                .sum();
            let repair_options = RepairOptions {
                cancel: options.cancel.clone(),
                progress: options.progress.clone(),
                memory_limit: options.memory_limit,
            };
            execute_repair_with_options(&plan, &self.set, &mut access, &repair_options)?;
        }

        Ok(RepairInstall {
            install_dir,
            bytes_copied,
            bytes_reconstructed,
        })
    }

    fn install_repaired_files(&self, install_dir: &Path) -> Result<()> {
        for file in self.files.iter().filter(|file| file.recoverable) {
            let src = install_dir.join(&file.safe_name);
            let dst = &file.safe_path;
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            if dst.exists() {
                let backup = unique_backup_path(dst);
                fs::rename(dst, backup)?;
            }
            fs::rename(src, dst)?;
        }
        Ok(())
    }

    fn outcome(
        &self,
        status: Par2RepairStatus,
        bytes_copied: u64,
        bytes_reconstructed: u64,
        packets: PacketDiagnostics,
        scan: ScanDiagnostics,
        verification: VerificationResult,
    ) -> Par2RepairOutcome {
        let mut files_complete = 0u32;
        let mut files_renamed = 0u32;
        let mut files_damaged = 0u32;
        let mut files_missing = 0u32;

        for file in self.files.iter().filter(|file| file.recoverable) {
            if self.is_canonical_complete(file) {
                files_complete += 1;
            } else if file.complete_location.is_some() {
                files_renamed += 1;
            } else if file.target_exists {
                files_damaged += 1;
            } else {
                files_missing += 1;
            }
        }

        let available_blocks = self
            .blocks
            .iter()
            .filter(|block| block.location.is_some())
            .count() as u32;
        let missing_blocks = self.blocks.len() as u32 - available_blocks;
        let recovery_blocks_used = verification
            .total_missing_blocks
            .min(self.set.recovery_block_count());

        Par2RepairOutcome {
            status,
            files_complete,
            files_renamed,
            files_damaged,
            files_missing,
            available_blocks,
            missing_blocks,
            recovery_blocks_available: self.set.recovery_block_count(),
            recovery_blocks_used,
            bytes_copied,
            bytes_reconstructed,
            packets,
            scan,
            verification,
        }
    }
}

struct VerificationHashTable {
    by_crc: HashMap<u32, Vec<usize>>,
    short_blocks: Vec<usize>,
    slice_size: u64,
}

impl VerificationHashTable {
    fn new(blocks: &[SourceBlock], slice_size: u64) -> Self {
        let mut by_crc: HashMap<u32, Vec<usize>> = HashMap::new();
        let mut short_blocks = Vec::new();
        for block in blocks {
            by_crc
                .entry(block.checksum.crc32)
                .or_default()
                .push(block.global_index);
            if block.expected_len < slice_size {
                short_blocks.push(block.global_index);
            }
        }
        Self {
            by_crc,
            short_blocks,
            slice_size,
        }
    }
}

struct RollingBlockScanner<'a> {
    table: &'a VerificationHashTable,
    window_table: [u32; 256],
}

impl<'a> RollingBlockScanner<'a> {
    fn new(table: &'a VerificationHashTable, slice_size: u64) -> Self {
        Self {
            table,
            window_table: generate_window_table(slice_size),
        }
    }

    fn scan_file(
        &self,
        path: &Path,
        kind: BlockLocationKind,
        files: &[SourceFileEntry],
        file_index_by_id: &HashMap<FileId, usize>,
        blocks: &mut [SourceBlock],
    ) -> Result<()> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(());
        }

        let map = unsafe { MmapOptions::new().map(&file)? };
        let slice_size = self.table.slice_size as usize;
        if slice_size > 0 && len >= slice_size {
            let mut crc = checksum::crc32(&map[..slice_size]);
            let last = len - slice_size;
            for offset in 0..=last {
                if let Some(candidates) = self.table.by_crc.get(&crc) {
                    for block_index in candidates {
                        let block = &blocks[*block_index];
                        if block.expected_len != self.table.slice_size {
                            continue;
                        }
                        let data = &map[offset..offset + slice_size];
                        let md5: [u8; 16] = Md5::digest(data).into();
                        if md5 == block.checksum.md5 {
                            record_block_location(
                                blocks,
                                *block_index,
                                BlockLocation {
                                    path: path.to_path_buf(),
                                    offset: offset as u64,
                                    len: block.expected_len,
                                    kind,
                                },
                            );
                        }
                    }
                }
                if offset < last {
                    crc = crc_slide_char(
                        crc,
                        map[offset + slice_size],
                        map[offset],
                        &self.window_table,
                    );
                }
            }
        }

        for block_index in &self.table.short_blocks {
            if blocks[*block_index].location.is_some() {
                continue;
            }
            let block = &blocks[*block_index];
            let short_len = block.expected_len as usize;
            if short_len == 0 || short_len > len {
                continue;
            }
            if let Some(file) = file_index_by_id
                .get(&block.file_id)
                .and_then(|idx| files.get(*idx))
                && file.safe_path == path
            {
                let offset = block.local_index as u64 * self.table.slice_size;
                if offset <= usize::MAX as u64 {
                    let offset = offset as usize;
                    if offset.checked_add(short_len).is_some_and(|end| end <= len)
                        && short_block_matches(
                            &map[offset..offset + short_len],
                            self.table.slice_size,
                            block,
                        )
                    {
                        record_block_location(
                            blocks,
                            *block_index,
                            BlockLocation {
                                path: path.to_path_buf(),
                                offset: offset as u64,
                                len: block.expected_len,
                                kind,
                            },
                        );
                        continue;
                    }
                }
            }
            for offset in 0..=len - short_len {
                if short_block_matches(
                    &map[offset..offset + short_len],
                    self.table.slice_size,
                    block,
                ) {
                    record_block_location(
                        blocks,
                        *block_index,
                        BlockLocation {
                            path: path.to_path_buf(),
                            offset: offset as u64,
                            len: block.expected_len,
                            kind,
                        },
                    );
                    break;
                }
            }
        }

        Ok(())
    }
}

fn record_block_location(blocks: &mut [SourceBlock], block_index: usize, location: BlockLocation) {
    let replace = blocks[block_index]
        .location
        .as_ref()
        .is_none_or(|existing| {
            location.kind < existing.kind
                || (location.kind == existing.kind && location.path < existing.path)
        });
    if replace {
        blocks[block_index].location = Some(location);
    }
}

fn short_block_matches(data: &[u8], slice_size: u64, block: &SourceBlock) -> bool {
    padded_crc(data, slice_size) == block.checksum.crc32
        && padded_md5(data, slice_size) == block.checksum.md5
}

fn check_cancel(options: &Par2RepairerOptions) -> Result<()> {
    if let Some(cancel) = options.cancel.as_ref()
        && cancel.is_cancelled()
    {
        return Err(Par2Error::Cancelled);
    }
    Ok(())
}

fn discover_adjacent_par2_files(base_dir: &Path) -> io::Result<Vec<PathBuf>> {
    discover_files_matching(base_dir, |path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("par2"))
    })
}

fn discover_candidate_files(base_dir: &Path) -> io::Result<Vec<PathBuf>> {
    discover_files_matching(base_dir, |path| {
        !path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("par2"))
    })
}

fn discover_files_matching<F>(base_dir: &Path, mut matches: F) -> io::Result<Vec<PathBuf>>
where
    F: FnMut(&Path) -> bool,
{
    let mut out = Vec::new();
    let mut stack = vec![base_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if !should_skip_candidate(&path) {
                    stack.push(path);
                }
            } else if path.is_file() && matches(&path) {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn should_skip_candidate(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(is_generated_par2_artifact_name)
}

fn read_first_16k(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; 16_384];
    let read = file.read(&mut buf)?;
    buf.truncate(read);
    Ok(buf)
}

fn hash_file(path: &Path) -> io::Result<[u8; 16]> {
    let mut file = File::open(path)?;
    let mut hasher = Md5::new();
    let mut buf = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().into())
}

fn copy_range(
    src: &Path,
    src_offset: u64,
    dst: &Path,
    dst_offset: u64,
    len: u64,
) -> io::Result<()> {
    let mut input = File::open(src)?;
    input.seek(SeekFrom::Start(src_offset))?;
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut output = OpenOptions::new().write(true).open(dst)?;
    output.seek(SeekFrom::Start(dst_offset))?;

    let mut remaining = len;
    let mut buf = [0u8; 64 * 1024];
    while remaining > 0 {
        let take = remaining.min(buf.len() as u64) as usize;
        input.read_exact(&mut buf[..take])?;
        output.write_all(&buf[..take])?;
        remaining -= take as u64;
    }
    Ok(())
}

fn unique_repair_dir(base_dir: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    base_dir.join(format!(".weaver-par2-repair-{stamp}"))
}

fn unique_backup_path(path: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("target");
    path.with_file_name(format!("{name}.weaver-par2-backup.{stamp}"))
}

fn padded_crc(data: &[u8], pad_to: u64) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    update_crc_zeros(&mut hasher, pad_to.saturating_sub(data.len() as u64));
    hasher.finalize()
}

fn padded_md5(data: &[u8], pad_to: u64) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(data);
    update_md5_zeros(&mut hasher, pad_to.saturating_sub(data.len() as u64));
    hasher.finalize().into()
}

fn update_crc_zeros(hasher: &mut crc32fast::Hasher, mut len: u64) {
    while len > 0 {
        let take = len.min(ZERO_PAD_CHUNK.len() as u64) as usize;
        hasher.update(&ZERO_PAD_CHUNK[..take]);
        len -= take as u64;
    }
}

fn update_md5_zeros(hasher: &mut Md5, mut len: u64) {
    while len > 0 {
        let take = len.min(ZERO_PAD_CHUNK.len() as u64) as usize;
        hasher.update(&ZERO_PAD_CHUNK[..take]);
        len -= take as u64;
    }
}

static CRC_TABLE: LazyLock<[u32; 256]> = LazyLock::new(|| {
    let mut table = [0u32; 256];
    for i in 0..=255u32 {
        let mut crc = i;
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0xEDB8_8320 } else { 0 };
        }
        table[i as usize] = crc;
    }
    table
});

static CRC_POWER: LazyLock<[u32; 32]> = LazyLock::new(|| {
    let mut power = [0u32; 32];
    let mut k = 0x8000_0000u32 >> 1;
    for i in 0..32 {
        power[(i + 32 - 3) & 31] = k;
        k = gf32_multiply(k, k, 0xEDB8_8320);
    }
    power
});

fn gf32_multiply(mut a: u32, mut b: u32, polynomial: u32) -> u32 {
    let mut product = 0u32;
    for _ in 0..31 {
        if b >> 31 != 0 {
            product ^= a;
        }
        a = (a >> 1) ^ if a & 1 != 0 { polynomial } else { 0 };
        b <<= 1;
    }
    if b >> 31 != 0 {
        product ^= a;
    }
    product
}

fn crc_exp8(mut n: u64) -> u32 {
    let mut result = 0x8000_0000u32;
    let mut power = 0usize;
    n %= 0xffff_ffff;
    while n != 0 {
        if n & 1 != 0 {
            result = gf32_multiply(result, CRC_POWER[power], 0xEDB8_8320);
        }
        n >>= 1;
        power = (power + 1) & 31;
    }
    result
}

fn generate_window_table(window: u64) -> [u32; 256] {
    let coeff = crc_exp8(window);
    let mut mask = gf32_multiply(!0, coeff, 0xEDB8_8320);
    mask = gf32_multiply(mask, 0x8080_0000, 0xEDB8_8320);
    mask ^= !0;

    let mut table = [0u32; 256];
    for i in 0..=255usize {
        table[i] = gf32_multiply(CRC_TABLE[i], coeff, 0xEDB8_8320) ^ mask;
    }
    table
}

fn crc_slide_char(crc: u32, new: u8, old: u8, window_table: &[u32; 256]) -> u32 {
    let crc = crc ^ !0;
    ((crc >> 8) & 0x00ff_ffff)
        ^ CRC_TABLE[((crc as u8) ^ new) as usize]
        ^ window_table[old as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::checksum::SliceChecksumState;
    use crate::types::RecoverySetId;
    use tempfile::tempdir;

    fn synthetic_set(files: &[(&str, &[u8])], slice_size: u64) -> Par2FileSet {
        let mut recovery_file_ids = Vec::new();
        let mut descriptions = HashMap::new();
        let mut slice_checksums = HashMap::new();

        for (index, (filename, bytes)) in files.iter().enumerate() {
            let mut raw_id = [0u8; 16];
            raw_id[12..].copy_from_slice(&((index as u32) + 1).to_be_bytes());
            let file_id = FileId::from_bytes(raw_id);
            recovery_file_ids.push(file_id);

            let hash_full = checksum::md5(bytes);
            let hash_16k = checksum::md5(&bytes[..bytes.len().min(16 * 1024)]);
            let mut checksums = Vec::new();
            for chunk in bytes.chunks(slice_size as usize) {
                let mut state = SliceChecksumState::new();
                state.update(chunk);
                let pad_to = ((chunk.len() as u64) < slice_size).then_some(slice_size);
                let (crc32, md5) = state.finalize(pad_to);
                checksums.push(SliceChecksum { crc32, md5 });
            }

            descriptions.insert(
                file_id,
                crate::par2_set::FileDescription {
                    file_id,
                    hash_full,
                    hash_16k,
                    length: bytes.len() as u64,
                    par2_name: (*filename).to_string(),
                    filename: (*filename).to_string(),
                },
            );
            slice_checksums.insert(file_id, checksums);
        }

        Par2FileSet {
            recovery_set_id: RecoverySetId::from_bytes([7; 16]),
            slice_size,
            recovery_file_ids,
            non_recovery_file_ids: Vec::new(),
            files: descriptions,
            slice_checksums,
            recovery_slices: BTreeMap::new(),
            creator: None,
        }
    }

    fn make_full_packet(packet_type: &[u8; 16], body: &[u8], recovery_set_id: [u8; 16]) -> Vec<u8> {
        let length = (crate::packet::header::HEADER_SIZE + body.len()) as u64;
        let mut hash_input = Vec::new();
        hash_input.extend_from_slice(&recovery_set_id);
        hash_input.extend_from_slice(packet_type);
        hash_input.extend_from_slice(body);
        let packet_hash = checksum::md5(&hash_input);

        let mut data = Vec::new();
        data.extend_from_slice(crate::packet::header::MAGIC);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(&packet_hash);
        data.extend_from_slice(&recovery_set_id);
        data.extend_from_slice(packet_type);
        data.extend_from_slice(body);
        data
    }

    #[test]
    fn rolling_crc_matches_direct_crc() {
        let data: Vec<u8> = (0..4096u32).map(|value| (value % 251) as u8).collect();
        let window = 257usize;
        let table = generate_window_table(window as u64);
        let mut crc = checksum::crc32(&data[..window]);
        for offset in 0..=data.len() - window {
            assert_eq!(crc, checksum::crc32(&data[offset..offset + window]));
            if offset < data.len() - window {
                crc = crc_slide_char(crc, data[offset + window], data[offset], &table);
            }
        }
    }

    #[test]
    fn scan_finds_complete_renamed_file_and_copy_only_repair_installs_canonical() {
        let dir = tempdir().unwrap();
        let data = b"block-zero--block-one--tail".to_vec();
        let set = synthetic_set(&[("nested/movie.r00", &data)], 8);
        let renamed = dir.path().join("scrambled.bin");
        fs::write(&renamed, &data).unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 0);
        assert!(!state.files_are_canonical_complete());
        assert_eq!(
            state
                .outcome(
                    Par2RepairStatus::RepairPossible,
                    0,
                    0,
                    PacketDiagnostics::default(),
                    ScanDiagnostics::default(),
                    verification.clone(),
                )
                .files_renamed,
            1
        );

        let repair = state.repair(&options, &verification).unwrap();
        let access = DiskFileAccess::new(repair.install_dir.clone(), &state.set);
        let post = verify_all(&state.set, &access);
        assert_eq!(post.total_missing_blocks, 0);

        state.install_repaired_files(&repair.install_dir).unwrap();
        assert_eq!(fs::read(dir.path().join("nested/movie.r00")).unwrap(), data);
        assert!(renamed.exists());
    }

    #[test]
    fn scan_uses_partial_blocks_from_extra_file() {
        let dir = tempdir().unwrap();
        let target = b"aaaabbbbccccdddd".to_vec();
        let set = synthetic_set(&[("target.bin", &target)], 4);
        fs::write(dir.path().join("partial.bin"), b"xxxxbbbbzzzzdddd").unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 2);
        assert_eq!(
            state
                .blocks
                .iter()
                .filter(|block| block.location.is_some())
                .count(),
            2
        );
    }

    #[test]
    fn recoverable_file_without_ifsc_verifies_by_full_hash() {
        let dir = tempdir().unwrap();
        let data = b"aaaabbbb".to_vec();
        let mut set = synthetic_set(&[("target.bin", &data)], 4);
        set.slice_checksums.clear();
        fs::write(dir.path().join("target.bin"), &data).unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 0);
        assert!(matches!(
            verification.files.first().map(|file| &file.status),
            Some(FileStatus::Complete)
        ));
        assert!(matches!(verification.repairable, Repairability::NotNeeded));
    }

    #[test]
    fn recoverable_file_without_ifsc_can_be_copy_only_adopted() {
        let dir = tempdir().unwrap();
        let data = b"aaaabbbb".to_vec();
        let mut set = synthetic_set(&[("target.bin", &data)], 4);
        set.slice_checksums.clear();
        fs::write(dir.path().join("renamed.bin"), &data).unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 0);
        assert!(matches!(
            verification.files.first().map(|file| &file.status),
            Some(FileStatus::Renamed(_))
        ));
        assert!(matches!(
            verification.repairable,
            Repairability::Repairable {
                blocks_needed: 0,
                ..
            }
        ));

        let repair = state.repair(&options, &verification).unwrap();
        state.install_repaired_files(&repair.install_dir).unwrap();

        assert_eq!(fs::read(dir.path().join("target.bin")).unwrap(), data);
    }

    #[test]
    fn inventory_discards_conflicting_recovery_only_packets() {
        let dir = tempdir().unwrap();
        let main_body = {
            let mut body = Vec::new();
            body.extend_from_slice(&4u64.to_le_bytes());
            body.extend_from_slice(&0u32.to_le_bytes());
            body
        };
        let active_set_id = checksum::md5(&main_body);
        fs::write(
            dir.path().join("active.par2"),
            make_full_packet(crate::packet::header::TYPE_MAIN, &main_body, active_set_id),
        )
        .unwrap();

        let mut recovery_body = Vec::new();
        recovery_body.extend_from_slice(&0u32.to_le_bytes());
        recovery_body.extend_from_slice(&[0xAA; 4]);
        fs::write(
            dir.path().join("other.vol00+01.par2"),
            make_full_packet(
                crate::packet::header::TYPE_RECOVERY,
                &recovery_body,
                [9; 16],
            ),
        )
        .unwrap();

        let repairer = Par2Repairer::new(Par2RepairerOptions::new(
            dir.path().to_path_buf(),
            vec![dir.path().join("active.par2")],
        ));
        let inventory = repairer.load_inventory().unwrap();

        assert_eq!(inventory.set.recovery_block_count(), 0);
        assert_eq!(inventory.diagnostics.conflicting_packets, 1);
    }
}
