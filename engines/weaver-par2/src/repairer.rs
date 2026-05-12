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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::checksum::{self, Md5State};
use crate::disk::DiskFileAccess;
use crate::error::{Par2Error, Result};
use crate::md5_simd;
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
use memmap2::MmapOptions;
use tracing::{debug, warn};

const ZERO_PAD_CHUNK: [u8; 8192] = [0u8; 8192];
const SCANNER_MD5_BATCH_LANES: usize = 4;
const SCANNER_MD5_BATCH_MEMORY_BYTES: usize = 4 * 1024 * 1024;
const SCANNER_IO_TARGET_BYTES: usize = 4 * 1024 * 1024;
const SCANNER_MMAP_FALLBACK_SLICE_BYTES: usize = 8 * 1024 * 1024;
const ORDERED_SCAN_SKIP_LEEWAY: usize = 64;
const SCANNER_SLOW_WARN_STEPS: u64 = 5_000_000;
const SCANNER_SLOW_WARN_DURATION: Duration = Duration::from_secs(5);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileScanMode {
    Complete,
    OrderedCanonical,
    RollingGeneric,
}

impl FileScanMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::OrderedCanonical => "ordered_canonical",
            Self::RollingGeneric => "rolling_generic",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FileScanStats {
    mode: FileScanMode,
    bytes_scanned: u64,
    windows_stepped: u64,
    jumps_taken: u64,
    max_consecutive_steps: u64,
}

impl FileScanStats {
    fn new(mode: FileScanMode, bytes_scanned: u64) -> Self {
        Self {
            mode,
            bytes_scanned,
            windows_stepped: 0,
            jumps_taken: 0,
            max_consecutive_steps: 0,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
            let status = match verification.repairable {
                Repairability::NotNeeded => Par2RepairStatus::Verified,
                Repairability::Repairable { .. } => Par2RepairStatus::RepairPossible,
                Repairability::Insufficient { .. } => Par2RepairStatus::Insufficient,
            };
            return Ok(state.outcome(status, 0, 0, packet_diagnostics, scan, verification));
        }

        if matches!(verification.repairable, Repairability::Insufficient { .. }) {
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
        let repaired_access = RepairVerificationAccess::new(
            &state.files,
            &repair.install_dir,
            &repair.staged_file_ids,
        );
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

        state.install_repaired_files(&repair)?;
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
        let mut main_seen = false;
        let mut creator_seen = false;
        let mut file_desc_ids = HashSet::<FileId>::new();
        let mut ifsc_ids = HashSet::<FileId>::new();
        let mut recovery_exponents = HashSet::<u32>::new();
        for packet_list in scanned_files {
            for scanned in packet_list {
                if let Some(active_set_id) = recovery_set_id
                    && scanned.recovery_set_id != active_set_id
                {
                    diagnostics.conflicting_packets += 1;
                    continue;
                }
                match &scanned.packet {
                    Packet::Main(_) if main_seen => diagnostics.duplicate_packets += 1,
                    Packet::Main(_) => main_seen = true,
                    Packet::FileDescription(packet) if !file_desc_ids.insert(packet.file_id) => {
                        diagnostics.duplicate_packets += 1;
                    }
                    Packet::InputFileSliceChecksum(packet) if !ifsc_ids.insert(packet.file_id) => {
                        diagnostics.duplicate_packets += 1;
                    }
                    Packet::RecoverySlice(packet)
                        if !recovery_exponents.insert(packet.exponent) =>
                    {
                        diagnostics.duplicate_packets += 1;
                    }
                    Packet::Creator(_) if creator_seen => diagnostics.duplicate_packets += 1,
                    Packet::Creator(_) => creator_seen = true,
                    Packet::Unknown { .. }
                    | Packet::FileDescription(_)
                    | Packet::InputFileSliceChecksum(_)
                    | Packet::RecoverySlice(_) => {}
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
    discarded_recoverable_files: u32,
}

struct RepairInstall {
    install_dir: PathBuf,
    staged_file_ids: HashSet<FileId>,
    bytes_copied: u64,
    bytes_reconstructed: u64,
}

struct RepairExecutionAccess {
    slice_size: u64,
    repair_paths: HashMap<FileId, PathBuf>,
    source_locations: HashMap<(FileId, u32), BlockLocation>,
}

impl RepairExecutionAccess {
    fn new(
        install_dir: PathBuf,
        files: &[SourceFileEntry],
        blocks: &[SourceBlock],
        staged_file_ids: &HashSet<FileId>,
        slice_size: u64,
    ) -> Self {
        let repair_paths = files
            .iter()
            .filter(|file| staged_file_ids.contains(&file.file_id))
            .map(|file| (file.file_id, install_dir.join(&file.safe_name)))
            .collect();
        let source_locations = blocks
            .iter()
            .filter_map(|block| {
                block
                    .location
                    .clone()
                    .map(|location| ((block.file_id, block.local_index), location))
            })
            .collect();

        Self {
            slice_size,
            repair_paths,
            source_locations,
        }
    }

    fn repair_path_for(&self, file_id: &FileId) -> io::Result<&Path> {
        self.repair_paths
            .get(file_id)
            .map(PathBuf::as_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "repair target not staged"))
    }
}

impl crate::verify::FileAccess for RepairExecutionAccess {
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        let read_len = self.read_file_range_into(file_id, offset, &mut buf)?;
        buf.truncate(read_len);
        Ok(buf)
    }

    fn read_file_range_into(
        &self,
        file_id: &FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        if let Some(slice_index) = offset.checked_div(self.slice_size) {
            let local_slice = slice_index as u32;
            let slice_offset = offset % self.slice_size;
            if let Some(location) = self.source_locations.get(&(*file_id, local_slice)) {
                if slice_offset >= location.len {
                    return Ok(0);
                }
                let len = (dst.len() as u64).min(location.len - slice_offset) as usize;
                let mut file = File::open(&location.path)?;
                file.seek(SeekFrom::Start(location.offset + slice_offset))?;
                return file.read(&mut dst[..len]);
            }
        }

        let path = self.repair_path_for(file_id)?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.read(dst)
    }

    fn open_sequential_reader(
        &self,
        file_id: &FileId,
    ) -> io::Result<Option<Box<dyn std::io::Read>>> {
        if self
            .source_locations
            .keys()
            .any(|(source_file_id, _)| source_file_id == file_id)
        {
            return Ok(None);
        }

        Ok(Some(Box::new(File::open(self.repair_path_for(file_id)?)?)))
    }

    fn file_exists(&self, file_id: &FileId) -> bool {
        self.repair_paths
            .get(file_id)
            .is_some_and(|path| path.exists())
    }

    fn file_length(&self, file_id: &FileId) -> Option<u64> {
        self.repair_paths
            .get(file_id)
            .and_then(|path| fs::metadata(path).ok())
            .map(|metadata| metadata.len())
    }

    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
        fs::read(self.repair_path_for(file_id)?)
    }

    fn write_file_range(&mut self, file_id: &FileId, offset: u64, data: &[u8]) -> io::Result<()> {
        let path = self.repair_path_for(file_id)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }
}

struct RepairVerificationAccess {
    paths: HashMap<FileId, PathBuf>,
}

impl RepairVerificationAccess {
    fn new(
        files: &[SourceFileEntry],
        install_dir: &Path,
        staged_file_ids: &HashSet<FileId>,
    ) -> Self {
        let paths = files
            .iter()
            .map(|file| {
                let path = if staged_file_ids.contains(&file.file_id) {
                    install_dir.join(&file.safe_name)
                } else {
                    file.safe_path.clone()
                };
                (file.file_id, path)
            })
            .collect();

        Self { paths }
    }

    fn path_for(&self, file_id: &FileId) -> io::Result<&Path> {
        self.paths
            .get(file_id)
            .map(PathBuf::as_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown file ID"))
    }
}

impl crate::verify::FileAccess for RepairVerificationAccess {
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let mut file = File::open(self.path_for(file_id)?)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        let read_len = file.read(&mut buf)?;
        buf.truncate(read_len);
        Ok(buf)
    }

    fn read_file_range_into(
        &self,
        file_id: &FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        let mut file = File::open(self.path_for(file_id)?)?;
        file.seek(SeekFrom::Start(offset))?;
        file.read(dst)
    }

    fn open_sequential_reader(
        &self,
        file_id: &FileId,
    ) -> io::Result<Option<Box<dyn std::io::Read>>> {
        Ok(Some(Box::new(File::open(self.path_for(file_id)?)?)))
    }

    fn file_exists(&self, file_id: &FileId) -> bool {
        self.paths.get(file_id).is_some_and(|path| path.exists())
    }

    fn file_length(&self, file_id: &FileId) -> Option<u64> {
        self.paths
            .get(file_id)
            .and_then(|path| fs::metadata(path).ok())
            .map(|metadata| metadata.len())
    }

    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
        fs::read(self.path_for(file_id)?)
    }

    fn write_file_range(
        &mut self,
        _file_id: &FileId,
        _offset: u64,
        _data: &[u8],
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "verification access is read-only",
        ))
    }
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
        let mut discarded_recoverable_files = 0;
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
                inconsistent_packets += 1;
                if recoverable {
                    discarded_recoverable_files += 1;
                }
                continue;
            };
            let safe_path = base_dir.join(&desc.filename);
            let first_block = blocks.len();
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
                } else if let Some(checksum_count) = set
                    .slice_checksums
                    .get(file_id)
                    .map(|checksums| checksums.len())
                {
                    if checksum_count != expected_blocks {
                        // Match par2cmdline-turbo's safety posture: a bad IFSC
                        // packet is unusable block metadata, not proof that the
                        // described file can be ignored.
                        set.slice_checksums.remove(file_id);
                        inconsistent_packets += 1;
                    } else if let Some(checksums) = set.slice_checksums.get(file_id) {
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
            discarded_recoverable_files,
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

            let started = Instant::now();
            let found_before = self
                .blocks
                .iter()
                .filter(|block| block.location.is_some())
                .count();
            if self.scan_complete_file(&path, kind)? {
                let found_after = self
                    .blocks
                    .iter()
                    .filter(|block| block.location.is_some())
                    .count();
                let blocks_confirmed = found_after.saturating_sub(found_before) as u32;
                diagnostics.blocks_found += blocks_confirmed;
                log_file_scan(
                    &path,
                    kind,
                    FileScanStats::new(FileScanMode::Complete, metadata.len()),
                    blocks_confirmed,
                    started.elapsed(),
                );
                continue;
            }
            let ordered_target = (kind == BlockLocationKind::Canonical)
                .then(|| {
                    self.files
                        .iter()
                        .find(|file| {
                            file.safe_path == path && file.recoverable && file.block_count > 0
                        })
                        .cloned()
                })
                .flatten();
            let scanner = RollingBlockScanner::new(&self.hash_table, self.set.slice_size);
            let stats = if let Some(target_file) = ordered_target.as_ref() {
                scanner.scan_file_ordered_canonical(
                    &path,
                    kind,
                    &self.files,
                    &self.file_index_by_id,
                    target_file,
                    &mut self.blocks,
                )?
            } else {
                scanner.scan_file(
                    &path,
                    kind,
                    &self.files,
                    &self.file_index_by_id,
                    &mut self.blocks,
                )?
            };
            let found_after = self
                .blocks
                .iter()
                .filter(|block| block.location.is_some())
                .count();
            let blocks_confirmed = found_after.saturating_sub(found_before) as u32;
            diagnostics.blocks_found += blocks_confirmed;
            log_file_scan(&path, kind, stats, blocks_confirmed, started.elapsed());
        }

        self.refresh_file_states();
        Ok(diagnostics)
    }

    fn scan_complete_file(&mut self, path: &Path, kind: BlockLocationKind) -> Result<bool> {
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
            return Ok(false);
        }

        let full = hash_file(path)?;
        let mut found_complete = false;
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
            found_complete = true;
        }

        Ok(found_complete)
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
        let mut missing_unrepairable_block_metadata = self.discarded_recoverable_files > 0;

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
            if missing > 0 && file.block_count < file.expected_block_count {
                missing_unrepairable_block_metadata = true;
            }

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
        let blocks_needed = total_missing_blocks.saturating_add(self.discarded_recoverable_files);
        let repairable = if total_missing_blocks == 0 && self.files_are_canonical_complete() {
            Repairability::NotNeeded
        } else if missing_unrepairable_block_metadata {
            Repairability::Insufficient {
                blocks_needed,
                blocks_available: recovery_blocks_available,
                deficit: blocks_needed
                    .saturating_sub(recovery_blocks_available)
                    .max(1),
            }
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
        if self.discarded_recoverable_files > 0 {
            return false;
        }
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
        let staged_file_ids: HashSet<FileId> = self
            .files
            .iter()
            .filter(|file| file.recoverable && !self.is_canonical_complete(file))
            .map(|file| file.file_id)
            .collect();

        for file in self
            .files
            .iter()
            .filter(|file| staged_file_ids.contains(&file.file_id))
        {
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
            .filter(|file| staged_file_ids.contains(&file.file_id) && file.block_count == 0)
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
            if !staged_file_ids.contains(&block.file_id) {
                continue;
            }
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
            let mut access = RepairExecutionAccess::new(
                install_dir.clone(),
                &self.files,
                &self.blocks,
                &staged_file_ids,
                self.set.slice_size,
            );
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
            staged_file_ids,
            bytes_copied,
            bytes_reconstructed,
        })
    }

    fn install_repaired_files(&self, repair: &RepairInstall) -> Result<()> {
        for file in self
            .files
            .iter()
            .filter(|file| repair.staged_file_ids.contains(&file.file_id))
        {
            let src = repair.install_dir.join(&file.safe_name);
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
        let mut files_missing = self.discarded_recoverable_files;

        for file in &verification.files {
            match file.status {
                FileStatus::Complete => {
                    files_complete += 1;
                }
                FileStatus::Renamed(_) => {
                    files_renamed += 1;
                }
                FileStatus::Damaged(_) => {
                    files_damaged += 1;
                }
                FileStatus::Missing => {
                    files_missing += 1;
                }
            }
        }

        let available_blocks = self
            .blocks
            .iter()
            .filter(|block| block.location.is_some())
            .count() as u32;
        let missing_blocks = verification.total_missing_blocks;
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

struct PendingMd5Check<'a> {
    block_index: usize,
    data: &'a [u8],
    offset: u64,
    len: u64,
    kind: BlockLocationKind,
}

struct OrderedWindowMatch<'a> {
    path: &'a Path,
    kind: BlockLocationKind,
    target_file_id: &'a FileId,
    expected_block: Option<usize>,
    data: &'a [u8],
    crc: u32,
    offset: u64,
}

struct OrderedWindowCursor<'a> {
    file: File,
    len: usize,
    block_size: usize,
    buffer: Vec<u8>,
    read_offset: usize,
    current_offset: usize,
    out_index: usize,
    in_index: usize,
    tail_index: usize,
    crc: u32,
    window_table: &'a [u32; 256],
}

impl<'a> OrderedWindowCursor<'a> {
    fn new(path: &Path, block_size: usize, window_table: &'a [u32; 256]) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        let buffer_len = block_size.checked_mul(2).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "scanner buffer overflow")
        })?;
        let mut cursor = Self {
            file,
            len,
            block_size,
            buffer: vec![0u8; buffer_len],
            read_offset: 0,
            current_offset: 0,
            out_index: 0,
            in_index: block_size,
            tail_index: 0,
            crc: 0,
            window_table,
        };
        cursor.fill(true)?;
        cursor.crc = checksum::crc32(&cursor.buffer[..block_size]);
        Ok(cursor)
    }

    fn last_full_offset(&self) -> usize {
        self.len - self.block_size
    }

    fn offset(&self) -> usize {
        self.current_offset
    }

    fn data(&self) -> &[u8] {
        &self.buffer[self.out_index..self.out_index + self.block_size]
    }

    fn crc(&self) -> u32 {
        self.crc
    }

    fn step(&mut self) -> io::Result<bool> {
        if self.current_offset >= self.last_full_offset() {
            self.current_offset = self.last_full_offset().saturating_add(1);
            return Ok(false);
        }

        self.current_offset += 1;
        if self.tail_index <= self.in_index {
            self.fill(true)?;
        }

        let incoming = self.buffer[self.in_index];
        let outgoing = self.buffer[self.out_index];
        self.in_index += 1;
        self.out_index += 1;
        self.crc = crc_slide_char(self.crc, incoming, outgoing, self.window_table);

        if self.out_index == self.block_size {
            self.buffer.copy_within(self.out_index..self.tail_index, 0);
            self.tail_index -= self.block_size;
            self.in_index -= self.block_size;
            self.out_index = 0;
        }

        Ok(true)
    }

    fn jump(&mut self, mut distance: usize) -> io::Result<bool> {
        if distance == 0 {
            return Ok(self.current_offset <= self.last_full_offset());
        }
        if distance == 1 {
            return self.step();
        }
        distance = distance.min(self.block_size);

        let next_offset = self.current_offset.saturating_add(distance);
        if next_offset > self.last_full_offset() {
            self.current_offset = self.last_full_offset().saturating_add(1);
            return Ok(false);
        }

        self.current_offset = next_offset;
        let discard_start = self.out_index + distance;
        let keep = self.tail_index.saturating_sub(discard_start);
        if keep > 0 {
            self.buffer.copy_within(discard_start..self.tail_index, 0);
        }
        self.tail_index = keep;
        self.out_index = 0;
        self.in_index = self.block_size;
        self.fill(true)?;
        self.crc = checksum::crc32(&self.buffer[..self.block_size]);
        Ok(true)
    }

    fn fill(&mut self, long_fill: bool) -> io::Result<()> {
        if self.read_offset >= self.len {
            return Ok(());
        }

        let target = if !long_fill && self.tail_index >= self.block_size {
            self.block_size
        } else {
            self.buffer.len()
        };

        while self.tail_index < target && self.read_offset < self.len {
            let want = (target - self.tail_index).min(self.len - self.read_offset);
            let read = self
                .file
                .read(&mut self.buffer[self.tail_index..self.tail_index + want])?;
            if read == 0 {
                break;
            }
            self.tail_index += read;
            self.read_offset += read;
        }

        if self.tail_index < self.buffer.len() {
            self.buffer[self.tail_index..].fill(0);
        }
        Ok(())
    }
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
    ) -> Result<FileScanStats> {
        if scanner_uses_mmap_fallback(self.table.slice_size) {
            return self.scan_file_mmap(path, kind, files, file_index_by_id, blocks);
        }

        self.scan_file_buffered_with_target(
            path,
            kind,
            files,
            file_index_by_id,
            blocks,
            SCANNER_IO_TARGET_BYTES,
        )
    }

    fn scan_file_ordered_canonical(
        &self,
        path: &Path,
        kind: BlockLocationKind,
        files: &[SourceFileEntry],
        file_index_by_id: &HashMap<FileId, usize>,
        target_file: &SourceFileEntry,
        blocks: &mut [SourceBlock],
    ) -> Result<FileScanStats> {
        let len = fs::metadata(path)?.len() as usize;
        let mut stats = FileScanStats::new(FileScanMode::OrderedCanonical, len as u64);
        let slice_size = self.table.slice_size as usize;
        if len == 0 || slice_size == 0 || len < slice_size {
            return Ok(stats);
        }
        let ordered_full_blocks: Vec<usize> = (0..target_file.block_count)
            .map(|local| target_file.first_block + local)
            .filter(|block_index| blocks[*block_index].expected_len == self.table.slice_size)
            .collect();
        let mut cursor = OrderedWindowCursor::new(path, slice_size, &self.window_table)?;
        let mut preferred_next = (!ordered_full_blocks.is_empty()).then_some(0usize);
        let scandistance = ORDERED_SCAN_SKIP_LEEWAY.saturating_mul(2).min(slice_size);
        let scanskip = slice_size.saturating_sub(scandistance);
        let mut scanoffset = scandistance / 2;
        let mut current_step_run = 0u64;

        while cursor.offset() <= cursor.last_full_offset() {
            let expected_block = preferred_next
                .and_then(|position| ordered_full_blocks.get(position))
                .copied();
            let selected = self.scan_ordered_window(
                OrderedWindowMatch {
                    path,
                    kind,
                    target_file_id: &target_file.file_id,
                    expected_block,
                    data: cursor.data(),
                    crc: cursor.crc(),
                    offset: cursor.offset() as u64,
                },
                blocks,
            );

            if let Some(selected) = selected {
                if blocks[selected].file_id == target_file.file_id {
                    preferred_next = ordered_full_blocks
                        .iter()
                        .position(|block_index| *block_index == selected)
                        .and_then(|position| {
                            ordered_full_blocks.get(position + 1).map(|_| position + 1)
                        });
                } else {
                    preferred_next = None;
                }

                stats.jumps_taken += 1;
                stats.max_consecutive_steps = stats.max_consecutive_steps.max(current_step_run);
                current_step_run = 0;
                scanoffset = scandistance / 2;

                if !cursor.jump(blocks[selected].expected_len as usize)? {
                    break;
                }
                continue;
            }

            preferred_next = None;
            if !cursor.step()? {
                break;
            }

            stats.windows_stepped += 1;
            current_step_run += 1;
            if scanskip > 0
                && {
                    scanoffset += 1;
                    scanoffset >= scandistance
                }
                && cursor.offset() < cursor.len
            {
                stats.max_consecutive_steps = stats.max_consecutive_steps.max(current_step_run);
                current_step_run = 0;
                stats.jumps_taken += 1;
                if !cursor.jump(scanskip)? {
                    break;
                }
                scanoffset = 0;
            }
        }

        stats.max_consecutive_steps = stats.max_consecutive_steps.max(current_step_run);

        scan_short_blocks_from_file(self.table, path, kind, files, file_index_by_id, blocks, len)?;

        Ok(stats)
    }

    fn scan_ordered_window(
        &self,
        window: OrderedWindowMatch<'_>,
        blocks: &mut [SourceBlock],
    ) -> Option<usize> {
        let mut selected = None;
        let mut md5 = None;

        if let Some(expected_block) = window.expected_block {
            let block = &blocks[expected_block];
            if block.expected_len == self.table.slice_size && block.checksum.crc32 == window.crc {
                let digest = checksum::md5(window.data);
                md5 = Some(digest);
                if block.checksum.md5 == digest
                    && can_select_ordered_match(
                        expected_block,
                        Some(expected_block),
                        window.path,
                        blocks,
                    )
                {
                    selected = Some(expected_block);
                }
            }
        }

        if let Some(candidates) = self.table.by_crc.get(&window.crc) {
            for block_index in candidates {
                let block = &blocks[*block_index];
                if block.expected_len != self.table.slice_size {
                    continue;
                }
                if Some(*block_index) == window.expected_block && selected == Some(*block_index) {
                    continue;
                }

                let digest = *md5.get_or_insert_with(|| checksum::md5(window.data));
                if block.checksum.md5 != digest {
                    continue;
                }

                if can_select_ordered_match(*block_index, window.expected_block, window.path, blocks)
                    && preferred_ordered_match(
                        selected,
                        *block_index,
                        window.expected_block,
                        *window.target_file_id,
                        blocks,
                    )
                {
                    selected = Some(*block_index);
                }
            }
        }

        if let Some(selected) = selected {
            let block = &blocks[selected];
            record_block_location(
                blocks,
                selected,
                BlockLocation {
                    path: window.path.to_path_buf(),
                    offset: window.offset,
                    len: block.expected_len,
                    kind: window.kind,
                },
            );
        }

        selected
    }

    fn scan_file_buffered_with_target(
        &self,
        path: &Path,
        kind: BlockLocationKind,
        files: &[SourceFileEntry],
        file_index_by_id: &HashMap<FileId, usize>,
        blocks: &mut [SourceBlock],
        read_target: usize,
    ) -> Result<FileScanStats> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        let mut stats = FileScanStats::new(FileScanMode::RollingGeneric, len as u64);
        if len == 0 {
            return Ok(stats);
        }

        let slice_size = self.table.slice_size as usize;
        if slice_size > 0 && len >= slice_size {
            let overlap = slice_size - 1;
            let fresh_read_target = slice_size.max(read_target);
            let buffer_len = overlap.checked_add(fresh_read_target).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "scanner buffer size overflow")
            })?;
            let mut buffer = vec![0u8; buffer_len];
            let mut valid_len = 0usize;
            let mut base_offset = 0usize;
            let mut next_unscanned_offset = 0usize;

            loop {
                if valid_len == buffer.len() {
                    let keep = overlap.min(valid_len);
                    buffer.copy_within(valid_len - keep..valid_len, 0);
                    base_offset += valid_len - keep;
                    valid_len = keep;
                }

                let read_len = file.read(&mut buffer[valid_len..])?;
                valid_len += read_len;

                stats.windows_stepped += scan_buffered_windows(
                    self,
                    &buffer[..valid_len],
                    base_offset,
                    &mut next_unscanned_offset,
                    path,
                    kind,
                    blocks,
                );

                if read_len == 0 {
                    break;
                }
            }
        }

        stats.max_consecutive_steps = stats.windows_stepped;
        scan_short_blocks_from_file(self.table, path, kind, files, file_index_by_id, blocks, len)?;

        Ok(stats)
    }

    fn scan_file_mmap(
        &self,
        path: &Path,
        kind: BlockLocationKind,
        files: &[SourceFileEntry],
        file_index_by_id: &HashMap<FileId, usize>,
        blocks: &mut [SourceBlock],
    ) -> Result<FileScanStats> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        let mut stats = FileScanStats::new(FileScanMode::RollingGeneric, len as u64);
        if len == 0 {
            return Ok(stats);
        }

        let map = unsafe { MmapOptions::new().map(&file)? };
        let slice_size = self.table.slice_size as usize;
        if slice_size > 0 && len >= slice_size {
            let mut crc = checksum::crc32(&map[..slice_size]);
            let last = len - slice_size;
            let scanner_batch_lanes = scanner_md5_batch_lanes(slice_size);
            let mut pending = Vec::with_capacity(scanner_batch_lanes);
            for offset in 0..=last {
                if let Some(candidates) = self.table.by_crc.get(&crc) {
                    for block_index in candidates {
                        let block = &blocks[*block_index];
                        if block.expected_len != self.table.slice_size {
                            continue;
                        }
                        let data = &map[offset..offset + slice_size];
                        if scanner_batch_lanes < 2 {
                            record_matching_md5_block(
                                blocks,
                                *block_index,
                                data,
                                path,
                                offset as u64,
                                block.expected_len,
                                kind,
                            );
                            continue;
                        }
                        pending.push(PendingMd5Check {
                            block_index: *block_index,
                            data,
                            offset: offset as u64,
                            len: block.expected_len,
                            kind,
                        });
                        if pending.len() == scanner_batch_lanes {
                            flush_pending_md5_checks(&mut pending, blocks, path);
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
                    stats.windows_stepped += 1;
                }
            }
            flush_pending_md5_checks(&mut pending, blocks, path);
        }
        stats.max_consecutive_steps = stats.windows_stepped;

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
            let tail_offset = len - short_len;
            if short_block_matches(
                &map[tail_offset..tail_offset + short_len],
                self.table.slice_size,
                block,
            ) {
                record_block_location(
                    blocks,
                    *block_index,
                    BlockLocation {
                        path: path.to_path_buf(),
                        offset: tail_offset as u64,
                        len: block.expected_len,
                        kind,
                    },
                );
            }
        }

        Ok(stats)
    }
}

fn ordered_match_rank(
    block_index: usize,
    expected_block: Option<usize>,
    preferred_file_id: FileId,
    blocks: &[SourceBlock],
) -> (u8, usize) {
    if Some(block_index) == expected_block {
        return (0, block_index);
    }
    if blocks[block_index].file_id == preferred_file_id {
        return (1, block_index);
    }
    (2, block_index)
}

fn can_select_ordered_match(
    block_index: usize,
    expected_block: Option<usize>,
    path: &Path,
    blocks: &[SourceBlock],
) -> bool {
    match blocks[block_index].location.as_ref() {
        None => true,
        Some(location) if Some(block_index) == expected_block => location.path != path,
        Some(_) => false,
    }
}

fn preferred_ordered_match(
    current: Option<usize>,
    candidate: usize,
    expected_block: Option<usize>,
    preferred_file_id: FileId,
    blocks: &[SourceBlock],
) -> bool {
    let candidate_rank = ordered_match_rank(candidate, expected_block, preferred_file_id, blocks);
    current.is_none_or(|current| {
        candidate_rank < ordered_match_rank(current, expected_block, preferred_file_id, blocks)
    })
}

fn log_file_scan(
    path: &Path,
    kind: BlockLocationKind,
    stats: FileScanStats,
    blocks_confirmed: u32,
    elapsed: Duration,
) {
    debug!(
        path = %path.display(),
        ?kind,
        scan_mode = stats.mode.as_str(),
        bytes_scanned = stats.bytes_scanned,
        windows_stepped = stats.windows_stepped,
        jumps_taken = stats.jumps_taken,
        max_consecutive_steps = stats.max_consecutive_steps,
        blocks_confirmed,
        elapsed_ms = elapsed.as_millis(),
        "completed par2 file scan"
    );

    if stats.max_consecutive_steps >= SCANNER_SLOW_WARN_STEPS
        || elapsed >= SCANNER_SLOW_WARN_DURATION
    {
        warn!(
            path = %path.display(),
            ?kind,
            scan_mode = stats.mode.as_str(),
            bytes_scanned = stats.bytes_scanned,
            windows_stepped = stats.windows_stepped,
            jumps_taken = stats.jumps_taken,
            max_consecutive_steps = stats.max_consecutive_steps,
            blocks_confirmed,
            elapsed_ms = elapsed.as_millis(),
            "slow par2 file scan"
        );
    }
}

fn scan_buffered_windows(
    scanner: &RollingBlockScanner<'_>,
    buffer: &[u8],
    base_offset: usize,
    next_unscanned_offset: &mut usize,
    path: &Path,
    kind: BlockLocationKind,
    blocks: &mut [SourceBlock],
) -> u64 {
    let slice_size = scanner.table.slice_size as usize;
    if slice_size == 0 || buffer.len() < slice_size {
        return 0;
    }

    let last_local_offset = buffer.len() - slice_size;
    let mut local_offset = next_unscanned_offset.saturating_sub(base_offset);
    if local_offset > last_local_offset {
        return 0;
    }

    let scanner_batch_lanes = scanner_md5_batch_lanes(slice_size);
    let mut pending = Vec::with_capacity(scanner_batch_lanes);
    let mut crc = checksum::crc32(&buffer[local_offset..local_offset + slice_size]);
    let mut steps = 0u64;

    loop {
        if let Some(candidates) = scanner.table.by_crc.get(&crc) {
            for block_index in candidates {
                let block = &blocks[*block_index];
                if block.expected_len != scanner.table.slice_size {
                    continue;
                }
                let data = &buffer[local_offset..local_offset + slice_size];
                let absolute_offset = (base_offset + local_offset) as u64;
                if scanner_batch_lanes < 2 {
                    record_matching_md5_block(
                        blocks,
                        *block_index,
                        data,
                        path,
                        absolute_offset,
                        block.expected_len,
                        kind,
                    );
                    continue;
                }
                pending.push(PendingMd5Check {
                    block_index: *block_index,
                    data,
                    offset: absolute_offset,
                    len: block.expected_len,
                    kind,
                });
                if pending.len() == scanner_batch_lanes {
                    flush_pending_md5_checks(&mut pending, blocks, path);
                }
            }
        }

        if local_offset == last_local_offset {
            break;
        }

        crc = crc_slide_char(
            crc,
            buffer[local_offset + slice_size],
            buffer[local_offset],
            &scanner.window_table,
        );
        local_offset += 1;
        steps += 1;
    }

    flush_pending_md5_checks(&mut pending, blocks, path);
    *next_unscanned_offset = base_offset + last_local_offset + 1;
    steps
}

fn scan_short_blocks_from_file(
    table: &VerificationHashTable,
    path: &Path,
    kind: BlockLocationKind,
    files: &[SourceFileEntry],
    file_index_by_id: &HashMap<FileId, usize>,
    blocks: &mut [SourceBlock],
    len: usize,
) -> Result<()> {
    let max_tail_len = table
        .short_blocks
        .iter()
        .filter_map(|block_index| {
            let block = &blocks[*block_index];
            let short_len = block.expected_len as usize;
            (block.location.is_none() && short_len > 0 && short_len <= len).then_some(short_len)
        })
        .max()
        .unwrap_or(0);

    let tail = if max_tail_len > 0 {
        read_exact_file_range(path, (len - max_tail_len) as u64, max_tail_len)?
    } else {
        Vec::new()
    };

    for block_index in &table.short_blocks {
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
            let offset = block.local_index as u64 * table.slice_size;
            if offset <= usize::MAX as u64 {
                let offset = offset as usize;
                if offset.checked_add(short_len).is_some_and(|end| end <= len) {
                    let data = read_exact_file_range(path, offset as u64, short_len)?;
                    if short_block_matches(&data, table.slice_size, block) {
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
        }

        let tail_offset = len - short_len;
        let tail_start = tail.len() - short_len;
        if short_block_matches(&tail[tail_start..], table.slice_size, block) {
            record_block_location(
                blocks,
                *block_index,
                BlockLocation {
                    path: path.to_path_buf(),
                    offset: tail_offset as u64,
                    len: block.expected_len,
                    kind,
                },
            );
        }
    }

    Ok(())
}

fn read_exact_file_range(path: &Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0u8; len];
    file.read_exact(&mut data)?;
    Ok(data)
}

fn scanner_uses_mmap_fallback(slice_size: u64) -> bool {
    slice_size > SCANNER_MMAP_FALLBACK_SLICE_BYTES as u64
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

fn scanner_md5_batch_lanes(slice_size: usize) -> usize {
    if slice_size == 0 {
        return 1;
    }
    (SCANNER_MD5_BATCH_MEMORY_BYTES / slice_size).clamp(1, SCANNER_MD5_BATCH_LANES)
}

fn record_matching_md5_block(
    blocks: &mut [SourceBlock],
    block_index: usize,
    data: &[u8],
    path: &Path,
    offset: u64,
    len: u64,
    kind: BlockLocationKind,
) {
    let md5 = checksum::md5(data);
    if blocks[block_index].checksum.md5 == md5 {
        record_block_location(
            blocks,
            block_index,
            BlockLocation {
                path: path.to_path_buf(),
                offset,
                len,
                kind,
            },
        );
    }
}

fn flush_pending_md5_checks(
    pending: &mut Vec<PendingMd5Check<'_>>,
    blocks: &mut [SourceBlock],
    path: &Path,
) {
    if pending.is_empty() {
        return;
    }

    let inputs = pending.iter().map(|check| check.data).collect::<Vec<_>>();
    let md5s = md5_simd::md5_multi(&inputs, None);
    for (check, md5) in pending.iter().zip(md5s) {
        if blocks[check.block_index].checksum.md5 == md5 {
            record_block_location(
                blocks,
                check.block_index,
                BlockLocation {
                    path: path.to_path_buf(),
                    offset: check.offset,
                    len: check.len,
                    kind: check.kind,
                },
            );
        }
    }
    pending.clear();
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
    let mut hasher = Md5State::new();
    let mut buf = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize())
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
    if usize::try_from(pad_to).is_ok_and(|pad_to| pad_to <= SCANNER_MD5_BATCH_MEMORY_BYTES) {
        md5_simd::md5_multi(&[data], Some(pad_to))[0]
    } else {
        let mut hasher = Md5State::new();
        hasher.update(data);
        update_md5_zeros(&mut hasher, pad_to.saturating_sub(data.len() as u64));
        hasher.finalize()
    }
}

fn update_crc_zeros(hasher: &mut crc32fast::Hasher, mut len: u64) {
    while len > 0 {
        let take = len.min(ZERO_PAD_CHUNK.len() as u64) as usize;
        hasher.update(&ZERO_PAD_CHUNK[..take]);
        len -= take as u64;
    }
}

fn update_md5_zeros(hasher: &mut Md5State, mut len: u64) {
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
    use std::path::{Path, PathBuf};

    use crate::checksum::SliceChecksumState;
    use crate::types::RecoverySetId;
    use tempfile::tempdir;

    #[cfg(feature = "slow-tests")]
    use std::ffi::OsStr;

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

    #[cfg(feature = "slow-tests")]
    fn crate_fixture_dir(name: &str) -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let crate_fixture = manifest_dir.join("tests/fixtures").join(name);
        if crate_fixture.is_dir() {
            return crate_fixture;
        }

        panic!(
            "missing slow-test fixture {name}; looked in {}",
            crate_fixture.display()
        );
    }

    #[cfg(feature = "slow-tests")]
    fn copy_dir_contents(src: &Path, dst: &Path) {
        for entry in fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                fs::create_dir_all(&dst_path).unwrap();
                copy_dir_contents(&src_path, &dst_path);
            } else {
                fs::copy(&src_path, &dst_path).unwrap();
            }
        }
    }

    #[cfg(feature = "slow-tests")]
    fn copy_fixture_dir(name: &str) -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        copy_dir_contents(&crate_fixture_dir(name), dir.path());
        dir
    }

    #[cfg(feature = "slow-tests")]
    fn collect_paths(dir: &Path, prefix: &str, extension: &str) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| {
                path.extension() == Some(OsStr::new(extension))
                    && path
                        .file_name()
                        .and_then(OsStr::to_str)
                        .is_some_and(|name| name.starts_with(prefix))
            })
            .collect();
        paths.sort();
        paths
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

    fn block_location_summary(
        blocks: &[SourceBlock],
    ) -> BlockLocationSummary {
        blocks
            .iter()
            .map(|block| {
                block.location.as_ref().map(|location| {
                    (
                        location.path.clone(),
                        location.offset,
                        location.len,
                        location.kind,
                    )
                })
            })
            .collect()
    }

            type BlockLocationSummaryEntry = (PathBuf, u64, u64, BlockLocationKind);
            type BlockLocationSummary = Vec<Option<BlockLocationSummaryEntry>>;

    fn scan_with_mmap(
        state: &RepairState,
        path: &Path,
        kind: BlockLocationKind,
    ) -> BlockLocationSummary {
        let scanner = RollingBlockScanner::new(&state.hash_table, state.set.slice_size);
        let mut blocks = state.blocks.clone();
        scanner
            .scan_file_mmap(
                path,
                kind,
                &state.files,
                &state.file_index_by_id,
                &mut blocks,
            )
            .unwrap();
        block_location_summary(&blocks)
    }

    fn scan_with_mmap_stats(
        state: &RepairState,
        path: &Path,
        kind: BlockLocationKind,
    ) -> (BlockLocationSummary, FileScanStats) {
        let scanner = RollingBlockScanner::new(&state.hash_table, state.set.slice_size);
        let mut blocks = state.blocks.clone();
        let stats = scanner
            .scan_file_mmap(
                path,
                kind,
                &state.files,
                &state.file_index_by_id,
                &mut blocks,
            )
            .unwrap();
        (block_location_summary(&blocks), stats)
    }

    fn scan_with_ordered_canonical(
        state: &RepairState,
        path: &Path,
    ) -> (BlockLocationSummary, FileScanStats) {
        let scanner = RollingBlockScanner::new(&state.hash_table, state.set.slice_size);
        let mut blocks = state.blocks.clone();
        let target = state
            .files
            .iter()
            .find(|file| file.safe_path == path)
            .unwrap();
        let stats = scanner
            .scan_file_ordered_canonical(
                path,
                BlockLocationKind::Canonical,
                &state.files,
                &state.file_index_by_id,
                target,
                &mut blocks,
            )
            .unwrap();
        (block_location_summary(&blocks), stats)
    }

    fn scan_with_buffered(
        state: &RepairState,
        path: &Path,
        kind: BlockLocationKind,
        read_target: usize,
    ) -> Vec<Option<(PathBuf, u64, u64, BlockLocationKind)>> {
        let scanner = RollingBlockScanner::new(&state.hash_table, state.set.slice_size);
        let mut blocks = state.blocks.clone();
        scanner
            .scan_file_buffered_with_target(
                path,
                kind,
                &state.files,
                &state.file_index_by_id,
                &mut blocks,
                read_target,
            )
            .unwrap();
        block_location_summary(&blocks)
    }

    #[test]
    fn buffered_scan_matches_mmap_for_intact_full_blocks() {
        let dir = tempdir().unwrap();
        let target: Vec<u8> = (0..256u32).map(|value| (value % 251) as u8).collect();
        let set = synthetic_set(&[("target.bin", &target)], 64);
        let candidate = dir.path().join("candidate.bin");
        fs::write(&candidate, &target).unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let mmap = scan_with_mmap(&state, &candidate, BlockLocationKind::Extra);
        let buffered = scan_with_buffered(&state, &candidate, BlockLocationKind::Extra, 96);

        assert_eq!(buffered, mmap);
    }

    #[test]
    fn buffered_scan_matches_mmap_for_damaged_partial_matches() {
        let dir = tempdir().unwrap();
        let target = b"aaaabbbbccccdddd".to_vec();
        let set = synthetic_set(&[("target.bin", &target)], 4);
        let candidate = dir.path().join("partial.bin");
        fs::write(&candidate, b"xxxxbbbbzzzzdddd").unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let mmap = scan_with_mmap(&state, &candidate, BlockLocationKind::Extra);
        let buffered = scan_with_buffered(&state, &candidate, BlockLocationKind::Extra, 7);

        assert_eq!(buffered, mmap);
    }

    #[test]
    fn ordered_canonical_scan_matches_generic_locations_for_shifted_damage() {
        let dir = tempdir().unwrap();
        let slice_size = 64u64;
        let mut target = Vec::new();
        let mut blocks = Vec::new();
        for block in 0..6u8 {
            let bytes = (0..slice_size as usize)
                .map(|index| block.wrapping_mul(37).wrapping_add(index as u8))
                .collect::<Vec<_>>();
            target.extend_from_slice(&bytes);
            blocks.push(bytes);
        }
        let set = synthetic_set(&[("target.bin", &target)], slice_size);
        let candidate = dir.path().join("target.bin");
        let mut damaged = Vec::new();
        damaged.extend_from_slice(&blocks[0]);
        damaged.extend_from_slice(&blocks[1]);
        damaged.extend_from_slice(&blocks[3]);
        damaged.extend_from_slice(&blocks[4]);
        damaged.extend_from_slice(&blocks[5]);
        fs::write(&candidate, damaged).unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let (generic_locations, generic_stats) =
            scan_with_mmap_stats(&state, &candidate, BlockLocationKind::Canonical);
        let (ordered_locations, ordered_stats) = scan_with_ordered_canonical(&state, &candidate);

        assert_eq!(ordered_locations, generic_locations);
        assert!(ordered_stats.jumps_taken >= 3);
        assert!(ordered_stats.windows_stepped < generic_stats.windows_stepped);
    }

    #[test]
    fn ordered_canonical_scan_preserves_mixed_block_harvesting() {
        let dir = tempdir().unwrap();
        let alpha = b"aaaabbbbccccdddd".to_vec();
        let beta = b"1111222233334444".to_vec();
        let set = synthetic_set(&[("alpha.bin", &alpha), ("beta.bin", &beta)], 4);
        let candidate = dir.path().join("alpha.bin");
        fs::write(&candidate, b"aaaa2222ccccxxxx").unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let (generic_locations, _) =
            scan_with_mmap_stats(&state, &candidate, BlockLocationKind::Canonical);
        let (ordered_locations, ordered_stats) = scan_with_ordered_canonical(&state, &candidate);

        assert_eq!(ordered_locations, generic_locations);
        assert_eq!(
            ordered_locations[5],
            Some((candidate.clone(), 4, 4, BlockLocationKind::Canonical))
        );
        assert!(ordered_stats.jumps_taken >= 1);
    }

    #[test]
    fn ordered_canonical_scan_ignores_already_used_duplicate_block_when_jumping() {
        let dir = tempdir().unwrap();
        let target = b"aaaabbbbaaaacccc".to_vec();
        let set = synthetic_set(&[("target.bin", &target)], 4);
        let candidate = dir.path().join("target.bin");
        fs::write(&candidate, b"aaaaaaaacccc").unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let (ordered_locations, ordered_stats) = scan_with_ordered_canonical(&state, &candidate);

        assert_eq!(
            ordered_locations[0],
            Some((candidate.clone(), 0, 4, BlockLocationKind::Canonical))
        );
        assert_eq!(
            ordered_locations[2],
            Some((candidate.clone(), 4, 4, BlockLocationKind::Canonical))
        );
        assert!(ordered_stats.jumps_taken >= 2);
    }

    #[test]
    fn ordered_canonical_scan_skips_through_long_miss_runs() {
        let dir = tempdir().unwrap();
        let slice_size = 1024u64;
        let block = |seed: u8| {
            (0..slice_size as usize)
                .map(|index| seed.wrapping_add(index as u8))
                .collect::<Vec<_>>()
        };
        let first = block(3);
        let second = block(71);
        let third = block(129);
        let target = [first.as_slice(), second.as_slice(), third.as_slice()].concat();
        let set = synthetic_set(&[("target.bin", &target)], slice_size);
        let candidate = dir.path().join("target.bin");
        let mut damaged = Vec::new();
        damaged.extend_from_slice(&first);
        damaged.extend(std::iter::repeat_n(0xEE, slice_size as usize * 3));
        damaged.extend_from_slice(&second);
        damaged.extend_from_slice(&third);
        fs::write(&candidate, damaged).unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let (generic_locations, generic_stats) =
            scan_with_mmap_stats(&state, &candidate, BlockLocationKind::Canonical);
        let (ordered_locations, ordered_stats) = scan_with_ordered_canonical(&state, &candidate);

        assert_eq!(ordered_locations, generic_locations);
        assert!(ordered_stats.jumps_taken > 3);
        assert!(ordered_stats.windows_stepped < generic_stats.windows_stepped / 2);
    }

    #[test]
    fn buffered_scan_finds_block_across_refill_overlap() {
        let dir = tempdir().unwrap();
        let target: Vec<u8> = (0..64u32)
            .map(|value| (value as u8).wrapping_mul(5).wrapping_add(9))
            .collect();
        let set = synthetic_set(&[("target.bin", &target)], 64);
        let mut candidate_data = vec![0xAA; 150];
        candidate_data.extend_from_slice(&target);
        candidate_data.extend_from_slice(&[0x55; 37]);
        let candidate = dir.path().join("cross-boundary.bin");
        fs::write(&candidate, candidate_data).unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();
        let scanner = RollingBlockScanner::new(&state.hash_table, state.set.slice_size);
        let mut blocks = state.blocks.clone();

        scanner
            .scan_file_buffered_with_target(
                &candidate,
                BlockLocationKind::Extra,
                &state.files,
                &state.file_index_by_id,
                &mut blocks,
                80,
            )
            .unwrap();

        let location = blocks[0].location.as_ref().unwrap();
        assert_eq!(location.path, candidate);
        assert_eq!(location.offset, 150);
    }

    #[test]
    fn buffered_short_block_checks_match_mmap() {
        let dir = tempdir().unwrap();
        let data = b"ABCDEFGH12345".to_vec();
        let set = synthetic_set(&[("target.bin", &data)], 8);
        let candidate = dir.path().join("target.bin");
        fs::write(&candidate, b"ABCDEFGH12345JUNK").unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();

        let mmap = scan_with_mmap(&state, &candidate, BlockLocationKind::Canonical);
        let buffered = scan_with_buffered(&state, &candidate, BlockLocationKind::Canonical, 8);

        assert_eq!(buffered, mmap);
    }

    #[test]
    fn large_slice_scanner_uses_mmap_fallback_and_remains_correct() {
        let dir = tempdir().unwrap();
        let slice_size = (SCANNER_MMAP_FALLBACK_SLICE_BYTES + 1) as u64;
        let data = (0..slice_size as usize)
            .map(|index| (index as u8).wrapping_mul(31).wrapping_add(1))
            .collect::<Vec<_>>();
        let set = synthetic_set(&[("large.bin", &data)], slice_size);
        let candidate = dir.path().join("large.bin");
        fs::write(&candidate, &data).unwrap();
        let state = RepairState::from_set(dir.path(), set).unwrap();
        let scanner = RollingBlockScanner::new(&state.hash_table, state.set.slice_size);
        let mut blocks = state.blocks.clone();

        assert!(scanner_uses_mmap_fallback(slice_size));
        scanner
            .scan_file(
                &candidate,
                BlockLocationKind::Canonical,
                &state.files,
                &state.file_index_by_id,
                &mut blocks,
            )
            .unwrap();

        assert!(blocks.iter().all(|block| block.location.is_some()));
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

        state.install_repaired_files(&repair).unwrap();
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
    fn copy_only_repair_assembles_mixed_target_from_extra_blocks() {
        let dir = tempdir().unwrap();
        let target = b"aaaabbbbccccdddd".to_vec();
        let set = synthetic_set(&[("target.bin", &target)], 4);
        fs::write(dir.path().join("target.bin"), b"aaaaxxxxccccyyyy").unwrap();
        fs::write(dir.path().join("extra.bin"), b"zzzzbbbbqqqqdddd").unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 0);
        assert!(!state.files_are_canonical_complete());
        assert!(matches!(
            verification.repairable,
            Repairability::Repairable {
                blocks_needed: 0,
                ..
            }
        ));

        let repair = state.repair(&options, &verification).unwrap();
        state.install_repaired_files(&repair).unwrap();
        assert_eq!(fs::read(dir.path().join("target.bin")).unwrap(), target);
    }

    #[test]
    fn copy_only_repair_corrects_swapped_complete_files() {
        let dir = tempdir().unwrap();
        let alpha = b"alpha---alpha---".to_vec();
        let beta = b"beta----beta----".to_vec();
        let set = synthetic_set(&[("alpha.bin", &alpha), ("beta.bin", &beta)], 8);
        fs::write(dir.path().join("alpha.bin"), &beta).unwrap();
        fs::write(dir.path().join("beta.bin"), &alpha).unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 0);
        assert_eq!(
            state
                .files
                .iter()
                .filter(|file| file.complete_location.is_some())
                .count(),
            2
        );
        assert!(!state.files_are_canonical_complete());

        let repair = state.repair(&options, &verification).unwrap();
        state.install_repaired_files(&repair).unwrap();
        assert_eq!(fs::read(dir.path().join("alpha.bin")).unwrap(), alpha);
        assert_eq!(fs::read(dir.path().join("beta.bin")).unwrap(), beta);
    }

    #[test]
    fn duplicate_basenames_in_different_directories_stay_distinct() {
        let dir = tempdir().unwrap();
        let first = b"first---payload".to_vec();
        let second = b"second--payload".to_vec();
        let set = synthetic_set(
            &[
                ("season1/episode.mkv", &first),
                ("season2/episode.mkv", &second),
            ],
            8,
        );
        fs::create_dir_all(dir.path().join("season1")).unwrap();
        fs::create_dir_all(dir.path().join("season2")).unwrap();
        fs::write(dir.path().join("season1/episode.mkv"), &first).unwrap();
        fs::write(dir.path().join("season2/episode.mkv"), &second).unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();
        let verification = state.verification_result();

        assert_eq!(verification.total_missing_blocks, 0);
        assert!(state.files_are_canonical_complete());
        assert!(matches!(verification.repairable, Repairability::NotNeeded));
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
    fn recoverable_file_with_invalid_ifsc_stays_visible_but_unrepairable() {
        let dir = tempdir().unwrap();
        let data = b"aaaabbbb".to_vec();
        let mut set = synthetic_set(&[("target.bin", &data)], 4);
        let file_id = set.recovery_file_ids[0];
        set.slice_checksums.get_mut(&file_id).unwrap().pop();

        let state = RepairState::from_set(dir.path(), set).unwrap();
        let verification = state.verification_result();

        assert_eq!(state.inconsistent_packets, 1);
        assert_eq!(state.files[0].block_count, 0);
        assert_eq!(verification.total_missing_blocks, 2);
        assert!(matches!(
            verification.files.first().map(|file| &file.status),
            Some(FileStatus::Missing)
        ));
        assert!(matches!(
            verification.repairable,
            Repairability::Insufficient { .. }
        ));
    }

    #[test]
    fn recoverable_file_without_description_is_discarded_like_turbo() {
        let dir = tempdir().unwrap();
        let data = b"aaaabbbb".to_vec();
        let mut set = synthetic_set(&[("target.bin", &data)], 4);
        let file_id = set.recovery_file_ids[0];
        set.files.remove(&file_id);
        set.slice_checksums.remove(&file_id);

        let state = RepairState::from_set(dir.path(), set).unwrap();
        let verification = state.verification_result();

        assert_eq!(state.inconsistent_packets, 1);
        assert_eq!(state.discarded_recoverable_files, 1);
        assert!(state.files.is_empty());
        assert_eq!(verification.files.len(), 0);
        assert_eq!(verification.total_missing_blocks, 0);
        assert!(matches!(
            verification.repairable,
            Repairability::Insufficient { .. }
        ));
        assert!(!state.files_are_canonical_complete());
    }

    #[test]
    fn recovery_packet_with_wrong_size_is_discarded_from_capacity() {
        let dir = tempdir().unwrap();
        let data = b"aaaabbbb".to_vec();
        let mut set = synthetic_set(&[("target.bin", &data)], 4);
        set.recovery_slices.insert(
            0,
            crate::par2_set::RecoverySlice {
                exponent: 0,
                data: crate::packet::recovery::RecoverySliceData::InMemory(
                    bytes::Bytes::from_static(b"bad"),
                ),
            },
        );

        let state = RepairState::from_set(dir.path(), set).unwrap();

        assert_eq!(state.discarded_recovery_blocks, 1);
        assert_eq!(state.set.recovery_block_count(), 0);
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
        state.install_repaired_files(&repair).unwrap();

        assert_eq!(fs::read(dir.path().join("target.bin")).unwrap(), data);
    }

    #[test]
    fn short_block_scan_matches_canonical_offset_with_trailing_garbage() {
        let dir = tempdir().unwrap();
        let data = b"ABCDEFGH12345".to_vec();
        let set = synthetic_set(&[("target.bin", &data)], 8);
        fs::write(dir.path().join("target.bin"), b"ABCDEFGH12345JUNK").unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();

        let file = state
            .files
            .iter()
            .find(|file| file.safe_name == "target.bin")
            .unwrap();
        let last_block = &state.blocks[file.first_block + file.block_count - 1];
        let location = last_block.location.as_ref().unwrap();
        assert_eq!(location.path, dir.path().join("target.bin"));
        assert_eq!(location.offset, 8);
    }

    #[test]
    fn short_block_scan_only_matches_true_file_tail_for_extra_files() {
        let dir = tempdir().unwrap();
        let data = b"ABCDEFGH12345".to_vec();
        let set = synthetic_set(&[("target.bin", &data)], 8);
        fs::write(dir.path().join("interior.bin"), b"xxxx12345yyyy").unwrap();
        fs::write(dir.path().join("tail.bin"), b"zzzz12345").unwrap();

        let mut state = RepairState::from_set(dir.path(), set).unwrap();
        let options = Par2RepairerOptions::new(dir.path().to_path_buf(), Vec::new());
        state.scan(&options).unwrap();

        let file = state
            .files
            .iter()
            .find(|file| file.safe_name == "target.bin")
            .unwrap();
        let last_block = &state.blocks[file.first_block + file.block_count - 1];
        let location = last_block.location.as_ref().unwrap();
        assert_eq!(location.path, dir.path().join("tail.bin"));
        assert_eq!(location.offset, 4);
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

    #[test]
    fn inventory_counts_duplicate_packets_without_changing_first_wins() {
        let dir = tempdir().unwrap();
        let main_body = {
            let mut body = Vec::new();
            body.extend_from_slice(&4u64.to_le_bytes());
            body.extend_from_slice(&0u32.to_le_bytes());
            body
        };
        let active_set_id = checksum::md5(&main_body);
        let main_packet =
            make_full_packet(crate::packet::header::TYPE_MAIN, &main_body, active_set_id);
        let mut par2_file = Vec::new();
        par2_file.extend_from_slice(&main_packet);
        par2_file.extend_from_slice(&main_packet);
        fs::write(dir.path().join("active.par2"), par2_file).unwrap();

        let repairer = Par2Repairer::new(Par2RepairerOptions::new(
            dir.path().to_path_buf(),
            vec![dir.path().join("active.par2")],
        ));
        let inventory = repairer.load_inventory().unwrap();

        assert_eq!(inventory.diagnostics.packets_loaded, 2);
        assert_eq!(inventory.diagnostics.duplicate_packets, 1);
        assert_eq!(inventory.set.recovery_file_ids.len(), 0);
    }

    #[cfg(feature = "slow-tests")]
    #[test]
    fn crate_fixture_missing_volume_repairs_and_reverifies_clean() {
        let temp = copy_fixture_dir("rar5_lz_plain");
        fs::remove_file(temp.path().join("fixture_rar5_lz_plain.part4.rar")).unwrap();

        let par2_paths = collect_paths(temp.path(), "fixture_rar5_lz_plain_repair", "par2");
        let mut preview = Par2RepairerOptions::new(temp.path().to_path_buf(), par2_paths.clone());
        preview.repair = false;
        let preview_outcome = Par2Repairer::new(preview).verify_or_repair().unwrap();
        assert_eq!(preview_outcome.status, Par2RepairStatus::RepairPossible);
        assert!(preview_outcome.verification.total_missing_blocks > 0);

        let outcome = Par2Repairer::new(Par2RepairerOptions::new(
            temp.path().to_path_buf(),
            par2_paths.clone(),
        ))
        .verify_or_repair()
        .unwrap();

        assert_eq!(outcome.status, Par2RepairStatus::Repaired);
        assert_eq!(outcome.verification.total_missing_blocks, 0);

        let mut reverify = Par2RepairerOptions::new(temp.path().to_path_buf(), par2_paths);
        reverify.repair = false;
        let clean = Par2Repairer::new(reverify).verify_or_repair().unwrap();
        assert_eq!(clean.status, Par2RepairStatus::Verified, "{clean:#?}");
        assert_eq!(clean.verification.total_missing_blocks, 0, "{clean:#?}");
    }
}
