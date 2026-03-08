//! Streaming verification session for incremental PAR2 verification during download.
//!
//! [`VerificationSession`] tracks verification state as data arrives, allowing
//! the scheduler to query file status and repairability at any time without
//! waiting for the full download to complete.

use std::collections::HashMap;
use std::sync::Arc;

use crate::checksum::{FileHashState, SliceChecksumState};
use crate::packet::Packet;
use crate::par2_set::Par2FileSet;
use crate::types::{FileId, SliceChecksum};
use crate::verify::{FileStatus, FileVerification, Repairability, VerificationResult};

/// Per-file verification state tracking.
struct FileVerificationState {
    /// Per-slice streaming checksum accumulators. `None` means the slice
    /// has not received any data yet.
    slice_states: Vec<Option<SliceChecksumState>>,
    /// Per-slice verification result. `None` means not yet finalized,
    /// `Some(true)` = valid, `Some(false)` = damaged.
    verified_slices: Vec<Option<bool>>,
    /// Streaming full-file MD5 hash (only valid if data arrives in order;
    /// tracked for completeness but not relied on for out-of-order feeds).
    #[allow(dead_code)]
    full_hash: FileHashState,
    /// Streaming hash for the first 16KB of the file.
    hash_16k: SliceChecksumState,
    /// Total bytes received for this file.
    bytes_received: u64,
    /// Expected file length from the PAR2 file description.
    file_length: u64,
    /// PAR2 slice size.
    slice_size: u64,
}

impl FileVerificationState {
    fn new(file_length: u64, slice_size: u64) -> Self {
        let num_slices = if file_length == 0 {
            0
        } else {
            file_length.div_ceil(slice_size) as usize
        };

        Self {
            slice_states: vec![None; num_slices],
            verified_slices: vec![None; num_slices],
            full_hash: FileHashState::new(),
            hash_16k: SliceChecksumState::new(),
            bytes_received: 0,
            file_length,
            slice_size,
        }
    }

    /// Feed data for a single slice. The offset must be slice-aligned and the
    /// data must cover exactly one slice (possibly shorter for the last slice).
    fn feed_slice_data(&mut self, offset: u64, data: &[u8]) {
        if self.slice_size == 0 {
            return;
        }

        let slice_index = (offset / self.slice_size) as usize;
        if slice_index >= self.slice_states.len() {
            return;
        }

        // Initialize accumulator if this is the first data for this slice.
        let state = self.slice_states[slice_index]
            .get_or_insert_with(SliceChecksumState::new);
        state.update(data);
        self.bytes_received += data.len() as u64;

        // Feed into the 16k hash if this data falls within the first 16KB.
        if offset < 16384 {
            let end = (offset + data.len() as u64).min(16384);
            let useful = &data[..(end - offset) as usize];
            self.hash_16k.update(useful);
        }
    }

    /// Try to finalize a slice if all its data has been received.
    fn try_finalize_slice(
        &mut self,
        slice_index: usize,
        expected: &SliceChecksum,
    ) -> Option<bool> {
        // Already verified?
        if let Some(result) = self.verified_slices[slice_index] {
            return Some(result);
        }

        let state = self.slice_states[slice_index].take()?;
        let expected_slice_len = if slice_index == self.slice_states.len() - 1 {
            // Last slice may be shorter.
            let remainder = self.file_length % self.slice_size;
            if remainder == 0 { self.slice_size } else { remainder }
        } else {
            self.slice_size
        };

        // Only finalize if we've received all data for this slice.
        if state.bytes_fed() < expected_slice_len {
            // Put the state back -- not complete yet.
            self.slice_states[slice_index] = Some(state);
            return None;
        }

        let pad_to = if state.bytes_fed() < self.slice_size {
            Some(self.slice_size)
        } else {
            None
        };
        let (crc, md5) = state.finalize(pad_to);
        let valid = crc == expected.crc32 && md5 == expected.md5;
        self.verified_slices[slice_index] = Some(valid);
        Some(valid)
    }

    /// Count how many slices have been verified as valid.
    fn verified_count(&self) -> usize {
        self.verified_slices.iter().filter(|v| **v == Some(true)).count()
    }

    /// Count how many slices have been verified as damaged.
    fn damaged_count(&self) -> usize {
        self.verified_slices.iter().filter(|v| **v == Some(false)).count()
    }

    /// Count how many slices are still pending (not yet finalized).
    fn pending_count(&self) -> usize {
        self.verified_slices.iter().filter(|v| v.is_none()).count()
    }

    /// Total number of slices.
    fn total_slices(&self) -> usize {
        self.verified_slices.len()
    }
}

/// Streaming verification session that tracks PAR2 verification state as data
/// arrives during download.
///
/// Usage:
/// 1. Call [`add_par2_data`] when PAR2 metadata packets arrive.
/// 2. Call [`feed_data`] as decoded file data arrives (slice-aligned).
/// 3. Query [`file_status`], [`repairability`], or [`is_complete`] at any time.
/// 4. Call [`verification_result`] once all data has been fed.
pub struct VerificationSession {
    par2_set: Option<Arc<Par2FileSet>>,
    file_states: HashMap<FileId, FileVerificationState>,
    /// Packets received before the PAR2 set was complete. These are buffered
    /// so that `add_par2_data` can be called incrementally.
    buffered_packets: Vec<Packet>,
}

impl VerificationSession {
    /// Create a new empty verification session.
    pub fn new() -> Self {
        Self {
            par2_set: None,
            file_states: HashMap::new(),
            buffered_packets: Vec::new(),
        }
    }

    /// Called when PAR2 metadata arrives. May be called multiple times as
    /// packets from different .par2 volumes arrive.
    ///
    /// Once a valid `Par2FileSet` can be built from the accumulated packets,
    /// per-file verification state is initialized.
    pub fn add_par2_data(&mut self, packets: &[Packet]) {
        self.buffered_packets.extend(packets.iter().cloned());

        // Try to build a Par2FileSet from all accumulated packets.
        match Par2FileSet::from_packets(self.buffered_packets.clone()) {
            Ok(set) => {
                let set = Arc::new(set);
                // Initialize file states for any files we haven't seen yet.
                for file_id in &set.recovery_file_ids {
                    if !self.file_states.contains_key(file_id)
                        && let Some(desc) = set.file_description(file_id) {
                            self.file_states.insert(
                                *file_id,
                                FileVerificationState::new(desc.length, set.slice_size),
                            );
                        }
                }
                self.par2_set = Some(set);
            }
            Err(_) => {
                // Not enough packets yet (e.g., no main packet). Keep buffering.
            }
        }
    }

    /// Feed decoded file data for a specific file.
    ///
    /// The data must be slice-aligned: `offset` must be a multiple of the PAR2
    /// slice size, and `data` should contain exactly one slice worth of data
    /// (or less for the last slice of a file). The assembly layer is responsible
    /// for aligning segments to slices before calling this.
    ///
    /// If PAR2 metadata has not yet arrived, the data is silently ignored.
    /// (The assembly layer should re-feed data after PAR2 metadata arrives
    /// if needed, or the caller can handle this at a higher level.)
    pub fn feed_data(&mut self, file_id: &FileId, offset: u64, data: &[u8]) {
        let par2_set = match &self.par2_set {
            Some(set) => Arc::clone(set),
            None => return, // No PAR2 metadata yet; ignore data.
        };

        // Ensure file state exists.
        if !self.file_states.contains_key(file_id) {
            if let Some(desc) = par2_set.file_description(file_id) {
                self.file_states.insert(
                    *file_id,
                    FileVerificationState::new(desc.length, par2_set.slice_size),
                );
            } else {
                return; // Unknown file ID.
            }
        }

        let state = self.file_states.get_mut(file_id).unwrap();
        state.feed_slice_data(offset, data);

        // Try to finalize the slice that was just fed.
        if par2_set.slice_size > 0 {
            let slice_index = (offset / par2_set.slice_size) as usize;
            if let Some(checksums) = par2_set.file_checksums(file_id)
                && slice_index < checksums.len() {
                    state.try_finalize_slice(slice_index, &checksums[slice_index]);
                }
        }
    }

    /// Query the current status of a specific file.
    pub fn file_status(&self, file_id: &FileId) -> Option<FileStatus> {
        let state = self.file_states.get(file_id)?;

        if state.total_slices() == 0 {
            return Some(FileStatus::Complete);
        }

        // If all slices are verified as valid, the file is complete.
        if state.verified_count() == state.total_slices() {
            return Some(FileStatus::Complete);
        }

        let damaged = state.damaged_count() as u32;
        if damaged > 0 {
            return Some(FileStatus::Damaged(damaged));
        }

        // Still pending -- report as complete only if everything known is valid.
        if state.pending_count() == state.total_slices() {
            // No data received yet.
            return Some(FileStatus::Missing);
        }

        // Partially verified, no damage found yet.
        // Report damaged with 0 to indicate "in progress" -- but the pending
        // slices could still be bad. Return Missing for files with no verified
        // slices, or Damaged(0) for partially verified.
        if state.bytes_received == 0 {
            Some(FileStatus::Missing)
        } else {
            Some(FileStatus::Damaged(0))
        }
    }

    /// Estimate repairability given the current state.
    ///
    /// This accounts for both verified-damaged slices and pending (unverified)
    /// slices, treating pending slices optimistically (assuming they will pass).
    pub fn repairability(&self) -> Repairability {
        let par2_set = match &self.par2_set {
            Some(set) => set,
            None => return Repairability::NotNeeded,
        };

        let mut total_damaged: u32 = 0;
        let mut total_missing: u32 = 0;

        for file_id in &par2_set.recovery_file_ids {
            if let Some(state) = self.file_states.get(file_id) {
                total_damaged += state.damaged_count() as u32;
                // Files that haven't received any data are considered missing.
                if state.bytes_received == 0 {
                    total_missing += state.total_slices() as u32;
                }
            } else {
                // File state not initialized yet; treat as missing.
                if let Some(desc) = par2_set.file_description(file_id) {
                    total_missing += par2_set.slice_count_for_file(desc.length);
                }
            }
        }

        let blocks_needed = total_damaged + total_missing;
        let blocks_available = par2_set.recovery_block_count();

        if blocks_needed == 0 {
            Repairability::NotNeeded
        } else if blocks_needed <= blocks_available {
            Repairability::Repairable {
                blocks_needed,
                blocks_available,
            }
        } else {
            Repairability::Insufficient {
                blocks_needed,
                blocks_available,
                deficit: blocks_needed - blocks_available,
            }
        }
    }

    /// Check if all files have been verified successfully.
    pub fn is_complete(&self) -> bool {
        let par2_set = match &self.par2_set {
            Some(set) => set,
            None => return false,
        };

        for file_id in &par2_set.recovery_file_ids {
            match self.file_states.get(file_id) {
                Some(state) => {
                    if state.verified_count() != state.total_slices() {
                        return false;
                    }
                }
                None => return false,
            }
        }

        true
    }

    /// Produce a full [`VerificationResult`] from the current session state.
    ///
    /// This can be passed to [`plan_repair`](crate::repair::plan_repair) if
    /// repair is needed. Returns `None` if PAR2 metadata has not been loaded.
    pub fn verification_result(&self) -> Option<VerificationResult> {
        let par2_set = self.par2_set.as_ref()?;

        let mut files = Vec::new();
        let mut total_missing_blocks = 0u32;

        for file_id in &par2_set.recovery_file_ids {
            let desc = match par2_set.file_description(file_id) {
                Some(d) => d,
                None => continue,
            };

            let state = match self.file_states.get(file_id) {
                Some(s) => s,
                None => {
                    // No state at all: file is missing.
                    let slice_count = par2_set.slice_count_for_file(desc.length);
                    total_missing_blocks += slice_count;
                    files.push(FileVerification {
                        file_id: *file_id,
                        filename: desc.filename.clone(),
                        status: FileStatus::Missing,
                        valid_slices: vec![false; slice_count as usize],
                        missing_slice_count: slice_count,
                    });
                    continue;
                }
            };

            let valid_slices: Vec<bool> = state
                .verified_slices
                .iter()
                .map(|v| v.unwrap_or(false))
                .collect();
            let missing_count = valid_slices.iter().filter(|&&v| !v).count() as u32;
            total_missing_blocks += missing_count;

            let status = if missing_count == 0 {
                FileStatus::Complete
            } else if state.bytes_received == 0 {
                FileStatus::Missing
            } else {
                FileStatus::Damaged(missing_count)
            };

            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status,
                valid_slices,
                missing_slice_count: missing_count,
            });
        }

        let recovery_blocks_available = par2_set.recovery_block_count();
        let repairable = if total_missing_blocks == 0 {
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

        Some(VerificationResult {
            files,
            recovery_blocks_available,
            total_missing_blocks,
            repairable,
        })
    }

    /// Verify a file's slices using pre-computed CRC32 values from download.
    ///
    /// Each entry in `slice_crcs` must be the CRC32 of the (possibly zero-padded)
    /// slice data. This is a CRC-only check (no MD5); it avoids re-reading files
    /// from disk when the download layer already computed per-slice CRCs.
    ///
    /// Returns per-slice validity, or `None` if PAR2 metadata isn't loaded or
    /// the file is unknown.
    pub fn verify_from_slice_crcs(
        &mut self,
        file_id: &FileId,
        slice_crcs: &[u32],
    ) -> Option<Vec<bool>> {
        let par2_set = self.par2_set.as_ref()?;
        let checksums = par2_set.file_checksums(file_id)?;

        // Ensure file state exists.
        if !self.file_states.contains_key(file_id) {
            if let Some(desc) = par2_set.file_description(file_id) {
                self.file_states.insert(
                    *file_id,
                    FileVerificationState::new(desc.length, par2_set.slice_size),
                );
            } else {
                return None;
            }
        }

        let state = self.file_states.get_mut(file_id)?;

        let results: Vec<bool> = checksums
            .iter()
            .enumerate()
            .map(|(i, expected)| {
                let valid = slice_crcs.get(i).map(|&crc| crc == expected.crc32).unwrap_or(false);
                // Update the verified_slices state (only if not already verified).
                if state.verified_slices[i].is_none() {
                    state.verified_slices[i] = Some(valid);
                    if valid {
                        // Mark bytes as received so file isn't treated as "missing".
                        state.bytes_received = state.bytes_received.max(1);
                    }
                }
                valid
            })
            .collect();

        Some(results)
    }

    /// Get a reference to the underlying PAR2 file set, if loaded.
    pub fn par2_set(&self) -> Option<&Arc<Par2FileSet>> {
        self.par2_set.as_ref()
    }
}

impl Default for VerificationSession {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum;
    use crate::packet::header;
    use crate::par2_set::RecoverySlice;
    use crate::types::SliceChecksum;
    use bytes::Bytes;
    use md5::{Digest, Md5};

    /// Helper to build a complete valid packet (header + body).
    fn make_full_packet(
        packet_type: &[u8; 16],
        body: &[u8],
        recovery_set_id: [u8; 16],
    ) -> Vec<u8> {
        let length = (header::HEADER_SIZE + body.len()) as u64;
        let mut hash_input = Vec::new();
        hash_input.extend_from_slice(&recovery_set_id);
        hash_input.extend_from_slice(packet_type);
        hash_input.extend_from_slice(body);
        let packet_hash: [u8; 16] = Md5::digest(&hash_input).into();

        let mut data = Vec::new();
        data.extend_from_slice(header::MAGIC);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(&packet_hash);
        data.extend_from_slice(&recovery_set_id);
        data.extend_from_slice(packet_type);
        data.extend_from_slice(body);
        data
    }

    /// Build PAR2 packets for a single file. Returns (packets_bytes, file_id, rsid).
    fn build_par2_packets(
        file_data: &[u8],
        slice_size: u64,
    ) -> (Vec<u8>, FileId, [u8; 16]) {
        let file_length = file_data.len() as u64;
        let hash_full = checksum::md5(file_data);
        let hash_16k_data = &file_data[..file_data.len().min(16384)];
        let hash_16k = checksum::md5(hash_16k_data);

        let filename = b"testfile.dat";
        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&file_length.to_le_bytes());
        id_input.extend_from_slice(filename);
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
        let file_id = FileId::from_bytes(file_id_bytes);

        let num_slices = if file_length == 0 {
            0
        } else {
            ((file_length + slice_size - 1) / slice_size) as usize
        };

        let mut checksums = Vec::new();
        for i in 0..num_slices {
            let offset = i as u64 * slice_size;
            let end = ((offset + slice_size) as usize).min(file_data.len());
            let slice_data = &file_data[offset as usize..end];
            let mut state = SliceChecksumState::new();
            state.update(slice_data);
            let pad_to = if (slice_data.len() as u64) < slice_size {
                Some(slice_size)
            } else {
                None
            };
            let (crc, md5) = state.finalize(pad_to);
            checksums.push(SliceChecksum { crc32: crc, md5 });
        }

        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&1u32.to_le_bytes());
        main_body.extend_from_slice(&file_id_bytes);
        let rsid: [u8; 16] = Md5::digest(&main_body).into();

        let mut fd_body = Vec::new();
        fd_body.extend_from_slice(&file_id_bytes);
        fd_body.extend_from_slice(&hash_full);
        fd_body.extend_from_slice(&hash_16k);
        fd_body.extend_from_slice(&file_length.to_le_bytes());
        fd_body.extend_from_slice(filename);
        while fd_body.len() % 4 != 0 {
            fd_body.push(0);
        }

        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&file_id_bytes);
        for cs in &checksums {
            ifsc_body.extend_from_slice(&cs.crc32.to_le_bytes());
            ifsc_body.extend_from_slice(&cs.md5);
        }

        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, &fd_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, &ifsc_body, rsid));

        (stream, file_id, rsid)
    }

    fn parse_packets(data: &[u8]) -> Vec<Packet> {
        crate::packet::scan_packets(data, 0)
            .into_iter()
            .map(|(p, _)| p)
            .collect()
    }

    #[test]
    fn session_feed_correct_data_all_pass() {
        let slice_size = 1024u64;
        let file_data: Vec<u8> = (0..2048u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();

        // Add PAR2 metadata first.
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);
        assert!(session.par2_set().is_some());

        // Feed data slice by slice.
        session.feed_data(&file_id, 0, &file_data[0..1024]);
        session.feed_data(&file_id, 1024, &file_data[1024..2048]);

        // All slices should pass.
        assert!(session.is_complete());
        assert!(matches!(session.file_status(&file_id), Some(FileStatus::Complete)));
        assert!(matches!(session.repairability(), Repairability::NotNeeded));

        // verification_result should show all valid.
        let result = session.verification_result().unwrap();
        assert_eq!(result.total_missing_blocks, 0);
        assert!(result.files[0].valid_slices.iter().all(|&v| v));
    }

    #[test]
    fn session_detects_corrupted_slice() {
        let slice_size = 1024u64;
        let file_data: Vec<u8> = (0..2048u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        // Feed correct first slice.
        session.feed_data(&file_id, 0, &file_data[0..1024]);

        // Feed corrupted second slice.
        let mut corrupted = file_data[1024..2048].to_vec();
        corrupted[0] ^= 0xFF;
        session.feed_data(&file_id, 1024, &corrupted);

        // Should not be complete.
        assert!(!session.is_complete());

        // File should be damaged.
        assert!(matches!(session.file_status(&file_id), Some(FileStatus::Damaged(1))));

        let result = session.verification_result().unwrap();
        assert_eq!(result.total_missing_blocks, 1);
        assert!(result.files[0].valid_slices[0]); // first slice valid
        assert!(!result.files[0].valid_slices[1]); // second slice damaged
    }

    #[test]
    fn session_handles_data_before_metadata() {
        let slice_size = 1024u64;
        let file_data: Vec<u8> = (0..2048u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();

        // Feed data before PAR2 metadata -- should be silently ignored.
        session.feed_data(&file_id, 0, &file_data[0..1024]);
        session.feed_data(&file_id, 1024, &file_data[1024..2048]);

        // No PAR2 set yet.
        assert!(session.par2_set().is_none());
        assert!(!session.is_complete());
        assert!(session.verification_result().is_none());

        // Now add metadata.
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        // Data was ignored, so nothing is verified yet.
        assert!(!session.is_complete());

        // Re-feed data after metadata.
        session.feed_data(&file_id, 0, &file_data[0..1024]);
        session.feed_data(&file_id, 1024, &file_data[1024..2048]);

        assert!(session.is_complete());
    }

    #[test]
    fn session_repairability_mid_stream() {
        let slice_size = 1024u64;
        let file_data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        // Before any data: all 4 slices are "missing" (no bytes received).
        match session.repairability() {
            Repairability::Insufficient { blocks_needed: 4, .. } => {}
            other => panic!("expected Insufficient with 4 blocks needed, got {other:?}"),
        }

        // Feed first slice correctly.
        session.feed_data(&file_id, 0, &file_data[0..1024]);

        // Now 3 slices haven't received data = missing.
        // But the repairability only counts files with 0 bytes as "missing".
        // After feeding 1 slice, bytes_received > 0, so no longer "missing" --
        // only actually damaged slices count.

        // Feed corrupted second slice.
        let mut corrupted = file_data[1024..2048].to_vec();
        corrupted[0] ^= 0xFF;
        session.feed_data(&file_id, 1024, &corrupted);

        // 1 damaged, file has data so not counted as fully missing.
        match session.repairability() {
            Repairability::Insufficient { blocks_needed: 1, blocks_available: 0, .. } => {}
            other => panic!("expected Insufficient with 1 block needed, got {other:?}"),
        }
    }

    #[test]
    fn session_integration_with_repair_plan() {
        use crate::gf;
        use crate::repair::plan_repair;

        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..256u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        // Add recovery blocks to the par2_set.
        {
            let set = Arc::make_mut(session.par2_set.as_mut().unwrap());
            let num_slices = 4; // 256 / 64
            let constants = gf::input_slice_constants(num_slices);
            let ss = slice_size as usize;
            let word_count = ss / 2;

            let mut padded = file_data.clone();
            padded.resize(num_slices * ss, 0);

            for r in 0..2u32 {
                let mut recovery = vec![0u8; ss];
                for (i, &constant) in constants.iter().enumerate() {
                    let factor = gf::pow(constant, r);
                    for w in 0..word_count {
                        let input_word = u16::from_le_bytes([
                            padded[i * ss + w * 2],
                            padded[i * ss + w * 2 + 1],
                        ]);
                        let contribution = gf::mul(input_word, factor);
                        let rec_word =
                            u16::from_le_bytes([recovery[w * 2], recovery[w * 2 + 1]]);
                        let new_val = gf::add(rec_word, contribution);
                        let bytes = new_val.to_le_bytes();
                        recovery[w * 2] = bytes[0];
                        recovery[w * 2 + 1] = bytes[1];
                    }
                }
                set.recovery_slices.insert(
                    r,
                    RecoverySlice {
                        exponent: r,
                        data: Bytes::from(recovery),
                    },
                );
            }
        }

        // Feed correct slices 0, 1, 3; corrupt slice 2.
        session.feed_data(&file_id, 0, &file_data[0..64]);
        session.feed_data(&file_id, 64, &file_data[64..128]);

        let mut corrupted = file_data[128..192].to_vec();
        corrupted[0] ^= 0xFF;
        session.feed_data(&file_id, 128, &corrupted);

        session.feed_data(&file_id, 192, &file_data[192..256]);

        // Get verification result and pass to plan_repair.
        let result = session.verification_result().unwrap();
        assert_eq!(result.total_missing_blocks, 1);

        let plan = plan_repair(session.par2_set().unwrap(), &result).unwrap();
        assert_eq!(plan.missing_slices.len(), 1);
        assert_eq!(plan.missing_slices[0], (file_id, 2));
    }

    #[test]
    fn session_partial_last_slice() {
        let slice_size = 1024u64;
        // File is 1500 bytes -> 2 slices, last is 476 bytes.
        let file_data: Vec<u8> = (0..1500u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        session.feed_data(&file_id, 0, &file_data[0..1024]);
        session.feed_data(&file_id, 1024, &file_data[1024..1500]);

        assert!(session.is_complete());
        let result = session.verification_result().unwrap();
        assert_eq!(result.total_missing_blocks, 0);
    }

    #[test]
    fn session_verify_from_slice_crcs() {
        let slice_size = 1024u64;
        let file_data: Vec<u8> = (0..2048u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        // Compute per-slice CRC32s (these would come from yEnc decode in practice).
        let crc0 = checksum::crc32(&file_data[0..1024]);
        let crc1 = checksum::crc32(&file_data[1024..2048]);

        let result = session.verify_from_slice_crcs(&file_id, &[crc0, crc1]).unwrap();
        assert_eq!(result, vec![true, true]);

        // Session should now show file as complete.
        assert!(session.is_complete());
        assert!(matches!(session.file_status(&file_id), Some(FileStatus::Complete)));
    }

    #[test]
    fn session_verify_from_slice_crcs_partial_damage() {
        let slice_size = 1024u64;
        let file_data: Vec<u8> = (0..2048u32).map(|i| (i % 256) as u8).collect();
        let (par2_bytes, file_id, _rsid) = build_par2_packets(&file_data, slice_size);

        let mut session = VerificationSession::new();
        let packets = parse_packets(&par2_bytes);
        session.add_par2_data(&packets);

        let crc0 = checksum::crc32(&file_data[0..1024]);
        let wrong_crc = 0xDEADBEEF;

        let result = session.verify_from_slice_crcs(&file_id, &[crc0, wrong_crc]).unwrap();
        assert_eq!(result, vec![true, false]);

        assert!(!session.is_complete());
        assert!(matches!(session.file_status(&file_id), Some(FileStatus::Damaged(1))));
    }

    #[test]
    fn session_empty_before_metadata() {
        let session = VerificationSession::new();
        assert!(!session.is_complete());
        assert!(session.verification_result().is_none());
        assert!(matches!(session.repairability(), Repairability::NotNeeded));
    }
}
