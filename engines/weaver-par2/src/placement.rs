//! Content-placement scan: match on-disk files to PAR2 file descriptions by hash.
//!
//! Usenet NZBs can deliver file data under the wrong filename (swapped volume
//! identities). Standard PAR2 tools detect this by scanning all candidate files
//! with a 16KB hash match followed by full-file MD5 confirmation. This module
//! implements the same approach for Weaver's verify/repair pipeline.
//!
//! Unlike [`crate::rename::scan_for_renames`], this scan checks ALL files in the
//! directory — including those whose names already match a PAR2 file description.
//! This is necessary to detect swaps where both files have valid PAR2 names but
//! each holds the other's data.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read};
use std::path::Path;

use tracing::{debug, warn};

use crate::checksum;
use crate::par2_set::Par2FileSet;
use crate::types::FileId;

/// A planned file move: where a file currently is and where it should go.
#[derive(Debug, Clone)]
pub struct PlacementEntry {
    /// The PAR2 file ID this file's data belongs to.
    pub file_id: FileId,
    /// Current filename on disk.
    pub current_name: String,
    /// Correct filename according to PAR2 metadata.
    pub correct_name: String,
}

/// Result of scanning on-disk files against PAR2 file descriptions.
#[derive(Debug, Clone)]
pub struct PlacementPlan {
    /// Files already at the correct path (hash confirmed).
    pub exact: Vec<FileId>,
    /// Pairs that need swapping (both exist, each has the other's data).
    pub swaps: Vec<(PlacementEntry, PlacementEntry)>,
    /// Files needing a simple rename to their correct PAR2 name.
    pub renames: Vec<PlacementEntry>,
    /// PAR2 file IDs with no matching on-disk file found.
    pub unresolved: Vec<FileId>,
    /// PAR2 file IDs where multiple on-disk files matched (ambiguous).
    pub conflicts: Vec<FileId>,
}

/// Scan a directory and match files to PAR2 file descriptions by content hash.
///
/// For each candidate file (non-PAR2, non-directory):
/// 1. Read first 16KB and compute MD5; match against PAR2 `hash_16k` values.
/// 2. If a 16KB match is found, confirm with full-file MD5 against `hash_full`.
/// 3. Classify results into exact matches, swaps, renames, unresolved, and conflicts.
///
/// Only files present in the PAR2 recovery set are considered as match targets.
/// Files with `.par2` extensions are skipped (they're recovery data, not input files).
pub fn scan_placement(dir: &Path, par2_set: &Par2FileSet) -> io::Result<PlacementPlan> {
    // Build hash_16k → list of (FileId, correct_name) from PAR2 descriptions.
    // Multiple file IDs could theoretically share a 16k hash (astronomically unlikely
    // but we handle it via the conflicts path).
    let mut hash_lookup: HashMap<[u8; 16], Vec<(FileId, String)>> = HashMap::new();
    for file_id in &par2_set.recovery_file_ids {
        if let Some(desc) = par2_set.file_description(file_id) {
            hash_lookup
                .entry(desc.hash_16k)
                .or_default()
                .push((*file_id, desc.filename.clone()));
        }
    }

    // Also index non-recovery file IDs (they're still PAR2-described files).
    for file_id in &par2_set.non_recovery_file_ids {
        if let Some(desc) = par2_set.file_description(file_id) {
            hash_lookup
                .entry(desc.hash_16k)
                .or_default()
                .push((*file_id, desc.filename.clone()));
        }
    }

    // Scan directory: for each candidate file, match by 16k hash + full MD5.
    // Result: disk_name → (file_id, correct_name) for confirmed matches.
    let mut matches: HashMap<String, (FileId, String)> = HashMap::new();
    // Track file_ids with multiple disk matches (conflicts).
    let mut match_counts: HashMap<FileId, u32> = HashMap::new();

    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        // Skip PAR2 files — they're recovery data, not input files.
        if file_name.to_ascii_lowercase().ends_with(".par2") {
            continue;
        }

        // Skip hidden/temp files from our own operations.
        if file_name.starts_with(".swap.") || file_name.starts_with(".chunk.") {
            continue;
        }

        // Read first 16KB.
        let data_16k = read_first_n_bytes(&path, 16384)?;
        if data_16k.is_empty() {
            continue;
        }
        let hash_16k = checksum::md5(&data_16k);

        // Check for 16k hash match.
        let candidates = match hash_lookup.get(&hash_16k) {
            Some(c) => c,
            None => continue,
        };

        // Try each candidate (usually exactly one).
        for (file_id, correct_name) in candidates {
            let desc = match par2_set.file_description(file_id) {
                Some(d) => d,
                None => continue,
            };

            // Confirm with full-file MD5.
            let full_data = fs::read(&path)?;
            if full_data.len() as u64 != desc.length {
                debug!(
                    file = %file_name,
                    expected_len = desc.length,
                    actual_len = full_data.len(),
                    "16k hash matched but file length differs — skipping"
                );
                continue;
            }

            let full_hash = checksum::md5(&full_data);
            if full_hash != desc.hash_full {
                debug!(
                    file = %file_name,
                    target = %correct_name,
                    "16k hash matched but full MD5 differs — skipping"
                );
                continue;
            }

            // Confirmed match.
            debug!(
                file = %file_name,
                target = %correct_name,
                "confirmed placement match (16k + full MD5)"
            );
            matches.insert(file_name.clone(), (*file_id, correct_name.clone()));
            *match_counts.entry(*file_id).or_default() += 1;
            break; // One match per disk file is enough.
        }
    }

    // Identify conflicts: file_ids matched by more than one disk file.
    let conflict_ids: HashSet<FileId> = match_counts
        .iter()
        .filter(|&(_, count)| *count > 1)
        .map(|(id, _)| *id)
        .collect();

    // Remove conflicting matches.
    matches.retain(|_, (file_id, _)| !conflict_ids.contains(file_id));

    // Build reverse map: file_id → disk_name (for swap detection).
    let mut id_to_disk: HashMap<FileId, String> = HashMap::new();
    for (disk_name, (file_id, _)) in &matches {
        id_to_disk.insert(*file_id, disk_name.clone());
    }

    // Classify matches.
    let mut exact = Vec::new();
    let mut swaps: Vec<(PlacementEntry, PlacementEntry)> = Vec::new();
    let mut renames = Vec::new();
    let mut seen_swap: HashSet<FileId> = HashSet::new();

    // All PAR2 file IDs we need to account for.
    let all_file_ids: Vec<FileId> = par2_set
        .recovery_file_ids
        .iter()
        .chain(par2_set.non_recovery_file_ids.iter())
        .copied()
        .collect();

    for file_id in &all_file_ids {
        if conflict_ids.contains(file_id) {
            continue; // Handled separately.
        }

        let disk_name = match id_to_disk.get(file_id) {
            Some(n) => n,
            None => continue, // Will be classified as unresolved below.
        };

        let correct_name = match par2_set.file_description(file_id) {
            Some(d) => &d.filename,
            None => continue,
        };

        if disk_name == correct_name {
            // File is already at the correct path.
            exact.push(*file_id);
        } else if !seen_swap.contains(file_id) {
            // File is at the wrong path. Check if it's a swap.
            // A swap: disk file at correct_name holds data for file_id's disk_name.
            let other_file_id = matches.get(correct_name.as_str()).map(|(id, _)| *id);

            if let Some(other_id) = other_file_id
                && other_id != *file_id
                && id_to_disk.get(&other_id).is_some_and(|n| n == correct_name)
            {
                // Mutual swap detected.
                let other_correct = par2_set
                    .file_description(&other_id)
                    .map(|d| d.filename.clone())
                    .unwrap_or_default();

                swaps.push((
                    PlacementEntry {
                        file_id: *file_id,
                        current_name: disk_name.clone(),
                        correct_name: correct_name.clone(),
                    },
                    PlacementEntry {
                        file_id: other_id,
                        current_name: correct_name.clone(),
                        correct_name: other_correct,
                    },
                ));
                seen_swap.insert(*file_id);
                seen_swap.insert(other_id);
            } else {
                // Simple rename (obfuscated name → correct name).
                renames.push(PlacementEntry {
                    file_id: *file_id,
                    current_name: disk_name.clone(),
                    correct_name: correct_name.clone(),
                });
            }
        }
    }

    // Unresolved: file IDs with no confirmed match and not conflicting.
    let matched_ids: HashSet<FileId> = id_to_disk.keys().copied().collect();
    let unresolved: Vec<FileId> = all_file_ids
        .iter()
        .filter(|id| !matched_ids.contains(id) && !conflict_ids.contains(id))
        .copied()
        .collect();

    let conflicts: Vec<FileId> = conflict_ids.into_iter().collect();

    Ok(PlacementPlan {
        exact,
        swaps,
        renames,
        unresolved,
        conflicts,
    })
}

/// Execute a placement plan by renaming/swapping files on disk.
///
/// Swaps use a temporary file to avoid data loss. If a swap partially fails,
/// the temporary file is moved back to restore the original state.
///
/// Returns the number of files successfully moved.
pub fn apply_placement_plan(dir: &Path, plan: &PlacementPlan) -> io::Result<u32> {
    let mut moved = 0u32;

    // Execute swaps first (most critical — these are mutual renames).
    for (entry_a, entry_b) in &plan.swaps {
        let path_a = dir.join(&entry_a.current_name);
        let path_b = dir.join(&entry_b.current_name);
        let temp = dir.join(format!(".swap.{}.tmp", &entry_a.current_name));

        // Atomic-ish three-step swap.
        fs::rename(&path_a, &temp).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("swap step 1 failed ({} → temp): {e}", entry_a.current_name),
            )
        })?;

        if let Err(e) = fs::rename(&path_b, &path_a) {
            // Rollback: restore A from temp.
            let _ = fs::rename(&temp, &path_a);
            return Err(io::Error::new(
                e.kind(),
                format!(
                    "swap step 2 failed ({} → {}): {e}",
                    entry_b.current_name, entry_a.current_name
                ),
            ));
        }

        if let Err(e) = fs::rename(&temp, &path_b) {
            // Partial state: A has B's data, temp has A's data. Try to restore.
            warn!(
                "swap step 3 failed (temp → {}): {e} — attempting recovery",
                entry_b.current_name
            );
            // Best-effort: move A back to B, temp back to A.
            let _ = fs::rename(&path_a, &path_b);
            let _ = fs::rename(&temp, &path_a);
            return Err(io::Error::new(
                e.kind(),
                format!("swap step 3 failed (temp → {}): {e}", entry_b.current_name),
            ));
        }

        debug!(
            a = %entry_a.current_name,
            b = %entry_b.current_name,
            "swapped file pair"
        );
        moved += 2;
    }

    // Execute simple renames.
    for entry in &plan.renames {
        let src = dir.join(&entry.current_name);
        let dst = dir.join(&entry.correct_name);

        // Don't overwrite an existing file at the destination.
        if dst.exists() {
            warn!(
                src = %entry.current_name,
                dst = %entry.correct_name,
                "rename target already exists — skipping"
            );
            continue;
        }

        fs::rename(&src, &dst).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "rename failed ({} → {}): {e}",
                    entry.current_name, entry.correct_name
                ),
            )
        })?;

        debug!(
            from = %entry.current_name,
            to = %entry.correct_name,
            "renamed file"
        );
        moved += 1;
    }

    Ok(moved)
}

/// Read up to `n` bytes from the start of a file.
fn read_first_n_bytes(path: &Path, n: usize) -> io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let mut buf = vec![0u8; n];
    let bytes_read = file.read(&mut buf)?;
    buf.truncate(bytes_read);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum::SliceChecksumState;
    use crate::packet::header;
    use crate::types::SliceChecksum;
    use md5::{Digest, Md5};
    use tempfile::TempDir;

    /// Helper to build a complete valid packet (header + body).
    fn make_full_packet(packet_type: &[u8; 16], body: &[u8], recovery_set_id: [u8; 16]) -> Vec<u8> {
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

    /// Build a Par2FileSet with multiple files for placement testing.
    fn setup_par2_set_multi(
        files: &[(&[u8], &str)],
        slice_size: u64,
    ) -> (Par2FileSet, Vec<FileId>) {
        let mut file_ids = Vec::new();
        let mut fd_bodies = Vec::new();
        let mut ifsc_bodies = Vec::new();

        for &(file_data, filename) in files {
            let file_length = file_data.len() as u64;
            let hash_full = checksum::md5(file_data);
            let hash_16k = checksum::md5(&file_data[..file_data.len().min(16384)]);

            let mut id_input = Vec::new();
            id_input.extend_from_slice(&hash_16k);
            id_input.extend_from_slice(&file_length.to_le_bytes());
            id_input.extend_from_slice(filename.as_bytes());
            let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
            file_ids.push(FileId::from_bytes(file_id_bytes));

            let num_slices = if file_length == 0 {
                0
            } else {
                file_length.div_ceil(slice_size) as usize
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

            let mut fd_body = Vec::new();
            fd_body.extend_from_slice(&file_id_bytes);
            fd_body.extend_from_slice(&hash_full);
            fd_body.extend_from_slice(&hash_16k);
            fd_body.extend_from_slice(&file_length.to_le_bytes());
            fd_body.extend_from_slice(filename.as_bytes());
            while fd_body.len() % 4 != 0 {
                fd_body.push(0);
            }
            fd_bodies.push(fd_body);

            let mut ifsc_body = Vec::new();
            ifsc_body.extend_from_slice(&file_id_bytes);
            for cs in &checksums {
                ifsc_body.extend_from_slice(&cs.md5);
                ifsc_body.extend_from_slice(&cs.crc32.to_le_bytes());
            }
            ifsc_bodies.push(ifsc_body);
        }

        let file_id_bytes_list: Vec<[u8; 16]> = file_ids.iter().map(|id| *id.as_bytes()).collect();

        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&(file_ids.len() as u32).to_le_bytes());
        for id_bytes in &file_id_bytes_list {
            main_body.extend_from_slice(id_bytes);
        }
        let rsid: [u8; 16] = Md5::digest(&main_body).into();

        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        for fd_body in &fd_bodies {
            stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, fd_body, rsid));
        }
        for ifsc_body in &ifsc_bodies {
            stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, ifsc_body, rsid));
        }

        let set = Par2FileSet::from_files(&[&stream]).unwrap();
        (set, file_ids)
    }

    #[test]
    fn exact_match_detected() {
        let dir = TempDir::new().unwrap();
        let data_a = b"File A content for placement test!!";
        let data_b = b"File B content for placement test!!";

        let (par2_set, _ids) =
            setup_par2_set_multi(&[(data_a, "file_a.rar"), (data_b, "file_b.rar")], 1024);

        fs::write(dir.path().join("file_a.rar"), data_a).unwrap();
        fs::write(dir.path().join("file_b.rar"), data_b).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert_eq!(plan.exact.len(), 2);
        assert!(plan.swaps.is_empty());
        assert!(plan.renames.is_empty());
        assert!(plan.unresolved.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn swap_detected() {
        let dir = TempDir::new().unwrap();
        let data_a = b"File A content for swap testing!!!!";
        let data_b = b"File B content for swap testing!!!!";

        let (par2_set, ids) =
            setup_par2_set_multi(&[(data_a, "file_a.rar"), (data_b, "file_b.rar")], 1024);

        // Write data at SWAPPED paths.
        fs::write(dir.path().join("file_a.rar"), data_b).unwrap();
        fs::write(dir.path().join("file_b.rar"), data_a).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert!(plan.exact.is_empty());
        assert_eq!(plan.swaps.len(), 1, "should detect one swap pair");
        assert!(plan.renames.is_empty());
        assert!(plan.conflicts.is_empty());

        // Verify the swap entries reference the correct file IDs.
        let (entry_a, entry_b) = &plan.swaps[0];
        let swap_ids: HashSet<FileId> = [entry_a.file_id, entry_b.file_id].into();
        assert!(swap_ids.contains(&ids[0]));
        assert!(swap_ids.contains(&ids[1]));
    }

    #[test]
    fn swap_apply_resolves() {
        let dir = TempDir::new().unwrap();
        let data_a = b"File A content for apply test!!!!!!";
        let data_b = b"File B content for apply test!!!!!!";

        let (par2_set, _ids) =
            setup_par2_set_multi(&[(data_a, "file_a.rar"), (data_b, "file_b.rar")], 1024);

        // Write swapped.
        fs::write(dir.path().join("file_a.rar"), data_b).unwrap();
        fs::write(dir.path().join("file_b.rar"), data_a).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert_eq!(plan.swaps.len(), 1);

        let moved = apply_placement_plan(dir.path(), &plan).unwrap();
        assert_eq!(moved, 2);

        // Verify files are now at correct paths.
        assert_eq!(fs::read(dir.path().join("file_a.rar")).unwrap(), data_a);
        assert_eq!(fs::read(dir.path().join("file_b.rar")).unwrap(), data_b);

        // Re-scan should show exact matches.
        let plan2 = scan_placement(dir.path(), &par2_set).unwrap();
        assert_eq!(plan2.exact.len(), 2);
        assert!(plan2.swaps.is_empty());
    }

    #[test]
    fn obfuscated_rename_detected() {
        let dir = TempDir::new().unwrap();
        let data = b"Content of the file for rename test";

        let (par2_set, _ids) = setup_par2_set_multi(&[(data, "correct_name.rar")], 1024);

        // Write with obfuscated name.
        fs::write(dir.path().join("abc123random.bin"), data).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert!(plan.exact.is_empty());
        assert!(plan.swaps.is_empty());
        assert_eq!(plan.renames.len(), 1);
        assert_eq!(plan.renames[0].current_name, "abc123random.bin");
        assert_eq!(plan.renames[0].correct_name, "correct_name.rar");
    }

    #[test]
    fn mixed_swap_and_obfuscated() {
        let dir = TempDir::new().unwrap();
        let data_a = b"Swap file A data for mixed test!!!!";
        let data_b = b"Swap file B data for mixed test!!!!";
        let data_c = b"Obfuscated file C for mixed test!!!";

        let (par2_set, _ids) = setup_par2_set_multi(
            &[
                (data_a, "vol_a.rar"),
                (data_b, "vol_b.rar"),
                (data_c, "vol_c.rar"),
            ],
            1024,
        );

        // A and B are swapped; C has an obfuscated name.
        fs::write(dir.path().join("vol_a.rar"), data_b).unwrap();
        fs::write(dir.path().join("vol_b.rar"), data_a).unwrap();
        fs::write(dir.path().join("random_xyz.bin"), data_c).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert_eq!(plan.swaps.len(), 1);
        assert_eq!(plan.renames.len(), 1);
        assert_eq!(plan.renames[0].correct_name, "vol_c.rar");
        assert!(plan.exact.is_empty());
    }

    #[test]
    fn full_md5_mismatch_rejected() {
        let dir = TempDir::new().unwrap();
        // Two files with the same first 16KB but different content after that.
        let data_real = vec![0xABu8; 32768];
        let mut data_fake = vec![0xABu8; 32768]; // same first 16k
        data_fake[16384] = 0xFF; // differ after 16k

        let (par2_set, _ids) = setup_par2_set_multi(&[(&data_real, "target.rar")], 1024);

        // Write the fake file at the correct name.
        fs::write(dir.path().join("target.rar"), &data_fake).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        // Should NOT match because full MD5 differs.
        assert!(plan.exact.is_empty());
        assert_eq!(plan.unresolved.len(), 1);
    }

    #[test]
    fn missing_file_is_unresolved() {
        let dir = TempDir::new().unwrap();
        let data = b"Some content that exists in PAR2 set";

        let (par2_set, _ids) = setup_par2_set_multi(&[(data, "missing.rar")], 1024);

        // Don't write any file.
        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert_eq!(plan.unresolved.len(), 1);
        assert!(plan.exact.is_empty());
    }

    #[test]
    fn par2_files_skipped() {
        let dir = TempDir::new().unwrap();
        let data = b"This is actually rar data not par2!";

        let (par2_set, _ids) = setup_par2_set_multi(&[(data, "file.rar")], 1024);

        // Write the data with a .par2 extension — should be skipped.
        fs::write(dir.path().join("file.par2"), data).unwrap();

        let plan = scan_placement(dir.path(), &par2_set).unwrap();
        assert_eq!(plan.unresolved.len(), 1);
        assert!(plan.exact.is_empty());
    }
}
