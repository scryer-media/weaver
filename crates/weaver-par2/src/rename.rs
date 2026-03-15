//! Obfuscated filename recovery and split-file detection.
//!
//! Usenet files are frequently uploaded with randomized filenames to evade
//! takedowns. PAR2 file descriptions contain the original filename and a 16KB
//! MD5 hash, which can be used to identify and rename obfuscated files.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tracing::debug;

use crate::checksum;
use crate::packet::header::{self, MAGIC, PacketHeader};
use crate::par2_set::Par2FileSet;
use crate::types::{FileId, RecoverySetId};

/// A suggestion to rename a file to its correct name.
#[derive(Debug, Clone)]
pub struct RenameSuggestion {
    /// Current path on disk.
    pub current_path: PathBuf,
    /// The correct filename from PAR2 metadata.
    pub correct_name: String,
    /// The file ID this matched against.
    pub file_id: FileId,
    /// How the match was determined.
    pub match_type: MatchType,
}

/// How an obfuscated file was identified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    /// Matched via MD5 hash of the first 16KB.
    Hash16k,
    /// Identified as a PAR2 file by its packet header recovery set ID.
    Par2File,
}

/// Scan a directory and match files against PAR2 file descriptions.
///
/// Reads the first 16KB of each file, computes its MD5 hash, and compares
/// against the `hash_16k` values from the PAR2 set. Returns rename suggestions
/// for files whose names don't match the PAR2 metadata.
///
/// Does **not** perform any actual renames — the caller decides what to do.
pub fn scan_for_renames(dir: &Path, par2_set: &Par2FileSet) -> io::Result<Vec<RenameSuggestion>> {
    // Build lookup: hash_16k -> (FileId, correct_filename)
    let mut hash_lookup: HashMap<[u8; 16], (FileId, &str)> = HashMap::new();
    for (file_id, desc) in &par2_set.files {
        hash_lookup.insert(desc.hash_16k, (*file_id, &desc.filename));
    }

    // Set of filenames already correctly named (so we don't suggest renaming
    // a file that already has the right name).
    let known_filenames: std::collections::HashSet<&str> = par2_set
        .files
        .values()
        .map(|d| d.filename.as_str())
        .collect();

    let mut suggestions = Vec::new();

    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        // Skip files that already have a known correct name.
        if known_filenames.contains(file_name) {
            continue;
        }

        // Read first 16KB and compute MD5.
        let data = read_first_n_bytes(&path, 16384)?;
        if data.is_empty() {
            continue;
        }
        let hash = checksum::md5(&data);

        if let Some(&(file_id, correct_name)) = hash_lookup.get(&hash)
            && file_name != correct_name
        {
            debug!(
                "rename match: {} -> {} (16k hash match)",
                file_name, correct_name
            );
            suggestions.push(RenameSuggestion {
                current_path: path,
                correct_name: correct_name.to_string(),
                file_id,
                match_type: MatchType::Hash16k,
            });
        }
    }

    Ok(suggestions)
}

/// Identify PAR2 files in a directory that belong to a specific recovery set.
///
/// Reads the first 64 bytes of each file to check for PAR2 magic and extract
/// the recovery set ID. Returns paths of files that match `expected_set_id`
/// but may have obfuscated names.
pub fn identify_par2_files(
    dir: &Path,
    expected_set_id: &RecoverySetId,
) -> io::Result<Vec<PathBuf>> {
    let mut matches = Vec::new();

    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let data = read_first_n_bytes(&path, header::HEADER_SIZE)?;
        if data.len() < header::HEADER_SIZE {
            continue;
        }

        // Check for PAR2 magic.
        if &data[0..8] != MAGIC {
            continue;
        }

        // Try to parse the header.
        if let Ok(hdr) = PacketHeader::parse(&data, 0)
            && hdr.recovery_set_id == *expected_set_id
        {
            debug!("identified par2 file: {}", path.display());
            matches.push(path);
        }
    }

    Ok(matches)
}

/// A group of split files that together form one logical file.
#[derive(Debug, Clone)]
pub struct SplitFileGroup {
    /// The base name without the numeric extension (e.g., "movie.mkv" for "movie.mkv.001").
    pub base_name: String,
    /// Paths of the split parts, sorted by part number.
    pub parts: Vec<PathBuf>,
    /// The part numbers found (e.g., [1, 2, 3]).
    pub part_numbers: Vec<u32>,
    /// Whether the sequence is contiguous starting from 1 (no gaps).
    pub contiguous: bool,
}

/// Detect split files (`.001`, `.002`, etc.) in a directory.
///
/// Groups files by base name and returns groups with 2 or more parts.
pub fn detect_split_files(dir: &Path) -> io::Result<Vec<SplitFileGroup>> {
    let mut groups: HashMap<String, Vec<(u32, PathBuf)>> = HashMap::new();

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

        // Check if the extension is purely numeric.
        if let Some((base, ext)) = file_name.rsplit_once('.')
            && !ext.is_empty()
            && ext.chars().all(|c| c.is_ascii_digit())
            && let Ok(num) = ext.parse::<u32>()
        {
            groups
                .entry(base.to_string())
                .or_default()
                .push((num, path));
        }
    }

    let mut result = Vec::new();
    for (base_name, mut parts) in groups {
        if parts.len() < 2 {
            continue;
        }
        parts.sort_by_key(|(n, _)| *n);

        let part_numbers: Vec<u32> = parts.iter().map(|(n, _)| *n).collect();
        let paths: Vec<PathBuf> = parts.into_iter().map(|(_, p)| p).collect();

        // Check contiguity: starts at 1 and no gaps.
        let contiguous =
            part_numbers[0] == 1 && part_numbers.array_windows().all(|&[a, b]| b == a + 1);

        result.push(SplitFileGroup {
            base_name,
            parts: paths,
            part_numbers,
            contiguous,
        });
    }

    // Sort by base name for deterministic output.
    result.sort_by(|a, b| a.base_name.cmp(&b.base_name));
    Ok(result)
}

/// Read up to `n` bytes from the start of a file.
fn read_first_n_bytes(path: &Path, n: usize) -> io::Result<Vec<u8>> {
    use std::io::Read;
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

    /// Build a Par2FileSet for a single file with known content.
    fn setup_par2_set(file_data: &[u8], slice_size: u64, filename: &str) -> (Par2FileSet, FileId) {
        let file_length = file_data.len() as u64;
        let hash_full = checksum::md5(file_data);
        let hash_16k_data = &file_data[..file_data.len().min(16384)];
        let hash_16k = checksum::md5(hash_16k_data);

        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&file_length.to_le_bytes());
        id_input.extend_from_slice(filename.as_bytes());
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
        let file_id = FileId::from_bytes(file_id_bytes);

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
        fd_body.extend_from_slice(filename.as_bytes());
        while fd_body.len() % 4 != 0 {
            fd_body.push(0);
        }

        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&file_id_bytes);
        for cs in &checksums {
            ifsc_body.extend_from_slice(&cs.md5);
            ifsc_body.extend_from_slice(&cs.crc32.to_le_bytes());
        }

        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, &fd_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, &ifsc_body, rsid));

        let set = Par2FileSet::from_files(&[&stream]).unwrap();
        (set, file_id)
    }

    #[test]
    fn scan_finds_obfuscated_file() {
        let dir = TempDir::new().unwrap();
        let file_data = b"This is the real file content for rename testing!!";
        let correct_name = "movie.rar";

        let (par2_set, file_id) = setup_par2_set(file_data, 1024, correct_name);

        // Write file with obfuscated name
        fs::write(dir.path().join("abc123def456.bin"), file_data).unwrap();

        let suggestions = scan_for_renames(dir.path(), &par2_set).unwrap();
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].correct_name, correct_name);
        assert_eq!(suggestions[0].file_id, file_id);
        assert_eq!(suggestions[0].match_type, MatchType::Hash16k);
    }

    #[test]
    fn scan_skips_correctly_named() {
        let dir = TempDir::new().unwrap();
        let file_data = b"This is the real file content for rename testing!!";
        let correct_name = "movie.rar";

        let (par2_set, _) = setup_par2_set(file_data, 1024, correct_name);

        // Write file with correct name
        fs::write(dir.path().join(correct_name), file_data).unwrap();

        let suggestions = scan_for_renames(dir.path(), &par2_set).unwrap();
        assert!(suggestions.is_empty());
    }

    #[test]
    fn scan_empty_dir() {
        let dir = TempDir::new().unwrap();
        let file_data = b"data";
        let (par2_set, _) = setup_par2_set(file_data, 1024, "test.bin");

        let suggestions = scan_for_renames(dir.path(), &par2_set).unwrap();
        assert!(suggestions.is_empty());
    }

    #[test]
    fn identify_par2_files_finds_match() {
        let dir = TempDir::new().unwrap();

        // Build a minimal PAR2 packet
        let rsid = [0x42u8; 16];
        let main_body_data = vec![0u8; 12]; // dummy main body
        let packet_data = make_full_packet(header::TYPE_MAIN, &main_body_data, rsid);

        // Write with obfuscated name
        fs::write(dir.path().join("random_name.bin"), &packet_data).unwrap();

        let expected_id = RecoverySetId::from_bytes(rsid);
        let matches = identify_par2_files(dir.path(), &expected_id).unwrap();
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn identify_par2_files_ignores_non_par2() {
        let dir = TempDir::new().unwrap();

        fs::write(dir.path().join("regular.txt"), b"not a par2 file").unwrap();

        let expected_id = RecoverySetId::from_bytes([0x42; 16]);
        let matches = identify_par2_files(dir.path(), &expected_id).unwrap();
        assert!(matches.is_empty());
    }

    #[test]
    fn detect_split_files_basic() {
        let dir = TempDir::new().unwrap();

        fs::write(dir.path().join("movie.mkv.001"), b"part1").unwrap();
        fs::write(dir.path().join("movie.mkv.002"), b"part2").unwrap();
        fs::write(dir.path().join("movie.mkv.003"), b"part3").unwrap();
        fs::write(dir.path().join("other.txt"), b"not split").unwrap();

        let groups = detect_split_files(dir.path()).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].base_name, "movie.mkv");
        assert_eq!(groups[0].part_numbers, vec![1, 2, 3]);
        assert!(groups[0].contiguous);
    }

    #[test]
    fn detect_split_files_with_gap() {
        let dir = TempDir::new().unwrap();

        fs::write(dir.path().join("data.bin.001"), b"p1").unwrap();
        fs::write(dir.path().join("data.bin.003"), b"p3").unwrap();

        let groups = detect_split_files(dir.path()).unwrap();
        assert_eq!(groups.len(), 1);
        assert!(!groups[0].contiguous);
        assert_eq!(groups[0].part_numbers, vec![1, 3]);
    }

    #[test]
    fn detect_split_files_single_part_ignored() {
        let dir = TempDir::new().unwrap();

        fs::write(dir.path().join("lonely.bin.001"), b"alone").unwrap();

        let groups = detect_split_files(dir.path()).unwrap();
        assert!(groups.is_empty());
    }

    #[test]
    fn detect_split_files_empty_dir() {
        let dir = TempDir::new().unwrap();
        let groups = detect_split_files(dir.path()).unwrap();
        assert!(groups.is_empty());
    }
}
