//! Quick diagnostic: load PAR2 files from a directory, remap obfuscated names, verify.

use std::collections::HashMap;
use std::path::PathBuf;
use weaver_par2::{DiskFileAccess, Par2FileSet, verify_all};

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: verify_dir <directory>");
    let dir = PathBuf::from(dir);

    // Collect all .par2 files.
    let mut par2_paths: Vec<PathBuf> = Vec::new();
    let mut rar_files: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".par2") {
            par2_paths.push(entry.path());
        } else if name.ends_with(".rar") {
            rar_files.push(name);
        }
    }
    par2_paths.sort();
    rar_files.sort();

    println!(
        "Found {} PAR2 files, {} RAR files",
        par2_paths.len(),
        rar_files.len()
    );

    // Parse all PAR2 files directly from disk so recovery slices stay file-backed.
    let mut par2_set = Par2FileSet::from_paths(&par2_paths).unwrap();

    println!(
        "\nPAR2 set: {} files, slice_size={}, recovery_blocks={}",
        par2_set.files.len(),
        par2_set.slice_size,
        par2_set.recovery_block_count()
    );

    // Show PAR2 internal filenames.
    println!("\n--- PAR2 internal filenames ---");
    for desc in par2_set.files.values() {
        println!("  {}", desc.filename);
    }

    // Check if names match disk.
    let has_matches = par2_set
        .files
        .values()
        .any(|desc| rar_files.contains(&desc.filename));
    println!("\nFilenames match disk: {}", has_matches);

    if !has_matches {
        // Remap by RAR volume number so obfuscated names can line up with PAR2 entries.
        let mut disk_by_volume: HashMap<u32, String> = HashMap::new();
        for name in &rar_files {
            if let Some(volume_number) = rar_volume_number(name) {
                disk_by_volume.insert(volume_number, name.clone());
            }
        }

        let mut remapped = 0u32;
        for desc in par2_set.files.values_mut() {
            if let Some(volume_number) = rar_volume_number(&desc.filename) {
                if let Some(real_name) = disk_by_volume.get(&volume_number) {
                    println!(
                        "  remap vol {}: {} -> {}",
                        volume_number, desc.filename, real_name
                    );
                    desc.filename = real_name.clone();
                    remapped += 1;
                } else {
                    println!(
                        "  remap vol {}: {} -> NO MATCH ON DISK",
                        volume_number, desc.filename
                    );
                }
            }
        }
        println!("\nRemapped {}/{} files", remapped, par2_set.files.len());
    }

    // Verify.
    println!("\n--- Running verify_all ---");
    let access = DiskFileAccess::new(dir.clone(), &par2_set);
    let result = verify_all(&par2_set, &access);

    println!("\nResults:");
    println!("  total_missing_blocks: {}", result.total_missing_blocks);
    println!(
        "  recovery_blocks_available: {}",
        result.recovery_blocks_available
    );
    println!("  is_repairable: {:?}", result.repairable);

    let mut missing = 0u32;
    let mut complete = 0u32;
    let mut damaged = 0u32;
    for fv in &result.files {
        match fv.status {
            weaver_par2::verify::FileStatus::Missing => {
                println!(
                    "  MISSING: {} ({} blocks)",
                    fv.filename, fv.missing_slice_count
                );
                missing += 1;
            }
            weaver_par2::verify::FileStatus::Damaged(n) => {
                println!("  DAMAGED: {} ({} bad blocks)", fv.filename, n);
                damaged += 1;
            }
            weaver_par2::verify::FileStatus::Complete => {
                complete += 1;
            }
            weaver_par2::verify::FileStatus::Renamed(ref p) => {
                println!("  RENAMED: {} -> {}", fv.filename, p.display());
            }
        }
    }
    println!(
        "\n  {} complete, {} missing, {} damaged",
        complete, missing, damaged
    );
}

fn rar_volume_number(name: &str) -> Option<u32> {
    let name = name.trim_end_matches('_').to_ascii_lowercase();

    if let Some(stem) = name.strip_suffix(".rar") {
        if let Some((_, part_suffix)) = stem.rsplit_once(".part") {
            let digits_len = part_suffix.bytes().take_while(u8::is_ascii_digit).count();
            let marker_suffix = &part_suffix[digits_len..];
            if digits_len > 0 && (marker_suffix.is_empty() || is_duplicate_marker(marker_suffix)) {
                let volume_number = part_suffix[..digits_len].parse::<u32>().ok()?;
                return volume_number.checked_sub(1);
            }
        }

        return Some(0);
    }

    let (_, ext) = name.rsplit_once('.')?;
    let mut chars = ext.chars();
    let prefix = chars.next()?;
    let digits = chars.as_str();
    if matches!(prefix, 'r' | 's')
        && digits.len() == 2
        && digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return digits.parse::<u32>().ok().map(|volume| volume + 1);
    }

    None
}

fn is_duplicate_marker(suffix: &str) -> bool {
    let Some(digits) = suffix.strip_prefix(".duplicate") else {
        return false;
    };

    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}
