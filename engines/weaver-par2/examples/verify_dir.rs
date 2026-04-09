//! Quick diagnostic: load PAR2 files from a directory, remap obfuscated names, verify.

use std::collections::HashMap;
use std::path::PathBuf;
use weaver_model::files::FileRole;
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
        // Remap by volume number (same logic as pipeline/metadata.rs).
        let mut disk_by_volume: HashMap<u32, String> = HashMap::new();
        for name in &rar_files {
            let role = FileRole::from_filename(name);
            if let FileRole::RarVolume { volume_number } = role {
                disk_by_volume.insert(volume_number, name.clone());
            }
        }

        let mut remapped = 0u32;
        for desc in par2_set.files.values_mut() {
            let role = FileRole::from_filename(&desc.filename);
            if let FileRole::RarVolume { volume_number } = role {
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
