//! Stress tests for PAR2 repair with moderate-size data.
//!
//! These tests generate synthetic PAR2 sets entirely in-memory and exercise
//! the full verify → plan → repair → re-verify pipeline with enough data to
//! make the hot paths (GF region multiply, matrix inversion, verification
//! hashing) measurable.

#![cfg(feature = "slow-tests")]

use std::time::Instant;

use bytes::Bytes;
use md5::{Digest, Md5};
use weaver_par2::checksum::SliceChecksumState;
use weaver_par2::packet::header;
use weaver_par2::{
    FileAccess, FileId, MemoryFileAccess, RecoverySlice, Repairability, execute_repair, gf_pow,
    input_slice_constants, mul_acc_region, par2_set::Par2FileSet, plan_repair, verify_all,
};

// ---------------------------------------------------------------------------
// Deterministic PRNG (SplitMix64) — no external dependency needed
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i + 8 <= buf.len() {
            let v = self.next_u64();
            buf[i..i + 8].copy_from_slice(&v.to_le_bytes());
            i += 8;
        }
        if i < buf.len() {
            let v = self.next_u64();
            let bytes = v.to_le_bytes();
            for (j, b) in buf[i..].iter_mut().enumerate() {
                *b = bytes[j];
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Synthetic PAR2 builder
// ---------------------------------------------------------------------------

struct SyntheticFile {
    data: Vec<u8>,
    file_id: FileId,
    file_id_bytes: [u8; 16],
    filename: String,
}

struct SyntheticPar2 {
    par2_set: Par2FileSet,
    files: Vec<SyntheticFile>,
}

fn build_synthetic_par2(
    file_sizes: &[usize],
    slice_size: u64,
    num_recovery: usize,
    rng: &mut Rng,
) -> SyntheticPar2 {
    let ss = slice_size as usize;

    // Generate random file data.
    let mut files: Vec<SyntheticFile> = Vec::new();
    for (idx, &size) in file_sizes.iter().enumerate() {
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        let filename = format!("testfile_{idx:03}.dat");

        let hash_16k = weaver_par2::checksum::md5(&data[..data.len().min(16384)]);

        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&(data.len() as u64).to_le_bytes());
        id_input.extend_from_slice(filename.as_bytes());
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
        let file_id = FileId::from_bytes(file_id_bytes);

        files.push(SyntheticFile {
            data,
            file_id,
            file_id_bytes,
            filename,
        });
    }

    // Compute total slices across all files (for global slice indexing).
    let total_slices: usize = files.iter().map(|f| f.data.len().div_ceil(ss)).sum();

    // Build padded input: all files concatenated, each file padded to full slices.
    let mut all_slices: Vec<Vec<u8>> = Vec::with_capacity(total_slices);
    for f in &files {
        let num_slices = f.data.len().div_ceil(ss);
        for i in 0..num_slices {
            let offset = i * ss;
            let end = (offset + ss).min(f.data.len());
            let mut slice_data = vec![0u8; ss];
            slice_data[..end - offset].copy_from_slice(&f.data[offset..end]);
            all_slices.push(slice_data);
        }
    }

    // Build PAR2 packets.
    let rsid: [u8; 16] = {
        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&(files.len() as u32).to_le_bytes());
        for f in &files {
            main_body.extend_from_slice(&f.file_id_bytes);
        }
        Md5::digest(&main_body).into()
    };

    let mut stream = Vec::new();

    // Main packet.
    {
        let mut body = Vec::new();
        body.extend_from_slice(&slice_size.to_le_bytes());
        body.extend_from_slice(&(files.len() as u32).to_le_bytes());
        for f in &files {
            body.extend_from_slice(&f.file_id_bytes);
        }
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &body, rsid));
    }

    // File description + IFSC packets for each file.
    for f in &files {
        let hash_full = weaver_par2::checksum::md5(&f.data);
        let hash_16k = weaver_par2::checksum::md5(&f.data[..f.data.len().min(16384)]);
        let file_length = f.data.len() as u64;
        let num_slices = f.data.len().div_ceil(ss);

        let mut fd_body = Vec::new();
        fd_body.extend_from_slice(&f.file_id_bytes);
        fd_body.extend_from_slice(&hash_full);
        fd_body.extend_from_slice(&hash_16k);
        fd_body.extend_from_slice(&file_length.to_le_bytes());
        fd_body.extend_from_slice(f.filename.as_bytes());
        while fd_body.len() % 4 != 0 {
            fd_body.push(0);
        }

        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&f.file_id_bytes);
        for i in 0..num_slices {
            let offset = i * ss;
            let end = (offset + ss).min(f.data.len());
            let slice_data = &f.data[offset..end];
            let mut state = SliceChecksumState::new();
            state.update(slice_data);
            let pad_to = if (slice_data.len() as u64) < slice_size {
                Some(slice_size)
            } else {
                None
            };
            let (crc, md5) = state.finalize(pad_to);
            ifsc_body.extend_from_slice(&md5);
            ifsc_body.extend_from_slice(&crc.to_le_bytes());
        }

        stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, &fd_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, &ifsc_body, rsid));
    }

    let mut set = Par2FileSet::from_files(&[&stream]).unwrap();

    // Generate recovery blocks using SIMD-accelerated mul_acc_region.
    let constants = input_slice_constants(total_slices);
    let t0 = Instant::now();

    for r in 0..num_recovery {
        let exp = r as u32;
        let mut recovery = vec![0u8; ss];

        for (i, &constant) in constants.iter().enumerate() {
            let factor = gf_pow(constant, exp);
            mul_acc_region(factor, &all_slices[i], &mut recovery);
        }

        set.recovery_slices.insert(
            exp,
            RecoverySlice {
                exponent: exp,
                data: Bytes::from(recovery).into(),
            },
        );
    }

    let elapsed = t0.elapsed();
    eprintln!(
        "  encoding: {} recovery blocks × {} slices × {}KB = {:.1}s",
        num_recovery,
        total_slices,
        ss / 1024,
        elapsed.as_secs_f64()
    );

    SyntheticPar2 {
        par2_set: set,
        files,
    }
}

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

fn run_repair_scenario(
    synthetic: &SyntheticPar2,
    damage: impl FnOnce(&[&SyntheticFile], &mut MemoryFileAccess),
    label: &str,
) {
    let mut access = MemoryFileAccess::new();
    for f in &synthetic.files {
        access.add_file(f.file_id, f.data.clone());
    }

    // Apply damage.
    damage(&synthetic.files.iter().collect::<Vec<_>>(), &mut access);

    // Verify — should detect damage.
    let t0 = Instant::now();
    let result = verify_all(&synthetic.par2_set, &access);
    let verify_ms = t0.elapsed().as_millis();
    eprintln!(
        "  verify (damaged): {verify_ms}ms — {} missing blocks",
        result.total_missing_blocks
    );
    assert!(
        matches!(result.repairable, Repairability::Repairable { .. }),
        "{label}: expected repairable, got {:?}",
        result.repairable
    );

    // Plan.
    let plan = plan_repair(&synthetic.par2_set, &result).unwrap();
    eprintln!(
        "  plan: {} missing slices, {} recovery exponents",
        plan.missing_slices.len(),
        plan.recovery_exponents.len()
    );

    // Repair.
    let t0 = Instant::now();
    execute_repair(&plan, &synthetic.par2_set, &mut access).unwrap();
    let repair_ms = t0.elapsed().as_millis();
    eprintln!("  repair: {repair_ms}ms");

    // Verify repaired data matches original byte-for-byte.
    for f in &synthetic.files {
        let repaired = access.read_file(&f.file_id).unwrap();
        assert_eq!(
            repaired.len(),
            f.data.len(),
            "{label}: file {} length mismatch",
            f.filename
        );
        assert_eq!(
            repaired, f.data,
            "{label}: file {} data mismatch after repair",
            f.filename
        );
    }

    // Re-verify — should be clean.
    let t0 = Instant::now();
    let result = verify_all(&synthetic.par2_set, &access);
    let reverify_ms = t0.elapsed().as_millis();
    eprintln!("  verify (repaired): {reverify_ms}ms");
    assert!(
        matches!(result.repairable, Repairability::NotNeeded),
        "{label}: expected NotNeeded after repair, got {:?}",
        result.repairable
    );
    assert_eq!(
        result.total_missing_blocks, 0,
        "{label}: blocks still missing"
    );

    eprintln!("  PASS: {label}");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn repair_single_file_4mb_8_missing() {
    eprintln!("\n=== repair_single_file_4mb_8_missing ===");
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_0001);
    let synthetic = build_synthetic_par2(&[4 * 1024 * 1024], 65536, 16, &mut rng);

    run_repair_scenario(
        &synthetic,
        |files, access| {
            // Damage 8 evenly-spaced slices out of 64.
            let f = &files[0];
            let ss = 65536usize;
            for i in (0..64).step_by(8) {
                let offset = i * ss;
                let end = offset + ss;
                let zeroed = vec![0xAA; ss];
                access.add_file(f.file_id, {
                    let mut data = access.read_file(&f.file_id).unwrap();
                    data[offset..end].copy_from_slice(&zeroed);
                    data
                });
            }
        },
        "4MB single file, 8/64 slices damaged",
    );
}

#[test]
fn repair_multi_file_set() {
    eprintln!("\n=== repair_multi_file_set ===");
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_0002);
    let synthetic =
        build_synthetic_par2(&[1024 * 1024, 1200 * 1024, 900 * 1024], 65536, 12, &mut rng);

    run_repair_scenario(
        &synthetic,
        |files, access| {
            let ss = 65536usize;
            // Damage 2 slices in file 0.
            {
                let f = &files[0];
                let mut data = access.read_file(&f.file_id).unwrap();
                for i in [1, 3] {
                    let offset = i * ss;
                    data[offset..offset + ss].fill(0xBB);
                }
                access.add_file(f.file_id, data);
            }
            // Damage 2 slices in file 1.
            {
                let f = &files[1];
                let mut data = access.read_file(&f.file_id).unwrap();
                for i in [0, 5] {
                    let offset = i * ss;
                    let end = (offset + ss).min(data.len());
                    data[offset..end].fill(0xCC);
                }
                access.add_file(f.file_id, data);
            }
            // Damage 2 slices in file 2.
            {
                let f = &files[2];
                let mut data = access.read_file(&f.file_id).unwrap();
                for i in [2, 4] {
                    let offset = i * ss;
                    let end = (offset + ss).min(data.len());
                    data[offset..end].fill(0xDD);
                }
                access.add_file(f.file_id, data);
            }
        },
        "3-file set, 6 slices damaged across files",
    );
}

#[test]
fn repair_many_recovery_blocks() {
    eprintln!("\n=== repair_many_recovery_blocks ===");
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_0003);
    let synthetic = build_synthetic_par2(&[2 * 1024 * 1024], 65536, 20, &mut rng);

    run_repair_scenario(
        &synthetic,
        |files, access| {
            // Damage 16 of 32 slices — uses all 20 recovery blocks (well, 16).
            let f = &files[0];
            let ss = 65536usize;
            let mut data = access.read_file(&f.file_id).unwrap();
            for i in 0..16 {
                let offset = (i * 2) * ss; // every other slice
                data[offset..offset + ss].fill(0xEE);
            }
            access.add_file(f.file_id, data);
        },
        "2MB, 16/32 slices damaged, 20 recovery blocks",
    );
}

#[test]
fn repair_large_slices() {
    eprintln!("\n=== repair_large_slices ===");
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_0004);
    let slice_size = 256 * 1024u64; // 256KB slices
    let synthetic = build_synthetic_par2(&[2 * 1024 * 1024], slice_size, 4, &mut rng);

    run_repair_scenario(
        &synthetic,
        |files, access| {
            let f = &files[0];
            let ss = slice_size as usize;
            let mut data = access.read_file(&f.file_id).unwrap();
            // Damage slices 1 and 5 (out of 8).
            for i in [1, 5] {
                let offset = i * ss;
                data[offset..offset + ss].fill(0xFF);
            }
            access.add_file(f.file_id, data);
        },
        "2MB, 256KB slices, 2/8 damaged",
    );
}

#[test]
fn repair_fully_missing_file() {
    eprintln!("\n=== repair_fully_missing_file ===");
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_0005);
    let synthetic = build_synthetic_par2(&[512 * 1024], 65536, 8, &mut rng);

    run_repair_scenario(
        &synthetic,
        |files, access| {
            // Zero the entire file — all 8 slices missing.
            let f = &files[0];
            let zeroed = vec![0u8; f.data.len()];
            access.add_file(f.file_id, zeroed);
        },
        "512KB fully missing, 8/8 slices from 8 recovery blocks",
    );
}
