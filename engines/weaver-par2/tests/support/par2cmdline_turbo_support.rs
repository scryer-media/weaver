#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use bytes::Bytes;
use md5::{Digest, Md5};
use tempfile::{Builder, TempDir};
use weaver_par2::checksum::{self, SliceChecksumState};
use weaver_par2::packet::header;
use weaver_par2::{
    FileAccess, FileId, Par2FileSet, Par2RepairOutcome, Par2RepairStatus, Par2Repairer,
    Par2RepairerOptions, RecoverySlice, Repairability, SliceChecksum, execute_repair, gf_pow,
    input_slice_constants, mul_acc_region, plan_repair, verify_all,
};

pub const UPSTREAM_FUNCTIONAL_CASE_COUNT: usize = 46;
pub const UPSTREAM_UNIT_CASE_COUNT: usize = 11;

pub fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/par2cmdline-turbo")
}

pub fn temp_case_dir(case_id: &str) -> TempDir {
    Builder::new()
        .prefix(&format!("par2cmdline-turbo-{case_id}-"))
        .tempdir()
        .expect("create temp case dir")
}

pub fn extract_fixture(archive_name: &str, dest: &Path) {
    fs::create_dir_all(dest).expect("create fixture extraction dir");
    let archive = fixture_root().join(archive_name);
    assert!(
        archive.is_file(),
        "missing fixture archive {}",
        archive.display()
    );

    let status = Command::new("tar")
        .arg("-xzf")
        .arg(&archive)
        .arg("-C")
        .arg(dest)
        .status()
        .expect("run tar");

    assert!(
        status.success(),
        "extracting fixture {} failed with status {status}",
        archive.display()
    );
}

pub fn run_verify(base_dir: &Path, par2_path: impl AsRef<Path>) -> Par2RepairOutcome {
    run_case(base_dir, par2_path, &[], false)
}

pub fn run_repair(
    base_dir: &Path,
    par2_path: impl AsRef<Path>,
    extra_paths: &[PathBuf],
) -> Par2RepairOutcome {
    run_case(base_dir, par2_path, extra_paths, true)
}

fn run_case(
    base_dir: &Path,
    par2_path: impl AsRef<Path>,
    extra_paths: &[PathBuf],
    repair: bool,
) -> Par2RepairOutcome {
    let mut options = Par2RepairerOptions::new(
        base_dir.to_path_buf(),
        vec![par2_path.as_ref().to_path_buf()],
    );
    options.extra_paths = extra_paths.to_vec();
    options.repair = repair;
    options.memory_limit = Some(512 * 1024 * 1024);
    Par2Repairer::new(options)
        .verify_or_repair()
        .expect("run par2 repairer")
}

pub fn assert_verified(outcome: &Par2RepairOutcome, context: &str) {
    assert_eq!(
        outcome.status,
        Par2RepairStatus::Verified,
        "{context}: expected Verified, got {:?}",
        outcome.status
    );
    assert_eq!(
        outcome.verification.total_missing_blocks, 0,
        "{context}: missing blocks remained"
    );
}

pub fn assert_repaired_or_verified(outcome: &Par2RepairOutcome, context: &str) {
    assert!(
        matches!(
            outcome.status,
            Par2RepairStatus::Repaired | Par2RepairStatus::Verified
        ),
        "{context}: expected Repaired or Verified, got {:?}",
        outcome.status
    );
    assert_eq!(
        outcome.verification.total_missing_blocks, 0,
        "{context}: missing blocks remained"
    );
}

pub fn assert_file_matches(left: impl AsRef<Path>, right: impl AsRef<Path>, context: &str) {
    let left = left.as_ref();
    let right = right.as_ref();
    let left_bytes = fs::read(left)
        .unwrap_or_else(|error| panic!("{context}: failed to read {}: {error}", left.display()));
    let right_bytes = fs::read(right)
        .unwrap_or_else(|error| panic!("{context}: failed to read {}: {error}", right.display()));
    assert_eq!(
        left_bytes,
        right_bytes,
        "{context}: file contents differed ({} vs {})",
        left.display(),
        right.display()
    );
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut offset = 0;
        while offset + 8 <= buf.len() {
            let value = self.next_u64();
            buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            offset += 8;
        }
        if offset < buf.len() {
            let value = self.next_u64();
            let tail = value.to_le_bytes();
            for (index, byte) in buf[offset..].iter_mut().enumerate() {
                *byte = tail[index];
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SyntheticFile {
    pub data: Vec<u8>,
    pub file_id: FileId,
    pub file_id_bytes: [u8; 16],
    pub filename: String,
}

#[derive(Clone, Debug)]
pub struct SyntheticPar2 {
    pub par2_set: Par2FileSet,
    pub files: Vec<SyntheticFile>,
    pub packet_bytes: Vec<u8>,
}

#[derive(Default)]
pub struct SyntheticFileAccess {
    files: HashMap<FileId, Vec<u8>>,
}

impl SyntheticFileAccess {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    pub fn add_file(&mut self, file_id: FileId, data: Vec<u8>) {
        self.files.insert(file_id, data);
    }

    pub fn remove_file(&mut self, file_id: FileId) -> Option<Vec<u8>> {
        self.files.remove(&file_id)
    }

    pub fn read_back(&self, file_id: &FileId) -> Option<&[u8]> {
        self.files.get(file_id).map(Vec::as_slice)
    }
}

impl FileAccess for SyntheticFileAccess {
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let data = self
            .files
            .get(file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(Vec::new());
        }
        let end = (offset + len as usize).min(data.len());
        Ok(data[offset..end].to_vec())
    }

    fn file_exists(&self, file_id: &FileId) -> bool {
        self.files.contains_key(file_id)
    }

    fn file_length(&self, file_id: &FileId) -> Option<u64> {
        self.files.get(file_id).map(|data| data.len() as u64)
    }

    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
        self.files
            .get(file_id)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn write_file_range(&mut self, file_id: &FileId, offset: u64, data: &[u8]) -> io::Result<()> {
        let file = self.files.entry(*file_id).or_default();
        let offset = offset as usize;
        let end = offset + data.len();
        if end > file.len() {
            file.resize(end, 0);
        }
        file[offset..end].copy_from_slice(data);
        Ok(())
    }
}

pub fn build_synthetic_par2(
    file_sizes: &[usize],
    slice_size: u64,
    num_recovery: usize,
    rng: &mut Rng,
) -> SyntheticPar2 {
    let slice_len = slice_size as usize;
    let mut files = Vec::with_capacity(file_sizes.len());

    for (index, &size) in file_sizes.iter().enumerate() {
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        let filename = format!("testfile_{index:03}.dat");
        let hash_16k = checksum::md5(&data[..data.len().min(16_384)]);

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

    let total_slices: usize = files
        .iter()
        .map(|file| file.data.len().div_ceil(slice_len))
        .sum();
    let mut all_slices = Vec::with_capacity(total_slices);

    for file in &files {
        let slice_count = file.data.len().div_ceil(slice_len);
        for slice_index in 0..slice_count {
            let offset = slice_index * slice_len;
            let end = (offset + slice_len).min(file.data.len());
            let mut slice = vec![0u8; slice_len];
            slice[..end - offset].copy_from_slice(&file.data[offset..end]);
            all_slices.push(slice);
        }
    }

    let recovery_set_id: [u8; 16] = {
        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&(files.len() as u32).to_le_bytes());
        for file in &files {
            main_body.extend_from_slice(&file.file_id_bytes);
        }
        Md5::digest(&main_body).into()
    };

    let mut stream = Vec::new();
    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for file in &files {
        main_body.extend_from_slice(&file.file_id_bytes);
    }
    stream.extend_from_slice(&make_full_packet(
        header::TYPE_MAIN,
        &main_body,
        recovery_set_id,
    ));

    for file in &files {
        let hash_full = checksum::md5(&file.data);
        let hash_16k = checksum::md5(&file.data[..file.data.len().min(16_384)]);
        let file_length = file.data.len() as u64;
        let slice_count = file.data.len().div_ceil(slice_len);

        let mut fd_body = Vec::new();
        fd_body.extend_from_slice(&file.file_id_bytes);
        fd_body.extend_from_slice(&hash_full);
        fd_body.extend_from_slice(&hash_16k);
        fd_body.extend_from_slice(&file_length.to_le_bytes());
        fd_body.extend_from_slice(file.filename.as_bytes());
        while fd_body.len() % 4 != 0 {
            fd_body.push(0);
        }

        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&file.file_id_bytes);
        for slice_index in 0..slice_count {
            let offset = slice_index * slice_len;
            let end = (offset + slice_len).min(file.data.len());
            let slice = &file.data[offset..end];
            let mut state = SliceChecksumState::new();
            state.update(slice);
            let pad_to = ((slice.len() as u64) < slice_size).then_some(slice_size);
            let (crc32, md5) = state.finalize(pad_to);
            ifsc_body.extend_from_slice(&md5);
            ifsc_body.extend_from_slice(&crc32.to_le_bytes());
        }

        stream.extend_from_slice(&make_full_packet(
            header::TYPE_FILE_DESC,
            &fd_body,
            recovery_set_id,
        ));
        stream.extend_from_slice(&make_full_packet(
            header::TYPE_IFSC,
            &ifsc_body,
            recovery_set_id,
        ));
    }

    let mut set = Par2FileSet::from_files(&[&stream]).expect("parse synthetic set");
    let constants = input_slice_constants(total_slices);

    for recovery_index in 0..num_recovery {
        let exponent = recovery_index as u32;
        let mut recovery = vec![0u8; slice_len];

        for (slice_index, &constant) in constants.iter().enumerate() {
            let factor = gf_pow(constant, exponent);
            mul_acc_region(factor, &all_slices[slice_index], &mut recovery);
        }

        let mut recovery_body = Vec::with_capacity(4 + recovery.len());
        recovery_body.extend_from_slice(&exponent.to_le_bytes());
        recovery_body.extend_from_slice(&recovery);
        stream.extend_from_slice(&make_full_packet(
            header::TYPE_RECOVERY,
            &recovery_body,
            recovery_set_id,
        ));

        set.recovery_slices.insert(
            exponent,
            RecoverySlice {
                exponent,
                data: Bytes::from(recovery).into(),
            },
        );
    }

    SyntheticPar2 {
        par2_set: set,
        files,
        packet_bytes: stream,
    }
}

pub fn run_repair_scenario(
    synthetic: &SyntheticPar2,
    damage: impl FnOnce(&[SyntheticFile], &mut SyntheticFileAccess),
    label: &str,
) {
    let mut access = SyntheticFileAccess::new();
    for file in &synthetic.files {
        access.add_file(file.file_id, file.data.clone());
    }

    damage(&synthetic.files, &mut access);

    let result = verify_all(&synthetic.par2_set, &access);
    assert!(
        result.total_missing_blocks > 0,
        "{label}: expected missing blocks after damage"
    );
    assert!(
        matches!(result.repairable, Repairability::Repairable { .. }),
        "{label}: expected repairable, got {:?}",
        result.repairable
    );

    let plan = plan_repair(&synthetic.par2_set, &result).expect("plan repair");
    execute_repair(&plan, &synthetic.par2_set, &mut access).expect("execute repair");

    for file in &synthetic.files {
        let repaired = access.read_back(&file.file_id).expect("repaired file");
        assert_eq!(
            repaired.len(),
            file.data.len(),
            "{label}: repaired length mismatch for {}",
            file.filename
        );
        assert_eq!(
            repaired,
            file.data.as_slice(),
            "{label}: repaired bytes mismatch for {}",
            file.filename
        );
    }

    let result = verify_all(&synthetic.par2_set, &access);
    assert!(
        matches!(result.repairable, Repairability::NotNeeded),
        "{label}: expected clean verification, got {:?}",
        result.repairable
    );
    assert_eq!(
        result.total_missing_blocks, 0,
        "{label}: missing blocks remained after repair"
    );
}

pub fn setup_par2_set(file_data: &[u8], slice_size: u64, filename: &str) -> (Par2FileSet, FileId) {
    let file_length = file_data.len() as u64;
    let hash_full = checksum::md5(file_data);
    let hash_16k = checksum::md5(&file_data[..file_data.len().min(16_384)]);

    let mut id_input = Vec::new();
    id_input.extend_from_slice(&hash_16k);
    id_input.extend_from_slice(&file_length.to_le_bytes());
    id_input.extend_from_slice(filename.as_bytes());
    let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
    let file_id = FileId::from_bytes(file_id_bytes);

    let slice_count = if file_length == 0 {
        0
    } else {
        file_length.div_ceil(slice_size) as usize
    };

    let mut checksums = Vec::with_capacity(slice_count);
    for slice_index in 0..slice_count {
        let offset = slice_index as u64 * slice_size;
        let end = ((offset + slice_size) as usize).min(file_data.len());
        let slice = &file_data[offset as usize..end];
        let mut state = SliceChecksumState::new();
        state.update(slice);
        let pad_to = ((slice.len() as u64) < slice_size).then_some(slice_size);
        let (crc32, md5) = state.finalize(pad_to);
        checksums.push(SliceChecksum { crc32, md5 });
    }

    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&1u32.to_le_bytes());
    main_body.extend_from_slice(&file_id_bytes);
    let recovery_set_id: [u8; 16] = Md5::digest(&main_body).into();

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
    for checksum in &checksums {
        ifsc_body.extend_from_slice(&checksum.md5);
        ifsc_body.extend_from_slice(&checksum.crc32.to_le_bytes());
    }

    let mut stream = Vec::new();
    stream.extend_from_slice(&make_full_packet(
        header::TYPE_MAIN,
        &main_body,
        recovery_set_id,
    ));
    stream.extend_from_slice(&make_full_packet(
        header::TYPE_FILE_DESC,
        &fd_body,
        recovery_set_id,
    ));
    stream.extend_from_slice(&make_full_packet(
        header::TYPE_IFSC,
        &ifsc_body,
        recovery_set_id,
    ));

    let set = Par2FileSet::from_files(&[&stream]).expect("parse single-file set");
    (set, file_id)
}

pub fn setup_par2_set_multi(
    files: &[(&[u8], &str)],
    slice_size: u64,
) -> (Par2FileSet, Vec<FileId>) {
    let mut file_ids = Vec::with_capacity(files.len());
    let mut fd_bodies = Vec::with_capacity(files.len());
    let mut ifsc_bodies = Vec::with_capacity(files.len());

    for &(file_data, filename) in files {
        let file_length = file_data.len() as u64;
        let hash_full = checksum::md5(file_data);
        let hash_16k = checksum::md5(&file_data[..file_data.len().min(16_384)]);

        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&file_length.to_le_bytes());
        id_input.extend_from_slice(filename.as_bytes());
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
        file_ids.push(FileId::from_bytes(file_id_bytes));

        let slice_count = if file_length == 0 {
            0
        } else {
            file_length.div_ceil(slice_size) as usize
        };

        let mut checksums = Vec::with_capacity(slice_count);
        for slice_index in 0..slice_count {
            let offset = slice_index as u64 * slice_size;
            let end = ((offset + slice_size) as usize).min(file_data.len());
            let slice = &file_data[offset as usize..end];
            let mut state = SliceChecksumState::new();
            state.update(slice);
            let pad_to = ((slice.len() as u64) < slice_size).then_some(slice_size);
            let (crc32, md5) = state.finalize(pad_to);
            checksums.push(SliceChecksum { crc32, md5 });
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
        for checksum in &checksums {
            ifsc_body.extend_from_slice(&checksum.md5);
            ifsc_body.extend_from_slice(&checksum.crc32.to_le_bytes());
        }
        ifsc_bodies.push(ifsc_body);
    }

    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&(file_ids.len() as u32).to_le_bytes());
    for file_id in &file_ids {
        main_body.extend_from_slice(file_id.as_bytes());
    }
    let recovery_set_id: [u8; 16] = Md5::digest(&main_body).into();

    let mut stream = Vec::new();
    stream.extend_from_slice(&make_full_packet(
        header::TYPE_MAIN,
        &main_body,
        recovery_set_id,
    ));
    for fd_body in &fd_bodies {
        stream.extend_from_slice(&make_full_packet(
            header::TYPE_FILE_DESC,
            fd_body,
            recovery_set_id,
        ));
    }
    for ifsc_body in &ifsc_bodies {
        stream.extend_from_slice(&make_full_packet(
            header::TYPE_IFSC,
            ifsc_body,
            recovery_set_id,
        ));
    }

    let set = Par2FileSet::from_files(&[&stream]).expect("parse multi-file set");
    (set, file_ids)
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
