//! RAR recovery-volume restore support.
//!
//! RAR recovery volumes are not PAR2 files. RAR5 uses GF(2^16) Cauchy matrix
//! semantics, while legacy RAR3 uses a byte-wise GF(2^8) erasure coder. This
//! module keeps that restore logic separate from archive extraction so existing
//! extraction APIs remain unchanged.

use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{RarError, RarResult};
use crate::probe::probe_volume;
use crate::types::ArchiveFormat;
use weaver_reed_solomon::rar3::Rar3RsCoder;
use weaver_reed_solomon::rar5::Rar5RsCoder;

const REV5_SIGN: &[u8; 8] = b"Rar!\x1aRev";
const REV5_PREFIX_LEN: usize = 16;
const MAX_RAR5_VOLUMES: usize = 65_535;
const RAR3_TOTAL_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const RAR5_TOTAL_BUFFER_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct RecoveryOptions {
    pub output_dir: Option<PathBuf>,
    pub overwrite_existing: bool,
    pub verify_restored: bool,
}

impl Default for RecoveryOptions {
    fn default() -> Self {
        Self {
            output_dir: None,
            overwrite_existing: false,
            verify_restored: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryReport {
    pub format: ArchiveFormat,
    pub restored_paths: Vec<PathBuf>,
    pub used_recovery_paths: Vec<PathBuf>,
    pub missing_volume_numbers: Vec<usize>,
}

/// Restore missing volume bytes from existing RAR recovery volumes.
///
/// This reconstructs volumes from recovery data. It does not author, compress,
/// build, or otherwise create new RAR archives.
pub fn restore_volumes_from_paths(
    paths: &[PathBuf],
    options: &RecoveryOptions,
) -> RarResult<RecoveryReport> {
    if paths.is_empty() {
        return Err(RarError::CorruptArchive {
            detail: "RAR recovery restore requires at least one path".into(),
        });
    }
    let expanded_paths = expand_recovery_paths(paths)?;
    let paths = expanded_paths.as_slice();

    let mut rar5_headers = Vec::new();
    let mut rar3_headers = Vec::new();
    for path in paths {
        if is_rev_path(path) {
            if let Some(header) = read_rar5_rev_header(path)? {
                rar5_headers.push(header);
            } else if let Some(header) = read_rar3_rev_header(path)? {
                rar3_headers.push(header);
            }
        }
    }

    if !rar5_headers.is_empty() {
        return restore_rar5(paths, options, rar5_headers);
    }
    if !rar3_headers.is_empty() {
        return restore_rar3(paths, options, rar3_headers);
    }

    if let Some(source) = first_embedded_recovery_record_path(paths)? {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "embedded {:?} recovery record detected in {}, but this API restores standalone .rev recovery volumes only and does not consume embedded RR/protect data",
                source.format,
                source.path.display()
            ),
        });
    }

    Err(RarError::CorruptArchive {
        detail: "no valid RAR recovery volumes were found".into(),
    })
}

fn expand_recovery_paths(paths: &[PathBuf]) -> RarResult<Vec<PathBuf>> {
    let mut seen = HashSet::new();
    let mut expanded = Vec::new();
    for path in paths {
        add_recovery_path(path.clone(), &mut seen, &mut expanded);
    }

    for path in paths {
        let Some(prefix) = recovery_discovery_prefix(path) else {
            continue;
        };
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let entries = match std::fs::read_dir(parent) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(RarError::Io(err)),
        };
        for entry in entries {
            let entry = entry.map_err(RarError::Io)?;
            let candidate = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }
            if recovery_candidate_matches_prefix(&candidate, &prefix) {
                add_recovery_path(candidate, &mut seen, &mut expanded);
            }
        }
    }

    Ok(expanded)
}

fn add_recovery_path(path: PathBuf, seen: &mut HashSet<PathBuf>, expanded: &mut Vec<PathBuf>) {
    if seen.insert(path.clone()) {
        expanded.push(path);
    }
}

fn recovery_discovery_prefix(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_string_lossy();
    if is_rev_path(path) && !rar3_rev_uses_new_style_footer(path) {
        let trimmed = stem.trim_end_matches(|ch: char| ch.is_ascii_digit() || ch == '_');
        return (!trimmed.is_empty()).then(|| trimmed.to_string());
    }
    if let Some((start, _)) = rar_volume_digit_run(&stem) {
        let prefix = &stem[..start];
        return (!prefix.is_empty()).then(|| prefix.to_string());
    }
    (!stem.is_empty()).then(|| stem.to_string())
}

fn recovery_candidate_matches_prefix(path: &Path, prefix: &str) -> bool {
    path.file_stem()
        .map(|stem| stem.to_string_lossy().starts_with(prefix))
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct EmbeddedRecoverySource {
    path: PathBuf,
    format: ArchiveFormat,
}

#[derive(Debug, Clone)]
struct Rar5RevHeader {
    path: PathBuf,
    data_count: usize,
    rec_count: usize,
    rec_num: usize,
    rev_crc: u32,
    data_offset: u64,
    data_volumes: Option<Vec<Rar5DataVolumeInfo>>,
}

#[derive(Debug, Clone)]
struct Rar5DataVolumeInfo {
    file_size: u64,
    crc32: u32,
}

#[derive(Debug, Clone)]
struct Rar5DataSlot {
    path: Option<PathBuf>,
    output_path: PathBuf,
    file_size: u64,
    crc32: u32,
    valid: bool,
}

#[derive(Debug, Clone)]
struct Rar5RecoverySlot {
    path: Option<PathBuf>,
    data_offset: u64,
    valid: bool,
}

fn restore_rar5(
    paths: &[PathBuf],
    options: &RecoveryOptions,
    rev_headers: Vec<Rar5RevHeader>,
) -> RarResult<RecoveryReport> {
    let first = rev_headers
        .iter()
        .find(|header| header.data_volumes.is_some())
        .ok_or_else(|| RarError::CorruptArchive {
            detail: "RAR5 recovery set is missing data volume metadata".into(),
        })?;
    let data_count = first.data_count;
    let rec_count = first.rec_count;
    let total_count = data_count + rec_count;
    let data_volumes = first
        .data_volumes
        .as_ref()
        .ok_or_else(|| RarError::CorruptArchive {
            detail: "RAR5 recovery set is missing data volume metadata".into(),
        })?;

    let mut data_slots = data_volumes
        .iter()
        .map(|info| Rar5DataSlot {
            path: None,
            output_path: PathBuf::new(),
            file_size: info.file_size,
            crc32: info.crc32,
            valid: false,
        })
        .collect::<Vec<_>>();
    let mut recovery_slots = vec![
        Rar5RecoverySlot {
            path: None,
            data_offset: 0,
            valid: false,
        };
        rec_count
    ];

    let reference_name = rar5_recovery_reference_name(paths, &rev_headers)?;

    for header in rev_headers {
        let counts_mismatch = header.data_count != data_count || header.rec_count != rec_count;
        let table_mismatch = header
            .data_volumes
            .as_ref()
            .is_some_and(|volumes| volumes.len() != data_count);
        if counts_mismatch || table_mismatch {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR5 recovery volume {} does not match the first recovery set",
                    header.path.display()
                ),
            });
        }
        if header.rec_num < data_count || header.rec_num >= total_count {
            continue;
        }
        if !verify_file_crc32_region(&header.path, header.data_offset, Some(header.rev_crc))? {
            continue;
        }
        let slot = header.rec_num - data_count;
        recovery_slots[slot] = Rar5RecoverySlot {
            path: Some(header.path),
            data_offset: header.data_offset,
            valid: true,
        };
    }

    let output_dir = recovery_output_dir(paths, options);

    for path in paths {
        if is_rev_path(path) {
            continue;
        }
        if let Some(index) = probe_rar5_volume_number(path)?
            && index < data_count
        {
            data_slots[index].path = Some(path.clone());
        }
    }

    for (index, slot) in data_slots.iter_mut().enumerate() {
        slot.output_path = slot
            .path
            .as_ref()
            .map(|path| output_dir.join(file_name_lossy(path)))
            .unwrap_or_else(|| {
                infer_numbered_volume_path(&reference_name, &output_dir, index, "rar")
            });

        slot.valid = if let Some(path) = &slot.path {
            let metadata = std::fs::metadata(path).map_err(RarError::Io)?;
            metadata.len() == slot.file_size && verify_file_crc32_region(path, 0, Some(slot.crc32))?
        } else {
            false
        };
    }

    let missing_volume_numbers = data_slots
        .iter()
        .enumerate()
        .filter_map(|(idx, slot)| (!slot.valid).then_some(idx))
        .collect::<Vec<_>>();

    if missing_volume_numbers.is_empty() {
        return Err(RarError::CorruptArchive {
            detail: "all RAR5 data volumes are already present and valid".into(),
        });
    }

    let valid_recovery_count = recovery_slots.iter().filter(|slot| slot.valid).count();
    if missing_volume_numbers.len() > valid_recovery_count {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "insufficient RAR5 recovery volumes: need {}, have {}",
                missing_volume_numbers.len(),
                valid_recovery_count
            ),
        });
    }

    for &missing_idx in &missing_volume_numbers {
        rename_invalid_rar5_data_volume(&mut data_slots[missing_idx], options)?;
    }

    for &missing_idx in &missing_volume_numbers {
        let path = &data_slots[missing_idx].output_path;
        if path.exists() && !options.overwrite_existing {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "refusing to overwrite existing restored volume {}",
                    path.display()
                ),
            });
        }
    }

    let mut valid_flags = vec![false; total_count];
    for (idx, slot) in data_slots.iter().enumerate() {
        valid_flags[idx] = slot.valid;
    }
    for (idx, slot) in recovery_slots.iter().enumerate() {
        valid_flags[data_count + idx] = slot.valid;
    }

    let coder = Rar5RsCoder::new_decoder(data_count, rec_count, &valid_flags).ok_or_else(|| {
        RarError::CorruptArchive {
            detail: "failed to initialize RAR5 recovery decoder".into(),
        }
    })?;

    let restored_paths = reconstruct_rar5(
        &coder,
        &data_slots,
        &recovery_slots,
        &missing_volume_numbers,
        options,
    )?;

    if options.verify_restored {
        for (&missing_idx, path) in missing_volume_numbers.iter().zip(restored_paths.iter()) {
            let expected_crc = data_slots[missing_idx].crc32;
            if !verify_file_crc32_region(path, 0, Some(expected_crc))? {
                return Err(RarError::DataCrcMismatch {
                    member: path.display().to_string(),
                    expected: expected_crc,
                    actual: crc32_file_region(path, 0)?,
                });
            }
        }
    }

    let used_recovery_paths = recovery_slots
        .iter()
        .filter(|slot| slot.valid)
        .filter_map(|slot| slot.path.clone())
        .collect();

    Ok(RecoveryReport {
        format: ArchiveFormat::Rar5,
        restored_paths,
        used_recovery_paths,
        missing_volume_numbers,
    })
}

fn rar5_recovery_reference_name(
    paths: &[PathBuf],
    rev_headers: &[Rar5RevHeader],
) -> RarResult<PathBuf> {
    for path in paths.iter().filter(|path| !is_rev_path(path)) {
        if probe_rar5_volume_number(path)?.is_some() {
            return Ok(path.clone());
        }
    }

    rev_headers
        .iter()
        .map(|header| header.path.clone())
        .max_by_key(|path| path.to_string_lossy().len())
        .ok_or_else(|| RarError::CorruptArchive {
            detail: "no input paths available for volume name inference".into(),
        })
}

fn rename_invalid_rar5_data_volume(
    slot: &mut Rar5DataSlot,
    options: &RecoveryOptions,
) -> RarResult<()> {
    if slot.valid {
        return Ok(());
    }
    let Some(path) = slot.path.as_ref() else {
        return Ok(());
    };
    if rename_invalid_data_volume(path, &slot.output_path, options)? {
        slot.path = None;
    }
    Ok(())
}

fn rename_invalid_data_volume(
    invalid_path: &Path,
    output_path: &Path,
    options: &RecoveryOptions,
) -> RarResult<bool> {
    if !same_existing_file(invalid_path, output_path)? {
        return Ok(false);
    }

    let bad_path = bad_volume_path(invalid_path);
    if bad_path.exists() {
        if options.overwrite_existing {
            std::fs::remove_file(&bad_path).map_err(RarError::Io)?;
        } else {
            return Err(RarError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "refusing to overwrite existing bad-volume backup {}",
                    bad_path.display()
                ),
            )));
        }
    }

    std::fs::rename(invalid_path, bad_path).map_err(RarError::Io)?;
    Ok(true)
}

fn same_existing_file(left: &Path, right: &Path) -> RarResult<bool> {
    let left = match std::fs::canonicalize(left) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(RarError::Io(err)),
    };
    let right = match std::fs::canonicalize(right) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(RarError::Io(err)),
    };
    Ok(left == right)
}

fn bad_volume_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".bad");
    PathBuf::from(name)
}

fn reconstruct_rar5(
    coder: &Rar5RsCoder,
    data_slots: &[Rar5DataSlot],
    recovery_slots: &[Rar5RecoverySlot],
    missing_volume_numbers: &[usize],
    options: &RecoveryOptions,
) -> RarResult<Vec<PathBuf>> {
    let missing_count = missing_volume_numbers.len();
    let data_count = data_slots.len();
    let max_volume_size = data_slots
        .iter()
        .map(|slot| slot.file_size)
        .max()
        .unwrap_or(0);
    let mut chunk_size = (RAR5_TOTAL_BUFFER_SIZE / missing_count.max(1)).max(2);
    if !chunk_size.is_multiple_of(2) {
        chunk_size -= 1;
    }

    let mut data_files = data_slots
        .iter()
        .map(|slot| {
            if slot.valid {
                let path = slot.path.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: "valid RAR5 data slot is missing a path".into(),
                })?;
                File::open(path).map(Some).map_err(RarError::Io)
            } else {
                Ok(None)
            }
        })
        .collect::<RarResult<Vec<_>>>()?;

    let mut recovery_files = recovery_slots
        .iter()
        .map(|slot| {
            if slot.valid {
                let path = slot.path.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: "valid RAR5 recovery slot is missing a path".into(),
                })?;
                let mut file = File::open(path).map_err(RarError::Io)?;
                file.seek(SeekFrom::Start(slot.data_offset))
                    .map_err(RarError::Io)?;
                Ok(Some(file))
            } else {
                Ok(None)
            }
        })
        .collect::<RarResult<Vec<_>>>()?;

    let mut outputs = Vec::with_capacity(missing_count);
    let mut restored_paths = Vec::with_capacity(missing_count);
    for &missing_idx in missing_volume_numbers {
        let path = &data_slots[missing_idx].output_path;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(RarError::Io)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(options.overwrite_existing)
            .create_new(!options.overwrite_existing)
            .open(path)
            .map_err(RarError::Io)?;
        outputs.push(file);
        restored_paths.push(path.clone());
    }

    let mut input_buf = vec![0u8; chunk_size + 2];
    let mut out_bufs = vec![vec![0u8; chunk_size + 2]; missing_count];
    let mut processed = 0u64;

    while processed < max_volume_size {
        let remaining = (max_volume_size - processed) as usize;
        let bytes_to_process = remaining.min(chunk_size);
        let rs_len = bytes_to_process + (bytes_to_process & 1);
        let mut next_recovery = 0usize;

        for data_num in 0..data_count {
            input_buf[..rs_len].fill(0);
            if data_slots[data_num].valid {
                if let Some(file) = &mut data_files[data_num] {
                    read_padded(file, &mut input_buf[..bytes_to_process])?;
                }
            } else {
                while next_recovery < recovery_files.len()
                    && recovery_files[next_recovery].is_none()
                {
                    next_recovery += 1;
                }
                let recovery = recovery_files
                    .get_mut(next_recovery)
                    .and_then(Option::as_mut)
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: "RAR5 recovery volume selection exhausted unexpectedly".into(),
                    })?;
                read_padded(recovery, &mut input_buf[..bytes_to_process])?;
                next_recovery += 1;
            }

            let mut output_slices = out_bufs
                .iter_mut()
                .map(|buf| &mut buf[..rs_len])
                .collect::<Vec<_>>();
            coder.update_outputs(data_num, &input_buf[..rs_len], &mut output_slices);
        }

        for (out_idx, &missing_idx) in missing_volume_numbers.iter().enumerate() {
            let remaining_for_file = data_slots[missing_idx].file_size.saturating_sub(processed);
            let write_len = remaining_for_file.min(bytes_to_process as u64) as usize;
            if write_len > 0 {
                outputs[out_idx]
                    .write_all(&out_bufs[out_idx][..write_len])
                    .map_err(RarError::Io)?;
            }
        }

        processed += bytes_to_process as u64;
    }

    for output in &mut outputs {
        output.flush().map_err(RarError::Io)?;
    }

    Ok(restored_paths)
}

fn read_rar5_rev_header(path: &Path) -> RarResult<Option<Rar5RevHeader>> {
    let mut file = File::open(path).map_err(RarError::Io)?;
    let mut prefix = [0u8; REV5_PREFIX_LEN];
    if file.read_exact(&mut prefix).is_err() {
        return Ok(None);
    }
    if &prefix[..REV5_SIGN.len()] != REV5_SIGN {
        return Ok(None);
    }

    let block_crc = le_u32(&prefix[REV5_SIGN.len()..REV5_SIGN.len() + 4]);
    let header_size = le_u32(&prefix[REV5_SIGN.len() + 4..REV5_SIGN.len() + 8]) as usize;
    if !(6..=0x100000).contains(&header_size) {
        return Ok(None);
    }

    let mut raw = vec![0u8; header_size];
    if let Err(err) = file.read_exact(&mut raw) {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(RarError::Io(err));
    }
    let mut crc_input = Vec::with_capacity(4 + raw.len());
    crc_input.extend_from_slice(&prefix[REV5_SIGN.len() + 4..REV5_SIGN.len() + 8]);
    crc_input.extend_from_slice(&raw);
    if crc32fast::hash(&crc_input) != block_crc {
        return Ok(None);
    }

    parse_rar5_rev_body(path, &raw, REV5_PREFIX_LEN as u64 + header_size as u64)
}

fn parse_rar5_rev_body(
    path: &Path,
    raw: &[u8],
    data_offset: u64,
) -> RarResult<Option<Rar5RevHeader>> {
    let mut reader = SliceReader::new(raw);
    let version = match reader.get_u8() {
        Ok(version) => version,
        Err(RarError::CorruptArchive { .. }) => return Ok(None),
        Err(err) => return Err(err),
    };
    if version != 1 {
        return Ok(None);
    }

    let Some(data_count) = read_rar5_rev_u16(&mut reader)? else {
        return Ok(None);
    };
    let Some(rec_count) = read_rar5_rev_u16(&mut reader)? else {
        return Ok(None);
    };
    let total_count = data_count + rec_count;
    let Some(rec_num) = read_rar5_rev_u16(&mut reader)? else {
        return Ok(None);
    };
    if data_count == 0 || rec_count == 0 || total_count > MAX_RAR5_VOLUMES || rec_num >= total_count
    {
        return Ok(None);
    }
    let Some(rev_crc) = read_rar5_rev_u32(&mut reader)? else {
        return Ok(None);
    };

    let table_size = data_count
        .checked_mul(12)
        .ok_or_else(|| RarError::CorruptArchive {
            detail: "RAR5 recovery data table size overflow".into(),
        })?;
    let data_volumes = if raw.len().saturating_sub(reader.pos) >= table_size {
        let mut volumes = Vec::with_capacity(data_count);
        for _ in 0..data_count {
            let Some(file_size) = read_rar5_rev_u64(&mut reader)? else {
                return Ok(None);
            };
            let Some(crc32) = read_rar5_rev_u32(&mut reader)? else {
                return Ok(None);
            };
            volumes.push(Rar5DataVolumeInfo { file_size, crc32 });
        }
        Some(volumes)
    } else {
        None
    };

    Ok(Some(Rar5RevHeader {
        path: path.to_path_buf(),
        data_count,
        rec_count,
        rec_num,
        rev_crc,
        data_offset,
        data_volumes,
    }))
}

fn read_rar5_rev_u16(reader: &mut SliceReader<'_>) -> RarResult<Option<usize>> {
    match reader.get_u16() {
        Ok(value) => Ok(Some(value as usize)),
        Err(RarError::CorruptArchive { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

fn read_rar5_rev_u32(reader: &mut SliceReader<'_>) -> RarResult<Option<u32>> {
    match reader.get_u32() {
        Ok(value) => Ok(Some(value)),
        Err(RarError::CorruptArchive { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

fn read_rar5_rev_u64(reader: &mut SliceReader<'_>) -> RarResult<Option<u64>> {
    match reader.get_u64() {
        Ok(value) => Ok(Some(value)),
        Err(RarError::CorruptArchive { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

#[derive(Debug, Clone)]
struct Rar3RevHeader {
    path: PathBuf,
    rec_position: usize,
    rec_count: usize,
    data_count: usize,
    new_style: bool,
}

fn restore_rar3(
    paths: &[PathBuf],
    options: &RecoveryOptions,
    rev_headers: Vec<Rar3RevHeader>,
) -> RarResult<RecoveryReport> {
    let first = &rev_headers[0];
    let data_count = first.data_count;
    let rec_count = first.rec_count;
    let new_style = first.new_style;
    let total_count = data_count + rec_count;
    if total_count > 255 {
        return Err(RarError::CorruptArchive {
            detail: "RAR3 recovery set exceeds 255 total volumes".into(),
        });
    }

    let output_dir = recovery_output_dir(paths, options);
    let reference_name = rar3_recovery_reference_name(paths)?;

    let mut slots = vec![None::<PathBuf>; total_count];
    let mut invalid_data_paths = vec![None::<PathBuf>; data_count];
    for header in rev_headers {
        if header.data_count != data_count
            || header.rec_count != rec_count
            || header.new_style != new_style
        {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR3 recovery volume {} does not match the first recovery set",
                    header.path.display()
                ),
            });
        }
        let idx = data_count + header.rec_position - 1;
        if idx < slots.len() {
            slots[idx] = Some(header.path);
        }
    }

    for path in paths {
        if is_rev_path(path) {
            continue;
        }
        if let Some(index) = parse_rar3_data_volume_number(path)
            && index < data_count
        {
            if rar3_data_volume_is_valid(path)? {
                slots[index] = Some(path.clone());
                invalid_data_paths[index] = None;
            } else {
                slots[index] = None;
                invalid_data_paths[index] = Some(path.clone());
            }
        }
    }

    let missing_volume_numbers = (0..data_count)
        .filter(|&idx| slots[idx].is_none())
        .collect::<Vec<_>>();
    if missing_volume_numbers.is_empty() {
        return Err(RarError::CorruptArchive {
            detail: "all RAR3 data volumes are already present".into(),
        });
    }

    let found_recovery_count = slots[data_count..]
        .iter()
        .filter(|slot| slot.is_some())
        .count();
    if missing_volume_numbers.len() > found_recovery_count {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "insufficient RAR3 recovery volumes: need {}, have {}",
                missing_volume_numbers.len(),
                found_recovery_count
            ),
        });
    }

    let old_numbering = rar3_uses_old_numbering(paths)?;
    let restored_paths = missing_volume_numbers
        .iter()
        .map(|&idx| infer_rar3_volume_path(reference_name, &output_dir, idx, old_numbering))
        .collect::<Vec<_>>();
    for (&idx, restored_path) in missing_volume_numbers.iter().zip(restored_paths.iter()) {
        if let Some(invalid_path) = invalid_data_paths[idx].as_ref() {
            rename_invalid_data_volume(invalid_path, restored_path, options)?;
        }
    }
    for path in &restored_paths {
        if path.exists() && !options.overwrite_existing {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "refusing to overwrite existing restored volume {}",
                    path.display()
                ),
            });
        }
    }

    reconstruct_rar3(
        &slots,
        data_count,
        rec_count,
        &missing_volume_numbers,
        &restored_paths,
        options,
        new_style,
    )?;

    if options.verify_restored {
        for path in &restored_paths {
            let mut file = File::open(path).map_err(RarError::Io)?;
            match probe_volume(&mut file) {
                Ok(probe) if probe.format == ArchiveFormat::Rar4 => {}
                Ok(probe) => {
                    return Err(RarError::CorruptArchive {
                        detail: format!(
                            "restored RAR3 volume {} probed as {:?}",
                            path.display(),
                            probe.format
                        ),
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    let used_recovery_paths = slots[data_count..]
        .iter()
        .filter_map(Clone::clone)
        .collect::<Vec<_>>();

    Ok(RecoveryReport {
        format: ArchiveFormat::Rar4,
        restored_paths,
        used_recovery_paths,
        missing_volume_numbers,
    })
}

fn rar3_recovery_reference_name(paths: &[PathBuf]) -> RarResult<&Path> {
    for path in paths.iter().filter(|path| !is_rev_path(path)) {
        if rar3_data_volume_is_valid(path)? {
            return Ok(path);
        }
    }

    Err(RarError::CorruptArchive {
        detail: "RAR3 recovery restore requires at least one valid data volume".into(),
    })
}

fn rar3_data_volume_is_valid(path: &Path) -> RarResult<bool> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(RarError::Io(err)),
    };
    match probe_volume(&mut file) {
        Ok(probe) if probe.format.is_rar4_family() => rar3_data_volume_end_crc_valid(path),
        Ok(_) => Ok(false),
        Err(
            RarError::InvalidSignature
            | RarError::UnsupportedFormat { .. }
            | RarError::CorruptArchive { .. }
            | RarError::HeaderCrcMismatch { .. },
        ) => Ok(false),
        Err(err) => Err(err),
    }
}

fn rar3_data_volume_end_crc_valid(path: &Path) -> RarResult<bool> {
    let mut file = File::open(path).map_err(RarError::Io)?;
    match crate::signature::read_signature(&mut file) {
        Ok(ArchiveFormat::Rar14) => return Ok(true),
        Ok(ArchiveFormat::Rar4) => {}
        Ok(_) => return Ok(false),
        Err(
            RarError::InvalidSignature
            | RarError::UnsupportedFormat { .. }
            | RarError::CorruptArchive { .. }
            | RarError::HeaderCrcMismatch { .. },
        ) => return Ok(false),
        Err(err) => return Err(err),
    }

    while let Some(raw) = crate::rar4::header::read_raw_header(&mut file)? {
        match raw.header_type {
            crate::rar4::types::Rar4HeaderType::File
            | crate::rar4::types::Rar4HeaderType::NewSub => {
                let fh = crate::rar4::header::parse_file_header(&raw)?;
                file.seek(SeekFrom::Current(fh.packed_size as i64))
                    .map_err(RarError::Io)?;
            }
            crate::rar4::types::Rar4HeaderType::EndArchive => {
                let end = crate::rar4::header::parse_end_header(&raw);
                if let Some(expected) = end.data_crc {
                    return Ok(crc32_file_prefix(path, raw.offset)? == expected);
                }
                return Ok(true);
            }
            _ => {
                if raw.data_area_size > 0 {
                    file.seek(SeekFrom::Current(raw.data_area_size as i64))
                        .map_err(RarError::Io)?;
                }
            }
        }
    }

    Ok(true)
}

fn reconstruct_rar3(
    slots: &[Option<PathBuf>],
    data_count: usize,
    rec_count: usize,
    missing_volume_numbers: &[usize],
    restored_paths: &[PathBuf],
    options: &RecoveryOptions,
    new_style: bool,
) -> RarResult<()> {
    let total_count = data_count + rec_count;
    let mut chunk_size = (RAR3_TOTAL_BUFFER_SIZE / total_count.max(1)).max(1);
    if chunk_size == 0 {
        chunk_size = 1;
    }

    let mut inputs = slots
        .iter()
        .map(|slot| {
            if let Some(path) = slot {
                File::open(path).map(Some).map_err(RarError::Io)
            } else {
                Ok(None)
            }
        })
        .collect::<RarResult<Vec<_>>>()?;

    let mut outputs = Vec::with_capacity(restored_paths.len());
    for path in restored_paths {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(RarError::Io)?;
        }
        outputs.push(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(options.overwrite_existing)
                .create_new(!options.overwrite_existing)
                .open(path)
                .map_err(RarError::Io)?,
        );
    }

    let erasures = (0..total_count)
        .filter(|&idx| slots[idx].is_none())
        .collect::<Vec<_>>();
    let mut buffers = vec![vec![0u8; chunk_size]; total_count];

    loop {
        let mut max_read = 0usize;
        for idx in 0..total_count {
            buffers[idx].fill(0);
            if let Some(file) = &mut inputs[idx] {
                let read = read_padded(file, &mut buffers[idx])?;
                max_read = max_read.max(read);
            }
        }
        if max_read == 0 {
            break;
        }

        let restored_columns = (0..max_read)
            .into_par_iter()
            .map(|pos| {
                let mut column = (0..total_count)
                    .map(|idx| buffers[idx][pos])
                    .collect::<Vec<_>>();
                let mut coder =
                    Rar3RsCoder::new(rec_count).ok_or_else(|| RarError::CorruptArchive {
                        detail: "RAR3 recovery set has invalid recovery count".into(),
                    })?;
                if !coder.decode(&mut column, &erasures) {
                    return Err(RarError::CorruptArchive {
                        detail: "RAR3 recovery decoder failed".into(),
                    });
                }
                Ok(missing_volume_numbers
                    .iter()
                    .map(|&idx| column[idx])
                    .collect::<Vec<_>>())
            })
            .collect::<RarResult<Vec<_>>>()?;

        for (pos, recovered) in restored_columns.iter().enumerate() {
            for (missing_idx, &byte) in recovered.iter().enumerate() {
                buffers[missing_volume_numbers[missing_idx]][pos] = byte;
            }
        }

        for (out_idx, &missing_idx) in missing_volume_numbers.iter().enumerate() {
            outputs[out_idx]
                .write_all(&buffers[missing_idx][..max_read])
                .map_err(RarError::Io)?;
        }
    }

    for (out_idx, output) in outputs.iter_mut().enumerate() {
        let is_last_data_volume = missing_volume_numbers[out_idx] + 1 == data_count;
        finalize_rar3_restored_output(output, new_style, is_last_data_volume)?;
    }
    Ok(())
}

fn finalize_rar3_restored_output(
    output: &mut File,
    new_style: bool,
    is_last_data_volume: bool,
) -> RarResult<()> {
    output.flush().map_err(RarError::Io)?;

    // RAR 3.10+ recovery sets carry a synthetic 7-byte footer; old-style
    // name#_#_#.rev volumes do not.
    if new_style {
        let len = output.seek(SeekFrom::End(0)).map_err(RarError::Io)?;
        if len >= 7 {
            output
                .seek(SeekFrom::Start(len - 7))
                .map_err(RarError::Io)?;
            output.write_all(&[0u8; 7]).map_err(RarError::Io)?;
            output.flush().map_err(RarError::Io)?;
        }
    }

    if is_last_data_volume {
        truncate_rar3_last_volume_padding(output)?;
    }
    Ok(())
}

fn truncate_rar3_last_volume_padding(output: &mut File) -> RarResult<()> {
    output.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;
    match crate::signature::read_signature(output) {
        Ok(format) if format.is_rar4_family() => {}
        Ok(_) => return Ok(()),
        Err(RarError::InvalidSignature) => return Ok(()),
        Err(err) => return Err(err),
    }

    while let Some(raw) = crate::rar4::header::read_raw_header(output)? {
        let next_offset = raw
            .offset
            .saturating_add(u64::from(raw.header_size))
            .saturating_add(raw.data_area_size);
        if raw.header_type == crate::rar4::types::Rar4HeaderType::EndArchive {
            let file_len = output.seek(SeekFrom::End(0)).map_err(RarError::Io)?;
            if next_offset >= file_len {
                return Ok(());
            }
            output
                .seek(SeekFrom::Start(next_offset))
                .map_err(RarError::Io)?;
            let mut probe = [0u8; 8192];
            let read = output.read(&mut probe).map_err(RarError::Io)?;
            if read > 0 && probe[..read].iter().all(|&byte| byte == 0) {
                output.set_len(next_offset).map_err(RarError::Io)?;
            }
            return Ok(());
        }

        if raw.data_area_size > 0 {
            output
                .seek(SeekFrom::Start(next_offset))
                .map_err(RarError::Io)?;
        }
    }

    Ok(())
}

fn read_rar3_rev_header(path: &Path) -> RarResult<Option<Rar3RevHeader>> {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(e) => return Err(RarError::Io(e)),
    };
    if !rar3_rev_uses_new_style_footer(path) {
        return Ok(
            parse_old_rar3_rev_name(path).map(|(data_count, rec_count, rec_position)| {
                Rar3RevHeader {
                    path: path.to_path_buf(),
                    rec_position,
                    rec_count,
                    data_count,
                    new_style: false,
                }
            }),
        );
    }
    if data.len() < 7 {
        return Ok(None);
    }

    let crc_offset = data.len() - 4;
    let expected_crc = le_u32(&data[crc_offset..]);
    if crc32fast::hash(&data[..crc_offset]) != expected_crc {
        return Ok(None);
    }

    let param_offset = data.len() - 7;
    let data_count = data[param_offset] as usize + 1;
    let rec_count = data[param_offset + 1] as usize + 1;
    let rec_position = data[param_offset + 2] as usize + 1;
    if data_count == 0
        || rec_count == 0
        || rec_position == 0
        || data_count + rec_count > 255
        || rec_position > rec_count
    {
        return Ok(None);
    }

    Ok(Some(Rar3RevHeader {
        path: path.to_path_buf(),
        rec_position,
        rec_count,
        data_count,
        new_style: true,
    }))
}

fn rar3_rev_uses_new_style_footer(path: &Path) -> bool {
    let Some(stem) = path.file_stem().map(|stem| stem.to_string_lossy()) else {
        return true;
    };
    let bytes = stem.as_bytes();
    let mut digit_groups = 0usize;
    let mut pos = bytes.len();
    while pos > 0 {
        pos -= 1;
        if bytes[pos].is_ascii_digit() {
            continue;
        }
        if bytes[pos] == b'_' && pos > 0 && bytes[pos - 1].is_ascii_digit() {
            digit_groups += 1;
        } else {
            break;
        }
    }
    digit_groups < 2
}

fn parse_old_rar3_rev_name(path: &Path) -> Option<(usize, usize, usize)> {
    let stem = path.file_stem()?.to_string_lossy();
    let mut parts = stem.rsplitn(3, '_');
    let rec_position = parse_rar3_rev_param(parts.next()?)?;
    let rec_count = parse_rar3_rev_param(parts.next()?)?;
    let data_part = parts.next()?;
    let data_start = data_part
        .as_bytes()
        .iter()
        .rposition(|byte| !byte.is_ascii_digit())
        .map_or(0, |idx| idx + 1);
    let data_count = parse_rar3_rev_param(&data_part[data_start..])?;

    if data_count + rec_count > 255 || data_count + rec_position - 1 > 255 {
        return None;
    }

    Some((data_count, rec_count, rec_position))
}

fn parse_rar3_rev_param(value: &str) -> Option<usize> {
    if value.is_empty() || !value.as_bytes().iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = value.parse::<usize>().ok()?;
    (1..=255).contains(&value).then_some(value)
}

fn probe_rar5_volume_number(path: &Path) -> RarResult<Option<usize>> {
    if !rar5_recovery_data_candidate_allowed(path)? {
        return Ok(None);
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) => return Err(RarError::Io(e)),
    };
    match probe_volume(&mut file) {
        Ok(probe) if probe.format == ArchiveFormat::Rar5 && probe.is_multi_volume => {
            Ok(parse_part_number(path).and_then(|number| number.checked_sub(1)))
        }
        Ok(_) | Err(RarError::InvalidSignature) => Ok(None),
        Err(e) => Err(e),
    }
}

fn rar5_recovery_data_candidate_allowed(path: &Path) -> RarResult<bool> {
    if path
        .extension()
        .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("rar"))
    {
        return Ok(true);
    }

    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(RarError::Io(err)),
    };
    match crate::signature::read_signature(&mut file) {
        Ok(ArchiveFormat::Rar5) => {
            let pos = file.stream_position().map_err(RarError::Io)?;
            Ok(pos > crate::signature::RAR5_SIGNATURE_LEN as u64)
        }
        Ok(_) | Err(RarError::InvalidSignature) => Ok(false),
        Err(err) => Err(err),
    }
}

fn parse_rar3_data_volume_number(path: &Path) -> Option<usize> {
    let ext = path.extension()?.to_string_lossy().to_ascii_lowercase();
    if ext == "rar" {
        if let Some(part_number) = parse_part_number(path) {
            return part_number.checked_sub(1);
        }
        return Some(0);
    }
    parse_old_rar3_numbered_extension(&ext)
}

fn has_old_rar3_extension(path: &Path) -> bool {
    let Some(ext) = path
        .extension()
        .map(|ext| ext.to_string_lossy().to_ascii_lowercase())
    else {
        return false;
    };
    parse_old_rar3_numbered_extension(&ext).is_some()
}

fn parse_old_rar3_numbered_extension(ext: &str) -> Option<usize> {
    let bytes = ext.as_bytes();
    if bytes.len() != 3 {
        return None;
    }

    if bytes.iter().all(u8::is_ascii_digit) {
        let number = usize::from(bytes[0] - b'0') * 100
            + usize::from(bytes[1] - b'0') * 10
            + usize::from(bytes[2] - b'0');
        return number.checked_sub(1);
    }

    if !bytes[1].is_ascii_digit() || !bytes[2].is_ascii_digit() {
        return None;
    }
    let prefix = bytes[0].to_ascii_lowercase();
    let number = usize::from(bytes[1] - b'0') * 10 + usize::from(bytes[2] - b'0');
    if (b'a'..b'r').contains(&prefix) {
        // Old numeric extension rollover increments .999 to .a00. Keep .r00
        // mapped to the classic .rar, .r00, .r01 sequence below.
        return Some(999 + usize::from(prefix - b'a') * 100 + number);
    }
    if prefix < b'r' {
        return None;
    }

    Some((usize::from(prefix - b'r') * 100) + number + 1)
}

fn rar3_uses_old_numbering(paths: &[PathBuf]) -> RarResult<bool> {
    if paths
        .iter()
        .filter(|path| !is_rev_path(path))
        .any(|path| has_old_rar3_extension(path))
    {
        return Ok(true);
    }

    for path in paths.iter().filter(|path| !is_rev_path(path)) {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(RarError::Io(err)),
        };
        match probe_volume(&mut file) {
            Ok(probe) if probe.format == ArchiveFormat::Rar4 => {
                return Ok(!probe.new_naming);
            }
            Ok(_) | Err(RarError::InvalidSignature) => {}
            Err(err) => return Err(err),
        }
    }

    Ok(false)
}

fn infer_rar3_volume_path(
    reference: &Path,
    output_dir: &Path,
    volume_index: usize,
    old_numbering: bool,
) -> PathBuf {
    if old_numbering {
        infer_old_rar3_volume_path(reference, output_dir, volume_index)
    } else {
        infer_numbered_volume_path(reference, output_dir, volume_index, "rar")
    }
}

fn infer_old_rar3_volume_path(reference: &Path, output_dir: &Path, volume_index: usize) -> PathBuf {
    let file_name = file_name_lossy(reference);
    let stem = reference
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or(file_name);
    let ext = if old_rar3_numeric_extension_style(reference) {
        old_numeric_rar3_numbered_extension(volume_index)
    } else if volume_index == 0 {
        "rar".to_string()
    } else {
        old_rar3_numbered_extension(volume_index)
    };
    output_dir.join(format!("{stem}.{ext}"))
}

fn old_rar3_numeric_extension_style(path: &Path) -> bool {
    let Some(ext) = path.extension().map(|ext| ext.to_string_lossy()) else {
        return false;
    };
    let ext = ext.to_ascii_lowercase();
    let bytes = ext.as_bytes();
    bytes.len() == 3
        && (bytes.iter().all(u8::is_ascii_digit)
            || ((b'a'..b'r').contains(&bytes[0])
                && bytes[1].is_ascii_digit()
                && bytes[2].is_ascii_digit()))
}

fn old_numeric_rar3_numbered_extension(volume_index: usize) -> String {
    if volume_index < 999 {
        format!("{:03}", volume_index + 1)
    } else {
        let rollover = volume_index - 999;
        let prefix = char::from_u32(u32::from(b'a') + (rollover / 100) as u32).unwrap_or('a');
        format!("{prefix}{:02}", rollover % 100)
    }
}

fn old_rar3_numbered_extension(volume_index: usize) -> String {
    let old_index = volume_index - 1;
    let prefix = char::from_u32(u32::from(b'r') + (old_index / 100) as u32).unwrap_or('r');
    format!("{prefix}{:02}", old_index % 100)
}

fn parse_part_number(path: &Path) -> Option<usize> {
    let stem = path.file_stem()?.to_string_lossy();
    let (start, end) = rar_volume_digit_run(&stem)?;
    stem[start..end].parse::<usize>().ok()
}

fn infer_numbered_volume_path(
    reference: &Path,
    output_dir: &Path,
    volume_index: usize,
    extension: &str,
) -> PathBuf {
    let file_name = file_name_lossy(reference);
    let stem = reference
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or(file_name.clone());
    let generated_name = if let Some((start, end)) = rar_volume_digit_run(&stem) {
        let width = end - start;
        let number = volume_index + 1;
        let mut next_stem = stem.clone();
        next_stem.replace_range(start..end, &format!("{number:0width$}"));
        format!("{next_stem}.{extension}")
    } else if volume_index == 0 {
        format!("{stem}.{extension}")
    } else {
        format!("{stem}.part{:02}.{extension}", volume_index + 1)
    };

    output_dir.join(generated_name)
}

fn rar_volume_digit_run(value: &str) -> Option<(usize, usize)> {
    let bytes = value.as_bytes();
    let end = bytes.iter().rposition(|b| b.is_ascii_digit())? + 1;
    let start = bytes[..end]
        .iter()
        .rposition(|b| !b.is_ascii_digit())
        .map_or(0, |idx| idx + 1);

    // For names like `name.part##of##.rar`, select the first numeric run after
    // a dot, not the final total count.
    let mut scan = start;
    while scan > 0 && bytes[scan - 1] != b'.' {
        scan -= 1;
        if bytes[scan].is_ascii_digit() {
            let candidate_end = scan + 1;
            let candidate_start = bytes[..candidate_end]
                .iter()
                .rposition(|b| !b.is_ascii_digit())
                .map_or(0, |idx| idx + 1);
            if bytes[..candidate_start].contains(&b'.') {
                return Some((candidate_start, candidate_end));
            }
            break;
        }
    }

    Some((start, end))
}

fn recovery_output_dir(paths: &[PathBuf], options: &RecoveryOptions) -> PathBuf {
    options.output_dir.clone().unwrap_or_else(|| {
        paths
            .first()
            .and_then(|path| path.parent())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    })
}

fn is_rev_path(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("rev"))
}

fn first_embedded_recovery_record_path(
    paths: &[PathBuf],
) -> RarResult<Option<EmbeddedRecoverySource>> {
    for path in paths {
        if is_rev_path(path) {
            continue;
        }

        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(RarError::Io(err)),
        };

        match probe_volume(&mut file) {
            Ok(probe) if probe.has_recovery_record => {
                return Ok(Some(EmbeddedRecoverySource {
                    path: path.clone(),
                    format: probe.format,
                }));
            }
            Ok(_) | Err(RarError::InvalidSignature) => {}
            Err(RarError::EncryptedArchive) => {}
            Err(err) => return Err(err),
        }
    }

    Ok(None)
}

fn file_name_lossy(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

fn read_padded(file: &mut File, buf: &mut [u8]) -> RarResult<usize> {
    let mut total = 0usize;
    while total < buf.len() {
        match file.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(RarError::Io(e)),
        }
    }
    if total < buf.len() {
        buf[total..].fill(0);
    }
    Ok(total)
}

fn verify_file_crc32_region(path: &Path, offset: u64, expected: Option<u32>) -> RarResult<bool> {
    let actual = crc32_file_region(path, offset)?;
    Ok(expected.is_none_or(|expected| actual == expected))
}

fn crc32_file_prefix(path: &Path, len: u64) -> RarResult<u32> {
    let mut file = File::open(path).map_err(RarError::Io)?;
    let mut remaining = len;
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    while remaining > 0 {
        let max_read = remaining.min(buf.len() as u64) as usize;
        let read = file.read(&mut buf[..max_read]).map_err(RarError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        remaining -= read as u64;
    }
    Ok(hasher.finalize())
}

fn crc32_file_region(path: &Path, offset: u64) -> RarResult<u32> {
    let mut file = File::open(path).map_err(RarError::Io)?;
    file.seek(SeekFrom::Start(offset)).map_err(RarError::Io)?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf).map_err(RarError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize())
}

fn le_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}

fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn le_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

struct SliceReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SliceReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read(&mut self, len: usize) -> RarResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR recovery header offset overflow".into(),
            })?;
        if end > self.data.len() {
            return Err(RarError::CorruptArchive {
                detail: "truncated RAR recovery header".into(),
            });
        }
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn get_u8(&mut self) -> RarResult<u8> {
        Ok(self.read(1)?[0])
    }

    fn get_u16(&mut self) -> RarResult<u16> {
        self.read(2).map(le_u16)
    }

    fn get_u32(&mut self) -> RarResult<u32> {
        self.read(4).map(le_u32)
    }

    fn get_u64(&mut self) -> RarResult<u64> {
        self.read(8).map(le_u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rar4_archive_with_recovery_flag() -> Vec<u8> {
        let mut bytes = b"Rar!\x1a\x07\0".to_vec();
        let mut header = vec![0u8; 7];
        header[2] = 0x73; // MAIN_HEAD
        header[3..5].copy_from_slice(&crate::rar4::types::archive_flags::RECOVERY.to_le_bytes());
        header[5..7].copy_from_slice(&7u16.to_le_bytes());
        let crc = (crc32fast::hash(&header[2..]) & 0xFFFF) as u16;
        header[0..2].copy_from_slice(&crc.to_le_bytes());
        bytes.extend_from_slice(&header);
        bytes
    }

    fn append_rar4_end_data_crc(bytes: &mut Vec<u8>, corrupt: bool) {
        let mut expected = crc32fast::hash(bytes);
        if corrupt {
            expected ^= 0xFFFF_FFFF;
        }

        let mut header = vec![0u8; 11];
        header[2] = 0x7B; // ENDARC_HEAD
        header[3..5].copy_from_slice(&crate::rar4::types::end_flags::DATA_CRC.to_le_bytes());
        header[5..7].copy_from_slice(&11u16.to_le_bytes());
        header[7..11].copy_from_slice(&expected.to_le_bytes());
        let crc = (crc32fast::hash(&header[2..]) & 0xFFFF) as u16;
        header[0..2].copy_from_slice(&crc.to_le_bytes());
        bytes.extend_from_slice(&header);
    }

    fn test_rar5_rev_header(path: &str) -> Rar5RevHeader {
        Rar5RevHeader {
            path: PathBuf::from(path),
            data_count: 1,
            rec_count: 1,
            rec_num: 1,
            rev_crc: 0,
            data_offset: 0,
            data_volumes: Some(vec![Rar5DataVolumeInfo {
                file_size: 0,
                crc32: 0,
            }]),
        }
    }

    fn build_rar5_rev_from_raw(raw: &[u8]) -> Vec<u8> {
        let mut bytes = REV5_SIGN.to_vec();
        let header_size = raw.len() as u32;
        let mut crc_input = header_size.to_le_bytes().to_vec();
        crc_input.extend_from_slice(raw);
        bytes.extend_from_slice(&crc32fast::hash(&crc_input).to_le_bytes());
        bytes.extend_from_slice(&header_size.to_le_bytes());
        bytes.extend_from_slice(raw);
        bytes
    }

    fn build_rar5_rev_bytes(include_table: bool) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.push(1); // version
        raw.extend_from_slice(&1u16.to_le_bytes()); // data count
        raw.extend_from_slice(&1u16.to_le_bytes()); // recovery count
        raw.extend_from_slice(&1u16.to_le_bytes()); // recovery volume number
        raw.extend_from_slice(&0xAABB_CCDDu32.to_le_bytes()); // recovery data CRC
        if include_table {
            raw.extend_from_slice(&123u64.to_le_bytes());
            raw.extend_from_slice(&0x1122_3344u32.to_le_bytes());
        }

        build_rar5_rev_from_raw(&raw)
    }

    fn build_rar5_header(header_type: u64, header_flags: u64, type_body: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&crate::vint::encode_vint(header_type));
        body.extend_from_slice(&crate::vint::encode_vint(header_flags));
        body.extend_from_slice(type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = crate::vint::encode_vint(header_size);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_size_bytes);
        hasher.update(&body);

        let mut result = Vec::new();
        result.extend_from_slice(&hasher.finalize().to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_probe_rar5_archive(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        let mut buf = crate::signature::RAR5_SIGNATURE.to_vec();
        let mut main_body = crate::vint::encode_vint(archive_flags);
        if let Some(volume_number) = volume_number {
            main_body.extend_from_slice(&crate::vint::encode_vint(volume_number));
        }
        buf.extend_from_slice(&build_rar5_header(1, 0, &main_body));
        buf.extend_from_slice(&build_rar5_header(5, 0, &crate::vint::encode_vint(0)));
        buf
    }

    fn path_list_contains(paths: &[PathBuf], needle: &Path) -> bool {
        paths.iter().any(|path| path == needle)
    }

    #[test]
    fn rar_volume_digit_run_finds_part_number() {
        assert_eq!(rar_volume_digit_run("movie.part0007"), Some((10, 14)));
        assert_eq!(rar_volume_digit_run("movie.part01of10"), Some((10, 12)));
        assert_eq!(rar_volume_digit_run("movie"), None);
    }

    #[test]
    fn recovery_path_expansion_discovers_rar5_siblings_from_rev() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("movie.part0002.rev");
        let data = dir.path().join("movie.part0001.rar");
        let next_rev = dir.path().join("movie.part0003.rev");
        let unrelated = dir.path().join("other.part0001.rar");
        for path in [&input, &data, &next_rev, &unrelated] {
            std::fs::write(path, b"x").unwrap();
        }

        let expanded = expand_recovery_paths(std::slice::from_ref(&input)).unwrap();

        assert!(path_list_contains(&expanded, &input));
        assert!(path_list_contains(&expanded, &data));
        assert!(path_list_contains(&expanded, &next_rev));
        assert!(!path_list_contains(&expanded, &unrelated));
    }

    #[test]
    fn recovery_path_expansion_discovers_rar3_old_style_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("movie5_3_1.rev");
        let data = dir.path().join("movie.rar");
        let next_rev = dir.path().join("movie5_3_2.rev");
        let unrelated = dir.path().join("other5_3_1.rev");
        for path in [&input, &data, &next_rev, &unrelated] {
            std::fs::write(path, b"x").unwrap();
        }

        let expanded = expand_recovery_paths(std::slice::from_ref(&input)).unwrap();

        assert!(path_list_contains(&expanded, &input));
        assert!(path_list_contains(&expanded, &data));
        assert!(path_list_contains(&expanded, &next_rev));
        assert!(!path_list_contains(&expanded, &unrelated));
    }

    #[test]
    fn rar5_recovery_data_candidate_accepts_rar_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        std::fs::write(&path, b"not checked before extension acceptance").unwrap();

        assert!(rar5_recovery_data_candidate_allowed(&path).unwrap());
    }

    #[test]
    fn rar5_recovery_data_candidate_rejects_non_rar_direct_archive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.bin");
        std::fs::write(&path, crate::signature::RAR5_SIGNATURE).unwrap();

        assert!(!rar5_recovery_data_candidate_allowed(&path).unwrap());
    }

    #[test]
    fn rar5_recovery_data_candidate_accepts_non_rar_sfx_archive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.exe");
        let mut bytes = vec![0xCCu8; 16];
        bytes.extend_from_slice(&crate::signature::RAR5_SIGNATURE);
        std::fs::write(&path, bytes).unwrap();

        assert!(rar5_recovery_data_candidate_allowed(&path).unwrap());
    }

    #[test]
    fn rar5_recovery_slot_probe_rejects_non_volume_archive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        std::fs::write(&path, build_probe_rar5_archive(0, None)).unwrap();

        assert_eq!(probe_rar5_volume_number(&path).unwrap(), None);
    }

    #[test]
    fn rar5_recovery_slot_probe_accepts_first_volume_without_number() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        std::fs::write(
            &path,
            build_probe_rar5_archive(crate::header::main_archive::flags::VOLUME, None),
        )
        .unwrap();

        assert_eq!(probe_rar5_volume_number(&path).unwrap(), Some(0));
    }

    #[test]
    fn rar5_recovery_slot_probe_accepts_numbered_volume() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part03.rar");
        std::fs::write(
            &path,
            build_probe_rar5_archive(
                crate::header::main_archive::flags::VOLUME
                    | crate::header::main_archive::flags::VOLUME_NUMBER,
                Some(2),
            ),
        )
        .unwrap();

        assert_eq!(probe_rar5_volume_number(&path).unwrap(), Some(2));
    }

    #[test]
    fn rar5_recovery_slot_probe_uses_filename_number_like_rar_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part03.rar");
        std::fs::write(
            &path,
            build_probe_rar5_archive(
                crate::header::main_archive::flags::VOLUME
                    | crate::header::main_archive::flags::VOLUME_NUMBER,
                Some(9),
            ),
        )
        .unwrap();

        assert_eq!(probe_rar5_volume_number(&path).unwrap(), Some(2));
    }

    #[test]
    fn rar5_recovery_slot_probe_rejects_volume_without_filename_number() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.rar");
        std::fs::write(
            &path,
            build_probe_rar5_archive(crate::header::main_archive::flags::VOLUME, None),
        )
        .unwrap();

        assert_eq!(probe_rar5_volume_number(&path).unwrap(), None);
    }

    #[test]
    fn rar5_rev_parser_reads_full_data_volume_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part0002.rev");
        std::fs::write(&path, build_rar5_rev_bytes(true)).unwrap();

        let header = read_rar5_rev_header(&path).unwrap().unwrap();
        let volumes = header.data_volumes.unwrap();

        assert_eq!(header.data_count, 1);
        assert_eq!(header.rec_count, 1);
        assert_eq!(header.rec_num, 1);
        assert_eq!(volumes[0].file_size, 123);
        assert_eq!(volumes[0].crc32, 0x1122_3344);
    }

    #[test]
    fn rar5_rev_parser_accepts_compact_non_first_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part0002.rev");
        std::fs::write(&path, build_rar5_rev_bytes(false)).unwrap();

        let header = read_rar5_rev_header(&path).unwrap().unwrap();

        assert_eq!(header.data_count, 1);
        assert_eq!(header.rec_count, 1);
        assert_eq!(header.rec_num, 1);
        assert!(header.data_volumes.is_none());
    }

    #[test]
    fn rar5_rev_parser_ignores_truncated_header_body_like_rar_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part0002.rev");
        let mut bytes = REV5_SIGN.to_vec();
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&[1, 0, 1]);
        std::fs::write(&path, bytes).unwrap();

        assert!(read_rar5_rev_header(&path).unwrap().is_none());
    }

    #[test]
    fn rar5_rev_parser_ignores_crc_valid_short_metadata_like_rar_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part0002.rev");
        std::fs::write(&path, build_rar5_rev_from_raw(&[1, 1, 0])).unwrap();

        assert!(read_rar5_rev_header(&path).unwrap().is_none());
    }

    #[test]
    fn rar5_recovery_reference_prefers_existing_data_volume() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("movie.part01.rar");
        let rev_path = dir.path().join("movie.part0000000002.rev");
        std::fs::write(
            &data_path,
            build_probe_rar5_archive(crate::header::main_archive::flags::VOLUME, None),
        )
        .unwrap();
        let paths = vec![data_path.clone(), rev_path.clone()];
        let headers = vec![test_rar5_rev_header(rev_path.to_str().unwrap())];

        let reference = rar5_recovery_reference_name(&paths, &headers).unwrap();

        assert_eq!(reference.as_path(), data_path);
    }

    #[test]
    fn rar5_recovery_reference_ignores_non_volume_rar_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("movie.part01.rar");
        let short_rev_path = dir.path().join("movie.part2.rev");
        let long_rev_path = dir.path().join("movie.part0000000010.rev");
        std::fs::write(&data_path, build_probe_rar5_archive(0, None)).unwrap();
        let paths = vec![data_path, short_rev_path.clone(), long_rev_path.clone()];
        let headers = vec![
            test_rar5_rev_header(short_rev_path.to_str().unwrap()),
            test_rar5_rev_header(long_rev_path.to_str().unwrap()),
        ];

        let reference = rar5_recovery_reference_name(&paths, &headers).unwrap();

        assert_eq!(reference.as_path(), long_rev_path);
    }

    #[test]
    fn rar5_recovery_reference_uses_longest_rev_when_no_data_volume() {
        let paths = vec![
            PathBuf::from("/tmp/movie.part2.rev"),
            PathBuf::from("/tmp/movie.part0000000010.rev"),
        ];
        let headers = vec![
            test_rar5_rev_header("/tmp/movie.part2.rev"),
            test_rar5_rev_header("/tmp/movie.part0000000010.rev"),
        ];

        let reference = rar5_recovery_reference_name(&paths, &headers).unwrap();

        assert_eq!(
            reference.as_path(),
            Path::new("/tmp/movie.part0000000010.rev")
        );
        assert_eq!(
            infer_numbered_volume_path(&reference, Path::new("/out"), 0, "rar"),
            Path::new("/out/movie.part0000000001.rar")
        );
    }

    #[test]
    fn rar5_recovery_renames_invalid_existing_data_volume_like_rar_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        std::fs::write(&path, b"bad data").unwrap();
        let mut slot = Rar5DataSlot {
            path: Some(path.clone()),
            output_path: path.clone(),
            file_size: 100,
            crc32: 0,
            valid: false,
        };

        rename_invalid_rar5_data_volume(&mut slot, &RecoveryOptions::default()).unwrap();

        assert!(slot.path.is_none());
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(bad_volume_path(&path)).unwrap(),
            b"bad data".to_vec()
        );
    }

    #[test]
    fn rar5_recovery_keeps_invalid_source_when_output_dir_differs() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("in").join("movie.part01.rar");
        let output = dir.path().join("out").join("movie.part01.rar");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, b"bad data").unwrap();
        let mut slot = Rar5DataSlot {
            path: Some(source.clone()),
            output_path: output,
            file_size: 100,
            crc32: 0,
            valid: false,
        };

        rename_invalid_rar5_data_volume(&mut slot, &RecoveryOptions::default()).unwrap();

        assert_eq!(slot.path.as_deref(), Some(source.as_path()));
        assert_eq!(std::fs::read(source).unwrap(), b"bad data".to_vec());
    }

    #[test]
    fn rar5_recovery_refuses_to_clobber_existing_bad_backup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        let bad_path = bad_volume_path(&path);
        std::fs::write(&path, b"bad data").unwrap();
        std::fs::write(&bad_path, b"previous backup").unwrap();
        let mut slot = Rar5DataSlot {
            path: Some(path.clone()),
            output_path: path.clone(),
            file_size: 100,
            crc32: 0,
            valid: false,
        };

        let err = rename_invalid_rar5_data_volume(&mut slot, &RecoveryOptions::default())
            .expect_err("existing .bad backup should be protected");

        assert!(matches!(err, RarError::Io(_)));
        assert!(path.exists());
        assert_eq!(
            std::fs::read(bad_path).unwrap(),
            b"previous backup".to_vec()
        );
    }

    #[test]
    fn rar3_data_volume_validity_accepts_rar4_family_archive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        std::fs::write(&path, rar4_archive_with_recovery_flag()).unwrap();

        assert!(rar3_data_volume_is_valid(&path).unwrap());
    }

    #[test]
    fn rar3_data_volume_validity_rejects_bad_signature() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        std::fs::write(&path, b"not a rar").unwrap();

        assert!(!rar3_data_volume_is_valid(&path).unwrap());
    }

    #[test]
    fn rar3_data_volume_validity_accepts_matching_end_data_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        let mut bytes = rar4_archive_with_recovery_flag();
        append_rar4_end_data_crc(&mut bytes, false);
        std::fs::write(&path, bytes).unwrap();

        assert!(rar3_data_volume_is_valid(&path).unwrap());
    }

    #[test]
    fn rar3_data_volume_validity_rejects_mismatched_end_data_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01.rar");
        let mut bytes = rar4_archive_with_recovery_flag();
        append_rar4_end_data_crc(&mut bytes, true);
        std::fs::write(&path, bytes).unwrap();

        assert!(!rar3_data_volume_is_valid(&path).unwrap());
    }

    #[test]
    fn rar3_recovery_reference_requires_valid_data_volume() {
        let paths = vec![PathBuf::from("/tmp/movie.part01_02_03.rev")];

        let err = rar3_recovery_reference_name(&paths)
            .expect_err("RAR3 .rev-only restore needs a data volume");

        match err {
            RarError::CorruptArchive { detail } => {
                assert!(detail.contains("requires at least one valid data volume"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rar3_recovery_reference_skips_invalid_data_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let invalid = dir.path().join("movie.part01.rar");
        let valid = dir.path().join("movie.part02.rar");
        std::fs::write(&invalid, b"not a rar").unwrap();
        std::fs::write(&valid, rar4_archive_with_recovery_flag()).unwrap();
        let paths = vec![invalid, valid.clone()];

        let reference = rar3_recovery_reference_name(&paths).unwrap();

        assert_eq!(reference, valid.as_path());
    }

    #[test]
    fn infer_numbered_volume_preserves_width() {
        let path = Path::new("/tmp/movie.part0001.rar");
        let output = infer_numbered_volume_path(path, Path::new("/out"), 6, "rar");
        assert_eq!(output, Path::new("/out/movie.part0007.rar"));
    }

    #[test]
    fn infer_numbered_volume_uses_first_part_digits_like_rar_behavior() {
        let path = Path::new("/tmp/movie.part01of10.rar");
        let output = infer_numbered_volume_path(path, Path::new("/out"), 1, "rar");
        assert_eq!(output, Path::new("/out/movie.part02of10.rar"));
    }

    #[test]
    fn infer_old_rar3_volume_path_preserves_r00_numbering() {
        let reference = Path::new("/tmp/movie.r00");

        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 0),
            Path::new("/out/movie.rar")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 1),
            Path::new("/out/movie.r00")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 2),
            Path::new("/out/movie.r01")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 100),
            Path::new("/out/movie.r99")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 101),
            Path::new("/out/movie.s00")
        );
    }

    #[test]
    fn infer_old_rar3_volume_path_preserves_numeric_numbering() {
        let reference = Path::new("/tmp/movie.001");

        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 0),
            Path::new("/out/movie.001")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 1),
            Path::new("/out/movie.002")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 998),
            Path::new("/out/movie.999")
        );
        assert_eq!(
            infer_old_rar3_volume_path(reference, Path::new("/out"), 999),
            Path::new("/out/movie.a00")
        );
    }

    #[test]
    fn parse_rar3_old_numbered_data_volume_names() {
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.rar")),
            Some(0)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.r00")),
            Some(1)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.r01")),
            Some(2)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.r99")),
            Some(100)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.s00")),
            Some(101)
        );
    }

    #[test]
    fn parse_rar3_numeric_old_numbered_data_volume_names() {
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.001")),
            Some(0)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.002")),
            Some(1)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.999")),
            Some(998)
        );
        assert_eq!(
            parse_rar3_data_volume_number(Path::new("movie.a00")),
            Some(999)
        );
        assert_eq!(parse_rar3_data_volume_number(Path::new("movie.000")), None);
    }

    #[test]
    fn rar3_footer_parser_accepts_new_style_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part03.rev");
        let mut bytes = b"payload".to_vec();
        // Footer bytes are read as P[2], P[1], P[0], each stored minus one.
        bytes.extend_from_slice(&[2, 1, 0]);
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();

        let header = read_rar3_rev_header(&path).unwrap().unwrap();
        assert_eq!(header.data_count, 3);
        assert_eq!(header.rec_count, 2);
        assert_eq!(header.rec_position, 1);
        assert!(header.new_style);
    }

    #[test]
    fn rar3_rev_parser_accepts_old_style_name_without_footer_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie5_3_1.rev");
        std::fs::write(&path, b"old style recovery payload").unwrap();

        let header = read_rar3_rev_header(&path).unwrap().unwrap();

        assert_eq!(header.data_count, 5);
        assert_eq!(header.rec_count, 3);
        assert_eq!(header.rec_position, 1);
        assert!(!header.new_style);
    }

    #[test]
    fn rar3_restored_output_footer_scrub_is_new_style_only() {
        let dir = tempfile::tempdir().unwrap();
        let old_style = dir.path().join("old-style.rar");
        let new_style = dir.path().join("new-style.rar");
        let payload = b"archive bytes plus footer";
        std::fs::write(&old_style, payload).unwrap();
        std::fs::write(&new_style, payload).unwrap();

        let mut old_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&old_style)
            .unwrap();
        finalize_rar3_restored_output(&mut old_file, false, false).unwrap();
        drop(old_file);

        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&new_style)
            .unwrap();
        finalize_rar3_restored_output(&mut new_file, true, false).unwrap();
        drop(new_file);

        assert_eq!(std::fs::read(&old_style).unwrap(), payload);

        let mut expected = payload.to_vec();
        let len = expected.len();
        expected[len - 7..].fill(0);
        assert_eq!(std::fs::read(&new_style).unwrap(), expected);
    }

    #[test]
    fn rar3_last_restored_volume_truncates_zero_padding_after_endarc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("last-volume.rar");
        let mut bytes = crate::signature::RAR4_SIGNATURE.to_vec();
        bytes.extend_from_slice(&[
            0x00, 0x00, // CRC16 placeholder; raw parser tolerates mismatch.
            0x7B, // ENDARC
            0x00, 0x00, // flags
            0x07, 0x00, // header size
        ]);
        let endarc_offset = bytes.len();
        bytes.extend_from_slice(&[0u8; 32]);
        std::fs::write(&path, bytes).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        finalize_rar3_restored_output(&mut file, false, true).unwrap();
        drop(file);

        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            endarc_offset as u64
        );
    }

    #[test]
    fn rar3_rev_parser_rejects_invalid_old_style_name_params() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie5_0_1.rev");
        std::fs::write(&path, b"old style recovery payload").unwrap();

        assert!(read_rar3_rev_header(&path).unwrap().is_none());
    }

    #[test]
    fn restore_reports_embedded_recovery_records_are_not_fake_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.rar");
        std::fs::write(&path, rar4_archive_with_recovery_flag()).unwrap();

        let err = restore_volumes_from_paths(&[path], &RecoveryOptions::default()).unwrap_err();
        match err {
            RarError::CorruptArchive { detail } => {
                assert!(detail.contains("embedded Rar4 recovery record detected"));
                assert!(detail.contains("standalone .rev recovery volumes only"));
                assert!(detail.contains("standalone .rev recovery volumes only"));
                assert!(detail.contains("does not consume embedded RR/protect data"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
