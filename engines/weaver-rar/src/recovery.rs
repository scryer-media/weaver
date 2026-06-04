//! RAR recovery-volume restore support.
//!
//! RAR recovery volumes are not PAR2 files. RAR5 uses UnRAR's GF(2^16)
//! `RSCoder16` Cauchy matrix semantics, while legacy RAR3 uses a byte-wise
//! GF(2^8) erasure coder. This module keeps that restore logic separate from
//! archive extraction so existing extraction APIs remain unchanged.

use rayon::prelude::*;
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

pub fn restore_volumes_from_paths(
    paths: &[PathBuf],
    options: &RecoveryOptions,
) -> RarResult<RecoveryReport> {
    if paths.is_empty() {
        return Err(RarError::CorruptArchive {
            detail: "RAR recovery restore requires at least one path".into(),
        });
    }

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

    Err(RarError::CorruptArchive {
        detail: "no valid RAR recovery volumes were found".into(),
    })
}

#[derive(Debug, Clone)]
struct Rar5RevHeader {
    path: PathBuf,
    data_count: usize,
    rec_count: usize,
    rec_num: usize,
    rev_crc: u32,
    data_offset: u64,
    data_volumes: Vec<Rar5DataVolumeInfo>,
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
    let first = &rev_headers[0];
    let data_count = first.data_count;
    let rec_count = first.rec_count;
    let total_count = data_count + rec_count;

    let mut data_slots = first
        .data_volumes
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

    for header in rev_headers {
        if header.data_count != data_count
            || header.rec_count != rec_count
            || header.data_volumes.len() != data_count
        {
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
    let reference_name = paths
        .iter()
        .find(|path| !is_rev_path(path))
        .or_else(|| paths.first())
        .ok_or_else(|| RarError::CorruptArchive {
            detail: "no input paths available for volume name inference".into(),
        })?;

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
                infer_numbered_volume_path(reference_name, &output_dir, index, "rar")
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
    file.read_exact(&mut raw).map_err(RarError::Io)?;
    let mut crc_input = Vec::with_capacity(4 + raw.len());
    crc_input.extend_from_slice(&prefix[REV5_SIGN.len() + 4..REV5_SIGN.len() + 8]);
    crc_input.extend_from_slice(&raw);
    if crc32fast::hash(&crc_input) != block_crc {
        return Ok(None);
    }

    let mut reader = SliceReader::new(&raw);
    let version = reader.get_u8()?;
    if version != 1 {
        return Ok(None);
    }

    let data_count = reader.get_u16()? as usize;
    let rec_count = reader.get_u16()? as usize;
    let total_count = data_count + rec_count;
    let rec_num = reader.get_u16()? as usize;
    if data_count == 0 || rec_count == 0 || total_count > MAX_RAR5_VOLUMES || rec_num >= total_count
    {
        return Ok(None);
    }
    let rev_crc = reader.get_u32()?;

    let mut data_volumes = Vec::with_capacity(data_count);
    for _ in 0..data_count {
        data_volumes.push(Rar5DataVolumeInfo {
            file_size: reader.get_u64()?,
            crc32: reader.get_u32()?,
        });
    }

    Ok(Some(Rar5RevHeader {
        path: path.to_path_buf(),
        data_count,
        rec_count,
        rec_num,
        rev_crc,
        data_offset: REV5_PREFIX_LEN as u64 + header_size as u64,
        data_volumes,
    }))
}

#[derive(Debug, Clone)]
struct Rar3RevHeader {
    path: PathBuf,
    rec_position: usize,
    rec_count: usize,
    data_count: usize,
}

fn restore_rar3(
    paths: &[PathBuf],
    options: &RecoveryOptions,
    rev_headers: Vec<Rar3RevHeader>,
) -> RarResult<RecoveryReport> {
    let first = &rev_headers[0];
    let data_count = first.data_count;
    let rec_count = first.rec_count;
    let total_count = data_count + rec_count;
    if total_count > 255 {
        return Err(RarError::CorruptArchive {
            detail: "RAR3 recovery set exceeds 255 total volumes".into(),
        });
    }

    let output_dir = recovery_output_dir(paths, options);
    let reference_name = paths
        .iter()
        .find(|path| !is_rev_path(path))
        .or_else(|| paths.first())
        .ok_or_else(|| RarError::CorruptArchive {
            detail: "no input paths available for RAR3 volume name inference".into(),
        })?;

    let mut slots = vec![None::<PathBuf>; total_count];
    for header in rev_headers {
        if header.data_count != data_count || header.rec_count != rec_count {
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
            slots[index] = Some(path.clone());
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

    let restored_paths = missing_volume_numbers
        .iter()
        .map(|&idx| infer_numbered_volume_path(reference_name, &output_dir, idx, "rar"))
        .collect::<Vec<_>>();
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

fn reconstruct_rar3(
    slots: &[Option<PathBuf>],
    data_count: usize,
    rec_count: usize,
    missing_volume_numbers: &[usize],
    restored_paths: &[PathBuf],
    options: &RecoveryOptions,
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
                let mut coder = Rar3RsCoder::new(rec_count)
                    .expect("rec_count was validated before RAR3 reconstruction");
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

    for output in &mut outputs {
        output.flush().map_err(RarError::Io)?;
        let len = output.seek(SeekFrom::End(0)).map_err(RarError::Io)?;
        if len >= 7 {
            output
                .seek(SeekFrom::Start(len - 7))
                .map_err(RarError::Io)?;
            output.write_all(&[0u8; 7]).map_err(RarError::Io)?;
            output.flush().map_err(RarError::Io)?;
        }
    }
    Ok(())
}

fn read_rar3_rev_header(path: &Path) -> RarResult<Option<Rar3RevHeader>> {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(e) => return Err(RarError::Io(e)),
    };
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
    }))
}

fn probe_rar5_volume_number(path: &Path) -> RarResult<Option<usize>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) => return Err(RarError::Io(e)),
    };
    match probe_volume(&mut file) {
        Ok(probe) if probe.format == ArchiveFormat::Rar5 => Ok(probe.volume_number.or(Some(0))),
        Ok(_) | Err(RarError::InvalidSignature) => Ok(None),
        Err(e) => Err(e),
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
    if ext.len() == 3 && ext.starts_with('r') {
        let number = ext[1..].parse::<usize>().ok()?;
        return Some(number + 1);
    }
    None
}

fn parse_part_number(path: &Path) -> Option<usize> {
    let stem = path.file_stem()?.to_string_lossy();
    let bytes = stem.as_bytes();
    let end = bytes.iter().rposition(|b| b.is_ascii_digit())? + 1;
    let start = bytes[..end]
        .iter()
        .rposition(|b| !b.is_ascii_digit())
        .map_or(0, |idx| idx + 1);
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
    let generated_name = if let Some((start, end)) = last_digit_run(&stem) {
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

fn last_digit_run(value: &str) -> Option<(usize, usize)> {
    let bytes = value.as_bytes();
    let end = bytes.iter().rposition(|b| b.is_ascii_digit())? + 1;
    let start = bytes[..end]
        .iter()
        .rposition(|b| !b.is_ascii_digit())
        .map_or(0, |idx| idx + 1);
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

    #[test]
    fn last_digit_run_finds_part_number() {
        assert_eq!(last_digit_run("movie.part0007"), Some((10, 14)));
        assert_eq!(last_digit_run("movie"), None);
    }

    #[test]
    fn infer_numbered_volume_preserves_width() {
        let path = Path::new("/tmp/movie.part0001.rar");
        let output = infer_numbered_volume_path(path, Path::new("/out"), 6, "rar");
        assert_eq!(output, Path::new("/out/movie.part0007.rar"));
    }

    #[test]
    fn rar3_footer_parser_accepts_new_style_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("movie.part01_02_03.rev");
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
    }
}
