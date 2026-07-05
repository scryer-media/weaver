use std::fs::{self, File};
use std::io::{self, Read, Seek};
use std::path::{Component, Path, PathBuf};

use tempfile::TempDir;
use weaver_model::files::{FileRole, archive_base_name};

use crate::security::RuntimeSecurityConfig;

trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

#[derive(Debug, Clone)]
pub(super) struct ExtractedNzb {
    pub filename: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Default)]
pub(super) struct IntakeOutput {
    pub nzbs: Vec<ExtractedNzb>,
    pub permanent_errors: Vec<String>,
}

impl IntakeOutput {
    fn one(nzb: ExtractedNzb) -> Self {
        Self {
            nzbs: vec![nzb],
            permanent_errors: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum IntakeError {
    #[error("transient intake error: {0}")]
    Transient(String),
    #[error("permanent intake error: {0}")]
    Permanent(String),
}

pub(super) fn is_supported_candidate(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".nzb")
        || lower.ends_with(".nzb.gz")
        || lower.ends_with(".nzb.zst")
        || lower.ends_with(".nzb.zstd")
        || matches!(
            FileRole::from_filename(name),
            FileRole::RarVolume { volume_number: 0 }
                | FileRole::SevenZipArchive
                | FileRole::SevenZipSplit { number: 0 }
                | FileRole::ZipArchive
                | FileRole::TarArchive
                | FileRole::TarGzArchive
                | FileRole::TarBz2Archive
                | FileRole::GzArchive
                | FileRole::DeflateArchive
                | FileRole::BrotliArchive
                | FileRole::ZstdArchive
                | FileRole::Bzip2Archive
                | FileRole::SplitFile { number: 0 }
        )
}

pub(super) fn related_candidate_paths(path: &Path) -> Result<Vec<PathBuf>, IntakeError> {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return Ok(vec![path.to_path_buf()]);
    };
    match FileRole::from_filename(name) {
        FileRole::RarVolume { volume_number: 0 } => adjacent_archive_parts(path, name, |role| {
            matches!(role, FileRole::RarVolume { .. })
        }),
        FileRole::SevenZipSplit { number: 0 } => adjacent_archive_parts(path, name, |role| {
            matches!(role, FileRole::SevenZipSplit { .. })
        }),
        FileRole::SplitFile { number: 0 } => adjacent_archive_parts(path, name, |role| {
            matches!(role, FileRole::SplitFile { .. })
        }),
        _ => Ok(vec![path.to_path_buf()]),
    }
}

#[cfg(test)]
pub(super) fn read_nzbs_from_path(path: &Path) -> Result<IntakeOutput, IntakeError> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| IntakeError::Permanent("input filename is not valid UTF-8".to_string()))?
        .to_string();
    read_nzbs_from_path_with_name(path, &name)
}

pub(super) fn read_nzbs_from_path_with_name(
    path: &Path,
    name: &str,
) -> Result<IntakeOutput, IntakeError> {
    let limit = RuntimeSecurityConfig::from_env_or_default_for_tests().nzb_decompressed_limit_bytes;
    let lower = name.to_ascii_lowercase();
    if lower.ends_with(".nzb") {
        return Ok(IntakeOutput::one(ExtractedNzb {
            filename: name.to_string(),
            bytes: read_file_limited(path, limit)?,
        }));
    }

    match FileRole::from_filename(name) {
        FileRole::ZipArchive => extract_zip_nzbs(path, limit),
        FileRole::TarArchive => extract_tar_nzbs(File::open(path).map_err(transient_io)?, limit),
        FileRole::TarGzArchive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = flate2::read::GzDecoder::new(file);
            extract_tar_nzbs(reader, limit)
        }
        FileRole::TarBz2Archive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = bzip2::read::BzDecoder::new(file);
            extract_tar_nzbs(reader, limit)
        }
        FileRole::GzArchive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = flate2::read::GzDecoder::new(file);
            Ok(IntakeOutput::one(single_stream_nzb(
                name,
                &[".gz"],
                reader,
                limit,
            )?))
        }
        FileRole::DeflateArchive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = flate2::read::DeflateDecoder::new(file);
            Ok(IntakeOutput::one(single_stream_nzb(
                name,
                &[".deflate"],
                reader,
                limit,
            )?))
        }
        FileRole::BrotliArchive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = brotli::Decompressor::new(file, 4096);
            Ok(IntakeOutput::one(single_stream_nzb(
                name,
                &[".br"],
                reader,
                limit,
            )?))
        }
        FileRole::ZstdArchive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = zstd::stream::read::Decoder::new(file)
                .map_err(|error| IntakeError::Permanent(format!("failed to open zstd: {error}")))?;
            Ok(IntakeOutput::one(single_stream_nzb(
                name,
                &[".zstd", ".zst"],
                reader,
                limit,
            )?))
        }
        FileRole::Bzip2Archive => {
            let file = File::open(path).map_err(transient_io)?;
            let reader = bzip2::read::BzDecoder::new(file);
            Ok(IntakeOutput::one(single_stream_nzb(
                name,
                &[".bz2"],
                reader,
                limit,
            )?))
        }
        FileRole::SevenZipArchive | FileRole::SevenZipSplit { number: 0 } => {
            extract_7z_nzbs(path, name, limit)
        }
        FileRole::RarVolume { volume_number: 0 } => extract_rar_nzbs(path, name, limit),
        FileRole::SplitFile { number: 0 } => extract_split_nzb(path, name, limit),
        FileRole::RarVolume { .. }
        | FileRole::SevenZipSplit { .. }
        | FileRole::SplitFile { .. } => Err(IntakeError::Transient(
            "waiting for first archive volume".to_string(),
        )),
        _ => Err(IntakeError::Permanent(format!(
            "unsupported watched-folder input: {name}"
        ))),
    }
    .and_then(require_nzb_entries)
}

fn require_nzb_entries(output: IntakeOutput) -> Result<IntakeOutput, IntakeError> {
    if output.nzbs.is_empty() {
        if output.permanent_errors.is_empty() {
            Err(IntakeError::Permanent(
                "input did not contain any NZB files".to_string(),
            ))
        } else {
            Err(IntakeError::Permanent(output.permanent_errors.join("; ")))
        }
    } else {
        Ok(output)
    }
}

fn read_file_limited(path: &Path, limit: u64) -> Result<Vec<u8>, IntakeError> {
    let file = File::open(path).map_err(transient_io)?;
    read_limited(file, limit).map_err(transient_io)
}

fn read_limited<R: Read>(mut reader: R, limit: u64) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            return Ok(out);
        }
        let next = out.len().saturating_add(read);
        if next as u64 > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("NZB exceeds {limit} bytes"),
            ));
        }
        out.extend_from_slice(&buf[..read]);
    }
}

fn single_stream_nzb<R: Read>(
    input_name: &str,
    suffixes: &[&str],
    reader: R,
    limit: u64,
) -> Result<ExtractedNzb, IntakeError> {
    let filename = strip_ascii_case_suffixes(input_name, suffixes)
        .unwrap_or(input_name)
        .to_string();
    let bytes = read_limited(reader, limit).map_err(|error| {
        IntakeError::Permanent(format!("failed to decompress {input_name}: {error}"))
    })?;
    Ok(ExtractedNzb { filename, bytes })
}

fn extract_zip_nzbs(path: &Path, limit: u64) -> Result<IntakeOutput, IntakeError> {
    let file = File::open(path).map_err(transient_io)?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|error| IntakeError::Permanent(format!("failed to read zip archive: {error}")))?;
    let mut output = IntakeOutput::default();

    for i in 0..archive.len() {
        let mut entry = match archive.by_index(i) {
            Ok(entry) => entry,
            Err(error) => {
                output
                    .permanent_errors
                    .push(format!("failed to read zip entry {i}: {error}"));
                continue;
            }
        };
        if entry.is_dir() {
            continue;
        }
        let entry_name = entry.name().to_string();
        let safe_path = match validate_archive_entry_path(&entry_name, "zip") {
            Ok(path) => path,
            Err(error) => {
                output.permanent_errors.push(error.to_string());
                continue;
            }
        };
        if !is_nzb_member_path(&safe_path) {
            continue;
        }
        match read_limited(&mut entry, limit) {
            Ok(bytes) => output.nzbs.push(ExtractedNzb {
                filename: member_filename(&safe_path),
                bytes,
            }),
            Err(error) => output.permanent_errors.push(format!(
                "failed to read zip NZB member {entry_name}: {error}"
            )),
        }
    }

    Ok(output)
}

fn extract_tar_nzbs<R: Read>(reader: R, limit: u64) -> Result<IntakeOutput, IntakeError> {
    let mut archive = tar::Archive::new(reader);
    let mut output = IntakeOutput::default();

    let tar_entries = archive
        .entries()
        .map_err(|error| IntakeError::Permanent(format!("failed to read tar entries: {error}")))?;
    for entry in tar_entries {
        let mut entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                output
                    .permanent_errors
                    .push(format!("failed to read tar entry: {error}"));
                break;
            }
        };
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let raw_path = match entry.path() {
            Ok(path) => path,
            Err(error) => {
                output
                    .permanent_errors
                    .push(format!("invalid tar entry path: {error}"));
                continue;
            }
        };
        let raw_name = raw_path.to_string_lossy().to_string();
        if raw_name.contains('\\') {
            output
                .permanent_errors
                .push(format!("unsafe tar entry path: {raw_name}"));
            continue;
        }
        let safe_path = match validate_archive_entry_path(&raw_name, "tar") {
            Ok(path) => path,
            Err(error) => {
                output.permanent_errors.push(error.to_string());
                continue;
            }
        };
        if !is_nzb_member_path(&safe_path) {
            continue;
        }
        match read_limited(&mut entry, limit) {
            Ok(bytes) => output.nzbs.push(ExtractedNzb {
                filename: member_filename(&safe_path),
                bytes,
            }),
            Err(error) => output
                .permanent_errors
                .push(format!("failed to read tar NZB member {raw_name}: {error}")),
        }
    }

    Ok(output)
}

fn extract_7z_nzbs(path: &Path, name: &str, limit: u64) -> Result<IntakeOutput, IntakeError> {
    let role = FileRole::from_filename(name);
    let reader: Box<dyn ReadSeek> = match role {
        FileRole::SevenZipSplit { number: 0 } => {
            let parts = adjacent_archive_parts(path, name, |role| {
                matches!(role, FileRole::SevenZipSplit { .. })
            })?;
            Box::new(
                crate::pipeline::archive::split_reader::SplitFileReader::open(&parts)
                    .map_err(transient_io)?,
            )
        }
        _ => Box::new(File::open(path).map_err(transient_io)?),
    };
    let temp = TempDir::new()
        .map_err(|error| IntakeError::Transient(format!("failed to create temp dir: {error}")))?;
    let password = sevenz_rust2::Password::empty();
    let mut output = IntakeOutput::default();

    let extract_fn = |entry: &sevenz_rust2::ArchiveEntry,
                      reader: &mut dyn Read,
                      _dest: &PathBuf|
     -> Result<bool, sevenz_rust2::Error> {
        if entry.is_directory() {
            return Ok(true);
        }
        let safe_path = match validate_archive_entry_path(entry.name(), "7z") {
            Ok(path) => path,
            Err(error) => {
                output.permanent_errors.push(error.to_string());
                return Ok(true);
            }
        };
        if is_nzb_member_path(&safe_path) {
            match read_limited(reader, limit) {
                Ok(bytes) => output.nzbs.push(ExtractedNzb {
                    filename: member_filename(&safe_path),
                    bytes,
                }),
                Err(error) => output.permanent_errors.push(format!(
                    "failed to read 7z NZB member {}: {error}",
                    entry.name()
                )),
            }
        }
        Ok(true)
    };

    let result = sevenz_rust2::decompress_with_extract_fn_and_password(
        reader,
        temp.path(),
        password,
        extract_fn,
    );
    if let Err(error) = result {
        let reason = format!("7z extraction failed: {error}");
        if output.nzbs.is_empty() {
            return Err(classify_archive_extraction_error(reason));
        }
        output.permanent_errors.push(reason);
    }
    Ok(output)
}

fn extract_rar_nzbs(path: &Path, name: &str, limit: u64) -> Result<IntakeOutput, IntakeError> {
    let first = File::open(path).map_err(transient_io)?;
    let mut archive = weaver_unrar::RarArchive::open(first)
        .map_err(|error| IntakeError::Permanent(format!("failed to read RAR archive: {error}")))?;

    let parts = adjacent_archive_parts(path, name, |role| {
        matches!(role, FileRole::RarVolume { .. })
    })?;
    for part in parts.into_iter().skip(1) {
        let Some(part_name) = part.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let FileRole::RarVolume { volume_number } = FileRole::from_filename(part_name) else {
            continue;
        };
        let file = File::open(&part).map_err(transient_io)?;
        archive
            .add_volume(volume_number as usize, Box::new(file))
            .map_err(|error| {
                IntakeError::Permanent(format!("failed to attach RAR volume: {error}"))
            })?;
    }

    let mut output = IntakeOutput::default();
    let options = weaver_unrar::ExtractOptions::default();
    let metadata = archive.metadata();
    for (idx, member) in metadata.members.iter().enumerate() {
        if member.is_directory || !member.name.to_ascii_lowercase().ends_with(".nzb") {
            continue;
        }
        let safe_path = match validate_archive_entry_path(&member.name, "rar") {
            Ok(path) => path,
            Err(error) => {
                output.permanent_errors.push(error.to_string());
                continue;
            }
        };
        let extracted = match archive.extract_member(idx, &options, None) {
            Ok(extracted) => extracted,
            Err(error) => match rar_member_error(error.to_string()) {
                IntakeError::Transient(reason) => return Err(IntakeError::Transient(reason)),
                IntakeError::Permanent(reason) => {
                    output.permanent_errors.push(format!(
                        "failed to extract RAR NZB member {}: {reason}",
                        member.name
                    ));
                    continue;
                }
            },
        };
        let bytes = match extracted.into_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                output.permanent_errors.push(format!(
                    "failed to read RAR NZB member {}: {error}",
                    member.name
                ));
                continue;
            }
        };
        if bytes.len() as u64 > limit {
            output.permanent_errors.push(format!(
                "RAR NZB member {} exceeds {limit} bytes",
                member.name
            ));
            continue;
        }
        output.nzbs.push(ExtractedNzb {
            filename: member_filename(&safe_path),
            bytes,
        });
    }

    Ok(output)
}

fn extract_split_nzb(path: &Path, name: &str, limit: u64) -> Result<IntakeOutput, IntakeError> {
    let parts = adjacent_archive_parts(path, name, |role| {
        matches!(role, FileRole::SplitFile { .. })
    })?;
    let reader = crate::pipeline::archive::split_reader::SplitFileReader::open(&parts)
        .map_err(transient_io)?;
    let filename = strip_ascii_case_suffixes(name, &[".001"])
        .unwrap_or(name)
        .to_string();
    let bytes = read_limited(reader, limit).map_err(transient_io)?;
    Ok(IntakeOutput::one(ExtractedNzb { filename, bytes }))
}

fn adjacent_archive_parts(
    path: &Path,
    name: &str,
    matches_role: impl Fn(&FileRole) -> bool,
) -> Result<Vec<PathBuf>, IntakeError> {
    let dir = path
        .parent()
        .ok_or_else(|| IntakeError::Transient("input has no parent directory".to_string()))?;
    let role = FileRole::from_filename(name);
    let base = archive_base_name(name, &role).ok_or_else(|| {
        IntakeError::Permanent(format!("could not derive archive set for {name}"))
    })?;
    let mut parts = vec![(archive_volume_number(&role), path.to_path_buf())];

    let entries = fs::read_dir(dir).map_err(transient_io)?;
    for entry in entries {
        let entry = entry.map_err(transient_io)?;
        let part_path = entry.path();
        if part_path == path {
            continue;
        }
        if !part_path.is_file() {
            continue;
        }
        let Some(part_name) = part_path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let part_role = FileRole::from_filename(part_name);
        if !matches_role(&part_role) {
            continue;
        }
        if archive_base_name(part_name, &part_role).as_deref() == Some(base.as_str()) {
            let volume = archive_volume_number(&part_role);
            parts.push((volume, part_path));
        }
    }
    parts.sort_by_key(|(volume, _)| *volume);
    Ok(parts.into_iter().map(|(_, path)| path).collect())
}

fn archive_volume_number(role: &FileRole) -> u32 {
    match role {
        FileRole::RarVolume { volume_number } => *volume_number,
        FileRole::SevenZipSplit { number } => *number,
        FileRole::SplitFile { number } => *number,
        _ => 0,
    }
}

fn rar_member_error(message: String) -> IntakeError {
    let lower = message.to_ascii_lowercase();
    if lower.contains("missing volume") || lower.contains("not available") {
        IntakeError::Transient(message)
    } else {
        IntakeError::Permanent(message)
    }
}

fn classify_archive_extraction_error(message: String) -> IntakeError {
    let lower = message.to_ascii_lowercase();
    if lower.contains("missing volume")
        || lower.contains("next volume")
        || lower.contains("unexpected eof")
        || lower.contains("failed to fill whole buffer")
    {
        IntakeError::Transient(message)
    } else {
        IntakeError::Permanent(message)
    }
}

fn validate_archive_entry_path(raw_name: &str, archive_kind: &str) -> Result<PathBuf, IntakeError> {
    if raw_name.contains('\0') {
        return Err(IntakeError::Permanent(format!(
            "unsafe {archive_kind} entry path: {raw_name}"
        )));
    }
    let normalized = raw_name.replace('\\', "/");
    let normalized = normalized.trim_end_matches('/');
    if normalized.is_empty() {
        return Err(IntakeError::Permanent(format!(
            "unsafe {archive_kind} entry path: {raw_name}"
        )));
    }
    let path = Path::new(normalized);
    if path.is_absolute() {
        return Err(IntakeError::Permanent(format!(
            "unsafe {archive_kind} entry path: {raw_name}"
        )));
    }

    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let value = part.to_string_lossy();
                if is_windows_drive_component(&value) {
                    return Err(IntakeError::Permanent(format!(
                        "unsafe {archive_kind} entry path: {raw_name}"
                    )));
                }
                safe.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(IntakeError::Permanent(format!(
                    "unsafe {archive_kind} entry path: {raw_name}"
                )));
            }
        }
    }

    if safe.as_os_str().is_empty() {
        return Err(IntakeError::Permanent(format!(
            "unsafe {archive_kind} entry path: {raw_name}"
        )));
    }

    Ok(safe)
}

fn is_windows_drive_component(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_nzb_member_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.to_ascii_lowercase().ends_with(".nzb"))
}

fn member_filename(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("watch-folder-member.nzb")
        .to_string()
}

fn strip_ascii_case_suffixes<'a>(name: &'a str, suffixes: &[&str]) -> Option<&'a str> {
    let lower = name.to_ascii_lowercase();
    suffixes
        .iter()
        .find(|suffix| lower.ends_with(**suffix))
        .map(|suffix| &name[..name.len() - suffix.len()])
}

fn transient_io(error: io::Error) -> IntakeError {
    IntakeError::Transient(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn minimal_nzb(name: &str) -> Vec<u8> {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
        )
        .into_bytes()
    }

    #[test]
    fn reads_raw_nzb() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("release.nzb");
        fs::write(&path, minimal_nzb("raw")).unwrap();

        let output = read_nzbs_from_path(&path).unwrap();

        assert_eq!(output.nzbs.len(), 1);
        assert_eq!(output.nzbs[0].filename, "release.nzb");
    }

    #[test]
    fn reads_multi_nzb_zip() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("batch.zip");
        let file = File::create(&path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("one.nzb", options).unwrap();
        zip.write_all(&minimal_nzb("one")).unwrap();
        zip.start_file("folder/two.nzb", options).unwrap();
        zip.write_all(&minimal_nzb("two")).unwrap();
        zip.finish().unwrap();

        let output = read_nzbs_from_path(&path).unwrap();

        assert_eq!(output.nzbs.len(), 2);
        assert_eq!(output.nzbs[0].filename, "one.nzb");
        assert_eq!(output.nzbs[1].filename, "two.nzb");
    }

    #[test]
    fn reads_valid_zip_members_and_reports_permanent_member_errors() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mixed.zip");
        let file = File::create(&path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("one.nzb", options).unwrap();
        zip.write_all(&minimal_nzb("one")).unwrap();
        zip.start_file("../bad.nzb", options).unwrap();
        zip.write_all(&minimal_nzb("bad")).unwrap();
        zip.finish().unwrap();

        let output = read_nzbs_from_path(&path).unwrap();

        assert_eq!(output.nzbs.len(), 1);
        assert_eq!(output.nzbs[0].filename, "one.nzb");
        assert_eq!(output.permanent_errors.len(), 1);
        assert!(output.permanent_errors[0].contains("unsafe zip entry path"));
    }

    #[test]
    fn rejects_unsafe_zip_member_path() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("unsafe.zip");
        let file = File::create(&path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("../bad.nzb", options).unwrap();
        zip.write_all(&minimal_nzb("bad")).unwrap();
        zip.finish().unwrap();

        let error = read_nzbs_from_path(&path).unwrap_err();

        assert!(error.to_string().contains("unsafe zip entry path"));
    }
}
