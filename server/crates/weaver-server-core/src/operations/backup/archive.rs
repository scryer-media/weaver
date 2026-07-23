use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};

use super::manifest::{
    BackupManifest, BackupServiceError, ManagedPackageInventory, io_err,
    validate_manifest_structure,
};
use super::permissions::set_file_owner_only;
use crate::post_processing::discovery::{hash_package, package_files};
use crate::security::RuntimeSecurityConfig;

pub(crate) struct ManagedPackageSource {
    pub inventory: ManagedPackageInventory,
    pub path: PathBuf,
}

#[cfg(test)]
pub(crate) fn write_plain_archive(
    dest: &Path,
    manifest: &BackupManifest,
    backup_db_path: &Path,
    managed_packages: &[ManagedPackageSource],
) -> Result<(), std::io::Error> {
    let file = File::create(dest)?;
    let encoder = zstd::stream::write::Encoder::new(file, 19)?;
    let mut tar = tar::Builder::new(encoder);

    let manifest_bytes = serde_json::to_vec_pretty(manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_bytes.as_slice())?;
    tar.append_path_with_name(backup_db_path, "backup.db")?;

    for package in managed_packages {
        let files = package_files(&package.path).map_err(std::io::Error::other)?;
        for (relative, absolute) in files {
            tar.append_path_with_name(
                absolute,
                Path::new(&package.inventory.archive_prefix).join(relative),
            )?;
        }
    }

    let encoder = tar.into_inner()?;
    encoder.finish()?;
    Ok(())
}

const BUNDLE_MAGIC: [u8; 8] = [0x57, 0x42, 0x45, 0x5f, 0x96, 0x31, 0xc4, 0x2a];
const BUNDLE_ENCRYPTION_VERSION: u8 = 2;
const BUNDLE_CHUNK_SIZE: usize = 1024 * 1024;
const BUNDLE_TAG_SIZE: usize = 16;
const BUNDLE_MAX_CIPHERTEXT_CHUNK: usize = BUNDLE_CHUNK_SIZE + BUNDLE_TAG_SIZE;
const BUNDLE_SALT_SIZE: usize = 16;
const BUNDLE_NONCE_PREFIX_SIZE: usize = 4;
const BUNDLE_METADATA_SIZE: usize = BUNDLE_SALT_SIZE + BUNDLE_NONCE_PREFIX_SIZE;
const MAX_BUNDLE_ARCHIVE_ENTRIES: usize = 250_000;
const MAX_BUNDLE_PATH_BYTES: usize = 4_096;
const MAX_BUNDLE_CUMULATIVE_PATH_BYTES: usize = 64 * 1024 * 1024;
const MAX_BACKUP_ZSTD_WINDOW_LOG: u32 = 26;

struct AtomicOutputWriter {
    temp: tempfile::NamedTempFile,
    writer: BufWriter<File>,
    destination: PathBuf,
}

impl AtomicOutputWriter {
    fn new(destination: &Path) -> Result<Self, std::io::Error> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let temp = tempfile::Builder::new()
            .prefix(".weaver-backup-")
            .tempfile_in(parent)?;
        set_file_owner_only(temp.path())?;
        let writer = BufWriter::new(temp.reopen()?);
        Ok(Self {
            temp,
            writer,
            destination: destination.to_path_buf(),
        })
    }

    fn finish(mut self) -> Result<(), std::io::Error> {
        self.writer.flush()?;
        let file = self
            .writer
            .into_inner()
            .map_err(|error| error.into_error())?;
        file.sync_all()?;
        drop(file);
        self.temp
            .persist(&self.destination)
            .map_err(|error| error.error)?;
        set_file_owner_only(&self.destination)?;
        #[cfg(unix)]
        std::fs::File::open(self.destination.parent().unwrap_or_else(|| Path::new(".")))?
            .sync_all()?;
        Ok(())
    }
}

impl Write for AtomicOutputWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.writer.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

struct BundleChunkWriter<W> {
    writer: W,
    cipher: Aes256Gcm,
    metadata: [u8; BUNDLE_METADATA_SIZE],
    nonce_prefix: [u8; BUNDLE_NONCE_PREFIX_SIZE],
    chunk_index: u64,
    buffer: Vec<u8>,
}

impl<W: Write> BundleChunkWriter<W> {
    fn new(mut writer: W, password: &str) -> Result<Self, std::io::Error> {
        let mut salt = [0_u8; BUNDLE_SALT_SIZE];
        let mut nonce_prefix = [0_u8; BUNDLE_NONCE_PREFIX_SIZE];
        getrandom::fill(&mut salt).map_err(std::io::Error::other)?;
        getrandom::fill(&mut nonce_prefix).map_err(std::io::Error::other)?;
        let mut metadata = [0_u8; BUNDLE_METADATA_SIZE];
        metadata[..BUNDLE_SALT_SIZE].copy_from_slice(&salt);
        metadata[BUNDLE_SALT_SIZE..].copy_from_slice(&nonce_prefix);
        let key = derive_bundle_key(password, &salt)?;
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(std::io::Error::other)?;

        writer.write_all(&BUNDLE_MAGIC)?;
        writer.write_all(&[BUNDLE_ENCRYPTION_VERSION])?;
        writer.write_all(&(BUNDLE_METADATA_SIZE as u32).to_be_bytes())?;
        writer.write_all(&metadata)?;
        Ok(Self {
            writer,
            cipher,
            metadata,
            nonce_prefix,
            chunk_index: 0,
            buffer: Vec::with_capacity(BUNDLE_CHUNK_SIZE),
        })
    }

    fn write_chunk(&mut self, plaintext: &[u8]) -> Result<(), std::io::Error> {
        let nonce_bytes = bundle_nonce(self.nonce_prefix, self.chunk_index);
        let aad = bundle_aad(&self.metadata, self.chunk_index);
        let ciphertext = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| std::io::Error::other("failed to encrypt backup chunk"))?;
        let length = u32::try_from(ciphertext.len())
            .map_err(|_| std::io::Error::other("encrypted backup chunk is too large"))?;
        self.writer.write_all(&length.to_be_bytes())?;
        self.writer.write_all(&ciphertext)?;
        self.chunk_index = self
            .chunk_index
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("backup chunk index overflow"))?;
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<(), std::io::Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let buffer = std::mem::take(&mut self.buffer);
        self.write_chunk(&buffer)
    }

    fn finish(mut self) -> Result<W, std::io::Error> {
        self.flush_buffer()?;
        self.write_chunk(&[])?;
        self.writer.flush()?;
        Ok(self.writer)
    }
}

impl<W: Write> Write for BundleChunkWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut consumed = 0;
        while consumed < bytes.len() {
            let available = BUNDLE_CHUNK_SIZE - self.buffer.len();
            let count = available.min(bytes.len() - consumed);
            self.buffer
                .extend_from_slice(&bytes[consumed..consumed + count]);
            consumed += count;
            if self.buffer.len() == BUNDLE_CHUNK_SIZE {
                self.flush_buffer()?;
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_buffer()?;
        self.writer.flush()
    }
}

type BundleStreamError = Arc<Mutex<Option<BackupServiceError>>>;

struct BundleChunkReader<R> {
    reader: R,
    cipher: Aes256Gcm,
    metadata: [u8; BUNDLE_METADATA_SIZE],
    nonce_prefix: [u8; BUNDLE_NONCE_PREFIX_SIZE],
    chunk_index: u64,
    plaintext: Vec<u8>,
    offset: usize,
    finished: bool,
    error: BundleStreamError,
}

impl<R: Read> BundleChunkReader<R> {
    fn new(mut reader: R, password: &str) -> Result<(Self, BundleStreamError), BackupServiceError> {
        let mut magic = [0_u8; BUNDLE_MAGIC.len()];
        reader.read_exact(&mut magic).map_err(io_err)?;
        if magic != BUNDLE_MAGIC {
            return Err(BackupServiceError::Validation(
                "backup does not use the logical bundle envelope".into(),
            ));
        }
        let mut version = [0_u8; 1];
        reader.read_exact(&mut version).map_err(io_err)?;
        if version[0] != BUNDLE_ENCRYPTION_VERSION {
            return Err(BackupServiceError::Validation(format!(
                "unsupported encrypted backup version {}",
                version[0]
            )));
        }
        let mut metadata_length = [0_u8; 4];
        reader.read_exact(&mut metadata_length).map_err(io_err)?;
        if u32::from_be_bytes(metadata_length) as usize != BUNDLE_METADATA_SIZE {
            return Err(BackupServiceError::Validation(
                "backup encryption metadata is invalid".into(),
            ));
        }
        let mut metadata = [0_u8; BUNDLE_METADATA_SIZE];
        reader.read_exact(&mut metadata).map_err(io_err)?;
        let mut salt = [0_u8; BUNDLE_SALT_SIZE];
        salt.copy_from_slice(&metadata[..BUNDLE_SALT_SIZE]);
        let mut nonce_prefix = [0_u8; BUNDLE_NONCE_PREFIX_SIZE];
        nonce_prefix.copy_from_slice(&metadata[BUNDLE_SALT_SIZE..]);
        let key = derive_bundle_key(password, &salt).map_err(io_err)?;
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|error| BackupServiceError::Io(error.to_string()))?;
        let error = Arc::new(Mutex::new(None));
        Ok((
            Self {
                reader,
                cipher,
                metadata,
                nonce_prefix,
                chunk_index: 0,
                plaintext: Vec::new(),
                offset: 0,
                finished: false,
                error: Arc::clone(&error),
            },
            error,
        ))
    }

    fn fill(&mut self) -> Result<(), BackupServiceError> {
        if self.finished {
            return Ok(());
        }
        let mut length = [0_u8; 4];
        let mut read = 0;
        while read < length.len() {
            match self.reader.read(&mut length[read..]).map_err(io_err)? {
                0 if read == 0 => {
                    return Err(BackupServiceError::Validation(
                        "encrypted backup is missing its authenticated terminator".into(),
                    ));
                }
                0 => {
                    return Err(BackupServiceError::Validation(
                        "encrypted backup chunk length is truncated".into(),
                    ));
                }
                count => read += count,
            }
        }
        let length = u32::from_be_bytes(length) as usize;
        if !(BUNDLE_TAG_SIZE..=BUNDLE_MAX_CIPHERTEXT_CHUNK).contains(&length) {
            return Err(BackupServiceError::Validation(
                "encrypted backup payload length is invalid".into(),
            ));
        }
        let mut ciphertext = vec![0_u8; length];
        self.reader.read_exact(&mut ciphertext).map_err(|_| {
            BackupServiceError::Validation("encrypted backup payload is truncated".into())
        })?;
        let nonce = bundle_nonce(self.nonce_prefix, self.chunk_index);
        let aad = bundle_aad(&self.metadata, self.chunk_index);
        self.plaintext = self
            .cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| BackupServiceError::InvalidPassword)?;
        self.offset = 0;
        self.chunk_index = self
            .chunk_index
            .checked_add(1)
            .ok_or_else(|| BackupServiceError::Validation("backup chunk index overflow".into()))?;
        if self.plaintext.is_empty() {
            let mut trailing = [0_u8; 1];
            if self.reader.read(&mut trailing).map_err(io_err)? != 0 {
                return Err(BackupServiceError::Validation(
                    "encrypted backup has data after its authenticated terminator".into(),
                ));
            }
            self.finished = true;
        }
        Ok(())
    }
}

impl<R: Read> Read for BundleChunkReader<R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        while self.offset >= self.plaintext.len() && !self.finished {
            if let Err(error) = self.fill() {
                let mut recorded = self
                    .error
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                if recorded.is_none() {
                    *recorded = Some(error.clone());
                }
                return Err(std::io::Error::other(error));
            }
        }
        if self.offset >= self.plaintext.len() || output.is_empty() {
            return Ok(0);
        }
        let available = &self.plaintext[self.offset..];
        let count = available.len().min(output.len());
        output[..count].copy_from_slice(&available[..count]);
        self.offset += count;
        Ok(count)
    }
}

pub(crate) fn write_bundle_archive(
    destination: &Path,
    password: &str,
    staging_root: &Path,
    managed_packages: &[ManagedPackageSource],
) -> Result<(), std::io::Error> {
    let atomic = AtomicOutputWriter::new(destination)?;
    let encrypted = BundleChunkWriter::new(atomic, password)?;
    let encoder = zstd::stream::write::Encoder::new(encrypted, 3)?;
    let mut archive = tar::Builder::new(encoder);
    append_regular_file(
        &mut archive,
        &staging_root.join("manifest.json"),
        "manifest.json",
        false,
    )?;
    append_regular_file(
        &mut archive,
        &staging_root.join("instance-secrets.json"),
        "instance-secrets.json",
        false,
    )?;
    let mut tables =
        std::fs::read_dir(staging_root.join("tables"))?.collect::<Result<Vec<_>, _>>()?;
    tables.sort_by_key(std::fs::DirEntry::file_name);
    for table in tables {
        let name = Path::new("tables").join(table.file_name());
        append_regular_file(&mut archive, &table.path(), &name, false)?;
    }
    for package in managed_packages {
        for (relative, absolute) in package_files(&package.path).map_err(std::io::Error::other)? {
            append_regular_file(
                &mut archive,
                &absolute,
                Path::new(&package.inventory.archive_prefix).join(relative),
                true,
            )?;
        }
    }
    let encoder = archive.into_inner()?;
    let encrypted = encoder.finish()?;
    let atomic = encrypted.finish()?;
    atomic.finish()
}

pub(crate) fn unpack_bundle_archive(
    input: &Path,
    output_dir: &Path,
    password: Option<String>,
) -> Result<BackupManifest, BackupServiceError> {
    let upload_limit =
        RuntimeSecurityConfig::from_env_or_default_for_tests().backup_upload_limit_bytes;
    if std::fs::metadata(input).map_err(io_err)?.len() > upload_limit {
        return Err(BackupServiceError::Validation(format!(
            "encrypted backup exceeds {upload_limit} bytes"
        )));
    }
    let password = password
        .filter(|value| !value.trim().is_empty())
        .ok_or(BackupServiceError::PasswordRequired)?;
    let file = File::open(input).map_err(io_err)?;
    let (encrypted, stream_error) = BundleChunkReader::new(BufReader::new(file), &password)?;
    let mut decoder = zstd::stream::read::Decoder::new(encrypted).map_err(|error| {
        take_bundle_stream_error(&stream_error).unwrap_or_else(|| map_bundle_stream_error(error))
    })?;
    decoder
        .window_log_max(MAX_BACKUP_ZSTD_WINDOW_LOG)
        .map_err(map_bundle_stream_error)?;
    let manifest = unpack_bundle_entries(&mut decoder, output_dir)
        .map_err(|error| take_bundle_stream_error(&stream_error).unwrap_or(error))?;
    std::io::copy(&mut decoder, &mut std::io::sink()).map_err(|error| {
        take_bundle_stream_error(&stream_error).unwrap_or_else(|| map_bundle_stream_error(error))
    })?;
    let mut encrypted = decoder.finish();
    let trailing_plaintext =
        std::io::copy(&mut encrypted, &mut std::io::sink()).map_err(|error| {
            take_bundle_stream_error(&stream_error)
                .unwrap_or_else(|| map_bundle_stream_error(error))
        })?;
    if trailing_plaintext != 0 {
        return Err(BackupServiceError::Validation(
            "backup contains plaintext after its zstd frame".into(),
        ));
    }
    Ok(manifest)
}

fn is_portable_archive_path(path: &Path) -> bool {
    path.components().all(|component| {
        let Component::Normal(component) = component else {
            return false;
        };
        let Some(component) = component.to_str() else {
            return false;
        };
        !component.is_empty()
            && !component.contains(['\\', ':', '\0'])
            && !component.chars().any(char::is_control)
            && !component.ends_with(['.', ' '])
            && !is_windows_device_name(component)
    })
}

fn is_windows_device_name(component: &str) -> bool {
    let stem = component
        .split_once('.')
        .map_or(component, |(stem, _)| stem)
        .to_ascii_uppercase();
    if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$") {
        return true;
    }
    ["COM", "LPT"].iter().any(|prefix| {
        stem.strip_prefix(prefix).is_some_and(|suffix| {
            matches!(
                suffix,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        })
    })
}

fn managed_package_path(
    path: &Path,
    package_indices: &HashMap<PathBuf, usize>,
) -> Option<(usize, bool)> {
    let mut components = path.components();
    let mut prefix = PathBuf::new();
    for _ in 0..3 {
        let Component::Normal(component) = components.next()? else {
            return None;
        };
        prefix.push(component);
    }
    package_indices
        .get(&prefix)
        .copied()
        .map(|index| (index, components.next().is_some()))
}

fn unpack_bundle_entries<R: Read>(
    reader: R,
    output_dir: &Path,
) -> Result<BackupManifest, BackupServiceError> {
    let mut archive = tar::Archive::new(reader);
    let expansion_limit =
        RuntimeSecurityConfig::from_env_or_default_for_tests().backup_upload_limit_bytes;
    let mut seen = HashSet::new();
    let mut manifest = None::<BackupManifest>;
    let mut package_stats = Vec::<(u64, u64)>::new();
    let mut package_indices = HashMap::<PathBuf, usize>::new();
    let mut total = 0_u64;
    let mut entry_count = 0_usize;
    let mut path_bytes = 0_usize;
    for item in archive.entries().map_err(map_bundle_stream_error)? {
        let mut entry = item.map_err(map_bundle_stream_error)?;
        entry_count = entry_count.saturating_add(1);
        if entry_count > MAX_BUNDLE_ARCHIVE_ENTRIES {
            return Err(BackupServiceError::Validation(format!(
                "backup archive contains more than {MAX_BUNDLE_ARCHIVE_ENTRIES} entries"
            )));
        }
        let kind = entry.header().entry_type();
        if !kind.is_file() {
            return Err(BackupServiceError::Validation(
                "backup archive contains an unsupported non-regular entry".into(),
            ));
        }
        let path = entry.path().map_err(map_bundle_stream_error)?.into_owned();
        let encoded_path_bytes = path.as_os_str().as_encoded_bytes().len();
        path_bytes = path_bytes
            .checked_add(encoded_path_bytes)
            .ok_or_else(|| BackupServiceError::Validation("backup paths are too large".into()))?;
        if path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
            || !is_portable_archive_path(&path)
            || path.components().count() > 64
            || encoded_path_bytes > MAX_BUNDLE_PATH_BYTES
            || path_bytes > MAX_BUNDLE_CUMULATIVE_PATH_BYTES
            || !seen.insert(path.clone())
        {
            return Err(BackupServiceError::Validation(
                "backup archive contains an unsafe or duplicate path".into(),
            ));
        }
        let size = entry.header().size().map_err(map_bundle_stream_error)?;
        total = total
            .checked_add(size)
            .ok_or_else(|| BackupServiceError::Validation("backup archive is too large".into()))?;
        if total > expansion_limit {
            return Err(BackupServiceError::Validation(format!(
                "backup archive expands beyond {expansion_limit} bytes"
            )));
        }
        let name = path.to_str().ok_or_else(|| {
            BackupServiceError::Validation("backup path is not valid UTF-8".into())
        })?;
        if name == "manifest.json" {
            if manifest.is_some() || seen.len() != 1 || size > 1024 * 1024 {
                return Err(BackupServiceError::Validation(
                    "manifest.json must be the first entry and at most 1 MiB".into(),
                ));
            }
            let mut bytes = Vec::with_capacity(size as usize);
            entry
                .read_to_end(&mut bytes)
                .map_err(map_bundle_stream_error)?;
            let parsed = serde_json::from_slice::<BackupManifest>(&bytes)
                .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
            if !parsed.format_version.is_bundle_v2() {
                return Err(BackupServiceError::UnsupportedFormat(
                    parsed.format_version.to_string(),
                ));
            }
            validate_manifest_structure(&parsed)?;
            std::fs::write(output_dir.join("manifest.json"), bytes).map_err(io_err)?;
            package_stats = vec![(0, 0); parsed.managed_packages.len()];
            package_indices = parsed
                .managed_packages
                .iter()
                .enumerate()
                .map(|(index, package)| (PathBuf::from(&package.archive_prefix), index))
                .collect();
            manifest = Some(parsed);
            continue;
        }
        let manifest = manifest.as_ref().ok_or_else(|| {
            BackupServiceError::Validation(
                "backup is missing manifest.json as its first entry".into(),
            )
        })?;
        if name == "instance-secrets.json" && size > 1024 * 1024 {
            return Err(BackupServiceError::Validation(
                "instance-secrets.json exceeds 1 MiB".into(),
            ));
        }
        let package_index = managed_package_path(&path, &package_indices)
            .and_then(|(index, has_relative)| has_relative.then_some(index));
        let declared = name == "instance-secrets.json"
            || path
                .strip_prefix("tables")
                .ok()
                .and_then(|relative| relative.to_str())
                .is_some_and(|relative| {
                    relative
                        .strip_suffix(".ndjson")
                        .is_some_and(|table| manifest.tables.contains_key(table))
                })
            || package_index.is_some();
        if !declared {
            return Err(BackupServiceError::Validation(format!(
                "backup archive contains undeclared entry {name}"
            )));
        }
        if let Some(index) = package_index {
            let package = &manifest.managed_packages[index];
            let stats = &mut package_stats[index];
            stats.0 = stats.0.saturating_add(1);
            stats.1 = stats.1.checked_add(size).ok_or_else(|| {
                BackupServiceError::Validation("managed package is too large".into())
            })?;
            if stats.0 > package.file_count as u64 || stats.1 > package.uncompressed_bytes {
                return Err(BackupServiceError::Validation(format!(
                    "managed package {} exceeds its declared expansion limits",
                    package.digest
                )));
            }
        }
        let destination = output_dir.join(&path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
        entry.unpack(destination).map_err(map_bundle_stream_error)?;
    }
    let manifest = manifest
        .ok_or_else(|| BackupServiceError::Validation("backup is missing manifest.json".into()))?;
    for (package, (files, bytes)) in manifest.managed_packages.iter().zip(package_stats) {
        if files != package.file_count as u64 || bytes != package.uncompressed_bytes {
            return Err(BackupServiceError::Validation(format!(
                "managed package {} does not match its declared inventory",
                package.digest
            )));
        }
    }
    Ok(manifest)
}

fn map_bundle_stream_error(error: std::io::Error) -> BackupServiceError {
    let mut source = Some(&error as &(dyn std::error::Error + 'static));
    while let Some(current) = source {
        if let Some(backup) = current.downcast_ref::<BackupServiceError>() {
            return backup.clone();
        }
        source = current.source();
    }
    if matches!(
        error.kind(),
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
    ) {
        BackupServiceError::Validation(error.to_string())
    } else {
        BackupServiceError::Io(error.to_string())
    }
}

fn take_bundle_stream_error(
    error: &Arc<Mutex<Option<BackupServiceError>>>,
) -> Option<BackupServiceError> {
    error
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .take()
}

pub(crate) fn is_bundle_encrypted(path: &Path) -> Result<bool, BackupServiceError> {
    let mut header = [0_u8; BUNDLE_MAGIC.len()];
    let mut file = File::open(path).map_err(io_err)?;
    let read = file.read(&mut header).map_err(io_err)?;
    Ok(read == header.len() && header == BUNDLE_MAGIC)
}

fn append_regular_file<W: Write>(
    archive: &mut tar::Builder<W>,
    source: &Path,
    destination: impl AsRef<Path>,
    preserve_executable: bool,
) -> Result<(), std::io::Error> {
    let metadata = std::fs::symlink_metadata(source)?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::other(format!(
            "{} is not a regular file",
            source.display()
        )));
    }
    let mut file = File::open(source)?;
    let mut header = tar::Header::new_gnu();
    header.set_size(metadata.len());
    let mut mode = 0o600;
    #[cfg(unix)]
    if preserve_executable {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o111 != 0 {
            mode = 0o700;
        }
    }
    #[cfg(not(unix))]
    let _ = preserve_executable;
    header.set_mode(mode);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    archive.append_data(&mut header, destination, &mut file)
}

fn derive_bundle_key(password: &str, salt: &[u8]) -> Result<[u8; 32], std::io::Error> {
    let params = Params::new(65_536, 3, 1, Some(32))
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0_u8; 32];
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(key)
}

fn bundle_nonce(prefix: [u8; BUNDLE_NONCE_PREFIX_SIZE], index: u64) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[..BUNDLE_NONCE_PREFIX_SIZE].copy_from_slice(&prefix);
    nonce[BUNDLE_NONCE_PREFIX_SIZE..].copy_from_slice(&index.to_be_bytes());
    nonce
}

fn bundle_aad(metadata: &[u8; BUNDLE_METADATA_SIZE], index: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(BUNDLE_MAGIC.len() + 1 + metadata.len() + 8);
    aad.extend_from_slice(&BUNDLE_MAGIC);
    aad.push(BUNDLE_ENCRYPTION_VERSION);
    aad.extend_from_slice(metadata);
    aad.extend_from_slice(&index.to_be_bytes());
    aad
}

/// File format: `WEAVER_ENC\0` (10 bytes) + salt (32 bytes) + nonce (12 bytes) + ciphertext+tag
const ENCRYPT_MAGIC: &[u8; 10] = b"WEAVER_ENC";
const SALT_LEN: usize = 32;
const PBKDF2_ROUNDS: u32 = 600_000;

fn derive_key(password: &str, salt: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password.as_bytes(), salt, PBKDF2_ROUNDS, &mut key);
    key
}

#[cfg(test)]
pub(crate) fn encrypt_archive(
    input: &Path,
    output: &Path,
    password: &str,
) -> Result<(), std::io::Error> {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};

    let mut plaintext = Vec::new();
    File::open(input)?.read_to_end(&mut plaintext)?;

    let mut salt = [0u8; SALT_LEN];
    getrandom::fill(&mut salt).map_err(|e| std::io::Error::other(e.to_string()))?;

    let key = derive_key(password, &salt);
    let cipher = Aes256Gcm::new(GenericArray::from_slice(&key));

    let mut nonce_bytes = [0u8; 12];
    getrandom::fill(&mut nonce_bytes).map_err(|e| std::io::Error::other(e.to_string()))?;
    let nonce = GenericArray::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_ref())
        .map_err(|e| std::io::Error::other(format!("encryption failed: {e}")))?;

    let mut out = File::create(output)?;
    use std::io::Write;
    out.write_all(ENCRYPT_MAGIC)?;
    out.write_all(&salt)?;
    out.write_all(&nonce_bytes)?;
    out.write_all(&ciphertext)?;
    Ok(())
}

pub(crate) fn maybe_decrypt_archive(
    input: &Path,
    password: Option<String>,
    work_dir: &Path,
) -> Result<PathBuf, BackupServiceError> {
    if !is_encrypted(input)? {
        return Ok(input.to_path_buf());
    }

    let password = password
        .filter(|value| !value.is_empty())
        .ok_or(BackupServiceError::PasswordRequired)?;
    let output = work_dir.join("backup.tar.zst");
    decrypt_archive(input, &output, &password)?;
    Ok(output)
}

fn decrypt_archive(input: &Path, output: &Path, password: &str) -> Result<(), BackupServiceError> {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};

    let limit = RuntimeSecurityConfig::from_env_or_default_for_tests().backup_upload_limit_bytes;
    let file_size = std::fs::metadata(input).map_err(io_err)?.len();
    if file_size > limit {
        return Err(BackupServiceError::Validation(format!(
            "encrypted backup exceeds {limit} bytes"
        )));
    }

    let mut data = Vec::new();
    File::open(input)
        .map_err(io_err)?
        .read_to_end(&mut data)
        .map_err(io_err)?;

    let header_len = ENCRYPT_MAGIC.len() + SALT_LEN + 12;
    if data.len() < header_len {
        return Err(BackupServiceError::InvalidPassword);
    }

    let salt = &data[ENCRYPT_MAGIC.len()..ENCRYPT_MAGIC.len() + SALT_LEN];
    let nonce_bytes = &data[ENCRYPT_MAGIC.len() + SALT_LEN..header_len];
    let ciphertext = &data[header_len..];

    let key = derive_key(password, salt);
    let cipher = Aes256Gcm::new(GenericArray::from_slice(&key));
    let nonce = GenericArray::from_slice(nonce_bytes);

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| BackupServiceError::InvalidPassword)?;

    std::fs::write(output, plaintext).map_err(io_err)?;
    Ok(())
}

pub(crate) fn unpack_plain_archive(
    input: &Path,
    output_dir: &Path,
) -> Result<BackupManifest, BackupServiceError> {
    let file = File::open(input).map_err(io_err)?;
    let mut decoder = zstd::stream::read::Decoder::new(file).map_err(io_err)?;
    decoder
        .window_log_max(MAX_BACKUP_ZSTD_WINDOW_LOG)
        .map_err(io_err)?;
    let mut archive = tar::Archive::new(decoder);
    let backup_db_limit =
        RuntimeSecurityConfig::from_env_or_default_for_tests().backup_upload_limit_bytes;
    let mut manifest: Option<BackupManifest> = None;
    let mut saw_backup_db = false;
    let mut seen_paths = HashSet::new();
    let mut package_stats: HashMap<String, (usize, u64)> = HashMap::new();
    let mut total_uncompressed_bytes = 0_u64;
    let mut entry_count = 0_usize;
    let mut path_bytes = 0_usize;

    for entry in archive.entries().map_err(io_err)? {
        let mut entry = entry.map_err(io_err)?;
        entry_count = entry_count.saturating_add(1);
        if entry_count > MAX_BUNDLE_ARCHIVE_ENTRIES {
            return Err(BackupServiceError::Validation(format!(
                "backup archive contains more than {MAX_BUNDLE_ARCHIVE_ENTRIES} entries"
            )));
        }
        let entry_type = entry.header().entry_type();
        if !entry_type.is_file() {
            return Err(BackupServiceError::Validation(
                "backup archive contains an unsupported non-regular entry".to_string(),
            ));
        }
        let path = entry.path().map_err(io_err)?.into_owned();
        let encoded_path_bytes = path.as_os_str().as_encoded_bytes().len();
        path_bytes = path_bytes
            .checked_add(encoded_path_bytes)
            .ok_or_else(|| BackupServiceError::Validation("backup paths are too large".into()))?;
        if path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
            || !is_portable_archive_path(&path)
            || path.components().count() > 64
            || encoded_path_bytes > MAX_BUNDLE_PATH_BYTES
            || path_bytes > MAX_BUNDLE_CUMULATIVE_PATH_BYTES
            || !seen_paths.insert(path.clone())
        {
            return Err(BackupServiceError::Validation(
                "backup archive contains an unsafe or duplicate path".into(),
            ));
        }
        let name = path.to_str().ok_or_else(|| {
            BackupServiceError::Validation("backup archive contains a non-utf8 entry".to_string())
        })?;
        let size = entry.header().size().map_err(io_err)?;
        total_uncompressed_bytes = total_uncompressed_bytes
            .checked_add(size)
            .ok_or_else(|| BackupServiceError::Validation("backup archive is too large".into()))?;
        if total_uncompressed_bytes > backup_db_limit {
            return Err(BackupServiceError::Validation(format!(
                "backup archive expands beyond {backup_db_limit} bytes"
            )));
        }
        match name {
            "manifest.json" => {
                if manifest.is_some() || seen_paths.len() != 1 || size > 1024 * 1024 {
                    return Err(BackupServiceError::Validation(
                        "manifest.json must be the first archive entry and at most 1 MiB".into(),
                    ));
                }
                let mut bytes = Vec::with_capacity(size as usize);
                entry.read_to_end(&mut bytes).map_err(io_err)?;
                let parsed: BackupManifest = serde_json::from_slice(&bytes)
                    .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
                if !parsed.format_version.is_legacy() {
                    return Err(BackupServiceError::UnsupportedFormat(
                        parsed.format_version.to_string(),
                    ));
                }
                validate_manifest_structure(&parsed)?;
                std::fs::write(output_dir.join("manifest.json"), &bytes).map_err(io_err)?;
                manifest = Some(parsed);
            }
            "backup.db" => {
                if manifest.is_none() {
                    return Err(BackupServiceError::Validation(
                        "backup archive is missing manifest.json as its first entry".into(),
                    ));
                }
                if saw_backup_db {
                    return Err(BackupServiceError::Validation(
                        "backup archive contains duplicate backup.db".to_string(),
                    ));
                }
                if size > backup_db_limit {
                    return Err(BackupServiceError::Validation(format!(
                        "backup.db exceeds {backup_db_limit} bytes"
                    )));
                }
                saw_backup_db = true;
                entry.unpack(output_dir.join("backup.db")).map_err(io_err)?;
            }
            other => {
                let manifest = manifest.as_ref().ok_or_else(|| {
                    BackupServiceError::Validation(
                        "backup archive is missing manifest.json as its first entry".into(),
                    )
                })?;
                let Some(package) = manifest.managed_packages.iter().find(|package| {
                    path.strip_prefix(&package.archive_prefix)
                        .is_ok_and(|relative| !relative.as_os_str().is_empty())
                }) else {
                    return Err(BackupServiceError::Validation(format!(
                        "backup archive contains unexpected entry {other}"
                    )));
                };
                let relative = path
                    .strip_prefix(&package.archive_prefix)
                    .map_err(|_| BackupServiceError::Validation("invalid package path".into()))?;
                if relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
                {
                    return Err(BackupServiceError::Validation(
                        "managed package contains an unsafe path".into(),
                    ));
                }
                let stats = package_stats
                    .entry(package.digest.clone())
                    .or_insert((0, 0));
                stats.0 = stats.0.saturating_add(1);
                stats.1 = stats.1.saturating_add(size);
                if stats.0 > package.file_count || stats.1 > package.uncompressed_bytes {
                    return Err(BackupServiceError::Validation(
                        "managed package exceeds its declared inventory".into(),
                    ));
                }
                let destination = output_dir.join(&package.archive_prefix).join(relative);
                if let Some(parent) = destination.parent() {
                    std::fs::create_dir_all(parent).map_err(io_err)?;
                }
                entry.unpack(destination).map_err(io_err)?;
            }
        }
    }
    let manifest = manifest.ok_or_else(|| {
        BackupServiceError::Validation("backup archive is missing manifest.json".to_string())
    })?;
    if !saw_backup_db {
        return Err(BackupServiceError::Validation(
            "backup archive is missing backup.db".to_string(),
        ));
    }
    for package in &manifest.managed_packages {
        if package_stats.get(&package.digest).copied()
            != Some((package.file_count, package.uncompressed_bytes))
        {
            return Err(BackupServiceError::Validation(format!(
                "managed package {} does not match its inventory",
                package.digest
            )));
        }
        let package_path = output_dir.join(&package.archive_prefix);
        let actual = hash_package(&package_path)
            .map_err(|error| BackupServiceError::Validation(error.to_string()))?;
        if actual.as_str() != package.digest {
            return Err(BackupServiceError::Validation(format!(
                "managed package {} failed digest verification",
                package.digest
            )));
        }
    }
    Ok(manifest)
}

fn is_encrypted(path: &Path) -> Result<bool, BackupServiceError> {
    let mut header = [0u8; 10];
    let mut file = File::open(path).map_err(io_err)?;
    let read = file.read(&mut header).map_err(io_err)?;
    Ok(read >= ENCRYPT_MAGIC.len() && header[..ENCRYPT_MAGIC.len()] == *ENCRYPT_MAGIC)
}

#[cfg(test)]
mod bundle_envelope_tests {
    use super::*;

    const HEADER_SIZE: usize =
        BUNDLE_MAGIC.len() + 1 + std::mem::size_of::<u32>() + BUNDLE_METADATA_SIZE;

    fn encrypted_fixture() -> (Vec<u8>, Vec<u8>) {
        let plaintext = (0..(BUNDLE_CHUNK_SIZE * 2 + 73))
            .map(|index| (index.wrapping_mul(31) % 251) as u8)
            .collect::<Vec<_>>();
        let mut writer = BundleChunkWriter::new(Vec::new(), " exact password ").unwrap();
        writer.write_all(&plaintext).unwrap();
        (writer.finish().unwrap(), plaintext)
    }

    fn decrypt(bytes: &[u8], password: &str) -> std::io::Result<Vec<u8>> {
        let (mut reader, _) = BundleChunkReader::new(std::io::Cursor::new(bytes), password)
            .map_err(std::io::Error::other)?;
        let mut plaintext = Vec::new();
        reader.read_to_end(&mut plaintext)?;
        Ok(plaintext)
    }

    fn chunk_ranges(bytes: &[u8]) -> Vec<std::ops::Range<usize>> {
        let mut ranges = Vec::new();
        let mut offset = HEADER_SIZE;
        while offset < bytes.len() {
            let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let end = offset + 4 + length;
            ranges.push(offset..end);
            offset = end;
        }
        ranges
    }

    #[test]
    fn bundle_envelope_round_trips_exact_password_bytes() {
        let (encrypted, plaintext) = encrypted_fixture();
        assert_eq!(decrypt(&encrypted, " exact password ").unwrap(), plaintext);
        assert!(decrypt(&encrypted, "exact password").is_err());
    }

    #[test]
    fn bundle_envelope_rejects_tampering_reordering_and_truncation() {
        let (encrypted, _) = encrypted_fixture();

        let mut metadata_tampered = encrypted.clone();
        metadata_tampered[BUNDLE_MAGIC.len() + 1 + 4] ^= 0x80;
        assert!(decrypt(&metadata_tampered, " exact password ").is_err());

        let mut oversized = encrypted.clone();
        oversized[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decrypt(&oversized, " exact password ").is_err());

        let ranges = chunk_ranges(&encrypted);
        assert!(ranges.len() >= 3);
        let mut reordered = encrypted[..HEADER_SIZE].to_vec();
        reordered.extend_from_slice(&encrypted[ranges[1].clone()]);
        reordered.extend_from_slice(&encrypted[ranges[0].clone()]);
        for range in &ranges[2..] {
            reordered.extend_from_slice(&encrypted[range.clone()]);
        }
        assert!(decrypt(&reordered, " exact password ").is_err());

        assert!(decrypt(&encrypted[..encrypted.len() - 3], " exact password ").is_err());
        let terminator = ranges.last().expect("authenticated terminator");
        assert_eq!(terminator.end - terminator.start, 4 + BUNDLE_TAG_SIZE);
        assert!(decrypt(&encrypted[..terminator.start], " exact password ").is_err());

        let mut appended = encrypted.clone();
        appended.push(0);
        assert!(decrypt(&appended, " exact password ").is_err());
    }

    #[test]
    fn archive_paths_are_portable_to_windows() {
        for path in [
            "managed-extensions/blake3/abc/CON",
            "managed-extensions/blake3/abc/com1.txt",
            "managed-extensions/blake3/abc/name:stream",
            "managed-extensions/blake3/abc/trailing.",
            r"managed-extensions\blake3\abc\script.ps1",
        ] {
            assert!(!is_portable_archive_path(Path::new(path)), "{path}");
        }
        assert!(is_portable_archive_path(Path::new(
            "managed-extensions/blake3/abc/script.ps1"
        )));
    }
}
