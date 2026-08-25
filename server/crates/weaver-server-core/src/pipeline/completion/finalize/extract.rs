use super::*;
use crate::pipeline::extraction::{BudgetedReader, RarExtractionOpenRequest};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

static XZ_MT_DECODER_PERMIT: Mutex<()> = Mutex::new(());

enum FilesystemXzDecoder<R: std::io::Read> {
    Sequential(liblzma::read::XzDecoder<R>),
    Parallel {
        decoder: liblzma::read::XzDecoder<R>,
        _permit: MutexGuard<'static, ()>,
    },
}

impl<R: std::io::Read> std::io::Read for FilesystemXzDecoder<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Sequential(decoder) => decoder.read(buffer),
            Self::Parallel { decoder, .. } => decoder.read(buffer),
        }
    }
}

struct CountingWriter<W> {
    inner: W,
    attempt: Arc<PhaseAttemptCounters>,
}

impl<W> CountingWriter<W> {
    fn new(inner: W, attempt: Arc<PhaseAttemptCounters>) -> Self {
        Self { inner, attempt }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.attempt.record_completed(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)?;
        self.attempt.record_completed(buf.len() as u64);
        Ok(())
    }
}

fn validate_zip_entry_path(raw_name: &str) -> Result<PathBuf, String> {
    validate_archive_entry_path(raw_name, "zip")
}

fn validate_tar_entry_path(raw_name: &str) -> Result<PathBuf, String> {
    if raw_name.contains('\\') {
        return Err(format!("unsafe tar entry path: {raw_name}"));
    }
    validate_archive_entry_path(raw_name, "tar")
}

fn is_tar_current_dir_entry(raw_name: &str) -> bool {
    if raw_name.contains(['\\', '\0']) {
        return false;
    }

    let normalized = raw_name.trim_end_matches('/');
    !normalized.is_empty()
        && Path::new(normalized)
            .components()
            .all(|component| matches!(component, Component::CurDir))
}

fn validate_archive_entry_path(raw_name: &str, archive_kind: &str) -> Result<PathBuf, String> {
    if raw_name.contains('\0') {
        return Err(format!("unsafe {archive_kind} entry path: {raw_name}"));
    }

    let normalized = raw_name.replace('\\', "/");
    let normalized = normalized.trim_end_matches('/');
    if normalized.is_empty() {
        return Err(format!("unsafe {archive_kind} entry path: {raw_name}"));
    }

    let path = Path::new(normalized);
    if path.is_absolute() {
        return Err(format!("unsafe {archive_kind} entry path: {raw_name}"));
    }

    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let value = part.to_string_lossy();
                if is_windows_drive_component(&value) {
                    return Err(format!("unsafe {archive_kind} entry path: {raw_name}"));
                }
                safe.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("unsafe {archive_kind} entry path: {raw_name}"));
            }
        }
    }

    if safe.as_os_str().is_empty() {
        return Err(format!("unsafe {archive_kind} entry path: {raw_name}"));
    }

    Ok(safe)
}

fn is_windows_drive_component(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn simple_decoder_memory_bytes(kind: SimpleArchiveKind, max_memory_bytes: u64) -> u64 {
    const MIB: u64 = 1024 * 1024;
    match kind {
        SimpleArchiveKind::Zstd => max_memory_bytes,
        SimpleArchiveKind::Xz | SimpleArchiveKind::TarXz => {
            max_memory_bytes.min(crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES)
        }
        SimpleArchiveKind::Brotli => 32 * MIB,
        SimpleArchiveKind::Zip | SimpleArchiveKind::TarBz2 | SimpleArchiveKind::Bzip2 => 8 * MIB,
        SimpleArchiveKind::Tar
        | SimpleArchiveKind::TarGz
        | SimpleArchiveKind::Gz
        | SimpleArchiveKind::Deflate
        | SimpleArchiveKind::Split => MIB,
    }
}

#[allow(clippy::too_many_arguments)]
fn extract_zip(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    password: Option<&str>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
    phase_counters: Option<Arc<PhaseCounters>>,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open zip: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| format!("failed to read zip archive: {e}"))?;
    let mut extracted = Vec::new();
    let mut known_total = 0u64;
    for i in 0..archive.len() {
        let entry = archive
            .by_index_raw(i)
            .map_err(|e| format!("failed to read zip entry {i}: {e}"))?;
        budget.check_member_metadata(entry.name(), entry.size())?;
        let raw_name = entry.name();
        validate_zip_entry_path(raw_name).map_err(|error| budget.reject_unsafe_path(error))?;
        if !entry.is_dir() {
            known_total = known_total.saturating_add(entry.size());
        }
    }
    if let Some(counters) = phase_counters.as_ref() {
        counters
            .total_bytes
            .fetch_add(known_total, Ordering::Relaxed);
    }

    for i in 0..archive.len() {
        let mut entry = if let Some(pw) = password {
            archive
                .by_index_decrypt(i, pw.as_bytes())
                .map_err(|e| format!("failed to read zip entry {i}: {e}"))?
        } else {
            archive
                .by_index(i)
                .map_err(|e| format!("failed to read zip entry {i}: {e}"))?
        };
        let raw_name = entry.name().to_string();
        let safe_path =
            validate_zip_entry_path(&raw_name).map_err(|error| budget.reject_unsafe_path(error))?;
        let name = safe_path.to_string_lossy().replace('\\', "/");

        if entry.is_dir() {
            root.create_dir(&safe_path, budget)?;
            continue;
        }

        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });

        let outfile = root.create_file(&safe_path, budget)?;
        let attempt = phase_counters
            .as_ref()
            .map(|counters| Arc::new(PhaseAttemptCounters::new(Arc::clone(counters))));
        let mut outfile: Box<dyn Write> = if let Some(attempt) = attempt.as_ref() {
            Box::new(CountingWriter::new(outfile, Arc::clone(attempt)))
        } else {
            Box::new(outfile)
        };
        let bytes_written = match std::io::copy(&mut entry, &mut outfile) {
            Ok(bytes) => {
                if let Some(attempt) = &attempt {
                    attempt.commit();
                }
                bytes
            }
            Err(error) => {
                if let Some(attempt) = &attempt {
                    attempt.rollback();
                }
                return Err(format!("failed to extract {name}: {error}"));
            }
        };

        let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });
        tracing::info!(job_id = job_id.0, member = %name, bytes_written, "zip member extracted");
        extracted.push(name);
    }

    Ok(extracted)
}

fn extract_tar(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open tar: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    extract_tar_from_reader(file, root, budget, event_tx, job_id, set_name)
}

fn extract_tar_gz(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open tar.gz: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let gz = flate2::read::GzDecoder::new(file);
    extract_tar_from_reader(gz, root, budget, event_tx, job_id, set_name)
}

fn extract_tar_bz2(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open tar.bz2: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let bz2 = bzip2::read::BzDecoder::new(file);
    extract_tar_from_reader(bz2, root, budget, event_tx, job_id, set_name)
}

#[allow(clippy::too_many_arguments)]
fn extract_tar_xz(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let xz = open_sequential_xz_decoder(archive_path, budget)?;
    extract_tar_from_reader(xz, root, budget, event_tx, job_id, set_name)
}

fn extract_tar_from_reader<R: std::io::Read>(
    reader: R,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let mut archive = tar::Archive::new(reader);
    let mut extracted = Vec::new();

    for entry in archive
        .entries()
        .map_err(|e| format!("failed to read tar entries: {e}"))?
    {
        let mut entry = entry.map_err(|e| format!("failed to read tar entry: {e}"))?;
        let raw_name = entry
            .path()
            .map_err(|e| format!("invalid tar entry path: {e}"))?
            .to_string_lossy()
            .to_string();
        if entry.header().entry_type().is_dir() && is_tar_current_dir_entry(&raw_name) {
            continue;
        }
        let safe_path =
            validate_tar_entry_path(&raw_name).map_err(|error| budget.reject_unsafe_path(error))?;
        let name = safe_path.to_string_lossy().replace('\\', "/");

        let entry_type = entry.header().entry_type();
        let mut has_sparse_metadata = false;
        if let Some(extensions) = entry
            .pax_extensions()
            .map_err(|error| format!("failed to inspect tar metadata for {name}: {error}"))?
        {
            for extension in extensions {
                let extension = extension
                    .map_err(|error| format!("failed to parse tar metadata for {name}: {error}"))?;
                has_sparse_metadata |= extension.key_bytes().starts_with(b"GNU.sparse");
            }
        }
        if has_sparse_metadata {
            return Err(budget.reject_unsupported_entry(format!(
                "tar entry '{name}' uses sparse-file metadata"
            )));
        }
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(budget.reject_unsupported_entry(format!(
                "tar entry '{name}' has forbidden type {entry_type:?}"
            )));
        }
        budget.check_member_metadata(&name, entry.size())?;

        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });

        let bytes_written = if entry_type.is_dir() {
            root.create_dir(&safe_path, budget)?;
            0
        } else {
            let mut output = root.create_file(&safe_path, budget)?;
            std::io::copy(&mut entry, &mut output)
                .map_err(|e| format!("failed to extract tar entry {name}: {e}"))?
        };
        if bytes_written > 0 {
            tracing::debug!(job_id = job_id.0, member = %name, bytes_written, "tar member bytes written");
        }

        let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });
        tracing::info!(job_id = job_id.0, member = %name, "tar member extracted");
        extracted.push(name);
    }

    Ok(extracted)
}

fn extract_gz(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open gz: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let mut gz = flate2::read::GzDecoder::new(file);

    // Output filename: strip .gz extension
    let archive_name = archive_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let output_name = archive_name
        .strip_suffix(".gz")
        .or_else(|| archive_name.strip_suffix(".GZ"))
        .unwrap_or(&archive_name);
    let safe_path = root
        .validate_relative_path(output_name)
        .map_err(|error| budget.reject_unsafe_path(error))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });

    let mut outfile = root.create_file(&safe_path, budget)?;
    let bytes_written = std::io::copy(&mut gz, &mut outfile)
        .map_err(|e| format!("failed to decompress gz: {e}"))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });
    tracing::info!(job_id = job_id.0, member = %output_name, bytes_written, "gz decompressed");

    Ok(vec![output_name.to_string()])
}

fn strip_ascii_case_suffix<'a>(name: &'a str, suffix: &str) -> Option<&'a str> {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with(suffix) {
        Some(&name[..name.len() - suffix.len()])
    } else {
        None
    }
}

#[cfg(test)]
mod tests;

fn derive_single_file_output_name<'a>(archive_name: &'a str, suffixes: &[&str]) -> &'a str {
    suffixes
        .iter()
        .find_map(|suffix| strip_ascii_case_suffix(archive_name, suffix))
        .unwrap_or(archive_name)
}

#[allow(clippy::too_many_arguments)]
fn extract_single_stream_to_file<R: std::io::Read>(
    mut reader: R,
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    suffixes: &[&str],
    format_name: &str,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let archive_name = archive_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let output_name = derive_single_file_output_name(&archive_name, suffixes);
    let safe_path = root
        .validate_relative_path(output_name)
        .map_err(|error| budget.reject_unsafe_path(error))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });

    let mut outfile = root.create_file(&safe_path, budget)?;
    let bytes_written = std::io::copy(&mut reader, &mut outfile)
        .map_err(|e| format!("failed to decompress {format_name}: {e}"))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });
    tracing::info!(job_id = job_id.0, member = %output_name, bytes_written, format = format_name, "compressed file decompressed");

    Ok(vec![output_name.to_string()])
}

fn extract_brotli(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open br: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let reader = brotli::Decompressor::new(file, 4096);
    extract_single_stream_to_file(
        reader,
        archive_path,
        root,
        budget,
        &[".br"],
        "br",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_deflate(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open deflate: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let reader = flate2::read::DeflateDecoder::new(file);
    extract_single_stream_to_file(
        reader,
        archive_path,
        root,
        budget,
        &[".deflate"],
        "deflate",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_zstd(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open zstd: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let mut reader =
        zstd::stream::read::Decoder::new(file).map_err(|e| format!("failed to open zstd: {e}"))?;
    let max_memory_bytes = budget.max_memory_bytes().max(1024);
    let window_log = (63 - max_memory_bytes.leading_zeros()).clamp(10, 31);
    reader
        .window_log_max(window_log)
        .map_err(|e| format!("failed to apply zstd memory limit: {e}"))?;
    extract_single_stream_to_file(
        reader,
        archive_path,
        root,
        budget,
        &[".zstd", ".zst"],
        "zstd",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_bzip2(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open bz2: {e}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let reader = bzip2::read::BzDecoder::new(file);
    extract_single_stream_to_file(
        reader,
        archive_path,
        root,
        budget,
        &[".bz2"],
        "bz2",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_xz(
    archive_path: &Path,
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
    xz_worker_threads: usize,
) -> Result<Vec<String>, String> {
    let xz = open_filesystem_xz_decoder(archive_path, budget, xz_worker_threads)?;
    extract_single_stream_to_file(
        xz,
        archive_path,
        root,
        budget,
        &[".xz"],
        "xz",
        event_tx,
        job_id,
        set_name,
    )
}

fn open_sequential_xz_decoder(
    archive_path: &Path,
    budget: &Arc<JobExtractionBudget>,
) -> Result<impl std::io::Read, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|error| format!("failed to open xz: {error}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    let memory_limit = crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES.min(budget.max_memory_bytes());
    crate::ingest::xz_multistream_decoder(file, memory_limit)
        .map_err(|error| format!("failed to open xz decoder: {error}"))
}

fn open_filesystem_xz_decoder(
    archive_path: &Path,
    budget: &Arc<JobExtractionBudget>,
    xz_worker_threads: usize,
) -> Result<FilesystemXzDecoder<BudgetedReader<std::fs::File>>, String> {
    let mut probe =
        std::fs::File::open(archive_path).map_err(|error| format!("failed to open xz: {error}"))?;
    let memory_limit = crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES.min(budget.max_memory_bytes());

    if matches!(
        crate::ingest::xz_filesystem_decoder_kind(&mut probe),
        crate::ingest::XzFilesystemDecoderKind::Parallel
    ) && let Ok(permit) = XZ_MT_DECODER_PERMIT.try_lock()
    {
        let file = std::fs::File::open(archive_path)
            .map_err(|error| format!("failed to open xz: {error}"))?;
        let file = BudgetedReader::new(file, Arc::clone(budget));
        if let Ok(decoder) =
            crate::ingest::xz_parallel_decoder(file, memory_limit, xz_worker_threads)
        {
            return Ok(FilesystemXzDecoder::Parallel {
                decoder,
                _permit: permit,
            });
        }
    }

    let file =
        std::fs::File::open(archive_path).map_err(|error| format!("failed to open xz: {error}"))?;
    let file = BudgetedReader::new(file, Arc::clone(budget));
    crate::ingest::xz_multistream_decoder(file, memory_limit)
        .map(FilesystemXzDecoder::Sequential)
        .map_err(|error| format!("failed to open xz decoder: {error}"))
}

#[allow(clippy::too_many_arguments)]
fn extract_split(
    file_paths: &[PathBuf],
    root: &ExtractionRoot,
    budget: &Arc<JobExtractionBudget>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
    phase_counters: Option<Arc<PhaseCounters>>,
    joined_output_already_present: Option<PathBuf>,
) -> Result<Vec<String>, String> {
    // Output filename: the base name from the set (e.g., "movie.mkv" from "movie.mkv.001")
    let first_name = file_paths[0]
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let output_name = if let Some(dot_pos) = first_name.rfind('.') {
        &first_name[..dot_pos]
    } else {
        &first_name
    };
    let safe_path = root
        .validate_relative_path(output_name)
        .map_err(|error| budget.reject_unsafe_path(error))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });

    // Joining is never an overwrite. The file the parts concatenate into can
    // already exist — a recovery set computed over it reads the parts as one
    // file and installs it, damage repaired — and a second join would put the
    // parts' bytes, hole and all, back over the copy that was verified. The
    // member is still reported extracted: it exists, so the job moves on and
    // finalization delivers the copy that is already there.
    if let Some(existing) = joined_output_already_present {
        let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
            job_id,
            set_name: set_name.to_string(),
            member: output_name.to_string(),
        });
        tracing::info!(
            job_id = job_id.0,
            member = %output_name,
            path = %existing.display(),
            parts = file_paths.len(),
            "split join skipped — the joined file is already present and verified"
        );
        return Ok(vec![output_name.to_string()]);
    }

    let reader = crate::pipeline::archive::split_reader::SplitFileReader::open(file_paths)
        .map_err(|e| format!("failed to open split files: {e}"))?;
    let mut reader = BudgetedReader::new(reader, Arc::clone(budget));
    let outfile = root.create_file(&safe_path, budget)?;
    let attempt = phase_counters.as_ref().map(|counters| {
        let total = file_paths
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok().map(|metadata| metadata.len()))
            .sum::<u64>();
        counters.total_bytes.fetch_add(total, Ordering::Relaxed);
        Arc::new(PhaseAttemptCounters::new(Arc::clone(counters)))
    });
    let mut outfile: Box<dyn Write> = if let Some(attempt) = attempt.as_ref() {
        Box::new(CountingWriter::new(outfile, Arc::clone(attempt)))
    } else {
        Box::new(outfile)
    };
    let bytes_written = match std::io::copy(&mut reader, &mut outfile) {
        Ok(bytes) => {
            if let Some(attempt) = &attempt {
                attempt.commit();
            }
            bytes
        }
        Err(error) => {
            if let Some(attempt) = &attempt {
                attempt.rollback();
            }
            return Err(format!("failed to concatenate split files: {error}"));
        }
    };

    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });
    tracing::info!(job_id = job_id.0, member = %output_name, bytes_written, parts = file_paths.len(), "split files joined");

    Ok(vec![output_name.to_string()])
}

impl Pipeline {
    pub(crate) async fn extract_rar_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (volume_paths, cached_headers, password_candidates) = {
            let _state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            (
                self.volume_paths_for_rar_set(job_id, set_name),
                self.load_rar_snapshot(job_id, set_name),
                self.archive_password_candidates_for_set(job_id, set_name),
            )
        };

        if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.to_string())) {
            set_state.active_workers = 1;
            set_state.in_flight_members.clear();
            set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
            if let Some(plan) = set_state.plan.as_mut() {
                plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
            }
        }
        self.inflight_extractions
            .entry(job_id)
            .or_default()
            .insert(set_name.to_string());

        // Collect already-extracted members so we skip them.
        let already_extracted: HashSet<String> = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_owned = set_name.to_string();
        let set_name_for_task = set_name.to_string();
        let event_tx = self.event_tx.clone();
        let output_dir = self.extraction_staging_dir(job_id);
        let budget = self.extraction_budget(job_id, &output_dir)?;
        let root = Arc::new(ExtractionRoot::open(&output_dir)?);
        let task_permit = budget.task_permit_for_root(root)?;
        let set_name_for_result = set_name_owned.clone();
        let shared_kdf_cache = self
            .rar_sets
            .get(&(job_id, set_name.to_string()))
            .map(|state| state.shared_kdf_cache.clone())
            .unwrap_or_else(|| std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()));
        let pp_pool = self.pp_pool.clone();
        let refresh_cached_headers =
            self.rar_volume_paths_need_header_refresh(job_id, set_name, &volume_paths);
        let open_mode = if refresh_cached_headers {
            crate::pipeline::extraction::RarArchiveOpenMode::RefreshProvidedVolumes
        } else {
            crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly
        };
        // Totals are reserved per member at extraction open (see
        // extract_rar_member_to_output); topology may not be rebuilt yet here.
        let phase_counters = self.phase_begin(job_id, JobPhase::Extracting, None);
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || pp_pool.install(move || {
                let _task_permit = task_permit;
                let root = _task_permit.root();
                if volume_paths.is_empty() {
                    return Err(format!("no on-disk RAR volumes for set '{set_name_owned}'"));
                }

                let selection = Self::open_rar_archive_for_extraction_with_password_candidates(
                    RarExtractionOpenRequest {
                        set_name: &set_name_owned,
                        volume_paths: volume_paths.clone(),
                        password_candidates: password_candidates.clone(),
                        cached_headers,
                        shared_kdf_cache: shared_kdf_cache.clone(),
                        open_mode,
                        requested_members: &[],
                        already_extracted: Some(&already_extracted),
                        budget: Some(Arc::clone(&budget)),
                    },
                )?;
                let _memory_permit =
                    budget.reserve_memory_wait(selection.decoder_memory_bytes)?;
                let mut archive = selection.archive;
                let selected_password = selection.password;

                let meta = archive.metadata();
                let archive_password_required = meta.is_encrypted;
                let options = unrar_rs::ExtractOptions {
                    verify: true,
                    password: selected_password.clone(),
                    restore_owners: false,
                };
                let is_solid = archive.is_solid();

                let mut extracted_members = Vec::new();
                let mut failed_members: Vec<(String, String)> = Vec::new();
                let mut validated_password = selection.validated_password;
                for (idx, member) in meta.members.iter().enumerate() {
                    if already_extracted.contains(&member.name) {
                        continue;
                    }

                    let member_password_required = archive_password_required || member.is_encrypted;
                    match Self::extract_rar_member_to_output(
                        &mut archive,
                        crate::pipeline::extraction::RarExtractionContext::new(
                            &volume_paths,
                            &event_tx,
                            job_id,
                            &set_name_for_task,
                            &output_dir,
                            &options,
                        )
                        .with_security(Arc::clone(&root), Arc::clone(&budget))
                        .with_phase_attempt(Some(Arc::new(PhaseAttemptCounters::new(Arc::clone(
                            &phase_counters,
                        ))))),
                        idx,
                    ) {
                        Ok((member_name, bytes_written, total_bytes)) => {
                            if validated_password.is_none() && member_password_required {
                                validated_password = selected_password.clone();
                            }
                            info!(job_id = job_id.0, member = %member_name, bytes_written, total_bytes, "member extracted");
                            let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                                job_id,
                                member: member_name.clone(),
                                bytes_written,
                                total_bytes,
                            });
                            let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
                                job_id,
                                set_name: set_name_for_task.clone(),
                                member: member_name.clone(),
                            });
                            extracted_members.push(member_name);
                        }
                        Err(e) => {
                            let error = e.to_string();
                            let _ = event_tx.send(PipelineEvent::ExtractionMemberFailed {
                                job_id,
                                set_name: set_name_for_task.clone(),
                                member: member.name.clone(),
                                error: error.clone(),
                            });
                            tracing::warn!(member = %member.name, error = %e, "member extraction failed, continuing with remaining members");
                            failed_members.push((member.name.clone(), error));
                            if is_solid {
                                break;
                            }
                        }
                    }
                }

                Ok(FullSetExtractionOutcome {
                    extracted: extracted_members,
                    failed: failed_members,
                    selected_password: validated_password,
                })
            }))
            .await;

            let result = match result {
                Ok(result) => result,
                Err(e) => Err(format!("extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_result,
                    result,
                })
                .await;
        });

        // Extraction runs in background — result comes through extract_done_tx channel.
        Ok(0)
    }

    /// Spawn extraction for a list of archives, tracking each in `inflight_extractions`.
    /// Dispatches to the correct extractor based on archive type. Returns the number
    /// of extractions successfully spawned.
    pub(super) async fn spawn_extractions(
        &mut self,
        job_id: JobId,
        archives: &[(String, crate::jobs::assembly::ArchiveType)],
    ) -> usize {
        let mut spawned = 0;
        for (name, archive_type) in archives {
            self.inflight_extractions
                .entry(job_id)
                .or_default()
                .insert(name.clone());

            let result = match archive_type {
                crate::jobs::assembly::ArchiveType::SevenZip => {
                    self.extract_7z_set(job_id, name).await
                }
                crate::jobs::assembly::ArchiveType::Rar => self.extract_rar_set(job_id, name).await,
                crate::jobs::assembly::ArchiveType::Zip => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Zip)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Tar => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Tar)
                        .await
                }
                crate::jobs::assembly::ArchiveType::TarGz => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::TarGz)
                        .await
                }
                crate::jobs::assembly::ArchiveType::TarBz2 => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::TarBz2)
                        .await
                }
                crate::jobs::assembly::ArchiveType::TarXz => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::TarXz)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Gz => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Gz)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Deflate => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Deflate)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Brotli => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Brotli)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Zstd => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Zstd)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Bzip2 => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Bzip2)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Xz => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Xz)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Split => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Split)
                        .await
                }
            };
            match result {
                Ok(_) => spawned += 1,
                Err(e) => {
                    warn!(job_id = job_id.0, archive = %name, error = %e, "failed to start extraction");
                    if JobExtractionBudget::is_rejection(&e) {
                        if let Some(budget) = self.extraction_budgets.get(&job_id) {
                            budget.cancel_with_error(&e);
                        }
                        self.fail_job(job_id, e);
                        return spawned;
                    }
                    if let Some(inflight) = self.inflight_extractions.get_mut(&job_id) {
                        inflight.remove(name);
                    }
                }
            }
        }
        spawned
    }

    /// Extract a single 7z archive set. Only collects files belonging to the named set.
    pub(crate) async fn extract_7z_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let file_paths = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state
                .assembly
                .archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for set '{set_name}'"))?;

            // Collect files belonging to this set using the topology's volume_map.
            let set_filenames: std::collections::HashSet<&str> =
                topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut parts: Vec<(u32, PathBuf)> = Vec::new();

            for file_asm in state.assembly.files() {
                let current_filename = self.current_filename_for_file(job_id, file_asm);
                if set_filenames.contains(current_filename.as_str()) {
                    let vol = topo.volume_map.get(&current_filename).copied().unwrap_or(0);
                    if let Some(path) = self.resolve_job_input_path(job_id, &current_filename) {
                        parts.push((vol, path));
                    }
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            parts.into_iter().map(|(_, p)| p).collect::<Vec<PathBuf>>()
        };
        let password = self.primary_archive_password_for_job(job_id);

        let output_dir = self.extraction_staging_dir(job_id);
        let budget = self.extraction_budget(job_id, &output_dir)?;
        let root = Arc::new(ExtractionRoot::open(&output_dir)?);
        let task_permit = budget.task_permit_for_root(root)?;
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();
        let pp_pool = self.pp_pool.clone();
        let phase_counters = self.phase_begin(job_id, JobPhase::Extracting, None);
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                pp_pool.install(move || {
                    let _task_permit = task_permit;
                    let root = _task_permit.root();
                    // 7z can encode its own header and payload with different codecs. Reserve
                    // the configured ceiling while this decoder is live so large archives do
                    // not inherit a hardcoded small allowance and concurrent decoders cannot
                    // exceed the shared job budget.
                    let _memory_permit = budget.reserve_memory_wait(budget.max_memory_bytes())?;
                    if file_paths.is_empty() {
                        return Err(format!("no 7z files found for set '{set_name_owned}'"));
                    }

                    let pw = if let Some(ref p) = password {
                        sevenz_rust2::Password::new(p)
                    } else {
                        sevenz_rust2::Password::empty()
                    };
                    let known_total = if file_paths.len() == 1 {
                        let file = std::fs::File::open(&file_paths[0])
                            .map_err(|e| format!("failed to open 7z file: {e}"))?;
                        let file = BudgetedReader::new(file, Arc::clone(&budget));
                        let archive_reader = sevenz_rust2::ArchiveReader::new(file, pw.clone())
                            .map_err(|e| format!("failed to read 7z archive: {e}"))?;
                        for entry in &archive_reader.archive().files {
                            budget.check_member_metadata(entry.name(), entry.size())?;
                            root.validate_relative_path(entry.name())
                                .map_err(|error| budget.reject_unsafe_path(error))?;
                        }
                        archive_reader
                            .archive()
                            .files
                            .iter()
                            .filter(|entry| !entry.is_directory())
                            .map(|entry| entry.size())
                            .sum::<u64>()
                    } else {
                        let reader = crate::pipeline::archive::split_reader::SplitFileReader::open(
                            &file_paths,
                        )
                        .map_err(|e| format!("failed to open 7z split files: {e}"))?;
                        let reader = BudgetedReader::new(reader, Arc::clone(&budget));
                        let archive_reader =
                            sevenz_rust2::ArchiveReader::new(reader, pw.clone())
                                .map_err(|e| format!("failed to read 7z archive: {e}"))?;
                        for entry in &archive_reader.archive().files {
                            budget.check_member_metadata(entry.name(), entry.size())?;
                            root.validate_relative_path(entry.name())
                                .map_err(|error| budget.reject_unsafe_path(error))?;
                        }
                        archive_reader
                            .archive()
                            .files
                            .iter()
                            .filter(|entry| !entry.is_directory())
                            .map(|entry| entry.size())
                            .sum::<u64>()
                    };
                    phase_counters
                        .total_bytes
                        .fetch_add(known_total, Ordering::Relaxed);

                    let mut extracted_members = Vec::new();
                    let extracted_members_ref = &mut extracted_members;
                    let event_tx_ref = &event_tx;
                    let root_ref = &root;
                    let budget_ref = &budget;

                    let extract_fn = |entry: &sevenz_rust2::ArchiveEntry,
                                      reader: &mut dyn std::io::Read,
                                      _dest: &PathBuf|
                     -> Result<bool, sevenz_rust2::Error> {
                        let safe_path =
                            root_ref
                                .validate_relative_path(entry.name())
                                .map_err(|error| {
                                    std::io::Error::other(budget_ref.reject_unsafe_path(error))
                                })?;
                        budget_ref
                            .check_member_metadata(entry.name(), entry.size())
                            .map_err(std::io::Error::other)?;
                        if entry.is_directory() {
                            root_ref
                                .create_dir(&safe_path, budget_ref)
                                .map_err(std::io::Error::other)?;
                            return Ok(true);
                        }

                        let _ = event_tx_ref.send(PipelineEvent::ExtractionMemberStarted {
                            job_id,
                            set_name: set_name_owned.clone(),
                            member: entry.name().to_string(),
                        });

                        let file = root_ref
                            .create_file(&safe_path, budget_ref)
                            .map_err(std::io::Error::other)?;
                        let attempt =
                            Arc::new(PhaseAttemptCounters::new(Arc::clone(&phase_counters)));
                        let mut file = CountingWriter::new(file, Arc::clone(&attempt));
                        let bytes_written = match std::io::copy(reader, &mut file) {
                            Ok(bytes) => {
                                attempt.commit();
                                bytes
                            }
                            Err(error) => {
                                attempt.rollback();
                                return Err(error.into());
                            }
                        };

                        tracing::info!(
                            job_id = job_id.0,
                            member = entry.name(),
                            bytes_written,
                            total_bytes = entry.size(),
                            "member extracted"
                        );
                        let _ = event_tx_ref.send(PipelineEvent::ExtractionProgress {
                            job_id,
                            member: entry.name().to_string(),
                            bytes_written,
                            total_bytes: entry.size(),
                        });
                        let _ = event_tx_ref.send(PipelineEvent::ExtractionMemberFinished {
                            job_id,
                            set_name: set_name_owned.clone(),
                            member: entry.name().to_string(),
                        });

                        extracted_members_ref.push(entry.name().to_string());
                        Ok(true)
                    };

                    if file_paths.len() == 1 {
                        let file = std::fs::File::open(&file_paths[0])
                            .map_err(|e| format!("failed to open 7z file: {e}"))?;
                        let file = BudgetedReader::new(file, Arc::clone(&budget));
                        sevenz_rust2::decompress_with_extract_fn_and_password(
                            file,
                            &output_dir,
                            pw,
                            extract_fn,
                        )
                        .map_err(|e| format!("7z extraction failed: {e}"))?;
                    } else {
                        let reader = crate::pipeline::archive::split_reader::SplitFileReader::open(
                            &file_paths,
                        )
                        .map_err(|e| format!("failed to open 7z split files: {e}"))?;
                        let reader = BudgetedReader::new(reader, Arc::clone(&budget));
                        sevenz_rust2::decompress_with_extract_fn_and_password(
                            reader,
                            &output_dir,
                            pw,
                            extract_fn,
                        )
                        .map_err(|e| format!("7z extraction failed: {e}"))?;
                    }

                    Ok(FullSetExtractionOutcome {
                        extracted: extracted_members,
                        failed: Vec::new(),
                        selected_password: None,
                    })
                })
            })
            .await;

            let result = match result {
                Ok(r) => r,
                Err(e) => Err(format!("7z extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_channel,
                    result,
                })
                .await;
        });

        // Return Ok(0) for now — actual result comes through the channel.
        Ok(0)
    }

    /// The file a split set's parts join into, when it is already on disk and
    /// there is reason to believe it is the whole thing.
    ///
    /// Two ways to believe it, and either is enough:
    ///
    /// - it measures the sum of the parts, which is what a join of those parts
    ///   would have produced anyway; or
    /// - the job carries a settled PAR2 verdict and the recovery set describes
    ///   a file of this name at exactly the length now on disk. That is the
    ///   stronger arm and the one that matters when a part landed short of its
    ///   articles: the sum of what arrived is then *under* the true length, and
    ///   only the recovery set knows what the file should measure.
    fn present_split_join_output(
        &self,
        job_id: JobId,
        set_name: &str,
        file_paths: &[std::path::PathBuf],
    ) -> Option<std::path::PathBuf> {
        let path = self.resolve_job_input_path(job_id, set_name)?;
        let metadata = std::fs::metadata(&path).ok()?;
        if !metadata.is_file() {
            return None;
        }
        let present_len = metadata.len();

        let parts_total: u64 = file_paths
            .iter()
            .filter_map(|part| std::fs::metadata(part).ok())
            .map(|part| part.len())
            .sum();
        if parts_total > 0 && parts_total == present_len {
            return Some(path);
        }

        if !self.par2_verified.contains(&job_id) {
            return None;
        }
        let sanitized_set_name = weaver_model::files::sanitize_download_filename(set_name);
        let described = self.par2_set(job_id)?.files.values().any(|description| {
            weaver_model::files::sanitize_download_filename(&description.filename)
                == sanitized_set_name
                && description.length == present_len
        });
        described.then_some(path)
    }

    /// Extract a simple (non-RAR, non-7z) archive: ZIP, tar, tar.gz, tar.bz2, tar.xz, gz,
    /// deflate, br, zstd, bz2, xz, or split.
    pub(crate) async fn extract_simple_archive(
        &mut self,
        job_id: JobId,
        set_name: &str,
        kind: SimpleArchiveKind,
    ) -> Result<u32, String> {
        let file_paths = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state
                .assembly
                .archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for set '{set_name}'"))?;

            let set_filenames: std::collections::HashSet<&str> =
                topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut parts: Vec<(u32, std::path::PathBuf)> = Vec::new();

            for file_asm in state.assembly.files() {
                let current_filename = self.current_filename_for_file(job_id, file_asm);
                if set_filenames.contains(current_filename.as_str()) {
                    let vol = topo.volume_map.get(&current_filename).copied().unwrap_or(0);
                    if let Some(path) = self.resolve_job_input_path(job_id, &current_filename) {
                        parts.push((vol, path));
                    }
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            parts
                .into_iter()
                .map(|(_, p)| p)
                .collect::<Vec<std::path::PathBuf>>()
        };
        let password = self.primary_archive_password_for_job(job_id);
        let joined_output_already_present = matches!(kind, SimpleArchiveKind::Split)
            .then(|| self.present_split_join_output(job_id, set_name, &file_paths))
            .flatten();

        let output_dir = self.extraction_staging_dir(job_id);
        let budget = self.extraction_budget(job_id, &output_dir)?;
        let root = Arc::new(ExtractionRoot::open(&output_dir)?);
        let task_permit = budget.task_permit_for_root(root)?;
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();
        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();
        let pp_pool = self.pp_pool.clone();
        let xz_worker_threads = pp_pool.current_num_threads();
        let phase_counters = self.phase_begin(job_id, JobPhase::Extracting, None);

        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                pp_pool.install(move || {
                    let _task_permit = task_permit;
                    let root = _task_permit.root();
                    let decoder_memory =
                        simple_decoder_memory_bytes(kind, budget.max_memory_bytes());
                    let _memory_permit = budget.reserve_memory_wait(decoder_memory)?;
                    if file_paths.is_empty() {
                        return Err(format!("no files found for set '{set_name_owned}'"));
                    }

                    let extracted_members = match kind {
                        SimpleArchiveKind::Zip => extract_zip(
                            &file_paths[0],
                            &root,
                            &budget,
                            password.as_deref(),
                            &event_tx,
                            job_id,
                            &set_name_owned,
                            Some(Arc::clone(&phase_counters)),
                        )?,
                        SimpleArchiveKind::Tar => extract_tar(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::TarGz => extract_tar_gz(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::TarBz2 => extract_tar_bz2(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::TarXz => extract_tar_xz(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Gz => extract_gz(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Deflate => extract_deflate(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Brotli => extract_brotli(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Zstd => extract_zstd(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Bzip2 => extract_bzip2(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Xz => extract_xz(
                            &file_paths[0],
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                            xz_worker_threads,
                        )?,
                        SimpleArchiveKind::Split => extract_split(
                            &file_paths,
                            &root,
                            &budget,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                            Some(Arc::clone(&phase_counters)),
                            joined_output_already_present,
                        )?,
                    };

                    Ok(FullSetExtractionOutcome {
                        extracted: extracted_members,
                        failed: Vec::new(),
                        selected_password: None,
                    })
                })
            })
            .await;

            let result = match result {
                Ok(r) => r,
                Err(e) => Err(format!("{kind:?} extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_channel,
                    result,
                })
                .await;
        });

        Ok(0)
    }

    /// Persist RAR volume eligibility without deleting source volumes.
    pub(crate) fn try_delete_volumes(&mut self, job_id: JobId, set_name: &str) {
        let key = (job_id, set_name.to_string());
        if self
            .rar_sets
            .get(&key)
            .is_some_and(|state| state.active_workers > 0 || !state.in_flight_members.is_empty())
        {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                "RAR eager delete deferred while extraction workers are active"
            );
            return;
        }
        let Some(plan) = self.rar_sets.get(&key).and_then(|state| state.plan.clone()) else {
            return;
        };
        let volumes: Vec<u32> = self
            .rar_sets
            .get(&key)
            .map(|state| state.facts.keys().copied().collect())
            .unwrap_or_default();
        if volumes.is_empty() {
            return;
        }
        let verified_suspect = self
            .rar_sets
            .get(&key)
            .map(|state| state.verified_suspect_volumes.clone())
            .unwrap_or_default();
        let par2_verification_pending = self.jobs.get(&job_id).is_some_and(|state| {
            state.spec.par2_bytes() > 0
                && !self.par2_bypassed.contains(&job_id)
                && !self.par2_verified.contains(&job_id)
        });
        let mut deleted_now = Vec::new();
        let mut ownership_ready = Vec::new();

        for volume in volumes {
            let Some(decision) = plan.delete_decisions.get(&volume) else {
                continue;
            };
            let Some(filename) =
                Self::rar_volume_filename(&plan.topology.volume_map, volume).map(str::to_string)
            else {
                debug!(
                    job_id = job_id.0,
                    set_name, volume, "RAR eager delete skipped: no filename for volume"
                );
                continue;
            };

            let claim_clean = Self::claim_clean_rar_volume(decision);
            let verification_blocked =
                par2_verification_pending || verified_suspect.contains(&volume);
            let solid_blocked = plan.is_solid;
            let waiting_on_retry = plan.waiting_on_volumes.contains(&volume);
            let failed_member_claim = !decision.failed_owners.is_empty();
            let already_deleted = self
                .eagerly_deleted
                .get(&job_id)
                .is_some_and(|deleted| deleted.contains(&filename));
            let should_delete = decision.ownership_eligible
                && !waiting_on_retry
                && !failed_member_claim
                && !verification_blocked
                && !solid_blocked
                && !already_deleted;

            if should_delete {
                let Some(path) = self.resolve_job_input_path(job_id, &filename) else {
                    return;
                };
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        self.eagerly_deleted
                            .entry(job_id)
                            .or_default()
                            .insert(filename.clone());
                        deleted_now.push(volume);
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            file = %filename,
                            owners = ?decision.owners,
                            "RAR volume eagerly deleted"
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            file = %filename,
                            owners = ?decision.owners,
                            "RAR eager delete found volume already missing"
                        );
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            file = %filename,
                            owners = ?decision.owners,
                            error = %error,
                            "RAR eager delete failed"
                        );
                    }
                }
            } else {
                let mut reasons = Vec::new();
                if !decision.pending_owners.is_empty() {
                    reasons.push(format!("pending_members={:?}", decision.pending_owners));
                }
                if !decision.failed_owners.is_empty() {
                    reasons.push(format!("failed_members={:?}", decision.failed_owners));
                }
                if decision.unresolved_boundary {
                    reasons.push("unresolved_boundary".to_string());
                }
                if waiting_on_retry {
                    reasons.push("waiting_on_retry".to_string());
                }
                if failed_member_claim {
                    reasons.push("failed_member_claim".to_string());
                }
                if solid_blocked {
                    reasons.push("solid_archive".to_string());
                }
                if !claim_clean {
                    reasons.push("claims_not_clean".to_string());
                }
                if verified_suspect.contains(&volume) {
                    reasons.push("verified_suspect".to_string());
                }
                if par2_verification_pending {
                    reasons.push("par2_verification_pending".to_string());
                }
                if already_deleted {
                    reasons.push("already_deleted".to_string());
                }
                if decision.ownership_eligible && !waiting_on_retry && !failed_member_claim {
                    ownership_ready.push(volume);
                }
                debug!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volume,
                    file = %filename,
                    owners = ?decision.owners,
                    clean_owners = ?decision.clean_owners,
                    failed_owners = ?decision.failed_owners,
                    pending_owners = ?decision.pending_owners,
                    reasons = ?reasons,
                    "RAR eager delete retained volume"
                );
            }
        }

        info!(
            job_id = job_id.0,
            set_name = %set_name,
            solid = plan.is_solid,
            ownership_ready = ?ownership_ready,
            deleted_now = ?deleted_now,
            verified_suspect_volumes = ?verified_suspect,
            "RAR eager delete audit"
        );
    }
}
