use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_graphql::UploadValue;
use tracing::{info, warn};

use weaver_core::classify::FileRole;
use weaver_core::config::SharedConfig;
use weaver_core::id::JobId;
use weaver_core::release_name::{append_original_title_metadata, derive_release_name};
use weaver_nzb::Nzb;
use weaver_scheduler::{FileSpec, JobSpec, SchedulerHandle, SegmentSpec};

/// Global counter for generating unique job IDs for externally submitted jobs.
static NEXT_API_JOB_ID: AtomicU64 = AtomicU64::new(10_000);
const NZB_COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadEncoding {
    Plain,
    Zstd,
    Gzip,
    Brotli,
    Deflate,
}

/// Seed the job ID counter so IDs are stable across restarts.
/// Call this on startup with the max job ID found in the journal + 1.
pub fn init_job_counter(start: u64) {
    NEXT_API_JOB_ID.store(start.max(10_000), Ordering::Relaxed);
}

#[derive(Clone)]
pub struct SubmittedJob {
    pub job_id: JobId,
    pub spec: JobSpec,
    pub created_at_epoch_ms: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitNzbError {
    #[error("NZB parse error: {0}")]
    Parse(#[from] weaver_nzb::NzbError),
    #[error("NZB contains no files")]
    Empty,
    #[error("failed to create NZB storage dir: {0}")]
    CreateStorageDir(std::io::Error),
    #[error("failed to save NZB: {0}")]
    Save(std::io::Error),
    #[error("failed to read uploaded NZB: {0}")]
    Upload(std::io::Error),
    #[error("scheduler error: {0}")]
    Scheduler(#[from] weaver_scheduler::SchedulerError),
    #[error("NZB fetch failed: {0}")]
    Fetch(String),
    #[error("NZB response was not valid XML")]
    NotXml,
}

pub async fn submit_nzb_bytes(
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_bytes: &[u8],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    let submit_started = Instant::now();
    let nzb = weaver_nzb::parse_nzb(nzb_bytes)?;
    if nzb.files.is_empty() {
        return Err(SubmitNzbError::Empty);
    }

    let resolved_category = resolve_submission_category(config, category.as_deref()).await;

    let job_id = JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed));
    let spec = nzb_to_spec(
        &nzb,
        filename.as_deref(),
        password,
        resolved_category,
        metadata,
    );

    let nzb_dir = nzb_storage_dir(config).await;
    tokio::fs::create_dir_all(&nzb_dir)
        .await
        .map_err(SubmitNzbError::CreateStorageDir)?;
    let nzb_path = nzb_dir.join(format!("{}.nzb", job_id.0));
    let write_path = nzb_path.clone();
    let compressed_bytes = nzb_bytes.to_vec();
    let persist_result =
        tokio::task::spawn_blocking(move || write_compressed_nzb(&write_path, &compressed_bytes))
            .await
            .map_err(|error| SubmitNzbError::Save(std::io::Error::other(error.to_string())))?;
    if let Err(error) = persist_result {
        cleanup_persisted_nzb(&nzb_path).await;
        return Err(SubmitNzbError::Save(error));
    }

    if let Err(error) = handle.add_job(job_id, spec.clone(), nzb_path.clone()).await {
        cleanup_persisted_nzb(&nzb_path).await;
        return Err(error.into());
    }

    info!(
        job_id = job_id.0,
        name = %spec.name,
        category = spec.category,
        metadata_len = spec.metadata.len(),
        elapsed_ms = submit_started.elapsed().as_millis() as u64,
        "submitted NZB job"
    );

    Ok(SubmittedJob {
        job_id,
        spec,
        created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
    })
}

pub async fn submit_uploaded_nzb(
    handle: &SchedulerHandle,
    config: &SharedConfig,
    upload: UploadValue,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    let submit_started = Instant::now();
    let resolved_category = resolve_submission_category(config, category.as_deref()).await;
    let job_id = JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed));
    let nzb_dir = nzb_storage_dir(config).await;
    tokio::fs::create_dir_all(&nzb_dir)
        .await
        .map_err(SubmitNzbError::CreateStorageDir)?;
    let nzb_path = nzb_dir.join(format!("{}.nzb", job_id.0));
    let persisted_path = nzb_path.clone();
    let persist_result =
        tokio::task::spawn_blocking(move || persist_uploaded_nzb(&persisted_path, upload))
            .await
            .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
    let nzb = match persist_result {
        Ok(nzb) => nzb,
        Err(error) => {
            cleanup_persisted_nzb(&nzb_path).await;
            return Err(error);
        }
    };
    if nzb.files.is_empty() {
        cleanup_persisted_nzb(&nzb_path).await;
        return Err(SubmitNzbError::Empty);
    }

    let spec = nzb_to_spec(
        &nzb,
        filename.as_deref(),
        password,
        resolved_category,
        metadata,
    );

    if let Err(error) = handle.add_job(job_id, spec.clone(), nzb_path.clone()).await {
        cleanup_persisted_nzb(&nzb_path).await;
        return Err(error.into());
    }

    info!(
        job_id = job_id.0,
        name = %spec.name,
        category = spec.category,
        metadata_len = spec.metadata.len(),
        elapsed_ms = submit_started.elapsed().as_millis() as u64,
        "submitted uploaded NZB job"
    );

    Ok(SubmittedJob {
        job_id,
        spec,
        created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
    })
}

async fn resolve_submission_category(
    config: &SharedConfig,
    category: Option<&str>,
) -> Option<String> {
    let cat = category?;
    let cfg = config.read().await;
    if cfg.categories.is_empty() {
        return Some(cat.to_string());
    }

    match weaver_core::config::resolve_category(&cfg.categories, cat) {
        Some(canonical) => Some(canonical),
        None => {
            warn!(category = %cat, "unknown category, submitting without category");
            None
        }
    }
}

async fn nzb_storage_dir(config: &SharedConfig) -> PathBuf {
    let cfg = config.read().await;
    PathBuf::from(&cfg.data_dir).join(".weaver-nzbs")
}

async fn cleanup_persisted_nzb(nzb_path: &Path) {
    if let Err(error) = tokio::fs::remove_file(nzb_path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            path = %nzb_path.display(),
            error = %error,
            "failed to remove orphaned persisted nzb"
        );
    }
}

fn write_compressed_nzb(nzb_path: &Path, nzb_bytes: &[u8]) -> Result<(), std::io::Error> {
    let file = std::fs::File::create(nzb_path)?;
    let writer = std::io::BufWriter::new(file);
    let mut encoder = zstd::stream::Encoder::new(writer, NZB_COMPRESSION_LEVEL)?;
    encoder.write_all(nzb_bytes)?;
    let mut writer = encoder.finish()?;
    writer.flush()?;
    Ok(())
}

fn detect_upload_encoding(upload: &UploadValue) -> UploadEncoding {
    let filename = upload.filename.trim().to_ascii_lowercase();
    if filename.ends_with(".zst") {
        return UploadEncoding::Zstd;
    }
    if filename.ends_with(".gz") || filename.ends_with(".gzip") {
        return UploadEncoding::Gzip;
    }
    if filename.ends_with(".br") {
        return UploadEncoding::Brotli;
    }
    if filename.ends_with(".deflate") {
        return UploadEncoding::Deflate;
    }

    let Some(content_type) = upload.content_type.as_deref() else {
        return UploadEncoding::Plain;
    };
    let normalized = content_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "application/zstd" | "application/x-zstd" | "application/octet-stream+zstd" => {
            UploadEncoding::Zstd
        }
        "application/gzip" | "application/x-gzip" | "application/octet-stream+gzip" => {
            UploadEncoding::Gzip
        }
        "application/brotli" | "application/x-brotli" | "application/octet-stream+brotli" => {
            UploadEncoding::Brotli
        }
        "application/deflate" | "application/x-deflate" | "application/octet-stream+deflate" => {
            UploadEncoding::Deflate
        }
        _ => UploadEncoding::Plain,
    }
}

fn normalize_uploaded_nzb_reader(upload: UploadValue) -> Result<Box<dyn Read>, SubmitNzbError> {
    let encoding = detect_upload_encoding(&upload);
    let source = upload.into_read();

    match encoding {
        UploadEncoding::Plain => Ok(Box::new(source)),
        UploadEncoding::Zstd => {
            let decoder =
                zstd::stream::read::Decoder::new(source).map_err(SubmitNzbError::Upload)?;
            Ok(Box::new(decoder))
        }
        UploadEncoding::Gzip => {
            let decoder = flate2::read::GzDecoder::new(source);
            Ok(Box::new(decoder))
        }
        UploadEncoding::Brotli => {
            let decoder = brotli::Decompressor::new(source, 64 * 1024);
            Ok(Box::new(decoder))
        }
        UploadEncoding::Deflate => {
            let decoder = flate2::read::DeflateDecoder::new(source);
            Ok(Box::new(decoder))
        }
    }
}

fn persist_uploaded_nzb(nzb_path: &Path, upload: UploadValue) -> Result<Nzb, SubmitNzbError> {
    let partial_path = nzb_path.with_extension("nzb.part");
    let persist_result = (|| {
        let mut source = normalize_uploaded_nzb_reader(upload)?;
        let file = std::fs::File::create(&partial_path).map_err(SubmitNzbError::Save)?;
        let writer = std::io::BufWriter::new(file);
        let mut encoder = zstd::stream::Encoder::new(writer, NZB_COMPRESSION_LEVEL)
            .map_err(SubmitNzbError::Save)?;
        std::io::copy(&mut source, &mut encoder).map_err(SubmitNzbError::Save)?;
        let mut writer = encoder.finish().map_err(SubmitNzbError::Save)?;
        writer.flush().map_err(SubmitNzbError::Save)?;
        std::fs::rename(&partial_path, nzb_path).map_err(SubmitNzbError::Save)?;
        let file = std::fs::File::open(nzb_path).map_err(SubmitNzbError::Save)?;
        let decoder = zstd::stream::read::Decoder::new(file).map_err(SubmitNzbError::Save)?;
        weaver_nzb::parse_nzb_reader(BufReader::new(decoder)).map_err(SubmitNzbError::Parse)
    })();

    if persist_result.is_err() {
        let _ = std::fs::remove_file(&partial_path);
    }

    persist_result
}

/// Fetch an NZB from a URL. Returns `(nzb_bytes, optional_filename)`.
pub async fn fetch_nzb_from_url(
    client: &reqwest::Client,
    url: &str,
) -> Result<(Vec<u8>, Option<String>), SubmitNzbError> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| SubmitNzbError::Fetch(format!("request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(SubmitNzbError::Fetch(format!("HTTP {}", response.status())));
    }

    let filename = extract_filename_from_response(&response, url);

    let nzb_bytes = response
        .bytes()
        .await
        .map_err(|e| SubmitNzbError::Fetch(format!("body read failed: {e}")))?;

    if nzb_bytes.is_empty() {
        return Err(SubmitNzbError::Fetch("empty response body".into()));
    }

    let nzb = weaver_nzb::parse_nzb(&nzb_bytes)?;
    if nzb.files.is_empty() {
        return Err(SubmitNzbError::Empty);
    }

    Ok((nzb_bytes.to_vec(), filename))
}

/// Extract a filename from the `Content-Disposition` header or URL path.
fn extract_filename_from_response(response: &reqwest::Response, url: &str) -> Option<String> {
    // Try Content-Disposition: attachment; filename="something.nzb"
    if let Some(cd) = response.headers().get(reqwest::header::CONTENT_DISPOSITION)
        && let Ok(value) = cd.to_str()
        && let Some(name) = parse_content_disposition_filename(value)
    {
        return Some(name);
    }

    // Fall back to last path segment of the URL.
    let path = url.split('?').next().unwrap_or(url);
    let segment = path
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty() && s.contains('.'))?;
    Some(segment.to_string())
}

/// Parse `filename="value"` or `filename=value` from a Content-Disposition header value.
fn parse_content_disposition_filename(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let idx = lower.find("filename=")?;
    let rest = &header[idx + "filename=".len()..];
    let rest = rest.trim_start();
    if let Some(stripped) = rest.strip_prefix('"') {
        // Quoted value.
        let end = stripped.find('"')?;
        let name = &stripped[..end];
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    } else {
        // Unquoted — take until semicolon or end.
        let name = rest.split(';').next()?.trim();
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    }
}

fn nzb_to_spec(
    nzb: &Nzb,
    filename: Option<&str>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> JobSpec {
    let metadata = append_original_title_metadata(
        metadata,
        filename.and_then(|f| f.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let name = derive_release_name(
        filename.and_then(|f| f.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let password = password.or_else(|| {
        nzb.meta
            .password
            .as_ref()
            .filter(|value| !value.is_empty())
            .cloned()
    });

    let mut files = Vec::with_capacity(nzb.files.len());
    let mut total_bytes: u64 = 0;

    for nzb_file in &nzb.files {
        // Prefer the enhanced extraction (handles PRiVATE format, higher confidence).
        // If the extracted name is obfuscated, keep it — PAR2 deobfuscation will
        // rename it post-repair using 16KB hash matching.
        let fname = nzb_file
            .extract_filename()
            .map(|(name, _confidence)| name)
            .or_else(|| nzb_file.filename().map(str::to_string))
            .unwrap_or_else(|| "unknown".to_string());
        let role = FileRole::from_filename(&fname);

        let segments: Vec<SegmentSpec> = nzb_file
            .segments
            .iter()
            .map(|seg| SegmentSpec {
                number: seg.number.saturating_sub(1),
                bytes: seg.bytes,
                message_id: seg.message_id.clone(),
            })
            .collect();

        let file_bytes: u64 = nzb_file.total_bytes();
        total_bytes += file_bytes;

        files.push(FileSpec {
            filename: fname,
            role,
            groups: nzb_file.groups.clone(),
            segments,
        });
    }

    JobSpec {
        name,
        password,
        files,
        total_bytes,
        category,
        metadata,
    }
}
