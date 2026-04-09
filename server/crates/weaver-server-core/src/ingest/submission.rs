use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{info, warn};

use crate::SchedulerHandle;
use crate::jobs::JobSpec;
use crate::jobs::ids::JobId;
use crate::settings::SharedConfig;
use weaver_nzb::Nzb;

use super::persisted_nzb;
use crate::ingest::{append_original_title_metadata, derive_release_name};
use crate::jobs::{FileSpec, SegmentSpec};
use weaver_model::files::FileRole;

static NEXT_API_JOB_ID: AtomicU64 = AtomicU64::new(10_000);

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
    Scheduler(#[from] crate::SchedulerError),
    #[error("NZB fetch failed: {0}")]
    Fetch(String),
    #[error("NZB response was not valid XML")]
    NotXml,
}

pub fn init_job_counter(start: u64) {
    NEXT_API_JOB_ID.store(start.max(10_000), Ordering::Relaxed);
}

pub fn next_submission_job_id() -> JobId {
    JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed))
}

pub fn nzb_to_submission_spec(
    nzb: &Nzb,
    filename: Option<&str>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> JobSpec {
    let metadata = append_original_title_metadata(
        metadata,
        filename.and_then(|value| value.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let name = derive_release_name(
        filename.and_then(|value| value.strip_suffix(".nzb")),
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
        // If the extracted name is obfuscated, keep it - PAR2 deobfuscation will
        // rename it post-repair using 16KB hash matching.
        let filename = nzb_file
            .extract_filename()
            .map(|(name, _confidence)| name)
            .or_else(|| nzb_file.filename().map(str::to_string))
            .unwrap_or_else(|| "unknown".to_string());
        let role = FileRole::from_filename(&filename);

        let segments: Vec<SegmentSpec> = nzb_file
            .segments
            .iter()
            .map(|segment| SegmentSpec {
                number: segment.number.saturating_sub(1),
                bytes: segment.bytes,
                message_id: segment.message_id.clone(),
            })
            .collect();

        let file_bytes: u64 = nzb_file.total_bytes();
        total_bytes += file_bytes;

        files.push(FileSpec {
            filename,
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

pub async fn resolve_submission_category(
    config: &SharedConfig,
    category: Option<&str>,
) -> Option<String> {
    let category = category?;
    let cfg = config.read().await;
    if cfg.categories.is_empty() {
        return Some(category.to_string());
    }

    match crate::categories::resolve_category(&cfg.categories, category) {
        Some(canonical) => Some(canonical),
        None => {
            warn!(category = %category, "unknown category, submitting without category");
            None
        }
    }
}

pub async fn nzb_storage_dir(config: &SharedConfig) -> PathBuf {
    let cfg = config.read().await;
    Path::new(&cfg.data_dir).join(".weaver-nzbs")
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

    let job_id = next_submission_job_id();
    let spec = nzb_to_submission_spec(
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
    let persist_result = tokio::task::spawn_blocking(move || {
        persisted_nzb::write_compressed_nzb(&write_path, &compressed_bytes)
    })
    .await
    .map_err(|error| SubmitNzbError::Save(std::io::Error::other(error.to_string())))?;
    if let Err(error) = persist_result {
        persisted_nzb::remove_persisted_nzb_if_exists(&nzb_path).await;
        return Err(SubmitNzbError::Save(error));
    }

    if let Err(error) = handle.add_job(job_id, spec.clone(), nzb_path.clone()).await {
        persisted_nzb::remove_persisted_nzb_if_exists(&nzb_path).await;
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
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
    })
}

pub async fn fetch_nzb_from_url(
    client: &reqwest::Client,
    url: &str,
) -> Result<(Vec<u8>, Option<String>), SubmitNzbError> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| SubmitNzbError::Fetch(format!("request failed: {error}")))?;

    if !response.status().is_success() {
        return Err(SubmitNzbError::Fetch(format!("HTTP {}", response.status())));
    }

    let filename = extract_filename_from_response(&response, url);

    let nzb_bytes = response
        .bytes()
        .await
        .map_err(|error| SubmitNzbError::Fetch(format!("body read failed: {error}")))?;

    if nzb_bytes.is_empty() {
        return Err(SubmitNzbError::Fetch("empty response body".into()));
    }

    let nzb = weaver_nzb::parse_nzb(&nzb_bytes)?;
    if nzb.files.is_empty() {
        return Err(SubmitNzbError::Empty);
    }

    Ok((nzb_bytes.to_vec(), filename))
}

pub async fn submit_uploaded_nzb_reader<R>(
    handle: &SchedulerHandle,
    config: &SharedConfig,
    source: R,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError>
where
    R: Read + Send + 'static,
{
    let resolved_category = resolve_submission_category(config, category.as_deref()).await;
    let job_id = next_submission_job_id();
    let nzb_dir = nzb_storage_dir(config).await;
    tokio::fs::create_dir_all(&nzb_dir)
        .await
        .map_err(SubmitNzbError::CreateStorageDir)?;
    let nzb_path = nzb_dir.join(format!("{}.nzb", job_id.0));
    let persisted_path = nzb_path.clone();
    let persist_result = tokio::task::spawn_blocking(move || {
        let mut source = source;
        super::persisted_nzb::persist_decoded_nzb_reader(&persisted_path, &mut source)
    })
    .await
    .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
    let nzb = match persist_result {
        Ok(nzb) => nzb,
        Err(super::persisted_nzb::PersistedNzbError::Io(error)) => {
            super::persisted_nzb::remove_persisted_nzb_if_exists(&nzb_path).await;
            return Err(SubmitNzbError::Save(error));
        }
        Err(super::persisted_nzb::PersistedNzbError::Parse(error)) => {
            super::persisted_nzb::remove_persisted_nzb_if_exists(&nzb_path).await;
            return Err(SubmitNzbError::Parse(error));
        }
    };
    if nzb.files.is_empty() {
        super::persisted_nzb::remove_persisted_nzb_if_exists(&nzb_path).await;
        return Err(SubmitNzbError::Empty);
    }

    let spec = nzb_to_submission_spec(
        &nzb,
        filename.as_deref(),
        password,
        resolved_category,
        metadata,
    );

    if let Err(error) = handle.add_job(job_id, spec.clone(), nzb_path.clone()).await {
        super::persisted_nzb::remove_persisted_nzb_if_exists(&nzb_path).await;
        return Err(error.into());
    }

    Ok(SubmittedJob {
        job_id,
        spec,
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
    })
}

fn extract_filename_from_response(response: &reqwest::Response, url: &str) -> Option<String> {
    if let Some(content_disposition) = response.headers().get(reqwest::header::CONTENT_DISPOSITION)
        && let Ok(value) = content_disposition.to_str()
        && let Some(name) = parse_content_disposition_filename(value)
    {
        return Some(name);
    }

    let path = url.split('?').next().unwrap_or(url);
    let segment = path
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty() && segment.contains('.'))?;
    Some(segment.to_string())
}

fn parse_content_disposition_filename(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let idx = lower.find("filename=")?;
    let rest = &header[idx + "filename=".len()..];
    let rest = rest.trim_start();
    if let Some(stripped) = rest.strip_prefix('"') {
        let end = stripped.find('"')?;
        let name = &stripped[..end];
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    } else {
        let name = rest.split(';').next()?.trim();
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    }
}
