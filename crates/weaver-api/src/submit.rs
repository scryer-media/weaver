use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{info, warn};

use weaver_core::config::SharedConfig;
use weaver_core::id::JobId;
use weaver_core::release_name::{append_original_title_metadata, derive_release_name};
use weaver_nzb::Nzb;
use weaver_scheduler::{FileSpec, JobSpec, SchedulerHandle, SegmentSpec};

/// Global counter for generating unique job IDs for externally submitted jobs.
static NEXT_API_JOB_ID: AtomicU64 = AtomicU64::new(10_000);
const NZB_COMPRESSION_LEVEL: i32 = 3;

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
    #[error("scheduler error: {0}")]
    Scheduler(#[from] weaver_scheduler::SchedulerError),
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

    // Resolve category against predefined categories (name or alias match).
    let resolved_category = if let Some(ref cat) = category {
        let cfg = config.read().await;
        if cfg.categories.is_empty() {
            // No categories defined yet — pass through unchanged.
            Some(cat.clone())
        } else {
            match weaver_core::config::resolve_category(&cfg.categories, cat) {
                Some(canonical) => Some(canonical),
                None => {
                    warn!(category = %cat, "unknown category, submitting without category");
                    None
                }
            }
        }
    } else {
        None
    };

    let job_id = JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed));
    let spec = nzb_to_spec(
        &nzb,
        filename.as_deref(),
        password,
        resolved_category,
        metadata,
    );

    let data_dir = {
        let cfg = config.read().await;
        PathBuf::from(&cfg.data_dir)
    };
    let nzb_dir = data_dir.join(".weaver-nzbs");
    tokio::fs::create_dir_all(&nzb_dir)
        .await
        .map_err(SubmitNzbError::CreateStorageDir)?;
    let nzb_path = nzb_dir.join(format!("{}.nzb", job_id.0));
    let write_path = nzb_path.clone();
    let compressed_bytes = nzb_bytes.to_vec();
    tokio::task::spawn_blocking(move || write_compressed_nzb(&write_path, &compressed_bytes))
        .await
        .map_err(|error| SubmitNzbError::Save(std::io::Error::other(error.to_string())))?
        .map_err(SubmitNzbError::Save)?;

    handle.add_job(job_id, spec.clone(), nzb_path).await?;

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

fn write_compressed_nzb(nzb_path: &Path, nzb_bytes: &[u8]) -> Result<(), std::io::Error> {
    let file = std::fs::File::create(nzb_path)?;
    let writer = std::io::BufWriter::new(file);
    let mut encoder = zstd::stream::Encoder::new(writer, NZB_COMPRESSION_LEVEL)?;
    encoder.write_all(nzb_bytes)?;
    let mut writer = encoder.finish()?;
    writer.flush()?;
    Ok(())
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
        let fname = nzb_file.filename().unwrap_or("unknown").to_string();
        let role = nzb_file.role();

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
