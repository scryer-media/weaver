use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::info;

use crate::ingest::{append_original_title_metadata, derive_release_name};
use crate::jobs::ids::JobId;
use crate::jobs::{
    ArchivePasswordCandidate, ArchivePasswordSource, FileSpec, JobSpec, SegmentSpec,
};
use weaver_model::files::{FileRole, unique_download_filenames};
use weaver_nzb::{Nzb, parse_nzb};

/// Global counter for generating unique job IDs.
static NEXT_JOB_ID: AtomicU64 = AtomicU64::new(1);

/// Import an NZB file and return a JobId + JobSpec ready for the scheduler.
pub fn import_nzb(nzb_bytes: &[u8], nzb_path: &Path) -> Result<(JobId, JobSpec), ImportError> {
    let nzb = parse_nzb(nzb_bytes).map_err(ImportError::Parse)?;

    if nzb.files.is_empty() {
        return Err(ImportError::Empty);
    }

    let job_id = JobId(NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed));
    let spec = nzb_to_spec(&nzb, nzb_path, None, vec![]);

    info!(
        job_id = job_id.0,
        name = %spec.name,
        files = spec.files.len(),
        total_bytes = spec.total_bytes,
        password = spec.password.is_some(),
        "imported NZB"
    );

    Ok((job_id, spec))
}

/// Convert a parsed NZB into a JobSpec. Reused by both fresh imports and
/// recovery (re-parsing an NZB to rebuild the spec for a recovered job).
pub fn nzb_to_spec(
    nzb: &Nzb,
    nzb_path: &Path,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> JobSpec {
    let metadata = append_original_title_metadata(
        metadata,
        nzb_path.file_stem().and_then(|stem| stem.to_str()),
        nzb.meta.title.as_deref(),
    );

    let name = derive_release_name(
        nzb_path.file_stem().and_then(|stem| stem.to_str()),
        nzb.meta.title.as_deref(),
    );

    let password = first_password_candidate(nzb, nzb_path, None);

    let mut files = Vec::with_capacity(nzb.files.len());
    let mut total_bytes: u64 = 0;
    let filenames = unique_download_filenames(
        nzb.files
            .iter()
            .map(|nzb_file| nzb_file.filename().unwrap_or("unknown")),
    );

    for (nzb_file, filename) in nzb.files.iter().zip(filenames) {
        let role = FileRole::from_filename(&filename);

        let segments: Vec<SegmentSpec> = nzb_file
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, segment)| SegmentSpec {
                ordinal: ordinal as u32,
                article_number: segment.number,
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

pub fn normalize_archive_password_candidate(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim();
    if value.is_empty() {
        return None;
    }
    let normalized = value.to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "0" | "1" | "true" | "false" | "yes" | "no" | "passworded" | "protected"
    ) {
        return None;
    }
    Some(value.to_string())
}

pub fn nzb_password_candidates(
    nzb: &Nzb,
    nzb_path: &Path,
    explicit_password: Option<&str>,
) -> Vec<ArchivePasswordCandidate> {
    let mut candidates = Vec::new();
    push_password_candidate(
        &mut candidates,
        ArchivePasswordSource::Explicit,
        explicit_password,
    );
    push_password_candidate(
        &mut candidates,
        ArchivePasswordSource::NzbMeta,
        nzb.meta.password.as_deref(),
    );

    if let Some(stem) = nzb_path.file_stem().and_then(|segment| segment.to_str())
        && let Some(start) = stem.find("{{")
        && let Some(end) = stem[start..].find("}}")
    {
        push_password_candidate(
            &mut candidates,
            ArchivePasswordSource::FilenameConvention,
            Some(&stem[start + 2..start + end]),
        );
    }

    candidates
}

fn first_password_candidate(
    nzb: &Nzb,
    nzb_path: &Path,
    explicit_password: Option<&str>,
) -> Option<String> {
    nzb_password_candidates(nzb, nzb_path, explicit_password)
        .into_iter()
        .next()
        .map(|candidate| candidate.value().to_string())
}

fn push_password_candidate(
    candidates: &mut Vec<ArchivePasswordCandidate>,
    source: ArchivePasswordSource,
    raw: Option<&str>,
) {
    let Some(value) = normalize_archive_password_candidate(raw) else {
        return;
    };
    if candidates
        .iter()
        .any(|candidate| candidate.value() == value.as_str())
    {
        return;
    }
    candidates.push(ArchivePasswordCandidate::new(source, value));
}

#[derive(Debug)]
pub enum ImportError {
    Parse(weaver_nzb::NzbError),
    Empty,
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(error) => write!(f, "NZB parse error: {error}"),
            Self::Empty => write!(f, "NZB contains no files"),
        }
    }
}

impl std::error::Error for ImportError {}
