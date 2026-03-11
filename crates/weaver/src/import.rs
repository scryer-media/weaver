use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::info;

use weaver_core::id::JobId;
use weaver_core::release_name::{append_original_title_metadata, derive_release_name};
use weaver_nzb::{Nzb, parse_nzb};
use weaver_scheduler::{FileSpec, JobSpec, SegmentSpec};

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
        nzb_path.file_stem().and_then(|s| s.to_str()),
        nzb.meta.title.as_deref(),
    );

    let name = derive_release_name(
        nzb_path.file_stem().and_then(|s| s.to_str()),
        nzb.meta.title.as_deref(),
    );

    let password = extract_password(nzb, nzb_path);

    let mut files = Vec::with_capacity(nzb.files.len());
    let mut total_bytes: u64 = 0;

    for nzb_file in &nzb.files {
        let filename = nzb_file.filename().unwrap_or("unknown").to_string();
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

/// Extract password from NZB metadata or the {{password}} filename convention.
fn extract_password(nzb: &Nzb, nzb_path: &Path) -> Option<String> {
    // 1. NZB meta password field.
    if let Some(pw) = &nzb.meta.password
        && !pw.is_empty()
    {
        return Some(pw.clone());
    }

    // 2. Filename convention: "Some.Release.{{password}}.nzb"
    if let Some(stem) = nzb_path.file_stem().and_then(|s| s.to_str())
        && let Some(start) = stem.find("{{")
        && let Some(end) = stem[start..].find("}}")
    {
        let pw = &stem[start + 2..start + end];
        if !pw.is_empty() {
            return Some(pw.to_string());
        }
    }

    None
}

#[derive(Debug)]
pub enum ImportError {
    Parse(weaver_nzb::NzbError),
    Empty,
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Parse(e) => write!(f, "NZB parse error: {e}"),
            ImportError::Empty => write!(f, "NZB contains no files"),
        }
    }
}

impl std::error::Error for ImportError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_password_from_filename() {
        let path = Path::new("/downloads/Some.Release.{{mysecret}}.nzb");
        let nzb = Nzb {
            meta: weaver_nzb::NzbMeta::default(),
            files: vec![],
        };
        assert_eq!(extract_password(&nzb, path), Some("mysecret".to_string()));
    }

    #[test]
    fn extract_password_from_meta() {
        let path = Path::new("/downloads/normal.nzb");
        let nzb = Nzb {
            meta: weaver_nzb::NzbMeta {
                password: Some("fromxml".to_string()),
                ..Default::default()
            },
            files: vec![],
        };
        assert_eq!(extract_password(&nzb, path), Some("fromxml".to_string()));
    }

    #[test]
    fn meta_password_takes_priority() {
        let path = Path::new("/downloads/release.{{filepw}}.nzb");
        let nzb = Nzb {
            meta: weaver_nzb::NzbMeta {
                password: Some("metapw".to_string()),
                ..Default::default()
            },
            files: vec![],
        };
        // Meta password takes priority.
        assert_eq!(extract_password(&nzb, path), Some("metapw".to_string()));
    }

    #[test]
    fn no_password() {
        let path = Path::new("/downloads/normal.nzb");
        let nzb = Nzb {
            meta: weaver_nzb::NzbMeta::default(),
            files: vec![],
        };
        assert_eq!(extract_password(&nzb, path), None);
    }
}
