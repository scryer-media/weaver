use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::categories::CategoryConfig;
use crate::ingest::{SubmitNzbError, submit_nzb_bytes};
use crate::settings::SharedConfig;
use crate::{Database, SchedulerHandle};

use super::WatchFolderConfig;
use super::intake::{self, IntakeError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanIssue {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkerRename {
    pub from: String,
    pub to: String,
    pub marker: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchFolderScanReport {
    pub discovered_files: Vec<String>,
    pub queued_nzbs: Vec<String>,
    pub skipped_inputs: Vec<ScanIssue>,
    pub permanent_errors: Vec<ScanIssue>,
    pub transient_errors: Vec<ScanIssue>,
    pub marker_renamed_sources: Vec<MarkerRename>,
}

impl WatchFolderScanReport {
    fn push_transient(&mut self, path: &Path, reason: impl Into<String>) {
        self.transient_errors.push(ScanIssue {
            path: display_path(path),
            reason: reason.into(),
        });
    }

    fn push_permanent(&mut self, path: &Path, reason: impl Into<String>) {
        self.permanent_errors.push(ScanIssue {
            path: display_path(path),
            reason: reason.into(),
        });
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WatchFolderScannerError {
    #[error("watch folder path is not configured")]
    MissingPath,
    #[error("watch folder path does not exist: {0}")]
    MissingDirectory(String),
    #[error("watch folder path is not a directory: {0}")]
    NotDirectory(String),
    #[error("failed to read watch folder: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
struct Candidate {
    path: PathBuf,
    category: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Clone)]
pub struct WatchFolderScanner {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    scan_lock: Arc<Mutex<()>>,
}

impl WatchFolderScanner {
    pub fn new(db: Database, handle: SchedulerHandle, config: SharedConfig) -> Self {
        Self {
            db,
            handle,
            config,
            scan_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn scan_once(
        &self,
        settings: WatchFolderConfig,
    ) -> Result<WatchFolderScanReport, WatchFolderScannerError> {
        let _guard = self.scan_lock.lock().await;
        let path = settings
            .normalized_path()
            .ok_or(WatchFolderScannerError::MissingPath)?;
        let root = PathBuf::from(path);
        if !root.exists() {
            return Err(WatchFolderScannerError::MissingDirectory(display_path(
                &root,
            )));
        }
        if !root.is_dir() {
            return Err(WatchFolderScannerError::NotDirectory(display_path(&root)));
        }

        let categories = {
            let cfg = self.config.read().await;
            cfg.categories.clone()
        };
        let (candidates, skipped_inputs) = discover_candidates(&root, &categories, &settings)?;
        let mut report = WatchFolderScanReport::default();
        report.skipped_inputs.extend(skipped_inputs);

        for candidate in candidates {
            report.discovered_files.push(display_path(&candidate.path));
            let related_paths = match intake::related_candidate_paths(&candidate.path) {
                Ok(paths) => paths,
                Err(IntakeError::Transient(reason)) => {
                    report.push_transient(&candidate.path, reason);
                    continue;
                }
                Err(IntakeError::Permanent(reason)) => {
                    report.push_permanent(&candidate.path, reason);
                    continue;
                }
            };
            if !await_stable_files(&related_paths, settings.stability_secs).await {
                report.push_transient(&candidate.path, "file is not stable yet");
                continue;
            }
            self.process_candidate(candidate, &mut report).await;
        }

        Ok(report)
    }

    async fn process_candidate(&self, candidate: Candidate, report: &mut WatchFolderScanReport) {
        let source_path = candidate.path.clone();
        let Some(original_name) = source_path
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string)
        else {
            report.push_permanent(&source_path, "source filename is not valid UTF-8");
            return;
        };
        let claimed = match ClaimedSource::claim(&source_path) {
            Ok(claimed) => claimed,
            Err(error) => {
                report.push_transient(&source_path, format!("failed to claim source: {error}"));
                return;
            }
        };
        let intake_result = tokio::task::spawn_blocking({
            let claimed_path = claimed.claimed_path.clone();
            let original_name = original_name.clone();
            move || intake::read_nzbs_from_path_with_name(&claimed_path, &original_name)
        })
        .await;
        let intake_output = match intake_result {
            Ok(Ok(output)) => output,
            Ok(Err(IntakeError::Transient(reason))) => {
                report.push_transient(&source_path, reason);
                claimed.restore(report);
                return;
            }
            Ok(Err(IntakeError::Permanent(reason))) => {
                report.push_permanent(&source_path, reason);
                claimed.mark(".error", report);
                return;
            }
            Err(error) => {
                report.push_transient(&source_path, format!("intake task failed: {error}"));
                claimed.restore(report);
                return;
            }
        };

        let mut queued = 0usize;
        let mut permanent = intake_output.permanent_errors.len();
        let mut transient = 0usize;
        for reason in intake_output.permanent_errors {
            report.push_permanent(&source_path, reason);
        }
        for nzb in intake_output.nzbs {
            let metadata = vec![
                ("source".to_string(), "watch_folder".to_string()),
                ("watch_path".to_string(), display_path(&source_path)),
            ];
            match submit_nzb_bytes(
                &self.db,
                &self.handle,
                &self.config,
                &nzb.bytes,
                Some(nzb.filename.clone()),
                None,
                candidate.category.clone(),
                metadata,
            )
            .await
            {
                Ok(job) => {
                    queued += 1;
                    report
                        .queued_nzbs
                        .push(format!("{}:{}", job.job_id.0, nzb.filename));
                }
                Err(error) if is_permanent_submit_error(&error) => {
                    permanent += 1;
                    report.push_permanent(&source_path, format!("{}: {error}", nzb.filename));
                }
                Err(error) => {
                    transient += 1;
                    report.push_transient(&source_path, format!("{}: {error}", nzb.filename));
                }
            }
        }

        if transient > 0 {
            claimed.restore(report);
            return;
        }
        match (queued, permanent) {
            (0, 0) => {
                report.push_permanent(&source_path, "input did not contain any NZB files");
                claimed.mark(".error", report);
            }
            (0, _) => claimed.mark(".error", report),
            (_, 0) => claimed.mark(".queued", report),
            (_, _) => claimed.mark(".partial", report),
        }
    }
}

fn discover_candidates(
    root: &Path,
    categories: &[CategoryConfig],
    settings: &WatchFolderConfig,
) -> Result<(Vec<Candidate>, Vec<ScanIssue>), WatchFolderScannerError> {
    let mut out = Vec::new();
    let mut skipped = Vec::new();
    discover_files_in_dir(root, None, &mut out, &mut skipped)?;

    if settings.category_from_subfolders {
        let category_map = categories
            .iter()
            .map(|category| (category.name.to_ascii_lowercase(), category.name.clone()))
            .collect::<HashMap<_, _>>();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let Some(category) = category_map.get(&name.to_ascii_lowercase()).cloned() else {
                continue;
            };
            discover_files_in_dir(&path, Some(category), &mut out, &mut skipped)?;
        }
    }

    Ok((out, skipped))
}

fn discover_files_in_dir(
    dir: &Path,
    category: Option<String>,
    out: &mut Vec<Candidate>,
    skipped: &mut Vec<ScanIssue>,
) -> Result<(), WatchFolderScannerError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        if should_ignore_file_name(name) {
            skipped.push(ScanIssue {
                path: display_path(&path),
                reason: "ignored marker-suffixed file".to_string(),
            });
            continue;
        }
        if intake::is_supported_candidate(&path) {
            out.push(Candidate {
                path,
                category: category.clone(),
            });
        } else {
            skipped.push(ScanIssue {
                path: display_path(&path),
                reason: "unsupported watch-folder input".to_string(),
            });
        }
    }
    Ok(())
}

fn should_ignore_file_name(name: &str) -> bool {
    if name.starts_with('.') {
        return true;
    }
    let lower = name.to_ascii_lowercase();
    [".queued", ".error", ".partial", ".processed"]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
}

async fn await_stable_files(paths: &[PathBuf], stability_secs: u64) -> bool {
    let first = match fingerprint_all(paths) {
        Ok(fingerprints) => fingerprints,
        Err(_) => return false,
    };
    if first.is_empty() || first.iter().any(|fingerprint| fingerprint.len == 0) {
        return false;
    }
    if stability_secs > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(stability_secs)).await;
    }
    fingerprint_all(paths).is_ok_and(|second| second == first)
}

fn fingerprint_all(paths: &[PathBuf]) -> std::io::Result<Vec<FileFingerprint>> {
    paths.iter().map(|path| fingerprint(path)).collect()
}

fn fingerprint(path: &Path) -> std::io::Result<FileFingerprint> {
    let metadata = fs::metadata(path)?;
    Ok(FileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

struct ClaimedSource {
    original_path: PathBuf,
    claimed_path: PathBuf,
}

impl ClaimedSource {
    fn claim(source_path: &Path) -> std::io::Result<Self> {
        let file_name = source_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| std::io::Error::other("source filename is not valid UTF-8"))?;
        let parent = source_path
            .parent()
            .ok_or_else(|| std::io::Error::other("source path has no parent"))?;
        for suffix in 0..1000 {
            let candidate_name = format!(".weaver-processing-{suffix}-{file_name}");
            let candidate = parent.join(candidate_name);
            if candidate.exists() {
                continue;
            }
            fs::rename(source_path, &candidate)?;
            return Ok(Self {
                original_path: source_path.to_path_buf(),
                claimed_path: candidate,
            });
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not find an unused processing filename",
        ))
    }

    fn restore(self, report: &mut WatchFolderScanReport) {
        if let Err(error) = fs::rename(&self.claimed_path, &self.original_path) {
            report.push_transient(
                &self.original_path,
                format!("failed to restore claimed source: {error}"),
            );
        }
    }

    fn mark(self, marker: &str, report: &mut WatchFolderScanReport) {
        match append_marker(&self.claimed_path, &self.original_path, marker) {
            Ok(marked_path) => report.marker_renamed_sources.push(MarkerRename {
                from: display_path(&self.original_path),
                to: display_path(&marked_path),
                marker: marker.to_string(),
            }),
            Err(error) => report.push_transient(
                &self.original_path,
                format!("failed to append marker {marker}: {error}"),
            ),
        }
    }
}

fn append_marker(
    current_path: &Path,
    original_path: &Path,
    marker: &str,
) -> std::io::Result<PathBuf> {
    let file_name = original_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| std::io::Error::other("source filename is not valid UTF-8"))?;
    let parent = original_path
        .parent()
        .ok_or_else(|| std::io::Error::other("source path has no parent"))?;
    for suffix in 0..1000 {
        let candidate_name = if suffix == 0 {
            format!("{file_name}{marker}")
        } else {
            format!("{file_name}.{suffix}{marker}")
        };
        let candidate = parent.join(candidate_name);
        if candidate.exists() {
            continue;
        }
        fs::rename(current_path, &candidate)?;
        return Ok(candidate);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not find an unused marker filename",
    ))
}

fn is_permanent_submit_error(error: &SubmitNzbError) -> bool {
    matches!(error, SubmitNzbError::Parse(_) | SubmitNzbError::Empty)
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn category(name: &str) -> CategoryConfig {
        CategoryConfig {
            id: 1,
            name: name.to_string(),
            dest_dir: None,
            aliases: String::new(),
        }
    }

    #[test]
    fn discovery_uses_sab_style_known_category_subfolders() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("root.nzb"), b"root").unwrap();
        fs::create_dir(temp.path().join("Movies")).unwrap();
        fs::write(temp.path().join("Movies/movie.nzb"), b"movie").unwrap();
        fs::create_dir(temp.path().join("Unknown")).unwrap();
        fs::write(temp.path().join("Unknown/ignored.nzb"), b"ignored").unwrap();

        let (candidates, skipped) = discover_candidates(
            temp.path(),
            &[category("Movies")],
            &WatchFolderConfig::default(),
        )
        .unwrap();
        assert!(skipped.is_empty());

        let paths = candidates
            .iter()
            .map(|candidate| {
                candidate
                    .path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert!(paths.contains(&"root.nzb".to_string()));
        assert!(paths.contains(&"movie.nzb".to_string()));
        assert!(!paths.contains(&"ignored.nzb".to_string()));
        assert_eq!(
            candidates
                .iter()
                .find(|candidate| candidate.path.ends_with("movie.nzb"))
                .unwrap()
                .category
                .as_deref(),
            Some("Movies")
        );
    }

    #[test]
    fn discovery_reports_skipped_marker_and_unsupported_inputs() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("release.nzb.queued"), b"done").unwrap();
        fs::write(temp.path().join("notes.txt"), b"notes").unwrap();

        let (candidates, skipped) =
            discover_candidates(temp.path(), &[], &WatchFolderConfig::default()).unwrap();

        assert!(candidates.is_empty());
        assert_eq!(skipped.len(), 2);
        assert!(
            skipped
                .iter()
                .any(|issue| issue.reason == "ignored marker-suffixed file")
        );
        assert!(
            skipped
                .iter()
                .any(|issue| issue.reason == "unsupported watch-folder input")
        );
    }

    #[test]
    fn marker_rename_appends_collision_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("release.nzb.gz");
        fs::write(&source, b"nzb").unwrap();
        fs::write(temp.path().join("release.nzb.gz.queued"), b"old").unwrap();

        let marked = append_marker(&source, &source, ".queued").unwrap();

        assert_eq!(
            marked.file_name().and_then(|value| value.to_str()),
            Some("release.nzb.gz.1.queued")
        );
    }
}
