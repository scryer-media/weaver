use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::categories::CategoryConfig;
use crate::ingest::{SubmissionOptions, SubmitNzbError, submit_nzb_bytes_with_options};
use crate::jobs::SubmissionOrigin;
use crate::runtime::fs as runtime_fs;
use crate::settings::SharedConfig;
use crate::{Database, SchedulerHandle};
use weaver_model::files::{FileRole, archive_base_name};

use super::WatchFolderConfig;
use super::intake::{self, IntakeError};

const PROCESSING_PREFIX: &str = ".weaver-processing-";
const CLAIM_STALE_AFTER: Duration = Duration::from_secs(15 * 60);
const TRANSIENT_ESCALATE_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
const MARKER_SUFFIXES: [&str; 4] = [".queued", ".error", ".partial", ".processed"];
const TERMINAL_MARKER_SUFFIXES: [&str; 3] = [".queued", ".error", ".partial"];

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

#[derive(Debug, Clone)]
struct PendingCandidate {
    candidate: Candidate,
    related_paths: Vec<PathBuf>,
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
    transient_tracker: Arc<Mutex<TransientTracker>>,
}

impl WatchFolderScanner {
    pub fn new(db: Database, handle: SchedulerHandle, config: SharedConfig) -> Self {
        Self {
            db,
            handle,
            config,
            scan_lock: Arc::new(Mutex::new(())),
            transient_tracker: Arc::new(Mutex::new(TransientTracker::default())),
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
        let mut report = WatchFolderScanReport::default();
        recover_stale_claims(&root, &categories, &mut report)?;
        reconcile_related_volume_residue(&root, &categories, &mut report)?;

        let (candidates, skipped_inputs) = discover_candidates(&root, &categories, &settings)?;
        report.skipped_inputs.extend(skipped_inputs);

        let mut pending = Vec::new();
        for candidate in candidates {
            report.discovered_files.push(display_path(&candidate.path));
            let related_paths = match intake::related_candidate_paths(&candidate.path) {
                Ok(paths) => paths,
                Err(IntakeError::Transient(reason)) => {
                    self.record_transient_or_escalate(&candidate.path, reason, &mut report)
                        .await;
                    continue;
                }
                Err(IntakeError::Permanent(reason)) => {
                    report.push_permanent(&candidate.path, reason);
                    continue;
                }
            };
            pending.push(PendingCandidate {
                candidate,
                related_paths,
            });
        }

        let (stable, unstable) = await_stable_candidates(pending, settings.stability_secs).await;
        for pending in unstable {
            self.record_transient_or_escalate(
                &pending.candidate.path,
                "file is not stable yet",
                &mut report,
            )
            .await;
        }
        for pending in stable {
            self.process_candidate(pending, &mut report).await;
        }

        Ok(report)
    }

    async fn process_candidate(
        &self,
        pending: PendingCandidate,
        report: &mut WatchFolderScanReport,
    ) {
        let candidate = pending.candidate;
        let source_path = candidate.path.clone();
        let Some(original_name) = source_path
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string)
        else {
            report.push_permanent(&source_path, "source filename is not valid UTF-8");
            return;
        };
        let claimed = match ClaimedSource::claim(&source_path, pending.related_paths) {
            Ok(claimed) => claimed,
            Err(error) => {
                self.record_transient_or_escalate(
                    &source_path,
                    format!("failed to claim source: {error}"),
                    report,
                )
                .await;
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
                claimed.restore(report);
                self.record_transient_or_escalate(&source_path, reason, report)
                    .await;
                return;
            }
            Ok(Err(IntakeError::Permanent(reason))) => {
                report.push_permanent(&source_path, reason);
                claimed.mark(".error", report);
                self.clear_transient(&source_path).await;
                return;
            }
            Err(error) => {
                claimed.restore(report);
                self.record_transient_or_escalate(
                    &source_path,
                    format!("intake task failed: {error}"),
                    report,
                )
                .await;
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
            match submit_nzb_bytes_with_options(
                &self.db,
                &self.handle,
                &self.config,
                &nzb.bytes,
                Some(nzb.filename.clone()),
                None,
                candidate.category.clone(),
                metadata,
                SubmissionOptions {
                    origin: SubmissionOrigin::WatchFolder,
                    ..SubmissionOptions::default()
                },
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

        match source_disposition(queued, permanent, transient) {
            SourceDisposition::RestoreForRetry => {
                claimed.restore(report);
                self.record_transient_or_escalate(
                    &source_path,
                    "all NZB submissions failed transiently",
                    report,
                )
                .await;
            }
            SourceDisposition::MarkError if queued == 0 && permanent == 0 => {
                report.push_permanent(&source_path, "input did not contain any NZB files");
                claimed.mark(".error", report);
                self.clear_transient(&source_path).await;
            }
            SourceDisposition::MarkError => {
                claimed.mark(".error", report);
                self.clear_transient(&source_path).await;
            }
            SourceDisposition::MarkQueued => {
                claimed.mark(".queued", report);
                self.clear_transient(&source_path).await;
            }
            SourceDisposition::MarkPartial => {
                claimed.mark(".partial", report);
                self.clear_transient(&source_path).await;
            }
        }
    }

    async fn record_transient_or_escalate(
        &self,
        path: &Path,
        reason: impl Into<String>,
        report: &mut WatchFolderScanReport,
    ) {
        let reason = reason.into();
        let fingerprint = fingerprint(path).ok();
        let should_escalate = {
            let mut tracker = self.transient_tracker.lock().await;
            tracker.record(path, fingerprint, SystemTime::now())
        };
        if !should_escalate {
            report.push_transient(path, reason);
            return;
        }

        report.push_permanent(
            path,
            format!(
                "transient input exceeded {} seconds: {reason}",
                TRANSIENT_ESCALATE_AFTER.as_secs()
            ),
        );
        mark_existing_source(path, ".error", report);
        self.clear_transient(path).await;
    }

    async fn clear_transient(&self, path: &Path) {
        self.transient_tracker.lock().await.clear(path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceDisposition {
    RestoreForRetry,
    MarkError,
    MarkQueued,
    MarkPartial,
}

fn source_disposition(queued: usize, permanent: usize, transient: usize) -> SourceDisposition {
    if transient > 0 {
        if queued > 0 {
            SourceDisposition::MarkPartial
        } else {
            SourceDisposition::RestoreForRetry
        }
    } else {
        match (queued, permanent) {
            (0, 0) => SourceDisposition::MarkError,
            (0, _) => SourceDisposition::MarkError,
            (_, 0) => SourceDisposition::MarkQueued,
            (_, _) => SourceDisposition::MarkPartial,
        }
    }
}

#[derive(Default)]
struct TransientTracker {
    entries: HashMap<String, TransientObservation>,
}

struct TransientObservation {
    fingerprint: Option<FileFingerprint>,
    first_seen: SystemTime,
}

impl TransientTracker {
    fn record(
        &mut self,
        path: &Path,
        fingerprint: Option<FileFingerprint>,
        now: SystemTime,
    ) -> bool {
        let key = transient_key(path);
        match self.entries.get_mut(&key) {
            Some(observation) if observation.fingerprint == fingerprint => {
                now.duration_since(observation.first_seen)
                    .unwrap_or_default()
                    >= TRANSIENT_ESCALATE_AFTER
            }
            Some(observation) => {
                observation.fingerprint = fingerprint;
                observation.first_seen = now;
                false
            }
            None => {
                self.entries.insert(
                    key,
                    TransientObservation {
                        fingerprint,
                        first_seen: now,
                    },
                );
                false
            }
        }
    }

    fn clear(&mut self, path: &Path) {
        self.entries.remove(&transient_key(path));
    }
}

fn transient_key(path: &Path) -> String {
    display_path(path)
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
            if should_ignore_file_name(name) {
                continue;
            }
            let Some(category) = category_map.get(&name.to_ascii_lowercase()).cloned() else {
                skipped.push(ScanIssue {
                    path: display_path(&path),
                    reason: "unknown category subfolder".to_string(),
                });
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
                reason: unsupported_input_reason(name).to_string(),
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
    MARKER_SUFFIXES.iter().any(|suffix| lower.ends_with(suffix))
}

fn unsupported_input_reason(name: &str) -> &'static str {
    match weaver_model::files::FileRole::from_filename(name) {
        weaver_model::files::FileRole::RarVolume { .. }
        | weaver_model::files::FileRole::SevenZipSplit { .. }
        | weaver_model::files::FileRole::SplitFile { .. } => "waiting for first archive volume",
        _ => "unsupported watch-folder input",
    }
}

async fn await_stable_candidates(
    candidates: Vec<PendingCandidate>,
    stability_secs: u64,
) -> (Vec<PendingCandidate>, Vec<PendingCandidate>) {
    let mut pending = Vec::new();
    let mut unstable = Vec::new();
    for candidate in candidates {
        match fingerprint_all(&candidate.related_paths) {
            Ok(first)
                if !first.is_empty() && first.iter().all(|fingerprint| fingerprint.len > 0) =>
            {
                pending.push((candidate, first));
            }
            _ => unstable.push(candidate),
        }
    }

    if stability_secs > 0 {
        tokio::time::sleep(Duration::from_secs(stability_secs)).await;
    }

    let mut stable = Vec::new();
    for (candidate, first) in pending {
        if fingerprint_all(&candidate.related_paths).is_ok_and(|second| second == first) {
            stable.push(candidate);
        } else {
            unstable.push(candidate);
        }
    }
    (stable, unstable)
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
    related_original_paths: Vec<PathBuf>,
}

impl ClaimedSource {
    fn claim(source_path: &Path, related_paths: Vec<PathBuf>) -> std::io::Result<Self> {
        let file_name = source_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| std::io::Error::other("source filename is not valid UTF-8"))?;
        let parent = source_path
            .parent()
            .ok_or_else(|| std::io::Error::other("source path has no parent"))?;
        for suffix in 0..1000 {
            let candidate_name = format!("{PROCESSING_PREFIX}{suffix}-{file_name}");
            let candidate = parent.join(candidate_name);
            match runtime_fs::rename_no_overwrite(source_path, &candidate) {
                Ok(()) => {
                    return Ok(Self {
                        original_path: source_path.to_path_buf(),
                        claimed_path: candidate,
                        related_original_paths: related_paths,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not find an unused processing filename",
        ))
    }

    fn restore(self, report: &mut WatchFolderScanReport) {
        if let Err(error) = runtime_fs::rename_no_overwrite(&self.claimed_path, &self.original_path)
        {
            report.push_transient(
                &self.original_path,
                format!("failed to restore claimed source: {error}"),
            );
        }
    }

    fn mark(self, marker: &str, report: &mut WatchFolderScanReport) {
        match append_marker(&self.claimed_path, &self.original_path, marker) {
            Ok(marked_path) => {
                report_marker_rename(report, &self.original_path, &marked_path, marker);
                for path in self
                    .related_original_paths
                    .into_iter()
                    .filter(|path| path != &self.original_path && path.exists())
                {
                    mark_existing_source(&path, marker, report);
                }
            }
            Err(error) => report.push_transient(
                &self.original_path,
                format!("failed to append marker {marker}: {error}"),
            ),
        }
    }
}

fn mark_existing_source(path: &Path, marker: &str, report: &mut WatchFolderScanReport) {
    match append_marker(path, path, marker) {
        Ok(marked_path) => report_marker_rename(report, path, &marked_path, marker),
        Err(error) => {
            report.push_transient(path, format!("failed to append marker {marker}: {error}"))
        }
    }
}

fn report_marker_rename(report: &mut WatchFolderScanReport, from: &Path, to: &Path, marker: &str) {
    report.marker_renamed_sources.push(MarkerRename {
        from: display_path(from),
        to: display_path(to),
        marker: marker.to_string(),
    });
}

fn recover_stale_claims(
    root: &Path,
    categories: &[CategoryConfig],
    report: &mut WatchFolderScanReport,
) -> Result<(), WatchFolderScannerError> {
    for dir in claim_recovery_dirs(root, categories) {
        recover_stale_claims_in_dir(&dir, report)?;
    }
    Ok(())
}

fn claim_recovery_dirs(root: &Path, categories: &[CategoryConfig]) -> Vec<PathBuf> {
    let mut dirs = vec![root.to_path_buf()];
    for category in categories {
        let path = root.join(&category.name);
        if path.is_dir() {
            dirs.push(path);
        }
    }
    dirs
}

fn recover_stale_claims_in_dir(
    dir: &Path,
    report: &mut WatchFolderScanReport,
) -> Result<(), WatchFolderScannerError> {
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                report.push_transient(
                    dir,
                    format!(
                        "failed to read watch folder entry during stale claim recovery: {error}"
                    ),
                );
                continue;
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                report.push_transient(
                    &path,
                    format!(
                        "failed to inspect watch folder entry during stale claim recovery: {error}"
                    ),
                );
                continue;
            }
        };
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(original_name) = original_name_from_claim(name) else {
            continue;
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                report.push_transient(
                    &path,
                    format!("failed to inspect stale processing claim: {error}"),
                );
                continue;
            }
        };
        let modified = metadata.modified().ok();
        if !modified.is_some_and(|modified| {
            SystemTime::now()
                .duration_since(modified)
                .unwrap_or_default()
                >= CLAIM_STALE_AFTER
        }) {
            continue;
        }
        let target = match stale_claim_restore_path(dir, original_name) {
            Ok(target) => target,
            Err(error) => {
                report.push_transient(
                    &path,
                    format!("failed to choose stale claim recovery path: {error}"),
                );
                continue;
            }
        };
        match runtime_fs::rename_no_overwrite(&path, &target) {
            Ok(()) => report_marker_rename(report, &path, &target, "recovered"),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                match recover_stale_claim_with_collision_suffix(&path, dir, original_name) {
                    Ok(marked_path) => {
                        report_marker_rename(report, &path, &marked_path, "recovered")
                    }
                    Err(error) => report.push_transient(
                        &path,
                        format!("failed to recover stale processing claim: {error}"),
                    ),
                }
            }
            Err(error) => report.push_transient(
                &target,
                format!("failed to recover stale processing claim: {error}"),
            ),
        }
    }
    Ok(())
}

fn original_name_from_claim(name: &str) -> Option<&str> {
    let rest = name.strip_prefix(PROCESSING_PREFIX)?;
    let (suffix, original) = rest.split_once('-')?;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) || original.is_empty()
    {
        return None;
    }
    Some(original)
}

fn stale_claim_restore_path(dir: &Path, original_name: &str) -> std::io::Result<PathBuf> {
    let original = dir.join(original_name);
    if !original.exists() {
        return Ok(original);
    }
    Ok(stale_claim_restore_collision_path(dir, original_name, 1))
}

fn recover_stale_claim_with_collision_suffix(
    path: &Path,
    dir: &Path,
    original_name: &str,
) -> std::io::Result<PathBuf> {
    for suffix in 1..1000 {
        let candidate = stale_claim_restore_collision_path(dir, original_name, suffix);
        match runtime_fs::rename_no_overwrite(path, &candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not find an unused stale claim recovery filename",
    ))
}

fn stale_claim_restore_collision_path(dir: &Path, original_name: &str, suffix: usize) -> PathBuf {
    dir.join(format!("recovered-{suffix}-{original_name}"))
}

fn reconcile_related_volume_residue(
    root: &Path,
    categories: &[CategoryConfig],
    report: &mut WatchFolderScanReport,
) -> Result<(), WatchFolderScannerError> {
    for dir in claim_recovery_dirs(root, categories) {
        reconcile_related_volume_residue_in_dir(&dir, report)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ArchiveSetKind {
    Rar,
    SevenZip,
    Split,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ArchiveSetKey {
    kind: ArchiveSetKind,
    base: String,
}

fn reconcile_related_volume_residue_in_dir(
    dir: &Path,
    report: &mut WatchFolderScanReport,
) -> Result<(), WatchFolderScannerError> {
    let mut marked_first_volumes = HashMap::<ArchiveSetKey, &'static str>::new();
    let mut unmarked_later_volumes = Vec::<(PathBuf, ArchiveSetKey)>::new();

    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                report.push_transient(
                    dir,
                    format!("failed to read watch folder entry during volume residue reconciliation: {error}"),
                );
                continue;
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                report.push_transient(
                    &path,
                    format!("failed to inspect watch folder entry during volume residue reconciliation: {error}"),
                );
                continue;
            }
        };
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with(PROCESSING_PREFIX) {
            continue;
        }
        if let Some((unmarked_name, marker)) = strip_terminal_marker(name) {
            if let Some((key, volume_number)) = archive_set_key_from_marked_name(unmarked_name)
                && volume_number == 0
            {
                marked_first_volumes.entry(key).or_insert(marker);
            }
            continue;
        }
        if should_ignore_file_name(name) {
            continue;
        }
        if let Some((key, volume_number)) = archive_set_key(name)
            && volume_number != 0
        {
            unmarked_later_volumes.push((path, key));
        }
    }

    for (path, key) in unmarked_later_volumes {
        if let Some(marker) = marked_first_volumes.get(&key) {
            mark_existing_source(&path, marker, report);
        }
    }

    Ok(())
}

fn strip_terminal_marker(name: &str) -> Option<(&str, &'static str)> {
    let lower = name.to_ascii_lowercase();
    TERMINAL_MARKER_SUFFIXES.iter().find_map(|marker| {
        lower
            .ends_with(marker)
            .then(|| (&name[..name.len() - marker.len()], *marker))
    })
}

fn archive_set_key_from_marked_name(name: &str) -> Option<(ArchiveSetKey, u32)> {
    archive_set_key(name).or_else(|| {
        let (without_suffix, suffix) = name.rsplit_once('.')?;
        suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| archive_set_key(without_suffix))
            .flatten()
    })
}

fn archive_set_key(name: &str) -> Option<(ArchiveSetKey, u32)> {
    let role = FileRole::from_filename(name);
    let (kind, volume_number) = match role {
        FileRole::RarVolume { volume_number } => (ArchiveSetKind::Rar, volume_number),
        FileRole::SevenZipSplit { number } => (ArchiveSetKind::SevenZip, number),
        FileRole::SplitFile { number } => (ArchiveSetKind::Split, number),
        _ => return None,
    };
    let base = archive_base_name(name, &role)?;
    Some((ArchiveSetKey { kind, base }, volume_number))
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
        match runtime_fs::rename_no_overwrite(current_path, &candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
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
    use std::time::UNIX_EPOCH;

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
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].reason, "unknown category subfolder");

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
        assert_eq!(
            fs::read(temp.path().join("release.nzb.gz.queued")).unwrap(),
            b"old"
        );
    }

    #[test]
    fn claim_uses_next_processing_name_without_overwriting_existing_claim() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("release.nzb");
        let existing_claim = temp.path().join(".weaver-processing-0-release.nzb");
        fs::write(&source, b"nzb").unwrap();
        fs::write(&existing_claim, b"existing").unwrap();

        let claimed = ClaimedSource::claim(&source, vec![source.clone()]).unwrap();

        assert_eq!(
            claimed
                .claimed_path
                .file_name()
                .and_then(|value| value.to_str()),
            Some(".weaver-processing-1-release.nzb")
        );
        assert_eq!(fs::read(existing_claim).unwrap(), b"existing");
        assert!(!source.exists());
    }

    #[test]
    fn stale_claim_recovery_restores_original_name() {
        let temp = tempfile::tempdir().unwrap();
        let claimed = temp.path().join(".weaver-processing-0-release.nzb");
        fs::write(&claimed, b"nzb").unwrap();
        make_stale(&claimed);
        let mut report = WatchFolderScanReport::default();

        recover_stale_claims(temp.path(), &[], &mut report).unwrap();

        assert!(temp.path().join("release.nzb").exists());
        assert!(!claimed.exists());
        assert_eq!(report.marker_renamed_sources.len(), 1);
        assert_eq!(report.marker_renamed_sources[0].marker, "recovered");
    }

    #[test]
    fn recent_claim_recovery_leaves_processing_file() {
        let temp = tempfile::tempdir().unwrap();
        let claimed = temp.path().join(".weaver-processing-0-release.nzb");
        fs::write(&claimed, b"nzb").unwrap();
        let mut report = WatchFolderScanReport::default();

        recover_stale_claims(temp.path(), &[], &mut report).unwrap();

        assert!(claimed.exists());
        assert!(!temp.path().join("release.nzb").exists());
        assert!(report.marker_renamed_sources.is_empty());
    }

    #[test]
    fn stale_claim_recovery_uses_collision_safe_recovered_suffix() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("release.nzb"), b"existing").unwrap();
        let claimed = temp.path().join(".weaver-processing-0-release.nzb");
        fs::write(&claimed, b"nzb").unwrap();
        make_stale(&claimed);
        let mut report = WatchFolderScanReport::default();

        recover_stale_claims(temp.path(), &[], &mut report).unwrap();

        assert!(temp.path().join("release.nzb").exists());
        let recovered = temp.path().join("recovered-1-release.nzb");
        assert!(recovered.exists());
        assert!(intake::is_supported_candidate(&recovered));
        assert!(!claimed.exists());
    }

    #[test]
    fn source_disposition_marks_partial_after_queued_transient() {
        assert_eq!(source_disposition(1, 0, 1), SourceDisposition::MarkPartial);
        assert_eq!(
            source_disposition(0, 0, 1),
            SourceDisposition::RestoreForRetry
        );
        assert_eq!(source_disposition(1, 0, 0), SourceDisposition::MarkQueued);
        assert_eq!(source_disposition(0, 1, 0), SourceDisposition::MarkError);
    }

    #[test]
    fn marker_rename_marks_related_archive_parts() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("release.part1.rar");
        let second = temp.path().join("release.part2.rar");
        fs::write(&first, b"first").unwrap();
        fs::write(&second, b"second").unwrap();
        let claimed = ClaimedSource::claim(&first, vec![first.clone(), second.clone()]).unwrap();
        let mut report = WatchFolderScanReport::default();

        claimed.mark(".queued", &mut report);

        assert!(temp.path().join("release.part1.rar.queued").exists());
        assert!(temp.path().join("release.part2.rar.queued").exists());
        assert_eq!(report.marker_renamed_sources.len(), 2);
    }

    #[test]
    fn related_volume_residue_reconciliation_marks_leftover_part_from_marked_first_volume() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("release.part1.rar.queued"), b"first").unwrap();
        fs::write(temp.path().join("release.part2.rar"), b"second").unwrap();
        let mut report = WatchFolderScanReport::default();

        reconcile_related_volume_residue(temp.path(), &[], &mut report).unwrap();

        assert!(temp.path().join("release.part2.rar.queued").exists());
        assert_eq!(report.marker_renamed_sources.len(), 1);
        assert_eq!(report.marker_renamed_sources[0].marker, ".queued");
    }

    #[test]
    fn related_volume_residue_reconciliation_handles_marked_first_volume_collision_suffix() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("release.part1.rar.1.partial"), b"first").unwrap();
        fs::write(temp.path().join("release.part2.rar"), b"second").unwrap();
        fs::write(temp.path().join("release.part2.rar.partial"), b"existing").unwrap();
        let mut report = WatchFolderScanReport::default();

        reconcile_related_volume_residue(temp.path(), &[], &mut report).unwrap();

        assert!(temp.path().join("release.part2.rar.1.partial").exists());
        assert_eq!(
            fs::read(temp.path().join("release.part2.rar.partial")).unwrap(),
            b"existing"
        );
        assert_eq!(report.marker_renamed_sources.len(), 1);
        assert_eq!(report.marker_renamed_sources[0].marker, ".partial");
    }

    #[test]
    fn transient_tracker_escalates_after_age_and_resets_on_fingerprint_change() {
        let path = Path::new("/watch/release.nzb");
        let first = FileFingerprint {
            len: 1,
            modified: Some(UNIX_EPOCH),
        };
        let changed = FileFingerprint {
            len: 2,
            modified: Some(UNIX_EPOCH),
        };
        let mut tracker = TransientTracker::default();

        assert!(!tracker.record(path, Some(first), UNIX_EPOCH));
        assert!(!tracker.record(
            path,
            Some(first),
            UNIX_EPOCH + TRANSIENT_ESCALATE_AFTER - Duration::from_secs(1)
        ));
        assert!(tracker.record(path, Some(first), UNIX_EPOCH + TRANSIENT_ESCALATE_AFTER));
        assert!(!tracker.record(
            path,
            Some(changed),
            UNIX_EPOCH + TRANSIENT_ESCALATE_AFTER + Duration::from_secs(1)
        ));
    }

    fn make_stale(path: &Path) {
        let stale_time = SystemTime::now() - CLAIM_STALE_AFTER - Duration::from_secs(1);
        filetime::set_file_mtime(path, filetime::FileTime::from_system_time(stale_time)).unwrap();
    }
}
