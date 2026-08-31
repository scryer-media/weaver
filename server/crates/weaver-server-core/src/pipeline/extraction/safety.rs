use std::ffi::OsStr;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use cap_fs_ext::DirExt;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};

use crate::operations::disk::disk_space;
use crate::operations::metrics::PipelineMetrics;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;
const MIN_MEMORY_LIMIT: u64 = 64 * MIB;
const MAX_MEMORY_LIMIT: u64 = 64 * GIB;
const DISK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub(crate) struct ExtractionLimits {
    pub(crate) max_job_bytes: u64,
    pub(crate) max_member_bytes: u64,
    pub(crate) max_entries: u64,
    pub(crate) max_ratio: u64,
    pub(crate) max_seconds: u64,
    pub(crate) min_free_bytes: u64,
    pub(crate) max_memory_bytes: u64,
}

impl ExtractionLimits {
    pub(crate) fn from_env(complete_dir: &Path) -> Result<Self, String> {
        let total_filesystem_bytes = disk_space(complete_dir).map(|space| space.total_bytes);
        let default_min_free = total_filesystem_bytes
            .map(|total| (total / 20).clamp(512 * MIB, 20 * GIB))
            .unwrap_or(512 * MIB);
        let detected_memory = crate::runtime::system_probe::detect_total_memory_bytes()
            .unwrap_or(2 * MAX_MEMORY_LIMIT);
        let default_memory = (detected_memory / 2).clamp(MIN_MEMORY_LIMIT, MAX_MEMORY_LIMIT);

        Ok(Self {
            max_job_bytes: parse_positive_u64("WEAVER_EXTRACTION_MAX_JOB_BYTES", 2 * TIB)?,
            max_member_bytes: parse_positive_u64("WEAVER_EXTRACTION_MAX_MEMBER_BYTES", TIB)?,
            max_entries: parse_positive_u64("WEAVER_EXTRACTION_MAX_ENTRIES", 100_000)?,
            max_ratio: parse_positive_u64("WEAVER_EXTRACTION_MAX_RATIO", 100)?,
            max_seconds: parse_positive_u64("WEAVER_EXTRACTION_MAX_SECONDS", 43_200)?,
            min_free_bytes: parse_positive_u64(
                "WEAVER_EXTRACTION_MIN_FREE_BYTES",
                default_min_free,
            )?,
            max_memory_bytes: parse_positive_u64(
                "WEAVER_EXTRACTION_MAX_MEMORY_BYTES",
                default_memory,
            )?,
        })
    }
}

fn parse_positive_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(raw) => {
            let value = raw
                .parse::<u64>()
                .map_err(|error| format!("invalid {name} value '{raw}': {error}"))?;
            if value == 0 {
                return Err(format!(
                    "invalid {name} value '{raw}': must be greater than zero"
                ));
            }
            Ok(value)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("failed to read {name}: {error}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtractionRejectionReason {
    UnsafePath,
    UnsupportedEntry,
    MemberBytes,
    JobBytes,
    Ratio,
    Entries,
    Deadline,
    Memory,
    DiskReserve,
    ContentPolicy,
}

impl ExtractionRejectionReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::UnsafePath => "unsafe_path",
            Self::UnsupportedEntry => "unsupported_entry",
            Self::MemberBytes => "member_bytes",
            Self::JobBytes => "job_bytes",
            Self::Ratio => "ratio",
            Self::Entries => "entries",
            Self::Deadline => "deadline",
            Self::Memory => "memory",
            Self::DiskReserve => "disk_reserve",
            Self::ContentPolicy => "content_policy",
        }
    }
}

#[derive(Debug, Clone)]
struct ExtractionFailure {
    reason: ExtractionRejectionReason,
    detail: String,
}

impl std::fmt::Display for ExtractionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "WEAVER_EXTRACTION_REJECTED[{}]: {}",
            self.reason.as_str(),
            self.detail
        )
    }
}

#[derive(Debug)]
struct DiskBudgetState {
    estimated_available: u64,
    last_refresh: Instant,
}

#[derive(Debug, Default)]
struct ActiveState {
    tasks: u64,
    writers: u64,
}

/// Decoder-window bytes reserved by every extraction job in this pipeline.
///
/// Decoder dictionaries are allocated inside third-party codecs, outside the
/// ordinary output budgets. Keep one charge for the whole process so several
/// jobs cannot each consume the configured memory allowance concurrently.
#[derive(Debug)]
pub(crate) struct ProcessMemoryBudget {
    limit: u64,
    reserved: AtomicU64,
    idle: Mutex<()>,
    released: Condvar,
}

impl ProcessMemoryBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: AtomicU64::new(0),
            idle: Mutex::new(()),
            released: Condvar::new(),
        }
    }

    fn reserve_wait<F>(
        self: &Arc<Self>,
        bytes: u64,
        mut check_active: F,
    ) -> Result<ProcessMemoryPermit, String>
    where
        F: FnMut() -> Result<(), String>,
    {
        if bytes > self.limit {
            return Err(format!(
                "decoder requires {bytes} bytes, process limit is {}",
                self.limit
            ));
        }

        let mut wait_guard = self.idle.lock().expect("process memory state poisoned");
        loop {
            check_active()?;
            if reserve_atomic(&self.reserved, bytes, self.limit).is_ok() {
                return Ok(ProcessMemoryPermit {
                    budget: Arc::clone(self),
                    bytes,
                });
            }
            let (guard, _) = self
                .released
                .wait_timeout(wait_guard, Duration::from_millis(250))
                .expect("process memory state poisoned");
            wait_guard = guard;
        }
    }

    #[cfg(test)]
    fn reserved_bytes(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct JobExtractionBudget {
    limits: Arc<ExtractionLimits>,
    process_memory: Arc<ProcessMemoryBudget>,
    root_path: PathBuf,
    ratio_limit_bytes: u64,
    effective_job_limit_bytes: u64,
    started_at: Instant,
    total_written: AtomicU64,
    entry_count: AtomicU64,
    memory_reserved: AtomicU64,
    cancelled: AtomicBool,
    failure: Mutex<Option<ExtractionFailure>>,
    disk: Mutex<DiskBudgetState>,
    active: Mutex<ActiveState>,
    idle: Condvar,
    metrics: Arc<PipelineMetrics>,
}

impl JobExtractionBudget {
    pub(crate) fn new(
        limits: Arc<ExtractionLimits>,
        root_path: PathBuf,
        declared_archive_bytes: u64,
        initial_entries: u64,
        initial_bytes: u64,
        metrics: Arc<PipelineMetrics>,
    ) -> Result<Arc<Self>, String> {
        let process_memory = Arc::new(ProcessMemoryBudget::new(limits.max_memory_bytes));
        Self::new_with_process_memory(
            limits,
            process_memory,
            root_path,
            declared_archive_bytes,
            initial_entries,
            initial_bytes,
            metrics,
        )
    }

    pub(crate) fn new_with_process_memory(
        limits: Arc<ExtractionLimits>,
        process_memory: Arc<ProcessMemoryBudget>,
        root_path: PathBuf,
        declared_archive_bytes: u64,
        initial_entries: u64,
        initial_bytes: u64,
        metrics: Arc<PipelineMetrics>,
    ) -> Result<Arc<Self>, String> {
        let ratio_limit_bytes = declared_archive_bytes
            .saturating_mul(limits.max_ratio)
            .max(GIB);
        let effective_job_limit_bytes = limits.max_job_bytes.min(ratio_limit_bytes);
        let initial_disk_space = disk_space(&root_path);
        let estimated_available = initial_disk_space
            .map(|space| space.available_bytes)
            .unwrap_or(0);
        let budget = Arc::new(Self {
            limits,
            process_memory,
            root_path,
            ratio_limit_bytes,
            effective_job_limit_bytes,
            started_at: Instant::now(),
            total_written: AtomicU64::new(initial_bytes),
            entry_count: AtomicU64::new(initial_entries),
            memory_reserved: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
            failure: Mutex::new(None),
            disk: Mutex::new(DiskBudgetState {
                estimated_available,
                last_refresh: Instant::now(),
            }),
            active: Mutex::new(ActiveState::default()),
            idle: Condvar::new(),
            metrics,
        });

        if initial_entries > budget.limits.max_entries {
            return Err(budget
                .reject(
                    ExtractionRejectionReason::Entries,
                    format!(
                        "staging tree contains {initial_entries} entries, limit is {}",
                        budget.limits.max_entries
                    ),
                )
                .to_string());
        }
        if initial_bytes > budget.effective_job_limit_bytes {
            return Err(budget
                .reject_job_bytes(initial_bytes, "existing staging output")
                .to_string());
        }
        if initial_disk_space.is_none() {
            return Err(budget
                .reject(
                    ExtractionRejectionReason::DiskReserve,
                    format!(
                        "failed to determine available space for extraction root '{}'",
                        budget.root_path.display()
                    ),
                )
                .to_string());
        }
        Ok(budget)
    }

    pub(crate) fn is_rejection(error: &str) -> bool {
        error.contains("WEAVER_EXTRACTION_REJECTED[")
    }

    #[cfg(test)]
    pub(crate) fn task_permit(self: &Arc<Self>) -> Result<TaskPermit, String> {
        self.task_permit_inner(None)
    }

    pub(crate) fn task_permit_for_root(
        self: &Arc<Self>,
        root: Arc<ExtractionRoot>,
    ) -> Result<TaskPermit, String> {
        self.task_permit_inner(Some(root))
    }

    fn task_permit_inner(
        self: &Arc<Self>,
        root: Option<Arc<ExtractionRoot>>,
    ) -> Result<TaskPermit, String> {
        self.check_active().map_err(|error| error.to_string())?;
        let mut active = self
            .active
            .lock()
            .expect("extraction active state poisoned");
        if self.cancelled.load(Ordering::Acquire) {
            return Err(self.current_failure().to_string());
        }
        active.tasks = active.tasks.saturating_add(1);
        Ok(TaskPermit {
            budget: Arc::clone(self),
            root,
        })
    }

    pub(crate) fn reserve_memory_wait(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<MemoryPermit, String> {
        if bytes > self.limits.max_memory_bytes {
            return Err(self
                .reject(
                    ExtractionRejectionReason::Memory,
                    format!(
                        "decoder requires {bytes} bytes, limit is {}",
                        self.limits.max_memory_bytes
                    ),
                )
                .to_string());
        }

        let mut wait_guard = self
            .active
            .lock()
            .expect("extraction active state poisoned");
        loop {
            self.check_active().map_err(|error| error.to_string())?;
            if reserve_atomic(&self.memory_reserved, bytes, self.limits.max_memory_bytes).is_ok() {
                let process_memory = self
                    .process_memory
                    .reserve_wait(bytes, || {
                        self.check_active().map_err(|error| error.to_string())
                    })
                    .map_err(|error| {
                        self.memory_reserved.fetch_sub(bytes, Ordering::AcqRel);
                        self.idle.notify_all();
                        self.reject(ExtractionRejectionReason::Memory, error)
                            .to_string()
                    })?;
                return Ok(MemoryPermit {
                    budget: Arc::clone(self),
                    _process_memory: process_memory,
                    bytes,
                });
            }
            let (guard, _) = self
                .idle
                .wait_timeout(wait_guard, Duration::from_millis(250))
                .expect("extraction active state poisoned");
            wait_guard = guard;
        }
    }

    pub(crate) fn max_memory_bytes(&self) -> u64 {
        self.limits.max_memory_bytes
    }

    pub(crate) fn check_member_metadata(&self, member: &str, bytes: u64) -> Result<(), String> {
        self.check_active().map_err(|error| error.to_string())?;
        if bytes > self.limits.max_member_bytes {
            return Err(self
                .reject(
                    ExtractionRejectionReason::MemberBytes,
                    format!(
                        "archive member '{member}' declares {bytes} bytes, limit is {}",
                        self.limits.max_member_bytes
                    ),
                )
                .to_string());
        }
        Ok(())
    }

    pub(crate) fn note_entry(&self, name: &Path) -> Result<(), String> {
        self.check_active().map_err(|error| error.to_string())?;
        reserve_atomic(&self.entry_count, 1, self.limits.max_entries).map_err(|requested| {
            self.reject(
                ExtractionRejectionReason::Entries,
                format!(
                    "creating '{}' would reach {requested} entries, limit is {}",
                    name.display(),
                    self.limits.max_entries
                ),
            )
            .to_string()
        })
    }

    pub(crate) fn reject_unsafe_path(&self, detail: impl Into<String>) -> String {
        self.reject(ExtractionRejectionReason::UnsafePath, detail.into())
            .to_string()
    }

    pub(crate) fn reject_unsupported_entry(&self, detail: impl Into<String>) -> String {
        self.reject(ExtractionRejectionReason::UnsupportedEntry, detail.into())
            .to_string()
    }

    /// Reject material that policy must not permit to reach publication. This
    /// shares the extraction budget's cancellation signal so sibling workers
    /// stop at their next checkpoint.
    pub(crate) fn reject_content_policy(&self, detail: impl Into<String>) -> String {
        self.reject(ExtractionRejectionReason::ContentPolicy, detail.into())
            .to_string()
    }

    pub(crate) fn cancel_with_error(&self, error: &str) {
        if !Self::is_rejection(error) {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
    }

    /// Stop active decoders at their next budgeted read, write, or admission
    /// checkpoint. The task permits notify cleanup once every worker has left.
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.idle.notify_all();
    }

    pub(crate) fn wait_for_idle(&self) {
        let mut active = self
            .active
            .lock()
            .expect("extraction active state poisoned");
        while active.tasks != 0 || active.writers != 0 {
            active = self
                .idle
                .wait(active)
                .expect("extraction active state poisoned while waiting");
        }
    }

    fn current_failure(&self) -> ExtractionFailure {
        self.failure
            .lock()
            .expect("extraction failure state poisoned")
            .clone()
            .unwrap_or_else(|| ExtractionFailure {
                reason: ExtractionRejectionReason::JobBytes,
                detail: "job extraction was cancelled".to_string(),
            })
    }

    fn check_active(&self) -> Result<(), ExtractionFailure> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(self.current_failure());
        }
        if self.started_at.elapsed() > Duration::from_secs(self.limits.max_seconds) {
            return Err(self.reject(
                ExtractionRejectionReason::Deadline,
                format!(
                    "extraction exceeded the {} second deadline",
                    self.limits.max_seconds
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn check_active_io(&self) -> io::Result<()> {
        self.check_active()
            .map_err(|error| io::Error::other(error.to_string()))
    }

    fn reserve_write(&self, member_written: u64, bytes: usize) -> Result<(), ExtractionFailure> {
        self.check_active()?;
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let requested_member = member_written.saturating_add(bytes);
        if requested_member > self.limits.max_member_bytes {
            return Err(self.reject(
                ExtractionRejectionReason::MemberBytes,
                format!(
                    "member output would reach {requested_member} bytes, limit is {}",
                    self.limits.max_member_bytes
                ),
            ));
        }

        reserve_atomic(&self.total_written, bytes, self.effective_job_limit_bytes)
            .map_err(|requested| self.reject_job_bytes(requested, "archive output"))?;

        let disk_result = self.reserve_disk(bytes);
        if let Err(error) = disk_result {
            self.total_written.fetch_sub(bytes, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }

    fn reserve_disk(&self, bytes: u64) -> Result<(), ExtractionFailure> {
        let mut disk = self.disk.lock().expect("extraction disk state poisoned");
        if disk.last_refresh.elapsed() >= DISK_REFRESH_INTERVAL {
            let space = disk_space(&self.root_path).ok_or_else(|| {
                self.reject(
                    ExtractionRejectionReason::DiskReserve,
                    format!(
                        "failed to refresh available space for extraction root '{}'",
                        self.root_path.display()
                    ),
                )
            })?;
            disk.estimated_available = space.available_bytes;
            disk.last_refresh = Instant::now();
        }
        let remaining = disk.estimated_available.saturating_sub(bytes);
        if remaining < self.limits.min_free_bytes {
            return Err(self.reject(
                ExtractionRejectionReason::DiskReserve,
                format!(
                    "writing {bytes} bytes would leave {remaining} bytes free, reserve is {}",
                    self.limits.min_free_bytes
                ),
            ));
        }
        disk.estimated_available = remaining;
        Ok(())
    }

    fn rollback_write_reservation(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.total_written.fetch_sub(bytes, Ordering::AcqRel);
        let mut disk = self.disk.lock().expect("extraction disk state poisoned");
        disk.estimated_available = disk.estimated_available.saturating_add(bytes);
    }

    fn reject_job_bytes(&self, requested: u64, context: &str) -> ExtractionFailure {
        let reason = if self.ratio_limit_bytes <= self.limits.max_job_bytes {
            ExtractionRejectionReason::Ratio
        } else {
            ExtractionRejectionReason::JobBytes
        };
        self.reject(
            reason,
            format!(
                "{context} would reach {requested} bytes, effective job limit is {}",
                self.effective_job_limit_bytes
            ),
        )
    }

    /// The job's metrics handle. Extraction resolves the budget once per
    /// member, so this is how the per-member timer reaches the histograms
    /// without threading a second handle through `RarExtractionContext`.
    pub(crate) fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    fn reject(&self, reason: ExtractionRejectionReason, detail: String) -> ExtractionFailure {
        let failure = ExtractionFailure { reason, detail };
        let first = !self.cancelled.swap(true, Ordering::AcqRel);
        let mut stored = self
            .failure
            .lock()
            .expect("extraction failure state poisoned");
        if first {
            *stored = Some(failure.clone());
            self.metrics.note_extraction_rejection(reason.as_str());
            tracing::warn!(
                reason = reason.as_str(),
                detail = %failure.detail,
                root = %self.root_path.display(),
                "extraction rejected by security budget"
            );
        }
        stored.clone().unwrap_or(failure)
    }

    fn writer_started(self: &Arc<Self>) {
        let mut active = self
            .active
            .lock()
            .expect("extraction active state poisoned");
        active.writers = active.writers.saturating_add(1);
    }

    fn writer_finished(&self) {
        let mut active = self
            .active
            .lock()
            .expect("extraction active state poisoned");
        active.writers = active.writers.saturating_sub(1);
        if active.tasks == 0 && active.writers == 0 {
            self.idle.notify_all();
        }
    }

    #[cfg(test)]
    pub(crate) fn total_written(&self) -> u64 {
        self.total_written.load(Ordering::Relaxed)
    }
}

fn reserve_atomic(counter: &AtomicU64, amount: u64, limit: u64) -> Result<(), u64> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            let requested = current.saturating_add(amount);
            (requested <= limit).then_some(requested)
        })
        .map(|_| ())
        .map_err(|current| current.saturating_add(amount))
}

#[derive(Debug)]
pub(crate) struct TaskPermit {
    budget: Arc<JobExtractionBudget>,
    root: Option<Arc<ExtractionRoot>>,
}

impl TaskPermit {
    pub(crate) fn root(&self) -> Arc<ExtractionRoot> {
        Arc::clone(
            self.root
                .as_ref()
                .expect("root-backed extraction task permit missing root"),
        )
    }
}

impl Drop for TaskPermit {
    fn drop(&mut self) {
        self.root.take();
        let mut active = self
            .budget
            .active
            .lock()
            .expect("extraction active state poisoned");
        active.tasks = active.tasks.saturating_sub(1);
        if active.tasks == 0 && active.writers == 0 {
            self.budget.idle.notify_all();
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryPermit {
    budget: Arc<JobExtractionBudget>,
    _process_memory: ProcessMemoryPermit,
    bytes: u64,
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        self.budget
            .memory_reserved
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.idle.notify_all();
    }
}

#[derive(Debug)]
struct ProcessMemoryPermit {
    budget: Arc<ProcessMemoryBudget>,
    bytes: u64,
}

impl Drop for ProcessMemoryPermit {
    fn drop(&mut self) {
        self.budget.reserved.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.released.notify_all();
    }
}

pub(crate) struct BudgetedReader<R> {
    inner: R,
    budget: Arc<JobExtractionBudget>,
}

impl<R> BudgetedReader<R> {
    pub(crate) fn new(inner: R, budget: Arc<JobExtractionBudget>) -> Self {
        Self { inner, budget }
    }
}

impl<R: Read> Read for BudgetedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.budget.check_active_io()?;
        let read = self.inner.read(buffer)?;
        self.budget.check_active_io()?;
        Ok(read)
    }
}

impl<R: Seek> Seek for BudgetedReader<R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.budget.check_active_io()?;
        let position = self.inner.seek(position)?;
        self.budget.check_active_io()?;
        Ok(position)
    }
}

pub(crate) struct BudgetedWriter<W> {
    inner: W,
    budget: Arc<JobExtractionBudget>,
    member_written: u64,
}

impl<W> BudgetedWriter<W> {
    fn new(inner: W, budget: Arc<JobExtractionBudget>) -> Self {
        budget.writer_started();
        Self {
            inner,
            budget,
            member_written: 0,
        }
    }
}

impl<W: Write> Write for BudgetedWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.budget
            .reserve_write(self.member_written, buffer.len())
            .map_err(|error| io::Error::other(error.to_string()))?;
        match self.inner.write(buffer) {
            Ok(written) => {
                let unwritten = buffer.len().saturating_sub(written) as u64;
                self.budget.rollback_write_reservation(unwritten);
                self.member_written = self.member_written.saturating_add(written as u64);
                Ok(written)
            }
            Err(error) => {
                self.budget.rollback_write_reservation(buffer.len() as u64);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl BudgetedWriter<cap_std::fs::File> {
    pub(crate) fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }
}

impl<W> Drop for BudgetedWriter<W> {
    fn drop(&mut self) {
        self.budget.writer_finished();
    }
}

#[derive(Debug)]
pub(crate) struct ExtractionRoot {
    path: PathBuf,
    dir: Dir,
}

impl ExtractionRoot {
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        let parent_path = path
            .parent()
            .ok_or_else(|| format!("extraction staging root has no parent: {}", path.display()))?;
        let anchor_path = parent_path.parent().ok_or_else(|| {
            format!(
                "extraction staging parent has no anchor: {}",
                parent_path.display()
            )
        })?;
        let parent_name = parent_path.file_name().ok_or_else(|| {
            format!(
                "extraction staging parent has no directory name: {}",
                parent_path.display()
            )
        })?;
        let root_name = path.file_name().ok_or_else(|| {
            format!(
                "extraction staging root has no directory name: {}",
                path.display()
            )
        })?;

        let anchor = Dir::open_ambient_dir(anchor_path, ambient_authority()).map_err(|error| {
            format!(
                "failed to open extraction staging anchor {}: {error}",
                anchor_path.display()
            )
        })?;
        let parent = open_or_create_directory_nofollow(
            &anchor,
            Path::new(parent_name),
            "extraction staging parent",
        )?;
        let dir = open_or_create_directory_nofollow(
            &parent,
            Path::new(root_name),
            "extraction staging root",
        )?;
        Ok(Self {
            path: path.to_path_buf(),
            dir,
        })
    }

    pub(crate) fn validate_relative_path(&self, raw_name: &str) -> Result<PathBuf, String> {
        if raw_name.contains('\0') {
            return Err(format!("unsafe archive entry path: {raw_name}"));
        }
        let normalized = raw_name.replace('\\', "/");
        let normalized = normalized.trim_end_matches('/');
        if normalized.is_empty() {
            return Err(format!("unsafe archive entry path: {raw_name}"));
        }
        let path = Path::new(normalized);
        if path.is_absolute() {
            return Err(format!("unsafe archive entry path: {raw_name}"));
        }
        let mut safe = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(part) if !is_windows_drive_component(part) => safe.push(part),
                Component::CurDir => {}
                Component::Normal(_)
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => {
                    return Err(format!("unsafe archive entry path: {raw_name}"));
                }
            }
        }
        if safe.as_os_str().is_empty() {
            return Err(format!("unsafe archive entry path: {raw_name}"));
        }
        Ok(safe)
    }

    pub(crate) fn create_dir(
        &self,
        relative: &Path,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<(), String> {
        budget.note_entry(relative)?;
        self.ensure_parents(relative, budget)?;
        match self.dir.create_dir(relative) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = self.dir.symlink_metadata(relative).map_err(|error| {
                    budget.reject_unsafe_path(format!(
                        "failed to inspect existing directory '{}': {error}",
                        relative.display()
                    ))
                })?;
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    Ok(())
                } else {
                    Err(budget.reject_unsafe_path(format!(
                        "archive directory '{}' collides with a non-directory entry",
                        relative.display()
                    )))
                }
            }
            Err(error) => Err(format!(
                "failed to create archive directory '{}': {error}",
                relative.display()
            )),
        }
    }

    pub(crate) fn create_file(
        &self,
        relative: &Path,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<BudgetedWriter<cap_std::fs::File>, String> {
        budget.note_entry(relative)?;
        self.ensure_parents(relative, budget)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let file = match self.dir.open_with(relative, &options) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = self.dir.symlink_metadata(relative).map_err(|error| {
                    budget.reject_unsafe_path(format!(
                        "failed to inspect existing output '{}': {error}",
                        relative.display()
                    ))
                })?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(budget.reject_unsafe_path(format!(
                        "archive file '{}' collides with a non-regular entry",
                        relative.display()
                    )));
                }
                self.dir.remove_file(relative).map_err(|error| {
                    format!(
                        "failed to replace archive output '{}': {error}",
                        relative.display()
                    )
                })?;
                self.dir.open_with(relative, &options).map_err(|error| {
                    format!(
                        "failed to create archive output '{}': {error}",
                        relative.display()
                    )
                })?
            }
            Err(error) => {
                return Err(format!(
                    "failed to create archive output '{}': {error}",
                    relative.display()
                ));
            }
        };
        Ok(BudgetedWriter::new(file, Arc::clone(budget)))
    }

    fn ensure_parents(
        &self,
        relative: &Path,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<(), String> {
        let Some(parent) = relative.parent() else {
            return Ok(());
        };
        let mut current = PathBuf::new();
        for component in parent.components() {
            let Component::Normal(part) = component else {
                return Err(budget.reject_unsafe_path(format!(
                    "unsafe parent path for '{}'",
                    relative.display()
                )));
            };
            current.push(part);
            match self.dir.symlink_metadata(&current) {
                Ok(metadata) => {
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(budget.reject_unsafe_path(format!(
                            "archive parent '{}' is not a real directory",
                            current.display()
                        )));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match self.dir.create_dir(&current) {
                        Ok(()) => budget.note_entry(&current)?,
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                            let metadata =
                                self.dir.symlink_metadata(&current).map_err(|error| {
                                    budget.reject_unsafe_path(format!(
                                        "failed to inspect raced parent '{}': {error}",
                                        current.display()
                                    ))
                                })?;
                            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                                return Err(budget.reject_unsafe_path(format!(
                                    "archive parent '{}' is not a real directory",
                                    current.display()
                                )));
                            }
                        }
                        Err(error) => {
                            return Err(format!(
                                "failed to create archive parent '{}': {error}",
                                current.display()
                            ));
                        }
                    }
                }
                Err(error) => {
                    return Err(format!(
                        "failed to inspect archive parent '{}': {error}",
                        current.display()
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn scan_no_links(
        &self,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<(u64, u64), String> {
        scan_capability_tree(&self.dir, &self.path).map_err(|detail| {
            if detail.contains("symlink") || detail.contains("reparse") {
                budget.reject_unsafe_path(detail)
            } else {
                budget.reject_unsupported_entry(detail)
            }
        })
    }

    pub(crate) fn snapshot_usage(path: &Path) -> Result<(u64, u64), String> {
        let root = Self::open(path)?;
        scan_capability_tree(&root.dir, path)
    }
}

fn open_or_create_directory_nofollow(
    parent: &Dir,
    relative: &Path,
    description: &str,
) -> Result<Dir, String> {
    match parent.open_dir_nofollow(relative) {
        Ok(dir) => Ok(dir),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match parent.create_dir(relative) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(format!(
                        "failed to create {description} '{}': {error}",
                        relative.display()
                    ));
                }
            }
            parent.open_dir_nofollow(relative).map_err(|error| {
                format!(
                    "failed to open {description} '{}' without following links: {error}",
                    relative.display()
                )
            })
        }
        Err(error) => Err(format!(
            "failed to open {description} '{}' without following links: {error}",
            relative.display()
        )),
    }
}

fn is_windows_drive_component(value: &OsStr) -> bool {
    let bytes = value.as_encoded_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn scan_capability_tree(root: &Dir, display_root: &Path) -> Result<(u64, u64), String> {
    let mut entries = 0u64;
    let mut bytes = 0u64;
    scan_capability_directory(root, Path::new(""), display_root, &mut entries, &mut bytes)?;
    Ok((entries, bytes))
}

fn scan_capability_directory(
    directory: &Dir,
    relative: &Path,
    display_root: &Path,
    entries: &mut u64,
    bytes: &mut u64,
) -> Result<(), String> {
    let children = directory.entries().map_err(|error| {
        format!(
            "failed to scan '{}': {error}",
            display_root.join(relative).display()
        )
    })?;
    for child in children {
        let child = child.map_err(|error| {
            format!(
                "failed to read entry beneath '{}': {error}",
                display_root.join(relative).display()
            )
        })?;
        let name = child.file_name();
        let child_relative = relative.join(&name);
        let display_path = display_root.join(&child_relative);
        let metadata = directory
            .symlink_metadata(&name)
            .map_err(|error| format!("failed to inspect '{}': {error}", display_path.display()))?;
        *entries = entries.saturating_add(1);
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "staging tree contains symlink '{}'",
                display_path.display()
            ));
        }
        #[cfg(target_os = "windows")]
        if is_windows_cap_reparse_point(&metadata) {
            return Err(format!(
                "staging tree contains reparse point '{}'",
                display_path.display()
            ));
        }
        if metadata.is_dir() {
            let child_dir = directory.open_dir_nofollow(&name).map_err(|error| {
                format!(
                    "failed to open staging directory '{}' without following links: {error}",
                    display_path.display()
                )
            })?;
            scan_capability_directory(&child_dir, &child_relative, display_root, entries, bytes)?;
        } else if metadata.is_file() {
            *bytes = bytes.saturating_add(metadata.len());
        } else {
            return Err(format!(
                "staging tree contains non-file/non-directory entry '{}'",
                display_path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn is_windows_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes()
        & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
        != 0
}

#[cfg(target_os = "windows")]
fn is_windows_cap_reparse_point(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt;
    metadata.file_attributes()
        & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
        != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Arc<ExtractionLimits> {
        Arc::new(ExtractionLimits {
            max_job_bytes: 32,
            max_member_bytes: 24,
            max_entries: 8,
            max_ratio: 1,
            max_seconds: 60,
            min_free_bytes: 1,
            max_memory_bytes: 64 * MIB,
        })
    }

    fn root_and_budget() -> (tempfile::TempDir, ExtractionRoot, Arc<JobExtractionBudget>) {
        let temp = tempfile::tempdir().unwrap();
        let root = ExtractionRoot::open(temp.path()).unwrap();
        let budget = JobExtractionBudget::new(
            limits(),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        (temp, root, budget)
    }

    #[test]
    fn capability_root_rejects_traversal_and_absolute_paths() {
        let (_temp, root, _budget) = root_and_budget();
        for unsafe_name in ["../escape", "/tmp/escape", "C:/escape", "\\\\host\\share"] {
            assert!(
                root.validate_relative_path(unsafe_name).is_err(),
                "{unsafe_name}"
            );
        }
        assert_eq!(
            root.validate_relative_path("safe/nested.txt").unwrap(),
            PathBuf::from("safe/nested.txt")
        );
    }

    #[test]
    fn actual_writes_enforce_member_limit() {
        let (_temp, root, budget) = root_and_budget();
        let mut writer = root.create_file(Path::new("member.bin"), &budget).unwrap();
        writer.write_all(&[0; 16]).unwrap();
        let error = writer.write_all(&[0; 16]).unwrap_err();
        assert!(error.to_string().contains("member_bytes"));
        assert_eq!(budget.total_written(), 16);
    }

    #[test]
    fn user_cancellation_stops_budgeted_io() {
        let (_temp, root, budget) = root_and_budget();
        let mut writer = root.create_file(Path::new("member.bin"), &budget).unwrap();
        budget.cancel();
        let error = writer.write_all(&[0; 1]).unwrap_err();
        assert!(error.to_string().contains("job extraction was cancelled"));
    }

    #[test]
    fn aggregate_job_and_entry_limits_are_shared() {
        let (_temp, root, budget) = root_and_budget();
        let mut first = root.create_file(Path::new("first.bin"), &budget).unwrap();
        first.write_all(&[0; 16]).unwrap();
        drop(first);
        let mut second = root.create_file(Path::new("second.bin"), &budget).unwrap();
        second.write_all(&[0; 16]).unwrap();
        let error = second.write_all(&[0; 1]).unwrap_err();
        assert!(error.to_string().contains("job_bytes"));

        let temp = tempfile::tempdir().unwrap();
        let entry_limits = Arc::new(ExtractionLimits {
            max_entries: 1,
            ..(*limits()).clone()
        });
        let entry_budget = JobExtractionBudget::new(
            entry_limits,
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        entry_budget.note_entry(Path::new("one")).unwrap();
        assert!(
            entry_budget
                .note_entry(Path::new("two"))
                .unwrap_err()
                .contains("entries")
        );
    }

    #[test]
    fn ratio_memory_deadline_and_disk_reserve_rejections_are_typed() {
        let temp = tempfile::tempdir().unwrap();
        let ratio_limits = Arc::new(ExtractionLimits {
            max_job_bytes: 2 * GIB,
            max_member_bytes: 2 * GIB,
            max_ratio: 1,
            ..(*limits()).clone()
        });
        let ratio_budget = JobExtractionBudget::new(
            ratio_limits,
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        let ratio_error = ratio_budget
            .reserve_write(0, (GIB + 1) as usize)
            .unwrap_err()
            .to_string();
        assert!(ratio_error.contains("ratio"));

        let (_temp, _root, memory_budget) = root_and_budget();
        assert!(
            memory_budget
                .reserve_memory_wait(65 * MIB)
                .unwrap_err()
                .contains("memory")
        );

        let temp = tempfile::tempdir().unwrap();
        let deadline_budget = JobExtractionBudget::new(
            Arc::new(ExtractionLimits {
                max_seconds: 0,
                ..(*limits()).clone()
            }),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(1));
        assert!(
            deadline_budget
                .task_permit()
                .unwrap_err()
                .contains("deadline")
        );

        let temp = tempfile::tempdir().unwrap();
        let root = ExtractionRoot::open(temp.path()).unwrap();
        let disk_budget = JobExtractionBudget::new(
            Arc::new(ExtractionLimits {
                min_free_bytes: u64::MAX,
                ..(*limits()).clone()
            }),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        let mut writer = root
            .create_file(Path::new("disk.bin"), &disk_budget)
            .unwrap();
        assert!(
            writer
                .write_all(&[1])
                .unwrap_err()
                .to_string()
                .contains("disk_reserve")
        );
    }

    #[test]
    fn large_decoder_reservations_are_accounted_without_allocating_them() {
        let temp = tempfile::tempdir().unwrap();
        let budget = JobExtractionBudget::new(
            Arc::new(ExtractionLimits {
                max_memory_bytes: 8 * GIB,
                ..(*limits()).clone()
            }),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();

        let first = budget.reserve_memory_wait(6 * GIB).unwrap();
        let waiting_budget = Arc::clone(&budget);
        let waiter = std::thread::spawn(move || {
            let _second = waiting_budget.reserve_memory_wait(4 * GIB).unwrap();
        });
        std::thread::sleep(Duration::from_millis(10));
        assert!(!waiter.is_finished());
        drop(first);
        waiter.join().unwrap();
    }

    #[test]
    fn decoder_memory_is_shared_across_job_budgets() {
        let temp = tempfile::tempdir().unwrap();
        let limits = Arc::new(ExtractionLimits {
            max_memory_bytes: 8 * GIB,
            ..(*limits()).clone()
        });
        let process_memory = Arc::new(ProcessMemoryBudget::new(8 * GIB));
        let first_budget = JobExtractionBudget::new_with_process_memory(
            Arc::clone(&limits),
            Arc::clone(&process_memory),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        let second_budget = JobExtractionBudget::new_with_process_memory(
            limits,
            Arc::clone(&process_memory),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();

        let first = first_budget.reserve_memory_wait(6 * GIB).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _second = second_budget.reserve_memory_wait(4 * GIB).unwrap();
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(process_memory.reserved_bytes(), 6 * GIB);
        assert!(!waiter.is_finished());

        drop(first);
        waiter.join().unwrap();
        assert_eq!(process_memory.reserved_bytes(), 0);
    }

    #[test]
    fn cancelled_job_leaves_shared_memory_wait_before_holder_releases() {
        let temp = tempfile::tempdir().unwrap();
        let limits = Arc::new(ExtractionLimits {
            max_memory_bytes: 8 * GIB,
            ..(*limits()).clone()
        });
        let process_memory = Arc::new(ProcessMemoryBudget::new(8 * GIB));
        let holder_budget = JobExtractionBudget::new_with_process_memory(
            Arc::clone(&limits),
            Arc::clone(&process_memory),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        let waiting_budget = JobExtractionBudget::new_with_process_memory(
            limits,
            Arc::clone(&process_memory),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();

        let holder = holder_budget.reserve_memory_wait(6 * GIB).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiting_budget_for_thread = Arc::clone(&waiting_budget);
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = waiting_budget_for_thread
                .reserve_memory_wait(4 * GIB)
                .map(|_| ());
            let _ = done_tx.send(result);
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(process_memory.reserved_bytes(), 6 * GIB);
        assert!(!waiter.is_finished());

        waiting_budget.cancel();

        let error = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("cancelled job should leave the shared-memory wait")
            .unwrap_err();
        assert!(error.contains("job extraction was cancelled"));
        assert_eq!(process_memory.reserved_bytes(), 6 * GIB);

        waiter.join().unwrap();
        drop(holder);
        assert_eq!(process_memory.reserved_bytes(), 0);
    }

    #[test]
    fn budgeted_reader_enforces_deadline_before_returning_input() {
        let temp = tempfile::tempdir().unwrap();
        let budget = JobExtractionBudget::new(
            Arc::new(ExtractionLimits {
                max_seconds: 0,
                ..(*limits()).clone()
            }),
            temp.path().to_path_buf(),
            1,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(1));

        let mut reader = BudgetedReader::new(io::Cursor::new(vec![1_u8]), budget);
        let mut byte = [0_u8; 1];
        assert!(
            reader
                .read(&mut byte)
                .unwrap_err()
                .to_string()
                .contains("deadline")
        );
    }

    #[cfg(unix)]
    #[test]
    fn unavailable_disk_probe_rejects_budget_creation() {
        use std::os::unix::ffi::OsStringExt;

        let invalid_path = PathBuf::from(std::ffi::OsString::from_vec(b"bad\0path".to_vec()));
        let error =
            JobExtractionBudget::new(limits(), invalid_path, 1, 0, 0, PipelineMetrics::new())
                .unwrap_err();
        assert!(error.contains("disk_reserve"));
    }

    #[cfg(unix)]
    #[test]
    fn staging_scan_rejects_symlinks_without_following_them() {
        use std::os::unix::fs::symlink;

        let (temp, root, budget) = root_and_budget();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), temp.path().join("link")).unwrap();
        let error = root.scan_no_links(&budget).unwrap_err();
        assert!(error.contains("unsafe_path"));
        assert!(outside.path().exists());
    }

    #[cfg(unix)]
    #[test]
    fn capability_root_open_rejects_a_symlinked_job_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let staging_parent = temp.path().join(".weaver-staging");
        std::fs::create_dir(&staging_parent).unwrap();
        let job_root = staging_parent.join("job-1");
        symlink(outside.path(), &job_root).unwrap();

        let error = ExtractionRoot::open(&job_root).unwrap_err();
        assert!(error.contains("without following links"));
        assert!(outside.path().exists());
    }
}
