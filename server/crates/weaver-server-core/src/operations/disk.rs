use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::operations::instrumentation::DiskSpaceSnapshot;

/// Capacity for the filesystem backing a path.
#[derive(Debug, Clone, Copy)]
pub struct DiskSpace {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

impl DiskSpace {
    pub fn used_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.available_bytes)
    }
}

/// Why a capacity probe produced no reading.
#[derive(Debug)]
pub enum DiskProbeError {
    /// The operating system rejected the query (missing path, permission,
    /// unmounted or unreachable filesystem, ...).
    Io(io::Error),
    /// The path cannot be handed to the operating system (interior NUL).
    InvalidPath,
    /// The filesystem answered with fields no reading can be built from.
    InvalidReading,
    /// This platform has no capacity query.
    Unsupported,
}

impl DiskProbeError {
    /// True when the failure means the path does not exist (yet), which is the
    /// only failure a probe may recover from by asking an ancestor instead.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Io(error) if error.kind() == io::ErrorKind::NotFound)
    }
}

impl fmt::Display for DiskProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::InvalidPath => f.write_str("path contains an interior NUL byte"),
            Self::InvalidReading => f.write_str("filesystem reported a zero fragment size"),
            Self::Unsupported => f.write_str("capacity queries are unsupported on this platform"),
        }
    }
}

impl std::error::Error for DiskProbeError {}

/// Query total/available capacity for the filesystem backing `path`
/// (`statfs` on Apple platforms, `statvfs` on other unix,
/// `GetDiskFreeSpaceExW` on Windows).
///
/// Apple's `statvfs` keeps block counts in 32 bits, so a volume with more
/// than 2^32 blocks — a 22 TB share counted in 1 KiB blocks — reads as a
/// small disk with more free space than total. Its `statfs` counts in 64 bits.
///
/// Fails when the path cannot be stat'd (e.g. it does not exist yet), when the
/// filesystem returns an unusable reading, or on unsupported platforms. The
/// error carries the operating-system reason so callers can log it and decide
/// between failing open, holding a stale reading, and refusing.
pub fn probe_disk_space(path: &Path) -> Result<DiskSpace, DiskProbeError> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::unix::ffi::OsStrExt;

        let path_cstr = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| DiskProbeError::InvalidPath)?;
        // SAFETY: `statfs` fills a zeroed `libc::statfs` for a valid C string path;
        // we check the return code before reading any fields.
        unsafe {
            let mut stat: libc::statfs = std::mem::zeroed();
            if libc::statfs(path_cstr.as_ptr(), &mut stat) != 0 {
                return Err(DiskProbeError::Io(io::Error::last_os_error()));
            }
            // `f_bsize` is the unit `f_blocks` and `f_bavail` are counted in.
            let unit = u64::from(stat.f_bsize);
            reading_from_blocks(stat.f_blocks, stat.f_bavail, unit, unit)
        }
    }

    #[cfg(all(unix, not(target_vendor = "apple")))]
    {
        use std::os::unix::ffi::OsStrExt;

        let path_cstr = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| DiskProbeError::InvalidPath)?;
        // SAFETY: `statvfs` fills a zeroed `libc::statvfs` for a valid C string path;
        // we check the return code before reading any fields.
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(path_cstr.as_ptr(), &mut stat) != 0 {
                return Err(DiskProbeError::Io(io::Error::last_os_error()));
            }
            #[allow(clippy::unnecessary_cast)]
            let frsize = stat.f_frsize as u64;
            #[allow(clippy::unnecessary_cast)]
            let bsize = stat.f_bsize as u64;
            #[allow(clippy::unnecessary_cast)]
            let blocks = stat.f_blocks as u64;
            #[allow(clippy::unnecessary_cast)]
            let bavail = stat.f_bavail as u64;
            reading_from_blocks(blocks, bavail, frsize, bsize)
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        // Relative paths are resolved against the current directory before the
        // query so a very long or UNC path can be handed to the API verbatim.
        let absolute = std::path::absolute(path).map_err(DiskProbeError::Io)?;
        let mut wide: Vec<u16> = absolute.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return Err(DiskProbeError::InvalidPath);
        }
        wide = windows_probe_path(&wide);
        wide.push(0);
        let mut free_bytes_available = 0u64;
        let mut total_bytes = 0u64;
        let mut total_free_bytes = 0u64;
        // SAFETY: `wide` is a NUL-terminated UTF-16 path and the out-pointers
        // are valid u64 slots for the duration of the call.
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut free_bytes_available,
                &mut total_bytes,
                &mut total_free_bytes,
            )
        };
        if ok == 0 {
            return Err(DiskProbeError::Io(io::Error::last_os_error()));
        }
        Ok(DiskSpace {
            total_bytes,
            available_bytes: free_bytes_available,
        })
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(DiskProbeError::Unsupported)
    }
}

/// Build a reading from `statvfs` block counts.
///
/// `f_frsize` is the unit the block counts are expressed in; a few
/// filesystems and emulation layers leave it zero and only fill `f_bsize`, so
/// that is used as the fallback unit. Both zero means no reading can be built.
#[cfg_attr(not(unix), allow(dead_code))]
fn reading_from_blocks(
    blocks: u64,
    available_blocks: u64,
    fragment_size: u64,
    block_size: u64,
) -> Result<DiskSpace, DiskProbeError> {
    let unit = if fragment_size != 0 {
        fragment_size
    } else if block_size != 0 {
        block_size
    } else {
        return Err(DiskProbeError::InvalidReading);
    };
    Ok(DiskSpace {
        total_bytes: blocks.saturating_mul(unit),
        available_bytes: available_blocks.saturating_mul(unit),
    })
}

/// Shape an absolute UTF-16 path for `GetDiskFreeSpaceExW`.
///
/// The API wants directories to end in a separator (a UNC root without one is
/// rejected) and only accepts paths beyond the classic length limit through
/// the verbatim `\\?\` prefix. UNC paths take `\\?\UNC\server\share` rather
/// than a bare prefix. Already-verbatim and device paths pass through
/// untouched; short drive paths stay short so the usual normalization rules
/// keep applying to them.
#[cfg_attr(not(windows), allow(dead_code))]
fn windows_probe_path(absolute: &[u16]) -> Vec<u16> {
    const MAX_CLASSIC_PATH: usize = 260;
    const BACKSLASH: u16 = b'\\' as u16;
    let starts_with = |prefix: &str| {
        let prefix: Vec<u16> = prefix.encode_utf16().collect();
        absolute.starts_with(&prefix)
    };

    let mut shaped: Vec<u16> = if starts_with("\\\\?\\") || starts_with("\\\\.\\") {
        absolute.to_vec()
    } else if starts_with("\\\\") {
        if absolute.len() + "\\\\?\\UNC\\".len() >= MAX_CLASSIC_PATH {
            let mut shaped: Vec<u16> = "\\\\?\\UNC\\".encode_utf16().collect();
            shaped.extend_from_slice(&absolute[2..]);
            shaped
        } else {
            absolute.to_vec()
        }
    } else if absolute.len() + "\\\\?\\".len() >= MAX_CLASSIC_PATH {
        let mut shaped: Vec<u16> = "\\\\?\\".encode_utf16().collect();
        shaped.extend_from_slice(absolute);
        shaped
    } else {
        absolute.to_vec()
    };
    if shaped.last() != Some(&BACKSLASH) {
        shaped.push(BACKSLASH);
    }
    shaped
}

/// Probe `path`, falling back to its nearest existing ancestor when the path
/// itself has not been created yet. Only use this for advisory startup
/// estimates: a missing path may also mean a disconnected storage root, and
/// its parent's capacity does not establish that the target is available.
/// Permission and I/O errors are returned unchanged.
pub fn probe_nearest_disk_space(path: &Path) -> Result<DiskSpace, DiskProbeError> {
    let mut candidate = path;
    loop {
        match probe_disk_space(candidate) {
            Err(error) if error.is_not_found() => match candidate.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => candidate = parent,
                _ => return Err(error),
            },
            result => return result,
        }
    }
}

/// Query total/available capacity, discarding the failure reason.
///
/// Callers that only want a best-effort number (metrics, informational
/// warnings) use this; anything that gates work on the answer should go
/// through [`probe_disk_space`] or a [`CapacitySampler`] so the failure is at
/// least logged.
pub fn disk_space(path: &Path) -> Option<DiskSpace> {
    probe_disk_space(path).ok()
}

/// True when an I/O error means the filesystem (or the caller's quota on it)
/// ran out of room, as opposed to a corrupt path, permission problem, or
/// hardware fault.
pub fn is_out_of_space(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    ) {
        return true;
    }
    let Some(code) = error.raw_os_error() else {
        return false;
    };
    #[cfg(unix)]
    {
        code == libc::ENOSPC || code == libc::EDQUOT
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{
            ERROR_DISK_FULL, ERROR_DISK_QUOTA_EXCEEDED, ERROR_HANDLE_DISK_FULL,
        };
        let code = code as u32;
        code == ERROR_DISK_FULL
            || code == ERROR_HANDLE_DISK_FULL
            || code == ERROR_DISK_QUOTA_EXCEEDED
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = code;
        false
    }
}

/// One capacity reading as seen by a [`CapacitySampler`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityReading {
    pub available_bytes: u64,
    pub total_bytes: u64,
    /// When the underlying probe succeeded. Debits since then are already
    /// applied to `available_bytes`.
    pub sampled_at: Instant,
    /// The most recent probe failed and this is the last good reading, kept
    /// so callers can keep accounting against something rather than nothing.
    pub stale: bool,
}

/// What a [`CapacitySampler`] currently knows about its filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capacity {
    Known(CapacityReading),
    /// No probe has ever succeeded for this path.
    Unknown,
}

impl Capacity {
    pub fn reading(self) -> Option<CapacityReading> {
        match self {
            Self::Known(reading) => Some(reading),
            Self::Unknown => None,
        }
    }

    /// Available bytes from a fresh reading only.
    pub fn fresh_available_bytes(self) -> Option<u64> {
        self.reading()
            .filter(|reading| !reading.stale)
            .map(|reading| reading.available_bytes)
    }

    /// Available bytes from the best reading held, fresh or stale.
    pub fn best_available_bytes(self) -> Option<u64> {
        self.reading().map(|reading| reading.available_bytes)
    }
}

type ProbeFn = dyn Fn(&Path) -> Result<DiskSpace, DiskProbeError> + Send + Sync;

/// Per-path capacity sampler shared by every admission check that gates work
/// on free space.
///
/// It bounds the probe rate with a TTL, keeps the last good reading (flagged
/// stale) across probe failures so a transient stat error does not turn into
/// a phantom "disk full", tracks bytes admitted against the cached reading
/// between probes, and logs each failure and recovery transition exactly once
/// with the operating-system reason. Callers keep it under their own lock;
/// nothing here blocks beyond the syscall.
pub struct CapacitySampler {
    path: PathBuf,
    ttl: Duration,
    probe: Box<ProbeFn>,
    last_good: Option<CapacityReading>,
    last_attempt: Option<Instant>,
    failing_since: Option<Instant>,
}

impl fmt::Debug for CapacitySampler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapacitySampler")
            .field("path", &self.path)
            .field("ttl", &self.ttl)
            .field("last_good", &self.last_good)
            .field("last_attempt", &self.last_attempt)
            .field("failing_since", &self.failing_since)
            .finish_non_exhaustive()
    }
}

impl CapacitySampler {
    /// Sample the exact storage path at most once per `ttl`. A missing root
    /// stays unavailable rather than borrowing its parent's capacity.
    /// No probe runs until the first `sample`.
    pub fn new(path: PathBuf, ttl: Duration) -> Self {
        Self::with_probe(path, ttl, Box::new(probe_disk_space))
    }

    /// Like [`Self::new`] with a caller-supplied probe (tests, injected
    /// accounting).
    pub fn with_probe(path: PathBuf, ttl: Duration, probe: Box<ProbeFn>) -> Self {
        Self {
            path,
            ttl,
            probe,
            last_good: None,
            last_attempt: None,
            failing_since: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Current knowledge without touching the filesystem.
    pub fn current(&self) -> Capacity {
        match self.last_good {
            Some(reading) => Capacity::Known(CapacityReading {
                stale: self.failing_since.is_some(),
                ..reading
            }),
            None => Capacity::Unknown,
        }
    }

    /// True when the most recent probe failed.
    pub fn is_failing(&self) -> bool {
        self.failing_since.is_some()
    }

    /// Return the current reading, probing first when the last attempt is
    /// older than the TTL.
    pub fn sample(&mut self) -> Capacity {
        self.sample_at(Instant::now())
    }

    /// Probe now regardless of the TTL.
    pub fn refresh(&mut self) -> Capacity {
        self.refresh_at(Instant::now())
    }

    pub fn sample_at(&mut self, now: Instant) -> Capacity {
        let within_ttl = self
            .last_attempt
            .is_some_and(|attempted| now.saturating_duration_since(attempted) < self.ttl);
        if within_ttl {
            return self.current();
        }
        self.refresh_at(now)
    }

    pub fn refresh_at(&mut self, now: Instant) -> Capacity {
        let started = Instant::now();
        let result = (self.probe)(&self.path);
        // A slow syscall must not consume its own cache lifetime. Preserve
        // the caller's clock origin while stamping completion, not initiation.
        let now = now + started.elapsed();
        self.last_attempt = Some(now);
        match result {
            Ok(space) => {
                if let Some(since) = self.failing_since.take() {
                    info!(
                        path = %self.path.display(),
                        unavailable_for_ms = now.saturating_duration_since(since).as_millis() as u64,
                        available_bytes = space.available_bytes,
                        "filesystem capacity readings recovered"
                    );
                }
                self.last_good = Some(CapacityReading {
                    available_bytes: space.available_bytes,
                    total_bytes: space.total_bytes,
                    sampled_at: now,
                    stale: false,
                });
            }
            Err(error) => {
                if self.failing_since.is_none() {
                    self.failing_since = Some(now);
                    warn!(
                        path = %self.path.display(),
                        error = %error,
                        last_good_available_bytes = self.last_good.map(|r| r.available_bytes),
                        "filesystem capacity reading unavailable; holding the last good reading"
                    );
                }
            }
        }
        self.current()
    }

    /// Account bytes admitted against the cached reading so a burst of
    /// admissions between probes cannot each see the same headroom.
    pub fn debit(&mut self, bytes: u64) {
        if let Some(reading) = self.last_good.as_mut() {
            reading.available_bytes = reading.available_bytes.saturating_sub(bytes);
        }
    }

    /// Undo a `debit` whose admission was rolled back.
    pub fn credit(&mut self, bytes: u64) {
        if let Some(reading) = self.last_good.as_mut() {
            reading.available_bytes = reading.available_bytes.saturating_add(bytes);
        }
    }
}

/// How often each storage root's sampler re-reads its filesystem.
///
/// A capacity probe is a filesystem round trip that a slow or overloaded
/// mount can hold for as long as its request queue is deep, so it never runs
/// where a caller is waiting on it. Every consumer — admission checks,
/// metrics, the NZBGet status — reads the last completed reading instead,
/// which is at most this old plus however long the probe itself took.
pub const STORAGE_CAPACITY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// The configured storage roots whose free space the runtime tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageRoot {
    Data,
    Intermediate,
    Complete,
}

impl StorageRoot {
    pub const ALL: [Self; 3] = [Self::Data, Self::Intermediate, Self::Complete];

    /// Stable label for metrics and logs.
    pub fn label(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Intermediate => "intermediate",
            Self::Complete => "complete",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Intermediate => 1,
            Self::Complete => 2,
        }
    }
}

/// A non-blocking view of one filesystem's latest capacity reading.
///
/// Reading it never touches the filesystem unless it was built with
/// [`Self::probing`]. Consumers that spend against the reading between
/// refreshes keep their own [`CapacityDebits`].
#[derive(Clone)]
pub struct CapacityReader(Arc<dyn Fn() -> Capacity + Send + Sync>);

impl fmt::Debug for CapacityReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CapacityReader")
            .field(&self.current())
            .finish()
    }
}

impl CapacityReader {
    /// A reader backed by `read`. Tests inject readings through it.
    pub fn from_fn(read: impl Fn() -> Capacity + Send + Sync + 'static) -> Self {
        Self(Arc::new(read))
    }

    /// A reader with no reading, which every consumer treats as a failed
    /// probe: nothing to enforce against.
    pub fn unknown() -> Self {
        Self::from_fn(|| Capacity::Unknown)
    }

    /// A reader that probes `path` itself, at most once per `ttl`, on the
    /// calling thread. Only for callers already off the pipeline with no
    /// runtime-owned sampler to read (a standalone extraction).
    pub fn probing(path: PathBuf, ttl: Duration) -> Self {
        let sampler = Mutex::new(CapacitySampler::new(path, ttl));
        Self::from_fn(move || {
            sampler
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .sample()
        })
    }

    pub fn current(&self) -> Capacity {
        (self.0)()
    }
}

/// Bytes one consumer has admitted against a shared reading since that
/// reading was taken, so a burst of admissions between refreshes cannot each
/// see the same headroom.
///
/// The debits belong to the reading they were made against: a newer reading
/// already reflects the bytes written since, so it starts the count again. A
/// stale reading keeps its timestamp, so debits keep accumulating across a
/// probe outage.
#[derive(Debug, Default, Clone, Copy)]
pub struct CapacityDebits {
    basis: Option<Instant>,
    debited: u64,
}

impl CapacityDebits {
    /// `capacity` less everything debited against the same reading.
    pub fn apply(&mut self, capacity: Capacity) -> Capacity {
        let Capacity::Known(reading) = capacity else {
            return Capacity::Unknown;
        };
        if self.basis != Some(reading.sampled_at) {
            self.basis = Some(reading.sampled_at);
            self.debited = 0;
        }
        Capacity::Known(CapacityReading {
            available_bytes: reading.available_bytes.saturating_sub(self.debited),
            ..reading
        })
    }

    pub fn debit(&mut self, bytes: u64) {
        self.debited = self.debited.saturating_add(bytes);
    }

    /// Undo a `debit` whose admission was rolled back.
    pub fn credit(&mut self, bytes: u64) {
        self.debited = self.debited.saturating_sub(bytes);
    }
}

type SharedProbeFn = Arc<ProbeFn>;

#[derive(Debug)]
struct RootState {
    path: PathBuf,
    capacity: Capacity,
    refresh_requested: bool,
    stopped: bool,
    completed_probes: u64,
}

#[derive(Debug)]
struct RootSlot {
    root: StorageRoot,
    state: Mutex<RootState>,
    wake: Condvar,
}

impl RootSlot {
    fn lock(&self) -> std::sync::MutexGuard<'_, RootState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One background capacity sampler per configured storage root.
///
/// Each root has its own thread that probes, publishes, and then waits a full
/// interval after the probe finished, so probes of one root never overlap and
/// a stalled mount delays only its own next reading. A failed probe keeps the
/// last good reading, flagged stale ([`CapacitySampler`]). Until a root's
/// first probe completes its readers see [`Capacity::Unknown`].
///
/// Dropping it stops the threads; one stuck in a probe exits when the probe
/// returns.
pub struct StorageCapacity {
    slots: [Arc<RootSlot>; 3],
}

impl fmt::Debug for StorageCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for slot in &self.slots {
            let state = slot.lock();
            list.entry(&(slot.root.label(), &state.path, state.capacity));
        }
        list.finish()
    }
}

impl StorageCapacity {
    /// Start sampling the real filesystems behind the three roots.
    pub fn start(data: PathBuf, intermediate: PathBuf, complete: PathBuf) -> Self {
        Self::with_probe(
            [data, intermediate, complete],
            STORAGE_CAPACITY_REFRESH_INTERVAL,
            Arc::new(probe_disk_space),
        )
    }

    /// Like [`Self::start`] with a caller-supplied probe and interval. `roots`
    /// is in [`StorageRoot::ALL`] order.
    pub fn with_probe(roots: [PathBuf; 3], interval: Duration, probe: SharedProbeFn) -> Self {
        let slots = StorageRoot::ALL.map(|root| {
            Arc::new(RootSlot {
                root,
                state: Mutex::new(RootState {
                    path: roots[root.index()].clone(),
                    capacity: Capacity::Unknown,
                    refresh_requested: false,
                    stopped: false,
                    completed_probes: 0,
                }),
                wake: Condvar::new(),
            })
        });
        for slot in &slots {
            let thread_slot = Arc::clone(slot);
            let thread_probe = Arc::clone(&probe);
            let spawned = std::thread::Builder::new()
                .name(format!("weaver-capacity-{}", slot.root.label()))
                .spawn(move || run_root_sampler(&thread_slot, interval, thread_probe));
            if let Err(error) = spawned {
                warn!(
                    root = slot.root.label(),
                    error = %error,
                    "could not start the storage capacity sampler; readings stay unknown"
                );
            }
        }
        Self { slots }
    }

    fn slot(&self, root: StorageRoot) -> &Arc<RootSlot> {
        &self.slots[root.index()]
    }

    /// The latest reading for `root`, without touching the filesystem.
    pub fn current(&self, root: StorageRoot) -> Capacity {
        self.slot(root).lock().capacity
    }

    /// A cloneable reader of `root`'s latest reading.
    pub fn reader(&self, root: StorageRoot) -> CapacityReader {
        let slot = Arc::clone(self.slot(root));
        CapacityReader::from_fn(move || slot.lock().capacity)
    }

    /// Point `root` at a new directory. The old directory's reading is
    /// dropped and the new one is probed as soon as the thread is free.
    pub fn retarget(&self, root: StorageRoot, path: PathBuf) {
        let slot = self.slot(root);
        let mut state = slot.lock();
        if state.path == path {
            return;
        }
        state.path = path;
        state.capacity = Capacity::Unknown;
        state.refresh_requested = true;
        slot.wake.notify_all();
    }

    /// Ask `root`'s thread to probe again without waiting out its interval.
    /// A request made while a probe is running is served after it finishes.
    pub fn request_refresh(&self, root: StorageRoot) {
        let slot = self.slot(root);
        slot.lock().refresh_requested = true;
        slot.wake.notify_all();
    }

    /// One row per root with a reading, fresh or held from the last good
    /// probe. A root that has never produced a reading is omitted rather than
    /// reported as zero capacity.
    pub fn snapshots(&self) -> Vec<DiskSpaceSnapshot> {
        self.slots
            .iter()
            .filter_map(|slot| {
                let state = slot.lock();
                state.capacity.reading().map(|reading| DiskSpaceSnapshot {
                    role: slot.root.label(),
                    path: state.path.display().to_string(),
                    total_bytes: reading.total_bytes,
                    available_bytes: reading.available_bytes,
                })
            })
            .collect()
    }

    /// Block until `root` has completed at least `probes` probes.
    #[cfg(test)]
    pub(crate) fn wait_for_probes(&self, root: StorageRoot, probes: u64) {
        let slot = self.slot(root);
        let state = slot.lock();
        drop(
            slot.wake
                .wait_while(state, |state| state.completed_probes < probes)
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }
}

impl Drop for StorageCapacity {
    fn drop(&mut self) {
        for slot in &self.slots {
            slot.lock().stopped = true;
            slot.wake.notify_all();
        }
    }
}

fn run_root_sampler(slot: &RootSlot, interval: Duration, probe: SharedProbeFn) {
    let mut sampler: Option<CapacitySampler> = None;
    loop {
        let path = {
            let mut state = slot.lock();
            if state.stopped {
                return;
            }
            state.refresh_requested = false;
            state.path.clone()
        };
        if sampler
            .as_ref()
            .is_none_or(|sampler| sampler.path() != path)
        {
            let probe = Arc::clone(&probe);
            sampler = Some(CapacitySampler::with_probe(
                path.clone(),
                Duration::ZERO,
                Box::new(move |path| probe(path)),
            ));
        }
        let capacity = sampler
            .as_mut()
            .expect("a sampler for the current path")
            .refresh();
        let mut state = slot.lock();
        // A retarget while the probe ran makes this reading the old
        // directory's; the thread probes the new one straight away.
        if state.path == path {
            state.capacity = capacity;
        }
        state.completed_probes += 1;
        slot.wake.notify_all();
        let state = slot
            .wake
            .wait_timeout_while(state, interval, |state| {
                !state.stopped && !state.refresh_requested
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0;
        if state.stopped {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    fn narrow(w: &[u16]) -> String {
        String::from_utf16(w).unwrap()
    }

    #[test]
    fn statvfs_reading_prefers_fragment_size_and_falls_back_to_block_size() {
        let reading = reading_from_blocks(100, 40, 4096, 512).unwrap();
        assert_eq!(reading.total_bytes, 409_600);
        assert_eq!(reading.available_bytes, 163_840);

        let reading = reading_from_blocks(100, 40, 0, 512).unwrap();
        assert_eq!(reading.total_bytes, 51_200);
        assert_eq!(reading.available_bytes, 20_480);

        assert!(matches!(
            reading_from_blocks(100, 40, 0, 0),
            Err(DiskProbeError::InvalidReading)
        ));

        let reading = reading_from_blocks(u64::MAX, u64::MAX, 4096, 4096).unwrap();
        assert_eq!(reading.total_bytes, u64::MAX, "block math saturates");
    }

    #[test]
    fn windows_probe_path_keeps_short_paths_and_adds_a_trailing_separator() {
        assert_eq!(
            narrow(&windows_probe_path(&wide(r"C:\weaver\tmp"))),
            r"C:\weaver\tmp\"
        );
        assert_eq!(narrow(&windows_probe_path(&wide(r"C:\"))), r"C:\");
        assert_eq!(
            narrow(&windows_probe_path(&wide(r"\\server\share"))),
            r"\\server\share\"
        );
        assert_eq!(
            narrow(&windows_probe_path(&wide(r"\\?\C:\already\verbatim"))),
            r"\\?\C:\already\verbatim\"
        );
        assert_eq!(
            narrow(&windows_probe_path(&wide(r"\\.\PhysicalDrive0\"))),
            r"\\.\PhysicalDrive0\"
        );
    }

    #[test]
    fn windows_probe_path_uses_verbatim_prefixes_only_for_long_paths() {
        let long_tail = "a".repeat(300);
        let drive = format!(r"C:\{long_tail}");
        let shaped = narrow(&windows_probe_path(&wide(&drive)));
        assert!(shaped.starts_with(r"\\?\C:\"), "{shaped}");
        assert!(shaped.ends_with('\\'));

        let unc = format!(r"\\server\share\{long_tail}");
        let shaped = narrow(&windows_probe_path(&wide(&unc)));
        assert!(shaped.starts_with(r"\\?\UNC\server\share\"), "{shaped}");
        assert!(
            !shaped.contains(r"\\?\\\"),
            "the UNC prefix replaces the leading slashes"
        );
    }

    #[test]
    fn out_of_space_recognizes_platform_codes_and_error_kinds() {
        assert!(is_out_of_space(&io::Error::from(
            io::ErrorKind::StorageFull
        )));
        assert!(is_out_of_space(&io::Error::from(
            io::ErrorKind::QuotaExceeded
        )));
        assert!(!is_out_of_space(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(!is_out_of_space(&io::Error::other("boom")));
        #[cfg(unix)]
        {
            assert!(is_out_of_space(&io::Error::from_raw_os_error(libc::ENOSPC)));
            assert!(is_out_of_space(&io::Error::from_raw_os_error(libc::EDQUOT)));
            assert!(!is_out_of_space(&io::Error::from_raw_os_error(libc::EIO)));
        }
        #[cfg(windows)]
        {
            assert!(is_out_of_space(&io::Error::from_raw_os_error(39)));
            assert!(is_out_of_space(&io::Error::from_raw_os_error(112)));
            assert!(is_out_of_space(&io::Error::from_raw_os_error(1295)));
            assert!(!is_out_of_space(&io::Error::from_raw_os_error(5)));
        }
    }

    #[test]
    fn probe_reports_a_reason_and_the_nearest_ancestor_fallback_only_covers_missing_paths() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("not").join("created").join("yet");
        let error = probe_disk_space(&missing).unwrap_err();
        assert!(error.is_not_found(), "{error}");
        assert!(!error.to_string().is_empty());

        let reading = probe_nearest_disk_space(&missing).expect("ancestor reading");
        assert!(reading.total_bytes > 0);
        let direct = probe_disk_space(dir.path()).expect("direct reading");
        assert_eq!(reading.total_bytes, direct.total_bytes);

        assert!(
            disk_space(&missing).is_none(),
            "the lossy wrapper does not walk up"
        );
    }

    fn scripted_sampler(
        ttl: Duration,
        results: Arc<Mutex<Vec<Result<u64, io::ErrorKind>>>>,
        calls: Arc<AtomicUsize>,
    ) -> CapacitySampler {
        CapacitySampler::with_probe(
            PathBuf::from("/scripted"),
            ttl,
            Box::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                let mut results = results.lock().unwrap();
                match results.remove(0) {
                    Ok(available) => Ok(DiskSpace {
                        total_bytes: 1 << 40,
                        available_bytes: available,
                    }),
                    Err(kind) => Err(DiskProbeError::Io(io::Error::from(kind))),
                }
            }),
        )
    }

    #[test]
    fn sampler_holds_the_last_good_reading_as_stale_across_probe_failures() {
        let calls = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(vec![
            Ok(1000),
            Err(io::ErrorKind::PermissionDenied),
            Ok(700),
        ]));
        let mut sampler = scripted_sampler(Duration::ZERO, results, calls.clone());

        assert_eq!(sampler.current(), Capacity::Unknown);
        let first = sampler.sample().reading().expect("fresh reading");
        assert_eq!(first.available_bytes, 1000);
        assert!(!first.stale);

        sampler.debit(100);
        let held = sampler.sample().reading().expect("held reading");
        assert!(held.stale, "a failed probe keeps the last good reading");
        assert_eq!(
            held.available_bytes, 900,
            "debits survive into the stale reading"
        );
        assert!(sampler.is_failing());
        assert_eq!(sampler.sample().fresh_available_bytes(), Some(700));
        assert!(!sampler.is_failing());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn sampler_waits_a_full_ttl_after_a_slow_probe_finishes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = calls.clone();
        let ttl = Duration::from_millis(10);
        let probe_delay = ttl * 2;
        let mut sampler = CapacitySampler::with_probe(
            PathBuf::from("/scripted"),
            ttl,
            Box::new(move |_| {
                probe_calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(probe_delay);
                Ok(DiskSpace {
                    total_bytes: 2000,
                    available_bytes: 1000,
                })
            }),
        );
        let started = Instant::now();
        sampler.sample_at(started);
        // This timestamp is at or before completion, independent of how long
        // the test thread was descheduled during the probe.
        sampler.sample_at(started + probe_delay);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "slow probes must not expire their own TTL"
        );
    }

    #[test]
    fn sampler_keeps_a_disappeared_root_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("storage");
        std::fs::create_dir(&root).unwrap();
        let mut sampler = CapacitySampler::new(root.clone(), Duration::ZERO);
        let first = sampler.sample().reading().unwrap();
        std::fs::remove_dir(&root).unwrap();
        let missing = sampler.sample().reading().unwrap();
        assert!(
            missing.stale,
            "parent capacity must not replace a disappeared storage root"
        );
        assert_eq!(missing.sampled_at, first.sampled_at);
        std::fs::create_dir(&root).unwrap();
        assert!(!sampler.sample().reading().unwrap().stale);
    }

    #[test]
    fn sampler_respects_the_ttl_and_accounts_debits_and_credits_between_probes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(vec![Ok(1000), Ok(5000)]));
        let mut sampler = scripted_sampler(Duration::from_secs(60), results, calls.clone());
        let start = Instant::now();

        assert_eq!(sampler.sample_at(start).best_available_bytes(), Some(1000));
        sampler.debit(300);
        sampler.credit(50);
        assert_eq!(
            sampler
                .sample_at(start + Duration::from_secs(30))
                .best_available_bytes(),
            Some(750)
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "within the TTL nothing is probed"
        );
        assert_eq!(
            sampler
                .sample_at(start + Duration::from_secs(61))
                .fresh_available_bytes(),
            Some(5000)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn sampler_stays_unknown_until_a_probe_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(vec![
            Err(io::ErrorKind::NotFound),
            Err(io::ErrorKind::NotFound),
            Ok(42),
        ]));
        let mut sampler = scripted_sampler(Duration::ZERO, results, calls);
        assert_eq!(sampler.sample(), Capacity::Unknown);
        sampler.debit(10);
        assert_eq!(sampler.sample(), Capacity::Unknown);
        assert_eq!(sampler.sample().fresh_available_bytes(), Some(42));
    }

    #[test]
    fn sampler_probes_the_real_filesystem_by_default() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut sampler = CapacitySampler::new(dir.path().join("pending"), Duration::from_secs(1));
        assert_eq!(sampler.sample(), Capacity::Unknown);
        std::fs::create_dir(dir.path().join("pending")).unwrap();
        let reading = sampler
            .refresh()
            .reading()
            .expect("reading of the created root");
        assert!(reading.total_bytes > 0);
        assert!(!reading.stale);
    }

    /// A year: long enough that no test ever sees an interval refresh, so
    /// every probe a test counts is one it asked for.
    const NEVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);

    fn counting_probe(calls: Arc<Mutex<Vec<PathBuf>>>) -> SharedProbeFn {
        Arc::new(move |path: &Path| {
            calls.lock().unwrap().push(path.to_path_buf());
            if path.ends_with("unmounted") {
                return Err(DiskProbeError::Io(io::Error::from(io::ErrorKind::NotFound)));
            }
            Ok(DiskSpace {
                total_bytes: 1000,
                available_bytes: path.as_os_str().len() as u64,
            })
        })
    }

    fn roots() -> [PathBuf; 3] {
        [
            PathBuf::from("/data"),
            PathBuf::from("/intermediate"),
            PathBuf::from("/unmounted"),
        ]
    }

    #[test]
    fn storage_readers_and_snapshots_serve_the_cached_reading_without_probing() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(roots(), NEVER, counting_probe(calls.clone()));
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        assert_eq!(calls.lock().unwrap().len(), 3, "one probe per root");

        let reader = storage.reader(StorageRoot::Intermediate);
        for _ in 0..5 {
            let snapshots = storage.snapshots();
            assert_eq!(
                snapshots
                    .iter()
                    .map(|snapshot| (snapshot.role, snapshot.available_bytes))
                    .collect::<Vec<_>>(),
                vec![("data", 5), ("intermediate", 13)],
                "a root that never read is omitted, not reported as empty"
            );
            assert_eq!(reader.current().fresh_available_bytes(), Some(13));
            assert_eq!(storage.current(StorageRoot::Complete), Capacity::Unknown);
        }
        assert_eq!(
            calls.lock().unwrap().len(),
            3,
            "reading the cache never probes"
        );
    }

    #[test]
    fn a_storage_root_never_runs_two_probes_at_once() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let most_in_flight = Arc::new(AtomicUsize::new(0));
        let probes = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let entered_tx = Mutex::new(entered_tx);
        let release_rx = Mutex::new(release_rx);
        let probe: SharedProbeFn = {
            let in_flight = in_flight.clone();
            let most_in_flight = most_in_flight.clone();
            let probes = probes.clone();
            Arc::new(move |path: &Path| {
                if path != Path::new("/data") {
                    return Ok(DiskSpace {
                        total_bytes: 1,
                        available_bytes: 1,
                    });
                }
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                most_in_flight.fetch_max(now, Ordering::SeqCst);
                if probes.fetch_add(1, Ordering::SeqCst) == 0 {
                    // The first probe stalls, as a slow mount would.
                    entered_tx.lock().unwrap().send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(DiskSpace {
                    total_bytes: 1000,
                    available_bytes: 500,
                })
            })
        };
        let storage = StorageCapacity::with_probe(roots(), NEVER, probe);

        entered_rx.recv().unwrap();
        assert_eq!(storage.current(StorageRoot::Data), Capacity::Unknown);
        // Refresh demands during the stalled probe queue behind it.
        storage.request_refresh(StorageRoot::Data);
        storage.request_refresh(StorageRoot::Data);
        release_tx.send(()).unwrap();

        storage.wait_for_probes(StorageRoot::Data, 2);
        assert_eq!(most_in_flight.load(Ordering::SeqCst), 1);
        assert_eq!(
            storage.current(StorageRoot::Data).fresh_available_bytes(),
            Some(500)
        );
    }

    #[test]
    fn a_retargeted_root_drops_the_old_reading_and_reads_the_new_path() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(roots(), NEVER, counting_probe(calls.clone()));
        storage.wait_for_probes(StorageRoot::Data, 1);
        assert_eq!(
            storage.current(StorageRoot::Data).fresh_available_bytes(),
            Some(5)
        );

        storage.retarget(StorageRoot::Data, PathBuf::from("/moved/data"));
        storage.wait_for_probes(StorageRoot::Data, 2);
        assert_eq!(
            storage.current(StorageRoot::Data).fresh_available_bytes(),
            Some(11)
        );
        assert!(
            calls
                .lock()
                .unwrap()
                .contains(&PathBuf::from("/moved/data"))
        );
    }

    #[test]
    fn debits_apply_to_the_reading_they_were_made_against() {
        let taken = Instant::now();
        let reading = |available_bytes, sampled_at, stale| {
            Capacity::Known(CapacityReading {
                available_bytes,
                total_bytes: 10_000,
                sampled_at,
                stale,
            })
        };
        let mut debits = CapacityDebits::default();
        assert_eq!(debits.apply(Capacity::Unknown), Capacity::Unknown);
        assert_eq!(
            debits
                .apply(reading(1000, taken, false))
                .best_available_bytes(),
            Some(1000)
        );
        debits.debit(300);
        debits.credit(100);
        assert_eq!(
            debits
                .apply(reading(1000, taken, true))
                .best_available_bytes(),
            Some(800),
            "debits survive a stale reading of the same probe"
        );
        let next = taken + Duration::from_secs(5);
        assert_eq!(
            debits
                .apply(reading(900, next, false))
                .best_available_bytes(),
            Some(900),
            "a newer reading already reflects what was written"
        );
    }
}
