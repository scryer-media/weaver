// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering, fence};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::operations::instrumentation::DiskSpaceSnapshot;
use crate::runtime::StartupDiskGate;
use crate::runtime::system_profile::{FilesystemType, StorageClass};

mod storage_kind;

pub(crate) use storage_kind::StorageKind;

// Capacity for the filesystem backing a path.
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

// Why a capacity probe produced no reading.
#[derive(Debug)]
pub enum DiskProbeError {
    // The operating system rejected the query (missing path, permission,
    // unmounted or unreachable filesystem, ...).
    Io(io::Error),
    // The path cannot be handed to the operating system (interior NUL).
    InvalidPath,
    // The filesystem answered with fields no reading can be built from.
    InvalidReading,
    // This platform has no capacity query.
    Unsupported,
}

impl DiskProbeError {
    // True when the failure means the path does not exist (yet), which is the
    // only failure a probe may recover from by asking an ancestor instead.
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

// Query total/available capacity for the filesystem backing `path`
// (`statfs` on Apple platforms, `statvfs` on other unix,
// `GetDiskFreeSpaceExW` on Windows).
//
// Apple's `statvfs` keeps block counts in 32 bits, so a volume with more
// than 2^32 blocks — a 22 TB share counted in 1 KiB blocks — reads as a
// small disk with more free space than total. Its `statfs` counts in 64 bits.
//
// Fails when the path cannot be stat'd (e.g. it does not exist yet), when the
// filesystem returns an unusable reading, or on unsupported platforms. The
// error carries the operating-system reason so callers can log it and decide
// between failing open, holding a stale reading, and refusing.
fn probe_disk_space(path: &Path) -> Result<DiskSpace, DiskProbeError> {
    #[cfg(any(test, feature = "test-support"))]
    count_disk_look();
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

// Build a reading from `statvfs` block counts.
//
// `f_frsize` is the unit the block counts are expressed in; a few
// filesystems and emulation layers leave it zero and only fill `f_bsize`, so
// that is used as the fallback unit. Both zero means no reading can be built.
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

// Shape an absolute UTF-16 path for `GetDiskFreeSpaceExW`.
//
// The API wants directories to end in a separator (a UNC root without one is
// rejected) and only accepts paths beyond the classic length limit through
// the verbatim `\\?\` prefix. UNC paths take `\\?\UNC\server\share` rather
// than a bare prefix. Already-verbatim and device paths pass through
// untouched; short drive paths stay short so the usual normalization rules
// keep applying to them.
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

// Probe `path`, falling back to its nearest existing ancestor when the path
// itself has not been created yet. Only use this for advisory startup
// estimates: a missing path may also mean a disconnected storage root, and
// its parent's capacity does not establish that the target is available.
// Permission and I/O errors are returned unchanged.
fn probe_nearest_disk_space(path: &Path) -> Result<DiskSpace, DiskProbeError> {
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

// Query total/available capacity, discarding the failure reason.
#[cfg(test)]
fn disk_space(path: &Path) -> Option<DiskSpace> {
    probe_disk_space(path).ok()
}

// The device `path` lies on, read at the nearest of its ancestors that
// exists, as one not yet made will be made there. `None` when it cannot be
// read, or the platform has none to read.
fn nearest_device(path: &Path) -> Option<u64> {
    #[cfg(any(test, feature = "test-support"))]
    count_disk_look();
    #[cfg(test)]
    if let Some(device) = crate::runtime::fs::test_devices::of(path) {
        return Some(device);
    }
    let existing = path
        .ancestors()
        .find(|at| std::fs::metadata(at).is_ok_and(|meta| meta.is_dir()))?;
    directory_device(existing)
}

// The kind of storage `path` lies on, read at the nearest of its ancestors
// that exists.
fn read_storage_kind(path: &Path) -> StorageKind {
    #[cfg(any(test, feature = "test-support"))]
    count_disk_look();
    storage_kind::storage_kind(path)
}

// The class of storage and the filesystem `path` lies on.
fn read_classification(path: &Path) -> Classification {
    #[cfg(any(test, feature = "test-support"))]
    count_disk_look();
    crate::runtime::system_probe::classify_storage(path)
}

#[cfg(unix)]
fn directory_device(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path).ok()?;
    metadata.is_dir().then(|| metadata.dev())
}

#[cfg(windows)]
fn directory_device(path: &Path) -> Option<u64> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, GetFileInformationByHandle,
    };
    if !std::fs::metadata(path).ok()?.is_dir() {
        return None;
    }
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .ok()?;
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    // SAFETY: the handle is open for the call and `info` is a valid slot.
    let result =
        unsafe { GetFileInformationByHandle(directory.as_raw_handle(), info.as_mut_ptr()) };
    if result == 0 {
        return None;
    }
    // SAFETY: the call succeeded, so it filled `info`.
    let info = unsafe { info.assume_init() };
    Some(u64::from(info.dwVolumeSerialNumber))
}

#[cfg(not(any(unix, windows)))]
fn directory_device(_path: &Path) -> Option<u64> {
    None
}

// Whether two directories lie on one device, each read now. A device that
// cannot be read counts as one device. The answer base read on the spot,
// kept for tests to compare the cached answer with.
#[cfg(test)]
pub(crate) fn same_device_now(a: &Path, b: &Path) -> bool {
    one_device(nearest_device(a), nearest_device(b))
}

// Two devices as read: one device unless both were read and differ. A
// device that cannot be read changes nothing.
fn one_device(a: Option<u64>, b: Option<u64>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

// True when an I/O error means the filesystem (or the caller's quota on it)
// ran out of room, as opposed to a corrupt path, permission problem, or
// hardware fault.
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

// One capacity reading as seen by a [`CapacitySampler`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityReading {
    pub available_bytes: u64,
    pub total_bytes: u64,
    // When the underlying probe succeeded. Debits since then are already
    // applied to `available_bytes`.
    pub sampled_at: Instant,
    // The most recent probe failed and this is the last good reading, kept
    // so callers can keep accounting against something rather than nothing.
    pub stale: bool,
}

// What a [`CapacitySampler`] currently knows about its filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capacity {
    Known(CapacityReading),
    // No probe has ever succeeded for this path.
    Unknown,
}

impl Capacity {
    pub fn reading(self) -> Option<CapacityReading> {
        match self {
            Self::Known(reading) => Some(reading),
            Self::Unknown => None,
        }
    }

    // Available bytes from a fresh reading only.
    pub fn fresh_available_bytes(self) -> Option<u64> {
        self.reading()
            .filter(|reading| !reading.stale)
            .map(|reading| reading.available_bytes)
    }

    // Available bytes from the best reading held, fresh or stale.
    pub fn best_available_bytes(self) -> Option<u64> {
        self.reading().map(|reading| reading.available_bytes)
    }
}

type ProbeFn = dyn Fn(&Path) -> Result<DiskSpace, DiskProbeError> + Send + Sync;

// Per-path capacity sampler shared by every admission check that gates work
// on free space.
//
// It bounds the probe rate with a TTL, keeps the last good reading (flagged
// stale) across probe failures so a transient stat error does not turn into
// a phantom "disk full", tracks bytes admitted against the cached reading
// between probes, and logs each failure and recovery transition exactly once
// with the operating-system reason. Callers keep it under their own lock;
// nothing here blocks beyond the syscall.
struct CapacitySampler {
    path: PathBuf,
    ttl: Duration,
    probe: Box<ProbeFn>,
    last_good: Option<CapacityReading>,
    last_attempt: Option<Instant>,
    failing_since: Option<Instant>,
    // Why the most recent probe failed, while it is failing.
    last_error: Option<String>,
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
    // Sample the exact storage path at most once per `ttl`. A missing root
    // stays unavailable rather than borrowing its parent's capacity.
    // No probe runs until the first `sample`.
    #[cfg(test)]
    pub fn new(path: PathBuf, ttl: Duration) -> Self {
        Self::with_probe(path, ttl, Box::new(probe_disk_space))
    }

    // Like [`Self::new`] with a caller-supplied probe (tests, injected
    // accounting).
    pub fn with_probe(path: PathBuf, ttl: Duration, probe: Box<ProbeFn>) -> Self {
        Self {
            path,
            ttl,
            probe,
            last_good: None,
            last_attempt: None,
            failing_since: None,
            last_error: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // Current knowledge without touching the filesystem.
    pub fn current(&self) -> Capacity {
        match self.last_good {
            Some(reading) => Capacity::Known(CapacityReading {
                stale: self.failing_since.is_some(),
                ..reading
            }),
            None => Capacity::Unknown,
        }
    }

    // True when the most recent probe failed.
    #[cfg(test)]
    pub fn is_failing(&self) -> bool {
        self.failing_since.is_some()
    }

    // Why the most recent probe failed; `None` while it succeeds.
    pub(crate) fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    // Return the current reading, probing first when the last attempt is
    // older than the TTL.
    #[cfg(test)]
    pub fn sample(&mut self) -> Capacity {
        self.sample_at(Instant::now())
    }

    // Probe now regardless of the TTL.
    pub fn refresh(&mut self) -> Capacity {
        self.refresh_at(Instant::now())
    }

    #[cfg(test)]
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
                self.last_error = None;
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
                self.last_error = Some(error.to_string());
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

    // Account bytes admitted against the cached reading so a burst of
    // admissions between probes cannot each see the same headroom.
    #[cfg(test)]
    pub fn debit(&mut self, bytes: u64) {
        if let Some(reading) = self.last_good.as_mut() {
            reading.available_bytes = reading.available_bytes.saturating_sub(bytes);
        }
    }

    // Undo a `debit` whose admission was rolled back.
    #[cfg(test)]
    pub fn credit(&mut self, bytes: u64) {
        if let Some(reading) = self.last_good.as_mut() {
            reading.available_bytes = reading.available_bytes.saturating_add(bytes);
        }
    }
}

// How often each storage root's sampler re-reads its filesystem.
//
// A capacity probe is a filesystem round trip that a slow or overloaded
// mount can hold for as long as its request queue is deep, so it never runs
// where a caller is waiting on it. Every consumer — admission checks,
// metrics, the NZBGet status — reads the last completed reading instead,
// which is at most this old plus however long the probe itself took.
pub const STORAGE_CAPACITY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

// The configured storage roots whose free space the runtime tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageRoot {
    Data,
    Intermediate,
    Complete,
}

impl StorageRoot {
    pub const ALL: [Self; 3] = [Self::Data, Self::Intermediate, Self::Complete];

    // Stable label for metrics and logs.
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

// What every writer on one filesystem has promised to write there and not
// yet written, shared by all of them, so two cannot each spend the same free
// space. A member's file takes its whole length on its first write, but the
// file is sparse and fills only as its bytes land: what was promised stays
// promised across readings until it lands, or until the writer lets it go.
//
// Each storage root has a ledger, and the ledgers of roots whose directories
// lie on one filesystem count together: a root's free space is its reading
// less what was promised on every root of its filesystem, and less what
// landed on any of them since the reading. A root whose filesystem is not
// known yet counts alone.
//
// What landed is subtracted from a reading taken before it landed: a reading
// counts the bytes from the one it follows, so each starts that count again.
// Every count is an atomic; nothing here takes a lock. A count read while
// another writer moves it errs on the side of less free space.
pub struct DiskLedger {
    counts: Arc<Counts>,
    // This ledger's root among `counts`.
    index: usize,
    // `basis` keeps a reading's `sampled_at` as its distance from this
    // instant, plus one, so that zero means no reading yet.
    base: Instant,
    // The reading the count of what landed since starts from, and what
    // landed on the filesystem stood at just before it was probed. A sampler
    // publishing to the ledger is the only writer of both; a ledger no
    // sampler publishes to sets them from the first reader that sees a new
    // reading.
    basis: AtomicU64,
    baseline: AtomicU64,
    sampled: bool,
    // The order writers began to wait for room here in.
    tickets: AtomicU64,
    // Rung on each reading published while anyone waits.
    readings: Arc<tokio::sync::Notify>,
    // Asks the filesystem's sampler for a reading without waiting out its
    // interval.
    refresh: Box<dyn Fn() + Send + Sync>,
}

// What anything but a sampler asked of the disk itself, on any thread, so
// a test can pin that a path reads only the sampler's cached reading: each
// capacity probe and each look at the filesystem a path lies on, and each
// time an early reading was asked of a sampler. A test runs in a process
// of its own, so the counts are its own.
#[cfg(any(test, feature = "test-support"))]
pub(crate) static DISK_LOOKS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
pub(crate) static REFRESH_ASKS: AtomicU64 = AtomicU64::new(0);

// The name each sampler's thread begins with.
const SAMPLER_THREAD: &str = "weaver-capacity-";

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn count_disk_look() {
    let sampler = std::thread::current()
        .name()
        .is_some_and(|name| name.starts_with(SAMPLER_THREAD));
    if !sampler {
        DISK_LOOKS.fetch_add(1, Ordering::Relaxed);
    }
}

// How many times anything but a sampler looked at the disk in this
// process, for other crates' tests.
#[cfg(feature = "test-support")]
pub fn disk_looks_for_test() -> u64 {
    DISK_LOOKS.load(Ordering::Relaxed)
}

// One look at `path`'s filesystem, made here, for other crates' tests to
// compare an answer with.
#[cfg(feature = "test-support")]
pub fn read_space_for_test(path: &Path) -> Result<DiskSpace, DiskProbeError> {
    probe_disk_space(path)
}

// How many ledgers count together at most: one per storage root.
const LEDGERS: usize = 3;

// The counts of every root's ledger, by root.
#[derive(Debug, Default)]
struct Counts {
    promised: [AtomicU64; LEDGERS],
    // Every byte that landed against the ledger, ever.
    landed: [AtomicU64; LEDGERS],
    // Writers waiting for room on the ledger's root.
    waiters: [AtomicU64; LEDGERS],
    // The filesystem the root's directory lies on as its sampler last found
    // it, plus one: zero while it is not known.
    device: [AtomicU64; LEDGERS],
    // How many times any of the ledgers rang those waiting for room.
    rings: AtomicU64,
}

impl fmt::Debug for DiskLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let index = self.index;
        f.debug_struct("DiskLedger")
            .field(
                "promised",
                &self.counts.promised[index].load(Ordering::Relaxed),
            )
            .field("landed", &self.counts.landed[index].load(Ordering::Relaxed))
            .field(
                "waiters",
                &self.counts.waiters[index].load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

// The free space a reading leaves once the ledger is taken from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Free {
    pub(crate) bytes: u64,
    // The reading is the last good one, held across a failed probe.
    pub(crate) stale: bool,
}

fn saturating_take(count: &AtomicU64, bytes: u64) {
    if bytes > 0 {
        let _ = count.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
            Some(held.saturating_sub(bytes))
        });
    }
}

impl DiskLedger {
    fn new(
        (counts, index): (Arc<Counts>, usize),
        readings: Arc<tokio::sync::Notify>,
        refresh: Box<dyn Fn() + Send + Sync>,
        sampled: bool,
    ) -> Self {
        Self {
            counts,
            index,
            base: Instant::now(),
            basis: AtomicU64::new(0),
            baseline: AtomicU64::new(0),
            sampled,
            tickets: AtomicU64::new(0),
            readings,
            refresh,
        }
    }

    // A ledger of its own, for a reader no sampler publishes to.
    pub(crate) fn standalone() -> Self {
        Self::new((Arc::default(), 0), Arc::default(), Box::new(|| {}), false)
    }

    // The ledgers that count with this one: those of the roots on its
    // filesystem, or itself alone while that is not known.
    fn peers(&self) -> [bool; LEDGERS] {
        let device = self.counts.device[self.index].load(Ordering::Acquire);
        std::array::from_fn(|index| {
            index == self.index
                || (device != 0 && self.counts.device[index].load(Ordering::Acquire) == device)
        })
    }

    // A count summed over the ledgers that count with this one.
    fn summed(&self, count: &[AtomicU64; LEDGERS]) -> u64 {
        self.peers()
            .iter()
            .zip(count)
            .filter(|(peer, _)| **peer)
            .fold(0u64, |sum, (_, count)| {
                sum.saturating_add(count.load(Ordering::Acquire))
            })
    }

    // Whether `other` counts with this ledger: one ledger, or the ledgers
    // of two roots on one filesystem.
    pub(crate) fn same_disk(&self, other: &DiskLedger) -> bool {
        Arc::ptr_eq(&self.counts, &other.counts) && self.peers()[other.index]
    }

    // The filesystem the root's directory lies on, as its sampler found it
    // before a probe: `None` when it could not tell, which keeps the one it
    // last found. Only a filesystem found moves the root to other peers.
    fn on_device(&self, device: Option<u64>) {
        if let Some(device) = device {
            self.counts.device[self.index].store(device.wrapping_add(1), Ordering::Release);
        }
    }

    // The root's directory changed: the filesystem it lies on is not known
    // until its sampler finds it.
    fn moved(&self) {
        self.counts.device[self.index].store(0, Ordering::Release);
    }

    // What `capacity` leaves free less everything promised, and less what
    // landed since it was read. `None` without a reading.
    #[cfg(test)]
    pub(crate) fn free(&self, capacity: Capacity) -> Option<Free> {
        self.free_with(|| capacity)
    }

    // [`Self::free`] of the reading `read` gives.
    //
    // On a sampled ledger this only reads, and reads the baseline before the
    // reading: the sampler publishes a reading before it moves the baseline
    // to that reading's mark, so a baseline read first is never newer than
    // the reading read after it. A reader between the two counts from the
    // older baseline: more landed, less free.
    pub(crate) fn free_with(&self, read: impl FnOnce() -> Capacity) -> Option<Free> {
        let sampled_baseline = self.baseline.load(Ordering::Acquire);
        let reading = read().reading()?;
        let landed = self.summed(&self.counts.landed);
        let baseline = if self.sampled {
            sampled_baseline
        } else {
            let key = self.key(reading);
            let basis = self.basis.load(Ordering::Acquire);
            if basis != key
                && self
                    .basis
                    .compare_exchange(basis, key, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.baseline.store(landed, Ordering::Release);
            }
            self.baseline.load(Ordering::Acquire)
        };
        let since = landed.saturating_sub(baseline);
        Some(Free {
            bytes: reading
                .available_bytes
                .saturating_sub(since)
                .saturating_sub(self.summed(&self.counts.promised)),
            stale: reading.stale,
        })
    }

    // `bytes` more are promised.
    pub(crate) fn promise(&self, bytes: u64) {
        if bytes > 0 {
            self.counts.promised[self.index].fetch_add(bytes, Ordering::AcqRel);
        }
    }

    // A promise that will not land, or not here. The room it gives back
    // rings as a reading does, so those waiting are judged again.
    pub(crate) fn release(&self, bytes: u64) {
        if bytes > 0 {
            saturating_take(&self.counts.promised[self.index], bytes);
            self.rang();
        }
    }

    // `promised` bytes of a promise landed, and `unpromised` bytes nobody
    // promised first.
    pub(crate) fn landed(&self, promised: u64, unpromised: u64) {
        saturating_take(&self.counts.promised[self.index], promised);
        let landed = promised.saturating_add(unpromised);
        if landed > 0 {
            self.counts.landed[self.index].fetch_add(landed, Ordering::AcqRel);
        }
    }

    // A writer begins to wait for room: the sampler is asked for a reading
    // now, and every reading from here rings until it stops waiting. The
    // ticket orders the writers waiting.
    pub(crate) fn wait(&self) -> u64 {
        self.counts.waiters[self.index].fetch_add(1, Ordering::AcqRel);
        (self.refresh)();
        self.tickets.fetch_add(1, Ordering::AcqRel)
    }

    // A writer no longer waits.
    pub(crate) fn unwait(&self) {
        saturating_take(&self.counts.waiters[self.index], 1);
    }

    // How many times a writer began to wait here: a count that moved since
    // a look means someone began to wait since.
    pub(crate) fn tickets(&self) -> u64 {
        self.tickets.load(Ordering::Acquire)
    }

    // Whether anyone waits for room here.
    #[cfg(test)]
    pub(crate) fn waited_on(&self) -> bool {
        self.counts.waiters[self.index].load(Ordering::Acquire) > 0
    }

    // A reading's key: its `sampled_at` as its distance from `base`, plus
    // one.
    fn key(&self, reading: CapacityReading) -> u64 {
        u64::try_from(
            reading
                .sampled_at
                .saturating_duration_since(self.base)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX - 1)
            + 1
    }

    // What landed on the filesystem so far, taken just before a probe:
    // every byte counted after it is one the probe may not have seen.
    fn mark(&self) -> u64 {
        self.summed(&self.counts.landed)
    }

    // The probe that followed `mark` published `capacity`: what landed is
    // counted from the mark, and whoever waits for room hears it. A failed
    // probe republishes the last good reading under its old time; what
    // landed is still counted from that reading's mark.
    fn published(&self, mark: u64, capacity: Capacity) {
        if let Some(reading) = capacity.reading() {
            let key = self.key(reading);
            if self.basis.load(Ordering::Acquire) != key {
                self.baseline.store(mark, Ordering::Release);
                self.basis.store(key, Ordering::Release);
            }
        }
        self.rang();
    }

    #[cfg(test)]
    pub(crate) fn sampled_for_test() -> Self {
        Self::new((Arc::default(), 0), Arc::default(), Box::new(|| {}), true)
    }

    // Whoever waits for room on the filesystem hears that its reading or
    // what is promised on it changed.
    fn rang(&self) {
        let waited = self
            .peers()
            .iter()
            .zip(&self.counts.waiters)
            .any(|(peer, waiters)| *peer && waiters.load(Ordering::Acquire) > 0);
        if waited {
            self.counts.rings.fetch_add(1, Ordering::Release);
            self.readings.notify_one();
        }
    }

    #[cfg(test)]
    pub(crate) fn promised(&self) -> u64 {
        self.counts.promised[self.index].load(Ordering::Acquire)
    }
}

// A non-blocking view of one filesystem's latest capacity reading, with the
// ledger every writer on that filesystem spends from.
//
// Reading it never touches the filesystem. Clones share the ledger; a reader
// built from a function has a ledger of its own.
#[derive(Clone)]
pub struct CapacityReader {
    read: Arc<dyn Fn() -> Capacity + Send + Sync>,
    ledger: Arc<DiskLedger>,
}

impl fmt::Debug for CapacityReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CapacityReader")
            .field(&self.current())
            .finish()
    }
}

impl CapacityReader {
    // A reader backed by `read`. Tests inject readings through it.
    pub fn from_fn(read: impl Fn() -> Capacity + Send + Sync + 'static) -> Self {
        Self {
            read: Arc::new(read),
            ledger: Arc::new(DiskLedger::standalone()),
        }
    }

    // The ledger every writer through this reader spends from.
    pub(crate) fn ledger(&self) -> &Arc<DiskLedger> {
        &self.ledger
    }

    // A reader with no reading, which every consumer treats as a failed
    // probe: nothing to enforce against.
    pub fn unknown() -> Self {
        Self::from_fn(|| Capacity::Unknown)
    }

    pub fn current(&self) -> Capacity {
        (self.read)()
    }
}

// Bytes one consumer has admitted against a shared reading since that
// reading was taken, so a burst of admissions between refreshes cannot each
// see the same headroom.
//
// The debits belong to the reading they were made against: a newer reading
// already reflects the bytes written since, so it starts the count again. A
// stale reading keeps its timestamp, so debits keep accumulating across a
// probe outage.
#[derive(Debug, Default, Clone, Copy)]
pub struct CapacityDebits {
    basis: Option<Instant>,
    debited: u64,
}

impl CapacityDebits {
    // `capacity` less everything debited against the same reading.
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

    // Undo a `debit` whose admission was rolled back.
    pub fn credit(&mut self, bytes: u64) {
        self.debited = self.debited.saturating_sub(bytes);
    }

    // The reading the debits count against now.
    pub fn basis(&self) -> Option<Instant> {
        self.basis
    }

    // Undo a `debit` made while the count was against `basis`. A newer
    // reading started the count again without it, so then nothing is
    // undone: crediting it would take back another's debit.
    pub fn credit_against(&mut self, basis: Option<Instant>, bytes: u64) {
        if self.basis == basis {
            self.credit(bytes);
        }
    }
}

type SharedProbeFn = Arc<ProbeFn>;

#[derive(Debug)]
struct RootState {
    path: PathBuf,
    refresh_requested: bool,
    stopped: bool,
    begun_probes: u64,
    completed_probes: u64,
    // A probe of `path` finished, and why the latest failed while it fails.
    // A retarget clears both.
    probed: bool,
    error: Option<String>,
    // The device `path` lay on at its latest look: `None` until a look
    // since the last retarget, `Some(None)` when it could not be read.
    device: Option<Option<u64>>,
    // The kind of storage `path` lies on, read once after each retarget.
    kind: Option<StorageKind>,
}

// What [`StorageCapacity::spaces`] tells of a path's free space.
#[derive(Debug, Clone)]
pub enum OnDemandSpace {
    // A current reading: the cache's, or the look just made.
    Read(DiskSpace),
    // The look failed, for this reason.
    Failed(String),
    // The look's worker ended without an answer, for this reason.
    Lost(String),
    // The look did not answer within [`ON_DEMAND_TIMEOUT`].
    TimedOut,
}

// How long an on-demand look may take before its asker is told it did not
// answer. A network mount that hangs must not hold a request.
pub const ON_DEMAND_TIMEOUT: Duration = Duration::from_secs(3);

// Every folder asked after that is not a root's directory with a current
// reading: each configured category's folder, a folder typed in, a job's
// destination. Each path has its own look, made on demand and only for
// that path: never on a timer, never as part of a pass over a list. A look
// reads the path's device first and hands it out at once, then its free
// space and the kind of storage it lies on. Each reading is current for one
// interval from when it was taken; an ask then answers from it with no
// look. A reading that failed is never answered from again: the next ask
// looks anew. An ask that finds nothing current starts one look, off the
// async runtime, unless one is already out for the path, which it shares.
// A look that hangs holds only the askers of its own path, each for at most
// [`ON_DEMAND_TIMEOUT`]. These readings have no ledger and take no part in
// admission, the reserve or the hold.
//
// A path's storage class and filesystem, which only diagnostics asks, are
// read by a look of their own, held and shared the same way.
//
// At most [`PATH_CACHE_MAX`] paths are held. A new path past that drops
// the one asked after longest ago that has no look out; a path with a look
// out is kept until its look returns.
#[derive(Debug, Default)]
struct PathCache {
    entries: std::collections::HashMap<PathBuf, PathEntry>,
    // Counts asks, so the path asked after longest ago is told apart even
    // when two asks read one instant.
    asks: u64,
}

const PATH_CACHE_MAX: usize = 256;

// How many times anything asked whether two paths share a filesystem.
#[cfg(test)]
static SAME_FILESYSTEM_ASKS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn same_filesystem_asks() -> u64 {
    SAME_FILESYSTEM_ASKS.load(Ordering::Relaxed)
}

// The owner's clock, by which a reading's age is told. A test may stop it,
// so an age is what the test makes it, whatever a loaded runner's own clock
// says.
#[derive(Debug)]
struct Clock {
    #[cfg(any(test, feature = "test-support"))]
    created: Instant,
    #[cfg(any(test, feature = "test-support"))]
    stopped: Mutex<Option<Instant>>,
}

impl Clock {
    fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test-support"))]
            created: Instant::now(),
            #[cfg(any(test, feature = "test-support"))]
            stopped: Mutex::new(None),
        }
    }

    fn now(&self) -> Instant {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(stopped) = *self
            .stopped
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            return stopped;
        }
        Instant::now()
    }

    // How long ago `at` was. A reading taken after a stopped clock's
    // instant is no age at all.
    fn age(&self, at: Instant) -> Duration {
        self.now().saturating_duration_since(at)
    }
}

type Classification = (StorageClass, FilesystemType);

type LookWatch = tokio::sync::watch::Receiver<LookProgress>;
type ClassWatch = tokio::sync::watch::Receiver<Option<Arc<ClassLook>>>;

#[derive(Debug, Default)]
struct PathEntry {
    // The latest device reading and space reading that returned.
    device: Option<DeviceLook>,
    space: Option<Arc<SpaceLook>>,
    // The look out for the path, which every asker shares.
    flight: Option<LookWatch>,
    // The latest classification that returned, and the one out.
    class: Option<Arc<ClassLook>>,
    class_flight: Option<ClassWatch>,
    // The ask count at the latest ask.
    asked: u64,
}

// The device a path lies on, read at the nearest folder that exists; `None`
// when it cannot be read.
#[derive(Debug, Clone, Copy)]
struct DeviceLook {
    device: Option<u64>,
    // When the read finished.
    at: Instant,
}

// What a look found of a path's free space, and the kind of storage.
#[derive(Debug)]
struct SpaceLook {
    // The path's own filesystem, or why it could not be read.
    exact: Result<DiskSpace, String>,
    // The filesystem of the nearest folder that exists, the path or one
    // above it: the one a folder not made yet would be made on.
    nearest: Result<DiskSpace, String>,
    kind: StorageKind,
    // When the reads finished.
    at: Instant,
}

impl SpaceLook {
    fn reading(&self, reach: Reach) -> &Result<DiskSpace, String> {
        match reach {
            Reach::Exact => &self.exact,
            Reach::Nearest => &self.nearest,
        }
    }
}

// A path's storage class and filesystem.
#[derive(Debug)]
struct ClassLook {
    class: Classification,
    at: Instant,
}

impl ClassLook {
    // Nothing at all could be read.
    fn failed(&self) -> bool {
        self.class.0 == StorageClass::Unknown
            && self.class.1 == FilesystemType::Unknown(String::new())
    }
}

// How far one look has come: its device as soon as that is read, then its
// space.
#[derive(Debug, Clone, Default)]
struct LookProgress {
    device: Option<DeviceLook>,
    space: Option<Arc<SpaceLook>>,
}

// What an asker wants of a path.
#[derive(Debug, Clone, Copy)]
enum Want {
    Device,
    Space(Reach),
}

impl Want {
    fn answered_by(self, progress: &LookProgress) -> bool {
        match self {
            Want::Device => progress.device.is_some(),
            Want::Space(_) => progress.space.is_some(),
        }
    }
}

// Which reading of a path an asker wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    // The path's own filesystem. A path that does not exist answers why.
    Exact,
    // The filesystem the path is or would be made on: its own, or that of
    // the nearest folder above it that exists.
    Nearest,
}

// An ask on its way: answered, or waiting on a look.
enum Ticket {
    Space(DiskSpace),
    Device(Option<u64>),
    Look(Arc<SpaceLook>),
    Wait(LookWatch),
}

// Why an ask has no look to answer from.
enum Unsettled {
    Lost,
    TimedOut,
}

// `path` when it exists, else the nearest folder above it that does.
fn nearest_existing_ancestor(path: &Path) -> PathBuf {
    path.ancestors()
        .find(|candidate| !candidate.as_os_str().is_empty() && candidate.exists())
        .unwrap_or(path)
        .to_path_buf()
}

// The free space a look reads at `path`: the path's own filesystem, and the
// filesystem of the nearest folder that exists, the path itself or one above
// it, whatever kept the path from being reached. That is the one a folder
// not made yet would be made on, and the kind of storage is read there too.
fn space_at(probe: &ProbeFn, clock: &Clock, path: &Path) -> SpaceLook {
    let ancestor = nearest_existing_ancestor(path);
    let exact = probe(path).map_err(|error| error.to_string());
    let nearest = if ancestor == path {
        exact.clone()
    } else {
        probe(&ancestor).map_err(|error| error.to_string())
    };
    let kind = read_storage_kind(&ancestor);
    SpaceLook {
        exact,
        nearest,
        kind,
        at: clock.now(),
    }
}

// Lock the path cache, whoever held it last.
fn lock_paths(cache: &Mutex<PathCache>) -> std::sync::MutexGuard<'_, PathCache> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// Start one look at `path` on the blocking pool, its one thread for the
// path. Its device is read first and handed out at once, so an asker of the
// device never waits on the free space; then the free space. Each part is
// stored in `cache`, when there is one, as it returns, and handed to every
// waiter. A look that panics closes the channel, and its waiters are told
// it was lost.
fn spawn_look(
    probe: SharedProbeFn,
    clock: Arc<Clock>,
    path: PathBuf,
    cache: Option<Arc<Mutex<PathCache>>>,
) -> LookWatch {
    let (sender, receiver) = tokio::sync::watch::channel(LookProgress::default());
    drop(tokio::task::spawn_blocking(move || {
        let device = DeviceLook {
            device: nearest_device(&path),
            at: clock.now(),
        };
        if let Some(cache) = &cache
            && let Some(entry) = lock_paths(cache).entries.get_mut(&path)
        {
            entry.device = Some(device);
        }
        sender.send_modify(|progress| progress.device = Some(device));
        let space = Arc::new(space_at(&*probe, &clock, &path));
        if let Some(cache) = &cache
            && let Some(entry) = lock_paths(cache).entries.get_mut(&path)
        {
            entry.space = Some(Arc::clone(&space));
            entry.flight = None;
        }
        sender.send_modify(|progress| progress.space = Some(space));
    }));
    receiver
}

// Start one look at `path`'s storage class and filesystem on the blocking
// pool, stored and handed out as [`spawn_look`] does.
fn spawn_class_look(clock: Arc<Clock>, path: PathBuf, cache: Arc<Mutex<PathCache>>) -> ClassWatch {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    drop(tokio::task::spawn_blocking(move || {
        let class = Arc::new(ClassLook {
            class: read_classification(&path),
            at: clock.now(),
        });
        if let Some(entry) = lock_paths(&cache).entries.get_mut(&path) {
            entry.class = Some(Arc::clone(&class));
            entry.class_flight = None;
        }
        sender.send_modify(|look| *look = Some(class));
    }));
    receiver
}

impl Ticket {
    // What this ask answers from, waiting until `deadline` at most.
    async fn settle(
        self,
        want: Want,
        deadline: tokio::time::Instant,
    ) -> Result<Settled, Unsettled> {
        match self {
            Ticket::Space(space) => Ok(Settled::Space(space)),
            Ticket::Device(device) => Ok(Settled::Device(device)),
            Ticket::Look(look) => Ok(Settled::Look(look)),
            Ticket::Wait(mut flight) => {
                let waited = tokio::time::timeout_at(
                    deadline,
                    flight.wait_for(|progress| want.answered_by(progress)),
                )
                .await;
                match waited {
                    Ok(Ok(progress)) => Ok(match want {
                        Want::Device => Settled::Device(
                            progress.device.expect("a device the wait accepted").device,
                        ),
                        Want::Space(_) => Settled::Look(Arc::clone(
                            progress.space.as_ref().expect("a space the wait accepted"),
                        )),
                    }),
                    Ok(Err(_)) => Err(Unsettled::Lost),
                    Err(_) => Err(Unsettled::TimedOut),
                }
            }
        }
    }
}

enum Settled {
    Space(DiskSpace),
    Device(Option<u64>),
    Look(Arc<SpaceLook>),
}

impl Settled {
    fn space(self, reach: Reach) -> OnDemandSpace {
        match self {
            Settled::Space(space) => OnDemandSpace::Read(space),
            Settled::Device(_) => unreachable!("a space ask is never answered with a device"),
            Settled::Look(look) => match look.reading(reach) {
                Ok(space) => OnDemandSpace::Read(*space),
                Err(error) => OnDemandSpace::Failed(error.clone()),
            },
        }
    }

    fn device(self) -> Option<u64> {
        match self {
            Settled::Device(device) => device,
            Settled::Look(_) | Settled::Space(_) => {
                unreachable!("a device ask is never answered with a space")
            }
        }
    }
}

// One root's latest reading, readable without a lock.
//
// Every write happens under the slot's lock, so a retarget and a reading that
// lands for the old directory stay ordered; a reader takes no lock. The
// sequence count is odd while a write is under way, and a read that overlaps
// one tries again. The reading lives only here.
#[derive(Debug)]
struct PublishedCapacity {
    // `sampled_at` is kept as its distance from this instant.
    base: Instant,
    seq: AtomicU64,
    available_bytes: AtomicU64,
    total_bytes: AtomicU64,
    sampled_after_base_ns: AtomicU64,
    flags: AtomicU8,
}

const CAPACITY_KNOWN: u8 = 1;
const CAPACITY_STALE: u8 = 2;

impl PublishedCapacity {
    fn unknown(base: Instant) -> Self {
        Self {
            base,
            seq: AtomicU64::new(0),
            available_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            sampled_after_base_ns: AtomicU64::new(0),
            flags: AtomicU8::new(0),
        }
    }

    // Called with the slot's lock held, so there is one writer at a time.
    fn publish(&self, capacity: Capacity) {
        let (available_bytes, total_bytes, sampled_after_base_ns, flags) = match capacity {
            Capacity::Known(reading) => (
                reading.available_bytes,
                reading.total_bytes,
                u64::try_from(
                    reading
                        .sampled_at
                        .saturating_duration_since(self.base)
                        .as_nanos(),
                )
                .unwrap_or(u64::MAX),
                CAPACITY_KNOWN | if reading.stale { CAPACITY_STALE } else { 0 },
            ),
            Capacity::Unknown => (0, 0, 0, 0),
        };
        let seq = self.seq.load(Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        self.available_bytes
            .store(available_bytes, Ordering::Relaxed);
        self.total_bytes.store(total_bytes, Ordering::Relaxed);
        self.sampled_after_base_ns
            .store(sampled_after_base_ns, Ordering::Relaxed);
        self.flags.store(flags, Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
    }

    fn read(&self) -> Capacity {
        loop {
            let before = self.seq.load(Ordering::Acquire);
            if before & 1 == 0 {
                let available_bytes = self.available_bytes.load(Ordering::Relaxed);
                let total_bytes = self.total_bytes.load(Ordering::Relaxed);
                let sampled_after_base_ns = self.sampled_after_base_ns.load(Ordering::Relaxed);
                let flags = self.flags.load(Ordering::Relaxed);
                fence(Ordering::Acquire);
                if self.seq.load(Ordering::Relaxed) == before {
                    if flags & CAPACITY_KNOWN == 0 {
                        return Capacity::Unknown;
                    }
                    return Capacity::Known(CapacityReading {
                        available_bytes,
                        total_bytes,
                        sampled_at: self.base + Duration::from_nanos(sampled_after_base_ns),
                        stale: flags & CAPACITY_STALE != 0,
                    });
                }
            }
            std::hint::spin_loop();
        }
    }
}

#[derive(Debug)]
struct RootSlot {
    root: StorageRoot,
    state: Mutex<RootState>,
    capacity: PublishedCapacity,
    // What every writer on the root has promised it.
    ledger: Arc<DiskLedger>,
    wake: Condvar,
    #[cfg(test)]
    locks: AtomicU64,
}

impl RootSlot {
    fn lock(&self) -> std::sync::MutexGuard<'_, RootState> {
        #[cfg(test)]
        self.locks.fetch_add(1, Ordering::Relaxed);
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn request_refresh(&self) {
        #[cfg(test)]
        REFRESH_ASKS.fetch_add(1, Ordering::Relaxed);
        self.lock().refresh_requested = true;
        self.wake.notify_all();
    }
}

// One background capacity sampler per configured storage root.
//
// Each root has its own thread that probes, publishes, and then waits a full
// interval after the probe finished, so probes of one root never overlap and
// a stalled mount delays only its own next reading. A failed probe keeps the
// last good reading, flagged stale ([`CapacitySampler`]). Until a root's
// first probe completes its readers see [`Capacity::Unknown`].
//
// Dropping it stops the threads; one stuck in a probe exits when the probe
// returns.
pub struct StorageCapacity {
    slots: [Arc<RootSlot>; 3],
    // Rung on a reading of any root that someone waits on for room.
    readings: Arc<tokio::sync::Notify>,
    // Every other path asked after, looked at on demand.
    paths: Arc<Mutex<PathCache>>,
    interval: Duration,
    probe: SharedProbeFn,
    clock: Arc<Clock>,
}

impl fmt::Debug for StorageCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for slot in &self.slots {
            let state = slot.lock();
            list.entry(&(slot.root.label(), &state.path, slot.capacity.read()));
        }
        list.finish()
    }
}

impl StorageCapacity {
    // Start sampling the real filesystems behind the three roots once
    // `startup_disk_work` opens. Until then every reading is unknown, which
    // readers already allow for before a root's first probe.
    pub fn start(
        data: PathBuf,
        intermediate: PathBuf,
        complete: PathBuf,
        startup_disk_work: StartupDiskGate,
    ) -> Self {
        Self::with_probe_after(
            [data, intermediate, complete],
            STORAGE_CAPACITY_REFRESH_INTERVAL,
            Arc::new(probe_disk_space),
            startup_disk_work,
        )
    }

    // Like [`Self::start`] with a caller-supplied probe and interval, and
    // nothing to wait for. `roots` is in [`StorageRoot::ALL`] order. For
    // tests: everything else reads the real filesystems through
    // [`Self::start`].
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_probe(roots: [PathBuf; 3], interval: Duration, probe: SharedProbeFn) -> Self {
        Self::with_probe_after(roots, interval, probe, StartupDiskGate::opened())
    }

    // Like [`Self::with_probe`], with each root's first probe held until
    // `startup_disk_work` opens.
    fn with_probe_after(
        roots: [PathBuf; 3],
        interval: Duration,
        probe: SharedProbeFn,
        startup_disk_work: StartupDiskGate,
    ) -> Self {
        let base = Instant::now();
        // Made before any root reads, so a stopped clock reads every
        // reading as taken no earlier than it.
        let clock = Arc::new(Clock::new());
        let readings = Arc::new(tokio::sync::Notify::new());
        // The roots' ledgers count together where they share a filesystem.
        let counts = Arc::new(Counts::default());
        let slots = StorageRoot::ALL.map(|root| {
            Arc::new_cyclic(|slot: &std::sync::Weak<RootSlot>| {
                let slot = slot.clone();
                let refresh = Box::new(move || {
                    if let Some(slot) = slot.upgrade() {
                        slot.request_refresh();
                    }
                });
                RootSlot {
                    root,
                    state: Mutex::new(RootState {
                        path: roots[root.index()].clone(),
                        refresh_requested: false,
                        stopped: false,
                        begun_probes: 0,
                        completed_probes: 0,
                        probed: false,
                        error: None,
                        device: None,
                        kind: None,
                    }),
                    capacity: PublishedCapacity::unknown(base),
                    ledger: Arc::new(DiskLedger::new(
                        (Arc::clone(&counts), root.index()),
                        Arc::clone(&readings),
                        refresh,
                        true,
                    )),
                    wake: Condvar::new(),
                    #[cfg(test)]
                    locks: AtomicU64::new(0),
                }
            })
        });
        for slot in &slots {
            // The sampler waits on its own slot's condition variable, so
            // the gate opening has to wake it there.
            let woken = Arc::clone(slot);
            startup_disk_work.on_open(move || {
                drop(woken.lock());
                woken.wake.notify_all();
            });
            let thread_slot = Arc::clone(slot);
            let thread_probe: SharedProbeFn = Arc::clone(&probe);
            let thread_gate = startup_disk_work.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("{SAMPLER_THREAD}{}", slot.root.label()))
                .spawn(move || {
                    run_root_sampler(&thread_slot, interval, thread_probe, &thread_gate)
                });
            if let Err(error) = spawned {
                warn!(
                    root = slot.root.label(),
                    error = %error,
                    "could not start the storage capacity sampler; readings stay unknown"
                );
            }
        }
        Self {
            slots,
            readings,
            paths: Arc::default(),
            interval,
            probe,
            clock,
        }
    }

    // The free space of each asked path, in the order asked: a root's
    // current reading, a current look, or a look made now. Every look this
    // needs starts before any is waited on, so the asks are looked at
    // together, and the whole call waits [`ON_DEMAND_TIMEOUT`] at most: a
    // path whose look does not return by then answers `TimedOut`, and the
    // others still answer. Without an owner (no pipeline) each path gets a
    // look of its own that nothing keeps.
    pub async fn spaces(storage: Option<&Self>, asks: &[(PathBuf, Reach)]) -> Vec<OnDemandSpace> {
        let deadline = tokio::time::Instant::now() + ON_DEMAND_TIMEOUT;
        let tickets: Vec<Ticket> = asks
            .iter()
            .map(|(path, reach)| match storage {
                Some(storage) => storage
                    .root_space(path)
                    .map_or_else(|| storage.begin(path, Want::Space(*reach)), Ticket::Space),
                None => Ticket::Wait(spawn_look(
                    Arc::new(probe_disk_space),
                    Arc::new(Clock::new()),
                    path.clone(),
                    None,
                )),
            })
            .collect();
        let mut answers = Vec::with_capacity(tickets.len());
        for (ticket, (_, reach)) in tickets.into_iter().zip(asks) {
            answers.push(match ticket.settle(Want::Space(*reach), deadline).await {
                Ok(settled) => settled.space(*reach),
                Err(Unsettled::Lost) => {
                    OnDemandSpace::Lost("the look at the folder ended without an answer".into())
                }
                Err(Unsettled::TimedOut) => OnDemandSpace::TimedOut,
            });
        }
        answers
    }

    // The free space of `path`, or of the nearest folder above it that
    // exists, as [`Self::spaces`] answers it.
    pub async fn path_space(storage: Option<&Self>, path: PathBuf) -> OnDemandSpace {
        Self::spaces(storage, &[(path, Reach::Nearest)])
            .await
            .pop()
            .expect("one answer per ask")
    }

    // Whether `a` and `b` lie on one filesystem: a root's device from its
    // latest look, any other from a current device read or one made now,
    // both looked at together. A device read never waits on a free-space
    // read. A device that cannot be read, a look that is lost, or one that
    // does not return within [`ON_DEMAND_TIMEOUT`] answers one filesystem: a
    // job then delivers from its working directory, and its final move
    // copies across devices.
    pub(crate) async fn same_filesystem(&self, a: &Path, b: &Path) -> bool {
        #[cfg(test)]
        SAME_FILESYSTEM_ASKS.fetch_add(1, Ordering::Relaxed);
        let deadline = tokio::time::Instant::now() + ON_DEMAND_TIMEOUT;
        let tickets = [a, b].map(|path| {
            self.root_device(path)
                .map_or_else(|| self.begin(path, Want::Device), Ticket::Device)
        });
        let mut devices = [None, None];
        for ((ticket, device), path) in tickets.into_iter().zip(&mut devices).zip([a, b]) {
            match ticket.settle(Want::Device, deadline).await {
                Ok(settled) => *device = settled.device(),
                Err(Unsettled::Lost) => {
                    debug!(path = %path.display(), "the device look was lost; counted as one filesystem");
                    return true;
                }
                Err(Unsettled::TimedOut) => {
                    debug!(path = %path.display(), "the device look did not answer in time; counted as one filesystem");
                    return true;
                }
            }
        }
        one_device(devices[0], devices[1])
    }

    // A root's current reading of its own directory `path`: read without a
    // failure since, and no older than two intervals, which a root that
    // keeps looking never is.
    fn root_space(&self, path: &Path) -> Option<DiskSpace> {
        for slot in &self.slots {
            let state = slot.lock();
            if state.path != path {
                continue;
            }
            if !state.probed || state.error.is_some() {
                return None;
            }
            return match slot.capacity.read() {
                Capacity::Known(reading)
                    if !reading.stale && self.clock.age(reading.sampled_at) < self.interval * 2 =>
                {
                    Some(DiskSpace {
                        total_bytes: reading.total_bytes,
                        available_bytes: reading.available_bytes,
                    })
                }
                _ => None,
            };
        }
        None
    }

    // The device a root's directory `path` lay on at its latest look, when
    // that look could read it.
    fn root_device(&self, path: &Path) -> Option<Option<u64>> {
        self.slots.iter().find_map(|slot| {
            let state = slot.lock();
            (state.path == path)
                .then_some(state.device)
                .flatten()
                .filter(Option::is_some)
        })
    }

    // The entry for `path`, made when there is none. Past the cap, the
    // entry asked after longest ago with no look out is dropped first.
    fn entry<'a>(cache: &'a mut PathCache, path: &Path) -> &'a mut PathEntry {
        cache.asks += 1;
        let asked = cache.asks;
        if !cache.entries.contains_key(path)
            && cache.entries.len() >= PATH_CACHE_MAX
            && let Some(oldest) = cache
                .entries
                .iter()
                .filter(|(_, entry)| entry.flight.is_none() && entry.class_flight.is_none())
                .min_by_key(|(_, entry)| entry.asked)
                .map(|(path, _)| path.clone())
        {
            cache.entries.remove(&oldest);
        }
        let entry = cache.entries.entry(path.to_path_buf()).or_default();
        entry.asked = asked;
        entry
    }

    // Ask `want` of `path`: a current reading that did not fail, else the
    // look out for it, else a new one. Never waits.
    fn begin(&self, path: &Path, want: Want) -> Ticket {
        let mut cache = lock_paths(&self.paths);
        let entry = Self::entry(&mut cache, path);
        match want {
            Want::Device => {
                if let Some(look) = entry.device
                    && look.device.is_some()
                    && self.clock.age(look.at) < self.interval
                {
                    return Ticket::Device(look.device);
                }
            }
            Want::Space(reach) => {
                if let Some(look) = &entry.space
                    && look.reading(reach).is_ok()
                    && self.clock.age(look.at) < self.interval
                {
                    return Ticket::Look(Arc::clone(look));
                }
            }
        }
        // A look that panicked closed its channel; the next ask starts
        // another. One that returned is no longer out.
        if let Some(flight) = &entry.flight
            && flight.has_changed().is_ok()
        {
            return Ticket::Wait(flight.clone());
        }
        let flight = spawn_look(
            Arc::clone(&self.probe),
            Arc::clone(&self.clock),
            path.to_path_buf(),
            Some(Arc::clone(&self.paths)),
        );
        entry.flight = Some(flight.clone());
        Ticket::Wait(flight)
    }

    // The storage class and filesystem of each path, in the order asked: a
    // current classification, or one made now, all made together. A path
    // whose look does not return within [`ON_DEMAND_TIMEOUT`], or is lost,
    // answers `None`, and the others still answer.
    pub async fn classifications(&self, paths: &[PathBuf]) -> Vec<Option<Classification>> {
        let deadline = tokio::time::Instant::now() + ON_DEMAND_TIMEOUT;
        let tickets: Vec<Result<Arc<ClassLook>, ClassWatch>> = paths
            .iter()
            .map(|path| {
                let mut cache = lock_paths(&self.paths);
                let entry = Self::entry(&mut cache, path);
                if let Some(look) = &entry.class
                    && !look.failed()
                    && self.clock.age(look.at) < self.interval
                {
                    return Ok(Arc::clone(look));
                }
                if let Some(flight) = &entry.class_flight
                    && flight.has_changed().is_ok()
                {
                    return Err(flight.clone());
                }
                let flight = spawn_class_look(
                    Arc::clone(&self.clock),
                    path.clone(),
                    Arc::clone(&self.paths),
                );
                entry.class_flight = Some(flight.clone());
                Err(flight)
            })
            .collect();
        let mut answers = Vec::with_capacity(tickets.len());
        for ticket in tickets {
            answers.push(match ticket {
                Ok(look) => Some(look.class.clone()),
                Err(mut flight) => {
                    match tokio::time::timeout_at(deadline, flight.wait_for(Option::is_some)).await
                    {
                        Ok(Ok(look)) => look.as_ref().map(|look| look.class.clone()),
                        Ok(Err(_)) | Err(_) => None,
                    }
                }
            });
        }
        answers
    }

    // The kind of storage `path` lies on, as the owner holds it: a root's,
    // read once after each retarget, or a folder's, from its latest look.
    // Unknown when neither is held. Never looks.
    pub(crate) fn kind(&self, path: &Path) -> StorageKind {
        for slot in &self.slots {
            let state = slot.lock();
            if state.path == path
                && let Some(kind) = state.kind
            {
                return kind;
            }
        }
        lock_paths(&self.paths)
            .entries
            .get(path)
            .and_then(|entry| entry.space.as_ref())
            .map_or(StorageKind::Unknown, |look| look.kind)
    }

    // Stop the owner's clock where it began, so every reading's age is
    // what a test makes it with [`Self::advance_clock`], however slowly
    // the test runs.
    #[cfg(any(test, feature = "test-support"))]
    pub fn stop_clock_for_test(&self) {
        *self
            .clock
            .stopped
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(self.clock.created);
    }

    // Move a stopped clock on by `by`.
    #[cfg(test)]
    fn advance_clock(&self, by: Duration) {
        let mut stopped = self
            .clock
            .stopped
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = stopped.expect("a stopped clock");
        *stopped = Some(now + by);
    }

    // Wait until every look out has read its free space.
    #[cfg(test)]
    async fn looks_returned(&self) {
        let flights: Vec<LookWatch> = lock_paths(&self.paths)
            .entries
            .values()
            .filter_map(|entry| entry.flight.clone())
            .collect();
        for mut flight in flights {
            let _ = flight.wait_for(|progress| progress.space.is_some()).await;
        }
    }

    // The paths held, in no order.
    #[cfg(test)]
    fn held_paths(&self) -> Vec<PathBuf> {
        lock_paths(&self.paths).entries.keys().cloned().collect()
    }

    // On-demand space of `path`, or the nearest existing folder above it,
    // for an asker that must have a number now and runs before or without
    // the samplers: start-up limits, a standalone extraction, the
    // application upgrade's free-space check. Blocks the calling thread for
    // one look per missing level; never call it on the async runtime.
    pub(crate) fn read_nearest_now(path: &Path) -> Result<DiskSpace, DiskProbeError> {
        probe_nearest_disk_space(path)
    }

    // Block until `root` is pointed at `path` and a look at it finished.
    #[cfg(any(test, feature = "test-support"))]
    pub fn wait_for_root_read(&self, root: StorageRoot, path: &Path) {
        let slot = self.slot(root);
        let state = slot.lock();
        drop(
            slot.wake
                .wait_while(state, |state| state.path != path || !state.probed)
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }

    // Rung when a root that someone waits on for room has a new reading.
    pub(crate) fn readings(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.readings)
    }

    // How many times it was rung: a count that moved since a look means it
    // rang since, heard by a turn that does not wait on it.
    pub(crate) fn rings(&self) -> u64 {
        self.slots[0].ledger.counts.rings.load(Ordering::Acquire)
    }

    fn slot(&self, root: StorageRoot) -> &Arc<RootSlot> {
        &self.slots[root.index()]
    }

    // The latest reading for `root`, without touching the filesystem or
    // taking a lock.
    pub fn current(&self, root: StorageRoot) -> Capacity {
        self.slot(root).capacity.read()
    }

    // A cloneable reader of `root`'s latest reading. Reading it takes no
    // lock, so a per-article admission check never waits on the sampler.
    pub fn reader(&self, root: StorageRoot) -> CapacityReader {
        let slot = Arc::clone(self.slot(root));
        let ledger = Arc::clone(&slot.ledger);
        CapacityReader {
            read: Arc::new(move || slot.capacity.read()),
            ledger,
        }
    }

    // What `root`'s latest reading leaves free less its ledger, read in
    // place: no reader is built, and nothing is allocated.
    pub(crate) fn free(&self, root: StorageRoot) -> Option<Free> {
        let slot = self.slot(root);
        slot.ledger.free_with(|| slot.capacity.read())
    }

    // The ledger every writer on `root`'s filesystem spends from.
    pub(crate) fn ledger(&self, root: StorageRoot) -> &DiskLedger {
        &self.slot(root).ledger
    }

    // Point `root` at a new directory. The old directory's reading is
    // dropped and the new one is probed as soon as the thread is free.
    pub fn retarget(&self, root: StorageRoot, path: PathBuf) {
        {
            let slot = self.slot(root);
            let mut state = slot.lock();
            if state.path == path {
                return;
            }
            state.path = path;
            state.probed = false;
            state.error = None;
            state.device = None;
            state.kind = None;
            slot.capacity.publish(Capacity::Unknown);
            slot.ledger.moved();
            slot.ledger.rang();
            state.refresh_requested = true;
            slot.wake.notify_all();
        }
    }

    // Ask `root`'s thread to probe again without waiting out its interval.
    // A request made while a probe is running is served after it finishes.
    pub fn request_refresh(&self, root: StorageRoot) {
        self.slot(root).request_refresh();
    }

    // One row per root with a reading, fresh or held from the last good
    // probe. A root that has never produced a reading is omitted rather than
    // reported as zero capacity.
    pub fn snapshots(&self) -> Vec<DiskSpaceSnapshot> {
        self.slots
            .iter()
            .filter_map(|slot| {
                let state = slot.lock();
                slot.capacity
                    .read()
                    .reading()
                    .map(|reading| DiskSpaceSnapshot {
                        role: slot.root.label(),
                        path: state.path.display().to_string(),
                        total_bytes: reading.total_bytes,
                        available_bytes: reading.available_bytes,
                    })
            })
            .collect()
    }

    // Block until `root`'s published reading is one `ready` accepts. A
    // reading is published under the slot's lock, and the waiter is woken
    // after, so none is missed.
    #[cfg(test)]
    pub(crate) fn wait_for_reading(
        &self,
        root: StorageRoot,
        ready: impl Fn(&CapacityReading) -> bool,
    ) {
        let slot = self.slot(root);
        let state = slot.lock();
        drop(
            slot.wake
                .wait_while(state, |_| {
                    !slot
                        .capacity
                        .read()
                        .reading()
                        .is_some_and(|reading| ready(&reading))
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }

    // Ask every root for a look and block until a look that began after
    // the ask has finished for each.
    #[cfg(test)]
    pub(crate) fn reread_roots(&self) {
        for slot in &self.slots {
            let mut state = slot.lock();
            state.refresh_requested = true;
            slot.wake.notify_all();
            let state = slot
                .wake
                .wait_while(state, |state| state.refresh_requested && !state.stopped)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let begun = state.begun_probes;
            drop(
                slot.wake
                    .wait_while(state, |state| {
                        state.completed_probes < begun && !state.stopped
                    })
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            );
        }
    }

    // Block until `root` has completed at least `probes` probes.
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

fn run_root_sampler(
    slot: &RootSlot,
    interval: Duration,
    probe: SharedProbeFn,
    startup_disk_work: &StartupDiskGate,
) {
    // Hold the first probe until the startup disk work is done. Stopping the
    // storage meanwhile ends the wait as well.
    {
        let state = slot.lock();
        let state = slot
            .wake
            .wait_while(state, |state| {
                !state.stopped && !startup_disk_work.is_open()
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.stopped {
            return;
        }
    }
    let mut sampler: Option<CapacitySampler> = None;
    loop {
        let (path, wants_kind) = {
            let mut state = slot.lock();
            if state.stopped {
                return;
            }
            state.refresh_requested = false;
            state.begun_probes += 1;
            (state.path.clone(), state.kind.is_none())
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
        // The filesystem is found again before each probe, so a root moved
        // to another one counts with its new peers from its next reading.
        let device = nearest_device(&path);
        slot.ledger.on_device(device);
        // The kind of storage is read once for each directory the root is
        // pointed at, here, so no one else has to look for it.
        let kind = wants_kind.then(|| read_storage_kind(&path));
        let mark = slot.ledger.mark();
        let sampler = sampler.as_mut().expect("a sampler for the current path");
        let capacity = sampler.refresh();
        let mut state = slot.lock();
        // A retarget while the probe ran makes this reading the old
        // directory's; the thread probes the new one straight away.
        if state.path == path {
            slot.capacity.publish(capacity);
            slot.ledger.published(mark, capacity);
            state.probed = true;
            state.device = Some(device);
            if kind.is_some() {
                state.kind = kind;
            }
            if state.error.as_deref() != sampler.last_error() {
                state.error = sampler.last_error().map(str::to_owned);
            }
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
    // Two roots found on one filesystem count together, and go on doing so
    // across a failed look: only a filesystem found otherwise, or a new
    // directory, parts them.
    #[test]
    fn a_failed_look_at_a_roots_filesystem_keeps_it_with_its_peers() {
        let counts = Arc::new(Counts::default());
        let ledger = |index| {
            DiskLedger::new(
                (Arc::clone(&counts), index),
                Arc::default(),
                Box::new(|| {}),
                true,
            )
        };
        let (output, working) = (ledger(0), ledger(1));
        assert!(!output.same_disk(&working), "nothing known yet");
        output.on_device(Some(7));
        working.on_device(Some(7));
        assert!(output.same_disk(&working));
        working.on_device(None);
        assert!(output.same_disk(&working), "a failed look keeps the disk");
        working.on_device(Some(8));
        assert!(!output.same_disk(&working), "another disk found parts them");
        working.on_device(Some(7));
        assert!(output.same_disk(&working));
        working.moved();
        assert!(!output.same_disk(&working), "a new directory is not known");
        working.on_device(None);
        assert!(!output.same_disk(&working), "nor after a failed look");
    }

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

    // A year: long enough that no test ever sees an interval refresh, so
    // every probe a test counts is one it asked for.
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

    // No root is probed before the startup disk work is done: every probe
    // records whether the gate was open when it ran.
    #[test]
    fn samplers_hold_their_first_probe_until_startup_disk_work_is_done() {
        let gate = StartupDiskGate::closed();
        let opened_at_probe = Arc::new(Mutex::new(Vec::new()));
        let probe: SharedProbeFn = {
            let gate = gate.clone();
            let opened_at_probe = Arc::clone(&opened_at_probe);
            Arc::new(move |_: &Path| {
                opened_at_probe.lock().unwrap().push(gate.is_open());
                Ok(DiskSpace {
                    total_bytes: 1000,
                    available_bytes: 500,
                })
            })
        };
        let storage = StorageCapacity::with_probe_after(roots(), NEVER, probe, gate.clone());
        for root in StorageRoot::ALL {
            assert!(
                matches!(storage.current(root), Capacity::Unknown),
                "nothing is known before the gate opens"
            );
        }

        gate.open();
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        let opened_at_probe = opened_at_probe.lock().unwrap();
        assert_eq!(opened_at_probe.len(), 3, "one probe per root");
        assert!(opened_at_probe.iter().all(|open| *open));
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

    // Admission reads a root's capacity once per article, so the reader must
    // hand back the sampler's reading without taking the root's lock.
    #[test]
    fn a_capacity_reader_takes_no_lock_and_returns_the_sampler_s_reading() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(roots(), NEVER, counting_probe(calls.clone()));
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        let slot = storage.slot(StorageRoot::Data);
        let reader = storage.reader(StorageRoot::Data);
        let locks = slot.locks.load(Ordering::Relaxed);
        let first = reader.current();
        for _ in 0..5 {
            assert_eq!(reader.current(), first);
            assert_eq!(storage.current(StorageRoot::Data), first);
        }
        assert_eq!(
            slot.locks.load(Ordering::Relaxed),
            locks,
            "reading a root's capacity takes no lock"
        );
        let Capacity::Known(reading) = first else {
            panic!("the data root has a reading");
        };
        assert_eq!(
            (reading.available_bytes, reading.total_bytes, reading.stale),
            (5, 1000, false)
        );
        assert!(reading.sampled_at >= slot.capacity.base);
        assert_eq!(
            storage.current(StorageRoot::Complete),
            Capacity::Unknown,
            "a root whose probe failed with no earlier reading stays unknown"
        );
    }

    // The published reading is the sampler's reading, timestamp included:
    // debits are keyed on `sampled_at`, so it has to come back exactly.
    #[test]
    fn a_published_reading_round_trips_exactly() {
        let base = Instant::now();
        let published = PublishedCapacity::unknown(base);
        assert_eq!(published.read(), Capacity::Unknown);
        for (offset_ns, stale) in [(0, false), (1, true), (5_000_000_123, false)] {
            let capacity = Capacity::Known(CapacityReading {
                available_bytes: 7 + offset_ns,
                total_bytes: u64::MAX,
                sampled_at: base + Duration::from_nanos(offset_ns),
                stale,
            });
            published.publish(capacity);
            assert_eq!(published.read(), capacity);
        }
        published.publish(Capacity::Unknown);
        assert_eq!(published.read(), Capacity::Unknown);
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

    // A debit taken back after the reading was taken again takes back
    // nothing: the newer reading started the count without it, and what is
    // counted now is another's. Under the same reading it is taken back.
    #[test]
    fn a_credit_takes_back_only_a_debit_of_the_same_reading() {
        let taken = Instant::now();
        let reading = |available_bytes, sampled_at| {
            Capacity::Known(CapacityReading {
                available_bytes,
                total_bytes: 10_000,
                sampled_at,
                stale: false,
            })
        };
        let mut debits = CapacityDebits::default();
        debits.apply(reading(1000, taken));
        debits.debit(300);
        let first = debits.basis();
        let next = taken + Duration::from_secs(5);
        debits.apply(reading(1000, next));
        debits.debit(200);
        debits.credit_against(first, 300);
        assert_eq!(
            debits.apply(reading(1000, next)).best_available_bytes(),
            Some(800),
            "the other's debit stands"
        );
        debits.credit_against(debits.basis(), 200);
        assert_eq!(
            debits.apply(reading(1000, next)).best_available_bytes(),
            Some(1000)
        );
    }

    fn read_space(space: OnDemandSpace) -> (u64, u64) {
        match space {
            OnDemandSpace::Read(space) => (space.total_bytes, space.available_bytes),
            other => panic!("expected a reading, got {other:?}"),
        }
    }

    async fn ask(storage: &StorageCapacity, paths: &[&str]) -> Vec<OnDemandSpace> {
        let asks: Vec<_> = paths
            .iter()
            .map(|path| (PathBuf::from(path), Reach::Exact))
            .collect();
        StorageCapacity::spaces(Some(storage), &asks).await
    }

    fn probes_of(calls: &Mutex<Vec<PathBuf>>, path: &str) -> usize {
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|called| called.as_path() == Path::new(path))
            .count()
    }

    // A probe that counts its calls, and holds a look at `held` until told
    // to go on: each look at it is announced on the returned receiver and
    // waits for one release. Dropping the release sender lets every held
    // look go on.
    fn holding_probe(
        calls: Arc<Mutex<Vec<PathBuf>>>,
        held: &'static str,
    ) -> (
        SharedProbeFn,
        tokio::sync::mpsc::UnboundedReceiver<PathBuf>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        let counting = counting_probe(calls);
        let probe: SharedProbeFn = Arc::new(move |path: &Path| {
            if path == Path::new(held) {
                let _ = entered_tx.send(path.to_path_buf());
                let _ = release_rx.lock().unwrap().recv();
            }
            counting(path)
        });
        (probe, entered_rx, release_tx)
    }

    // A root with a current reading is answered from it, with no look; a
    // root whose look fails is looked at on demand, as any folder, and
    // reports the look's own reason. A failed look is not kept: each ask
    // looks again.
    #[tokio::test(start_paused = true)]
    async fn a_root_is_answered_from_its_reading_without_a_look() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let probe = counting_probe(calls.clone());
        let storage = StorageCapacity::with_probe(roots(), NEVER, Arc::clone(&probe));
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        for _ in 0..3 {
            let answers = ask(&storage, &["/data", "/intermediate", "/unmounted"]).await;
            assert_eq!(read_space(answers[0].clone()), (1000, 5));
            assert_eq!(read_space(answers[1].clone()), (1000, 13));
            let OnDemandSpace::Failed(error) = &answers[2] else {
                panic!("a root whose look failed reports why");
            };
            assert_eq!(
                *error,
                io::Error::from(io::ErrorKind::NotFound).to_string(),
                "the failure is the probe's own reason"
            );
        }
        assert_eq!(probes_of(&calls, "/data"), 1, "the root's own look alone");
        assert_eq!(probes_of(&calls, "/intermediate"), 1);
        assert_eq!(
            probes_of(&calls, "/unmounted"),
            4,
            "the root's look, and one look on demand for each ask: a failure is not kept"
        );
    }

    // Before a root's first look (start-up disk work still running) an ask
    // is answered by a look made on demand, never left blank.
    #[tokio::test(start_paused = true)]
    async fn a_root_not_read_yet_is_looked_at_on_demand() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe_after(
            roots(),
            NEVER,
            counting_probe(calls.clone()),
            StartupDiskGate::closed(),
        );
        let answers = ask(&storage, &["/data", "/data"]).await;
        assert_eq!(read_space(answers[0].clone()), (1000, 5));
        assert_eq!(read_space(answers[1].clone()), (1000, 5));
        assert_eq!(probes_of(&calls, "/data"), 1, "one look for both asks");
        assert_eq!(storage.slot(StorageRoot::Data).lock().completed_probes, 0);
    }

    // A failed look after a good one keeps the last good reading for
    // admission, flagged stale, but an asker is told the look failed and
    // why; the next good look answers again.
    #[tokio::test(start_paused = true)]
    async fn a_failing_root_reports_why_while_its_reading_is_held_stale() {
        let failing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe: SharedProbeFn = {
            let failing = Arc::clone(&failing);
            Arc::new(move |path: &Path| {
                if path == Path::new("/data") && failing.load(Ordering::SeqCst) {
                    return Err(DiskProbeError::Io(io::Error::from(
                        io::ErrorKind::PermissionDenied,
                    )));
                }
                Ok(DiskSpace {
                    total_bytes: 1000,
                    available_bytes: 600,
                })
            })
        };
        let storage = StorageCapacity::with_probe(roots(), NEVER, probe);
        storage.wait_for_probes(StorageRoot::Data, 1);
        assert_eq!(
            read_space(ask(&storage, &["/data"]).await.remove(0)),
            (1000, 600)
        );

        failing.store(true, Ordering::SeqCst);
        storage.request_refresh(StorageRoot::Data);
        storage.wait_for_probes(StorageRoot::Data, 2);
        let Capacity::Known(held) = storage.current(StorageRoot::Data) else {
            panic!("the last good reading is held");
        };
        assert!(held.stale);
        let OnDemandSpace::Failed(error) = ask(&storage, &["/data"]).await.remove(0) else {
            panic!("a failing look is reported");
        };
        assert_eq!(
            error,
            io::Error::from(io::ErrorKind::PermissionDenied).to_string()
        );

        failing.store(false, Ordering::SeqCst);
        storage.request_refresh(StorageRoot::Data);
        storage.wait_for_probes(StorageRoot::Data, 3);
        assert_eq!(
            read_space(ask(&storage, &["/data"]).await.remove(0)),
            (1000, 600)
        );
    }

    // A retarget moves the root's answer to its new directory; the old
    // directory is then a folder like any other, looked at on demand.
    #[tokio::test(start_paused = true)]
    async fn space_follows_a_retarget() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(roots(), NEVER, counting_probe(calls.clone()));
        storage.wait_for_root_read(StorageRoot::Data, Path::new("/data"));
        storage.retarget(StorageRoot::Data, PathBuf::from("/moved/data"));
        storage.wait_for_root_read(StorageRoot::Data, Path::new("/moved/data"));
        let answers = ask(&storage, &["/moved/data", "/data"]).await;
        assert_eq!(read_space(answers[0].clone()), (1000, 11));
        assert_eq!(read_space(answers[1].clone()), (1000, 5));
        assert_eq!(probes_of(&calls, "/moved/data"), 1, "the root's look alone");
        assert_eq!(
            probes_of(&calls, "/data"),
            2,
            "its root's, then one on demand"
        );
    }

    // A folder is looked at only when asked after with no current look:
    // many asks within the interval share one look, and a look an interval
    // old is never served; the next ask looks again.
    #[tokio::test(start_paused = true)]
    async fn a_folder_is_looked_at_once_per_interval_however_often_asked() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let interval = Duration::from_secs(60);
        let storage = StorageCapacity::with_probe(roots(), interval, counting_probe(calls.clone()));
        storage.stop_clock_for_test();
        for _ in 0..100 {
            assert_eq!(
                read_space(ask(&storage, &["/folders/tv"]).await.remove(0)),
                (1000, 11)
            );
        }
        assert_eq!(probes_of(&calls, "/folders/tv"), 1);

        storage.advance_clock(interval);
        assert_eq!(
            read_space(ask(&storage, &["/folders/tv"]).await.remove(0)),
            (1000, 11)
        );
        assert_eq!(
            probes_of(&calls, "/folders/tv"),
            2,
            "an old look is not served"
        );
        assert_eq!(storage.held_paths(), vec![PathBuf::from("/folders/tv")]);
    }

    // Many askers of one path whose look is out share that look: one probe
    // however many ask, and every one of them is answered by it.
    #[tokio::test(start_paused = true)]
    async fn concurrent_askers_of_one_path_share_one_look() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (probe, mut entered, release) = holding_probe(calls.clone(), "/folders/tv");
        let storage = Arc::new(StorageCapacity::with_probe(roots(), NEVER, probe));
        let first = tokio::spawn({
            let storage = Arc::clone(&storage);
            async move { ask(&storage, &["/folders/tv"]).await.remove(0) }
        });
        entered.recv().await.expect("the look began");
        let mut askers = Vec::new();
        for _ in 0..50 {
            let storage = Arc::clone(&storage);
            askers.push(tokio::spawn(async move {
                ask(&storage, &["/folders/tv"]).await.remove(0)
            }));
        }
        // An asker that began before the look returns shares it; one that
        // began after is answered by it as current. Either way, no second
        // look.
        release.send(()).unwrap();
        assert_eq!(read_space(first.await.unwrap()), (1000, 11));
        for asker in askers {
            assert_eq!(read_space(asker.await.unwrap()), (1000, 11));
        }
        assert_eq!(probes_of(&calls, "/folders/tv"), 1);
    }

    // A look held on one folder delays no other folder: an ask for another
    // is answered while it is out, and an ask for both answers the other
    // and reports the held one as not answering once the limit passes.
    #[tokio::test(start_paused = true)]
    async fn a_held_look_delays_only_its_own_path() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (probe, mut entered, release) = holding_probe(calls.clone(), "/folders/slow");
        let storage = Arc::new(StorageCapacity::with_probe(roots(), NEVER, probe));
        let both = tokio::spawn({
            let storage = Arc::clone(&storage);
            async move { ask(&storage, &["/folders/slow", "/folders/fast"]).await }
        });
        entered.recv().await.expect("the slow look began");
        // The other folder answers while the slow look is out.
        assert_eq!(
            read_space(ask(&storage, &["/folders/fast"]).await.remove(0)),
            (1000, 13)
        );
        tokio::time::advance(ON_DEMAND_TIMEOUT).await;
        let answers = both.await.unwrap();
        assert!(matches!(answers[0], OnDemandSpace::TimedOut));
        assert_eq!(read_space(answers[1].clone()), (1000, 13));
        assert_eq!(
            probes_of(&calls, "/folders/fast"),
            1,
            "shared, not repeated"
        );
        drop(release);
    }

    // The paths held are bounded: past the cap, a new path drops the one
    // asked after longest ago that has no look out.
    #[tokio::test(start_paused = true)]
    async fn held_paths_are_bounded() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(roots(), NEVER, counting_probe(calls));
        for n in 0..=PATH_CACHE_MAX {
            let _ = ask(&storage, &[&format!("/asked/{n}")]).await;
        }
        let held = storage.held_paths();
        assert_eq!(held.len(), PATH_CACHE_MAX);
        assert!(!held.contains(&PathBuf::from("/asked/0")), "oldest dropped");
    }

    // Devices for the same-filesystem tests, on directories no other test
    // names.
    fn device_layout(tag: &str) -> ([PathBuf; 3], PathBuf, PathBuf) {
        let base = PathBuf::from(format!("/device-layout-{tag}"));
        let data = base.join("data");
        let intermediate = base.join("intermediate");
        let complete = base.join("complete");
        let movies = base.join("movies");
        let beside = base.join("beside");
        crate::runtime::fs::test_devices::assign(&data, 1);
        crate::runtime::fs::test_devices::assign(&intermediate, 1);
        crate::runtime::fs::test_devices::assign(&complete, 2);
        crate::runtime::fs::test_devices::assign(&movies, 3);
        crate::runtime::fs::test_devices::assign(&beside, 1);
        ([data, intermediate, complete], movies, beside)
    }

    fn staging_choice(one_device: bool, root: &Path) -> Option<PathBuf> {
        crate::pipeline::queue::staging_dir_on(one_device, root, crate::jobs::ids::JobId(7))
    }

    // The staging choice from the owner's devices is the one made by
    // reading both devices on the spot: over one device, two devices, a
    // category folder, and a destination under the complete root that is
    // no configured folder. A root's device is its latest look's, with no
    // look; any other is one look, and a current one is not looked at again.
    #[tokio::test(start_paused = true)]
    async fn one_filesystem_is_answered_as_a_look_on_the_spot_would() {
        let ([data, intermediate, complete], movies, beside) = device_layout("cached");
        let elsewhere = complete.join("elsewhere");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(
            [data.clone(), intermediate.clone(), complete.clone()],
            NEVER,
            counting_probe(calls),
        );
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        let targets = [complete.clone(), movies.clone(), beside.clone(), elsewhere];
        let base: Vec<_> = targets
            .iter()
            .map(|root| staging_choice(same_device_now(&intermediate, root), root))
            .collect();
        let looks = DISK_LOOKS.load(Ordering::Relaxed);
        let mut owned = Vec::new();
        for root in &targets {
            let one = storage.same_filesystem(&intermediate, root).await;
            owned.push(staging_choice(one, root));
        }
        assert_eq!(owned, base);
        assert_eq!(
            owned,
            vec![
                Some(complete.join(".weaver-staging").join("7")),
                Some(movies.join(".weaver-staging").join("7")),
                None,
                Some(targets[3].join(".weaver-staging").join("7")),
            ]
        );
        // The device is answered before its look reads the rest; the count
        // is taken once every look is done.
        storage.looks_returned().await;
        assert_eq!(
            DISK_LOOKS.load(Ordering::Relaxed),
            looks + 3 * 2,
            "one look for each folder that is not a root, its device and its kind; none for the roots"
        );
        for root in &targets {
            let _ = storage.same_filesystem(&intermediate, root).await;
        }
        storage.looks_returned().await;
        assert_eq!(
            DISK_LOOKS.load(Ordering::Relaxed),
            looks + 3 * 2,
            "current looks are not repeated"
        );
    }

    // With start-up disk work still running no root has looked, and the
    // devices are read on demand at once, as a look on the spot read them:
    // a job then stages on a destination on another device.
    #[tokio::test(start_paused = true)]
    async fn one_filesystem_is_answered_before_the_roots_look() {
        let ([data, intermediate, complete], movies, beside) = device_layout("gate");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe_after(
            [data, intermediate.clone(), complete.clone()],
            NEVER,
            counting_probe(calls),
            StartupDiskGate::closed(),
        );
        assert!(!storage.same_filesystem(&intermediate, &complete).await);
        assert!(!storage.same_filesystem(&intermediate, &movies).await);
        assert!(storage.same_filesystem(&intermediate, &beside).await);
        assert_eq!(
            staging_choice(false, &complete),
            Some(complete.join(".weaver-staging").join("7"))
        );
        for root in StorageRoot::ALL {
            assert_eq!(storage.slot(root).lock().completed_probes, 0);
        }
    }

    // A device read of the destination that does not answer within the
    // limit gives the safe answer, one filesystem, and delays no other
    // destination's answer.
    #[tokio::test(start_paused = true)]
    async fn one_filesystem_is_the_answer_when_a_look_does_not_return() {
        let ([data, intermediate, complete], movies, _) = device_layout("held");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let shows = PathBuf::from("/device-layout-held/shows");
        crate::runtime::fs::test_devices::assign(&shows, 4);
        let (release, mut entered) = crate::runtime::fs::test_devices::hold(&shows);
        let storage = Arc::new(StorageCapacity::with_probe(
            [data, intermediate.clone(), complete],
            NEVER,
            counting_probe(calls),
        ));
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        let held = tokio::spawn({
            let storage = Arc::clone(&storage);
            let intermediate = intermediate.clone();
            async move { storage.same_filesystem(&intermediate, &shows).await }
        });
        entered.recv().await.expect("the look began");
        assert!(
            !storage.same_filesystem(&intermediate, &movies).await,
            "another destination is answered while the look is out"
        );
        tokio::time::advance(ON_DEMAND_TIMEOUT).await;
        assert!(held.await.unwrap(), "the safe answer");
        drop(release);
    }

    // A typed folder is answered by a current reading, else by one look at
    // the folder or, when it is not made yet, at the nearest folder above
    // it that exists.
    #[tokio::test(start_paused = true)]
    async fn path_space_uses_a_current_reading_else_looks_once() {
        let dir = tempfile::tempdir().expect("temp dir");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = StorageCapacity::with_probe(roots(), NEVER, counting_probe(calls.clone()));
        storage.wait_for_probes(StorageRoot::Data, 1);
        assert_eq!(
            read_space(StorageCapacity::path_space(Some(&storage), PathBuf::from("/data")).await),
            (1000, 5)
        );
        assert_eq!(probes_of(&calls, "/data"), 1, "answered by the root");

        let real = StorageCapacity::with_probe(roots(), NEVER, Arc::new(probe_disk_space));
        let missing = dir.path().join("not/made/yet");
        let OnDemandSpace::Read(space) = StorageCapacity::path_space(Some(&real), missing).await
        else {
            panic!("the nearest existing folder's disk");
        };
        let direct = probe_disk_space(dir.path()).expect("direct reading");
        assert_eq!(space.total_bytes, direct.total_bytes);
    }

    // A device is answered as soon as it is read: a look whose free space
    // does not return holds no asker of the device.
    #[tokio::test(start_paused = true)]
    async fn a_device_is_answered_while_its_free_space_is_still_read() {
        let ([data, intermediate, complete], movies, _) = device_layout("space-held");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (probe, mut entered, release) =
            holding_probe(calls, "/device-layout-space-held/movies");
        let storage =
            StorageCapacity::with_probe([data, intermediate.clone(), complete], NEVER, probe);
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        assert!(
            !storage.same_filesystem(&intermediate, &movies).await,
            "two devices, told without the free space"
        );
        entered
            .recv()
            .await
            .expect("the free-space read is still out");
        drop(release);
    }

    // A reading's age counts from when it was read, not from when its look
    // began: a look slower than the interval is current when it returns,
    // and the next ask is answered from it.
    #[tokio::test(start_paused = true)]
    async fn a_slow_look_is_current_when_it_returns() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let interval = Duration::from_secs(5);
        let (probe, mut entered, release) = holding_probe(calls.clone(), "/folders/slow");
        let storage = Arc::new(StorageCapacity::with_probe(roots(), interval, probe));
        storage.stop_clock_for_test();
        let first = tokio::spawn({
            let storage = Arc::clone(&storage);
            async move { ask(&storage, &["/folders/slow"]).await.remove(0) }
        });
        entered.recv().await.expect("the look began");
        storage.advance_clock(interval * 2);
        release.send(()).unwrap();
        assert_eq!(read_space(first.await.unwrap()), (1000, 13));
        assert_eq!(
            read_space(ask(&storage, &["/folders/slow"]).await.remove(0)),
            (1000, 13)
        );
        assert_eq!(
            probes_of(&calls, "/folders/slow"),
            1,
            "the look that returned is current"
        );
    }

    // A folder looked at before it was made answers once it is made: the
    // failed look is not kept as an answer.
    #[tokio::test(start_paused = true)]
    async fn a_folder_made_after_a_failed_look_answers_on_the_next_ask() {
        let dir = tempfile::tempdir().expect("temp dir");
        let later = dir.path().join("later");
        let storage = StorageCapacity::with_probe(roots(), NEVER, Arc::new(probe_disk_space));
        let asks = [(later.clone(), Reach::Exact)];
        assert!(matches!(
            StorageCapacity::spaces(Some(&storage), &asks).await[0],
            OnDemandSpace::Failed(_)
        ));
        std::fs::create_dir(&later).unwrap();
        let OnDemandSpace::Read(space) = StorageCapacity::spaces(Some(&storage), &asks)
            .await
            .remove(0)
        else {
            panic!("the folder made since is read");
        };
        let direct = probe_disk_space(&later).expect("direct reading");
        assert_eq!(space.total_bytes, direct.total_bytes);
    }

    // A path that cannot be reached for any reason, not only one not made
    // yet, answers the disk of the nearest folder above it that exists, as
    // a typed folder always did: here, a path through a file. The path's
    // own reading still reports why it failed.
    #[tokio::test(start_paused = true)]
    async fn a_path_that_cannot_be_reached_answers_its_nearest_existing_folder() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file = dir.path().join("file");
        std::fs::write(&file, b"x").unwrap();
        let through = file.join("sub");
        let error = probe_disk_space(&through).expect_err("a path through a file");
        assert!(!error.is_not_found(), "{error}");
        let storage = StorageCapacity::with_probe(roots(), NEVER, Arc::new(probe_disk_space));
        let OnDemandSpace::Read(space) =
            StorageCapacity::path_space(Some(&storage), through.clone()).await
        else {
            panic!("the nearest existing folder's disk");
        };
        let direct = probe_disk_space(dir.path()).expect("direct reading");
        assert_eq!(space.total_bytes, direct.total_bytes);
        assert!(matches!(
            StorageCapacity::spaces(Some(&storage), &[(through, Reach::Exact)]).await[0],
            OnDemandSpace::Failed(_)
        ));
    }

    // The kind of storage is read by the owner, a root's on its own thread
    // and a folder's in its look, and answered from what is held with no
    // look of its own.
    #[tokio::test(start_paused = true)]
    async fn the_kind_of_storage_is_answered_from_what_is_held() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("root");
        let folder = dir.path().join("folder");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&folder).unwrap();
        let storage = StorageCapacity::with_probe(
            [
                root.clone(),
                PathBuf::from("/intermediate"),
                PathBuf::from("/unmounted"),
            ],
            NEVER,
            Arc::new(probe_disk_space),
        );
        storage.wait_for_probes(StorageRoot::Data, 1);
        let looks = DISK_LOOKS.load(Ordering::Relaxed);
        assert_eq!(storage.kind(&root), storage_kind::storage_kind(&root));
        assert_eq!(
            storage.kind(&folder),
            StorageKind::Unknown,
            "nothing held for it yet"
        );
        assert_eq!(DISK_LOOKS.load(Ordering::Relaxed), looks, "no look");

        let _ = StorageCapacity::path_space(Some(&storage), folder.clone()).await;
        let looks = DISK_LOOKS.load(Ordering::Relaxed);
        assert_eq!(storage.kind(&folder), storage_kind::storage_kind(&folder));
        assert_eq!(DISK_LOOKS.load(Ordering::Relaxed), looks, "no look");
    }

    // A folder's storage class and filesystem are read by a look of the
    // owner's, as a direct read finds them, and answered from while
    // current. Nothing read at all is not kept.
    #[tokio::test(start_paused = true)]
    async fn a_classification_is_read_by_the_owner_and_held() {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage = StorageCapacity::with_probe(roots(), NEVER, Arc::new(probe_disk_space));
        let paths = [dir.path().to_path_buf()];
        let direct = crate::runtime::system_probe::classify_storage(dir.path());
        let kept = !(direct.0 == StorageClass::Unknown
            && direct.1 == FilesystemType::Unknown(String::new()));
        let looks = DISK_LOOKS.load(Ordering::Relaxed);
        assert_eq!(
            storage.classifications(&paths).await,
            vec![Some(direct.clone())]
        );
        assert_eq!(DISK_LOOKS.load(Ordering::Relaxed), looks + 1);
        assert_eq!(storage.classifications(&paths).await, vec![Some(direct)]);
        assert_eq!(
            DISK_LOOKS.load(Ordering::Relaxed),
            looks + if kept { 1 } else { 2 },
            "a current classification is not read again"
        );
    }

    fn known(available_bytes: u64, sampled_at: Instant, stale: bool) -> Capacity {
        Capacity::Known(CapacityReading {
            available_bytes,
            total_bytes: 10_000,
            sampled_at,
            stale,
        })
    }

    // A failed probe republishes the last good reading under its old time.
    // What landed since that reading is still taken from it, at each failed
    // interval, until a good probe reads the disk again.
    #[test]
    fn a_failed_probe_keeps_counting_what_landed_since_the_last_good_reading() {
        let ledger = DiskLedger::sampled_for_test();
        let taken = Instant::now();
        let good = known(1000, taken, false);
        let mark = ledger.mark();
        ledger.published(mark, good);
        ledger.landed(0, 300);
        let held = known(1000, taken, true);
        for _ in 0..3 {
            let mark = ledger.mark();
            ledger.published(mark, held);
            assert_eq!(
                ledger.free(held).map(|free| free.bytes),
                Some(700),
                "the bytes landed since the good reading stay counted"
            );
        }
        let mark = ledger.mark();
        let next = known(700, taken + Duration::from_secs(5), false);
        ledger.published(mark, next);
        assert_eq!(ledger.free(next).map(|free| free.bytes), Some(700));
    }

    // The sampler publishes the reading and then its mark. A reader between
    // the two must not move the baseline past the mark: bytes that landed
    // while the probe ran are ones the reading may not have seen.
    #[test]
    fn a_reader_between_a_reading_and_its_mark_does_not_lose_what_landed_during_the_probe() {
        let ledger = DiskLedger::sampled_for_test();
        let taken = Instant::now();
        let first = known(1000, taken, false);
        let mark = ledger.mark();
        ledger.published(mark, first);
        let mark = ledger.mark();
        ledger.landed(0, 200);
        let next = known(1000, taken + Duration::from_secs(5), false);
        assert_eq!(
            ledger.free(next).map(|free| free.bytes),
            Some(800),
            "a reader before the mark counts from the older baseline"
        );
        ledger.published(mark, next);
        assert_eq!(
            ledger.free(next).map(|free| free.bytes),
            Some(800),
            "what landed during the probe is still taken from the reading"
        );
    }
}
