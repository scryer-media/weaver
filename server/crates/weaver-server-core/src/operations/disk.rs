use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
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
/// (`statvfs` on unix, `GetDiskFreeSpaceExW` on Windows).
///
/// Fails when the path cannot be stat'd (e.g. it does not exist yet), when the
/// filesystem returns an unusable reading, or on unsupported platforms. The
/// error carries the operating-system reason so callers can log it and decide
/// between failing open, holding a stale reading, and refusing.
pub fn probe_disk_space(path: &Path) -> Result<DiskSpace, DiskProbeError> {
    #[cfg(unix)]
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
/// itself has not been created yet. Every other failure is returned as-is so a
/// permission problem or an unmounted filesystem is never masked by an
/// ancestor that lives on a different device.
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
    /// Sample the filesystem behind `path` (or its nearest existing ancestor)
    /// at most once per `ttl`. No probe runs until the first `sample`.
    pub fn new(path: PathBuf, ttl: Duration) -> Self {
        Self::with_probe(path, ttl, Box::new(probe_nearest_disk_space))
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
        self.last_attempt = Some(now);
        match (self.probe)(&self.path) {
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

/// TTL-cached capacity sampler for the configured directory roles.
///
/// `statvfs`/`GetDiskFreeSpaceExW` are cheap but not free, and a metrics scrape
/// can arrive far more often than free space meaningfully changes. The cache
/// keeps a scrape storm from turning into a syscall storm. Nothing here runs on
/// a pipeline path — it is called only from the exporter.
#[derive(Debug)]
pub struct DiskSpaceCollector {
    roles: Vec<(&'static str, PathBuf)>,
    cache: Mutex<Option<(Instant, Vec<DiskSpaceSnapshot>)>>,
}

impl DiskSpaceCollector {
    /// `roles` pairs a stable role label (`data`, `intermediate`, `complete`)
    /// with the directory configured for it.
    pub fn new(roles: Vec<(&'static str, PathBuf)>) -> Self {
        Self {
            roles,
            cache: Mutex::new(None),
        }
    }

    /// Sample every role, re-using the previous result while it is younger than
    /// `ttl`. Roles whose path cannot be stat'd (not created yet, unmounted)
    /// are omitted rather than reported as zero-capacity.
    pub fn sample(&self, ttl: Duration) -> Vec<DiskSpaceSnapshot> {
        let now = Instant::now();
        {
            let cache = self
                .cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some((sampled_at, snapshots)) = cache.as_ref()
                && now.duration_since(*sampled_at) < ttl
            {
                return snapshots.clone();
            }
        }

        let snapshots = self
            .roles
            .iter()
            .filter_map(|(role, path)| {
                disk_space(path).map(|space| DiskSpaceSnapshot {
                    role,
                    path: path.display().to_string(),
                    total_bytes: space.total_bytes,
                    available_bytes: space.available_bytes,
                })
            })
            .collect::<Vec<_>>();

        *self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((now, snapshots.clone()));
        snapshots
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
        let reading = sampler.sample().reading().expect("reading via ancestor");
        assert!(reading.total_bytes > 0);
        assert!(!reading.stale);
    }

    #[test]
    fn collector_reports_a_row_per_stattable_role() {
        let dir = tempfile::tempdir().expect("temp dir");
        let collector = DiskSpaceCollector::new(vec![
            ("data", dir.path().to_path_buf()),
            ("intermediate", dir.path().join("does-not-exist")),
        ]);
        let snapshots = collector.sample(Duration::from_secs(30));
        assert_eq!(snapshots.len(), 1, "unstattable roles are omitted");
        assert_eq!(snapshots[0].role, "data");
        assert!(snapshots[0].total_bytes > 0);
    }

    #[test]
    fn collector_serves_the_cache_within_the_ttl() {
        let dir = tempfile::tempdir().expect("temp dir");
        let collector = DiskSpaceCollector::new(vec![("data", dir.path().to_path_buf())]);
        let first = collector.sample(Duration::from_secs(3600));
        let second = collector.sample(Duration::from_secs(3600));
        assert_eq!(first, second);

        // A zero TTL always re-samples; the shape must stay stable.
        let third = collector.sample(Duration::ZERO);
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].role, "data");
    }
}
