use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

/// Query total/available capacity for the filesystem backing `path`
/// (`statvfs` on unix, `GetDiskFreeSpaceExW` on Windows).
///
/// Returns `None` when the path cannot be stat'd (e.g. it does not exist yet) or on
/// unsupported platforms. Mirrors the free-space check used by the download pipeline.
pub fn disk_space(path: &Path) -> Option<DiskSpace> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;

        let path_cstr = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: `statvfs` fills a zeroed `libc::statvfs` for a valid C string path;
        // we check the return code before reading any fields.
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(path_cstr.as_ptr(), &mut stat) != 0 {
                return None;
            }
            #[allow(clippy::unnecessary_cast)]
            let frsize = stat.f_frsize as u64;
            #[allow(clippy::unnecessary_cast)]
            let total_bytes = (stat.f_blocks as u64).saturating_mul(frsize);
            #[allow(clippy::unnecessary_cast)]
            let available_bytes = (stat.f_bavail as u64).saturating_mul(frsize);
            Some(DiskSpace {
                total_bytes,
                available_bytes,
            })
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return None;
        }
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
            return None;
        }
        Some(DiskSpace {
            total_bytes,
            available_bytes: free_bytes_available,
        })
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        None
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
    use super::*;

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
