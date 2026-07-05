use std::path::Path;

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

/// Query total/available capacity for the filesystem backing `path` via `statvfs`.
///
/// Returns `None` when the path cannot be stat'd (e.g. it does not exist yet) or on
/// unsupported platforms. Mirrors the free-space check used by the download pipeline.
pub fn disk_space(path: &Path) -> Option<DiskSpace> {
    #[cfg(unix)]
    {
        let path_cstr = std::ffi::CString::new(path.to_str()?.as_bytes()).ok()?;
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

    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}
