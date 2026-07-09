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

/// Query total/available capacity for the filesystem backing `path`
/// (`statvfs` on unix, `GetDiskFreeSpaceExW` on Windows).
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
