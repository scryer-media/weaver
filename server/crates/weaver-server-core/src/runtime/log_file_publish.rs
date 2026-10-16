use std::io;
use std::path::Path;

// Unlike std::fs::rename, these APIs never replace an occupied destination.
// Both paths are in the same directory; the source is a closed, synced gzip.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn rename_exclusive(source: &Path, destination: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in log path"))?;
    let destination = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in archive path"))?;
    #[cfg(target_os = "linux")]
    let result = {
        // The raw syscall: musl exports no `renameat2` wrapper, and the release
        // binaries link statically against musl.
        // SAFETY: both C strings are NUL-terminated and live for this call.
        unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            ) as i32
        }
    };
    #[cfg(target_os = "macos")]
    let result = {
        unsafe extern "C" {
            fn renamex_np(
                old: *const std::ffi::c_char,
                new: *const std::ffi::c_char,
                flags: u32,
            ) -> i32;
        }
        // SAFETY: both C strings are NUL-terminated and live for this call.
        // RENAME_EXCL = 0x4 prevents replacing any existing destination.
        unsafe { renamex_np(source.as_ptr(), destination.as_ptr(), 0x4) }
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
pub(super) fn rename_exclusive(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    fn wide(path: &Path) -> io::Result<Vec<u16>> {
        let mut value: Vec<_> = path.as_os_str().encode_wide().collect();
        if value.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NUL in log path",
            ));
        }
        value.push(0);
        Ok(value)
    }
    let source = wide(source)?;
    let destination = wide(destination)?;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(source: *const u16, destination: *const u16, flags: u32) -> i32;
    }
    // SAFETY: both UTF-16 buffers are NUL-terminated and live for this call.
    // MOVEFILE_WRITE_THROUGH = 0x8, without MOVEFILE_REPLACE_EXISTING.
    let result = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), 0x8) };
    if result != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn rename_exclusive(_source: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exclusive archive publication unavailable",
    ))
}
