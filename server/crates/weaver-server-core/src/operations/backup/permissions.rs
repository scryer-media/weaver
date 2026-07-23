use std::io;
use std::path::Path;

#[cfg(unix)]
pub(crate) fn set_file_owner_only(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let current = std::fs::metadata(path)?.permissions().mode();
    let mode = if current & 0o111 == 0 { 0o600 } else { 0o700 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(unix)]
pub(crate) fn set_directory_owner_only(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(windows)]
pub(crate) fn set_file_owner_only(path: &Path) -> io::Result<()> {
    set_windows_owner_only(path, false)
}

#[cfg(windows)]
pub(crate) fn set_directory_owner_only(path: &Path) -> io::Result<()> {
    set_windows_owner_only(path, true)
}

#[cfg(windows)]
fn set_windows_owner_only(path: &Path, directory: bool) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::ptr;

    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        SetFileSecurityW,
    };

    let mut path_wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if path_wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains an embedded NUL",
        ));
    }
    path_wide.push(0);

    let sddl = if directory {
        "D:P(A;OICI;FA;;;OW)"
    } else {
        "D:P(A;;FA;;;OW)"
    };
    let mut sddl_wide = sddl.encode_utf16().collect::<Vec<_>>();
    sddl_wide.push(0);

    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }

    let applied = unsafe {
        SetFileSecurityW(
            path_wide.as_ptr(),
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            descriptor,
        )
    };
    let result = if applied == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    unsafe {
        LocalFree(descriptor);
    }
    result
}

#[cfg(all(not(unix), not(windows)))]
pub(crate) fn set_file_owner_only(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
pub(crate) fn set_directory_owner_only(_path: &Path) -> io::Result<()> {
    Ok(())
}
