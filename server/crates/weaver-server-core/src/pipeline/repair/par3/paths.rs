//! Whether an authenticated PAR3 path may become a file on this machine.
//!
//! A set's File and Directory packets carry paths chosen by whoever built the
//! set, authenticated only in the sense that they have not been altered since.
//! Authentication says nothing about intent, so every resolved path is checked
//! here before anything is planned for it. A path that fails is refused and
//! named; it is never quietly rewritten into a safe one, because a set whose
//! own names cannot be honoured is a set weaver has no business reconstructing
//! under different names.

/// Why a resolved path cannot be installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum UnsafePath {
    /// Anchored at a filesystem root rather than at the job's directory.
    Absolute,
    /// An empty component: a leading, trailing or doubled separator.
    EmptyComponent,
    /// A `.` component.
    CurrentDirectory,
    /// A `..` component, which walks out of the job's directory.
    ParentDirectory,
    /// A NUL byte, which no path may carry.
    Nul,
    /// A platform separator or drive letter inside a single component.
    SeparatorInComponent,
    /// A Windows reserved device name, with or without an extension.
    ReservedDeviceName,
    /// Longer than the reconstructed-output identity allows.
    TooLong,
}

impl UnsafePath {
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Absolute => "an absolute path",
            Self::EmptyComponent => "an empty path component",
            Self::CurrentDirectory => "a `.` component",
            Self::ParentDirectory => "a `..` component",
            Self::Nul => "a NUL byte",
            Self::SeparatorInComponent => "a separator inside a component",
            Self::ReservedDeviceName => "a reserved device name",
            Self::TooLong => "more bytes than an output identity allows",
        }
    }
}

impl std::fmt::Display for UnsafePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

/// The longest path weaver will reserve an output identity for. The
/// reconstructed-output record enforces the same bound when it is written, so
/// refusing here keeps the verdict typed instead of surfacing as a database
/// rejection after planning has already begun.
const MAX_PATH_BYTES: usize = 1024;

/// Windows device names, which resolve to a device rather than a file even
/// when a path claims otherwise. They are refused on every platform: a set is
/// not portable if it can only be installed on some of them.
const RESERVED_DEVICE_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Whether one component names a Windows device, ignoring any extension and
/// any trailing dots or spaces, which Windows strips before resolving.
fn is_reserved_device_name(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or(component);
    let stem = stem.trim_end_matches([' ', '.']);
    RESERVED_DEVICE_NAMES
        .iter()
        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
}

/// Check one resolved PAR3 path. `Ok(())` means weaver may create exactly this
/// relative path under the job's working directory.
///
/// PAR3 paths are `/`-separated regardless of platform, so `/` is the only
/// separator a path may use and any other platform separator inside a
/// component is a name that means two different things on two machines.
pub(in crate::pipeline) fn check_install_path(path: &str) -> Result<(), UnsafePath> {
    if path.is_empty() {
        return Err(UnsafePath::EmptyComponent);
    }
    if path.len() > MAX_PATH_BYTES {
        return Err(UnsafePath::TooLong);
    }
    if path.contains('\0') {
        return Err(UnsafePath::Nul);
    }
    if path.starts_with('/') {
        return Err(UnsafePath::Absolute);
    }
    // `C:\names` and `C:/names` are absolute on Windows and a relative name
    // with a colon everywhere else, so the same set would install to two
    // different places. Refuse the ambiguity rather than pick a side.
    if path
        .split('/')
        .next()
        .is_some_and(|first| first.len() >= 2 && first.as_bytes()[1] == b':')
    {
        return Err(UnsafePath::Absolute);
    }
    for component in path.split('/') {
        if component.is_empty() {
            return Err(UnsafePath::EmptyComponent);
        }
        if component == "." {
            return Err(UnsafePath::CurrentDirectory);
        }
        if component == ".." {
            return Err(UnsafePath::ParentDirectory);
        }
        if component.contains(['\\', ':']) {
            return Err(UnsafePath::SeparatorInComponent);
        }
        if is_reserved_device_name(component) {
            return Err(UnsafePath::ReservedDeviceName);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names a well-formed set uses are installed exactly as written,
    /// including the ones that merely look unusual.
    #[test]
    fn ordinary_relative_paths_are_installable() {
        for path in [
            "a.bin",
            "sub/c.bin",
            "deep/nested/tree/leaf.dat",
            "spaces in a name.txt",
            "unicode-\u{00e9}\u{00e8}.bin",
            "console.log",
            "com10.bin",
            "connect/prnt.bin",
        ] {
            assert_eq!(check_install_path(path), Ok(()), "{path}");
        }
    }

    /// Each refusal class is reached by the shape that names it, so a change
    /// to one rule cannot silently take over another's cases.
    #[test]
    fn every_unsafe_shape_is_refused_under_its_own_reason() {
        for (path, expected) in [
            ("", UnsafePath::EmptyComponent),
            ("/etc/passwd", UnsafePath::Absolute),
            ("C:/windows/system32/cmd.exe", UnsafePath::Absolute),
            ("sub//c.bin", UnsafePath::EmptyComponent),
            ("sub/", UnsafePath::EmptyComponent),
            ("./a.bin", UnsafePath::CurrentDirectory),
            ("sub/./a.bin", UnsafePath::CurrentDirectory),
            ("../a.bin", UnsafePath::ParentDirectory),
            ("sub/../../a.bin", UnsafePath::ParentDirectory),
            ("a\0.bin", UnsafePath::Nul),
            ("sub\\c.bin", UnsafePath::SeparatorInComponent),
            ("stream:name", UnsafePath::SeparatorInComponent),
            ("CON", UnsafePath::ReservedDeviceName),
            ("nul", UnsafePath::ReservedDeviceName),
            ("sub/Aux.txt", UnsafePath::ReservedDeviceName),
            ("COM9.bin", UnsafePath::ReservedDeviceName),
            ("LPT1", UnsafePath::ReservedDeviceName),
            ("con .txt", UnsafePath::ReservedDeviceName),
        ] {
            assert_eq!(check_install_path(path), Err(expected), "{path:?}");
        }
    }

    /// A path past the identity bound is refused before anything is reserved
    /// for it, and every reason renders as its own words.
    #[test]
    fn a_path_past_the_identity_bound_is_refused_and_every_reason_is_named() {
        use std::collections::BTreeSet;
        let long = "x".repeat(MAX_PATH_BYTES + 1);
        assert_eq!(check_install_path(&long), Err(UnsafePath::TooLong));
        assert_eq!(check_install_path(&"y".repeat(MAX_PATH_BYTES)), Ok(()));

        let reasons = [
            UnsafePath::Absolute,
            UnsafePath::EmptyComponent,
            UnsafePath::CurrentDirectory,
            UnsafePath::ParentDirectory,
            UnsafePath::Nul,
            UnsafePath::SeparatorInComponent,
            UnsafePath::ReservedDeviceName,
            UnsafePath::TooLong,
        ];
        assert_eq!(
            reasons
                .iter()
                .map(|reason| reason.to_string())
                .collect::<BTreeSet<_>>()
                .len(),
            reasons.len()
        );
    }
}
