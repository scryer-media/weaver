// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What kind of storage a directory lies on: a solid-state disk, a spinning
// one, or a share over the network. Read from the filesystem the directory
// is on and, for a local disk, whether its block device rotates. What
// cannot be read is unknown, and is written to as a spinning disk would be.

use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageKind {
    // Only Linux reads whether a local disk rotates.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Solid,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Rotational,
    Network,
    Unknown,
}

impl StorageKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Solid => "ssd",
            Self::Rotational => "platter",
            Self::Network => "network",
            Self::Unknown => "unknown",
        }
    }
}

// The kind of storage `path` lies on, read at the nearest of its ancestors
// that exists.
pub(crate) fn storage_kind(path: &Path) -> StorageKind {
    let Some(existing) = path.ancestors().find(|at| at.exists()) else {
        return StorageKind::Unknown;
    };
    probe(existing)
}

// Filesystem magic numbers `statfs` reports for shares: NFS, SMB, CIFS,
// SMB2 and FUSE, which network filesystems such as sshfs and rclone mount
// through.
#[cfg(target_os = "linux")]
const NETWORK_MAGIC: [u64; 5] = [0x6969, 0x517B, 0xFF53_4D42, 0xFE53_4D42, 0x6573_5546];

#[cfg(target_os = "linux")]
fn probe(path: &Path) -> StorageKind {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return StorageKind::Unknown;
    };
    // SAFETY: `statfs` fills a zeroed `libc::statfs` for a valid C string
    // path; the return code is checked before any field is read.
    let magic = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::statfs(c_path.as_ptr(), &mut stat) != 0 {
            return StorageKind::Unknown;
        }
        (stat.f_type as u64) & 0xFFFF_FFFF
    };
    if NETWORK_MAGIC.contains(&magic) {
        return StorageKind::Network;
    }
    let Ok(metadata) = std::fs::metadata(path) else {
        return StorageKind::Unknown;
    };
    rotational(metadata.dev())
}

// Whether the block device `dev` rotates, from sysfs. A partition has no
// queue of its own: its disk's is read.
#[cfg(target_os = "linux")]
fn rotational(dev: u64) -> StorageKind {
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    let Ok(device) = std::fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")) else {
        return StorageKind::Unknown;
    };
    let read = |dir: &Path| std::fs::read_to_string(dir.join("queue/rotational")).ok();
    let flag = read(&device).or_else(|| device.parent().and_then(read));
    match flag.as_deref().map(str::trim) {
        Some("0") => StorageKind::Solid,
        Some("1") => StorageKind::Rotational,
        _ => StorageKind::Unknown,
    }
}

#[cfg(target_vendor = "apple")]
fn probe(path: &Path) -> StorageKind {
    use std::os::unix::ffi::OsStrExt;

    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return StorageKind::Unknown;
    };
    // SAFETY: as above; `f_fstypename` is a NUL-terminated array.
    let name = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::statfs(c_path.as_ptr(), &mut stat) != 0 {
            return StorageKind::Unknown;
        }
        std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr())
            .to_string_lossy()
            .into_owned()
    };
    // Whether a local disk rotates needs IOKit: it is unknown.
    match name.as_str() {
        "nfs" | "smbfs" | "afpfs" | "webdav" | "macfuse" | "osxfuse" => StorageKind::Network,
        _ => StorageKind::Unknown,
    }
}

#[cfg(windows)]
fn probe(path: &Path) -> StorageKind {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, GetVolumePathNameW};

    // What `GetDriveTypeW` returns for a network drive.
    const DRIVE_REMOTE: u32 = 4;
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut root = [0u16; 261];
    // SAFETY: `wide` is NUL-terminated and `root` is as long as the length
    // passed; the return code is checked before `root` is read.
    let found = unsafe { GetVolumePathNameW(wide.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
    if found == 0 {
        return StorageKind::Unknown;
    }
    // SAFETY: `root` holds the NUL-terminated volume path just written.
    if unsafe { GetDriveTypeW(root.as_ptr()) } == DRIVE_REMOTE {
        StorageKind::Network
    } else {
        StorageKind::Unknown
    }
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
fn probe(_: &Path) -> StorageKind {
    StorageKind::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_not_yet_made_is_read_at_its_nearest_ancestor() {
        let root = tempfile::tempdir().expect("temp dir");
        assert_eq!(
            storage_kind(&root.path().join("not/yet/made")),
            storage_kind(root.path())
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_partition_is_read_by_its_disk() {
        // Whatever the disk, a local directory is never called a share.
        let root = tempfile::tempdir().expect("temp dir");
        assert_ne!(storage_kind(root.path()), StorageKind::Network);
    }
}
