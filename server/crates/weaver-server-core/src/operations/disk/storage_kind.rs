// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What kind of storage a directory lies on: a solid-state disk, a spinning
// one, or a share over the network. Read from the filesystem the directory
// is on and, for a local disk, from what the OS says of the disk behind it:
// on Linux whether its block device rotates, on macOS whether the disk is
// solid state, on Windows whether it incurs a seek penalty. What cannot be
// read is unknown, and is written to as a spinning disk would be.
//
// This file is the one place that reads it. The disk owner,
// [`super::StorageCapacity`], reads a root's kind on its own thread and a
// folder's in its look, the volumes read a volume's kind when they first
// name it, and the system report asks here too. None of them is on the
// async runtime or on a write's path. What the OS says of a disk is kept
// per device once it answered, so each disk is asked once; a disk that did
// not answer is asked again next time.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, PoisonError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageKind {
    Solid,
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
    let look = look(existing);
    kind_from(look.network, || cached_rotates(look.disk, look.rotates))
}

// What the OS says of the storage behind a path.
struct Look<R> {
    // The filesystem is a share over the network.
    network: bool,
    // The disk, as the OS names it, to keep its answer by; `None` when it
    // has no name to keep it by, and is asked each time.
    disk: Option<String>,
    // Ask whether the disk rotates: `None` when it does not say.
    rotates: R,
}

// The kind, from whether the filesystem is a share and, for one that is
// not, whether its disk rotates. The disk is not asked about a share.
fn kind_from(network: bool, rotates: impl FnOnce() -> Option<bool>) -> StorageKind {
    if network {
        return StorageKind::Network;
    }
    match rotates() {
        Some(true) => StorageKind::Rotational,
        Some(false) => StorageKind::Solid,
        None => StorageKind::Unknown,
    }
}

// The disks that answered, by name, and what they said.
static ANSWERED: Mutex<Option<HashMap<String, bool>>> = Mutex::new(None);

// Whether `disk` rotates: its kept answer, or `ask` once it gives one.
fn cached_rotates(disk: Option<String>, ask: impl FnOnce() -> Option<bool>) -> Option<bool> {
    let lock = || ANSWERED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(disk) = &disk
        && let Some(rotates) = lock().as_ref().and_then(|answered| answered.get(disk))
    {
        return Some(*rotates);
    }
    // Asked outside the lock: an answer may take a moment.
    let rotates = ask()?;
    if let Some(disk) = disk {
        lock()
            .get_or_insert_with(HashMap::new)
            .insert(disk, rotates);
    }
    Some(rotates)
}

// Filesystem magic numbers `statfs` reports for shares: NFS, SMB, CIFS,
// SMB2 and FUSE, which network filesystems such as sshfs and rclone mount
// through.
#[cfg(target_os = "linux")]
const NETWORK_MAGIC: [u64; 5] = [0x6969, 0x517B, 0xFF53_4D42, 0xFE53_4D42, 0x6573_5546];

#[cfg(target_os = "linux")]
fn look(path: &Path) -> Look<impl FnOnce() -> Option<bool>> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let unknown = |network| Look {
        network,
        disk: None,
        rotates: None::<u64>,
    };
    let looked = (|| {
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: `statfs` fills a zeroed `libc::statfs` for a valid C string
        // path; the return code is checked before any field is read.
        let magic = unsafe {
            let mut stat: libc::statfs = std::mem::zeroed();
            if libc::statfs(c_path.as_ptr(), &mut stat) != 0 {
                return None;
            }
            (stat.f_type as u64) & 0xFFFF_FFFF
        };
        if NETWORK_MAGIC.contains(&magic) {
            return Some(unknown(true));
        }
        let dev = std::fs::metadata(path).ok()?.dev();
        Some(Look {
            network: false,
            disk: Some(format!("dev:{dev}")),
            rotates: Some(dev),
        })
    })()
    .unwrap_or_else(|| unknown(false));
    Look {
        network: looked.network,
        disk: looked.disk,
        rotates: move || looked.rotates.and_then(rotational),
    }
}

// Whether the block device `dev` rotates, from sysfs. A partition has no
// queue of its own: its disk's is read.
#[cfg(target_os = "linux")]
fn rotational(dev: u64) -> Option<bool> {
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    let device = std::fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")).ok()?;
    let read = |dir: &Path| std::fs::read_to_string(dir.join("queue/rotational")).ok();
    let flag = read(&device).or_else(|| device.parent().and_then(read));
    rotational_flag(flag.as_deref()?)
}

// What sysfs's `queue/rotational` says.
#[cfg(any(target_os = "linux", test))]
fn rotational_flag(flag: &str) -> Option<bool> {
    match flag.trim() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

// The filesystem's own record of the mount `path` lies on.
#[cfg(target_vendor = "apple")]
pub(super) fn mount_stat(path: &Path) -> Option<libc::statfs> {
    use std::os::unix::ffi::OsStrExt;

    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statfs` fills a zeroed `libc::statfs` for a valid C string
    // path; the return code is checked before the record is handed out.
    unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        (libc::statfs(c_path.as_ptr(), &mut stat) == 0).then_some(stat)
    }
}

#[cfg(target_vendor = "apple")]
fn look(path: &Path) -> Look<impl FnOnce() -> Option<bool>> {
    let stat = mount_stat(path);
    let text = |field: &[libc::c_char]| {
        // SAFETY: the field is a NUL-terminated array `statfs` filled.
        unsafe { std::ffi::CStr::from_ptr(field.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    };
    let name = stat.as_ref().map(|stat| text(&stat.f_fstypename));
    let network = matches!(
        name.as_deref(),
        Some("nfs" | "smbfs" | "afpfs" | "webdav" | "macfuse" | "osxfuse")
    );
    // The device the volume is mounted from, such as `/dev/disk3s5`.
    let from = stat
        .as_ref()
        .map(|stat| text(&stat.f_mntfromname))
        .filter(|from| from.starts_with("/dev/"));
    Look {
        network,
        disk: from.clone(),
        rotates: move || {
            let from = from?;
            let said = std::process::Command::new("diskutil")
                .arg("info")
                .arg(&from)
                .output()
                .ok()?;
            said.status
                .success()
                .then(|| solid_state_said(&String::from_utf8_lossy(&said.stdout)))?
                .map(|solid| !solid)
        },
    }
}

// What `diskutil info` says in its "Solid State" line: whether the disk the
// volume lives on is solid state.
#[cfg(any(target_vendor = "apple", test))]
pub(crate) fn solid_state_said(info: &str) -> Option<bool> {
    let line = info
        .lines()
        .map(str::trim_start)
        .find(|line| line.starts_with("Solid State:"))?;
    match line["Solid State:".len()..].trim() {
        "Yes" => Some(true),
        "No" => Some(false),
        _ => None,
    }
}

// The root of the volume `path` lies on, such as `D:\`, NUL-terminated.
#[cfg(windows)]
pub(super) fn volume_root(path: &Path) -> Option<[u16; 261]> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetVolumePathNameW;

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut root = [0u16; 261];
    // SAFETY: `wide` is NUL-terminated and `root` is as long as the length
    // passed; the return code is checked before `root` is handed out.
    let found = unsafe { GetVolumePathNameW(wide.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
    (found != 0).then_some(root)
}

#[cfg(windows)]
fn look(path: &Path) -> Look<impl FnOnce() -> Option<bool>> {
    use windows_sys::Win32::Storage::FileSystem::GetDriveTypeW;

    // What `GetDriveTypeW` returns for a network drive.
    const DRIVE_REMOTE: u32 = 4;
    let root = volume_root(path);
    // SAFETY: `root` holds the NUL-terminated volume path just written.
    let network = root
        .as_ref()
        .is_some_and(|root| unsafe { GetDriveTypeW(root.as_ptr()) } == DRIVE_REMOTE);
    let letter = root.as_ref().and_then(|root| drive_letter(root));
    Look {
        network,
        disk: letter.map(|letter| format!("{}:", char::from(letter as u8))),
        rotates: move || seek_penalty(letter?),
    }
}

// The drive letter of a volume root such as `C:\` or `\\?\C:\`; `None` for
// a root that is not a drive's, such as a volume mounted in a folder: the
// drive the folder is on is another volume, and not asked for it.
#[cfg(any(windows, test))]
fn drive_letter(root: &[u16]) -> Option<u16> {
    let verbatim: Vec<u16> = "\\\\?\\".encode_utf16().collect();
    let root = root.strip_prefix(verbatim.as_slice()).unwrap_or(root);
    let end = root
        .iter()
        .position(|&unit| unit == 0)
        .unwrap_or(root.len());
    match &root[..end] {
        [letter, colon] | [letter, colon, _]
            if *colon == u16::from(b':')
                && root[2..end].iter().all(|&unit| unit == u16::from(b'\\'))
                && u8::try_from(*letter).is_ok_and(|c| c.is_ascii_alphabetic()) =>
        {
            Some(*letter)
        }
        _ => None,
    }
}

// Whether the storage device behind drive `letter` incurs a seek penalty,
// as the storage stack answers `StorageDeviceSeekPenaltyProperty`: `None`
// when the device does not answer, as some USB bridges and spanned volumes
// do not.
#[cfg(windows)]
fn seek_penalty(letter: u16) -> Option<bool> {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::{
        DEVICE_SEEK_PENALTY_DESCRIPTOR, IOCTL_STORAGE_QUERY_PROPERTY, PropertyStandardQuery,
        STORAGE_PROPERTY_QUERY, StorageDeviceSeekPenaltyProperty,
    };

    // `X:` as the volume device `\\.\X:`.
    let device: Vec<u16> = "\\\\.\\"
        .encode_utf16()
        .chain([letter, u16::from(b':'), 0])
        .collect();
    // SAFETY: `device` is NUL-terminated; zero desired access is enough for
    // a property query and needs no privilege.
    let handle = unsafe {
        CreateFileW(
            device.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return None;
    }
    let query = STORAGE_PROPERTY_QUERY {
        PropertyId: StorageDeviceSeekPenaltyProperty,
        QueryType: PropertyStandardQuery,
        AdditionalParameters: [0],
    };
    let mut descriptor = DEVICE_SEEK_PENALTY_DESCRIPTOR {
        Version: 0,
        Size: 0,
        IncursSeekPenalty: false,
    };
    let mut returned: u32 = 0;
    // SAFETY: the in and out buffers are sized structs that outlive the call.
    let ok = unsafe {
        DeviceIoControl(
            handle,
            IOCTL_STORAGE_QUERY_PROPERTY,
            (&query as *const STORAGE_PROPERTY_QUERY).cast(),
            std::mem::size_of::<STORAGE_PROPERTY_QUERY>() as u32,
            (&mut descriptor as *mut DEVICE_SEEK_PENALTY_DESCRIPTOR).cast(),
            std::mem::size_of::<DEVICE_SEEK_PENALTY_DESCRIPTOR>() as u32,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    // SAFETY: `handle` came from a successful CreateFileW and is closed once.
    unsafe {
        CloseHandle(handle);
    }
    if ok == 0 || (returned as usize) < std::mem::size_of::<DEVICE_SEEK_PENALTY_DESCRIPTOR>() {
        return None;
    }
    Some(descriptor.IncursSeekPenalty)
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
fn look(_: &Path) -> Look<impl FnOnce() -> Option<bool>> {
    Look {
        network: false,
        disk: None,
        rotates: || None,
    }
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

    #[test]
    fn a_share_is_network_and_its_disk_is_not_asked() {
        let asked = std::cell::Cell::new(false);
        let kind = kind_from(true, || {
            asked.set(true);
            Some(false)
        });
        assert_eq!(kind, StorageKind::Network);
        assert!(!asked.get());
    }

    #[test]
    fn a_local_disk_is_what_it_says_and_unknown_when_it_says_nothing() {
        assert_eq!(kind_from(false, || Some(true)), StorageKind::Rotational);
        assert_eq!(kind_from(false, || Some(false)), StorageKind::Solid);
        assert_eq!(kind_from(false, || None), StorageKind::Unknown);
    }

    #[test]
    fn a_disk_that_answered_is_not_asked_again_and_one_that_did_not_is() {
        let disk = || Some("test:a_disk_that_answered".to_owned());
        assert_eq!(cached_rotates(disk(), || None), None);
        assert_eq!(cached_rotates(disk(), || Some(true)), Some(true));
        assert_eq!(cached_rotates(disk(), || panic!("asked again")), Some(true));
        // A disk with no name to keep it by is asked each time.
        assert_eq!(cached_rotates(None, || Some(false)), Some(false));
        assert_eq!(cached_rotates(None, || Some(true)), Some(true));
    }

    #[test]
    fn the_flags_each_os_gives_read_as_they_say() {
        assert_eq!(rotational_flag("1\n"), Some(true));
        assert_eq!(rotational_flag("0\n"), Some(false));
        assert_eq!(rotational_flag(""), None);
        let info = "   Device Location:           Internal\n   Solid State:               Yes\n";
        assert_eq!(solid_state_said(info), Some(true));
        assert_eq!(
            solid_state_said("   Solid State:               No\n"),
            Some(false)
        );
        assert_eq!(
            solid_state_said("   Solid State:               Info not available\n"),
            None
        );
        assert_eq!(
            solid_state_said("   Protocol:                  USB\n"),
            None
        );
    }

    #[test]
    fn a_volume_root_gives_its_drive_letter_and_a_folder_mount_none() {
        let wide = |text: &str| text.encode_utf16().chain(Some(0)).collect::<Vec<u16>>();
        assert_eq!(drive_letter(&wide("C:\\")), Some(u16::from(b'C')));
        assert_eq!(drive_letter(&wide("\\\\?\\D:\\")), Some(u16::from(b'D')));
        assert_eq!(drive_letter(&wide("\\\\?\\Volume{0000}\\")), None);
        assert_eq!(drive_letter(&wide("C:\\mounted\\")), None);
        assert_eq!(drive_letter(&wide("E:")), Some(u16::from(b'E')));
    }
}
