// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Sparse marking for the files the graph engine writes with holes in them.
//
// A leaf's file is written at the offsets its articles cover, in whatever
// order the wire delivers them. On Unix a file is sparse the moment a write
// skips a region, so the marker is a no-op that succeeds. On Windows it is
// not: NTFS allocates and zero-fills everything below the high-water mark
// unless the file carries the sparse attribute, which `FSCTL_SET_SPARSE` sets.
//
// The control code applies to the file, not to a write, so it is issued
// immediately after creation and before any `set_len` or write. Marking is an
// allocation optimization: if the filesystem refuses it, the file stays usable
// with ordinary zero-filled holes. Open, resize and write failures still
// propagate through their normal error paths.

use std::fs::File;
use std::io;
use std::path::Path;

// Marks a freshly created file sparse.
//
// Implementors must be callable on a handle that has had nothing written to
// it and no length set — see the module note on ordering.
pub(crate) trait SparseMarker {
    fn mark_sparse(&self, file: &File) -> io::Result<()>;
}

// The platform's own marker.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlatformMarker;

impl SparseMarker for PlatformMarker {
    fn mark_sparse(&self, file: &File) -> io::Result<()> {
        mark_sparse_native(file)
    }
}

// Creates `path` and attempts to mark it sparse before it can hold a hole.
//
// The file is created read/write and **not** truncated: a restart re-marks a
// destination that already holds routed bytes, and truncating it there would
// throw away the coverage the checkpoint just claimed. Truncation is the
// caller's business (holds scratch wants it, member partials must never have
// it).
//
// A marking failure keeps the open file, including any pre-existing bytes.
// Only an actual open failure prevents the caller from using it.
pub(crate) fn create_sparse<M: SparseMarker>(path: &Path, marker: &M) -> io::Result<File> {
    create_sized_with(path, marker, None, makes_holes)
}

// Whether this platform can tell a filesystem that makes holes, and so ever
// gives a file its declared length when it is made. Where it cannot, every
// file grows as it is written. Read by tests, which accept a sized file only
// where one can be made.
#[cfg(test)]
pub(crate) const PRESIZES: bool = cfg!(any(windows, target_os = "linux", target_vendor = "apple"));

// Creates `path` as `create_sparse` does and gives it `length` when its
// filesystem is known to make holes; any other file grows as it is written.
//
// A file set to its length first is written into holes inside it, and its
// end of file does not move with every write at a scattered offset. That is
// only free where the unwritten length takes no space: on a filesystem that
// would fill it with zeroes, or one that cannot be told, the length would be
// space and writes for bytes never downloaded. No space is ever reserved:
// the length is set, never allocated.
//
// On Windows the sparse mark is the test, taken first and the length after
// it: a mount that refuses the mark is not sized. On Linux and macOS it is
// the type of filesystem the open file is on; a share over the network and a
// filesystem not known are not sized.
//
// Only an empty file is sized: one that already holds bytes is a restart's,
// whose length the restore reads as how far its bytes reach.
pub(crate) fn create_sized<M: SparseMarker>(
    path: &Path,
    marker: &M,
    length: Option<u64>,
) -> io::Result<File> {
    create_sized_with(path, marker, length, makes_holes)
}

fn create_sized_with<M: SparseMarker>(
    path: &Path,
    marker: &M,
    length: Option<u64>,
    makes_holes: impl FnOnce(&File) -> bool,
) -> io::Result<File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let marked = marker.mark_sparse(&file).is_ok();
    if !marked {
        crate::runtime::perf_probe::record_value("graph.sparse.mark_failed", 1);
    }
    let Some(length) = length.filter(|length| marked && *length > 0) else {
        return Ok(file);
    };
    // Sizing is an allocation hint like the mark: a file it fails on stays
    // usable and grows by its writes. The filesystem is asked about last,
    // and only for a file that would be sized.
    let sized = file.metadata().and_then(|metadata| match metadata.len() {
        0 if makes_holes(&file) => file.set_len(length),
        _ => Ok(()),
    });
    if sized.is_err() {
        crate::runtime::perf_probe::record_value("graph.sparse.presize_failed", 1);
    }
    Ok(file)
}

// Whether the filesystem `file` is on makes a hole of a length set past the
// bytes written. On Windows a file that took the sparse mark does, and only
// such a file is asked about.
#[cfg(windows)]
fn makes_holes(_file: &File) -> bool {
    true
}

#[cfg(target_os = "linux")]
fn makes_holes(file: &File) -> bool {
    use std::os::fd::AsRawFd;

    // SAFETY: `fstatfs` fills a zeroed `libc::statfs` for an open
    // descriptor; the return code is checked before any field is read.
    let magic = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::fstatfs(file.as_raw_fd(), &mut stat) != 0 {
            return false;
        }
        (stat.f_type as u64) & 0xFFFF_FFFF
    };
    linux_makes_holes(magic)
}

#[cfg(target_vendor = "apple")]
fn makes_holes(file: &File) -> bool {
    use std::os::fd::AsRawFd;

    // SAFETY: as above; `f_fstypename` is a NUL-terminated array.
    let name = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::fstatfs(file.as_raw_fd(), &mut stat) != 0 {
            return false;
        }
        std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr())
            .to_string_lossy()
            .into_owned()
    };
    apple_makes_holes(&name)
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
fn makes_holes(_file: &File) -> bool {
    false
}

// The filesystem magic numbers `fstatfs` reports for the local Linux
// filesystems that keep an unwritten length as a hole: ext2 to ext4, XFS,
// Btrfs, ZFS, F2FS, bcachefs and tmpfs. Every other, a share over the
// network among them, is not known to.
#[cfg(any(test, target_os = "linux"))]
const HOLE_MAGIC: [u64; 7] = [
    0xEF53,
    0x5846_5342,
    0x9123_683E,
    0x2FC1_2FC1,
    0xF2F5_2010,
    0xCA45_1A4E,
    0x0102_1994,
];

#[cfg(any(test, target_os = "linux"))]
fn linux_makes_holes(magic: u64) -> bool {
    HOLE_MAGIC.contains(&magic)
}

// On macOS only APFS keeps an unwritten length as a hole; HFS+ fills it with
// zeroes, and a share is not known.
#[cfg(any(test, target_vendor = "apple"))]
fn apple_makes_holes(filesystem: &str) -> bool {
    filesystem == "apfs"
}

// `FSCTL_SET_SPARSE` with no input buffer, which is the "make it sparse" form
// (the optional `FILE_SET_SPARSE_BUFFER` only exists to turn it back off).
#[cfg(windows)]
fn mark_sparse_native(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let mut returned: u32 = 0;
    let ok = unsafe {
        DeviceIoControl(
            file.as_raw_handle() as _,
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

// Unix files are sparse by writing past a hole, so there is nothing to ask
// for.
#[cfg(not(windows))]
fn mark_sparse_native(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use super::*;

    // A filesystem that refuses the sparse attribute.
    struct Refusing;

    impl SparseMarker for Refusing {
        fn mark_sparse(&self, _file: &File) -> io::Result<()> {
            Err(io::Error::other("sparse marking refused"))
        }
    }

    // A refused mark leaves the file open and its bytes in place: the file
    // takes ordinary zero-filled holes instead.
    #[test]
    fn a_refused_mark_keeps_the_file_and_its_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ember.bin");
        std::fs::write(&path, b"routed").unwrap();

        let mut file = create_sparse(&path, &Refusing).unwrap();
        file.seek(SeekFrom::Start(16)).unwrap();
        file.write_all(b"tail").unwrap();
        file.flush().unwrap();

        let mut bytes = Vec::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut bytes).unwrap();
        let mut expected = b"routed".to_vec();
        expected.resize(16, 0);
        expected.extend_from_slice(b"tail");
        assert_eq!(bytes, expected);
    }

    // The platform's marker on whatever filesystem holds the test: accepted
    // or refused, the file opens with its bytes in place.
    #[test]
    fn the_platform_mark_keeps_the_file_and_its_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ember.bin");
        std::fs::write(&path, b"routed").unwrap();

        let mut file = create_sparse(&path, &PlatformMarker).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"routed");
    }

    // A filesystem that takes the sparse attribute.
    struct Taking;

    impl SparseMarker for Taking {
        fn mark_sparse(&self, _file: &File) -> io::Result<()> {
            Ok(())
        }
    }

    fn sized(
        marker: &impl SparseMarker,
        held: Option<&[u8]>,
        length: Option<u64>,
        makes_holes: bool,
    ) -> Vec<u8> {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ember.bin");
        if let Some(held) = held {
            std::fs::write(&path, held).unwrap();
        }
        let mut file = create_sized_with(&path, marker, length, |_| makes_holes).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        bytes
    }

    // On a filesystem that makes holes, a new file the mark took reaches its
    // declared length from the start, every byte of it a hole.
    #[test]
    fn a_marked_new_file_takes_its_declared_length() {
        assert_eq!(sized(&Taking, None, Some(4096), true), vec![0; 4096]);
    }

    // A mount that refuses the mark gets no length: it would be zero-filled
    // space for bytes never written.
    #[test]
    fn a_refused_mark_leaves_the_file_unsized() {
        assert!(sized(&Refusing, None, Some(4096), true).is_empty());
    }

    // On a filesystem not known to make holes, the file stays as long as
    // what is written to it.
    #[test]
    fn a_filesystem_not_known_to_make_holes_leaves_the_file_unsized() {
        assert!(sized(&Taking, None, Some(4096), false).is_empty());
    }

    // A file that already holds bytes, a restart's, keeps its length: the
    // restore reads it as how far its bytes reach.
    #[test]
    fn a_file_holding_bytes_keeps_its_length() {
        assert_eq!(sized(&Taking, Some(b"routed"), Some(4096), true), b"routed");
    }

    // No declared length, nothing to size.
    #[test]
    fn a_file_with_no_declared_length_stays_unsized() {
        assert!(sized(&Taking, None, None, true).is_empty());
    }

    // The filesystem is asked about only for a file that would be sized:
    // never for one with no length, a refused mark, or bytes already in it.
    #[test]
    fn the_filesystem_is_asked_about_only_for_a_file_that_would_be_sized() {
        let asked = |marker_takes: bool, held: Option<&[u8]>, length: Option<u64>| {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("ember.bin");
            if let Some(held) = held {
                std::fs::write(&path, held).unwrap();
            }
            let asked = std::cell::Cell::new(false);
            let makes_holes = |_: &File| {
                asked.set(true);
                true
            };
            if marker_takes {
                create_sized_with(&path, &Taking, length, makes_holes).unwrap();
            } else {
                create_sized_with(&path, &Refusing, length, makes_holes).unwrap();
            }
            asked.get()
        };
        assert!(asked(true, None, Some(4096)));
        assert!(!asked(true, None, None));
        assert!(!asked(true, None, Some(0)));
        assert!(!asked(false, None, Some(4096)));
        assert!(!asked(true, Some(b"routed"), Some(4096)));
    }

    // The local Linux filesystems that make holes are sized; a share and a
    // filesystem not known are not.
    #[test]
    fn linux_sizes_only_filesystems_known_to_make_holes() {
        // ext4, XFS, Btrfs, ZFS, tmpfs.
        for magic in [0xEF53, 0x5846_5342, 0x9123_683E, 0x2FC1_2FC1, 0x0102_1994] {
            assert!(linux_makes_holes(magic), "{magic:#x}");
        }
        // NFS, SMB, CIFS, SMB2, FUSE, an overlay, FAT, and nothing at all.
        for magic in [
            0x6969,
            0x517B,
            0xFF53_4D42,
            0xFE53_4D42,
            0x6573_5546,
            0x794C_7630,
            0x4D44,
            0,
        ] {
            assert!(!linux_makes_holes(magic), "{magic:#x}");
        }
    }

    // On macOS APFS is sized; HFS+, a share and a filesystem not known are
    // not.
    #[test]
    fn macos_sizes_only_apfs() {
        assert!(apple_makes_holes("apfs"));
        for filesystem in ["hfs", "smbfs", "nfs", "afpfs", "exfat", "msdos", "macfuse", ""] {
            assert!(!apple_makes_holes(filesystem), "{filesystem}");
        }
    }

    // On whatever filesystem holds the test, a new file is either given its
    // whole declared length or left to grow: never anything between.
    #[test]
    fn the_platform_rule_sizes_a_file_whole_or_not_at_all() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ember.bin");
        let file = create_sized(&path, &PlatformMarker, Some(1 << 20)).unwrap();
        let length = file.metadata().unwrap().len();
        assert!(length == 0 || length == 1 << 20, "{length}");
        if !PRESIZES {
            assert_eq!(length, 0);
        }
    }

    // A file made with no declared length is never sized, on any platform.
    #[test]
    fn a_file_made_without_a_length_is_empty() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ember.bin");
        let file = create_sparse(&path, &PlatformMarker).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }
}
