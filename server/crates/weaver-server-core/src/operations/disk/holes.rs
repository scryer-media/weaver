// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Whether a filesystem keeps a length set past the bytes written as a hole,
// taking no space for it.
//
// The answer is a fact about a filesystem, not about a file, so it is asked
// once for each device and held here: every later file made on that device
// reads the held answer and makes no filesystem call. A call that fails is
// not an answer and is never held; the next file asks again.

use std::fs::{File, Metadata};
use std::sync::Mutex;

// The most devices an answer is held for. A host has a handful; past this
// the oldest answers are dropped and asked again.
const HELD_MAX: usize = 64;

// The answer held for each device asked about.
#[derive(Debug, Default)]
pub(crate) struct HoleAnswers {
    held: Mutex<Vec<(u64, bool)>>,
}

impl HoleAnswers {
    pub(crate) const fn new() -> Self {
        Self {
            held: Mutex::new(Vec::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(u64, bool)>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // The answer for `device`: the one held, or `ask`'s, which is then held.
    // `ask` answers `None` when it could not tell; that reads as "does not"
    // for this file only. A file with no device to hold by is asked about
    // each time.
    pub(crate) fn answer(&self, device: Option<u64>, ask: impl FnOnce() -> Option<bool>) -> bool {
        let Some(device) = device else {
            return ask().unwrap_or(false);
        };
        if let Some((_, held)) = self.lock().iter().find(|(held, _)| *held == device) {
            return *held;
        }
        // Asked with the lock let go: a slow mount holds no other file.
        let Some(answer) = ask() else {
            return false;
        };
        let mut held = self.lock();
        if !held.iter().any(|(held, _)| *held == device) {
            if held.len() >= HELD_MAX {
                held.remove(0);
            }
            held.push((device, answer));
        }
        answer
    }
}

static ANSWERS: HoleAnswers = HoleAnswers::new();

// Whether the filesystem the open `file` is on makes holes, from the answer
// held for its device. `metadata` is the file's own, which the caller has
// already read.
pub(crate) fn makes_holes(file: &File, metadata: &Metadata) -> bool {
    ANSWERS.answer(device_of(metadata), || filesystem_makes_holes(file))
}

// The device a file's metadata names; none where the platform's metadata
// carries none.
#[cfg(unix)]
pub(crate) fn device_of(metadata: &Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.dev())
}

#[cfg(not(unix))]
pub(crate) fn device_of(_metadata: &Metadata) -> Option<u64> {
    None
}

// The filesystem asked directly, with nothing held: `None` when the call
// failed. On Windows a file that took the sparse mark makes holes, only such
// a file is asked about, and no call is made.
#[cfg(windows)]
pub(crate) fn filesystem_makes_holes(_file: &File) -> Option<bool> {
    Some(true)
}

#[cfg(target_os = "linux")]
pub(crate) fn filesystem_makes_holes(file: &File) -> Option<bool> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fstatfs` fills a zeroed `libc::statfs` for an open
    // descriptor; the return code is checked before any field is read.
    let magic = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::fstatfs(file.as_raw_fd(), &mut stat) != 0 {
            return None;
        }
        (stat.f_type as u64) & 0xFFFF_FFFF
    };
    Some(linux_makes_holes(magic))
}

#[cfg(target_vendor = "apple")]
pub(crate) fn filesystem_makes_holes(file: &File) -> Option<bool> {
    use std::os::fd::AsRawFd;

    // SAFETY: as above; `f_fstypename` is a NUL-terminated array.
    let name = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::fstatfs(file.as_raw_fd(), &mut stat) != 0 {
            return None;
        }
        std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr())
            .to_string_lossy()
            .into_owned()
    };
    Some(apple_makes_holes(&name))
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
pub(crate) fn filesystem_makes_holes(_file: &File) -> Option<bool> {
    Some(false)
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    // One device is asked about once, however many files are made on it.
    #[test]
    fn a_device_is_asked_about_once() {
        let answers = HoleAnswers::new();
        let asked = Cell::new(0);
        for _ in 0..100 {
            assert!(answers.answer(Some(11), || {
                asked.set(asked.get() + 1);
                Some(true)
            }));
        }
        assert_eq!(asked.get(), 1);
    }

    // Each device has its own answer.
    #[test]
    fn each_device_holds_its_own_answer() {
        let answers = HoleAnswers::new();
        assert!(answers.answer(Some(11), || Some(true)));
        assert!(!answers.answer(Some(12), || Some(false)));
        let unasked = || -> Option<bool> { panic!("a held device is not asked about again") };
        assert!(answers.answer(Some(11), unasked));
        assert!(!answers.answer(Some(12), unasked));
    }

    // A call that failed is not an answer: the file is not sized, and the
    // next file on the device asks again.
    #[test]
    fn a_failed_call_is_never_held() {
        let answers = HoleAnswers::new();
        let asked = Cell::new(0);
        for _ in 0..3 {
            assert!(!answers.answer(Some(11), || {
                asked.set(asked.get() + 1);
                None
            }));
        }
        assert_eq!(asked.get(), 3);
        assert!(answers.answer(Some(11), || Some(true)));
        assert!(answers.answer(Some(11), || None));
    }

    // With no device to hold by, every file is asked about.
    #[test]
    fn a_file_with_no_device_is_asked_about_each_time() {
        let answers = HoleAnswers::new();
        let asked = Cell::new(0);
        for _ in 0..3 {
            answers.answer(None, || {
                asked.set(asked.get() + 1);
                Some(true)
            });
        }
        assert_eq!(asked.get(), 3);
    }

    // No more than the limit is held, and a device dropped is asked again.
    #[test]
    fn the_answers_held_are_bounded() {
        let answers = HoleAnswers::new();
        for device in 0..(HELD_MAX as u64 + 10) {
            answers.answer(Some(device), || Some(true));
        }
        assert_eq!(answers.lock().len(), HELD_MAX);
        let asked = Cell::new(false);
        answers.answer(Some(0), || {
            asked.set(true);
            Some(true)
        });
        assert!(asked.get());
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
}
