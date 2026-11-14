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
    create_sized_with(path, marker, None, PRESIZES)
}

// Whether a file marked sparse takes its declared length when it is made.
// On Windows a file that grows write by write across scattered offsets
// keeps moving its end of file, and NTFS keeps the cached pages of such a
// file out of the standby list, so the first read of a finished output goes
// to disk; one set to its length first is written into holes inside it. On
// Unix nothing is gained: a write past the end makes a hole as cheaply, and
// the length stays the reach of the bytes written.
pub(crate) const PRESIZES: bool = cfg!(windows);

// Creates `path` as `create_sparse` does and, where the platform presizes,
// gives it `length`. Only a file the mark took is sized: on a mount that
// refuses the mark the length would be zero-filled space for bytes never
// written. Only an empty file is sized: one that already holds bytes is a
// restart's, whose length the restore reads as how far its bytes reach.
pub(crate) fn create_sized<M: SparseMarker>(
    path: &Path,
    marker: &M,
    length: Option<u64>,
) -> io::Result<File> {
    #[cfg(test)]
    if let Some(sized) = sizing::said() {
        return create_sized_with(path, marker, length, sized);
    }
    create_sized_with(path, marker, length, PRESIZES)
}

// A test that must see one outcome whatever platform it runs on says
// whether every new file is given its length, for as long as it holds the
// word. The word is the process's: a test run of its own, as the runner
// gives each test, is the only one that hears it.
#[cfg(test)]
pub(crate) mod sizing {
    use std::sync::atomic::{AtomicU8, Ordering};

    static SAID: AtomicU8 = AtomicU8::new(0);

    pub(crate) struct Sizing(());

    // Every new file with a declared length is given it.
    pub(crate) fn always() -> Sizing {
        SAID.store(1, Ordering::SeqCst);
        Sizing(())
    }

    // Every new file grows as it is written.
    pub(crate) fn never() -> Sizing {
        SAID.store(2, Ordering::SeqCst);
        Sizing(())
    }

    pub(super) fn said() -> Option<bool> {
        match SAID.load(Ordering::SeqCst) {
            1 => Some(true),
            2 => Some(false),
            _ => None,
        }
    }

    impl Drop for Sizing {
        fn drop(&mut self) {
            SAID.store(0, Ordering::SeqCst);
        }
    }
}

fn create_sized_with<M: SparseMarker>(
    path: &Path,
    marker: &M,
    length: Option<u64>,
    presizes: bool,
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
    let Some(length) = length.filter(|length| presizes && marked && *length > 0) else {
        return Ok(file);
    };
    // Sizing is an allocation hint like the mark: a file it fails on stays
    // usable and grows by its writes.
    let sized = file.metadata().and_then(|metadata| match metadata.len() {
        0 => file.set_len(length),
        _ => Ok(()),
    });
    if sized.is_err() {
        crate::runtime::perf_probe::record_value("graph.sparse.presize_failed", 1);
    }
    Ok(file)
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
        presizes: bool,
    ) -> Vec<u8> {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ember.bin");
        if let Some(held) = held {
            std::fs::write(&path, held).unwrap();
        }
        let mut file = create_sized_with(&path, marker, length, presizes).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        bytes
    }

    // Where the platform presizes, a new file the mark took reaches its
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

    // Where the platform does not presize, the file stays as long as what is
    // written to it.
    #[test]
    fn a_platform_that_does_not_presize_leaves_the_file_unsized() {
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

    // Only Windows presizes.
    #[test]
    fn only_windows_presizes() {
        assert_eq!(PRESIZES, cfg!(windows));
    }
}
