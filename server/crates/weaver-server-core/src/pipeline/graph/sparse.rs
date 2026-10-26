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
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    if marker.mark_sparse(&file).is_err() {
        crate::runtime::perf_probe::record(
            "direct_store.sparse.mark_failed",
            std::time::Duration::from_nanos(1),
        );
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
}
