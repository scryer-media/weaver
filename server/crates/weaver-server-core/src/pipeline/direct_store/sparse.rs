// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Sparse marking for the files direct-store creates with holes in them.
//
// Three populations are sparse by construction, and all three are created by
// this subsystem rather than by the extractor:
//
// - **member partials** (`.direct.partial`), written at logical member offsets
//   as the articles that cover them arrive, in whatever order the wire
//   delivers;
// - **envelope files**, which are a sparse image of their source volume by
//   definition — every non-member byte at its true physical offset, and
//   nothing in between;
// - **repair and holds scratch**, which the reconstruction sweep writes
//   covered-run by covered-run, seeking past the holes a refetch will fill.
//
// On Unix a file is sparse the moment a write skips a region, so the marker is
// a no-op that succeeds. **On Windows it is not**: NTFS allocates and
// zero-fills everything below the high-water mark unless the file carries the
// sparse attribute, so a 40 GiB member partial whose first routed byte lands
// near the end costs 40 GiB of real disk immediately. `FSCTL_SET_SPARSE` sets
// that attribute.
//
// # Ordering and best-effort marking
//
// The control code applies to the file, not to a write, so it must be issued
// **immediately after creation and before any `set_len` or write** — marking a
// file that already spans a hole does not reclaim what NTFS already allocated.
// Marking is an allocation optimization: if the filesystem refuses it, the
// file remains usable with ordinary zero-filled holes. Direct placement can
// continue, at the cost of additional disk allocation and I/O. Open, resize,
// and write failures still propagate through their normal error paths.
//
// # Testing
//
// [`SparseMarking`] is a `Copy` enum rather than a `dyn` trait object so the
// router, the runtime and the reconstruction sweep can each hold one without
// an allocation or a lifetime. Its test-only [`SparseMarking::AlwaysFail`]
// variant exercises the allocation fallback on every platform — the real
// `FSCTL_SET_SPARSE` call is exercised only on Windows, and that validation is
// deferred to the Windows host rather than simulated here.

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

// Which marker a set uses. `Platform` everywhere but in the tests that drive
// the allocation fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SparseMarking {
    #[default]
    Platform,
    // Test-only: exercise a filesystem that refuses sparse marking.
    #[cfg(test)]
    AlwaysFail,
}

impl SparseMarker for SparseMarking {
    fn mark_sparse(&self, file: &File) -> io::Result<()> {
        match self {
            Self::Platform => mark_sparse_native(file),
            #[cfg(test)]
            Self::AlwaysFail => Err(io::Error::other("sparse marking failed (test injection)")),
        }
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
    mark_sparse_best_effort(&file, marker);
    Ok(file)
}

// Shared with openers that require their own truncation or sharing options.
pub(crate) fn mark_sparse_best_effort<M: SparseMarker>(file: &File, marker: &M) {
    if marker.mark_sparse(file).is_err() {
        crate::runtime::perf_probe::record(
            "direct_store.sparse.mark_failed",
            std::time::Duration::from_nanos(1),
        );
    }
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
