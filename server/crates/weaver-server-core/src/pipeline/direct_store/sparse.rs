//! Sparse marking for the files direct-store creates with holes in them.
//!
//! Three populations are sparse by construction, and all three are created by
//! this subsystem rather than by the extractor:
//!
//! - **member partials** (`.direct.partial`), written at logical member offsets
//!   as the articles that cover them arrive, in whatever order the wire
//!   delivers;
//! - **envelope files**, which are a sparse image of their source volume by
//!   definition — every non-member byte at its true physical offset, and
//!   nothing in between;
//! - **repair and holds scratch**, which the reconstruction sweep writes
//!   covered-run by covered-run, seeking past the holes a refetch will fill.
//!
//! On Unix a file is sparse the moment a write skips a region, so the marker is
//! a no-op that succeeds. **On Windows it is not**: NTFS allocates and
//! zero-fills everything below the high-water mark unless the file carries the
//! sparse attribute, so a 40 GiB member partial whose first routed byte lands
//! near the end costs 40 GiB of real disk immediately. `FSCTL_SET_SPARSE` sets
//! that attribute.
//!
//! # Ordering, and why failure demotes
//!
//! The control code applies to the file, not to a write, so it must be issued
//! **immediately after creation and before any `set_len` or write** — marking a
//! file that already spans a hole does not reclaim what NTFS already allocated.
//! That gives the rule this module states: a marking failure demotes the set
//! *before any long-lived hole exists*, so the worst case is a set that pays
//! the conventional path's disk cost, never one that quietly pays 1× per volume
//! in allocated zeros.
//!
//! [`create_sparse`] is therefore the only way this subsystem creates one of
//! those files: it creates, marks, and on a marking failure **removes the file
//! it just created** before returning the error, so a caller that demotes
//! leaves nothing behind for the restart sweep to reason about.
//!
//! # Testing
//!
//! [`SparseMarking`] is a `Copy` enum rather than a `dyn` trait object so the
//! router, the runtime and the reconstruction sweep can each hold one without
//! an allocation or a lifetime. Its test-only [`SparseMarking::AlwaysFail`]
//! variant is what drives the demotion paths on every platform — the real
//! `FSCTL_SET_SPARSE` call is exercised only on Windows, and that validation is
//! deferred to the Windows host rather than simulated here.

use std::fs::File;
use std::io;
use std::path::Path;

/// Marks a freshly created file sparse.
///
/// Implementors must be callable on a handle that has had nothing written to
/// it and no length set — see the module note on ordering.
pub(crate) trait SparseMarker {
    fn mark_sparse(&self, file: &File) -> io::Result<()>;
}

/// Which marker a set uses. `Platform` everywhere but in the tests that drive
/// the demotion paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SparseMarking {
    #[default]
    Platform,
    /// Test-only: every marking attempt fails, which is the only way to reach
    /// the demotion arms on a platform whose real marker cannot fail.
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

/// Why [`create_sparse`] refused, kept apart because the two halves are
/// different diagnoses.
///
/// `Open` is an ordinary filesystem failure — a missing directory, a full disk,
/// a permission problem — and it is what would have happened at the first write
/// anyway, so callers report it as a destination failure. `Mark` is the one
/// this rule cares about: the file exists and is writable, but this filesystem
/// will not give it the sparse attribute. Merging them would have made every
/// ENOSPC read as a Windows-sparseness problem in the metrics.
#[derive(Debug)]
pub(crate) enum SparseCreateError {
    Open(io::Error),
    Mark(io::Error),
}

impl std::fmt::Display for SparseCreateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(error) => write!(formatter, "could not be created: {error}"),
            Self::Mark(error) => write!(formatter, "could not be marked sparse: {error}"),
        }
    }
}

/// Creates `path` and marks it sparse before it can hold a hole.
///
/// The file is created read/write and **not** truncated: a restart re-marks a
/// destination that already holds routed bytes, and truncating it there would
/// throw away the coverage the checkpoint just claimed. Truncation is the
/// caller's business (holds scratch wants it, member partials must never have
/// it).
///
/// On a marking failure the file is removed again — but only when this call
/// created it. A pre-existing destination is left exactly as it was found, so a
/// marking failure on restart demotes without destroying the bytes the
/// conventional path is about to reconstruct from.
pub(crate) fn create_sparse<M: SparseMarker>(
    path: &Path,
    marker: &M,
) -> Result<File, SparseCreateError> {
    let existed = path.try_exists().unwrap_or(false);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(SparseCreateError::Open)?;
    if let Err(error) = marker.mark_sparse(&file) {
        drop(file);
        if !existed {
            let _ = std::fs::remove_file(path);
        }
        crate::runtime::perf_probe::record(
            "direct_store.sparse.mark_failed",
            std::time::Duration::from_nanos(1),
        );
        return Err(SparseCreateError::Mark(error));
    }
    Ok(file)
}

/// `FSCTL_SET_SPARSE` with no input buffer, which is the "make it sparse" form
/// (the optional `FILE_SET_SPARSE_BUFFER` only exists to turn it back off).
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

/// Unix files are sparse by writing past a hole, so there is nothing to ask
/// for. Succeeding rather than returning `Unsupported` is deliberate: the
/// caller's failure arm is a demotion, and demoting every set on Linux would be
/// an outage rather than a hardening.
#[cfg(not(windows))]
fn mark_sparse_native(_file: &File) -> io::Result<()> {
    Ok(())
}
