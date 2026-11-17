// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Scratch memory: memory that is a mapping of a scratch file of its own,
// lent to a job for bytes too large to hold in RAM. One `Scratch` per job
// lends it; every job's leases on one disk are charged to that disk's
// `ScratchLedger`, and every disk keeps the holds' disk reserve free. A
// lease is refused, never lent past those, and gives everything back when
// it is dropped. What a refusal means is the user's to decide: a graph
// run's decode window (`pipeline::graph::window`) follows its profile's
// policy, scratch-then-fail or scratch-then-RAM.
//
// The file goes first under the job's chunk directory. Where that refuses
// it (the disk reserve, or a step below fails), it goes under Weaver's own
// data folder, in a subfolder of window files only, named for the job as
// well. Where both refuse, the refusal names the bytes and, for each folder,
// the step that failed.
//
// The file holds nothing durable. No checkpoint names it: a restore sweeps
// any left in the job's folder (`restore::sweep_unnamed`), startup sweeps
// any left in the data folder's (`sweep_spare`), and neither is adopted.
//
// A window is only lent once the disk has truly given the space to the
// file and a mapping of it has been seen to work: a sparse file that runs
// out of disk under a mapping kills the process, and some filesystems lose
// the bytes silently or refuse a shared writable mapping. So each window is
// preallocated with the platform's own call, its allocated blocks checked
// to cover it, mapped, and probed (a page written at each end, synced, read
// back through the file) before the decoder sees it. A failed step leaves
// no file behind.
//
// What none of this can turn into an error: a real I/O error of the disk
// under a page the decoder touches later raises SIGBUS like any mapped file.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use weaver_graph::holds::HoldsAccountant;

use crate::operations::{CapacityDebits, CapacityReader};

// The largest lease lent at all: RAR's format maximum dictionary, the
// widest decode window.
pub(crate) const CEILING_BYTES: u64 = 64 << 30;

// Whether scratch memory can be mapped here at all. Where none can, every
// lease is refused, and each user takes its own way without one: a graph
// run whose window is over its grant is placed in RAM from the start.
pub(crate) const MAPS: bool = cfg!(any(target_os = "linux", target_os = "macos"));

// What scratch leases took from one disk's free-space reading since it was
// taken, for every job that lends on that disk: two jobs never both see the
// same room. One per place jobs share, kept by the pipeline.
#[derive(Clone, Debug, Default)]
pub(crate) struct ScratchLedger(Arc<Mutex<CapacityDebits>>);

impl ScratchLedger {
    fn lock(&self) -> std::sync::MutexGuard<'_, CapacityDebits> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// The data folder's subfolder of scratch files. Nothing else is put there.
// The names are the ones decode windows first had, kept so a file a past
// process left is still swept.
pub(crate) const SPARE_DIR: &str = ".weaver-windows";

const PREFIX: &str = "graph-window-";
const SUFFIX: &str = ".scratch";

// A lease's file, under the job's working directory, for `owner`, the part
// of the job it was lent to (a graph node's id).
pub(crate) fn file_name(owner: u32, serial: u64) -> String {
    format!(
        "{}/{PREFIX}{owner}-{serial}{SUFFIX}",
        weaver_graph::names::INTERNAL_DIR,
    )
}

// A lease's file under the data folder, which every job shares.
pub(crate) fn spare_name(job: u64, owner: u32, serial: u64) -> String {
    format!("{SPARE_DIR}/{PREFIX}{job}-{owner}-{serial}{SUFFIX}")
}

// Remove every scratch file a past process left in the data folder's
// window subfolder, and nothing else: only names a window takes, only in
// that subfolder, and nothing through a link. Run at startup, before any
// job can lend one.
pub(crate) fn sweep_spare(data_dir: &Path) {
    let dir = data_dir.join(SPARE_DIR);
    match std::fs::symlink_metadata(&dir) {
        Ok(meta) if meta.file_type().is_dir() => {}
        _ => return,
    }
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let is_window = name
            .to_str()
            .is_some_and(|name| name.starts_with(PREFIX) && name.ends_with(SUFFIX));
        let is_file = entry.file_type().is_ok_and(|kind| kind.is_file());
        if is_window && is_file {
            sys::remove(&entry.path());
        }
    }
}

// Why a window was not lent.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("decode window of {bytes} bytes refused: {why}")]
pub(crate) struct Refused {
    pub(crate) bytes: u64,
    pub(crate) why: Why,
}

impl Refused {
    // The refusal a failed lend carries, when that is why it failed. Read
    // where a decoder's refusal becomes the member's outcome.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn of(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref::<Self>()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Why {
    Empty,
    // More than any window may take.
    OverCeiling(u64),
    // Every folder tried refused it, in the order tried.
    Nowhere(Vec<Tried>),
}

impl fmt::Display for Why {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("an empty window is not lent"),
            Self::OverCeiling(ceiling) => {
                write!(f, "more than the {ceiling} bytes a window may take")
            }
            Self::Nowhere(tried) => {
                f.write_str("no folder could hold it")?;
                for tried in tried {
                    write!(f, "; {}: {}", tried.folder.display(), tried.step)?;
                    if !tried.detail.is_empty() {
                        write!(f, " ({})", tried.detail)?;
                    }
                }
                Ok(())
            }
        }
    }
}

// One folder that refused a window, and the step it refused at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tried {
    pub(crate) folder: PathBuf,
    pub(crate) step: Step,
    pub(crate) detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    // The window would take the disk below the free space it keeps.
    NoSpace,
    // The steps of making a mapping, only where one can be made.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Directory,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Create,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    PreallocateUnsupported,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Preallocate,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    NotReserved,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    MapUnsupported,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Map,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Probe,
    // Only where no window can be mapped.
    #[cfg_attr(any(target_os = "linux", target_os = "macos"), allow(dead_code))]
    Unsupported,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoSpace => "not enough free space",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Directory => "the folder is not usable",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Create => "the file could not be created",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::PreallocateUnsupported => "preallocation not supported",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Preallocate => "preallocation failed",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::NotReserved => "the filesystem did not reserve the space",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::MapUnsupported => "mapping not supported",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Map => "mapping failed",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Probe => "the mapping did not hold its bytes",
            Self::Unsupported => "scratch windows are not supported on this platform",
        })
    }
}

// The ledgers of the two disks scratch goes on, which every job shares: the
// intermediate root's, under each job's folder, and the data folder's.
#[derive(Clone, Debug, Default)]
pub(crate) struct ScratchLedgers {
    pub(crate) intermediate: ScratchLedger,
    pub(crate) data: ScratchLedger,
}

// A folder leases may go in: where, how its files are named, the free
// space of its disk, and what leases took from it.
struct Place {
    root: PathBuf,
    // The job's id, in the names of a folder jobs share.
    job: Option<u64>,
    free_disk: CapacityReader,
    ledger: ScratchLedger,
    // The bytes this job has lent here now.
    lent: AtomicU64,
}

impl Place {
    fn name(&self, owner: u32, serial: u64) -> String {
        match self.job {
            Some(job) => spare_name(job, owner, serial),
            None => file_name(owner, serial),
        }
    }

    fn folder(&self) -> PathBuf {
        match self.job {
            Some(_) => self.root.join(SPARE_DIR),
            None => self.root.join(weaver_graph::names::INTERNAL_DIR),
        }
    }
}

// One job's scratch: the folders its files may go in, in order, the free
// space each disk keeps, and what is lent now.
pub(crate) struct Scratch {
    places: Vec<Place>,
    // The holds' budgets: their disk reserve is the free space scratch
    // never takes a disk below, the one a page-out keeps.
    accountant: Arc<HoldsAccountant>,
    ceiling: u64,
    serial: AtomicU64,
    lent: Mutex<Lent>,
    // A quota refused a lease's file since this was last asked.
    quota: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct Lent {
    leases: u64,
    bytes: u64,
}

impl fmt::Debug for Scratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (leases, bytes) = self.live();
        f.debug_struct("Scratch")
            .field(
                "folders",
                &self.places.iter().map(Place::folder).collect::<Vec<_>>(),
            )
            .field("ceiling", &self.ceiling)
            .field("leases", &leases)
            .field("bytes", &bytes)
            .finish()
    }
}

impl Scratch {
    // Scratch under `working_dir`, the job's folder, on the disk
    // `free_disk` reads, whose leases every job's are charged to in
    // `ledger`.
    pub(crate) fn new(
        working_dir: PathBuf,
        free_disk: CapacityReader,
        ledger: ScratchLedger,
        accountant: Arc<HoldsAccountant>,
        ceiling: u64,
    ) -> Self {
        Self {
            places: vec![Place {
                root: working_dir,
                job: None,
                free_disk,
                ledger,
                lent: AtomicU64::new(0),
            }],
            accountant,
            ceiling,
            serial: AtomicU64::new(0),
            lent: Mutex::new(Lent::default()),
            quota: std::sync::atomic::AtomicBool::new(false),
        }
    }

    // Whether a quota refused a lease's file since this was last asked.
    pub(crate) fn take_quota(&self) -> bool {
        self.quota.swap(false, Ordering::Relaxed)
    }

    // And, where the job's folder refuses one, under the data folder
    // `data_dir`, on the disk `free_disk` reads, named for job `job`.
    pub(crate) fn or_spare(
        mut self,
        data_dir: PathBuf,
        job: u64,
        free_disk: CapacityReader,
        ledger: ScratchLedger,
    ) -> Self {
        self.places.push(Place {
            root: data_dir,
            job: Some(job),
            free_disk,
            ledger,
            lent: AtomicU64::new(0),
        });
        self
    }

    // The largest lease lent at all.
    pub(crate) fn ceiling(&self) -> u64 {
        self.ceiling
    }

    // The most any one folder could take now before its disk reaches the
    // free space it keeps, less what this job has lent in its other
    // folders, which may be on the same disk under a ledger of their own:
    // the folders are never added up, as by default the job's folder is
    // inside the data folder, on one disk. It may say less than the disks
    // have, never more. `None` where a reading is unknown or no free space
    // is kept, as `admit` then judges nothing.
    pub(crate) fn room(&self) -> Option<u64> {
        let reserve = self.accountant.budgets().disk_reserve;
        if reserve == 0 {
            return None;
        }
        let lent: u64 = self
            .places
            .iter()
            .map(|place| place.lent.load(Ordering::Relaxed))
            .sum();
        self.places.iter().try_fold(0u64, |room, place| {
            let free = place
                .ledger
                .lock()
                .apply(place.free_disk.current())
                .best_available_bytes()?;
            let elsewhere = lent - place.lent.load(Ordering::Relaxed).min(lent);
            Some(room.max(free.saturating_sub(reserve).saturating_sub(elsewhere)))
        })
    }

    // The free space every folder's disk keeps past any lease.
    pub(crate) fn kept_free(&self) -> u64 {
        self.accountant.budgets().disk_reserve
    }

    // The leases lent now, and their bytes.
    pub(crate) fn live(&self) -> (u64, u64) {
        let lent = self.lock();
        (lent.leases, lent.bytes)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Lent> {
        self.lent.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // A lease of `len` bytes for `owner`, the part of the job that asked,
    // on a file of its own in the first folder that can hold it.
    pub(crate) fn allocate(self: &Arc<Self>, owner: u32, len: usize) -> io::Result<Lease> {
        let bytes = len as u64;
        let refuse = |why| {
            let kind = match why {
                Why::Nowhere(_) => io::ErrorKind::StorageFull,
                _ => io::ErrorKind::InvalidInput,
            };
            io::Error::new(kind, Refused { bytes, why })
        };
        if len == 0 {
            return Err(refuse(Why::Empty));
        }
        if bytes > self.ceiling {
            return Err(refuse(Why::OverCeiling(self.ceiling)));
        }
        let mut tried = Vec::new();
        for (at, place) in self.places.iter().enumerate() {
            match self.place(at, place, owner, len) {
                Ok(lease) => return Ok(lease),
                Err((step, detail)) => tried.push(Tried {
                    folder: place.folder(),
                    step,
                    detail,
                }),
            }
        }
        Err(refuse(Why::Nowhere(tried)))
    }

    fn place(
        self: &Arc<Self>,
        at: usize,
        place: &Place,
        owner: u32,
        len: usize,
    ) -> Result<Lease, (Step, String)> {
        let bytes = len as u64;
        let basis = self.admit(place, bytes)?;
        let path = place
            .root
            .join(place.name(owner, self.serial.fetch_add(1, Ordering::Relaxed)));
        match sys::Mapping::create(&path, len) {
            Ok(mapping) => {
                let mut lent = self.lock();
                lent.leases += 1;
                lent.bytes += bytes;
                drop(lent);
                place.lent.fetch_add(bytes, Ordering::Relaxed);
                Ok(Lease {
                    mapping: Some(mapping),
                    path,
                    place: at,
                    basis,
                    scratch: Arc::clone(self),
                })
            }
            Err((step, error)) => {
                if crate::operations::is_quota_exceeded(&error) {
                    self.quota.store(true, Ordering::Relaxed);
                }
                place.ledger.lock().credit_against(basis, bytes);
                Err((step, error.to_string()))
            }
        }
    }

    // Charge `bytes` against the place's disk, for every job that lends on
    // it, refused when it would leave less free than the reserve. An
    // unknown reading has nothing to judge against, as a page-out has not;
    // the preallocation still fails on a disk that is really full. Says
    // the reading the charge was counted against, so taking it back takes
    // back only it (`CapacityDebits::credit_against`).
    fn admit(&self, place: &Place, bytes: u64) -> Result<Option<Instant>, (Step, String)> {
        let reserve = self.accountant.budgets().disk_reserve;
        let mut ledger = place.ledger.lock();
        let free = ledger
            .apply(place.free_disk.current())
            .best_available_bytes();
        if let Some(free) = free
            && reserve != 0
            && reserve.checked_add(bytes).is_none_or(|need| need > free)
        {
            return Err((
                Step::NoSpace,
                format!("{free} bytes free, {reserve} must stay free"),
            ));
        }
        ledger.debit(bytes);
        Ok(ledger.basis())
    }

    fn release(&self, at: usize, bytes: u64, basis: Option<Instant>) {
        let mut lent = self.lock();
        lent.leases -= 1;
        lent.bytes -= bytes;
        drop(lent);
        let place = &self.places[at];
        place.lent.fetch_sub(bytes, Ordering::Relaxed);
        place.ledger.lock().credit_against(basis, bytes);
    }
}

// One lease: a mapping of its own scratch file. Dropping it unmaps the file,
// removes it and gives back what it was charged.
pub(crate) struct Lease {
    mapping: Option<sys::Mapping>,
    path: PathBuf,
    // Which of the job's folders it lies in, and the reading of that
    // folder's disk its charge was counted against.
    place: usize,
    basis: Option<Instant>,
    scratch: Arc<Scratch>,
}

impl Lease {
    // The start of the lease's bytes. A lease is never empty, and its
    // address holds until it is dropped, also when the lease moves.
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mapping
            .as_ref()
            .map_or(std::ptr::null_mut(), sys::Mapping::ptr)
    }

    pub(crate) fn len(&self) -> usize {
        self.mapping.as_ref().map_or(0, sys::Mapping::len)
    }

    // The lease's bytes, to fill.
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        let len = self.len();
        if len == 0 {
            return &mut [];
        }
        let ptr = self.as_mut_ptr();
        // SAFETY: the mapping is `len` readable and writable bytes that stay
        // mapped while `self` lives, and the `&mut self` borrow makes this
        // slice the only access to them for its lifetime.
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }
}

// A lease read as bytes, as a lease filled and then handed on is: the
// bytes stay leased until the last reader of them lets go.
impl AsRef<[u8]> for Lease {
    fn as_ref(&self) -> &[u8] {
        let len = self.len();
        let Some(mapping) = self.mapping.as_ref() else {
            return &[];
        };
        if len == 0 {
            return &[];
        }
        // SAFETY: the mapping is `len` readable bytes that stay mapped while
        // `self` lives; the `&self` borrow allows no `as_mut_slice` while the
        // slice is read.
        unsafe { std::slice::from_raw_parts(mapping.ptr(), len) }
    }
}

// For the tests of leases that are mapped.
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
impl Lease {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl fmt::Debug for Lease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease")
            .field("path", &self.path)
            .field("len", &self.len())
            .finish()
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let len = self.len();
        // Unmapped before its file goes.
        self.mapping = None;
        sys::remove(&self.path);
        self.scratch.release(self.place, len as u64, self.basis);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod sys {
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{FileExt, OpenOptionsExt};
    use std::path::Path;
    use std::ptr::NonNull;

    use super::Step;

    // A shared writable mapping of a whole file.
    pub(super) struct Mapping {
        ptr: NonNull<u8>,
        len: usize,
    }

    // SAFETY: the mapping is plain memory owned by this value; moving the
    // owner to another thread moves nothing the mapping depends on, and
    // shared references expose only its address and length.
    unsafe impl Send for Mapping {}
    // SAFETY: as above; no `&self` method reads or writes the bytes.
    unsafe impl Sync for Mapping {}

    impl Mapping {
        pub(super) fn ptr(&self) -> *mut u8 {
            self.ptr.as_ptr()
        }

        pub(super) fn len(&self) -> usize {
            self.len
        }

        // Create the file at `path` new, reserve `len` bytes of disk for it,
        // map it and prove the mapping. Nothing is left at `path` on error.
        pub(super) fn create(path: &Path, len: usize) -> Result<Self, (Step, io::Error)> {
            let dir = path.parent().expect("a window lies in a directory");
            std::fs::create_dir_all(dir).map_err(|error| (Step::Directory, error))?;
            // The folder is Weaver's own: one that is a link leads
            // somewhere Weaver does not own.
            let meta = std::fs::symlink_metadata(dir).map_err(|error| (Step::Directory, error))?;
            if !meta.file_type().is_dir() {
                return Err((
                    Step::Directory,
                    io::Error::other(format!("{} is a link, not a folder", dir.display())),
                ));
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .map_err(|error| (Step::Create, error))?;
            // From here the file is ours, and goes on any failure.
            let made = Made(path);
            preallocate(&file, len).map_err(|error| {
                let step = match unsupported(&error) {
                    true => Step::PreallocateUnsupported,
                    false if crate::operations::is_out_of_space(&error) => Step::NoSpace,
                    false => Step::Preallocate,
                };
                (step, error)
            })?;
            reserved(&file, len).map_err(|error| (Step::NotReserved, error))?;
            let mapping = map(&file, len).map_err(|error| {
                let step = match unsupported(&error) {
                    true => Step::MapUnsupported,
                    false => Step::Map,
                };
                (step, error)
            })?;
            mapping.probe(&file).map_err(|error| (Step::Probe, error))?;
            std::mem::forget(made);
            Ok(mapping)
        }

        // A page written at each end, synced to the file and read back
        // through it: a filesystem that drops mapped writes fails here,
        // before the decoder trusts it.
        fn probe(&self, file: &File) -> io::Result<()> {
            // SAFETY: sysconf has no preconditions.
            let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
                .ok()
                .filter(|page| *page > 0)
                .unwrap_or(4096)
                .min(self.len);
            let last = self.len - page;
            let pattern = |at: usize| (at as u8) ^ 0xa5;
            for start in [0, last] {
                // SAFETY: `start + page <= len`, inside the mapping, which
                // nothing else touches yet.
                let bytes = unsafe { std::slice::from_raw_parts_mut(self.ptr().add(start), page) };
                for (at, byte) in bytes.iter_mut().enumerate() {
                    *byte = pattern(start + at);
                }
            }
            // SAFETY: the range is the whole mapping, page aligned at its
            // start as mmap returned it.
            if unsafe { libc::msync(self.ptr().cast(), self.len, libc::MS_SYNC) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let mut back = vec![0u8; page];
            for start in [0, last] {
                file.read_exact_at(&mut back, start as u64)?;
                if back
                    .iter()
                    .enumerate()
                    .any(|(at, byte)| *byte != pattern(start + at))
                {
                    return Err(io::Error::other(format!(
                        "the bytes written through the mapping at {start} did not reach the file"
                    )));
                }
            }
            Ok(())
        }
    }

    impl Drop for Mapping {
        fn drop(&mut self) {
            // SAFETY: `ptr` and `len` are exactly what mmap returned, and
            // the only references into the mapping were borrowed from its
            // owner, which is being dropped.
            unsafe {
                libc::munmap(self.ptr().cast(), self.len);
            }
        }
    }

    // Removes the file at the path unless forgotten.
    struct Made<'a>(&'a Path);

    impl Drop for Made<'_> {
        fn drop(&mut self) {
            remove(self.0);
        }
    }

    pub(super) fn remove(path: &Path) {
        match std::fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => tracing::warn!(
                path = %path.display(),
                %error,
                "a decode window's scratch file could not be removed"
            ),
            _ => {}
        }
    }

    fn unsupported(error: &io::Error) -> bool {
        // EOPNOTSUPP and ENOTSUP are one code on Linux and two on macOS.
        error.raw_os_error().is_some_and(|code| {
            [libc::EOPNOTSUPP, libc::ENOTSUP, libc::ENODEV, libc::ENOSYS].contains(&code)
        })
    }

    fn off(len: usize) -> io::Result<libc::off_t> {
        libc::off_t::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "window too large"))
    }

    // The native reservation: blocks the filesystem allocates now, never a
    // zero-filling fallback that some filesystems do not count as allocated.
    #[cfg(target_os = "linux")]
    fn preallocate(file: &File, len: usize) -> io::Result<()> {
        let len = off(len)?;
        loop {
            // SAFETY: a valid open descriptor; mode 0 allocates and extends
            // the file to `len`.
            if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn preallocate(file: &File, len: usize) -> io::Result<()> {
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: off(len)?,
            fst_bytesalloc: 0,
        };
        // SAFETY: a valid open descriptor and a live fstore_t.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
            return Err(io::Error::last_os_error());
        }
        file.set_len(len as u64)
    }

    // The file is `len` long and the filesystem holds blocks for all of it.
    fn reserved(file: &File, len: usize) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        let allocated = meta.blocks().saturating_mul(512);
        if meta.len() != len as u64 || allocated < len as u64 {
            return Err(io::Error::other(format!(
                "{allocated} of {len} bytes allocated for a file {} long",
                meta.len()
            )));
        }
        Ok(())
    }

    fn map(file: &File, len: usize) -> io::Result<Mapping> {
        // SAFETY: a valid descriptor open for reading and writing, of a file
        // `len` long; the kernel picks the address.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Mapping {
            ptr: NonNull::new(ptr.cast()).ok_or_else(|| io::Error::other("mmap returned null"))?,
            len,
        })
    }
}

// Elsewhere no window is lent: the Windows build does not enable the
// memory-mapping API in its dependencies, and no other target is built.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod sys {
    use std::io;
    use std::path::Path;

    use super::Step;

    pub(super) enum Mapping {}

    impl Mapping {
        pub(super) fn ptr(&self) -> *mut u8 {
            match *self {}
        }

        pub(super) fn len(&self) -> usize {
            match *self {}
        }

        pub(super) fn create(_path: &Path, _len: usize) -> Result<Self, (Step, io::Error)> {
            Err((
                Step::Unsupported,
                io::Error::from(io::ErrorKind::Unsupported),
            ))
        }
    }

    pub(super) fn remove(path: &Path) {
        let _ = std::fs::remove_file(path);
    }
}

// The leases themselves, where they can be mapped.
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;

// Where no window can be mapped, asking for one is a clean refusal.
#[cfg(all(test, not(any(target_os = "linux", target_os = "macos"))))]
#[test]
fn a_lease_is_refused_where_it_is_not_supported() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Scratch::new(
        dir.path().to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        Arc::new(HoldsAccountant::new(weaver_graph::holds::HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        })),
        CEILING_BYTES,
    ));
    let error = windows.allocate(1, 1 << 20).unwrap_err();
    let refused = Refused::of(&error).unwrap();
    let Why::Nowhere(tried) = &refused.why else {
        panic!("{error}");
    };
    assert_eq!(tried[0].step, Step::Unsupported);
    assert_eq!(windows.live(), (0, 0));
    assert!(!dir.path().join(weaver_graph::names::INTERNAL_DIR).exists());
}
