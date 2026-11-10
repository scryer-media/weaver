// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decode windows on a scratch file. A member whose decoder window (a RAR,
// LZMA, LZMA2 or xz dictionary, a 7z coder's) is larger than the profile
// lets a decoder hold in RAM is lent memory that is a mapping of a scratch
// file, so it decodes instead of being refused. RAM never goes above the
// profile's ceiling for it: a window no folder can hold is refused.
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

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::holds::HoldsAccountant;
use weaver_graph::{NodeId, WindowLender, WindowMemory};

use crate::operations::{CapacityDebits, CapacityReader};

// The largest window lent at all: RAR's format maximum dictionary.
pub(crate) const WINDOW_CEILING_BYTES: u64 = 64 << 30;

// Whether a window can be mapped here at all. Where none can, no run is
// lent one: a run whose window is over its grant is placed in RAM from the
// start, not lent, refused and started again.
pub(crate) const MAPS: bool = cfg!(any(target_os = "linux", target_os = "macos"));

// What scratch windows took from one disk's free-space reading since it was
// taken, for every job that lends on that disk: two jobs never both see the
// same room. One per place jobs share, kept by the pipeline.
#[derive(Clone, Debug, Default)]
pub(crate) struct ScratchLedger(Arc<Mutex<CapacityDebits>>);

impl ScratchLedger {
    fn lock(&self) -> std::sync::MutexGuard<'_, CapacityDebits> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// The data folder's subfolder of window files. Nothing else is put there.
pub(crate) const SPARE_DIR: &str = ".weaver-windows";

const PREFIX: &str = "graph-window-";
const SUFFIX: &str = ".scratch";

// A window's file, under the job's working directory.
pub(crate) fn window_name(node: NodeId, serial: u64) -> String {
    format!(
        "{}/{PREFIX}{}-{serial}{SUFFIX}",
        weaver_graph::names::INTERNAL_DIR,
        node.0
    )
}

// A window's file under the data folder, which every job shares.
pub(crate) fn spare_name(job: u64, node: NodeId, serial: u64) -> String {
    format!("{SPARE_DIR}/{PREFIX}{job}-{}-{serial}{SUFFIX}", node.0)
}

impl crate::pipeline::Pipeline {
    // Weaver's data folder, the one holding its database, and the free
    // space of its disk: the second place a decode window may go.
    pub(crate) fn window_data_dir(&self) -> (PathBuf, CapacityReader, ScratchLedger) {
        (
            self.script_data_dir.clone(),
            self.storage_capacity
                .reader(crate::operations::StorageRoot::Data),
            self.window_ledgers.data.clone(),
        )
    }
}

// Remove every window file a past process left in the data folder's
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

// The ledgers of the two disks windows go on, which every job shares: the
// intermediate root's, under each job's folder, and the data folder's.
#[derive(Clone, Debug, Default)]
pub(crate) struct WindowLedgers {
    pub(crate) intermediate: ScratchLedger,
    pub(crate) data: ScratchLedger,
}

// A folder windows may go in: where, how its files are named, the free
// space of its disk, and what windows took from it.
struct Place {
    root: PathBuf,
    // The job's id, in the names of a folder jobs share.
    job: Option<u64>,
    free_disk: CapacityReader,
    ledger: ScratchLedger,
}

impl Place {
    fn name(&self, node: NodeId, serial: u64) -> String {
        match self.job {
            Some(job) => spare_name(job, node, serial),
            None => window_name(node, serial),
        }
    }

    fn folder(&self) -> PathBuf {
        match self.job {
            Some(_) => self.root.join(SPARE_DIR),
            None => self.root.join(weaver_graph::names::INTERNAL_DIR),
        }
    }
}

// One job's windows: the folders their files may go in, in order, the free
// space each disk keeps, and what is lent now.
pub(crate) struct Windows {
    places: Vec<Place>,
    // The holds' budgets: their disk reserve is the free space scratch
    // never takes a disk below, the one a page-out keeps.
    accountant: Arc<HoldsAccountant>,
    ceiling: u64,
    serial: AtomicU64,
    // The starts its lenders were handed for, counted.
    starts: AtomicU64,
    lent: Mutex<Lent>,
}

#[derive(Default)]
struct Lent {
    windows: u64,
    bytes: u64,
    // What each node's run asked for since it was last handed a lender,
    // under the start it was handed it for.
    asked: HashMap<NodeId, Asked>,
}

// One start of a run that was handed a lender: the lender knows it by this,
// so a run stopped before it, still asking, is not heard as the new one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Start(u64);

// What a run asked of its lender under one start.
struct Asked {
    start: Start,
    // The widest window it asked for, lent or refused.
    widest: u64,
    // Whether a window was refused because no folder had room for it.
    refused: bool,
}

impl fmt::Debug for Windows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (windows, bytes) = self.live();
        f.debug_struct("Windows")
            .field(
                "folders",
                &self.places.iter().map(Place::folder).collect::<Vec<_>>(),
            )
            .field("ceiling", &self.ceiling)
            .field("windows", &windows)
            .field("bytes", &bytes)
            .finish()
    }
}

impl Windows {
    // Windows under `working_dir`, the job's folder, on the disk
    // `free_disk` reads.
    // Windows under `working_dir`, the job's folder, on the disk
    // `free_disk` reads, whose windows every job's are charged to in
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
            }],
            accountant,
            ceiling,
            serial: AtomicU64::new(0),
            starts: AtomicU64::new(0),
            lent: Mutex::new(Lent::default()),
        }
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
        });
        self
    }

    // What the node's run borrows its windows through, as its decoder asks
    // for them, and the start it is handed it for. A run handed it starts
    // with nothing asked on record; what a run handed one before asks from
    // here is not recorded.
    pub(crate) fn for_node(self: &Arc<Self>, node: NodeId) -> (Arc<dyn WindowLender>, Start) {
        let start = Start(self.starts.fetch_add(1, Ordering::Relaxed));
        self.lock().asked.insert(
            node,
            Asked {
                start,
                widest: 0,
                refused: false,
            },
        );
        let lender = Arc::new(NodeWindows {
            windows: Arc::clone(self),
            node,
            start,
        });
        (lender, start)
    }

    // The widest window the node's run asked for under `start`, lent or
    // refused, once one was refused because no folder had room for it
    // (`io::ErrorKind::StorageFull`): `None` while none was.
    pub(crate) fn refused(&self, node: NodeId, start: Start) -> Option<u64> {
        self.lock()
            .asked
            .get(&node)
            .filter(|asked| asked.start == start && asked.refused)
            .map(|asked| asked.widest)
    }

    // The bytes every folder could take now before its disk reaches the
    // free space it keeps, all together. `None` where a reading is unknown
    // or no free space is kept, as `admit` then judges nothing.
    pub(crate) fn room(&self) -> Option<u64> {
        let reserve = self.accountant.budgets().disk_reserve;
        if reserve == 0 {
            return None;
        }
        self.places.iter().try_fold(0u64, |room, place| {
            let free = place
                .ledger
                .lock()
                .apply(place.free_disk.current())
                .best_available_bytes()?;
            Some(room.saturating_add(free.saturating_sub(reserve)))
        })
    }

    // The windows lent now, and their bytes.
    pub(crate) fn live(&self) -> (u64, u64) {
        let lent = self.lock();
        (lent.windows, lent.bytes)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Lent> {
        self.lent.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // A window of `len` bytes for `node`'s run, on a file of its own in
    // the first folder that can hold it.
    pub(crate) fn allocate(self: &Arc<Self>, node: NodeId, len: usize) -> io::Result<Window> {
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
            match self.place(at, place, node, len) {
                Ok(window) => return Ok(window),
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
        node: NodeId,
        len: usize,
    ) -> Result<Window, (Step, String)> {
        let bytes = len as u64;
        self.admit(place, bytes)?;
        let path = place
            .root
            .join(place.name(node, self.serial.fetch_add(1, Ordering::Relaxed)));
        match sys::Mapping::create(&path, len) {
            Ok(mapping) => {
                let mut lent = self.lock();
                lent.windows += 1;
                lent.bytes += bytes;
                drop(lent);
                Ok(Window {
                    mapping: Some(mapping),
                    path,
                    place: at,
                    windows: Arc::clone(self),
                })
            }
            Err((step, error)) => {
                place.ledger.lock().credit(bytes);
                Err((step, error.to_string()))
            }
        }
    }

    // Charge `bytes` against the place's disk, for every job that lends on
    // it, refused when it would leave less free than the reserve. An
    // unknown reading has nothing to judge against, as a page-out has not;
    // the preallocation still fails on a disk that is really full.
    fn admit(&self, place: &Place, bytes: u64) -> Result<(), (Step, String)> {
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
        Ok(())
    }

    fn release(&self, at: usize, bytes: u64) {
        let mut lent = self.lock();
        lent.windows -= 1;
        lent.bytes -= bytes;
        drop(lent);
        self.places[at].ledger.lock().credit(bytes);
    }
}

// A run's view of its job's windows, naming their files for its node.
struct NodeWindows {
    windows: Arc<Windows>,
    node: NodeId,
    start: Start,
}

impl NodeWindows {
    // What the run asked, kept under its start alone: a run started again
    // since was handed a lender of its own.
    fn record(&self, len: usize, refused: bool) {
        let mut lent = self.windows.lock();
        if let Some(asked) = lent
            .asked
            .get_mut(&self.node)
            .filter(|asked| asked.start == self.start)
        {
            asked.widest = asked.widest.max(len as u64);
            asked.refused |= refused;
        }
    }
}

impl WindowLender for NodeWindows {
    // Every window lent is kept, and every one refused for want of room,
    // before the decoder hears it, so the owner reads them when the
    // decoder's refusal reaches it. A window that may not be lent at all
    // (empty, or over the ceiling) is the decoder's own refusal.
    fn lend(&self, len: usize) -> io::Result<Box<dyn WindowMemory>> {
        let lent = self.windows.allocate(self.node, len);
        match &lent {
            Ok(_) => self.record(len, false),
            Err(error) if error.kind() == io::ErrorKind::StorageFull => self.record(len, true),
            Err(_) => {}
        }
        Ok(Box::new(lent?))
    }

    fn ceiling(&self) -> u64 {
        self.windows.ceiling
    }

    fn room(&self) -> Option<u64> {
        self.windows.room()
    }
}

// One lent window: a mapping of its own scratch file. Dropping it unmaps
// the file and removes it.
pub(crate) struct Window {
    mapping: Option<sys::Mapping>,
    path: PathBuf,
    // Which of the job's folders it lies in.
    place: usize,
    windows: Arc<Windows>,
}

// For the tests of windows that are mapped.
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
impl Window {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        let len = WindowMemory::len(self);
        let ptr = WindowMemory::as_mut_ptr(self);
        // SAFETY: the mapping is `len` readable and writable bytes that stay
        // mapped while `self` lives, and the `&mut self` borrow makes this
        // slice the only access to them for its lifetime.
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }
}

impl fmt::Debug for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Window")
            .field("path", &self.path)
            .field("len", &WindowMemory::len(self))
            .finish()
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        let len = self.mapping.as_ref().map_or(0, sys::Mapping::len);
        // Unmapped before its file goes.
        self.mapping = None;
        sys::remove(&self.path);
        self.windows.release(self.place, len as u64);
    }
}

// SAFETY: `as_mut_ptr` is the start of a shared mapping of `len` bytes of
// a file this window alone created and alone maps; the address is fixed
// until `Drop` unmaps it, and nothing else in the process reads or writes
// it.
unsafe impl WindowMemory for Window {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mapping
            .as_ref()
            .map_or(std::ptr::null_mut(), sys::Mapping::ptr)
    }

    fn len(&self) -> usize {
        self.mapping.as_ref().map_or(0, sys::Mapping::len)
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

// The windows themselves, where they can be mapped.
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;

// Where no window can be mapped, asking for one is a clean refusal.
#[cfg(all(test, not(any(target_os = "linux", target_os = "macos"))))]
#[test]
fn a_window_is_refused_where_it_is_not_supported() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
        dir.path().to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        Arc::new(HoldsAccountant::new(weaver_graph::holds::HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        })),
        WINDOW_CEILING_BYTES,
    ));
    let error = windows.allocate(NodeId(1), 1 << 20).unwrap_err();
    let refused = Refused::of(&error).unwrap();
    let Why::Nowhere(tried) = &refused.why else {
        panic!("{error}");
    };
    assert_eq!(tried[0].step, Step::Unsupported);
    assert_eq!(windows.live(), (0, 0));
    assert!(!dir.path().join(weaver_graph::names::INTERNAL_DIR).exists());
}
