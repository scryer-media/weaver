// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's files on disk, as its metainfo lists them under the directory
// they are placed in. Verified pieces are written to them by the writer of
// the volume they are on (`super::writes`), and peers are served and pieces
// checked from here: these files are the torrent's own, whatever unpacking
// makes of them beside.
//
// Handles to read are shared by every torrent through one pool, so a file read again
// and again, as a seed's are by its peers, is opened once rather than at
// every read (libtorrent `file_pool`). A handle is known by a key, never by
// path: a file takes fresh keys whenever it may have moved, so no handle
// outlives the file it was opened on. At most `CAPACITY` stay open, the
// least recently used closed first; one not used for `IDLE` is closed by a
// sweep that runs only while any is open.

use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

use tokio::time::Instant;
use weaver_torrent::session_stats::disk::FileEnds;

use super::layout::PieceLayout;

// What every torrent's file calls and the handle pool did since the process
// started: one relaxed add per call, read by a scrape without a lock. The
// handles are shared by every route, so these are not counted by route.
#[derive(Debug, Default)]
pub(crate) struct FileStats {
    // Files opened because no handle was open under their key, and handles
    // closed to make room for one or for going unused.
    opens: AtomicU64,
    closed_for_room: AtomicU64,
    closed_idle: AtomicU64,
    // Positional calls. Their bytes and their time are counted once, by the
    // session whose disk call they serve.
    writes: AtomicU64,
    reads: AtomicU64,
    // Calls that did not start where the last one of the same file ended.
    write_seeks: AtomicU64,
    read_seeks: AtomicU64,
}

/// One direction of the file calls.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileCallsSnapshot {
    pub calls: u64,
    /// Calls that did not start where the last one of the same file ended.
    pub seeks: u64,
}

/// The file calls of every torrent, and the handle pool, since the process
/// started.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileStatsSnapshot {
    pub opens: u64,
    pub closed_for_room: u64,
    pub closed_idle: u64,
    pub writes: FileCallsSnapshot,
    pub reads: FileCallsSnapshot,
}

pub(crate) static STATS: FileStats = FileStats::new();

impl FileStats {
    const fn new() -> Self {
        Self {
            opens: AtomicU64::new(0),
            closed_for_room: AtomicU64::new(0),
            closed_idle: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            write_seeks: AtomicU64::new(0),
            read_seeks: AtomicU64::new(0),
        }
    }

    pub(crate) fn snapshot(&self) -> FileStatsSnapshot {
        FileStatsSnapshot {
            opens: self.opens.load(Ordering::Relaxed),
            closed_for_room: self.closed_for_room.load(Ordering::Relaxed),
            closed_idle: self.closed_idle.load(Ordering::Relaxed),
            writes: FileCallsSnapshot {
                calls: self.writes.load(Ordering::Relaxed),
                seeks: self.write_seeks.load(Ordering::Relaxed),
            },
            reads: FileCallsSnapshot {
                calls: self.reads.load(Ordering::Relaxed),
                seeks: self.read_seeks.load(Ordering::Relaxed),
            },
        }
    }
}

// A call of `len` bytes at `offset` of a file whose last call of the kind
// ended at `last_end`: counted in `calls`, and in `seeks` when it does not
// start there.
fn counted(last_end: &AtomicU64, offset: u64, len: usize, calls: &AtomicU64, seeks: &AtomicU64) {
    calls.fetch_add(1, Ordering::Relaxed);
    if last_end.swap(offset + len as u64, Ordering::Relaxed) != offset {
        seeks.fetch_add(1, Ordering::Relaxed);
    }
}

// qBittorrent's `FilePoolSize` default; libtorrent's own `file_pool_size`
// is 40.
pub(crate) const CAPACITY: usize = 100;
// libtorrent `close_file_interval` on Windows. libtorrent closes nothing for
// idleness elsewhere; here every platform lets an idle handle go, so a
// torrent no peer reads holds no descriptor.
pub(crate) const IDLE: Duration = Duration::from_secs(240);

struct Handle {
    key: u64,
    file: Arc<File>,
    used: Instant,
    // When it was last used, in uses: the least recently used is the one
    // with the lowest, whatever the clock says.
    turn: u64,
}

struct Handles {
    open: Vec<Handle>,
    turns: u64,
    // Whether a sweep is running; set and cleared under the lock, so a
    // handle opened as the last sweep ends starts the next.
    sweeping: bool,
}

// A pool of handles. Every torrent's files open through `SHARED`; a test
// makes its own, so no other test's opens or clock reach it.
pub(crate) struct HandlePool(Mutex<Handles>);

pub(crate) static SHARED: HandlePool = HandlePool::new();

impl std::fmt::Debug for HandlePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlePool").finish_non_exhaustive()
    }
}

static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

// A key no handle is open under, in any pool.
fn key() -> u64 {
    NEXT_KEY.fetch_add(1, Ordering::Relaxed)
}

impl Handles {
    // The handle under `key`, marked used now.
    fn used(&mut self, key: u64, now: Instant) -> Option<Arc<File>> {
        self.turns += 1;
        let turn = self.turns;
        let handle = self.open.iter_mut().find(|handle| handle.key == key)?;
        handle.used = now;
        handle.turn = turn;
        Some(Arc::clone(&handle.file))
    }
}

impl HandlePool {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Handles {
            open: Vec::new(),
            turns: 0,
            sweeping: false,
        }))
    }

    fn handles(&self) -> std::sync::MutexGuard<'_, Handles> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // The handle open under `key`, opened by `open` when none is.
    fn open(
        &'static self,
        key: u64,
        open: impl FnOnce() -> io::Result<File>,
    ) -> io::Result<Arc<File>> {
        let now = Instant::now();
        if let Some(file) = self.handles().used(key, now) {
            return Ok(file);
        }
        // Opened outside the lock: another open never waits on it.
        let file = Arc::new(open()?);
        STATS.opens.fetch_add(1, Ordering::Relaxed);
        let mut handles = self.handles();
        if let Some(file) = handles.used(key, now) {
            return Ok(file);
        }
        if handles.open.len() >= CAPACITY
            && let Some(oldest) = (0..handles.open.len()).min_by_key(|&at| handles.open[at].turn)
        {
            handles.open.swap_remove(oldest);
            STATS.closed_for_room.fetch_add(1, Ordering::Relaxed);
        }
        handles.turns += 1;
        let turn = handles.turns;
        handles.open.push(Handle {
            key,
            file: Arc::clone(&file),
            used: now,
            turn,
        });
        if !handles.sweeping
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            handles.sweeping = true;
            runtime.spawn(self.sweep());
        }
        Ok(file)
    }

    // The handles under `keys` are closed once their last use ends.
    fn forget(&self, keys: &[u64]) {
        self.handles()
            .open
            .retain(|handle| !keys.contains(&handle.key));
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self, key: u64) -> bool {
        self.handles().open.iter().any(|handle| handle.key == key)
    }

    // Closes each handle once it has gone `IDLE` unused, sleeping until the
    // first one could, and ends when none is open.
    async fn sweep(&'static self) {
        loop {
            let next = {
                let mut handles = self.handles();
                let now = Instant::now();
                let before = handles.open.len();
                handles
                    .open
                    .retain(|handle| now.duration_since(handle.used) < IDLE);
                let closed = before - handles.open.len();
                if closed > 0 {
                    STATS
                        .closed_idle
                        .fetch_add(closed as u64, Ordering::Relaxed);
                }
                let Some(oldest) = handles.open.iter().map(|handle| handle.used).min() else {
                    handles.sweeping = false;
                    return;
                };
                oldest + IDLE
            };
            tokio::time::sleep_until(next).await;
        }
    }
}

pub(crate) fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < bytes.len() {
            // A zero-byte `seek_write` is no progress, and looping on it
            // would never end.
            let progress = file.seek_write(&bytes[written..], offset + written as u64)?;
            if progress == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            written += progress;
        }
        Ok(())
    }
}

pub(crate) fn read_exact_at(file: &File, offset: u64, out: &mut [u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(out, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut read = 0usize;
        while read < out.len() {
            let progress = file.seek_read(&mut out[read..], offset + read as u64)?;
            if progress == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            read += progress;
        }
        Ok(())
    }
}

// Where the files are, and the keys their handles to read are open under.
struct At {
    dir: PathBuf,
    reads: Vec<u64>,
}

impl At {
    fn new(dir: PathBuf, count: usize) -> Self {
        Self {
            dir,
            reads: (0..count).map(|_| key()).collect(),
        }
    }
}

pub(crate) struct Files {
    // Each file's path under the directory; none for padding, which is
    // never on disk.
    names: Vec<Option<PathBuf>>,
    at: RwLock<At>,
    pool: &'static HandlePool,
    // Where each file's last read and last write ended.
    ends: Vec<FileEnds>,
}

// A metainfo name as a path under the directory, or why it cannot be one:
// it must stay under it, so no component climbs out or roots it.
fn relative(name: &str) -> io::Result<PathBuf> {
    let path: PathBuf = name.split('/').collect();
    let inside = name
        .split('/')
        .all(|part| !part.is_empty() && part != "." && part != "..")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if !inside {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the torrent's file {name:?} would land outside its directory"),
        ));
    }
    Ok(path)
}

impl Files {
    pub(crate) fn new(layout: &PieceLayout, dir: PathBuf) -> io::Result<Self> {
        Self::in_pool(layout, dir, &SHARED)
    }

    pub(crate) fn in_pool(
        layout: &PieceLayout,
        dir: PathBuf,
        pool: &'static HandlePool,
    ) -> io::Result<Self> {
        let names = layout
            .files()
            .map(|file| {
                if file.padding {
                    Ok(None)
                } else {
                    relative(&file.name).map(Some)
                }
            })
            .collect::<io::Result<Vec<_>>>()?;
        let at = At::new(dir, names.len());
        Ok(Self {
            ends: names.iter().map(|_| FileEnds::default()).collect(),
            names,
            at: RwLock::new(at),
            pool,
        })
    }

    fn at(&self) -> RwLockReadGuard<'_, At> {
        self.at.read().unwrap_or_else(PoisonError::into_inner)
    }

    // The directory the files are under now.
    pub(crate) fn dir(&self) -> PathBuf {
        self.at().dir.clone()
    }

    // Each file's path under the directory; padding has none.
    pub(crate) fn names(&self) -> impl Iterator<Item = Option<&Path>> + '_ {
        self.names.iter().map(Option::as_deref)
    }

    // Where the file is now; none for padding.
    #[cfg(test)]
    pub(crate) fn path(&self, file: usize) -> Option<PathBuf> {
        let name = self.names.get(file)?.as_ref()?;
        Some(self.at().dir.join(name))
    }

    fn name(&self, file: usize) -> io::Result<&Path> {
        self.names
            .get(file)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("the torrent has no file {file} on disk"),
                )
            })
    }

    // Where the files are, held there while the guard is: a move waits for
    // it, so writes queued under it are queued for the files where they
    // are.
    pub(crate) fn placed(&self) -> Placed<'_> {
        Placed {
            files: self,
            at: self.at(),
        }
    }

    // The file's bytes from `offset` into `out`, which is filled whole or
    // not at all: a file not there, or too short, says so.
    pub(crate) fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let name = self.name(file)?;
        let at = self.at();
        let path = at.dir.join(name);
        let handle = self.pool.open(at.reads[file], || File::open(&path))?;
        counted(
            &self.ends[file].read,
            offset,
            out.len(),
            &STATS.reads,
            &STATS.read_seeks,
        );
        read_exact_at(&handle, offset, out)
    }

    // The file's `len` bytes from `offset` are about to be read in order:
    // the system is told, and reads them ahead. A file not there is left
    // to the read that follows.
    pub(crate) fn will_read(&self, file: usize, offset: u64, len: u64) {
        let Ok(name) = self.name(file) else {
            return;
        };
        let at = self.at();
        let path = at.dir.join(name);
        if let Ok(handle) = self.pool.open(at.reads[file], || File::open(&path)) {
            advise_will_need(&handle, offset, len);
        }
    }

    // `outs` filled one after the other from the file's bytes at `offset`,
    // in one positional read where the platform has one.
    pub(crate) fn readv(&self, file: usize, offset: u64, outs: &mut [&mut [u8]]) -> io::Result<()> {
        let name = self.name(file)?;
        let at = self.at();
        let path = at.dir.join(name);
        let handle = self.pool.open(at.reads[file], || File::open(&path))?;
        let len = outs.iter().map(|out| out.len()).sum();
        counted(
            &self.ends[file].read,
            offset,
            len,
            &STATS.reads,
            &STATS.read_seeks,
        );
        weaver_torrent::storage::filesystem::pread_exact_vectored(&handle, offset, outs)
    }

    // How many bytes the file holds on disk, by its size alone: none when
    // what is there is not a file, and nothing known when nothing can be
    // found there at all.
    pub(crate) fn held(&self, file: usize) -> Option<u64> {
        let name = self.name(file).ok()?;
        let path = self.at().dir.join(name);
        #[cfg(test)]
        LOOKS.with(|looks| looks.set(looks.get() + 1));
        let found = std::fs::metadata(path).ok()?;
        Some(if found.is_file() { found.len() } else { 0 })
    }

    // Every handle to read let go, so the files can be moved, removed or
    // replaced; the next read opens them again under fresh keys.
    pub(crate) fn release(&self) {
        let mut at = self.at.write().unwrap_or_else(PoisonError::into_inner);
        self.pool.forget(&at.reads);
        let dir = std::mem::take(&mut at.dir);
        *at = At::new(dir, self.names.len());
    }

    // Move the files, under the lock no write or read gets past, with
    // `relocate`, which is given where they are and says where they went:
    // the files are read and written there from then on. One that fails
    // leaves them where it found them.
    pub(crate) fn relocate<T, E>(
        &self,
        relocate: impl FnOnce(&Path) -> Result<(PathBuf, T), E>,
    ) -> Result<T, E> {
        let mut at = self.at.write().unwrap_or_else(PoisonError::into_inner);
        self.pool.forget(&at.reads);
        let (to, outcome) = relocate(&at.dir)?;
        *at = At::new(to, self.names.len());
        Ok(outcome)
    }
}

// The files where they are, while no move can take them elsewhere.
pub(crate) struct Placed<'a> {
    files: &'a Files,
    at: RwLockReadGuard<'a, At>,
}

impl Placed<'_> {
    // Where the file is; an error for padding, which is never on disk.
    pub(crate) fn path(&self, file: usize) -> io::Result<PathBuf> {
        Ok(self.at.dir.join(self.files.name(file)?))
    }

    // A write of `len` bytes at `offset` of the file, counted as it is
    // handed to its writer.
    pub(crate) fn count_write(&self, file: usize, offset: u64, len: usize) {
        counted(
            &self.files.ends[file].write,
            offset,
            len,
            &STATS.writes,
            &STATS.write_seeks,
        );
    }
}

// What of a torrent's files, named as its metainfo names them, and of the
// folders they go in is already under `dir`, before Weaver writes any: those
// are never Weaver's to delete. A name that cannot be a path under `dir` is
// never written, so it is not looked for.
pub(crate) fn adopted(dir: &Path, names: &[String]) -> super::model::Adopted {
    let mut adopted = super::model::Adopted::default();
    if !dir.is_dir() {
        return adopted;
    }
    adopted.dirs.push(String::new());
    let mut seen = HashSet::new();
    for name in names {
        let Ok(path) = relative(name) else {
            continue;
        };
        if std::fs::symlink_metadata(dir.join(&path)).is_ok() {
            adopted.files.push(name.clone());
        }
        for folder in path.ancestors().skip(1) {
            // The folders above one already looked at were looked at too.
            if folder.as_os_str().is_empty() || !seen.insert(folder.to_path_buf()) {
                break;
            }
            if dir.join(folder).is_dir() {
                adopted.dirs.push(folder.to_string_lossy().into_owned());
            }
        }
    }
    if !names.is_empty() && adopted.files.len() == names.len() {
        adopted.files.clear();
        adopted.all_files = true;
    }
    adopted
}

// The files [`adopted`] found, as paths under the directory.
pub(crate) fn adopted_paths(adopted: &super::model::Adopted) -> HashSet<PathBuf> {
    adopted
        .files
        .iter()
        .filter_map(|name| relative(name).ok())
        .collect()
}

// How many times this thread looked at a file's size.
#[cfg(test)]
thread_local! {
    pub(crate) static LOOKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// The system is told the file's `len` bytes from `offset` are about to be
// read, and starts reading them. Advice only: a refusal changes nothing.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn advise_will_need(file: &File, offset: u64, len: u64) {
    use std::os::fd::AsRawFd;

    // The system reads ahead no more per call than the device's largest
    // request, 128 KiB on many, so the range is told in steps of that.
    const STEP: u64 = 128 << 10;
    let mut at = offset;
    let end = offset.saturating_add(len);
    while at < end {
        let (Ok(from), Ok(step)) = (
            libc::off_t::try_from(at),
            libc::off_t::try_from(STEP.min(end - at)),
        ) else {
            return;
        };
        // SAFETY: the descriptor is open for as long as `file` is
        // borrowed, and the call only advises the system about its cache.
        let _ =
            unsafe { libc::posix_fadvise(file.as_raw_fd(), from, step, libc::POSIX_FADV_WILLNEED) };
        at += STEP;
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn advise_will_need(_file: &File, _offset: u64, _len: u64) {}

#[cfg(test)]
mod tests {
    use super::super::layout::PieceFile;
    use super::*;

    fn layout(names: &[(&str, u64)]) -> PieceLayout {
        PieceLayout::new(
            4,
            names
                .iter()
                .map(|(name, length)| PieceFile {
                    name: (*name).to_owned(),
                    length: *length,
                    padding: name.starts_with(".pad"),
                })
                .collect(),
        )
        .unwrap()
    }

    fn pool() -> &'static HandlePool {
        Box::leak(Box::new(HandlePool::new()))
    }

    // `bytes` put at `offset` of the file at `path`, as its writer puts a
    // piece's share.
    fn put(path: &Path, offset: u64, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        write_all_at(&file, offset, bytes).unwrap();
    }

    // A file reads back what was written to it where its name places it; a
    // file not there is not read as zeros.
    #[test]
    fn a_file_reads_back_what_was_written_where_it_is_placed() {
        let dir = tempfile::tempdir().unwrap();
        let files = Files::in_pool(
            &layout(&[("a/b.bin", 8), ("c.bin", 4)]),
            dir.path().into(),
            pool(),
        )
        .unwrap();
        let path = files.placed().path(0).unwrap();
        assert_eq!(path, dir.path().join("a").join("b.bin"));
        put(&path, 4, b"wxyz");
        let mut out = [0; 4];
        files.read(0, 4, &mut out).unwrap();
        assert_eq!(&out, b"wxyz");
        assert_eq!(
            files.read(1, 0, &mut out).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    // Told what is read next, a file is not made, changed or grown: one
    // not there is still not there, one there reads back the same, past
    // its end included, and padding is no file to tell of.
    #[test]
    fn telling_what_is_read_next_changes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let files = Files::in_pool(
            &layout(&[("a.bin", 8), (".pad/4", 4), ("c.bin", 4)]),
            dir.path().into(),
            pool(),
        )
        .unwrap();
        put(&files.placed().path(0).unwrap(), 0, b"abcdwxyz");
        assert!(files.placed().path(1).is_err());
        files.will_read(0, 0, 8);
        files.will_read(0, 4, 1 << 30);
        files.will_read(1, 0, 4);
        files.will_read(2, 0, 4);
        files.will_read(9, 0, 4);
        let mut out = [0; 8];
        files.read(0, 0, &mut out).unwrap();
        assert_eq!(&out, b"abcdwxyz");
        assert_eq!(
            std::fs::metadata(dir.path().join("a.bin")).unwrap().len(),
            8
        );
        assert_eq!(
            files.read(2, 0, &mut out[..4]).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(!dir.path().join("c.bin").exists());
    }

    // A name that would climb out of the directory, or root itself, is no
    // file of the torrent's.
    #[test]
    fn a_name_outside_the_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["../x", "/etc/x", "a/../../x", ""] {
            assert!(
                Files::in_pool(&layout(&[(name, 1)]), dir.path().into(), pool()).is_err(),
                "{name}"
            );
        }
    }

    // Let go, a file is opened again under a fresh key: the handle open on
    // the file before is not the one used after.
    #[test]
    fn released_handles_are_never_used_again() {
        let dir = tempfile::tempdir().unwrap();
        let pool = pool();
        let files = Files::in_pool(&layout(&[("a.bin", 4)]), dir.path().into(), pool).unwrap();
        put(&dir.path().join("a.bin"), 0, b"abcd");
        let mut out = [0; 4];
        files.read(0, 0, &mut out).unwrap();
        let before = files.at().reads[0];
        assert!(pool.is_open(before));
        files.release();
        assert!(!pool.is_open(before));
        files.read(0, 0, &mut out).unwrap();
        assert!(pool.is_open(files.at().reads[0]));
        assert_ne!(files.at().reads[0], before);
    }
}
