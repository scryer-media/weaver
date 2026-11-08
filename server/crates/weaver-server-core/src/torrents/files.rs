// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's files on disk, as its metainfo lists them under the directory
// they are placed in. Verified pieces are written here, and peers are served
// and pieces checked from here: these files are the torrent's own, whatever
// unpacking makes of them beside.
//
// Handles are shared by every torrent through one pool, so a file read again
// and again, as a seed's are by its peers, is opened once rather than at
// every read (libtorrent `file_pool`). A handle is known by a key, never by
// path: a file takes fresh keys whenever it may have moved, so no handle
// outlives the file it was opened on. At most `CAPACITY` stay open, the
// least recently used closed first; one not used for `IDLE` is closed by a
// sweep that runs only while any is open.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

use tokio::time::Instant;

use super::layout::PieceLayout;

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
        let mut handles = self.handles();
        if let Some(file) = handles.used(key, now) {
            return Ok(file);
        }
        if handles.open.len() >= CAPACITY
            && let Some(oldest) = (0..handles.open.len()).min_by_key(|&at| handles.open[at].turn)
        {
            handles.open.swap_remove(oldest);
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
                handles
                    .open
                    .retain(|handle| now.duration_since(handle.used) < IDLE);
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

// Where the files are, and the keys their handles are open under: one to
// read each file, one to write it.
struct At {
    dir: PathBuf,
    reads: Vec<u64>,
    writes: Vec<u64>,
}

impl At {
    fn new(dir: PathBuf, count: usize) -> Self {
        Self {
            dir,
            reads: (0..count).map(|_| key()).collect(),
            writes: (0..count).map(|_| key()).collect(),
        }
    }

    fn keys(&self) -> Vec<u64> {
        self.reads.iter().chain(&self.writes).copied().collect()
    }
}

pub(crate) struct Files {
    // Each file's path under the directory; none for padding, which is
    // never on disk.
    names: Vec<Option<PathBuf>>,
    at: RwLock<At>,
    pool: &'static HandlePool,
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

    // `bytes` written at `offset` of the file, which is made, with the
    // directories it is in, when it is not there yet.
    pub(crate) fn write(&self, file: usize, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let name = self.name(file)?;
        let at = self.at();
        let path = at.dir.join(name);
        let handle = self.pool.open(at.writes[file], || open_for_write(&path))?;
        write_all_at(&handle, offset, bytes)
    }

    // The file's bytes from `offset` into `out`, which is filled whole or
    // not at all: a file not there, or too short, says so.
    pub(crate) fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let name = self.name(file)?;
        let at = self.at();
        let path = at.dir.join(name);
        let handle = self.pool.open(at.reads[file], || File::open(&path))?;
        read_exact_at(&handle, offset, out)
    }

    // `outs` filled one after the other from the file's bytes at `offset`,
    // in one positional read where the platform has one.
    pub(crate) fn readv(&self, file: usize, offset: u64, outs: &mut [&mut [u8]]) -> io::Result<()> {
        let name = self.name(file)?;
        let at = self.at();
        let path = at.dir.join(name);
        let handle = self.pool.open(at.reads[file], || File::open(&path))?;
        weaver_torrent::storage::filesystem::pread_exact_vectored(&handle, offset, outs)
    }

    // The file made `length` long when it is shorter, so a file whose
    // pieces are all had is there whole, an empty one among them. A longer
    // one stays as it is.
    pub(crate) fn grow(&self, file: usize, length: u64) -> io::Result<()> {
        let name = self.name(file)?;
        let at = self.at();
        let path = at.dir.join(name);
        let handle = self.pool.open(at.writes[file], || open_for_write(&path))?;
        if handle.metadata()?.len() < length {
            handle.set_len(length)?;
        }
        Ok(())
    }

    // Every handle let go, so the files can be moved, removed or replaced;
    // the next use opens them again under fresh keys.
    pub(crate) fn release(&self) {
        let mut at = self.at.write().unwrap_or_else(PoisonError::into_inner);
        self.pool.forget(&at.keys());
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
        self.pool.forget(&at.keys());
        let (to, outcome) = relocate(&at.dir)?;
        *at = At::new(to, self.names.len());
        Ok(outcome)
    }
}

fn open_for_write(path: &Path) -> io::Result<File> {
    let open = || {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
    };
    match open() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            open()
        }
        opened => opened,
    }
}

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

    // A file is made, directories and all, at its first write, and reads
    // back what was written; a file not there is not read as zeros.
    #[test]
    fn a_file_is_made_at_its_first_write_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let files = Files::in_pool(
            &layout(&[("a/b.bin", 8), ("c.bin", 4)]),
            dir.path().into(),
            pool(),
        )
        .unwrap();
        files.write(0, 4, b"wxyz").unwrap();
        let mut out = [0; 4];
        files.read(0, 4, &mut out).unwrap();
        assert_eq!(&out, b"wxyz");
        assert_eq!(
            files.read(1, 0, &mut out).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        files.grow(1, 4).unwrap();
        assert_eq!(
            std::fs::metadata(dir.path().join("c.bin")).unwrap().len(),
            4
        );
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
        files.write(0, 0, b"abcd").unwrap();
        let before = files.at().writes[0];
        assert!(pool.is_open(before));
        files.release();
        assert!(!pool.is_open(before));
        files.write(0, 0, b"dcba").unwrap();
        assert!(pool.is_open(files.at().writes[0]));
        assert_ne!(files.at().writes[0], before);
    }
}
