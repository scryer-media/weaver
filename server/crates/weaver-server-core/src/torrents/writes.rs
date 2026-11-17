// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's writes, through the writer of the volume its files lie on: the
// one thread that volume has for the whole process, which usenet jobs write
// through as well. The torrents are one owner among the writers' owners
// (`DiskOwner`), and each torrent writes under a key of its own, its job id.
//
// A verified piece's spans are handed to the writer as slices of the buffer
// the piece was hashed in: nothing is copied for them. The bytes are counted
// against the volume's hard limit from the moment the piece is started
// (`Writes::reserve`), and the writer gives them back as it writes them. A
// piece the volume has no room for is not started: the session waits for the
// room the writer rings once the volume has written enough.
//
// What a piece's write came to is told once, by `Done`: after its spans were
// written and what follows a write (the side store, the pieces had, the tee)
// was done, or with the error the writes met, or never, when the writes were
// given up (a pause, a removal, a move), in which case `Done` is dropped.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak, mpsc};

use bytes::Bytes;
use tokio::sync::Notify;

use crate::operations::StorageCapacity;
use crate::operations::VolumeBuffers;
use crate::operations::disk::volume::{Reservation, Volume};
use crate::operations::disk::writer::{
    Barrier, Create, DiskOwner, Failure, OpenSteps, Opened, Owner, Submitter, VolumeWriters,
};

// What a piece handed to be written came to, told once on whatever thread
// it ended on. Dropped untold when the writes were given up.
pub(crate) type Done = Box<dyn FnOnce(anyhow::Result<()>) + Send>;

// The files one torrent keeps open for writing at once: the least recently
// written is let go past this. The writer bounds no key's open files.
const OPEN_FILES: usize = 16;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// The torrents' place among the volumes' writers: one for the service.
pub(crate) struct TorrentDisk {
    writers: Arc<VolumeWriters>,
    owner: Owner,
    heard: Arc<Heard>,
    // The volumes torrents' files lie on, read as the pipeline reads them;
    // none for a service with no disks to read, whose torrents all write to
    // `unread`.
    capacity: Option<Arc<StorageCapacity>>,
    unread: Arc<Volume>,
    // The one sizing of every volume's limits, shared with the pipeline:
    // a volume a torrent writes to is sized as one a usenet job writes to.
    buffers: Option<Arc<VolumeBuffers>>,
    // Where what follows a write that may stand still runs, off the
    // writer's thread (the tee's share of a piece).
    runtime: Option<tokio::runtime::Handle>,
}

impl TorrentDisk {
    pub(crate) fn new(
        writers: Arc<VolumeWriters>,
        capacity: Option<Arc<StorageCapacity>>,
        buffers: Option<Arc<VolumeBuffers>>,
    ) -> Arc<Self> {
        let heard = Arc::new(Heard::default());
        let owner = writers.owner(Arc::clone(&heard) as Arc<dyn DiskOwner>);
        if let Some(buffers) = &buffers {
            // A limit raised may let a waiting piece start.
            let heard = Arc::downgrade(&heard);
            buffers.on_resize(Arc::new(move || {
                if let Some(heard) = heard.upgrade() {
                    heard.room();
                }
            }));
        }
        Arc::new(Self {
            writers,
            owner,
            heard,
            capacity,
            unread: Arc::new(Volume::new(None, "torrents".to_owned())),
            buffers,
            runtime: tokio::runtime::Handle::try_current().ok(),
        })
    }

    // Writers of their own, for a service with no pipeline.
    pub(crate) fn alone() -> Arc<Self> {
        Self::new(Arc::new(VolumeWriters::new()), None, None)
    }

    // The writes of the torrent `key` to its files under `dir`. Reads the
    // disk the first time `dir` is asked after: never on the async runtime.
    pub(crate) fn writes(self: &Arc<Self>, key: u64, dir: &Path) -> Writes {
        if let Some(capacity) = &self.capacity {
            capacity.read_volume(dir);
        }
        let current = Arc::new(Current {
            disk: Arc::clone(self),
            key,
            state: Mutex::new(State {
                dir: dir.to_path_buf(),
                generation: None,
            }),
        });
        lock(&self.heard.keys).insert(key, Arc::downgrade(&current));
        Writes { current }
    }

    // The volume `dir` lies on, from memory alone.
    fn volume(&self, dir: &Path) -> Arc<Volume> {
        match &self.capacity {
            Some(capacity) => capacity.volume(dir),
            None => Arc::clone(&self.unread),
        }
    }

    // Bytes counted on `volume` were given back here, not by its writer:
    // whoever waits for the line they reached hears it.
    fn gave_back(&self, volume: &Volume) {
        if self.writers.take_wake(volume, &self.owner) {
            self.heard.room();
        }
    }

    #[cfg(test)]
    pub(crate) fn writers(&self) -> &Arc<VolumeWriters> {
        &self.writers
    }
}

// The torrents as the writers' owner: a failure goes to the torrent whose
// key it names, and room wakes every torrent that waits for it.
#[derive(Default)]
struct Heard {
    keys: Mutex<HashMap<u64, Weak<Current>>>,
    waiting: Mutex<Vec<Arc<Notify>>>,
}

impl Heard {
    // `room` is rung at the next room heard.
    fn wait(&self, room: &Arc<Notify>) {
        let mut waiting = lock(&self.waiting);
        if !waiting.iter().any(|known| Arc::ptr_eq(known, room)) {
            waiting.push(Arc::clone(room));
        }
    }
}

impl DiskOwner for Heard {
    fn failed(&self, failure: Failure) {
        let torrent = lock(&self.keys).get(&failure.key).and_then(Weak::upgrade);
        if let Some(torrent) = torrent {
            torrent.failed(&failure);
        }
    }

    fn room(&self) {
        let waiting = std::mem::take(&mut *lock(&self.waiting));
        for room in waiting {
            room.notify_waiters();
        }
    }
}

// One torrent's writes.
#[derive(Clone)]
pub(crate) struct Writes {
    current: Arc<Current>,
}

struct Current {
    disk: Arc<TorrentDisk>,
    key: u64,
    state: Mutex<State>,
}

struct State {
    // The directory its files are under, whose volume a new generation
    // writes to.
    dir: PathBuf,
    generation: Option<Arc<Generation>>,
}

impl Drop for Current {
    fn drop(&mut self) {
        let mut keys = lock(&self.disk.heard.keys);
        if keys
            .get(&self.key)
            .is_some_and(|known| std::ptr::eq(known.as_ptr(), self))
        {
            keys.remove(&self.key);
        }
    }
}

impl Current {
    // A failure of the torrent's key: the generation it stopped fails, and
    // every piece it still held hears the error. One of a generation given
    // up, or already replaced, is not this torrent's to hear.
    fn failed(&self, failure: &Failure) {
        let generation = lock(&self.state).generation.clone();
        if let Some(generation) = generation
            && generation.submitter.barrier().failed()
        {
            generation.lost.fail(failure);
        }
    }
}

// The writes made under one submitter: from the first write after the
// torrent's start, its move or a failure, until it is given up or fails.
// It counts once among the writers of its volume while it is held.
struct Generation {
    submitter: Arc<Submitter>,
    lost: Arc<Lost>,
    // The files it holds open, least recently written first.
    open: Mutex<VecDeque<u64>>,
    buffers: Option<Arc<VolumeBuffers>>,
}

impl Generation {
    // Nothing more of it is written: what it still holds is not run, and
    // the pieces waiting on it are dropped untold.
    fn abandon(&self) -> Barrier {
        let pending = self.lost.abandon();
        drop(pending);
        self.submitter.abandon()
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        if let Some(buffers) = &self.buffers {
            buffers.left(self.submitter.volume());
        }
    }
}

// What becomes of a piece whose landing the writer dropped unrun: it waits
// for the generation's failure, hears it, or is dropped once the generation
// was given up.
#[derive(Default)]
struct Lost(Mutex<Fate>);

enum Fate {
    Live(Vec<Done>),
    Failed(Gone),
    Abandoned,
}

impl Default for Fate {
    fn default() -> Self {
        Self::Live(Vec::new())
    }
}

// A failure, kept to be told to every piece it stopped: the error as the
// system gave it, so it reads and classifies as it did.
struct Gone {
    os: Option<i32>,
    kind: io::ErrorKind,
    message: String,
}

impl Gone {
    fn of(error: &io::Error) -> Self {
        Self {
            os: error.raw_os_error(),
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn error(&self) -> anyhow::Error {
        let error = match self.os {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::new(self.kind, self.message.clone()),
        };
        anyhow::Error::from(error)
    }
}

impl Lost {
    fn lose(&self, done: Done) {
        let mut fate = lock(&self.0);
        match &mut *fate {
            Fate::Live(pending) => pending.push(done),
            Fate::Failed(gone) => {
                let error = gone.error();
                drop(fate);
                done(Err(error));
            }
            Fate::Abandoned => {
                drop(fate);
                drop(done);
            }
        }
    }

    fn fail(&self, failure: &Failure) {
        let pending = {
            let mut fate = lock(&self.0);
            let Fate::Live(pending) = &mut *fate else {
                return;
            };
            let pending = std::mem::take(pending);
            *fate = Fate::Failed(Gone::of(&failure.error));
            pending
        };
        let gone = Gone::of(&failure.error);
        for done in pending {
            done(Err(gone.error()));
        }
    }

    fn abandon(&self) -> Vec<Done> {
        match std::mem::replace(&mut *lock(&self.0), Fate::Abandoned) {
            Fate::Live(pending) => pending,
            Fate::Failed(_) | Fate::Abandoned => Vec::new(),
        }
    }

    // Whether the generation failed or was given up.
    fn ended(&self) -> bool {
        !matches!(*lock(&self.0), Fate::Live(_))
    }
}

// Bytes counted on a volume for a piece being assembled, from its start
// until the writer writes it. Given back when dropped unwritten.
pub(crate) struct Paid {
    reservation: Reservation,
    disk: Arc<TorrentDisk>,
}

impl Paid {
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.reservation.bytes()
    }
}

impl Drop for Paid {
    fn drop(&mut self) {
        let bytes = self.reservation.bytes();
        if bytes > 0 {
            self.reservation.give_back(bytes);
            self.disk.gave_back(self.reservation.volume());
        }
    }
}

// What a volume said to a piece's start.
pub(crate) enum VolumeRoom {
    // Nothing counts its bytes against a volume.
    Unmetered,
    Held(Paid),
    // No room: the piece is not started, and the room asked about rings
    // once the volume has some.
    Refused,
}

impl Writes {
    // The generation writes go to now, made when there is none or the one
    // there ended. Its volume joins the volumes sized as written to.
    fn generation(&self) -> Arc<Generation> {
        let current = &self.current;
        let mut state = lock(&current.state);
        if let Some(generation) = &state.generation
            && !generation.lost.ended()
        {
            return Arc::clone(generation);
        }
        let disk = &current.disk;
        let volume = disk.volume(&state.dir);
        let submitter = disk.writers.submitter(&volume, current.key, &disk.owner);
        if let Some(buffers) = &disk.buffers {
            buffers.joined(&volume);
        }
        let generation = Arc::new(Generation {
            submitter,
            lost: Arc::default(),
            open: Mutex::new(VecDeque::new()),
            buffers: disk.buffers.clone(),
        });
        // One replaced is dropped outside the lock.
        let replaced = state.generation.replace(Arc::clone(&generation));
        drop(state);
        drop(replaced);
        generation
    }

    // Count `bytes` on the volume the torrent writes to, for a piece about
    // to start: they stay counted until the writer writes the piece. Refused
    // when they do not fit below the volume's hard limit less the share its
    // writes keep (`Volume::write_piece`); `room` is rung once the volume
    // has written enough for them.
    pub(crate) fn reserve(&self, bytes: u64, room: &Arc<Notify>) -> VolumeRoom {
        let generation = self.generation();
        let volume = Arc::clone(generation.submitter.volume());
        let disk = &self.current.disk;
        loop {
            let mut reservation = Reservation::none(Arc::clone(&volume));
            if reservation.reserve(bytes, volume.write_piece()) {
                return VolumeRoom::Held(Paid {
                    reservation,
                    disk: Arc::clone(disk),
                });
            }
            disk.heard.wait(room);
            let (_, hard) = volume.buffer_limits();
            let fits = hard
                .saturating_sub(volume.write_piece())
                .saturating_sub(bytes)
                .min(volume.resume_line());
            // Already below: the room is there, and the next ask takes it.
            if generation.submitter.wake_below(fits) {
                return VolumeRoom::Refused;
            }
        }
    }

    // What writes queued now are paid out of: `paid`, when it is counted
    // on the volume the torrent writes to; handed back when it is counted
    // on another, where its files were before they moved.
    pub(crate) fn pay(&self, paid: Option<Paid>) -> Result<Paying, Paid> {
        let generation = self.generation();
        let volume = generation.submitter.volume();
        let reservation = match paid {
            Some(mut paid) if Arc::ptr_eq(paid.reservation.volume(), volume) => {
                std::mem::replace(&mut paid.reservation, Reservation::none(Arc::clone(volume)))
            }
            Some(paid) => return Err(paid),
            None => Reservation::none(Arc::clone(volume)),
        };
        Ok(Paying {
            generation,
            reservation,
        })
    }

    // Queue writes through `queue`, paid first out of `paying`: what the
    // writes do not take is given back as `queue` returns. Past it, each
    // write is counted on its volume as it fits, the caller standing still
    // for the writer until it does.
    pub(crate) fn queue_paying<T>(
        &self,
        paying: Paying,
        queue: impl FnOnce(&mut Batch<'_>) -> T,
    ) -> T {
        let Paying {
            generation,
            reservation,
        } = paying;
        let mut open = lock(&generation.open);
        let mut batch = Batch {
            generation: &generation,
            open: &mut open,
            paid: Paid {
                reservation,
                disk: Arc::clone(&self.current.disk),
            },
        };
        queue(&mut batch)
    }

    // Queue writes no reservation pays for: each is counted as it fits.
    pub(crate) fn queue<T>(&self, queue: impl FnOnce(&mut Batch<'_>) -> T) -> T {
        let generation = self.generation();
        let reservation = Reservation::none(Arc::clone(generation.submitter.volume()));
        self.queue_paying(
            Paying {
                generation,
                reservation,
            },
            queue,
        )
    }

    // Wait until everything queued so far has been written, for a thread
    // that may wait on a disk. True when all of it was.
    pub(crate) fn settle(&self) -> bool {
        let generation = lock(&self.current.state).generation.clone();
        let Some(generation) = generation else {
            return true;
        };
        let barrier = generation.submitter.barrier();
        barrier.wait_aside();
        !barrier.unwritten()
    }

    // Wait until everything queued so far has been written, then let the
    // files go: the next write opens them where they are then. For a thread
    // that may wait on a disk. True when all of it was written.
    pub(crate) fn drain(&self) -> bool {
        let generation = lock(&self.current.state).generation.take();
        let Some(generation) = generation else {
            return true;
        };
        let barrier = generation.submitter.barrier();
        barrier.wait_aside();
        let written = !barrier.unwritten();
        generation.abandon().wait_aside();
        written
    }

    // The files are under `dir` from now on; for a thread that may wait on
    // a disk, which reads the volume `dir` is on. Called once the writes
    // were drained.
    pub(crate) fn moved(&self, dir: &Path) {
        if let Some(capacity) = &self.current.disk.capacity {
            capacity.read_volume(dir);
        }
        lock(&self.current.state).dir = dir.to_path_buf();
    }

    // The read turns of the volume the files are under now, from memory
    // alone: the files' reads share them with every other reader of that
    // volume, usenet's too. A volume whose kind is not known reads as a
    // solid-state one.
    pub(crate) fn read_turns(&self) -> weaver_torrent::storage::ReadTurns {
        let dir = lock(&self.current.state).dir.clone();
        let disk = &self.current.disk;
        let volume = disk.volume(&dir);
        let reads = match &disk.capacity {
            Some(capacity) => capacity.volume_reads(&volume),
            None => volume.reads(|| crate::operations::disk::StorageKind::Unknown),
        };
        weaver_torrent::storage::ReadTurns::new(Arc::clone(reads.turns()), reads.whole_pieces())
    }

    // The same, for a torrent that writes nothing and whose volume is read
    // when it next does.
    pub(crate) fn placed(&self, dir: &Path) {
        lock(&self.current.state).dir = dir.to_path_buf();
    }

    // Whatever is still queued is given up: it is not written, the pieces
    // it held are dropped untold, and the files are let go. The barrier
    // passes once nothing of it can touch the disk again.
    pub(crate) fn abandon(&self) -> Option<Barrier> {
        let generation = lock(&self.current.state).generation.take()?;
        Some(generation.abandon())
    }

    // The writes given up, awaited until nothing of them can touch the
    // disk again.
    pub(crate) async fn forget(&self) {
        let Some(barrier) = self.abandon() else {
            return;
        };
        let (told, passed) = tokio::sync::oneshot::channel();
        barrier.then(move || {
            let _ = told.send(());
        });
        let _ = passed.await;
    }

    // The torrent has every piece: it holds its volume's writer no longer.
    // What is queued is still written.
    pub(crate) fn let_go(&self) {
        let generation = lock(&self.current.state).generation.take();
        drop(generation);
    }

    // Whether anything queued has yet to be written.
    pub(crate) fn out(&self) -> bool {
        lock(&self.current.state)
            .generation
            .as_ref()
            .is_some_and(|generation| !generation.submitter.barrier().returned())
    }

    // The file at `path` made at least `length` long, after every write
    // queued before: run on the writer, waited for here. For a thread that
    // may wait on a disk.
    pub(crate) fn grow(&self, path: PathBuf, length: u64) -> io::Result<()> {
        let generation = self.generation();
        let (told, grown) = mpsc::sync_channel(1);
        let at = path.clone();
        generation.submitter.ask(path, move || {
            let _ = told.send(grow_file(&at, length));
        });
        drop(generation);
        grown.recv().unwrap_or_else(|_| {
            Err(io::Error::other(
                "the torrent's writes were given up before its file was made whole",
            ))
        })
    }

    // Run `run` off the writer's thread, where it may stand still: on the
    // runtime's blocking threads, or here when there is no runtime.
    pub(crate) fn aside(&self, run: impl FnOnce() + Send + 'static) {
        match &self.current.disk.runtime {
            Some(runtime) => {
                runtime.spawn_blocking(run);
            }
            None => run(),
        }
    }

    // The writes of a test's torrent, through writers of their own.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        TorrentDisk::alone().writes(1, Path::new(""))
    }

    #[cfg(test)]
    pub(crate) fn volume(&self) -> Arc<Volume> {
        Arc::clone(self.generation().submitter.volume())
    }

    #[cfg(test)]
    pub(crate) fn disk(&self) -> &Arc<TorrentDisk> {
        &self.current.disk
    }
}

// The room writes about to be queued are paid out of, on the volume they
// go to.
pub(crate) struct Paying {
    generation: Arc<Generation>,
    reservation: Reservation,
}

// Writes being queued for one torrent, paid out of one reservation.
pub(crate) struct Batch<'a> {
    generation: &'a Arc<Generation>,
    open: &'a mut VecDeque<u64>,
    paid: Paid,
}

impl Batch<'_> {
    // `bytes` written at `offset` of `file`, which is opened at the path
    // `path` gives, and made when it is not there.
    pub(crate) fn write(
        &mut self,
        file: usize,
        offset: u64,
        bytes: Bytes,
        path: impl FnOnce() -> PathBuf,
    ) {
        let file = file as u64;
        let submitter = &self.generation.submitter;
        // Open already: the writer holds its handle, and needs no path.
        let create = match self.open.iter().position(|open| *open == file) {
            Some(at) => {
                if at + 1 != self.open.len() {
                    self.open.remove(at);
                    self.open.push_back(file);
                }
                None
            }
            None => {
                self.open.push_back(file);
                if self.open.len() > OPEN_FILES
                    && let Some(oldest) = self.open.pop_front()
                {
                    submitter.release(oldest);
                }
                Some(Create {
                    path: path(),
                    length: None,
                    open: open_piece_file,
                })
            }
        };
        submitter.write_from(&mut self.paid.reservation, file, offset, bytes, create);
    }

    // Run `land` on the writer once every write queued before it is
    // written: it tells `done` what the piece came to. When the writes
    // failed instead, `done` hears their error; when they were given up, it
    // is dropped.
    pub(crate) fn then(&mut self, done: Done, land: impl FnOnce(Done) + Send + 'static) {
        let landing = Landing {
            land: Some(land),
            done: Some(done),
            lost: Arc::clone(&self.generation.lost),
        };
        self.generation
            .submitter
            .ask(PathBuf::new(), move || landing.run());
    }

    // Everything queued in the batch so far.
    pub(crate) fn barrier(&self) -> Barrier {
        self.generation.submitter.barrier()
    }
}

// What follows a piece's writes, run on the writer after them. `land` is
// held as it is: the step the writer runs is boxed once, with it inside.
struct Landing<L: FnOnce(Done)> {
    land: Option<L>,
    done: Option<Done>,
    lost: Arc<Lost>,
}

impl<L: FnOnce(Done)> Landing<L> {
    fn run(mut self) {
        if let (Some(land), Some(done)) = (self.land.take(), self.done.take()) {
            land(done);
        }
    }
}

impl<L: FnOnce(Done)> Drop for Landing<L> {
    // Dropped unrun: the writes before it failed or were given up.
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            self.lost.lose(done);
        }
    }
}

// A torrent's file opened for writing, made with the folders it is in when
// it is not there, as the torrent's files have always been made. A name the
// system will not take fails the torrent: it is no refusal of one file.
fn open_piece_file(path: &Path, _length: Option<u64>) -> io::Result<Opened> {
    let file = open_for_write(path).map_err(|error| {
        if error.kind() == io::ErrorKind::InvalidFilename {
            io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
        } else {
            error
        }
    })?;
    Ok(Opened {
        file,
        length: None,
        steps: OpenSteps::default(),
    })
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

// The file made `length` long when it is shorter, so a file whose pieces
// are all had is there whole, an empty one among them. A longer one stays
// as it is.
fn grow_file(path: &Path, length: u64) -> io::Result<()> {
    let file = open_for_write(path)?;
    if file.metadata()?.len() < length {
        file.set_len(length)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
