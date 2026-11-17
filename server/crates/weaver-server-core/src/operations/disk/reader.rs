// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The reads of one volume, beside its writer: every owner that reads files
// on the volume (usenet's verify and extract, a torrent's uploads and
// checks) takes a turn here, so the reads at work on the volume at once are
// bounded for the whole process, not per caller.
//
// A solid-state volume, and one whose kind is not known, has as many turns
// as a torrent session had read threads (`SOLID_READERS`). A spinning one
// has one: one read at a time, so its head does not move between readers,
// and its readers read large (`VolumeReads::whole_pieces`). The kind is the
// one the disk owner holds for the volume's device when its reads are
// first asked for (`StorageCapacity::volume_reads`); nothing here looks at
// a disk.
//
// A turn is a permit of a `tokio::sync::Semaphore`: taken without waiting
// when one is free, one compare-and-swap; waited for on the async runtime
// by a caller that may not block, or on the calling thread by one that may.
// The read itself runs on the thread that holds the turn: a turn hands
// nothing to another thread. A torrent's reads take the same semaphore
// through `weaver_torrent::storage::ReadTurns`, handed over once when the
// torrent starts.

#[cfg(test)]
use std::io;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};

use super::StorageKind;
#[cfg(test)]
use super::writer::Submitter;

// Reads at once on a solid-state volume: what one torrent session read
// with before, now shared by every reader of the volume.
pub(crate) const SOLID_READERS: usize = 8;
// Reads at once on a spinning volume.
pub(crate) const SPINNING_READERS: usize = 1;

// The read turns of one volume.
pub(crate) struct VolumeReads {
    turns: Arc<Semaphore>,
    spinning: bool,
}

impl VolumeReads {
    // The turns of a volume of `kind`. Only a volume known to spin has one
    // turn; any other, an unknown one included, reads as a solid-state one.
    pub(crate) fn of(kind: StorageKind) -> Self {
        let spinning = kind == StorageKind::Rotational;
        Self {
            turns: Arc::new(Semaphore::new(if spinning {
                SPINNING_READERS
            } else {
                SOLID_READERS
            })),
            spinning,
        }
    }

    // The volume's turns, for a caller that waits for one on the async
    // runtime and then reads where it is.
    pub(crate) fn turns(&self) -> &Arc<Semaphore> {
        &self.turns
    }

    // How many reads the volume takes at once.
    #[cfg(test)]
    pub(crate) fn readers(&self) -> usize {
        if self.spinning {
            SPINNING_READERS
        } else {
            SOLID_READERS
        }
    }

    // Whether a reader reads a whole piece of what it reads at a time: a
    // spinning volume's readers do, so one read is one seek.
    pub(crate) fn whole_pieces(&self) -> bool {
        self.spinning
    }

    // A turn, waited for on this thread: for a caller off the async runtime
    // that may block. Never called on the runtime.
    #[cfg(test)]
    pub(crate) fn turn_here(&self) -> OwnedSemaphorePermit {
        self.owned_here()
    }

    // A turn, waited for on this thread, kept until dropped: for a reader
    // that holds one across a run of reads (`Reading`). Never called on the
    // async runtime.
    pub(crate) fn owned_here(&self) -> OwnedSemaphorePermit {
        match Arc::clone(&self.turns).try_acquire_owned() {
            Ok(turn) => turn,
            Err(_) => block_here(Arc::clone(&self.turns).acquire_owned())
                .expect("a volume's read turns are never closed"),
        }
    }

    // A turn for one read, waited for on this thread. Never called on the
    // async runtime.
    pub(crate) fn one_here(&self) -> SemaphorePermit<'_> {
        match self.turns.try_acquire() {
            Ok(turn) => turn,
            Err(_) => {
                block_here(self.turns.acquire()).expect("a volume's read turns are never closed")
            }
        }
    }

    // `read` run in a turn, after every operation `submitter` queued
    // through `seq` has returned (`Submitter::wait_for`): a read of a file
    // whose writes are still queued sees them. The caller's thread waits
    // for the writes and the turn, and reads.
    #[cfg(test)]
    pub(crate) fn read_after<R>(
        &self,
        submitter: &Submitter,
        seq: u64,
        read: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        submitter.wait_for(seq)?;
        let _turn = self.turn_here();
        read()
    }
}

// Wait for `future` on this thread, parked between polls: the wait of a
// reader off the async runtime for a turn another reader holds. Only when
// no turn is free; a free one is taken without it.
fn block_here<F: Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

// One usenet reader's turns across a run of reads: a member's check, an
// extraction's reads of its volumes, a repair's reads. On a volume with
// more than one turn it takes one at its first read and keeps it, so a
// read in the run costs nothing more than a look at what it holds; it lets
// it go when the run waits on anything but the disk (`let_go`), and when it
// ends. On a spinning volume, whose one turn every reader of the volume
// shares, each read takes the turn and gives it back, so a torrent's upload
// is never held behind a whole run. It holds at most one turn: a read on
// another volume gives the one it holds back before it waits for one there.
#[derive(Debug, Default)]
pub(crate) struct Reading {
    held: Option<OwnedSemaphorePermit>,
}

impl Reading {
    // `read`, in a turn of `reads`.
    pub(crate) fn read<R>(&mut self, reads: &VolumeReads, read: impl FnOnce() -> R) -> R {
        if reads.spinning {
            self.held = None;
            let _turn = reads.one_here();
            return read();
        }
        if !self.holds(reads) {
            self.held = None;
            self.held = Some(reads.owned_here());
        }
        read()
    }

    // Whether a read of `reads` may go on in the turn held: one of a volume
    // with more than one turn, held already.
    pub(crate) fn holds(&self, reads: &VolumeReads) -> bool {
        !reads.spinning
            && self
                .held
                .as_ref()
                .is_some_and(|held| Arc::ptr_eq(held.semaphore(), &reads.turns))
    }

    // A turn a reader waited for where it held nothing else
    // (`VolumeReads::owned_here`), kept for its next reads. The one held
    // before goes back.
    pub(crate) fn keep(&mut self, turn: OwnedSemaphorePermit) {
        self.held = Some(turn);
    }

    // The run waits on something other than the disk: the turn goes back
    // until its next read.
    pub(crate) fn let_go(&mut self) {
        self.held = None;
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> bool {
        self.held.is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::channel;

    use super::{SOLID_READERS, SPINNING_READERS, VolumeReads};
    use crate::operations::disk::StorageKind;
    use crate::operations::disk::volume::Volume;
    use crate::operations::disk::writer::{Create, OpenSteps, Opened, VolumeWriters};

    // A spinning volume reads one at a time; any other, an unknown one
    // included, as many at once as a solid-state one.
    #[test]
    fn a_spinning_volume_has_one_turn_and_reads_whole_pieces() {
        let spinning = VolumeReads::of(StorageKind::Rotational);
        assert_eq!(spinning.turns().available_permits(), SPINNING_READERS);
        assert_eq!(spinning.readers(), 1);
        assert!(spinning.whole_pieces());
        for kind in [
            StorageKind::Solid,
            StorageKind::Network,
            StorageKind::Unknown,
        ] {
            let reads = VolumeReads::of(kind);
            assert_eq!(reads.turns().available_permits(), SOLID_READERS, "{kind:?}");
            assert_eq!(reads.readers(), SOLID_READERS, "{kind:?}");
            assert!(!reads.whole_pieces(), "{kind:?}");
        }
    }

    // The readers of one volume never read more at once than it has turns,
    // whoever they are: each holds a turn for its read, and the next waits
    // for one to come back.
    #[test]
    fn the_reads_at_work_on_a_volume_never_pass_its_turns() {
        for kind in [StorageKind::Rotational, StorageKind::Solid] {
            let reads = Arc::new(VolumeReads::of(kind));
            let most = reads.readers();
            let at_work = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let (started_tx, started_rx) = channel::<()>();
            let (release_tx, release_rx) = channel::<()>();
            let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
            let readers = most * 3;
            let threads: Vec<_> = (0..readers)
                .map(|_| {
                    let (reads, at_work, peak) = (reads.clone(), at_work.clone(), peak.clone());
                    let (started, release) = (started_tx.clone(), release_rx.clone());
                    std::thread::spawn(move || {
                        let _turn = reads.turn_here();
                        let now = at_work.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        started.send(()).unwrap();
                        release.lock().unwrap().recv().unwrap();
                        at_work.fetch_sub(1, Ordering::SeqCst);
                    })
                })
                .collect();
            // Every turn is taken before any comes back; each one released
            // lets exactly one more reader start.
            for _ in 0..most {
                started_rx.recv().unwrap();
            }
            assert_eq!(reads.turns().available_permits(), 0, "{kind:?}");
            for _ in most..readers {
                release_tx.send(()).unwrap();
                started_rx.recv().unwrap();
            }
            for _ in 0..most {
                release_tx.send(()).unwrap();
            }
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(peak.load(Ordering::SeqCst), most, "{kind:?}");
            assert_eq!(reads.turns().available_permits(), most, "{kind:?}");
        }
    }

    // A usenet reader on a solid-state volume takes one turn at its first
    // read and reads on in it; on a spinning one each read takes the one
    // turn and gives it back. A read on another volume gives back the turn
    // held first, and letting go gives back the last.
    #[test]
    fn a_reading_holds_one_turn_on_solid_state_and_one_per_read_on_a_spinning_volume() {
        let solid = VolumeReads::of(StorageKind::Solid);
        let other = VolumeReads::of(StorageKind::Unknown);
        let spinning = VolumeReads::of(StorageKind::Rotational);
        let mut reading = super::Reading::default();
        for at in 0..3 {
            let free = reading.read(&solid, || solid.turns().available_permits());
            assert_eq!(free, SOLID_READERS - 1, "read {at}");
            assert!(reading.holds(&solid));
        }
        assert_eq!(solid.turns().available_permits(), SOLID_READERS - 1);
        let free = reading.read(&other, || {
            (
                solid.turns().available_permits(),
                other.turns().available_permits(),
            )
        });
        assert_eq!(free, (SOLID_READERS, SOLID_READERS - 1));
        for at in 0..3 {
            let free = reading.read(&spinning, || {
                (
                    spinning.turns().available_permits(),
                    other.turns().available_permits(),
                )
            });
            assert_eq!(free, (0, SOLID_READERS), "read {at}");
            assert_eq!(spinning.turns().available_permits(), SPINNING_READERS);
            assert!(!reading.held() && !reading.holds(&spinning));
        }
        reading.read(&solid, || ());
        reading.let_go();
        assert!(!reading.held());
        for reads in [&solid, &other, &spinning] {
            assert_eq!(reads.turns().available_permits(), reads.readers());
        }
    }

    // A reader off the async runtime reads once a turn another holds comes
    // back, whether it came to wait for it or found it free.
    #[test]
    fn a_reader_reads_once_a_turn_held_elsewhere_comes_back() {
        let reads = Arc::new(VolumeReads::of(StorageKind::Rotational));
        let held = reads.turn_here();
        let (read_tx, read_rx) = channel();
        let reader = {
            let reads = Arc::clone(&reads);
            std::thread::spawn(move || {
                let mut reading = super::Reading::default();
                reading.read(&reads, || read_tx.send(()).unwrap());
            })
        };
        drop(held);
        read_rx.recv().unwrap();
        reader.join().unwrap();
        assert_eq!(reads.turns().available_permits(), SPINNING_READERS);
    }

    // A read after queued writes to the same file sees them: it waits at
    // the writer's barrier for the operations through the write's number,
    // then reads in a turn. The bytes are those a read made after the
    // writes ended reads (`Submitter::read_after`, the writer's own read).
    #[test]
    fn a_read_after_queued_writes_sees_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        let writers = VolumeWriters::reporting(|_| {});
        let volume = Arc::new(Volume::new(None, "test".to_owned()));
        let submitter = writers.test_submitter(&volume, 1);
        let reads = VolumeReads::of(StorageKind::Rotational);
        fn open(path: &std::path::Path, _length: Option<u64>) -> std::io::Result<Opened> {
            Ok(Opened {
                file: std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)?,
                length: None,
                steps: OpenSteps::default(),
            })
        }
        let create = || Create {
            path: path.clone(),
            length: None,
            open,
        };
        let mut seq = 0;
        for (at, chunk) in [b"abcd", b"efgh", b"ijkl"].into_iter().enumerate() {
            seq = submitter.write(
                1,
                at as u64 * 4,
                bytes::Bytes::from_static(chunk),
                Some(create()),
            );
        }
        let read = reads
            .read_after(&submitter, seq, || {
                std::fs::read(&path).map(|bytes| bytes[2..10].to_vec())
            })
            .unwrap();
        assert!(submitter.barrier_at(seq).passed());
        assert_eq!(read, b"cdefghij");
        // The writer's own read after the same writes reads the same bytes.
        assert_eq!(submitter.read_after(seq, path, 2, 8).unwrap(), read);
        assert_eq!(reads.turns().available_permits(), SPINNING_READERS);
    }
}
