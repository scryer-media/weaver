// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Disk work handed off a uTP thread, and its results handed back.
//!
//! A peer on a uTP thread reads the blocks it uploads on the session's disk
//! threads, as it may not block. Each read is a job on a [`DiskQueue`]; a
//! thread that is busy takes the next reads queued in the same wake. The
//! results come back to the uTP thread together: each finished read joins
//! the thread's list of finished work, and only the first to join it while
//! no hand-back is on its way wakes the thread. One task there then wakes
//! every waiting peer, on that thread, with no further cross-thread wake.
//! This is libtorrent's hand-back of finished disk jobs to its network
//! thread: one `call_job_handlers` in flight at a time, finished jobs
//! gathered meanwhile (mmap_disk_io.cpp:1801-1810 and :1822, v2.0.15).

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::disk::{DiskJob, DiskQueue};

/// A read on a disk thread for a peer on a uTP thread.
pub(crate) struct ReadJob {
    key: usize,
    work: Option<Box<dyn FnOnce() + Send>>,
    // Settled when the job is dropped without running: its reader wakes
    // and finds no result.
    unrun: Option<Arc<dyn Settle>>,
}

trait Settle: Send + Sync {
    fn settle(&self);
}

impl DiskJob for ReadJob {
    fn key(&self) -> usize {
        self.key
    }

    fn run(mut self) {
        if let Some(work) = self.work.take() {
            work();
        }
    }
}

impl Drop for ReadJob {
    fn drop(&mut self) {
        if let Some(unrun) = self.unrun.take() {
            unrun.settle();
        }
    }
}

// Where a read's result waits for its reader.
struct Slot<R> {
    // Set once, by the read or, when it never gave one, by its job's drop.
    given: AtomicBool,
    result: Mutex<Option<Option<R>>>,
    waker: AtomicWaker,
    // The uTP thread's finished work, where the slot is put when filled.
    inbox: Arc<Inbox>,
}

impl<R: Send + 'static> Slot<R> {
    fn fill(self: &Arc<Self>, result: Option<R>) {
        if self.given.swap(true, Ordering::AcqRel) {
            return;
        }
        *self.result.lock() = Some(result);
        self.inbox.finished(self.clone());
    }
}

impl<R: Send + 'static> Settle for Slot<R> {
    // From the job's drop: a read that gave no result, unrun or panicked,
    // wakes its reader with none, directly, as this is no common path.
    fn settle(&self) {
        if self.given.swap(true, Ordering::AcqRel) {
            return;
        }
        *self.result.lock() = Some(None);
        self.waker.wake();
    }
}

trait Wake: Send + Sync {
    fn wake(&self);
}

impl<R: Send + 'static> Wake for Slot<R> {
    fn wake(&self) {
        self.waker.wake();
    }
}

/// The finished work of one uTP thread.
struct Inbox {
    finished: Mutex<Vec<Arc<dyn Wake>>>,
    // A hand-back is on its way to the thread.
    in_flight: AtomicBool,
    ring: Notify,
    // Times a disk thread rang the uTP thread.
    #[cfg(test)]
    rings: AtomicUsize,
}

impl Inbox {
    // On a disk thread: `slot` finished. The thread is woken only when no
    // hand-back is on its way already.
    fn finished(&self, slot: Arc<dyn Wake>) {
        self.finished.lock().push(slot);
        if !self.in_flight.swap(true, Ordering::AcqRel) {
            #[cfg(test)]
            self.rings.fetch_add(1, Ordering::Relaxed);
            self.ring.notify_one();
        }
    }

    // On the uTP thread: wakes every reader whose read finished, for as long
    // as the thread runs.
    async fn hand_back(self: Arc<Self>) {
        let mut taken = Vec::new();
        loop {
            self.ring.notified().await;
            // Cleared before the list is taken: a read finishing after the
            // take rings again.
            self.in_flight.store(false, Ordering::Release);
            std::mem::swap(&mut *self.finished.lock(), &mut taken);
            for slot in taken.drain(..) {
                slot.wake();
            }
        }
    }
}

thread_local! {
    // This thread's finished work, with its hand-back task started on the
    // thread's runtime when first asked for.
    static INBOX: RefCell<Option<Arc<Inbox>>> = const { RefCell::new(None) };
}

// The calling thread's inbox. Called on a uTP thread, inside its runtime.
fn inbox() -> Arc<Inbox> {
    INBOX.with(|inbox| {
        inbox
            .borrow_mut()
            .get_or_insert_with(|| {
                let inbox = Arc::new(Inbox {
                    finished: Mutex::new(Vec::new()),
                    in_flight: AtomicBool::new(false),
                    ring: Notify::new(),
                    #[cfg(test)]
                    rings: AtomicUsize::new(0),
                });
                drop(tokio::spawn(inbox.clone().hand_back()));
                inbox
            })
            .clone()
    })
}

static KEYS: AtomicUsize = AtomicUsize::new(0);

/// A key of its own for a reader: its reads run in order, and other
/// readers' run beside them.
pub(crate) fn reader_key() -> usize {
    KEYS.fetch_add(1, Ordering::Relaxed)
}

/// Runs `f` on `queue`'s threads under `key`, and waits for its result on
/// this uTP thread's hand-back. None when the job was dropped unrun, as
/// when the session's runtime shut down.
pub(crate) fn read_off_shard<F, R>(queue: &DiskQueue<ReadJob>, key: usize, f: F) -> ReadDone<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let slot = Arc::new(Slot {
        given: AtomicBool::new(false),
        result: Mutex::new(None),
        waker: AtomicWaker::new(),
        inbox: inbox(),
    });
    let filled = slot.clone();
    let work = move || {
        let result = f();
        filled.fill(Some(result));
    };
    queue.push(ReadJob {
        key,
        work: Some(Box::new(work)),
        unrun: Some(slot.clone()),
    });
    ReadDone(slot)
}

/// A read's result, once the uTP thread's hand-back has it.
pub(crate) struct ReadDone<R>(Arc<Slot<R>>);

impl<R: Send + 'static> Future for ReadDone<R> {
    type Output = Option<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<R>> {
        let slot = &self.0;
        slot.waker.register(cx.waker());
        match slot.result.lock().take() {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{INBOX, ReadJob, read_off_shard, reader_key};
    use crate::disk::DiskQueue;

    fn shard() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    // Reads that finish while the uTP thread is busy come back in one wake
    // of the thread, each reader with its own result.
    #[test]
    fn reads_finished_together_wake_the_thread_once() {
        let session = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue: DiskQueue<ReadJob> = DiskQueue::new(session.handle().clone(), 4);
        let (go, gate) = std::sync::mpsc::channel::<()>();
        let gate = std::sync::Arc::new(std::sync::Mutex::new(gate));
        let (results, rings) = shard().block_on(async {
            let mut reads: Vec<_> = (0..8)
                .map(|i| {
                    let gate = gate.clone();
                    read_off_shard(&queue, reader_key(), move || {
                        gate.lock().unwrap().recv().unwrap();
                        i * 10
                    })
                })
                .collect();
            // Every reader waits before any read finishes.
            for read in &mut reads {
                assert!(futures::poll!(read).is_pending());
            }
            // The reads finish while this thread is held: each result waits
            // for the one hand-back.
            for _ in 0..8 {
                go.send(()).unwrap();
            }
            std::thread::scope(|s| {
                s.spawn(|| session.block_on(queue.idle())).join().unwrap();
            });
            let mut results = Vec::new();
            for read in reads {
                results.push(read.await.unwrap());
            }
            let rings = INBOX.with(|inbox| {
                let inbox = inbox.borrow();
                inbox
                    .as_ref()
                    .unwrap()
                    .rings
                    .load(std::sync::atomic::Ordering::Relaxed)
            });
            (results, rings)
        });
        assert_eq!(results, (0..8).map(|i| i * 10).collect::<Vec<_>>());
        assert_eq!(rings, 1, "one cross-thread wake for eight reads");
    }

    // A read whose reader is dropped while it runs, as when its peer's
    // connection ends, keeps its disk thread until it ends: the threads bound
    // the reads at work, not the readers waiting on them. The next read
    // queues behind it and runs after it.
    #[test]
    fn a_read_dropped_midway_keeps_its_thread_until_it_ends() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let session = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue: DiskQueue<ReadJob> = DiskQueue::new(session.handle().clone(), 1);
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let first_ended = Arc::new(AtomicBool::new(false));
        let got = shard().block_on(async {
            let ended = first_ended.clone();
            let first = read_off_shard(&queue, reader_key(), move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                ended.store(true, Ordering::Release);
            });
            started_rx.recv().unwrap();
            // The connection ends while its read runs.
            drop(first);
            let ended = first_ended.clone();
            let next = read_off_shard(&queue, reader_key(), move || ended.load(Ordering::Acquire));
            assert_eq!(
                queue.at_work(),
                (1, 1),
                "the dropped read still holds the one thread; the next one waits"
            );
            release_tx.send(()).unwrap();
            next.await
        });
        assert_eq!(
            got,
            Some(true),
            "the next read ran only after the first ended"
        );
    }

    // A read dropped unrun, as when the session's runtime has shut down,
    // gives its reader no result instead of leaving it waiting.
    #[test]
    fn a_read_dropped_unrun_ends_its_wait() {
        let session = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue: DiskQueue<ReadJob> = DiskQueue::new(session.handle().clone(), 1);
        session.shutdown_background();
        let got = shard().block_on(async { read_off_shard(&queue, reader_key(), || 1).await });
        assert_eq!(got, None);
    }
}
