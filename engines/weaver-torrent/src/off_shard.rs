// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Disk work handed off a uTP thread, and its results handed back.
//!
//! A peer on a uTP thread reads the blocks it uploads on a blocking thread
//! of the session's runtime, as it may not block. It first takes a read turn
//! of its files' volume ([`ReadTurns`]) on its own thread, without blocking,
//! and hands the read and the turn over together: the turns bound the reads
//! at work on the volume, whoever reads, and the thread that runs a read
//! holds its turn until it ends. The results come back to the uTP thread
//! together: each finished read joins the thread's list of finished work,
//! and only the first to join it while no hand-back is on its way wakes the
//! thread. One task there then wakes every waiting peer, on that thread,
//! with no further cross-thread wake. This is libtorrent's hand-back of
//! finished disk jobs to its network thread: one `call_job_handlers` in
//! flight at a time, finished jobs gathered meanwhile
//! (mmap_disk_io.cpp:1801-1810 and :1822, v2.0.15).

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use futures::task::AtomicWaker;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::session_stats::disk::DiskStats;
use crate::storage::ReadTurns;

/// A read handed to a blocking thread for a peer on a uTP thread, with the
/// turn it holds while it runs. Dropped without running, as a runtime that
/// shut down drops the work it never started, or run and panicked, it wakes
/// its reader with no result.
struct ReadWork<F, R: Send + 'static> {
    work: Option<F>,
    slot: Arc<Slot<R>>,
    stats: Arc<DiskStats>,
    _turn: tokio::sync::OwnedSemaphorePermit,
}

impl<F: FnOnce(&DiskStats) -> R, R: Send + 'static> ReadWork<F, R> {
    fn run(mut self) {
        if let Some(work) = self.work.take() {
            self.stats.read_taken();
            let result = work(&self.stats);
            self.slot.fill(Some(result));
        }
    }
}

impl<F, R: Send + 'static> Drop for ReadWork<F, R> {
    // A read that gave its result settles nothing (`Slot::settle` once
    // given).
    fn drop(&mut self) {
        if self.work.take().is_some() {
            self.stats.read_taken();
        }
        self.slot.settle();
    }
}

// Where a read's result waits for its reader.
struct Slot<R> {
    // Set once, by the read or, when it never gave one, by its work's drop.
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

    // From the work's drop: a read that gave no result, unrun or panicked,
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

/// Runs `f` on a blocking thread of `runtime` in a turn of `turns`, taken
/// here first without blocking, and waits for its result on this uTP
/// thread's hand-back. None when the read was dropped unrun, as when the
/// session's runtime shut down, or panicked. A reader dropped while its
/// read runs leaves the read its thread and its turn until it ends.
pub(crate) async fn read_off_shard<F, R>(
    runtime: &tokio::runtime::Handle,
    turns: &ReadTurns,
    stats: &Arc<DiskStats>,
    f: F,
) -> Option<R>
where
    F: FnOnce(&DiskStats) -> R + Send + 'static,
    R: Send + 'static,
{
    let turn = turns.owned_turn(stats).await;
    stats.read_queued();
    let slot = Arc::new(Slot {
        given: AtomicBool::new(false),
        result: Mutex::new(None),
        waker: AtomicWaker::new(),
        inbox: inbox(),
    });
    let work = ReadWork {
        work: Some(f),
        slot: slot.clone(),
        stats: stats.clone(),
        _turn: turn,
    };
    // A runtime shut down drops the work where it is, which settles it.
    drop(runtime.spawn_blocking(move || work.run()));
    ReadDone(slot).await
}

/// A read's result, once the uTP thread's hand-back has it.
struct ReadDone<R>(Arc<Slot<R>>);

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{INBOX, read_off_shard};
    use crate::session_stats::disk::DiskStats;
    use crate::storage::ReadTurns;

    fn shard() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    fn session(threads: usize) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .build()
            .unwrap()
    }

    fn turns(count: usize) -> ReadTurns {
        ReadTurns::new(Arc::new(tokio::sync::Semaphore::new(count)), false)
    }

    // Waits on this thread until every turn of `turns` is back: every read
    // handed off with one has ended and handed its result back.
    fn every_turn_back(turns: &ReadTurns, count: usize) {
        let semaphore = turns.semaphore().clone();
        std::thread::scope(|s| {
            s.spawn(|| {
                drop(futures::executor::block_on(
                    semaphore.acquire_many(u32::try_from(count).unwrap()),
                ))
            })
            .join()
            .unwrap();
        });
    }

    fn rings() -> usize {
        INBOX.with(|inbox| {
            inbox
                .borrow()
                .as_ref()
                .unwrap()
                .rings
                .load(Ordering::Relaxed)
        })
    }

    // Reads that finish while the uTP thread is busy come back in one wake
    // of the thread, each reader with its own result.
    #[test]
    fn reads_finished_together_wake_the_thread_once() {
        let session = session(1);
        let turns = turns(8);
        let stats = Arc::new(DiskStats::default());
        let (go, gate) = std::sync::mpsc::channel::<()>();
        let gate = Arc::new(std::sync::Mutex::new(gate));
        let (results, rings) = shard().block_on(async {
            let mut reads: Vec<_> = (0..8)
                .map(|i| {
                    let gate = gate.clone();
                    Box::pin(read_off_shard(
                        session.handle(),
                        &turns,
                        &stats,
                        move |_| {
                            gate.lock().unwrap().recv().unwrap();
                            i * 10
                        },
                    ))
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
            every_turn_back(&turns, 8);
            let mut results = Vec::new();
            for read in reads {
                results.push(read.await.unwrap());
            }
            (results, rings())
        });
        assert_eq!(results, (0..8).map(|i| i * 10).collect::<Vec<_>>());
        assert_eq!(rings, 1, "one cross-thread wake for eight reads");
        assert_eq!(stats.snapshot().permit_acquisitions, 8);
        // Every read handed off began: none is still counted as queued.
        assert_eq!(stats.snapshot().read_jobs_queued, 0);
    }

    // Two uTP threads reading through the one volume's turns each get their
    // own reads back, on their own thread and in one wake of it.
    #[test]
    fn reads_from_two_threads_come_back_to_their_own() {
        let session = session(2);
        let turns = turns(8);
        let stats = Arc::new(DiskStats::default());
        // Both threads hold their four turns before either waits for all
        // eight: a wait for all of them takes each turn as it comes free,
        // and would take turns the other thread's reads have yet to.
        let held = std::sync::Barrier::new(2);
        let on_thread = |base: usize| {
            let (go, gate) = std::sync::mpsc::channel::<()>();
            let gate = Arc::new(std::sync::Mutex::new(gate));
            let (turns, stats, session, held) = (&turns, &stats, &session, &held);
            shard().block_on(async move {
                let mut reads: Vec<_> = (0..4)
                    .map(|i| {
                        let gate = gate.clone();
                        Box::pin(read_off_shard(session.handle(), turns, stats, move |_| {
                            gate.lock().unwrap().recv().unwrap();
                            base + i
                        }))
                    })
                    .collect();
                for read in &mut reads {
                    assert!(futures::poll!(read).is_pending());
                }
                held.wait();
                for _ in 0..4 {
                    go.send(()).unwrap();
                }
                // Both threads' reads end while this thread is held.
                every_turn_back(turns, 8);
                let mut results = Vec::new();
                for read in reads {
                    results.push(read.await.unwrap());
                }
                (results, rings())
            })
        };
        let (first, second) = std::thread::scope(|s| {
            let first = s.spawn(|| on_thread(100));
            let second = s.spawn(|| on_thread(200));
            (first.join().unwrap(), second.join().unwrap())
        });
        assert_eq!(first, (vec![100, 101, 102, 103], 1));
        assert_eq!(second, (vec![200, 201, 202, 203], 1));
    }

    // A read whose reader is dropped while it runs, as when its peer's
    // connection ends, keeps its turn until it ends: the turns bound the
    // reads at work, not the readers waiting on them. The next read waits
    // for that turn and runs after it.
    #[test]
    fn a_read_dropped_midway_keeps_its_turn_until_it_ends() {
        let session = session(1);
        let turns = turns(1);
        let stats = Arc::new(DiskStats::default());
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let first_ended = Arc::new(AtomicBool::new(false));
        let got = shard().block_on(async {
            let ended = first_ended.clone();
            let mut first = Box::pin(read_off_shard(
                session.handle(),
                &turns,
                &stats,
                move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    ended.store(true, Ordering::Release);
                },
            ));
            assert!(futures::poll!(first.as_mut()).is_pending());
            started_rx.recv().unwrap();
            // The connection ends while its read runs.
            drop(first);
            let ended = first_ended.clone();
            let mut next = Box::pin(read_off_shard(
                session.handle(),
                &turns,
                &stats,
                move |_| ended.load(Ordering::Acquire),
            ));
            assert!(futures::poll!(next.as_mut()).is_pending());
            assert_eq!(
                turns.semaphore().available_permits(),
                0,
                "the dropped read still holds the one turn; the next one waits"
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
    // gives its reader no result instead of leaving it waiting, and its
    // turn comes back.
    #[test]
    fn a_read_dropped_unrun_ends_its_wait() {
        let session = session(1);
        let handle = session.handle().clone();
        session.shutdown_background();
        let turns = turns(1);
        let stats = Arc::new(DiskStats::default());
        let got = shard().block_on(read_off_shard(&handle, &turns, &stats, |_| 1));
        assert_eq!(got, None);
        assert_eq!(turns.semaphore().available_permits(), 1);
        assert_eq!(stats.snapshot().read_jobs_queued, 0);
    }

    // A read that panics gives its reader no result, and its turn comes
    // back.
    #[test]
    fn a_read_that_panics_ends_its_wait() {
        let session = session(1);
        let turns = turns(1);
        let stats = Arc::new(DiskStats::default());
        let got = shard().block_on(read_off_shard(
            session.handle(),
            &turns,
            &stats,
            |_| -> u32 { panic!("the block could not be read") },
        ));
        assert_eq!(got, None);
        every_turn_back(&turns, 1);
    }
}
