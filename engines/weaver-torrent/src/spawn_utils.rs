// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::disk::{BlockPool, DiskQueue, MAX_QUEUED_DISK_BYTES};
use crate::session_stats::disk::{DiskStats, DiskStatsSnapshot};
use crate::torrent_state::{HashJob, WriteJob};

/// Threads that check pieces. libtorrent's default is one
/// (settings_pack.cpp:353); a second starts only while a backlog waits, as a
/// check here also reads the piece back.
const HASH_THREADS: usize = 2;

/// Whether every disk thread counts against the blocking permits.
///
/// False: block writes, the reads of uTP uploads, and the work done in
/// place under a permit (the reads of TCP uploads, a piece checked and
/// written whole from memory, a torrent's check) each have a bound of their
/// own, as many threads as there are permits, so with N permits up to 3N
/// threads are in a disk call at once, and the piece checks beside them.
///
/// True: they share the one bound, so at most N threads are in a disk call
/// at once, whatever the mix (libtorrent's `aio_threads`, one pool for
/// reads and writes, mmap_disk_io.cpp:1685), and the piece checks of the
/// `hash` queue run beside them on threads of their own (libtorrent's
/// `hashing_threads`).
pub(crate) const ONE_DISK_POOL: bool = false;

/// A tool to limit the number of blocking threads used concurrently to prevent
/// runtime starvation.
///
/// Also shortcuts for single-threaded tokio runtime to simply call the function, unlike
/// "tokio::task::block_in_place" which would panic.
#[derive(Clone)]
pub struct BlockingSpawner {
    allow_block_in_place: bool,
    concurrent_block_in_place_semaphore: Arc<Semaphore>,
    permits: usize,
    // The session's runtime, whose blocking threads take work handed off a
    // thread that must not block.
    handle: tokio::runtime::Handle,
    // The blocks received from peers, and the threads that write them.
    pool: BlockPool,
    disk: DiskQueue<WriteJob>,
    // The threads that check a piece whose last block was written.
    hash: DiskQueue<HashJob>,
    // The threads that read the blocks peers on uTP threads upload.
    reads: DiskQueue<crate::off_shard::ReadJob>,
    // What the session's disk calls, permits and peer sockets did.
    stats: Arc<DiskStats>,
}

// A queue of `threads` disk threads, counted against `permits` when the
// disk work shares one bound.
fn disk_queue<J: crate::disk::DiskJob>(
    handle: &tokio::runtime::Handle,
    threads: usize,
    permits: &Arc<Semaphore>,
) -> DiskQueue<J> {
    if ONE_DISK_POOL {
        DiskQueue::sharing(handle.clone(), threads, permits.clone())
    } else {
        DiskQueue::new(handle.clone(), threads)
    }
}

impl std::fmt::Debug for BlockingSpawner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingSpawner")
            .field("allow_block_in_place", &self.allow_block_in_place)
            .field("permits", &self.permits)
            .finish_non_exhaustive()
    }
}

impl BlockingSpawner {
    pub fn new(max_blocking_threads: usize) -> Self {
        let handle = tokio::runtime::Handle::current();
        let allow_block_in_place = match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => false,
            tokio::runtime::RuntimeFlavor::MultiThread => true,
            _ => true,
        };
        let permits = max_blocking_threads.max(1);
        let semaphore = Arc::new(Semaphore::new(permits));
        let write_queue = disk_queue(&handle, permits, &semaphore);
        let read_queue = disk_queue(&handle, permits, &semaphore);
        Self {
            allow_block_in_place,
            permits,
            pool: BlockPool::new(MAX_QUEUED_DISK_BYTES),
            // As many disk threads as blocking permits (libtorrent's
            // `aio_threads`, a pool of its own beside the network thread).
            disk: write_queue,
            // Checks have threads of their own, so a write never waits behind
            // one (libtorrent `settings_pack::hashing_threads`, a pool beside
            // `aio_threads`, mmap_disk_io.cpp:1685).
            hash: DiskQueue::new(handle.clone(), HASH_THREADS),
            // Uploads are read on threads as many again, or on the same
            // bound when the disk work shares one (libtorrent reads on its
            // `aio_threads` too).
            reads: read_queue,
            concurrent_block_in_place_semaphore: semaphore,
            handle,
            stats: Arc::default(),
        }
    }

    /// The session's disk and socket counters.
    pub fn stats(&self) -> &Arc<DiskStats> {
        &self.stats
    }

    /// The counters now, with the gauges the permits, the queues and the
    /// block buffers keep. Takes no lock.
    pub fn disk_snapshot(&self) -> DiskStatsSnapshot {
        let mut snapshot = self.stats.snapshot();
        let free = self.concurrent_block_in_place_semaphore.available_permits();
        snapshot.permits = self.permits as u64;
        snapshot.permits_in_use = self.permits.saturating_sub(free) as u64;
        snapshot.write_jobs_queued = self.disk.waiting();
        snapshot.hash_jobs_queued = self.hash.waiting();
        snapshot.read_jobs_queued = self.reads.waiting();
        snapshot.queued_write_bytes = self.pool.queued_bytes();
        snapshot
    }

    // A blocking permit, its wait timed only when it has to wait.
    async fn permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        let semaphore = &self.concurrent_block_in_place_semaphore;
        if let Ok(permit) = semaphore.try_acquire() {
            self.stats.permit_taken();
            return permit;
        }
        let since = std::time::Instant::now();
        let permit = semaphore.acquire().await.unwrap();
        self.stats.permit_waited(since);
        permit
    }

    /// The session's block buffers.
    pub(crate) fn pool(&self) -> &BlockPool {
        &self.pool
    }

    /// The threads that write received blocks.
    pub(crate) fn disk(&self) -> &DiskQueue<WriteJob> {
        &self.disk
    }

    /// The threads that check pieces.
    pub(crate) fn hash(&self) -> &DiskQueue<HashJob> {
        &self.hash
    }

    /// Only call this if you can't call the async function block_in_place_with_semaphore
    /// E.g. if you you have non-send objects on the stack.
    pub fn block_in_place<F: FnOnce() -> R, R>(&self, f: F) -> R {
        no_block_on_utp_thread();
        if self.allow_block_in_place {
            return tokio::task::block_in_place(f);
        }

        f()
    }

    /// like "block_in_place" but limit concurrency.
    pub async fn block_in_place_with_semaphore<F: FnOnce() -> R, R>(&self, f: F) -> R {
        no_block_on_utp_thread();
        if self.allow_block_in_place {
            let _permit = self.permit().await;
            return tokio::task::block_in_place(f);
        }

        f()
    }

    /// Like [`Self::block_in_place_with_semaphore`], holding every permit
    /// free once one is had, up to `most`: `f` is given how many it holds,
    /// the threads it may run at once.
    pub async fn block_in_place_with_permits<F: FnOnce(usize) -> R, R>(
        &self,
        most: usize,
        f: F,
    ) -> R {
        no_block_on_utp_thread();
        let semaphore = &self.concurrent_block_in_place_semaphore;
        let mut permits = vec![self.permit().await];
        while permits.len() < most
            && let Ok(permit) = semaphore.try_acquire()
        {
            permits.push(permit);
        }
        let threads = permits.len();
        if self.allow_block_in_place {
            return tokio::task::block_in_place(|| f(threads));
        }
        f(threads)
    }

    /// The most threads a check hashes on: half the blocking permits, at
    /// least one, so peers' reads and piece writes always find the rest.
    /// libtorrent hashes on its own `hashing_threads` pool (default 1)
    /// beside the disk threads; here the two share one pool, and the cap
    /// keeps the share a check takes of it bounded.
    pub fn check_threads(&self) -> usize {
        (self.permits / 2).max(1)
    }

    pub fn semaphore(&self) -> Arc<Semaphore> {
        self.concurrent_block_in_place_semaphore.clone()
    }

    /// Runs `f` on one of the session's blocking threads once it holds a
    /// blocking permit, from any thread, a uTP thread too: the work runs on
    /// the session's pool, never on the calling runtime's own, and disk
    /// work that cannot run in place counts against the same bound as the
    /// work that does. None when the session's runtime shut down before it ran, or it
    /// panicked.
    pub(crate) async fn spawn_blocking_with_permit<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let semaphore = &self.concurrent_block_in_place_semaphore;
        let permit = match Arc::clone(semaphore).try_acquire_owned() {
            Ok(permit) => {
                self.stats.permit_taken();
                permit
            }
            Err(_) => {
                let since = std::time::Instant::now();
                let permit = Arc::clone(semaphore).acquire_owned().await.ok()?;
                self.stats.permit_waited(since);
                permit
            }
        };
        self.handle
            .spawn_blocking(move || {
                let _permit = permit;
                f()
            })
            .await
            .ok()
    }

    /// Runs `fut` on the session's runtime, from any thread: a task that
    /// blocks in place must not land on a uTP thread's runtime.
    pub(crate) fn spawn<F>(&self, fut: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(fut)
    }

    /// Runs `f` on a disk thread for a reader on a uTP thread, its reads in
    /// order under `key`, and hands the result back with the others that
    /// finish meanwhile, in one wake of the uTP thread. None when the
    /// session's runtime shut down before it ran, or it panicked.
    pub(crate) fn read_off_shard<F, R>(&self, key: usize, f: F) -> crate::off_shard::ReadDone<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        crate::off_shard::read_off_shard(&self.reads, key, f)
    }
}

/// A value whose drop never runs on a uTP thread. Dropped on one, it goes
/// to the session's blocking threads and is dropped there: a torrent's
/// storage closes its files when dropped, and libtorrent closes files on
/// its disk threads, never its network thread.
pub(crate) struct DropOffShard<T: Send + 'static> {
    value: std::mem::ManuallyDrop<T>,
    handle: tokio::runtime::Handle,
}

impl<T: Send + 'static> DropOffShard<T> {
    /// `value`, dropped off a uTP thread on `spawner`'s blocking threads.
    pub(crate) fn new(value: T, spawner: &BlockingSpawner) -> Self {
        Self {
            value: std::mem::ManuallyDrop::new(value),
            handle: spawner.handle.clone(),
        }
    }
}

impl<T: Send + 'static> std::ops::Deref for DropOffShard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Send + 'static> Drop for DropOffShard<T> {
    fn drop(&mut self) {
        // SAFETY: `value` is taken once, here, and never used after.
        let value = unsafe { std::mem::ManuallyDrop::take(&mut self.value) };
        if weaver_utp::on_shard_thread() {
            // A runtime shut down drops the closure where it is.
            drop(self.handle.spawn_blocking(move || drop(value)));
        } else {
            drop(value);
        }
    }
}

/// Checks, in debug builds and tests, that the calling thread may block: a
/// uTP thread may not, as every stream on it would wait.
#[track_caller]
pub(crate) fn no_block_on_utp_thread() {
    if cfg!(any(test, debug_assertions)) {
        assert!(
            !weaver_utp::on_shard_thread(),
            "a blocking call on a uTP thread"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::BlockingSpawner;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_check_hashes_on_at_most_half_the_blocking_permits() {
        assert_eq!(BlockingSpawner::new(16).check_threads(), 8);
        assert_eq!(BlockingSpawner::new(3).check_threads(), 1);
        assert_eq!(BlockingSpawner::new(1).check_threads(), 1);
        assert_eq!(BlockingSpawner::new(0).check_threads(), 1);
    }

    // Work handed off another runtime's thread, as a uTP thread's, runs on
    // the session's runtime: a task there may block in place, and a
    // blocking call runs on the session's pool.
    #[test]
    fn work_handed_from_another_runtime_runs_on_the_sessions() {
        let session = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let spawner = session.block_on(async { BlockingSpawner::new(4) });
        let other = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (in_place, flavor) = other.block_on(async {
            let in_place = spawner
                .spawn(async { tokio::task::block_in_place(|| 7) })
                .await
                .unwrap();
            let flavor = spawner
                .spawn_blocking_with_permit(|| tokio::runtime::Handle::current().runtime_flavor())
                .await
                .unwrap();
            (in_place, flavor)
        });
        assert_eq!(in_place, 7);
        assert_eq!(flavor, tokio::runtime::RuntimeFlavor::MultiThread);
    }

    // Blocking work handed off with a permit holds it while it runs: with
    // every permit held, the next does not start until one comes back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_handed_off_with_a_permit_counts_against_the_blocking_bound() {
        use std::sync::mpsc::channel;

        let spawner = BlockingSpawner::new(2);
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel::<()>();
        let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
        let job = |n: u32| {
            let (started, release) = (started_tx.clone(), release_rx.clone());
            let spawner = spawner.clone();
            tokio::spawn(async move {
                spawner
                    .spawn_blocking_with_permit(move || {
                        started.send(n).unwrap();
                        release.lock().unwrap().recv().unwrap();
                        n
                    })
                    .await
            })
        };
        let (first, second) = (job(1), job(2));
        let started = tokio::task::spawn_blocking(move || {
            let two = [started_rx.recv().unwrap(), started_rx.recv().unwrap()];
            (two, started_rx)
        });
        let (mut two, started_rx) = started.await.unwrap();
        two.sort_unstable();
        assert_eq!(two, [1, 2]);
        assert_eq!(spawner.semaphore().available_permits(), 0);

        // The third waits for a permit, which only a finished job gives.
        let third = job(3);
        for _ in 0..3 {
            release_tx.send(()).unwrap();
        }
        assert_eq!(first.await.unwrap(), Some(1));
        assert_eq!(second.await.unwrap(), Some(2));
        assert_eq!(third.await.unwrap(), Some(3));
        assert_eq!(started_rx.try_recv(), Ok(3));
        assert_eq!(spawner.semaphore().available_permits(), 2);
    }

    // Says, when dropped, whether it was dropped on a uTP thread.
    struct Dropped(Option<tokio::sync::oneshot::Sender<bool>>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(weaver_utp::on_shard_thread());
            }
        }
    }

    // A value dropped by a task on a uTP thread, as a peer's task drops the
    // last hold of a torrent's storage, is dropped on the session's
    // blocking threads instead; dropped anywhere else, it is dropped there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_value_dropped_on_a_utp_thread_is_dropped_off_it() {
        use std::net::Ipv4Addr;

        let spawner = BlockingSpawner::new(2);
        let factory = crate::test_support::socket_factory();
        let bind = |shards| {
            crate::listen::bind_utp(
                factory.as_ref(),
                (Ipv4Addr::LOCALHOST, 0).into(),
                weaver_utp::SocketOpts {
                    shards,
                    ..Default::default()
                },
                crate::udp_options::STREAM_WINDOW,
                None,
            )
        };
        let server = bind(Some(weaver_utp::Shards::new("utp-drop-test", 1)))
            .await
            .unwrap();
        let client = bind(None).await.unwrap();
        let (accepted, connected) =
            tokio::join!(server.accept(), client.connect(server.bind_addr()));
        let accepted = accepted.unwrap();
        let home = accepted.home().cloned().expect("a stream on a uTP thread");

        let (tx, rx) = tokio::sync::oneshot::channel();
        let held = super::DropOffShard::new(Dropped(Some(tx)), &spawner);
        home.spawn(async move {
            assert!(weaver_utp::on_shard_thread());
            drop(held);
        })
        .await
        .unwrap();
        assert!(!rx.await.unwrap(), "dropped on the uTP thread");

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        drop(super::DropOffShard::new(Dropped(Some(tx)), &spawner));
        assert_eq!(rx.try_recv(), Ok(false), "dropped where it was");
        drop((accepted, connected));
    }
}
