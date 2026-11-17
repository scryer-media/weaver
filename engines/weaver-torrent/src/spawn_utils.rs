// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::disk::{BlockPool, DiskQueue, MAX_QUEUED_CHECK_BYTES, MAX_QUEUED_DISK_BYTES};
use crate::session_stats::disk::{DiskStats, DiskStatsSnapshot};
use crate::torrent_state::{HashJob, WriteJob};

/// Threads of a session that check pieces: one, libtorrent's default
/// (`settings_pack::hashing_threads`, settings_pack.cpp:353, its one
/// session's). The session's one thread checks the pieces of every torrent
/// of it, from peers over TCP and over uTP alike. One thread hashes a piece
/// as fast as its core does, and the checks of a torrent end in the order
/// they were queued, so the pieces a storage holds in memory reach the write
/// queue, and the files, in that order.
const HASH_THREADS: usize = 1;

/// A piece check counted as waiting for the hashing thread, among its
/// session's, until the thread takes it or it is dropped.
pub(crate) struct QueuedCheck(Arc<DiskStats>);

impl Drop for QueuedCheck {
    fn drop(&mut self) {
        self.0.check_taken();
    }
}

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
    // The whole pieces storages hold in memory that wait for their check
    // and their write, counted by the marks the blocks are.
    checks: BlockPool,
    disk: DiskQueue<WriteJob>,
    // The thread that checks a piece whose last block landed: the session's
    // one, on the session's runtime, there while it has a check to run.
    hash: DiskQueue<HashJob>,
    // What the session's disk calls, permits, read turns and peer sockets
    // did.
    stats: Arc<DiskStats>,
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
        Self {
            allow_block_in_place,
            permits,
            pool: BlockPool::new(MAX_QUEUED_DISK_BYTES),
            checks: BlockPool::new(MAX_QUEUED_CHECK_BYTES),
            // As many disk threads as blocking permits (libtorrent's
            // `aio_threads`, a pool of its own beside the network thread).
            disk: DiskQueue::new(handle.clone(), permits),
            // Checks have a thread of their own, so a write never waits behind
            // one (libtorrent `settings_pack::hashing_threads`, a pool beside
            // `aio_threads`, mmap_disk_io.cpp:1685).
            hash: DiskQueue::new(handle.clone(), HASH_THREADS),
            concurrent_block_in_place_semaphore: Arc::new(Semaphore::new(permits)),
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
        snapshot.queued_write_bytes = self.pool.queued_bytes();
        snapshot.check_pieces_waiting = self.checks.spans();
        snapshot.check_bytes_waiting = self.checks.queued_bytes();
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

    /// What counts the whole pieces that wait for their check and write.
    pub(crate) fn checks(&self) -> &BlockPool {
        &self.checks
    }

    /// The threads that write received blocks and checked pieces.
    pub(crate) fn disk(&self) -> &DiskQueue<WriteJob> {
        &self.disk
    }

    /// The thread that checks pieces, the session's one.
    pub(crate) fn hash(&self) -> &DiskQueue<HashJob> {
        &self.hash
    }

    /// Counts a check about to be queued for the hashing thread.
    pub(crate) fn check_queued(&self) -> QueuedCheck {
        self.stats.check_queued();
        QueuedCheck(self.stats.clone())
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

    /// Like [`Self::block_in_place_with_semaphore`], in a read turn of
    /// `turns` in the place of a blocking permit: a read of a torrent's
    /// files, which its volume bounds with every other reader of it.
    pub(crate) async fn block_in_place_in_turn<F: FnOnce() -> R, R>(
        &self,
        turns: &crate::storage::ReadTurns,
        f: F,
    ) -> R {
        no_block_on_utp_thread();
        let _turn = turns.turn(&self.stats).await;
        self.block_in_place(f)
    }

    /// The most threads the initial check of a torrent runs on, its reader
    /// and its hashers: half the blocking permits, at least one, as before
    /// the check's reads took turns on their volume. The reader reads a
    /// piece at a time in one read turn; the hashers read nothing and take
    /// no turn. libtorrent hashes on its own `hashing_threads` pool
    /// (default 1) beside the disk threads; here the cap keeps the threads
    /// a check runs on bounded.
    pub fn check_threads(&self) -> usize {
        (self.permits / 2).max(1)
    }

    pub fn semaphore(&self) -> Arc<Semaphore> {
        self.concurrent_block_in_place_semaphore.clone()
    }

    /// One of the session's blocking permits, held for as long as it is
    /// kept, counted as every permit is: its wait timed only when it has to
    /// wait. An error once the permits are closed.
    pub(crate) async fn owned_permit(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        let semaphore = &self.concurrent_block_in_place_semaphore;
        if let Ok(permit) = Arc::clone(semaphore).try_acquire_owned() {
            self.stats.permit_taken();
            return Ok(permit);
        }
        let since = std::time::Instant::now();
        let permit = Arc::clone(semaphore).acquire_owned().await?;
        self.stats.permit_waited(since);
        Ok(permit)
    }

    /// Runs `f` on one of the session's blocking threads for a reader on a
    /// uTP thread, in a read turn of `turns` taken first without blocking,
    /// and hands the result back with the others that finish meanwhile, in
    /// one wake of the uTP thread. None when the session's runtime shut
    /// down before it ran, or it panicked.
    pub(crate) async fn read_off_shard<F, R>(
        &self,
        turns: &crate::storage::ReadTurns,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&DiskStats) -> R + Send + 'static,
        R: Send + 'static,
    {
        crate::off_shard::read_off_shard(&self.handle, turns, &self.stats, f).await
    }

    /// Runs `f` on one of the session's blocking threads once it holds a
    /// read turn of `turns`, from any thread, a uTP thread too: the work
    /// runs on the session's pool, never on the calling runtime's own, and
    /// a read that cannot run in place counts against the same bound, its
    /// volume's reads, as the reads that do. An error when the session's
    /// runtime shut down before it ran, or it panicked: the error says
    /// which, and carries what the panic said.
    pub(crate) async fn spawn_blocking_in_turn<F, R>(
        &self,
        turns: &crate::storage::ReadTurns,
        f: F,
    ) -> anyhow::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let turn = turns.owned_turn(&self.stats).await;
        Ok(self
            .handle
            .spawn_blocking(move || {
                let _turn = turn;
                f()
            })
            .await?)
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
    // the session's runtime: a blocking call runs on the session's pool.
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
        let turns = turns(4);
        let flavor = other.block_on(async {
            spawner
                .spawn_blocking_in_turn(&turns, || {
                    tokio::runtime::Handle::current().runtime_flavor()
                })
                .await
                .unwrap()
        });
        assert_eq!(flavor, tokio::runtime::RuntimeFlavor::MultiThread);
    }

    fn turns(n: usize) -> crate::storage::ReadTurns {
        crate::storage::ReadTurns::new(std::sync::Arc::new(tokio::sync::Semaphore::new(n)), false)
    }

    // Work handed off in a read turn that panics gives its caller an error
    // that carries what the panic said, and the turn comes back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panic_in_work_handed_off_in_a_turn_is_told_to_its_caller() {
        let spawner = BlockingSpawner::new(1);
        let turns = turns(1);
        let failed = spawner
            .spawn_blocking_in_turn(&turns, || -> u32 {
                panic!("the piece could not be read")
            })
            .await
            .unwrap_err();
        assert!(
            format!("{failed:#}").contains("the piece could not be read"),
            "{failed:#}"
        );
        assert_eq!(turns.semaphore().available_permits(), 1);
    }

    // Blocking work handed off in a read turn holds it while it runs: with
    // every turn held, the next does not start until one comes back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_handed_off_in_a_turn_counts_against_the_volumes_reads() {
        use std::sync::mpsc::channel;

        let spawner = BlockingSpawner::new(8);
        let turns = turns(2);
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel::<()>();
        let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
        let job = |n: u32| {
            let (started, release) = (started_tx.clone(), release_rx.clone());
            let (spawner, turns) = (spawner.clone(), turns.clone());
            tokio::spawn(async move {
                spawner
                    .spawn_blocking_in_turn(&turns, move || {
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
        assert_eq!(turns.semaphore().available_permits(), 0);

        // The third waits for a turn, which only a finished job gives.
        let third = job(3);
        for _ in 0..3 {
            release_tx.send(()).unwrap();
        }
        assert_eq!(first.await.unwrap().unwrap(), 1);
        assert_eq!(second.await.unwrap().unwrap(), 2);
        assert_eq!(third.await.unwrap().unwrap(), 3);
        assert_eq!(started_rx.try_recv(), Ok(3));
        assert_eq!(turns.semaphore().available_permits(), 2);
        assert_eq!(spawner.disk_snapshot().permit_acquisitions, 3);
    }

    // An owned permit counts as every permit does: one taken free is a
    // taking, one that had to wait is a taking and a wait.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_owned_permit_moves_the_permit_counters() {
        let spawner = BlockingSpawner::new(1);
        let free = spawner.owned_permit().await.unwrap();
        let counted = spawner.disk_snapshot();
        assert_eq!((counted.permit_acquisitions, counted.permit_waits), (1, 0));

        let mut waiting = std::pin::pin!(spawner.owned_permit());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(free);
        drop(waiting.await.unwrap());
        let counted = spawner.disk_snapshot();
        assert_eq!((counted.permit_acquisitions, counted.permit_waits), (2, 1));
        assert_eq!(spawner.semaphore().available_permits(), 1);
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
