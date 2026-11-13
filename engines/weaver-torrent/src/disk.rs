// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Blocks received from peers on their way to the files.
//!
//! A peer receives each block of a Piece message into a block buffer of its
//! own, taken from the session's [`BlockPool`]. The filled buffer moves to
//! the [`DiskQueue`], whose threads write it and hand it back to the pool:
//! the block is never copied between the socket and the write.
//!
//! The pool follows libtorrent's `disk_buffer_pool` (v2.0.15): the blocks
//! queued for writing are counted against `max_queued_disk_bytes`, a block
//! is always taken (the bound is soft), and past the high mark the pool says
//! so, and a peer that had a write outstanding stops reading until its
//! writes are done or the pool falls to the low mark.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::Notify};

use crate::core::constants::CHUNK_SIZE;

/// The bytes of one block buffer: a whole block.
pub(crate) const BLOCK_LEN: usize = CHUNK_SIZE as usize;

/// The bytes of blocks that may wait for the disk before the pool says it
/// is exceeded (libtorrent `settings_pack::max_queued_disk_bytes`, 1 MiB by
/// default, settings_pack.cpp:277).
pub(crate) const MAX_QUEUED_DISK_BYTES: usize = 1024 * 1024;

/// The session's block buffers, counted while they hold a block queued for
/// the disk.
#[derive(Clone)]
pub(crate) struct BlockPool(Arc<PoolInner>);

struct PoolInner {
    state: Mutex<PoolState>,
    // libtorrent `m_max_use`: max(1, max_queued_disk_bytes / block size).
    max_use: usize,
    // libtorrent `m_low_watermark`: half of it (`set_settings`).
    low_watermark: usize,
    // The free buffers kept for reuse; one returned past this is freed.
    kept: usize,
    // Rung once the pool falls to the low mark after it was exceeded
    // (libtorrent `check_buffer_level`, `watermark_callback`).
    below: Notify,
    // Buffers ever allocated, for the steady-state check.
    allocated: AtomicUsize,
}

struct PoolState {
    free: Vec<Box<[u8]>>,
    in_use: usize,
    // libtorrent `m_exceeded_max_size`: set at the high mark, cleared at
    // the low one.
    exceeded: bool,
    // How many times the pool fell to the low mark: a peer that stopped at
    // one generation reads again at the next.
    generation: u64,
}

/// What the pool said when a block was queued: whether it is exceeded, and
/// at which generation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Charge {
    pub(crate) exceeded: bool,
    pub(crate) generation: u64,
}

impl BlockPool {
    pub(crate) fn new(max_queued_bytes: usize) -> Self {
        let max_use = (max_queued_bytes / BLOCK_LEN).max(1);
        BlockPool(Arc::new(PoolInner {
            state: Mutex::new(PoolState {
                free: Vec::new(),
                in_use: 0,
                exceeded: false,
                generation: 0,
            }),
            max_use,
            low_watermark: max_use / 2,
            kept: max_use * 2,
            below: Notify::new(),
            allocated: AtomicUsize::new(0),
        }))
    }

    /// A buffer for one block.
    pub(crate) fn take(&self) -> BlockBuf {
        let buf = self.0.state.lock().free.pop();
        BlockBuf {
            buf: buf.unwrap_or_else(|| self.allocate()),
            pool: self.clone(),
            charged: false,
        }
    }

    /// Buffers until `into` holds `count`, under one lock.
    pub(crate) fn fill(&self, into: &mut Vec<BlockBuf>, count: usize) {
        if into.len() >= count {
            return;
        }
        let mut state = self.0.state.lock();
        while into.len() < count {
            let buf = match state.free.pop() {
                Some(buf) => buf,
                None => self.allocate(),
            };
            into.push(BlockBuf {
                buf,
                pool: self.clone(),
                charged: false,
            });
        }
    }

    fn allocate(&self) -> Box<[u8]> {
        self.0.allocated.fetch_add(1, Ordering::Relaxed);
        vec![0u8; BLOCK_LEN].into_boxed_slice()
    }

    /// `block` now holds a block queued for the disk, and counts against the
    /// bound until it is dropped (libtorrent `allocate_buffer_impl`: past
    /// `low + (max - low) / 2` the pool is exceeded).
    pub(crate) fn charge(&self, block: &mut BlockBuf) -> Charge {
        debug_assert!(!block.charged);
        block.charged = true;
        let inner = &*self.0;
        let mut state = inner.state.lock();
        state.in_use += 1;
        if state.in_use >= inner.low_watermark + (inner.max_use - inner.low_watermark) / 2 {
            state.exceeded = true;
        }
        Charge {
            exceeded: state.exceeded,
            generation: state.generation,
        }
    }

    /// Whether the pool fell to its low mark since `generation`.
    pub(crate) fn fell_since(&self, generation: u64) -> bool {
        self.0.state.lock().generation != generation
    }

    /// Resolves once the pool falls to its low mark. Enable it before
    /// looking at [`Self::fell_since`].
    pub(crate) fn fallen(&self) -> tokio::sync::futures::Notified<'_> {
        self.0.below.notified()
    }

    fn give_back(&self, buf: Box<[u8]>, charged: bool) {
        let inner = &*self.0;
        let mut state = inner.state.lock();
        let mut fell = false;
        if charged {
            state.in_use -= 1;
            if state.exceeded && state.in_use <= inner.low_watermark {
                state.exceeded = false;
                state.generation += 1;
                fell = true;
            }
        }
        if state.free.len() < inner.kept && !buf.is_empty() {
            state.free.push(buf);
        }
        drop(state);
        if fell {
            inner.below.notify_waiters();
        }
    }

    /// Blocks queued for the disk now.
    #[cfg(test)]
    pub(crate) fn in_use(&self) -> usize {
        self.0.state.lock().in_use
    }

    /// Buffers ever allocated.
    #[cfg(test)]
    pub(crate) fn allocated(&self) -> usize {
        self.0.allocated.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn max_use(&self) -> usize {
        self.0.max_use
    }
}

/// One block's buffer. Dropped, it goes back to its pool.
pub(crate) struct BlockBuf {
    buf: Box<[u8]>,
    pool: BlockPool,
    charged: bool,
}

impl BlockBuf {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    /// The pool the buffer goes back to.
    pub(crate) fn pool(&self) -> &BlockPool {
        &self.pool
    }

    /// Where the buffer's bytes are, to tell one buffer from another.
    #[cfg(test)]
    pub(crate) fn addr(&self) -> usize {
        self.buf.as_ptr() as usize
    }
}

impl Drop for BlockBuf {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        self.pool.give_back(buf, self.charged);
    }
}

/// A block received whole, in its own buffer.
pub struct Block {
    pub index: u32,
    pub begin: u32,
    len: usize,
    buf: BlockBuf,
}

impl Block {
    pub(crate) fn new(index: u32, begin: u32, len: usize, buf: BlockBuf) -> Self {
        debug_assert!(len <= BLOCK_LEN);
        Block {
            index,
            begin,
            len,
            buf,
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.buf.as_slice()[..self.len]
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// The block as a borrowed Piece message.
    pub(crate) fn piece(&self) -> crate::peer_protocol::Piece<crate::buffers::ByteBuf<'_>> {
        crate::peer_protocol::Piece::from_data(self.index, self.begin, self.data())
    }

    pub(crate) fn buf_mut(&mut self) -> &mut BlockBuf {
        &mut self.buf
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("index", &self.index)
            .field("begin", &self.begin)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// A connection's writes queued and not yet done, and whether it stopped
/// reading for them (libtorrent `m_outstanding_writing_bytes`, the
/// `bw_disk` state of the download channel).
#[derive(Default)]
pub(crate) struct PeerDisk {
    outstanding: AtomicUsize,
    blocked: AtomicBool,
    // The pool generation the connection stopped at.
    generation: AtomicU64,
    // Rung when the last outstanding write of a stopped connection is done.
    done: Notify,
}

impl PeerDisk {
    /// A write of the connection's is queued, the pool having said
    /// `charge`. Every peer may have one write outstanding whatever the
    /// pool says: one that had one already stops reading when the pool is
    /// exceeded (libtorrent `peer_connection::incoming_piece`).
    pub(crate) fn queued(&self, charge: Charge) {
        let before = self.outstanding.fetch_add(1, Ordering::AcqRel);
        if charge.exceeded && before > 0 {
            self.generation.store(charge.generation, Ordering::Release);
            self.blocked.store(true, Ordering::Release);
        }
    }

    /// A write of the connection's is done, or dropped (libtorrent
    /// `peer_connection::on_disk_write_complete`: with none outstanding the
    /// connection reads again).
    pub(crate) fn written(&self) {
        if self.outstanding.fetch_sub(1, Ordering::AcqRel) == 1
            && self.blocked.load(Ordering::Acquire)
        {
            self.done.notify_one();
        }
    }

    /// Whether the connection stopped reading for the disk.
    pub(crate) fn blocked(&self) -> bool {
        self.blocked.load(Ordering::Acquire)
    }

    /// Waits until the connection may read again: its writes are done, or
    /// the pool fell to its low mark (libtorrent `peer_connection::on_disk`).
    pub(crate) async fn room(&self, pool: &BlockPool) {
        while self.blocked() {
            let done = self.done.notified();
            let fallen = pool.fallen();
            tokio::pin!(done, fallen);
            done.as_mut().enable();
            fallen.as_mut().enable();
            if self.outstanding.load(Ordering::Acquire) == 0
                || pool.fell_since(self.generation.load(Ordering::Acquire))
            {
                break;
            }
            tokio::select! {
                () = done => {}
                () = fallen => {}
            }
        }
        self.blocked.store(false, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Acquire)
    }
}

/// Work for the disk threads.
pub(crate) trait DiskJob: Send + 'static + Sized {
    fn run(self);

    /// The jobs of one key run on one thread at a time, in the order queued:
    /// the writes of one torrent do not race each other for its files.
    fn key(&self) -> usize {
        0
    }

    /// Runs every job a thread took in one wake, in order.
    fn run_batch(batch: &mut VecDeque<Self>) {
        for job in batch.drain(..) {
            job.run();
        }
    }
}

/// The session's disk work, done on blocking threads off the async runtime.
/// A thread takes every queued job of one key, runs them, and looks again,
/// so one wake runs a batch, and it stays on a key while that key has work.
/// The jobs of one key never run on two threads at once; another thread
/// starts only for a key no thread is on (libtorrent's `aio_threads` pool,
/// which starts threads as jobs queue, `disk_io_thread_pool::job_queued`).
/// Each key has a lane of its own, and the keys with jobs and no thread wait
/// in turn, so queueing a job and taking a key's jobs cost the same however
/// many jobs and keys are queued.
pub(crate) struct DiskQueue<J: DiskJob> {
    inner: Arc<QueueInner<J>>,
}

impl<J: DiskJob> Clone for DiskQueue<J> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct QueueInner<J> {
    handle: Handle,
    jobs: Mutex<Jobs<J>>,
    threads: usize,
    // Rung when the last thread finds the queue empty.
    idle: Notify,
    wakes: AtomicUsize,
}

// One key's jobs, in the order queued.
struct Lane<J> {
    jobs: VecDeque<J>,
    // A thread is on the key.
    busy: bool,
    // The key waits in `ready`.
    ready: bool,
}

struct Jobs<J> {
    // A key has a lane while it has jobs queued or a thread on it.
    lanes: HashMap<usize, Lane<J>>,
    // The keys with jobs queued and no thread on them, in the order they
    // became so.
    ready: VecDeque<usize>,
    queued: usize,
    running: usize,
    // Threads on a key.
    busy: usize,
}

impl<J: DiskJob> Jobs<J> {
    // Queues `job`; whether its key became ready.
    fn push(&mut self, job: J) -> bool {
        let key = job.key();
        let lane = self.lanes.entry(key).or_insert_with(|| Lane {
            jobs: VecDeque::new(),
            busy: false,
            ready: false,
        });
        lane.jobs.push_back(job);
        self.queued += 1;
        if lane.busy || lane.ready {
            return false;
        }
        lane.ready = true;
        self.ready.push_back(key);
        true
    }
}

impl<J> Jobs<J> {
    // Every queued job of the key that has waited longest with no thread on
    // it, in order, and that key, now busy.
    fn take(&mut self, batch: &mut VecDeque<J>) -> Option<usize> {
        let key = self.ready.pop_front()?;
        let lane = self.lanes.get_mut(&key)?;
        lane.ready = false;
        lane.busy = true;
        std::mem::swap(&mut lane.jobs, batch);
        self.queued -= batch.len();
        self.busy += 1;
        Some(key)
    }

    // The thread on `key` left it: the key waits again if it has jobs.
    fn release(&mut self, key: usize) {
        let Some(lane) = self.lanes.get_mut(&key) else {
            return;
        };
        if !lane.busy {
            return;
        }
        lane.busy = false;
        self.busy -= 1;
        if lane.jobs.is_empty() {
            self.lanes.remove(&key);
        } else {
            lane.ready = true;
            self.ready.push_back(key);
        }
    }

    // A thread is started when none is looking for work and a key waits.
    fn wants_thread(&self, threads: usize) -> bool {
        !self.ready.is_empty() && self.running < threads && self.running == self.busy
    }

    fn is_idle(&self) -> bool {
        self.running == 0 && self.queued == 0
    }
}

impl<J: DiskJob> DiskQueue<J> {
    pub(crate) fn new(handle: Handle, threads: usize) -> Self {
        DiskQueue {
            inner: Arc::new(QueueInner {
                handle,
                jobs: Mutex::new(Jobs {
                    lanes: HashMap::new(),
                    ready: VecDeque::new(),
                    queued: 0,
                    running: 0,
                    busy: 0,
                }),
                threads: threads.max(1),
                idle: Notify::new(),
                wakes: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn push(&self, job: J) {
        let mut jobs = self.inner.jobs.lock();
        // A thread is started when none is looking for work, and this job's
        // key has no thread on it nor jobs already waiting for one.
        let start = jobs.push(job) && jobs.wants_thread(self.inner.threads);
        if start {
            jobs.running += 1;
        }
        drop(jobs);
        if start {
            spawn_drain(&self.inner);
        }
    }

    /// Times a disk thread was woken.
    #[cfg(test)]
    pub(crate) fn wakes(&self) -> usize {
        self.inner.wakes.load(Ordering::Relaxed)
    }

    /// Threads at work, and jobs queued for one.
    #[cfg(test)]
    pub(crate) fn at_work(&self) -> (usize, usize) {
        let jobs = self.inner.jobs.lock();
        (jobs.running, jobs.queued)
    }

    /// Resolves once no job is queued or running.
    #[cfg(test)]
    pub(crate) async fn idle(&self) {
        loop {
            let idle = self.inner.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.inner.jobs.lock().is_idle() {
                return;
            }
            idle.await;
        }
    }
}

// Starts a thread, already counted as running.
fn spawn_drain<J: DiskJob>(inner: &Arc<QueueInner<J>>) {
    inner.wakes.fetch_add(1, Ordering::Relaxed);
    let drain = Drain {
        inner: Some(inner.clone()),
        key: None,
        started: false,
    };
    drop(inner.handle.spawn_blocking(move || drain.run()));
}

// A disk thread's turn.
//
// One whose job panicked unwinds out of `run`: its key is let go and,
// when keys still wait and no other thread looks, a thread is started in
// its place, so no other key's jobs are lost. The jobs of its batch not
// yet run are dropped, and each settles as a job dropped unrun does.
//
// One dropped without running, as a runtime that shuts down drops the
// blocking tasks it never started: when it was the last thread, it drops
// the jobs it would have run, each of which settles (its buffer goes back,
// its peer is told the write is done, a piece whose check is lost is
// fetched again), so nothing waits on them.
struct Drain<J: DiskJob> {
    inner: Option<Arc<QueueInner<J>>>,
    // The key this thread is on.
    key: Option<usize>,
    started: bool,
}

impl<J: DiskJob> Drain<J> {
    fn run(mut self) {
        let Some(inner) = self.inner.clone() else {
            return;
        };
        self.started = true;
        let mut batch = VecDeque::new();
        loop {
            {
                let mut jobs = inner.jobs.lock();
                if let Some(key) = self.key.take() {
                    jobs.release(key);
                }
                self.key = jobs.take(&mut batch);
                if self.key.is_none() {
                    self.inner = None;
                    jobs.running -= 1;
                    let idle = jobs.is_idle();
                    drop(jobs);
                    if idle {
                        inner.idle.notify_waiters();
                    }
                    return;
                }
            }
            J::run_batch(&mut batch);
            debug_assert!(batch.is_empty());
        }
    }
}

impl<J: DiskJob> Drop for Drain<J> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut dropped = Vec::new();
        let start = {
            let mut jobs = inner.jobs.lock();
            if let Some(key) = self.key.take() {
                jobs.release(key);
            }
            jobs.running -= 1;
            if self.started {
                let start = jobs.wants_thread(inner.threads);
                if start {
                    jobs.running += 1;
                }
                start
            } else {
                if jobs.running == 0 {
                    jobs.ready.clear();
                    jobs.queued = 0;
                    dropped.extend(jobs.lanes.drain().map(|(_, lane)| lane.jobs));
                }
                false
            }
        };
        // Outside the lock: a job that settles may queue work.
        drop(dropped);
        if start {
            spawn_drain(&inner);
        }
        inner.idle.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};

    use super::{BLOCK_LEN, BlockPool, DiskJob, DiskQueue, PeerDisk};

    // libtorrent's watermarks: 64 blocks, the low mark at 32, exceeded
    // from 48 until the pool falls back to 32.
    #[test]
    fn the_pool_is_exceeded_at_its_high_mark_until_it_falls_to_its_low_one() {
        let pool = BlockPool::new(64 * BLOCK_LEN);
        assert_eq!(pool.max_use(), 64);
        let mut held = Vec::new();
        for i in 1..=48 {
            let mut block = pool.take();
            let charge = pool.charge(&mut block);
            assert_eq!(charge.exceeded, i >= 48, "{i}");
            held.push(block);
        }
        let generation = pool.charge(&mut pool.take()).generation;
        // Dropping to 33 leaves it exceeded; at 32 it falls.
        while pool.in_use() > 33 {
            held.pop();
        }
        assert!(!pool.fell_since(generation));
        held.pop();
        assert_eq!(pool.in_use(), 32);
        assert!(pool.fell_since(generation));
        let mut block = pool.take();
        assert!(!pool.charge(&mut block).exceeded);
    }

    // Buffers are reused: blocks taken and given back in turn allocate
    // only the first time.
    #[test]
    fn buffers_go_back_to_the_pool_and_are_reused() {
        let pool = BlockPool::new(64 * BLOCK_LEN);
        let first = pool.take();
        let addr = first.addr();
        drop(first);
        for _ in 0..1000 {
            let mut block = pool.take();
            assert_eq!(block.addr(), addr);
            pool.charge(&mut block);
        }
        assert_eq!(pool.allocated(), 1);
        assert_eq!(pool.in_use(), 0);
    }

    // A peer with a write outstanding stops at the high mark, and reads
    // again once that write is done; the first write never stops it.
    #[tokio::test(start_paused = true)]
    async fn a_peer_stops_at_the_high_mark_and_reads_again_when_its_writes_are_done() {
        let pool = BlockPool::new(4 * BLOCK_LEN);
        let others: Vec<_> = (0..2)
            .map(|_| {
                let mut b = pool.take();
                pool.charge(&mut b);
                b
            })
            .collect();
        let peer = PeerDisk::default();
        let mut first = pool.take();
        peer.queued(pool.charge(&mut first));
        assert!(!peer.blocked(), "the first write never stops a peer");
        let mut second = pool.take();
        peer.queued(pool.charge(&mut second));
        assert!(peer.blocked());
        let room = peer.room(&pool);
        tokio::pin!(room);
        assert!(futures::poll!(room.as_mut()).is_pending());
        drop(first);
        peer.written();
        assert!(futures::poll!(room.as_mut()).is_pending());
        drop(second);
        peer.written();
        room.await;
        assert!(!peer.blocked());
        drop(others);
    }

    // A stopped peer reads again once the pool falls to its low mark, its
    // own writes still outstanding.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_peer_reads_again_when_the_pool_falls() {
        let pool = BlockPool::new(4 * BLOCK_LEN);
        let mut others: Vec<_> = (0..2)
            .map(|_| {
                let mut b = pool.take();
                pool.charge(&mut b);
                b
            })
            .collect();
        let peer = PeerDisk::default();
        let mut mine: Vec<_> = (0..2).map(|_| pool.take()).collect();
        for b in &mut mine {
            peer.queued(pool.charge(b));
        }
        assert!(peer.blocked());
        let room = peer.room(&pool);
        tokio::pin!(room);
        assert!(futures::poll!(room.as_mut()).is_pending());
        others.clear();
        room.await;
        assert_eq!(peer.outstanding(), 2);
    }

    struct Count(Arc<AtomicUsize>);

    impl DiskJob for Count {
        fn run(self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Jobs queued while the disk thread is away are taken in one wake.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn the_disk_thread_takes_every_queued_job_per_wake() {
        let queue = DiskQueue::new(tokio::runtime::Handle::current(), 4);
        let ran = Arc::new(AtomicUsize::new(0));
        for _ in 0..10 {
            queue.push(Count(ran.clone()));
        }
        queue.idle().await;
        assert_eq!(ran.load(Ordering::Relaxed), 10);
        assert_eq!(queue.wakes(), 1, "ten jobs of one key, one wake");
    }

    struct Gated {
        key: usize,
        name: &'static str,
        log: Arc<parking_lot::Mutex<Vec<String>>>,
        entered: Option<std::sync::mpsc::Sender<()>>,
        release: Option<std::sync::mpsc::Receiver<()>>,
    }

    impl DiskJob for Gated {
        fn key(&self) -> usize {
            self.key
        }

        fn run(self) {
            self.log.lock().push(format!("{}:start", self.name));
            if let Some(entered) = self.entered {
                entered.send(()).unwrap();
            }
            if let Some(release) = self.release {
                release.recv().unwrap();
            }
            self.log.lock().push(format!("{}:end", self.name));
        }
    }

    // A key's jobs wait for the thread on that key, while another key's
    // run beside it on a thread of their own.
    #[test]
    fn one_keys_jobs_run_on_one_thread_and_another_key_runs_beside_them() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue = DiskQueue::new(rt.handle().clone(), 4);
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let job = |key, name, entered, release| Gated {
            key,
            name,
            log: log.clone(),
            entered,
            release,
        };
        let (a_in, a_entered) = std::sync::mpsc::channel();
        let (a_go, a_release) = std::sync::mpsc::channel();
        queue.push(job(1, "a1", Some(a_in), Some(a_release)));
        a_entered.recv().unwrap();
        queue.push(job(1, "a2", None, None));
        let (b_in, b_entered) = std::sync::mpsc::channel();
        queue.push(job(2, "b1", Some(b_in), None));
        // b1 runs while a1 holds its thread; a2 waits for a1.
        b_entered.recv().unwrap();
        assert!(!log.lock().iter().any(|e| e == "a2:start"));
        a_go.send(()).unwrap();
        rt.block_on(queue.idle());
        let log = log.lock();
        let at = |e: &str| log.iter().position(|x| x == e).unwrap();
        assert!(at("a1:end") < at("a2:start"), "{log:?}");
        assert_eq!(queue.wakes(), 2, "one thread per key");
    }

    struct Panics;
    impl DiskJob for Panics {
        fn key(&self) -> usize {
            1
        }
        fn run(self) {
            panic!("a job that panics");
        }
    }

    struct Recorded {
        key: usize,
        ran: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
        done: bool,
    }
    impl DiskJob for Recorded {
        fn key(&self) -> usize {
            self.key
        }
        fn run(mut self) {
            self.done = true;
            self.ran.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Drop for Recorded {
        fn drop(&mut self) {
            if !self.done {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    enum Either {
        Panics(Panics),
        Recorded(Recorded),
        Gate(
            usize,
            std::sync::mpsc::Receiver<()>,
            std::sync::mpsc::Sender<()>,
        ),
    }
    impl DiskJob for Either {
        fn key(&self) -> usize {
            match self {
                Either::Panics(p) => p.key(),
                Either::Recorded(r) => r.key(),
                Either::Gate(key, ..) => *key,
            }
        }
        fn run(self) {
            match self {
                Either::Panics(p) => p.run(),
                Either::Recorded(r) => r.run(),
                Either::Gate(_, release, entered) => {
                    entered.send(()).unwrap();
                    release.recv().unwrap();
                }
            }
        }
    }

    // One disk thread. A job of torrent 1 panics while a job of torrent 2
    // waits in the queue, behind it or ahead of it. The panic lets go of
    // torrent 1 and, when torrent 2 still waits, another thread starts for
    // it: its job runs, and nothing is dropped unrun.
    #[test]
    fn a_panicking_job_loses_no_other_torrents_queued_job() {
        // The thread is first held on torrent 1, so torrent 2 runs before
        // the panic, then on torrent 3, so the panic comes first.
        for (gate, wakes) in [(1, 1), (3, 2)] {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .build()
                .unwrap();
            let queue = DiskQueue::new(rt.handle().clone(), 1);
            let ran = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let (go, release) = std::sync::mpsc::channel();
            let (entered_tx, entered) = std::sync::mpsc::channel();
            // Holds the one thread so the next two jobs queue behind it.
            queue.push(Either::Gate(gate, release, entered_tx));
            entered.recv().unwrap();
            queue.push(Either::Panics(Panics));
            queue.push(Either::Recorded(Recorded {
                key: 2,
                ran: ran.clone(),
                dropped: dropped.clone(),
                done: false,
            }));
            go.send(()).unwrap();
            rt.block_on(queue.idle());
            assert_eq!(
                (ran.load(Ordering::SeqCst), dropped.load(Ordering::SeqCst)),
                (1, 0),
                "gate {gate}: torrent 2's queued job was dropped unrun"
            );
            assert_eq!(queue.wakes(), wakes, "gate {gate}");
        }
    }

    // A runtime that shuts down before a started thread runs drops the
    // thread's turn: the queued jobs are dropped, each settling, and the
    // queue is left idle.
    #[test]
    fn a_turn_dropped_unrun_drops_the_queued_jobs() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue = DiskQueue::new(rt.handle().clone(), 1);
        rt.shutdown_background();
        let ran = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        for key in [1, 2, 1] {
            queue.push(Recorded {
                key,
                ran: ran.clone(),
                dropped: dropped.clone(),
                done: false,
            });
        }
        assert_eq!(
            (ran.load(Ordering::SeqCst), dropped.load(Ordering::SeqCst)),
            (0, 3)
        );
        assert!(queue.inner.jobs.lock().is_idle());
    }
}
