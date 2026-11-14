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
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::{
    runtime::Handle,
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
};

use crate::core::constants::CHUNK_SIZE;

/// The bytes of one block buffer: a whole block.
pub(crate) const BLOCK_LEN: usize = CHUNK_SIZE as usize;

/// The bytes of blocks that may wait for the disk before the pool says it
/// is exceeded (libtorrent `settings_pack::max_queued_disk_bytes`, 1 MiB by
/// default, settings_pack.cpp:277).
pub(crate) const MAX_QUEUED_DISK_BYTES: usize = 1024 * 1024;

/// The bytes of whole pieces a storage holds in memory that may wait for
/// their check and their write before the connections that completed them
/// stop reading: by the pool's marks, reads stop at three quarters of it,
/// 48 MiB, and go on at half, 32 MiB. A connection whose piece leaves the
/// count past the high mark stops with that piece (`PeerDisk::queued_piece`),
/// so the count passes the mark by no more than the one piece each
/// connection was completing as it was reached: bytes the storage held
/// already, in the room it gave the piece. In bytes, the count is at most
/// 48 MiB plus one piece for each connection of the session, or, with a
/// piece larger than 48 MiB, that piece in place of the 48 MiB: with 100
/// connections and 16 MiB pieces, 48 MiB + 1,600 MiB. A piece is the unit
/// that waits here, so the bound is one that holds some pieces, where the
/// block bound holds some blocks. Each session counts its own.
pub(crate) const MAX_QUEUED_CHECK_BYTES: usize = 64 * 1024 * 1024;

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
    // Blocks queued for the disk now, as `PoolState::in_use` counts them:
    // kept apart so a scrape reads it without the pool's lock.
    queued: AtomicUsize,
    // Spans of bytes counted without a buffer of the pool's now
    // (`BlockPool::charge_bytes`).
    spans: AtomicUsize,
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

/// What the pool said when a block was queued: whether it is exceeded,
/// whether nothing else waits in it, and at which generation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Charge {
    pub(crate) exceeded: bool,
    pub(crate) alone: bool,
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
            queued: AtomicUsize::new(0),
            spans: AtomicUsize::new(0),
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
        self.add(1)
    }

    /// `bytes` held elsewhere wait for the disk, as a whole piece its
    /// storage keeps in memory does until it is checked and written: they
    /// count against the bound as so many blocks do, by the same marks,
    /// until the span is dropped.
    pub(crate) fn charge_bytes(&self, bytes: u64) -> (Charge, Span) {
        let blocks = usize::try_from(bytes.div_ceil(BLOCK_LEN as u64)).unwrap_or(usize::MAX);
        self.0.spans.fetch_add(1, Ordering::Relaxed);
        let span = Span {
            pool: self.clone(),
            blocks,
        };
        (self.add(blocks), span)
    }

    fn add(&self, blocks: usize) -> Charge {
        let inner = &*self.0;
        inner.queued.fetch_add(blocks, Ordering::Relaxed);
        let mut state = inner.state.lock();
        state.in_use += blocks;
        if state.in_use >= inner.low_watermark + (inner.max_use - inner.low_watermark) / 2 {
            state.exceeded = true;
        }
        Charge {
            exceeded: state.exceeded,
            alone: state.in_use == blocks,
            generation: state.generation,
        }
    }

    // `blocks` no longer wait, with the pool's lock held; whether the pool
    // fell to its low mark for it, which the caller rings once the lock is
    // let go.
    fn sub(&self, state: &mut PoolState, blocks: usize) -> bool {
        let inner = &*self.0;
        inner.queued.fetch_sub(blocks, Ordering::Relaxed);
        state.in_use -= blocks;
        if state.exceeded && state.in_use <= inner.low_watermark {
            state.exceeded = false;
            state.generation += 1;
            return true;
        }
        false
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
        let fell = charged && self.sub(&mut state, 1);
        if state.free.len() < inner.kept && !buf.is_empty() {
            state.free.push(buf);
        }
        drop(state);
        if fell {
            inner.below.notify_waiters();
        }
    }

    /// Bytes of blocks queued for the disk now, read without a lock.
    pub(crate) fn queued_bytes(&self) -> u64 {
        (self.0.queued.load(Ordering::Relaxed) * BLOCK_LEN) as u64
    }

    /// Spans counted now ([`Self::charge_bytes`]), read without a lock.
    pub(crate) fn spans(&self) -> u64 {
        self.0.spans.load(Ordering::Relaxed) as u64
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

/// Bytes counted against a pool's bound without a buffer of its own
/// ([`BlockPool::charge_bytes`]), until this is dropped.
pub(crate) struct Span {
    pool: BlockPool,
    blocks: usize,
}

impl Drop for Span {
    fn drop(&mut self) {
        let inner = &*self.pool.0;
        inner.spans.fetch_sub(1, Ordering::Relaxed);
        let fell = self.pool.sub(&mut inner.state.lock(), self.blocks);
        if fell {
            inner.below.notify_waiters();
        }
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
    ///
    /// True when the connection stops reading for it.
    pub(crate) fn queued(&self, charge: Charge) -> bool {
        let before = self.outstanding.fetch_add(1, Ordering::AcqRel);
        self.stops(charge, charge.exceeded && before > 0)
    }

    /// A whole piece the connection completed waits for its check and its
    /// write, the pool having said `charge`. A piece is too much to allow
    /// each connection one more past the mark: the connection stops reading
    /// with the piece that leaves the pool exceeded, unless nothing else
    /// waits in it, so one piece larger than the mark is not waited on by
    /// its own connection alone.
    ///
    /// True when the connection stops reading for it.
    pub(crate) fn queued_piece(&self, charge: Charge) -> bool {
        self.outstanding.fetch_add(1, Ordering::AcqRel);
        self.stops(charge, charge.exceeded && !charge.alone)
    }

    fn stops(&self, charge: Charge, stops: bool) -> bool {
        if stops {
            self.generation.store(charge.generation, Ordering::Release);
            self.blocked.store(true, Ordering::Release);
        }
        stops
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

    /// True for jobs that share nothing of their key's: a thread takes one
    /// of them at a time, and a key with more waiting goes to the back of
    /// the keys that wait. So the keys are served in turn, and one key's
    /// jobs run on as many threads as no other key waits for.
    const SIDE_BY_SIDE: bool = false;

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
/// The jobs of one key never run on two threads at once, unless they are
/// [`DiskJob::SIDE_BY_SIDE`]; another thread starts only for a key no
/// thread is on, or one whose jobs run side by side (libtorrent's `aio_threads` pool,
/// which starts threads as jobs queue, `disk_io_thread_pool::job_queued`).
/// Each key has a lane of its own, and the keys with jobs and no thread wait
/// in turn, so queueing a job and taking a key's jobs cost the same however
/// many jobs and keys are queued.
///
/// A queue made with [`DiskQueue::sharing`] counts its threads against a
/// bound it shares with other disk work: a thread holds one permit of the
/// bound while it runs a key's jobs, and between two batches it gives the
/// permit to whoever waited longest for one, so neither a busy queue nor
/// the work beside it starves the other.
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
    // The bound the queue's threads share with other disk work.
    shared: Option<Arc<Semaphore>>,
    // Rung when the last thread finds the queue empty.
    idle: Notify,
    wakes: AtomicUsize,
    // Jobs waiting for a thread, as `Jobs::queued` counts them: kept apart
    // so a scrape reads it without the queue's lock.
    waiting: AtomicUsize,
}

// One key's jobs, in the order queued.
struct Lane<J> {
    jobs: VecDeque<J>,
    // Threads on the key.
    busy: usize,
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
            busy: 0,
            ready: false,
        });
        lane.jobs.push_back(job);
        self.queued += 1;
        if lane.ready || (lane.busy > 0 && !J::SIDE_BY_SIDE) {
            return false;
        }
        lane.ready = true;
        self.ready.push_back(key);
        true
    }
}

impl<J> Jobs<J> {
    // Every queued job of the key that has waited longest with no thread on
    // it, in order, and that key, now busy. Of a key whose jobs run side by
    // side, the first job only: the key waits again behind the others while
    // it has more.
    fn take(&mut self, batch: &mut VecDeque<J>, side_by_side: bool) -> Option<usize> {
        let key = self.ready.pop_front()?;
        let lane = self.lanes.get_mut(&key)?;
        lane.busy += 1;
        if side_by_side {
            batch.extend(lane.jobs.pop_front());
            lane.ready = !lane.jobs.is_empty();
            if lane.ready {
                self.ready.push_back(key);
            }
        } else {
            lane.ready = false;
            std::mem::swap(&mut lane.jobs, batch);
        }
        self.queued -= batch.len();
        self.busy += 1;
        Some(key)
    }

    // The thread on `key` left it: the key waits again if it has jobs.
    fn release(&mut self, key: usize) {
        let Some(lane) = self.lanes.get_mut(&key) else {
            return;
        };
        if lane.busy == 0 {
            return;
        }
        lane.busy -= 1;
        self.busy -= 1;
        if lane.jobs.is_empty() {
            if lane.busy == 0 {
                self.lanes.remove(&key);
            }
        } else if !lane.ready {
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
        Self::with_bound(handle, threads, None)
    }

    /// A queue whose threads each hold a permit of `shared` while they run
    /// jobs: with the other holders of its permits, never more at once than
    /// it has.
    pub(crate) fn sharing(handle: Handle, threads: usize, shared: Arc<Semaphore>) -> Self {
        Self::with_bound(handle, threads, Some(shared))
    }

    fn with_bound(handle: Handle, threads: usize, shared: Option<Arc<Semaphore>>) -> Self {
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
                shared,
                idle: Notify::new(),
                wakes: AtomicUsize::new(0),
                waiting: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn push(&self, job: J) {
        self.inner.waiting.fetch_add(1, Ordering::Relaxed);
        let mut jobs = self.inner.jobs.lock();
        // A thread is started when none is looking for work, and this job's
        // key has no thread on it nor jobs already waiting for one, or its
        // jobs run side by side.
        let start = (jobs.push(job) || J::SIDE_BY_SIDE) && jobs.wants_thread(self.inner.threads);
        if start {
            jobs.running += 1;
        }
        drop(jobs);
        if start {
            spawn_drain(&self.inner);
        }
    }

    /// Whether `other` is this queue.
    #[cfg(test)]
    pub(crate) fn is(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Jobs waiting for a thread now, read without a lock.
    pub(crate) fn waiting(&self) -> u64 {
        self.inner.waiting.load(Ordering::Relaxed) as u64
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
    Drain {
        inner: Some(inner.clone()),
        key: None,
        started: false,
        permit: None,
    }
    .start(inner);
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
    // The thread's permit of the bound the queue shares.
    permit: Option<OwnedSemaphorePermit>,
}

impl<J: DiskJob> Drain<J> {
    // Runs the turn on a blocking thread: at once, or, when the queue
    // shares a bound and no permit is free, once one is. The turn is
    // counted as running while it waits, so one turn waits at most.
    fn start(mut self, inner: &Arc<QueueInner<J>>) {
        let handle = &inner.handle;
        let Some(shared) = &inner.shared else {
            drop(handle.spawn_blocking(move || self.run()));
            return;
        };
        match Arc::clone(shared).try_acquire_owned() {
            Ok(permit) => {
                self.permit = Some(permit);
                drop(handle.spawn_blocking(move || self.run()));
            }
            Err(_) => {
                // The turn takes its place among those waiting for a permit
                // here, not when its task first runs: the permits go to the
                // waiters in the order they asked.
                let mut permit = Box::pin(Arc::clone(shared).acquire_owned());
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                match permit.as_mut().poll(&mut cx) {
                    std::task::Poll::Ready(Ok(permit)) => {
                        self.permit = Some(permit);
                        drop(handle.spawn_blocking(move || self.run()));
                    }
                    std::task::Poll::Ready(Err(_)) => {}
                    std::task::Poll::Pending => {
                        let blocking = handle.clone();
                        // A runtime that shuts down drops the turn where it
                        // waits, which settles the queued jobs.
                        drop(handle.spawn(async move {
                            if let Ok(permit) = permit.await {
                                self.permit = Some(permit);
                                drop(blocking.spawn_blocking(move || self.run()));
                            }
                        }));
                    }
                }
            }
        }
    }

    // Between two batches the permit goes to whoever waited longest for
    // one. False when someone took it: this thread lets go of its key and,
    // when a key waits, the turn waits for a permit again.
    fn keeps_permit(&mut self, inner: &Arc<QueueInner<J>>) -> bool {
        let Some(shared) = &inner.shared else {
            return true;
        };
        self.permit = None;
        match Arc::clone(shared).try_acquire_owned() {
            Ok(permit) => {
                self.permit = Some(permit);
                true
            }
            Err(_) => false,
        }
    }

    fn run(mut self) {
        let Some(inner) = self.inner.clone() else {
            return;
        };
        self.started = true;
        let mut batch = VecDeque::new();
        loop {
            let beside = {
                let mut jobs = inner.jobs.lock();
                if let Some(key) = self.key.take() {
                    jobs.release(key);
                }
                self.key = jobs.take(&mut batch, J::SIDE_BY_SIDE);
                inner.waiting.fetch_sub(batch.len(), Ordering::Relaxed);
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
                // A key whose jobs run side by side may wait still, with
                // this thread on one of its jobs and no other looking.
                let beside = J::SIDE_BY_SIDE && jobs.wants_thread(inner.threads);
                if beside {
                    jobs.running += 1;
                }
                beside
            };
            if beside {
                spawn_drain(&inner);
            }
            J::run_batch(&mut batch);
            debug_assert!(batch.is_empty());
            if !self.keeps_permit(&inner) {
                let mut jobs = inner.jobs.lock();
                if let Some(key) = self.key.take() {
                    jobs.release(key);
                }
                if jobs.ready.is_empty() {
                    self.inner = None;
                    jobs.running -= 1;
                    let idle = jobs.is_idle();
                    drop(jobs);
                    if idle {
                        inner.idle.notify_waiters();
                    }
                } else {
                    drop(jobs);
                    self.started = false;
                    self.start(&inner);
                }
                return;
            }
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
                    inner.waiting.fetch_sub(jobs.queued, Ordering::Relaxed);
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

    // Whole pieces are counted by the same marks, each as its blocks: a
    // peer with one waiting stops at the high mark, 48 of 64 blocks, and
    // reads again once the count falls to the low one, 32, its own pieces
    // still waiting. The pieces waiting are counted too.
    #[tokio::test(start_paused = true)]
    async fn pieces_waiting_stop_a_peer_at_the_high_mark_until_the_low_one() {
        let pool = BlockPool::new(64 * BLOCK_LEN);
        let piece = 8 * BLOCK_LEN as u64;
        let peer = PeerDisk::default();
        let mut mine = Vec::new();
        for waiting in 1..=6 {
            let (charge, span) = pool.charge_bytes(piece);
            assert_eq!(charge.exceeded, waiting == 6, "{waiting}");
            assert_eq!(peer.queued(charge), waiting == 6);
            mine.push(span);
        }
        assert_eq!(pool.spans(), 6);
        assert_eq!(pool.queued_bytes(), 6 * piece);
        // A piece that is not whole blocks counts the block it ends in.
        let (_, odd) = pool.charge_bytes(BLOCK_LEN as u64 + 1);
        assert_eq!(pool.in_use(), 50);
        drop(odd);

        let room = peer.room(&pool);
        tokio::pin!(room);
        assert!(futures::poll!(room.as_mut()).is_pending());
        mine.pop();
        peer.written();
        assert_eq!(pool.in_use(), 40);
        assert!(futures::poll!(room.as_mut()).is_pending());
        mine.pop();
        peer.written();
        assert_eq!(pool.in_use(), 32);
        room.await;
        assert!(!peer.blocked());
        assert_eq!(peer.outstanding(), 4);
        assert_eq!(pool.spans(), 4);
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

    struct Beside(Gated);

    impl DiskJob for Beside {
        const SIDE_BY_SIDE: bool = true;

        fn key(&self) -> usize {
            self.0.key
        }

        fn run(self) {
            self.0.run();
        }
    }

    // A key whose jobs run side by side takes every thread while no other
    // key waits, and takes turns with one that does: the other key's job
    // waits for one job of the busy key, not for all it has queued.
    #[test]
    fn side_by_side_jobs_take_every_thread_and_keys_take_turns() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue = DiskQueue::new(rt.handle().clone(), 2);
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let job = |key, name, entered, release| {
            Beside(Gated {
                key,
                name,
                log: log.clone(),
                entered,
                release,
            })
        };
        // Two jobs of key 1 hold both threads.
        let (a1_in, a1_entered) = std::sync::mpsc::channel();
        let (a1_go, a1_release) = std::sync::mpsc::channel();
        queue.push(job(1, "a1", Some(a1_in), Some(a1_release)));
        a1_entered.recv().unwrap();
        let (a2_in, a2_entered) = std::sync::mpsc::channel();
        let (a2_go, a2_release) = std::sync::mpsc::channel();
        queue.push(job(1, "a2", Some(a2_in), Some(a2_release)));
        a2_entered.recv().unwrap();
        assert_eq!(queue.wakes(), 2, "one thread for each of the key's jobs");

        // More of key 1 queue, then one of key 2.
        queue.push(job(1, "a3", None, None));
        queue.push(job(1, "a4", None, None));
        queue.push(job(1, "a5", None, None));
        let (b_in, b_entered) = std::sync::mpsc::channel();
        queue.push(job(2, "b1", Some(b_in), None));
        // The thread a1 held runs one job of key 1, then key 2's.
        a1_go.send(()).unwrap();
        b_entered.recv().unwrap();
        a2_go.send(()).unwrap();
        rt.block_on(queue.idle());
        let log = log.lock();
        let at = |e: &str| log.iter().position(|x| x == e).unwrap();
        assert!(at("a3:start") < at("b1:start"), "{log:?}");
        assert!(at("b1:start") < at("a4:start"), "{log:?}");
    }

    // Both threads are held by two other keys while six jobs of key 1
    // queue, the first of them held, and then one of key 2. Key 2's job
    // starts on the second thread that comes free: none of key 1's queued
    // jobs runs ahead of it.
    #[test]
    fn a_busy_keys_side_by_side_jobs_do_not_hold_back_another_keys() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let queue = DiskQueue::new(rt.handle().clone(), 2);
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let job = |key, name, entered, release| {
            Beside(Gated {
                key,
                name,
                log: log.clone(),
                entered,
                release,
            })
        };
        let (c_in, c_entered) = std::sync::mpsc::channel();
        let (c_go, c_release) = std::sync::mpsc::channel();
        queue.push(job(3, "c", Some(c_in), Some(c_release)));
        c_entered.recv().unwrap();
        let (d_in, d_entered) = std::sync::mpsc::channel();
        let (d_go, d_release) = std::sync::mpsc::channel();
        queue.push(job(4, "d", Some(d_in), Some(d_release)));
        d_entered.recv().unwrap();

        let (a0_go, a0_release) = std::sync::mpsc::channel();
        queue.push(job(1, "a0", None, Some(a0_release)));
        for name in ["a1", "a2", "a3", "a4", "a5"] {
            queue.push(job(1, name, None, None));
        }
        let (b_in, b_entered) = std::sync::mpsc::channel();
        queue.push(job(2, "b", Some(b_in), None));
        c_go.send(()).unwrap();
        d_go.send(()).unwrap();
        b_entered.recv().unwrap();
        a0_go.send(()).unwrap();
        rt.block_on(queue.idle());
        let log = log.lock();
        let b_started = log.iter().position(|e| e == "b:start").unwrap();
        let ended = |log: &[String]| {
            log.iter()
                .filter(|e| e.starts_with('a') && e.ends_with(":end"))
                .count()
        };
        assert_eq!(ended(&log[..b_started]), 0, "{log:?}");
        assert_eq!(ended(&log), 6, "{log:?}");
        assert_eq!(queue.wakes(), 2);
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

    // Two queues and work in place share a bound of one: a queue's thread
    // starts only once the permit is free, a busy queue gives the permit up
    // between batches to the one that waited, and nothing runs beside the
    // permit's holder.
    #[test]
    fn queues_sharing_a_bound_never_run_more_at_once_than_it_has() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let shared = Arc::new(tokio::sync::Semaphore::new(1));
        let writes = DiskQueue::sharing(rt.handle().clone(), 4, shared.clone());
        let reads = DiskQueue::sharing(rt.handle().clone(), 4, shared.clone());
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let job = |key, name, entered, release| Gated {
            key,
            name,
            log: log.clone(),
            entered,
            release,
        };

        // Work in place holds the permit: a queued job has a turn waiting,
        // and does not run.
        let held = shared.clone().try_acquire_owned().unwrap();
        let (w_in, w_entered) = std::sync::mpsc::channel();
        let (w_go, w_release) = std::sync::mpsc::channel();
        writes.push(job(1, "w1", Some(w_in), Some(w_release)));
        assert_eq!(writes.at_work(), (1, 1));
        assert!(log.lock().is_empty());
        drop(held);
        w_entered.recv().unwrap();
        assert_eq!(shared.available_permits(), 0);

        // The write holds it: a read, and a second write of another key,
        // wait, the read first.
        let (r_in, r_entered) = std::sync::mpsc::channel();
        let (r_go, r_release) = std::sync::mpsc::channel();
        reads.push(job(7, "r1", Some(r_in), Some(r_release)));
        assert_eq!(reads.at_work(), (1, 1));
        writes.push(job(1, "w2", None, None));
        writes.push(job(2, "w3", None, None));
        w_go.send(()).unwrap();

        // The writes' thread gives the permit up after its batch, with two
        // more of its own waiting: the read runs before them.
        r_entered.recv().unwrap();
        assert_eq!(*log.lock(), ["w1:start", "w1:end", "r1:start"]);
        r_go.send(()).unwrap();
        rt.block_on(async {
            reads.idle().await;
            writes.idle().await;
        });
        let log = log.lock();
        assert_eq!(log[..4], ["w1:start", "w1:end", "r1:start", "r1:end"]);
        assert_eq!(log.len(), 8, "{log:?}");
        assert_eq!(shared.available_permits(), 1);
    }

    // A turn that waits for a permit when the runtime shuts down is
    // dropped, and its queued jobs settle.
    #[test]
    fn a_turn_dropped_waiting_for_a_permit_drops_the_queued_jobs() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let shared = Arc::new(tokio::sync::Semaphore::new(1));
        let queue = DiskQueue::sharing(rt.handle().clone(), 1, shared.clone());
        let _held = shared.clone().try_acquire_owned().unwrap();
        let ran = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        for key in [1, 2] {
            queue.push(Recorded {
                key,
                ran: ran.clone(),
                dropped: dropped.clone(),
                done: false,
            });
        }
        drop(rt);
        assert_eq!(
            (ran.load(Ordering::SeqCst), dropped.load(Ordering::SeqCst)),
            (0, 2)
        );
        assert!(queue.inner.jobs.lock().is_idle());
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

    // The count of whole pieces waiting is past its high mark with other
    // connections' pieces. A connection with none waiting completes one: it
    // stops with that piece, so it adds no second one past the mark. Once
    // the count is empty, a connection whose one piece passes the mark by
    // itself reads on.
    #[test]
    fn a_connection_adds_at_most_one_piece_past_the_high_mark() {
        let piece = 4 * BLOCK_LEN as u64;
        // 16 blocks: low mark 8, high mark 12.
        let pool = BlockPool::new(16 * BLOCK_LEN);
        let others = PeerDisk::default();
        let (charge, others_waiting) = pool.charge_bytes(3 * piece);
        assert!(charge.exceeded && charge.alone);
        assert!(!others.queued_piece(charge), "nothing else waits");

        let peer = PeerDisk::default();
        let (first, first_waiting) = pool.charge_bytes(piece);
        assert!(peer.queued_piece(first), "its first piece stops it");
        assert!(peer.blocked());
        assert_eq!(pool.in_use(), 12 + 4);

        drop((others_waiting, first_waiting));
        assert_eq!(pool.in_use(), 0);
        let (large, _large_waiting) = pool.charge_bytes(4 * piece);
        assert!(large.exceeded && large.alone);
        assert!(!PeerDisk::default().queued_piece(large));
    }

    // Connections complete pieces in turn, each until it stops reading. No
    // connection adds more than one piece once the count is past its high
    // mark, so the count ends past the mark by less than one piece for each
    // connection (`MAX_QUEUED_CHECK_BYTES`). A piece larger than the mark,
    // waiting alone, stands in for the mark.
    #[test]
    fn the_pieces_waiting_pass_the_high_mark_by_at_most_one_piece_per_connection() {
        // 16 blocks: high mark 12. Pieces of 5 blocks cross it mid-piece.
        let (high, piece_blocks) = (12, 5);
        let pool = BlockPool::new(16 * BLOCK_LEN);
        let peers: Vec<PeerDisk> = (0..4).map(|_| PeerDisk::default()).collect();
        let mut past_mark = vec![0usize; peers.len()];
        let mut waiting = Vec::new();
        while peers.iter().any(|peer| !peer.blocked()) {
            for (i, peer) in peers.iter().enumerate().filter(|(_, p)| !p.blocked()) {
                let (charge, span) = pool.charge_bytes((piece_blocks * BLOCK_LEN) as u64);
                waiting.push(span);
                if charge.exceeded {
                    past_mark[i] += 1;
                }
                peer.queued_piece(charge);
                assert!(past_mark[i] <= 1, "connection {i} added a second piece");
            }
        }
        assert!(pool.in_use() < high + peers.len() * piece_blocks);
        assert_eq!(pool.in_use(), 30);
        drop(waiting);

        // One piece of 20 blocks, alone: its connection reads on, and stops
        // with its next.
        let peer = PeerDisk::default();
        let large = (20 * BLOCK_LEN) as u64;
        let (alone, _alone_waiting) = pool.charge_bytes(large);
        assert!(!peer.queued_piece(alone));
        let (next, _next_waiting) = pool.charge_bytes(large);
        assert!(peer.queued_piece(next));
        assert_eq!(pool.in_use(), 20 + 20);
    }
}
