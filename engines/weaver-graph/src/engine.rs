// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Engines. An engine is an object that cannot read its volumes as they
// stream, so it reads them as files instead: whole, or still growing. Its
// volumes' bytes are its envelope, kept on disk by its owner, and a run of
// the engine reads that envelope on a thread of the owner's and says what it
// found through the same emits a streaming object says in `deliver`.
//
// The object stays in the tree while a run is out: it keeps answering where
// its bytes went, bids for the volumes the run will reach next, and retracts
// what the run built when a repair replaces bytes the run already consumed.
// The run is a value of its own, so the two never share a lock.

use std::io;
use std::sync::Arc;

use crate::deliver::Emit;
use crate::seek::Seek;
use crate::volume::{ByteRange, VolumeIndex};

// Bytes of a volume nothing will bring: what a read that starts on them
// fails with, as the payload of its `io::Error`, naming the whole span from
// where the read started to the next byte that is there, within the read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{volume} lost {range}: nothing will bring those bytes")]
pub struct Lost {
    pub volume: VolumeIndex,
    pub range: ByteRange,
}

impl Lost {
    // The lost span a failed read names, when that is why it failed.
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref::<Self>().copied()
    }
}

// A run's view of its volumes: files that may still be growing. A read waits
// until the bytes are there, returns short where a closed volume ends or
// where bytes nothing will bring begin, and fails when it starts on those,
// with `Lost`, or when the run was cancelled.
pub trait Volumes: Send + Sync {
    // The volume after `after` in the set's order, or its first for `None`;
    // `None` past the last bound once no member beside the volumes is still
    // deciding its owner. An index orders the volumes and need not be
    // dense, so a gap before the next one bound is waited on while a volume
    // can still join there, and passed over once none can.
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>>;

    // The volume's length, once it is closed.
    fn len(&self, volume: VolumeIndex) -> Option<u64>;

    // Whether no read of the volume waits: it is closed, and every byte of
    // it is in or will never come. Work on a lent pool reads only such
    // volumes. A view that cannot tell says no.
    fn settled(&self, _volume: VolumeIndex) -> bool {
        false
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize>;
}

// Where a run says what it found. `false` means stop: the run was cancelled
// by a retraction, a repair, or the job's end, and nothing more is heard.
pub type Sink<'a> = dyn FnMut(Emit) -> bool + 'a;

// What the owner grants one piece of object work as it starts: a run of an
// engine or a repair of a recovery set. An object sizes its decoders and its
// repair workspace from `memory_bytes`, and parallelises across no more
// than `threads`: a repair on whatever pool its owner runs it inside, a run
// on the pool the grant lends it. The grant is the one limit: an object's
// own allowance is what it holds to only when the owner granted no memory.
// The grant is fixed for the work it was given to: a later grant reaches the
// next run or repair, never one already going.
#[derive(Clone, Default)]
pub struct Budget {
    pub memory_bytes: Option<u64>,
    pub threads: usize,
    // The owner's threads, which work that spreads borrows instead of
    // starting its own, so the threads of every job together stay within
    // the pool. None lends threads of the work's own, started for it.
    pub pool: Option<Arc<dyn Pool>>,
}

// Threads the owner lends work that spreads. A pool thread must never wait
// on bytes still downloading: it would hold back every piece of work queued
// behind it.
pub trait Pool: Send + Sync {
    // `work` on `workers` threads at once, returning once every one has.
    fn spread(&self, workers: usize, work: &(dyn Fn() + Sync));
}

// Threads started for the work alone, for an owner that lends none.
struct Own;

impl Pool for Own {
    fn spread(&self, workers: usize, work: &(dyn Fn() + Sync)) {
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(work);
            }
        });
    }
}

impl std::fmt::Debug for Budget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Budget")
            .field("memory_bytes", &self.memory_bytes)
            .field("threads", &self.threads)
            .field("pool", &self.pool.is_some())
            .finish()
    }
}

// Two grants are alike when they grant the same memory and threads on the
// same pool.
impl PartialEq for Budget {
    fn eq(&self, other: &Self) -> bool {
        self.memory_bytes == other.memory_bytes
            && self.threads == other.threads
            && match (&self.pool, &other.pool) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for Budget {}

impl Budget {
    // The threads to spread work across: never none.
    pub fn threads(&self) -> usize {
        self.threads.max(1)
    }

    // The pool work spreads on: the owner's, or threads of its own.
    pub fn pool(&self) -> &dyn Pool {
        match &self.pool {
            Some(pool) => pool.as_ref(),
            None => &Own,
        }
    }

    // The memory the work may hold: the grant, or `own`, the object's own
    // allowance, when the owner granted none.
    pub fn memory_or(&self, own: u64) -> u64 {
        self.memory_bytes.unwrap_or(own)
    }
}

// One execution of an engine over its volumes, owned by the thread it runs
// on. It emits `Member`, `Bytes` and `MemberClosed` as it goes, and ends with
// nothing, with `NeedsPassword`, or with `Refuse` carrying why. The volumes
// are shared, not borrowed: a decoder holds a reader per volume for as long
// as it likes, and a reader that outlives no borrow is one it can keep.
pub trait Extraction: Send {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>, budget: Budget);
}

// The engine face of an object: what the owner runs.
pub trait Engine {
    // A run from the object's present state: its password, if one opened
    // it, and which volumes it has. A question an earlier run asked was
    // asked of bytes this run reads again, so it is the new run's to ask.
    fn start(&mut self) -> Box<dyn Extraction>;

    // What the object wants ahead of the normal order once its run reached
    // `reached`, the furthest volume it read: the volumes it reads next.
    // The owner keeps how far the run read and asks as it goes; the object
    // keeps nothing of it.
    fn bid(&self, _reached: VolumeIndex, _out: &mut Vec<Seek>) {}

    // The bytes a run holds resident while it runs: a decoder's window, read
    // from the first bytes the object was delivered. The owner reserves it
    // before it starts the run, and a run never holds more, so a reservation
    // never grows once taken. `None` while the bytes that size it have not
    // all come: no run starts before then. Nothing for an engine whose
    // library sizes its own memory.
    fn reserve(&self) -> Option<u64> {
        Some(0)
    }
}
