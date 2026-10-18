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
use crate::volume::VolumeIndex;

// A run's view of its volumes: files that may still be growing. A read waits
// until the bytes are there, returns short only at a closed volume's end,
// and fails when they will never come, or when the run was cancelled.
pub trait Volumes: Send + Sync {
    fn count(&self) -> usize;

    // The volume's length, once it is closed.
    fn len(&self, volume: VolumeIndex) -> Option<u64>;

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize>;
}

// Where a run says what it found. `false` means stop: the run was cancelled
// by a retraction, a repair, or the job's end, and nothing more is heard.
pub type Sink<'a> = dyn FnMut(Emit) -> bool + 'a;

// One execution of an engine over its volumes, owned by the thread it runs
// on. It emits `Member`, `Bytes` and `MemberClosed` as it goes, and ends with
// nothing, with `NeedsPassword`, or with `Refuse` carrying why. The volumes
// are shared, not borrowed: a decoder holds a reader per volume for as long
// as it likes, and a reader that outlives no borrow is one it can keep.
pub trait Extraction: Send {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>);
}

// The engine face of an object: what the owner runs.
pub trait Engine {
    // A run from the object's present state: its password, if one opened
    // it, and which volumes it has.
    fn start(&self) -> Box<dyn Extraction>;

    // How far the run has read: the furthest volume it reached and the
    // offset in it. The owner tells the object as the run goes, so the
    // object can bid for the volumes after it; the object's state is
    // otherwise its own.
    fn reached(&mut self, _volume: VolumeIndex, _offset: u64) {}

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
