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
use crate::volume::VolumeIndex;

// A run's view of its volumes: files that may still be growing. A read waits
// until the bytes are there, returns short only at a closed volume's end,
// and fails when they will never come, or when the run was cancelled.
pub trait Volumes: Send + Sync {
    // The volume after `after` in the set's order, or its first for `None`;
    // `None` past the last bound once no member beside the volumes is still
    // deciding its owner. An index orders the volumes and need not be
    // dense, so a gap before the next one bound is waited on while a volume
    // can still join there, and passed over once none can.
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>>;

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
