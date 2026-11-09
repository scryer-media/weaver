// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decode windows the owner lends. A decoder whose window (an LZ or PPMd
// dictionary) is larger than the memory it may hold in RAM can be lent
// memory the owner backs with something else, a scratch file mapped into
// memory, so the member decodes instead of being refused. The decoder keeps
// choosing: a window within `Budget::window_or` stays in RAM, a larger one
// up to the lender's ceiling is lent.

use std::io;

// Memory lent to one decoder for one window.
//
// # Safety
//
// An implementor promises that `as_mut_ptr` points at no fewer than `len`
// writable bytes, that the address stays the same until the value is
// dropped, and that nothing else reads or writes those bytes while the value
// lives. The bytes need not be zeroed. Dropping the value releases them.
pub unsafe trait WindowMemory: Send + Sync {
    fn as_mut_ptr(&mut self) -> *mut u8;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// What lends a run its windows. A run is handed one through its `Budget`
// when its owner can lend; each call lends one window, refused with an
// `io::Error` naming why (too large, no room on disk, the filesystem cannot
// map it). A refusal is the member's "window too large", never a crash.
pub trait WindowLender: Send + Sync {
    fn lend(&self, len: usize) -> io::Result<Box<dyn WindowMemory>>;

    // The largest window lent at all: a decoder sets its own maximum
    // dictionary to this when it has a lender.
    fn ceiling(&self) -> u64;
}
