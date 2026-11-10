// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decode windows the owner lends. A decoder whose window (an LZ or PPMd
// dictionary) is larger than the memory it may hold in RAM can be lent
// memory the owner backs with something else, a scratch file mapped into
// memory, so the member decodes instead of being refused. The decoder keeps
// choosing: a window within `Budget::window_or` stays in RAM, a larger one
// up to the lender's ceiling is lent.

use std::io;

/// Memory lent to one decoder for one window.
///
/// # Safety
///
/// An implementor promises, from the call to `as_mut_ptr` until the value is
/// dropped:
///
/// - the pointer is non-null and valid for reads and writes of `len` bytes;
/// - those bytes are initialized, to any value: they need not be zeroed, but
///   they must hold some value, as a file mapping's do and a raw `malloc`'s
///   do not;
/// - the address stays the same, also when the value or the `Box` holding
///   it moves: the bytes live outside the value, as a mapping's do;
/// - nothing else reads or writes those bytes while the value lives.
///
/// Dropping the value releases them. These are the terms the decoders'
/// libraries ask of the memory they are lent (lzma-turbo's and unrar-rs's
/// `WindowMemory`), so memory lent here may be handed to either.
pub unsafe trait WindowMemory: Send + Sync {
    fn as_mut_ptr(&mut self) -> *mut u8;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// What lends a run its windows. A run is handed one through its `Budget`
// when its owner can lend; each call lends one window of at least `len`
// bytes, from any thread, or is refused with an `io::Error` naming why. A
// refusal of kind `StorageFull` means no folder could hold the window, a
// filesystem's refusal; any other kind means the window may not be lent at
// all (empty, or over the ceiling). A refusal is the member's "window too
// large", never a crash.
pub trait WindowLender: Send + Sync {
    fn lend(&self, len: usize) -> io::Result<Box<dyn WindowMemory>>;

    // The largest window lent at all: a decoder sets its own maximum
    // dictionary to this when it has a lender.
    fn ceiling(&self) -> u64;

    // The bytes the lender could lend now, all its windows together: a
    // decoder that borrows several at once sizes how many to what fits.
    // `None` where it cannot tell, and nothing then bounds them but each
    // window's own refusal.
    fn room(&self) -> Option<u64> {
        None
    }
}
