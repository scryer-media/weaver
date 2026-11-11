// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The windows a run's decoders borrow from their owner, in the terms each
// decoder's library asks for them. The owner lends `weaver_graph`
// `WindowMemory` through the run's `Budget::lender`; lzma-turbo and unrar-rs
// each take memory through an allocator trait of their own. This is the one
// place the two meet, and the one place each library is told how much it
// keeps in RAM and how much it may borrow:
//
//   in RAM    for RAR and xz, the window the run was granted
//             (`Budget::window_or`), as without a lender; for 7z, what
//             the run's memory holds beside the decoder's own state
//             (`sevenz_of`)
//   borrowed  anything over it, up to the lender's ceiling
//
// Without a lender every library is configured exactly as it always was.
//
// sevenz-turbo is adapted the same way: an `unsafe impl` of its memory trait
// for `Lent`, an `impl` of its allocator trait for `Lender`, and its limits
// built from the same two numbers (`sevenz`).
//
// A library keeps only the `io::ErrorKind` of a refusal, so a refusal means
// what its kind says (`refused`).

use std::io;
use std::sync::Arc;

use weaver_graph::{Budget, Why, WindowLender, WindowMemory};

// The run's lender, as each library's allocator.
pub(crate) struct Lender(Arc<dyn WindowLender>);

impl Lender {
    // The lender the run was handed, if it was handed one.
    pub(crate) fn of(budget: &Budget) -> Option<Arc<Self>> {
        budget.lender.clone().map(|lender| Arc::new(Self(lender)))
    }

    // The largest window lent at all.
    pub(crate) fn ceiling(&self) -> u64 {
        self.0.ceiling()
    }

    fn lend(&self, len: usize) -> io::Result<Lent> {
        self.0.lend(len).map(Lent)
    }
}

// One lent window, as a library holds it.
struct Lent(Box<dyn WindowMemory>);

impl Lent {
    fn ptr(&mut self) -> *mut u8 {
        WindowMemory::as_mut_ptr(&mut *self.0)
    }

    fn len(&self) -> usize {
        WindowMemory::len(&*self.0)
    }
}

// SAFETY: lzma-turbo asks for a non-null pointer to at least the length it
// asked, initialized, valid for reads and writes, at an address that holds
// until drop also when the `Box` moves, and exclusive to the decoder. The
// `weaver_graph::WindowMemory` this wraps promises each of those for its own
// pointer and `len` (see its `# Safety`), and the bytes live behind its own
// `Box`, so moving `Lent` moves no byte. `weaver_graph::WindowLender::lend`
// lends at least the length asked, and lzma-turbo checks the pointer and
// length it is given before it uses them all the same. `Send` is required
// and held: the wrapped memory is `Send + Sync`.
unsafe impl lzma_turbo::WindowMemory for Lent {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr()
    }

    fn len(&self) -> usize {
        Lent::len(self)
    }
}

// SAFETY: unrar-rs asks the same as lzma-turbo, for a window and for a PPMd
// arena alike: a non-null pointer to at least the length asked, initialized,
// readable and writable, at an address stable until drop across moves of
// the value, and exclusive to the decoder. The wrapped
// `weaver_graph::WindowMemory` promises each, as above; unrar-rs checks
// null and short memory itself before use. It asks only `Send`, which the
// wrapped `Send + Sync` memory gives.
unsafe impl unrar_rs::WindowMemory for Lent {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr()
    }

    fn len(&self) -> usize {
        Lent::len(self)
    }
}

// SAFETY: sevenz-turbo asks, from `as_mut_ptr` until drop: a non-null
// pointer valid for reads and writes of `len()` bytes, at least the length
// it asked; those bytes initialized, to any value; an address stable until
// drop, also when the `Box` holding the value moves; and the memory
// exclusive to the decoder. The wrapped `weaver_graph::WindowMemory`
// promises each, as above, and `weaver_graph::WindowLender::lend` lends at
// least the length asked; sevenz-turbo checks null and short memory itself
// before use. It asks only `Send`, which the wrapped `Send + Sync` memory
// gives.
unsafe impl sevenz_turbo::WindowMemory for Lent {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr()
    }

    fn len(&self) -> usize {
        Lent::len(self)
    }
}

// Each library may ask from any thread, and more than once at a time (one
// window per xz block in flight, per RAR worker, or per 7z folder and
// threaded LZMA2 worker): the lender is `Send + Sync` and lends each window
// on its own.
impl lzma_turbo::WindowAllocator for Lender {
    fn allocate(&self, len: usize) -> io::Result<Box<dyn lzma_turbo::WindowMemory>> {
        Ok(Box::new(self.lend(len)?))
    }
}

impl unrar_rs::WindowAllocator for Lender {
    fn allocate(&self, len: usize) -> io::Result<Box<dyn unrar_rs::WindowMemory>> {
        Ok(Box::new(self.lend(len)?))
    }
}

impl sevenz_turbo::WindowAllocator for Lender {
    fn allocate(&self, len: usize) -> io::Result<Box<dyn sevenz_turbo::WindowMemory>> {
        Ok(Box::new(self.lend(len)?))
    }
}

// The 7z decoder's limits for a run: the memory the run was granted, or the
// decoder's own limits when none was. Without a lender that is all, as it
// always was.
//
// With a lender, every decode buffer over the window the run was granted (a
// dictionary, a PPMd arena, a threaded LZMA2 worker's buffer) is borrowed,
// up to the lender's ceiling, and counts nothing against the memory limit,
// which bounds what stays in RAM. A decode of a parsed archive keeps in RAM
// what its memory holds instead (`sevenz_of`). A threaded decode, or folders
// decoded side by side, borrows several windows at once (`sevenz_threads`).
pub(crate) fn sevenz(budget: &Budget) -> sevenz_turbo::ArchiveLimits {
    let own = sevenz_turbo::ArchiveLimits::default();
    let limits = sevenz_turbo::ArchiveLimits {
        memory_limit_bytes: budget.memory_or(own.memory_limit_bytes),
        ..own
    };
    let Some(lender) = Lender::of(budget) else {
        return limits;
    };
    let ceiling = lender.ceiling();
    let mut limits = limits.with_window_allocator(lender, budget.window_or(u64::MAX));
    limits.max_lent_window_bytes = ceiling;
    limits
}

// The 7z decoder's limits for a decode of `archive`: as `sevenz`, but with
// a lender a buffer the run's memory holds beside the decoder's own state
// stays in RAM, as it would without one, and only what would not fit there
// is lent. A dictionary as wide as the memory is lent, not refused for the
// state beside it. A folder with more than one such buffer keeps in RAM
// what the run's window holds, or lends every one, whichever first fits.
pub(crate) fn sevenz_of(
    archive: &sevenz_turbo::Archive,
    budget: &Budget,
) -> sevenz_turbo::ArchiveLimits {
    let limits = sevenz(budget);
    if limits.window_allocator.is_none() {
        return limits;
    }
    let memory = limits.memory_limit_bytes;
    let keeping = |in_ram: u64| {
        let limits = sevenz_turbo::ArchiveLimits {
            max_in_ram_window_bytes: in_ram,
            ..limits.clone()
        };
        let resident = archive.decoder_memory(&limits, 1).ok()?.resident_bytes;
        Some((resident, limits))
    };
    // What the decoder holds in RAM with every buffer lent.
    let Some((state, _)) = keeping(0) else {
        return limits;
    };
    [memory.saturating_sub(state), budget.window_or(u64::MAX), 0]
        .into_iter()
        .filter_map(keeping)
        .find(|(resident, _)| *resident <= memory)
        .map_or(limits, |(_, limits)| limits)
}

// The threads a 7z decode of `archive` under `limits` runs on: `most`, or
// fewer, down to one, so the windows it can have lent at once fit the room
// its lender has now. A lender that cannot tell its room, or none, leaves
// `most`. One thread is the least: a refusal from there is the scratch's.
pub(crate) fn sevenz_threads(
    archive: &sevenz_turbo::Archive,
    limits: &sevenz_turbo::ArchiveLimits,
    budget: &Budget,
    most: u32,
) -> u32 {
    let Some(room) = budget.lender.as_ref().and_then(|lender| lender.room()) else {
        return most;
    };
    (1..=most.max(1))
        .rev()
        .find(|&threads| {
            archive
                .decoder_memory(limits, threads)
                .is_ok_and(|memory| memory.lent_bytes <= room)
        })
        .unwrap_or(1)
}

// What a 7z decode of `archive` holds in RAM on one thread with nothing
// lent: the memory a run the scratch refused needs to decode it in RAM.
pub(crate) fn sevenz_in_ram(
    archive: &sevenz_turbo::Archive,
    limits: &sevenz_turbo::ArchiveLimits,
) -> Option<u64> {
    let limits = sevenz_turbo::ArchiveLimits {
        window_allocator: None,
        max_in_ram_window_bytes: u64::MAX,
        ..limits.clone()
    };
    archive
        .decoder_memory(&limits, 1)
        .ok()
        .map(|memory| memory.resident_bytes)
}

// The xz decoder's backing for a run that was handed a lender: a dictionary
// over `in_ram`, the window the run was granted, is borrowed, up to the
// lender's ceiling. `None` without a lender: the decoder holds every
// dictionary itself, as it always did.
pub(crate) fn xz(budget: &Budget, in_ram: u64) -> Option<lzma_turbo::WindowBacking> {
    let lender = Lender::of(budget)?;
    let ceiling = lender.ceiling();
    Some(lzma_turbo::WindowBacking::new(lender, in_ram).with_max_len(ceiling))
}

// The RAR decoder's limits for a run. A dictionary is held to the window
// the run was granted, or the decoder's own limit when none was; a RAR3
// PPMd arena keeps the format's own ceiling, narrowed by the window too.
//
// With a lender, a dictionary or a PPMd arena over that window is borrowed
// instead of refused: a dictionary up to the lender's ceiling, an arena up
// to the format's (256 MiB), no more than the lender's.
pub(crate) fn rar(budget: &Budget) -> unrar_rs::Limits {
    let mut limits = unrar_rs::Limits::default();
    limits.max_dict_size = budget.window_or(limits.max_dict_size);
    let Some(lender) = Lender::of(budget) else {
        limits.max_ppmd_arena_size = limits.max_ppmd_arena_size.min(budget.window_or(u64::MAX));
        return limits;
    };
    let ceiling = lender.ceiling();
    limits.max_ppmd_arena_size = limits.max_ppmd_arena_size.min(ceiling);
    let in_ram = limits.max_dict_size.min(limits.max_ppmd_arena_size);
    limits
        .with_window_allocator(lender)
        .with_max_allocated_dict_size(ceiling)
        .with_max_heap_ppmd_arena_size(in_ram)
}

// What a lender's refusal of a `needed`-byte window means for the member,
// read from the kind of the error it refused with, which is all a library
// keeps of it. `StorageFull` is the filesystem's refusal: the window is too
// large for any scratch folder, and the member needs `needed` bytes the owner
// may give it in RAM, where its policy lets a run go above its ceiling
// (`Runs::widen`). Any other kind is a window that may not be lent at all,
// which nothing the job does changes.
pub(crate) fn refused(needed: u64, kind: io::ErrorKind, allowed: Option<u64>) -> Why {
    match kind {
        io::ErrorKind::StorageFull => Why::TooLarge {
            needed: Some(needed),
            allowed,
        },
        _ => Why::Unsupported,
    }
}

#[cfg(test)]
pub(crate) mod testing;
