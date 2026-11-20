// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![deny(clippy::await_holding_lock)]

pub mod clock;
pub mod e2e_clock;
pub mod e2e_failpoint;
pub mod error;
pub mod events;
pub mod jobs {
    pub mod ids;
    pub mod model;
}
pub mod operations {
    pub mod instrumentation;
    pub mod metrics;
    pub mod usage_history;
}
pub mod runtime;
pub mod security;

pub use error::Error;

// Allocation counter for the tests that assert a hot path allocates nothing.
//
// Test builds only. It forwards every request to the system allocator and
// counts allocations *per thread*, which is the only way to prove from inside
// a multi-threaded test binary that one closure performed none: a
// process-wide counter would pick up every other test running beside it.
#[cfg(test)]
pub(crate) mod alloc_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    // `const`-initialized and `Drop`-free, so reading or writing it cannot
    // itself allocate or re-enter the allocator during thread teardown.
    thread_local! {
        static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    }

    fn record() {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
    }

    pub(crate) struct Counting;

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record();
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record();
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    // Allocations the calling thread has made so far.
    pub(crate) fn allocations() -> u64 {
        ALLOCATIONS.try_with(Cell::get).unwrap_or(0)
    }
}

#[cfg(test)]
#[global_allocator]
static ALLOCATOR: alloc_probe::Counting = alloc_probe::Counting;
pub mod ingest;
