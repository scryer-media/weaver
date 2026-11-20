// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![deny(clippy::await_holding_lock)]

pub mod bandwidth;
pub mod history;
pub mod jobs;
pub mod pipeline;
pub mod settings;
pub mod torrents;

// The composed Database and the domain code composed over it, under the
// paths this crate's code names (`crate::Database`, `crate::proxies`, ...).
// Imported, not re-exported: the facade of the old paths is
// weaver-server-core's.
use weaver_server_composition::*;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
use weaver_server_ingest::watch_folder;

pub use jobs::{JobState, SchedulerHandle, SharedPipelineState};
pub use pipeline::Pipeline;
pub use pipeline::download::{DownloadQueue, DownloadWork};

// Allocation counter for the tests that assert a hot path allocates nothing.
//
// Test builds only. It forwards every request to the system allocator and
// counts allocations *per thread*, which is the only way to prove from inside
// a multi-threaded test binary that one closure performed none: a
// process-wide counter would pick up every other test running beside it.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
#[doc(hidden)]
pub mod alloc_probe {
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

    pub struct Counting;

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
    pub fn allocations() -> u64 {
        ALLOCATIONS.try_with(Cell::get).unwrap_or(0)
    }
}

#[cfg(test)]
#[global_allocator]
static ALLOCATOR: alloc_probe::Counting = alloc_probe::Counting;
