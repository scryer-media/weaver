// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod auth;
pub mod backup;
pub mod categories;
pub mod context;
pub mod history;
pub mod jobs;
pub mod networking;
mod observability;
pub mod post_processing;
pub mod proxies;
pub mod rss;
pub mod scalars;
pub mod schema;
pub mod servers;
pub mod settings;
pub mod speed_test;
pub mod system;

// Allocation counter for the tests that assert a page push allocates by its
// page, not by the queue. Test builds only; it counts per thread, so tests
// running beside one another do not disturb each other's count.
#[cfg(test)]
pub(crate) mod alloc_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

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

pub use auth::types::*;
pub use backup::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupService, BackupServiceError,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, RestoreOptions, RestoreReport,
    backup_error_status_code,
};
pub use categories::types::*;
pub use context::{
    SchemaContext, SystemRuntimeContext, WeaverSchema, build_schema, export_schema_sdl,
};
pub use history::types::*;
pub use jobs::release_display::release_display_names;
pub use jobs::scheduled_resume::{ScheduledResumeCoordinator, ScheduledResumeError};
pub use jobs::types::*;
pub use observability::{TestDbTaskHookGuard, install_test_db_task_hook};
pub use post_processing::types::*;
pub use rss::RssService;
pub use rss::types::*;
pub use schema::{MutationRoot, QueryRoot, SubscriptionRoot};
pub use servers::types::*;
pub use settings::types::*;
pub use system::runtime::load_global_pause_from_db;
pub use system::types::*;
pub use weaver_server_core::ingest::{
    SubmissionOptions, SubmitNzbError, SubmittedJob, fetch_nzb_from_url, init_job_counter,
    submit_nzb_bytes, submit_nzb_bytes_with_options,
};
