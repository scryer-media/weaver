// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Hooks the binary installs so the pipeline can hand allocator memory back
// at the two points where it knows the memory is no longer wanted: a thread
// going to sleep, and a job reaching a terminal state.
//
// The download lanes are long-lived blocking threads. Under a per-thread-heap
// allocator each lane allocates the read chunks that the decode and writer
// threads later free, so the freed pages land back on the lane's own heap and
// stay there: the allocator reclaims them the next time that thread
// allocates, which a parked lane never does. Working set then tracks the
// configured connection count rather than the work in flight.
//
// This crate must not know which allocator the binary linked, so each
// release is a function pointer the binary installs at start-up. With no hook
// installed — tests, a build whose global allocator is the system one — the
// call is a no-op.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

static IDLE_THREAD_RELEASE: OnceLock<fn()> = OnceLock::new();
static JOB_COMPLETION_RELEASE: OnceLock<fn()> = OnceLock::new();
static LANE_THREAD_START: OnceLock<fn()> = OnceLock::new();

// Install what the allocator does as a download lane thread starts: under
// jemalloc, bind it to the arena the lanes keep their article pages in until
// one of the releases below. The first installation wins.
pub fn install_lane_thread_start(hook: fn()) {
    let _ = LANE_THREAD_START.set(hook);
}

// Run the allocator's lane start, if a hook is installed. Called once, on
// the lane thread itself, before it allocates anything for an article.
#[inline]
pub fn start_lane_thread() {
    if let Some(hook) = LANE_THREAD_START.get() {
        hook();
    }
}

// Install the allocator's idle-thread release. The first installation wins;
// a later one is ignored so a stray second call cannot swap the hook out
// from under a running pipeline.
pub fn install_idle_thread_release(hook: fn()) {
    let _ = IDLE_THREAD_RELEASE.set(hook);
}

// Release the calling thread's idle allocator memory, if a hook is installed.
//
// Must be called on the thread whose memory is being released: the hook
// operates on the caller's own heap.
#[inline]
pub fn release_idle_thread_memory() {
    if let Some(hook) = IDLE_THREAD_RELEASE.get() {
        hook();
    }
}

// Whether a hook has been installed. Exists for tests and diagnostics.
pub fn idle_thread_release_installed() -> bool {
    IDLE_THREAD_RELEASE.get().is_some()
}

// Install the allocator's end-of-job release. As with the idle hook, the
// first installation wins.
pub fn install_job_completion_release(hook: fn()) {
    let _ = JOB_COMPLETION_RELEASE.set(hook);
}

// Release what a finished job left behind, if a hook is installed.
//
// Unlike the idle release this is not thread-scoped: it runs on whichever
// thread retires the job and asks the allocator to give back everything it
// is holding, including the pages abandoned by pipeline threads that have
// already gone quiet.
#[inline]
pub fn release_job_completion_memory() {
    if let Some(hook) = JOB_COMPLETION_RELEASE.get() {
        hook();
    }
}

// Whether a release asked for off the runtime is queued and not yet begun.
static RELEASE_QUEUED: AtomicBool = AtomicBool::new(false);

// The end-of-job release for a job class that ends its downloads on the
// async runtime: the purge returns pages to the system one call at a time,
// so it runs on a blocking thread. A release asked for while one is queued
// and not yet begun is that one, so a burst of endings, such as a removal of
// many jobs at once, costs one purge. With no hook installed nothing is
// queued; outside a runtime it runs where it is asked.
pub(crate) fn release_job_completion_memory_off_runtime() {
    drop(queued_release());
}

// `release_job_completion_memory_off_runtime`, with the task it queued.
fn queued_release() -> Option<tokio::task::JoinHandle<()>> {
    let hook = *JOB_COMPLETION_RELEASE.get()?;
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        hook();
        return None;
    };
    if RELEASE_QUEUED.swap(true, Ordering::AcqRel) {
        return None;
    }
    Some(runtime.spawn_blocking(move || {
        // Cleared before the purge: pages freed after this point are left
        // to the next release asked for.
        RELEASE_QUEUED.store(false, Ordering::Release);
        hook();
    }))
}

// Release what the process is holding for work that has stopped, if a hook is
// installed: the pipeline has just run out of work.
//
// Without a background purge thread the allocator returns freed pages only
// while it is being used, so pages freed just before the process goes quiet
// (the last articles of a download, the parse of a bulk add, a start-up
// restore) would otherwise stay resident until the next burst of work. The
// release is the same whole-process purge a finished job asks for.
#[inline]
pub(crate) fn release_idle_process_memory() {
    release_job_completion_memory();
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counting_hook() {
        CALLS.fetch_add(1, Ordering::Relaxed);
    }

    // One test, because the hook is process-wide and installed once.
    #[test]
    fn an_installed_hook_runs_on_every_release_and_cannot_be_replaced() {
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        // Before installation the release must be a silent no-op.
        release_idle_thread_memory();
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);

        install_idle_thread_release(counting_hook);
        assert!(idle_thread_release_installed());
        release_idle_thread_memory();
        release_idle_thread_memory();
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);

        // A second installation is ignored rather than taking over.
        fn other_hook() {
            CALLS.fetch_add(100, Ordering::Relaxed);
        }
        install_idle_thread_release(other_hook);
        release_idle_thread_memory();
        assert_eq!(CALLS.load(Ordering::Relaxed), 3);
    }

    static JOB_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counting_job_hook() {
        JOB_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    // One test, because the hook is process-wide and installed once. It never
    // touches the idle hook: the two tests share a process and the idle test
    // asserts on the state of that one.
    #[test]
    fn an_installed_job_completion_hook_runs_on_every_release_and_cannot_be_replaced() {
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 0);
        release_job_completion_memory();
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 0);

        install_job_completion_release(counting_job_hook);
        release_job_completion_memory();
        release_job_completion_memory();
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 2);

        // A pipeline going idle asks for the same whole-process release.
        release_idle_process_memory();
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 3);

        fn other_job_hook() {
            JOB_CALLS.fetch_add(100, Ordering::Relaxed);
        }
        install_job_completion_release(other_job_hook);
        release_job_completion_memory();
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 4);

        // Off the runtime: the same hook, once, on a blocking thread.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            queued_release().expect("queued").await.unwrap();
        });
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 5);
        // One asked for while another is queued and not begun is that one.
        RELEASE_QUEUED.store(true, Ordering::Release);
        runtime.block_on(async { assert!(queued_release().is_none()) });
        RELEASE_QUEUED.store(false, Ordering::Release);
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 5);
        // Outside a runtime it runs where it is asked.
        release_job_completion_memory_off_runtime();
        assert_eq!(JOB_CALLS.load(Ordering::Relaxed), 6);
    }
}
