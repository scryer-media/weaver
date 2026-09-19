//! A hook the binary installs so pipeline threads can hand their idle
//! allocator memory back when they go to sleep.
//!
//! The download lanes are long-lived blocking threads. Under a per-thread-heap
//! allocator each lane allocates the read chunks that the decode and writer
//! threads later free, so the freed pages land back on the lane's own heap and
//! stay there: the allocator reclaims them the next time that thread
//! allocates, which a parked lane never does. Working set then tracks the
//! configured connection count rather than the work in flight.
//!
//! This crate must not know which allocator the binary linked, so the release
//! is a function pointer the binary installs at start-up. With no hook
//! installed — tests, a build whose global allocator is the system one — the
//! call is a no-op.

use std::sync::OnceLock;

static IDLE_THREAD_RELEASE: OnceLock<fn()> = OnceLock::new();

/// Install the allocator's idle-thread release. The first installation wins;
/// a later one is ignored so a stray second call cannot swap the hook out
/// from under a running pipeline.
pub fn install_idle_thread_release(hook: fn()) {
    let _ = IDLE_THREAD_RELEASE.set(hook);
}

/// Release the calling thread's idle allocator memory, if a hook is installed.
///
/// Must be called on the thread whose memory is being released: the hook
/// operates on the caller's own heap.
#[inline]
pub fn release_idle_thread_memory() {
    if let Some(hook) = IDLE_THREAD_RELEASE.get() {
        hook();
    }
}

/// Whether a hook has been installed. Exists for tests and diagnostics.
pub fn idle_thread_release_installed() -> bool {
    IDLE_THREAD_RELEASE.get().is_some()
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
}
