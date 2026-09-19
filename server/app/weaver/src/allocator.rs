//! The process allocator: mimalloc with the arena behaviour tuned for this
//! workload.
//!
//! Every article the pipeline moves is a few allocations in the 64 KiB to
//! 1 MiB range that are made on one thread (a connection reader or a decode
//! worker) and freed on another (the disk writer). mimalloc v3 serves those
//! from 4 MiB pages carved out of eagerly committed 1 GiB arenas, and a page
//! whose owner is not the freeing thread is abandoned to the arena rather
//! than handed back. The pages stay committed, so resident memory follows
//! the peak article rate and ratchets up across jobs instead of returning to
//! the idle floor. The reclaim and purge options do not reach this size class.
//!
//! Three options change the page lifecycle for that class and were measured
//! to cost nothing in throughput: `disallow_arena_alloc` makes large pages
//! OS-mapped so an all-free page is unmapped instead of parked in an arena,
//! `arena_eager_commit=0` stops the arena being committed up front, and
//! `page_commit_on_demand` commits page memory as blocks are touched. The
//! options are applied from the first allocation the process makes, which is
//! before mimalloc reserves its first arena; setting them from `main` would
//! be too late for that arena. An explicit `MIMALLOC_*` variable in the
//! environment for any of the three wins over the value here, so the
//! allocator can still be tuned from outside for a measurement.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicBool, Ordering};

use libmimalloc_sys::{mi_option_set, mi_option_t};
use mimalloc::MiMalloc;

/// Option indices of the bundled mimalloc v3 (libmimalloc-sys is pinned to
/// the exact version whose `mi_option_e` enum these were read from; the v2
/// allocator numbers its options differently, so the pin must move together
/// with any change to the `mimalloc` feature set).
const MI_OPTION_ARENA_EAGER_COMMIT: mi_option_t = 4;
const MI_OPTION_DISALLOW_ARENA_ALLOC: mi_option_t = 26;
const MI_OPTION_PAGE_COMMIT_ON_DEMAND: mi_option_t = 40;

/// Each option with the environment variable mimalloc itself reads for it and
/// the value applied when that variable is absent.
const TUNING: [(mi_option_t, &[u8], libc::c_long); 3] = [
    (
        MI_OPTION_DISALLOW_ARENA_ALLOC,
        b"MIMALLOC_DISALLOW_ARENA_ALLOC\0",
        1,
    ),
    (
        MI_OPTION_ARENA_EAGER_COMMIT,
        b"MIMALLOC_ARENA_EAGER_COMMIT\0",
        0,
    ),
    (
        MI_OPTION_PAGE_COMMIT_ON_DEMAND,
        b"MIMALLOC_PAGE_COMMIT_ON_DEMAND\0",
        1,
    ),
];

/// mimalloc, with [`TUNING`] applied before the first allocation goes through.
pub(crate) struct TunedMiMalloc;

static TUNED: AtomicBool = AtomicBool::new(false);

/// Applies the tuning exactly once. The first allocation of a Rust program
/// happens during runtime start-up on the main thread, before any other
/// thread exists, so the flag does not need to make later threads wait for
/// the options to be written.
#[inline]
fn tune_once() {
    if !TUNED.swap(true, Ordering::AcqRel) {
        apply_tuning();
    }
}

#[cold]
fn apply_tuning() {
    for (option, env_name, value) in TUNING {
        // `getenv` rather than `std::env`: this runs inside the allocator, and
        // the std accessors allocate their result.
        let overridden = unsafe { !libc::getenv(env_name.as_ptr().cast()).is_null() };
        if !overridden {
            unsafe { mi_option_set(option, value) };
        }
    }
}

/// Hand the calling thread's free allocator pages back.
///
/// mimalloc gives every thread its own heap. A download lane allocates the
/// article buffers that the decode and writer threads free, so those blocks
/// return to the lane thread's heap and the pages behind them are only
/// reclaimed when that thread allocates again — which a parked lane does not
/// do. `mi_collect(true)` forces the collection from the lane thread itself,
/// which is the only thread that can reach its own heap.
pub(crate) fn collect_idle_thread() {
    // SAFETY: `mi_collect` takes no pointers and is safe to call from any
    // thread that allocates through mimalloc.
    unsafe { libmimalloc_sys::mi_collect(true) };
}

unsafe impl GlobalAlloc for TunedMiMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        tune_once();
        unsafe { MiMalloc.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        tune_once();
        unsafe { MiMalloc.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        tune_once();
        unsafe { MiMalloc.realloc(ptr, layout, new_size) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { MiMalloc.dealloc(ptr, layout) }
    }
}

#[cfg(test)]
mod tests {
    use libmimalloc_sys::mi_option_get;

    use super::*;

    #[test]
    fn tuning_is_applied_before_the_first_allocation_completes() {
        // The test harness has allocated long before this runs, so the
        // options must already read back as the tuned values.
        let _touch = vec![0u8; 64 * 1024];
        assert!(TUNED.load(Ordering::Acquire));
        for (option, env_name, value) in TUNING {
            let name = std::str::from_utf8(&env_name[..env_name.len() - 1]).unwrap();
            if std::env::var_os(name).is_some() {
                continue;
            }
            assert_eq!(unsafe { mi_option_get(option) }, value, "{name}");
        }
    }

    #[test]
    fn collect_idle_thread_runs_on_a_worker_thread() {
        // The interesting caller is a long-lived blocking thread that has
        // allocated and then freed, which is the lane park shape.
        let handle = std::thread::spawn(|| {
            let buffer = vec![0u8; 4 * 1024 * 1024];
            drop(buffer);
            collect_idle_thread();
            // Still usable afterwards: the collection must not poison the heap.
            let again = vec![1u8; 64 * 1024];
            again.len()
        });
        assert_eq!(handle.join().unwrap(), 64 * 1024);
        collect_idle_thread();
    }
}
