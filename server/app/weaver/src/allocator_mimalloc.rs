//! The process allocator on Windows: mimalloc with the arena behaviour tuned
//! for this workload.
//!
//! Every other target builds `allocator_jemalloc` instead. jemalloc has no
//! Windows support, and the Windows system heap is the one this binary can
//! least afford to fall back to, so Windows keeps the tuned mimalloc and the
//! two modules present the same three items to `main`.
//!
//! Every article the pipeline moves is a few allocations in the 64 KiB to
//! 1 MiB range that are made on one thread (a connection reader or a decode
//! worker) and freed on another (the disk writer). The same shapes recur
//! thousands of times a job at roughly one cycle per article, and then stop
//! completely when the job ends or the lane parks. The allocator regime has
//! to serve two opposite needs: inside a job the pages must be recycled, not
//! re-mapped and re-faulted per article; at the boundaries the memory must go
//! back to the operating system instead of ratcheting up across jobs.
//!
//! The regime here is "recycle inside a bounded arena, release at the
//! boundaries":
//!
//! - `disallow_arena_alloc = 0` keeps the arena in the path, so a freed
//!   article-sized page is reused by the next article instead of costing an
//!   unmap and a fresh set of first-touch faults every time.
//! - `arena_reserve` is cut from the 1 GiB default to 128 MiB, so the
//!   reservation granularity is the size of a working set rather than the
//!   size of a machine.
//! - `arena_eager_commit = 0` and `page_commit_on_demand = 1` keep commit
//!   proportional to what is actually touched; with the arena on, that first
//!   touch is paid once per arena page instead of once per article.
//! - `purge_delay` is raised to 3 s so pages comfortably outlive the ~1 s
//!   article cycle and are reused, while `purge_decommits` (the default,
//!   stated here so the pair reads together) still returns them to the
//!   operating system once the delay expires.
//!
//! The boundaries are handled in code rather than by the clock:
//! [`collect_idle_thread`] on lane park and [`collect_after_job`] when a job
//! reaches a terminal state.
//!
//! The options are applied from the first allocation the process makes, which
//! is before mimalloc reserves its first arena; setting them from `main` would
//! be too late for that arena. An explicit `MIMALLOC_*` variable in the
//! environment for any option set here wins over the value here, so the
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
const MI_OPTION_PURGE_DECOMMITS: mi_option_t = 5;
const MI_OPTION_PURGE_DELAY: mi_option_t = 15;
const MI_OPTION_ARENA_RESERVE: mi_option_t = 23;
const MI_OPTION_DISALLOW_ARENA_ALLOC: mi_option_t = 26;
const MI_OPTION_PAGE_COMMIT_ON_DEMAND: mi_option_t = 40;

/// Arena reservation granularity, in KiB (the unit mimalloc stores this
/// option in). 128 MiB: a few times the in-flight article bytes of a busy
/// job, and small enough that an idle process is not holding a gigabyte of
/// address space per arena.
const ARENA_RESERVE_KIB: libc::c_long = 128 * 1024;

/// How long a freed page waits before it is handed back, in milliseconds.
/// Longer than the article cycle so pages are reused within a job, short
/// enough that an idle process drains within a few seconds.
const PURGE_DELAY_MS: libc::c_long = 3_000;

/// Each option with the environment variable mimalloc itself reads for it and
/// the value applied when that variable is absent.
const TUNING: [(mi_option_t, &[u8], libc::c_long); 6] = [
    (
        MI_OPTION_DISALLOW_ARENA_ALLOC,
        b"MIMALLOC_DISALLOW_ARENA_ALLOC\0",
        0,
    ),
    (
        MI_OPTION_ARENA_RESERVE,
        b"MIMALLOC_ARENA_RESERVE\0",
        ARENA_RESERVE_KIB,
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
    (
        MI_OPTION_PURGE_DELAY,
        b"MIMALLOC_PURGE_DELAY\0",
        PURGE_DELAY_MS,
    ),
    (MI_OPTION_PURGE_DECOMMITS, b"MIMALLOC_PURGE_DECOMMITS\0", 1),
];

/// mimalloc, with [`TUNING`] applied before the first allocation goes through.
pub(crate) struct TunedMiMalloc;

/// The allocator `main` installs. Both allocator modules define this name, so
/// the installation site does not change with the target.
pub(crate) type ProcessAllocator = TunedMiMalloc;

/// The value for that static.
pub(crate) const PROCESS_ALLOCATOR: ProcessAllocator = TunedMiMalloc;

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

/// Hand back everything the finished job was holding.
///
/// Within a job the allocator deliberately keeps freed article pages around
/// so the next article reuses them; a job ending is the point where that
/// tenancy is over. Waiting for the purge clock would leave the whole
/// job-sized working set resident for the delay and, for pages abandoned by
/// threads that have since gone quiet, longer than that. This forces the
/// collection instead, so resident memory is back at the idle floor by the
/// time the job shows up as finished.
pub(crate) fn collect_after_job() {
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
