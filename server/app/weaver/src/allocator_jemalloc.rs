//! The process allocator everywhere except Windows: jemalloc, configured for
//! this workload.
//!
//! Every article the pipeline moves is a few allocations in the 64 KiB to
//! 1 MiB range that are made on one thread (a connection reader or a decode
//! worker) and freed on another (the disk writer). The same shapes recur
//! thousands of times a job at roughly one cycle per article, and then stop
//! completely when the job ends or the lane parks. An allocator has to serve
//! two opposite needs here: inside a job the pages must be recycled rather
//! than re-mapped and re-faulted per article, and at the boundaries the
//! memory must go back to the operating system instead of ratcheting up
//! across jobs.
//!
//! jemalloc holds a job's peak lower than the alternative this binary used
//! before it, for a small and measured amount of extra work per article,
//! because its decay returns pages steadily instead of either holding a whole
//! job's working set until a purge deadline or refusing to hold any page at
//! all between two articles. The settings below ask for that behaviour
//! explicitly rather than relying on the defaults:
//!
//! - `dirty_decay_ms` is the article cycle's order of magnitude, so a page
//!   freed by the writer is still there for the next article that needs one.
//! - `muzzy_decay_ms:0` returns those pages once that delay expires instead
//!   of parking them in a second, longer-lived stage where they stay resident
//!   without being reusable quickly.
//! - `narenas` caps the arena count. The default is four per core, and every
//!   arena keeps its own cache of extents, which for buffers this large is
//!   paid in resident pages. Arenas are created lazily, so the cap costs
//!   nothing on a small host and bounds the caches on a large one.
//!
//! Background purge threads are deliberately not requested: they are not
//! available on every target this binary builds for, and the two collections
//! below already cover the boundaries where a purge matters.
//!
//! One build note that does not belong in a comment anywhere else: jemalloc
//! fixes its page size when it is compiled, and a binary built for a smaller
//! page than the kernel it runs on will not work. The release build sets
//! `JEMALLOC_SYS_WITH_LG_PAGE` per target for that reason; a native build on
//! the host it runs on detects the right value on its own.

use std::alloc::{GlobalAlloc, Layout};

use tikv_jemallocator::Jemalloc;

/// Read by jemalloc's constructor, before `main` runs.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:3000,muzzy_decay_ms:0,narenas:16\0";

/// jemalloc, configured by [`malloc_conf`].
pub(crate) struct ConfiguredJemalloc;

/// The allocator `main` installs. Both allocator modules define this name, so
/// the installation site does not change with the target.
pub(crate) type ProcessAllocator = ConfiguredJemalloc;

/// The value for that static.
pub(crate) const PROCESS_ALLOCATOR: ProcessAllocator = ConfiguredJemalloc;

/// Purges the dirty pages of every arena now.
///
/// `mallctl` names all arenas at once with this index; it is the documented
/// spelling, not an arena that exists.
const PURGE_ALL_ARENAS: &std::ffi::CStr = c"arena.4096.purge";

fn purge_all_arenas() {
    // SAFETY: a purge reads no input and writes no output, so all four
    // pointers are null and both lengths are zero. The name outlives the call.
    unsafe {
        tikv_jemalloc_sys::mallctl(
            PURGE_ALL_ARENAS.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        );
    }
}

/// Hand back the pages a now-idle lane was holding.
///
/// A download lane allocates the article buffers that the decode and writer
/// threads free. Those pages sit in the arena the lane was bound to, and a
/// parked lane allocates nothing more, so nothing prompts their return until
/// the decay clock expires. This asks for them immediately instead.
pub(crate) fn collect_idle_thread() {
    purge_all_arenas();
}

/// Hand back everything the finished job was holding.
///
/// Within a job the allocator deliberately keeps freed article pages so the
/// next article reuses them; a job ending is the point where that tenancy is
/// over. Waiting for the decay clock would leave a job-sized working set
/// resident past the moment the job reports itself finished.
pub(crate) fn collect_after_job() {
    purge_all_arenas();
}

unsafe impl GlobalAlloc for ConfiguredJemalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { Jemalloc.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { Jemalloc.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe { Jemalloc.realloc(ptr, layout, new_size) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { Jemalloc.dealloc(ptr, layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_configured_settings_are_the_ones_jemalloc_starts_with() {
        // `opt.*` is what the constructor read; reading it back proves the
        // configuration string reached jemalloc rather than being ignored.
        fn opt_ms(name: &std::ffi::CStr) -> i64 {
            let mut value: i64 = -1;
            let mut len = std::mem::size_of::<i64>();
            // SAFETY: the output buffer and its length match the type
            // `mallctl` documents for these options, and no input is given.
            let rc = unsafe {
                tikv_jemalloc_sys::mallctl(
                    name.as_ptr(),
                    std::ptr::from_mut(&mut value).cast(),
                    &mut len,
                    std::ptr::null_mut(),
                    0,
                )
            };
            assert_eq!(rc, 0, "mallctl read failed");
            value
        }

        assert_eq!(opt_ms(c"opt.dirty_decay_ms"), 3000);
        assert_eq!(opt_ms(c"opt.muzzy_decay_ms"), 0);
    }

    #[test]
    fn a_purge_after_a_job_is_safe_to_call_from_any_thread() {
        let worker = std::thread::spawn(|| {
            let buffer = vec![0u8; 512 * 1024];
            drop(buffer);
            collect_after_job();
        });
        worker.join().expect("the purge must not panic");
        collect_idle_thread();
    }
}
