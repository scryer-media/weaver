// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The process allocator everywhere except Windows: jemalloc, configured for
// this workload.
//
// Every article the pipeline moves is a few allocations in the 64 KiB to
// 1 MiB range that are made on one thread (a connection reader or a decode
// worker) and freed on another (the disk writer). The same shapes recur
// thousands of times a job at roughly one cycle per article, and then stop
// completely when the job ends or the lane parks. An allocator has to serve
// two opposite needs here: inside a job the pages must be recycled rather
// than re-mapped and re-faulted per article, and at the boundaries the
// memory must go back to the operating system instead of ratcheting up
// across jobs.
//
// jemalloc holds a job's peak lower than the alternative this binary used
// before it, for a small and measured amount of extra work per article,
// because its decay returns pages steadily instead of either holding a whole
// job's working set until a purge deadline or refusing to hold any page at
// all between two articles. The settings below ask for that behaviour
// explicitly rather than relying on the defaults:
//
// - `dirty_decay_ms` is the article cycle's order of magnitude, so a page
//   freed by the writer is still there for the next article that needs one.
// - `muzzy_decay_ms:0` returns those pages once that delay expires instead
//   of parking them in a second, longer-lived stage where they stay resident
//   without being reusable quickly.
// - `narenas` caps the arena count. The default is four per core, and every
//   arena keeps its own cache of extents, which for buffers this large is
//   paid in resident pages. Arenas are created lazily, so the cap costs
//   nothing on a small host and bounds the caches on a large one.
// - `lg_extent_max_active_fit:20` lets a freed run of any size serve a
//   smaller request. Freed article buffers merge into runs of many
//   megabytes, and by default a run more than 64 times the request is passed
//   over and the request takes fresh pages instead. With the lane arena
//   keeping its runs for reuse (see [`lane_arena`]), that left them beside
//   new memory: at 30 connections the process held about 130 MB more than
//   the lanes ever had live.
//
// On glibc Linux, `background_thread` also asks jemalloc for its purge
// threads (at most four), so freed pages decay back to the system on their
// own between the boundaries the two collections below cover, even while no
// thread is allocating. Every other target runs without them: Mach-O has no
// background thread, and the allocator crate marks musl as unsupported. The
// collections stay on every target; they purge at once at the boundary
// instead of waiting for the decay clock.
//
// One build note that does not belong in a comment anywhere else: jemalloc
// fixes its page size when it is compiled, and a binary built for a smaller
// page than the kernel it runs on will not work. The release build sets
// `JEMALLOC_SYS_WITH_LG_PAGE` per target for that reason; a native build on
// the host it runs on detects the right value on its own.

use std::alloc::{GlobalAlloc, Layout};

use tikv_jemallocator::Jemalloc;

// Read by jemalloc's constructor, before `main` runs.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"dirty_decay_ms:3000,muzzy_decay_ms:0,narenas:16,lg_extent_max_active_fit:20,background_thread:true\0";

// Without purge threads, `oversize_threshold:0` is needed as well. By default,
// when freed buffers merge into a free run of 8 MiB or more, jemalloc
// gives the whole run back to the system at once, and decay never gets a
// say. The disk writers free a burst of adjacent article buffers together, so
// a burst's memory went back as soon as it was written, and the next burst
// took page faults to get it again. With the threshold off, those pages
// decay like any other, and the collections below still return them at job
// end and at idle.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"dirty_decay_ms:3000,muzzy_decay_ms:0,narenas:16,lg_extent_max_active_fit:20,oversize_threshold:0\0";

// jemalloc, configured by [`malloc_conf`].
pub(crate) struct ConfiguredJemalloc;

// The allocator `main` installs. Both allocator modules define this name, so
// the installation site does not change with the target.
pub(crate) type ProcessAllocator = ConfiguredJemalloc;

// The value for that static.
pub(crate) const PROCESS_ALLOCATOR: ProcessAllocator = ConfiguredJemalloc;

// Purges the dirty pages of every arena now.
//
// `mallctl` names all arenas at once with this index; it is the documented
// spelling, not an arena that exists.
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

// Hand back the pages a now-idle lane was holding.
//
// A download lane allocates the article buffers that the decode and writer
// threads free. Those pages sit in the arena the lane was bound to, and a
// parked lane allocates nothing more, so nothing prompts their return until
// the decay clock expires. This asks for them immediately instead.
pub(crate) fn collect_idle_thread() {
    purge_all_arenas();
}

// Hand back everything the finished job was holding.
//
// Within a job the allocator deliberately keeps freed article pages so the
// next article reuses them; a job ending is the point where that tenancy is
// over. Waiting for the decay clock would leave a job-sized working set
// resident past the moment the job reports itself finished.
pub(crate) fn collect_after_job() {
    purge_all_arenas();
}

// The arena every download lane allocates from, created on first use. `None`
// when jemalloc refused to create or configure it; the lanes then stay on
// the arenas jemalloc gives every thread, as before.
static LANE_ARENA: std::sync::OnceLock<Option<u32>> = std::sync::OnceLock::new();

// Creates the lane arena with decay off.
//
// A lane allocates an article buffer, the disk writer frees it once it is
// written, and the lane allocates the next one into the pages that came
// back. A full volume buffer stalls the lanes until it has drained well
// below its limit, so the writer frees tens of megabytes while no lane
// allocates. jemalloc's decay does not judge pages by their age: at each
// step it keeps only as many dirty pages as its count has grown by, and
// pages freed again after they were reused count as no growth. So each such
// stretch went back to the system within one step, whatever the decay
// time, and the lanes faulted it all in again as they ran on: about 20
// thousand pages a 6 GB job, at 2 connections.
//
// Here decay is off, so a lane's freed pages stay for the next article.
// What the arena holds is bounded by the most the lanes ever had live at
// once, which the volume buffers already bound, and both collections above
// purge it with every other arena: a parked lane and a finished job still
// return it all.
fn lane_arena() -> Option<u32> {
    *LANE_ARENA.get_or_init(|| {
        let mut arena: u32 = 0;
        let mut len = std::mem::size_of::<u32>();
        // SAFETY: `arenas.create` writes the new arena's index, a `u32`,
        // into the output buffer whose length is given; no input.
        let rc = unsafe {
            tikv_jemalloc_sys::mallctl(
                c"arenas.create".as_ptr(),
                std::ptr::from_mut(&mut arena).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 {
            return None;
        }
        let name = std::ffi::CString::new(format!("arena.{arena}.dirty_decay_ms"))
            .expect("no interior nul");
        let mut never: isize = -1;
        // SAFETY: `arena.<i>.dirty_decay_ms` is an `ssize_t`; the input
        // pointer and length match it and nothing is read back.
        let rc = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::from_mut(&mut never).cast(),
                std::mem::size_of::<isize>(),
            )
        };
        (rc == 0).then_some(arena)
    })
}

// Binds the calling download lane thread to the lane arena. Called once, as
// the lane thread starts.
pub(crate) fn bind_lane_thread() {
    let Some(mut arena) = lane_arena() else {
        return;
    };
    // SAFETY: `thread.arena` takes the arena index as a `u32`; the input
    // pointer and length match it and nothing is read back.
    unsafe {
        tikv_jemalloc_sys::mallctl(
            c"thread.arena".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::from_mut(&mut arena).cast(),
            std::mem::size_of::<u32>(),
        );
    }
}

// SQLite's allocations, served by jemalloc on macOS.
//
// SQLite is compiled into the binary with its default allocator, the C
// library's malloc. On macOS that heap keeps the pages its callers free, so a
// start-up's or a restore's SQLite page cache and statements stay resident
// long after they are freed, and asking the zone for relief returns none of
// them. Served by jemalloc instead, those pages decay back with the rest and
// go at once with every purge above.
//
// Must run before SQLite is first used: SQLite refuses a new allocator once
// it is initialized, and then keeps its own.
//
// Only macOS. The Linux release builds link musl, whose allocator returns
// freed memory on its own; Windows builds the other allocator module.
#[cfg(target_os = "macos")]
pub(crate) fn route_sqlite_allocations() {
    use std::ffi::{c_int, c_void};

    // `sqlite3_mem_methods`, field for field.
    #[repr(C)]
    struct MemMethods {
        x_malloc: unsafe extern "C" fn(c_int) -> *mut c_void,
        x_free: unsafe extern "C" fn(*mut c_void),
        x_realloc: unsafe extern "C" fn(*mut c_void, c_int) -> *mut c_void,
        x_size: unsafe extern "C" fn(*mut c_void) -> c_int,
        x_roundup: unsafe extern "C" fn(c_int) -> c_int,
        x_init: unsafe extern "C" fn(*mut c_void) -> c_int,
        x_shutdown: unsafe extern "C" fn(*mut c_void),
        app_data: *mut c_void,
    }

    const SQLITE_CONFIG_MALLOC: c_int = 4;

    unsafe extern "C" {
        // Linked from the SQLite the database layer bundles.
        fn sqlite3_config(op: c_int, ...) -> c_int;
    }

    // SQLite asks only for positive sizes below `i32::MAX`; jemalloc may round
    // the largest up past it, so a size reported back is capped there, which
    // only understates the room SQLite has.
    fn reported(size: usize) -> c_int {
        c_int::try_from(size).unwrap_or(c_int::MAX)
    }
    unsafe extern "C" fn malloc(size: c_int) -> *mut c_void {
        // SAFETY: plain allocation of a positive size.
        unsafe { tikv_jemalloc_sys::malloc(size.max(1) as usize) }
    }
    unsafe extern "C" fn free(ptr: *mut c_void) {
        // SAFETY: SQLite frees only what `malloc` or `realloc` above gave it.
        unsafe { tikv_jemalloc_sys::free(ptr) }
    }
    unsafe extern "C" fn realloc(ptr: *mut c_void, size: c_int) -> *mut c_void {
        // SAFETY: as `free`; the size is positive.
        unsafe { tikv_jemalloc_sys::realloc(ptr, size.max(1) as usize) }
    }
    unsafe extern "C" fn size(ptr: *mut c_void) -> c_int {
        if ptr.is_null() {
            return 0;
        }
        // SAFETY: as `free`.
        reported(unsafe { tikv_jemalloc_sys::malloc_usable_size(ptr) })
    }
    unsafe extern "C" fn roundup(size: c_int) -> c_int {
        // SAFETY: a size query; it allocates nothing.
        reported(unsafe { tikv_jemalloc_sys::nallocx(size.max(1) as usize, 0) })
    }
    unsafe extern "C" fn init(_: *mut c_void) -> c_int {
        0
    }
    unsafe extern "C" fn shutdown(_: *mut c_void) {}

    let methods = MemMethods {
        x_malloc: malloc,
        x_free: free,
        x_realloc: realloc,
        x_size: size,
        x_roundup: roundup,
        x_init: init,
        x_shutdown: shutdown,
        app_data: std::ptr::null_mut(),
    };
    // SAFETY: `SQLITE_CONFIG_MALLOC` takes one pointer to a
    // `sqlite3_mem_methods` and copies it before returning. Called on the
    // main thread before any other thread exists. If SQLite was somehow
    // already initialized it refuses and keeps its own allocator, which is
    // the behaviour without this call.
    unsafe {
        sqlite3_config(SQLITE_CONFIG_MALLOC, &raw const methods);
    }
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
        // `opt.lg_extent_max_active_fit` is a `size_t`, read here as the
        // same eight bytes.
        assert_eq!(opt_ms(c"opt.lg_extent_max_active_fit"), 20);
        // Off where no purge thread runs: a merged free run of any size
        // decays instead of going back to the system the moment it forms.
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        assert_eq!(opt_ms(c"opt.oversize_threshold"), 0);
    }

    // Reads a boolean `mallctl` value.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn read_bool(name: &std::ffi::CStr) -> bool {
        let mut value = false;
        let mut len = std::mem::size_of::<bool>();
        // SAFETY: both names below are documented as `bool`, the output
        // buffer and its length match that type, and no input is given.
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

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn glibc_linux_runs_the_background_purge_threads() {
        // `opt.background_thread` is what the configuration asked for;
        // `background_thread` is whether jemalloc actually turned it on.
        assert!(read_bool(c"opt.background_thread"));
        assert!(read_bool(c"background_thread"));
    }

    // Reads a `u32` `mallctl` value, optionally writing a pointer first.
    fn read_u32(name: &std::ffi::CStr, input: Option<*const u8>) -> u32 {
        let mut value: u32 = u32::MAX;
        let mut len = std::mem::size_of::<u32>();
        let mut ptr = input.unwrap_or(std::ptr::null());
        let (new, new_len) = match input {
            Some(_) => (
                std::ptr::from_mut(&mut ptr).cast(),
                std::mem::size_of::<*const u8>(),
            ),
            None => (std::ptr::null_mut(), 0),
        };
        // SAFETY: every name read here is an `unsigned`; the one input,
        // `arenas.lookup`, takes a pointer, and its length is given.
        let rc = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr(),
                std::ptr::from_mut(&mut value).cast(),
                &mut len,
                new,
                new_len,
            )
        };
        assert_eq!(rc, 0, "mallctl failed for {name:?}");
        value
    }

    #[test]
    fn a_lane_allocates_from_an_arena_that_keeps_its_pages_until_a_collection() {
        let lane = std::thread::spawn(|| {
            bind_lane_thread();
            let bound = read_u32(c"thread.arena", None);
            // An article-sized buffer, as a lane decodes into.
            let buffer = vec![1u8; 768 * 1024];
            let home = read_u32(c"arenas.lookup", Some(buffer.as_ptr()));
            (bound, home, buffer)
        });
        let (bound, home, buffer) = lane.join().expect("the lane thread must not panic");
        let arena = lane_arena().expect("jemalloc creates the lane arena");
        assert_eq!(bound, arena);
        assert_eq!(home, arena);
        // Freed on another thread, as the disk writer does: the pages go
        // back to the arena they came from.
        drop(buffer);

        // Decay is off for the lane arena only.
        fn decay_ms(arena: u32) -> isize {
            let name = std::ffi::CString::new(format!("arena.{arena}.dirty_decay_ms"))
                .expect("no interior nul");
            let mut value: isize = 0;
            let mut len = std::mem::size_of::<isize>();
            // SAFETY: `arena.<i>.dirty_decay_ms` is an `ssize_t`; the output
            // buffer and its length match it, and no input is given.
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
        assert_eq!(decay_ms(arena), -1);
        assert_eq!(decay_ms(0), 3000);
        // Both collections purge every arena below `arenas.narenas`, so the
        // lane arena is one of them.
        assert!(arena < read_u32(c"arenas.narenas", None));
        collect_after_job();
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
