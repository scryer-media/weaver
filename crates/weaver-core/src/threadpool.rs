use std::sync::Arc;

/// Build a dedicated rayon thread pool for post-processing work
/// (extraction, PAR2 verify/repair). Threads are niced on Unix
/// so the OS scheduler prefers download/decode threads when CPU
/// is contended.
pub fn build_postprocess_pool(thread_count: usize) -> Arc<rayon::ThreadPool> {
    Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .thread_name(|i| format!("weaver-pp-{i}"))
            .start_handler(|_| {
                #[cfg(unix)]
                {
                    // SAFETY: setpriority with PRIO_PROCESS and tid 0 (current thread)
                    // is a well-defined POSIX call. nice(10) = lower priority.
                    unsafe {
                        libc::setpriority(libc::PRIO_PROCESS, 0, 10);
                    }
                }
            })
            .build()
            .expect("failed to build post-processing thread pool"),
    )
}
