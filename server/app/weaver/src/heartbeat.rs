//! Periodic liveness and process-memory reporting.
//!
//! A process that vanishes without writing a line leaves nothing to correlate
//! against: the log simply stops. A steady pulse turns that into evidence —
//! the gap between the last beat and the next startup bounds when the process
//! died, the beat counter distinguishes "stopped logging" from "stopped
//! running", and the resident-set numbers beside it show whether memory was
//! climbing towards the end.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tracing::info;

/// How often the heartbeat line is written. Long enough to be free at rest,
/// short enough that the death window it brackets is useful.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Highest resident set size any sample has seen, in bytes; zero until the
/// first successful sample. A process-wide atomic rather than task state so
/// shutdown can report the same high-water mark without reaching into the
/// running task.
static PEAK_RSS_BYTES: AtomicU64 = AtomicU64::new(0);

/// Spawns the heartbeat task. It runs until aborted.
pub(crate) fn spawn_heartbeat_task() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let started = Instant::now();
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        // A stalled process must not emit a burst of catch-up beats once it
        // recovers: the whole point is that one beat means one interval lived.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick of a tokio interval completes immediately. Startup is
        // already logged elsewhere, so it is consumed rather than reported.
        interval.tick().await;

        let mut beat: u64 = 0;
        loop {
            interval.tick().await;
            beat += 1;
            let uptime_s = started.elapsed().as_secs();
            let rss = sample_and_record_rss();
            match rss {
                Some(rss_bytes) => info!(
                    uptime_s,
                    beat,
                    rss_bytes,
                    rss_peak_bytes = peak_rss_bytes(),
                    "weaver heartbeat"
                ),
                None => info!(uptime_s, beat, rss_bytes = ?rss, "weaver heartbeat"),
            }
        }
    })
}

/// Logs the high-water resident set size for this run, once.
///
/// The last heartbeat can be up to a full interval old by the time a shutdown
/// completes, so this takes one more sample first: a spike in the final
/// minute would otherwise never be recorded anywhere.
pub(crate) fn log_peak_rss() {
    let _ = sample_and_record_rss();
    let peak = peak_rss_bytes();
    if peak > 0 {
        info!(rss_peak_bytes = peak, "weaver peak memory at shutdown");
    }
}

/// The highest resident set size recorded so far, in bytes. Zero means no
/// sample has ever succeeded on this platform.
fn peak_rss_bytes() -> u64 {
    PEAK_RSS_BYTES.load(Ordering::Relaxed)
}

/// Samples the resident set size and folds it into the high-water mark.
fn sample_and_record_rss() -> Option<u64> {
    let rss = resident_set_bytes()?;
    PEAK_RSS_BYTES.fetch_max(rss, Ordering::Relaxed);
    Some(rss)
}

/// Resident set size of this process in bytes, or `None` when the platform
/// declines to report it. Never fails loudly: this is diagnostics, and a
/// refused sample must not disturb the run it is describing.
#[cfg(target_os = "linux")]
fn resident_set_bytes() -> Option<u64> {
    // `/proc/self/statm` is a single line of page counts; the second field is
    // the resident set. Cheaper than `/proc/self/status`, which formats every
    // field as text with units.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    resident_pages.checked_mul(page_size as u64)
}

#[cfg(target_os = "macos")]
fn resident_set_bytes() -> Option<u64> {
    // `proc_pidinfo` reports the task's resident size directly, which is what
    // Activity Monitor shows; there is no `/proc` to read here.
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let written = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    // A short write means the kernel filled a different structure than the one
    // asked for, so the resident field cannot be trusted.
    (written == size).then_some(info.pti_resident_size)
}

#[cfg(windows)]
fn resident_set_bytes() -> Option<u64> {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    // The working set is Windows' nearest equivalent of a resident set: the
    // pages of this process currently backed by physical memory.
    let ok = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
    (ok != 0).then(|| counters.WorkingSetSize as u64)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn resident_set_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_set_is_reported_on_this_platform() {
        let rss = resident_set_bytes().expect("this platform reports a resident set size");

        assert!(rss > 0, "resident set size should be non-zero, got {rss}");
    }

    #[test]
    fn sampling_raises_the_high_water_mark() {
        let sampled = sample_and_record_rss().expect("sample succeeds on this platform");

        assert!(peak_rss_bytes() >= sampled);
    }
}
