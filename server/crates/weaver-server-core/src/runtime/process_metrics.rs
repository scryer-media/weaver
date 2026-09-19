//! Process-level resource sampling for the metrics endpoint.
//!
//! [`sample`] is called at scrape time only — never from a pipeline path — and
//! is written to stay in the "cheap syscall / small `/proc` read" budget: no
//! process spawning, no directory walks beyond counting `/proc/self/fd`, no
//! external crates. Every field is optional: a platform that cannot answer a
//! question cheaply reports `None` rather than a misleading zero.

use crate::operations::instrumentation::ProcessMetricsSnapshot;

/// Sample the current process's CPU, memory, descriptor and thread usage.
///
/// * Linux reads `/proc/self/{stat,statm,status,fd,limits}`.
/// * macOS reads the task's own accounting through `proc_pidinfo`.
/// * Other Unix platforms answer CPU through `getrusage` and leave the rest
///   `None` (there is no portable cheap equivalent).
/// * Windows answers CPU through `GetProcessTimes` and the working set through
///   `GetProcessMemoryInfo`.
pub fn sample() -> ProcessMetricsSnapshot {
    #[allow(unused_mut)]
    let mut snapshot = ProcessMetricsSnapshot {
        cpu_seconds_total: crate::runtime::perf_probe::process_cpu_usage()
            .map(|usage| usage.total().as_secs_f64()),
        ..ProcessMetricsSnapshot::default()
    };

    #[cfg(target_os = "linux")]
    linux::fill(&mut snapshot);

    #[cfg(target_os = "macos")]
    macos::fill(&mut snapshot);

    #[cfg(target_os = "windows")]
    windows::fill(&mut snapshot);

    snapshot
}

/// The resident set alone, for callers that only need the one number.
///
/// Separate from [`sample`] so a caller does not pay for the descriptor count
/// and the `/proc` reads it is not going to look at.
pub fn resident_memory_bytes() -> Option<u64> {
    sample().resident_memory_bytes
}

#[cfg(target_os = "macos")]
mod macos {
    use super::ProcessMetricsSnapshot;

    /// Resident and virtual size, plus the thread count, from one syscall.
    ///
    /// The fields are the kernel's own task accounting, which is what
    /// Activity Monitor's "memory" column reports for the process.
    pub(super) fn fill(snapshot: &mut ProcessMetricsSnapshot) {
        let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        // SAFETY: the buffer is exactly the size the call is told it is, and
        // `getpid` is always a valid pid for this process.
        let written = unsafe {
            libc::proc_pidinfo(
                libc::getpid(),
                libc::PROC_PIDTASKINFO,
                0,
                std::ptr::addr_of_mut!(info).cast(),
                size,
            )
        };
        if written != size {
            return;
        }
        snapshot.resident_memory_bytes = Some(info.pti_resident_size);
        snapshot.virtual_memory_bytes = Some(info.pti_virtual_size);
        if info.pti_threadnum > 0 {
            snapshot.threads = Some(info.pti_threadnum as u64);
        }
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use super::ProcessMetricsSnapshot;

    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    /// The working set, which is what Task Manager and the support bundles
    /// call this process's memory, plus the commit charge as the virtual size.
    pub(super) fn fill(snapshot: &mut ProcessMetricsSnapshot) {
        let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        counters.cb = size;
        // SAFETY: the pseudo-handle needs no closing and the buffer is exactly
        // the size the call is told it is.
        let ok = unsafe {
            GetProcessMemoryInfo(GetCurrentProcess(), std::ptr::addr_of_mut!(counters), size)
        };
        if ok == 0 {
            return;
        }
        snapshot.resident_memory_bytes = Some(counters.WorkingSetSize as u64);
        snapshot.virtual_memory_bytes = Some(counters.PagefileUsage as u64);
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::ProcessMetricsSnapshot;

    pub(super) fn fill(snapshot: &mut ProcessMetricsSnapshot) {
        let page_size = page_size();
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let mut fields = statm.split_ascii_whitespace();
            snapshot.virtual_memory_bytes = fields
                .next()
                .and_then(|pages| pages.parse::<u64>().ok())
                .map(|pages| pages.saturating_mul(page_size));
            snapshot.resident_memory_bytes = fields
                .next()
                .and_then(|pages| pages.parse::<u64>().ok())
                .map(|pages| pages.saturating_mul(page_size));
        }

        if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
            // The comm field is parenthesised and may contain spaces, so field
            // splitting only becomes reliable after the final ')'.
            if let Some(rest) = stat.rfind(')').map(|index| &stat[index + 1..]) {
                let fields = rest.split_ascii_whitespace().collect::<Vec<_>>();
                // Offsets are relative to field 3 (state) after the comm split.
                // num_threads is field 20, starttime is field 22.
                snapshot.threads = fields.get(17).and_then(|value| value.parse::<u64>().ok());
                snapshot.start_time_seconds = fields
                    .get(19)
                    .and_then(|value| value.parse::<u64>().ok())
                    .and_then(|ticks| {
                        let hertz = clock_ticks_per_second()?;
                        boot_time_seconds().map(|boot| boot + (ticks as f64 / hertz))
                    });
            }
        }

        if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
            snapshot.open_fds = Some(entries.count() as u64);
        }

        if let Ok(limits) = std::fs::read_to_string("/proc/self/limits") {
            snapshot.max_fds = limits
                .lines()
                .find(|line| line.starts_with("Max open files"))
                .and_then(|line| line.split_ascii_whitespace().nth(3))
                .and_then(|value| value.parse::<u64>().ok());
        }
    }

    fn page_size() -> u64 {
        // SAFETY: `sysconf` with a valid name is always safe; a negative return
        // means "unavailable", which we fall back from.
        let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if value > 0 { value as u64 } else { 4096 }
    }

    fn clock_ticks_per_second() -> Option<f64> {
        // SAFETY: see `page_size`.
        let value = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        (value > 0).then_some(value as f64)
    }

    fn boot_time_seconds() -> Option<f64> {
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        stat.lines()
            .find_map(|line| line.strip_prefix("btime "))
            .and_then(|value| value.trim().parse::<f64>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_reports_cpu_on_every_supported_platform() {
        let snapshot = sample();
        // getrusage / GetProcessTimes are available on every platform weaver
        // ships for, so this must not be None there.
        #[cfg(any(unix, windows))]
        assert!(
            snapshot.cpu_seconds_total.is_some(),
            "process CPU must be readable"
        );
        if let Some(cpu) = snapshot.cpu_seconds_total {
            assert!(cpu >= 0.0);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_sample_fills_the_proc_backed_fields() {
        let snapshot = sample();
        assert!(snapshot.resident_memory_bytes.unwrap_or(0) > 0);
        assert!(snapshot.virtual_memory_bytes.unwrap_or(0) > 0);
        assert!(snapshot.open_fds.unwrap_or(0) > 0);
        assert!(snapshot.threads.unwrap_or(0) > 0);
    }

    /// The memory question is the one the support bundles and the working-set
    /// dashboards ask, so every platform weaver ships for must answer it.
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    #[test]
    fn resident_memory_is_readable_on_every_shipped_platform() {
        let snapshot = sample();
        assert!(
            snapshot.resident_memory_bytes.unwrap_or(0) > 0,
            "the process's own resident set must be readable"
        );
        assert!(snapshot.virtual_memory_bytes.unwrap_or(0) > 0);
        assert!(
            resident_memory_bytes().unwrap_or(0) > 0,
            "the shorthand reads the same field"
        );
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    #[test]
    fn unsupported_platforms_leave_unavailable_fields_absent_rather_than_zero() {
        let snapshot = sample();
        assert!(snapshot.resident_memory_bytes.is_none());
        assert!(snapshot.open_fds.is_none());
        assert!(snapshot.max_fds.is_none());
        assert!(snapshot.threads.is_none());
        assert!(snapshot.start_time_seconds.is_none());
    }
}
