// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// System capability detection using only std primitives.
//
// Runs once at startup to build a [`SystemProfile`] describing the host
// machine. All subprocess calls use [`std::process::Command`] (blocking)
// since this only executes once during initialization.

#[cfg(any(target_os = "linux", all(test, unix)))]
use std::path::Component;
use std::path::{Path, PathBuf};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::system_profile::*;
use tracing::{debug, info, warn};

// Probe the running system and return a fully measured [`SystemProfile`].
//
// Detection is best-effort: any individual probe that fails silently
// falls back to a safe default rather than aborting startup.
pub fn detect(output_dir: &Path) -> SystemProfile {
    let mut profile = detect_startup_profile(output_dir);
    let measured = startup_disk_measurement(output_dir);
    profile.disk.sequential_write_mbps = measured.sequential_write_mbps;
    profile.disk.random_read_iops = measured.random_read_iops;
    profile
}

// Classify the storage behind one directory without measuring it.
//
// The startup profile only ever describes a single directory, so a caller that
// wants the class and filesystem of a *second* configured root (the
// intermediate or complete directory, say) has no way to ask for it. This is
// the same classification the startup profile uses, with no benchmark attached:
// it runs the platform's filesystem lookup and nothing else.
pub fn classify_storage(path: &Path) -> (StorageClass, FilesystemType) {
    detect_disk_info(path)
}

// Collect the fast system facts needed to start the runtime.
//
// Random-read IOPS and sequential write throughput are intentionally left at
// zero until [`startup_disk_measurement`] runs. Zero IOPS is conservative:
// the tuner admits only one extraction until the measured values are
// applied. Zero write throughput reads as "not measured".
pub fn detect_startup_profile(output_dir: &Path) -> SystemProfile {
    let cpu = detect_cpu();
    let memory = detect_memory();
    let (storage_class, filesystem) = detect_disk_info(output_dir);
    let disk = DiskProfile {
        storage_class,
        filesystem,
        sequential_write_mbps: 0.0,
        random_read_iops: 0.0,
        same_filesystem: true,
    };

    SystemProfile { cpu, memory, disk }
}

// ---------------------------------------------------------------------------
// CPU
// ---------------------------------------------------------------------------

fn detect_cpu() -> CpuProfile {
    let logical_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // Without sysinfo we cannot reliably distinguish physical from logical
    // cores using only std. Use logical as a reasonable stand-in; the
    // adaptive runtime will self-correct based on actual throughput.
    let physical_cores = detect_physical_cores().unwrap_or(logical_cores);

    let simd = detect_simd();
    let cgroup_limit = detect_cgroup_cpu_limit();

    CpuProfile {
        physical_cores,
        logical_cores,
        simd,
        cgroup_limit,
    }
}

// Try to read physical core count from the OS.
fn detect_physical_cores() -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("sysctl")
            .arg("-n")
            .arg("hw.physicalcpu")
            .output()
            .ok()?;
        let s = String::from_utf8_lossy(&output.stdout);
        s.trim().parse().ok()
    }

    #[cfg(target_os = "linux")]
    {
        // Count unique physical core IDs across all processors.
        let contents = std::fs::read_to_string("/proc/cpuinfo").ok()?;
        let mut core_ids = std::collections::HashSet::new();
        let mut current_physical = String::new();
        let mut current_core = String::new();

        for line in contents.lines() {
            if let Some(val) = line.strip_prefix("physical id")
                && let Some(v) = val.split(':').nth(1)
            {
                current_physical = v.trim().to_string();
            }
            if let Some(val) = line.strip_prefix("core id")
                && let Some(v) = val.split(':').nth(1)
            {
                current_core = v.trim().to_string();
            }
            if line.trim().is_empty() && !current_physical.is_empty() {
                core_ids.insert(format!("{current_physical}:{current_core}"));
                current_physical.clear();
                current_core.clear();
            }
        }
        // Final entry (file may not end with blank line).
        if !current_physical.is_empty() {
            core_ids.insert(format!("{current_physical}:{current_core}"));
        }

        if core_ids.is_empty() {
            None
        } else {
            Some(core_ids.len())
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

fn detect_simd() -> SimdSupport {
    SimdSupport {
        #[cfg(target_arch = "x86_64")]
        sse42: std::arch::is_x86_feature_detected!("sse4.2"),
        #[cfg(not(target_arch = "x86_64"))]
        sse42: false,

        #[cfg(target_arch = "x86_64")]
        avx2: std::arch::is_x86_feature_detected!("avx2"),
        #[cfg(not(target_arch = "x86_64"))]
        avx2: false,

        #[cfg(target_arch = "x86_64")]
        avx512: std::arch::is_x86_feature_detected!("avx512f"),
        #[cfg(not(target_arch = "x86_64"))]
        avx512: false,

        #[cfg(target_arch = "aarch64")]
        neon: true, // NEON is mandatory on AArch64.
        #[cfg(not(target_arch = "aarch64"))]
        neon: false,
    }
}

// Read cgroup v2 CPU quota if running in a container.
fn detect_cgroup_cpu_limit() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        let content = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
        let mut parts = content.split_whitespace();
        let quota_str = parts.next()?;
        let period_str = parts.next()?;

        if quota_str == "max" {
            return None; // Unlimited.
        }

        let quota: f64 = quota_str.parse().ok()?;
        let period: f64 = period_str.parse().ok()?;
        Some(quota / period)
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct ProcessMemoryFacts {
    memory_bytes: Option<(u64, u64)>,
    cgroup_limit: Option<u64>,
}

static PROCESS_MEMORY_FACTS: OnceLock<ProcessMemoryFacts> = OnceLock::new();

fn cached_process_memory_facts(
    cache: &OnceLock<ProcessMemoryFacts>,
    detect_memory_bytes: impl FnOnce() -> Option<(u64, u64)>,
    detect_cgroup_memory_limit: impl FnOnce() -> Option<u64>,
) -> &ProcessMemoryFacts {
    cache.get_or_init(|| ProcessMemoryFacts {
        memory_bytes: detect_memory_bytes(),
        cgroup_limit: detect_cgroup_memory_limit(),
    })
}

fn process_memory_facts() -> &'static ProcessMemoryFacts {
    cached_process_memory_facts(
        &PROCESS_MEMORY_FACTS,
        detect_memory_bytes,
        detect_cgroup_memory_limit,
    )
}

fn memory_profile_from_facts(facts: &ProcessMemoryFacts) -> MemoryProfile {
    let (total, available) = facts.memory_bytes.unwrap_or((
        16 * 1024 * 1024 * 1024, // 16 GiB default
        8 * 1024 * 1024 * 1024,  // 8 GiB default
    ));

    MemoryProfile {
        total_bytes: total,
        available_bytes: available,
        cgroup_limit: facts.cgroup_limit,
    }
}

pub(crate) fn detect_memory() -> MemoryProfile {
    memory_profile_from_facts(process_memory_facts())
}

// Total memory the process can actually use: physical total, clamped by any
// cgroup v2 limit (containers see the host's /proc/meminfo, not their own
// budget). Used for memory-scaled policy defaults (e.g. the RAR
// dictionary-size ceiling).
pub(crate) fn detect_total_memory_bytes() -> Option<u64> {
    let facts = process_memory_facts();
    let total = facts.memory_bytes.map(|(total, _)| total)?;
    Some(match facts.cgroup_limit {
        Some(limit) => total.min(limit),
        None => total,
    })
}

// Returns (total_bytes, available_bytes).
fn detect_memory_bytes() -> Option<(u64, u64)> {
    #[cfg(target_os = "macos")]
    {
        // Total memory via sysctl.
        let total_output = Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
            .ok()?;
        let total: u64 = String::from_utf8_lossy(&total_output.stdout)
            .trim()
            .parse()
            .ok()?;

        // Available memory: use vm_stat to approximate.
        let available = macos_available_memory().unwrap_or(total / 2);

        Some((total, available))
    }

    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
        let mut total: Option<u64> = None;
        let mut available: Option<u64> = None;

        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                total = parse_meminfo_kb(rest);
            } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
                available = parse_meminfo_kb(rest);
            }
            if total.is_some() && available.is_some() {
                break;
            }
        }

        Some((total?, available?))
    }

    #[cfg(windows)]
    {
        windows_memory_bytes()
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        None
    }
}

// Windows: `GlobalMemoryStatusEx` reports physical total and available.
#[cfg(windows)]
fn windows_memory_bytes() -> Option<(u64, u64)> {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    let mut status = MEMORYSTATUSEX {
        dwLength: std::mem::size_of::<MEMORYSTATUSEX>() as u32,
        dwMemoryLoad: 0,
        ullTotalPhys: 0,
        ullAvailPhys: 0,
        ullTotalPageFile: 0,
        ullAvailPageFile: 0,
        ullTotalVirtual: 0,
        ullAvailVirtual: 0,
        ullAvailExtendedVirtual: 0,
    };
    // SAFETY: `status` is a properly sized, initialized MEMORYSTATUSEX and
    // outlives the call.
    let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
    if ok == 0 || status.ullTotalPhys == 0 {
        return None;
    }
    Some((status.ullTotalPhys, status.ullAvailPhys))
}

// Parse a `/proc/meminfo` value like `"  16384000 kB"` into bytes.
#[cfg(target_os = "linux")]
fn parse_meminfo_kb(s: &str) -> Option<u64> {
    let kb: u64 = s.split_whitespace().next()?.parse().ok()?;
    Some(kb * 1024)
}

// Approximate available memory on macOS using `vm_stat`.
#[cfg(target_os = "macos")]
fn macos_available_memory() -> Option<u64> {
    let output = Command::new("vm_stat").output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);

    // vm_stat reports page counts. First line has the page size.
    let page_size: u64 = text
        .lines()
        .next()?
        .split("page size of ")
        .nth(1)?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;

    let mut free: u64 = 0;
    let mut inactive: u64 = 0;
    let mut purgeable: u64 = 0;

    for line in text.lines() {
        if let Some(pages) = extract_vm_stat_pages(line, "Pages free") {
            free = pages;
        } else if let Some(pages) = extract_vm_stat_pages(line, "Pages inactive") {
            inactive = pages;
        } else if let Some(pages) = extract_vm_stat_pages(line, "Pages purgeable") {
            purgeable = pages;
        }
    }

    Some((free + inactive + purgeable) * page_size)
}

// Extract the page count from a vm_stat line like `"Pages free:    12345."`.
#[cfg(target_os = "macos")]
fn extract_vm_stat_pages(line: &str, prefix: &str) -> Option<u64> {
    if !line.starts_with(prefix) {
        return None;
    }
    let after_colon = line.split(':').nth(1)?;
    after_colon.trim().trim_end_matches('.').parse().ok()
}

// Read the process's cgroup memory limit, including nested cgroup v2 and v1
// hierarchies. `/sys/fs/cgroup/memory.max` alone is only the namespace root;
// a systemd service can be constrained farther down that tree.
fn detect_cgroup_memory_limit() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let cgroups = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
        cgroup_memory_limit_from(&cgroups, &mountinfo, |path| {
            std::fs::read_to_string(path).ok()
        })
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[derive(Debug, Clone)]
struct CgroupMount {
    root: PathBuf,
    mount_point: PathBuf,
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn cgroup_memory_limit_from(
    cgroups: &str,
    mountinfo: &str,
    mut read_file: impl FnMut(&Path) -> Option<String>,
) -> Option<u64> {
    let v2 = cgroup_path_for_controller(cgroups, None)
        .zip(cgroup_mount_for(mountinfo, "cgroup2", None))
        .and_then(|(path, mount)| {
            cgroup_limit_in_hierarchy(&mount, path, "memory.max", &mut read_file)
        });
    let v1 = cgroup_path_for_controller(cgroups, Some("memory"))
        .zip(cgroup_mount_for(mountinfo, "cgroup", Some("memory")))
        .and_then(|(path, mount)| {
            cgroup_limit_in_hierarchy(&mount, path, "memory.limit_in_bytes", &mut read_file)
        });

    match (v2, v1) {
        (Some(v2), Some(v1)) => Some(v2.min(v1)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn cgroup_path_for_controller<'a>(cgroups: &'a str, controller: Option<&str>) -> Option<&'a str> {
    cgroups.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let _hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?;
        let matches = match controller {
            Some(controller) => controllers.split(',').any(|entry| entry == controller),
            None => controllers.is_empty(),
        };
        matches.then_some(path)
    })
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn cgroup_mount_for(
    mountinfo: &str,
    filesystem: &str,
    controller: Option<&str>,
) -> Option<CgroupMount> {
    mountinfo.lines().find_map(|line| {
        let (left, right) = line.split_once(" - ")?;
        let mut fields = left.split_whitespace();
        let _mount_id = fields.next()?;
        let _parent_id = fields.next()?;
        let _major_minor = fields.next()?;
        let root = fields.next()?;
        let mount_point = fields.next()?;
        let fs_type = right.split_whitespace().next()?;
        if fs_type != filesystem {
            return None;
        }
        if let Some(controller) = controller
            && !right
                .split_whitespace()
                .flat_map(|field| field.split(','))
                .any(|entry| entry == controller)
        {
            return None;
        }
        Some(CgroupMount {
            root: PathBuf::from(root),
            mount_point: PathBuf::from(mount_point),
        })
    })
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn cgroup_limit_in_hierarchy(
    mount: &CgroupMount,
    cgroup_path: &str,
    limit_file: &str,
    read_file: &mut impl FnMut(&Path) -> Option<String>,
) -> Option<u64> {
    let mut directory = cgroup_directory(mount, cgroup_path)?;
    let mut limit = None;
    loop {
        if let Some(value) = read_file(&directory.join(limit_file))
            .as_deref()
            .and_then(parse_cgroup_memory_limit)
        {
            limit = Some(limit.map_or(value, |current: u64| current.min(value)));
        }
        if directory == mount.mount_point {
            break;
        }
        if !directory.pop() || !directory.starts_with(&mount.mount_point) {
            break;
        }
    }
    limit
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn cgroup_directory(mount: &CgroupMount, cgroup_path: &str) -> Option<PathBuf> {
    let group = Path::new(cgroup_path).strip_prefix("/").ok()?;
    let root = mount.root.strip_prefix("/").ok()?;
    let relative = if root.as_os_str().is_empty() {
        group
    } else {
        group.strip_prefix(root).ok()?
    };
    let mut directory = mount.mount_point.clone();
    for component in relative.components() {
        match component {
            Component::Normal(part) => directory.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(directory)
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn parse_cgroup_memory_limit(value: &str) -> Option<u64> {
    let value = value.trim();
    if value == "max" {
        return None;
    }
    let limit = value.parse::<u64>().ok()?;
    // cgroup v1 represents no limit as a value just below i64::MAX.
    (limit < (1_u64 << 60)).then_some(limit)
}

// ---------------------------------------------------------------------------
// Disk
// ---------------------------------------------------------------------------

// What the startup disk probe measured: sequential write-through throughput
// and random 4 KiB read IOPS on the data directory's filesystem. A zero
// `sequential_write_mbps` means it was not measured.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DiskMeasurement {
    pub sequential_write_mbps: f64,
    pub random_read_iops: f64,
}

// The measurement kept between starts, with the time it was taken.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
struct StoredDiskMeasurement {
    measured_at: u64,
    sequential_write_mbps: f64,
    random_read_iops: f64,
}

// Where the measurement is kept, in the probed directory.
const DISK_PROBE_RECORD: &str = "disk-probe.json";
// The probe's scratch file. A plain name with a text extension rather than a
// dotfile: security software watching the data directory treats a hidden
// file written with write-through as suspicious and scans it on every write.
const DISK_PROBE_FILE: &str = "weaver-disk-probe.txt";
// A stored measurement older than this is taken again. The disk under a data
// directory rarely changes, and the probe is the disk work a start can skip.
const DISK_PROBE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
// Used when the probe cannot run at all: SSD-class reads, writes unmeasured.
const DISK_PROBE_FALLBACK_IOPS: f64 = 10_000.0;

// The write phase writes this buffer repeatedly, then the read phase reads
// back from what it laid down.
const PROBE_CHUNK_BYTES: usize = 16 * 1024 * 1024;
// How many buffers each successive write chunk holds: 16, 64, 144, 256 MiB.
// Growing chunks keep a fast disk from finishing inside timer noise while a
// slow one stops after the first.
const PROBE_WRITE_REPEATS: [usize; 4] = [1, 4, 9, 16];
// The write phase stops before the next chunk once this much write-and-sync
// time has been measured.
const PROBE_WRITE_BUDGET: Duration = Duration::from_secs(1);
const PROBE_READ_SIZE: usize = 4096;
const PROBE_READ_COUNT: usize = 200;

// The disk measurement to apply at startup.
//
// `WEAVER_STARTUP_IOPS` pins the read IOPS and skips the probe entirely, so
// benchmark and e2e runs neither wait on it nor read a stored result. Other-
// wise a measurement stored within the last seven days is used as is, and a
// missing, unreadable or older one is measured again and stored.
pub fn startup_disk_measurement(dir: &Path) -> DiskMeasurement {
    if let Some(pinned) = startup_iops_override() {
        debug!(iops = pinned, "disk probe skipped: WEAVER_STARTUP_IOPS");
        return DiskMeasurement {
            sequential_write_mbps: 0.0,
            random_read_iops: pinned,
        };
    }
    stored_or_measured_disk_profile(dir, SystemTime::now, measure_disk_profile)
}

// The stored-or-measured decision with its clock and probe passed in, so a
// test can age the record without waiting and without touching the disk.
fn stored_or_measured_disk_profile(
    dir: &Path,
    now: impl Fn() -> SystemTime,
    measure: impl FnOnce(&Path) -> Option<DiskMeasurement>,
) -> DiskMeasurement {
    let record = dir.join(DISK_PROBE_RECORD);
    let now_secs = unix_seconds(now());
    if let Some(stored) = read_stored_disk_measurement(&record)
        && let Some(age) = now_secs.checked_sub(stored.measured_at)
        && age < DISK_PROBE_MAX_AGE.as_secs()
    {
        info!(
            age_days = age / (24 * 60 * 60),
            write_mbps = format!("{:.0}", stored.sequential_write_mbps),
            iops = format!("{:.0}", stored.random_read_iops),
            "using stored disk measurement"
        );
        return DiskMeasurement {
            sequential_write_mbps: stored.sequential_write_mbps,
            random_read_iops: stored.random_read_iops,
        };
    }

    let started = Instant::now();
    let Some(measured) = measure(dir) else {
        // Nothing is stored, so the next start tries again.
        warn!(
            dir = %dir.display(),
            "disk probe could not run; assuming fast reads and unmeasured writes"
        );
        return DiskMeasurement {
            sequential_write_mbps: 0.0,
            random_read_iops: DISK_PROBE_FALLBACK_IOPS,
        };
    };
    info!(
        write_mbps = format!("{:.0}", measured.sequential_write_mbps),
        iops = format!("{:.0}", measured.random_read_iops),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "measured disk"
    );
    let stored = StoredDiskMeasurement {
        measured_at: unix_seconds(now()),
        sequential_write_mbps: measured.sequential_write_mbps,
        random_read_iops: measured.random_read_iops,
    };
    if let Err(error) = write_stored_disk_measurement(&record, &stored) {
        debug!(
            path = %record.display(),
            error = %error,
            "could not store the disk measurement; it is taken again next start"
        );
    }
    measured
}

fn unix_seconds(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

fn read_stored_disk_measurement(path: &Path) -> Option<StoredDiskMeasurement> {
    let bytes = std::fs::read(path).ok()?;
    let stored: StoredDiskMeasurement = serde_json::from_slice(&bytes).ok()?;
    let usable = |value: f64| value.is_finite() && value >= 0.0;
    (usable(stored.sequential_write_mbps)
        && usable(stored.random_read_iops)
        && stored.random_read_iops > 0.0)
        .then_some(stored)
}

fn write_stored_disk_measurement(
    path: &Path,
    stored: &StoredDiskMeasurement,
) -> std::io::Result<()> {
    let json = serde_json::to_vec(stored).map_err(std::io::Error::other)?;
    std::fs::write(path, json)
}

// `WEAVER_STARTUP_IOPS=<positive number>` skips the startup disk probe and
// pins `random_read_iops` to that value, so benchmark and e2e runs are not
// skewed by the probe's I/O wait (and tuning stays deterministic across
// runs). Unset or invalid values run the probe.
fn startup_iops_override() -> Option<f64> {
    parse_startup_iops(std::env::var("WEAVER_STARTUP_IOPS").ok().as_deref())
}

fn parse_startup_iops(raw: Option<&str>) -> Option<f64> {
    let value: f64 = raw?.trim().parse().ok()?;
    (value.is_finite() && value > 0.0).then_some(value)
}

// Removes the probe's scratch file however the probe ends.
struct ProbeFileGuard(PathBuf);

impl Drop for ProbeFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

// Measure sequential write-through throughput, then random 4 KiB reads on
// the file the writes laid down, in the given directory.
//
// The writes go through the cache to the device and the reads bypass it
// where the platform allows, so neither figure is the page cache's. `None`
// when the directory cannot hold the probe file or any I/O fails.
pub fn measure_disk_profile(dir: &Path) -> Option<DiskMeasurement> {
    let path = dir.join(DISK_PROBE_FILE);
    let _guard = ProbeFileGuard(path.clone());
    let (written, sequential_write_mbps) = measure_sequential_write(&path)?;
    let random_read_iops = measure_random_read(&path, written)?;
    Some(DiskMeasurement {
        sequential_write_mbps,
        random_read_iops,
    })
}

// Random bytes, so a compressing filesystem or drive firmware cannot shrink
// the writes. Prepared before any timing starts.
fn probe_write_buffer() -> Vec<u8> {
    let mut buffer = vec![0u8; PROBE_CHUNK_BYTES];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for word in buffer.chunks_exact_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        word.copy_from_slice(&state.to_le_bytes());
    }
    buffer
}

// Returns the bytes written and the throughput in MB/s (10^6 bytes).
fn measure_sequential_write(path: &Path) -> Option<(u64, f64)> {
    use std::io::Write;

    let buffer = probe_write_buffer();
    let mut file = open_write_through(path).ok()?;
    let mut written: u64 = 0;
    let mut measured = Duration::ZERO;
    for repeats in PROBE_WRITE_REPEATS {
        if measured >= PROBE_WRITE_BUDGET {
            break;
        }
        let started = Instant::now();
        for _ in 0..repeats {
            file.write_all(&buffer).ok()?;
        }
        file.sync_data().ok()?;
        measured += started.elapsed();
        written += (repeats * PROBE_CHUNK_BYTES) as u64;
    }
    let seconds = measured.as_secs_f64();
    let mbps = if seconds > 0.0 {
        written as f64 / seconds / 1_000_000.0
    } else {
        // Sub-resolution timing: as if it took a microsecond, faster than
        // anything the tuner tells apart.
        written as f64
    };
    Some((written, mbps))
}

#[cfg(unix)]
fn open_write_through(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_SYNC)
        .open(path)
}

#[cfg(windows)]
fn open_write_through(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_TEMPORARY, FILE_FLAG_WRITE_THROUGH,
    };
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(FILE_FLAG_WRITE_THROUGH)
        .attributes(FILE_ATTRIBUTE_TEMPORARY)
        .open(path)
}

#[cfg(not(any(unix, windows)))]
fn open_write_through(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::create(path)
}

// 4 KiB-aligned offsets inside the first `file_len` bytes, from a fixed
// xorshift sequence so every run reads the same pattern.
fn probe_read_offsets(file_len: u64) -> Vec<u64> {
    let blocks = (file_len / PROBE_READ_SIZE as u64).max(1);
    let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    (0..PROBE_READ_COUNT)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % blocks) * PROBE_READ_SIZE as u64
        })
        .collect()
}

fn reads_per_second(elapsed: f64) -> f64 {
    if elapsed > 0.0 {
        PROBE_READ_COUNT as f64 / elapsed
    } else {
        PROBE_READ_COUNT as f64 * 1_000_000.0 // sub-microsecond → very fast
    }
}

// Times one read per offset. `read` reports whether it got the whole block; a
// read that failed or came up short ends the measurement, because it returns
// far faster than the device and would read as impossibly high IOPS.
#[cfg(any(unix, windows))]
fn timed_reads(offsets: Vec<u64>, mut read: impl FnMut(u64) -> bool) -> Option<f64> {
    let start = Instant::now();
    for offset in offsets {
        if !read(offset) {
            return None;
        }
    }
    Some(reads_per_second(start.elapsed().as_secs_f64()))
}

// Random 4 KiB `pread`s. The file's cached pages are dropped first where the
// platform can be asked to, so the reads reach the device.
#[cfg(unix)]
fn measure_random_read(path: &Path, file_len: u64) -> Option<f64> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(path).ok()?;
    let raw_fd = file.as_raw_fd();
    // SAFETY: `raw_fd` is an open descriptor owned by `file` for this call.
    #[cfg(target_os = "linux")]
    let _ = unsafe { libc::posix_fadvise(raw_fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    // SAFETY: as above; F_NOCACHE only changes caching for this descriptor.
    #[cfg(target_os = "macos")]
    let _ = unsafe { libc::fcntl(raw_fd, libc::F_NOCACHE, 1) };
    let offsets = probe_read_offsets(file_len);
    let mut read_buf = [0u8; PROBE_READ_SIZE];

    timed_reads(offsets, |offset| {
        // SAFETY: `read_buf` is a live, writable buffer of PROBE_READ_SIZE
        // bytes and `raw_fd` stays open for the loop.
        let read = unsafe {
            libc::pread(
                raw_fd,
                read_buf.as_mut_ptr() as *mut libc::c_void,
                PROBE_READ_SIZE,
                offset as libc::off_t,
            )
        };
        read == PROBE_READ_SIZE as isize
    })
}

// Random 4 KiB positional reads with the system cache bypassed. Unbuffered
// reads need sector-aligned offsets, lengths and buffers: the offsets and
// length are 4 KiB multiples and the buffer is allocated 4 KiB aligned.
#[cfg(windows)]
fn measure_random_read(path: &Path, file_len: u64) -> Option<f64> {
    use std::os::windows::fs::{FileExt, OpenOptionsExt};
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_NO_BUFFERING)
        .open(path)
        .ok()?;
    let mut read_buf = AlignedProbeBuffer::new(PROBE_READ_SIZE, PROBE_READ_SIZE)?;
    let offsets = probe_read_offsets(file_len);

    timed_reads(offsets, |offset| {
        // `seek_read` is the positional read: it does not move the cursor.
        matches!(
            file.seek_read(read_buf.as_mut_slice(), offset),
            Ok(PROBE_READ_SIZE)
        )
    })
}

// Fallback for platforms without a positional-read benchmark: assume
// SSD-class IOPS. The tuner will adjust based on observed throughput.
#[cfg(not(any(unix, windows)))]
fn measure_random_read(_path: &Path, _file_len: u64) -> Option<f64> {
    Some(50_000.0)
}

// A zeroed heap buffer with a chosen alignment, for unbuffered reads.
#[cfg(windows)]
struct AlignedProbeBuffer {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
}

#[cfg(windows)]
impl AlignedProbeBuffer {
    fn new(len: usize, align: usize) -> Option<Self> {
        let layout = std::alloc::Layout::from_size_align(len, align).ok()?;
        if layout.size() == 0 {
            return None;
        }
        // SAFETY: the layout has a non-zero size, checked above.
        let ptr = std::ptr::NonNull::new(unsafe { std::alloc::alloc_zeroed(layout) })?;
        Some(Self { ptr, layout })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is a live allocation of `layout.size()` zero-
        // initialized bytes owned by `self`, borrowed mutably here.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

#[cfg(windows)]
impl Drop for AlignedProbeBuffer {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this same layout and not yet freed.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

// Opened once the startup disk work is done: the disk probe ran, was skipped
// for a stored measurement or a pinned figure, or failed.
//
// Work that touches the disk at startup and can wait (database maintenance,
// the free-space samplers) waits on this, so it does not compete with the
// probe or with the first seconds of serving. Both async tasks and plain
// threads wait on it; once open it stays open.
#[derive(Clone)]
pub struct StartupDiskGate(Arc<StartupDiskGateInner>);

struct StartupDiskGateInner {
    open: AtomicBool,
    // Guards the waiters below and orders `open` against them.
    waiters: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    blocked: Condvar,
    notify: tokio::sync::Notify,
}

impl std::fmt::Debug for StartupDiskGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartupDiskGate")
            .field("open", &self.is_open())
            .finish()
    }
}

impl StartupDiskGate {
    // A gate that waits for [`Self::open`].
    pub fn closed() -> Self {
        Self(Arc::new(StartupDiskGateInner {
            open: AtomicBool::new(false),
            waiters: Mutex::new(Vec::new()),
            blocked: Condvar::new(),
            notify: tokio::sync::Notify::new(),
        }))
    }

    // A gate with nothing to wait for.
    pub fn opened() -> Self {
        let gate = Self::closed();
        gate.open();
        gate
    }

    pub fn is_open(&self) -> bool {
        self.0.open.load(Ordering::Acquire)
    }

    // Let every waiter through, now and from here on.
    pub fn open(&self) {
        let waiters = {
            let mut waiters = self
                .0
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.0.open.store(true, Ordering::Release);
            std::mem::take(&mut *waiters)
        };
        self.0.blocked.notify_all();
        self.0.notify.notify_waiters();
        for waiter in waiters {
            waiter();
        }
    }

    // Run `waiter` once the gate opens, or now if it already has. It runs on
    // the opening thread, outside the gate's lock.
    pub fn on_open(&self, waiter: impl FnOnce() + Send + 'static) {
        {
            let mut waiters = self
                .0
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !self.0.open.load(Ordering::Acquire) {
                waiters.push(Box::new(waiter));
                return;
            }
        }
        waiter();
    }

    // Block this thread until the gate opens.
    pub fn wait_blocking(&self) {
        let waiters = self
            .0
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(
            self.0
                .blocked
                .wait_while(waiters, |_| !self.0.open.load(Ordering::Acquire))
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }

    // Wait, without blocking a runtime thread, until the gate opens.
    pub async fn wait(&self) {
        loop {
            // Enabled before the check: a `notified` future only counts as
            // a waiter once polled or enabled, so an `open` between the
            // check and the first poll would otherwise be missed.
            let mut opened = std::pin::pin!(self.0.notify.notified());
            opened.as_mut().enable();
            if self.is_open() {
                return;
            }
            opened.await;
        }
    }
}

// Opens the gate when dropped, so a startup task that ends any way at all,
// a panic included, never leaves the gate's waiters waiting.
pub struct StartupDiskGateOpener(StartupDiskGate);

impl StartupDiskGateOpener {
    pub fn new(gate: StartupDiskGate) -> Self {
        Self(gate)
    }
}

impl Drop for StartupDiskGateOpener {
    fn drop(&mut self) {
        self.0.open();
    }
}

fn detect_disk_info(output_dir: &Path) -> (StorageClass, FilesystemType) {
    #[cfg(target_os = "macos")]
    {
        macos_disk_info(output_dir)
    }

    #[cfg(target_os = "linux")]
    {
        linux_disk_info(output_dir)
    }

    #[cfg(windows)]
    {
        windows_disk_info(output_dir)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        let _ = output_dir;
        (
            StorageClass::Unknown,
            FilesystemType::Unknown(String::new()),
        )
    }
}

// Windows: the volume behind the directory answers the filesystem name, the
// drive type says whether it is a network share, and the storage device
// behind the volume says whether it incurs a seek penalty (rotational).
#[cfg(windows)]
fn windows_disk_info(output_dir: &Path) -> (StorageClass, FilesystemType) {
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use windows_sys::Win32::Storage::FileSystem::{
        GetDriveTypeW, GetVolumeInformationW, GetVolumePathNameW,
    };
    // `GetDriveTypeW` result for a network drive. The constant lives in a
    // windows-sys module this crate does not otherwise pull in.
    const DRIVE_REMOTE: u32 = 4;

    let target = std::fs::canonicalize(output_dir)
        .or_else(|_| std::fs::canonicalize(output_dir.parent().unwrap_or(Path::new("."))))
        .unwrap_or_else(|_| output_dir.to_path_buf());
    let wide: Vec<u16> = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let mut volume_root = [0u16; 260];
    // SAFETY: both buffers are valid for the lengths passed.
    let ok = unsafe {
        GetVolumePathNameW(
            wide.as_ptr(),
            volume_root.as_mut_ptr(),
            volume_root.len() as u32,
        )
    };
    if ok == 0 {
        return (
            StorageClass::Unknown,
            FilesystemType::Unknown(String::new()),
        );
    }
    let root_len = volume_root
        .iter()
        .position(|&c| c == 0)
        .unwrap_or(volume_root.len());
    let root_wide = &volume_root[..root_len];

    let mut fs_name = [0u16; 64];
    // SAFETY: `volume_root` is NUL-terminated; the name buffer is sized as passed.
    let ok = unsafe {
        GetVolumeInformationW(
            volume_root.as_ptr(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            fs_name.as_mut_ptr(),
            fs_name.len() as u32,
        )
    };
    let filesystem = if ok == 0 {
        FilesystemType::Unknown(String::new())
    } else {
        let len = fs_name
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(fs_name.len());
        let name = std::ffi::OsString::from_wide(&fs_name[..len])
            .to_string_lossy()
            .into_owned();
        parse_filesystem_type(&name)
    };

    // SAFETY: `volume_root` is NUL-terminated.
    let drive_type = unsafe { GetDriveTypeW(volume_root.as_ptr()) };
    if drive_type == DRIVE_REMOTE {
        return (StorageClass::Network, filesystem);
    }

    let storage_class = storage_class_from_seek_penalty(windows_seek_penalty(root_wide));
    (storage_class, filesystem)
}

#[cfg(any(windows, test))]
fn storage_class_from_seek_penalty(penalty: Option<bool>) -> StorageClass {
    match penalty {
        Some(true) => StorageClass::Hdd,
        Some(false) => StorageClass::Ssd,
        None => StorageClass::Unknown,
    }
}

// Ask the storage device behind a volume root like `C:\` whether it incurs a
// seek penalty. `None` when the volume has no drive letter (a mounted folder)
// or the device does not answer the query.
#[cfg(windows)]
fn windows_seek_penalty(volume_root: &[u16]) -> Option<bool> {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::{
        DEVICE_SEEK_PENALTY_DESCRIPTOR, IOCTL_STORAGE_QUERY_PROPERTY, PropertyStandardQuery,
        STORAGE_PROPERTY_QUERY, StorageDeviceSeekPenaltyProperty,
    };

    // A root derived from a canonicalized path carries the verbatim prefix
    // (`\\?\C:\`); the drive letter follows it.
    let verbatim: Vec<u16> = "\\\\?\\".encode_utf16().collect();
    let volume_root = volume_root
        .strip_prefix(verbatim.as_slice())
        .unwrap_or(volume_root);

    // `X:\` → `\\.\X:`; anything without a drive letter is not addressable
    // as a volume device this way.
    let letter = match volume_root {
        [letter, colon, ..]
            if *colon == u16::from(b':')
                && u8::try_from(*letter).is_ok_and(|c| c.is_ascii_alphabetic()) =>
        {
            *letter
        }
        _ => return None,
    };
    let device: Vec<u16> = "\\\\.\\"
        .encode_utf16()
        .chain([letter, u16::from(b':'), 0])
        .collect();

    // SAFETY: `device` is NUL-terminated; zero desired access is enough for
    // a property query and needs no privilege.
    let handle = unsafe {
        CreateFileW(
            device.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return None;
    }

    let query = STORAGE_PROPERTY_QUERY {
        PropertyId: StorageDeviceSeekPenaltyProperty,
        QueryType: PropertyStandardQuery,
        AdditionalParameters: [0],
    };
    let mut descriptor = DEVICE_SEEK_PENALTY_DESCRIPTOR {
        Version: 0,
        Size: 0,
        IncursSeekPenalty: false,
    };
    let mut returned: u32 = 0;
    // SAFETY: the in/out buffers are properly sized structs that outlive the call.
    let ok = unsafe {
        DeviceIoControl(
            handle,
            IOCTL_STORAGE_QUERY_PROPERTY,
            (&query as *const STORAGE_PROPERTY_QUERY).cast(),
            std::mem::size_of::<STORAGE_PROPERTY_QUERY>() as u32,
            (&mut descriptor as *mut DEVICE_SEEK_PENALTY_DESCRIPTOR).cast(),
            std::mem::size_of::<DEVICE_SEEK_PENALTY_DESCRIPTOR>() as u32,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    // SAFETY: `handle` came from a successful CreateFileW and is closed once.
    unsafe {
        CloseHandle(handle);
    }
    if ok == 0 || (returned as usize) < std::mem::size_of::<DEVICE_SEEK_PENALTY_DESCRIPTOR>() {
        return None;
    }
    Some(descriptor.IncursSeekPenalty)
}

// macOS: use `diskutil info` to detect storage class and filesystem.
#[cfg(target_os = "macos")]
fn macos_disk_info(output_dir: &Path) -> (StorageClass, FilesystemType) {
    // Resolve the path to handle symlinks, then find its mount point.
    let target = std::fs::canonicalize(output_dir)
        .or_else(|_| std::fs::canonicalize(output_dir.parent().unwrap_or(Path::new("/"))))
        .unwrap_or_else(|_| Path::new("/").to_path_buf());

    let mount_point = find_mount_point_macos(&target);

    let output = Command::new("diskutil")
        .arg("info")
        .arg(&mount_point)
        .output();

    let text = match output {
        Ok(o) => String::from_utf8_lossy(&o.stdout).to_string(),
        Err(_) => {
            return (
                StorageClass::Unknown,
                FilesystemType::Unknown(String::new()),
            );
        }
    };

    let storage_class = if text
        .lines()
        .any(|l| l.contains("Solid State:") && l.contains("Yes"))
    {
        StorageClass::Ssd
    } else if text
        .lines()
        .any(|l| l.contains("Solid State:") && l.contains("No"))
    {
        StorageClass::Hdd
    } else {
        StorageClass::Unknown
    };

    let filesystem = text
        .lines()
        .find(|l| {
            l.trim_start().starts_with("Type (Bundle):")
                || l.trim_start().starts_with("File System Personality:")
        })
        .map(|l| {
            let val = l.split(':').nth(1).unwrap_or("").trim();
            parse_filesystem_type(val)
        })
        .unwrap_or(FilesystemType::Unknown(String::new()));

    (storage_class, filesystem)
}

// Find the mount point for a given path on macOS using `df`.
#[cfg(target_os = "macos")]
fn find_mount_point_macos(path: &Path) -> String {
    // `df <path>` outputs: "Filesystem ... Mounted on\n/dev/disk3s1 ... /System/Volumes/Data"
    // The mount point is the last whitespace-separated field on the second line.
    // However, mount points can contain spaces, so we need to be careful.
    // Fortunately, `df` also supports custom output format on macOS (BSD df).
    // We can't use --output (that's GNU df), but the mount point is always
    // the last field after the percentage column.
    let output = Command::new("df").arg(path).output();
    if let Ok(o) = output {
        let text = String::from_utf8_lossy(&o.stdout);
        if let Some(line) = text.lines().nth(1) {
            // macOS df has two % columns (capacity and %iused).
            // The mount point follows the last "XX%" field.
            if let Some(pct_pos) = line.rfind('%') {
                let after_pct = &line[pct_pos + 1..];
                let mp = after_pct.trim();
                if !mp.is_empty() {
                    return mp.to_string();
                }
            }
        }
    }

    "/".to_string()
}

// Linux: check rotational flag and /proc/mounts for filesystem type.
#[cfg(target_os = "linux")]
fn linux_disk_info(output_dir: &Path) -> (StorageClass, FilesystemType) {
    let target = std::fs::canonicalize(output_dir)
        .or_else(|_| std::fs::canonicalize(output_dir.parent().unwrap_or(Path::new("/"))))
        .unwrap_or_else(|_| Path::new("/").to_path_buf());

    let storage_class = linux_storage_class(&target);
    let filesystem = linux_filesystem_type(&target);

    (storage_class, filesystem)
}

// Detect SSD vs HDD on Linux by reading the rotational flag.
#[cfg(target_os = "linux")]
fn linux_storage_class(path: &Path) -> StorageClass {
    // Find the device for the path using df.
    let device = match Command::new("df").arg("--output=source").arg(path).output() {
        Ok(o) => {
            let text = String::from_utf8_lossy(&o.stdout);
            text.lines().nth(1).unwrap_or("").trim().to_string()
        }
        Err(_) => return StorageClass::Unknown,
    };

    // Extract the base block device name (e.g., /dev/sda1 -> sda, /dev/nvme0n1p1 -> nvme0n1).
    let dev_name = device.strip_prefix("/dev/").unwrap_or(&device);

    // Strip partition suffix to get the base device.
    let base_device = strip_partition_suffix(dev_name);

    let rotational_path = format!("/sys/block/{base_device}/queue/rotational");
    match std::fs::read_to_string(&rotational_path) {
        Ok(content) => match content.trim() {
            "0" => StorageClass::Ssd,
            "1" => StorageClass::Hdd,
            _ => StorageClass::Unknown,
        },
        Err(_) => StorageClass::Unknown,
    }
}

// Strip partition number suffix from a device name.
// `sda1` -> `sda`, `nvme0n1p1` -> `nvme0n1`, `vda2` -> `vda`.
#[cfg(target_os = "linux")]
fn strip_partition_suffix(dev: &str) -> &str {
    // NVMe: nvme0n1p1 -> nvme0n1
    if let Some(idx) = dev.rfind('p')
        && dev[..idx].ends_with(|c: char| c.is_ascii_digit())
        && dev[idx + 1..].chars().all(|c| c.is_ascii_digit())
        && !dev[idx + 1..].is_empty()
    {
        return &dev[..idx];
    }

    // Traditional: sda1 -> sda, vda2 -> vda
    let end = dev
        .rfind(|c: char| !c.is_ascii_digit())
        .map(|i| i + 1)
        .unwrap_or(dev.len());
    &dev[..end]
}

// Detect filesystem type on Linux from /proc/mounts.
#[cfg(target_os = "linux")]
fn linux_filesystem_type(path: &Path) -> FilesystemType {
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return FilesystemType::Unknown(String::new()),
    };

    let path_str = path.to_string_lossy();

    // Find the longest mount point that is a prefix of our path.
    let mut best_mount = "";
    let mut best_fstype = "";

    for line in mounts.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }
        let mount_point = parts[1];
        let fstype = parts[2];

        if path_str.starts_with(mount_point) && mount_point.len() > best_mount.len() {
            best_mount = mount_point;
            best_fstype = fstype;
        }
    }

    parse_filesystem_type(best_fstype)
}

// Map a filesystem name string to our enum.
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn parse_filesystem_type(s: &str) -> FilesystemType {
    let lower = s.to_lowercase();
    if lower.contains("apfs") {
        FilesystemType::Apfs
    } else if lower.contains("ext4") {
        FilesystemType::Ext4
    } else if lower == "xfs" {
        FilesystemType::Xfs
    } else if lower.contains("zfs") {
        FilesystemType::Zfs
    } else if lower.contains("btrfs") {
        FilesystemType::Btrfs
    } else if lower.contains("ntfs") {
        FilesystemType::Ntfs
    } else if lower.contains("nfs") {
        FilesystemType::Nfs
    } else if lower.contains("smb") || lower.contains("cifs") {
        FilesystemType::Smb
    } else if s.is_empty() {
        FilesystemType::Unknown(String::new())
    } else {
        FilesystemType::Unknown(s.to_string())
    }
}

#[cfg(test)]
mod tests;
