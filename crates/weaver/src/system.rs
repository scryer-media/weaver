//! System capability detection using only std primitives.
//!
//! Runs once at startup to build a [`SystemProfile`] describing the host
//! machine. All subprocess calls use [`std::process::Command`] (blocking)
//! since this only executes once during initialization.

use std::path::Path;
use std::process::Command;

use tracing::debug;
use weaver_core::system::*;

/// Probe the running system and return a [`SystemProfile`].
///
/// Detection is best-effort: any individual probe that fails silently
/// falls back to a safe default rather than aborting startup.
pub fn detect(output_dir: &Path) -> SystemProfile {
    let cpu = detect_cpu();
    let memory = detect_memory();
    let disk = detect_disk(output_dir);

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

/// Try to read physical core count from the OS.
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
            if let Some(val) = line.strip_prefix("physical id") {
                if let Some(v) = val.split(':').nth(1) {
                    current_physical = v.trim().to_string();
                }
            }
            if let Some(val) = line.strip_prefix("core id") {
                if let Some(v) = val.split(':').nth(1) {
                    current_core = v.trim().to_string();
                }
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

/// Read cgroup v2 CPU quota if running in a container.
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

fn detect_memory() -> MemoryProfile {
    let (total, available) = detect_memory_bytes().unwrap_or((
        16 * 1024 * 1024 * 1024, // 16 GiB default
        8 * 1024 * 1024 * 1024,  // 8 GiB default
    ));

    let cgroup_limit = detect_cgroup_memory_limit();

    MemoryProfile {
        total_bytes: total,
        available_bytes: available,
        cgroup_limit,
    }
}

/// Returns (total_bytes, available_bytes).
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

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

/// Parse a `/proc/meminfo` value like `"  16384000 kB"` into bytes.
#[cfg(target_os = "linux")]
fn parse_meminfo_kb(s: &str) -> Option<u64> {
    let kb: u64 = s.split_whitespace().next()?.parse().ok()?;
    Some(kb * 1024)
}

/// Approximate available memory on macOS using `vm_stat`.
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

/// Extract the page count from a vm_stat line like `"Pages free:    12345."`.
#[cfg(target_os = "macos")]
fn extract_vm_stat_pages(line: &str, prefix: &str) -> Option<u64> {
    if !line.starts_with(prefix) {
        return None;
    }
    let after_colon = line.split(':').nth(1)?;
    after_colon.trim().trim_end_matches('.').parse().ok()
}

/// Read cgroup v2 memory limit.
fn detect_cgroup_memory_limit() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let content = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
        let trimmed = content.trim();
        if trimmed == "max" {
            return None;
        }
        trimmed.parse().ok()
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

// ---------------------------------------------------------------------------
// Disk
// ---------------------------------------------------------------------------

fn detect_disk(output_dir: &Path) -> DiskProfile {
    let (storage_class, filesystem) = detect_disk_info(output_dir);

    debug!(?storage_class, ?filesystem, "disk probe");

    DiskProfile {
        storage_class,
        filesystem,
        // Benchmarking is deferred to a later phase; use conservative defaults.
        sequential_write_mbps: 500.0,
        random_read_iops: 10_000.0,
        same_filesystem: true,
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

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = output_dir;
        (
            StorageClass::Unknown,
            FilesystemType::Unknown(String::new()),
        )
    }
}

/// macOS: use `diskutil info` to detect storage class and filesystem.
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

/// Find the mount point for a given path on macOS using `df`.
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

/// Linux: check rotational flag and /proc/mounts for filesystem type.
#[cfg(target_os = "linux")]
fn linux_disk_info(output_dir: &Path) -> (StorageClass, FilesystemType) {
    let target = std::fs::canonicalize(output_dir)
        .or_else(|_| std::fs::canonicalize(output_dir.parent().unwrap_or(Path::new("/"))))
        .unwrap_or_else(|_| Path::new("/").to_path_buf());

    let storage_class = linux_storage_class(&target);
    let filesystem = linux_filesystem_type(&target);

    (storage_class, filesystem)
}

/// Detect SSD vs HDD on Linux by reading the rotational flag.
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

/// Strip partition number suffix from a device name.
/// `sda1` -> `sda`, `nvme0n1p1` -> `nvme0n1`, `vda2` -> `vda`.
#[cfg(target_os = "linux")]
fn strip_partition_suffix(dev: &str) -> &str {
    // NVMe: nvme0n1p1 -> nvme0n1
    if let Some(idx) = dev.rfind('p') {
        if dev[..idx].ends_with(|c: char| c.is_ascii_digit())
            && dev[idx + 1..].chars().all(|c| c.is_ascii_digit())
            && !dev[idx + 1..].is_empty()
        {
            return &dev[..idx];
        }
    }

    // Traditional: sda1 -> sda, vda2 -> vda
    let end = dev
        .rfind(|c: char| !c.is_ascii_digit())
        .map(|i| i + 1)
        .unwrap_or(dev.len());
    &dev[..end]
}

/// Detect filesystem type on Linux from /proc/mounts.
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

/// Map a filesystem name string to our enum.
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
mod tests {
    use super::*;

    #[test]
    fn parse_filesystem_types() {
        assert_eq!(parse_filesystem_type("APFS"), FilesystemType::Apfs);
        assert_eq!(parse_filesystem_type("ext4"), FilesystemType::Ext4);
        assert_eq!(parse_filesystem_type("xfs"), FilesystemType::Xfs);
        assert_eq!(parse_filesystem_type("zfs"), FilesystemType::Zfs);
        assert_eq!(parse_filesystem_type("btrfs"), FilesystemType::Btrfs);
        assert_eq!(parse_filesystem_type("ntfs"), FilesystemType::Ntfs);
        assert_eq!(parse_filesystem_type("nfs4"), FilesystemType::Nfs);
        assert_eq!(parse_filesystem_type("cifs"), FilesystemType::Smb);
        assert_eq!(
            parse_filesystem_type("fuse"),
            FilesystemType::Unknown("fuse".to_string())
        );
    }

    #[test]
    fn detect_returns_valid_profile() {
        let profile = detect(Path::new("/tmp"));
        assert!(profile.cpu.logical_cores > 0);
        assert!(profile.cpu.physical_cores > 0);
        assert!(profile.memory.total_bytes > 0);
    }
}
