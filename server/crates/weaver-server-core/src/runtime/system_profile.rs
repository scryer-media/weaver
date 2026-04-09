use serde::{Deserialize, Serialize};

/// Detected system capabilities, computed once at startup.
///
/// Used by the adaptive runtime tuner to pick initial parameters and
/// by the scheduler to set concurrency limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemProfile {
    pub cpu: CpuProfile,
    pub memory: MemoryProfile,
    pub disk: DiskProfile,
}

/// CPU capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuProfile {
    /// Physical core count (for CPU-bound thread pools).
    pub physical_cores: usize,
    /// Logical core count (including hyperthreads).
    pub logical_cores: usize,
    /// SIMD instruction set support.
    pub simd: SimdSupport,
    /// Fractional CPU limit from cgroup (containers).
    pub cgroup_limit: Option<f64>,
}

/// SIMD instruction set availability.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SimdSupport {
    pub sse42: bool,
    pub avx2: bool,
    pub avx512: bool,
    pub neon: bool,
}

/// Memory capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProfile {
    /// Total system RAM in bytes.
    pub total_bytes: u64,
    /// Available (free + reclaimable) RAM in bytes.
    pub available_bytes: u64,
    /// Memory limit from cgroup (containers).
    pub cgroup_limit: Option<u64>,
}

/// Disk/storage capabilities for the working directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskProfile {
    /// Detected storage class.
    pub storage_class: StorageClass,
    /// Detected filesystem type.
    pub filesystem: FilesystemType,
    /// Sequential write throughput in MB/s (from startup benchmark).
    pub sequential_write_mbps: f64,
    /// Random 4K read IOPS (from startup benchmark).
    pub random_read_iops: f64,
    /// Whether temp dir and final output dir are on the same filesystem.
    pub same_filesystem: bool,
}

/// Storage medium classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageClass {
    Ssd,
    Hdd,
    Network,
    Unknown,
}

/// Filesystem type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilesystemType {
    Ext4,
    Xfs,
    Zfs,
    Btrfs,
    Apfs,
    Ntfs,
    Nfs,
    Smb,
    Unknown(String),
}

#[cfg(test)]
mod tests;
