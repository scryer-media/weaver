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
mod tests {
    use super::*;

    #[test]
    fn system_profile_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SystemProfile>();
    }

    #[test]
    fn serde_roundtrip() {
        let profile = SystemProfile {
            cpu: CpuProfile {
                physical_cores: 8,
                logical_cores: 16,
                simd: SimdSupport {
                    sse42: true,
                    avx2: true,
                    avx512: false,
                    neon: false,
                },
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 32 * 1024 * 1024 * 1024,
                available_bytes: 16 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50000.0,
                same_filesystem: true,
            },
        };

        let json = serde_json::to_string(&profile).unwrap();
        let back: SystemProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cpu.physical_cores, 8);
        assert_eq!(back.disk.storage_class, StorageClass::Ssd);
    }
}
