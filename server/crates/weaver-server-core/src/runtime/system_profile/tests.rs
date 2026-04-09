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
