use super::*;

#[test]
fn limits_scale_with_effective_memory_without_exceeding_the_cap() {
    for (memory, total, metadata) in [
        (None, 256 << 20, 16 << 20),
        (Some(1 << 30), 128 << 20, 8 << 20),
        (Some(8 << 30), 1 << 30, 64 << 20),
        (Some(16 << 30), 2 << 30, 64 << 20),
        (Some(u64::MAX), 2 << 30, 64 << 20),
        (Some(0), 0, 0),
    ] {
        let limits = Limits::for_memory(memory, DEFAULT_SHARE);
        assert_eq!(limits.native + limits.metadata + limits.payload, total);
        assert_eq!(limits.native, total / 2);
        assert_eq!(limits.metadata, metadata);
    }
}

/// The total each profile gives PAR3: the widest profile is exactly the share
/// a process with no profile gets, the smaller ones less, and none of them
/// takes a small host below an eighth of its memory up to 128 MiB.
#[test]
fn each_hardware_profile_sizes_the_total_and_the_widest_matches_the_default() {
    use crate::runtime::HardwareProfile;
    use crate::runtime::system_profile::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;
    let machine = |cores: usize, memory: u64| SystemProfile {
        cpu: CpuProfile {
            physical_cores: cores,
            logical_cores: cores * 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: memory,
            available_bytes: memory / 2,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 2000.0,
            random_read_iops: 50000.0,
            same_filesystem: true,
        },
    };
    let total = |profile: HardwareProfile, cores: usize, memory: Option<u64>| {
        let share = profile
            .tuning(&machine(cores, memory.unwrap_or(GIB)))
            .par3_memory;
        let limits = Limits::for_memory(memory, share);
        (limits.native + limits.metadata + limits.payload) as u64
    };

    assert_eq!(
        HardwareProfile::Performance
            .tuning(&machine(16, 64 * GIB))
            .par3_memory,
        DEFAULT_SHARE
    );
    // (cores, memory, efficient, balanced, performance)
    for (cores, memory, efficient, balanced, performance) in [
        (4, Some(8 * GIB), 256 * MIB, GIB, GIB),
        (16, Some(64 * GIB), 256 * MIB, GIB, 2 * GIB),
        (2, Some(GIB), 128 * MIB, 128 * MIB, 128 * MIB),
        (1, Some(512 * MIB), 64 * MIB, 64 * MIB, 64 * MIB),
        (4, None, 256 * MIB, 256 * MIB, 256 * MIB),
    ] {
        assert_eq!(total(HardwareProfile::Efficient, cores, memory), efficient);
        assert_eq!(total(HardwareProfile::Balanced, cores, memory), balanced);
        assert_eq!(
            total(HardwareProfile::Performance, cores, memory),
            performance
        );
        // Today's sizing, before any profile took part.
        let unprofiled = memory.map_or(256 * MIB, |bytes| (bytes / 8).min(2 * GIB));
        assert_eq!(performance, unprofiled);
    }
}

#[test]
fn real_world_payload_is_not_charged_to_metadata_and_shared_images_pay_once() {
    let budgets = Budgets::new(Limits {
        native: 0,
        metadata: 4096,
        payload: 32 << 20,
    });
    let bytes = bytes::Bytes::from(vec![7; 24 << 20]);
    let first = budgets.retain(&bytes).unwrap();
    let reader = budgets.retain(&bytes).unwrap();
    assert!(Arc::ptr_eq(&first, &reader));
    assert_eq!(budgets.payload.used.load(Ordering::Acquire), bytes.len());
    assert_eq!(budgets.metadata.used.load(Ordering::Acquire), 256);
    drop(first);
    assert_eq!(budgets.payload.used.load(Ordering::Acquire), bytes.len());
    drop(reader);
    assert_eq!(budgets.payload.used.load(Ordering::Acquire), 0);
    assert_eq!(budgets.metadata.used.load(Ordering::Acquire), 0);
}

#[test]
fn failed_admission_releases_partial_leases_and_replacements_do_not_accumulate() {
    let budgets = Budgets::new(Limits {
        native: 0,
        metadata: 4096,
        payload: 128,
    });
    let bytes = bytes::Bytes::from(vec![1; 100]);
    let first = budgets.retain(&bytes).unwrap();
    let second = bytes::Bytes::from(vec![2; 100]);
    assert_eq!(
        budgets.retain(&second).as_ref().err().and_then(limit_label),
        Some("PAR3 retained payload")
    );
    assert_eq!(budgets.payload.used.load(Ordering::Acquire), 100);
    assert_eq!(budgets.metadata.used.load(Ordering::Acquire), 256);
    drop(first);
    for _ in 0..100 {
        drop(budgets.retain(&second).unwrap());
    }
    assert_eq!(budgets.payload.used.load(Ordering::Acquire), 0);
    assert!(budgets.allocations.lock().unwrap().is_empty());
}

#[test]
fn concurrent_jobs_share_a_single_live_allocation_lease() {
    let budgets = Budgets::new(Limits {
        native: 0,
        metadata: 4096,
        payload: 1024,
    });
    let bytes = bytes::Bytes::from(vec![1; 1024]);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                let lease = budgets.retain(&bytes).unwrap();
                barrier.wait();
                assert_eq!(budgets.payload.used.load(Ordering::Acquire), 1024);
                barrier.wait();
                drop(lease);
            });
        }
    });
    assert_eq!(budgets.payload.used.load(Ordering::Acquire), 0);
}

#[test]
fn only_explicit_host_pressure_selects_disk_fallback() {
    assert!(is_host_pressure(&host_limit("PAR3 retained payload")));
    assert!(is_host_pressure(&EngineError::Io(std::io::Error::other(
        host_limit("PAR3 host state")
    ))));
    assert!(!is_host_pressure(&host_limit("placement read work")));
    assert!(!is_host_pressure(&EngineError::Io(std::io::Error::other(
        "disk failed"
    ))));
    // A weaver ceiling and an engine ceiling answer the same question the
    // same way, whichever error shape carried it.
    assert_eq!(
        limit_label(&host_budget_limit("PAR3 host state", 9, 8, 0)),
        Some("PAR3 host state")
    );
    assert!(is_native_pressure(&host_limit(
        par3_rs::runtime::MemoryCategory::CodecScratch.name()
    )));
    assert!(!is_limit(&EngineError::Cancelled));
}

#[test]
fn concurrent_spills_respect_free_space_and_release_claims() {
    let first = DiskReservation::acquire(600, 1000, 200).unwrap();
    assert!(DiskReservation::acquire(201, 1000, 200).is_err());
    let second = DiskReservation::acquire(200, 1000, 200).unwrap();
    drop(first);
    let replacement = DiskReservation::acquire(600, 1000, 200).unwrap();
    assert!(DiskReservation::acquire(u64::MAX, 1000, 200).is_err());
    drop(second);
    drop(replacement);
    assert_eq!(DISK_RESERVED.load(Ordering::Acquire), 0);
}
