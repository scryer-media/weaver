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
        let limits = Limits::for_memory(memory);
        assert_eq!(limits.native + limits.metadata + limits.payload, total);
        assert_eq!(limits.native, total / 2);
        assert_eq!(limits.metadata, metadata);
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
