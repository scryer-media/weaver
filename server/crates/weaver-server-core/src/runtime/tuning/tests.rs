use crate::runtime::system_profile::*;

use super::*;

fn ssd_profile(cores: usize) -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: cores,
            logical_cores: cores * 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 16 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 2000.0,
            random_read_iops: 50000.0,
            same_filesystem: true,
        },
    }
}

fn hdd_profile(cores: usize) -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: cores,
            logical_cores: cores * 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 16 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Hdd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 150.0,
            random_read_iops: 200.0,
            same_filesystem: true,
        },
    }
}

fn empty_metrics() -> MetricsSnapshot {
    MetricsSnapshot {
        bytes_downloaded: 0,
        bytes_decoded: 0,
        bytes_committed: 0,
        download_queue_depth: 0,
        decode_pending: 0,
        commit_pending: 0,
        write_buffered_bytes: 0,
        write_buffered_segments: 0,
        direct_write_evictions: 0,
        segments_downloaded: 0,
        segments_decoded: 0,
        segments_committed: 0,
        articles_not_found: 0,
        decode_errors: 0,
        verify_active: 0,
        repair_active: 0,
        extract_active: 0,
        disk_write_latency_us: 0,
        segments_retried: 0,
        segments_failed_permanent: 0,
        current_download_speed: 0,
        crc_errors: 0,
        recovery_queue_depth: 0,
        articles_per_sec: 0.0,
        decode_rate_mbps: 0.0,
    }
}

/// Standard test connection limit (mimics a typical config).
const TEST_CONNECTIONS: usize = 20;

#[test]
fn initial_params_ssd() {
    let tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 20); // SSD uses all connections
    assert_eq!(p.max_decode_queue, 40); // 20 * 2
    assert_eq!(p.max_write_queue, 40);
    assert_eq!(p.min_free_buffers, 4);
    assert_eq!(p.decode_thread_count, 8);
}

#[test]
fn initial_params_hdd() {
    let tuner = RuntimeTuner::with_connection_limit(hdd_profile(8), TEST_CONNECTIONS);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 20); // all configured connections
    assert_eq!(p.max_decode_queue, 40);
    assert_eq!(p.max_write_queue, 40);
    assert_eq!(p.decode_thread_count, 8);
}

#[test]
fn adjust_reduces_downloads() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);
    let initial = tuner.params().max_concurrent_downloads;

    // Simulate decode pressure (decode_pending > max_decode_queue).
    let mut m = empty_metrics();
    m.decode_pending = tuner.params().max_decode_queue + 10;

    // Need 3 consecutive observations before adjustment.
    assert!(!tuner.adjust(&m));
    assert!(!tuner.adjust(&m));
    assert!(tuner.adjust(&m));
    assert_eq!(tuner.params().max_concurrent_downloads, initial - 1);
}

#[test]
fn adjust_increases_downloads() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);

    // First reduce downloads so there's room to increase.
    let mut pressure = empty_metrics();
    pressure.decode_pending = tuner.params().max_decode_queue + 10;
    for _ in 0..3 {
        tuner.adjust(&pressure);
    }
    let reduced = tuner.params().max_concurrent_downloads;

    // Now simulate idle download queue.
    let mut idle = empty_metrics();
    idle.download_queue_depth = 0;

    assert!(!tuner.adjust(&idle));
    assert!(!tuner.adjust(&idle));
    assert!(tuner.adjust(&idle));
    assert_eq!(tuner.params().max_concurrent_downloads, reduced + 1);
}

#[test]
fn adjust_respects_minimums() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(1), 2);
    // With 1 core, 2 connections on SSD: max_concurrent_downloads = 2.

    let mut pressure = empty_metrics();
    pressure.decode_pending = 1000;

    // Reduce from 2 to 1.
    for _ in 0..3 {
        tuner.adjust(&pressure);
    }
    assert_eq!(tuner.params().max_concurrent_downloads, 1);

    // Try to reduce further -- should stay at 1.
    for _ in 0..6 {
        tuner.adjust(&pressure);
    }
    assert_eq!(tuner.params().max_concurrent_downloads, 1);
}

#[test]
fn pp_throttles_hdd_aggressively() {
    let mut tuner = RuntimeTuner::with_connection_limit(hdd_profile(8), TEST_CONNECTIONS);
    assert_eq!(tuner.params().max_concurrent_downloads, 20);

    let mut m = empty_metrics();
    m.extract_active = 1;

    // PP streak threshold is 2.
    tuner.adjust(&m);
    assert_eq!(tuner.params().max_concurrent_downloads, 20); // not yet
    tuner.adjust(&m);
    // HDD: reduced to baseline/4 = 20/4 = 5.
    assert_eq!(tuner.params().max_concurrent_downloads, 5);
}

#[test]
fn parse_max_concurrent_extractions_override_accepts_positive_values() {
    assert_eq!(
        parse_max_concurrent_extractions_override(Some("1")),
        Some(1)
    );
    assert_eq!(
        parse_max_concurrent_extractions_override(Some("6")),
        Some(6)
    );
}

#[test]
fn parse_max_concurrent_extractions_override_rejects_invalid_values() {
    assert_eq!(parse_max_concurrent_extractions_override(None), None);
    assert_eq!(parse_max_concurrent_extractions_override(Some("")), None);
    assert_eq!(parse_max_concurrent_extractions_override(Some("0")), None);
    assert_eq!(parse_max_concurrent_extractions_override(Some("-1")), None);
    assert_eq!(
        parse_max_concurrent_extractions_override(Some("nope")),
        None
    );
}

#[test]
fn max_concurrent_extractions_honors_override() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    tuner.max_concurrent_extractions_override = Some(1);
    assert_eq!(tuner.max_concurrent_extractions(), 1);
}

#[test]
fn pp_throttles_ssd_gently() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    // SSD with 20 connections: max_concurrent_downloads = 20.
    assert_eq!(tuner.params().max_concurrent_downloads, 20);

    let mut m = empty_metrics();
    m.verify_active = 2;

    tuner.adjust(&m);
    tuner.adjust(&m);
    // SSD: reduced to baseline*3/4 = 20*3/4 = 15.
    assert_eq!(tuner.params().max_concurrent_downloads, 15);
}

#[test]
fn pp_restores_after_completion() {
    let mut tuner = RuntimeTuner::with_connection_limit(hdd_profile(8), TEST_CONNECTIONS);
    assert_eq!(tuner.params().max_concurrent_downloads, 20);

    // Trigger PP throttling.
    let mut pp = empty_metrics();
    pp.repair_active = 1;
    tuner.adjust(&pp);
    tuner.adjust(&pp);
    assert_eq!(tuner.params().max_concurrent_downloads, 5);

    // PP finishes.
    let idle = empty_metrics();
    tuner.adjust(&idle);
    // Should restore to pre-PP value.
    assert_eq!(tuner.params().max_concurrent_downloads, 20);
}

#[test]
fn recovery_slots_high_bandwidth() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    assert_eq!(tuner.params().recovery_slots, 0); // starts at 0

    // Simulate high bandwidth (60 MB/s = 62914560 bytes/sec).
    let mut m = empty_metrics();
    m.current_download_speed = 60 * 1024 * 1024;

    // EMA needs a few iterations to ramp up past 50 MB/s threshold.
    for _ in 0..10 {
        tuner.adjust(&m);
    }
    // 25% of 20 concurrent downloads = 5, max(5, 2) = 5.
    assert!(tuner.params().recovery_slots >= 2);
}

#[test]
fn recovery_slots_medium_bandwidth() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);

    // Simulate medium bandwidth (20 MB/s).
    let mut m = empty_metrics();
    m.current_download_speed = 20 * 1024 * 1024;

    for _ in 0..10 {
        tuner.adjust(&m);
    }
    // Medium: max(20/8, 1) = max(2, 1) = 2.
    assert!(tuner.params().recovery_slots >= 1);
}

#[test]
fn recovery_slots_low_bandwidth() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);

    // Simulate low bandwidth (5 MB/s).
    let mut m = empty_metrics();
    m.current_download_speed = 5 * 1024 * 1024;

    for _ in 0..10 {
        tuner.adjust(&m);
    }
    assert_eq!(tuner.params().recovery_slots, 0);
}

#[test]
fn no_idle_increase_during_pp() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);
    // Start: max_concurrent_downloads = 20.

    // Reduce via decode pressure first.
    let mut pressure = empty_metrics();
    pressure.decode_pending = tuner.params().max_decode_queue + 10;
    for _ in 0..3 {
        tuner.adjust(&pressure);
    }
    let reduced = tuner.params().max_concurrent_downloads;

    // Now PP is active with empty download queue — should NOT increase.
    let mut pp_idle = empty_metrics();
    pp_idle.extract_active = 1;
    pp_idle.download_queue_depth = 0;
    for _ in 0..5 {
        tuner.adjust(&pp_idle);
    }
    // Should be PP-throttled, not increased.
    assert!(tuner.params().max_concurrent_downloads <= reduced);
}

#[test]
fn set_connection_limit_increases_capacity() {
    // Start with 0 connections (no servers configured).
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 0);
    assert_eq!(tuner.params().max_concurrent_downloads, 0);

    // Add a server with 20 connections.
    tuner.set_connection_limit(20);
    assert_eq!(tuner.params().max_concurrent_downloads, 20); // SSD uses all
    assert_eq!(tuner.params().max_decode_queue, 40);
}

#[test]
fn set_connection_limit_decreases_capacity() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 20);
    assert_eq!(tuner.params().max_concurrent_downloads, 20);

    // Remove a server, now only 5 connections.
    tuner.set_connection_limit(5);
    assert_eq!(tuner.params().max_concurrent_downloads, 5);
}
