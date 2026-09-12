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

/// Standard test connection limit (mimics a typical config).
const TEST_CONNECTIONS: usize = 20;

#[test]
fn initial_params_ssd() {
    let tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 20); // SSD uses all connections
    assert_eq!(p.decode_thread_count, 8);
}

#[test]
fn initial_params_hdd() {
    let tuner = RuntimeTuner::with_connection_limit(hdd_profile(8), TEST_CONNECTIONS);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 20); // all configured connections
    assert_eq!(p.decode_thread_count, 8);
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
fn set_connection_limit_increases_capacity() {
    // Start with 0 connections (no servers configured).
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 0);
    assert_eq!(tuner.params().max_concurrent_downloads, 0);

    // Add a server with 20 connections.
    tuner.set_connection_limit(20);
    assert_eq!(tuner.params().max_concurrent_downloads, 20); // SSD uses all
}

#[test]
fn set_connection_limit_decreases_capacity() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 20);
    assert_eq!(tuner.params().max_concurrent_downloads, 20);

    // Remove a server, now only 5 connections.
    tuner.set_connection_limit(5);
    assert_eq!(tuner.params().max_concurrent_downloads, 5);
}

#[test]
fn random_read_iops_update_changes_future_extraction_admission_limit() {
    let mut profile = hdd_profile(4);
    profile.disk.random_read_iops = 0.0;
    let mut tuner = RuntimeTuner::with_connection_limit(profile, 8);
    assert_eq!(tuner.max_concurrent_extractions(), 1);

    tuner.set_random_read_iops(1_500.0);
    assert_eq!(tuner.max_concurrent_extractions(), 4);

    tuner.set_random_read_iops(200.0);
    assert_eq!(tuner.max_concurrent_extractions(), 1);
}
