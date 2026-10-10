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

// Standard test connection limit (mimics a typical config).
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
fn the_efficient_profile_caps_downloads_without_reducing_a_smaller_pool() {
    use crate::runtime::hardware_profile::HardwareProfile;

    let profile = ssd_profile(8);
    let tuning = HardwareProfile::Efficient.tuning(&profile);
    let mut tuner = RuntimeTuner::with_profile_tuning(profile, TEST_CONNECTIONS, tuning);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 10);
    assert_eq!(p.decode_thread_count, 2);
    assert_eq!(p.extract_thread_count, 4);

    // A connection pool smaller than the cap is still the binding limit.
    tuner.set_connection_limit(4);
    assert_eq!(tuner.params().max_concurrent_downloads, 4);
    tuner.set_connection_limit(40);
    assert_eq!(tuner.params().max_concurrent_downloads, 10);
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
fn fast_storage_extractions_follow_the_cpus_the_process_may_use() {
    use crate::runtime::hardware_profile::HardwareProfile;

    let mut profile = ssd_profile(16);
    profile.cpu.logical_cores = 3;
    let tuning = HardwareProfile::Performance.tuning(&profile);
    let mut tuner = RuntimeTuner::with_profile_tuning(profile, 8, tuning);
    tuner.max_concurrent_extractions_override = None;
    assert_eq!(tuner.max_concurrent_extractions(), 3);
}

// The profile caps how many extractions run at once on fast storage; the
// widest one keeps the cores-between-2-and-6 rule exactly.
#[test]
fn each_profile_caps_concurrent_extractions() {
    use crate::runtime::hardware_profile::HardwareProfile;

    // (cores, efficient, balanced, performance) on fast storage.
    for (cores, efficient, balanced, performance) in
        [(1, 2, 2, 2), (4, 2, 4, 4), (8, 2, 4, 6), (16, 2, 4, 6)]
    {
        for (hardware, expected) in [
            (HardwareProfile::Efficient, efficient),
            (HardwareProfile::Balanced, balanced),
            (HardwareProfile::Performance, performance),
        ] {
            let profile = ssd_profile(cores);
            let tuning = hardware.tuning(&profile);
            let mut tuner = RuntimeTuner::with_profile_tuning(profile, TEST_CONNECTIONS, tuning);
            tuner.max_concurrent_extractions_override = None;
            assert_eq!(
                tuner.max_concurrent_extractions(),
                expected,
                "{} on {cores} cores",
                hardware.as_str()
            );
        }
        assert_eq!(
            performance,
            cores.clamp(2, 6),
            "the widest profile is unchanged"
        );
    }

    // Slow storage is already below every cap.
    for hardware in HardwareProfile::ALL {
        let mut profile = hdd_profile(16);
        let tuning = hardware.tuning(&profile);
        profile.disk.random_read_iops = 800.0;
        let mut tuner = RuntimeTuner::with_profile_tuning(profile, TEST_CONNECTIONS, tuning);
        tuner.max_concurrent_extractions_override = None;
        assert_eq!(tuner.max_concurrent_extractions(), 2);
        tuner.set_random_read_iops(200.0);
        assert_eq!(tuner.max_concurrent_extractions(), 1);
    }

    // A live profile change moves the next admission, and the environment
    // override still beats every profile.
    let profile = ssd_profile(16);
    let performance = HardwareProfile::Performance.tuning(&profile);
    let efficient = HardwareProfile::Efficient.tuning(&profile);
    let mut tuner = RuntimeTuner::with_profile_tuning(profile, TEST_CONNECTIONS, performance);
    tuner.max_concurrent_extractions_override = None;
    assert_eq!(tuner.max_concurrent_extractions(), 6);
    tuner.set_profile_tuning(efficient);
    assert_eq!(tuner.max_concurrent_extractions(), 2);
    tuner.max_concurrent_extractions_override = Some(5);
    assert_eq!(tuner.max_concurrent_extractions(), 5);
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

#[test]
fn a_new_profile_moves_every_limit_and_keeps_the_connection_limit() {
    use crate::runtime::hardware_profile::HardwareProfile;

    let profile = ssd_profile(16);
    let performance = HardwareProfile::Performance.tuning(&profile);
    let efficient = HardwareProfile::Efficient.tuning(&profile);
    let mut tuner = RuntimeTuner::with_profile_tuning(profile, TEST_CONNECTIONS, performance);
    assert_eq!(tuner.params().max_concurrent_downloads, 20);
    assert_eq!(tuner.params().decode_thread_count, 16);
    assert_eq!(tuner.params().extract_thread_count, 8);

    tuner.set_profile_tuning(efficient);
    assert_eq!(tuner.profile_tuning(), efficient);
    assert_eq!(tuner.params().max_concurrent_downloads, 10);
    assert_eq!(tuner.params().decode_thread_count, 2);
    assert_eq!(tuner.params().extract_thread_count, 4);

    // The configured connections still bound the profile's cap, and lifting
    // the cap never goes past them.
    tuner.set_connection_limit(6);
    assert_eq!(tuner.params().max_concurrent_downloads, 6);
    tuner.set_profile_tuning(performance);
    assert_eq!(tuner.params().max_concurrent_downloads, 6);
    assert_eq!(tuner.params().decode_thread_count, 16);
    assert_eq!(tuner.params().extract_thread_count, 8);
    assert_eq!(tuner.system_profile().cpu.physical_cores, 16);
}
