// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn startup_and_extraction_memory_consumers_share_cached_process_facts() {
    let cache = OnceLock::new();
    let memory_probes = AtomicUsize::new(0);
    let cgroup_probes = AtomicUsize::new(0);

    let facts = cached_process_memory_facts(
        &cache,
        || {
            memory_probes.fetch_add(1, Ordering::Relaxed);
            Some((16_u64 << 30, 8_u64 << 30))
        },
        || {
            cgroup_probes.fetch_add(1, Ordering::Relaxed);
            Some(6_u64 << 30)
        },
    );
    let profile = memory_profile_from_facts(facts);
    let effective_total = facts
        .memory_bytes
        .map(|(total, _)| total.min(facts.cgroup_limit.unwrap_or(total)));

    let same_facts = cached_process_memory_facts(
        &cache,
        || panic!("memory detection must be cached"),
        || panic!("cgroup detection must be cached"),
    );

    assert!(std::ptr::eq(facts, same_facts));
    assert_eq!(profile.total_bytes, 16_u64 << 30);
    assert_eq!(profile.cgroup_limit, Some(6_u64 << 30));
    assert_eq!(effective_total, Some(6_u64 << 30));
    assert_eq!(memory_probes.load(Ordering::Relaxed), 1);
    assert_eq!(cgroup_probes.load(Ordering::Relaxed), 1);
}

#[cfg(unix)]
#[test]
fn nested_cgroup_v2_memory_limits_include_systemd_ancestors() {
    let limit = cgroup_memory_limit_from(
        "0::/system.slice/weaver.service\n",
        "29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        |path| match path.to_str() {
            Some("/sys/fs/cgroup/system.slice/weaver.service/memory.max") => {
                Some((3_u64 << 30).to_string())
            }
            Some("/sys/fs/cgroup/system.slice/memory.max") => Some((2_u64 << 30).to_string()),
            Some("/sys/fs/cgroup/memory.max") => Some("max".to_string()),
            _ => None,
        },
    );

    assert_eq!(limit, Some(2_u64 << 30));
}

#[cfg(unix)]
#[test]
fn cgroup_v1_memory_limit_ignores_unlimited_ancestors() {
    let limit = cgroup_memory_limit_from(
        "5:cpu,memory:/docker/worker/task\n",
        "33 23 0:30 / /sys/fs/cgroup/memory rw,memory - cgroup cgroup rw,memory\n",
        |path| match path.to_str() {
            Some("/sys/fs/cgroup/memory/docker/worker/task/memory.limit_in_bytes") => {
                Some((6_u64 << 30).to_string())
            }
            Some("/sys/fs/cgroup/memory/docker/worker/memory.limit_in_bytes") => {
                Some((1_u64 << 30).to_string())
            }
            Some("/sys/fs/cgroup/memory/memory.limit_in_bytes") => {
                Some("9223372036854771712".to_string())
            }
            _ => None,
        },
    );

    assert_eq!(limit, Some(1_u64 << 30));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
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
fn parse_startup_iops_override() {
    assert_eq!(parse_startup_iops(Some("50000")), Some(50000.0));
    assert_eq!(parse_startup_iops(Some(" 12500.5 ")), Some(12500.5));
    assert_eq!(parse_startup_iops(None), None);
    assert_eq!(parse_startup_iops(Some("")), None);
    assert_eq!(parse_startup_iops(Some("off")), None);
    assert_eq!(parse_startup_iops(Some("0")), None);
    assert_eq!(parse_startup_iops(Some("-100")), None);
    assert_eq!(parse_startup_iops(Some("inf")), None);
    assert_eq!(parse_startup_iops(Some("NaN")), None);
}

// A stored measurement stands in for the probe, so this reads the host's
// facts without writing hundreds of megabytes to the test machine's disk.
#[test]
fn detect_returns_valid_profile() {
    let dir = tempfile::tempdir().unwrap();
    let stored = StoredDiskMeasurement {
        measured_at: unix_seconds(SystemTime::now()),
        sequential_write_mbps: 420.0,
        random_read_iops: 3_000.0,
    };
    write_stored_disk_measurement(&dir.path().join(DISK_PROBE_RECORD), &stored).unwrap();

    let profile = detect(dir.path());
    assert!(profile.cpu.logical_cores > 0);
    assert!(profile.cpu.physical_cores > 0);
    assert!(profile.memory.total_bytes > 0);
    if startup_iops_override().is_none() {
        assert_eq!(profile.disk.sequential_write_mbps, 420.0);
        assert_eq!(profile.disk.random_read_iops, 3_000.0);
    }
}

#[test]
fn startup_profile_defers_disk_measurement() {
    let profile = detect_startup_profile(Path::new("/tmp"));
    assert_eq!(profile.disk.random_read_iops, 0.0);
    assert_eq!(profile.disk.sequential_write_mbps, 0.0);
}

const DAY: u64 = 24 * 60 * 60;
const MEASURED_AT: u64 = 1_800_000_000;

fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn no_probe(_: &Path) -> Option<DiskMeasurement> {
    panic!("a fresh stored measurement must skip the probe")
}

// The probe runs at most once a week: a measurement younger than seven days
// is used as stored, an older one is taken again and stored in its place.
#[test]
fn a_fresh_stored_measurement_skips_the_probe_and_an_old_one_is_retaken() {
    let dir = tempfile::tempdir().unwrap();
    let first = DiskMeasurement {
        sequential_write_mbps: 420.0,
        random_read_iops: 3_000.0,
    };
    let second = DiskMeasurement {
        sequential_write_mbps: 90.0,
        random_read_iops: 180.0,
    };

    let measured = stored_or_measured_disk_profile(dir.path(), || at(MEASURED_AT), |_| Some(first));
    assert_eq!(measured, first);
    assert!(dir.path().join(DISK_PROBE_RECORD).is_file());

    let six_days = MEASURED_AT + 6 * DAY;
    assert_eq!(
        stored_or_measured_disk_profile(dir.path(), || at(six_days), no_probe),
        first
    );

    let eight_days = MEASURED_AT + 8 * DAY;
    assert_eq!(
        stored_or_measured_disk_profile(dir.path(), || at(eight_days), |_| Some(second)),
        second
    );
    assert_eq!(
        stored_or_measured_disk_profile(dir.path(), || at(eight_days + DAY), no_probe),
        second,
        "the retaken measurement is the one stored"
    );
}

// A record that cannot be read, or one dated after the clock, is measured
// again rather than trusted.
#[test]
fn an_unreadable_or_future_record_is_measured_again() {
    let dir = tempfile::tempdir().unwrap();
    let record = dir.path().join(DISK_PROBE_RECORD);
    let measured = DiskMeasurement {
        sequential_write_mbps: 250.0,
        random_read_iops: 8_000.0,
    };

    std::fs::write(&record, b"not json").unwrap();
    assert_eq!(
        stored_or_measured_disk_profile(dir.path(), || at(MEASURED_AT), |_| Some(measured)),
        measured
    );

    let future = StoredDiskMeasurement {
        measured_at: MEASURED_AT + DAY,
        sequential_write_mbps: 1.0,
        random_read_iops: 1.0,
    };
    write_stored_disk_measurement(&record, &future).unwrap();
    assert_eq!(
        stored_or_measured_disk_profile(dir.path(), || at(MEASURED_AT), |_| Some(measured)),
        measured
    );
}

// A probe that cannot run falls back without storing anything, so the next
// start tries again.
#[test]
fn a_failed_probe_falls_back_and_stores_nothing() {
    let dir = tempfile::tempdir().unwrap();

    let fallback = stored_or_measured_disk_profile(dir.path(), || at(MEASURED_AT), |_| None);
    assert_eq!(fallback.sequential_write_mbps, 0.0, "writes unmeasured");
    assert_eq!(fallback.random_read_iops, DISK_PROBE_FALLBACK_IOPS);
    assert!(!dir.path().join(DISK_PROBE_RECORD).exists());
}

// The scratch file goes however the probe ends.
#[test]
fn the_probe_file_is_removed_when_its_guard_drops() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(DISK_PROBE_FILE);
    {
        let _guard = ProbeFileGuard(path.clone());
        std::fs::write(&path, b"probe").unwrap();
    }
    assert!(!path.exists());
}

#[test]
fn probe_reads_stay_aligned_inside_what_was_written() {
    let written = (PROBE_CHUNK_BYTES * PROBE_WRITE_REPEATS[0]) as u64;
    let offsets = probe_read_offsets(written);
    assert_eq!(offsets.len(), PROBE_READ_COUNT);
    for offset in offsets {
        assert_eq!(offset % PROBE_READ_SIZE as u64, 0);
        assert!(offset + PROBE_READ_SIZE as u64 <= written);
    }
}

// A read that fails or comes up short returns almost at once, so counting it
// would report impossibly high IOPS. Reading past the end of a real file makes
// every platform's read path hit that case.
#[test]
fn a_read_that_does_not_fill_the_block_ends_the_measurement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(DISK_PROBE_FILE);
    let len = 64 * PROBE_READ_SIZE as u64;
    std::fs::write(&path, vec![7u8; len as usize]).unwrap();

    assert!(measure_random_read(&path, len).is_some_and(|iops| iops > 0.0));
    assert_eq!(measure_random_read(&path, 1 << 40), None);
}

// The gate lets a blocked thread, an async task and a registered callback
// through when it opens, and anything that comes later straight through.
#[tokio::test]
async fn the_startup_disk_gate_releases_every_kind_of_waiter() {
    let gate = StartupDiskGate::closed();
    let (called_tx, called_rx) = std::sync::mpsc::channel();
    gate.on_open(move || called_tx.send(()).unwrap());
    let blocked = {
        let gate = gate.clone();
        std::thread::spawn(move || gate.wait_blocking())
    };
    let waiting = {
        let gate = gate.clone();
        tokio::spawn(async move { gate.wait().await })
    };
    assert!(
        called_rx.try_recv().is_err(),
        "nothing passes a closed gate"
    );

    drop(StartupDiskGateOpener::new(gate.clone()));

    called_rx.recv().unwrap();
    blocked.join().unwrap();
    waiting.await.unwrap();
    assert!(gate.is_open());
    gate.wait().await;
    gate.wait_blocking();
    let (late_tx, late_rx) = std::sync::mpsc::channel();
    gate.on_open(move || late_tx.send(()).unwrap());
    late_rx
        .try_recv()
        .expect("a callback on an open gate runs at once");
}

#[test]
fn storage_seek_penalty_requires_device_evidence() {
    assert_eq!(
        storage_class_from_seek_penalty(Some(true)),
        StorageClass::Hdd
    );
    assert_eq!(
        storage_class_from_seek_penalty(Some(false)),
        StorageClass::Ssd
    );
    assert_eq!(storage_class_from_seek_penalty(None), StorageClass::Unknown);
}

// Memory and filesystem queries must answer on the runner. A device may
// decline the optional seek-penalty query, so its class can remain Unknown.
#[cfg(windows)]
#[test]
fn windows_probes_read_real_memory_and_disk() {
    let (total, available) = windows_memory_bytes().expect("GlobalMemoryStatusEx answers");
    assert!(total > 0, "total memory must be reported");
    assert!(
        available > 0 && available <= total,
        "{available} of {total}"
    );
    assert_eq!(detect_total_memory_bytes(), Some(total));

    let dir = std::env::temp_dir();
    let (storage_class, filesystem) = windows_disk_info(&dir);
    assert!(
        !matches!(&filesystem, FilesystemType::Unknown(name) if name.is_empty()),
        "temp dir filesystem must be named, got {filesystem:?}"
    );
    let (class_again, fs_again) = detect_disk_info(&dir);
    assert_eq!(class_again, storage_class);
    assert_eq!(fs_again, filesystem);
}
