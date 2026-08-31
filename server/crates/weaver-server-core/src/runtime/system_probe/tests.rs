use super::*;

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

#[test]
fn detect_returns_valid_profile() {
    let profile = detect(Path::new("/tmp"));
    assert!(profile.cpu.logical_cores > 0);
    assert!(profile.cpu.physical_cores > 0);
    assert!(profile.memory.total_bytes > 0);
}

#[test]
fn startup_profile_defers_random_read_measurement() {
    let profile = detect_startup_profile(Path::new("/tmp"));
    assert_eq!(profile.disk.random_read_iops, 0.0);
}
