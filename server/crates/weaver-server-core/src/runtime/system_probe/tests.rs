use super::*;

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
