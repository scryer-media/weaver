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
fn detect_returns_valid_profile() {
    let profile = detect(Path::new("/tmp"));
    assert!(profile.cpu.logical_cores > 0);
    assert!(profile.cpu.physical_cores > 0);
    assert!(profile.memory.total_bytes > 0);
}
