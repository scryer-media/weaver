use super::rar::install;

#[test]
fn installs_only_verified_members() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("chase");
    let target = dir.path().join("output");
    std::fs::create_dir_all(source.join("folder")).unwrap();
    std::fs::write(source.join("folder/member.txt"), b"verified").unwrap();
    std::fs::write(source.join("stale.txt"), b"previous run").unwrap();
    let timestamp = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    filetime::set_file_mtime(source.join("folder"), timestamp).unwrap();
    install(
        &source,
        &target,
        &["folder".into(), "folder/member.txt".into()],
    )
    .unwrap();
    assert_eq!(
        std::fs::read(target.join("folder/member.txt")).unwrap(),
        b"verified"
    );
    assert!(!target.join("stale.txt").exists());
    assert_eq!(
        filetime::FileTime::from_last_modification_time(
            &std::fs::metadata(target.join("folder")).unwrap()
        ),
        timestamp
    );
}

#[test]
fn collision_preserves_both_sources_before_any_file_is_installed() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("chase");
    let target = dir.path().join("output");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&target).unwrap();
    std::fs::write(source.join("first"), b"first").unwrap();
    std::fs::write(source.join("second"), b"second").unwrap();
    std::fs::write(target.join("second"), b"existing").unwrap();
    assert!(install(&source, &target, &["first".into(), "second".into()]).is_err());
    assert!(!target.join("first").exists());
    assert_eq!(std::fs::read(source.join("first")).unwrap(), b"first");
    assert_eq!(std::fs::read(source.join("second")).unwrap(), b"second");
    assert_eq!(std::fs::read(target.join("second")).unwrap(), b"existing");
}

#[cfg(unix)]
#[test]
fn rejects_a_linked_destination_parent() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("chase");
    let target = dir.path().join("output");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(source.join("folder")).unwrap();
    std::fs::create_dir(&target).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(source.join("folder/member"), b"verified").unwrap();
    std::os::unix::fs::symlink(&outside, target.join("folder")).unwrap();
    assert!(install(&source, &target, &["folder/member".into()]).is_err());
    assert!(!outside.join("member").exists());
    assert!(source.join("folder/member").exists());
}
