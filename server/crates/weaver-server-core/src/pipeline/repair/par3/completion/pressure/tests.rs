use super::*;

#[test]
fn pressure_cleanup_removes_only_reported_native_staging_files() {
    let root = tempfile::tempdir().unwrap();
    let installed = root.path().join("verified.bin");
    let staging = root.path().join(".par3-repair-1-1.tmp");
    let unrelated = root.path().join(".par3-repair-1-2.tmp");
    for path in [&installed, &staging, &unrelated] {
        std::fs::write(path, b"bytes").unwrap();
    }
    clear_temporaries(root.path(), std::slice::from_ref(&staging)).unwrap();
    clear_temporaries(root.path(), std::slice::from_ref(&staging)).unwrap();
    assert!(!staging.exists());
    assert_eq!(std::fs::read(&installed).unwrap(), b"bytes");
    assert_eq!(std::fs::read(&unrelated).unwrap(), b"bytes");
    assert!(clear_temporaries(root.path(), std::slice::from_ref(&installed)).is_err());
    assert!(installed.exists());
}

#[test]
fn pressure_cleanup_refuses_an_external_temporary() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = outside.path().join(".par3-repair-1-1.tmp");
    std::fs::write(&path, b"outside").unwrap();
    assert!(clear_temporaries(root.path(), std::slice::from_ref(&path)).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"outside");
}
