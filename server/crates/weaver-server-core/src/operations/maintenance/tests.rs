use super::*;
use std::io::Write as _;
use std::path::Path;

fn touch_until_after(path: &Path, older_than: SystemTime) -> SystemTime {
    for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(10));
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"x").unwrap();
        drop(file);

        let modified = std::fs::metadata(path).unwrap().modified().unwrap();
        if modified > older_than {
            return modified;
        }
    }

    panic!("failed to advance file mtime for {}", path.display());
}

#[test]
fn staging_cleanup_removes_only_inactive_expired_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join(".weaver-staging");
    let active = staging.join("10");
    let inactive = staging.join("11");
    std::fs::create_dir_all(&active).unwrap();
    std::fs::create_dir_all(&inactive).unwrap();
    std::fs::write(active.join("active.bin"), b"active").unwrap();
    std::fs::write(inactive.join("inactive.bin"), b"inactive").unwrap();

    let active_ids = HashSet::from([10_u64]);
    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &active_ids,
        Duration::ZERO,
        SystemTime::now() + Duration::from_secs(1),
    )
    .unwrap();

    assert_eq!(report.removed_count, 1);
    assert_eq!(report.removed_bytes, b"inactive".len() as u64);
    assert!(active.exists());
    assert!(!inactive.exists());
}

#[test]
fn staging_cleanup_preserves_old_dir_with_recent_child_file() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join(".weaver-staging").join("13");
    let child = staging.join("payload.bin");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(&child, b"initial").unwrap();

    let root_modified = std::fs::metadata(&staging).unwrap().modified().unwrap();
    let child_modified = touch_until_after(&child, root_modified);
    let root_modified = std::fs::metadata(&staging).unwrap().modified().unwrap();
    let ttl = child_modified.duration_since(root_modified).unwrap();

    let report =
        cleanup_stale_staging_dirs_at(temp.path(), &HashSet::new(), ttl, child_modified).unwrap();

    assert_eq!(report.removed_count, 0);
    assert!(staging.exists());
}

#[test]
fn staging_cleanup_preserves_old_dir_with_recent_nested_child() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join(".weaver-staging").join("14");
    let nested = staging.join("nested");
    let child = nested.join("payload.bin");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(&child, b"initial").unwrap();

    let root_modified = std::fs::metadata(&staging).unwrap().modified().unwrap();
    let child_modified = touch_until_after(&child, root_modified);
    let root_modified = std::fs::metadata(&staging).unwrap().modified().unwrap();
    let ttl = child_modified.duration_since(root_modified).unwrap();

    let report =
        cleanup_stale_staging_dirs_at(temp.path(), &HashSet::new(), ttl, child_modified).unwrap();

    assert_eq!(report.removed_count, 0);
    assert!(staging.exists());
}

#[test]
fn staging_cleanup_preserves_recent_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join(".weaver-staging").join("12");
    std::fs::create_dir_all(&staging).unwrap();

    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &HashSet::new(),
        Duration::from_secs(24 * 60 * 60),
        SystemTime::now(),
    )
    .unwrap();

    assert_eq!(report.removed_count, 0);
    assert!(staging.exists());
}

#[test]
fn staging_cleanup_ignores_par2_backup_names() {
    let temp = tempfile::tempdir().unwrap();
    let backup = temp.path().join(".weaver-par2-backup.123");
    std::fs::create_dir_all(&backup).unwrap();

    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &HashSet::new(),
        Duration::ZERO,
        SystemTime::now(),
    )
    .unwrap();

    assert_eq!(report.removed_count, 0);
    assert!(backup.exists());
}

#[test]
fn staging_cleanup_preserves_non_numeric_staging_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join(".weaver-staging").join("not-a-job");
    std::fs::create_dir_all(&staging).unwrap();

    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &HashSet::new(),
        Duration::ZERO,
        SystemTime::now() + Duration::from_secs(1),
    )
    .unwrap();

    assert_eq!(report.removed_count, 0);
    assert!(staging.exists());
}
