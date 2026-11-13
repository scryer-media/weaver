// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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

// Direct unpack stages outside the conventional tree, so nothing in the job
// lifecycle deletes it after a crash. The sweep treats it exactly like the
// conventional staging root: keyed by job id, removed when the job is gone.
#[test]
fn staging_cleanup_removes_orphaned_direct_unpack_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join(".weaver-direct-unpack");
    let active = root.join("20").join("silver_horizon.7z");
    let orphan = root.join("21").join("silver_horizon.7z");
    let not_a_job = root.join("scratch");
    std::fs::create_dir_all(&active).unwrap();
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::create_dir_all(&not_a_job).unwrap();
    std::fs::write(active.join("member.bin"), b"active").unwrap();
    std::fs::write(orphan.join("member.bin"), b"orphan").unwrap();
    std::fs::write(not_a_job.join("member.bin"), b"kept").unwrap();

    let active_ids = HashSet::from([20_u64]);
    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &active_ids,
        Duration::ZERO,
        SystemTime::now() + Duration::from_secs(1),
    )
    .unwrap();

    assert_eq!(report.removed_count, 1);
    assert!(
        active.exists(),
        "an active job's chase staging must survive"
    );
    assert!(!root.join("21").exists(), "the orphan must be removed");
    assert!(
        not_a_job.exists(),
        "a non-numeric entry is not a job dir and is left alone"
    );
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
    let backup = temp.path().join(".par2-rs-backup.123");
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

// A failed job whose delivery could not go back to its working directory
// leaves it in staging, and the job is in the history, not live. The sweep
// leaves it for as long as the record stands, however old it is, and takes
// it once the record is deleted.
#[test]
fn staging_cleanup_keeps_a_history_job_s_staging_until_its_record_goes() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join(".weaver-staging").join("31");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join("Silver Horizon.mkv"), b"delivered").unwrap();
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&crate::JobHistoryRow {
        job_id: 31,
        job_hash: None,
        name: "Silver Horizon".to_string(),
        status: "failed".to_string(),
        error_message: Some("a volume could not be repaired".to_string()),
        total_bytes: 9,
        downloaded_bytes: 9,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_001_000,
        metadata: None,
        server_attribution: None,
        files_removed_at: None,
    })
    .unwrap();
    let past_the_ttl = SystemTime::now() + STALE_STAGING_TTL * 2;

    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &db.recorded_job_ids().unwrap(),
        STALE_STAGING_TTL,
        past_the_ttl,
    )
    .unwrap();
    assert_eq!(report.removed_count, 0);
    assert_eq!(
        std::fs::read(staging.join("Silver Horizon.mkv")).unwrap(),
        b"delivered"
    );

    db.delete_job_histories_and_forget_duplicate_identities(&[crate::JobId(31)])
        .unwrap();
    let report = cleanup_stale_staging_dirs_at(
        temp.path(),
        &db.recorded_job_ids().unwrap(),
        STALE_STAGING_TTL,
        past_the_ttl,
    )
    .unwrap();
    assert_eq!(report.removed_count, 1);
    assert!(!staging.exists());
}
