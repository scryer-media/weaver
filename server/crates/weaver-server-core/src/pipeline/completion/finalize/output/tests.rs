use super::*;

#[test]
fn copy_fallback_copies_file_then_removes_source() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source.bin");
    let dst = temp.path().join("dest").join("source.bin");
    std::fs::write(&src, b"copied payload").unwrap();

    let counters = PhaseCounters::default();
    move_path_with_copy_fallback(&src, &dst, &counters).unwrap();

    assert!(!src.exists());
    assert_eq!(std::fs::read(&dst).unwrap(), b"copied payload");
    assert_eq!(
        counters.completed_bytes.load(Ordering::Relaxed),
        b"copied payload".len() as u64
    );
}

#[test]
fn copy_fallback_does_not_overwrite_destination() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source.bin");
    let dst = temp.path().join("dest.bin");
    std::fs::write(&src, b"source payload").unwrap();
    std::fs::write(&dst, b"existing payload").unwrap();

    let counters = PhaseCounters::default();
    let error = move_path_with_copy_fallback(&src, &dst, &counters).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&src).unwrap(), b"source payload");
    assert_eq!(std::fs::read(&dst).unwrap(), b"existing payload");
}

#[test]
fn copy_cleanup_does_not_remove_replaced_parent_destination() {
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("dest");
    let replaced_parent = temp.path().join("dest-replaced");
    let dst = parent.join("payload.bin");
    std::fs::create_dir(&parent).unwrap();
    let parent_fingerprint = crate::runtime::fs::prepare_destination_parent(&dst).unwrap();
    std::fs::write(&dst, b"copied payload").unwrap();

    std::fs::rename(&parent, &replaced_parent).unwrap();
    std::fs::create_dir(&parent).unwrap();
    std::fs::write(&dst, b"new occupant").unwrap();

    cleanup_copy_destination_if_parent_matches(&dst, &parent_fingerprint);

    assert_eq!(std::fs::read(&dst).unwrap(), b"new occupant");
    assert_eq!(
        std::fs::read(replaced_parent.join("payload.bin")).unwrap(),
        b"copied payload"
    );
}

#[tokio::test]
async fn final_move_does_not_overwrite_existing_destination_file() {
    let temp = tempfile::tempdir().unwrap();
    let working = temp.path().join("working");
    let staging = temp.path().join("staging");
    let dest = temp.path().join("complete");
    std::fs::create_dir_all(&working).unwrap();
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::create_dir_all(&dest).unwrap();
    let src = staging.join("payload.bin");
    let dst = dest.join("payload.bin");
    std::fs::write(&src, b"new payload").unwrap();
    std::fs::write(&dst, b"existing payload").unwrap();

    let error = match run_move_to_complete(
        JobId(1),
        working,
        Some(staging),
        dest,
        Arc::new(PhaseCounters::default()),
    )
    .await
    {
        Ok(_) => panic!("final move should reject an occupied destination"),
        Err(error) => error,
    };

    assert!(error.contains("destination already exists"));
    assert_eq!(std::fs::read(&src).unwrap(), b"new payload");
    assert_eq!(std::fs::read(&dst).unwrap(), b"existing payload");
}

#[test]
fn copy_fallback_moves_nested_directory_contents() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source-dir");
    let nested = src.join("nested");
    let dst = temp.path().join("dest-dir");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("payload.bin"), b"nested payload").unwrap();

    let counters = PhaseCounters::default();
    move_path_with_copy_fallback(&src, &dst, &counters).unwrap();

    assert!(!src.exists());
    assert_eq!(
        std::fs::read(dst.join("nested").join("payload.bin")).unwrap(),
        b"nested payload"
    );
}

#[test]
fn copy_fallback_does_not_overwrite_destination_directory() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source-dir");
    let dst = temp.path().join("dest-dir");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("source.bin"), b"source payload").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("existing.bin"), b"existing payload").unwrap();

    let counters = PhaseCounters::default();
    let error = move_path_with_copy_fallback(&src, &dst, &counters).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        std::fs::read(src.join("source.bin")).unwrap(),
        b"source payload"
    );
    assert_eq!(
        std::fs::read(dst.join("existing.bin")).unwrap(),
        b"existing payload"
    );
}

#[cfg(unix)]
#[test]
fn copy_fallback_preserves_nested_symlink_entries() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source-dir");
    let nested = src.join("nested");
    let dst = temp.path().join("dest-dir");
    let target = temp.path().join("target.bin");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(&target, b"target payload").unwrap();
    std::os::unix::fs::symlink(&target, nested.join("linked.bin")).unwrap();

    let counters = PhaseCounters::default();
    move_path_with_copy_fallback(&src, &dst, &counters).unwrap();

    assert!(!src.exists());
    let placed = dst.join("nested").join("linked.bin");
    assert!(
        std::fs::symlink_metadata(&placed)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read_link(placed).unwrap(), target);
}

#[cfg(unix)]
#[tokio::test]
async fn final_move_preserves_symlink_entries() {
    let temp = tempfile::tempdir().unwrap();
    let working = temp.path().join("working");
    let staging = temp.path().join("staging");
    let dest = temp.path().join("complete");
    let target = temp.path().join("target.bin");
    std::fs::create_dir_all(&working).unwrap();
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(&target, b"target payload").unwrap();
    std::os::unix::fs::symlink(&target, staging.join("linked.bin")).unwrap();

    let result = run_move_to_complete(
        JobId(1),
        working,
        Some(staging),
        dest.clone(),
        Arc::new(PhaseCounters::default()),
    )
    .await
    .unwrap();

    assert_eq!(result.moved_entries, 1);
    let placed = dest.join("linked.bin");
    assert!(
        std::fs::symlink_metadata(&placed)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read_link(placed).unwrap(), target);
}
