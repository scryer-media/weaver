use super::*;

#[test]
fn safe_move_uses_rename_without_copy_on_the_same_filesystem() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source.bin");
    let dst = temp.path().join("dest.bin");
    std::fs::write(&src, b"renamed payload").unwrap();
    let counters = Arc::new(PhaseCounters::default());
    let rename_calls = std::cell::Cell::new(0usize);
    let copy_calls = std::cell::Cell::new(0usize);

    move_path_with_safe_rename_or_copy_fallback_using(
        &src,
        &dst,
        Arc::clone(&counters),
        |src, dst| {
            rename_calls.set(rename_calls.get() + 1);
            crate::runtime::fs::rename_no_overwrite(src, dst)
        },
        |_, _, _| {
            copy_calls.set(copy_calls.get() + 1);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(rename_calls.get(), 1);
    assert_eq!(copy_calls.get(), 0);
    assert!(!src.exists());
    assert_eq!(std::fs::read(&dst).unwrap(), b"renamed payload");
    assert_eq!(
        counters.completed_bytes.load(Ordering::Relaxed),
        b"renamed payload".len() as u64
    );
}

#[test]
fn same_filesystem_directory_publication_renames_without_copy() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source");
    let dst = temp.path().join("dest");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("payload.bin"), b"renamed payload").unwrap();
    let copy_calls = std::cell::Cell::new(0usize);

    move_path_with_safe_rename_or_copy_fallback_using(
        &src,
        &dst,
        Arc::new(PhaseCounters::default()),
        rename_path_for_publication,
        |_, _, _| {
            copy_calls.set(copy_calls.get() + 1);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(copy_calls.get(), 0);
    assert!(!src.exists());
    assert_eq!(
        std::fs::read(dst.join("payload.bin")).unwrap(),
        b"renamed payload"
    );
}

#[test]
fn safe_move_uses_exactly_one_copy_after_cross_device_rename_failure() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source.bin");
    let dst = temp.path().join("dest.bin");
    std::fs::write(&src, b"copied payload").unwrap();
    let counters = Arc::new(PhaseCounters::default());
    let rename_calls = std::cell::Cell::new(0usize);
    let copy_calls = std::cell::Cell::new(0usize);

    move_path_with_safe_rename_or_copy_fallback_using(
        &src,
        &dst,
        Arc::clone(&counters),
        |_, _| {
            rename_calls.set(rename_calls.get() + 1);
            Err(std::io::Error::new(
                std::io::ErrorKind::CrossesDevices,
                "simulated EXDEV",
            ))
        },
        |src, dst, counters| {
            copy_calls.set(copy_calls.get() + 1);
            move_path_with_copy_fallback(src, dst, counters)
        },
    )
    .unwrap();

    assert_eq!(rename_calls.get(), 1);
    assert_eq!(copy_calls.get(), 1);
    assert!(!src.exists());
    assert_eq!(std::fs::read(&dst).unwrap(), b"copied payload");
    assert_eq!(
        counters.completed_bytes.load(Ordering::Relaxed),
        b"copied payload".len() as u64
    );
}

#[test]
fn safe_move_does_not_copy_after_an_unrelated_rename_failure() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source.bin");
    let dst = temp.path().join("dest.bin");
    std::fs::write(&src, b"unmoved payload").unwrap();
    let counters = Arc::new(PhaseCounters::default());
    let copy_calls = std::cell::Cell::new(0usize);

    let error = move_path_with_safe_rename_or_copy_fallback_using(
        &src,
        &dst,
        counters,
        |_, _| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated",
            ))
        },
        |_, _, _| {
            copy_calls.set(copy_calls.get() + 1);
            Ok(())
        },
    )
    .unwrap_err();

    assert_eq!(error.0.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(copy_calls.get(), 0);
    assert_eq!(std::fs::read(&src).unwrap(), b"unmoved payload");
    assert!(!dst.exists());
}

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
        None,
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
        None,
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

/// The seam the rename pass runs at has to see both delivery routes as one set.
/// Extraction writes members into the working root and direct-store commits
/// them into staging; only after this move do they share a directory, and the
/// dominance test that picks the payload is meaningless before then.
#[tokio::test]
async fn the_rename_pass_sees_staging_and_working_output_as_one_delivery() {
    let temp = tempfile::tempdir().unwrap();
    let working = temp.path().join("working");
    let staging = temp.path().join("staging");
    let dest = temp.path().join("complete");
    std::fs::create_dir_all(&working).unwrap();
    std::fs::create_dir_all(&staging).unwrap();

    // The payload arrives by the direct-store route...
    let payload = std::fs::File::create(staging.join("Yb5drZSkNi20UCMkb.mkv")).unwrap();
    payload.set_len(64 * 1024 * 1024).unwrap();
    // ...and its subtitle by the extraction route.
    std::fs::write(working.join("Yb5drZSkNi20UCMkb.eng.srt"), b"1\n").unwrap();

    let result = run_move_to_complete(
        JobId(1),
        working,
        Some(staging),
        dest.clone(),
        Arc::new(PhaseCounters::default()),
        Some(DeliveryNamingPlan {
            job_display_name: "Silver Horizon 2024".to_string(),
            srrdb: None,
        }),
    )
    .await
    .unwrap();

    assert_eq!(result.moved_entries, 2);
    assert_eq!(result.renamed_members, 2);
    assert!(dest.join("Silver Horizon 2024.mkv").is_file());
    assert!(dest.join("Silver Horizon 2024.eng.srt").is_file());
    assert!(!dest.join("Yb5drZSkNi20UCMkb.mkv").exists());
}

/// The pass is a policy, not a stage: with it off the move places exactly what
/// it was given.
#[tokio::test]
async fn a_disabled_rename_pass_places_the_obfuscated_names_untouched() {
    let temp = tempfile::tempdir().unwrap();
    let working = temp.path().join("working");
    let dest = temp.path().join("complete");
    std::fs::create_dir_all(&working).unwrap();
    let payload = std::fs::File::create(working.join("Yb5drZSkNi20UCMkb.mkv")).unwrap();
    payload.set_len(64 * 1024 * 1024).unwrap();

    let result = run_move_to_complete(
        JobId(1),
        working,
        None,
        dest.clone(),
        Arc::new(PhaseCounters::default()),
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.renamed_members, 0);
    assert!(dest.join("Yb5drZSkNi20UCMkb.mkv").is_file());
}

#[test]
fn prepublication_scan_checks_both_delivery_roots() {
    let temp = tempfile::tempdir().unwrap();
    let working = temp.path().join("working");
    let staging = temp.path().join("staging");
    std::fs::create_dir_all(working.join("nested")).unwrap();
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(working.join("safe.mkv"), b"safe").unwrap();
    std::fs::write(staging.join("nested.exe"), b"rejected").unwrap();

    let settings = PostProcessingSettings {
        unacceptable_extensions: vec!["EXE".into()],
        ..PostProcessingSettings::default()
    }
    .normalized()
    .unwrap();
    let rejection = scan_delivery_sources(&working, Some(&staging), &settings)
        .unwrap()
        .expect("staging output must be inspected");

    assert_eq!(rejection.pattern, "exe");
    assert_eq!(rejection.relative_path, "nested.exe");
    assert!(working.join("safe.mkv").exists());
    assert!(staging.join("nested.exe").exists());
}

#[cfg(unix)]
#[test]
fn prepublication_scan_does_not_follow_symlinked_directories() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let working = temp.path().join("working");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&working).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("payload.exe"), b"rejected if followed").unwrap();
    symlink(&outside, working.join("linked")).unwrap();

    let settings = PostProcessingSettings {
        unacceptable_extensions: vec!["exe".into()],
        ..PostProcessingSettings::default()
    };

    assert!(
        scan_delivery_sources(&working, None, &settings)
            .unwrap()
            .is_none()
    );
}
