//! uuencode sequential assembly
//! PAR2 binding by content (obfuscation)

use super::*;

// ---- uuencode sequential assembly ----

#[tokio::test]
async fn uu_segments_assemble_sequentially_at_decoded_offsets() {
    // Placement must come from cumulative DECODED lengths. The declared sizes
    // in the spec are ~1.38x larger; if any of them leaked into the offset
    // computation the file would be scattered and this comparison would fail.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20061);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 900], vec![b'b'; 700], vec![b'c'; 350]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for (index, part) in parts.iter().enumerate() {
        let last = index == parts.len() - 1;
        submit_uu_segment(&mut pipeline, file_id, index as u32, part, false, last).await;
    }

    let expected: Vec<u8> = parts.concat();
    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, expected);

    // Direct store is excluded: the bytes materialised as an ordinary file on
    // the conventional assembly path even though this pipeline has direct
    // routing enabled.
    assert!(working_dir.join("silver-horizon.bin").is_file());
    // And the dual-CRC grid was never fed, so it can claim nothing.
    assert!(pipeline.block_crc_verdicts(file_id).is_none());
}

#[tokio::test]
async fn uu_segments_park_until_their_prefix_arrives() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20062);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 600], vec![b'b'; 450], vec![b'c'; 300]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Reverse order: nothing can be placed until part 0 lands.
    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.parked.len()),
        Some(2),
        "both later parts must be waiting on their prefix"
    );
    assert_eq!(pipeline.write_buffered_bytes, 750);
    assert_eq!(pipeline.write_buffered_segments, 2);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        750
    );
    assert_eq!(
        pipeline.metrics.write_pending_bytes.load(Ordering::Relaxed),
        750
    );
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_spooled_segments, 0);
    assert_eq!(pipeline.uu_parked_segments, 2);

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, parts.concat());
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_parked_segments, 0);
    assert_eq!(
        pipeline.metrics.write_pending_bytes.load(Ordering::Relaxed),
        0
    );
    assert!(!pipeline.uu_spool_root.join(job_id.0.to_string()).exists());
}

#[tokio::test]
async fn uu_spills_reassemble_before_obfuscated_sfv_verification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;

    let job_id = JobId(20169);
    let payload_filename = "silver-horizon.bin";
    let listing_filename = "oBfU5CaTeD-0001";
    let parts = [vec![b'a'; 80], vec![b'b'; 90], vec![b'c'; 100]];
    let payload = parts.concat();
    let listing = format!(
        "{payload_filename} {:08x}\n",
        par2_rs::checksum::crc32(&payload)
    );
    let declared_sizes = parts
        .iter()
        .map(|part| (part.len() as f64 * 1.38).ceil() as u32)
        .collect::<Vec<_>>();
    let mut spec = standalone_job_spec(
        "UU Spill Then Obfuscated SFV",
        &[
            (payload_filename.to_string(), declared_sizes[0]),
            (listing_filename.to_string(), listing.len() as u32),
        ],
    );
    let payload_file = &mut spec.files[0];
    payload_file.segments.clear();
    for (index, bytes) in declared_sizes.iter().enumerate() {
        payload_file.segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("uu-sfv-{index}@example.com"),
        });
    }
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment_named(
        &mut pipeline,
        payload_file_id,
        2,
        &parts[2],
        false,
        true,
        payload_filename,
    )
    .await;
    submit_uu_segment_named(
        &mut pipeline,
        payload_file_id,
        1,
        &parts[1],
        false,
        false,
        payload_filename,
    )
    .await;

    let first_spool = match pipeline
        .uu_files
        .get(&payload_file_id)
        .and_then(|uu| uu.parked.get(&1))
    {
        Some(UuParkedEntry::Spilled { path, .. }) => path.to_path_buf(),
        _ => panic!("part 1 must spill while the prefix is missing"),
    };
    let second_spool = match pipeline
        .uu_files
        .get(&payload_file_id)
        .and_then(|uu| uu.parked.get(&2))
    {
        Some(UuParkedEntry::Spilled { path, .. }) => path.to_path_buf(),
        _ => panic!("part 2 must spill while the prefix is missing"),
    };
    assert_eq!(std::fs::read(&first_spool).unwrap(), parts[1]);
    assert_eq!(std::fs::read(&second_spool).unwrap(), parts[2]);
    assert_eq!(pipeline.uu_spooled_bytes, parts[1].len() + parts[2].len());
    assert_eq!(pipeline.uu_spooled_segments, 2);

    submit_uu_segment_named(
        &mut pipeline,
        payload_file_id,
        0,
        &parts[0],
        false,
        false,
        payload_filename,
    )
    .await;
    assert_eq!(
        tokio::fs::read(working_dir.join(payload_filename))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_spooled_segments, 0);

    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        listing_filename,
        listing.as_bytes(),
    )
    .await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();

    pipeline.check_job_completion(job_id).await;

    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "reassembled UU payload must pass its obfuscated SFV listing"
    );
    assert_eq!(pipeline.sfv_verify_read_splits, vec![(0usize, 1usize)]);
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

#[tokio::test]
async fn uu_park_spills_after_resident_budget_and_drains_in_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // The comparison is strict: part 2 stays resident, then part 1 spills.
    pipeline.write_backlog_budget_bytes = 600;
    let job_id = JobId(20170);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 500], vec![b'b'; 450], vec![b'c'; 300]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|part| part.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;

    let parked = pipeline.uu_files.get(&file_id).expect("UU park");
    assert!(matches!(
        parked.parked.get(&2),
        Some(UuParkedEntry::Memory(_))
    ));
    assert!(matches!(
        parked.parked.get(&1),
        Some(UuParkedEntry::Spilled { .. })
    ));
    assert_eq!(pipeline.write_buffered_bytes, parts[2].len());
    assert_eq!(pipeline.uu_spooled_bytes, parts[1].len());
    assert_eq!(pipeline.uu_spooled_segments, 1);
    assert_eq!(pipeline.uu_parked_segments, 2);
    assert_eq!(
        pipeline.metrics.write_pending_bytes.load(Ordering::Relaxed),
        (parts[1].len() + parts[2].len()) as u64
    );

    // Disk-spooled UU bytes must not become shared pressure on yEnc. The
    // resident 300 bytes are below the shared soft threshold of 420 bytes.
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::None.as_code()
    );

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, parts.concat());
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_spooled_segments, 0);
    assert_eq!(pipeline.uu_parked_segments, 0);
    assert!(!pipeline.uu_spool_root.join(job_id.0.to_string()).exists());
    assert_eq!(
        pipeline.metrics.write_pending_bytes.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn uu_park_admission_requeues_ahead_parts_at_byte_segment_and_disk_limits() {
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 80], vec![b'b'; 90]];
    for (case, max_bytes, max_segments, available_bytes) in [
        ("byte", parts[1].len() - 1, usize::MAX, u64::MAX),
        ("segment", usize::MAX, 0, u64::MAX),
        ("disk", usize::MAX, usize::MAX, 0),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.write_backlog_budget_bytes = 1;
        pipeline.uu_spool_max_bytes = max_bytes;
        pipeline.uu_spool_max_segments = max_segments;
        pipeline.uu_spool_available_bytes_for_test = Some(Some(available_bytes));
        let job_id = JobId(20180 + u64::from(case == "segment") + 2 * u64::from(case == "disk"));
        insert_active_job(
            &mut pipeline,
            job_id,
            uu_job_spec(&parts.iter().map(|part| part.len()).collect::<Vec<_>>()),
        )
        .await;
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };

        submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, true).await;

        assert_eq!(
            pipeline.uu_files.get(&file_id).map(|uu| uu.parked.len()),
            Some(0),
            "{case} admission cap must not retain the ahead part"
        );
        assert_eq!(pipeline.uu_spooled_bytes, 0, "{case}");
        assert_eq!(pipeline.uu_spooled_segments, 0, "{case}");
        assert_eq!(pipeline.uu_parked_segments, 0, "{case}");
        assert!(
            !pipeline.uu_spool_root.join(job_id.0.to_string()).exists(),
            "{case} admission cap must not create a spool directory"
        );
    }
}

#[tokio::test]
async fn uu_spilled_replacement_and_displacement_remove_old_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;
    pipeline.write_buf_max_pending = 2;
    let job_id = JobId(20171);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 100], vec![b'b'; 110], vec![b'c'; 120]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|part| part.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;
    let replaced_path = match pipeline
        .uu_files
        .get(&file_id)
        .and_then(|uu| uu.parked.get(&1))
    {
        Some(UuParkedEntry::Spilled { path, .. }) => path.to_path_buf(),
        _ => panic!("expected spilled ordinal 1"),
    };

    // Same ahead-of-cursor ordinal replaces its old spill entry without
    // double-charging the spool ledger.
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;
    assert!(
        !replaced_path.exists(),
        "replacement must unlink the old spool"
    );
    assert_eq!(pipeline.uu_spooled_bytes, parts[1].len() + parts[2].len());
    assert_eq!(pipeline.uu_spooled_segments, 2);
    let retained_path = match pipeline
        .uu_files
        .get(&file_id)
        .and_then(|uu| uu.parked.get(&1))
    {
        Some(UuParkedEntry::Spilled { path, .. }) => path.to_path_buf(),
        _ => panic!("expected replacement spool"),
    };
    pipeline.clear_job_write_backlog(job_id);
    assert!(
        !retained_path.exists(),
        "teardown must remove UU spool files"
    );
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_spooled_segments, 0);

    // Lower the bound in an independent job. Parking ordinal 1 after ordinal
    // 2 evicts the farthest entry and removes its TempPath immediately.
    pipeline.write_buf_max_pending = 1;
    let displaced_job_id = JobId(20172);
    insert_active_job(
        &mut pipeline,
        displaced_job_id,
        uu_job_spec(&parts.iter().map(|part| part.len()).collect::<Vec<_>>()),
    )
    .await;
    let displaced_file_id = NzbFileId {
        job_id: displaced_job_id,
        file_index: 0,
    };
    submit_uu_segment(&mut pipeline, displaced_file_id, 2, &parts[2], false, true).await;
    let farthest_path = match pipeline
        .uu_files
        .get(&displaced_file_id)
        .and_then(|uu| uu.parked.get(&2))
    {
        Some(UuParkedEntry::Spilled { path, .. }) => path.to_path_buf(),
        _ => panic!("expected spilled ordinal 2"),
    };
    submit_uu_segment(&mut pipeline, displaced_file_id, 1, &parts[1], false, false).await;
    assert!(
        !farthest_path.exists(),
        "displacement must unlink the farthest spool"
    );
    assert!(matches!(
        pipeline
            .uu_files
            .get(&displaced_file_id)
            .and_then(|uu| uu.parked.get(&1)),
        Some(UuParkedEntry::Spilled { .. })
    ));
    assert_eq!(pipeline.uu_spooled_bytes, parts[1].len());
    assert_eq!(pipeline.uu_spooled_segments, 1);
    pipeline.clear_job_write_backlog(displaced_job_id);
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_spooled_segments, 0);
    assert!(
        !pipeline
            .uu_spool_root
            .join(displaced_job_id.0.to_string())
            .exists()
    );
    assert_eq!(
        pipeline.metrics.write_pending_bytes.load(Ordering::Relaxed),
        0
    );
}

#[test]
fn uu_spool_startup_cleanup_stays_inside_reserved_root() {
    let temp_dir = tempfile::tempdir().unwrap();
    let spool_root = temp_dir.path().join("intermediate").join(".uu-park");
    let stale_job = spool_root.join("20173");
    std::fs::create_dir_all(&stale_job).unwrap();
    std::fs::write(stale_job.join("part-stale"), b"stale").unwrap();
    std::fs::write(spool_root.join("orphan"), b"stale").unwrap();
    let outside = temp_dir.path().join("outside");
    std::fs::write(&outside, b"keep").unwrap();

    clear_stale_uu_park_root(&spool_root).unwrap();

    assert_eq!(std::fs::read_dir(&spool_root).unwrap().count(), 0);
    assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
}

#[tokio::test]
async fn uu_missing_spool_file_fails_job_without_leaking_accounting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;
    let job_id = JobId(20174);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 80], vec![b'b'; 90]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|part| part.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, true).await;
    let spool_path = match pipeline
        .uu_files
        .get(&file_id)
        .and_then(|uu| uu.parked.get(&1))
    {
        Some(UuParkedEntry::Spilled { path, .. }) => path.to_path_buf(),
        _ => panic!("expected a spilled UU part"),
    };
    std::fs::remove_file(&spool_path).unwrap();

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_spooled_segments, 0);
    assert_eq!(
        pipeline.metrics.write_pending_bytes.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn uu_job_teardown_releases_only_its_parked_resident_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 80], vec![b'b'; 90]];
    let first_job = JobId(20175);
    let second_job = JobId(20176);
    for job_id in [first_job, second_job] {
        insert_active_job(
            &mut pipeline,
            job_id,
            uu_job_spec(&parts.iter().map(|part| part.len()).collect::<Vec<_>>()),
        )
        .await;
        submit_uu_segment(
            &mut pipeline,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            1,
            &parts[1],
            false,
            true,
        )
        .await;
    }

    assert_eq!(pipeline.write_buffered_bytes, parts[1].len() * 2);
    pipeline.clear_job_write_backlog(first_job);
    assert_eq!(pipeline.write_buffered_bytes, parts[1].len());
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        parts[1].len() as u64
    );
    pipeline.clear_job_write_backlog(second_job);
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

#[tokio::test]
async fn uu_duplicate_segment_is_replaced_at_its_original_offset() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20063);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 500], vec![b'b'; 400]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    // The same article again: it must land on its own bytes, not shift the
    // cursor or append.
    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, true).await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(
        written,
        parts.concat(),
        "a duplicate must not lengthen the file"
    );
}

#[tokio::test]
async fn uu_single_segment_file_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20064);
    let payload = vec![b'z'; 777];
    let working_dir = insert_active_job(&mut pipeline, job_id, uu_job_spec(&[payload.len()])).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &payload, false, true).await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, payload);
}

#[tokio::test]
async fn uu_missing_middle_shifts_survivors_and_marks_damage() {
    // A part that will never arrive cannot wedge the file forever. The cursor
    // closes the gap, which shifts every later part down by the hole's width —
    // the file's tail is MISALIGNED, not merely short. The file is flagged
    // damaged so nothing downstream reads it as clean; PAR2 is the authority on
    // whether it can be recovered, and without PAR2 it is simply wrong.
    //
    // Ten parts with one failure keeps job health above its critical floor, so
    // this exercises the shift itself. The companion test below pins what
    // happens when the failure rate crosses that floor instead.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20065);
    let parts: Vec<Vec<u8>> = (0..10u8)
        .map(|i| vec![b'a' + i; 100 + i as usize])
        .collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    const HOLE: u32 = 3;
    for (index, part) in parts.iter().enumerate() {
        if index as u32 == HOLE {
            continue;
        }
        let last = index == parts.len() - 1;
        submit_uu_segment(&mut pipeline, file_id, index as u32, part, false, last).await;
    }

    // Everything from the hole onward is still parked: the cursor cannot know
    // where part 4 begins until part 3's decoded length is known.
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.next_index),
        Some(HOLE),
        "the cursor waits at the hole"
    );

    // Part 3 exhausts its retries. `book_failed_segment` also runs the job
    // health policy, which in a synthetic pipeline with no recorded successful
    // fetches fails the job outright; that policy is pinned separately by
    // `uu_file_losing_too_many_segments_fails_the_job`. Here the gap-closing
    // step is exercised on its own.
    pipeline.skip_failed_uu_segment(SegmentId {
        file_id,
        segment_number: HOLE,
    });

    assert!(
        pipeline.uu_files.get(&file_id).is_some_and(|uu| uu.damaged),
        "closing a gap must mark the file damaged"
    );
    // Terminal, not wedged: the job survived this failure and the cursor moved.
    assert!(pipeline.jobs.contains_key(&job_id));

    // Re-delivering a survivor now lands it at the shifted offset.
    submit_uu_segment(
        &mut pipeline,
        file_id,
        HOLE + 1,
        &parts[(HOLE + 1) as usize],
        false,
        false,
    )
    .await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    let mut expected: Vec<u8> = Vec::new();
    for part in parts.iter().take(HOLE as usize) {
        expected.extend_from_slice(part);
    }
    expected.extend_from_slice(&parts[(HOLE + 1) as usize]);
    assert!(
        written.starts_with(&expected),
        "survivors must commit at offsets shifted down over the hole"
    );
}

#[tokio::test]
async fn uu_file_losing_too_many_segments_fails_the_job() {
    // The observed policy when a uuencode file loses enough parts to cross the
    // job health floor: the job fails outright rather than assembling a badly
    // misaligned file. Pinned as-observed — this is the existing no-PAR2
    // damaged-file behaviour, not a uuencode-specific rule.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20067);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 300], vec![b'b'; 200], vec![b'c'; 250]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    pipeline.book_failed_segment(SegmentId {
        file_id,
        segment_number: 1,
    });

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    // Teardown released the parked bytes with the write buffers.
    assert!(!pipeline.uu_files.contains_key(&file_id));
}

#[tokio::test]
async fn uu_file_missing_its_end_marker_is_damaged() {
    // Every listed ordinal arrived, but the post itself was truncated: no
    // `end` line ever appeared. uuencode ships no checksum, so this is the only
    // signal that the file is short.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20066);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 400], vec![b'b'; 400]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    let state_before = pipeline
        .uu_files
        .get(&file_id)
        .map(|uu| uu.saw_end)
        .unwrap_or(true);
    assert!(!state_before, "no end marker has been seen yet");

    // Final part arrives without `ended`.
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;

    // The file completed, so its uu state was closed out and the damage
    // reported. The entry survives as a tombstone — it is what keeps a restart
    // checkpoint from ever being written for a uuencode file — but it holds no
    // bytes. Nothing may claim a verified reading of the file either: every
    // uuencode segment commits with part_crc_verified false.
    let closed = pipeline.uu_files.get(&file_id).expect("uu tombstone");
    assert!(closed.finished);
    assert!(closed.parked.is_empty());
    assert!(pipeline.block_crc_verdicts(file_id).is_none());
}

#[tokio::test]
async fn uu_park_displacement_requeues_instead_of_losing_the_segment() {
    // A displaced part's bytes are dropped, and the download layer already
    // counts that segment as delivered — so without a fresh fetch its data
    // exists nowhere and the cursor wedges forever when it reaches that
    // ordinal. The displacement must put it back on the queue.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_buf_max_pending = 1; // force displacement on the second park
    let job_id = JobId(20068);
    let parts: Vec<Vec<u8>> = (0..4u8).map(|i| vec![b'a' + i; 120 + i as usize]).collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let queued_before = queued_segment_count(&pipeline, job_id);

    // Parts 1, 2 and 3 all arrive before part 0. With a one-slot park, two of
    // them must be displaced.
    for index in [1u32, 2, 3] {
        submit_uu_segment(
            &mut pipeline,
            file_id,
            index,
            &parts[index as usize],
            false,
            index == 3,
        )
        .await;
    }
    assert!(
        queued_segment_count(&pipeline, job_id) > queued_before,
        "displaced parts must be requeued for download, not dropped"
    );
    // Retry budget is untouched: park pressure is an ordering condition.
    assert!(
        pipeline.decode_retries.is_empty(),
        "displacement must not spend any segment's retry budget"
    );

    // Their re-fetches arrive, then the prefix, and the file completes whole.
    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    for index in [1u32, 2, 3] {
        submit_uu_segment(
            &mut pipeline,
            file_id,
            index,
            &parts[index as usize],
            false,
            index == 3,
        )
        .await;
    }

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(
        written,
        parts.concat(),
        "no wedge: every part found its home"
    );
}

#[tokio::test]
async fn uu_park_pressure_storm_fails_nothing() {
    // Many parts arriving far ahead of a slow lowest ordinal is an ordering
    // pathology, not corruption. It must not manufacture a single permanent
    // failure, and the file must still assemble byte-identically.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_buf_max_pending = 2;
    let job_id = JobId(20069);
    let parts: Vec<Vec<u8>> = (0..12u8).map(|i| vec![b'a' + i; 60 + i as usize]).collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Storm: every part except 0, repeatedly, in descending order.
    for _ in 0..3 {
        for index in (1..parts.len() as u32).rev() {
            submit_uu_segment(
                &mut pipeline,
                file_id,
                index,
                &parts[index as usize],
                false,
                index as usize == parts.len() - 1,
            )
            .await;
        }
    }
    assert!(
        pipeline.decode_retries.is_empty(),
        "an ordering storm must never charge retry budget"
    );
    assert!(pipeline.jobs.contains_key(&job_id), "the job must survive");

    // Now drain in order; every ordinal places.
    for index in 0..parts.len() as u32 {
        submit_uu_segment(
            &mut pipeline,
            file_id,
            index,
            &parts[index as usize],
            false,
            index as usize == parts.len() - 1,
        )
        .await;
    }

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, parts.concat());
}

#[tokio::test]
async fn uu_stale_arrival_after_a_shift_is_dropped_without_retry() {
    // Once the cursor shifts past a booked-failed ordinal, a late copy of that
    // article has no home and never will. Re-fetching it would fetch bytes that
    // can never be placed, so it is dropped terminally.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20070);
    let parts: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'a' + i; 90 + i as usize]).collect();
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    pipeline.skip_failed_uu_segment(SegmentId {
        file_id,
        segment_number: 1,
    });
    let damaged_before = pipeline.uu_files.get(&file_id).map(|uu| uu.damaged);
    let queued_before = queued_segment_count(&pipeline, job_id);

    // Part 1 shows up anyway, from a server that was slow rather than missing.
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;

    assert_eq!(
        queued_segment_count(&pipeline, job_id),
        queued_before,
        "a stale arrival must not be requeued"
    );
    assert!(
        pipeline.decode_retries.is_empty(),
        "a stale arrival must not charge retry budget"
    );
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.damaged),
        damaged_before,
        "damage state is unchanged by a stale arrival"
    );
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.next_index),
        Some(2),
        "the cursor must not move for a stale arrival"
    );
}

#[tokio::test]
async fn uu_file_never_records_a_restart_checkpoint_floor() {
    // The restart checkpoint is a count of DECODED bytes, and the restore path
    // that reads it back walks the NZB's DECLARED (encoded) segment sizes to
    // decide which ordinals those bytes cover. yEnc declares ~1.03x its decoded
    // bytes, so the walk merely stops a segment early. uuencode declares
    // ~1.38x, so it would mark a prefix of ordinals received whose decoded end
    // nobody can compute — and a uuencode part's offset IS the decoded length
    // of its whole prefix. The resumed cursor would sit at ordinal 0 with every
    // arriving part parked behind ordinals that were never queued.
    //
    // So a uuencode file writes no checkpoint at all, for its final write as
    // much as for every write before it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20071);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 900], vec![b'b'; 800], vec![b'c'; 400]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for (index, part) in parts.iter().enumerate() {
        let last = index == parts.len() - 1;
        submit_uu_segment(&mut pipeline, file_id, index as u32, part, false, last).await;
        assert!(
            !pipeline.pending_file_progress.contains_key(&file_id),
            "a uuencode file must never record a restart checkpoint floor"
        );
    }
    assert!(!pipeline.persisted_file_progress.contains_key(&file_id));

    // Control: the same seam on the yEnc path still records one, so the
    // suppression above is specific to uuencode rather than a dead checkpoint.
    let yenc_job = JobId(20072);
    let payload = vec![b'y'; 512];
    insert_active_job(
        &mut pipeline,
        yenc_job,
        standalone_job_spec("Silver Horizon YEnc", &[("silver-horizon.bin".into(), 528)]),
    )
    .await;
    let yenc_file = NzbFileId {
        job_id: yenc_job,
        file_index: 0,
    };
    submit_decoded_segment(
        &mut pipeline,
        yenc_file,
        0,
        0,
        &payload,
        "silver-horizon.bin",
        None,
    )
    .await;
    assert_eq!(
        pipeline.pending_file_progress.get(&yenc_file).copied(),
        Some(payload.len() as u64),
        "a yEnc file still records its checkpoint floor"
    );
}

#[tokio::test]
async fn uu_begin_name_from_the_opening_part_survives_to_completion() {
    // yEnc repeats `name=` on every article, so the article that completes a
    // file carries the name to the identity seam. uuencode states it once, on
    // the part that opens the body — normally the first, never the last. The
    // name is retained per file so a uuencode file reaches the same identity
    // and rebind reasoning a yEnc file does.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20073);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 700], vec![b'b'; 500], vec![b'c'; 300]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Only the opening part carries a `begin` header; the rest are bare data.
    submit_uu_segment_named(
        &mut pipeline,
        file_id,
        0,
        &parts[0],
        false,
        false,
        "silver-horizon.bin",
    )
    .await;
    submit_uu_segment_named(&mut pipeline, file_id, 1, &parts[1], false, false, "").await;
    submit_uu_segment_named(&mut pipeline, file_id, 2, &parts[2], false, true, "").await;

    let closed = pipeline.uu_files.get(&file_id).expect("uu tombstone");
    assert!(closed.finished, "the file completed");
    assert_eq!(
        closed.filename.as_deref(),
        Some("silver-horizon.bin"),
        "the begin-line name must still be available when the last, nameless part completes the file"
    );
}

#[tokio::test]
async fn uu_begin_name_registers_identity_even_when_the_part_is_parked() {
    // The name-carrying part is a part like any other: it can arrive ahead of
    // its prefix and be parked, and the placement branch returns early when it
    // is. Identity must be recorded before that decision, not after it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20074);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 400], vec![b'b'; 400]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Ordinal 1 arrives first and parks, and it is the one carrying the name —
    // which the reference decoders apply regardless of which part states it.
    submit_uu_segment_named(
        &mut pipeline,
        file_id,
        1,
        &parts[1],
        false,
        true,
        "silver-horizon.vol000+01.par2",
    )
    .await;
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.parked.len()),
        Some(1),
        "the part is parked, so its placement branch returned early"
    );
    assert_eq!(
        pipeline
            .uu_files
            .get(&file_id)
            .and_then(|uu| uu.filename.as_deref()),
        Some("silver-horizon.vol000+01.par2"),
        "the name was retained before the placement decision"
    );
    // And the same seam the yEnc path uses for PAR2 recovery-count registration
    // ran too, rather than being skipped along with the placement.
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&0))
            .map(|file| file.recovery_blocks),
        Some(1),
    );
}

#[tokio::test]
async fn a_mixed_yenc_and_uuencode_job_writes_every_file_byte_identically() {
    // One NZB, two encodings. The two placement rules are completely different
    // — a yEnc part states its own offset, a uuencode part's offset is the
    // cumulative DECODED length of its prefix — and the two files' declared
    // sizes are wrong by different factors (1.03x against 1.38x). This is the
    // shape where a leaked declared byte count in either rule scatters exactly
    // one of the files while the other still looks fine.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20080);

    let yenc_a: Vec<Vec<u8>> = vec![vec![b'A'; 1500], vec![b'B'; 1500], vec![b'C'; 640]];
    let yenc_b: Vec<Vec<u8>> = vec![vec![b'D'; 900], vec![b'E'; 375]];
    let uu: Vec<Vec<u8>> = vec![vec![b'u'; 1100], vec![b'v'; 1100], vec![b'w'; 425]];

    let (spec, yenc_indices, uu_index) = mixed_encoding_job_spec(
        &[
            (
                "silver-horizon.s01e01.mkv",
                &yenc_a.iter().map(Vec::len).collect::<Vec<_>>(),
            ),
            (
                "silver-horizon.s01e02.mkv",
                &yenc_b.iter().map(Vec::len).collect::<Vec<_>>(),
            ),
        ],
        (
            "silver-horizon.readme.txt",
            &uu.iter().map(Vec::len).collect::<Vec<_>>(),
        ),
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let file_a = NzbFileId {
        job_id,
        file_index: yenc_indices[0],
    };
    let file_b = NzbFileId {
        job_id,
        file_index: yenc_indices[1],
    };
    let file_uu = NzbFileId {
        job_id,
        file_index: uu_index,
    };

    // Interleaved arrival across all three files, with each file's own parts out
    // of order where its rules allow it: the yEnc files take arbitrary order,
    // the uuencode file's early part parks until its prefix lands.
    submit_uu_segment_named(
        &mut pipeline,
        file_uu,
        0,
        &uu[0],
        false,
        false,
        "silver-horizon.readme.txt",
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_a,
        2,
        (yenc_a[0].len() + yenc_a[1].len()) as u64,
        &yenc_a[2],
        "silver-horizon.s01e01.mkv",
        None,
    )
    .await;
    submit_uu_segment_named(&mut pipeline, file_uu, 2, &uu[2], false, true, "").await;
    submit_decoded_segment(
        &mut pipeline,
        file_b,
        1,
        yenc_b[0].len() as u64,
        &yenc_b[1],
        "silver-horizon.s01e02.mkv",
        None,
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_a,
        0,
        0,
        &yenc_a[0],
        "silver-horizon.s01e01.mkv",
        None,
    )
    .await;
    // Releases the parked uuencode part behind it in the same call.
    submit_uu_segment_named(&mut pipeline, file_uu, 1, &uu[1], false, false, "").await;
    submit_decoded_segment(
        &mut pipeline,
        file_b,
        0,
        0,
        &yenc_b[0],
        "silver-horizon.s01e02.mkv",
        None,
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_a,
        1,
        yenc_a[0].len() as u64,
        &yenc_a[1],
        "silver-horizon.s01e01.mkv",
        None,
    )
    .await;

    for (file_id, name, expected) in [
        (file_a, "silver-horizon.s01e01.mkv", yenc_a.concat()),
        (file_b, "silver-horizon.s01e02.mkv", yenc_b.concat()),
        (file_uu, "silver-horizon.readme.txt", uu.concat()),
    ] {
        assert!(
            pipeline
                .jobs
                .get(&job_id)
                .and_then(|state| state.assembly.file(file_id))
                .is_some_and(|file| file.is_complete()),
            "{name} did not complete"
        );
        let written = tokio::fs::read(working_dir.join(name)).await.unwrap();
        assert_eq!(written, expected, "{name} was not written byte-identically");
    }

    // The two encodings kept their own evidence rules: the yEnc files each
    // proved a contiguous decoded tiling, the uuencode file fed the grid
    // nothing and recorded no restart checkpoint.
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        state
            .assembly
            .file(file_a)
            .unwrap()
            .contiguous_placements_proven()
    );
    assert!(
        state
            .assembly
            .file(file_b)
            .unwrap()
            .contiguous_placements_proven()
    );
    assert!(pipeline.block_crc_verdicts(file_uu).is_none());
    assert!(!pipeline.pending_file_progress.contains_key(&file_uu));
    assert!(pipeline.pending_file_progress.contains_key(&file_a));
}

// ---- PAR2 binding by content (obfuscation) ----

#[tokio::test]
async fn a_complete_content_match_with_the_wrong_length_is_refused() {
    let payload = binding_payload(29, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20110),
        "c841ef20.bin",
        &[("silver-horizon.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        &payload[..4_096],
        "c841ef20.bin",
        None,
    )
    .await;
    pipeline.file_prefix_16k.insert(
        file_id,
        payload[..crate::pipeline::PAR2_HASH_16K_BYTES].to_vec(),
    );
    pipeline.file_declared_size.remove(&file_id);

    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("test file");
    assert!(file.is_complete(), "the fixture must be complete");
    assert_ne!(file.received_bytes(), payload.len() as u64);
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a complete file with contradictory decoded length must not content-bind"
    );
}

#[tokio::test]
async fn an_incomplete_content_match_at_or_under_the_described_length_still_binds() {
    let payload = binding_payload(31, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20111),
        "9d2a18ce.bin",
        &[("silver-horizon.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("test file");
    assert!(!file.is_complete(), "the fixture must remain incomplete");
    assert!(file.received_bytes() <= payload.len() as u64);
    assert!(!pipeline.file_declared_size.contains_key(&file_id));
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_some(),
        "in-stream verification needs incomplete, noncontradictory files to bind"
    );
}

#[tokio::test]
async fn an_incomplete_fragment_with_a_contradictory_declared_size_is_refused() {
    let payload = binding_payload(37, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20112),
        "onyx-prairie.mkv.001",
        &[("onyx-prairie.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;
    pipeline.file_declared_size.insert(file_id, 16_384);

    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a contradictory yEnc declaration must refuse the content match"
    );
}

#[tokio::test]
async fn an_incomplete_obfuscated_file_with_a_contradictory_declared_size_is_refused() {
    let payload = binding_payload(39, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20115),
        "7e4c19ab.bin",
        &[("onyx-prairie.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;
    pipeline.file_declared_size.insert(file_id, 16_384);

    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("test file");
    assert!(!file.is_complete(), "the fixture must remain incomplete");
    assert!(file.received_bytes() <= payload.len() as u64);
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a contradictory yEnc declaration must refuse an otherwise matching content bind"
    );
}

#[tokio::test]
async fn an_absent_declared_size_does_not_change_content_binding() {
    let payload = binding_payload(41, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20113),
        "b0d0c6aa.bin",
        &[("ivory-meadow.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    assert!(!pipeline.file_declared_size.contains_key(&file_id));
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_some(),
        "without a declared size, a matching prefix remains usable"
    );
}

#[tokio::test]
async fn a_split_fragment_prefix_match_is_refused() {
    let payload = binding_payload(43, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20114),
        "ivory-meadow.mkv.001",
        &[("ivory-meadow.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    assert!(!pipeline.file_declared_size.contains_key(&file_id));
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a numeric split fragment cannot bind to the joined description by prefix"
    );
}

#[tokio::test]
async fn an_obfuscated_file_binds_to_its_description_by_content() {
    // The point of the whole seam. The post says `a7f3e91c8b2d.bin`; the
    // recovery set describes `silver-horizon.s01e01.mkv`. Nothing matches by
    // name, so before this the file bound to no description at all — and an
    // unbound file has nothing to measure its in-stream block verdicts against,
    // so the entire dual-CRC grid lapsed for it.
    let payload = binding_payload(7, 40_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20090),
        "a7f3e91c8b2d.bin",
        &[("silver-horizon.s01e01.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let expected = *set.files.keys().next().expect("one description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(expected),
        "the bytes are what the obfuscation did not touch"
    );
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.described_length),
        Some(payload.len() as u64),
        "and the binding carries the DESCRIBED length, never a declared one"
    );
}

#[tokio::test]
async fn content_binding_disambiguates_equal_length_obfuscated_descriptions() {
    let first = binding_payload(73, 40_000);
    let second = binding_payload(74, 40_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(200901),
        "a7f3e91c8b2d.bin",
        &[
            ("silver-horizon.s01e01.mkv", &first),
            ("silver-horizon.s01e02.mkv", &second),
        ],
        &first[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let expected = set
        .files
        .iter()
        .find_map(|(id, description)| {
            (description.filename == "silver-horizon.s01e01.mkv").then_some(*id)
        })
        .expect("first description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(expected),
        "the hash identifies one description even though length alone is ambiguous"
    );
}

#[tokio::test]
async fn a_short_description_binds_from_its_whole_file_hash() {
    // The spec trap. A description shorter than 16 KiB hashes its whole file
    // with no padding, so a matcher that demanded 16 KiB of prefix — or that
    // padded the short one out — would refuse exactly the small files an
    // obfuscated set tends to open with.
    let payload = binding_payload(19, 5_000);
    assert!(payload.len() < crate::pipeline::PAR2_HASH_16K_BYTES);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20091),
        "3f92aa10.bin",
        &[("silver-horizon.nfo", &payload)],
        &payload,
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let expected = *set.files.keys().next().expect("one description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(expected)
    );
}

#[tokio::test]
async fn two_descriptions_sharing_a_prefix_fail_closed() {
    // Volumes of one set routinely open with identical headers, so a shared
    // 16 KiB prefix is a real shape rather than a contrived one. Binding either
    // way would be a guess; the file stays unbound and is read at completion,
    // exactly as every file was before the grid existed.
    let shared = binding_payload(3, crate::pipeline::PAR2_HASH_16K_BYTES);
    let mut first = shared.clone();
    first.extend_from_slice(&binding_payload(101, 4_096));
    let mut second = shared.clone();
    second.extend_from_slice(&binding_payload(202, 4_096));

    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20092),
        "bb17c40e.bin",
        &[
            ("silver-horizon.part01.rar", &first),
            ("silver-horizon.part02.rar", &second),
        ],
        &shared,
    )
    .await;

    // Non-vacuity: the two descriptions really do share the hash being matched.
    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let hashes: HashSet<[u8; 16]> = set.files.values().map(|desc| desc.hash_16k).collect();
    assert_eq!(
        hashes.len(),
        1,
        "the fixture must produce one shared hash_16k, or nothing is ambiguous"
    );

    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "an ambiguous content match must refuse, on the same terms an ambiguous \
         name match always has"
    );
}

#[tokio::test]
async fn a_name_match_still_wins_over_content() {
    // Content binding is the fallback and must never become the rule: it is
    // consulted only when no description answers to any of the file's names.
    // Here the posted name matches one description while the captured bytes
    // match a different one, and the name has to win.
    let named = binding_payload(11, 20_000);
    let other = binding_payload(77, 20_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20093),
        "silver-horizon.s01e02.mkv",
        &[
            ("silver-horizon.s01e02.mkv", &named),
            ("amber-trail.s01e01.mkv", &other),
        ],
        // The bytes of the OTHER description.
        &other[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let by_name = *set
        .files
        .iter()
        .find(|(_, desc)| desc.filename == "silver-horizon.s01e02.mkv")
        .map(|(file_id, _)| file_id)
        .expect("the named description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(by_name),
        "the name path runs first and returns before any hashing happens"
    );
}

#[tokio::test]
async fn a_file_whose_first_article_never_arrived_never_content_binds() {
    // The capture is anchored at offset 0 and grows only from its own end, so a
    // file missing its opening article has no prefix at all — and a file with a
    // hole in its prefix would be hashing bytes that are not the file's first
    // bytes. Both answer "no binding" rather than guessing, and neither panics.
    let payload = binding_payload(23, 40_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20094),
        "c81be004.bin",
        &[("silver-horizon.s01e03.mkv", &payload)],
        &[],
    )
    .await;
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "an empty capture binds nothing"
    );

    // No entry at all — the shape when the first article is still outstanding.
    pipeline.file_prefix_16k.remove(&file_id);
    assert!(pipeline.resolve_par2_file_binding(file_id).is_none());

    // A capture that is real but shorter than the description's window: the
    // second article landed, the first did not, and the buffer refused to
    // stitch it in at its offset.
    pipeline
        .file_prefix_16k
        .insert(file_id, payload[..1_000].to_vec());
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a partial prefix cannot be tested against a 16 KiB window without \
         inventing the bytes it is missing"
    );
}

#[tokio::test]
async fn the_prefix_capture_takes_only_an_offset_zero_anchored_run() {
    // The capture rule itself, at the seam. Articles arriving out of order must
    // not leave a stitched-together buffer that hashes to nothing real.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20095);
    let parts: Vec<Vec<u8>> = vec![
        binding_payload(1, 1_000),
        binding_payload(2, 1_000),
        binding_payload(3, 1_000),
    ];
    let declared: Vec<u32> = parts.iter().map(|part| part.len() as u32 + 32).collect();
    let mut spec = standalone_job_spec("Silver Horizon Prefix", &[("obf.bin".into(), declared[0])]);
    spec.files[0].segments.clear();
    for (index, bytes) in declared.iter().enumerate() {
        spec.files[0].segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("pfx-{index}@example.com"),
        });
    }
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Segment 1 lands first: it starts past offset 0, so nothing is captured.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        1,
        parts[0].len() as u64,
        &parts[1],
        "obf.bin",
        None,
    )
    .await;
    assert!(
        pipeline
            .file_prefix_16k
            .get(&file_id)
            .is_none_or(Vec::is_empty),
        "a span that does not start at the captured end is skipped, never stitched"
    );

    // Segment 0 lands and opens the run.
    submit_decoded_segment(&mut pipeline, file_id, 0, 0, &parts[0], "obf.bin", None).await;
    assert_eq!(
        pipeline.file_prefix_16k.get(&file_id).map(Vec::len),
        Some(parts[0].len()),
        "the run starts at zero"
    );

    // Segment 2 extends it; the hole segment 1 left is never closed, because
    // the capture only ever grows from its own end.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        2,
        (parts[0].len() + parts[1].len()) as u64,
        &parts[2],
        "obf.bin",
        None,
    )
    .await;
    assert_eq!(
        pipeline.file_prefix_16k.get(&file_id).map(Vec::len),
        Some(parts[0].len()),
        "and a later span past the hole cannot extend it either"
    );
    assert_eq!(
        pipeline.file_prefix_16k.get(&file_id).map(Vec::as_slice),
        Some(parts[0].as_slice()),
        "what was captured is exactly the file's own first bytes"
    );
}
