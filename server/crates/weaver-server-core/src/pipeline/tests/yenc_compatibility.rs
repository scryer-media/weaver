use super::*;

#[tokio::test(start_paused = true)]
async fn retained_overlap_never_exhausts_clean_backup_in_either_order() {
    use weaver_yenc::CrcVerification::{Mismatch, Verified};
    for damage_first in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, file_id, path) = setup(&temp, 40219, &[8, 8]).await;
        pipeline.jobs.get_mut(&file_id.job_id).unwrap().status = JobStatus::Paused;
        if damage_first {
            deliver_from(&mut pipeline, file_id, 0, 0, b"bad!X", Mismatch, Some(0)).await;
        }
        deliver_from(&mut pipeline, file_id, 1, 4, b"tail", Verified, Some(1)).await;
        if !damage_first {
            deliver_from(&mut pipeline, file_id, 0, 0, b"bad!X", Mismatch, Some(0)).await;
            assert_eq!(std::fs::read(&path).unwrap(), b"bad!");
        }
        let neighbor = SegmentId {
            file_id,
            segment_number: 1,
        };
        assert!(!pipeline.decode_retries.contains_key(&neighbor));
        assert!(!pipeline.segment_terminal_states.contains_key(&neighbor));
        deliver_from(&mut pipeline, file_id, 0, 0, b"head", Verified, Some(1)).await;
        assert_eq!(std::fs::read(&path).unwrap(), b"headtail");
        let file = pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap();
        assert!(file.is_complete());
        assert!(!file.has_retained_damage());
        assert!(pipeline.segment_terminal_states.is_empty());
        assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
        assert_eq!(
            pipeline.metrics.segments_committed.load(Ordering::Relaxed),
            2
        );
    }
}

#[tokio::test(start_paused = true)]
async fn parked_damage_masks_accepted_and_reconstructed_ranges_after_reset() {
    for reconstructed in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, file_id, path) = setup(&temp, 40220, &[8, 8]).await;
        pipeline.jobs.get_mut(&file_id.job_id).unwrap().status = JobStatus::Paused;
        deliver(&mut pipeline, file_id, 1, 4, b"tail", true).await;
        let file = pipeline
            .jobs
            .get_mut(&file_id.job_id)
            .unwrap()
            .assembly
            .file_mut(file_id)
            .unwrap();
        file.reset();
        if reconstructed {
            std::fs::write(&path, b"....tail").unwrap();
            file.record_reconstructed_placement(1, 4, 4);
        }
        // This payload was parked before the reset; split chunks also exercise
        // masking across chunk boundaries without making a contiguous copy.
        let damaged = BufferedDecodedSegment {
            segment_id: SegmentId {
                file_id,
                segment_number: 0,
            },
            damaged_source: Some(Box::new(RetainedArticleDamage {
                source: SegmentSource {
                    source_server_idx: None,
                    exclude_servers: Vec::new(),
                },
                status: weaver_yenc::CrcVerification::Mismatch,
                write_spans: Vec::new(),
            })),
            decoded_size: 5,
            encoding: SegmentEncoding::Yenc,
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            data: DecodedChunk::from(vec![
                b"bad".to_vec().into_boxed_slice(),
                b"!X".to_vec().into_boxed_slice(),
            ]),
            part_crc: 0,
            part_crc_verified: false,
            yenc_name: "sample.bin".into(),
            segments: Vec::new(),
        };
        pipeline.note_write_buffered(5, 1);
        let buffer = pipeline.write_buffers.get_mut(&file_id).unwrap();
        buffer.insert(0, damaged);
        let (ready, contiguous_end) = buffer.drain_ready_with_contiguous_end();
        assert_eq!(contiguous_end, 0, "damage must not advance coverage");
        assert_eq!(buffer.buffered_len(), 1, "clean tail remains parked");
        pipeline
            .persist_ready_segments(file_id, ready, contiguous_end)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            if reconstructed {
                b"bad!tail".as_slice()
            } else {
                b"bad!".as_slice()
            }
        );
        deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
        assert_eq!(std::fs::read(&path).unwrap(), b"headtail");
        assert_eq!(pipeline.write_buffered_bytes, 0);
        assert_eq!(
            pipeline.metrics.segments_committed.load(Ordering::Relaxed),
            2
        );
    }
}

#[tokio::test]
async fn parked_article_restores_placement_after_assembly_reset() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40218, &[8, 8]).await;
    pipeline.jobs.get_mut(&file_id.job_id).unwrap().status = JobStatus::Paused;
    deliver(&mut pipeline, file_id, 1, 4, b"tail", true).await;
    assert_eq!(pipeline.write_buffers[&file_id].buffered_len(), 1);
    let file = pipeline
        .jobs
        .get_mut(&file_id.job_id)
        .unwrap()
        .assembly
        .file_mut(file_id)
        .unwrap();
    assert_eq!(file.placement_of(1), Some((4, 4)));
    // Reconstruction resets assembly while the ordinary writer still owns
    // this buffered article. Its durable handoff must restore the range.
    file.reset();
    deliver(&mut pipeline, file_id, 0, 0, b"head", true).await;
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert_eq!(file.decoded_coverage_end(), Some(8));
    assert!(!file.requires_file_verification());
    assert_eq!(std::fs::read(path).unwrap(), b"headtail");
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        2
    );
}

#[tokio::test]
async fn verified_late_replacement_clears_exhausted_failure_once() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40211, &[8, 8]).await;
    let segment = SegmentId {
        file_id,
        segment_number: 0,
    };
    pipeline.decode_retries.insert(segment, MAX_SEGMENT_RETRIES);
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 8);
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
    assert!(!pipeline.segment_terminal_states.contains_key(&segment));
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 1, 4, b"tail", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodtail");
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn unverified_replacement_does_not_erase_known_damage() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40212, &[8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    let _retry = pipeline.retry_rx.recv().await.unwrap();
    deliver_status(
        &mut pipeline,
        file_id,
        0,
        0,
        b"data",
        weaver_yenc::CrcVerification::Unverified,
    )
    .await;
    assert_eq!(std::fs::read(path).unwrap(), b"data");
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(!file.is_complete());
    assert!(file.has_retained_damage());
    assert!(!pipeline.pending_file_progress.contains_key(&file_id));
}

#[tokio::test]
async fn checksum_absence_alone_is_not_known_damage() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40213, &[8]).await;
    deliver_status(
        &mut pipeline,
        file_id,
        0,
        0,
        b"data",
        weaver_yenc::CrcVerification::Unverified,
    )
    .await;
    assert_eq!(std::fs::read(path).unwrap(), b"data");
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(file.is_complete());
    assert!(!file.has_retained_damage());
    assert!(pipeline.segment_terminal_states.is_empty());
}

async fn setup(temp: &TempDir, id: u64, sizes: &[u32]) -> (Pipeline, NzbFileId, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let job_id = JobId(id);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("yenc compatibility", "sample.bin", sizes),
    )
    .await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.next_health_probe_failed_bytes = u64::MAX;
    state.par2_bytes = state.spec.total_bytes;
    // These tests deliver the results of already dispatched requests.
    state.download_queue.drain_all();
    let path = state.working_dir.join("sample.bin");
    (
        pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        path,
    )
}

pub(super) async fn deliver(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    valid: bool,
) {
    let status = if valid {
        weaver_yenc::CrcVerification::Verified
    } else {
        weaver_yenc::CrcVerification::Mismatch
    };
    deliver_status(pipeline, file_id, number, offset, bytes, status).await;
}

async fn deliver_status(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
) {
    deliver_from(pipeline, file_id, number, offset, bytes, status, None).await;
}

async fn deliver_from(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    number: u32,
    offset: u64,
    bytes: &[u8],
    status: weaver_yenc::CrcVerification,
    source_server_idx: Option<usize>,
) {
    let len = bytes.len() as u64;
    let yenc_name = pipeline
        .current_filename_for_file_id(file_id.job_id, file_id)
        .unwrap_or_else(|| "sample.bin".to_owned());
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id: SegmentId {
                    file_id,
                    segment_number: number,
                },
                raw_size: len,
                encoding: SegmentEncoding::Yenc,
                // Consistently wrong part, count, size and end must never override
                // usable begin plus actual bytes, even after the old threshold.
                yenc_layout: YencLayoutAssertions {
                    file_size: 6,
                    part: Some(99),
                    total: Some(402),
                    begin: Some(offset + 1),
                    end: None,
                },
                crc_valid: status != weaver_yenc::CrcVerification::Mismatch,
                part_crc_verified: status == weaver_yenc::CrcVerification::Verified,
                part_crc: par2_rs::checksum::crc32(bytes),
                expected_file_crc: None,
                data: DecodedChunk::from(bytes.to_vec()),
                yenc_name,
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: vec![weaver_yenc::Segment {
                    file_offset: offset,
                    len,
                    crc32: par2_rs::checksum::crc32(bytes),
                }],
            },
            SegmentSource {
                source_server_idx,
                exclude_servers: Vec::new(),
            },
        )
        .await;
    settle_direct_demotion_work(pipeline).await;
}

#[tokio::test]
async fn stale_metadata_assembles_twenty_parts_without_abandonment() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40201, &[8; 20]).await;
    for number in 0..20 {
        deliver(
            &mut pipeline,
            file_id,
            number,
            u64::from(number) * 4,
            &[number as u8; 4],
            true,
        )
        .await;
    }
    let expected: Vec<u8> = (0..20).flat_map(|number| [number; 4]).collect();
    assert_eq!(std::fs::read(path).unwrap(), expected);
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(file.is_complete());
    assert!(file.contiguous_placements_proven());
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        20
    );
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn damaged_bytes_are_written_before_retry_and_verified_replacement_commits_once() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40202, &[8, 8]).await;
    let segment = SegmentId {
        file_id,
        segment_number: 0,
    };
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    assert_eq!(std::fs::read(&path).unwrap(), b"bad!");
    assert!(
        !pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .has_segment(0)
    );
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.jobs[&file_id.job_id].downloaded_bytes, 0);
    assert!(!pipeline.file_prefix_16k.contains_key(&file_id));
    assert!(!pipeline.pending_file_progress.contains_key(&file_id));
    // Paused Tokio time advances to the scheduled retry when this receive is
    // the only work left. The observable message, not elapsed time, is proof.
    let retry = pipeline.retry_rx.recv().await.unwrap();
    assert_eq!(retry.work.segment_id, segment);
    assert_eq!(std::fs::read(&path).unwrap(), b"bad!");
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 1, 4, b"tail", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodtail");
    assert!(
        pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        2
    );
    assert_eq!(pipeline.metrics.bytes_committed.load(Ordering::Relaxed), 8);
    assert_eq!(pipeline.jobs[&file_id.job_id].downloaded_bytes, 8);
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn exhausted_damage_keeps_bytes_but_never_success_or_restart_coverage() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40203, &[8, 8]).await;
    let segment = SegmentId {
        file_id,
        segment_number: 0,
    };
    pipeline.decode_retries.insert(segment, MAX_SEGMENT_RETRIES);
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    deliver(&mut pipeline, file_id, 1, 4, b"tail", true).await;
    pipeline.flush_quiescent_write_backlog().await;
    pipeline.note_file_progress_floor(file_id, 8, false);
    assert_eq!(std::fs::read(path).unwrap(), b"bad!tail");
    let file = pipeline.jobs[&file_id.job_id]
        .assembly
        .file(file_id)
        .unwrap();
    assert!(!file.is_complete());
    assert!(!file.has_segment(0));
    assert!(file.has_segment(1));
    assert_eq!(
        pipeline.segment_terminal_states.get(&segment),
        Some(&SegmentTerminalState::DecodeExhausted)
    );
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 8);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
    assert!(!pipeline.pending_file_progress.contains_key(&file_id));
    pipeline.handle_decode_failure(segment, "duplicate failure", &[], None);
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 8);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn shorter_verified_final_replacement_truncates_once_without_failed_segments() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40204, &[16]).await;
    // Already-dispatched results still settle while paused; hold publication
    // so repeated arrivals can be checked against the same durable file.
    pipeline.jobs.get_mut(&file_id.job_id).unwrap().status = JobStatus::Paused;
    deliver(&mut pipeline, file_id, 0, 0, b"bad tail", false).await;
    let _retry = pipeline.retry_rx.recv().await.unwrap();
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"good");
    assert!(
        pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
    assert!(!pipeline.pending_file_progress.contains_key(&file_id));
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        1
    );
    assert_eq!(pipeline.metrics.bytes_committed.load(Ordering::Relaxed), 4);
    assert_eq!(pipeline.jobs[&file_id.job_id].failed_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn shorter_replacement_next_segment_overwrites_the_old_tail() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40205, &[16, 16]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad tail", false).await;
    let _retry = pipeline.retry_rx.recv().await.unwrap();
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 1, 4, b"next", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodnext");
    assert!(
        pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
    assert_eq!(pipeline.metrics.bytes_committed.load(Ordering::Relaxed), 8);
}

#[tokio::test]
async fn a_late_damaged_duplicate_cannot_overwrite_committed_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40206, &[8, 8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    assert_eq!(std::fs::read(path).unwrap(), b"good");
    assert!(pipeline.decode_retries.is_empty());
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn retained_blocks_make_repair_possible_when_discarding_the_article_does_not() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40207, &[32]).await;
    let original = b"aaaabbbbcccc";
    let segment = SegmentId {
        file_id,
        segment_number: 0,
    };
    pipeline.decode_retries.insert(segment, MAX_SEGMENT_RETRIES);
    // One article covers three PAR2 blocks. Only the middle block is damaged;
    // its two usable neighbours are necessary with one recovery block.
    deliver(&mut pipeline, file_id, 0, 0, b"aaaaBROKcccc", false).await;
    let set = build_repairable_par2_set("sample.bin", original, 4, 1);
    let discarded = temp.path().join("discarded");
    std::fs::create_dir(&discarded).unwrap();
    let mut options = par2_rs::Par2RepairerOptions::new(discarded, Vec::new());
    options.file_set = Some(set.clone());
    options.repair = true;
    let missing = par2_rs::Par2Repairer::new(options)
        .verify_or_repair()
        .unwrap();
    assert_eq!(missing.status, par2_rs::Par2RepairStatus::Insufficient);
    let mut options =
        par2_rs::Par2RepairerOptions::new(path.parent().unwrap().to_path_buf(), Vec::new());
    options.file_set = Some(set);
    options.repair = true;
    let repaired = par2_rs::Par2Repairer::new(options)
        .verify_or_repair()
        .unwrap();
    assert_eq!(repaired.status, par2_rs::Par2RepairStatus::Repaired);
    assert_eq!(std::fs::read(path).unwrap(), original);
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn retained_damage_without_repair_data_cannot_be_delivered() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _) = setup(&temp, 40208, &[8, 8]).await;
    pipeline.decode_retries.insert(
        SegmentId {
            file_id,
            segment_number: 0,
        },
        MAX_SEGMENT_RETRIES,
    );
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    deliver(&mut pipeline, file_id, 1, 4, b"good", true).await;
    let error = pipeline
        .reconcile_terminal_delivery(file_id.job_id)
        .unwrap_err();
    assert!(error.contains("require verification or repair"), "{error}");
}

#[tokio::test]
async fn restart_refetches_retained_damage_and_overwrites_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40209, &[8, 8]).await;
    let job_id = file_id.job_id;
    let spec = pipeline.jobs[&job_id].spec.clone();
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    pipeline.decode_retries.insert(
        SegmentId {
            file_id,
            segment_number: 0,
        },
        MAX_SEGMENT_RETRIES,
    );
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    deliver(&mut pipeline, file_id, 1, 4, b"tail", true).await;
    pipeline.flush_quiescent_write_backlog().await;
    pipeline
        .flush_file_progress_batch_awaited("test.restart")
        .await
        .unwrap();
    pipeline.db.flush_write_queue().await.unwrap();
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    assert!(recovered.complete_files.is_empty());
    assert!(recovered.file_progress.is_empty());
    drop(pipeline);
    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: recovered.complete_files,
            file_progress: recovered.file_progress,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();
    let file = restored.jobs[&job_id].assembly.file(file_id).unwrap();
    assert!(!file.has_segment(0));
    assert!(
        !file.has_retained_damage(),
        "damage is not new persisted state"
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"bad!tail");
    deliver(&mut restored, file_id, 0, 0, b"good", true).await;
    deliver(&mut restored, file_id, 1, 4, b"tail", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodtail");
    assert!(
        restored.jobs[&job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_does_not_allow_a_late_retry_to_write() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40210, &[8, 8]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad!", false).await;
    pipeline.fail_job(file_id.job_id, "cancelled fixture".to_string());
    let retry = pipeline.retry_rx.recv().await.unwrap();
    assert_eq!(retry.work.segment_id.file_id, file_id);
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"bad!");
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

#[tokio::test]
async fn stale_first_size_hint_cannot_poison_content_binding_or_verified_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _) = setup(&temp, 40214, &[32768; 3]).await;
    let first = vec![7; 40000];
    let second = vec![9; 40000];
    let set = build_repairable_par2_set_for_files(
        &[("first.bin", &first), ("second.bin", &second)],
        1024,
        1,
    );
    let first_id = *set
        .files
        .iter()
        .find(|(_, desc)| desc.filename == "first.bin")
        .unwrap()
        .0;
    let second_id = *set
        .files
        .iter()
        .find(|(_, desc)| desc.filename == "second.bin")
        .unwrap()
        .0;
    let set_id = set.recovery_set_id;
    let runtime = pipeline.ensure_par2_runtime(file_id.job_id);
    runtime.served = Some(set_id);
    runtime.ensure_set_runtime(set_id).set = Some(std::sync::Arc::new(set));
    deliver(&mut pipeline, file_id, 0, 0, &first[..16384], true).await;
    assert_eq!(pipeline.file_declared_size[&file_id], 6);
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .unwrap()
            .par2_file_id,
        first_id
    );
    // Same ordinal and placement: refreshed prefix evidence must describe the
    // replacement, not the first arrival's otherwise valid part checksum.
    deliver(&mut pipeline, file_id, 0, 0, &second[..16384], true).await;
    assert_eq!(pipeline.file_declared_size[&file_id], 6);
    assert_eq!(pipeline.file_prefix_16k[&file_id], second[..16384]);
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .unwrap()
            .par2_file_id,
        second_id
    );
}

#[tokio::test(start_paused = true)]
async fn unverified_intermediate_replacement_stays_unsettled_until_verified() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40215, &[16, 16]).await;
    deliver(&mut pipeline, file_id, 0, 0, b"bad tail", false).await;
    let _retry = pipeline.retry_rx.recv().await.unwrap();
    deliver_status(
        &mut pipeline,
        file_id,
        0,
        0,
        b"old tail",
        weaver_yenc::CrcVerification::Unverified,
    )
    .await;
    let retry = pipeline.retry_rx.recv().await.unwrap();
    assert_eq!(retry.work.segment_id.segment_number, 0);
    assert!(
        !pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .has_segment(0)
    );
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.metrics.crc_errors.load(Ordering::Relaxed), 1);
    assert!(!pipeline.file_prefix_16k.contains_key(&file_id));
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    deliver(&mut pipeline, file_id, 1, 4, b"next", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"goodnext");
    assert_eq!(
        pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .received_bytes(),
        8
    );
    assert_eq!(pipeline.metrics.bytes_committed.load(Ordering::Relaxed), 8);
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        2
    );
    assert!(pipeline.segment_terminal_states.is_empty());
}

async fn restart_partial(mut pipeline: Pipeline, temp: &TempDir, file_id: NzbFileId) -> Pipeline {
    let job_id = file_id.job_id;
    let spec = pipeline.jobs[&job_id].spec.clone();
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    pipeline.flush_quiescent_write_backlog().await;
    pipeline
        .flush_file_progress_batch_awaited("test.yenc_restart")
        .await
        .unwrap();
    pipeline.db.flush_write_queue().await.unwrap();
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    assert!(recovered.complete_files.is_empty());
    drop(pipeline);
    let (mut restored, _, _) = new_direct_pipeline(temp).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            working_dir,
            complete_files: recovered.complete_files,
            file_progress: recovered.file_progress,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
        })
        .await
        .unwrap();
    restored
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .drain_all();
    // Inspect durable completion rows before post-processing removes them.
    restored.jobs.get_mut(&job_id).unwrap().status = JobStatus::Paused;
    restored
}

#[tokio::test]
async fn restart_shorter_verified_final_article_removes_stale_suffix() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = setup(&temp, 40216, &[16]).await;
    pipeline.decode_retries.insert(
        SegmentId {
            file_id,
            segment_number: 0,
        },
        MAX_SEGMENT_RETRIES,
    );
    deliver(&mut pipeline, file_id, 0, 0, b"bad tail", false).await;
    let mut pipeline = restart_partial(pipeline, &temp, file_id).await;
    assert!(
        !pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .has_retained_damage()
    );
    deliver(&mut pipeline, file_id, 0, 0, b"good", true).await;
    assert_eq!(std::fs::read(path).unwrap(), b"good");
    assert!(pipeline.segment_terminal_states.is_empty());
    pipeline.db.flush_write_queue().await.unwrap();
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&file_id.job_id)
        .unwrap();
    assert!(recovered.complete_files.contains(&file_id));
}

#[tokio::test]
async fn restart_checkpoint_preserves_good_prefix_but_cannot_conceal_a_gap() {
    for gap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, file_id, path) = setup(&temp, 40217, &[14, 14, 8, 8]).await;
        deliver(&mut pipeline, file_id, 0, 0, b"0123456789", true).await;
        deliver(&mut pipeline, file_id, 1, 10, b"abcdefghij", true).await;
        pipeline.decode_retries.insert(
            SegmentId {
                file_id,
                segment_number: 2,
            },
            MAX_SEGMENT_RETRIES,
        );
        deliver(&mut pipeline, file_id, 2, 20, b"bad!", false).await;
        deliver(&mut pipeline, file_id, 3, 24, b"tail", true).await;
        let mut pipeline = restart_partial(pipeline, &temp, file_id).await;
        assert!(
            pipeline.jobs[&file_id.job_id]
                .assembly
                .file(file_id)
                .unwrap()
                .has_segment(0)
        );
        deliver(&mut pipeline, file_id, 1, 10, b"abcdefghij", true).await;
        deliver(
            &mut pipeline,
            file_id,
            2,
            20,
            if gap { b"ok" } else { b"good" },
            true,
        )
        .await;
        deliver(&mut pipeline, file_id, 3, 24, b"tail", true).await;
        // The restore cursor is conservatively rounded in encoded units;
        // exercise the actor's quiescent drain before observing disk state.
        pipeline.flush_quiescent_write_backlog().await;
        let file = pipeline.jobs[&file_id.job_id]
            .assembly
            .file(file_id)
            .unwrap();
        assert_eq!(file.requires_file_verification(), gap);
        assert!(
            pipeline.segment_terminal_states.is_empty(),
            "verified parts never become failed parts"
        );
        pipeline.db.flush_write_queue().await.unwrap();
        let recovered = pipeline
            .db
            .load_active_jobs()
            .unwrap()
            .remove(&file_id.job_id)
            .unwrap();
        assert_eq!(
            recovered.complete_files.contains(&file_id),
            !gap,
            "gap={gap}; {}; disk={:?}",
            debug_job_state(&pipeline, file_id.job_id),
            std::fs::read(&path)
        );
        assert_eq!(
            pipeline
                .reconcile_terminal_delivery(file_id.job_id)
                .is_err(),
            gap
        );
        if !gap {
            assert_eq!(
                std::fs::read(path).unwrap(),
                b"0123456789abcdefghijgoodtail"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn real_wire_crc32_only_retry_clears_damage_through_every_adapter() {
    for adapter in ["fused", "pooled", "owned"] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, file_id, path) = setup(&temp, 40218, &[256]).await;
        pipeline.jobs.get_mut(&file_id.job_id).unwrap().status = JobStatus::Paused;
        let segment_id = SegmentId {
            file_id,
            segment_number: 0,
        };
        let mut good = Vec::new();
        weaver_yenc::encode(b"data", &mut good, 128, "sample.bin").unwrap();
        assert!(!good.windows(7).any(|w| w == b"pcrc32="));
        let crc_start = good.windows(6).position(|w| w == b"crc32=").unwrap() + 6;
        let mut bad = good.clone();
        bad[crc_start..crc_start + 8].copy_from_slice(b"00000000");
        for (valid, raw) in [(false, bad), (true, good)] {
            if adapter == "fused" {
                let mut wire = b"222 article follows\r\n".to_vec();
                wire.extend_from_slice(&raw);
                wire.extend_from_slice(b".\r\n");
                let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
                let mut input = bytes::BytesMut::new();
                let mut decoded = None;
                for byte in wire {
                    input.extend_from_slice(&[byte]);
                    if let Some(article) = decoder.decode_available(&mut input).unwrap() {
                        decoded = Some(article);
                    }
                }
                let article = decoded.unwrap();
                let trace = weaver_nntp::client::DecodedBodyTrace {
                    attempts: Vec::new(),
                    result: Ok(weaver_nntp::client::DecodedBody {
                        raw_size: raw.len() as u32,
                        decoded: article.chunks,
                        body: article.body,
                        cpu: Default::default(),
                        io: Default::default(),
                    }),
                };
                let (payload, _, _) = Pipeline::download_data_from_decoded_trace(segment_id, trace);
                let DownloadPayload::Decoded(result) = payload.unwrap() else {
                    panic!("decoded payload required")
                };
                assert_eq!(result.part_crc_verified, valid, "{adapter}");
                pipeline
                    .handle_decode_success(
                        result,
                        SegmentSource {
                            source_server_idx: None,
                            exclude_servers: Vec::new(),
                        },
                    )
                    .await;
            } else {
                let tier = crate::runtime::buffers::BufferTier::for_size(raw.len());
                let mut held = Vec::new();
                if adapter == "owned" {
                    while let Some(buffer) = pipeline.buffers.try_acquire(tier) {
                        held.push(buffer);
                    }
                }
                pipeline.metrics.note_decode_work_queued(raw.len() as u64);
                pipeline.pending_decode.push_back(PendingDecodeWork {
                    segment_id,
                    raw: Bytes::from(raw),
                    source_server_idx: None,
                    exclude_servers: Vec::new(),
                });
                pipeline.pump_decode_queue();
                let done = pipeline.decode_done_rx.recv().await.unwrap();
                let DecodeDone::Success { ref result, .. } = done else {
                    panic!("CRC damage must retain decoded bytes")
                };
                assert_eq!(result.part_crc_verified, valid, "{adapter}");
                assert_eq!(
                    matches!(result.data, DecodedChunk::Pooled(_)),
                    adapter == "pooled"
                );
                pipeline.handle_decode_done(done).await;
                drop(held);
            }
            if !valid {
                let retry = pipeline.retry_rx.recv().await.unwrap();
                assert_eq!(retry.work.segment_id, segment_id);
                assert!(
                    !pipeline.jobs[&file_id.job_id]
                        .assembly
                        .file(file_id)
                        .unwrap()
                        .has_segment(0)
                );
            }
        }
        assert_eq!(std::fs::read(path).unwrap(), b"data");
        assert!(
            !pipeline.jobs[&file_id.job_id]
                .assembly
                .file(file_id)
                .unwrap()
                .requires_file_verification()
        );
        assert_eq!(pipeline.metrics.crc_errors.load(Ordering::Relaxed), 1);
        assert_eq!(
            pipeline.metrics.segments_committed.load(Ordering::Relaxed),
            1
        );
        assert_eq!(pipeline.metrics.bytes_committed.load(Ordering::Relaxed), 4);
    }
}
